/*
Copyright 2026 The gateway-api-openstack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package graph

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// CollectBackendTLSInputs observes policies for authorized Service ports. The
// caller uses an uncached reader both for planning and just before mutation.
// A namespaced policy list finds concurrent winners that indexed lookups could miss.
func CollectBackendTLSInputs(ctx context.Context, reader client.Reader, input *model.Input, ancestor *backendtls.GatewayAncestor, bundles *backendtls.ClusterTrustBundles) error {
	input.BackendTLSPolicies = make(map[model.ServicePortKey]backendtls.Selection)
	services := make(map[types.NamespacedName]*corev1.Service, len(input.Services))
	for i := range input.Services {
		service := &input.Services[i]
		if service.DeletionTimestamp.IsZero() {
			services[client.ObjectKeyFromObject(service)] = service
		}
	}
	observer := NewBackendTLSObserver(reader, bundles)
	for i := range input.Routes {
		route := &input.Routes[i]
		kind := model.HTTPProfileRouteKind(route)
		for _, rule := range route.Spec.Rules {
			for _, backend := range rule.BackendRefs {
				if ptr.Deref(backend.Weight, 1) == 0 && kind != model.GRPCRouteKind {
					continue
				}
				key, serviceRef := model.BackendServiceReference(route.Namespace, backend.BackendObjectReference)
				if !serviceRef || !model.BackendReferenceAllowedFor(kind, route.Namespace, key, input.ReferenceGrants) {
					continue
				}
				service := services[key]
				if service == nil || service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeExternalName {
					continue
				}
				port, diagnostic := model.ResolveTCPServicePort(service, backend.Port)
				if diagnostic != nil {
					continue
				}
				target := model.ServicePortKey{Service: key, Port: port.Port}
				selection, observed := input.BackendTLSPolicies[target]
				if !observed {
					var err error
					selection, err = observer.Observe(ctx, service, port, ancestor)
					if err != nil {
						return err
					}
				}
				input.BackendTLSPolicies[target] = selection
			}
		}
	}
	return nil
}

// BackendTLSObserver shares a live policy and CA inventory across resolved Service ports.
type BackendTLSObserver struct {
	reader   client.Reader
	policies map[string][]gatewayv1.BackendTLSPolicy
	bundles  *backendtls.ClusterTrustBundles
	cas      backendtls.CAInputs
}

func NewBackendTLSObserver(reader client.Reader, bundles *backendtls.ClusterTrustBundles) *BackendTLSObserver {
	observer := &BackendTLSObserver{
		reader: reader, bundles: bundles, policies: map[string][]gatewayv1.BackendTLSPolicy{},
		cas: backendtls.CAInputs{ConfigMaps: map[types.NamespacedName]*corev1.ConfigMap{}},
	}
	if bundles != nil {
		observer.cas.ClusterTrustBundles = make(map[string]backendtls.CAObservation)
	}
	return observer
}

func (o *BackendTLSObserver) policiesInNamespace(ctx context.Context, namespace string) ([]gatewayv1.BackendTLSPolicy, error) {
	items, observed := o.policies[namespace]
	if !observed {
		var list gatewayv1.BackendTLSPolicyList
		if err := o.reader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
			return nil, fmt.Errorf("list BackendTLSPolicies in namespace %s: %w", namespace, err)
		}
		if list.Continue != "" {
			return nil, fmt.Errorf("incomplete BackendTLSPolicy list in namespace %s", namespace)
		}
		items = list.Items
		o.policies[namespace] = items
	}
	return items, nil
}

// HasPolicy checks if a Service port requires backend authentication, without
// reading a CA when the caller cannot originate TLS.
func (o *BackendTLSObserver) HasPolicy(ctx context.Context, service *corev1.Service, port *corev1.ServicePort) (bool, error) {
	items, err := o.policiesInNamespace(ctx, service.Namespace)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(items, func(policy gatewayv1.BackendTLSPolicy) bool {
		return policy.DeletionTimestamp.IsZero() && backendtls.AppliesToServicePort(&policy, service, port)
	}), nil
}

// Observe requires the caller to authorize the Service reference first.
func (o *BackendTLSObserver) Observe(ctx context.Context, service *corev1.Service, port *corev1.ServicePort, ancestor *backendtls.GatewayAncestor) (backendtls.Selection, error) {
	items, err := o.policiesInNamespace(ctx, service.Namespace)
	if err != nil {
		return backendtls.Selection{}, err
	}
	for i := range items {
		policy := &items[i]
		if !policy.DeletionTimestamp.IsZero() || !backendtls.AppliesToServicePort(policy, service, port) {
			continue
		}
		key, supported := backendtls.CAKey(policy, o.bundles != nil)
		if !supported {
			continue
		}
		if key.Namespace == "" {
			if _, observed := o.cas.ClusterTrustBundles[key.Name]; !observed {
				bundle, err := o.bundles.Read(ctx, key.Name)
				if err != nil {
					return backendtls.Selection{}, err
				}
				o.cas.ClusterTrustBundles[key.Name] = bundle
			}
			continue
		}
		if _, observed := o.cas.ConfigMaps[key]; observed {
			continue
		}
		var configMap corev1.ConfigMap
		if err := o.reader.Get(ctx, key, &configMap); err != nil {
			if ctx.Err() != nil {
				return backendtls.Selection{}, ctx.Err()
			}
			// A known policy still governs this port when its CA cannot be
			// read. The mutation fence repeats the read, and later watches
			// and resyncs can restore this backend independently of its peers.
			o.cas.ConfigMaps[key] = nil
		} else {
			o.cas.ConfigMaps[key] = &configMap
		}
	}
	selection := backendtls.Evaluate(service, port, items, o.cas)
	if ancestor != nil && selection.Winner != nil {
		for i := range items {
			if items[i].Name == selection.Winner.Key.Name {
				selection.Winner.AncestorSlot = ancestor.Slot(&items[i])
				break
			}
		}
	}
	return selection, nil
}
