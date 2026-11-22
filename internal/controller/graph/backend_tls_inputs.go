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
func CollectBackendTLSInputs(ctx context.Context, reader client.Reader, input *model.Input, ancestor *backendtls.GatewayAncestor) error {
	input.BackendTLSPolicies = make(map[model.ServicePortKey]backendtls.Selection)
	services := make(map[types.NamespacedName]*corev1.Service, len(input.Services))
	for i := range input.Services {
		service := &input.Services[i]
		if service.DeletionTimestamp.IsZero() {
			services[client.ObjectKeyFromObject(service)] = service
		}
	}
	policies := map[string][]gatewayv1.BackendTLSPolicy{}
	cas := map[types.NamespacedName]*corev1.ConfigMap{}
	for i := range input.Routes {
		route := &input.Routes[i]
		kind := model.HTTPProfileRouteKind(route)
		for _, rule := range route.Spec.Rules {
			for _, backend := range rule.BackendRefs {
				if ptr.Deref(backend.Weight, 1) == 0 {
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
				if _, observed := input.BackendTLSPolicies[target]; observed {
					continue
				}
				selection, err := observeBackendTLS(ctx, reader, service, port, policies, cas)
				if err != nil {
					return err
				}
				if ancestor != nil && selection.Winner != nil {
					for i := range policies[key.Namespace] {
						policy := &policies[key.Namespace][i]
						if policy.Name == selection.Winner.Key.Name {
							selection.Winner.AncestorSlot = ancestor.Slot(policy)
							break
						}
					}
				}
				input.BackendTLSPolicies[target] = selection
			}
		}
	}
	return nil
}

func observeBackendTLS(ctx context.Context, reader client.Reader, service *corev1.Service, port *corev1.ServicePort, policies map[string][]gatewayv1.BackendTLSPolicy, cas map[types.NamespacedName]*corev1.ConfigMap) (backendtls.Selection, error) {
	namespace := service.Namespace
	items, observed := policies[namespace]
	if !observed {
		var list gatewayv1.BackendTLSPolicyList
		if err := reader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
			return backendtls.Selection{}, fmt.Errorf("list BackendTLSPolicies in namespace %s: %w", namespace, err)
		}
		if list.Continue != "" {
			return backendtls.Selection{}, fmt.Errorf("incomplete BackendTLSPolicy list in namespace %s", namespace)
		}
		items = list.Items
		policies[namespace] = items
	}
	for i := range items {
		policy := &items[i]
		if !policy.DeletionTimestamp.IsZero() || !backendtls.AppliesToServicePort(policy, service, port) {
			continue
		}
		key, supported := backendtls.ConfigMapKey(policy)
		if !supported {
			continue
		}
		if _, observed := cas[key]; observed {
			continue
		}
		var configMap corev1.ConfigMap
		if err := reader.Get(ctx, key, &configMap); err != nil {
			if ctx.Err() != nil {
				return backendtls.Selection{}, ctx.Err()
			}
			// A known policy still governs this port when its CA cannot be
			// read. The mutation fence repeats the read, and later watches
			// and resyncs can restore this backend independently of its peers.
			cas[key] = nil
		} else {
			cas[key] = &configMap
		}
	}
	return backendtls.Evaluate(service, port, items, cas), nil
}

// HasValidBackendTLSWinner reports whether any backend can use authenticated TLS.
func HasValidBackendTLSWinner(selections map[model.ServicePortKey]backendtls.Selection) bool {
	for _, selection := range selections {
		if selection.Winner != nil && selection.Winner.Validation != nil && selection.Winner.AncestorSlot == backendtls.AncestorSlotHeld {
			return true
		}
	}
	return false
}
