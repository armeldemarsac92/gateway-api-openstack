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
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
func CollectBackendTLSInputs(ctx context.Context, reader client.Reader, input *model.Input, ancestor *backendtls.GatewayAncestor, bundles *backendtls.ClusterTrustBundles, inlineEndpoints bool) error {
	return NewBackendTLSObserver(reader, bundles).CollectServiceInputs(ctx, input, ancestor, inlineEndpoints)
}

// CollectServiceInputs reuses the observer's CA inventory for authorized Service
// ports when the same snapshot also contains XBackends.
func (o *BackendTLSObserver) CollectServiceInputs(ctx context.Context, input *model.Input, ancestor *backendtls.GatewayAncestor, inlineEndpoints bool) error {
	input.BackendTLSPolicies = make(map[model.ServicePortKey]backendtls.Selection)
	services := make(map[types.NamespacedName]*corev1.Service, len(input.Services))
	for i := range input.Services {
		service := &input.Services[i]
		if service.DeletionTimestamp.IsZero() {
			services[client.ObjectKeyFromObject(service)] = service
		}
	}
	type use struct {
		service          *corev1.Service
		port             *corev1.ServicePort
		inline, ordinary bool
	}
	uses := map[model.ServicePortKey]use{}
	for i := range input.Routes {
		route := &input.Routes[i]
		kind := model.HTTPProfileRouteKind(route)
		add := func(ref gatewayv1.BackendObjectReference, inline bool) {
			key, serviceRef := model.BackendServiceReference(route.Namespace, ref)
			if !serviceRef || !model.BackendReferenceAllowedFor(kind, route.Namespace, key, input.ReferenceGrants) {
				return
			}
			service := services[key]
			if service == nil || service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeExternalName {
				return
			}
			port, diagnostic := model.ResolveTCPServicePort(service, ref.Port)
			if diagnostic != nil {
				return
			}
			target := model.ServicePortKey{Service: key, Port: port.Port}
			current := uses[target]
			current.service, current.port = service, port
			current.inline, current.ordinary = current.inline || inline, current.ordinary || !inline
			uses[target] = current
		}
		for _, rule := range route.Spec.Rules {
			inline := inlineEndpoints && model.RouteRulePersistence(rule) != nil
			for _, ref := range rule.BackendRefs {
				positive := ptr.Deref(ref.Weight, 1) != 0
				if positive || kind == model.GRPCRouteKind {
					add(ref.BackendObjectReference, inline && positive)
				}
			}
			for _, ref := range model.RequestMirrorReferences(rule) {
				add(ref, false)
			}
		}
	}
	var trustErr error
	var protectedInline bool
	for target, use := range uses {
		selection, err := o.observeService(ctx, use.service, use.port, ancestor, use.inline)
		if err != nil && !errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable) {
			return err
		}
		input.BackendTLSPolicies[target] = selection
		protectedInline = protectedInline || use.inline && selection.Winner != nil
		// Two rules can use the same Service port and have distinct pools.
		if use.ordinary && err != nil {
			trustErr = err
		}
	}
	if trustErr != nil && !protectedInline {
		return trustErr
	}
	input.BackendTrustWithdrawalOnly = trustErr != nil
	return nil
}

// BackendTLSObserver shares a live policy and CA inventory across resolved Service ports.
type BackendTLSObserver struct {
	reader               client.Reader
	policies             map[string][]gatewayv1.BackendTLSPolicy
	bundles              *backendtls.ClusterTrustBundles
	cas                  backendtls.CAInputs
	configMapUnavailable map[types.NamespacedName]bool
}

func NewBackendTLSObserver(reader client.Reader, bundles *backendtls.ClusterTrustBundles) *BackendTLSObserver {
	observer := &BackendTLSObserver{
		reader: reader, bundles: bundles, policies: map[string][]gatewayv1.BackendTLSPolicy{},
		cas:                  backendtls.CAInputs{ConfigMaps: map[types.NamespacedName]*corev1.ConfigMap{}},
		configMapUnavailable: make(map[types.NamespacedName]bool),
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
	return o.observe(ctx, service, port, ancestor, false)
}

// ObserveWithPendingCA keeps unavailable CA observations scoped to each Service.
func (o *BackendTLSObserver) ObserveWithPendingCA(ctx context.Context, service *corev1.Service, port *corev1.ServicePort, ancestor *backendtls.GatewayAncestor) (backendtls.Selection, error) {
	return o.observe(ctx, service, port, ancestor, true)
}

func (o *BackendTLSObserver) observe(ctx context.Context, service *corev1.Service, port *corev1.ServicePort, ancestor *backendtls.GatewayAncestor, allowPendingCA bool) (backendtls.Selection, error) {
	selection, err := o.observeService(ctx, service, port, ancestor, allowPendingCA)
	if allowPendingCA && errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable) {
		return selection, nil
	}
	if err != nil {
		return backendtls.Selection{}, err
	}
	return selection, nil
}

func (o *BackendTLSObserver) observeService(ctx context.Context, service *corev1.Service, port *corev1.ServicePort, ancestor *backendtls.GatewayAncestor, allowPendingCA bool) (backendtls.Selection, error) {
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
		if supported {
			if err := o.observeCA(ctx, key); err != nil {
				return backendtls.Selection{}, err
			}
		}
	}
	inputs := o.cas
	if allowPendingCA {
		inputs.UnreadableConfigMaps = o.configMapUnavailable
	}
	selection := backendtls.Evaluate(service, port, items, inputs)
	if selection.Winner == nil {
		return selection, nil
	}
	for i := range items {
		policy := &items[i]
		if policy.Name != selection.Winner.Key.Name {
			continue
		}
		if ancestor != nil {
			selection.Winner.AncestorSlot = ancestor.Slot(policy)
		}
		if key, ok := backendtls.CAKey(policy, o.bundles != nil); ok && key.Namespace == "" {
			return selection, o.cas.ClusterTrustBundles[key.Name].Unavailable
		}
		break
	}
	return selection, nil
}

// ObserveValidation evaluates inline backend TLS without looking up Service policies.
func (o *BackendTLSObserver) ObserveValidation(ctx context.Context, namespace string, validation gatewayv1.BackendTLSPolicyValidation) (backendtls.ValidationResult, error) {
	if key, supported := backendtls.ValidationCAKey(namespace, validation, o.bundles != nil); supported {
		if err := o.observeCA(ctx, key); err != nil {
			return backendtls.ValidationResult{}, err
		}
		if o.configMapUnavailable[key] {
			return backendtls.ValidationResult{Pending: true, Reason: "Pending", Message: fmt.Sprintf("Waiting to read the referenced CA ConfigMap %s", key)}, nil
		}
	}
	return backendtls.EvaluateValidation(namespace, validation, o.cas), nil
}

func (o *BackendTLSObserver) observeCA(ctx context.Context, key types.NamespacedName) error {
	if key.Namespace == "" {
		if _, observed := o.cas.ClusterTrustBundles[key.Name]; !observed {
			bundle, err := o.bundles.Read(ctx, key.Name)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			bundle.Unavailable = err
			o.cas.ClusterTrustBundles[key.Name] = bundle
		}
		return nil
	}
	if _, observed := o.cas.ConfigMaps[key]; observed {
		return nil
	}
	var configMap corev1.ConfigMap
	if err := o.reader.Get(ctx, key, &configMap); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A known TLS requirement still governs this backend when its CA
		// cannot be read. Watches and resyncs can restore it independently.
		o.cas.ConfigMaps[key] = nil
		if !apierrors.IsNotFound(err) {
			o.configMapUnavailable[key] = true
		}
	} else {
		o.cas.ConfigMaps[key] = &configMap
	}
	return nil
}
