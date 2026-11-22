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

package model

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ListenerSetRefKey resolves an HTTPRoute parentRef that targets a v1 ListenerSet.
func ListenerSetRefKey(ref gatewayv1.ParentReference, namespace string) types.NamespacedName {
	if ref.Kind == nil || *ref.Kind != "ListenerSet" || ref.Group != nil && *ref.Group != gatewayv1.GroupName || ref.Name == "" {
		return types.NamespacedName{}
	}
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	return types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}
}

// ListenerSetParentKey resolves the Gateway named in a ListenerSet parentRef.
func ListenerSetParentKey(set *gatewayv1.ListenerSet) types.NamespacedName {
	ref := set.Spec.ParentRef
	if ref.Name == "" || ref.Group != nil && *ref.Group != gatewayv1.GroupName || ref.Kind != nil && *ref.Kind != "Gateway" {
		return types.NamespacedName{}
	}
	namespace := set.Namespace
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	return types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}
}

// AllowsListenerSetNamespace applies the Gateway's ListenerSet admission policy.
func AllowsListenerSetNamespace(gateway *gatewayv1.Gateway, namespace string, observed *corev1.Namespace) bool {
	if GatewayDisallowsListenerSets(gateway) {
		return false
	}
	policy := gateway.Spec.AllowedListeners.Namespaces
	switch *policy.From {
	case gatewayv1.NamespacesFromSame:
		return gateway.Namespace == namespace
	case gatewayv1.NamespacesFromAll:
		return true
	case gatewayv1.NamespacesFromSelector:
		if observed == nil || observed.Name != namespace || !observed.DeletionTimestamp.IsZero() || observed.Status.Phase == corev1.NamespaceTerminating || policy.Selector == nil {
			return false
		}
		selector, err := metav1.LabelSelectorAsSelector(policy.Selector)
		return err == nil && selector.Matches(labels.Set(observed.Labels))
	}
	return false
}

// GatewayDisallowsListenerSets recognizes the deny-by-default v1 policy and its
// explicitly defaulted forms. Other policies require evaluating the ListenerSet.
func GatewayDisallowsListenerSets(gateway *gatewayv1.Gateway) bool {
	policy := gateway.Spec.AllowedListeners
	return policy == nil || policy.Namespaces == nil || policy.Namespaces.From == nil ||
		*policy.Namespaces.From == gatewayv1.NamespacesFromNone
}

// ListenerSetRefObservesRouteNamespace identifies label-sensitive candidate
// sections even while authorization is denied.
func ListenerSetRefObservesRouteNamespace(set *gatewayv1.ListenerSet, ref gatewayv1.ParentReference) bool {
	return slices.ContainsFunc(set.Spec.Listeners, func(entry gatewayv1.ListenerEntry) bool {
		return entry.Protocol == gatewayv1.HTTPProtocolType &&
			(ref.SectionName == nil || *ref.SectionName == entry.Name) &&
			(ref.Port == nil || *ref.Port == entry.Port) &&
			ObservesRouteNamespace(&gatewayv1.Listener{AllowedRoutes: entry.AllowedRoutes})
	})
}
