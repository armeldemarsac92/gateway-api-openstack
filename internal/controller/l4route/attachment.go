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

package l4route

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeParent struct {
	ref       gatewayv1.ParentReference
	gateway   *gatewayv1.Gateway
	class     *gatewayv1.GatewayClass
	listeners []*gatewayv1.Listener
	rejected  model.Diagnostic
}

func gatewayKey(route *transportRoute, parent gatewayv1.ParentReference) client.ObjectKey {
	namespace := route.GetNamespace()
	if parent.Namespace != nil {
		namespace = string(*parent.Namespace)
	}
	return client.ObjectKey{Namespace: namespace, Name: string(parent.Name)}
}

func (r *Reconciler) managedParents(ctx context.Context, route *transportRoute, key client.ObjectKey) ([]routeParent, string, error) {
	gateway := &gatewayv1.Gateway{}
	if err := r.APIReader.Get(ctx, key, gateway); err != nil {
		return nil, "", client.IgnoreNotFound(err)
	}
	uid := string(gateway.UID)
	refs := r.gatewayRefs(route, gateway)
	if len(refs) == 0 {
		return nil, uid, nil
	}
	class, err := r.gatewayClass(ctx, gateway)
	if err != nil || class == nil {
		return nil, uid, err
	}
	var parents []routeParent
	namespaces := make(routeNamespaceCache)
	for _, ref := range refs {
		parent := routeParent{ref: ref, gateway: gateway, class: class}
		parent.listeners, parent.rejected, err = r.attachment(ctx, route, gateway, ref, namespaces)
		if err != nil {
			return nil, uid, err
		}
		parents = append(parents, parent)
	}
	return parents, uid, nil
}

func (r *Reconciler) gatewayRefs(route *transportRoute, gateway *gatewayv1.Gateway) []gatewayv1.ParentReference {
	var selected []gatewayv1.ParentReference
	key := client.ObjectKeyFromObject(gateway)
	for _, ref := range model.EffectiveParentRefs(route.commonSpec, route.GetNamespace(), gateway, r.Config.DefaultGatewaysEnabled()) {
		if controller.IsGatewayParentRef(ref) && gatewayKey(route, ref) == key {
			selected = append(selected, ref)
		}
	}
	return selected
}

func (r *Reconciler) gatewayClass(ctx context.Context, gateway *gatewayv1.Gateway) (*gatewayv1.GatewayClass, error) {
	class := &gatewayv1.GatewayClass{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(gateway.Spec.GatewayClassName)}, class); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return class, nil
}

func (r *Reconciler) attachment(ctx context.Context, route *transportRoute, gateway *gatewayv1.Gateway, ref gatewayv1.ParentReference, namespaces routeNamespaceCache) ([]*gatewayv1.Listener, model.Diagnostic, error) {
	noMatch := model.Diagnostic{Reason: string(gatewayv1.RouteReasonNoMatchingParent), Message: "parent reference does not select a live Gateway listener"}
	unsupported := model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: fmt.Sprintf("the Gateway must support the selected %s listener", r.protocol())}
	if !gateway.DeletionTimestamp.IsZero() {
		return nil, noMatch, nil
	}
	selected := false
	unsupportedSelection := false
	var admitted, supported []*gatewayv1.Listener
	for i := range gateway.Spec.Listeners {
		candidate := &gateway.Spec.Listeners[i]
		if !parentSelects(ref, candidate) {
			continue
		}
		selected = true
		allowed, err := r.listenerAllows(ctx, candidate, gateway.Namespace, route.GetNamespace(), namespaces)
		if err != nil {
			return nil, model.Diagnostic{}, err
		}
		if !allowed {
			continue
		}
		if supported == nil {
			supported, err = gatewaycontroller.SupportedListenersForConfig(r.Config, gateway)
			if err != nil {
				return nil, unsupported, nil
			}
		}
		if !slices.Contains(supported, candidate) {
			unsupportedSelection = true
			continue
		}
		admitted = append(admitted, candidate)
	}
	if len(admitted) != 0 {
		return admitted, model.Diagnostic{}, nil
	}
	if unsupportedSelection {
		return nil, unsupported, nil
	}
	if !selected {
		return nil, noMatch, nil
	}
	return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonNotAllowedByListeners), Message: fmt.Sprintf("no selected listener allows this %s kind and namespace", r.kind())}, nil
}

func parentSelects(ref gatewayv1.ParentReference, listener *gatewayv1.Listener) bool {
	return (ref.SectionName == nil || *ref.SectionName == listener.Name) && (ref.Port == nil || *ref.Port == listener.Port)
}

type routeNamespaceCache map[string][]corev1.Namespace

func (r *Reconciler) listenerAllows(ctx context.Context, listener *gatewayv1.Listener, gatewayNamespace, routeNamespace string, namespaces routeNamespaceCache) (bool, error) {
	if listener.Protocol != r.protocol() {
		return false, nil
	}
	allowed := listener.AllowedRoutes
	if allowed != nil && len(allowed.Kinds) != 0 && !slices.ContainsFunc(allowed.Kinds, func(kind gatewayv1.RouteGroupKind) bool {
		return kind.Kind == r.kind() && (kind.Group == nil || *kind.Group == gatewayv1.GroupName)
	}) {
		return false, nil
	}
	if model.ObservesRouteNamespace(listener) {
		if _, checked := namespaces[routeNamespace]; !checked {
			var namespace corev1.Namespace
			err := r.APIReader.Get(ctx, client.ObjectKey{Name: routeNamespace}, &namespace)
			if err == nil {
				namespaces[routeNamespace] = []corev1.Namespace{namespace}
			} else if apierrors.IsNotFound(err) {
				namespaces[routeNamespace] = nil
			} else {
				return false, err
			}
		}
	}
	return model.AllowsRouteNamespace(listener, gatewayNamespace, routeNamespace, namespaces[routeNamespace]), nil
}

func (r *Reconciler) listenerCandidates(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, routes []*transportRoute) []*transportRoute {
	var candidates []*transportRoute
	for _, route := range routes {
		if !route.GetDeletionTimestamp().IsZero() {
			continue
		}
		if slices.ContainsFunc(r.gatewayRefs(route, gateway), func(ref gatewayv1.ParentReference) bool { return parentSelects(ref, listener) }) {
			candidates = append(candidates, route)
		}
	}
	slices.SortFunc(candidates, func(left, right *transportRoute) int {
		created := left.GetCreationTimestamp()
		return cmp.Or(created.Compare(right.GetCreationTimestamp().Time), cmp.Compare(left.GetNamespace(), right.GetNamespace()), cmp.Compare(left.GetName(), right.GetName()))
	})
	return candidates
}

// selectedRoute uses only attachment inputs. An unusable oldest route still
// wins the listener and prevents traffic from leaking to a younger route.
func (r *Reconciler) selectedRoute(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, candidates []*transportRoute, namespaces routeNamespaceCache) (*transportRoute, error) {
	for _, route := range candidates {
		allowed, err := r.listenerAllows(ctx, listener, gateway.Namespace, route.GetNamespace(), namespaces)
		if allowed || err != nil {
			return route, err
		}
	}
	return nil, nil
}
