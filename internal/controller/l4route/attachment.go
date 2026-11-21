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
	ref      gatewayv1.ParentReference
	gateway  *gatewayv1.Gateway
	class    *gatewayv1.GatewayClass
	listener *gatewayv1.Listener
	rejected model.Diagnostic
}

func gatewayKey(route *transportRoute, parent gatewayv1.ParentReference) client.ObjectKey {
	namespace := route.GetNamespace()
	if parent.Namespace != nil {
		namespace = string(*parent.Namespace)
	}
	return client.ObjectKey{Namespace: namespace, Name: string(parent.Name)}
}

func (r *Reconciler) managedParents(ctx context.Context, route *transportRoute) ([]routeParent, error) {
	var parents []routeParent
	for _, ref := range route.parentRefs {
		if !controller.IsGatewayParentRef(ref) {
			continue
		}
		gateway := &gatewayv1.Gateway{}
		if err := r.APIReader.Get(ctx, gatewayKey(route, ref), gateway); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		class, err := r.gatewayClass(ctx, gateway)
		if err != nil {
			return nil, err
		}
		if class == nil {
			continue
		}
		parent := routeParent{ref: ref, gateway: gateway, class: class}
		parent.listener, parent.rejected, err = r.attachment(ctx, route, gateway, ref)
		if err != nil {
			return nil, err
		}
		parents = append(parents, parent)
	}
	return parents, nil
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

func (r *Reconciler) attachment(ctx context.Context, route *transportRoute, gateway *gatewayv1.Gateway, ref gatewayv1.ParentReference) (*gatewayv1.Listener, model.Diagnostic, error) {
	noMatch := model.Diagnostic{Reason: string(gatewayv1.RouteReasonNoMatchingParent), Message: "parent reference does not select a live Gateway listener"}
	if !gateway.DeletionTimestamp.IsZero() {
		return nil, noMatch, nil
	}
	selected := false
	for i := range gateway.Spec.Listeners {
		candidate := &gateway.Spec.Listeners[i]
		if !parentSelects(ref, candidate) {
			continue
		}
		selected = true
		allowed, err := r.listenerAllows(ctx, candidate, gateway.Namespace, route.GetNamespace())
		if err != nil {
			return nil, model.Diagnostic{}, err
		}
		if !allowed {
			continue
		}
		listener, err := gatewaycontroller.Validate(gateway)
		if err != nil || listener.Protocol != r.protocol() || listener.Name != candidate.Name {
			return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: fmt.Sprintf("the Gateway must support the selected %s listener", r.protocol())}, nil
		}
		return listener, model.Diagnostic{}, nil
	}
	if !selected {
		return nil, noMatch, nil
	}
	return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonNotAllowedByListeners), Message: fmt.Sprintf("no selected listener allows this %s kind and namespace", r.kind())}, nil
}

func parentSelects(ref gatewayv1.ParentReference, listener *gatewayv1.Listener) bool {
	return (ref.SectionName == nil || *ref.SectionName == listener.Name) && (ref.Port == nil || *ref.Port == listener.Port)
}

func (r *Reconciler) listenerAllows(ctx context.Context, listener *gatewayv1.Listener, gatewayNamespace, routeNamespace string) (bool, error) {
	if listener.Protocol != r.protocol() {
		return false, nil
	}
	allowed := listener.AllowedRoutes
	if allowed != nil && len(allowed.Kinds) != 0 && !slices.ContainsFunc(allowed.Kinds, func(kind gatewayv1.RouteGroupKind) bool {
		return kind.Kind == r.kind() && (kind.Group == nil || *kind.Group == gatewayv1.GroupName)
	}) {
		return false, nil
	}
	var namespaces []corev1.Namespace
	if model.ObservesRouteNamespace(listener) {
		var namespace corev1.Namespace
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: routeNamespace}, &namespace); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		namespaces = []corev1.Namespace{namespace}
	}
	return model.AllowsRouteNamespace(listener, gatewayNamespace, routeNamespace, namespaces), nil
}

func listenerCandidates(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, routes []*transportRoute) []*transportRoute {
	var candidates []*transportRoute
	key := client.ObjectKeyFromObject(gateway)
	for _, route := range routes {
		if !route.GetDeletionTimestamp().IsZero() || len(route.parentRefs) != 1 {
			continue
		}
		ref := route.parentRefs[0]
		if controller.IsGatewayParentRef(ref) && gatewayKey(route, ref) == key && parentSelects(ref, listener) {
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
func (r *Reconciler) selectedRoute(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, candidates []*transportRoute) (*transportRoute, error) {
	for _, route := range candidates {
		allowed, err := r.listenerAllows(ctx, listener, gateway.Namespace, route.GetNamespace())
		if allowed || err != nil {
			return route, err
		}
	}
	return nil, nil
}
