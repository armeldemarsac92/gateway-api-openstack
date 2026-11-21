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

package tcproute

import (
	"cmp"
	"context"
	"errors"
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

func gatewayKey(route *gatewayv1.TCPRoute, parent gatewayv1.ParentReference) client.ObjectKey {
	namespace := route.Namespace
	if parent.Namespace != nil {
		namespace = string(*parent.Namespace)
	}
	return client.ObjectKey{Namespace: namespace, Name: string(parent.Name)}
}

func (r *Reconciler) managedParents(ctx context.Context, route *gatewayv1.TCPRoute) ([]routeParent, error) {
	var parents []routeParent
	for _, ref := range route.Spec.ParentRefs {
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

func (r *Reconciler) attachment(ctx context.Context, route *gatewayv1.TCPRoute, gateway *gatewayv1.Gateway, ref gatewayv1.ParentReference) (*gatewayv1.Listener, model.Diagnostic, error) {
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
		allowed, err := r.listenerAllows(ctx, candidate, gateway.Namespace, route.Namespace)
		if err != nil {
			return nil, model.Diagnostic{}, err
		}
		if !allowed {
			continue
		}
		listener, err := gatewaycontroller.Validate(gateway)
		if err != nil || listener.Protocol != gatewayv1.TCPProtocolType || listener.Name != candidate.Name {
			return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "the Gateway must support the selected TCP listener"}, nil
		}
		return listener, model.Diagnostic{}, nil
	}
	if !selected {
		return nil, noMatch, nil
	}
	return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonNotAllowedByListeners), Message: "no selected listener allows this TCPRoute kind and namespace"}, nil
}

func parentSelects(ref gatewayv1.ParentReference, listener *gatewayv1.Listener) bool {
	return (ref.SectionName == nil || *ref.SectionName == listener.Name) && (ref.Port == nil || *ref.Port == listener.Port)
}

func (r *Reconciler) listenerAllows(ctx context.Context, listener *gatewayv1.Listener, gatewayNamespace, routeNamespace string) (bool, error) {
	if listener.Protocol != gatewayv1.TCPProtocolType {
		return false, nil
	}
	allowed := listener.AllowedRoutes
	if allowed != nil && len(allowed.Kinds) != 0 && !slices.ContainsFunc(allowed.Kinds, func(kind gatewayv1.RouteGroupKind) bool {
		return kind.Kind == "TCPRoute" && (kind.Group == nil || *kind.Group == gatewayv1.GroupName)
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

func (r *Reconciler) listRoutes(ctx context.Context) ([]gatewayv1.TCPRoute, error) {
	var list gatewayv1.TCPRouteList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list TCPRoute attachments: %w", err)
	}
	if list.Continue != "" {
		return nil, errors.New("incomplete TCPRoute attachment list")
	}
	return list.Items, nil
}

// selectedRoute uses only attachment inputs. An unusable oldest route still
// wins the listener and prevents traffic from leaking to a younger route.
func (r *Reconciler) selectedRoute(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, routes []gatewayv1.TCPRoute) (*gatewayv1.TCPRoute, error) {
	var candidates []*gatewayv1.TCPRoute
	key := client.ObjectKeyFromObject(gateway)
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() || len(route.Spec.ParentRefs) != 1 {
			continue
		}
		ref := route.Spec.ParentRefs[0]
		if controller.IsGatewayParentRef(ref) && gatewayKey(route, ref) == key && parentSelects(ref, listener) {
			candidates = append(candidates, route)
		}
	}
	slices.SortFunc(candidates, func(left, right *gatewayv1.TCPRoute) int {
		return cmp.Or(left.CreationTimestamp.Compare(right.CreationTimestamp.Time), cmp.Compare(left.Namespace, right.Namespace), cmp.Compare(left.Name, right.Name))
	})
	for _, route := range candidates {
		allowed, err := r.listenerAllows(ctx, listener, gateway.Namespace, route.Namespace)
		if allowed || err != nil {
			return route, err
		}
	}
	return nil, nil
}
