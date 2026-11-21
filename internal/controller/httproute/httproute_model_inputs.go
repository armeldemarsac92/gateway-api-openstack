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

package httproute

import (
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// routeModelInputs keeps selection and lowering on the same request-local
// observation. The graph source supplies the complete current route inventory.
type routeModelInputs struct {
	input     model.Input
	logical   model.Gateway
	admission routeGraphAdmission
}

type routeGraphAdmission struct {
	bindings   []routeGraphBinding
	eligible   map[types.NamespacedName]routeGraphCandidate
	active     []gatewayv1.HTTPRoute
	namespaces []corev1.Namespace
}

type routeGraphCandidate struct {
	routeUID, namespaceUID types.UID
	section                gatewayv1.SectionName
}

func (r *Reconciler) observeRouteGraphAdmission(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (routeGraphAdmission, error) {
	identity := controller.GatewayIdentity(r.Config, gateway)
	http, _ := gatewaycontroller.HTTPRouteListeners(r.Config, gateway)
	admission := routeGraphAdmission{
		bindings: r.routeGraphBindings(routes, identity),
		eligible: make(map[types.NamespacedName]routeGraphCandidate),
	}
	byName := make(map[string][]corev1.Namespace)
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() || !structurallySupportedRoute(route) ||
			route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
			continue
		}
		ref, err := r.soleParentForManagedGateway(ctx, route, gateway)
		if err != nil {
			return routeGraphAdmission{}, err
		}
		if ref == nil {
			continue
		}
		parent := managedRouteParent{ref: *ref, gateway: gateway}
		candidates, err := routeParentCandidates(route, parent, http)
		if err != nil {
			continue
		}
		namespaces, observed := byName[route.Namespace]
		if !observed && slices.ContainsFunc(candidates, model.ObservesRouteNamespace) {
			namespaces, err = readRouteNamespaces(ctx, r.APIReader, candidates, route.Namespace)
			if err != nil {
				return routeGraphAdmission{}, err
			}
			byName[route.Namespace] = namespaces
			admission.namespaces = append(admission.namespaces, namespaces...)
		}
		selected, err := selectRouteAttachment(route, gateway, candidates, namespaces)
		if err != nil {
			continue
		}
		candidate := routeGraphCandidate{routeUID: route.UID, section: selected.Name}
		if len(namespaces) != 0 && model.ObservesRouteNamespace(selected) {
			candidate.namespaceUID = namespaces[0].UID
		}
		admission.eligible[client.ObjectKeyFromObject(route)] = candidate
		if controller.RouteClaimsGateway(r.Config, route, identity) || requested != nil && route.UID == requested.UID && client.ObjectKeyFromObject(route) == client.ObjectKeyFromObject(requested) {
			admission.active = append(admission.active, *route)
		}
	}
	return admission, nil
}

func (r *Reconciler) collectGraphRouteModelInputs(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (*routeModelInputs, error) {
	admission, err := r.observeRouteGraphAdmission(ctx, gateway, routes, requested)
	if err != nil {
		return nil, err
	}
	inputs, err := r.collectRouteModelInputs(ctx, r.APIReader, gateway, admission.active, admission.namespaces)
	if err != nil {
		return nil, err
	}
	inputs.admission = admission
	return inputs, nil
}

func (r *Reconciler) validateGraphRouteModel(ctx context.Context, inputs *routeModelInputs, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) error {
	current, err := r.observeRouteGraphAdmission(ctx, inputs.input.Gateway, routes, requested)
	if err != nil {
		return err
	}
	if !maps.Equal(inputs.admission.eligible, current.eligible) ||
		!slices.Equal(inputs.admission.bindings, current.bindings) ||
		!slices.EqualFunc(inputs.admission.active, current.active, func(a, b gatewayv1.HTTPRoute) bool {
			return a.UID == b.UID && a.Generation == b.Generation && a.CreationTimestamp.Equal(&b.CreationTimestamp) &&
				equality.Semantic.DeepEqual(a.Spec, b.Spec)
		}) {
		return errHTTPRouteChanged
	}
	// Namespace authorization and admitted route specs were checked above.
	// Compare only the backend dependencies used to build this model here.
	return inputs.validateBackends(ctx, r.APIReader)
}

func (r *Reconciler) collectRouteModelInputs(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace) (*routeModelInputs, error) {
	input, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, gateway, routes, namespaces)
	if err != nil {
		return nil, err
	}
	if r.Config.OperatorHTTPS && slices.ContainsFunc(gateway.Spec.Listeners, func(l gatewayv1.Listener) bool { return l.Protocol == gatewayv1.HTTPSProtocolType }) {
		listeners, err := gatewaycontroller.HTTPRouteListeners(r.Config, gateway)
		if err != nil {
			return nil, err
		}
		input.Gateway = gateway.DeepCopy()
		input.Gateway.Spec.Listeners = make([]gatewayv1.Listener, 0, len(listeners))
		for _, listener := range listeners {
			input.Gateway.Spec.Listeners = append(input.Gateway.Spec.Listeners, *listener)
		}
		input.OperatorHTTPS = true
	}
	logical, err := model.Compile(input)
	if err != nil {
		return nil, fmt.Errorf("compile Gateway backend model: %w", err)
	}
	return &routeModelInputs{input: input, logical: logical}, nil
}

func (inputs *routeModelInputs) validateBackends(ctx context.Context, reader client.Reader) error {
	current, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, inputs.input.Gateway, inputs.input.Routes, inputs.input.Namespaces)
	if err != nil {
		return err
	}
	for _, route := range inputs.input.Routes {
		ref := route.Spec.Rules[0].BackendRefs[0].BackendObjectReference
		key, _ := model.BackendServiceReference(route.Namespace, ref)
		if model.BackendReferenceAllowed(route.Namespace, key, inputs.input.ReferenceGrants) !=
			model.BackendReferenceAllowed(route.Namespace, key, current.ReferenceGrants) {
			return errHTTPRouteChanged
		}
	}
	if !equality.Semantic.DeepEqual(inputs.input.Services, current.Services) ||
		!equality.Semantic.DeepEqual(inputs.input.EndpointSlices, current.EndpointSlices) {
		return errHTTPRouteChanged
	}
	return nil
}

func (inputs *routeModelInputs) service(key types.NamespacedName) *corev1.Service {
	for i := range inputs.input.Services {
		service := &inputs.input.Services[i]
		if client.ObjectKeyFromObject(service) == key {
			return service
		}
	}
	return nil
}

func (inputs *routeModelInputs) endpointSlices(key types.NamespacedName) []discoveryv1.EndpointSlice {
	var result []discoveryv1.EndpointSlice
	for _, slice := range inputs.input.EndpointSlices {
		if slice.Namespace == key.Namespace && slice.Labels[discoveryv1.LabelServiceName] == key.Name {
			result = append(result, slice)
		}
	}
	return result
}

// routeFragment preserves all rules and backends of one route and the precedence
// assigned to its matches across the entire listener.
func (inputs *routeModelInputs) routeFragment(route *gatewayv1.HTTPRoute) model.Gateway {
	logical := inputs.logical
	logical.Routes = nil
	for _, candidate := range inputs.logical.Routes {
		if candidate.Source.UID == route.UID && candidate.Source.Namespace == route.Namespace && candidate.Source.Name == route.Name {
			logical.Routes = []model.Route{candidate}
			break
		}
	}
	logical.Listeners = slices.Clone(logical.Listeners)
	// The compiler orders each logical section independently. The physical
	// listener needs one distinct rank while preserving each section's order.
	sectionOffset := 0
	for i := range logical.Listeners {
		listener := &logical.Listeners[i]
		listener.Rules = nil
		for _, rule := range inputs.logical.Listeners[i].Rules {
			if rule.Route.UID == route.UID && rule.Route.Namespace == route.Namespace && rule.Route.Name == route.Name {
				rule.Precedence += sectionOffset
				listener.Rules = append(listener.Rules, rule)
			}
		}
		sectionOffset += len(inputs.logical.Listeners[i].Rules)
	}
	return logical
}
