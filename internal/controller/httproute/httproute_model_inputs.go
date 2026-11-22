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
	"cmp"
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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// routeModelInputs keeps selection and lowering on the same request-local
// observation. The graph source supplies the complete current route inventory.
type routeModelInputs struct {
	gateway                *gatewayv1.Gateway
	input                  model.Input
	logical                model.Gateway
	backendTLSAvailability backendTLSAvailability
	backendTLSAncestor     *backendtls.GatewayAncestor
	admission              routeGraphAdmission
}

type routeGraphAdmission struct {
	listenerSets     map[types.NamespacedName][]gatewayv1.Listener
	bindings         []routeGraphBinding
	eligible         map[routeObjectKey]routeGraphCandidate
	rejectedSections map[routeObjectKey][]model.ListenerSection
	active           []gatewayv1.HTTPRoute
	namespaces       []corev1.Namespace
}

func (a routeGraphAdmission) profileRejectedParent(key routeObjectKey) bool {
	_, eligible := a.eligible[key]
	return !eligible && len(a.rejectedSections[key]) != 0
}

type routeGraphCandidate struct {
	routeUID, namespaceUID types.UID
	sections               []sectionIdentity
}

type routeProfileCandidate struct {
	route     *gatewayv1.HTTPRoute
	selection routeSelection
	namespace types.UID
}

type routeObjectKey struct {
	kind gatewayv1.Kind
	key  types.NamespacedName
}

func routeKey(route *gatewayv1.HTTPRoute) routeObjectKey {
	return routeObjectKey{kind: model.HTTPProfileRouteKind(route), key: client.ObjectKeyFromObject(route)}
}

func (r *Reconciler) liveRouteGraphAdmission(ctx context.Context, gateway *gatewayv1.Gateway, route *gatewayv1.HTTPRoute) (routeGraphAdmission, error) {
	routes, err := r.liveGraphRoutes(ctx, controller.GatewayIdentity(r.Config, gateway))
	if err != nil {
		return routeGraphAdmission{}, err
	}
	return r.observeRouteGraphAdmission(ctx, gateway, routes, route)
}

func (r *Reconciler) observeRouteGraphAdmission(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (routeGraphAdmission, error) {
	identity := controller.GatewayIdentity(r.Config, gateway)
	sets, err := gatewaycontroller.ObserveListenerSets(ctx, r.APIReader, r.Config, gateway)
	if err != nil {
		return routeGraphAdmission{}, err
	}
	admission := routeGraphAdmission{
		listenerSets: sets.Sections(),
		bindings:     r.routeGraphBindings(routes, identity),
		eligible:     make(map[routeObjectKey]routeGraphCandidate),
	}
	namespacesByName := make(map[string][]corev1.Namespace)
	var candidatesForElection []routeProfileCandidate
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() || !r.structurallySupportedRoute(route) ||
			route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
			continue
		}
		parents, err := r.parentsForGateway(ctx, route, gateway)
		if err != nil {
			return routeGraphAdmission{}, err
		}
		selection, namespaces, err := r.selectRouteParentsWithNamespaces(ctx, route, parents, admission.listenerSets, namespacesByName)
		if err != nil {
			return routeGraphAdmission{}, err
		}
		selected := selection.listeners()
		if len(selected) == 0 {
			continue
		}
		candidate := routeProfileCandidate{route: route, selection: selection}
		if len(namespaces) != 0 && slices.ContainsFunc(selected, model.ObservesRouteNamespace) {
			candidate.namespace = namespaces[0].UID
		}
		candidatesForElection = append(candidatesForElection, candidate)
	}
	for _, namespace := range namespacesByName {
		admission.namespaces = append(admission.namespaces, namespace...)
	}
	slices.SortFunc(admission.namespaces, func(a, b corev1.Namespace) int { return cmp.Compare(a.Name, b.Name) })
	admission.rejectedSections = electRouteProfiles(candidatesForElection)
	for _, candidate := range candidatesForElection {
		route := candidate.route
		key := routeKey(route)
		var sections []sectionIdentity
		for _, listener := range candidate.selection.withoutProfileConflicts(admission.rejectedSections[key]).listeners() {
			sections = append(sections, listenerSectionIdentity(*listener))
		}
		if len(sections) == 0 {
			continue
		}
		admission.eligible[key] = routeGraphCandidate{routeUID: route.UID, sections: sections, namespaceUID: candidate.namespace}
		if controller.RouteClaimsGateway(r.Config, route, identity) || requested != nil && route.UID == requested.UID && routeKey(route) == routeKey(requested) {
			admission.active = append(admission.active, *route)
		}
	}
	return admission, nil
}

// One listener section cannot direct the same effective hostname to both route kinds
// without allowing HTTPRoute fallbacks to consume unmatched RPCs. Elect the
// oldest eligible Route, then namespace/name, as allowed by the GRPCRoute API.
func electRouteProfiles(candidates []routeProfileCandidate) map[routeObjectKey][]model.ListenerSection {
	if !slices.ContainsFunc(candidates, func(candidate routeProfileCandidate) bool {
		return model.HTTPProfileRouteKind(candidate.route) == model.GRPCRouteKind
	}) {
		return nil
	}
	slices.SortStableFunc(candidates, func(a, b routeProfileCandidate) int {
		return cmp.Or(a.route.CreationTimestamp.Compare(b.route.CreationTimestamp.Time),
			cmp.Compare(a.route.Namespace, b.route.Namespace), cmp.Compare(a.route.Name, b.route.Name))
	})
	type hostProfile struct {
		port    gatewayv1.PortNumber
		section model.ListenerSection
		kind    gatewayv1.Kind
	}
	accepted := make(map[hostProfile][]gatewayv1.Hostname)
	conflicts := make(map[routeObjectKey][]model.ListenerSection)
	for _, candidate := range candidates {
		kind := model.HTTPProfileRouteKind(candidate.route)
		peer := model.GRPCRouteKind
		if kind == model.GRPCRouteKind {
			peer = model.HTTPRouteKind
		}
		key := routeKey(candidate.route)
		for _, parent := range candidate.selection {
			if parent.rejection != nil {
				continue
			}
			for _, listener := range parent.listeners {
				section := parent.section(listener.Name)
				profile := hostProfile{port: listener.Port, section: section, kind: kind}
				var hostnames []gatewayv1.Hostname
				for _, hostname := range model.IntersectHostnames(listener.Hostname, candidate.route.Spec.Hostnames) {
					hostnames = append(hostnames, gatewayv1.Hostname(hostname))
				}
				peerHosts := accepted[hostProfile{port: listener.Port, section: section, kind: peer}]
				if slices.ContainsFunc(peerHosts, func(other gatewayv1.Hostname) bool {
					return other == "" || slices.Contains(hostnames, "") || len(model.IntersectHostnames(&other, hostnames)) != 0
				}) {
					conflicts[key] = append(conflicts[key], section)
					continue
				}
				accepted[profile] = append(accepted[profile], hostnames...)
			}
		}
	}
	return conflicts
}

func (r *Reconciler) collectGraphRouteModelInputs(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (*routeModelInputs, error) {
	admission, err := r.observeRouteGraphAdmission(ctx, gateway, routes, requested)
	if err != nil {
		return nil, err
	}
	inputs, err := r.collectRouteModelInputsForSets(ctx, r.APIReader, gateway, admission.active, admission.namespaces, admission.listenerSets)
	if err != nil {
		return nil, err
	}
	inputs.admission = admission
	return inputs, nil
}

func (r *Reconciler) validateGraphRouteModel(ctx context.Context, inputs *routeModelInputs, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) error {
	current, err := r.observeRouteGraphAdmission(ctx, inputs.gateway, routes, requested)
	if err != nil {
		return err
	}
	if !equality.Semantic.DeepEqual(inputs.admission.listenerSets, current.listenerSets) || !maps.EqualFunc(inputs.admission.eligible, current.eligible, func(a, b routeGraphCandidate) bool {
		return a.routeUID == b.routeUID && a.namespaceUID == b.namespaceUID && slices.Equal(a.sections, b.sections)
	}) ||
		!maps.EqualFunc(inputs.admission.rejectedSections, current.rejectedSections, slices.Equal[[]model.ListenerSection]) ||
		!slices.Equal(inputs.admission.bindings, current.bindings) ||
		!slices.EqualFunc(inputs.admission.active, current.active, func(a, b gatewayv1.HTTPRoute) bool {
			return model.HTTPProfileRouteKind(&a) == model.HTTPProfileRouteKind(&b) && a.UID == b.UID && a.Generation == b.Generation && a.CreationTimestamp.Equal(&b.CreationTimestamp) &&
				equality.Semantic.DeepEqual(a.Spec, b.Spec)
		}) {
		return errHTTPRouteChanged
	}
	// Namespace authorization and admitted route specs were checked above.
	// Compare only the backend dependencies used to build this model here.
	return inputs.validateBackends(ctx, r.APIReader)
}

func (r *Reconciler) collectRouteModelInputs(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace) (*routeModelInputs, error) {
	return r.collectRouteModelInputsForSets(ctx, reader, gateway, routes, namespaces, nil)
}

func (r *Reconciler) collectRouteModelInputsForSets(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace, sets map[types.NamespacedName][]gatewayv1.Listener) (*routeModelInputs, error) {
	input, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, gateway, routes, namespaces)
	if err != nil {
		return nil, err
	}
	input.ListenerSetSections = sets
	input.RequestHeaderModifier = r.Config.RequestHeaderModifier
	input.ResponseHeaderModifier = r.Config.ResponseHeaderModifier
	input.HTTPRequestRedirect = r.Config.HTTPRequestRedirect
	input.BackendH2C = r.Config.BackendH2C
	input.EnableXBackend = r.Config.XBackendEnabled()
	if input.EnableXBackend {
		mode, err := r.xBackendMemberMode(gateway)
		if err != nil {
			return nil, err
		}
		input.MemberMode = mode
		if err := graph.CollectXBackendInputs(ctx, reader, &input); err != nil {
			return nil, err
		}
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
	var availability backendTLSAvailability
	var ancestor *backendtls.GatewayAncestor
	if r.Config.BackendTLSPolicy {
		ancestor = &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
		if err := graph.CollectBackendTLSInputs(ctx, reader, &input, ancestor); err != nil {
			return nil, err
		}
		if graph.HasValidBackendTLSWinner(input.BackendTLSPolicies) {
			availability = r.backendTLSAvailability(ctx)
		}
	}
	logical, err := model.Compile(input)
	if err != nil {
		return nil, fmt.Errorf("compile Gateway backend model: %w", err)
	}
	return &routeModelInputs{gateway: gateway, input: input, logical: logical, backendTLSAvailability: availability, backendTLSAncestor: ancestor}, nil
}

func (r *Reconciler) xBackendMemberMode(gateway *gatewayv1.Gateway) (cloud.MemberMode, error) {
	if !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return "", nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	return topology.Topology.MemberMode, err
}

func (inputs *routeModelInputs) validateBackends(ctx context.Context, reader client.Reader) error {
	current, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, inputs.input.Gateway, inputs.input.Routes, inputs.input.Namespaces)
	if err != nil {
		return err
	}
	current.EnableXBackend = inputs.input.EnableXBackend
	current.MemberMode = inputs.input.MemberMode
	current.BackendH2C = inputs.input.BackendH2C
	if err := graph.CollectXBackendInputs(ctx, reader, &current); err != nil {
		return err
	}
	for _, route := range inputs.input.Routes {
		for _, rule := range route.Spec.Rules {
			for _, ref := range rule.BackendRefs {
				key, supported := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
				if supported && model.BackendReferenceAllowedFor(model.HTTPProfileRouteKind(&route), route.Namespace, key, inputs.input.ReferenceGrants) !=
					model.BackendReferenceAllowedFor(model.HTTPProfileRouteKind(&route), route.Namespace, key, current.ReferenceGrants) {
					return errHTTPRouteChanged
				}
			}
		}
	}
	if inputs.input.BackendTLSPolicies != nil {
		if err := graph.CollectBackendTLSInputs(ctx, reader, &current, inputs.backendTLSAncestor); err != nil {
			return err
		}
	}
	if !equality.Semantic.DeepEqual(inputs.input.BackendTLSPolicies, current.BackendTLSPolicies) ||
		!equality.Semantic.DeepEqual(inputs.input.Services, current.Services) ||
		!equality.Semantic.DeepEqual(inputs.input.EndpointSlices, current.EndpointSlices) ||
		!equality.Semantic.DeepEqual(inputs.input.XBackends, current.XBackends) {
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
	rejected := inputs.admission.rejectedSections[routeKey(route)]
	for _, candidate := range inputs.logical.Routes {
		if candidate.Source.UID == route.UID && candidate.Source.Namespace == route.Namespace && candidate.Source.Name == route.Name {
			if len(rejected) != 0 {
				candidate.Parents = slices.Clone(candidate.Parents)
				for i := range candidate.Parents {
					parent := &candidate.Parents[i]
					parent.Listeners = slices.DeleteFunc(slices.Clone(parent.Listeners), func(listener model.ListenerAttachment) bool {
						return slices.Contains(rejected, model.ListenerSection{ListenerSet: model.ListenerSetRefKey(parent.Reference, route.Namespace), Name: listener.Name})
					})
				}
			}
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
			if rule.Route.UID == route.UID && rule.Route.Namespace == route.Namespace && rule.Route.Name == route.Name &&
				!slices.Contains(rejected, model.ListenerSection{ListenerSet: listener.ListenerSet, Name: listener.Name}) {
				rule.Precedence += sectionOffset
				listener.Rules = append(listener.Rules, rule)
			}
		}
		sectionOffset += len(inputs.logical.Listeners[i].Rules)
	}
	return logical
}
