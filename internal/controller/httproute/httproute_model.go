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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeErrorKind string

const (
	routeErrorUnsupported         routeErrorKind = "Unsupported"
	routeErrorUnknownFilter       routeErrorKind = "UnknownFilter"
	routeErrorInvalidKind         routeErrorKind = "InvalidKind"
	routeErrorInvalid             routeErrorKind = "Invalid"
	routeErrorIncompatibleFilters routeErrorKind = routeErrorKind(gatewayv1.RouteReasonIncompatibleFilters)
	routeErrorBackendNotFound     routeErrorKind = "BackendNotFound"
	routeErrorRefNotPermitted     routeErrorKind = "RefNotPermitted"
	routeErrorUnsupportedProtocol routeErrorKind = "UnsupportedProtocol"
	routeErrorNoMatchingParent    routeErrorKind = "NoMatchingParent"
	routeErrorNoMatchingHostname  routeErrorKind = "NoMatchingListenerHostname"
	routeErrorNotAllowed          routeErrorKind = "NotAllowedByListeners"
	routeErrorPending             routeErrorKind = "Pending"
)

type routeBuildError struct {
	kind    routeErrorKind
	message string
}

func (e *routeBuildError) Error() string { return e.message }

func newRouteBuildError(kind routeErrorKind, format string, args ...any) error {
	return &routeBuildError{kind: kind, message: fmt.Sprintf(format, args...)}
}

func (r *Reconciler) structurallySupportedRoute(route *gatewayv1.HTTPRoute) bool {
	_, err := r.validateRouteStructure(route)
	return err == nil
}

// validateRouteStructure limits programming to the configured HTTP path profile.
func (r *Reconciler) validateRouteStructure(route *gatewayv1.HTTPRoute) ([]gatewayv1.BackendObjectReference, error) {
	grpc := model.HTTPProfileRouteKind(route) == model.GRPCRouteKind
	if grpc {
		if !r.Config.GRPCExactMethodEnabled() {
			return nil, grpcProfileDisabled()
		}
		if len(route.Spec.Rules) == 0 {
			return nil, newRouteBuildError(routeErrorUnsupported, "GRPCRoute rules require backendRefs and support an optional exact service and method, exact header matches, and a rule-level RequestHeaderModifier")
		}
	}
	if len(route.Spec.Rules) == 0 {
		return nil, newRouteBuildError(routeErrorUnsupported, "at least one HTTPRoute rule is required")
	}
	rules, _, err := r.admittedHTTPRouteRules(route)
	if err != nil {
		return nil, err
	}
	references := make([]gatewayv1.BackendObjectReference, 0, len(rules))
	for _, rule := range rules {
		for _, backend := range rule.BackendRefs {
			references = append(references, backend.BackendObjectReference)
		}
	}
	return references, nil
}

// admittedHTTPRouteRules drops invalid rules when a route with request filters also
// contains valid rules. Keep the published object available for status and
// create a projection with only admitted rules for backend reads and writes.
func (r *Reconciler) admittedHTTPRouteRules(route *gatewayv1.HTTPRoute) ([]gatewayv1.HTTPRouteRule, string, error) {
	grpc := model.HTTPProfileRouteKind(route) == model.GRPCRouteKind
	partial := !grpc && hasProjectableHTTPFilters(route)
	var first error
	var dropped []string
	var valid []gatewayv1.HTTPRouteRule
	for index, rule := range route.Spec.Rules {
		if _, diagnostic := model.NormalizeHTTPRequestPolicy(rule, !grpc && r.Config.HTTPRequestPolicy); diagnostic != nil {
			return nil, "", newRouteBuildError(routeErrorUnsupported, "%s", diagnostic.Message)
		}
		err := r.validateRouteRule(rule, grpc)
		if err == nil {
			valid = append(valid, rule)
			continue
		}
		var buildError *routeBuildError
		if !partial || errors.As(err, &buildError) && buildError.kind == routeErrorUnknownFilter ||
			!r.Config.FixedHTTPResponses && !r.Config.WeightedHTTPTargets &&
				slices.ContainsFunc(rule.BackendRefs, func(backend gatewayv1.HTTPBackendRef) bool {
					return model.UnresolvedHTTPBackendExtension(backend) != nil
				}) {
			return nil, "", err
		}
		if first == nil {
			first = err
		}
		ruleName := strconv.Itoa(index + 1)
		if rule.Name != nil {
			ruleName = string(*rule.Name)
		}
		dropped = append(dropped, fmt.Sprintf("%s (%v)", ruleName, err))
	}
	if len(dropped) == 0 {
		return route.Spec.Rules, "", nil
	}
	if len(valid) == 0 {
		return nil, "", first
	}
	return valid, "Dropped Rule " + strings.Join(dropped, ", "), nil
}

func hasProjectableHTTPFilters(route *gatewayv1.HTTPRoute) bool {
	return slices.ContainsFunc(route.Spec.Rules, func(rule gatewayv1.HTTPRouteRule) bool {
		return slices.ContainsFunc(rule.BackendRefs, func(backend gatewayv1.HTTPBackendRef) bool {
			return len(backend.Filters) != 0
		}) || slices.ContainsFunc(rule.Filters, func(filter gatewayv1.HTTPRouteFilter) bool {
			return filter.Type == gatewayv1.HTTPRouteFilterURLRewrite || filter.Type == gatewayv1.HTTPRouteFilterCORS || model.IsRequestMirror(filter)
		})
	})
}

func (r *Reconciler) projectedHTTPFilterRules(route *gatewayv1.HTTPRoute) ([]gatewayv1.HTTPRouteRule, string) {
	if hasProjectableHTTPFilters(route) {
		if valid, dropped, _ := r.admittedHTTPRouteRules(route); dropped != "" {
			return valid, dropped
		}
	}
	return route.Spec.Rules, ""
}

func (r *Reconciler) usesHTTPBackendGroups(route *gatewayv1.HTTPRoute) bool {
	rules, _ := r.projectedHTTPFilterRules(route)
	if r.Config.FixedHTTPResponses || r.Config.WeightedHTTPTargets || len(rules) > 1 {
		return true
	}
	if len(rules) == 0 {
		return false
	}
	rule := rules[0]
	if model.HasHTTPRequestPolicy(rule) || model.HasRequestMirrors(rule) {
		return true
	}
	if r.Config.XBackendEnabled() && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind {
		for _, ref := range rule.BackendRefs {
			if _, supported := model.XBackendReference(route.Namespace, ref.BackendObjectReference); supported {
				return true
			}
		}
	}
	if len(rule.Matches) > 1 || len(rule.Matches) == 1 && (len(rule.Matches[0].Headers) != 0 || rule.Matches[0].Method != nil) {
		return true
	}
	request, response, rewrite, cors, _ := model.NormalizeHTTPForwardingRuleFilters(rule, r.Config.RequestHeaderModifier, r.Config.ResponseHeaderModifier, r.Config.HTTPRequestURLRewrite, r.Config.HTTPCORS)
	return len(request) != 0 || len(response) != 0 || rewrite != nil || cors != nil
}

func (r *Reconciler) validateRouteRule(rule gatewayv1.HTTPRouteRule, grpc bool) error {
	_, diagnostic := model.NormalizeRequestMirrors(rule.Filters, r.Config.HTTPRequestMirror && !grpc, len(rule.BackendRefs) > 0)
	if diagnostic != nil {
		return routeActionError(diagnostic)
	}
	rule.Filters = model.WithoutRequestMirrorFilters(rule.Filters)
	fixedResponses := r.Config.FixedHTTPResponses || grpc
	weightedTargets := r.Config.WeightedHTTPTargets && (!grpc || len(rule.BackendRefs) > 1 ||
		len(rule.BackendRefs) == 1 && ptr.Deref(rule.BackendRefs[0].Weight, 1) == 0)
	exactHeaders := r.Config.HTTPHeaderExact
	exactQueryParams := r.Config.HTTPQueryParamExact && r.Config.FixedHTTPResponses && !grpc
	methodMatch := r.Config.HTTPMethodMatch && !grpc
	if rule.SessionPersistence != nil { //nolint:staticcheck // The release still serves this deprecated Experimental field.
		return newRouteBuildError(routeErrorUnsupported, "session persistence is not supported")
	}
	if len(rule.Filters) != 0 || !fixedResponses && !weightedTargets {
		if err := compileRouteAction(rule, r.Config.RequestHeaderModifier, r.Config.ResponseHeaderModifier && !grpc, r.Config.HTTPRequestRedirect && !grpc, r.Config.HTTPRequestURLRewrite && !grpc, r.Config.HTTPCORS && !grpc); err != nil {
			return err
		}
	}
	for _, match := range rule.Matches {
		if _, _, err := supportedMatch(match, exactHeaders, methodMatch, exactQueryParams); err != nil {
			return err
		}
	}
	if len(rule.Filters) == 1 && rule.Filters[0].Type == gatewayv1.HTTPRouteFilterRequestRedirect {
		return nil
	}
	if len(rule.BackendRefs) == 0 && (fixedResponses || weightedTargets) {
		return nil
	}
	if weightedTargets {
		request, _, rewrite, cors, _ := model.NormalizeHTTPForwardingRuleFilters(rule, r.Config.RequestHeaderModifier, r.Config.ResponseHeaderModifier, r.Config.HTTPRequestURLRewrite, r.Config.HTTPCORS && !grpc)
		for _, backend := range rule.BackendRefs {
			if _, diagnostic := model.NormalizeRequestMirrors(backend.Filters, r.Config.HTTPRequestMirror && !grpc, true); diagnostic != nil {
				return routeActionError(diagnostic)
			}
			backend.Filters = model.WithoutRequestMirrorFilters(backend.Filters)
			operations, _, diagnostic := model.NormalizeHTTPBackendRequestFilters(rule, backend, r.Config.BackendRequestFilters && !grpc, request, rewrite)
			if diagnostic != nil && diagnostic.Condition != string(gatewayv1.RouteConditionResolvedRefs) {
				return routeActionError(diagnostic)
			}
			if cors != nil {
				if diagnostic := model.ValidateCORSHeaderComposition(operations, nil); diagnostic != nil {
					return routeActionError(diagnostic)
				}
			}
		}
		return validateWeightedBackendRefs(rule.BackendRefs, grpc)
	}
	if len(rule.BackendRefs) != 1 {
		if grpc {
			return newRouteBuildError(routeErrorUnsupported, "multiple GRPCRoute backendRefs require --octavia-weighted-http-targets")
		}
		return newRouteBuildError(routeErrorUnsupported, "exactly one backendRef is supported")
	}
	backend := rule.BackendRefs[0]
	if slices.ContainsFunc(backend.Filters, model.IsRequestMirror) {
		return newRouteBuildError(routeErrorUnsupported, "backendRef RequestMirror requires --octavia-http-request-mirror and --octavia-weighted-http-targets")
	}
	if len(backend.Filters) != 0 {
		_, _, diagnostic := model.NormalizeHTTPBackendRequestFilters(rule, backend, false, nil, nil)
		if diagnostic != nil && (diagnostic.Scope == model.ScopeRoute ||
			slices.ContainsFunc(backend.Filters, func(filter gatewayv1.HTTPRouteFilter) bool { return filter.Type == gatewayv1.HTTPRouteFilterCORS })) {
			return routeActionError(diagnostic)
		}
		if fixedResponses && !grpc && diagnostic != nil && diagnostic.Condition == string(gatewayv1.RouteConditionResolvedRefs) &&
			ptr.Deref(backend.Weight, 1) > 0 {
			if slices.ContainsFunc(rule.Filters, func(filter gatewayv1.HTTPRouteFilter) bool { return filter.Type == gatewayv1.HTTPRouteFilterCORS }) {
				return newRouteBuildError(routeErrorUnsupported, "Octavia CORS on an unresolved backend requires --octavia-weighted-http-targets")
			}
			return nil
		}
		return newRouteBuildError(routeErrorUnsupported, "backendRef filters are not supported")
	}
	if backend.Weight != nil && *backend.Weight <= 0 {
		return newRouteBuildError(routeErrorUnsupported, "a single backendRef must have positive weight")
	}
	if _, supported := model.XBackendReference("", backend.BackendObjectReference); supported && r.Config.XBackendEnabled() && !grpc {
		if backend.Port != nil {
			return newRouteBuildError(routeErrorUnsupported, "backendRef.port must be omitted for XBackend")
		}
		return nil
	}
	if backend.Group != nil && *backend.Group != gatewayv1.Group("") {
		return newRouteBuildError(routeErrorInvalidKind, "backendRef group %q is unsupported; use a core Service", *backend.Group)
	}
	if backend.Kind != nil && *backend.Kind != gatewayv1.Kind("Service") {
		return newRouteBuildError(routeErrorInvalidKind, "backendRef kind %q is unsupported; use Service", *backend.Kind)
	}
	if !validBackendPort(backend.Port) {
		return newRouteBuildError(routeErrorUnsupported, "backendRef must specify a port between 1 and 65535")
	}
	return nil
}

func validBackendPort(port *gatewayv1.PortNumber) bool {
	return port != nil && *port >= 1 && *port <= 65535
}

func validateWeightedBackendRefs(refs []gatewayv1.HTTPBackendRef, grpc bool) error {
	if len(refs) < 1 || len(refs) > 16 {
		return newRouteBuildError(routeErrorUnsupported, "a weighted rule requires between 1 and 16 backendRefs")
	}
	positive := false
	for _, ref := range refs {
		weight := ptr.Deref(ref.Weight, 1)
		if weight < 0 || weight > 1000000 {
			return newRouteBuildError(routeErrorUnsupported, "backendRef weight must be between 0 and 1000000")
		}
		positive = positive || weight > 0
	}
	if !positive && !grpc {
		return newRouteBuildError(routeErrorUnsupported, "Octavia weighted rules require a positive total weight")
	}
	return nil
}

func backendReferenceRejection(kind gatewayv1.Kind, namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) *routeBuildError {
	if !model.BackendReferenceAllowedFor(kind, namespace, target, grants) {
		return &routeBuildError{kind: routeErrorRefNotPermitted, message: fmt.Sprintf("backend Service %s is not permitted by a ReferenceGrant", target)}
	}
	return nil
}

var errRouteAttachmentRejected = errors.New("route attachment rejected")

func (r *Reconciler) buildRouteModelWithReader(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
) (cloud.RouteSpec, error) {
	if _, err := r.validateRouteStructure(route); err != nil {
		return cloud.RouteSpec{}, err
	}
	if _, err := gatewaycontroller.ValidatedHTTPRouteListeners(r.Config, gateway); err != nil {
		return cloud.RouteSpec{}, newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	parents, err := r.parentsForGateway(ctx, reader, route, gateway)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	selection, namespaces, sets, err := r.observeRouteSelection(ctx, reader, route, parents)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	listeners := selection.listeners()
	if len(listeners) == 0 {
		rejection := newRouteBuildError(routeErrorNoMatchingParent, "HTTPRoute no longer targets the bound Gateway")
		if len(selection) != 0 {
			rejection = selection[0].rejection
		}
		return cloud.RouteSpec{}, errors.Join(rejection, errRouteAttachmentRejected)
	}
	inputs, err := r.collectRouteModelInputsForSets(ctx, reader, gateway, []gatewayv1.HTTPRoute{*route}, namespaces, sets)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	spec, _, err := r.buildRouteModelFromInputs(ctx, reader, route, gateway, selection, inputs)
	return spec, err
}

type routeModelDiagnostics struct {
	backend                 model.Diagnostic
	corsFallback            bool
	retryBudgetAncestorFull bool
}

func (r *Reconciler) buildRouteModelFromInputs(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
	selection routeSelection,
	inputs *routeModelInputs,
) (cloud.RouteSpec, routeModelDiagnostics, error) {
	matches, rules, err := r.lowerableRouteRules(route, inputs, selection.sections())
	if err != nil {
		return cloud.RouteSpec{}, routeModelDiagnostics{}, err
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return cloud.RouteSpec{}, routeModelDiagnostics{}, err
	}
	admitted, err := gatewaycontroller.HTTPRouteListeners(r.Config, gateway)
	if err != nil {
		return cloud.RouteSpec{}, routeModelDiagnostics{}, err
	}
	plainHosts := gatewaycontroller.HTTPListenerHostnames(admitted)
	portMatches := make(map[gatewayv1.PortNumber][]routeLoweredMatch)
	portListeners := make(map[gatewayv1.PortNumber]*gatewayv1.Listener)
	var ports []gatewayv1.PortNumber
	for _, listener := range selection.listeners() {
		if portListeners[listener.Port] == nil {
			ports = append(ports, listener.Port)
			portListeners[listener.Port] = listener
		}
	}
	for _, match := range matches {
		listener := portListeners[match.port]
		if gatewaycontroller.SharedHTTPSListener(r.Config, gateway, listener) {
			match.httpsListenerHostname = match.sectionHostname
		}
		if len(plainHosts[int(match.port)]) != 0 {
			match.httpListenerHostname = match.sectionHostname
		}
		portMatches[match.port] = append(portMatches[match.port], match)
	}
	slices.Sort(ports)
	anchor, _ := strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if index := slices.Index(ports, gatewayv1.PortNumber(anchor)); index > 0 {
		ports[0], ports[index] = ports[index], ports[0]
	}
	var desired cloud.RouteSpec
	diagnostics := routeModelDiagnostics{retryBudgetAncestorFull: inputs.retryBudgetAncestorFull(matches, rules, model.HTTPProfileRouteKind(route) == model.GRPCRouteKind)}
	for i, port := range ports {
		spec, portDiagnostics, err := r.buildRoutePortModel(ctx, reader, route, gateway, *portListeners[port], inputs, topology, portMatches[port], rules)
		if err != nil {
			return cloud.RouteSpec{}, routeModelDiagnostics{}, err
		}
		diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, portDiagnostics.backend)
		diagnostics.corsFallback = diagnostics.corsFallback || portDiagnostics.corsFallback
		if i == 0 {
			desired = spec
		} else {
			desired.AdditionalHTTPPorts = append(desired.AdditionalHTTPPorts, spec)
		}
	}
	return desired, diagnostics, nil
}

func (r *Reconciler) buildRoutePortModel(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
	listener gatewayv1.Listener,
	inputs *routeModelInputs,
	topology classconfig.Resolved,
	matches []routeLoweredMatch,
	rules []model.Rule,
) (cloud.RouteSpec, routeModelDiagnostics, error) {
	ordinaryHTTP := model.HTTPProfileRouteKind(route) == model.HTTPRouteKind
	mode := topology.Topology.MemberMode
	spec := cloud.RouteSpec{
		Identity:           controller.RouteIdentity(r.Config, gateway, route),
		BackendTLSObserved: inputs.input.BackendTLSPolicies != nil,
		GatewayRequirements: cloud.GatewayRequirements{
			Provider:          "amphora",
			VIPSubnetID:       topology.Topology.VIPSubnetID,
			ExternalNetworkID: topology.Topology.ExternalNetworkID,
			ListenerPort:      int(listener.Port),
			FlavorID:          topology.Topology.FlavorID, AvailabilityZone: topology.Topology.AvailabilityZone,
			VIPSecurityGroupIDs: slices.Clone(topology.Topology.VIPSecurityGroupIDs),
		},
		MemberMode:              mode,
		MemberSubnetID:          topology.Topology.MemberSubnetID,
		BackendSecurityGroupIDs: slices.Clone(topology.Topology.BackendSecurityGroupIDs),
		HealthMonitor:           topology.HealthMonitor,
		GRPC:                    model.HTTPProfileRouteKind(route) == model.GRPCRouteKind,
	}
	if controller.GatewayListenerScope(r.Config, gateway) == gatewayv1.HTTPProtocolType {
		spec.GatewayRequirements.ListenerPort, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolHTTP
		spec.ScopedListenerPorts = []int{int(listener.Port)}
		spec.ScopeHTTPResources = ordinaryHTTP && int(listener.Port) != spec.GatewayRequirements.ListenerPort
	}
	if len(matches) == 1 && !r.usesHTTPBackendGroups(route) && rules[0].Redirect == nil {
		match := matches[0]
		backend := rules[0].Backends[0]
		spec.HTTPListenerHostname = match.httpListenerHostname
		spec.HTTPSListenerHostname = match.httpsListenerHostname
		spec.Hostname, spec.PathType, spec.PathValue, spec.Precedence = match.Hostname, cloud.PathMatchType(match.Match.PathType), match.Match.PathValue, match.Precedence
		spec.BackendTLS = inputs.backendTLSValidation(backend, spec.GRPC)
		if spec.BackendTLS != nil && spec.BackendTLS.Denied {
			return spec, routeModelDiagnostics{}, nil
		}
		members, err := routeBackendMembers(ctx, reader, backend, inputs, mode, topology.NodeAddressType)
		if err != nil {
			return cloud.RouteSpec{}, routeModelDiagnostics{}, err
		}
		spec.Members = members
		spec.BackendH2C = spec.BackendTLS == nil && backend.UsesH2C()
		spec.BackendRetryBudget, spec.BackendRetryBudgetPending = inputs.backendRetryBudget(backend)
		return spec, routeModelDiagnostics{}, nil
	}
	diagnostic, err := r.lowerHTTPBackends(ctx, reader, listener, &spec, inputs, matches, rules, topology.NodeAddressType)
	if err != nil {
		return cloud.RouteSpec{}, routeModelDiagnostics{}, err
	}
	return spec, diagnostic, nil
}

func routeBackendMembers(ctx context.Context, reader client.Reader, backend model.BackendGroup, inputs *routeModelInputs, mode cloud.MemberMode, nodeAddressType corev1.NodeAddressType) ([]cloud.Member, error) {
	if backend.XBackend.Name != "" {
		members := make([]cloud.Member, 0, len(backend.Endpoints))
		for _, endpoint := range backend.Endpoints {
			members = append(members, cloud.Member{Address: endpoint.Address, Port: int(endpoint.Port)})
		}
		return members, nil
	}
	key := backend.Service
	if backend.Authorization == model.BackendAuthorizationDenied {
		return nil, newRouteBuildError(routeErrorRefNotPermitted, "backend Service %s is not permitted by a ReferenceGrant", key)
	}
	service := inputs.service(key)
	if service == nil {
		return nil, newRouteBuildError(routeErrorBackendNotFound, "backend Service %s was not found", key)
	}
	port, err := servicePortForBackend(service, *backend.Reference.Port, mode, inputs.input.BackendH2C, backend.BackendTLS.Winner != nil)
	if err != nil {
		return nil, err
	}
	if mode == cloud.MemberModeEndpoint {
		return endpointMembers(service, backend.Endpoints)
	}
	return nodePortMembersFromSlices(ctx, reader, service, port, nodeAddressType, inputs.endpointSlices(key))
}

type backendKey struct {
	kind   cloud.HTTPBackendKind
	object types.NamespacedName
	port   int
}

type httpBackendResolution struct {
	index      *int
	diagnostic model.Diagnostic
}

func (r *Reconciler) lowerHTTPBackends(ctx context.Context, reader client.Reader, listener gatewayv1.Listener, spec *cloud.RouteSpec, inputs *routeModelInputs, matches []routeLoweredMatch, rules []model.Rule, nodeAddressType corev1.NodeAddressType) (routeModelDiagnostics, error) {
	type ruleTarget struct {
		pool         *int
		weighted     int
		redirect     *int
		fixed        *int
		corsFallback bool
		mirrors      []cloud.HTTPMirrorTarget
	}
	groups := make(map[backendKey]httpBackendResolution, len(rules))
	targets := make([]ruleTarget, len(rules))
	var diagnostics routeModelDiagnostics
	for _, rule := range rules {
		if redirect := rule.Redirect; redirect != nil {
			targets[rule.Index].redirect = ptr.To(len(spec.HTTPRedirects))
			spec.HTTPRedirects = append(spec.HTTPRedirects, lowerRequestRedirect(rule.Index, *redirect, listener))
			continue
		}
		if len(rule.Backends) == 0 {
			targets[rule.Index].fixed = ptr.To(len(spec.FixedHTTPRules))
			spec.FixedHTTPRules = append(spec.FixedHTTPRules, cloud.HTTPFixedRule{RuleIndex: rule.Index})
			continue
		}
		mirrors, diagnostic, err := r.lowerRequestMirrors(ctx, reader, spec, inputs, rule.RequestMirrors, nodeAddressType, groups)
		if err != nil {
			return routeModelDiagnostics{}, err
		}
		diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, diagnostic)
		targets[rule.Index].mirrors = mirrors
		zeroOnly := spec.GRPC && !slices.ContainsFunc(rule.Backends, func(backend model.BackendGroup) bool { return backend.Weight > 0 })
		if !spec.GRPC && len(rule.Backends) == 1 && rule.Backends[0].FilterDiagnostic != nil && !r.Config.WeightedHTTPTargets {
			diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, *rule.Backends[0].FilterDiagnostic)
			targets[rule.Index].fixed = ptr.To(len(spec.FixedHTTPRules))
			spec.FixedHTTPRules = append(spec.FixedHTTPRules, cloud.HTTPFixedRule{RuleIndex: rule.Index})
			continue
		}
		direct := !zeroOnly && (rule.CORS == nil || !r.Config.WeightedHTTPTargets) && len(rule.Backends) == 1 &&
			hasHTTPBackendKey(rule.Backends[0]) && rule.Backends[0].FilterDiagnostic == nil &&
			len(rule.Backends[0].RequestHeaderOperations) == 0 && rule.Backends[0].URLRewrite == nil &&
			len(rule.Backends[0].RequestMirrors) == 0
		weighted := cloud.HTTPWeightedRule{RuleIndex: rule.Index}
		for _, backend := range rule.Backends {
			if zeroOnly {
				// Resolve the Kubernetes references without creating forwarding pools.
				if backend.Diagnostic != nil {
					diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, *backend.Diagnostic)
				}
				weighted.Targets = append(weighted.Targets, cloud.HTTPWeightedTarget{})
				continue
			}
			use := httpBackendWeighted
			if direct {
				use = httpBackendDirect
			}
			index, diagnostic, err := r.lowerHTTPBackend(ctx, reader, spec, inputs, backend, nodeAddressType, use, groups)
			if err != nil {
				return routeModelDiagnostics{}, err
			}
			diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, diagnostic)
			if direct {
				if rule.CORS != nil && index != nil && r.Config.FixedHTTPResponses && spec.HTTPBackends[*index].InvalidReference {
					// Preserve this match as the usual fixed 500 response.
					// Octavia cannot attach CORS to a standalone fixed response.
					targets[rule.Index].corsFallback = true
					diagnostics.corsFallback = true
				}
				targets[rule.Index].pool = index
			} else {
				target := cloud.HTTPWeightedTarget{BackendIndex: index, Weight: int(backend.Weight)}
				if index != nil && backend.Weight > 0 {
					target.RequestHeaderOperations = backend.RequestHeaderOperations
					target.URLRewrite = backend.URLRewrite
				}
				mirrorBackendGroups := groups
				if index == nil || backend.Weight == 0 || spec.HTTPBackends[*index].BackendTLS != nil && spec.HTTPBackends[*index].BackendTLS.Denied {
					// This primary slot cannot be selected for forwarding.
					mirrorBackendGroups = nil
				}
				mirrors, diagnostic, err := r.lowerRequestMirrors(ctx, reader, spec, inputs, backend.RequestMirrors, nodeAddressType, mirrorBackendGroups)
				if err != nil {
					return routeModelDiagnostics{}, err
				}
				diagnostics.backend = preferredBackendDiagnostic(diagnostics.backend, diagnostic)
				target.RequestMirrors = mirrors
				weighted.Targets = append(weighted.Targets, target)
			}
		}
		if !direct {
			targets[rule.Index].weighted = len(spec.WeightedHTTPRules)
			spec.WeightedHTTPRules = append(spec.WeightedHTTPRules, weighted)
		}
	}
	for _, match := range matches {
		target := targets[match.RuleIndex]
		var paths *[]cloud.HTTPPathMatch
		switch {
		case target.fixed != nil:
			paths = &spec.FixedHTTPRules[*target.fixed].Matches
		case target.redirect != nil:
			paths = &spec.HTTPRedirects[*target.redirect].Matches
		case target.pool != nil:
			paths = &spec.HTTPBackends[*target.pool].Matches
		default:
			paths = &spec.WeightedHTTPRules[target.weighted].Matches
		}
		var operations []cloud.HTTPHeaderOperation
		var requestPolicy *cloud.HTTPRequestPolicy
		if target.pool != nil || target.fixed == nil && target.redirect == nil {
			operations = rules[match.RuleIndex].RequestHeaderOperations
			requestPolicy = rules[match.RuleIndex].RequestPolicy
		}
		if len(target.mirrors) != 0 {
			operations = rules[match.RuleIndex].RequestHeaderOperations
		}
		cors := rules[match.RuleIndex].CORS
		if target.corsFallback {
			cors = nil
		}
		*paths = appendHTTPPathMatch(*paths, cloud.HTTPPathMatch{
			Hostname: match.Hostname, PathType: cloud.PathMatchType(match.Match.PathType), PathValue: match.Match.PathValue, Precedence: match.Precedence,
			HTTPListenerHostname: match.httpListenerHostname, HTTPSListenerHostname: match.httpsListenerHostname,
			Method: match.Match.Method, Headers: match.Match.Headers, QueryParams: match.Match.QueryParams,
			RequestHeaderOperations:  operations,
			ResponseHeaderOperations: rules[match.RuleIndex].ResponseHeaderOperations,
			URLRewrite:               rules[match.RuleIndex].URLRewrite,
			CORS:                     cors,
			RequestPolicy:            requestPolicy,
			RequestMirrors:           target.mirrors,
		})
	}
	inputs.attachHTTPBackendRetryBudgets(spec, matches, rules, groups)
	return diagnostics, nil
}

func (inputs *routeModelInputs) attachHTTPBackendRetryBudgets(spec *cloud.RouteSpec, matches []routeLoweredMatch, rules []model.Rule, groups map[backendKey]httpBackendResolution) {
	if len(inputs.retryBudgets) == 0 {
		return
	}
	for backend := range activeRetryBudgetBackends(matches, rules, inputs.retryBudgetTransportUsable(spec.GRPC)) {
		key := backendKey{object: backend.Service, port: int(ptr.Deref(backend.Reference.Port, 0))}
		resolved := groups[key]
		if resolved.index == nil || resolved.diagnostic.Reason != "" {
			continue
		}
		desired := &spec.HTTPBackends[*resolved.index]
		desired.BackendRetryBudget, desired.BackendRetryBudgetPending = inputs.backendRetryBudget(backend)
	}
}

// A rejected destination contributes its own diagnostic but no forwarding
// target. The primary and the other mirrors continue to use their normal pools.
func (r *Reconciler) lowerRequestMirrors(ctx context.Context, reader client.Reader, spec *cloud.RouteSpec, inputs *routeModelInputs, mirrors []model.RequestMirror, nodeAddressType corev1.NodeAddressType, groups map[backendKey]httpBackendResolution) ([]cloud.HTTPMirrorTarget, model.Diagnostic, error) {
	var targets []cloud.HTTPMirrorTarget
	var diagnostic model.Diagnostic
	for _, mirror := range mirrors {
		backend := mirror.Backend
		if backend.Diagnostic != nil {
			diagnostic = preferredBackendDiagnostic(diagnostic, *backend.Diagnostic)
			continue
		}
		if groups == nil {
			continue
		}
		if tls := inputs.backendTLSValidation(backend, false); tls != nil && tls.Denied {
			diagnostic = preferredBackendDiagnostic(diagnostic, model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "RequestMirror backend TLS validation is unavailable",
			})
			continue
		}
		index, issue, err := r.lowerHTTPBackend(ctx, reader, spec, inputs, backend, nodeAddressType, httpBackendMirror, groups)
		if err != nil {
			return nil, model.Diagnostic{}, err
		}
		diagnostic = preferredBackendDiagnostic(diagnostic, issue)
		if index != nil {
			targets = append(targets, cloud.HTTPMirrorTarget{BackendIndex: *index, Numerator: mirror.Numerator,
				Denominator: mirror.Denominator, Phase: mirror.Phase})
		}
	}
	return targets, diagnostic, nil
}

func hasHTTPBackendKey(backend model.BackendGroup) bool {
	return backend.XBackend.Name != "" || backend.Service.Name != "" && validBackendPort(backend.Reference.Port)
}

type httpBackendUse uint8

const (
	httpBackendDirect httpBackendUse = iota
	httpBackendWeighted
	httpBackendMirror
)

func (r *Reconciler) lowerHTTPBackend(ctx context.Context, reader client.Reader, spec *cloud.RouteSpec, inputs *routeModelInputs, backend model.BackendGroup, nodeAddressType corev1.NodeAddressType, use httpBackendUse, groups map[backendKey]httpBackendResolution) (*int, model.Diagnostic, error) {
	if backend.FilterDiagnostic != nil {
		return nil, *backend.FilterDiagnostic, nil
	}
	if !hasHTTPBackendKey(backend) {
		if backend.Diagnostic != nil {
			return nil, *backend.Diagnostic, nil
		}
		return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: "backendRef does not identify a Service port"}, nil
	}
	key := backendKey{object: backend.Service, port: int(ptr.Deref(backend.Reference.Port, 0))}
	port := key.port
	if backend.XBackend.Name != "" {
		key = backendKey{kind: cloud.HTTPBackendKindXBackend, object: backend.XBackend}
		port = backend.XBackendPort
	}
	result, observed := groups[key]
	var members []cloud.Member
	var clientCertificate string
	tls := inputs.backendTLSValidation(backend, spec.GRPC)
	h2cPending := backend.XBackendUsesH2C && inputs.xBackendH2CIssue.Reason == string(gatewayv1.RouteReasonPending)
	if tls != nil && !tls.Denied {
		clientCertificate = backend.XBackendClientCertificateReference
	}
	if !observed {
		switch {
		case backend.XBackendTLSRequired && tls.Denied:
			result.diagnostic = inputs.xBackendTLSDiagnostic(backend)
		case backend.XBackendUsesH2C && !h2cPending && inputs.xBackendH2CIssue.Reason != "":
			result.diagnostic = inputs.xBackendH2CIssue
		case backend.Diagnostic != nil && spec.GRPC:
			result.diagnostic = *backend.Diagnostic
		case tls == nil || !tls.Denied:
			var err error
			members, result.diagnostic, err = groupedBackendMembers(ctx, reader, backend, inputs, spec.MemberMode, nodeAddressType)
			if err != nil {
				return nil, model.Diagnostic{}, err
			}
		}
	}
	if spec.GRPC && backend.Weight == 0 && tls != nil {
		return nil, result.diagnostic, nil
	}
	invalid := result.diagnostic.Reason != ""
	if invalid && use == httpBackendMirror {
		return nil, result.diagnostic, nil
	}
	// Keep a denied policy's pool identity so the graph can quarantine it
	// before rewriting the weighted rule to send its share to the failure pool.
	if use == httpBackendWeighted && invalid && tls == nil {
		groups[key] = result
		return nil, result.diagnostic, nil
	}
	if result.index == nil {
		result.index = ptr.To(len(spec.HTTPBackends))
		// Keep an ordinary rule's pool identity when moving its sole share to
		// the existing fixed-response policy. Weighted rules carry errors in
		// their target list and do not need placeholder pools.
		// A gRPC client maps the empty pool's HTTP 503 to UNAVAILABLE.
		// The HTTP fixed 500 response would instead map to UNKNOWN.
		invalidReference := invalid && !spec.GRPC && result.diagnostic.Reason != string(gatewayv1.RouteReasonPending) &&
			(key.kind == cloud.HTTPBackendKindXBackend || r.Config.WeightedHTTPTargets ||
				result.diagnostic.Reason == string(gatewayv1.RouteReasonRefNotPermitted) ||
				result.diagnostic.Reason == string(gatewayv1.RouteReasonBackendNotFound))
		spec.HTTPBackends = append(spec.HTTPBackends, cloud.HTTPBackend{
			Kind: key.kind, Namespace: key.object.Namespace, Name: key.object.Name, Port: port,
			RoutedExternal:        backend.XBackendRoutedExternal && !invalidReference,
			SessionCookieName:     backend.XBackendCookieName,
			SessionCookiePath:     backend.XBackendCookiePath,
			SessionCookieDeadline: backend.XBackendCookieDeadline,
			Members:               members, Unavailable: len(members) == 0, InvalidReference: invalidReference,
			BackendH2C: !invalid && tls == nil && backend.UsesH2C(), BackendH2CPending: h2cPending, BackendTLS: tls,
			XBackendClientCertificateReference: clientCertificate,
		})
	}
	groups[key] = result
	if h2cPending {
		return result.index, inputs.xBackendH2CIssue, nil
	}
	return result.index, result.diagnostic, nil
}

func appendHTTPPathMatch(matches []cloud.HTTPPathMatch, match cloud.HTTPPathMatch) []cloud.HTTPPathMatch {
	// Equivalent alternatives targeting the same action share one policy identity.
	if slices.ContainsFunc(matches, func(previous cloud.HTTPPathMatch) bool {
		return previous.HTTPListenerHostname == match.HTTPListenerHostname && previous.HTTPSListenerHostname == match.HTTPSListenerHostname && previous.Hostname == match.Hostname &&
			previous.PathType == match.PathType && previous.PathValue == match.PathValue &&
			previous.Method == match.Method && slices.Equal(previous.Headers, match.Headers) && slices.Equal(previous.QueryParams, match.QueryParams)
	}) {
		return matches
	}
	return append(matches, match)
}

func preferredBackendDiagnostic(current, next model.Diagnostic) model.Diagnostic {
	// Prefer an authorization failure when several references cannot resolve.
	pending := string(gatewayv1.RouteReasonPending)
	if current.Reason == "" || current.Reason == pending && next.Reason != "" && next.Reason != pending ||
		current.Reason != string(gatewayv1.RouteReasonRefNotPermitted) && next.Reason == string(gatewayv1.RouteReasonRefNotPermitted) {
		return next
	}
	return current
}

func groupedBackendMembers(ctx context.Context, reader client.Reader, backend model.BackendGroup, inputs *routeModelInputs, mode cloud.MemberMode, nodeAddressType corev1.NodeAddressType) ([]cloud.Member, model.Diagnostic, error) {
	if backend.Diagnostic != nil {
		return nil, *backend.Diagnostic, nil
	}
	members, err := routeBackendMembers(ctx, reader, backend, inputs, mode, nodeAddressType)
	if err == nil {
		return members, model.Diagnostic{}, nil
	}
	var buildError *routeBuildError
	if !errors.As(err, &buildError) {
		return nil, model.Diagnostic{}, err
	}
	if buildError.kind == routeErrorPending {
		return nil, model.Diagnostic{}, nil
	}
	return nil, model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: buildError.message}, nil
}

type routeLoweredMatch struct {
	model.RouteMatch
	httpsListenerHostname string
	httpListenerHostname  string
	sectionHostname       string
	port                  gatewayv1.PortNumber
}

// supportedRouteModel verifies that the compiled attachments match the sections
// selected by admission before lowering any backend or policy.
func (r *Reconciler) lowerableRouteRules(route *gatewayv1.HTTPRoute, inputs *routeModelInputs, selected map[model.ListenerSection]*gatewayv1.Listener) ([]routeLoweredMatch, []model.Rule, error) {
	ordinaryHTTP := model.HTTPProfileRouteKind(route) == model.HTTPRouteKind
	return supportedRouteModel(inputs.routeFragment(route), selected, r.Config.FixedHTTPResponses || r.Config.WeightedHTTPTargets,
		r.Config.WeightedHTTPTargets, r.Config.HTTPHeaderExact, r.Config.HTTPMethodMatch && ordinaryHTTP,
		r.Config.HTTPQueryParamExact && r.Config.FixedHTTPResponses && ordinaryHTTP)
}

func supportedRouteModel(logical model.Gateway, selected map[model.ListenerSection]*gatewayv1.Listener, fixedResponses, weightedTargets, exactHeaders, methodMatch, exactQueryParams bool) ([]routeLoweredMatch, []model.Rule, error) {
	unsupported := func() ([]routeLoweredMatch, []model.Rule, error) {
		return nil, nil, newRouteBuildError(routeErrorUnsupported, "logical Gateway configuration is outside the supported HTTP profile")
	}
	if len(logical.Diagnostics) != 0 || len(logical.Routes) != 1 {
		return unsupported()
	}
	route := logical.Routes[0]
	if len(route.Diagnostics) != 0 {
		return unsupported()
	}
	accepted := make(map[model.ListenerSection]bool)
	for _, parent := range route.Parents {
		if parent.State != model.AttachmentAccepted {
			continue
		}
		key := model.ListenerSetRefKey(parent.Reference, route.Source.Namespace)
		for _, attachment := range parent.Listeners {
			if attachment.State == model.AttachmentAccepted {
				accepted[model.ListenerSection{ListenerSet: key, Name: attachment.Name}] = true
			}
		}
	}
	if len(accepted) == 0 || len(accepted) != len(selected) {
		return unsupported()
	}
	for _, rule := range route.Rules {
		if len(rule.Diagnostics) != 0 {
			return unsupported()
		}
		if rule.Redirect != nil || len(rule.Backends) == 0 && fixedResponses {
			continue
		}
		if !weightedTargets && (len(rule.Backends) != 1 ||
			(!hasHTTPBackendKey(rule.Backends[0]) && (!fixedResponses || rule.Backends[0].FilterDiagnostic == nil)) ||
			rule.Backends[0].Weight <= 0) {
			return unsupported()
		}
	}
	var matches []routeLoweredMatch
	matchedSections := 0
	for _, listener := range logical.Listeners {
		section := model.ListenerSection{ListenerSet: listener.ListenerSet, Name: listener.Name}
		expected := selected[section]
		if expected == nil {
			if len(listener.Rules) != 0 {
				return unsupported()
			}
			continue
		}
		expectedIdentity := listenerSectionIdentity(section, *expected)
		if !accepted[section] || expectedIdentity.port != listener.Port || expectedIdentity.protocol != listener.Protocol || expectedIdentity.hostname != listener.Hostname ||
			len(listener.Diagnostics) != 0 || len(listener.Rules) == 0 {
			return unsupported()
		}
		matchedSections++
		for _, match := range listener.Rules {
			if !exactHeaders && len(match.Match.Headers) != 0 || !methodMatch && match.Match.Method != "" ||
				!exactQueryParams && len(match.Match.QueryParams) != 0 {
				return unsupported()
			}
			lowered := routeLoweredMatch{RouteMatch: match, port: listener.Port, sectionHostname: gatewaycontroller.HTTPVirtualHostname(expected)}
			if section.ListenerSet.Name != "" && listener.Protocol == gatewayv1.HTTPSProtocolType {
				lowered.httpsListenerHostname = lowered.sectionHostname
			}
			matches = append(matches, lowered)
		}
	}
	if matchedSections != len(selected) {
		return unsupported()
	}
	return matches, route.Rules, nil
}

func supportedMatch(match gatewayv1.HTTPRouteMatch, exactHeaders, methodMatch, exactQueryParams bool) (cloud.PathMatchType, string, error) {
	if len(match.Headers) != 0 && !exactHeaders {
		return "", "", newRouteBuildError(routeErrorUnsupported, "exact HTTP header matching requires --octavia-http-header-exact")
	}
	if match.Method != nil && !methodMatch {
		return "", "", newRouteBuildError(routeErrorUnsupported, "HTTP method matching requires --octavia-http-method-match")
	}
	if len(match.QueryParams) != 0 && !exactQueryParams {
		return "", "", newRouteBuildError(routeErrorUnsupported, "exact HTTP query parameter matching requires --octavia-http-query-param-exact and --octavia-fixed-http-responses")
	}
	normalized, err := model.NormalizeMatch(match)
	if err != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	if _, err := cloud.NormalizeHTTPHeaderMatches(normalized.Headers); err != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	pathType := cloud.PathMatchType(normalized.PathType)
	if _, err := amphora.PathRuleValues(normalized.PathValue, pathType == cloud.PathMatchPrefix); err != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	return pathType, normalized.PathValue, nil
}
