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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeErrorKind string

const (
	routeErrorUnsupported         routeErrorKind = "Unsupported"
	routeErrorInvalidKind         routeErrorKind = "InvalidKind"
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
			return nil, newRouteBuildError(routeErrorUnsupported, "GRPCRoute requires an exact service and method or exact header match and one backendRef per rule. Only rule-level RequestHeaderModifier is supported.")
		}
	}
	if len(route.Spec.Rules) == 0 {
		return nil, newRouteBuildError(routeErrorUnsupported, "at least one HTTPRoute rule is required")
	}
	references := make([]gatewayv1.BackendObjectReference, 0, len(route.Spec.Rules))
	for _, rule := range route.Spec.Rules {
		if err := r.validateRouteRule(rule, grpc); err != nil {
			return nil, err
		}
		for _, backend := range rule.BackendRefs {
			references = append(references, backend.BackendObjectReference)
		}
	}
	return references, nil
}

func (r *Reconciler) usesHTTPBackendGroups(route *gatewayv1.HTTPRoute) bool {
	if r.Config.FixedHTTPResponses || r.Config.WeightedHTTPTargets || len(route.Spec.Rules) > 1 {
		return true
	}
	if len(route.Spec.Rules) == 0 {
		return false
	}
	rule := route.Spec.Rules[0]
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
	request, response, _ := model.NormalizeHTTPHeaderFilters(rule.Filters, r.Config.RequestHeaderModifier, r.Config.ResponseHeaderModifier)
	return len(request) != 0 || len(response) != 0
}

func (r *Reconciler) validateRouteRule(rule gatewayv1.HTTPRouteRule, grpc bool) error {
	fixedResponses := r.Config.FixedHTTPResponses || grpc
	weightedTargets := r.Config.WeightedHTTPTargets && !grpc
	exactHeaders := r.Config.HTTPHeaderExact
	methodMatch := r.Config.HTTPMethodMatch && !grpc
	if rule.Timeouts != nil || rule.Retry != nil || rule.SessionPersistence != nil { //nolint:staticcheck // The release still serves this deprecated Experimental field.
		return newRouteBuildError(routeErrorUnsupported, "timeouts, retries, and session persistence are not supported")
	}
	if len(rule.Filters) != 0 || !fixedResponses && !weightedTargets {
		if err := compileRouteAction(rule, r.Config.RequestHeaderModifier, r.Config.ResponseHeaderModifier && !grpc); err != nil {
			return err
		}
	}
	for _, match := range rule.Matches {
		if _, _, err := supportedMatch(match, exactHeaders, methodMatch); err != nil {
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
		return validateWeightedBackendRefs(rule.BackendRefs)
	}
	if len(rule.BackendRefs) != 1 {
		return newRouteBuildError(routeErrorUnsupported, "exactly one backendRef is supported")
	}
	backend := rule.BackendRefs[0]
	if len(backend.Filters) != 0 {
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

func validateWeightedBackendRefs(refs []gatewayv1.HTTPBackendRef) error {
	if len(refs) < 1 || len(refs) > 16 {
		return newRouteBuildError(routeErrorUnsupported, "a weighted rule requires between 1 and 16 backendRefs")
	}
	positive := false
	for _, ref := range refs {
		if len(ref.Filters) != 0 {
			return newRouteBuildError(routeErrorUnsupported, "backendRef filters are not supported")
		}
		weight := ptr.Deref(ref.Weight, 1)
		if weight < 0 || weight > 1000000 {
			return newRouteBuildError(routeErrorUnsupported, "backendRef weight must be between 0 and 1000000")
		}
		positive = positive || weight > 0
	}
	if !positive {
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

func (r *Reconciler) buildRouteModelWithReader(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
) (cloud.RouteSpec, error) {
	if _, err := r.validateRouteStructure(route); err != nil {
		return cloud.RouteSpec{}, err
	}
	http, err := gatewaycontroller.ValidatedHTTPRouteListeners(r.Config, gateway)
	if err != nil {
		return cloud.RouteSpec{}, newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	namespaces, err := readRouteNamespaces(ctx, reader, http, route.Namespace)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	listeners, err := r.boundRouteAttachments(route, gateway, http, namespaces)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	inputs, err := r.collectRouteModelInputs(ctx, reader, gateway, []gatewayv1.HTTPRoute{*route}, namespaces)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	spec, _, err := r.buildRouteModelFromInputs(ctx, reader, route, gateway, listeners, inputs)
	return spec, err
}

func (r *Reconciler) buildRouteModelFromInputs(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
	listeners []*gatewayv1.Listener,
	inputs *routeModelInputs,
) (cloud.RouteSpec, model.Diagnostic, error) {
	ordinaryHTTP := model.HTTPProfileRouteKind(route) == model.HTTPRouteKind
	listener := *listeners[0]
	isolated := gatewaycontroller.SharedHTTPSListener(r.Config, gateway, &listener)
	matches, rules, err := supportedRouteModel(inputs.routeFragment(route), listeners, isolated,
		r.Config.FixedHTTPResponses || r.Config.WeightedHTTPTargets, r.Config.WeightedHTTPTargets,
		r.Config.HTTPHeaderExact, r.Config.HTTPMethodMatch && ordinaryHTTP)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
	}
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
		TCPHealthMonitor:        model.HTTPProfileRouteKind(route) == model.GRPCRouteKind,
	}
	if controller.GatewayListenerScope(r.Config, gateway) == gatewayv1.HTTPProtocolType {
		spec.GatewayRequirements.ListenerPort, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolHTTP
		spec.ScopedListenerPorts = []int{int(listener.Port)}
	}
	if len(listeners) == 1 && isolated {
		spec.HTTPSListenerHostname = gatewaycontroller.HTTPSVirtualHostname(&listener)
	}
	if len(matches) == 1 && !r.usesHTTPBackendGroups(route) && rules[0].Redirect == nil {
		match := matches[0]
		backend := rules[0].Backends[0]
		spec.Hostname, spec.PathType, spec.PathValue, spec.Precedence = match.Hostname, cloud.PathMatchType(match.Match.PathType), match.Match.PathValue, match.Precedence
		spec.BackendTLS = inputs.backendTLSAvailability.validation(backend, ordinaryHTTP)
		if spec.BackendTLS != nil && spec.BackendTLS.Denied {
			return spec, model.Diagnostic{}, nil
		}
		members, err := routeBackendMembers(ctx, reader, backend, inputs, mode, topology.NodeAddressType)
		if err != nil {
			return cloud.RouteSpec{}, model.Diagnostic{}, err
		}
		spec.Members = members
		spec.BackendH2C = spec.BackendTLS == nil && backend.UsesH2C()
		return spec, model.Diagnostic{}, nil
	}
	diagnostic, err := r.lowerHTTPBackends(ctx, reader, route, listener, &spec, inputs, matches, rules, topology.NodeAddressType)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
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

func (r *Reconciler) lowerHTTPBackends(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute, listener gatewayv1.Listener, spec *cloud.RouteSpec, inputs *routeModelInputs, matches []routeLoweredMatch, rules []model.Rule, nodeAddressType corev1.NodeAddressType) (model.Diagnostic, error) {
	type ruleTarget struct {
		pool     *int
		weighted int
		redirect *int
		fixed    *int
	}
	groups := make(map[backendKey]httpBackendResolution, len(rules))
	targets := make([]ruleTarget, len(rules))
	var firstDiagnostic model.Diagnostic
	for _, rule := range rules {
		if redirect := rule.Redirect; redirect != nil {
			targets[rule.Index].redirect = ptr.To(len(spec.HTTPRedirects))
			spec.HTTPRedirects = append(spec.HTTPRedirects, cloud.HTTPRedirect{
				RuleIndex: rule.Index, Prefix: routeRedirectPrefix(*redirect, listener), StatusCode: redirect.StatusCode,
			})
			continue
		}
		if len(rule.Backends) == 0 {
			targets[rule.Index].fixed = ptr.To(len(spec.FixedHTTPRules))
			spec.FixedHTTPRules = append(spec.FixedHTTPRules, cloud.HTTPFixedRule{RuleIndex: rule.Index})
			continue
		}
		direct := len(rule.Backends) == 1 && hasHTTPBackendKey(rule.Backends[0])
		weighted := cloud.HTTPWeightedRule{RuleIndex: rule.Index}
		for _, backend := range rule.Backends {
			index, diagnostic, err := r.lowerHTTPBackend(ctx, reader, route, spec, inputs, backend, nodeAddressType, direct, groups)
			if err != nil {
				return model.Diagnostic{}, err
			}
			firstDiagnostic = preferredBackendDiagnostic(firstDiagnostic, diagnostic)
			if direct {
				targets[rule.Index].pool = index
			} else {
				weighted.Targets = append(weighted.Targets, cloud.HTTPWeightedTarget{BackendIndex: index, Weight: int(backend.Weight)})
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
		if target.pool != nil || target.fixed == nil && target.redirect == nil {
			operations = rules[match.RuleIndex].RequestHeaderOperations
		}
		*paths = appendHTTPPathMatch(*paths, cloud.HTTPPathMatch{
			Hostname: match.Hostname, PathType: cloud.PathMatchType(match.Match.PathType), PathValue: match.Match.PathValue, Precedence: match.Precedence,
			HTTPSListenerHostname: match.httpsListenerHostname,
			Method:                match.Match.Method, Headers: match.Match.Headers, RequestHeaderOperations: operations,
			ResponseHeaderOperations: rules[match.RuleIndex].ResponseHeaderOperations,
		})
	}
	return firstDiagnostic, nil
}

func hasHTTPBackendKey(backend model.BackendGroup) bool {
	return backend.XBackend.Name != "" || backend.Service.Name != "" && validBackendPort(backend.Reference.Port)
}

func (r *Reconciler) lowerHTTPBackend(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute, spec *cloud.RouteSpec, inputs *routeModelInputs, backend model.BackendGroup, nodeAddressType corev1.NodeAddressType, direct bool, groups map[backendKey]httpBackendResolution) (*int, model.Diagnostic, error) {
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
	tls := inputs.backendTLSAvailability.validation(backend, model.HTTPProfileRouteKind(route) == model.HTTPRouteKind)
	if !observed && (tls == nil || !tls.Denied) {
		var err error
		members, result.diagnostic, err = groupedBackendMembers(ctx, reader, backend, inputs, spec.MemberMode, nodeAddressType)
		if err != nil {
			return nil, model.Diagnostic{}, err
		}
	}
	invalid := result.diagnostic.Reason != ""
	if !direct && invalid {
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
		invalidReference := invalid && model.HTTPProfileRouteKind(route) != model.GRPCRouteKind && (key.kind == cloud.HTTPBackendKindXBackend || r.Config.WeightedHTTPTargets ||
			result.diagnostic.Reason == string(gatewayv1.RouteReasonRefNotPermitted) ||
			result.diagnostic.Reason == string(gatewayv1.RouteReasonBackendNotFound))
		spec.HTTPBackends = append(spec.HTTPBackends, cloud.HTTPBackend{
			Kind: key.kind, Namespace: key.object.Namespace, Name: key.object.Name, Port: port,
			Members: members, Unavailable: len(members) == 0, InvalidReference: invalidReference,
			BackendH2C: !invalid && tls == nil && backend.UsesH2C(), BackendTLS: tls,
		})
	}
	groups[key] = result
	return result.index, result.diagnostic, nil
}

func appendHTTPPathMatch(matches []cloud.HTTPPathMatch, match cloud.HTTPPathMatch) []cloud.HTTPPathMatch {
	// Equivalent alternatives targeting the same action share one policy identity.
	if slices.ContainsFunc(matches, func(previous cloud.HTTPPathMatch) bool {
		return previous.HTTPSListenerHostname == match.HTTPSListenerHostname && previous.Hostname == match.Hostname &&
			previous.PathType == match.PathType && previous.PathValue == match.PathValue &&
			previous.Method == match.Method && slices.Equal(previous.Headers, match.Headers)
	}) {
		return matches
	}
	return append(matches, match)
}

func preferredBackendDiagnostic(current, next model.Diagnostic) model.Diagnostic {
	// Prefer an authorization failure when several references cannot resolve.
	if current.Reason == "" || current.Reason != string(gatewayv1.RouteReasonRefNotPermitted) && next.Reason == string(gatewayv1.RouteReasonRefNotPermitted) {
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
}

// supportedRouteModel verifies that the compiled attachments match the sections
// selected by admission before lowering any backend or policy.
func supportedRouteModel(logical model.Gateway, selected []*gatewayv1.Listener, isolated, fixedResponses, weightedTargets, exactHeaders, methodMatch bool) ([]routeLoweredMatch, []model.Rule, error) {
	unsupported := func() ([]routeLoweredMatch, []model.Rule, error) {
		return nil, nil, newRouteBuildError(routeErrorUnsupported, "logical Gateway configuration is outside the supported HTTP profile")
	}
	if len(logical.Diagnostics) != 0 || len(logical.Routes) != 1 {
		return unsupported()
	}
	route := logical.Routes[0]
	if len(route.Diagnostics) != 0 || len(route.Parents) != 1 {
		return unsupported()
	}
	parent := route.Parents[0]
	if parent.State != model.AttachmentAccepted {
		return unsupported()
	}
	sections := make(map[gatewayv1.SectionName]*gatewayv1.Listener, len(selected))
	for _, listener := range selected {
		sections[listener.Name] = listener
	}
	attached := 0
	for _, attachment := range parent.Listeners {
		if attachment.State != model.AttachmentAccepted {
			continue
		}
		if sections[attachment.Name] == nil {
			return unsupported()
		}
		attached++
	}
	if attached == 0 || attached != len(sections) {
		return unsupported()
	}
	for _, rule := range route.Rules {
		if len(rule.Diagnostics) != 0 {
			return unsupported()
		}
		if rule.Redirect != nil || len(rule.Backends) == 0 && fixedResponses {
			continue
		}
		if !weightedTargets && (len(rule.Backends) != 1 || !hasHTTPBackendKey(rule.Backends[0]) || rule.Backends[0].Weight <= 0) {
			return unsupported()
		}
	}
	var matches []routeLoweredMatch
	for _, listener := range logical.Listeners {
		expected := sections[listener.Name]
		if expected == nil {
			if len(listener.Rules) != 0 {
				return unsupported()
			}
			continue
		}
		if listener.Port != expected.Port || listener.Protocol != expected.Protocol || len(listener.Diagnostics) != 0 || len(listener.Rules) == 0 {
			return unsupported()
		}
		delete(sections, listener.Name)
		var hostname string
		if isolated {
			hostname = gatewaycontroller.HTTPSVirtualHostname(expected)
		}
		for _, match := range listener.Rules {
			if !exactHeaders && len(match.Match.Headers) != 0 || !methodMatch && match.Match.Method != "" {
				return unsupported()
			}
			matches = append(matches, routeLoweredMatch{RouteMatch: match, httpsListenerHostname: hostname})
		}
	}
	if len(sections) != 0 {
		return unsupported()
	}
	return matches, route.Rules, nil
}

func supportedMatch(match gatewayv1.HTTPRouteMatch, exactHeaders, methodMatch bool) (cloud.PathMatchType, string, error) {
	if len(match.Headers) != 0 && !exactHeaders {
		return "", "", newRouteBuildError(routeErrorUnsupported, "exact HTTP header matching requires --octavia-http-header-exact")
	}
	if match.Method != nil && !methodMatch {
		return "", "", newRouteBuildError(routeErrorUnsupported, "HTTP method matching requires --octavia-http-method-match")
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
