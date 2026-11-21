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

func structurallySupportedRoute(route *gatewayv1.HTTPRoute) bool {
	_, err := validateRouteStructure(route)
	return err == nil
}

// validateRouteStructure limits programming to the stock HTTP path profile.
// The logical compiler can represent a wider configuration without enabling it.
func validateRouteStructure(route *gatewayv1.HTTPRoute) ([]gatewayv1.BackendObjectReference, error) {
	if len(route.Spec.Rules) == 0 {
		return nil, newRouteBuildError(routeErrorUnsupported, "at least one HTTPRoute rule is required")
	}
	references := make([]gatewayv1.BackendObjectReference, 0, len(route.Spec.Rules))
	for _, rule := range route.Spec.Rules {
		if err := validateRouteRule(rule); err != nil {
			return nil, err
		}
		if len(rule.BackendRefs) != 0 {
			references = append(references, rule.BackendRefs[0].BackendObjectReference)
		}
	}
	return references, nil
}

func hasMultipleRoutePaths(route *gatewayv1.HTTPRoute) bool {
	return len(route.Spec.Rules) > 1 || len(route.Spec.Rules) == 1 && len(route.Spec.Rules[0].Matches) > 1
}

func validateRouteRule(rule gatewayv1.HTTPRouteRule) error {
	if rule.Name != nil || rule.Timeouts != nil || rule.Retry != nil || rule.SessionPersistence != nil { //nolint:staticcheck // The release still serves this deprecated Experimental field.
		return newRouteBuildError(routeErrorUnsupported, "named rules, timeouts, retries, and session persistence are not supported")
	}
	if err := compileRouteAction(rule); err != nil {
		return err
	}
	for _, match := range rule.Matches {
		if _, _, err := supportedMatch(match); err != nil {
			return err
		}
	}
	if len(rule.Filters) != 0 {
		// The only supported filter is RequestRedirect, which has no backend.
		return nil
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
	if backend.Group != nil && *backend.Group != gatewayv1.Group("") {
		return newRouteBuildError(routeErrorInvalidKind, "backendRef group %q is unsupported; use a core Service", *backend.Group)
	}
	if backend.Kind != nil && *backend.Kind != gatewayv1.Kind("Service") {
		return newRouteBuildError(routeErrorInvalidKind, "backendRef kind %q is unsupported; use Service", *backend.Kind)
	}
	if backend.Port == nil || *backend.Port < 1 || *backend.Port > 65535 {
		return newRouteBuildError(routeErrorUnsupported, "backendRef must specify a port between 1 and 65535")
	}
	return nil
}

func backendReferenceRejection(namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) *routeBuildError {
	if !model.BackendReferenceAllowed(namespace, target, grants) {
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
	if _, err := validateRouteStructure(route); err != nil {
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
	listener, err := boundRouteAttachment(route, gateway, http, namespaces)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	inputs, err := r.collectRouteModelInputs(ctx, reader, gateway, []gatewayv1.HTTPRoute{*route}, namespaces)
	if err != nil {
		return cloud.RouteSpec{}, err
	}
	spec, _, err := r.buildRouteModelFromInputs(ctx, reader, route, gateway, *listener, inputs)
	return spec, err
}

func (r *Reconciler) buildRouteModelFromInputs(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
	listener gatewayv1.Listener,
	inputs *routeModelInputs,
) (cloud.RouteSpec, model.Diagnostic, error) {
	matches, rules, err := supportedRouteModel(inputs.routeFragment(route), listener)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
	}
	mode := topology.Topology.MemberMode
	spec := cloud.RouteSpec{
		Identity: controller.RouteIdentity(r.Config, gateway, route),
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
	}
	if controller.GatewayListenerScope(r.Config, gateway) == gatewayv1.HTTPProtocolType {
		spec.GatewayRequirements.ListenerPort, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolHTTP
		spec.ScopedListenerPorts = []int{int(listener.Port)}
	}
	if !hasMultipleRoutePaths(route) && rules[0].Redirect == nil {
		match := matches[0]
		members, err := routeBackendMembers(ctx, reader, route.Namespace, rules[0].Backends[0], inputs, mode, topology.NodeAddressType)
		if err != nil {
			return cloud.RouteSpec{}, model.Diagnostic{}, err
		}
		spec.Hostname, spec.PathType, spec.PathValue, spec.Precedence = match.Hostname, cloud.PathMatchType(match.Match.PathType), match.Match.PathValue, match.Precedence
		spec.Members = members
		return spec, model.Diagnostic{}, nil
	}
	diagnostic, err := lowerHTTPRules(ctx, reader, route, listener, &spec, inputs, matches, rules, topology.NodeAddressType)
	if err != nil {
		return cloud.RouteSpec{}, model.Diagnostic{}, err
	}
	return spec, diagnostic, nil
}

func routeBackendMembers(ctx context.Context, reader client.Reader, sourceNamespace string, backend model.BackendGroup, inputs *routeModelInputs, mode cloud.MemberMode, nodeAddressType corev1.NodeAddressType) ([]cloud.Member, error) {
	key := backend.Service
	if rejection := backendReferenceRejection(sourceNamespace, key, inputs.input.ReferenceGrants); rejection != nil {
		return nil, rejection
	}
	service := inputs.service(key)
	if service == nil {
		return nil, newRouteBuildError(routeErrorBackendNotFound, "backend Service %s was not found", key)
	}
	port, err := servicePortForBackend(service, *backend.Reference.Port, mode)
	if err != nil {
		return nil, err
	}
	if mode == cloud.MemberModeEndpoint {
		return endpointMembers(service, backend.Endpoints)
	}
	return nodePortMembersFromSlices(ctx, reader, service, port, nodeAddressType, inputs.endpointSlices(key))
}

type backendKey struct {
	service types.NamespacedName
	port    int
}

func lowerHTTPRules(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute, listener gatewayv1.Listener, spec *cloud.RouteSpec, inputs *routeModelInputs, matches []model.RouteMatch, rules []model.Rule, nodeAddressType corev1.NodeAddressType) (model.Diagnostic, error) {
	groups := make(map[backendKey]int, len(rules))
	ruleGroups := make([]int, len(rules))
	var firstDiagnostic model.Diagnostic
	for i, rule := range rules {
		if redirect := rule.Redirect; redirect != nil {
			ruleGroups[i] = len(spec.HTTPRedirects)
			spec.HTTPRedirects = append(spec.HTTPRedirects, cloud.HTTPRedirect{
				RuleIndex: rule.Index, Prefix: routeRedirectPrefix(*redirect, listener), StatusCode: redirect.StatusCode,
			})
			continue
		}
		backend := rule.Backends[0]
		key := backendKey{service: backend.Service, port: int(*backend.Reference.Port)}
		index, found := groups[key]
		if !found {
			members, diagnostic, err := groupedBackendMembers(ctx, reader, route.Namespace, backend, inputs, spec.MemberMode, nodeAddressType)
			if err != nil {
				return model.Diagnostic{}, err
			}
			// Prefer an authorization failure when several references cannot resolve.
			if firstDiagnostic.Reason == "" || firstDiagnostic.Reason != string(gatewayv1.RouteReasonRefNotPermitted) &&
				diagnostic.Reason == string(gatewayv1.RouteReasonRefNotPermitted) {
				firstDiagnostic = diagnostic
			}
			index = len(spec.HTTPBackends)
			groups[key] = index
			spec.HTTPBackends = append(spec.HTTPBackends, cloud.HTTPBackend{
				Namespace: key.service.Namespace, Name: key.service.Name, Port: key.port,
				Members: members, Unavailable: len(members) == 0,
			})
		}
		ruleGroups[i] = index
	}
	for _, match := range matches {
		loweredMatch := cloud.HTTPPathMatch{
			Hostname: match.Hostname, PathType: cloud.PathMatchType(match.Match.PathType), PathValue: match.Match.PathValue, Precedence: match.Precedence,
		}
		index := ruleGroups[match.RuleIndex]
		var groupMatches *[]cloud.HTTPPathMatch
		if rules[match.RuleIndex].Redirect != nil {
			groupMatches = &spec.HTTPRedirects[index].Matches
		} else {
			groupMatches = &spec.HTTPBackends[index].Matches
		}
		// Matches arrive in precedence order. Equivalent alternatives for
		// the same action share the highest-ranked policy.
		if slices.ContainsFunc(*groupMatches, func(previous cloud.HTTPPathMatch) bool {
			return previous.Hostname == loweredMatch.Hostname && previous.PathType == loweredMatch.PathType && previous.PathValue == loweredMatch.PathValue
		}) {
			continue
		}
		*groupMatches = append(*groupMatches, loweredMatch)
	}
	return firstDiagnostic, nil
}

func groupedBackendMembers(ctx context.Context, reader client.Reader, sourceNamespace string, backend model.BackendGroup, inputs *routeModelInputs, mode cloud.MemberMode, nodeAddressType corev1.NodeAddressType) ([]cloud.Member, model.Diagnostic, error) {
	if backend.Diagnostic != nil {
		return nil, *backend.Diagnostic, nil
	}
	members, err := routeBackendMembers(ctx, reader, sourceNamespace, backend, inputs, mode, nodeAddressType)
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

// supportedRouteModel only admits fragments on one section. Logical rule
// diagnostics must be checked before creating any backend group.
func supportedRouteModel(logical model.Gateway, selected gatewayv1.Listener) ([]model.RouteMatch, []model.Rule, error) {
	unsupported := func() ([]model.RouteMatch, []model.Rule, error) {
		return nil, nil, newRouteBuildError(routeErrorUnsupported, "logical Gateway configuration is outside the supported HTTP profile")
	}
	if len(logical.Diagnostics) != 0 || len(logical.Routes) != 1 {
		return unsupported()
	}
	var listener *model.Listener
	for i := range logical.Listeners {
		candidate := &logical.Listeners[i]
		if candidate.Name == selected.Name && candidate.Port == selected.Port && candidate.Protocol == selected.Protocol && listener == nil {
			listener = candidate
			continue
		}
		// Lower one fragment only when no other section contains this route.
		if candidate.Name == selected.Name || len(candidate.Rules) != 0 {
			return unsupported()
		}
	}
	route := logical.Routes[0]
	if listener == nil || len(listener.Diagnostics) != 0 || len(route.Diagnostics) != 0 || len(listener.Rules) == 0 || len(route.Parents) != 1 {
		return unsupported()
	}
	parent := route.Parents[0]
	if parent.State != model.AttachmentAccepted {
		return unsupported()
	}
	attached := false
	for _, attachment := range parent.Listeners {
		if attachment.State != model.AttachmentAccepted {
			continue
		}
		if attached || attachment.Name != selected.Name {
			return unsupported()
		}
		attached = true
	}
	if !attached {
		return unsupported()
	}
	for _, rule := range route.Rules {
		if len(rule.Diagnostics) != 0 {
			return unsupported()
		}
		if rule.Redirect == nil && (len(rule.Backends) != 1 || rule.Backends[0].Reference.Port == nil || rule.Backends[0].Weight <= 0) {
			return unsupported()
		}
	}
	for _, match := range listener.Rules {
		if len(match.Match.Headers) != 0 {
			return unsupported()
		}
	}
	return listener.Rules, route.Rules, nil
}

func supportedMatch(match gatewayv1.HTTPRouteMatch) (cloud.PathMatchType, string, error) {
	if len(match.Headers) != 0 || len(match.QueryParams) != 0 || match.Method != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "header, query parameter, and method matches are not supported")
	}
	normalized, err := model.NormalizeMatch(match)
	if err != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	pathType := cloud.PathMatchType(normalized.PathType)
	if _, err := amphora.PathRuleValues(normalized.PathValue, pathType == cloud.PathMatchPrefix); err != nil {
		return "", "", newRouteBuildError(routeErrorUnsupported, "%s", err)
	}
	return pathType, normalized.PathValue, nil
}
