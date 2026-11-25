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
	"strings"

	"golang.org/x/net/http/httpguts"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// HTTPExternalAuth is the normalized configuration and the unresolved Service reference.
type HTTPExternalAuth struct {
	Config     cloud.ListenerExternalAuth
	BackendRef gatewayv1.BackendObjectReference
}

// NormalizeHTTPExternalAuth validates the bodyless HTTP profile shared by route and listener filters.
func NormalizeHTTPExternalAuth(filter *gatewayv1.HTTPExternalAuthFilter) (*HTTPExternalAuth, *Diagnostic) {
	invalid := func(message string) (*HTTPExternalAuth, *Diagnostic) {
		return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: "Invalid", Message: message, Scope: ScopeRule}
	}
	if filter == nil {
		return invalid("ExternalAuth requires an externalAuth configuration")
	}
	if filter.ExternalAuthProtocol != gatewayv1.HTTPRouteExternalAuthHTTPProtocol {
		return nil, unsupported("ExternalAuth supports HTTP authorization", ScopeRule)
	}
	if filter.GRPCAuthConfig != nil || filter.ForwardBody != nil && filter.ForwardBody.MaxSize < 0 {
		return invalid("Invalid HTTP ExternalAuth options")
	}
	if filter.ForwardBody != nil && filter.ForwardBody.MaxSize > 0 {
		return nil, unsupported("ExternalAuth request body forwarding is unsupported", ScopeRule)
	}
	var auth cloud.ListenerExternalAuth
	if options := filter.HTTPAuthConfig; options != nil {
		if !cloud.ValidExternalAuthPathPrefix(options.Path) {
			return invalid("ExternalAuth HTTP path prefix is invalid")
		}
		if !validAuthHeaders(options.AllowedRequestHeaders) || !validAuthHeaders(options.AllowedResponseHeaders) {
			return invalid("ExternalAuth header list contains an invalid name")
		}
		auth.PathPrefix = options.Path
		auth.AllowedRequestHeaders = normalizeAuthHeaders(options.AllowedRequestHeaders)
		auth.AllowedResponseHeaders = normalizeAuthHeaders(options.AllowedResponseHeaders)
	}
	if key, supported := BackendServiceReference("", filter.BackendRef); !supported || key.Name == "" {
		return nil, backendDiagnostic(gatewayv1.RouteReasonInvalidKind, "ExternalAuth backendRef must refer to a core Service")
	}
	return &HTTPExternalAuth{Config: auth, BackendRef: filter.BackendRef}, nil
}

// HasHTTPRouteExternalAuth also recognizes misplaced backend filters so the
// rule can be rejected locally without leaving its match available to a fallback.
func HasHTTPRouteExternalAuth(rule gatewayv1.HTTPRouteRule) bool {
	if slices.ContainsFunc(rule.Filters, isExternalAuth) {
		return true
	}
	return slices.ContainsFunc(rule.BackendRefs, func(backend gatewayv1.HTTPBackendRef) bool {
		return slices.ContainsFunc(backend.Filters, isExternalAuth)
	})
}

// HTTPRouteExternalAuthReferences provides the declared authorization
// dependency for route watches, independent of the current grant or endpoints.
func HTTPRouteExternalAuthReferences(rule gatewayv1.HTTPRouteRule) []gatewayv1.BackendObjectReference {
	var refs []gatewayv1.BackendObjectReference
	for _, filter := range rule.Filters {
		if isExternalAuth(filter) && filter.ExternalAuth != nil {
			refs = append(refs, filter.ExternalAuth.BackendRef)
		}
	}
	return refs
}

func isExternalAuth(filter gatewayv1.HTTPRouteFilter) bool {
	return filter.Type == gatewayv1.HTTPRouteFilterExternalAuth
}

// NormalizeHTTPRouteExternalAuth checks the supported shape of a rule containing ExternalAuth.
func NormalizeHTTPRouteExternalAuth(rule gatewayv1.HTTPRouteRule, enabled, weighted bool) (*HTTPExternalAuth, *Diagnostic) {
	incompatible := func(message string) (*HTTPExternalAuth, *Diagnostic) {
		return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonIncompatibleFilters), Message: message, Scope: ScopeRule}
	}
	if !enabled {
		return nil, unsupported("HTTPRoute ExternalAuth requires --octavia-http-route-external-auth", ScopeRule)
	}
	if len(rule.Filters) != 1 || !isExternalAuth(rule.Filters[0]) ||
		slices.ContainsFunc(rule.BackendRefs, func(backend gatewayv1.HTTPBackendRef) bool { return len(backend.Filters) != 0 }) {
		return incompatible("ExternalAuth must be the only filter on a rule and cannot be used in backendRefs")
	}
	if filter := rule.Filters[0]; filter.RequestHeaderModifier != nil || filter.ResponseHeaderModifier != nil ||
		filter.RequestMirror != nil || filter.RequestRedirect != nil || filter.URLRewrite != nil ||
		filter.ExtensionRef != nil || filter.CORS != nil {
		return incompatible("ExternalAuth cannot be combined with other filter configurations")
	}
	if HasHTTPRequestPolicy(rule) || RouteRulePersistence(rule) != nil {
		return incompatible("ExternalAuth cannot be combined with rule timeouts, retries or session persistence")
	}
	if len(rule.BackendRefs) == 0 || len(rule.BackendRefs) > 16 {
		return nil, unsupported("ExternalAuth requires between 1 and 16 forwarding backendRefs", ScopeRule)
	}
	if len(rule.BackendRefs) > 1 && !weighted {
		return nil, unsupported("ExternalAuth with multiple forwarding backendRefs requires --octavia-weighted-http-targets", ScopeRule)
	}
	positive := false
	for _, backend := range rule.BackendRefs {
		if key, supported := BackendServiceReference("", backend.BackendObjectReference); !supported || key.Name == "" {
			return nil, backendDiagnostic(gatewayv1.RouteReasonInvalidKind, "ExternalAuth forwarding backendRefs must refer to core Services")
		}
		if port := ptr.Deref(backend.Port, 0); port < 1 || port > 65535 {
			return nil, unsupported("ExternalAuth forwarding backendRefs must specify a port between 1 and 65535", ScopeRule)
		}
		weight := ptr.Deref(backend.Weight, 1)
		if weight < 0 || weight > 1000000 {
			return nil, unsupported("backendRef weight must be between 0 and 1000000", ScopeRule)
		}
		positive = positive || weight > 0
	}
	if !positive {
		return nil, unsupported("ExternalAuth requires a positive forwarding backend weight", ScopeRule)
	}
	return NormalizeHTTPExternalAuth(rule.Filters[0].ExternalAuth)
}

func normalizeAuthHeaders(headers []string) []string {
	if len(headers) == 0 {
		return nil
	}
	names := make([]string, len(headers))
	for i, name := range headers {
		names[i] = strings.ToLower(name)
		if names[i] == ":authority" {
			// Octavia accepts HTTP field names. Retain a targeted list
			// containing only the protected authority as a targeted list.
			names[i] = "host"
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func validAuthHeaders(headers []string) bool {
	return len(headers) <= 64 && !slices.ContainsFunc(headers, func(name string) bool {
		return name != ":authority" && (len(name) > 256 || !httpguts.ValidHeaderFieldName(name))
	})
}
