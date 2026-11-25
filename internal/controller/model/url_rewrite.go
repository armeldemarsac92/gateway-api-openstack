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
	"fmt"
	"slices"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// NormalizeHTTPRuleFilters validates forwarding filters with independent
// opt-ins for header operations and URL rewriting.
func NormalizeHTTPRuleFilters(rule gatewayv1.HTTPRouteRule, requestEnabled, responseEnabled, rewriteEnabled bool) (
	[]cloud.HTTPHeaderOperation, []cloud.HTTPHeaderOperation, *cloud.HTTPURLRewrite, *Diagnostic,
) {
	filters := rule.Filters
	index := slices.IndexFunc(filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterURLRewrite
	})
	var rewrite *cloud.HTTPURLRewrite
	if index >= 0 {
		if slices.ContainsFunc(filters[index+1:], func(filter gatewayv1.HTTPRouteFilter) bool {
			return filter.Type == gatewayv1.HTTPRouteFilterURLRewrite
		}) {
			return nil, nil, nil, invalidURLRewrite("only one URLRewrite filter is allowed per rule")
		}
		var diagnostic *Diagnostic
		rewrite, diagnostic = normalizeURLRewrite(filters[index].URLRewrite, rule.Matches)
		if diagnostic != nil {
			return nil, nil, nil, diagnostic
		}
		if rewrite != nil {
			if !rewriteEnabled {
				return nil, nil, nil, unsupported("URLRewrite requires --octavia-request-url-rewrite", ScopeRule)
			}
			if len(rule.BackendRefs) == 0 {
				return nil, nil, nil, unsupported("URLRewrite requires a forwarding rule with backendRefs", ScopeRule)
			}
		}
		filters = slices.Delete(slices.Clone(filters), index, index+1)
	}
	request, response, err := normalizeHTTPHeaderFilters(filters, requestEnabled, responseEnabled)
	if err != nil {
		return nil, nil, nil, unsupported(err.Error(), ScopeRule)
	}
	// Amphora applies request headers first, then rewrites the authority
	// in both the Host header and absolute-form request targets.
	if rewrite != nil && rewrite.Hostname != "" && slices.ContainsFunc(rule.Filters[index+1:], func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterRequestHeaderModifier
	}) && slices.ContainsFunc(request, func(operation cloud.HTTPHeaderOperation) bool {
		return operation.Name == "host" && (operation.Operation != cloud.HTTPHeaderSet || operation.Value != rewrite.Hostname)
	}) {
		return nil, nil, nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted),
			Reason: string(gatewayv1.RouteReasonIncompatibleFilters), Scope: ScopeRule,
			Message: "RequestHeaderModifier cannot change Host after URLRewrite changes the hostname on Octavia"}
	}
	return request, response, rewrite, nil
}

func normalizeURLRewrite(filter *gatewayv1.HTTPURLRewriteFilter, matches []gatewayv1.HTTPRouteMatch) (*cloud.HTTPURLRewrite, *Diagnostic) {
	if filter == nil {
		return nil, invalidURLRewrite("URLRewrite must specify urlRewrite")
	}
	if filter.Hostname == nil && filter.Path == nil {
		return nil, nil
	}
	rewrite := &cloud.HTTPURLRewrite{}
	if filter.Hostname != nil {
		rewrite.Hostname = string(*filter.Hostname)
		if !validURLModifierHostname(rewrite.Hostname) {
			return nil, invalidURLRewrite("URLRewrite hostname must be a fixed DNS name")
		}
	}
	if path := filter.Path; path != nil {
		switch path.Type {
		case gatewayv1.FullPathHTTPPathModifier:
			if path.ReplaceFullPath == nil || path.ReplacePrefixMatch != nil || !cloud.ValidHTTPPathReplacement(*path.ReplaceFullPath) {
				return nil, invalidURLRewrite("URLRewrite ReplaceFullPath requires only a replaceFullPath value of up to 1024 characters")
			}
			rewrite.ReplaceFullPath = path.ReplaceFullPath
		case gatewayv1.PrefixMatchHTTPPathModifier:
			if path.ReplacePrefixMatch == nil || path.ReplaceFullPath != nil || !cloud.ValidHTTPPathReplacement(*path.ReplacePrefixMatch) {
				return nil, invalidURLRewrite("URLRewrite ReplacePrefixMatch requires only a replacePrefixMatch value of up to 1024 characters")
			}
			if len(matches) != 1 || matches[0].Path == nil || matches[0].Path.Type != nil && *matches[0].Path.Type != gatewayv1.PathMatchPathPrefix {
				return nil, invalidURLRewrite("URLRewrite ReplacePrefixMatch requires exactly one PathPrefix match")
			}
			rewrite.ReplacePrefixMatch = path.ReplacePrefixMatch
		default:
			return nil, unsupported(fmt.Sprintf("URLRewrite path modifier type %q is unsupported", path.Type), ScopeRule)
		}
	}
	return rewrite, nil
}

func invalidURLRewrite(message string) *Diagnostic {
	return &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: "Invalid", Message: message, Scope: ScopeRule}
}

// NormalizeHTTPBackendRequestFilters validates filters for one backend occurrence
// using the enclosing rule's path matches and request transformations.
func NormalizeHTTPBackendRequestFilters(rule gatewayv1.HTTPRouteRule, backend gatewayv1.HTTPBackendRef, kind gatewayv1.Kind, enabled bool,
	ruleRequest []cloud.HTTPHeaderOperation, ruleRewrite *cloud.HTTPURLRewrite,
) ([]cloud.HTTPHeaderOperation, *cloud.HTTPURLRewrite, *Diagnostic) {
	if len(backend.Filters) == 0 {
		return nil, nil, nil
	}
	if kind == GRPCRouteKind {
		for _, filter := range backend.Filters {
			if filter.Type != gatewayv1.HTTPRouteFilterRequestHeaderModifier {
				return nil, nil, unsupported(fmt.Sprintf("GRPCRoute backendRef filter %q is not supported", filter.Type), ScopeRule)
			}
		}
	}
	for _, filter := range backend.Filters {
		if !knownFilter(filter.Type) {
			return nil, nil, unsupported(fmt.Sprintf("backendRef filter %q is not supported", filter.Type), ScopeRoute)
		}
	}
	if slices.ContainsFunc(backend.Filters, isCORSFilter) {
		return nil, nil, unsupported("Octavia CORS is supported on HTTPRoute rules only", ScopeRule)
	}
	if unresolved := UnresolvedHTTPBackendExtension(backend); unresolved != nil {
		return nil, nil, unresolved
	}
	if !enabled {
		return nil, nil, unsupported("backendRef request filters require --octavia-http-backend-request-filters and --octavia-weighted-http-targets", ScopeRule)
	}
	matches := rule.Matches
	for _, filter := range backend.Filters {
		switch filter.Type {
		case gatewayv1.HTTPRouteFilterRequestHeaderModifier:
		case gatewayv1.HTTPRouteFilterURLRewrite:
			if filter.URLRewrite != nil && filter.URLRewrite.Path != nil &&
				filter.URLRewrite.Path.Type == gatewayv1.PrefixMatchHTTPPathModifier {
				for _, match := range rule.Matches {
					if match.Path != nil && match.Path.Type != nil && *match.Path.Type != gatewayv1.PathMatchPathPrefix {
						return nil, nil, invalidURLRewrite("backendRef URLRewrite ReplacePrefixMatch requires PathPrefix matches")
					}
				}
				// The normalized replacement is reused for each individual
				// PathPrefix when compiling the weighted policies.
				matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{}}}
			}
		default:
			return nil, nil, unsupported(fmt.Sprintf("backendRef filter %q is not supported", filter.Type), ScopeRule)
		}
	}
	request, _, rewrite, diagnostic := NormalizeHTTPRuleFilters(gatewayv1.HTTPRouteRule{
		Filters: backend.Filters, BackendRefs: []gatewayv1.HTTPBackendRef{backend}, Matches: matches,
	}, true, false, true)
	if diagnostic != nil {
		return nil, nil, diagnostic
	}
	if kind == GRPCRouteKind {
		if err := cloud.ValidateGRPCRequestMetadata(request); err != nil {
			return nil, nil, unsupported(err.Error(), ScopeRule)
		}
	}
	if err := cloud.ValidateHTTPBackendRequestFilterComposition(ruleRequest, request, ruleRewrite, rewrite); err != nil {
		return nil, nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonIncompatibleFilters),
			Scope: ScopeRule, Message: err.Error()}
	}
	return request, rewrite, nil
}

// UnresolvedHTTPBackendExtension reports a known ExtensionRef type whose
// custom filter cannot be resolved by this controller.
func UnresolvedHTTPBackendExtension(backend gatewayv1.HTTPBackendRef) *Diagnostic {
	for _, filter := range backend.Filters {
		if filter.Type != gatewayv1.HTTPRouteFilterExtensionRef {
			continue
		}
		if ref := filter.ExtensionRef; ref != nil {
			return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue,
				fmt.Sprintf("backendRef ExtensionRef filter %s/%s %q cannot be resolved", ref.Group, ref.Kind, ref.Name))
		}
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backendRef ExtensionRef filter is not supported")
	}
	return nil
}
