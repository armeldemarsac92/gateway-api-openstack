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

// NormalizeHTTPForwardingRuleFilters validates and preserves each rule's
// policy-level CORS settings alongside its existing forwarding filters.
func NormalizeHTTPForwardingRuleFilters(rule gatewayv1.HTTPRouteRule, kind gatewayv1.Kind, requestEnabled, responseEnabled, rewriteEnabled, corsEnabled bool) (
	[]cloud.HTTPHeaderOperation, []cloud.HTTPHeaderOperation, *cloud.HTTPURLRewrite, *cloud.HTTPCORS, *Diagnostic,
) {
	var cors *cloud.HTTPCORS
	index := slices.IndexFunc(rule.Filters, isCORSFilter)
	if index >= 0 {
		for _, filter := range rule.Filters {
			if !knownFilter(filter.Type) {
				return nil, nil, nil, nil, unsupported(fmt.Sprintf("HTTPRoute filter %s is not supported", filter.Type), ScopeRoute)
			}
		}
		var diagnostic *Diagnostic
		cors, diagnostic = normalizeCORS(rule, index, corsEnabled)
		if diagnostic != nil {
			return nil, nil, nil, nil, diagnostic
		}
		rule.Filters = slices.Delete(slices.Clone(rule.Filters), index, index+1)
	}
	request, response, rewrite, diagnostic := NormalizeHTTPRuleFilters(rule, requestEnabled, responseEnabled, rewriteEnabled)
	if diagnostic == nil && kind == GRPCRouteKind {
		diagnostic = validateGRPCResponseMetadata(response)
	}
	if diagnostic == nil && cors != nil {
		diagnostic = ValidateCORSHeaderComposition(request, response)
	}
	return request, response, rewrite, cors, diagnostic
}

func isCORSFilter(filter gatewayv1.HTTPRouteFilter) bool {
	return filter.Type == gatewayv1.HTTPRouteFilterCORS
}

func normalizeCORS(rule gatewayv1.HTTPRouteRule, index int, enabled bool) (*cloud.HTTPCORS, *Diagnostic) {
	if !enabled {
		return nil, unsupported("CORS requires --octavia-request-cors", ScopeRule)
	}
	filter := rule.Filters[index].CORS
	if filter == nil || slices.ContainsFunc(rule.Filters[index+1:], isCORSFilter) {
		return nil, corsDiagnostic("Invalid", "a rule must specify exactly one cors object for its CORS filter")
	}
	if slices.ContainsFunc(rule.Filters, func(f gatewayv1.HTTPRouteFilter) bool { return f.Type == gatewayv1.HTTPRouteFilterRequestRedirect }) {
		return nil, corsDiagnostic(gatewayv1.RouteReasonIncompatibleFilters, "Octavia CORS cannot be combined with RequestRedirect")
	}
	if len(rule.BackendRefs) == 0 {
		return nil, unsupported("Octavia CORS requires a forwarding rule with backendRefs", ScopeRule)
	}
	cors := &cloud.HTTPCORS{
		AllowOrigins: corsStrings(filter.AllowOrigins), AllowMethods: corsStrings(filter.AllowMethods),
		AllowHeaders: corsStrings(filter.AllowHeaders), ExposeHeaders: corsStrings(filter.ExposeHeaders),
		AllowCredentials: filter.AllowCredentials, MaxAge: filter.MaxAge,
	}
	if err := cors.Validate(); err != nil {
		return nil, unsupported(err.Error(), ScopeRule)
	}
	return cors, nil
}

func corsStrings[T ~string](values []T) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	return result
}

func ValidateCORSHeaderComposition(request, response []cloud.HTTPHeaderOperation) *Diagnostic {
	if err := cloud.ValidateHTTPCORSComposition(request, response); err != nil {
		return corsDiagnostic(gatewayv1.RouteReasonIncompatibleFilters, err.Error())
	}
	return nil
}

func corsDiagnostic(reason gatewayv1.RouteConditionReason, message string) *Diagnostic {
	return &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(reason), Scope: ScopeRule, Message: message}
}
