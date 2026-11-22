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
	"cmp"
	"fmt"
	"slices"
	"strings"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// NormalizeMatch applies Gateway API defaults and normalizes supported HTTP matches.
// Unsupported predicates return a diagnostic rather than widening the match.
func NormalizeMatch(input gatewayv1.HTTPRouteMatch) (Match, error) {
	result := Match{PathType: gatewayv1.PathMatchPathPrefix, PathValue: "/"}
	if input.Method != nil {
		if !cloud.ValidHTTPMethod(string(*input.Method)) {
			return Match{}, unsupported(fmt.Sprintf("unknown HTTP method %q", *input.Method), ScopeRoute)
		}
		result.Method = string(*input.Method)
	}
	seenQueries := make(map[gatewayv1.HTTPHeaderName]bool, len(input.QueryParams))
	for _, query := range input.QueryParams {
		result.QueryParams = append(result.QueryParams, cloud.HTTPQueryParamMatch{Name: string(query.Name), Value: query.Value})
		if seenQueries[query.Name] {
			continue
		}
		seenQueries[query.Name] = true
		if query.Type != nil && *query.Type != gatewayv1.QueryParamMatchExact {
			scope := ScopeRoute
			if *query.Type == gatewayv1.QueryParamMatchRegularExpression {
				scope = ScopeRule
			}
			return Match{}, unsupported(fmt.Sprintf("query parameter match type %q is not implemented", *query.Type), scope)
		}
	}
	queryParams, err := cloud.NormalizeHTTPQueryParamMatches(result.QueryParams)
	if err != nil {
		return Match{}, unsupported(err.Error(), ScopeRule)
	}
	result.QueryParams = queryParams
	if input.Path != nil {
		if input.Path.Type != nil {
			result.PathType = *input.Path.Type
		}
		if input.Path.Value != nil {
			result.PathValue = *input.Path.Value
		}
	}
	switch result.PathType {
	case gatewayv1.PathMatchExact, gatewayv1.PathMatchPathPrefix:
	case gatewayv1.PathMatchRegularExpression:
		return Match{}, unsupported("regular expression paths are not implemented", ScopeRule)
	default:
		return Match{}, unsupported(fmt.Sprintf("unknown path match type %q", result.PathType), ScopeRoute)
	}
	if !cloud.ValidHTTPPath(result.PathValue) {
		return Match{}, unsupported(fmt.Sprintf("path %q is not a valid absolute Gateway API path", result.PathValue), ScopeRule)
	}
	if result.PathType == gatewayv1.PathMatchPathPrefix && len(result.PathValue) > 1 {
		result.PathValue = strings.TrimSuffix(result.PathValue, "/")
	}
	seen := make(map[string]bool, len(input.Headers))
	for _, header := range input.Headers {
		name := strings.ToLower(string(header.Name))
		// Gateway API uses the first case-equivalent header entry.
		if seen[name] {
			continue
		}
		seen[name] = true
		if header.Type != nil && *header.Type != gatewayv1.HeaderMatchExact {
			scope := ScopeRoute
			if *header.Type == gatewayv1.HeaderMatchRegularExpression {
				scope = ScopeRule
			}
			return Match{}, unsupported(fmt.Sprintf("header match type %q is not implemented", *header.Type), scope)
		}
		result.Headers = append(result.Headers, HeaderMatch{Name: name, Value: header.Value})
	}
	slices.SortFunc(result.Headers, func(a, b HeaderMatch) int { return cmp.Compare(a.Name, b.Name) })
	return result, nil
}

func unsupported(message string, scope DiagnosticScope) *Diagnostic {
	return &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: message, Scope: scope}
}

// MatchingRouteHostname returns the most specific original route hostname that
// matches the effective hostname selected through listener intersection.
func MatchingRouteHostname[T ~string](effective string, hostnames []T) string {
	best := ""
	for _, hostname := range hostnames {
		host := string(hostname)
		if host == effective {
			return host
		}
		if wildcardMatches(host, effective) && len(host) > len(best) {
			best = host
		}
	}
	return best
}

// compareMatches uses original route hostnames to break ties introduced by
// listener intersection before applying path, method, header, query and route precedence.
func compareMatches(a, b RouteMatch) int {
	if order := CompareHostnameSpecificity(a.Hostname, b.Hostname); order != 0 {
		return order
	}
	if order := CompareHostnameSpecificity(a.routeHostname, b.routeHostname); order != 0 {
		return order
	}
	if a.Match.PathType != b.Match.PathType {
		if a.Match.PathType == gatewayv1.PathMatchExact {
			return -1
		}
		return 1
	}
	if a.Match.PathType == gatewayv1.PathMatchPathPrefix {
		if order := cmp.Compare(len(b.Match.PathValue), len(a.Match.PathValue)); order != 0 {
			return order
		}
	}
	if (a.Match.Method == "") != (b.Match.Method == "") {
		if a.Match.Method != "" {
			return -1
		}
		return 1
	}
	if order := cmp.Compare(len(b.Match.Headers), len(a.Match.Headers)); order != 0 {
		return order
	}
	if order := cmp.Compare(len(b.Match.QueryParams), len(a.Match.QueryParams)); order != 0 {
		return order
	}
	if order := a.Route.CreationTimestamp.Compare(b.Route.CreationTimestamp.Time); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Route.Namespace+"/"+a.Route.Name, b.Route.Namespace+"/"+b.Route.Name); order != 0 {
		return order
	}
	if order := cmp.Compare(a.RuleIndex, b.RuleIndex); order != 0 {
		return order
	}
	if order := cmp.Compare(a.MatchIndex, b.MatchIndex); order != 0 {
		return order
	}
	return cmp.Compare(a.Hostname, b.Hostname)
}

// CompareHostnameSpecificity orders a more specific Gateway API hostname first.
func CompareHostnameSpecificity(a, b string) int {
	aExact, aLength := hostnameSpecificity(a)
	bExact, bLength := hostnameSpecificity(b)
	if order := cmp.Compare(bExact, aExact); order != 0 {
		return order
	}
	return cmp.Compare(bLength, aLength)
}

func hostnameSpecificity(hostname string) (int, int) {
	if strings.HasPrefix(hostname, "*.") {
		return 0, len(hostname)
	}
	return len(hostname), len(hostname)
}
