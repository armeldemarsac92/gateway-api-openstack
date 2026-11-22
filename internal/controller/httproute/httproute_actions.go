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
	"net/http"
	"slices"
	"strconv"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// compileRouteAction rejects actions that the current Amphora model cannot emit.
func compileRouteAction(rule gatewayv1.HTTPRouteRule, requestHeaderModifier, responseHeaderModifier, requestAction, requestURLRewrite bool) error {
	if len(rule.Filters) == 0 {
		if len(rule.BackendRefs) == 0 {
			return compileHTTPResponse(http.StatusInternalServerError)
		}
		return nil
	}
	if slices.ContainsFunc(rule.Filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterRequestRedirect
	}) {
		_, diagnostic := model.NormalizeRequestRedirect(rule, requestAction)
		return routeActionError(diagnostic)
	}
	_, _, _, diagnostic := model.NormalizeHTTPRuleFilters(rule, requestHeaderModifier, responseHeaderModifier, requestURLRewrite)
	return routeActionError(diagnostic)
}

func routeActionError(diagnostic *model.Diagnostic) error {
	if diagnostic == nil {
		return nil
	}
	kind := routeErrorUnsupported
	switch diagnostic.Reason {
	case string(routeErrorInvalid):
		kind = routeErrorInvalid
	case string(routeErrorIncompatibleFilters):
		kind = routeErrorIncompatibleFilters
	}
	return newRouteBuildError(kind, "%s", diagnostic.Message)
}

// lowerRequestRedirect resolves the destination as seen by the selected listener.
func lowerRequestRedirect(ruleIndex int, redirect model.RequestRedirect, listener gatewayv1.Listener) cloud.HTTPRedirect {
	scheme := redirect.Scheme
	if scheme == "" {
		scheme = "http"
		if listener.Protocol == gatewayv1.HTTPSProtocolType {
			scheme = "https"
		}
	}
	wellKnownPort := 80
	if scheme == "https" {
		wellKnownPort = 443
	}
	port := redirect.Port
	if port == 0 {
		port = wellKnownPort
		if redirect.Scheme == "" {
			port = int(listener.Port)
		}
	}
	result := cloud.HTTPRedirect{RuleIndex: ruleIndex, StatusCode: redirect.StatusCode}
	if redirect.NeedsRequestAction() {
		result.Request = &cloud.HTTPRedirectRequest{
			Scheme: scheme, Port: port, Hostname: redirect.Hostname,
			ReplaceFullPath: redirect.ReplaceFullPath, ReplacePrefixMatch: redirect.ReplacePrefixMatch,
		}
		return result
	}
	result.Prefix = scheme + "://" + redirect.Hostname
	if port != 0 && port != wellKnownPort {
		result.Prefix += ":" + strconv.Itoa(port)
	}
	return result
}

func validateRouteRedirectPrefixes(route *gatewayv1.HTTPRoute, listener gatewayv1.Listener, requestAction bool) error {
	const maxRedirectPrefixLength = 255 // Octavia's redirect_prefix SQL column.
	for _, rule := range route.Spec.Rules {
		if len(rule.Filters) != 1 || rule.Filters[0].Type != gatewayv1.HTTPRouteFilterRequestRedirect {
			continue
		}
		redirect, diagnostic := model.NormalizeRequestRedirect(rule, requestAction)
		// Structural validation owns the diagnostics for malformed actions.
		if diagnostic == nil && len(lowerRequestRedirect(0, redirect, listener).Prefix) > maxRedirectPrefixLength {
			return newRouteBuildError(routeErrorUnsupported, "RequestRedirect prefix must fit in %d characters in Octavia", maxRedirectPrefixLength)
		}
	}
	return nil
}

func compileHTTPResponse(statusCode int) error {
	// TODO(octavia): Lower explicit 404 and 500 responses when the public API
	// provides fixed responses and an unmatched-route fallback. L7 REJECT
	// returns 403, and an empty pool returns 503, so neither is a substitute.
	return newRouteBuildError(routeErrorUnsupported, "HTTP %d responses require a fixed-response action in Octavia", statusCode)
}
