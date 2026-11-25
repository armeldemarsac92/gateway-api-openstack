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
	"errors"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Compile builds a deterministic HTTP model without reading or changing inputs.
// Input diagnostics remain in the returned model. They are not cloud deletion
// decisions, status patches, or permission to omit unsupported traffic rules.
func Compile(input Input) (Gateway, error) {
	if input.Gateway == nil {
		return Gateway{}, errors.New("gateway input is required")
	}
	result := Gateway{Source: source(input.Gateway), Listeners: compileListeners(input.Gateway.Spec.Listeners, input.OperatorHTTPS, input.GatewayListenerRequestFilters)}
	if UnsupportedGatewayWideTLS(input.Gateway.Spec.TLS) {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{
			Condition: string(gatewayv1.GatewayConditionAccepted), Reason: string(gatewayv1.GatewayReasonInvalidParameters),
			Message: "Gateway-wide spec.tls requires frontend or backend configuration",
		})
	}
	gatewayListeners := result.Listeners
	setListeners := make(map[types.NamespacedName][]Listener, len(input.ListenerSetSections))
	for key, sections := range input.ListenerSetSections {
		compiled := compileListeners(sections, input.OperatorHTTPS, nil)
		for i := range compiled {
			compiled[i].ListenerSet = key
		}
		setListeners[key] = compiled
		result.Listeners = append(result.Listeners, compiled...)
	}
	seenRoutes := make(map[string]bool, len(input.Routes))
	for i := range input.Routes {
		route := &input.Routes[i]
		kind := HTTPProfileRouteKind(route)
		key := string(kind) + "/" + route.Namespace + "/" + route.Name
		if seenRoutes[key] {
			return Gateway{}, fmt.Errorf("duplicate route input %s", key)
		}
		seenRoutes[key] = true
		refs := EffectiveParentRefs(route.Spec.CommonRouteSpec, route.Namespace, input.Gateway, input.DefaultGateways)
		var parents []int
		for index, parent := range refs {
			_, delegated := setListeners[ListenerSetRefKey(parent, route.Namespace)]
			if targetsGateway(parent, route.Namespace, input.Gateway) || kind == HTTPRouteKind && delegated {
				parents = append(parents, index)
			}
		}
		if len(parents) == 0 {
			continue
		}
		compiled := compileRoute(route, input)
		for _, index := range parents {
			ref := refs[index]
			key := ListenerSetRefKey(ref, route.Namespace)
			sections, listeners, namespace := input.Gateway.Spec.Listeners, gatewayListeners, input.Gateway.Namespace
			if key.Name != "" {
				sections, listeners, namespace = input.ListenerSetSections[key], setListeners[key], key.Namespace
			}
			specIndex := index
			if index == len(route.Spec.ParentRefs) {
				specIndex = -1
			}
			parent := attachParent(input, route, ref, specIndex, sections, listeners, namespace)
			if len(compiled.Diagnostics) != 0 {
				parent.State = AttachmentRejected
				parent.Diagnostics = append(parent.Diagnostics, compiled.Diagnostics...)
			}
			compiled.Parents = append(compiled.Parents, parent)
		}
		result.Routes = append(result.Routes, compiled)
	}
	slices.SortFunc(result.Routes, func(a, b Route) int {
		return cmp.Compare(a.Source.Namespace+"/"+a.Source.Name, b.Source.Namespace+"/"+b.Source.Name)
	})
	for i := range result.Listeners {
		populateListener(&result.Listeners[i], result.Routes)
	}
	slices.SortFunc(result.Listeners, func(a, b Listener) int {
		return cmp.Or(cmp.Compare(a.ListenerSet.Namespace, b.ListenerSet.Namespace), cmp.Compare(a.ListenerSet.Name, b.ListenerSet.Name), cmp.Compare(a.Name, b.Name))
	})
	return result, nil
}

// UnsupportedGatewayWideTLS rejects an empty Gateway TLS configuration.
func UnsupportedGatewayWideTLS(tls *gatewayv1.GatewayTLSConfig) bool {
	return tls != nil && tls.Frontend == nil && tls.Backend == nil
}

// FrontendClientValidation selects the configuration shared by all HTTPS
// sections on a port. An override without validation replaces the default too.
func FrontendClientValidation(tls *gatewayv1.GatewayTLSConfig, port gatewayv1.PortNumber) *gatewayv1.FrontendTLSValidation {
	if tls == nil || tls.Frontend == nil {
		return nil
	}
	for _, override := range tls.Frontend.PerPort {
		if override.Port == port {
			return override.TLS.Validation
		}
	}
	return tls.Frontend.Default.Validation
}

func compileListeners(sections []gatewayv1.Listener, operatorHTTPS bool, handledFilters map[gatewayv1.SectionName]bool) []Listener {
	result := make([]Listener, 0, len(sections))
	for _, listener := range sections {
		compiled := Listener{Name: listener.Name, Port: listener.Port, Protocol: listener.Protocol}
		if listener.Hostname != nil {
			compiled.Hostname = string(*listener.Hostname)
		}
		switch {
		case listener.Filters != nil && len(listener.Filters.Requests) > 0 && !handledFilters[listener.Name]:
			compiled.Diagnostics = append(compiled.Diagnostics, Diagnostic{
				Condition: string(gatewayv1.ListenerConditionAccepted), Reason: string(gatewayv1.ListenerReasonUnsupportedValue),
				Message: "pre-routing request filters are not implemented by the HTTP compiler",
			})
		case listener.Protocol != gatewayv1.HTTPProtocolType && (listener.Protocol != gatewayv1.HTTPSProtocolType || !operatorHTTPS):
			compiled.Diagnostics = append(compiled.Diagnostics, Diagnostic{
				Condition: string(gatewayv1.ListenerConditionAccepted), Reason: string(gatewayv1.ListenerReasonUnsupportedProtocol),
				Message: "only HTTP listeners can be resolved without certificate inputs",
			})
		case listener.Protocol == gatewayv1.HTTPProtocolType && listener.TLS != nil:
			compiled.Diagnostics = append(compiled.Diagnostics, Diagnostic{
				Condition: string(gatewayv1.ListenerConditionAccepted), Reason: string(gatewayv1.ListenerReasonUnsupportedValue),
				Message: "TLS configuration is unsupported on an HTTP listener",
			})
		case listener.Port < 1 || listener.Port > 65535:
			compiled.Diagnostics = append(compiled.Diagnostics, Diagnostic{
				Condition: string(gatewayv1.ListenerConditionAccepted), Reason: string(gatewayv1.ListenerReasonPortUnavailable),
				Message: "listener port must be between 1 and 65535",
			})
		}
		if listener.AllowedRoutes != nil {
			for _, kind := range listener.AllowedRoutes.Kinds {
				supportedKind := kind.Kind == HTTPRouteKind || kind.Kind == GRPCRouteKind && listener.Protocol == gatewayv1.HTTPProtocolType
				if !supportedKind || kind.Group != nil && *kind.Group != gatewayv1.GroupName {
					compiled.Diagnostics = append(compiled.Diagnostics, Diagnostic{
						Condition: string(gatewayv1.ListenerConditionResolvedRefs), Reason: string(gatewayv1.ListenerReasonInvalidRouteKinds),
						Message: "listener allowedRoutes includes a kind not supported by the HTTP compiler",
					})
					break
				}
			}
		}
		result = append(result, compiled)
	}
	// Only supported protocols participate in conflicts. In particular, an
	// unsupported TCP listener cannot displace HTTP on the same port.
	// Exact and wildcard hostnames may overlap, but identical HTTP or HTTPS hostnames
	// on the same port conflict regardless of input order or TLS configuration.
	for i := range result {
		protocol := result[i].Protocol
		if protocol != gatewayv1.HTTPProtocolType && (protocol != gatewayv1.HTTPSProtocolType || !operatorHTTPS) {
			continue
		}
		for j := i + 1; j < len(result); j++ {
			if result[j].Protocol != protocol || result[i].Port != result[j].Port {
				continue
			}
			if result[i].Hostname == result[j].Hostname {
				diagnostic := Diagnostic{Condition: string(gatewayv1.ListenerConditionConflicted), Reason: string(gatewayv1.ListenerReasonHostnameConflict), Message: fmt.Sprintf("%s listeners on the same port have identical hostnames", protocol)}
				result[i].Diagnostics = append(result[i].Diagnostics, diagnostic)
				result[j].Diagnostics = append(result[j].Diagnostics, diagnostic)
			}
		}
	}
	for i := range result {
		slices.SortFunc(result[i].Diagnostics, func(a, b Diagnostic) int {
			return cmp.Compare(a.Condition+"/"+a.Reason+"/"+a.Message, b.Condition+"/"+b.Reason+"/"+b.Message)
		})
		result[i].Diagnostics = slices.Compact(result[i].Diagnostics)
	}
	return result
}

func compileRoute(route *gatewayv1.HTTPRoute, input Input) Route {
	result := Route{Source: source(route)}
	for _, hostname := range route.Spec.Hostnames {
		result.Hostnames = append(result.Hostnames, string(hostname))
	}
	slices.Sort(result.Hostnames)
	result.Hostnames = slices.Compact(result.Hostnames)
	switch route.Spec.UseDefaultGateways {
	case "", gatewayv1.GatewayDefaultScopeNone, gatewayv1.GatewayDefaultScopeAll:
	default:
		result.Diagnostics = append(result.Diagnostics, *unsupported(fmt.Sprintf("unsupported default Gateway scope %q", route.Spec.UseDefaultGateways), ScopeRoute))
	}
	supportedRules := 0
	for i, rule := range route.Spec.Rules {
		// Rule names identify API sections. The index groups matches by rule and breaks precedence ties.
		compiled := Rule{Index: i}
		kind := HTTPProfileRouteKind(route)
		ordinaryHTTP := kind == HTTPRouteKind
		if ordinaryHTTP && input.CheckRouteExternalAuth && HasHTTPRouteExternalAuth(rule) {
			compiled.ExternalAuth, compiled.ExternalAuthDiagnostic = NormalizeHTTPRouteExternalAuth(rule, input.RouteExternalAuth, input.RouteExternalAuthGRPC, input.WeightedHTTPTargets)
			rule.Filters = nil
			if compiled.ExternalAuthDiagnostic != nil {
				// Preserve the rule's matches for local rejection.
				rule.BackendRefs, rule.Timeouts, rule.Retry = nil, nil, nil
				rule.SessionPersistence = nil //nolint:staticcheck // Still served in the v1.7 Experimental bundle.
			}
		}
		compiled.Backends = ResolveBackends(route, rule, input)
		mirrors, diagnostic := NormalizeRequestMirrors(rule.Filters, input.HTTPRequestMirror, len(rule.BackendRefs) > 0)
		if diagnostic != nil {
			compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
		}
		ResolveRequestMirrorBackends(route, mirrors, input)
		compiled.RequestMirrors = mirrors
		if persistence := RouteRulePersistence(rule); persistence != nil {
			if diagnostic := ValidateRouteSessionPersistence(rule, kind, input.RouteSessionPersistence); diagnostic != nil {
				compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
			} else {
				position := i
				if original := input.OriginalRulePositions[route.UID]; len(original) > i {
					position = original[i]
				}
				for j := range compiled.Backends {
					backend := &compiled.Backends[j]
					if backend.Service.Name != "" {
						backend.RouteSessionPersistence = routeSessionPersistence(route.UID, rule.Name, position, *backend, *persistence)
					}
				}
			}
		}
		requestPolicy, diagnostic := NormalizeHTTPRequestPolicy(rule, input.HTTPRequestPolicy && HTTPProfileRouteKind(route) == HTTPRouteKind)
		if diagnostic != nil {
			compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
		}
		compiled.RequestPolicy = requestPolicy
		if len(rule.Filters) == 1 && rule.Filters[0].Type == gatewayv1.HTTPRouteFilterRequestRedirect {
			redirect, err := NormalizeRequestRedirect(rule, input.HTTPRequestRedirect)
			if err != nil {
				compiled.Diagnostics = append(compiled.Diagnostics, *err)
			} else {
				compiled.Redirect = &redirect
			}
		} else {
			forwarding := rule
			forwarding.Filters = WithoutRequestMirrorFilters(rule.Filters)
			request, response, rewrite, cors, diagnostic := NormalizeHTTPForwardingRuleFilters(forwarding, kind, input.RequestHeaderModifier,
				input.ResponseHeaderModifier, input.HTTPRequestURLRewrite && ordinaryHTTP, input.HTTPCORS && ordinaryHTTP)
			if diagnostic != nil {
				compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
			} else {
				compiled.RequestHeaderOperations, compiled.ResponseHeaderOperations, compiled.URLRewrite, compiled.CORS = request, response, rewrite, cors
			}
		}
		for i, backend := range rule.BackendRefs {
			mirrors, diagnostic := NormalizeRequestMirrors(backend.Filters, input.HTTPRequestMirror, true)
			if diagnostic != nil {
				compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
			}
			ResolveRequestMirrorBackends(route, mirrors, input)
			compiled.Backends[i].RequestMirrors = mirrors
			backend.Filters = WithoutRequestMirrorFilters(backend.Filters)
			request, response, rewrite, diagnostic := NormalizeHTTPBackendFilters(rule, backend,
				kind, input.BackendRequestFilters, input.BackendResponseHeaders,
				compiled.RequestHeaderOperations, compiled.URLRewrite)
			if diagnostic != nil {
				if diagnostic.Condition == string(gatewayv1.RouteConditionResolvedRefs) {
					compiled.Backends[i].FilterDiagnostic = diagnostic
					compiled.Backends[i].State = BackendInvalid
				} else {
					compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
				}
				continue
			}
			if compiled.CORS != nil {
				if diagnostic := ValidateCORSHeaderComposition(request, response); diagnostic != nil {
					compiled.Diagnostics = append(compiled.Diagnostics, *diagnostic)
					continue
				}
			}
			compiled.Backends[i].RequestHeaderOperations = request
			compiled.Backends[i].ResponseHeaderOperations = response
			compiled.Backends[i].URLRewrite = rewrite
		}
		matches := rule.Matches
		if len(matches) == 0 {
			matches = []gatewayv1.HTTPRouteMatch{{}}
		}
		for matchIndex, match := range matches {
			normalized, err := NormalizeMatch(match)
			if err != nil {
				compiled.Diagnostics = append(compiled.Diagnostics, *err.(*Diagnostic))
				continue
			}
			normalized.Index = matchIndex
			compiled.Matches = append(compiled.Matches, normalized)
		}
		for _, diagnostic := range compiled.Diagnostics {
			if diagnostic.Scope == ScopeRoute {
				result.Diagnostics = append(result.Diagnostics, diagnostic)
			}
		}
		if len(compiled.Diagnostics) == 0 {
			supportedRules++
		}
		result.Rules = append(result.Rules, compiled)
	}
	if len(result.Rules) != 0 && supportedRules == 0 && len(result.Diagnostics) == 0 {
		result.Diagnostics = append(result.Diagnostics, *unsupported("HTTPRoute has no supported rules", ScopeRoute))
	}
	return result
}

func knownFilter(kind gatewayv1.HTTPRouteFilterType) bool {
	switch kind {
	case gatewayv1.HTTPRouteFilterRequestHeaderModifier, gatewayv1.HTTPRouteFilterResponseHeaderModifier,
		gatewayv1.HTTPRouteFilterRequestMirror, gatewayv1.HTTPRouteFilterRequestRedirect,
		gatewayv1.HTTPRouteFilterURLRewrite, gatewayv1.HTTPRouteFilterExtensionRef,
		gatewayv1.HTTPRouteFilterCORS, gatewayv1.HTTPRouteFilterExternalAuth:
		return true
	default:
		return false
	}
}

func populateListener(listener *Listener, routes []Route) {
	for _, route := range routes {
		hostnames := make([]string, 0)
		for _, parent := range route.Parents {
			if parent.State != AttachmentAccepted || ListenerSetRefKey(parent.Reference, route.Source.Namespace) != listener.ListenerSet {
				continue
			}
			for _, attachment := range parent.Listeners {
				if attachment.Name == listener.Name && attachment.State == AttachmentAccepted {
					hostnames = append(hostnames, attachment.Hostnames...)
				}
			}
		}
		if len(hostnames) == 0 {
			continue
		}
		listener.AttachedRoutes++
		slices.Sort(hostnames)
		hostnames = slices.Compact(hostnames)
		for _, hostname := range hostnames {
			original := MatchingRouteHostname(hostname, route.Hostnames)
			for _, rule := range route.Rules {
				// Keep unsupported actions in the order so lowering can see their diagnostics.
				for _, match := range rule.Matches {
					listener.Rules = append(listener.Rules, RouteMatch{
						Route: route.Source, RuleIndex: rule.Index, MatchIndex: match.Index,
						Hostname: hostname, routeHostname: original, Match: match,
					})
				}
			}
		}
	}
	slices.SortStableFunc(listener.Rules, compareMatches)
	for i := range listener.Rules {
		listener.Rules[i].Precedence = i
	}
}
