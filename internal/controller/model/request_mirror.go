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

	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// RequestMirror keeps the sampling ratio and execution point of one occurrence.
type RequestMirror struct {
	Backend     BackendGroup
	Numerator   int32
	Denominator int32
	Phase       cloud.HTTPMirrorPhase
}

func IsRequestMirror(filter gatewayv1.HTTPRouteFilter) bool {
	return filter.Type == gatewayv1.HTTPRouteFilterRequestMirror
}

func HasRequestMirrors(rule gatewayv1.HTTPRouteRule) bool {
	return slices.ContainsFunc(rule.Filters, IsRequestMirror) || slices.ContainsFunc(rule.BackendRefs, func(backend gatewayv1.HTTPBackendRef) bool {
		return slices.ContainsFunc(backend.Filters, IsRequestMirror)
	})
}

// RequestMirrorReferences includes each declared destination, including zero
// samples: they still participate in reference authorization and status.
func RequestMirrorReferences(rule gatewayv1.HTTPRouteRule) []gatewayv1.BackendObjectReference {
	var refs []gatewayv1.BackendObjectReference
	add := func(filters []gatewayv1.HTTPRouteFilter) {
		for _, filter := range filters {
			if IsRequestMirror(filter) && filter.RequestMirror != nil {
				refs = append(refs, filter.RequestMirror.BackendRef)
			}
		}
	}
	add(rule.Filters)
	for _, backend := range rule.BackendRefs {
		add(backend.Filters)
	}
	return refs
}

// HTTPServiceBackendReferences includes mirror destinations for dependency
// observation, without treating them as weighted forwarding targets.
func HTTPServiceBackendReferences(rule gatewayv1.HTTPRouteRule, includeZeroWeight bool) []gatewayv1.BackendObjectReference {
	refs := make([]gatewayv1.BackendObjectReference, 0, len(rule.BackendRefs))
	for _, backend := range rule.BackendRefs {
		if includeZeroWeight || ptr.Deref(backend.Weight, 1) != 0 {
			refs = append(refs, backend.BackendObjectReference)
		}
	}
	return append(refs, RequestMirrorReferences(rule)...)
}

// WithoutRequestMirrorFilters leaves transformations for their established
// validators. It never changes the supplied rule or its backing arrays.
func WithoutRequestMirrorFilters(filters []gatewayv1.HTTPRouteFilter) []gatewayv1.HTTPRouteFilter {
	if !slices.ContainsFunc(filters, IsRequestMirror) {
		return filters
	}
	return slices.DeleteFunc(slices.Clone(filters), IsRequestMirror)
}

func NormalizeRequestMirrors(filters []gatewayv1.HTTPRouteFilter, enabled, forwarding bool) ([]RequestMirror, *Diagnostic) {
	if !slices.ContainsFunc(filters, IsRequestMirror) {
		return nil, nil
	}
	if !enabled {
		return nil, unsupported("RequestMirror requires --octavia-http-request-mirror", ScopeRule)
	}
	if slices.ContainsFunc(filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterRequestRedirect
	}) {
		return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted),
			Reason: string(gatewayv1.RouteReasonIncompatibleFilters), Scope: ScopeRule,
			Message: "Octavia RequestMirror cannot be combined with RequestRedirect"}
	}
	if !forwarding {
		return nil, unsupported("RequestMirror requires a forwarding rule with backendRefs", ScopeRule)
	}

	headers := slices.IndexFunc(filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		h := filter.RequestHeaderModifier
		return filter.Type == gatewayv1.HTTPRouteFilterRequestHeaderModifier && h != nil && (len(h.Set)+len(h.Add)+len(h.Remove) > 0)
	})
	rewrite := slices.IndexFunc(filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		rw := filter.URLRewrite
		return filter.Type == gatewayv1.HTTPRouteFilterURLRewrite && rw != nil && (rw.Hostname != nil || rw.Path != nil)
	})
	var mirrors []RequestMirror
	for i, filter := range filters {
		if !IsRequestMirror(filter) {
			continue
		}
		phase := cloud.HTTPMirrorAfterRewrite
		switch {
		case rewrite >= 0 && rewrite < i && headers > i:
			return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted),
				Reason: string(gatewayv1.RouteReasonIncompatibleFilters), Scope: ScopeRule,
				Message: "Octavia cannot mirror after URLRewrite and before RequestHeaderModifier"}
		case rewrite >= 0 && rewrite > i && headers >= 0 && headers < i:
			phase = cloud.HTTPMirrorAfterHeaders
		case rewrite > i || headers > i:
			phase = cloud.HTTPMirrorBeforeTransforms
		}

		mirror := filter.RequestMirror
		if mirror == nil || mirror.Percent != nil && mirror.Fraction != nil {
			return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: "Invalid", Scope: ScopeRule,
				Message: "RequestMirror must specify requestMirror and at most one of percent and fraction"}
		}
		numerator, denominator := int32(1), int32(1)
		if mirror.Percent != nil {
			numerator, denominator = *mirror.Percent, 100
		} else if mirror.Fraction != nil {
			numerator, denominator = mirror.Fraction.Numerator, ptr.Deref(mirror.Fraction.Denominator, 100)
		}
		if !cloud.ValidHTTPMirrorRatio(numerator, denominator) {
			return nil, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: "Invalid", Scope: ScopeRule,
				Message: "RequestMirror requires a ratio between zero and one"}
		}
		mirrors = append(mirrors, RequestMirror{Numerator: numerator, Denominator: denominator, Phase: phase,
			Backend: BackendGroup{Reference: gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: *mirror.BackendRef.DeepCopy(),
			}}}})
	}
	return mirrors, nil
}

func ResolveRequestMirrorBackends(route *gatewayv1.HTTPRoute, mirrors []RequestMirror, input Input) {
	input.EnableXBackend = false
	for i := range mirrors {
		mirrors[i].Backend = resolveBackend(route, mirrors[i].Backend.Reference, input)
	}
}

// ResolveBackendsAndMirrors enumerates TLS policy consumers without assigning
// the sampling ratio or the primary selection weight to a mirror destination.
func ResolveBackendsAndMirrors(route *gatewayv1.HTTPRoute, rule gatewayv1.HTTPRouteRule, input Input) []BackendGroup {
	groups := ResolveBackends(route, rule, input)
	input.EnableXBackend = false
	for _, reference := range RequestMirrorReferences(rule) {
		groups = append(groups, resolveBackend(route, gatewayv1.HTTPBackendRef{
			BackendRef: gatewayv1.BackendRef{BackendObjectReference: reference},
		}, input))
	}
	return groups
}
