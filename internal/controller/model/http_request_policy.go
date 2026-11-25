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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package model

import (
	"regexp"
	"slices"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// HasHTTPRequestPolicy reports whether a rule requires Octavia's forwarding
// extension. Empty timeouts leave both defaults in place.
func HasHTTPRequestPolicy(rule gatewayv1.HTTPRouteRule) bool {
	return rule.Retry != nil || rule.Timeouts != nil && (rule.Timeouts.Request != nil || rule.Timeouts.BackendRequest != nil)
}

func NormalizeHTTPRequestPolicy(rule gatewayv1.HTTPRouteRule, enabled bool) (*cloud.HTTPRequestPolicy, *Diagnostic) {
	if !HasHTTPRequestPolicy(rule) {
		return nil, nil
	}
	if !enabled {
		return nil, unsupported("HTTPRoute timeouts and retries require --octavia-http-request-policy", ScopeRoute)
	}
	if len(rule.BackendRefs) == 0 || slices.ContainsFunc(rule.Filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterRequestRedirect
	}) {
		return nil, unsupported("HTTPRoute timeouts and retries require a forwarding rule", ScopeRoute)
	}
	policy := &cloud.HTTPRequestPolicy{}
	if rule.Timeouts != nil {
		var ok bool
		policy.RequestTimeoutMS, ok = durationMilliseconds(rule.Timeouts.Request)
		if !ok {
			return nil, unsupported("HTTPRoute timeouts.request is not a valid Gateway API duration", ScopeRoute)
		}
		policy.BackendRequestTimeoutMS, ok = durationMilliseconds(rule.Timeouts.BackendRequest)
		if !ok {
			return nil, unsupported("HTTPRoute timeouts.backendRequest is not a valid Gateway API duration", ScopeRoute)
		}
	}
	if rule.Retry != nil {
		backoff, ok := durationMilliseconds(rule.Retry.Backoff)
		if !ok {
			return nil, unsupported("HTTPRoute retry.backoff is not a valid Gateway API duration", ScopeRoute)
		}
		policy.Retry = &cloud.HTTPRetry{Attempts: ptr.To(ptr.Deref(rule.Retry.Attempts, 1)), BackoffMS: backoff}
		for _, code := range rule.Retry.Codes {
			policy.Retry.Codes = append(policy.Retry.Codes, int(code))
		}
		slices.Sort(policy.Retry.Codes)
	}
	if err := policy.Validate(); err != nil {
		return nil, unsupported(err.Error(), ScopeRoute)
	}
	return policy, nil
}

// Match the v1.7.0 Gateway API duration grammar even for in-memory callers.
var gatewayDurationPattern = regexp.MustCompile(`^([0-9]{1,5}(h|m|s|ms)){1,4}$`)

func durationMilliseconds(value *gatewayv1.Duration) (*int64, bool) {
	if value == nil {
		return nil, true
	}
	if !gatewayDurationPattern.MatchString(string(*value)) {
		return nil, false
	}
	duration, err := time.ParseDuration(string(*value))
	return ptr.To(duration.Milliseconds()), err == nil
}
