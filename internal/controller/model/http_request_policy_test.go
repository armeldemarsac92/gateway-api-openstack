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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestNormalizeHTTPRequestPolicy(t *testing.T) {
	rule := gatewayv1.HTTPRouteRule{
		BackendRefs: []gatewayv1.HTTPBackendRef{{}},
		Timeouts: &gatewayv1.HTTPRouteTimeouts{
			Request:        ptr.To(gatewayv1.Duration("1m2s3ms")),
			BackendRequest: ptr.To(gatewayv1.Duration("0s")),
		},
		Retry: &gatewayv1.HTTPRouteRetry{Codes: []gatewayv1.HTTPRouteRetryStatusCode{504, 500, 503, 502}},
	}
	got, diagnostic := NormalizeHTTPRequestPolicy(rule, true)
	require.Nil(t, diagnostic)
	require.Equal(t, &cloud.HTTPRequestPolicy{
		RequestTimeoutMS: ptr.To(int64(62003)), BackendRequestTimeoutMS: ptr.To(int64(0)),
		Retry: &cloud.HTTPRetry{Attempts: ptr.To(1), Codes: []int{500, 502, 503, 504}},
	}, got)

	rule.Timeouts = nil
	rule.Retry.Attempts = ptr.To(0)
	got, diagnostic = NormalizeHTTPRequestPolicy(rule, true)
	require.Nil(t, diagnostic)
	require.Nil(t, got.RequestTimeoutMS)
	require.Zero(t, *got.Retry.Attempts)

	rule.Retry = nil
	rule.Timeouts = &gatewayv1.HTTPRouteTimeouts{}
	got, diagnostic = NormalizeHTTPRequestPolicy(rule, false)
	require.Nil(t, diagnostic)
	require.Nil(t, got)
}
