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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestRequestMirrorPhasesAndRatios(t *testing.T) {
	mirror := func(name string) gatewayv1.HTTPRouteFilter {
		return gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterRequestMirror, RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{
			BackendRef: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(name), Port: ptr.To(gatewayv1.PortNumber(80))},
		}}
	}
	before, between, after, zero := mirror("first"), mirror("second"), mirror("second"), mirror("zero")
	between.RequestMirror.Percent = ptr.To(int32(20))
	after.RequestMirror.Fraction = &gatewayv1.Fraction{Numerator: 32123, Denominator: ptr.To(int32(98765))}
	zero.RequestMirror.Fraction = &gatewayv1.Fraction{Numerator: 0}
	headers := gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterRequestHeaderModifier, RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{Remove: []string{"X-Private"}}}
	rewrite := gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterURLRewrite, URLRewrite: &gatewayv1.HTTPURLRewriteFilter{Hostname: ptr.To(gatewayv1.PreciseHostname("mirror.example"))}}
	filters := []gatewayv1.HTTPRouteFilter{before, headers, between, rewrite, after, zero, after}
	got, diagnostic := NormalizeRequestMirrors(filters, true, true)
	require.Nil(t, diagnostic)
	type sample struct {
		name  gatewayv1.ObjectName
		phase cloud.HTTPMirrorPhase
		n, d  int32
	}
	var samples []sample
	for _, item := range got {
		samples = append(samples, sample{item.Backend.Reference.Name, item.Phase, item.Numerator, item.Denominator})
	}
	require.Equal(t, []sample{
		{"first", cloud.HTTPMirrorBeforeTransforms, 1, 1},
		{"second", cloud.HTTPMirrorAfterHeaders, 20, 100},
		{"second", cloud.HTTPMirrorAfterRewrite, 32123, 98765},
		{"zero", cloud.HTTPMirrorAfterRewrite, 0, 100},
		{"second", cloud.HTTPMirrorAfterRewrite, 32123, 98765},
	}, samples)
	onlyHeaders, diagnostic := NormalizeRequestMirrors([]gatewayv1.HTTPRouteFilter{headers, after}, true, true)
	require.Nil(t, diagnostic)
	require.Equal(t, cloud.HTTPMirrorAfterRewrite, onlyHeaders[0].Phase)
	_, diagnostic = NormalizeRequestMirrors([]gatewayv1.HTTPRouteFilter{rewrite, between, headers}, true, true)
	require.NotNil(t, diagnostic)
	require.Equal(t, string(gatewayv1.RouteReasonIncompatibleFilters), diagnostic.Reason)
	_, diagnostic = NormalizeRequestMirrors(filters, false, true)
	require.Contains(t, diagnostic.Message, "--octavia-http-request-mirror")
}
