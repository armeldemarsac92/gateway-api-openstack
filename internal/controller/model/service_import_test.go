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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
)

func TestServiceImportTransport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   gatewayv1.Kind
		change func(*mcs.ServiceImportSpec)
		want   gatewayv1.RouteConditionReason
	}{
		{name: "gRPC requires advertised h2c", kind: GRPCRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.Ports[0].AppProtocol = nil
		}, want: gatewayv1.RouteReasonUnsupportedProtocol},
		{name: "UDP port", kind: HTTPRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.Ports[0].Protocol = corev1.ProtocolUDP
		}, want: gatewayv1.RouteReasonUnsupportedProtocol},
		{name: "Headless even with an IP", kind: HTTPRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.Type = mcs.Headless
		}, want: gatewayv1.RouteReasonUnsupportedValue},
		{name: "IPv6 only", kind: HTTPRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.IPs = []string{"2001:db8::1"}
		}, want: gatewayv1.RouteReasonUnsupportedValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := mcs.ServiceImportSpec{Type: mcs.ClusterSetIP, IPs: []string{"10.2.0.21"},
				Ports: []mcs.ServicePort{{Port: 8080, AppProtocol: ptr.To("kubernetes.io/h2c")}},
			}
			tc.change(&spec)
			port, vip, diagnostic := serviceImportDestination(&spec, ptr.To(gatewayv1.PortNumber(8080)), tc.kind, true)
			require.NotNil(t, diagnostic)
			require.Equal(t, string(tc.want), diagnostic.Reason)
			require.Nil(t, port)
			require.Empty(t, vip)
		})
	}
}

func TestServiceImportReadinessUsesCurrentGeneration(t *testing.T) {
	imported := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	require.Nil(t, ServiceImportReadiness(imported))
	ready := mcs.NewServiceImportCondition(mcs.ServiceImportConditionReady, metav1.ConditionFalse,
		mcs.ServiceImportReasonIPFamilyNotSupported, "The IP family is unavailable")
	imported.Status.Conditions = []metav1.Condition{ready}
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), ServiceImportReadiness(imported).Reason)
	imported.Status.Conditions[0].ObservedGeneration = 1
	require.Nil(t, ServiceImportReadiness(imported))
	imported.Status.Conditions[0].ObservedGeneration = imported.Generation
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), ServiceImportReadiness(imported).Reason)
}
