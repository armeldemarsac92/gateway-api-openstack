//go:build envtest && kindtest

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

package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/conformance/utils/udp"
)

// The upstream grant case checks status after deletion. This local case checks
// the data plane while the same Gateway, route and both echo Deployments stay up.
var weightedUDPGrantTraffic = suite.ConformanceTest{
	ShortName: "LocalUDPWeightedGrantTraffic",
	Manifests: tests.UDPRouteReferenceGrant.Manifests,
	Features:  tests.UDPRouteReferenceGrant.Features,
	Test:      testWeightedUDPGrantTraffic,
}

func testWeightedUDPGrantTraffic(t *testing.T, s *suite.ConformanceTestSuite) {
	ns := suite.InfrastructureNamespace
	routeKey := client.ObjectKey{Namespace: ns, Name: "udp-route-reference-grant"}
	gatewayKey := client.ObjectKey{Namespace: ns, Name: "udp-gateway-reference-grant"}
	var route gatewayv1.UDPRoute
	require.NoError(t, s.Client.Get(t.Context(), routeKey, &route))
	remote := route.Spec.Rules[0].BackendRefs[0]
	local := gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Name: "udp-echo", Port: ptr.To(gatewayv1.PortNumber(8080)),
	}}
	setWeight := func(weight int32) {
		t.Helper()
		require.NoError(t, s.Client.Get(t.Context(), routeKey, &route))
		base := route.DeepCopy()
		remote.Weight = &weight
		route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{local, remote}
		require.NoError(t, s.Client.Patch(t.Context(), &route, client.MergeFrom(base)))
	}
	setWeight(1)
	addr := kubernetes.GatewayAndUDPRoutesMustBeAccepted(t, s.Client, s.TimeoutConfig, s.ControllerName, kubernetes.NewGatewayRef(gatewayKey), routeKey)
	for _, backend := range []udp.ExpectedResponse{
		{Service: string(local.Name), Namespace: ns},
		{Service: string(remote.Name), Namespace: suite.AppBackendNamespace},
	} {
		udp.ExpectEchoResponseFromBackend(t, s.TimeoutConfig, addr, backend)
	}
	require.False(t, t.Failed(), "both Services must answer before revocation")
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: suite.AppBackendNamespace, Name: "udp-reference-grant"}}
	require.NoError(t, s.Client.Delete(t.Context(), grant))
	denied := metav1.Condition{Type: "ResolvedRefs", Status: metav1.ConditionFalse, Reason: string(gatewayv1.RouteReasonRefNotPermitted)}
	kubernetes.UDPRouteMustHaveCondition(t, s.Client, s.TimeoutConfig, routeKey, gatewayKey, denied)
	probe := func(phase string) (kindUDPEcho, error) {
		return probeKindUDPEcho(addr, fmt.Sprintf("hibiscus-grant-%s-%d", phase, time.Now().UnixNano()))
	}
	rejected := func(phase string) (bool, error) {
		_, err := probe(phase)
		var timeout net.Error
		if errors.Is(err, syscall.ECONNREFUSED) || errors.As(err, &timeout) && timeout.Timeout() {
			return true, nil
		}
		return false, err
	}
	require.NoError(t, wait.PollUntilContextTimeout(t.Context(), s.TimeoutConfig.DefaultPollInterval,
		s.TimeoutConfig.MaxTimeToConsistency, true, func(_ context.Context) (bool, error) { return rejected("transition") }))
	for range 3 {
		dropped, err := rejected("withdrawn")
		require.NoError(t, err)
		require.True(t, dropped, "the withdrawn VIP returned data after revocation")
	}
	t.Log("New UDP sockets received no reply after revocation at positive weight")

	setWeight(0)
	kubernetes.UDPRouteMustHaveCondition(t, s.Client, s.TimeoutConfig, routeKey, gatewayKey, denied)
	udp.ExpectEchoResponseFromBackend(t, s.TimeoutConfig, addr, udp.ExpectedResponse{Service: string(local.Name), Namespace: ns})
	for range 20 {
		got, err := probe("permitted")
		require.NoError(t, err)
		require.Equal(t, ns, got.Namespace)
		require.Equal(t, string(local.Name), got.Service)
	}
	t.Log("The permitted Service answered 20 fresh requests on the same VIP with the revoked reference at zero weight")
}

type kindUDPEcho struct{ Namespace, Service, Request string }

// probeKindUDPEcho sends one datagram from a fresh socket and verifies its payload.
func probeKindUDPEcho(address, payload string) (kindUDPEcho, error) {
	var got kindUDPEcho
	conn, err := net.DialTimeout("udp", address, time.Second)
	if err != nil {
		return got, err
	}
	defer func() { _ = conn.Close() }()
	if err = conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return got, err
	}
	if _, err = conn.Write([]byte(payload)); err != nil {
		return got, err
	}
	var response [2048]byte
	n, err := conn.Read(response[:])
	if err != nil {
		return got, err
	}
	if err := json.Unmarshal(response[:n], &got); err != nil {
		return got, err
	}
	if got.Request != payload {
		return got, fmt.Errorf("backend echoed %q, want %q", got.Request, payload)
	}
	return got, nil
}
