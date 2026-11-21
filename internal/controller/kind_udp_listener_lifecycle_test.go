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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/conformance/utils/udp"
	"sigs.k8s.io/gateway-api/pkg/features"
)

func TestKindUDPListenerLifecycle(t *testing.T) {
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if channel != "standard" && channel != "experimental" {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	base, manifests := kindConformanceManifests(t, requireKindTrafficEnv(t, "HIBISCUS_STATUS_BASE_DIR"))
	runKindTrafficConformance(t, channel, "HIBISCUS_UDP", suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName: "kind-udp-listener-lifecycle", CleanupBaseResources: true, CleanupTestResources: true,
			DisableParallelTests: true, SupportedFeatures: []features.FeatureName{features.SupportGateway, features.SupportUDPRoute},
			TimeoutConfig: config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second},
		},
		BaseManifests: base,
		ManifestFS:    manifests,
	}, []suite.ConformanceTest{{
		ShortName: "LocalUDPListenerLifecycle",
		Manifests: tests.UDPRouteTest.Manifests,
		Features:  tests.UDPRouteTest.Features,
		Test:      testUDPListenerLifecycle,
	}})
}

func testUDPListenerLifecycle(t *testing.T, s *suite.ConformanceTestSuite) {
	const ns = suite.InfrastructureNamespace
	gatewayKey := client.ObjectKey{Namespace: ns, Name: "udp-gateway"}
	routeKey := client.ObjectKey{Namespace: ns, Name: "udp-route"}
	var route gatewayv1.UDPRoute
	require.NoError(t, s.Client.Get(t.Context(), routeKey, &route))
	routeBefore := route.DeepCopy()
	// A Gateway-wide parent stays eligible on both ports as listeners change.
	route.Spec.ParentRefs[0].SectionName = nil
	require.NoError(t, s.Client.Patch(t.Context(), &route, client.MergeFrom(routeBefore)))

	var gateway gatewayv1.Gateway
	require.NoError(t, s.Client.Get(t.Context(), gatewayKey, &gateway))
	require.Len(t, gateway.Spec.Listeners, 1)
	original := gateway.Spec.Listeners[0]
	sibling := original
	sibling.Name, sibling.Port = "udp-sibling", original.Port+1

	checkGateway := func(generation int64, want ...gatewayv1.SectionName) string {
		t.Helper()
		var current gatewayv1.Gateway
		err := wait.PollUntilContextTimeout(t.Context(), s.TimeoutConfig.DefaultPollInterval,
			s.TimeoutConfig.MaxTimeToConsistency, true, func(ctx context.Context) (bool, error) {
				if err := s.Client.Get(ctx, gatewayKey, &current); err != nil {
					return false, err
				}
				accepted := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
				if accepted == nil || accepted.ObservedGeneration != generation {
					return false, nil
				}
				if accepted.Status == metav1.ConditionFalse {
					return false, fmt.Errorf("Gateway generation %d rejected listeners %v: %s: %s", generation, want, accepted.Reason, accepted.Message)
				}
				if accepted.Status != metav1.ConditionTrue || len(current.Status.Addresses) != 1 || len(current.Status.Listeners) != len(want) {
					return false, nil
				}
				for _, listener := range current.Status.Listeners {
					if !slices.Contains(want, listener.Name) || listener.AttachedRoutes != 1 ||
						len(listener.SupportedKinds) != 1 || listener.SupportedKinds[0].Kind != "UDPRoute" {
						return false, nil
					}
					for _, kind := range []gatewayv1.ListenerConditionType{gatewayv1.ListenerConditionAccepted,
						gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerConditionProgrammed} {
						condition := meta.FindStatusCondition(listener.Conditions, string(kind))
						if condition == nil || condition.ObservedGeneration != generation || condition.Status != metav1.ConditionTrue {
							return false, nil
						}
					}
				}
				return true, nil
			})
		require.NoError(t, err, "Gateway state for listeners %v: %+v", want, current.Status)
		return current.Status.Addresses[0].Value
	}

	backend := udp.ExpectedResponse{Service: "udp-echo", Namespace: ns}
	probe := func(vip string, port gatewayv1.PortNumber) {
		t.Helper()
		udp.ExpectEchoResponseFromBackend(t, s.TimeoutConfig, net.JoinHostPort(vip, strconv.Itoa(int(port))), backend)
		require.False(t, t.Failed(), "UDP echo failed on port %d", port)
	}
	vip := checkGateway(gateway.Generation, original.Name)
	kubernetes.UDPRouteMustHaveCondition(t, s.Client, s.TimeoutConfig, routeKey, gatewayKey,
		metav1.Condition{Type: string(gatewayv1.RouteConditionResolvedRefs), Status: metav1.ConditionTrue, Reason: string(gatewayv1.RouteReasonResolvedRefs)})
	probe(vip, original.Port)

	endpoint := "http://" + requireKindTrafficEnv(t, "HIBISCUS_UDP_FIXTURE") + "/"
	octavia := &gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Timeout: 5 * time.Second}},
		Endpoint:       endpoint, ResourceBase: endpoint + "v2/",
	}
	lbPage, err := loadbalancers.List(octavia, loadbalancers.ListOpts{
		ProjectID: requireKindTrafficEnv(t, "HIBISCUS_UDP_PROJECT"), VipAddress: vip,
	}).AllPages(t.Context())
	require.NoError(t, err)
	lbs, err := loadbalancers.ExtractLoadBalancers(lbPage)
	require.NoError(t, err)
	require.Len(t, lbs, 1)
	lbID := lbs[0].ID

	checkOctavia := func(want ...gatewayv1.PortNumber) map[gatewayv1.PortNumber]listeners.Listener {
		t.Helper()
		var active map[gatewayv1.PortNumber]listeners.Listener
		err := wait.PollUntilContextTimeout(t.Context(), s.TimeoutConfig.DefaultPollInterval,
			s.TimeoutConfig.MaxTimeToConsistency, true, func(ctx context.Context) (bool, error) {
				lb, err := loadbalancers.Get(ctx, octavia, lbID).Extract()
				if err != nil {
					return false, err
				}
				if lb.VipAddress != vip || lb.ProvisioningStatus != "ACTIVE" ||
					len(lb.Listeners) != len(want) || len(lb.Pools) != 1 {
					return false, nil
				}
				active = make(map[gatewayv1.PortNumber]listeners.Listener, len(want))
				for _, ref := range lb.Listeners {
					listener, err := listeners.Get(ctx, octavia, ref.ID).Extract()
					if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
						return false, nil
					}
					if err != nil {
						return false, err
					}
					port := gatewayv1.PortNumber(listener.ProtocolPort)
					if !slices.Contains(want, port) || listener.Protocol != "UDP" || listener.DefaultPoolID != lb.Pools[0].ID {
						return false, nil
					}
					active[port] = *listener
				}
				return len(active) == len(want), nil
			})
		require.NoError(t, err, "Octavia load balancer %s did not converge to UDP ports %v using one pool", lbID, want)
		return active
	}
	baseline := checkOctavia(original.Port)[original.Port]
	require.NotEmpty(t, baseline.DefaultPoolID)

	updateListeners := func(next ...gatewayv1.Listener) {
		t.Helper()
		require.NoError(t, s.Client.Get(t.Context(), gatewayKey, &gateway))
		before := gateway.DeepCopy()
		gateway.Spec.Listeners = next
		require.NoError(t, s.Client.Patch(t.Context(), &gateway, client.MergeFrom(before)))
	}
	updateListeners(original, sibling)
	require.Equal(t, vip, checkGateway(gateway.Generation, original.Name, sibling.Name))
	added := checkOctavia(original.Port, sibling.Port)
	require.Equal(t, baseline.ID, added[original.Port].ID)
	require.Equal(t, baseline.DefaultPoolID, added[sibling.Port].DefaultPoolID)
	probe(vip, original.Port)
	probe(vip, sibling.Port)

	updateListeners(sibling)
	// Probe from a new socket at every readback of the retiring listener.
	// An individual error fails the test, including during cloud convergence.
	siblingAddress := net.JoinHostPort(vip, strconv.Itoa(int(sibling.Port)))
	samples := 0
	require.NoError(t, wait.PollUntilContextTimeout(t.Context(), 250*time.Millisecond,
		s.TimeoutConfig.MaxTimeToConsistency, true, func(ctx context.Context) (bool, error) {
			samples++
			got, err := probeKindUDPEcho(siblingAddress, fmt.Sprintf("hibiscus-sibling-removal-%d", samples))
			if err != nil {
				return false, err
			}
			if got.Service != backend.Service || got.Namespace != ns {
				return false, fmt.Errorf("unexpected sibling backend %s/%s", got.Namespace, got.Service)
			}
			_, err = listeners.Get(ctx, octavia, baseline.ID).Extract()
			if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
				return true, nil
			}
			return false, err
		}), "the sibling must keep answering while Octavia retires the original port")
	t.Logf("The sibling answered all %d fresh UDP probes while waiting for removal of the original listener", samples)
	require.Equal(t, vip, checkGateway(gateway.Generation, sibling.Name))
	remaining := checkOctavia(sibling.Port)[sibling.Port]
	require.Equal(t, added[sibling.Port].ID, remaining.ID)
	require.Equal(t, baseline.DefaultPoolID, remaining.DefaultPoolID)
	removedAddress := net.JoinHostPort(vip, strconv.Itoa(int(original.Port)))
	require.NoError(t, wait.PollUntilContextTimeout(t.Context(), s.TimeoutConfig.DefaultPollInterval,
		s.TimeoutConfig.MaxTimeToConsistency, true, func(context.Context) (bool, error) {
			return udpPortWithdrawn(removedAddress)
		}), "removed listener %s still forwarded datagrams", removedAddress)
	for range 3 {
		probe(vip, sibling.Port)
		dropped, err := udpPortWithdrawn(removedAddress)
		require.NoError(t, err)
		require.True(t, dropped, "removed listener %s forwarded a datagram", removedAddress)
	}
}

func udpPortWithdrawn(address string) (bool, error) {
	_, err := probeKindUDPEcho(address, "hibiscus-withdrawn-listener")
	// A refused socket or a timeout means no backend replied to this probe.
	var timeout net.Error
	if errors.Is(err, syscall.ECONNREFUSED) || errors.As(err, &timeout) && timeout.Timeout() {
		return true, nil
	}
	return false, err
}
