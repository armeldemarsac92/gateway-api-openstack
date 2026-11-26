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

package l4route

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/stretchr/testify/require"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestTCPXBackendRejectsForeignNamespaceAndPortOverride(t *testing.T) {
	for _, test := range []struct {
		name      string
		namespace *gatewayv1.Namespace
		port      *gatewayv1.PortNumber
		reason    gatewayv1.RouteConditionReason
	}{
		{name: "foreign namespace", namespace: ptr.To(gatewayv1.Namespace("foreign")), reason: gatewayv1.RouteReasonRefNotPermitted},
		{name: "port override", port: ptr.To(gatewayv1.PortNumber(9000)), reason: gatewayv1.RouteReasonUnsupportedValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, class, route := routeFixture()
			ref := &route.Spec.Rules[0].BackendRefs[0]
			ref.Group, ref.Kind = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend"))
			ref.Namespace, ref.Port = test.namespace, test.port
			r := testReconcilerForChannel(t, "experimental", gateway, class, route)
			route = reconcileTCPRoute(t, r, route)
			checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, string(test.reason))
		})
	}
}

func TestTCPXBackendFencesPodMembershipChange(t *testing.T) {
	gateway, class, route := routeFixture()
	ref := &route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "backend", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector,
			Port: gatewayx.BackendPort{Number: 9000},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "selected"},
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: "selected", Labels: map[string]string{"app": "selected"}},
		Status: corev1.PodStatus{
			PodIP:      "10.1.2.3",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	r := testReconcilerForChannel(t, "experimental", gateway, class, route, backend, pod)
	r.Coordinator = &graph.Coordinator{}
	id := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	r.Writer = readyRouteWriter(t, r)
	route = reconcileTCPRoute(t, r, route)
	inputs, err := r.ObserveRouteGraph(t.Context(), viewRoute(route).graphRequest(id, graph.Ensure, gateway), gateway)
	require.NoError(t, err)
	require.Len(t, inputs.Nodes, 1)
	require.Equal(t, []cloud.Member{{Address: "10.1.2.3", Port: 9000}}, inputs.Nodes[0].Spec.Members)
	delete(pod.Labels, "app")
	require.NoError(t, r.Update(t.Context(), pod))
	require.ErrorIs(t, inputs.Validate(t.Context()), controller.ErrGraphInputChanged)
}

func TestTCPExternalHostnameRefreshesRoutedMembers(t *testing.T) {
	gateway, class, route := routeFixture()
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.30"}}
	ref := route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "backend", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeExternalHostname, Protocol: ptr.To(gatewayx.BackendProtocolTCP),
			Port: gatewayx.BackendPort{Number: 9000}, TLS: &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeNone},
			ExternalHostname: &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"},
		},
	}
	// A same-named Service must remain a distinct, local transport member.
	localRef, local, endpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, backend.Name, 3, "10.1.2.3")
	disabledRef := ref
	disabledRef.Name, disabledRef.Weight = "disabled", ptr.To(int32(0))
	disabled := backend.DeepCopy()
	disabled.Name, disabled.UID, disabled.Spec.ExternalHostname.Hostname = "disabled", "disabled", "disabled.example.test"
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{localRef, ref, disabledRef}
	r := testReconcilerForChannel(t, "experimental", gateway, class, route, backend, disabled, local, endpoints)
	r.Config.XBackendExternalHostnameClasses = []string{class.Name}
	r.Config.XBackendExternalHostnameCIDRs = []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}
	r.Config.XBackendServiceDNSDomains = []string{"cluster.example"}
	r.Config.OpenStackResyncInterval = 2 * time.Minute
	r.Config.TCPConnectionRejection = true
	r.TCPConnectionRejectionAvailable = func(context.Context) (bool, error) { return true, nil }
	r.Coordinator = &graph.Coordinator{}
	now := time.Unix(100, 0)
	r.externalHostnames.now = func() time.Time { return now }
	var lookups atomic.Int32
	var dnsAddresses []netip.Addr
	var dnsError error
	r.externalHostnames.lookup = func(_ context.Context, network, hostname string) ([]netip.Addr, error) {
		lookups.Add(1)
		if network != "ip4" || hostname != "origin.example.test." {
			t.Errorf("unexpected DNS question %s %s", network, hostname)
		}
		return dnsAddresses, dnsError
	}
	dnsAddresses = []netip.Addr{netip.MustParseAddr("::ffff:198.51.100.11"), netip.MustParseAddr("198.51.100.11"), netip.MustParseAddr("2001:db8::20")}
	id := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	r.Writer = readyRouteWriter(t, r)
	key := client.ObjectKeyFromObject(route)
	check := func(address string) graph.RouteInputs {
		t.Helper()
		result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		if result.Requeue {
			result, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
		}
		require.Positive(t, result.RequeueAfter)
		require.LessOrEqual(t, result.RequeueAfter, 36*time.Second)
		require.NoError(t, r.Get(t.Context(), key, route))
		checkRouteCondition(t, route, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
		checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
		checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
		ancestors, err := r.ObserveXBackendAncestors(t.Context(), backend)
		require.NoError(t, err)
		condition := ancestors[client.ObjectKeyFromObject(gateway)]
		require.Equal(t, metav1.ConditionTrue, condition.Status)
		require.Equal(t, backend.Generation, condition.ObservedGeneration)

		input, err := r.ObserveRouteGraph(t.Context(), viewRoute(route).graphRequest(id, graph.Ensure, gateway), gateway)
		require.NoError(t, err)
		require.Len(t, input.Nodes, 1)
		require.Equal(t, cloud.GraphEnsure, input.Nodes[0].Disposition)
		require.NoError(t, input.Validate(t.Context()))
		spec := input.Nodes[0].Spec
		localMember := cloud.Member{Address: "10.1.2.3", Port: 9090, Weight: 3}
		if address == "" {
			localMember.Weight = 1
			require.Equal(t, []cloud.Member{localMember}, spec.Members)
			require.Equal(t, &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}, spec.TCPConnectionRejection)
		} else {
			require.Equal(t, []cloud.Member{localMember, {Address: address, Port: 9000, Weight: 1, RoutedExternal: true}}, spec.Members)
			require.Nil(t, spec.TCPConnectionRejection)
		}
		return input
	}

	before := check("198.51.100.11")
	require.EqualValues(t, 1, lookups.Load())
	now = now.Add(time.Minute)
	require.NoError(t, before.Validate(t.Context()))
	require.EqualValues(t, 1, lookups.Load(), "validating a sealed graph does not refresh DNS")
	dnsAddresses = []netip.Addr{netip.MustParseAddr("203.0.113.20")}
	check("")
	require.EqualValues(t, 2, lookups.Load())

	now = now.Add(time.Minute)
	dnsAddresses, dnsError = nil, &net.DNSError{IsNotFound: true}
	check("")
	require.EqualValues(t, 3, lookups.Load())

	now = now.Add(time.Minute)
	dnsAddresses, dnsError = []netip.Addr{netip.MustParseAddr("198.51.100.12")}, nil
	before = check("198.51.100.12")
	require.EqualValues(t, 4, lookups.Load())

	backend.Spec.ExternalHostname.Hostname = "database.svc.cluster.example"
	backend.Generation++
	require.NoError(t, r.Update(t.Context(), backend))
	require.ErrorIs(t, before.Validate(t.Context()), controller.ErrGraphInputChanged)
	route = reconcileTCPRoute(t, r, route)
	route = reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "UnsupportedValue")
	ancestors, err := r.ObserveXBackendAncestors(t.Context(), backend)
	require.NoError(t, err)
	condition := ancestors[client.ObjectKeyFromObject(gateway)]
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "UnsupportedValue", condition.Reason)
	require.Equal(t, backend.Generation, condition.ObservedGeneration)
	require.EqualValues(t, 4, lookups.Load(), "refused hostnames never reach the resolver")
}

func tcpExternalHostnameAttachmentFixture(t *testing.T) (*Reconciler, *gatewayv1.Gateway, *gatewayv1.TCPRoute, *gatewayx.XBackend, *corev1.Namespace) {
	t.Helper()
	gateway, class, route := routeFixture()
	gateway.Status.Conditions = []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionTrue, ObservedGeneration: gateway.Generation}}
	gateway.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{
		Namespaces: &gatewayv1.RouteNamespaces{From: ptr.To(gatewayv1.NamespacesFromSelector),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"forward": "yes"}}},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: route.Namespace, UID: "route-namespace", Labels: map[string]string{"forward": "yes"},
	}}
	ref := &route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "external-backend", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeExternalHostname, Port: gatewayx.BackendPort{Number: 9000},
			ExternalHostname: &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"},
		},
	}
	r := testReconcilerForChannel(t, "experimental", gateway, class, route, backend, namespace)
	r.Config.XBackendExternalHostnameClasses = []string{class.Name}
	r.Config.XBackendExternalHostnameCIDRs = []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}
	r.Coordinator = &graph.Coordinator{}
	return r, gateway, route, backend, namespace
}

func TestTCPExternalHostnameWaitsForMemberTopology(t *testing.T) {
	r, gateway, route, backend, _ := tcpExternalHostnameAttachmentFixture(t)
	r.externalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		t.Error("DNS lookup before the Gateway member mode was bound")
		return nil, nil
	}
	route = reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionUnknown, "Pending")
	ancestors, err := r.ObserveXBackendAncestors(t.Context(), backend)
	require.NoError(t, err)
	condition := ancestors[client.ObjectKeyFromObject(gateway)]
	require.Equal(t, metav1.ConditionUnknown, condition.Status)
	require.Equal(t, "Pending", condition.Reason)
}

func TestTCPExternalHostnameRefreshesAfterNamespaceReadmission(t *testing.T) {
	r, gateway, route, _, namespace := tcpExternalHostnameAttachmentFixture(t)
	ctx := t.Context()
	r.externalHostnames.now = func() time.Time { return time.Unix(100, 0) }
	answer := netip.MustParseAddr("198.51.100.11")
	var lookups int
	r.externalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		lookups++
		return []netip.Addr{answer}, nil
	}
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	ready := readyRouteWriter(t, r)
	var detaches int
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		if request.Action == graph.Ensure {
			return ready(ctx, request)
		}
		require.Equal(t, graph.DeleteRoute, request.Action)
		detaches++
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{
			{Identity: request.Identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}},
		}}, nil
	})
	route = reconcileTCPRoute(t, r, route)
	previous, err := r.ObserveRouteGraph(ctx, viewRoute(route).graphRequest(identity, graph.Ensure, gateway), gateway)
	require.NoError(t, err)
	require.Len(t, previous.Nodes, 1)
	require.Equal(t, []cloud.Member{{Address: answer.String(), Port: 9000, RoutedExternal: true}}, previous.Nodes[0].Spec.Members)
	require.Equal(t, 1, lookups)

	admitNamespace := func(forward string) {
		t.Helper()
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(namespace), namespace))
		namespace.Labels["forward"] = forward
		require.NoError(t, r.Update(ctx, namespace))
	}
	admitNamespace("no")
	require.ErrorIs(t, previous.Validate(ctx), controller.ErrGraphInputChanged)
	for range 3 {
		route = reconcileTCPRoute(t, r, route)
	}
	require.Equal(t, 1, detaches)
	require.Equal(t, 1, lookups)

	answer = netip.MustParseAddr("198.51.100.12")
	admitNamespace("yes")
	for range 3 {
		route = reconcileTCPRoute(t, r, route)
	}
	current, err := r.ObserveRouteGraph(ctx, viewRoute(route).graphRequest(identity, graph.Ensure, gateway), gateway)
	require.NoError(t, err)
	require.Len(t, current.Nodes, 1)
	require.Equal(t, []cloud.Member{{Address: answer.String(), Port: 9000, RoutedExternal: true}}, current.Nodes[0].Spec.Members)
	require.Equal(t, 2, lookups)

	// The Gateway writer can withdraw the listener before the route's
	// detach reconcile runs. Readmission still requires a fresh answer.
	admitNamespace("no")
	withdrawn, err := r.ObserveRouteGraph(ctx, graph.Request{
		Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway,
	}, gateway)
	require.NoError(t, err)
	require.Len(t, withdrawn.Nodes, 1)
	require.Equal(t, cloud.GraphPreserve, withdrawn.Nodes[0].Disposition)
	require.Equal(t, 2, lookups)
	answer = netip.MustParseAddr("198.51.100.13")
	admitNamespace("yes")
	for range 2 {
		route = reconcileTCPRoute(t, r, route)
	}
	current, err = r.ObserveRouteGraph(ctx, viewRoute(route).graphRequest(identity, graph.Ensure, gateway), gateway)
	require.NoError(t, err)
	require.Equal(t, []cloud.Member{{Address: answer.String(), Port: 9000, RoutedExternal: true}}, current.Nodes[0].Spec.Members)
	require.Equal(t, 3, lookups)
}

func TestTCPExternalHostnameSharesConcurrentRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, gateway, route, _, _ := tcpExternalHostnameAttachmentFixture(t)
		original := netip.MustParseAddr("198.51.100.11")
		r.externalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) { return []netip.Addr{original}, nil }
		identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
		synctest.Wait()
		// Advance the lookup clock beyond the cached window without changing its inputs.
		now := r.externalHostnames.time().Add(time.Minute)
		r.externalHostnames.now = func() time.Time { return now }

		refreshed := netip.MustParseAddr("198.51.100.12")
		started, release := make(chan struct{}), make(chan struct{})
		var lookups atomic.Int32
		r.externalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
			if lookups.Add(1) == 1 {
				close(started)
				<-release
				return []netip.Addr{refreshed}, nil
			}
			return []netip.Addr{original}, nil
		}

		planned, observed := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := r.planRoute(t.Context(), viewRoute(route), identity, true)
			planned <- err
		}()
		<-started
		go func() {
			_, err := r.ObserveRouteGraph(t.Context(), graph.Request{
				Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway,
			}, gateway)
			observed <- err
		}()
		synctest.Wait()
		close(release)
		require.NoError(t, <-planned)
		require.NoError(t, <-observed)
		require.EqualValues(t, 1, lookups.Load())
		plan, err := r.planRoute(t.Context(), viewRoute(route), identity, true)
		require.NoError(t, err)
		require.NotNil(t, plan.desired)
		require.Equal(t, []cloud.Member{{Address: refreshed.String(), Port: 9000, RoutedExternal: true}}, plan.desired.Members)
	})
}
