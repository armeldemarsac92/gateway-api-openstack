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
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestUDPPacketDropFencesGrantWithdrawalAndRestoresTraffic(t *testing.T) {
	for _, mode := range []cloud.MemberMode{cloud.MemberModeEndpoint, cloud.MemberModeNodePort} {
		t.Run(string(mode), func(t *testing.T) { testUDPPacketDropGrantLifecycle(t, mode) })
	}
}

func testUDPPacketDropGrantLifecycle(t *testing.T, mode cloud.MemberMode) {
	t.Helper()
	gateway, class, route := udpRouteFixture()
	localRef, local, localEndpoints := udpBackendFixture(route.Namespace, "primary", 3, "10.1.2.3")
	remoteRef, remote, remoteEndpoints := udpBackendFixture("remote", "secondary", 1, "10.1.2.4")
	remoteRef.Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	originalRefs := []gatewayv1.BackendRef{localRef, remoteRef}
	route.Spec.Rules[0].BackendRefs = slices.Clone(originalRefs)
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow-udp", UID: "grant"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "UDPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: &remoteRef.Name}},
		},
	}
	objects := []client.Object{gateway, class, route, local, localEndpoints, remote, remoteEndpoints, grant}
	members := []cloud.Member{{Address: "10.1.2.3", Port: 9090, Weight: 3}, {Address: "10.1.2.4", Port: 9090, Weight: 1}}
	var worker *corev1.Node
	if mode == cloud.MemberModeNodePort {
		worker = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Spec: corev1.NodeSpec{ProviderID: "openstack:///worker"},
			Status: corev1.NodeStatus{
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			},
		}
		objects = append(objects, worker)
		for i, service := range []*corev1.Service{local, remote} {
			service.Spec.Type = corev1.ServiceTypeNodePort
			service.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
			service.Spec.Ports[0].NodePort = int32(32000 + i)
		}
		localEndpoints.Endpoints[0].NodeName, remoteEndpoints.Endpoints[0].NodeName = &worker.Name, &worker.Name
		members = []cloud.Member{
			{Address: "192.0.2.44", Port: 32000, Weight: 3, ProviderID: worker.Spec.ProviderID},
			{Address: "192.0.2.44", Port: 32001, Weight: 1, ProviderID: worker.Spec.ProviderID},
		}
	}
	r := testReconciler(t, objects...)
	r.Protocol = gatewayv1.UDPProtocolType
	r.Coordinator = &graph.Coordinator{}
	r.Config.UDPPacketDrop = mode == cloud.MemberModeEndpoint
	r.Config.UDPNodePortPacketDrop = mode == cloud.MemberModeNodePort
	r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
	id := bindFixture(t, r, gateway, route, mode)
	base := route.DeepCopy()
	controller.SealRouteAnchor(r.Config, route, id, 8080)
	if err := r.Patch(t.Context(), route, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	controller.SetGatewayListenerScope(r.Config, gateway, gatewayv1.UDPProtocolType)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "second", Protocol: gatewayv1.UDPProtocolType, Port: 9311})
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	r.Writer = readyRouteWriter(t, r)
	reconcile := func() graph.RouteInputs {
		t.Helper()
		route = reconcileUDPRoute(t, r, route)
		route = reconcileUDPRoute(t, r, route)
		input, err := r.ObserveRouteGraph(t.Context(), viewRoute(route).graphRequest(id, graph.Ensure, gateway), gateway)
		if err != nil || len(input.Nodes) != 1 {
			t.Fatalf("observe UDPRoute = %+v, %v", input.Nodes, err)
		}
		return input
	}
	check := func(input graph.RouteInputs, drop cloud.UDPPacketDrop, want []cloud.Member, status metav1.ConditionStatus, reason string) {
		t.Helper()
		spec := input.Nodes[0].Spec
		if spec.UDPPacketDrop == nil || *spec.UDPPacketDrop != drop || !slices.Equal(spec.Members, want) ||
			!slices.Equal(spec.ScopedListenerPorts, []int{8080, 9311}) || spec.Identity != id ||
			spec.HealthMonitor != (cloud.HealthMonitorConfig{}) || string(route.UID) != id.RouteUID || !reflect.DeepEqual(route.Spec.Rules[0].BackendRefs, originalRefs) {
			t.Fatalf("unexpected UDP forwarding target: %+v, want %+v with %v", spec, drop, want)
		}
		checkRouteCondition(t, route, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
		checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", status, reason)
		checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
	}
	original := reconcile()
	check(original, cloud.UDPPacketDrop{Denominator: 1}, members, metav1.ConditionTrue, "ResolvedRefs")

	if worker != nil {
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(worker), worker); err != nil {
			t.Fatal(err)
		}
		worker.Status.Addresses = append([]corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: "127.0.0.1"},
			{Type: corev1.NodeInternalIP, Address: "::ffff:192.0.2.44"},
		}, worker.Status.Addresses...)
		if err := r.Status().Update(t.Context(), worker); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	r.APIReader = denyServiceRead{Reader: r.Client, namespace: remote.Namespace}
	if err := original.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("a grant withdrawal did not fence the previous UDP graph: %v", err)
	}
	partial := slices.Clone(members[:1])
	partial[0].Weight = 1
	beforeRouteCheckpoint := func(share cloud.UDPPacketDrop, wanted []cloud.Member) graph.RouteInputs {
		t.Helper()
		request := graph.Request{Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway}
		input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
		if err != nil || len(input.Nodes) != 1 {
			t.Fatalf("observe UDPRoute from Gateway = %+v, %v", input.Nodes, err)
		}
		node := input.Nodes[0]
		if node.Disposition != cloud.GraphPreserve || !node.RestrictUDPPacketDrop || !slices.Equal(node.RetainListenerPorts, []int{8080, 9311}) ||
			node.Spec.UDPPacketDrop == nil || *node.Spec.UDPPacketDrop != share || !slices.Equal(node.Spec.Members, wanted) {
			t.Fatalf("Gateway observation did not expose the fresh authorized target without checkpointing it: %+v", node)
		}
		return input
	}
	withdrawal := beforeRouteCheckpoint(cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, partial)
	revoked := reconcile()
	check(revoked, cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, partial, metav1.ConditionFalse, "RefNotPermitted")
	r.Config.UDPPacketDrop, r.Config.UDPNodePortPacketDrop = mode == cloud.MemberModeNodePort, mode == cloud.MemberModeEndpoint
	if plan, err := r.planRoute(t.Context(), viewRoute(route), id, true); err != nil || plan.desired != nil || !plan.detach {
		t.Fatalf("the other member mode's option programmed %s discard: %+v, %v", mode, plan, err)
	}
	r.Config.UDPPacketDrop, r.Config.UDPNodePortPacketDrop = mode == cloud.MemberModeEndpoint, mode == cloud.MemberModeNodePort

	grant.ResourceVersion = ""
	if err := r.Create(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	r.APIReader = r.Client
	if err := revoked.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("a restored grant did not fence the old drop fraction: %v", err)
	}
	if err := withdrawal.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("the Gateway's pre-checkpoint withdrawal was not fenced: %v", err)
	}
	beforeRouteCheckpoint(cloud.UDPPacketDrop{Denominator: 1}, members)
	check(reconcile(), cloud.UDPPacketDrop{Denominator: 1}, members, metav1.ConditionTrue, "ResolvedRefs")

	if worker != nil {
		forgetEndpointNode(t, r, remoteEndpoints)
	} else if err := r.Delete(t.Context(), remoteEndpoints); err != nil {
		t.Fatal(err)
	}
	check(reconcile(), cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, partial, metav1.ConditionTrue, "ResolvedRefs")
	r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return false, errors.New("discovery unavailable") }
	plan, err := r.planRoute(t.Context(), viewRoute(route), id, true)
	if err != nil || plan.desired == nil || plan.detach || plan.desired.UDPPacketDrop == nil ||
		*plan.desired.UDPPacketDrop != (cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}) || !slices.Equal(plan.desired.Members, partial) {
		t.Fatalf("a transient discovery error discarded the current authorized intent: %+v, %v", plan, err)
	}
	r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return false, nil }
	plan, err = r.planRoute(t.Context(), viewRoute(route), id, true)
	if err != nil || plan.desired != nil || !plan.detach {
		t.Fatalf("unavailable drop capability did not fail closed: %+v, %v", plan, err)
	}
	if worker != nil {
		r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
		forgetEndpointNode(t, r, localEndpoints)
		check(reconcile(), cloud.UDPPacketDrop{Numerator: 1, Denominator: 1}, nil, metav1.ConditionTrue, "ResolvedRefs")
	}
}

func forgetEndpointNode(t *testing.T, r *Reconciler, endpoints *discoveryv1.EndpointSlice) {
	t.Helper()
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(endpoints), endpoints); err != nil {
		t.Fatal(err)
	}
	endpoints.Endpoints[0].NodeName = nil
	if err := r.Update(t.Context(), endpoints); err != nil {
		t.Fatal(err)
	}
}

func TestUDPPacketDropKeepsInvalidZeroWeightOutOfTheFraction(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	local, service, endpoints := udpBackendFixture(route.Namespace, "primary", 3, "10.1.2.3")
	missing, _, _ := udpBackendFixture(route.Namespace, "missing", 0)
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{local, missing}
	r := testReconciler(t, gateway, class, route, service, endpoints)
	r.Protocol, r.Config.UDPPacketDrop = gatewayv1.UDPProtocolType, true
	r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
	id := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	plan, err := r.planRoute(t.Context(), viewRoute(route), id, true)
	if err != nil || plan.desired == nil || plan.detach || plan.resolved.Reason != "BackendNotFound" ||
		plan.desired.UDPPacketDrop == nil || *plan.desired.UDPPacketDrop != (cloud.UDPPacketDrop{Denominator: 1}) {
		t.Fatalf("zero-weight reference altered the UDP forwarding share: %+v, %v", plan, err)
	}
}

func TestUDPNodePortSelectsVIPFamilyOnDualStackWorker(t *testing.T) {
	gateway, _, route := udpRouteFixture()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: route.Namespace, UID: "udp-service"}, Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeNodePort, IPFamilies: []corev1.IPFamily{corev1.IPv6Protocol, corev1.IPv4Protocol},
		Ports: []corev1.ServicePort{{Name: "udp", Protocol: corev1.ProtocolUDP, Port: 8080, NodePort: 32000}},
	}}
	worker := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{
		Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "2001:db8::44"}, {Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}}
	objects := []client.Object{worker}
	for family, address := range map[discoveryv1.AddressType]string{discoveryv1.AddressTypeIPv4: "10.1.2.3", discoveryv1.AddressTypeIPv6: "2001:db8:1::3"} {
		objects = append(objects, &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(string(family)), Namespace: service.Namespace, Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
			AddressType: family, Ports: []discoveryv1.EndpointPort{{Name: ptr.To("udp"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(8080))}},
			Endpoints: []discoveryv1.Endpoint{{NodeName: &worker.Name, Addresses: []string{address}}},
		})
	}
	r := testReconciler(t, objects...)
	r.Protocol = gatewayv1.UDPProtocolType
	placement := classconfig.Resolved{Topology: cloud.Topology{MemberMode: cloud.MemberModeNodePort}, NodeAddressType: corev1.NodeInternalIP}
	backends := []transportBackend{{ref: backendReference{port: ptr.To(gatewayv1.PortNumber(8080)), weight: 1}, service: service, port: &service.Spec.Ports[0]}}
	for _, tc := range []struct{ vip, workerIP string }{{"192.0.2.30", "192.0.2.44"}, {"2001:db8::30", "2001:db8::44"}} {
		gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: tc.vip}}
		spec, _, diagnostic, err := r.routeSpec(t.Context(), viewRoute(route), gateway, &gateway.Spec.Listeners[0], backends, placement)
		if err != nil || diagnostic != nil || len(spec.Members) != 1 || spec.Members[0].Address != tc.workerIP || spec.Members[0].Port != 32000 {
			t.Errorf("VIP %s requires UDP NodePort on %s: members %v, diagnostic %v, error %v", tc.vip, tc.workerIP, spec.Members, diagnostic, err)
		}
	}
	if err := r.Delete(t.Context(), &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "ipv4", Namespace: service.Namespace}}); err != nil {
		t.Fatal(err)
	}
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.30"}}
	spec, _, diagnostic, err := r.routeSpec(t.Context(), viewRoute(route), gateway, &gateway.Spec.Listeners[0], backends, placement)
	if err != nil || diagnostic == nil || diagnostic.Reason != "Pending" || len(spec.Members) != 0 {
		t.Fatalf("IPv4 VIP with only IPv6 endpoints: members %v, diagnostic %v, error %v", spec.Members, diagnostic, err)
	}
}

func udpRouteFixture() (*gatewayv1.Gateway, *gatewayv1.GatewayClass, *gatewayv1.UDPRoute) {
	gateway, class, tcp := routeFixture()
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.30"}}
	gateway.Spec.Listeners[0].Name, gateway.Spec.Listeners[0].Protocol = "udp", gatewayv1.UDPProtocolType
	route := &gatewayv1.UDPRoute{
		ObjectMeta: tcp.ObjectMeta,
		Spec: gatewayv1.UDPRouteSpec{CommonRouteSpec: tcp.Spec.CommonRouteSpec,
			Rules: []gatewayv1.UDPRouteRule{{BackendRefs: tcp.Spec.Rules[0].BackendRefs}},
		},
	}
	return gateway, class, route
}

func TestUDPRouteProgramsWeightedLocalNodePortPool(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	gateway.Spec.Infrastructure = &gatewayv1.GatewayInfrastructure{Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"team": "edge"}}
	workerA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: corev1.NodeSpec{ProviderID: "openstack:///worker-a"},
		Status: corev1.NodeStatus{
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	workerB := workerA.DeepCopy()
	workerB.Name, workerB.Spec.ProviderID, workerB.Status.Addresses[0].Address = "worker-b", "openstack:///worker-b", "192.0.2.45"
	primaryRef, primary, primaryEndpoints := udpBackendFixture(route.Namespace, "primary", 3, "10.1.2.1", "10.1.2.2")
	secondaryRef, secondary, secondaryEndpoints := udpBackendFixture(route.Namespace, "secondary", 1, "10.1.2.3")
	disabledRef, disabled, disabledEndpoints := udpBackendFixture(route.Namespace, "disabled", 0, "10.1.2.4")
	for i, service := range []*corev1.Service{primary, secondary, disabled} {
		service.Spec.Type = corev1.ServiceTypeNodePort
		service.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
		service.Spec.Ports[0].NodePort = 32000 + int32(i)
	}
	primaryEndpoints.Endpoints[0].NodeName = &workerA.Name
	primaryEndpoints.Endpoints[1].NodeName = &workerB.Name
	secondaryEndpoints.Endpoints[0].NodeName = &workerA.Name
	disabledEndpoints.Endpoints[0].NodeName = &workerB.Name
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{primaryRef, secondaryRef, disabledRef}
	r := testReconciler(t, gateway, class, route, workerA, workerB, primary, primaryEndpoints, secondary, secondaryEndpoints, disabled, disabledEndpoints)
	r.Config.GatewayInfrastructureMetadata = true
	r.Protocol = gatewayv1.UDPProtocolType
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeNodePort)
	want := []cloud.Member{
		{Address: "192.0.2.44", Port: 32000, ProviderID: "openstack:///worker-a", Weight: 3},
		{Address: "192.0.2.44", Port: 32001, ProviderID: "openstack:///worker-a", Weight: 2},
		{Address: "192.0.2.45", Port: 32000, ProviderID: "openstack:///worker-b", Weight: 3},
	}
	route = reconcileUDPRouteWithGraph(t, r, route, identity, func(nodes []cloud.RouteGraphNode) {
		spec := nodes[0].Spec
		if spec.MemberMode != cloud.MemberModeNodePort || !slices.Equal(spec.Members, want) {
			t.Fatalf("expected 3:1 total NodePort shares across eligible workers, got %+v", spec.Members)
		}
	})
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")

	disabled.Spec.Type, disabled.Spec.Ports[0].NodePort = corev1.ServiceTypeClusterIP, 0
	if err := r.Update(t.Context(), disabled); err != nil {
		t.Fatal(err)
	}
	route = reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "UnsupportedValue")
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")

	request := graph.Request{Identity: identity, Action: graph.Ensure, Gateway: gateway, UDPRoute: route}
	input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(t.Context(), secondaryEndpoints); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("losing the last eligible worker must invalidate the sealed graph, got %v", err)
	}
	route = reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")
	if route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatal("losing a positive share did not checkpoint withdrawal")
	}
}

func TestUDPRouteProgramsWeightedEndpointPool(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "allow-disabled"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "UDPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("disabled"))}},
		},
	}
	objects := []client.Object{gateway, class, route, grant}
	var refs []gatewayv1.BackendRef
	for _, backend := range []struct {
		name      string
		weight    int32
		addresses []string
	}{
		{"primary", 70, []string{"10.1.2.1", "10.1.2.2"}},
		{"secondary", 30, []string{"10.1.2.3"}},
		{"disabled", 0, []string{"10.1.2.4"}},
	} {
		namespace := route.Namespace
		if backend.weight == 0 {
			namespace = grant.Namespace
		}
		ref, service, endpoints := udpBackendFixture(namespace, backend.name, backend.weight, backend.addresses...)
		if namespace != route.Namespace {
			ref.Namespace = ptr.To(gatewayv1.Namespace(namespace))
		}
		refs = append(refs, ref)
		objects = append(objects, service, endpoints)
	}
	route.Spec.Rules[0].BackendRefs = refs
	r := testReconciler(t, objects...)
	r.Protocol = gatewayv1.UDPProtocolType
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	route = reconcileUDPRouteWithGraph(t, r, route, identity, func(nodes []cloud.RouteGraphNode) {
		members := nodes[0].Spec.Members
		if len(members) != 3 || members[0].Address != "10.1.2.1" || members[1].Address != "10.1.2.2" || members[2].Address != "10.1.2.3" ||
			members[0].Weight <= 0 || members[0].Weight != members[1].Weight || members[0].Weight*6 != members[2].Weight*7 ||
			members[0].Port != 9090 || members[1].Port != 9090 || members[2].Port != 9090 {
			t.Fatalf("expected endpoint shares 35:35:30 and no zero-weight backend, got %+v", members)
		}
	})
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
	gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Type: ptr.To(gatewayv1.HostnameAddressType), Value: "dns.example.net"}}
	gateway.Generation++
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	route = reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, "Accepted")
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
	disabled := &corev1.Service{}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: grant.Namespace, Name: "disabled"}, disabled); err != nil {
		t.Fatal(err)
	}
	disabled.Spec.Type = corev1.ServiceTypeExternalName
	disabled.Spec.ExternalName = "disabled.example.com"
	if err := r.Update(t.Context(), disabled); err != nil {
		t.Fatal(err)
	}
	current := reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "UnsupportedValue")
	checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
	if err := r.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	r.APIReader = denyServiceRead{Reader: r.Client, namespace: grant.Namespace}
	current = reconcileUDPRoute(t, r, current)
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
}

func TestUDPRouteProgramsApproximateWeights(t *testing.T) {
	for _, mode := range []cloud.MemberMode{cloud.MemberModeEndpoint, cloud.MemberModeNodePort} {
		t.Run(string(mode), func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			firstRef, first, firstEndpoints := udpBackendFixture(route.Namespace, "primary", 257, "10.1.2.1")
			secondRef, second, secondEndpoints := udpBackendFixture(route.Namespace, "secondary", 1, "10.1.2.2")
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{firstRef, secondRef}
			objects := []client.Object{gateway, class, route, first, firstEndpoints, second, secondEndpoints}
			want := []cloud.Member{
				{Address: "10.1.2.1", Port: 9090, Weight: 256},
				{Address: "10.1.2.2", Port: 9090, Weight: 1},
			}
			if mode == cloud.MemberModeNodePort {
				worker := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "worker"},
					Status: corev1.NodeStatus{
						Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
						Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
					},
				}
				objects = append(objects, worker)
				for i, service := range []*corev1.Service{first, second} {
					service.Spec.Type = corev1.ServiceTypeNodePort
					service.Spec.Ports[0].NodePort = 32000 + int32(i)
					want[i].Address, want[i].Port = worker.Status.Addresses[0].Address, int(service.Spec.Ports[0].NodePort)
				}
			}
			r := testReconciler(t, objects...)
			r.Protocol = gatewayv1.UDPProtocolType
			r.Config.UDPPacketDrop = mode == cloud.MemberModeEndpoint
			r.Config.UDPNodePortPacketDrop = mode == cloud.MemberModeNodePort
			r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
			r.UDPExtendedWeightsAvailable = func(context.Context) (bool, error) {
				t.Fatal("a native allocation must keep its weights without discovering the extension")
				return false, nil
			}
			identity := bindFixture(t, r, gateway, route, mode)
			current := reconcileUDPRouteWithGraph(t, r, route, identity, func(nodes []cloud.RouteGraphNode) {
				if got := nodes[0].Spec.Members; !slices.Equal(got, want) {
					t.Fatalf("257:1 shares produced %+v, want %+v", got, want)
				}
			})
			checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
		})
	}
}

func TestUDPRouteExtendedWeightsRespectReferenceGrant(t *testing.T) {
	for _, mode := range []cloud.MemberMode{cloud.MemberModeEndpoint, cloud.MemberModeNodePort} {
		t.Run(string(mode), func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			primaryRef, primary, primaryEndpoints := udpBackendFixture(route.Namespace, "primary", 1000, "10.1.2.1")
			secondRef, second, secondEndpoints := udpBackendFixture(route.Namespace, "secondary", 1, "10.1.2.2")
			remoteRef, remote, remoteEndpoints := udpBackendFixture("remote", "remote", 1, "10.1.2.3")
			remoteRef.Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{primaryRef, secondRef, remoteRef}
			grant := &gatewayv1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow-udp"},
				Spec: gatewayv1.ReferenceGrantSpec{
					From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "UDPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
					To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: &remoteRef.Name}},
				},
			}
			objects := []client.Object{gateway, class, route, primary, primaryEndpoints, second, secondEndpoints, remote, remoteEndpoints, grant}
			members := []cloud.Member{
				{Address: "10.1.2.1", Port: 9090, Weight: 1000}, {Address: "10.1.2.2", Port: 9090, Weight: 1}, {Address: "10.1.2.3", Port: 9090, Weight: 1},
			}
			if mode == cloud.MemberModeNodePort {
				worker := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "worker"},
					Status: corev1.NodeStatus{
						Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
						Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
					},
				}
				objects = append(objects, worker)
				for i, service := range []*corev1.Service{primary, second, remote} {
					service.Spec.Type = corev1.ServiceTypeNodePort
					service.Spec.Ports[0].NodePort = 32000 + int32(i)
					members[i].Address, members[i].Port = worker.Status.Addresses[0].Address, int(service.Spec.Ports[0].NodePort)
				}
			}
			r := testReconciler(t, objects...)
			r.Protocol = gatewayv1.UDPProtocolType
			r.Config.UDPPacketDrop = mode == cloud.MemberModeEndpoint
			r.Config.UDPNodePortPacketDrop = mode == cloud.MemberModeNodePort
			r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
			extendedAvailable := true
			r.UDPExtendedWeightsAvailable = func(context.Context) (bool, error) { return extendedAvailable, nil }
			identity := bindFixture(t, r, gateway, route, mode)
			route = reconcileUDPRouteWithGraph(t, r, route, identity, func(nodes []cloud.RouteGraphNode) {
				spec := nodes[0].Spec
				if spec.UDPPacketDrop == nil || *spec.UDPPacketDrop != (cloud.UDPPacketDrop{Denominator: 1}) || !slices.Equal(spec.Members, members) {
					t.Fatalf("authorized 1000:1:1 target = %+v, want %v", spec, members)
				}
			})
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
			request := viewRoute(route).graphRequest(identity, graph.Ensure, gateway)
			input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
			if err != nil {
				t.Fatal(err)
			}
			extendedAvailable = false
			plan, err := r.planRoute(t.Context(), viewRoute(route), identity, true)
			if err != nil || plan.desired != nil || !plan.detach || plan.programmed.Reason != "UnsupportedValue" {
				t.Fatalf("the wide ratio survived explicit capability loss: %+v, %v", plan, err)
			}
			if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
				t.Fatalf("capability loss did not fence the sealed target: %v", err)
			}
			extendedAvailable = true
			if err := r.Delete(t.Context(), grant); err != nil {
				t.Fatal(err)
			}
			if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
				t.Fatalf("grant withdrawal did not fence the sealed target: %v", err)
			}
			plan, err = r.planRoute(t.Context(), viewRoute(route), identity, true)
			want := slices.Clone(members[:2])
			want[0].Weight = 256
			if err != nil || plan.desired == nil || plan.desired.UDPPacketDrop == nil ||
				*plan.desired.UDPPacketDrop != (cloud.UDPPacketDrop{Numerator: 1, Denominator: 1002}) || !slices.Equal(plan.desired.Members, want) {
				t.Fatalf("revoked grant did not reserve the removed share with native weights: %+v, %v", plan, err)
			}
		})
	}
}

func TestWeightedUDPRouteOutsideWeightToleranceWithdrawsPool(t *testing.T) {
	for _, test := range []struct {
		name             string
		deniedWeight     *int32
		resolved         metav1.ConditionStatus
		refReason        string
		programmedReason string
	}{
		{name: "resolved references", resolved: metav1.ConditionTrue, refReason: "ResolvedRefs", programmedReason: "UnsupportedValue"},
		{name: "denied zero-weight reference", deniedWeight: ptr.To(int32(0)), resolved: metav1.ConditionFalse, refReason: "RefNotPermitted", programmedReason: "UnsupportedValue"},
		{name: "denied positive-weight reference with discard", deniedWeight: ptr.To(int32(1)), resolved: metav1.ConditionFalse, refReason: "RefNotPermitted", programmedReason: "RefNotPermitted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			firstRef, first, firstEndpoints := udpBackendFixture(route.Namespace, "primary", 70, "10.1.2.1", "10.1.2.2")
			secondRef, second, secondEndpoints := udpBackendFixture(route.Namespace, "secondary", 30, "10.1.2.3", "10.1.2.4", "10.1.2.5", "10.1.2.6")
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{firstRef, secondRef}
			r := testReconciler(t, gateway, class, route, first, firstEndpoints, second, secondEndpoints)
			r.Protocol = gatewayv1.UDPProtocolType
			if test.deniedWeight != nil && *test.deniedWeight > 0 {
				r.Config.UDPPacketDrop = true
				r.UDPPacketDropAvailable = func(context.Context) (bool, error) { return true, nil }
				r.UDPExtendedWeightsAvailable = func(context.Context) (bool, error) { return false, nil }
			}
			identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
			route = reconcileUDPRouteWithGraph(t, r, route, identity, nil)
			// Each member is within 1/256 of its ideal share. Together the
			// secondary members would get 4/516 of the weight instead of
			// 1/1001, exceeding the tolerance for their group.
			route.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(1000))
			route.Spec.Rules[0].BackendRefs[1].Weight = ptr.To(int32(1))
			if test.deniedWeight != nil {
				route.Spec.Rules[0].BackendRefs = append(route.Spec.Rules[0].BackendRefs, gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{Name: "disabled", Namespace: ptr.To(gatewayv1.Namespace("other")), Port: firstRef.Port},
					Weight:                 test.deniedWeight,
				})
			}
			route.Generation++
			if err := r.Update(t.Context(), route); err != nil {
				t.Fatal(err)
			}
			current := reconcileUDPRoute(t, r, route)
			checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", test.resolved, test.refReason)
			checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, test.programmedReason)
			if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
				t.Fatal("a ratio outside the UDP weight tolerance did not checkpoint pool withdrawal")
			}
		})
	}
}

func TestWeightedUDPRouteWithdrawsWhenReferenceGrantIsRevoked(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	localRef, local, localEndpoints := udpBackendFixture(route.Namespace, "primary", 70, "10.1.2.1")
	remoteRef, remote, remoteEndpoints := udpBackendFixture("other", "secondary", 30, "10.1.2.2")
	remoteRef.Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{localRef, remoteRef}
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow-udp"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "UDPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: &remoteRef.Name}},
		},
	}
	r := testReconciler(t, gateway, class, route, local, localEndpoints, remote, remoteEndpoints, grant)
	r.Protocol = gatewayv1.UDPProtocolType
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	route = reconcileUDPRouteWithGraph(t, r, route, identity, nil)
	request := graph.Request{Identity: identity, Action: graph.Ensure, Gateway: gateway, UDPRoute: route}
	input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	if keys := r.enqueueGrant(t.Context(), grant); len(keys) != 1 || keys[0].NamespacedName != client.ObjectKeyFromObject(route) {
		t.Fatalf("deleting the grant should enqueue the weighted route, got %+v", keys)
	}
	r.APIReader = denyServiceRead{Reader: r.Client}
	if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("revoking a grant must invalidate the sealed graph, got %v", err)
	}
	route = reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	if route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatal("revoking a weighted backend grant did not checkpoint pool withdrawal")
	}
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		input, err := r.ObserveRouteGraph(ctx, request, nil)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if request.Action != graph.DeleteRoute || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphDelete || input.Nodes[0].Spec.Identity != identity {
			t.Fatalf("expected withdrawal of the owned UDP pool, got %+v", input.Nodes)
		}
		return readyUDPGraphResult(identity), input.Validate(ctx)
	})
	if current := reconcileUDPRoute(t, r, route); controller.RouteHasControllerBinding(r.Config, current) {
		t.Fatal("confirmed deletion of the weighted pool did not clear its binding")
	}
}

func udpBackendFixture(namespace, name string, weight int32, addresses ...string) (gatewayv1.BackendRef, *corev1.Service, *discoveryv1.EndpointSlice) {
	return transportBackendFixture(corev1.ProtocolUDP, namespace, name, weight, addresses...)
}

func transportBackendFixture(protocol corev1.Protocol, namespace, name string, weight int32, addresses ...string) (gatewayv1.BackendRef, *corev1.Service, *discoveryv1.EndpointSlice) {
	ref := gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(name), Port: ptr.To(gatewayv1.PortNumber(8080))}, Weight: &weight}
	portName := strings.ToLower(string(protocol))
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name + "-uid")},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: portName, Port: 8080, Protocol: protocol}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{discoveryv1.LabelServiceName: name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &portName, Protocol: &protocol, Port: ptr.To(int32(9090))}},
	}
	for _, address := range addresses {
		endpoints.Endpoints = append(endpoints.Endpoints, discoveryv1.Endpoint{Addresses: []string{address}})
	}
	return ref, service, endpoints
}

func reconcileUDPRouteWithGraph(t *testing.T, r *Reconciler, route *gatewayv1.UDPRoute, identity cloud.Identity, check func([]cloud.RouteGraphNode)) *gatewayv1.UDPRoute {
	t.Helper()
	r.Coordinator = &graph.Coordinator{}
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if request.Action != graph.Ensure || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.Identity != identity ||
			input.Nodes[0].Spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolUDP {
			t.Fatalf("expected one owned UDP pool graph, got %+v", input.Nodes)
		}
		if check != nil {
			check(input.Nodes)
		}
		return readyUDPGraphResult(identity), input.Validate(ctx)
	})
	current := reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	return current
}

func readyUDPGraphResult(identity cloud.Identity) cloud.GatewayGraphResult {
	return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{
		{Identity: identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}},
	}}
}

func reconcileUDPRoute(t *testing.T, r *Reconciler, route *gatewayv1.UDPRoute) *gatewayv1.UDPRoute {
	t.Helper()
	key := client.ObjectKeyFromObject(route)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var current gatewayv1.UDPRoute
	if err := r.Get(t.Context(), key, &current); err != nil {
		t.Fatal(err)
	}
	return &current
}

func TestUDPRouteRejectsTCPGrantBeforeReadingRemoteService(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("other"))
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "tcp-only"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TCPRoute", Namespace: "app"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service"}},
		},
	}
	_, _, tcp := routeFixture()
	tcp.UID = "tcp-route"
	r := testReconciler(t, gateway, class, route, tcp, grant)
	r.Protocol = gatewayv1.UDPProtocolType
	r.APIReader = denyServiceRead{Reader: r.Client}
	current := reconcileUDPRoute(t, r, route)
	checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(tcp), tcp); err != nil {
		t.Fatal(err)
	}
	if len(tcp.Status.Parents) != 0 {
		t.Fatalf("UDP reconcile changed the TCPRoute with the same name: %v", tcp.Status.Parents)
	}
}

func TestUDPRouteGraphFencesTypedTriggerAndOtherCandidates(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: "backend", UID: "udp-service"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolUDP}}},
	}
	r := testReconciler(t, gateway, class, route, service)
	r.Protocol = gatewayv1.UDPProtocolType
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	request := graph.Request{Identity: identity, Action: graph.Ensure, Gateway: gateway, UDPRoute: route}
	input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure ||
		input.Nodes[0].Spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolUDP {
		t.Fatalf("UDP route graph = %+v", input.Nodes)
	}
	for _, change := range []struct {
		name   string
		mutate func(*gatewayv1.UDPRoute)
	}{
		{"replaced UID", func(route *gatewayv1.UDPRoute) { route.UID = "stale-uid" }},
		{"stale resource version", func(route *gatewayv1.UDPRoute) { route.ResourceVersion = "1" }},
		{"edited full spec", func(route *gatewayv1.UDPRoute) { route.Spec.Rules[0].Name = ptr.To(gatewayv1.SectionName("changed")) }},
	} {
		t.Run(change.name, func(t *testing.T) {
			stale := request
			stale.UDPRoute = route.DeepCopy()
			change.mutate(stale.UDPRoute)
			if _, err := r.ObserveRouteGraph(t.Context(), stale, gateway); !errors.Is(err, controller.ErrGraphInputChanged) {
				t.Fatalf("stale UDP trigger returned %v", err)
			}
		})
	}
	newest := route.DeepCopy()
	newest.Name, newest.UID, newest.ResourceVersion = "newest", "route-newest", ""
	newest.Annotations, newest.Finalizers = nil, nil
	newest.CreationTimestamp = metav1.NewTime(route.CreationTimestamp.Add(time.Minute))
	if err := r.Create(t.Context(), newest); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("adding a matching candidate did not fence the UDP graph: %v", err)
	}

	// A listener changed to TCP can no longer authorize UDP mutations.
	gateway.Spec.Listeners[0].Protocol = gatewayv1.TCPProtocolType
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ObserveRouteGraph(t.Context(), request, gateway); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("UDP trigger accepted against TCP listener: %v", err)
	}
}

func TestUDPRouteAcceptsBothCandidatesAndDoesNotSkipInvalidWinner(t *testing.T) {
	gateway, class, oldest := udpRouteFixture()
	newest := oldest.DeepCopy()
	newest.Name, newest.UID = "z-newest", "route-newest"
	newest.Spec.Rules[0].BackendRefs[0].Name = "available"
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: newest.Namespace, Name: "available"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolUDP}}},
	}
	r := testReconciler(t, gateway, class, oldest, newest, service)
	r.Protocol = gatewayv1.UDPProtocolType
	current := reconcileUDPRoute(t, r, oldest)
	checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "BackendNotFound")

	// These routes have the same timestamp. The alphabetical winner's
	// missing Service must not admit the valid backend on the other route.
	current = reconcileUDPRoute(t, r, newest)
	checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "NotSelected")
}

func TestUDPRouteElectionKeepsOtherListenerWhenOlderTargetIsInvalid(t *testing.T) {
	gateway, class, broad := udpRouteFixture()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: broad.Namespace, Name: "backend", UID: "udp-service"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolUDP}}},
	}
	r := testReconciler(t, gateway, class, broad, service)
	r.Protocol = gatewayv1.UDPProtocolType
	identity := bindFixture(t, r, gateway, broad, cloud.MemberModeEndpoint)

	base := broad.DeepCopy()
	controller.SealRouteAnchor(r.Config, broad, identity, 8080)
	if err := r.Patch(t.Context(), broad, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	controller.SetGatewayListenerScope(r.Config, gateway, gatewayv1.UDPProtocolType)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
		Name: "other-udp", Protocol: gatewayv1.UDPProtocolType, Port: 8053,
	})
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	older := broad.DeepCopy()
	older.Name, older.UID, older.ResourceVersion = "older-specific", "route-specific", ""
	older.Finalizers, older.Annotations, older.Status.Parents = nil, nil, nil
	older.CreationTimestamp = metav1.NewTime(broad.CreationTimestamp.Add(-time.Minute))
	older.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("udp"))
	older.Spec.ParentRefs[0].Port = ptr.To(gatewayv1.PortNumber(8080))
	older.Spec.Rules[0].BackendRefs[0].Name = "missing"
	if err := r.Create(t.Context(), older); err != nil {
		t.Fatal(err)
	}
	request := graph.Request{Identity: identity, Action: graph.Ensure, Gateway: gateway, UDPRoute: broad}
	input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure ||
		!slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, []int{8053}) ||
		input.Nodes[0].Spec.GatewayRequirements.ListenerPort != 8080 {
		t.Fatalf("UDP graph with invalid winner on the anchor: %+v", input.Nodes)
	}

	older.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("other-udp"))
	if err := r.Update(t.Context(), older); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("changed conjunction did not fence the graph: %v", err)
	}
	input, err = r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || !slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, []int{8053, 8080}) {
		t.Fatalf("UDP graph for the broad route after a mismatched section and port: %+v", input.Nodes)
	}

	// An invalid older route winning both listeners withdraws the younger
	// pool's defaults, even before its separate detach reconcile runs.
	older.Spec.ParentRefs[0] = gatewayv1.ParentReference{Name: "edge"}
	if err := r.Update(t.Context(), older); err != nil {
		t.Fatal(err)
	}
	gatewayRequest := graph.Request{Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway}
	input, err = r.ObserveRouteGraph(t.Context(), gatewayRequest, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphPreserve ||
		input.Nodes[0].RetainListenerPorts == nil || len(input.Nodes[0].RetainListenerPorts) != 0 {
		t.Fatalf("losing UDP pool should withdraw every default before cleanup: %+v", input.Nodes)
	}
}

func TestUDPRouteRejectsAChangedElectionBeforeReportingProgrammed(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	gateway.Status.Conditions = []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionTrue, ObservedGeneration: gateway.Generation}}
	primary := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &gateway.Spec.Listeners[0].Name}
	secondary := gatewayv1.ParentReference{Name: primary.Name, SectionName: ptr.To(gatewayv1.SectionName("second"))}
	ref, service, endpoints := udpBackendFixture(route.Namespace, "backend", 1, "10.1.2.1")
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
	older := route.DeepCopy()
	older.Name, older.UID = "older", "older-uid"
	older.CreationTimestamp = metav1.NewTime(route.CreationTimestamp.Add(-time.Minute))
	older.Spec.ParentRefs = []gatewayv1.ParentReference{secondary}
	r := testReconciler(t, gateway, class, route, service, endpoints)
	r.Protocol, r.Coordinator = gatewayv1.UDPProtocolType, &graph.Coordinator{}
	bindGatewayFixture(t, r, gateway, cloud.MemberModeEndpoint)
	controller.SetGatewayListenerScope(r.Config, gateway, gatewayv1.UDPProtocolType)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: *secondary.SectionName, Protocol: gatewayv1.UDPProtocolType, Port: 9311})
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{primary, secondary}
	if err := r.Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	route = reconcileUDPRoute(t, r, route)
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		if err := r.Create(ctx, older); err != nil {
			t.Fatal(err)
		}
		input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if len(input.Nodes) != 1 || !slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, []int{8080}) {
			t.Fatalf("expected the older route to win the second port: %+v", input.Nodes)
		}
		return cloud.GatewayGraphResult{
			Outcome: cloud.ReadyOutcome(),
			Routes:  []cloud.RouteGraphResult{{Identity: request.Identity, Result: cloud.RouteReadyResult(cloud.RouteState{})}},
		}, input.Validate(ctx)
	})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	if _, err := r.Reconcile(t.Context(), request); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("expected a changed election to fence the planned route status, got %v", err)
	}
	if err := r.Get(t.Context(), request.NamespacedName, route); err != nil {
		t.Fatal(err)
	}
	programmed := r.Config.Domain() + "/Programmed"
	checkRouteCondition(t, route, r.Config.ControllerName, programmed, metav1.ConditionFalse, "Pending")

	r.Writer = readyRouteWriter(t, r)
	route = reconcileUDPRoute(t, r, route)
	route = reconcileUDPRoute(t, r, route)
	if first, second := r.currentParentCondition(viewRoute(route), primary, programmed), r.currentParentCondition(viewRoute(route), secondary, programmed); first == nil ||
		first.Status != metav1.ConditionTrue || second == nil || second.Status != metav1.ConditionFalse || second.Reason != "NotSelected" {
		t.Fatalf("route did not report the current winner: first=%+v, second=%+v", first, second)
	}
}

func TestUDPRouteRetainsOnlyCommittedTargetDuringSiblingProgress(t *testing.T) {
	for _, scenario := range []string{"same target", "changed ports", "changed backend", "own listener pending", "missing route observation"} {
		t.Run(scenario, func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
				Name: "other-udp", Protocol: gatewayv1.UDPProtocolType, Port: 8053,
			})
			ref, service, endpoints := udpBackendFixture(route.Namespace, "backend", 1, "10.1.2.1")
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
			older := route.DeepCopy()
			older.Name, older.UID = "older-specific", "route-specific"
			older.CreationTimestamp = metav1.NewTime(route.CreationTimestamp.Add(-time.Minute))
			older.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("other-udp"))
			r := testReconciler(t, gateway, class, route, service, endpoints)
			r.Protocol, r.Coordinator = gatewayv1.UDPProtocolType, &graph.Coordinator{}
			if scenario != "changed ports" {
				if err := r.Create(t.Context(), older); err != nil {
					t.Fatal(err)
				}
			}
			identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
			controller.SetGatewayListenerScope(r.Config, gateway, gatewayv1.UDPProtocolType)
			if err := r.Update(t.Context(), gateway); err != nil {
				t.Fatal(err)
			}
			base := route.DeepCopy()
			controller.SealRouteAnchor(r.Config, route, identity, 8080)
			if err := r.Patch(t.Context(), route, controller.OptimisticMergeFrom(base)); err != nil {
				t.Fatal(err)
			}
			// The first pass checkpoints the new, port-scoped target.
			route = reconcileUDPRoute(t, r, route)
			r.Writer = readyRouteWriter(t, r)
			route = reconcileUDPRoute(t, r, route)
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")

			switch scenario {
			case "changed ports":
				if err := r.Create(t.Context(), older); err != nil {
					t.Fatal(err)
				}
			case "changed backend":
				if err := r.Get(t.Context(), client.ObjectKeyFromObject(endpoints), endpoints); err != nil {
					t.Fatal(err)
				}
				endpoints.Endpoints[0].Addresses = []string{"10.1.2.2"}
				if err := r.Update(t.Context(), endpoints); err != nil {
					t.Fatal(err)
				}
			}
			observed := false
			r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
				input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
				if err != nil {
					return cloud.GatewayGraphResult{}, err
				}
				if len(input.Nodes) != 1 || !slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, []int{8080}) {
					t.Fatalf("selected UDP ports = %+v", input.Nodes)
				}
				observed = true
				pending := cloud.ProgressingOutcome("Waiting for a UDP listener", time.Second)
				result := cloud.GatewayGraphResult{Outcome: pending}
				if scenario != "missing route observation" {
					result.Routes = []cloud.RouteGraphResult{{Identity: request.Identity, Result: cloud.RouteResult{
						Outcome: pending, ObservationDeferred: scenario != "own listener pending",
					}}}
				}
				return result, input.Validate(ctx)
			})
			route = reconcileUDPRoute(t, r, route)
			if !observed {
				checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")
				route = reconcileUDPRoute(t, r, route)
			}
			if !observed {
				t.Fatal("did not reconcile the deferred provider observation")
			}
			if scenario == "same target" {
				checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
			} else {
				checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")
			}
		})
	}
}

func TestUDPRouteDetachesAfterRestartWithoutUDPListener(t *testing.T) {
	gateway, class, route := udpRouteFixture()
	r := testReconciler(t, gateway, class, route)
	r.Protocol, r.Coordinator = gatewayv1.UDPProtocolType, &graph.Coordinator{}
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	gateway.Spec.Listeners[0].Protocol = gatewayv1.TCPProtocolType
	if err := r.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	current := reconcileUDPRoute(t, r, route)
	if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("losing the UDP listener did not checkpoint detach: %v", current.Annotations)
	}

	restarted := &Reconciler{Client: r.Client, APIReader: r.APIReader, Config: r.Config, Protocol: r.Protocol, Coordinator: &graph.Coordinator{}}
	restarted.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		if request.Action != graph.DeleteRoute || request.UDPRoute == nil || request.UDPRoute.UID != route.UID || request.TCPRoute != nil {
			t.Fatalf("restart did not request typed UDP cleanup: %+v", request)
		}
		input, err := restarted.ObserveRouteGraph(ctx, request, nil)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphDelete ||
			input.Nodes[0].Spec.Identity != identity || input.Nodes[0].Spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolUDP {
			t.Fatalf("restart cleanup graph = %+v", input.Nodes)
		}
		if err := input.Validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{
			{Identity: identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}},
		}}, nil
	})
	current = reconcileUDPRoute(t, restarted, route)
	if controller.RouteHasControllerBinding(r.Config, current) {
		t.Fatalf("verified UDP cleanup retained the binding: %v", current.Finalizers)
	}
}

func TestBundleSkewWithdrawsChangedUDPNodePortTarget(t *testing.T) {
	for _, change := range []string{"released NodePort", "reallocated NodePort", "replacement Service", "removed endpoints"} {
		t.Run(change, func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: route.Namespace, UID: "dns-service"}, Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Name: "dns", Protocol: corev1.ProtocolUDP, Port: 8080, NodePort: 32053}},
			}}
			worker := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}},
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			}}
			endpoints := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace: service.Namespace, Name: "dns", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name},
			}, AddressType: discoveryv1.AddressTypeIPv4,
				Ports:     []discoveryv1.EndpointPort{{Name: ptr.To("dns"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(1053))}},
				Endpoints: []discoveryv1.Endpoint{{NodeName: &worker.Name, Addresses: []string{"10.244.0.7"}}},
			}
			r := testReconciler(t, gateway, class, route, service, worker, endpoints)
			r.Protocol, r.Coordinator = gatewayv1.UDPProtocolType, &graph.Coordinator{}
			bindFixture(t, r, gateway, route, cloud.MemberModeNodePort)
			r.Writer = readyRouteWriter(t, r)
			route = reconcileUDPRoute(t, r, route)
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")

			switch change {
			case "released NodePort":
				service.Spec.Type = corev1.ServiceTypeClusterIP
				service.Spec.Ports[0].NodePort = 0
				if err := r.Update(t.Context(), service); err != nil {
					t.Fatal(err)
				}
			case "reallocated NodePort":
				service.Spec.Ports[0].NodePort = 32054
				if err := r.Update(t.Context(), service); err != nil {
					t.Fatal(err)
				}
				input, err := r.ObserveRouteGraph(t.Context(), graph.Request{
					Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway,
				}, gateway)
				if err != nil || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphPreserve {
					t.Fatalf("Gateway graph admitted an unchecked target: %+v, %v", input.Nodes, err)
				}
			case "replacement Service":
				if err := r.Delete(t.Context(), service); err != nil {
					t.Fatal(err)
				}
				service.UID, service.ResourceVersion = "other-service", ""
				if err := r.Create(t.Context(), service); err != nil {
					t.Fatal(err)
				}
			case "removed endpoints":
				if err := r.Delete(t.Context(), endpoints); err != nil {
					t.Fatal(err)
				}
			}
			setBundleSkew(t, r)
			route = reconcileUDPRoute(t, r, route)
			if route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
				t.Fatalf("changed UDP target retained forwarding: %v", route.Status.Parents)
			}
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionUnknown, "UnsupportedVersion")
		})
	}
}
