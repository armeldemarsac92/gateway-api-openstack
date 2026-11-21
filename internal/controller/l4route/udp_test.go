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
	backends := []serviceBackend{{ref: serviceReference{port: ptr.To(gatewayv1.PortNumber(8080)), weight: 1}, service: service, port: &service.Spec.Ports[0]}}
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

func TestWeightedUDPRouteWithUnsupportedRatioWithdrawsPool(t *testing.T) {
	for _, test := range []struct {
		name      string
		denied    bool
		resolved  metav1.ConditionStatus
		refReason string
	}{
		{name: "resolved references", resolved: metav1.ConditionTrue, refReason: "ResolvedRefs"},
		{name: "denied zero-weight reference", denied: true, resolved: metav1.ConditionFalse, refReason: "RefNotPermitted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, class, route := udpRouteFixture()
			firstRef, first, firstEndpoints := udpBackendFixture(route.Namespace, "primary", 70, "10.1.2.1")
			secondRef, second, secondEndpoints := udpBackendFixture(route.Namespace, "secondary", 30, "10.1.2.2")
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{firstRef, secondRef}
			r := testReconciler(t, gateway, class, route, first, firstEndpoints, second, secondEndpoints)
			r.Protocol = gatewayv1.UDPProtocolType
			identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
			route = reconcileUDPRouteWithGraph(t, r, route, identity, nil)
			route.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(257))
			route.Spec.Rules[0].BackendRefs[1].Weight = ptr.To(int32(1))
			if test.denied {
				route.Spec.Rules[0].BackendRefs = append(route.Spec.Rules[0].BackendRefs, gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{Name: "disabled", Namespace: ptr.To(gatewayv1.Namespace("other")), Port: firstRef.Port},
					Weight:                 ptr.To(int32(0)),
				})
			}
			route.Generation++
			if err := r.Update(t.Context(), route); err != nil {
				t.Fatal(err)
			}
			current := reconcileUDPRoute(t, r, route)
			checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", test.resolved, test.refReason)
			checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "UnsupportedValue")
			if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
				t.Fatal("a ratio outside Octavia's member-weight range did not checkpoint pool withdrawal")
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
	ref := gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(name), Port: ptr.To(gatewayv1.PortNumber(8080))}, Weight: &weight}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name + "-uid")},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "udp", Port: 8080, Protocol: corev1.ProtocolUDP}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{discoveryv1.LabelServiceName: name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("udp"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(9090))}},
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
	controller.SealUDPRouteAnchor(r.Config, broad, identity, 8080)
	if err := r.Patch(t.Context(), broad, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	controller.SetGatewayUDPListenerMode(r.Config, gateway, true)
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
		!slices.Equal(input.Nodes[0].Spec.UDPListenerPorts, []int{8053}) ||
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
	if len(input.Nodes) != 1 || !slices.Equal(input.Nodes[0].Spec.UDPListenerPorts, []int{8053, 8080}) {
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
		input.Nodes[0].RetainUDPListenerPorts == nil || len(input.Nodes[0].RetainUDPListenerPorts) != 0 {
		t.Fatalf("losing UDP pool should withdraw every default before cleanup: %+v", input.Nodes)
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
