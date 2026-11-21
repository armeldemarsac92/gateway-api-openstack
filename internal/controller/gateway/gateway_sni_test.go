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

package gateway

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"

	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestSNIPassthroughRequiresOptInAndDedicatedListener(t *testing.T) {
	gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{
		Name: "sni", Port: 443, Protocol: gatewayv1.TLSProtocolType,
		TLS: &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModePassthrough)},
	}}}}
	config := controller.Config{}
	if _, err := SupportedListenersForConfig(config, gateway); err == nil {
		t.Fatal("accepted TLS passthrough with the flag disabled")
	}
	config.TLSSNIPassthrough = true
	if got, err := SupportedListenersForConfig(config, gateway); err != nil || len(got) != 1 || got[0].Protocol != gatewayv1.TLSProtocolType {
		t.Fatalf("wanted one TLS listener, got %v, %v", got, err)
	}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType})
	if _, err := SupportedListenersForConfig(config, gateway); err == nil {
		t.Fatal("accepted a mixed Gateway")
	}
}

func TestSNIRouteGrantWithdrawalLeavesOtherHostname(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, gatewayv1.Install} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	config := controller.Config{ControllerName: "example.net/gateway", ClusterID: "cluster", OpenStackProjectID: "project", TLSSNIPassthrough: true}
	listener := gatewayv1.Listener{Name: "sni", Port: 443, Protocol: gatewayv1.TLSProtocolType}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway-uid"}}
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener, topology: classconfig.Resolved{Topology: cloud.Topology{MemberMode: cloud.MemberModeEndpoint}}}
	route := func(name, hostname, target string) *gatewayv1.TLSRoute {
		return &gatewayv1.TLSRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "edge", UID: types.UID(name + "-uid")},
			Spec: gatewayv1.TLSRouteSpec{
				CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "gateway", SectionName: ptr.To(gatewayv1.SectionName("sni"))}}},
				Hostnames:       []gatewayv1.Hostname{gatewayv1.Hostname(hostname)},
				Rules: []gatewayv1.TLSRouteRule{{BackendRefs: []gatewayv1.BackendRef{{BackendObjectReference: gatewayv1.BackendObjectReference{
					Name: "backend", Namespace: ptr.To(gatewayv1.Namespace(target)), Port: ptr.To(gatewayv1.PortNumber(8443)),
				}}}}},
			},
		}
	}
	a, b := route("a", "a.example.test", "backend-a"), route("b", "b.example.test", "edge")
	service := func(namespace, uid, address string) (*corev1.Service, *discoveryv1.EndpointSlice) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: namespace, UID: types.UID(uid)},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tls", Port: 8443, Protocol: corev1.ProtocolTCP}}},
		}
		endpoints := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Name: "backend-1", Namespace: namespace, Labels: map[string]string{discoveryv1.LabelServiceName: svc.Name},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(svc, corev1.SchemeGroupVersion.WithKind("Service"))}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("tls"), Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(int32(9443))}},
			Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{address}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
		}
		return svc, endpoints
	}
	svcA, endpointsA := service("backend-a", "service-a", "192.0.2.101")
	svcB, endpointsB := service("edge", "service-b", "192.0.2.102")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "grant-a", Namespace: "backend-a"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TLSRoute", Namespace: "edge"}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(svcA.Name))}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(a, b, svcA, svcB, endpointsA, endpointsB, grant).Build()
	r := &Reconciler{APIReader: client, Config: config}
	observed, err := r.observeSNIRoutes(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := observed.targets(); len(got) != 2 || got[0].Hostname != "a.example.test" || got[1].Hostname != "b.example.test" ||
		len(got[0].Members) != 1 || got[0].Members[0].Address != "192.0.2.101" || got[0].Members[0].Port != 9443 {
		t.Fatalf("expected two disjoint endpoint targets, got %#v", got)
	}
	if err := client.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	observed, err = r.observeSNIRoutes(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := observed.targets(); len(got) != 1 || got[0].Hostname != "b.example.test" {
		t.Fatalf("grant withdrawal must withdraw A only, got %#v", got)
	}
	if observed[0].resolvedReason != gatewayv1.RouteReasonRefNotPermitted {
		t.Fatalf("expected RefNotPermitted, got %#v", observed[0])
	}
}

func TestSNIFlagOffPreservesMixedGatewayHTTP(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Generation++
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, testSNIListener())
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	if len(provider.deletedGateways) != 0 || len(provider.gatewaySpecs) == 0 || provider.gatewaySpecs[0].ListenerPort != 80 {
		t.Fatalf("HTTP graph did not stay in service with SNI off: ensures=%v, deletes=%v", provider.gatewaySpecs, provider.deletedGateways)
	}
}

func TestTLSGatewayCleanupClearsOnlyItsOwnRouteParentStatus(t *testing.T) {
	for _, deleting := range []bool{true, false} {
		cfg := testConfig()
		cfg.TLSSNIPassthrough = true
		gateway := programmedTestGateway(cfg)
		gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.TLSProtocolType)
		gateway.Spec.Listeners[0].Protocol = gatewayv1.TCPProtocolType
		if deleting {
			gateway.DeletionTimestamp = ptr.To(metav1.Now())
		}
		route := &gatewayv1.TLSRoute{ObjectMeta: metav1.ObjectMeta{Name: "sni", Namespace: gateway.Namespace, UID: "route", Generation: 1}}
		parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
		other := gatewayv1.ParentReference{Name: "other-gateway"}
		programmed := []metav1.Condition{
			controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Attached", 1),
			controller.Condition(cfg.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed", "Read back", 1),
		}
		route.Spec.ParentRefs = []gatewayv1.ParentReference{parent}
		route.Status.Parents = []gatewayv1.RouteParentStatus{
			{ParentRef: parent, ControllerName: cfg.ControllerName, Conditions: programmed},
			{ParentRef: parent, ControllerName: "other.example/controller", Conditions: programmed},
			{ParentRef: other, ControllerName: cfg.ControllerName, Conditions: programmed},
		}
		class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
		kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).WithObjects(class, gateway, route).Build()
		provider := &recordingProvider{}
		r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
		for range 2 {
			if _, err := r.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
		}
		var got gatewayv1.TLSRoute
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Status.Parents) != 2 || got.Status.Parents[0].ControllerName != "other.example/controller" || got.Status.Parents[1].ParentRef.Name != other.Name {
			t.Fatalf("deleting=%t: should preserve other parents and controllers only, got %#v", deleting, got.Status.Parents)
		}
	}
}

func TestSNIGatewayCannotReplaceStoredNodePortTopology(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	gateway := programmedTestGateway(cfg)
	gateway.Generation++
	gateway.Spec.Listeners[0] = testSNIListener()
	gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.TCPProtocolType)
	completeGatewayTopologyFixture(cfg, gateway)
	topology, err := controller.StoredGatewayTopology(cfg, gateway)
	if err != nil {
		t.Fatal(err)
	}
	topology.Topology.MemberMode = cloud.MemberModeNodePort
	controller.ClearGatewayTopology(cfg, gateway)
	if err := controller.BindGatewayTopology(cfg, gateway, topology); err != nil {
		t.Fatal(err)
	}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	for range 3 {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	var got gatewayv1.Gateway
	if err := kube.Get(t.Context(), request.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[cfg.GatewayListenerProtocolAnnotation()] != string(gatewayv1.TCPProtocolType) || len(provider.gatewaySpecs) != 0 {
		t.Fatalf("NodePort topology must prevent the replacement TLS binding: spec=%#v annotation=%q", provider.gatewaySpecs, got.Annotations[cfg.GatewayListenerProtocolAnnotation()])
	}
}

func TestSNIRoutePendingSiblingUsesOnlySameAuthorizedTarget(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Spec.Listeners[0] = testSNIListener()
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	route := &gatewayv1.TLSRoute{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: gateway.Namespace, UID: "route-b", Generation: 1}}
	target := cloud.TLSSNIRoute{Identity: controller.RouteIdentity(cfg, gateway, route), Hostname: "b.example.test", ServiceUID: "service-b",
		Members: []cloud.Member{{Address: "192.0.2.21", Port: 443}}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(route).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{Identity: target.Identity}}}
	report := func(want bool) {
		t.Helper()
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
			t.Fatal(err)
		}
		obs := sniRouteObservation{{route: route, parents: []gatewayv1.ParentReference{parent}, target: &target,
			acceptedReason: gatewayv1.RouteReasonAccepted, acceptedMessage: "Attached",
			resolvedReason: gatewayv1.RouteReasonResolvedRefs, resolvedMessage: "Resolved"}}
		if err := r.reportSNIRoutes(t.Context(), gateway, obs, result, nil); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
			t.Fatal(err)
		}
		if got := meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed"); got != want {
			t.Fatalf("Programmed=%t, want %t: %#v", got, want, route.Status.Parents[0].Conditions)
		}
	}
	result.Routes[0].Result = cloud.RouteReadyResult(cloud.RouteState{})
	report(true)
	result.Routes[0].Result = cloud.RouteResult{ObservationDeferred: true}
	report(true)
	target.ServiceUID = "service-b-recreated"
	report(false)
}

func testSNIListener() gatewayv1.Listener {
	return gatewayv1.Listener{Name: "sni", Port: 443, Protocol: gatewayv1.TLSProtocolType,
		TLS: &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModePassthrough)}}
}
