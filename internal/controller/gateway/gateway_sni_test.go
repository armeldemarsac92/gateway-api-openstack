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
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
	for _, hostname := range []*gatewayv1.Hostname{nil, ptr.To(gatewayv1.Hostname("a.example.test")), ptr.To(gatewayv1.Hostname("*.example.test"))} {
		gateway.Spec.Listeners[0].Hostname = hostname
		if got, err := SupportedListenersForConfig(config, gateway); err != nil || len(got) != 1 || got[0].Protocol != gatewayv1.TLSProtocolType {
			t.Fatalf("wanted one TLS listener with hostname %v, got %v, %v", ptr.Deref(hostname, ""), got, err)
		}
	}
	terminating := gateway.Spec.Listeners[0].DeepCopy()
	terminating.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	if kinds := listenerSupportedRouteKinds(config, *terminating); len(kinds) != 0 {
		t.Fatalf("TLS termination advertises unsupported route kinds: %v", kinds)
	}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType})
	if _, err := SupportedListenersForConfig(config, gateway); err == nil {
		t.Fatal("accepted a mixed Gateway")
	}
}

func testSNIRoute(gateway *gatewayv1.Gateway, name, target string, hostnames ...gatewayv1.Hostname) *gatewayv1.TLSRoute {
	return &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: gateway.Namespace, UID: types.UID(name + "-uid"), Generation: 1},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("sni"))}}},
			Hostnames:       hostnames,
			Rules: []gatewayv1.TLSRouteRule{{BackendRefs: []gatewayv1.BackendRef{{BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: "backend", Namespace: ptr.To(gatewayv1.Namespace(target)), Port: ptr.To(gatewayv1.PortNumber(8443)),
			}}}}},
		},
	}
}

func testSNIBackend(namespace, uid, address string) (*corev1.Service, *discoveryv1.EndpointSlice) {
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

func TestSNIRouteHostnameIntersection(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	for _, tc := range []struct {
		name     string
		suffix   bool
		listener *gatewayv1.Hostname
		route    []gatewayv1.Hostname
		want     []string
		reason   gatewayv1.RouteConditionReason
	}{
		{"unconstrained listener", false, nil, []gatewayv1.Hostname{"z.test", "a.test", "a.test"}, []string{"a.test", "z.test"}, gatewayv1.RouteReasonAccepted},
		{"exact listener", false, ptr.To(gatewayv1.Hostname("a.deep.example.test")), []gatewayv1.Hostname{"outside.test", "*.example.test"}, []string{"a.deep.example.test"}, gatewayv1.RouteReasonAccepted},
		{"wildcard listener", false, ptr.To(gatewayv1.Hostname("*.example.test")), []gatewayv1.Hostname{"a.deep.example.test", "example.test", "b.example.test", "outside.test"}, []string{"a.deep.example.test", "b.example.test"}, gatewayv1.RouteReasonAccepted},
		{"disjoint", false, ptr.To(gatewayv1.Hostname("*.example.test")), []gatewayv1.Hostname{"example.test", "*.outside.test"}, nil, gatewayv1.RouteReasonNoMatchingListenerHostname},
		{"unrepresentable", false, ptr.To(gatewayv1.Hostname("*.example.test")), []gatewayv1.Hostname{"a.example.test", "*.deep.example.test"}, nil, gatewayv1.RouteReasonUnsupportedValue},
		{"nested wildcard with suffix enabled", true, ptr.To(gatewayv1.Hostname("*.example.test")), []gatewayv1.Hostname{"*.test", "*.shop.example.test", "example.test"}, []string{"*.example.test", "*.shop.example.test"}, gatewayv1.RouteReasonAccepted},
		{"catch-all remains unsupported", true, nil, nil, nil, gatewayv1.RouteReasonUnsupportedValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg.TLSSNISuffix = tc.suffix
			listener := testSNIListener()
			listener.Hostname = tc.listener
			gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway-uid"}}
			route := testSNIRoute(gateway, "route", gateway.Namespace, tc.route...)
			service, endpoints := testSNIBackend(gateway.Namespace, "service", "192.0.2.101")
			kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(route, service, endpoints).Build()
			r := &Reconciler{APIReader: kube, Config: cfg}
			observed, err := r.observeSNIRoutes(t.Context(), &gatewayMutationSnapshot{gateway: gateway, listener: &listener})
			if err != nil {
				t.Fatal(err)
			}
			if len(observed) != 1 || observed[0].acceptedReason != tc.reason {
				t.Fatalf("expected Accepted reason %q, got %#v", tc.reason, observed)
			}
			var got []string
			for _, target := range observed.targets() {
				got = append(got, target.Hostname)
				if target.Identity.RouteUID != string(route.UID) || target.ServiceUID != string(service.UID) {
					t.Fatalf("target lost route or backend identity: %#v", target)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("target hostnames = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSNIRouteHostnamePrecedence(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	const hostname = "a.deep.example.test"
	for _, tc := range []struct {
		name         string
		listenerHost gatewayv1.Hostname
		firstHost    gatewayv1.Hostname
		secondHost   gatewayv1.Hostname
		firstCreated int64
		wantWinner   string
	}{
		{"exact beats older wildcard", hostname, "*.deep.example.test", hostname, 1, "z"},
		{"longer wildcard beats older wildcard", hostname, "*.example.test", "*.deep.example.test", 1, "z"},
		{"listener narrows both routes to the same wildcard", "*.example.test", "*.test", "*.example.test", 1, "z"},
		{"older route beats alphabetical order", hostname, hostname, hostname, 3, "z"},
		{"alphabetical tie", hostname, hostname, hostname, 2, "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := testSNIListener()
			listener.Hostname = &tc.listenerHost
			cfg.TLSSNISuffix = tc.listenerHost == "*.example.test"
			gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway-uid"}}
			first := testSNIRoute(gateway, "a", gateway.Namespace, tc.firstHost)
			second := testSNIRoute(gateway, "z", gateway.Namespace, tc.secondHost)
			first.CreationTimestamp = metav1.NewTime(time.Unix(tc.firstCreated, 0))
			second.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
			service, endpoints := testSNIBackend(gateway.Namespace, "service", "192.0.2.101")
			kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(first, second).WithObjects(first, second, service, endpoints).Build()
			r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
			snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
			observed, err := r.observeSNIRoutes(t.Context(), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if got := observed.targets(); len(got) != 1 || got[0].Hostname != string(tc.listenerHost) || got[0].Identity.RouteName != tc.wantWinner {
				t.Fatalf("target = %#v, want %s to serve %s", got, tc.wantWinner, tc.listenerHost)
			}
			for _, entry := range observed {
				if entry.acceptedReason != gatewayv1.RouteReasonAccepted || entry.resolvedReason != gatewayv1.RouteReasonResolvedRefs {
					t.Fatalf("%s must be attached with resolved references: %#v", entry.route.Name, entry)
				}
			}
			if err := r.reportSNIRoutes(t.Context(), snapshot, observed, cloud.GatewayGraphResult{}, nil); err != nil {
				t.Fatal(err)
			}
			if counts, err := r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{&listener}); err != nil || counts.counts[listener.Name] != 2 {
				t.Fatalf("attached routes = %v, want 2: %v", counts, err)
			}
			listener.Hostname = ptr.To(gatewayv1.Hostname("changed.other.invalid"))
			if counts, err := r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{&listener}); err != nil || counts.counts[listener.Name] != 0 {
				t.Fatalf("attached routes after listener edit = %v, want 0: %v", counts, err)
			}
		})
	}
}

func TestSNIRouteGrantWithdrawalLeavesOtherHostname(t *testing.T) {
	config := testConfig()
	config.TLSSNIPassthrough = true
	listener := testSNIListener()
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway-uid"}}
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener, topology: classconfig.Resolved{Topology: cloud.Topology{MemberMode: cloud.MemberModeEndpoint}}}
	a := testSNIRoute(gateway, "a", "backend-a", "a.example.test")
	b := testSNIRoute(gateway, "b", "edge", "a.example.test", "b.example.test")
	svcA, endpointsA := testSNIBackend("backend-a", "service-a", "192.0.2.101")
	svcB, endpointsB := testSNIBackend("edge", "service-b", "192.0.2.102")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "grant-a", Namespace: "backend-a"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TLSRoute", Namespace: "edge"}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(svcA.Name))}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(a, b, svcA, svcB, endpointsA, endpointsB, grant).Build()
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

func TestSNISuffixGrantLifecyclePreservesNarrowerClaim(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough, cfg.TLSSNISuffix = true, true
	listener := testSNIListener()
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway-uid"}}
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
	a := testSNIRoute(gateway, "a", "backend-a", "*.shop.example.test")
	b := testSNIRoute(gateway, "b", "edge", "*.example.test", "vip.shop.example.test")
	svcA, endpointsA := testSNIBackend("backend-a", "service-a", "192.0.2.101")
	svcB, endpointsB := testSNIBackend("edge", "service-b", "192.0.2.102")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "grant-a", Namespace: "backend-a"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: tlsRouteKind, Namespace: "edge"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(svcA.Name))}},
		},
	}
	forbiddenRead := false
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(a, b).
		WithObjects(a, b, svcA, svcB, endpointsA, endpointsB, grant).
		WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, service := obj.(*corev1.Service); forbiddenRead && service && key == client.ObjectKeyFromObject(svcA) {
				t.Error("read the narrow route's Service without a ReferenceGrant")
			}
			return c.Get(ctx, key, obj, opts...)
		}}).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	check := func(deny, resolved bool, reason string) {
		t.Helper()
		observed, err := r.observeSNIRoutes(t.Context(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		targets := observed.targets()
		if len(targets) != 3 || targets[0].Hostname != "*.shop.example.test" || targets[0].Deny != deny ||
			(deny && (targets[0].ServiceUID != "" || len(targets[0].Members) != 0)) ||
			targets[1].Hostname != "*.example.test" || targets[1].Deny || targets[2].Hostname != "vip.shop.example.test" || targets[2].Deny {
			t.Fatalf("targets = %#v", targets)
		}
		result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{
			{Identity: controller.RouteIdentity(cfg, gateway, a), Result: cloud.RouteReadyResult(cloud.RouteState{})},
			{Identity: controller.RouteIdentity(cfg, gateway, b), Result: cloud.RouteReadyResult(cloud.RouteState{})},
		}}
		if err := r.reportSNIRoutes(t.Context(), snapshot, observed, result, nil); err != nil {
			t.Fatal(err)
		}
		var current gatewayv1.TLSRoute
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(a), &current); err != nil {
			t.Fatal(err)
		}
		conditions := current.Status.Parents[0].Conditions
		refStatus := meta.FindStatusCondition(conditions, "ResolvedRefs")
		if !meta.IsStatusConditionTrue(conditions, "Accepted") ||
			(refStatus.Status == metav1.ConditionTrue) != resolved || refStatus.Reason != reason ||
			meta.IsStatusConditionTrue(conditions, cfg.Domain()+"/Programmed") == deny ||
			(meta.FindStatusCondition(conditions, cfg.Domain()+"/ForwardingTarget") == nil) != deny {
			t.Fatalf("narrow route conditions = %#v", conditions)
		}
	}
	check(false, true, string(gatewayv1.RouteReasonResolvedRefs))
	if err := kube.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	forbiddenRead = true
	check(true, false, string(gatewayv1.RouteReasonRefNotPermitted))
	forbiddenRead = false
	grant.ResourceVersion = ""
	if err := kube.Create(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	check(false, true, string(gatewayv1.RouteReasonResolvedRefs))
	if err := kube.Delete(t.Context(), endpointsA); err != nil {
		t.Fatal(err)
	}
	check(true, true, string(gatewayv1.RouteReasonResolvedRefs))
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

func TestTLSRouteRejectedByHTTPListenerReportsEachReferencedSection(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	gateway := programmedTestGateway(cfg)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName)}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	route := testSNIRoute(gateway, "tls", "unused", "app.example.test")
	listener := gateway.Spec.Listeners[0].Name
	missing := gatewayv1.SectionName("absent")
	route.Spec.ParentRefs = []gatewayv1.ParentReference{
		{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &listener},
		{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &missing},
	}
	other := gatewayv1.RouteParentStatus{ParentRef: route.Spec.ParentRefs[0], ControllerName: "other.example/controller"}
	route.Status.Parents = []gatewayv1.RouteParentStatus{other}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).WithObjects(class, gateway, route).Build()
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	var got gatewayv1.TLSRoute
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	want := map[gatewayv1.SectionName]gatewayv1.RouteConditionReason{
		listener: gatewayv1.RouteReasonNotAllowedByListeners,
		missing:  gatewayv1.RouteReasonNoMatchingParent,
	}
	for _, parent := range got.Status.Parents {
		if parent.ControllerName == other.ControllerName {
			continue
		}
		section := ptr.Deref(parent.ParentRef.SectionName, "")
		reason, found := want[section]
		accepted := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionAccepted))
		if !found || parent.ControllerName != cfg.ControllerName || accepted == nil || accepted.Status != metav1.ConditionFalse ||
			accepted.Reason != string(reason) || accepted.ObservedGeneration != got.Generation {
			t.Fatalf("unexpected parent status for section %q: %#v", section, parent)
		}
		delete(want, section)
	}
	if len(want) > 0 {
		t.Fatalf("missing TLSRoute parent status for sections %v", want)
	}
	got.Spec.ParentRefs = nil
	if err := kube.Update(t.Context(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Parents) != 1 || got.Status.Parents[0].ControllerName != other.ControllerName {
		t.Fatalf("detached TLSRoute should only retain the other controller's status: %#v", got.Status.Parents)
	}
}

func TestTLSRouteRejectionSurvivesTerminatingGatewayClass(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	gateway := programmedTestGateway(cfg)
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              string(gateway.Spec.GatewayClassName),
			DeletionTimestamp: ptr.To(metav1.Now()),
			Finalizers:        []string{gatewayv1.GatewayClassFinalizerGatewaysExist},
		},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	route := testSNIRoute(gateway, "tls", "unused", "app.example.test")
	route.Spec.ParentRefs[0].SectionName = ptr.To(gateway.Spec.Listeners[0].Name)
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef:      route.Spec.ParentRefs[0],
		ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{controller.Condition("Accepted", metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNotAllowedByListeners), "No selected listener accepts TLSRoutes", route.Generation)},
	}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).WithObjects(class, gateway, route).Build()
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
		t.Fatal(err)
	}
	var got gatewayv1.TLSRoute
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Parents) != 1 || !meta.IsStatusConditionFalse(got.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) {
		t.Fatalf("the Gateway must retain its rejection while its GatewayClass still exists: %#v", got.Status.Parents)
	}
}

func TestTLSRouteParentStatusCapacityPreservesForeignEntriesAndOwnedUpdates(t *testing.T) {
	const parentLimit = 32
	cfg := testConfig()
	cfg.TLSSNIPassthrough = true
	gateway := programmedTestGateway(cfg)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName)}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	route := testSNIRoute(gateway, "tls", "unused", "app.example.test")
	route.Spec.ParentRefs = []gatewayv1.ParentReference{
		{Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gateway.Spec.Listeners[0].Name)},
		{Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("absent"))},
	}
	foreign := make([]gatewayv1.RouteParentStatus, parentLimit-1)
	for i := range foreign {
		foreign[i] = gatewayv1.RouteParentStatus{
			ParentRef:      route.Spec.ParentRefs[0],
			ControllerName: gatewayv1.GatewayController(fmt.Sprintf("other-%d.example/controller", i)),
		}
	}
	route.Status.Parents = append([]gatewayv1.RouteParentStatus{{
		ParentRef:      gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("detached"))},
		ControllerName: cfg.ControllerName,
	}}, foreign...)
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).WithObjects(class, gateway, route).Build()
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	reconcile := func() *gatewayv1.TLSRoute {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
			t.Fatal(err)
		}
		var got gatewayv1.TLSRoute
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Status.Parents) != parentLimit || !equality.Semantic.DeepEqual(got.Status.Parents[:len(foreign)], foreign) {
			t.Fatalf("expected %d parent statuses with the foreign entries unchanged, got %d", parentLimit, len(got.Status.Parents))
		}
		status := got.Status.Parents[len(foreign)]
		accepted := meta.FindStatusCondition(status.Conditions, string(gatewayv1.RouteConditionAccepted))
		if status.ControllerName != cfg.ControllerName || !equality.Semantic.DeepEqual(status.ParentRef, route.Spec.ParentRefs[0]) ||
			accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.ObservedGeneration != got.Generation {
			t.Fatalf("expected the current rejection in the remaining slot: %#v", status)
		}
		return &got
	}
	got := reconcile()
	got.Generation++
	if err := kube.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile()
}

func TestTLSGatewayCleanupClearsOnlyItsOwnForwardingStatus(t *testing.T) {
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
		if len(got.Status.Parents) < 2 || got.Status.Parents[0].ControllerName != "other.example/controller" || got.Status.Parents[1].ParentRef.Name != other.Name {
			t.Fatalf("deleting=%t: other parents and controllers must be preserved, got %#v", deleting, got.Status.Parents)
		}
		if deleting {
			if len(got.Status.Parents) != 2 {
				t.Fatalf("deleted Gateway must leave no owned parent status: %#v", got.Status.Parents)
			}
		} else {
			if len(got.Status.Parents) != 3 {
				t.Fatalf("replacement TCP listener must leave a rejected parent status: %#v", got.Status.Parents)
			}
			conditions := got.Status.Parents[2].Conditions
			accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
			if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) ||
				!meta.IsStatusConditionFalse(conditions, cfg.Domain()+"/Programmed") {
				t.Fatalf("replacement TCP listener should clear the old TLS attachment: %#v", conditions)
			}
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

func TestSNIRouteDeferredReadbackRequiresSameAuthorizedTargets(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	listener := testSNIListener()
	gateway.Spec.Listeners[0] = listener
	route := testSNIRoute(gateway, "b", gateway.Namespace, "b.example.test", "a.example.test")
	winner := testSNIRoute(gateway, "a", gateway.Namespace, "a.example.test")
	service, endpoints := testSNIBackend(gateway.Namespace, "service", "192.0.2.101")
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(route, winner).WithObjects(route, service, endpoints).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
	result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{Identity: controller.RouteIdentity(cfg, gateway, route)}}}
	report := func(want bool) {
		t.Helper()
		observed, err := r.observeSNIRoutes(t.Context(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.reportSNIRoutes(t.Context(), snapshot, observed, result, nil); err != nil {
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
	if counts, err := r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{&listener}); err != nil || counts.counts[listener.Name] != 1 {
		t.Fatalf("attached routes = %v, want one route for both hostnames: %v", counts, err)
	}

	if err := kube.Create(t.Context(), winner); err != nil {
		t.Fatal(err)
	}
	report(false)
	result.Routes[0].Result = cloud.RouteReadyResult(cloud.RouteState{})
	report(true)
	if err := kube.Delete(t.Context(), winner); err != nil {
		t.Fatal(err)
	}
	result.Routes[0].Result = cloud.RouteResult{ObservationDeferred: true}
	report(false)
	result.Routes[0].Result = cloud.RouteReadyResult(cloud.RouteState{})
	report(true)
	for _, object := range []client.Object{endpoints, service} {
		if err := kube.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	newService, newEndpoints := testSNIBackend(gateway.Namespace, "service-recreated", "192.0.2.101")
	for _, object := range []client.Object{newService, newEndpoints} {
		if err := kube.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	result.Routes[0].Result = cloud.RouteResult{ObservationDeferred: true}
	report(false)

}

func testSNIListener() gatewayv1.Listener {
	return gatewayv1.Listener{Name: "sni", Port: 443, Protocol: gatewayv1.TLSProtocolType,
		TLS: &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModePassthrough)}}
}

func TestTLSRouteTerminateUsesCertificateBindingAndListenerHostname(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.TCPTLSTermination = true, true
	gateway := programmedTestGateway(cfg)
	listener := testHTTPSListener("sni", 8443)
	listener.Protocol = gatewayv1.TLSProtocolType
	listener.Hostname = ptr.To(gatewayv1.Hostname("tls.example.com"))
	listener.AllowedRoutes = &gatewayv1.AllowedRoutes{Kinds: []gatewayv1.RouteGroupKind{{Kind: tlsRouteKind}}}
	listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	listener.TLS.CertificateRefs[0].Namespace = ptr.To(gatewayv1.Namespace("certs"))
	gateway.Spec.Listeners = []gatewayv1.Listener{listener}
	gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = cloud.ListenerProtocolTLSTerminate
	gateway.Annotations[cfg.GatewayListenerPortAnnotation()] = "8443"
	topology := testBoundTopology()
	topology.Topology.MemberMode = cloud.MemberModeEndpoint
	if err := controller.BindGatewayTopology(cfg, gateway, topology); err != nil {
		t.Fatal(err)
	}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	route := testSNIRoute(gateway, "route", gateway.Namespace)
	service, endpoints := testSNIBackend(gateway.Namespace, "service-a", "192.0.2.101")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "certificate", Namespace: "certs"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: gatewayv1.Namespace(gateway.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Secret", Name: ptr.To(gatewayv1.ObjectName("certificate"))}},
		},
	}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(class, gateway, route).WithObjects(class, gateway, route, service, endpoints, grant).Build()
	const certificate = "https://barbican.example/v1/secrets/approved"
	var last cloud.GatewayGraphNode
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(ctx context.Context, _ *gatewayv1.Gateway, _ *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			if err := kube.Get(ctx, client.ObjectKeyFromObject(grant), &gatewayv1.ReferenceGrant{}); err != nil {
				return "", gatewayv1.ListenerReasonRefNotPermitted
			}
			return certificate, ""
		}),
	}, httpsGraphProvider(func(_ context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		last = spec.Gateway
		result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(cloud.GatewayState{VIPAddress: "192.0.2.10"})}
		if last.Disposition == cloud.GraphEnsure && last.TCPTLSCertificateRef != "" {
			result.Routes = []cloud.RouteGraphResult{{
				Identity: controller.RouteIdentity(cfg, gateway, route),
				Result:   cloud.RouteReadyResult(cloud.RouteState{}),
			}}
		}
		return result, nil
	}))
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if requests := r.gatewayRequestsForCertificateGrant(t.Context(), grant); !slices.Equal(requests, []ctrl.Request{request}) {
		t.Fatalf("certificate grant requests = %#v", requests)
	}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), request.NamespacedName, gateway); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if last.Spec.ListenerProtocol != cloud.ListenerProtocolTLSTerminate || last.TCPTLSCertificateRef != certificate ||
		len(last.TLSSNIRoutes) != 1 || last.TLSSNIRoutes[0].Hostname != string(*listener.Hostname) ||
		!slices.Equal(last.TLSSNIRoutes[0].Members, []cloud.Member{{Address: "192.0.2.101", Port: 9443}}) {
		t.Fatalf("Gateway graph = %#v", last)
	}
	status := gateway.Status.Listeners[0]
	if status.AttachedRoutes != 1 || !equality.Semantic.DeepEqual(status.SupportedKinds, supportedRouteKinds(tlsRouteKind)) ||
		!meta.IsStatusConditionTrue(status.Conditions, string(gatewayv1.ListenerConditionAccepted)) ||
		!meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed") {
		t.Fatalf("Listener = %#v, route = %#v", status, route.Status)
	}
	route.Spec.Hostnames = []gatewayv1.Hostname{*listener.Hostname}
	if err := kube.Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if len(last.TLSSNIRoutes) != 1 || last.TLSSNIRoutes[0].Hostname != string(*listener.Hostname) ||
		!meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed") {
		t.Fatalf("named upstream route graph = %#v, status = %#v", last, route.Status)
	}

	foreign := route.Status.Parents[0].DeepCopy()
	foreign.ControllerName = "other.example/controller"
	route.Status.Parents = append(route.Status.Parents, *foreign)
	if err := kube.Status().Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	controller.SetCondition(&class.Status.Conditions, controller.Condition(string(gatewayv1.GatewayClassConditionStatusSupportedVersion),
		metav1.ConditionFalse, string(gatewayv1.GatewayClassReasonUnsupportedVersion), "Unsupported version", class.Generation))
	if err := kube.Status().Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	reconcile()
	owned := route.Status.Parents[0].Conditions
	if last.Disposition != cloud.GraphWithdrawTLS || last.TCPTLSCertificateRef != "" ||
		!meta.IsStatusConditionFalse(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)) ||
		!meta.IsStatusConditionFalse(gateway.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) ||
		!meta.IsStatusConditionFalse(owned, cfg.Domain()+"/Programmed") || meta.FindStatusCondition(owned, cfg.Domain()+"/ForwardingTarget") != nil ||
		!meta.IsStatusConditionTrue(owned, string(gatewayv1.RouteConditionAccepted)) ||
		!equality.Semantic.DeepEqual(route.Status.Parents[1], *foreign) {
		t.Fatalf("version skew: graph = %#v Gateway = %#v listeners = %#v route = %#v", last, gateway.Status.Conditions, gateway.Status.Listeners, route.Status)
	}
	controller.SetCondition(&class.Status.Conditions, controller.Condition(string(gatewayv1.GatewayClassConditionStatusSupportedVersion),
		metav1.ConditionTrue, string(gatewayv1.GatewayClassReasonSupportedVersion), "Supported version", class.Generation))
	if err := kube.Status().Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if !meta.IsStatusConditionTrue(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)) ||
		!meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed") {
		t.Fatalf("programming did not recover: Gateway = %#v route = %#v", gateway.Status.Conditions, route.Status)
	}

	if err := kube.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	reconcile()
	status = gateway.Status.Listeners[0]
	refs := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
	if last.TCPTLSCertificateRef != "" || refs == nil || refs.Status != metav1.ConditionFalse || refs.Reason != string(gatewayv1.ListenerReasonRefNotPermitted) ||
		!meta.IsStatusConditionFalse(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed") {
		t.Fatalf("certificate revocation graph = %#v, listener = %#v, route = %#v", last, status, route.Status)
	}

	r.Config.TCPTLSTermination = false
	reconcile()
	if last.Disposition != cloud.GraphWithdrawTLS || last.TCPTLSCertificateRef != "" ||
		last.Spec.ListenerProtocol != cloud.ListenerProtocolTLSTerminate ||
		!meta.IsStatusConditionFalse(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) {
		t.Fatalf("opt-out graph = %#v, route = %#v", last, route.Status)
	}
}

func TestTLSListenerModeStatus(t *testing.T) {
	cfg := testConfig()
	termination := testHTTPSListener("terminate", 8443)
	termination.Protocol = gatewayv1.TLSProtocolType
	passthrough := testSNIListener()
	passthrough.Port = termination.Port
	gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{termination}}}
	r := &Reconciler{Config: cfg}
	verify := func(reason gatewayv1.ListenerConditionReason) {
		t.Helper()
		_, err := SupportedListenersForConfig(r.Config, gateway)
		if err == nil {
			t.Fatal("listener configuration should be rejected")
		}
		r.setGatewayFailure(gateway, err)
		for _, listener := range gateway.Status.Listeners {
			accepted := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionAccepted))
			if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(reason) || listener.AttachedRoutes != 0 {
				t.Fatalf("listener %s Accepted = %#v, routes = %d, expected %s", listener.Name, accepted, listener.AttachedRoutes, reason)
			}
		}
	}
	verify(gatewayv1.ListenerReasonUnsupportedValue)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, passthrough)
	r.Config.TCPTLSTermination, r.Config.TLSSNIPassthrough = true, true
	verify(gatewayv1.ListenerReasonProtocolConflict)
	for _, listener := range gateway.Status.Listeners {
		if len(listener.SupportedKinds) != 0 {
			t.Fatalf("conflicting listener %s advertises route kinds %v", listener.Name, listener.SupportedKinds)
		}
	}
}
