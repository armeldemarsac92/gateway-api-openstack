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

package httproute

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func routeBindingFinalizers(cfg Config, gatewayNamespace, gatewayName, gatewayUID string) []string {
	return []string{
		cfg.RouteFinalizer(),
		cfg.RouteBindingFinalizer(cfg.ClusterID, cfg.OpenStackProjectID, gatewayNamespace, gatewayName, gatewayUID),
	}
}

func TestHTTPRouteDeletionUsesStoredGatewayUID(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	now := metav1.Now()
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "api",
			UID:               "route-uid",
			DeletionTimestamp: &now,
			Finalizers:        routeBindingFinalizers(cfg, "default", "old-gateway", "old-gateway-uid"),
			Annotations: map[string]string{
				cfg.RouteLifecycleAnnotation():        controller.RouteDetaching,
				cfg.RouteGatewayNamespaceAnnotation(): "default",
				cfg.RouteGatewayNameAnnotation():      "old-gateway",
				cfg.RouteGatewayUIDAnnotation():       "old-gateway-uid",
			},
		},
	}
	client := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(route).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: client, Coordinator: &graph.Coordinator{}, APIReader: client, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "api"}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0].GatewayUID != "old-gateway-uid" {
		t.Fatalf("deleted route identities = %#v", provider.deletedRoutes)
	}
}

func TestHTTPRouteDeletionRejectsTamperedStoredGatewayIdentity(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	now := metav1.Now()
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "api",
			UID:               "route-uid",
			DeletionTimestamp: &now,
			Finalizers:        routeBindingFinalizers(cfg, "default", "original-gateway", "original-gateway-uid"),
			Annotations: map[string]string{
				cfg.RouteGatewayNamespaceAnnotation(): "default",
				cfg.RouteGatewayNameAnnotation():      "different-gateway",
				cfg.RouteGatewayUIDAnnotation():       "different-gateway-uid",
			},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(route).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: route.Namespace, Name: route.Name}}); err == nil {
		t.Fatal("Reconcile() accepted a tampered stored Gateway identity")
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatalf("DeleteRoute calls = %d, want 0", len(provider.deletedRoutes))
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: route.Namespace, Name: route.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) {
		t.Fatal("tampered binding removed the safety finalizer")
	}
}

func TestRoutesForGatewayIncludesCrossNamespaceReferences(t *testing.T) {
	scheme := testScheme(t)
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "gateway-system", Name: "edge"}}
	parentNamespace := gatewayv1.Namespace(gateway.Namespace)
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "application", Name: "api"},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
			Name: gatewayv1.ObjectName(gateway.Name), Namespace: &parentNamespace,
		}}}},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithObjects(gateway, route).Build()
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Config: testConfig()}, nil)
	requests := reconciler.enqueueHTTPRoutesForGateway(context.Background(), gateway)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: route.Namespace, Name: route.Name}) {
		t.Fatalf("enqueueHTTPRoutesForGateway() = %#v", requests)
	}
}

func TestHTTPRouteReconcileBuildsNodePortMembers(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1,
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
		Status: gatewayv1.GatewayStatus{Conditions: []metav1.Condition{{
			Type:               string(gatewayv1.GatewayConditionProgrammed),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: 1,
			Reason:             string(gatewayv1.GatewayReasonProgrammed),
			LastTransitionTime: metav1.Now(),
		}}},
	}
	topology := testBoundTopology()
	topology.Topology.VIPSecurityGroupIDs = []string{"33333333-3333-3333-3333-333333333333"}
	topology.Topology.BackendSecurityGroupIDs = []string{"44444444-4444-4444-4444-444444444444"}
	if err := controller.BindGatewayTopology(cfg, gateway, topology); err != nil {
		t.Fatal(err)
	}
	pathType := gatewayv1.PathMatchPathPrefix
	pathValue := "/api"
	backendPort := gatewayv1.PortNumber(8080)
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "api", UID: "route-uid", CreationTimestamp: metav1.Now()},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
			Rules:           []gatewayv1.HTTPRouteRule{{Matches: []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Type: &pathType, Value: &pathValue}}}, BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: "backend", Port: &backendPort}}}}}},
		},
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backend"}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: 8080, NodePort: 30080}}}}
	ready := true
	slice := &discoveryv1.EndpointSlice{AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(8080))}}, ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backend-1", Labels: map[string]string{discoveryv1.LabelServiceName: "backend"}}, Endpoints: []discoveryv1.Endpoint{{Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.11"}}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
	node.Spec.ProviderID = "openstack:///55555555-5555-5555-5555-555555555555"
	client := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(class, gateway, route, service, slice, node).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: client, Coordinator: &graph.Coordinator{}, APIReader: client, Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "api"}}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result != (ctrl.Result{Requeue: true}) || len(provider.gatewaySpecs)+len(provider.routeSpecs) != 0 {
		t.Fatalf("binding checkpoint result = %#v, Gateway calls = %d, route calls = %d", result, len(provider.gatewaySpecs), len(provider.routeSpecs))
	}
	secondResult, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	wantResync := controller.OpenStackResyncAfter(cfg.OpenStackResyncInterval, route.UID)
	if secondResult.RequeueAfter != wantResync {
		t.Fatalf("second Reconcile() RequeueAfter = %s, want %s", secondResult.RequeueAfter, wantResync)
	}
	if len(provider.routeSpecs) != 1 {
		var current gatewayv1.HTTPRoute
		_ = client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "api"}, &current)
		t.Fatalf("route specs count = %d, status = %#v", len(provider.routeSpecs), current.Status)
	}
	got := provider.routeSpecs[0]
	wantGatewayRequirements := cloud.GatewayRequirements{
		Provider: "amphora", VIPSubnetID: testVIPSubnetID,
		ListenerPort:        80,
		VIPSecurityGroupIDs: topology.Topology.VIPSecurityGroupIDs,
	}
	if !got.GatewayRequirements.Equal(wantGatewayRequirements) || got.PathType != cloud.PathMatchPrefix || got.PathValue != "/api" ||
		!slices.Equal(got.BackendSecurityGroupIDs, topology.Topology.BackendSecurityGroupIDs) ||
		len(got.Members) != 1 || got.Members[0] != (cloud.Member{Address: "10.0.0.11", Port: 30080, ProviderID: node.Spec.ProviderID}) {
		t.Fatalf("route spec = %#v", got)
	}
	var reconciled gatewayv1.HTTPRoute
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "api"}, &reconciled); err != nil {
		t.Fatal(err)
	}
	if reconciled.Annotations[cfg.RouteGatewayUIDAnnotation()] != "gateway-uid" {
		t.Fatalf("stored Gateway UID = %q", reconciled.Annotations[cfg.RouteGatewayUIDAnnotation()])
	}
	resourceVersion := reconciled.ResourceVersion
	thirdResult, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("third Reconcile() error = %v", err)
	}
	if thirdResult.RequeueAfter != wantResync || len(provider.routeSpecs) != 2 {
		t.Fatalf("third Reconcile() result = %#v, EnsureRoute calls = %d", thirdResult, len(provider.routeSpecs))
	}
	if err := client.Get(context.Background(), request.NamespacedName, &reconciled); err != nil {
		t.Fatal(err)
	}
	if reconciled.ResourceVersion != resourceVersion {
		t.Fatalf("converged resync changed HTTPRoute resourceVersion from %s to %s", resourceVersion, reconciled.ResourceVersion)
	}
}

func TestHTTPRouteProgramsOnlyHTTPListenerOnMixedGateway(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit parent"
		if explicit {
			name = "explicit parent"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			class, gateway := providerRouteParentObjects(cfg)
			http := gateway.Spec.Listeners[0]
			http.Hostname = ptr.To(gatewayv1.Hostname("api.example.test"))
			rejected := gatewayv1.Listener{Name: "aaa-rejected", Port: 8443, Protocol: "custom.example/test"}
			gateway.Spec.Listeners = []gatewayv1.Listener{rejected, http}
			foreign := controller.Condition("example.net/Custom", metav1.ConditionTrue, "Preserve", "Foreign status", gateway.Generation)
			gateway.Status.Listeners = []gatewayv1.ListenerStatus{
				{Name: rejected.Name, Conditions: []metav1.Condition{foreign}},
				{Name: http.Name, Conditions: []metav1.Condition{foreign}},
			}
			parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
			if explicit {
				parent.SectionName, parent.Port = &http.Name, &http.Port
			}
			route := mapperTestHTTPRoute(gateway.Namespace, "api", parent, "backend")
			route.UID, route.Generation = "api-route", 1
			rejectedParent := gatewayv1.ParentReference{Name: parent.Name, SectionName: &rejected.Name}
			if explicit {
				rejectedParent.SectionName, rejectedParent.Port = nil, &rejected.Port
			}
			other := mapperTestHTTPRoute(gateway.Namespace, "aaa-rejected", rejectedParent, "backend")
			other.UID, other.Generation = "rejected-route", 1
			mismatched := mapperTestHTTPRoute(gateway.Namespace, "aaa-mismatched", gatewayv1.ParentReference{
				Name: parent.Name, SectionName: &rejected.Name, Port: &http.Port,
			}, "backend")
			mismatched.UID, mismatched.Generation = "mismatched-route", 1
			service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route, other, mismatched).
				WithObjects(class, gateway, route, other, mismatched, service, endpoints, node).Build()
			provider := &recordingProvider{}
			coordinator := &graph.Coordinator{}
			gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
				Client: kubeClient, APIReader: kubeClient, Coordinator: coordinator, Config: cfg,
			}, provider)
			reconciler := &Reconciler{
				Client: kubeClient, APIReader: kubeClient, Coordinator: coordinator, Writer: gatewayReconciler.Writer, Config: cfg,
			}
			gatewayRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
			if _, err := gatewayReconciler.Reconcile(ctx, gatewayRequest); err != nil {
				t.Fatal(err)
			}
			if len(provider.gatewaySpecs) != 1 || provider.gatewaySpecs[0].ListenerPort != int(http.Port) {
				t.Fatalf("provider Gateway listeners = %#v, want only %s", provider.gatewaySpecs, http.Name)
			}
			for _, candidate := range []*gatewayv1.HTTPRoute{other, mismatched, route, route} {
				if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(candidate)}); err != nil {
					t.Fatal(err)
				}
			}
			if len(provider.routeSpecs) != 1 {
				t.Fatalf("provider routes = %#v, want one programmed HTTPRoute", provider.routeSpecs)
			}
			spec := provider.routeSpecs[0]
			if spec.Identity.RouteUID != string(route.UID) || spec.GatewayRequirements.ListenerPort != int(http.Port) ||
				spec.Hostname != string(*http.Hostname) || len(spec.Members) != 1 {
				t.Fatalf("route graph = %#v, want HTTPRoute only on %s:%d with backend members", spec, *http.Hostname, http.Port)
			}
			for _, rejectedRoute := range []struct {
				route  *gatewayv1.HTTPRoute
				reason gatewayv1.RouteConditionReason
			}{
				{other, gatewayv1.RouteReasonNotAllowedByListeners},
				{mismatched, gatewayv1.RouteReasonNoMatchingParent},
			} {
				route := rejectedRoute.route
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
					t.Fatal(err)
				}
				if len(route.Status.Parents) != 1 {
					t.Fatalf("route %s parent statuses = %#v", route.Name, route.Status.Parents)
				}
				accepted := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(rejectedRoute.reason) {
					t.Fatalf("route %s Accepted = %#v, want False/%s", route.Name, accepted, rejectedRoute.reason)
				}
			}
			if _, err := gatewayReconciler.Reconcile(ctx, gatewayRequest); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Get(ctx, gatewayRequest.NamespacedName, gateway); err != nil {
				t.Fatal(err)
			}
			listeners := gateway.Status.Listeners
			if len(listeners) != 2 || listeners[0].Name != rejected.Name || listeners[0].AttachedRoutes != 0 || len(listeners[0].SupportedKinds) != 0 ||
				listeners[1].Name != http.Name || listeners[1].AttachedRoutes != 1 || !meta.IsStatusConditionTrue(listeners[1].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
				t.Fatalf("listener statuses = %#v", listeners)
			}
			for _, status := range listeners {
				if !meta.IsStatusConditionTrue(status.Conditions, foreign.Type) {
					t.Fatalf("missing foreign condition in listener status %#v", status)
				}
			}
		})
	}
}

// Matches the two HTTP sections and explicit parent selector in the upstream
// GatewayWithAttachedRoutesWithPort8080 case.
func TestGatewayWithAttachedRoutesWithPort8080(t *testing.T) {
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	selected := gatewayv1.Listener{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}
	other := gatewayv1.Listener{Name: "http-unattached", Port: 8080, Protocol: gatewayv1.HTTPProtocolType}
	gateway.Spec.Listeners = []gatewayv1.Listener{other, selected}
	route := mapperTestHTTPRoute(gateway.Namespace, "attached", gatewayv1.ParentReference{
		Name: gatewayv1.ObjectName(gateway.Name), SectionName: &selected.Name,
	}, "backend")
	route.UID, route.Generation = "attached-uid", 1
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).
		WithObjects(class, gateway, route, service, endpoints, node).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", ListenerID: "listener-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 8080}
	provider := &recordingProvider{gatewayResult: &ready}
	coordinator := &graph.Coordinator{}
	gr := newTestGatewayReconciler(gatewaycontroller.Reconciler{Client: kube, APIReader: kube, Coordinator: coordinator, Config: cfg}, provider)
	r := &Reconciler{Client: kube, APIReader: kube, Writer: gr.Writer, Coordinator: coordinator, Config: cfg}
	gatewayReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	routeReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	for range 2 {
		if _, err := gr.Reconcile(t.Context(), gatewayReq); err != nil {
			t.Fatal(err)
		}
	}
	if len(provider.gatewaySpecs) != 1 || len(provider.gatewaySpecs[0].ScopedListeners) != 2 {
		t.Fatalf("expected two physical listener ports: %#v", provider.gatewaySpecs)
	}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), routeReq); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := gr.Reconcile(t.Context(), gatewayReq); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), gatewayReq.NamespacedName, gateway); err != nil {
		t.Fatal(err)
	}
	for name, count := range map[gatewayv1.SectionName]int32{"http": 1, "http-unattached": 0} {
		i := slices.IndexFunc(gateway.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == name })
		if i < 0 {
			t.Fatalf("missing section %s: %#v", name, gateway.Status.Listeners)
		}
		status := gateway.Status.Listeners[i]
		if status.AttachedRoutes != count || len(status.SupportedKinds) != 1 || status.SupportedKinds[0].Kind != "HTTPRoute" ||
			!meta.IsStatusConditionTrue(status.Conditions, string(gatewayv1.ListenerConditionAccepted)) ||
			!meta.IsStatusConditionTrue(status.Conditions, string(gatewayv1.ListenerConditionResolvedRefs)) {
			t.Fatalf("section %s should accept HTTPRoute and report %d attachment(s): %#v", name, count, status)
		}
	}
	if len(provider.routeSpecs) == 0 || !slices.Equal(provider.routeSpecs[len(provider.routeSpecs)-1].ScopedListenerPorts, []int{80}) {
		t.Fatalf("HTTPRoute must be assigned only to port 80: %#v", provider.routeSpecs)
	}
}

func TestHTTPRouteParentTracksAddedAndRemovedPorts(t *testing.T) {
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	http := gateway.Spec.Listeners[0]
	http.Hostname = ptr.To(gatewayv1.Hostname("http.example.test"))
	other := gatewayv1.Listener{Name: "other", Port: 8080, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("other.example.test"))}
	blocked := gatewayv1.Listener{Name: "blocked", Port: 8081, Protocol: gatewayv1.HTTPProtocolType, Hostname: other.Hostname,
		AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{From: ptr.To(gatewayv1.NamespacesFromSelector),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"routes": "allowed"}}}}}
	unmatched := gatewayv1.Listener{Name: "unmatched", Port: 8082, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("unmatched.other.test"))}
	both := []gatewayv1.Listener{http, other, blocked, unmatched}
	gateway.Spec.Listeners = both
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	route := mapperTestHTTPRoute(gateway.Namespace, "attached", parent, "backend")
	route.UID, route.Generation = "attached-uid", 1
	route.Spec.Hostnames = []gatewayv1.Hostname{"*.example.test"}
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).
		WithObjects(class, gateway, route, service, endpoints, node).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", ListenerID: "listener-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 8080, 8081, 8082}
	provider := &recordingProvider{gatewayResult: &ready}
	coordinator := &graph.Coordinator{}
	gr := newTestGatewayReconciler(gatewaycontroller.Reconciler{Client: kube, APIReader: kube, Coordinator: coordinator, Config: cfg}, provider)
	r := &Reconciler{Client: kube, APIReader: kube, Writer: gr.Writer, Coordinator: coordinator, Config: cfg}

	ctx := t.Context()
	gatewayReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	routeReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	check := func(sections []gatewayv1.Listener, programmed bool, wantPorts []int, attached map[gatewayv1.SectionName]int32) {
		t.Helper()
		if err := kube.Get(ctx, gatewayReq.NamespacedName, gateway); err != nil {
			t.Fatal(err)
		}
		if !slices.EqualFunc(gateway.Spec.Listeners, sections, func(a, b gatewayv1.Listener) bool { return a.Name == b.Name }) {
			gateway.Spec.Listeners = sections
			gateway.Generation++
			if err := kube.Update(ctx, gateway); err != nil {
				t.Fatal(err)
			}
		}
		for attempt := range 2 {
			if _, err := gr.Reconcile(ctx, gatewayReq); err != nil {
				t.Fatalf("Gateway reconciliation %d: %v", attempt, err)
			}
		}
		for attempt := range 2 {
			if _, err := r.Reconcile(ctx, routeReq); err != nil {
				t.Fatalf("HTTPRoute reconciliation %d: %v", attempt, err)
			}
		}
		if _, err := gr.Reconcile(ctx, gatewayReq); err != nil {
			t.Fatalf("Gateway reconciliation after route update: %v", err)
		}
		if err := kube.Get(ctx, routeReq.NamespacedName, route); err != nil {
			t.Fatal(err)
		}
		if len(route.Status.Parents) != 1 {
			t.Fatalf("expected one status for the original parentRef, got %#v", route.Status.Parents)
		}
		assertElectionParentAccepted(t, route, parent, metav1.ConditionTrue)
		hasProgrammed := meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed")
		if hasProgrammed != programmed {
			t.Fatalf("expected Programmed=%t, got %#v", programmed, route.Status.Parents[0].Conditions)
		}
		if err := kube.Get(ctx, gatewayReq.NamespacedName, gateway); err != nil {
			t.Fatal(err)
		}
		for name, count := range attached {
			i := slices.IndexFunc(gateway.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == name })
			if i < 0 || gateway.Status.Listeners[i].AttachedRoutes != count {
				t.Fatalf("expected %d attachment(s) on %q, got %#v", count, name, gateway.Status.Listeners)
			}
		}
		if len(provider.routeSpecs) == 0 {
			t.Fatal("controller provided no route graph")
		}
		spec := provider.routeSpecs[len(provider.routeSpecs)-1]
		var ports []int
		for port := range spec.HTTPPortSpecs() {
			if len(port.ScopedListenerPorts) != 1 {
				t.Fatalf("expected one physical port per compiled graph, got %v", port.ScopedListenerPorts)
			}
			number := port.ScopedListenerPorts[0]
			ports = append(ports, number)
			wantHost := map[int]string{80: string(*http.Hostname), 8080: string(*other.Hostname)}
			if port.Hostname != wantHost[number] {
				t.Fatalf("graph for port %d has hostname %q, want %q", number, port.Hostname, wantHost[number])
			}
		}
		if !slices.Equal(ports, wantPorts) {
			t.Fatalf("compiled listener ports = %v, want %v", ports, wantPorts)
		}
	}

	pending := cloud.RouteProgressingResult("Second port's policy is still pending", time.Second)
	provider.routeResult = &pending
	counts := map[gatewayv1.SectionName]int32{http.Name: 1, other.Name: 1, blocked.Name: 0, unmatched.Name: 0}
	check(both, false, []int{80, 8080}, counts)
	provider.routeResult = nil
	check(both, true, []int{80, 8080}, counts)
	check([]gatewayv1.Listener{http, blocked, unmatched}, true, []int{80}, map[gatewayv1.SectionName]int32{http.Name: 1, blocked.Name: 0, unmatched.Name: 0})
	check(both, true, []int{80, 8080}, counts)

	route.Spec.ParentRefs[0].Port = &other.Port
	route.Spec.ParentRefs[0].SectionName = &other.Name
	route.Generation++
	if err := kube.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	parent = route.Spec.ParentRefs[0]
	check(both, true, []int{8080}, map[gatewayv1.SectionName]int32{http.Name: 0, other.Name: 1, blocked.Name: 0, unmatched.Name: 0})
}

type routeTestCertificate struct {
	authorized bool
}

func (c *routeTestCertificate) Resolve(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	if c.authorized {
		return "https://barbican.example/v1/secrets/approved", ""
	}
	return "", gatewayv1.ListenerReasonInvalidCertificateRef
}

func (c *routeTestCertificate) ReferenceStatus(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	_, reason := c.Resolve(ctx, gateway, listener)
	return reason
}

func TestHTTPRouteBindsWithOneSelectedListenerReady(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS = true
	class, gateway := providerRouteParentObjects(cfg)
	gateway.Status = gatewayv1.GatewayStatus{}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
		Name: "https", Port: 443, Protocol: gatewayv1.HTTPSProtocolType,
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "certificate"}}},
	})
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	route := mapperTestHTTPRoute(gateway.Namespace, "attached", parent, "backend")
	route.UID, route.Generation = "attached-uid", 1
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).
		WithObjects(class, gateway, route, service, endpoints, node).Build()
	certificate := &routeTestCertificate{}
	gatewayResult := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", ListenerID: "listener-1", VIPAddress: "192.0.2.10"})
	gatewayResult.ScopedListenerReadyPorts = []int{80}
	routeResult := cloud.RouteProgressingResult("Waiting for the HTTPS listener", time.Second)
	provider := &recordingProvider{gatewayResult: &gatewayResult, routeResult: &routeResult}
	coordinator := &graph.Coordinator{}
	gr := newTestGatewayReconciler(gatewaycontroller.Reconciler{Client: kube, APIReader: kube, Coordinator: coordinator, Config: cfg, Certificates: certificate}, provider)
	r := &Reconciler{Client: kube, APIReader: kube, Writer: gr.Writer, Coordinator: coordinator, Config: cfg}

	check := func(programmed bool) {
		t.Helper()
		for range 2 {
			if _, err := gr.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(gateway), gateway); err != nil {
			t.Fatal(err)
		}
		if !programmed && (!listenerProgrammedForGeneration(gateway, "http") || listenerProgrammedForGeneration(gateway, "https")) {
			t.Fatalf("expected HTTP available with HTTPS pending, got %#v", gateway.Status.Listeners)
		}
		for range 2 {
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
			t.Fatal(err)
		}
		if len(provider.routeSpecs) == 0 || !controllerutil.ContainsFinalizer(route, cfg.RouteFinalizer()) {
			t.Fatal("an unavailable HTTPS listener prevented binding and programming the HTTP port")
		}
		last := provider.routeSpecs[len(provider.routeSpecs)-1]
		if !slices.Equal(last.ScopedListenerPorts, []int{80}) || len(last.AdditionalHTTPPorts) != 1 ||
			!slices.Equal(last.AdditionalHTTPPorts[0].ScopedListenerPorts, []int{443}) {
			t.Fatalf("expected the provider to reconcile both selected ports, got %+v", last)
		}
		status := metav1.ConditionFalse
		if programmed {
			status = metav1.ConditionTrue
		}
		if len(route.Status.Parents) != 1 || !meta.IsStatusConditionPresentAndEqual(route.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed", status) {
			t.Fatalf("expected one parent with Programmed=%s, got %#v", status, route.Status.Parents)
		}
		assertElectionParentAccepted(t, route, parent, metav1.ConditionTrue)
	}
	check(false)
	certificate.authorized = true
	gatewayResult.ScopedListenerReadyPorts = []int{80, 443}
	routeResult = cloud.RouteReadyResult(cloud.RouteState{})
	check(true)
}

func TestExactHTTPSectionsShareOneGatewayListener(t *testing.T) {
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	first := &gateway.Spec.Listeners[0]
	first.Hostname = ptr.To(gatewayv1.Hostname("bar.com"))
	second := *first.DeepCopy()
	second.Name = "foo"
	second.Hostname = ptr.To(gatewayv1.Hostname("foo.com"))

	bar := mapperTestHTTPRoute(gateway.Namespace, "bar", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "bar-service")
	bar.UID, bar.Generation = "bar-uid", 1
	bar.Spec.Hostnames = []gatewayv1.Hostname{"bar.com"}
	foo := mapperTestHTTPRoute(gateway.Namespace, "foo", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &second.Name, Port: &second.Port}, "foo-service")
	foo.UID, foo.Generation = "foo-uid", 1
	portRoute := mapperTestHTTPRoute(gateway.Namespace, "by-port", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Port: &second.Port}, "bar-service")
	portRoute.UID, portRoute.Generation = "by-port-uid", 1
	portRoute.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{
		Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/by-port")},
	}}
	wrongPort := mapperTestHTTPRoute(gateway.Namespace, "wrong-port", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &second.Name, Port: ptr.To(gatewayv1.PortNumber(8443))}, "foo-service")
	wrongPort.UID, wrongPort.Generation = "wrong-port-uid", 1
	wrongNamespace := mapperTestHTTPRoute("tenant", "wrong-namespace", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace)), SectionName: &second.Name}, "foo-service")
	wrongNamespace.UID, wrongNamespace.Generation = "wrong-namespace-uid", 1
	barService, barEndpoints, node := validNodePortBackend(gateway.Namespace, "bar-service")
	fooService, fooEndpoints, _ := validNodePortBackend(gateway.Namespace, "foo-service")
	fooService.Spec.Ports[0].NodePort = 30081

	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, foo, bar, portRoute, wrongPort, wrongNamespace).
		WithObjects(class, gateway, foo, bar, portRoute, wrongPort, wrongNamespace, barService, barEndpoints, fooService, fooEndpoints, node).Build()
	coordinator, provider := &graph.Coordinator{}, &recordingProvider{}
	gr := newTestGatewayReconciler(gatewaycontroller.Reconciler{Client: kube, APIReader: kube, Coordinator: coordinator, Config: cfg}, provider)
	r := &Reconciler{Client: kube, APIReader: kube, Writer: gr.Writer, Coordinator: coordinator, Config: cfg}
	reconcileGateway := func() {
		t.Helper()
		if _, err := gr.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(gateway), gateway); err != nil {
			t.Fatal(err)
		}
	}
	reconcileRoute := func(route *gatewayv1.HTTPRoute) {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
			t.Fatal(err)
		}
	}
	editSections := func(listeners ...gatewayv1.Listener) {
		t.Helper()
		gateway.Spec.Listeners = listeners
		gateway.Generation++
		if err := kube.Update(t.Context(), gateway); err != nil {
			t.Fatal(err)
		}
		reconcileGateway()
	}
	checkSections := func(count int32, names ...gatewayv1.SectionName) {
		t.Helper()
		if !gatewayProgrammedForGeneration(gateway) || len(gateway.Status.Listeners) != len(names) {
			t.Fatalf("Gateway generation %d did not program %v: %#v", gateway.Generation, names, gateway.Status)
		}
		for i, listener := range gateway.Status.Listeners {
			programmed := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionProgrammed))
			if listener.Name != names[i] || listener.AttachedRoutes != count || programmed == nil || programmed.Status != metav1.ConditionTrue || programmed.ObservedGeneration != gateway.Generation {
				t.Fatalf("incorrect status for section %q: %#v", names[i], listener)
			}
		}
	}

	reconcileGateway()
	reconcileRoute(bar)
	reconcileRoute(bar)
	reconcileGateway()
	savedBarBinding := bar.Annotations[cfg.RouteGatewayUIDAnnotation()]
	listenerBinding := gateway.Annotations[cfg.GatewayListenerPortAnnotation()]
	checkSections(1, first.Name)

	editSections(second, *first)
	reconcileRoute(foo)
	reconcileRoute(foo)
	if len(foo.Status.Parents) != 1 || !meta.IsStatusConditionTrue(foo.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) {
		t.Fatalf("foo route did not attach: %#v", foo.Status.Parents)
	}
	for _, test := range []struct {
		route  *gatewayv1.HTTPRoute
		reason gatewayv1.RouteConditionReason
	}{
		{wrongPort, gatewayv1.RouteReasonNoMatchingParent},
		{wrongNamespace, gatewayv1.RouteReasonNotAllowedByListeners},
	} {
		reconcileRoute(test.route)
		if len(test.route.Status.Parents) != 1 {
			t.Fatalf("route %q did not report its parent: %#v", test.route.Name, test.route.Status.Parents)
		}
		accepted := meta.FindStatusCondition(test.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
		if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(test.reason) || len(test.route.Finalizers) != 0 {
			t.Fatalf("route %q did not reject attachment: %#v", test.route.Name, test.route.Status)
		}
	}
	reconcileGateway()
	checkSections(1, second.Name, first.Name)
	if gateway.Annotations[cfg.GatewayListenerPortAnnotation()] != listenerBinding || bar.Annotations[cfg.RouteGatewayUIDAnnotation()] != savedBarBinding {
		t.Fatal("adding a same-port section replaced a stored binding")
	}
	byHost := map[string]cloud.RouteSpec{}
	for _, spec := range provider.routeSpecs {
		byHost[spec.Hostname] = spec
	}
	if len(byHost) != 2 || byHost["bar.com"].Identity.RouteUID != string(bar.UID) || byHost["foo.com"].Identity.RouteUID != string(foo.UID) ||
		len(byHost["bar.com"].Members) != 1 || byHost["bar.com"].Members[0].Port != 30080 ||
		len(byHost["foo.com"].Members) != 1 || byHost["foo.com"].Members[0].Port != 30081 ||
		byHost["bar.com"].Precedence == byHost["foo.com"].Precedence {
		t.Fatalf("incorrect forwarding or precedence across shared sections: %#v", byHost)
	}

	reconcileRoute(portRoute)
	reconcileRoute(portRoute)
	if len(portRoute.Status.Parents) != 1 || !meta.IsStatusConditionTrue(portRoute.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) {
		t.Fatalf("route to both sections was not accepted: %#v", portRoute.Status)
	}
	var hostMatches []string
	for i := len(provider.routeSpecs) - 1; i >= 0; i-- {
		spec := provider.routeSpecs[i]
		if spec.Identity.RouteUID != string(portRoute.UID) {
			continue
		}
		if len(spec.HTTPBackends) != 1 || len(spec.HTTPBackends[0].Members) != 1 {
			t.Fatalf("expected one Service for this port: %#v", spec)
		}
		for _, match := range spec.HTTPBackends[0].Matches {
			if match.HTTPSListenerHostname != "" || match.PathValue != "/by-port" {
				t.Fatalf("unexpected plain HTTP match: %#v", match)
			}
			hostMatches = append(hostMatches, match.Hostname)
		}
		break
	}
	slices.Sort(hostMatches)
	if !slices.Equal(hostMatches, []string{"bar.com", "foo.com"}) {
		t.Fatalf("forwarding hosts on the selected port = %v", hostMatches)
	}
	reconcileGateway()
	checkSections(2, second.Name, first.Name)

	second.Hostname = ptr.To(gatewayv1.Hostname("new.foo.com"))
	preEditGraph := len(provider.routeSpecs)
	editSections(second, *first)
	reconcileRoute(foo)
	var fooHosts []string
	for _, spec := range provider.routeSpecs[preEditGraph:] {
		if spec.Identity.RouteUID == string(foo.UID) {
			fooHosts = append(fooHosts, spec.Hostname)
		}
	}
	if len(fooHosts) == 0 || slices.ContainsFunc(fooHosts, func(host string) bool { return host != "new.foo.com" }) {
		t.Fatalf("hostname change did not rewrite the bound route: %v", fooHosts)
	}

	editSections(*first)
	reconcileRoute(portRoute)
	reconcileRoute(foo)
	reconcileRoute(foo)
	reconcileRoute(foo)
	reconcileGateway()
	checkSections(2, first.Name)
	if len(foo.Finalizers) != 0 || len(provider.deletedGateways) != 0 || gateway.Annotations[cfg.GatewayListenerPortAnnotation()] != listenerBinding {
		t.Fatalf("removing the section did not release its route and retain the physical binding: finalizers %v, annotations %v", foo.Finalizers, gateway.Annotations)
	}
}

func TestHTTPRouteParentRefSelectorsMustMatchTheSameListener(t *testing.T) {
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	first := gateway.Spec.Listeners[0]
	second := gatewayv1.Listener{Name: "second", Port: 8081, Protocol: gatewayv1.HTTPProtocolType}
	gateway.Spec.Listeners = []gatewayv1.Listener{first, second}
	parent := gatewayv1.ParentReference{
		Name: gatewayv1.ObjectName(gateway.Name), SectionName: &first.Name, Port: &second.Port,
	}
	route := mapperTestHTTPRoute(gateway.Namespace, "api", parent, "missing-service")
	route.UID, route.Generation = "route-uid", 2
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).
		WithObjects(class, gateway, route).Build()
	provider := &recordingProvider{}
	r := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
	key := client.ObjectKeyFromObject(route)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(t.Context(), key, route); err != nil {
		t.Fatal(err)
	}
	parents := route.Status.Parents
	if len(parents) != 1 || parents[0].ControllerName != cfg.ControllerName ||
		!controller.ParentRefsEqual(parents[0].ParentRef, route.Namespace, parent, route.Namespace) {
		t.Fatalf("parent statuses = %#v, want the controller's status for %#v", parents, parent)
	}
	conditions := parents[0].Conditions
	accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNoMatchingParent) || accepted.ObservedGeneration != route.Generation {
		t.Fatalf("Accepted = %#v, want False/NoMatchingParent at generation %d", accepted, route.Generation)
	}
	resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	if resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonBackendNotFound) || resolved.ObservedGeneration != route.Generation {
		t.Fatalf("ResolvedRefs = %#v, want False/BackendNotFound at generation %d", resolved, route.Generation)
	}
	if len(provider.routeSpecs) != 0 || len(route.Finalizers) != 0 {
		t.Fatalf("rejected route got forwarding state: routes=%#v, finalizers=%v", provider.routeSpecs, route.Finalizers)
	}
}

func TestHTTPRouteResolvesBackendReferencesWhenAttachmentIsDenied(t *testing.T) {
	for _, test := range []struct {
		name     string
		port     int32
		remote   bool
		grant    bool
		disjoint bool
		tlsOnly  bool
		group    gatewayv1.Group
		kind     gatewayv1.Kind
		resolved metav1.ConditionStatus
		reason   gatewayv1.RouteConditionReason
	}{
		{name: "TLS-only listener", port: 8080, tlsOnly: true, resolved: metav1.ConditionTrue, reason: gatewayv1.RouteReasonResolvedRefs},
		{name: "ClusterIP Service", port: 8080, resolved: metav1.ConditionTrue, reason: gatewayv1.RouteReasonResolvedRefs},
		{name: "disjoint hostname", port: 8080, disjoint: true, resolved: metav1.ConditionTrue, reason: gatewayv1.RouteReasonResolvedRefs},
		{name: "missing Service", resolved: metav1.ConditionFalse, reason: gatewayv1.RouteReasonBackendNotFound},
		{name: "missing port", port: 80, resolved: metav1.ConditionFalse, reason: gatewayv1.RouteReasonBackendNotFound},
		{name: "ungranted remote Service", port: 8080, remote: true, resolved: metav1.ConditionFalse, reason: gatewayv1.RouteReasonRefNotPermitted},
		{name: "granted remote Service", port: 8080, remote: true, grant: true, resolved: metav1.ConditionTrue, reason: gatewayv1.RouteReasonResolvedRefs},
		{name: "unsupported backend kind", port: 8080, remote: true, kind: "Pod", resolved: metav1.ConditionFalse, reason: gatewayv1.RouteReasonInvalidKind},
		{name: "unsupported backend group", port: 8080, remote: true, group: "example.test", resolved: metav1.ConditionFalse, reason: gatewayv1.RouteReasonInvalidKind},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			class, gateway := providerRouteParentObjects(cfg)
			if test.tlsOnly {
				gateway.Spec.Listeners = []gatewayv1.Listener{{
					Name: "tls", Protocol: gatewayv1.TLSProtocolType, Port: 443,
					TLS:           &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModePassthrough)},
					AllowedRoutes: &gatewayv1.AllowedRoutes{Kinds: []gatewayv1.RouteGroupKind{{Kind: "TLSRoute"}}},
				}}
				cfg.TLSSNIPassthrough = true
			}
			parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))}
			route := mapperTestHTTPRoute("tenant", "api", parent, "backend")
			route.UID, route.Generation = "route-uid", 1
			if test.disjoint {
				listener := &gateway.Spec.Listeners[0]
				listener.Hostname = ptr.To(gatewayv1.Hostname("gateway.example.test"))
				route.Spec.Hostnames = []gatewayv1.Hostname{"route.example.test"}
				_, rejection := (&Reconciler{Config: cfg}).selectRouteAttachments(route, managedRouteParent{ref: parent, gateway: gateway}, []*gatewayv1.Listener{listener}, nil)
				status := statusForRouteBuildError(rejection)
				if status.acceptedReason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
					t.Fatalf("bound route attachment reason = %s, want NotAllowedByListeners", status.acceptedReason)
				}
			}
			backend := &route.Spec.Rules[0].BackendRefs[0]
			backend.Port = ptr.To(gatewayv1.PortNumber(8080))
			if test.group != "" {
				backend.Group = &test.group
			}
			if test.kind != "" {
				backend.Kind = &test.kind
			}
			foreign := gatewayv1.RouteParentStatus{ParentRef: parent, ControllerName: "example.test/other"}
			route.Status.Parents = []gatewayv1.RouteParentStatus{foreign}
			serviceNamespace := route.Namespace
			if test.remote {
				serviceNamespace = "shared"
				backend.Namespace = ptr.To(gatewayv1.Namespace(serviceNamespace))
			}
			objects := []client.Object{class, gateway, route}
			if test.port != 0 {
				objects = append(objects, &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Namespace: serviceNamespace, Name: "backend"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: test.port}}},
				})
			}
			if test.grant {
				objects = append(objects, serviceReferenceGrant(serviceNamespace, route.Namespace, "backend"))
			}
			baseClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(objects...).Build()
			kubeClient := interceptor.NewClient(baseClient, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, service := obj.(*corev1.Service); service && test.remote && !test.grant {
					t.Fatal("read a remote Service without a ReferenceGrant")
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
			key := client.ObjectKeyFromObject(route)
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Get(ctx, key, route); err != nil {
				t.Fatal(err)
			}
			parents := route.Status.Parents
			if len(parents) != 2 || parents[0].ControllerName != foreign.ControllerName || parents[1].ControllerName != cfg.ControllerName {
				t.Fatalf("expected foreign and managed parents, got %#v", parents)
			}
			conditions := parents[1].Conditions
			accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
			if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
				t.Fatalf("Accepted = %#v, want False/NotAllowedByListeners", accepted)
			}
			resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
			if resolved == nil || resolved.Status != test.resolved || resolved.Reason != string(test.reason) || resolved.ObservedGeneration != route.Generation {
				t.Fatalf("ResolvedRefs = %#v, want %s/%s at generation %d", resolved, test.resolved, test.reason, route.Generation)
			}
			if len(provider.routeSpecs) != 0 || len(route.Finalizers) != 0 || !meta.IsStatusConditionFalse(conditions, cfg.Domain()+"/Programmed") {
				t.Fatalf("denied route got forwarding state: routes=%#v, finalizers=%v, conditions=%v", provider.routeSpecs, route.Finalizers, conditions)
			}
		})
	}
}

func TestHTTPRoutePathLimitsPreserveHealthyPeer(t *testing.T) {
	for _, test := range []struct {
		pathType gatewayv1.PathMatchType
		maxLen   int
	}{
		{gatewayv1.PathMatchExact, 255},
		{gatewayv1.PathMatchPathPrefix, 254},
	} {
		t.Run(string(test.pathType), func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			class, gateway := providerRouteParentObjects(cfg)
			parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
			makeRoute := func(name, path string) *gatewayv1.HTTPRoute {
				route := mapperTestHTTPRoute(gateway.Namespace, name, parent, "backend")
				route.UID, route.Generation = types.UID(name), 1
				route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{
					Path: &gatewayv1.HTTPPathMatch{Type: &test.pathType, Value: &path},
				}}
				return route
			}
			validPath := "/" + strings.Repeat("a", test.maxLen-1)
			longRoute, validRoute := makeRoute("too-long", validPath+"a"), makeRoute("at-limit", validPath)
			service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithStatusSubresource(longRoute, validRoute).
				WithObjects(class, gateway, longRoute, validRoute, service, endpoints, node).Build()
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
			for _, candidate := range []*gatewayv1.HTTPRoute{longRoute, validRoute, validRoute} {
				if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(candidate)}); err != nil {
					t.Fatalf("Reconcile(%s): %v", candidate.Name, err)
				}
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(longRoute), longRoute); err != nil {
				t.Fatal(err)
			}
			if len(longRoute.Finalizers) != 0 || routeHasBindingAnnotations(cfg, longRoute) || len(longRoute.Status.Parents) != 1 {
				t.Fatalf("expected the overlong route to be rejected before binding, got finalizers=%v annotations=%v parents=%v", longRoute.Finalizers, longRoute.Annotations, longRoute.Status.Parents)
			}
			accepted := meta.FindStatusCondition(longRoute.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
			if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
				t.Fatalf("overlong route Accepted = %#v, want False/UnsupportedValue", accepted)
			}
			if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Identity.RouteName != validRoute.Name || provider.routeSpecs[0].PathValue != validPath {
				t.Fatalf("expected only the route at the supported limit to reach the provider, got %#v", provider.routeSpecs)
			}
		})
	}
}

func TestHTTPRouteDetachDeletesStoredResourcesAndMetadata(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "api",
			UID:        "route-uid",
			Finalizers: routeBindingFinalizers(cfg, "default", "old-gateway", "old-gateway-uid"),
			Annotations: map[string]string{
				cfg.RouteGatewayNamespaceAnnotation(): "default",
				cfg.RouteGatewayNameAnnotation():      "old-gateway",
				cfg.RouteGatewayUIDAnnotation():       "old-gateway-uid",
			},
		},
		Status: gatewayv1.HTTPRouteStatus{RouteStatus: gatewayv1.RouteStatus{Parents: []gatewayv1.RouteParentStatus{{
			ParentRef:      gatewayv1.ParentReference{Name: "old-gateway"},
			ControllerName: cfg.ControllerName,
			Conditions: []metav1.Condition{{
				Type:               string(gatewayv1.RouteConditionAccepted),
				Status:             metav1.ConditionTrue,
				Reason:             string(gatewayv1.RouteReasonAccepted),
				LastTransitionTime: metav1.Now(),
			}},
		}}}},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(route).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: route.Namespace, Name: route.Name}}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	} else if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("first Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatalf("DeleteRoute calls before status checkpoint = %d, want 0", len(provider.deletedRoutes))
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatal("cloud deletion preceded the Detaching checkpoint")
	}
	checkpoint := &gatewayv1.HTTPRoute{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.Annotations[cfg.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("lifecycle=%q", checkpoint.Annotations[cfg.RouteLifecycleAnnotation()])
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("third Reconcile() error=%v", err)
	}
	if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0].GatewayUID != "old-gateway-uid" {
		t.Fatalf("deleted route identities = %#v", provider.deletedRoutes)
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: route.Namespace, Name: route.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 0 || got.Annotations[cfg.RouteGatewayUIDAnnotation()] != "" {
		t.Fatalf("detached route metadata = finalizers %#v, annotations %#v", got.Finalizers, got.Annotations)
	}
	if len(got.Status.Parents) != 0 {
		t.Fatalf("detached route status parents = %#v", got.Status.Parents)
	}
}

func TestHTTPRouteParentChangeDetachesOldGatewayWhileNewGatewayPending(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	newGateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "new-gateway", UID: "new-gateway-uid", Generation: 1,
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "api", UID: "route-uid",
			Finalizers: routeBindingFinalizers(cfg, "default", "old-gateway", "old-gateway-uid"),
			Annotations: map[string]string{
				cfg.RouteGatewayNamespaceAnnotation(): "default",
				cfg.RouteGatewayNameAnnotation():      "old-gateway",
				cfg.RouteGatewayUIDAnnotation():       "old-gateway-uid",
			},
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "new-gateway"}}},
			Rules:           []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: "backend", Port: portNumberPointer(80)}}}}}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(class, newGateway, route).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: route.Namespace, Name: route.Name}}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	} else if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("first Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatalf("DeleteRoute calls before status checkpoint = %d, want 0", len(provider.deletedRoutes))
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatal("cloud deletion preceded the Detaching checkpoint")
	}
	checkpoint := &gatewayv1.HTTPRoute{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.Annotations[cfg.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("lifecycle=%q", checkpoint.Annotations[cfg.RouteLifecycleAnnotation()])
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("third Reconcile() error=%v", err)
	}
	if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0].GatewayUID != "old-gateway-uid" {
		t.Fatalf("deleted route identities = %#v", provider.deletedRoutes)
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: route.Namespace, Name: route.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) || got.Annotations[cfg.RouteGatewayUIDAnnotation()] != "" {
		t.Fatalf("pending route retained old binding: finalizers %#v, annotations %#v", got.Finalizers, got.Annotations)
	}
}

func TestHTTPRouteMissingBackendSetsResolvedRefsWhileGatewayPending(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := programmedTestGateway(cfg)
	gateway.Status = gatewayv1.GatewayStatus{}
	backendPort := gatewayv1.PortNumber(8080)
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "api", UID: "route-uid"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
			Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{Name: "missing", Port: &backendPort},
			}}}}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(route).WithObjects(class, gateway, route).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: route.Namespace, Name: route.Name}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: route.Namespace, Name: route.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Parents) != 1 {
		t.Fatalf("status parents = %#v", got.Status.Parents)
	}
	resolved := meta.FindStatusCondition(got.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	if resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonBackendNotFound) {
		t.Fatalf("ResolvedRefs condition = %#v", resolved)
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls = %d, want 0", len(provider.routeSpecs))
	}
}

func TestHTTPRouteMultipleDeclaredHostnamesPreservePrecedenceAndDetach(t *testing.T) {
	ctx := t.Context()
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("api.example.com"))
	parent := gatewayv1.ParentReference{Name: "edge"}
	route := mapperTestHTTPRoute(gateway.Namespace, "api", parent, "backend")
	route.UID, route.Generation = "api-route", 1
	route.Spec.Hostnames = []gatewayv1.Hostname{"*.example.com", "api.example.com", "ignored.example.net"}
	route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{
		Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/api"),
	}}}
	sibling := mapperTestHTTPRoute(gateway.Namespace, "sibling", parent, "backend")
	sibling.UID, sibling.Generation = "sibling-route", 1
	sibling.Spec.Hostnames = []gatewayv1.Hostname{"*.example.com"}
	sibling.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{
		Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/api"),
	}}}
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(class, gateway, route, sibling).
		WithObjects(class, gateway, route, sibling, service, endpoints, node).Build()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	reconcile := func(route *gatewayv1.HTTPRoute) {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(sibling)
	reconcile(sibling)
	reconcile(route)
	reconcile(route)
	for _, candidate := range []*gatewayv1.HTTPRoute{route, sibling} {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate), candidate); err != nil {
			t.Fatal(err)
		}
		if reconciler.currentRouteProgrammedCondition(candidate, parent) == nil {
			t.Fatalf("route %s was never programmed: %#v", candidate.Name, candidate.Status)
		}
	}
	if len(provider.routeSpecs) < 2 {
		t.Fatalf("expected the two route components, got %#v", provider.routeSpecs)
	}
	programmed := provider.routeSpecs[len(provider.routeSpecs)-2:]
	if programmed[0].Identity.RouteName != route.Name || programmed[1].Identity.RouteName != sibling.Name ||
		programmed[0].Hostname != "api.example.com" || programmed[1].Hostname != programmed[0].Hostname ||
		programmed[0].Precedence != 0 || programmed[1].Precedence != 1 {
		t.Fatalf("exact declaration must outrank a wildcard even against a more specific path: %#v", programmed)
	}

	route.Spec.Hostnames = []gatewayv1.Hostname{"example.com", "ignored.example.net"}
	route.Generation++
	if err := kubeClient.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	before := len(provider.routeSpecs)
	for range 4 {
		reconcile(route)
	}
	if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0] != programmed[0].Identity ||
		len(provider.deletedGateways) != 0 || len(provider.routeSpecs) != before {
		t.Fatalf("revocation should delete only the edited route: deletes %#v, Gateway deletes %#v, ensures %#v",
			provider.deletedRoutes, provider.deletedGateways, provider.routeSpecs[before:])
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
		t.Fatal(err)
	}
	if len(route.Status.Parents) != 1 {
		t.Fatalf("expected one route parent after revocation, got %#v", route.Status.Parents)
	}
	accepted := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse ||
		accepted.Reason != string(gatewayv1.RouteReasonNoMatchingListenerHostname) || accepted.ObservedGeneration != route.Generation {
		t.Fatalf("edited route Accepted = %#v, want current NoMatchingListenerHostname", accepted)
	}
	reconcile(sibling)
	last := provider.routeSpecs[len(provider.routeSpecs)-1]
	if len(provider.routeSpecs) != before+1 || last.Identity != programmed[1].Identity ||
		last.Hostname != "api.example.com" || last.Precedence != 0 {
		t.Fatalf("sibling did not retain its forwarding after the edited route detached: %#v", last)
	}
}

func TestUnsupportedRouteIsRejectedBeforeBackendLookup(t *testing.T) {
	for _, test := range []struct {
		name   string
		host   *gatewayv1.Hostname
		change func(*gatewayv1.HTTPRoute)
		kind   routeErrorKind
	}{
		{name: "no matching declared hostname", host: ptr.To(gatewayv1.Hostname("*.example.test")), change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Hostnames = []gatewayv1.Hostname{"example.test", "other.test"}
		}, kind: routeErrorNoMatchingHostname},
		{name: "hostname exceeds Octavia L7 rule value limit after encoding", change: func(route *gatewayv1.HTTPRoute) {
			label := strings.Repeat("a", 60)
			route.Spec.Hostnames = []gatewayv1.Hostname{gatewayv1.Hostname(strings.Join([]string{label, label, label, label}, "."))}
		}, kind: routeErrorUnsupported},
		{name: "backend kind", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs[0].Kind = ptr.To(gatewayv1.Kind("Pod"))
		}, kind: routeErrorInvalidKind},
		{name: "cross-namespace backend", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("other"))
		}, kind: routeErrorRefNotPermitted},
		{name: "zero backend weight", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(0))
		}, kind: routeErrorUnsupported},
		{name: "missing backend port", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs[0].Port = nil
		}, kind: routeErrorUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			parent := gatewayv1.ParentReference{Name: "edge"}
			route := mapperTestHTTPRoute("default", "route", parent, "backend")
			test.change(route)
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(route).Build()
			gateway := programmedTestGateway(cfg)
			gateway.Spec.Listeners[0].Hostname = test.host
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, nil)
			_, err := reconciler.buildRouteModelWithReader(t.Context(), kubeClient, route, gateway)
			var buildError *routeBuildError
			if !errors.As(err, &buildError) || buildError.kind != test.kind {
				t.Fatalf("unsupported route error = %v, want %s before backend lookup", err, test.kind)
			}
			if test.kind == routeErrorUnsupported {
				status := statusForRouteBuildError(err)
				if status.accepted != metav1.ConditionFalse || status.acceptedReason != string(gatewayv1.RouteReasonUnsupportedValue) {
					t.Fatalf("unsupported route status = %+v, want Accepted=False/UnsupportedValue", status)
				}
			}
		})
	}
}

func TestNodePortRouteRejectsTerminalServiceProfiles(t *testing.T) {
	port := gatewayv1.PortNumber(80)
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "candidate"},
		Spec:       gatewayv1.HTTPRouteSpec{Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: "backend", Port: &port}}}}}}},
	}
	unsupportedAppProtocol := "example.com/unsupported"
	tests := []struct {
		name string
		spec corev1.ServiceSpec
	}{
		{name: "ClusterIP", spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP}},
		{name: "UDP", spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080, Protocol: corev1.ProtocolUDP}}}},
		{name: "unsupported appProtocol", spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080, AppProtocol: &unsupportedAppProtocol}}}},
		{name: "missing NodePort", spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: 80}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backend"}, Spec: test.spec}
			kubeClient := indexedFakeClientBuilder(testScheme(t), testConfig()).WithObjects(service).Build()
			reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Config: testConfig()}, nil)
			_, gateway := providerRouteParentObjects(testConfig())
			err := reconciler.checkRouteBackendProfile(t.Context(), kubeClient, route.Namespace, route.Spec.Rules[0].BackendRefs[0].BackendObjectReference, gateway)
			var rejection *routeBuildError
			if !errors.As(err, &rejection) {
				t.Fatalf("unsupported backend profile error = %v", err)
			}
		})
	}
}

func TestTerminalServiceProfileDoesNotSelfWinOrInflateAttachedRoutes(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	gateway := programmedTestGateway(cfg)
	older := metav1.NewTime(time.Unix(1, 0))
	newer := metav1.NewTime(time.Unix(2, 0))
	terminalRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "terminal", UID: "terminal-route-uid", Generation: 1, CreationTimestamp: older},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
			Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: "udp-backend", Port: portNumberPointer(80),
			}}}}}},
		},
	}
	validRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "valid", UID: "valid-route-uid", Generation: 1, CreationTimestamp: newer},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
			Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: "tcp-backend", Port: portNumberPointer(80),
			}}}}}},
		},
	}
	udpService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "udp-backend"},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{
			Port: 80, NodePort: 30080, Protocol: corev1.ProtocolUDP,
		}}},
	}
	tcpService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tcp-backend"},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{
			Port: 80, NodePort: 30081, Protocol: corev1.ProtocolTCP,
		}}},
	}
	ready := true
	slice := &discoveryv1.EndpointSlice{
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		ObjectMeta:  metav1.ObjectMeta{Namespace: "default", Name: "tcp-backend-1", Labels: map[string]string{discoveryv1.LabelServiceName: "tcp-backend"}},
		Endpoints:   []discoveryv1.Endpoint{{Conditions: discoveryv1.EndpointConditions{Ready: &ready}}},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Status: corev1.NodeStatus{
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.11"}},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).
		WithStatusSubresource(&gatewayv1.Gateway{}, &gatewayv1.HTTPRoute{}).
		WithObjects(class, gateway, terminalRoute, validRoute, udpService, tcpService, slice, node).
		Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	ctx := context.Background()
	for _, route := range []*gatewayv1.HTTPRoute{terminalRoute, validRoute} {
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
			t.Fatalf("Reconcile(%s) error = %v", route.Name, err)
		}
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls before binding checkpoint = %d, want 0", len(provider.routeSpecs))
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(validRoute)}); err != nil {
		t.Fatalf("second Reconcile(%s) error = %v", validRoute.Name, err)
	}

	var gotTerminal gatewayv1.HTTPRoute
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(terminalRoute), &gotTerminal); err != nil {
		t.Fatal(err)
	}
	if len(gotTerminal.Status.Parents) != 1 {
		t.Fatalf("terminal route parent statuses = %#v", gotTerminal.Status.Parents)
	}
	accepted := meta.FindStatusCondition(gotTerminal.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse {
		t.Fatalf("terminal route Accepted condition = %#v", accepted)
	}
	resolved := meta.FindStatusCondition(gotTerminal.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	if resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonUnsupportedProtocol) {
		t.Fatalf("terminal route ResolvedRefs condition = %#v", resolved)
	}

	var gotValid gatewayv1.HTTPRoute
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(validRoute), &gotValid); err != nil {
		t.Fatal(err)
	}
	if len(gotValid.Status.Parents) != 1 {
		t.Fatalf("valid route parent statuses = %#v", gotValid.Status.Parents)
	}
	accepted = meta.FindStatusCondition(gotValid.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionTrue {
		t.Fatalf("valid route Accepted condition = %#v", accepted)
	}
	programmed := meta.FindStatusCondition(gotValid.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed")
	if programmed == nil || programmed.Status != metav1.ConditionTrue {
		t.Fatalf("valid route Programmed condition = %#v", programmed)
	}
	if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Identity.RouteName != validRoute.Name {
		t.Fatalf("EnsureRoute specs = %#v", provider.routeSpecs)
	}

	gatewayReconciler := *newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	if _, err := gatewayReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	var gotGateway gatewayv1.Gateway
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if len(gotGateway.Status.Listeners) != 1 || gotGateway.Status.Listeners[0].AttachedRoutes != 1 {
		t.Fatalf("Gateway listener status = %#v, want one attached route", gotGateway.Status.Listeners)
	}
}

func TestBindRouteRefetchesBeforePatchingMetadata(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	gateway := programmedTestGateway(cfg)
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName), Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).
		WithObjects(route, gateway, gatewayClass, service, endpointSlice, node).
		Build()

	staleRoute := route.DeepCopy()
	current := &gatewayv1.HTTPRoute{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	current.Annotations = map[string]string{"example.com/concurrent": "preserve"}
	if err := kubeClient.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}

	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, &recordingProvider{})
	bindingStored, err := reconciler.bindRoute(context.Background(), staleRoute, gateway)
	if err != nil {
		t.Fatalf("bindRoute() error = %v", err)
	}
	if !bindingStored {
		t.Fatal("bindRoute() did not report the metadata checkpoint")
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations["example.com/concurrent"] != "preserve" {
		t.Fatalf("concurrent annotation was lost: %#v", current.Annotations)
	}
	if current.Annotations[cfg.RouteGatewayUIDAnnotation()] != string(gateway.UID) ||
		!controllerutil.ContainsFinalizer(current, cfg.RouteFinalizer()) {
		t.Fatalf("HTTPRoute binding was not patched: annotations %#v, finalizers %#v", current.Annotations, current.Finalizers)
	}
}

func TestBindRouteRejectsStaleGeneration(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	current := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace:  "default",
		Name:       "api",
		UID:        "route-uid",
		Generation: 2,
	}}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithObjects(current).Build()
	staleRoute := current.DeepCopy()
	staleRoute.Generation = 1

	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, &recordingProvider{})
	if _, err := reconciler.bindRoute(context.Background(), staleRoute, programmedTestGateway(cfg)); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("bindRoute() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(current, cfg.RouteFinalizer()) {
		t.Fatal("stale reconciliation added the HTTPRoute finalizer")
	}
}

func TestBindRouteChecksGatewayProgrammedBeforeBackendModel(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "missing-backend")
	route.UID = "route-uid"
	route.Generation = 1
	gateway := programmedTestGateway(cfg)
	gateway.Status.Conditions = nil
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName), Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	kubeClient := indexedFakeClientBuilder(scheme, cfg).
		WithObjects(route, gateway, gatewayClass).
		Build()
	reconciler := *newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient, Config: cfg,
	}, &recordingProvider{})

	_, err := reconciler.bindRoute(context.Background(), route, gateway)
	if !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("bindRoute() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if apierrors.IsNotFound(err) {
		t.Fatalf("bindRoute() read the backend before checking Gateway Programmed: %v", err)
	}
}

func TestEnsureRouteGraphCheckpointsBeforeProviderCall(t *testing.T) {
	for _, test := range []struct {
		name       string
		programmed bool
		wantState  routeBindingState
	}{
		{name: "Gateway pending", wantState: routeBindingGatewayPending},
		{name: "binding persisted", programmed: true, wantState: routeBindingPersisted},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID, route.Generation = "route-uid", 1
			gateway := programmedTestGateway(cfg)
			if !test.programmed {
				gateway.Status.Conditions = nil
			}
			gatewayClass := &gatewayv1.GatewayClass{
				ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName), Generation: 1},
				Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
			}
			service, endpointSlice, node := validNodePortBackend("default", "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithObjects(route, gateway, gatewayClass, service, endpointSlice, node).
				Build()
			liveReader := &endpointSliceCountingReader{Reader: kubeClient}
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{
				Client: kubeClient, APIReader: liveReader,
				Coordinator: &graph.Coordinator{}, Config: cfg,
			}, provider)

			result, err := reconciler.ensureRouteGraph(t.Context(), route, gateway)
			if err != nil || result.bindingState != test.wantState {
				t.Fatalf("ensureRouteGraph() = %#v, %v; want binding state %d", result, err, test.wantState)
			}
			wantLists := 1
			if test.programmed {
				wantLists++ // Reobserve membership before persisting the binding.
			}
			if liveReader.endpointSliceLists != wantLists {
				t.Fatalf("EndpointSlice lists = %d, want %d for model and binding validation", liveReader.endpointSliceLists, wantLists)
			}
			if len(provider.gatewaySpecs)+len(provider.routeSpecs) != 0 {
				t.Fatal("called the provider before the binding checkpoint")
			}
			var current gatewayv1.HTTPRoute
			if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(route), &current); err != nil {
				t.Fatal(err)
			}
			_, bound, err := reconciler.storedRouteIdentity(&current)
			if err != nil || bound != test.programmed || controllerutil.ContainsFinalizer(&current, cfg.RouteFinalizer()) != test.programmed {
				t.Fatalf("binding persisted before Gateway readiness: bound=%t, finalizers=%v, error=%v", bound, current.Finalizers, err)
			}
		})
	}
}

func TestEnsureRouteGraphRejectsUnsafeBindingTransition(t *testing.T) {
	tests := []struct {
		name        string
		prepare     func(Config, *Reconciler, *gatewayv1.HTTPRoute)
		wantChanged bool
		wantMessage string
	}{
		{
			name: "incomplete binding",
			prepare: func(cfg Config, _ *Reconciler, route *gatewayv1.HTTPRoute) {
				route.Finalizers = []string{cfg.RouteFinalizer()}
			},
			wantMessage: "has the controller finalizer but no complete stored Gateway identity",
		},
		{
			name: "different Gateway binding",
			prepare: func(cfg Config, reconciler *Reconciler, route *gatewayv1.HTTPRoute) {
				oldGateway := programmedTestGateway(cfg)
				oldGateway.Name = "old-edge"
				oldGateway.UID = "old-gateway-uid"
				if err := reconciler.applyRouteBinding(route, oldGateway); err != nil {
					t.Fatal(err)
				}
			},
			wantChanged: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID = "route-uid"
			route.Generation = 1
			gateway := programmedTestGateway(cfg)
			gatewayClass := &gatewayv1.GatewayClass{
				ObjectMeta: metav1.ObjectMeta{Name: string(gateway.Spec.GatewayClassName), Generation: 1},
				Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
			}
			provider := &recordingProvider{}
			reconciler := *newTestReconciler(Reconciler{Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
			test.prepare(cfg, &reconciler, route)
			originalGatewayUID := route.Annotations[cfg.RouteGatewayUIDAnnotation()]
			service, endpointSlice, node := validNodePortBackend("default", "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithObjects(route, gateway, gatewayClass, service, endpointSlice, node).
				Build()
			reconciler.Client = kubeClient
			reconciler.APIReader = kubeClient

			_, err := reconciler.ensureRouteGraph(context.Background(), route, gateway)
			if test.wantChanged {
				if !errors.Is(err, errHTTPRouteChanged) {
					t.Fatalf("ensureRouteGraph() error = %v, want %v", err, errHTTPRouteChanged)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("ensureRouteGraph() error = %v, want message %q", err, test.wantMessage)
			}
			if len(provider.routeSpecs) != 0 {
				t.Fatalf("EnsureRoute calls = %d, want none", len(provider.routeSpecs))
			}
			var current gatewayv1.HTTPRoute
			if getErr := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &current); getErr != nil {
				t.Fatal(getErr)
			}
			if current.Annotations[cfg.RouteGatewayUIDAnnotation()] != originalGatewayUID {
				t.Fatalf(
					"stored Gateway UID = %q, want unchanged %q",
					current.Annotations[cfg.RouteGatewayUIDAnnotation()],
					originalGatewayUID,
				)
			}
		})
	}
}

func TestDetachRouteRejectsStaleGeneration(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace:  "default",
		Name:       "api",
		UID:        "route-uid",
		Generation: 2,
		Finalizers: routeBindingFinalizers(cfg, "default", "edge", "gateway-uid"),
		Annotations: map[string]string{
			cfg.RouteGatewayNamespaceAnnotation(): "default",
			cfg.RouteGatewayNameAnnotation():      "edge",
			cfg.RouteGatewayUIDAnnotation():       "gateway-uid",
		},
	}}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithObjects(route).Build()
	staleRoute := route.DeepCopy()
	staleRoute.Generation = 1
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Config: cfg}, provider)

	if _, err := reconciler.detachRoute(context.Background(), staleRoute); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("detachRoute() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatalf("DeleteRoute calls = %d, want 0", len(provider.deletedRoutes))
	}
	current := &gatewayv1.HTTPRoute{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(current, cfg.RouteFinalizer()) {
		t.Fatal("stale reconciliation removed the HTTPRoute finalizer")
	}
}

func TestDetachRouteDoesNotRepeatCloudDeleteAfterPatchConflict(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace:  "default",
		Name:       "api",
		UID:        "route-uid",
		Generation: 1,
		Finalizers: routeBindingFinalizers(cfg, "default", "edge", "gateway-uid"),
		Annotations: map[string]string{
			cfg.RouteLifecycleAnnotation():        controller.RouteDetaching,
			cfg.RouteGatewayNamespaceAnnotation(): "default",
			cfg.RouteGatewayNameAnnotation():      "edge",
			cfg.RouteGatewayUIDAnnotation():       "gateway-uid",
		},
	}}
	baseClient := indexedFakeClientBuilder(scheme, cfg).WithObjects(route).Build()
	cachedRoute := &gatewayv1.HTTPRoute{}
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(route), cachedRoute); err != nil {
		t.Fatal(err)
	}
	kubeClient := &conflictOncePatchClient{Client: baseClient, cachedRoute: cachedRoute}
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{
		Client:    kubeClient,
		APIReader: baseClient,

		Coordinator: &graph.Coordinator{},
		Config:      cfg,
	}, provider)

	if _, err := reconciler.detachRoute(context.Background(), route.DeepCopy()); err != nil {
		t.Fatalf("detachRoute() error = %v", err)
	}
	if len(provider.deletedRoutes) != 1 {
		t.Fatalf("DeleteRoute calls = %d, want 1", len(provider.deletedRoutes))
	}
	if kubeClient.patchCalls != 2 {
		t.Fatalf("Patch calls = %d, want 2", kubeClient.patchCalls)
	}
	current := &gatewayv1.HTTPRoute{}
	if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations["example.com/concurrent"] != "preserve" {
		t.Fatalf("concurrent annotation = %q, want preserve", current.Annotations["example.com/concurrent"])
	}
	if controllerutil.ContainsFinalizer(current, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute finalizer was not removed")
	}
	if _, present, err := reconciler.storedRouteIdentity(current); err != nil || present {
		t.Fatalf("storedRouteIdentity() = present %t, error %v; want no stored binding", present, err)
	}
}

func TestSetRouteParentStatusesRefetchesCurrentStatus(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace:  "default",
		Name:       "api",
		UID:        "route-uid",
		Generation: 1,
	}}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).
		WithStatusSubresource(&gatewayv1.HTTPRoute{}).
		WithObjects(route).
		Build()
	staleRoute := route.DeepCopy()

	current := &gatewayv1.HTTPRoute{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	foreignController := gatewayv1.GatewayController("example.net/other-controller")
	current.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef:      gatewayv1.ParentReference{Name: "other-gateway"},
		ControllerName: foreignController,
	}}
	if err := kubeClient.Status().Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}

	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Config: cfg}, nil)
	update := parentStatusUpdate{
		parent: gatewayv1.ParentReference{Name: "edge"},
		status: programmedRouteStatus(),
	}
	if err := reconciler.setRouteParentStatuses(context.Background(), staleRoute, []parentStatusUpdate{update}); err != nil {
		t.Fatalf("setRouteParentStatuses() error = %v", err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Parents) != 2 {
		t.Fatalf("HTTPRoute parent statuses = %#v, want both controllers", current.Status.Parents)
	}
	if current.Status.Parents[0].ControllerName != foreignController || current.Status.Parents[1].ControllerName != cfg.ControllerName {
		t.Fatalf("HTTPRoute parent status controllers = %#v", current.Status.Parents)
	}
}

type conflictOncePatchClient struct {
	client.Client
	cachedRoute *gatewayv1.HTTPRoute
	patchCalls  int
}

type endpointSliceCountingReader struct {
	client.Reader
	endpointSliceLists int
}

func (r *endpointSliceCountingReader) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
		r.endpointSliceLists++
	}
	return r.Reader.List(ctx, list, options...)
}

func (c *conflictOncePatchClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	route, ok := object.(*gatewayv1.HTTPRoute)
	if ok && c.cachedRoute != nil && key == client.ObjectKeyFromObject(c.cachedRoute) {
		candidate := c.cachedRoute.DeepCopy()
		*route = *candidate
		return nil
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (c *conflictOncePatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.patchCalls++
	if c.patchCalls == 1 {
		current := &gatewayv1.HTTPRoute{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(object), current); err != nil {
			return err
		}
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		current.Annotations["example.com/concurrent"] = "preserve"
		if err := c.Update(ctx, current); err != nil {
			return err
		}
		return apierrors.NewConflict(
			schema.GroupResource{Group: gatewayv1.GroupName, Resource: "httproutes"},
			object.GetName(),
			errors.New("injected metadata conflict"),
		)
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

func portNumberPointer(value gatewayv1.PortNumber) *gatewayv1.PortNumber { return &value }

func TestNodePortMembersHonorsExternalTrafficPolicyLocal(t *testing.T) {
	scheme := testScheme(t)
	ready, notReady := true, false
	workerOne, workerTwo := "worker-1", "worker-2"
	slice := &discoveryv1.EndpointSlice{
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		ObjectMeta:  metav1.ObjectMeta{Namespace: "default", Name: "backend-1", Labels: map[string]string{discoveryv1.LabelServiceName: "backend"}},
		Endpoints: []discoveryv1.Endpoint{
			{NodeName: &workerOne, Conditions: discoveryv1.EndpointConditions{Ready: &ready}},
			{NodeName: &workerTwo, Conditions: discoveryv1.EndpointConditions{Ready: &notReady}},
		},
	}
	nodes := []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: workerOne}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.11"}}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: workerTwo}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
	}
	objects := append([]client.Object{slice}, nodes...)
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithObjects(objects...).Build()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "backend"}, Spec: corev1.ServiceSpec{ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal, Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080}}}}
	members, err := nodePortMembersFromReader(context.Background(), kubeClient, service, &service.Spec.Ports[0], corev1.NodeInternalIP)
	if err != nil {
		t.Fatalf("nodePortMembers() error = %v", err)
	}
	want := []cloud.Member{{Address: "10.0.0.11", Port: 30080}}
	if len(members) != len(want) || members[0] != want[0] {
		t.Fatalf("members = %#v, want %#v", members, want)
	}
}

func programmedTestGateway(cfg Config) *gatewayv1.Gateway {
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1,
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
		Status: gatewayv1.GatewayStatus{Conditions: []metav1.Condition{{
			Type:               string(gatewayv1.GatewayConditionProgrammed),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: 1,
			Reason:             string(gatewayv1.GatewayReasonProgrammed),
			LastTransitionTime: metav1.Now(),
		}}},
	}
	completeGatewayTopologyFixture(cfg, gateway)
	return gateway
}
