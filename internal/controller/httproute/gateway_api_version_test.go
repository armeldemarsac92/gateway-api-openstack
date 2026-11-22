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
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestHTTPRouteMutationBoundaryChecksLiveGatewayAPIVersion(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	cachedClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithObjects(gatewayClass, gateway, route, service, endpointSlice, node).
		Build()
	liveReader := liveReaderWithUnsupportedVersion(cachedClient)
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client:    cachedClient,
		APIReader: liveReader,

		Coordinator: &graph.Coordinator{},
		Config:      cfg,
	}, provider)

	_, err := reconciler.ensureRouteGraph(context.Background(), route.DeepCopy(), gateway.DeepCopy())
	if !errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
		t.Fatalf("ensureRouteGraph() error = %v, want unsupported Gateway API version", err)
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls = %d, want 0", len(provider.routeSpecs))
	}
}

func TestHTTPRouteReconcileChecksLiveVersionBeforeDetach(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	route.Spec.ParentRefs = append(route.Spec.ParentRefs, gatewayv1.ParentReference{Name: "edge"})
	cachedClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(gatewayClass, gateway, route).
		Build()
	liveReader := liveReaderWithUnsupportedVersion(cachedClient)
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: cachedClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	request := ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("first Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedRoutes) != 0 || len(provider.routeSpecs) != 0 {
		t.Fatalf("provider calls before status checkpoint = deletes %d, ensures %d; want none", len(provider.deletedRoutes), len(provider.routeSpecs))
	}

	result, err = reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("live Gateway API version mismatch did not schedule a fallback recheck")
	}
	if len(provider.deletedRoutes) != 0 || len(provider.routeSpecs) != 0 {
		t.Fatalf("provider calls = deletes %d, ensures %d; want none", len(provider.deletedRoutes), len(provider.routeSpecs))
	}
	var got gatewayv1.HTTPRoute
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute detach removed the stored binding during a CRD version mismatch")
	}
}

func TestUnsupportedGatewayAPIVersionFreezesBoundGraph(t *testing.T) {
	cfg := testConfig()
	gatewayClass := unsupportedVersionGatewayClass(cfg)
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	routeBinder := newTestReconciler(Reconciler{Config: cfg}, nil)
	if err := routeBinder.applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef:      route.Spec.ParentRefs[0],
		ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{
			Condition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, string(gatewayv1.RouteReasonAccepted), "HTTPRoute is accepted", route.Generation),
			Condition(string(gatewayv1.RouteConditionResolvedRefs), metav1.ConditionTrue, string(gatewayv1.RouteReasonResolvedRefs), "References are resolved", route.Generation),
			Condition(cfg.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed", "HTTPRoute is programmed", route.Generation),
		},
	}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(gatewayClass, gateway, route).
		Build()
	provider := &recordingProvider{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	routeReconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	_, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	})
	if err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	_, err = routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	})
	if err != nil {
		t.Fatalf("HTTPRoute Reconcile() error = %v", err)
	}
	if len(provider.gatewaySpecs) != 0 || len(provider.routeSpecs) != 0 ||
		len(provider.deletedGateways) != 0 || len(provider.deletedRoutes) != 0 {
		t.Fatalf(
			"provider calls = EnsureGateway %d, EnsureRoute %d, DeleteGateway %d, DeleteRoute %d; want none",
			len(provider.gatewaySpecs), len(provider.routeSpecs), len(provider.deletedGateways), len(provider.deletedRoutes),
		)
	}

	var gotGateway gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&gotGateway, cfg.GatewayFinalizer()) {
		t.Fatal("Gateway binding was removed for an unsupported CRD version")
	}
	accepted := meta.FindStatusCondition(gotGateway.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionUnknown ||
		accepted.Reason != string(gatewayv1.GatewayReasonPending) {
		t.Fatalf("Gateway Accepted condition = %#v", accepted)
	}
	programmed := meta.FindStatusCondition(gotGateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.Status != metav1.ConditionTrue {
		t.Fatalf("Gateway Programmed condition = %#v, want the existing programmed state preserved", programmed)
	}

	var gotRoute gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &gotRoute); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&gotRoute, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute binding was removed for an unsupported CRD version")
	}
	if len(gotRoute.Status.Parents) != 1 {
		t.Fatalf("HTTPRoute parent statuses = %#v", gotRoute.Status.Parents)
	}
	routeAccepted := meta.FindStatusCondition(gotRoute.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
	if routeAccepted == nil || routeAccepted.Status != metav1.ConditionUnknown ||
		routeAccepted.Reason != string(gatewayv1.RouteReasonPending) {
		t.Fatalf("HTTPRoute Accepted condition = %#v", routeAccepted)
	}
	routeProgrammed := meta.FindStatusCondition(gotRoute.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed")
	if routeProgrammed == nil || routeProgrammed.Status != metav1.ConditionTrue || routeProgrammed.Reason != "Programmed" {
		t.Fatalf("HTTPRoute Programmed condition = %#v, want the existing programmed state preserved", routeProgrammed)
	}
}

func TestUnsupportedGatewayAPIVersionRetainsUniqueUnsectionedHTTPRoute(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "unique hostname"
		if ambiguous {
			name = "ambiguous hostname"
		}
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			class := unsupportedVersionGatewayClass(cfg)
			gateway := programmedTestGateway(cfg)
			gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.HTTPProtocolType)
			controller.SetGatewayListenerScope(cfg, gateway, gatewayv1.HTTPProtocolType)
			host := gatewayv1.Hostname("api.example.test")
			gateway.Spec.Listeners[0].Hostname = &host
			otherHost := gatewayv1.Hostname("other.example.test")
			if ambiguous {
				otherHost = host
			}
			gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
				Name: "other", Protocol: gatewayv1.HTTPProtocolType, Port: 8080, Hostname: &otherHost,
			})
			for _, listener := range gateway.Spec.Listeners {
				gateway.Status.Listeners = append(gateway.Status.Listeners, gatewayv1.ListenerStatus{
					Name: listener.Name,
					Conditions: []metav1.Condition{Condition(string(gatewayv1.ListenerConditionProgrammed),
						metav1.ConditionTrue, string(gatewayv1.ListenerReasonProgrammed), "Listener is programmed", gateway.Generation)},
				})
			}
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID, route.Generation = "route-uid", 1
			route.Spec.Hostnames = []gatewayv1.Hostname{host}
			route.Status.Parents = []gatewayv1.RouteParentStatus{{
				ParentRef: route.Spec.ParentRefs[0], ControllerName: cfg.ControllerName,
				Conditions: []metav1.Condition{Condition(cfg.Domain()+"/Programmed",
					metav1.ConditionTrue, "Programmed", "HTTPRoute is programmed", route.Generation)},
			}}
			if err := (&Reconciler{Config: cfg}).applyRouteBinding(route, gateway); err != nil {
				t.Fatal(err)
			}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).
				WithObjects(class, gateway, route).Build()
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{
				Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg,
			}, provider)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
			for range 3 {
				if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			if err := kubeClient.Get(t.Context(), request.NamespacedName, route); err != nil {
				t.Fatal(err)
			}
			if got := controllerutil.ContainsFinalizer(route, cfg.RouteFinalizer()); got == ambiguous || len(provider.routeSpecs) != 0 {
				t.Fatalf("bundle skew with %s: bound = %t, ensures = %d", name, got, len(provider.routeSpecs))
			}
			if got := len(provider.deletedRoutes); got != 0 && !ambiguous || got != 1 && ambiguous {
				t.Fatalf("bundle skew with %s: got %d route deletions", name, got)
			}
		})
	}
}

func TestListenerHostnameChangesRemoveStaleRouteForwarding(t *testing.T) {
	for _, test := range []struct {
		name                string
		versionSkew         bool
		routeHosts          []gatewayv1.Hostname
		initialListenerHost gatewayv1.Hostname
		initialSecondHost   gatewayv1.Hostname
		updatedListenerHost gatewayv1.Hostname
		otherListenerHost   gatewayv1.Hostname
		wantAccepted        metav1.ConditionStatus
		wantReason          string
	}{
		{
			name: "apex outside wildcard", routeHosts: []gatewayv1.Hostname{"example.test"}, updatedListenerHost: "*.example.test",
			wantAccepted: metav1.ConditionFalse, wantReason: "NoMatchingListenerHostname",
		},
		{
			name: "new listener restriction during skew", versionSkew: true, updatedListenerHost: "*.example.test",
			wantAccepted: metav1.ConditionUnknown, wantReason: "Pending",
		},
		{
			name: "different exact hostname during skew", versionSkew: true, routeHosts: []gatewayv1.Hostname{"*.example.test"},
			initialListenerHost: "api.example.test", updatedListenerHost: "other.example.test",
			wantAccepted: metav1.ConditionUnknown, wantReason: "Pending",
		},
		{
			name: "listener edit admits two distinct constraints", routeHosts: []gatewayv1.Hostname{"api.example.test", "other.example.test"},
			initialListenerHost: "api.example.test", updatedListenerHost: "*.example.test",
			wantAccepted: metav1.ConditionTrue, wantReason: "Accepted",
		},
		{
			name: "remove one of two established sections during skew", versionSkew: true,
			initialListenerHost: "api.example.test", initialSecondHost: "other.example.test", updatedListenerHost: "api.example.test",
			wantAccepted: metav1.ConditionUnknown, wantReason: "Pending",
		},
		{
			name: "added section changes the established host set during skew", versionSkew: true,
			initialListenerHost: "api.example.test", updatedListenerHost: "api.example.test", otherListenerHost: "other.example.test",
			wantAccepted: metav1.ConditionUnknown, wantReason: "Pending",
		},
		{
			name: "listener edit selects a formerly excluded declaration during skew", versionSkew: true,
			routeHosts:          []gatewayv1.Hostname{"api.example.test", "other.example.test"},
			initialListenerHost: "api.example.test", updatedListenerHost: "other.example.test",
			wantAccepted: metav1.ConditionUnknown, wantReason: "Pending",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			class := unsupportedVersionGatewayClass(cfg)
			class.Status.Conditions = nil
			gateway := programmedTestGateway(cfg)
			if test.initialListenerHost != "" {
				gateway.Spec.Listeners[0].Hostname = &test.initialListenerHost
			}
			if test.initialSecondHost != "" {
				second := *gateway.Spec.Listeners[0].DeepCopy()
				second.Name, second.Hostname = "other", &test.initialSecondHost
				gateway.Spec.Listeners = append(gateway.Spec.Listeners, second)
			}
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID, route.Generation = "route-uid", 1
			route.Spec.Hostnames = test.routeHosts
			service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithStatusSubresource(class, gateway, route).
				WithObjects(class, gateway, route, service, endpoints, node).
				Build()
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{
				Client: kubeClient, APIReader: kubeClient,
				Coordinator: &graph.Coordinator{}, Config: cfg,
			}, provider)
			reconcile := func() {
				t.Helper()
				if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
					t.Fatal(err)
				}
			}
			reconcile()
			reconcile()
			if len(provider.routeSpecs) != 1 {
				t.Fatalf("route was never programmed: %v", provider.routeSpecs)
			}
			if test.initialSecondHost != "" {
				groups := provider.routeSpecs[0].HTTPBackends
				if len(groups) != 1 || len(groups[0].Matches) != 2 {
					t.Fatalf("both original hostname policies must be established: %#v", groups)
				}
			}
			if test.versionSkew {
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
					t.Fatal(err)
				}
				class.Status = unsupportedVersionGatewayClass(cfg).Status
				if err := kubeClient.Status().Update(ctx, class); err != nil {
					t.Fatal(err)
				}
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(gateway), gateway); err != nil {
				t.Fatal(err)
			}
			gateway.Spec.Listeners[0].Hostname = &test.updatedListenerHost
			if test.initialSecondHost != "" {
				gateway.Spec.Listeners = gateway.Spec.Listeners[:1]
			}
			if test.otherListenerHost != "" {
				second := *gateway.Spec.Listeners[0].DeepCopy()
				second.Name, second.Hostname = "other", &test.otherListenerHost
				gateway.Spec.Listeners = append(gateway.Spec.Listeners, second)
			}
			gateway.Generation++
			if err := kubeClient.Update(ctx, gateway); err != nil {
				t.Fatal(err)
			}
			reconcile()
			if test.otherListenerHost != "" {
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
					t.Fatal(err)
				}
				accepted := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionUnknown || accepted.Reason != string(gatewayv1.RouteReasonPending) {
					t.Fatalf("route with newly eligible sections during version skew = %#v", accepted)
				}
			}
			for range 3 {
				reconcile()
			}
			if test.wantAccepted == metav1.ConditionTrue {
				if len(provider.deletedRoutes) != 0 || len(provider.routeSpecs) < 2 {
					t.Fatalf("expanding the hostname set did not update forwarding: ensured %d, removed %d", len(provider.routeSpecs), len(provider.deletedRoutes))
				}
				groups := provider.routeSpecs[len(provider.routeSpecs)-1].HTTPBackends
				if len(groups) != 1 || len(groups[0].Matches) != 2 {
					t.Fatalf("expanded hostname policies = %#v", groups)
				}
				hosts := []string{groups[0].Matches[0].Hostname, groups[0].Matches[1].Hostname}
				slices.Sort(hosts)
				if !slices.Equal(hosts, []string{"api.example.test", "other.example.test"}) {
					t.Fatalf("expanded hostnames = %v", hosts)
				}
			} else if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0] != provider.routeSpecs[0].Identity ||
				len(provider.routeSpecs) != 1 || len(provider.deletedGateways) != 0 {
				t.Fatalf("expected cleanup of only the stale route graph, got route ensures %v, route deletions %v, Gateway deletions %v",
					provider.routeSpecs, provider.deletedRoutes, provider.deletedGateways)
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
				t.Fatal(err)
			}
			if len(route.Status.Parents) != 1 {
				t.Fatalf("expected one route parent, got %v", route.Status.Parents)
			}
			accepted := meta.FindStatusCondition(route.Status.Parents[0].Conditions, "Accepted")
			if accepted == nil || accepted.Status != test.wantAccepted || accepted.Reason != test.wantReason {
				t.Fatalf("route Accepted = %+v, want %s/%s", accepted, test.wantAccepted, test.wantReason)
			}
		})
	}
}

func TestSharedHTTPSectionNamespaceMoveWithdrawsForwardingDuringVersionSkew(t *testing.T) {
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	first := &gateway.Spec.Listeners[0]
	first.Hostname = ptr.To(gatewayv1.Hostname("first.example.com"))
	first.AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
		From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"section": "first"}},
	}}
	second := *first.DeepCopy()
	second.Name, second.Hostname = "second", ptr.To(gatewayv1.Hostname("second.example.com"))
	second.AllowedRoutes.Namespaces.Selector.MatchLabels["section"] = "second"
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, second)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: gateway.Namespace, Labels: map[string]string{"section": "first"}}}
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID, route.Generation = "route-uid", 1
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(class, gateway, route).
		WithObjects(class, gateway, namespace, route, service, endpoints, node).Build()
	provider := &recordingProvider{}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	reconcile()
	if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Hostname != string(*first.Hostname) {
		t.Fatalf("initial forwarding did not select the first section: %#v", provider.routeSpecs)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(class), class); err != nil {
		t.Fatal(err)
	}
	class.Status = unsupportedVersionGatewayClass(cfg).Status
	if err := kube.Status().Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(namespace), namespace); err != nil {
		t.Fatal(err)
	}
	namespace.Labels["section"] = "second"
	if err := kube.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		reconcile()
	}
	if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0] != provider.routeSpecs[0].Identity {
		t.Fatalf("namespace move left outdated host forwarding during skew: ensures %#v, deletions %#v", provider.routeSpecs, provider.deletedRoutes)
	}
}

func TestUnsupportedGatewayAPIVersionKeepsExactForwardingUnderWildcard(t *testing.T) {
	ctx := t.Context()
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("*.example.test"))
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID, route.Generation = "route-uid", 1
	route.Spec.Hostnames = []gatewayv1.Hostname{"api.example.test"}
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(class, gateway, route).
		WithObjects(class, gateway, route, service, endpoints, node).Build()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient, Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	reconcile()
	if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Hostname != "api.example.test" {
		t.Fatalf("initial forwarding = %#v, want one exact hostname", provider.routeSpecs)
	}
	// Octavia locks the load balancer while applying a sibling's change.
	// This observation carries no update about our previously programmed route.
	writer := reconciler.Writer
	reconciler.Writer = fixedRouteGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ProgressingOutcome("Waiting for load balancer", time.Second)}}
	reconcile()
	reconciler.Writer = writer
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(gateway), gateway); err != nil {
		t.Fatal(err)
	}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
		Name: "unsupported", Port: 8443, Protocol: "custom.example/test",
	})
	gateway.Generation++
	if err := kubeClient.Update(ctx, gateway); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
		t.Fatal(err)
	}
	class.Status = unsupportedVersionGatewayClass(cfg).Status
	if err := kubeClient.Status().Update(ctx, class); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		reconcile()
	}
	if err := kubeClient.Get(ctx, request.NamespacedName, route); err != nil {
		t.Fatal(err)
	}
	if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 ||
		!controllerutil.ContainsFinalizer(route, cfg.RouteFinalizer()) ||
		reconciler.currentRouteProgrammedCondition(route, route.Spec.ParentRefs[0]) == nil {
		t.Fatalf("unrelated listener edit changed established forwarding: ensures = %d, deletes = %d, finalizers = %v, status = %#v",
			len(provider.routeSpecs), len(provider.deletedRoutes), route.Finalizers, route.Status)
	}
}

func TestUnsupportedGatewayAPIVersionRevokesBoundServiceReference(t *testing.T) {
	t.Run("unchanged backend", func(t *testing.T) { testServiceReferenceRevocationDuringVersionSkew(t, false) })
	t.Run("edited backend", func(t *testing.T) { testServiceReferenceRevocationDuringVersionSkew(t, true) })
}

func TestUnsupportedGatewayAPIVersionRemovesOldTenantForwarding(t *testing.T) {
	for _, action := range []string{"selector revoked", "parent retargeted"} {
		t.Run(action, func(t *testing.T) {
			cfg := testConfig()
			gatewayClass := unsupportedVersionGatewayClass(cfg)
			unsupported := gatewayClass.Status.DeepCopy()
			gatewayClass.Status.Conditions = nil
			gateway := programmedTestGateway(cfg)
			gateway.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
				From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "blue"}},
			}}
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant", Labels: map[string]string{"team": "blue"}}}
			parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))}
			route := mapperTestHTTPRoute(namespace.Name, "api", parent, "backend")
			route.UID, route.Generation = "tenant-route", 1
			foreign := gatewayv1.RouteParentStatus{ControllerName: "other.example/controller", ParentRef: parent}
			route.Status.Parents = []gatewayv1.RouteParentStatus{foreign}
			service, endpoints, node := validNodePortBackend(route.Namespace, "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithStatusSubresource(gatewayClass, route).
				WithObjects(gatewayClass, gateway, route, namespace, service, endpoints, node).
				Build()
			provider := &recordingProvider{}
			newReconciler := func(reader client.Reader) *Reconciler {
				return newTestReconciler(Reconciler{Client: kubeClient, APIReader: reader, Coordinator: &graph.Coordinator{}, Config: cfg}, provider)
			}
			reconciler := newReconciler(kubeClient)
			key := client.ObjectKeyFromObject(route)
			reconcile := func() {
				t.Helper()
				if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
			}
			reconcile()
			reconcile()
			identity := controller.RouteIdentity(cfg, gateway, route)
			if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Identity != identity {
				t.Fatalf("tenant route was not ensured with its own identity: %#v", provider.routeSpecs)
			}
			if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(gatewayClass), gatewayClass); err != nil {
				t.Fatal(err)
			}
			gatewayClass.Status = *unsupported
			if err := kubeClient.Status().Update(t.Context(), gatewayClass); err != nil {
				t.Fatal(err)
			}
			reconciler = newReconciler(liveReaderWithUnsupportedVersion(kubeClient))
			reconcile()
			if len(provider.deletedRoutes) != 0 {
				t.Fatal("bundle skew removed an authorized tenant route")
			}
			if err := kubeClient.Get(t.Context(), key, route); err != nil {
				t.Fatal(err)
			}
			if action == "selector revoked" {
				namespace.Labels["team"] = "red"
				if err := kubeClient.Update(t.Context(), namespace); err != nil {
					t.Fatal(err)
				}
			} else {
				route.Spec.ParentRefs[0].Name = "other"
				route.Generation++
				if err := kubeClient.Update(t.Context(), route); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				reconcile()
			}
			if len(provider.routeSpecs) != 1 || len(provider.deletedGateways) != 0 || len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0] != identity {
				t.Fatalf("old tenant fragment was not selectively removed: ensures %d, route deletes %v, Gateway deletes %v", len(provider.routeSpecs), provider.deletedRoutes, provider.deletedGateways)
			}
			if err := kubeClient.Get(t.Context(), key, route); err != nil {
				t.Fatal(err)
			}
			if len(route.OwnerReferences) != 0 || controllerutil.ContainsFinalizer(route, cfg.RouteFinalizer()) || route.Status.Parents[0].ControllerName != foreign.ControllerName {
				t.Fatalf("tenant detach changed unrelated ownership or status: %#v", route)
			}
			if action == "selector revoked" {
				accepted := meta.FindStatusCondition(route.Status.Parents[1].Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
					t.Fatalf("selector revocation status = %#v", accepted)
				}
			}
		})
	}
}

func testServiceReferenceRevocationDuringVersionSkew(t *testing.T, editBackend bool) {
	t.Helper()
	ctx := t.Context()
	cfg := testConfig()
	gatewayClass := unsupportedVersionGatewayClass(cfg)
	gatewayClass.Status.Conditions = nil
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID, route.Generation = "route-uid", 1
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("shared"))
	service, endpoints, node := validNodePortBackend("shared", "backend")
	grant := serviceReferenceGrant(service.Namespace, route.Namespace, service.Name)
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gatewayClass, route).
		WithObjects(gatewayClass, gateway, route, service, endpoints, node, grant).
		Build()
	provider := &recordingProvider{}
	newReconciler := func() *Reconciler {
		return newTestReconciler(Reconciler{
			Client: kubeClient, APIReader: kubeClient,
			Coordinator: &graph.Coordinator{}, Config: cfg,
		}, provider)
	}
	reconciler := newReconciler()
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatalf("reconcile HTTPRoute: %v", err)
		}
	}
	reconcile()
	reconcile()
	if len(provider.routeSpecs) != 1 {
		t.Fatalf("programming with a valid grant produced %d route ensures, want one", len(provider.routeSpecs))
	}
	identity := provider.routeSpecs[0].Identity

	var crd apiextensionsv1.CustomResourceDefinition
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "gatewayclasses.gateway.networking.k8s.io"}, &crd); err != nil {
		t.Fatal(err)
	}
	supportedVersion := crd.Annotations[gatewayconsts.BundleVersionAnnotation]
	crd.Annotations[gatewayconsts.BundleVersionAnnotation] = "v1.6.1"
	if err := kubeClient.Update(ctx, &crd); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(gatewayClass), gatewayClass); err != nil {
		t.Fatal(err)
	}
	supportedClassStatus := gatewayClass.Status.DeepCopy()
	gatewayClass.Status = unsupportedVersionGatewayClass(cfg).Status
	if err := kubeClient.Status().Update(ctx, gatewayClass); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 {
		t.Fatal("version skew changed forwarding while the grant remained valid")
	}

	var current gatewayv1.HTTPRoute
	readRoute := func() {
		t.Helper()
		if err := kubeClient.Get(ctx, request.NamespacedName, &current); err != nil {
			t.Fatal(err)
		}
	}
	if editBackend {
		readRoute()
		ref := &current.Spec.Rules[0].BackendRefs[0]
		ref.Namespace, ref.Name = nil, "replacement"
		current.Generation++
		if err := kubeClient.Update(ctx, &current); err != nil {
			t.Fatal(err)
		}
	}
	if err := kubeClient.Delete(ctx, grant); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		reconcile()
		readRoute()
		if current.Annotations[cfg.RouteLifecycleAnnotation()] == controller.RouteDetaching {
			break
		}
	}
	if current.Annotations[cfg.RouteLifecycleAnnotation()] != controller.RouteDetaching || len(provider.deletedRoutes) != 0 {
		t.Fatalf("expected to persist the route's detach checkpoint before provider deletion, got lifecycle %q and deletions %v",
			current.Annotations[cfg.RouteLifecycleAnnotation()], provider.deletedRoutes)
	}
	reconciler = newReconciler()
	reconcile()
	reconcile()
	if len(provider.deletedRoutes) != 1 || provider.deletedRoutes[0] != identity || len(provider.deletedGateways) != 0 || len(provider.routeSpecs) != 1 {
		t.Fatalf("expected a single deletion for the originally bound route during skew, got routes %v, gateway deletions %v, and %d route ensures",
			provider.deletedRoutes, provider.deletedGateways, len(provider.routeSpecs))
	}
	readRoute()
	if controllerutil.ContainsFinalizer(&current, cfg.RouteFinalizer()) || len(current.Status.Parents) != 1 {
		t.Fatalf("expected the route to be detached with retained diagnostics: %#v", current)
	}
	conditions := current.Status.Parents[0].Conditions
	resolved := meta.FindStatusCondition(conditions, "ResolvedRefs")
	programmed := meta.FindStatusCondition(conditions, cfg.Domain()+"/Programmed")
	wantResolved, wantReason := metav1.ConditionFalse, "RefNotPermitted"
	if editBackend {
		wantResolved, wantReason = metav1.ConditionUnknown, "Pending"
	}
	if resolved == nil || resolved.Status != wantResolved || resolved.Reason != wantReason ||
		programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.ObservedGeneration != current.Generation {
		t.Fatalf("expected revoked reference and stopped forwarding: %#v", conditions)
	}

	if err := kubeClient.Create(ctx, serviceReferenceGrant(service.Namespace, route.Namespace, service.Name)); err != nil {
		t.Fatal(err)
	}
	if editBackend {
		ref := &current.Spec.Rules[0].BackendRefs[0]
		ref.Namespace, ref.Name = ptr.To(gatewayv1.Namespace(service.Namespace)), gatewayv1.ObjectName(service.Name)
		current.Generation++
		if err := kubeClient.Update(ctx, &current); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 1 {
		t.Fatal("restoring permission changed forwarding while provisioning was frozen")
	}
	crd.Annotations[gatewayconsts.BundleVersionAnnotation] = supportedVersion
	if err := kubeClient.Update(ctx, &crd); err != nil {
		t.Fatal(err)
	}
	gatewayClass.Status = *supportedClassStatus
	if err := kubeClient.Status().Update(ctx, gatewayClass); err != nil {
		t.Fatal(err)
	}
	reconcile()
	reconcile()
	if len(provider.routeSpecs) != 2 || provider.routeSpecs[1].Identity != identity || len(provider.deletedRoutes) != 1 {
		t.Fatalf("expected forwarding to recover on the same Gateway after the bundle recovers, got routes %v", provider.routeSpecs)
	}
}

func TestUnsupportedVersionStatusUsesLiveHTTPRouteProgrammedState(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(unsupportedVersionGatewayClass(cfg), gateway, route).
		Build()
	var liveRoute gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &liveRoute); err != nil {
		t.Fatal(err)
	}
	liveRoute.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef:      liveRoute.Spec.ParentRefs[0],
		ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{Condition(
			cfg.Domain()+"/Programmed",
			metav1.ConditionTrue,
			"Programmed",
			"HTTPRoute is programmed",
			liveRoute.Generation,
		)},
	}}
	liveReader := &liveObjectOverrideReader{Client: kubeClient, route: &liveRoute}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Parents) != 1 {
		t.Fatalf("HTTPRoute parent statuses = %#v", got.Status.Parents)
	}
	programmed := meta.FindStatusCondition(got.Status.Parents[0].Conditions, cfg.Domain()+"/Programmed")
	if programmed == nil || programmed.Status != metav1.ConditionTrue || programmed.Reason != "Programmed" {
		t.Fatalf("HTTPRoute Programmed condition = %#v, want live programmed state", programmed)
	}
}

func TestUnsupportedGatewayAPIVersionDoesNotCreateBindings(t *testing.T) {
	cfg := testConfig()
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(unsupportedVersionGatewayClass(cfg), gateway, route).
		Build()
	provider := &recordingProvider{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	routeReconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	if _, err := routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}); err != nil {
		t.Fatalf("HTTPRoute Reconcile() error = %v", err)
	}
	var gotGateway gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&gotGateway, cfg.GatewayFinalizer()) || len(gotGateway.Annotations) != 0 {
		t.Fatalf("Gateway binding = finalizers %#v, annotations %#v; want none", gotGateway.Finalizers, gotGateway.Annotations)
	}
	var gotRoute gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &gotRoute); err != nil {
		t.Fatal(err)
	}
	_, routeBindingPresent, err := routeReconciler.storedRouteIdentity(&gotRoute)
	if err != nil {
		t.Fatalf("storedRouteIdentity() error = %v", err)
	}
	if controllerutil.ContainsFinalizer(&gotRoute, cfg.RouteFinalizer()) || routeBindingPresent {
		t.Fatalf("HTTPRoute binding = finalizers %#v, annotations %#v; want none", gotRoute.Finalizers, gotRoute.Annotations)
	}
	if len(provider.gatewaySpecs) != 0 || len(provider.routeSpecs) != 0 {
		t.Fatalf("provider ensures = gateways %d, routes %d; want none", len(provider.gatewaySpecs), len(provider.routeSpecs))
	}
}

func TestUnsupportedGatewayAPIVersionAllowsDeletion(t *testing.T) {
	cfg := testConfig()
	now := metav1.Now()
	gateway := programmedTestGateway(cfg)
	gateway.DeletionTimestamp = &now
	route, _ := deletingBoundRoute(cfg)
	objects := append(
		[]client.Object{unsupportedVersionGatewayClass(cfg), gateway, route},
		unsupportedGatewayAPICRDs()...,
	)
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(objects...).
		Build()
	provider := &recordingProvider{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	routeReconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	if _, err := routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: route.Namespace, Name: route.Name},
	}); err != nil {
		t.Fatalf("HTTPRoute Reconcile() error = %v", err)
	}
	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name},
	}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 1 || len(provider.deletedGateways) != 1 {
		t.Fatalf("provider deletions = routes %d, gateways %d; want one each", len(provider.deletedRoutes), len(provider.deletedGateways))
	}
}

func TestUnsupportedGatewayAPIVersionAllowsControllerHandoffCleanup(t *testing.T) {
	cfg := testConfig()
	otherClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 2},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "other.example/controller"},
	}
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	cachedClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(otherClass, gateway, route).
		Build()
	liveReader := liveReaderWithUnsupportedVersion(cachedClient)
	provider := &recordingProvider{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: cachedClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	routeReconciler := newTestReconciler(Reconciler{
		Client: cachedClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	routeRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	if result, err := routeReconciler.Reconcile(context.Background(), routeRequest); err != nil {
		t.Fatalf("first HTTPRoute Reconcile() error = %v", err)
	} else if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("first HTTPRoute Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatalf("route deletions before status checkpoint = %d, want 0", len(provider.deletedRoutes))
	}
	if _, err := routeReconciler.Reconcile(context.Background(), routeRequest); err != nil {
		t.Fatalf("second HTTPRoute Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 0 {
		t.Fatal("HTTPRoute deleted resources before its durable detach checkpoint")
	}
	if _, err := routeReconciler.Reconcile(context.Background(), routeRequest); err != nil {
		t.Fatalf("resume HTTPRoute handoff cleanup: %v", err)
	}
	if liveReader.crdListCalls != 0 {
		t.Fatalf("HTTPRoute handoff cleanup listed Gateway API CRDs %d times, want 0", liveReader.crdListCalls)
	}
	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 1 || len(provider.deletedGateways) != 1 {
		t.Fatalf("provider deletions = routes %d, gateways %d; want one each", len(provider.deletedRoutes), len(provider.deletedGateways))
	}
}

func TestVersionMismatchDoesNotTrustCachedControllerHandoff(t *testing.T) {
	cfg := testConfig()
	cachedClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 2},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "other.example/controller"},
	}
	liveClass := cachedClass.DeepCopy()
	liveClass.Spec.ControllerName = cfg.ControllerName
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	cachedClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(cachedClass, gateway, route).
		Build()
	liveReader := &unsupportedGatewayAPIVersionReader{Client: cachedClient, gatewayClass: liveClass}
	provider := &recordingProvider{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: cachedClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)
	routeReconciler := newTestReconciler(Reconciler{
		Client: cachedClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	if _, err := routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("HTTPRoute Reconcile() error = %v, want live ownership change", err)
	}
	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	if len(provider.deletedRoutes) != 0 || len(provider.deletedGateways) != 0 {
		t.Fatalf("provider deletions = routes %d, gateways %d; want none", len(provider.deletedRoutes), len(provider.deletedGateways))
	}
	var gotGateway gatewayv1.Gateway
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if !controller.GatewayHasControllerBinding(cfg, &gotGateway) {
		t.Fatal("Gateway binding was removed using a stale controller handoff")
	}
	var gotRoute gatewayv1.HTTPRoute
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(route), &gotRoute); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&gotRoute, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute binding was removed using a stale controller handoff")
	}
}

func TestVersionMismatchDoesNotWriteStatusAfterLiveVersionRecovery(t *testing.T) {
	cfg := testConfig()
	cachedClass := unsupportedVersionGatewayClass(cfg)
	liveClass := cachedClass.DeepCopy()
	SetCondition(&liveClass.Status.Conditions, Condition(
		string(gatewayv1.GatewayClassConditionStatusSupportedVersion),
		metav1.ConditionTrue,
		string(gatewayv1.GatewayClassReasonSupportedVersion),
		"Gateway API version is supported",
		liveClass.Generation,
	))
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(cachedClass, gateway, route).
		Build()
	liveReader := &unsupportedGatewayAPIVersionReader{
		Client: kubeClient, gatewayClass: liveClass, useClientCRDs: true,
	}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})
	routeReconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})

	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("Gateway Reconcile() error = %v, want changed graph inputs", err)
	}
	if _, err := routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("HTTPRoute Reconcile() error = %v, want %v", err, errHTTPRouteChanged)
	}
	var gotGateway gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if len(gotGateway.Status.Conditions) != 0 {
		t.Fatalf("Gateway conditions = %#v, want none after version recovery", gotGateway.Status.Conditions)
	}
	var gotRoute gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &gotRoute); err != nil {
		t.Fatal(err)
	}
	if len(gotRoute.Status.Parents) != 0 {
		t.Fatalf("HTTPRoute parent statuses = %#v, want none after version recovery", gotRoute.Status.Parents)
	}
}

func TestVersionMismatchDoesNotWriteStatusAfterLiveControllerHandoff(t *testing.T) {
	cfg := testConfig()
	cachedClass := unsupportedVersionGatewayClass(cfg)
	liveClass := cachedClass.DeepCopy()
	liveClass.Spec.ControllerName = "other.example/controller"
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(cachedClass, gateway, route).
		Build()
	liveReader := &unsupportedGatewayAPIVersionReader{
		Client: kubeClient, gatewayClass: liveClass, useClientCRDs: true,
	}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})
	routeReconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})

	if _, err := gatewayReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); err != nil {
		t.Fatalf("Gateway Reconcile() error = %v", err)
	}
	if _, err := routeReconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	}); err != nil {
		t.Fatalf("HTTPRoute Reconcile() error = %v", err)
	}
	var gotGateway gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &gotGateway); err != nil {
		t.Fatal(err)
	}
	if len(gotGateway.Status.Conditions) != 0 {
		t.Fatalf("Gateway conditions = %#v, want none after live controller handoff", gotGateway.Status.Conditions)
	}
	var gotRoute gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &gotRoute); err != nil {
		t.Fatal(err)
	}
	if len(gotRoute.Status.Parents) != 0 {
		t.Fatalf("HTTPRoute parent statuses = %#v, want none after live controller handoff", gotRoute.Status.Parents)
	}
}

func TestHTTPRouteBindingChecksControllerOwnershipAgainAfterEnsure(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	foreignClass := gatewayClass.DeepCopy()
	foreignClass.Spec.ControllerName = "other.example/controller"
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(gatewayClass, gateway, route, service, endpointSlice, node).
		Build()
	liveReader := &gatewayClassFlipReader{
		Client: kubeClient, className: gatewayClass.Name, afterFirst: foreignClass,
	}
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(route),
	})
	if !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("Reconcile() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls = %d, want 0 before the binding checkpoint", len(provider.routeSpecs))
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute received a binding after live controller handoff")
	}
}

func TestHTTPRouteProviderMutationRechecksControllerOwnership(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	foreignClass := gatewayClass.DeepCopy()
	foreignClass.Spec.ControllerName = "other.example/controller"
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(gatewayClass, gateway, route, service, endpointSlice, node).
		Build()
	liveReader := &gatewayClassFlipReader{
		Client: kubeClient, className: gatewayClass.Name, afterFirst: foreignClass,
	}
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	if _, err := reconciler.ensureRouteGraph(context.Background(), route, gateway); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("ensureRouteGraph() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls = %d, want 0 after live controller handoff", len(provider.routeSpecs))
	}
}

func TestHTTPRouteProviderMutationRechecksGatewayBinding(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	gateway := programmedTestGateway(cfg)
	changedGateway := gateway.DeepCopy()
	changedGateway.Annotations[cfg.GatewayListenerPortAnnotation()] = "443"
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, gateway); err != nil {
		t.Fatal(err)
	}
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(gatewayClass, gateway, route, service, endpointSlice, node).
		Build()
	liveReader := &gatewayBindingFlipReader{Client: kubeClient, afterFirst: changedGateway}
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, provider)

	if _, err := reconciler.ensureRouteGraph(context.Background(), route, gateway); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("ensureRouteGraph() error = %v, want %v", err, errHTTPRouteChanged)
	}
	if len(provider.routeSpecs) != 0 {
		t.Fatalf("EnsureRoute calls = %d, want 0 after the Gateway binding changed", len(provider.routeSpecs))
	}
}

func TestHTTPRouteBindingMutationRechecksControllerOwnership(t *testing.T) {
	cfg := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
	}
	foreignClass := gatewayClass.DeepCopy()
	foreignClass.Spec.ControllerName = "other.example/controller"
	gateway := programmedTestGateway(cfg)
	route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID = "route-uid"
	route.Generation = 1
	service, endpointSlice, node := validNodePortBackend("default", "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(route).
		WithObjects(gatewayClass, gateway, route, service, endpointSlice, node).
		Build()
	liveReader := &gatewayClassFlipReader{
		Client: kubeClient, className: gatewayClass.Name, afterFirst: foreignClass,
	}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: liveReader,
		Coordinator: &graph.Coordinator{}, Config: cfg,
	}, &recordingProvider{})

	if _, err := reconciler.bindRoute(context.Background(), route, gateway); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("bindRoute() error = %v, want %v", err, errHTTPRouteChanged)
	}
	var got gatewayv1.HTTPRoute
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &got); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) {
		t.Fatal("HTTPRoute received a binding after live controller handoff")
	}
}

func TestHTTPRouteCleanupRebuildsLiveDesiredState(t *testing.T) {
	tests := []struct {
		name            string
		mutateCached    func(*gatewayv1.Gateway, *corev1.Service)
		overrideGateway bool
		overrideService bool
	}{
		{
			name: "Gateway listener recovered",
			mutateCached: func(gateway *gatewayv1.Gateway, _ *corev1.Service) {
				gateway.Spec.Listeners[0].Protocol = gatewayv1.HTTPSProtocolType
			},
			overrideGateway: true,
		},
		{
			name: "backend Service recovered",
			mutateCached: func(_ *gatewayv1.Gateway, service *corev1.Service) {
				service.Spec.Type = corev1.ServiceTypeClusterIP
			},
			overrideService: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			gatewayClass := &gatewayv1.GatewayClass{
				ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
				Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
			}
			liveGateway := programmedTestGateway(cfg)
			cachedGateway := liveGateway.DeepCopy()
			liveService, endpointSlice, node := validNodePortBackend("default", "backend")
			cachedService := liveService.DeepCopy()
			test.mutateCached(cachedGateway, cachedService)
			route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID = "route-uid"
			route.Generation = 1
			if err := (newTestReconciler(Reconciler{Config: cfg}, nil)).applyRouteBinding(route, liveGateway); err != nil {
				t.Fatal(err)
			}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithStatusSubresource(route).
				WithObjects(gatewayClass, cachedGateway, route, cachedService, endpointSlice, node).
				Build()
			liveReader := &liveObjectOverrideReader{Client: kubeClient}
			if test.overrideGateway {
				liveReader.gateway = liveGateway
			}
			if test.overrideService {
				liveReader.service = liveService
			}
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{
				Client: kubeClient, APIReader: liveReader,
				Coordinator: &graph.Coordinator{}, Config: cfg,
			}, provider)

			request := ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(route),
			}
			result, err := reconciler.Reconcile(context.Background(), request)
			if err != nil {
				t.Fatalf("first Reconcile() error = %v", err)
			}
			if result != (ctrl.Result{Requeue: true}) {
				t.Fatalf("first Reconcile() result = %#v, want status checkpoint", result)
			}
			if len(provider.deletedRoutes) != 0 {
				t.Fatalf("DeleteRoute calls before status checkpoint = %d, want 0", len(provider.deletedRoutes))
			}

			_, err = reconciler.Reconcile(context.Background(), request)
			if !errors.Is(err, errHTTPRouteChanged) {
				t.Fatalf("second Reconcile() error = %v, want %v", err, errHTTPRouteChanged)
			}
			if len(provider.deletedRoutes) != 0 {
				t.Fatalf("DeleteRoute calls = %d, want 0", len(provider.deletedRoutes))
			}
			var got gatewayv1.HTTPRoute
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(route), &got); err != nil {
				t.Fatal(err)
			}
			if !controllerutil.ContainsFinalizer(&got, cfg.RouteFinalizer()) {
				t.Fatal("HTTPRoute binding was removed using stale dependency state")
			}
		})
	}
}

func validNodePortBackend(namespace, name string) (*corev1.Service, *discoveryv1.EndpointSlice, *corev1.Node) {
	ready := true
	nodeName := "worker-0"
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080, Protocol: corev1.ProtocolTCP}},
		},
	}
	endpointSlice := &discoveryv1.EndpointSlice{
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name + "-1",
			Labels:    map[string]string{discoveryv1.LabelServiceName: name},
		},
		Endpoints: []discoveryv1.Endpoint{{
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			NodeName:   &nodeName,
		}},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	return service, endpointSlice, node
}

func unsupportedVersionGatewayClass(cfg Config) *gatewayv1.GatewayClass {
	return &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
		Status: gatewayv1.GatewayClassStatus{Conditions: []metav1.Condition{Condition(
			string(gatewayv1.GatewayClassConditionStatusSupportedVersion),
			metav1.ConditionFalse,
			string(gatewayv1.GatewayClassReasonUnsupportedVersion),
			"unsupported test bundle",
			1,
		)}},
	}
}

func unsupportedGatewayAPICRDs() []client.Object {
	definitions := supportedGatewayAPICRDs()
	for _, definition := range definitions {
		if definition.GetName() == "httproutes.gateway.networking.k8s.io" {
			definition.SetAnnotations(map[string]string{gatewayconsts.BundleVersionAnnotation: "v1.5.0"})
		}
	}
	return definitions
}

type gatewayClassFlipReader struct {
	client.Client
	mu         sync.Mutex
	className  string
	classGets  int
	afterFirst *gatewayv1.GatewayClass
}

type gatewayBindingFlipReader struct {
	client.Client
	mu          sync.Mutex
	gatewayGets int
	afterFirst  *gatewayv1.Gateway
}

func (r *gatewayBindingFlipReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if gateway, ok := object.(*gatewayv1.Gateway); ok && key == client.ObjectKeyFromObject(r.afterFirst) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.gatewayGets++
		if r.gatewayGets > 1 {
			r.afterFirst.DeepCopyInto(gateway)
			return nil
		}
	}
	return r.Client.Get(ctx, key, object, opts...)
}

func (r *gatewayClassFlipReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if gatewayClass, ok := object.(*gatewayv1.GatewayClass); ok && key.Name == r.className {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.classGets++
		if r.classGets > 1 {
			r.afterFirst.DeepCopyInto(gatewayClass)
			return nil
		}
	}
	return r.Client.Get(ctx, key, object, opts...)
}

type liveObjectOverrideReader struct {
	client.Client
	gateway *gatewayv1.Gateway
	service *corev1.Service
	route   *gatewayv1.HTTPRoute
}

func (r *liveObjectOverrideReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	switch current := object.(type) {
	case *gatewayv1.Gateway:
		if r.gateway != nil && key == client.ObjectKeyFromObject(r.gateway) {
			r.gateway.DeepCopyInto(current)
			return nil
		}
	case *corev1.Service:
		if r.service != nil && key == client.ObjectKeyFromObject(r.service) {
			r.service.DeepCopyInto(current)
			return nil
		}
	case *gatewayv1.HTTPRoute:
		if r.route != nil && key == client.ObjectKeyFromObject(r.route) {
			r.route.DeepCopyInto(current)
			return nil
		}
	}
	return r.Client.Get(ctx, key, object, opts...)
}

type unsupportedGatewayAPIVersionReader struct {
	client.Client
	gatewayClass  *gatewayv1.GatewayClass
	useClientCRDs bool
	crdListCalls  int
}

func (r *unsupportedGatewayAPIVersionReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	gatewayClass, ok := object.(*gatewayv1.GatewayClass)
	if ok && r.gatewayClass != nil && key.Name == r.gatewayClass.Name {
		r.gatewayClass.DeepCopyInto(gatewayClass)
		return nil
	}
	return r.Client.Get(ctx, key, object, opts...)
}

func (r *unsupportedGatewayAPIVersionReader) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	definitions, ok := list.(*apiextensionsv1.CustomResourceDefinitionList)
	metadata, metadataOK := list.(*metav1.PartialObjectMetadataList)
	if (!ok && !metadataOK) || r.useClientCRDs {
		return r.Client.List(ctx, list, opts...)
	}
	r.crdListCalls++
	if ok {
		definitions.Items = nil
	}
	if metadataOK {
		metadata.Items = nil
	}
	for _, object := range unsupportedGatewayAPICRDs() {
		definition := object.(*apiextensionsv1.CustomResourceDefinition)
		if ok {
			definitions.Items = append(definitions.Items, *definition.DeepCopy())
		}
		if metadataOK {
			metadata.Items = append(metadata.Items, metav1.PartialObjectMetadata{
				ObjectMeta: *definition.ObjectMeta.DeepCopy(),
			})
		}
	}
	return nil
}

func liveReaderWithUnsupportedVersion(kubeClient client.Client) *unsupportedGatewayAPIVersionReader {
	return &unsupportedGatewayAPIVersionReader{Client: kubeClient}
}
