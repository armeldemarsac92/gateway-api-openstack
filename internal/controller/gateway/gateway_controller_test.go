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
	"errors"
	"reflect"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestValidateDistinctUDPListeners(t *testing.T) {
	gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{
		{Name: "private", Port: 5353, Protocol: gatewayv1.UDPProtocolType},
		{Name: "public", Port: 53, Protocol: gatewayv1.UDPProtocolType},
	}}}
	if _, err := Validate(gateway); err != nil {
		t.Fatalf("distinct UDP listeners must be supported: %v", err)
	}
	gateway.Spec.Listeners[1].Port = 5353
	if _, err := Validate(gateway); err == nil {
		t.Fatal("UDP listeners sharing a port must be rejected")
	}
}

func TestSharedHTTPPortRejectsUnsupportedListenerShapes(t *testing.T) {
	first := gatewayv1.Listener{Name: "first", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("a.example.com"))}
	for _, second := range []gatewayv1.Listener{
		{Name: "same-host", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: first.Hostname},
		{Name: "https", Port: 80, Protocol: gatewayv1.HTTPSProtocolType, Hostname: ptr.To(gatewayv1.Hostname("b.example.com"))},
	} {
		t.Run(string(second.Name), func(t *testing.T) {
			gw := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{first, second}}}
			if _, err := SupportedListeners(gw); err == nil {
				t.Fatal("unsupported HTTP listener set was admitted")
			}
		})
	}
}

func TestSharedHTTPPortAdmitsExactNestedWildcardAndCatchAll(t *testing.T) {
	listeners := []gatewayv1.Listener{
		{Name: "exact", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("foo.bar.com"))},
		{Name: "nested", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("*.foo.bar.com"))},
		{Name: "wildcard", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("*.bar.com"))},
		{Name: "catch-all", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
	}
	gw := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: listeners}}
	admitted, err := SupportedListeners(gw)
	if err != nil || len(admitted) != len(listeners) {
		t.Fatalf("listeners = %v, error = %v", admitted, err)
	}
	want := []string{"*", "*.bar.com", "*.foo.bar.com", "foo.bar.com"}
	if got := HTTPListenerHostnames(admitted); !slices.Equal(got[80], want) {
		t.Fatalf("hostname selectors = %v, want %v", got[80], want)
	}
	gw.Spec.Listeners[0].Hostname = nil
	if _, err := SupportedListeners(gw); err == nil {
		t.Fatal("duplicate catch-all sections were admitted")
	}
}

func TestBackendTLSOwnershipSurvivesOptOutAndIsUsedAtDeletion(t *testing.T) {
	cfg := testConfig()
	cfg.BackendTLSPolicy = true
	gw := programmedTestGateway(cfg)
	completeGatewayTopologyFixture(cfg, gw)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(class, gw).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	observe := func(action graph.Action) graph.GatewayInput {
		t.Helper()
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(gw), gw); err != nil {
			t.Fatal(err)
		}
		input, err := r.ObserveGatewayGraph(t.Context(), graph.Request{Identity: controller.GatewayIdentity(cfg, gw), Action: action, Gateway: gw})
		if err != nil {
			t.Fatal(err)
		}
		return input
	}
	before := observe(graph.Ensure)
	if before.Node.BackendCAOwned || before.Node.ClaimBackendCA == nil {
		t.Fatal("new Gateway must checkpoint before publishing its first CA")
	}
	if err := before.Node.ClaimBackendCA(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := before.Validate(t.Context()); err == nil {
		t.Fatal("old snapshot was allowed to publish after the checkpoint")
	}
	r.Config.BackendTLSPolicy = false
	if after := observe(graph.Ensure); !after.Node.BackendCAOwned || after.Node.ClaimBackendCA != nil {
		t.Fatal("opt-out lost ownership of previously published CAs")
	}
	tls := &gatewayv1.GatewayTLSConfig{Backend: &gatewayv1.GatewayBackendTLS{
		ClientCertificateRef: &gatewayv1.SecretObjectReference{Name: "backend-client"},
	}}
	unbound := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: gw.Spec.Listeners, TLS: tls}}
	if _, err := SupportedListenersForConfig(r.Config, unbound); err == nil {
		t.Fatal("a new Gateway requested a client identity without policy observation")
	}
	gw.Spec.TLS = tls
	if err := kube.Update(t.Context(), gw); err != nil {
		t.Fatal(err)
	}
	if _, err := SupportedListenersForConfig(r.Config, gw); err != nil {
		t.Fatalf("opt-out invalidated the listener of the bound Gateway: %v", err)
	}
	withdrawal := observe(graph.WithdrawBackendClientCertificate)
	if withdrawal.Node.Disposition != cloud.GraphWithdrawBackendClientCertificate ||
		!withdrawal.Node.BackendClientCertificate.Denied || withdrawal.Validate(t.Context()) != nil {
		t.Fatal("opt-out did not preserve narrow withdrawal authority for the bound Gateway")
	}
	if err := kube.Delete(t.Context(), gw); err != nil {
		t.Fatal(err)
	}
	if removal := observe(graph.DeleteGateway); !removal.Node.BackendCAOwned || removal.Node.ClaimBackendCA != nil {
		t.Fatal("Gateway finalization lost its CA cleanup obligation")
	}
}

func TestBackendClientCertificateRemovalKeepsCurrentListenerFailure(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Status: gatewayv1.GatewayStatus{
			Conditions: []metav1.Condition{controller.Condition(string(gatewayv1.GatewayConditionResolvedRefs),
				metav1.ConditionTrue, string(gatewayv1.GatewayReasonResolvedRefs), "Client certificate enrolled", 1)},
			Listeners: []gatewayv1.ListenerStatus{{Name: "https", Conditions: []metav1.Condition{
				controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), metav1.ConditionFalse,
					string(gatewayv1.ListenerReasonInvalidCertificateRef), "Certificate unavailable", 2),
			}}},
		},
	}
	(&Reconciler{}).setBackendClientCertificateStatus(t.Context(), gw)
	got := meta.FindStatusCondition(gw.Status.Conditions, string(gatewayv1.GatewayConditionResolvedRefs))
	if got == nil || got.Status != metav1.ConditionFalse || got.Reason != string(gatewayv1.GatewayReasonListenersNotResolved) || got.ObservedGeneration != 2 {
		t.Fatalf("lost current listener failure while removing backend identity: %+v", got)
	}
}

func TestScopedUDPRouteCleanupUsesItsSealedAnchorWithoutGateway(t *testing.T) {
	config := testConfig()
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "old-gateway"}}
	route := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dns", UID: "udp-route"}}
	identity := controller.RouteIdentity(config, gateway, route)
	controller.ApplyRouteBinding(config, route, identity)
	controller.SealRouteAnchor(config, route, identity, 5353)
	route.Annotations[config.RouteLifecycleAnnotation()] = controller.RouteDetaching
	kube := indexedFakeClientBuilder(testScheme(t), config).Build()
	r := &Reconciler{APIReader: kube, Config: config}
	request := graph.Request{Identity: identity, Action: graph.DeleteRoute, UDPRoute: route}

	input, err := r.ObserveGatewayGraph(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	spec := input.Node.Spec
	if input.Node.Disposition != cloud.GraphPreserve || spec.ScopedListeners == nil || len(spec.ScopedListeners) != 0 ||
		spec.ListenerPort != 5353 || spec.ListenerProtocol != cloud.ListenerProtocolUDP {
		t.Fatalf("cleanup lost its exact scoped anchor: %+v", input.Node)
	}
	gateway.Annotations = gatewayBindingAnnotations(config, "443")
	gateway.Annotations[config.GatewayListenerProtocolAnnotation()] = cloud.ListenerProtocolTCP
	gateway.Finalizers = []string{config.GatewayFinalizer()}
	if err := controller.BindGatewayTopology(config, gateway, testBoundTopology()); err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	input, err = r.ObserveGatewayGraph(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if input.Node.Disposition != cloud.GraphPreserve || input.Node.Spec.ScopedListeners != nil || input.Node.Spec.ListenerPort != 0 {
		t.Fatalf("a verified protocol replacement should preserve its current graph: %+v", input.Node)
	}

	delete(route.Annotations, config.RouteListenerPortAnnotation(gatewayv1.UDPProtocolType))
	if _, err := r.ObserveGatewayGraph(t.Context(), request); err == nil {
		t.Fatal("a damaged route checkpoint fell back to legacy cleanup")
	}
}

func TestGatewayRequestsForHTTPRoute(t *testing.T) {
	config := testConfig()
	gatewayNamespace := gatewayv1.Namespace("gateway-system")
	otherGroup := gatewayv1.Group("example.com")
	otherKind := gatewayv1.Kind("Other")
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "api",
			Annotations: map[string]string{
				config.RouteGatewayNamespaceAnnotation(): "previous",
				config.RouteGatewayNameAnnotation():      "bound",
				config.RouteGatewayUIDAnnotation():       "bound-uid",
			},
		},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{
			{Name: "edge"},
			{Name: "edge"},
			{Name: "shared", Namespace: &gatewayNamespace},
			{Name: "other-group", Group: &otherGroup},
			{Name: "other-kind", Kind: &otherKind},
		}}},
		Status: gatewayv1.HTTPRouteStatus{RouteStatus: gatewayv1.RouteStatus{Parents: []gatewayv1.RouteParentStatus{
			{ParentRef: gatewayv1.ParentReference{Name: "status-parent"}, ControllerName: config.ControllerName},
			{ParentRef: gatewayv1.ParentReference{Name: "foreign-status"}, ControllerName: "example.com/foreign"},
		}}},
	}

	reconciler := newTestReconciler(Reconciler{Config: config}, nil)
	requests := reconciler.gatewayRequestsForRoute(context.Background(), route)
	want := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "edge"}},
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "status-parent"}},
		{NamespacedName: types.NamespacedName{Namespace: "gateway-system", Name: "shared"}},
		{NamespacedName: types.NamespacedName{Namespace: "previous", Name: "bound"}},
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("gatewayRequestsForRoute() = %#v, want %#v", requests, want)
	}
	if requests := reconciler.gatewayRequestsForRoute(context.Background(), &gatewayv1.Gateway{}); requests != nil {
		t.Fatalf("requests for non-HTTPRoute object = %#v, want nil", requests)
	}
}

func TestGatewayRequestsForGatewayClass(t *testing.T) {
	matchingGateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "openstack"},
	}
	otherGateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "other"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "other"},
	}
	kubeClient := indexedFakeClientBuilder(testScheme(t), testConfig()).
		WithObjects(matchingGateway, otherGateway).
		Build()
	reconciler := newTestReconciler(Reconciler{Client: kubeClient}, nil)

	requests := reconciler.gatewayRequestsForGatewayClass(context.Background(), &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack"},
	})
	want := []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(matchingGateway)}}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("gatewayRequestsForGatewayClass() = %#v, want %#v", requests, want)
	}
	if requests := reconciler.gatewayRequestsForGatewayClass(context.Background(), matchingGateway); requests != nil {
		t.Fatalf("requests for non-GatewayClass object = %#v, want nil", requests)
	}
}

func TestGatewayWatchesUDPRouteStatusAndPreviousBinding(t *testing.T) {
	config := testConfig()
	reconciler := newTestReconciler(Reconciler{Config: config}, nil)
	old := &gatewayv1.UDPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "backend", Annotations: map[string]string{
			config.RouteGatewayNamespaceAnnotation(): "old-gateways",
			config.RouteGatewayNameAnnotation():      "old-edge",
			config.RouteGatewayUIDAnnotation():       "old-uid",
		}},
	}
	status := old.DeepCopy()
	status.Status.Parents = []gatewayv1.RouteParentStatus{
		{ParentRef: gatewayv1.ParentReference{Name: "edge"}, ControllerName: config.ControllerName},
		{ParentRef: gatewayv1.ParentReference{Name: "foreign"}, ControllerName: "example.com/foreign"},
	}
	unbound := old.DeepCopy()
	unbound.Annotations = nil
	predicate := controller.RouteForGatewayPredicate(config)
	if !predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: status}) ||
		!predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: unbound}) {
		t.Fatal("status or binding update was filtered out")
	}
	requests := reconciler.gatewayRequestsForRoute(t.Context(), status)
	want := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "apps", Name: "edge"}},
		{NamespacedName: types.NamespacedName{Namespace: "old-gateways", Name: "old-edge"}},
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("Gateway requests = %v, want %v", requests, want)
	}
}

func TestCleanupGatewayRejectsStaleResourceVersion(t *testing.T) {
	config := testConfig()
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "edge",
			UID:         "gateway-uid",
			Finalizers:  []string{config.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(config, "80"),
		},
	}
	kubeClient := indexedFakeClientBuilder(testScheme(t), config).
		WithObjects(gateway).
		Build()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, Config: config}, provider)

	expected := gateway.DeepCopy()
	expected.ResourceVersion = "stale-resource-version"
	scope := &gatewayScope{gateway: expected, original: expected.DeepCopy()}
	_, err := reconciler.cleanupGateway(context.Background(), scope)
	if !apierrors.IsConflict(err) {
		t.Fatalf("cleanupGateway() error = %v, want conflict", err)
	}
	if len(provider.deletedGateways) != 0 {
		t.Fatalf("DeleteGateway calls = %d, want 0", len(provider.deletedGateways))
	}
}

func TestGatewayBindingWithoutFinalizerRemainsCleanupResponsibility(t *testing.T) {
	config := testConfig()
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/other-controller"},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "edge",
			UID:         "gateway-uid",
			Annotations: gatewayBindingAnnotations(config, "80"),
		},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack"},
	}
	kubeClient := indexedFakeClientBuilder(testScheme(t), config).
		WithObjects(gatewayClass, gateway).
		Build()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{
		Client: kubeClient, APIReader: kubeClient,
		Coordinator: &graph.Coordinator{}, Config: config,
	}, provider)

	if _, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(gateway),
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(provider.deletedGateways) != 1 {
		t.Fatalf("DeleteGateway calls = %d, want 1", len(provider.deletedGateways))
	}
	var current gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), &current); err != nil {
		t.Fatal(err)
	}
	if controller.GatewayHasControllerBinding(config, &current) {
		t.Fatalf("Gateway binding remained after cleanup: finalizers %#v, annotations %#v", current.Finalizers, current.Annotations)
	}
}

func TestGatewayScopeRefreshRejectsConcurrentStatusChange(t *testing.T) {
	config := testConfig()
	original := programmedTestGateway(config)
	original.ResourceVersion = "1"
	desired := original.DeepCopy()
	SetCondition(&desired.Status.Conditions, Condition(
		string(gatewayv1.GatewayConditionAccepted),
		metav1.ConditionFalse,
		string(gatewayv1.GatewayReasonInvalid),
		"Gateway configuration changed",
		desired.Generation,
	))
	live := original.DeepCopy()
	live.ResourceVersion = "2"
	live.Status.Listeners = []gatewayv1.ListenerStatus{{
		Name: "http",
		Conditions: []metav1.Condition{Condition(
			"example.net/Observed",
			metav1.ConditionTrue,
			"Observed",
			"Preserve this condition",
			live.Generation,
		)},
	}}
	scope := &gatewayScope{gateway: desired, original: original}

	if err := scope.refresh(live); !errors.Is(err, errGatewayChanged) {
		t.Fatalf("refresh() error = %v, want %v", err, errGatewayChanged)
	}
	if !reflect.DeepEqual(scope.gateway.Status, desired.Status) || scope.gateway.ResourceVersion != "1" {
		t.Fatalf("refresh() changed scope after conflict: %#v", scope.gateway)
	}
}
