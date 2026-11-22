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
	"errors"
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestHTTPRouteUsesOnlyManagedParent(t *testing.T) {
	for _, test := range []struct {
		name          string
		foreignFirst  bool
		foreignExists bool
	}{
		{name: "missing Gateway after managed"},
		{name: "foreign Gateway before managed", foreignFirst: true, foreignExists: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			class, gateway, route, service, endpoints := endpointRouteFixture(t)
			managedRef := route.Spec.ParentRefs[0]
			managedRef.Namespace = ptr.To(gatewayv1.Namespace(gateway.Namespace))
			managedRef.SectionName = ptr.To(gateway.Spec.Listeners[0].Name)
			managedRef.Port = ptr.To(gateway.Spec.Listeners[0].Port)
			foreignRef := gatewayv1.ParentReference{Name: "other"}
			route.Spec.ParentRefs = []gatewayv1.ParentReference{managedRef, foreignRef}
			if test.foreignFirst {
				slices.Reverse(route.Spec.ParentRefs)
			}
			foreignStatus := gatewayv1.RouteParentStatus{
				ParentRef: foreignRef, ControllerName: "other.example/controller",
				Conditions: []metav1.Condition{controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Foreign status", route.Generation)},
			}
			route.Status.Parents = []gatewayv1.RouteParentStatus{foreignStatus}
			route.Generation++
			builder := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(class, gateway, route, service, endpoints)
			if test.foreignExists {
				otherClass := &gatewayv1.GatewayClass{
					ObjectMeta: metav1.ObjectMeta{Name: "foreign"},
					Spec:       gatewayv1.GatewayClassSpec{ControllerName: foreignStatus.ControllerName},
				}
				other := gateway.DeepCopy()
				other.Name, other.UID, other.Spec.GatewayClassName = string(foreignRef.Name), "other-uid", gatewayv1.ObjectName(otherClass.Name)
				builder.WithObjects(otherClass, other)
			}
			kubeClient := builder.Build()
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
			for range 2 {
				if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			var current gatewayv1.HTTPRoute
			if err := kubeClient.Get(t.Context(), req.NamespacedName, &current); err != nil {
				t.Fatal(err)
			}
			if len(current.Status.Parents) != 2 {
				t.Fatalf("parent status = %#v, want one entry per controller", current.Status.Parents)
			}
			for _, status := range current.Status.Parents {
				switch status.ControllerName {
				case foreignStatus.ControllerName:
					if !reflect.DeepEqual(status, foreignStatus) {
						t.Fatalf("foreign status changed from %#v to %#v", foreignStatus, status)
					}
				case cfg.ControllerName:
					accepted := meta.FindStatusCondition(status.Conditions, "Accepted")
					if !reflect.DeepEqual(status.ParentRef, managedRef) || accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != current.Generation {
						t.Fatalf("managed parent %v has Accepted=%#v, want current Accepted=True for %v", status.ParentRef, accepted, managedRef)
					}
				default:
					t.Fatalf("unexpected status entry %#v", status)
				}
			}
			want := controller.RouteIdentity(cfg, gateway, route)
			if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Identity != want ||
				current.Annotations[cfg.RouteGatewayUIDAnnotation()] != string(gateway.UID) {
				t.Fatalf("want one route graph for %v, got %v with binding %q", want, provider.routeSpecs, current.Annotations[cfg.RouteGatewayUIDAnnotation()])
			}
		})
	}
}

func TestHTTPRouteGraphRemainsValidWhenAnotherGatewayBecomesManaged(t *testing.T) {
	f := newRouteModelInputFixture(t)
	other := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.gateway.Namespace, Name: "other", UID: "other-uid"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "late-class"},
	}
	f.route.Spec.ParentRefs = append(f.route.Spec.ParentRefs, gatewayv1.ParentReference{Name: gatewayv1.ObjectName(other.Name)})
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	f.create(t, other)
	input := f.observe(t)
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure {
		t.Fatalf("with the other GatewayClass absent, want one ensured route, got %v", input.Nodes)
	}

	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: string(other.Spec.GatewayClassName)},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: f.reconciler.Config.ControllerName},
	}
	f.create(t, class)
	key := client.ObjectKeyFromObject(f.route)
	if requests := f.reconciler.enqueueHTTPRoutesForGatewayClass(t.Context(), class); !slices.Equal(requests, []ctrl.Request{{NamespacedName: key}}) {
		t.Fatalf("new GatewayClass should enqueue %v, got %v", key, requests)
	}
	if err := input.Validate(t.Context()); err != nil {
		t.Fatalf("the other GatewayClass must not invalidate this Gateway's graph: %v", err)
	}
	identity := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	cleanup, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, identity)
	if err != nil || cleanup.disposition != routeCleanupStillDesired {
		t.Fatalf("the newly managed parent must not detach the original fragment, got %v and error %v", cleanup.disposition, err)
	}
}

func TestHTTPRouteRechecksBundleCompatibilityAtCleanupSeal(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.route.Spec.ParentRefs = append(f.route.Spec.ParentRefs, gatewayv1.ParentReference{Name: "missing"})
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	identity := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	reader := liveReaderWithUnsupportedVersion(f.client)
	f.reconciler.APIReader = reader
	cleanup, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, identity)
	if err != nil || cleanup.disposition != routeCleanupRequired {
		t.Fatalf("cleanup with an unsupported bundle = %v, %v, want required cleanup", cleanup.disposition, err)
	}
	if err := f.reconciler.sealRouteCleanupSnapshot(t.Context(), cleanup); err != nil {
		t.Fatalf("cleanup seal with the bundle still unsupported = %v, want success", err)
	}

	reader.useClientCRDs = true
	if err := f.reconciler.sealRouteCleanupSnapshot(t.Context(), cleanup); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("cleanup seal after bundle recovery = %v, want stale cleanup", err)
	}
	current, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, identity)
	if err != nil || current.disposition != routeCleanupStillDesired {
		t.Fatalf("cleanup after bundle recovery = %v, %v, want forwarding to remain desired", current.disposition, err)
	}
}

func TestHTTPRouteRechecksSingletonGatewayClassRecoveryAtCleanupSeal(t *testing.T) {
	f := newRouteModelInputFixture(t)
	class := unsupportedVersionGatewayClass(f.reconciler.Config)
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(class), class); err != nil {
		t.Fatal(err)
	}
	supported := class.Status.DeepCopy()
	class.Status = unsupportedVersionGatewayClass(f.reconciler.Config).Status
	if err := f.client.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	identity := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	cleanup, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, identity)
	if err != nil || cleanup.disposition != routeCleanupRequired {
		t.Fatalf("cleanup with an unsupported class = %v, %v, want required cleanup", cleanup.disposition, err)
	}
	class.Status = *supported
	if err := f.client.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.sealRouteCleanupSnapshot(t.Context(), cleanup); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("cleanup seal after class recovery = %v, want stale cleanup", err)
	}
}

func TestHTTPRouteRefusesSeveralRefsToSameGateway(t *testing.T) {
	cfg := testConfig()
	class, gateway, route, service, endpoints := endpointRouteFixture(t)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "other", Protocol: gatewayv1.HTTPProtocolType, Port: 81})
	route.Spec.ParentRefs[0].SectionName = ptr.To(gateway.Spec.Listeners[0].Name)
	route.Spec.ParentRefs = append(route.Spec.ParentRefs, gatewayv1.ParentReference{
		Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("other")),
	})
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(class, gateway, route, service, endpoints).Build()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	var current gatewayv1.HTTPRoute
	if err := kubeClient.Get(t.Context(), req.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Parents) != 2 || len(current.Finalizers) != 0 || len(provider.routeSpecs) != 0 {
		t.Fatalf("want two rejections with no binding or forwarding, got status=%#v finalizers=%v route specs=%v",
			current.Status, current.Finalizers, provider.routeSpecs)
	}
	for _, status := range current.Status.Parents {
		accepted := meta.FindStatusCondition(status.Conditions, "Accepted")
		if status.ControllerName != cfg.ControllerName || accepted == nil || accepted.Status != metav1.ConditionFalse ||
			accepted.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
			t.Fatalf("managed parent %v has Accepted=%#v, want False/UnsupportedValue", status.ParentRef, accepted)
		}
	}
}

func TestHTTPAndGRPCRouteMultipleGatewayLifecycle(t *testing.T) {
	for _, kind := range []gatewayv1.Kind{model.HTTPRouteKind, model.GRPCRouteKind} {
		t.Run(string(kind), func(t *testing.T) {
			cfg := testConfig()
			class, first, http, service, endpoints := endpointRouteFixture(t)
			second := first.DeepCopy()
			second.Namespace, second.Name, second.UID = "infra-two", "other", "other-uid"
			second.Status.Conditions = nil
			second.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{
				Namespaces: &gatewayv1.RouteNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)},
			}
			firstRef := http.Spec.ParentRefs[0]
			secondRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(second.Name), Namespace: ptr.To(gatewayv1.Namespace(second.Namespace))}
			meshRef := gatewayv1.ParentReference{Group: ptr.To(gatewayv1.Group("")), Kind: ptr.To(gatewayv1.Kind("Service")), Name: "mesh"}
			meshStatus := gatewayv1.RouteParentStatus{
				ParentRef: meshRef, ControllerName: cfg.ControllerName,
				Conditions: []metav1.Condition{controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Mesh status", http.Generation)},
			}
			http.Spec.ParentRefs = []gatewayv1.ParentReference{firstRef, meshRef}
			http.Status.Parents = []gatewayv1.RouteParentStatus{meshStatus}
			var route client.Object = http
			if kind == model.GRPCRouteKind {
				cfg.BackendH2C, cfg.FixedHTTPResponses = true, true
				service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
				grpc := &gatewayv1.GRPCRoute{ObjectMeta: http.ObjectMeta, Status: gatewayv1.GRPCRouteStatus{RouteStatus: http.Status.RouteStatus}}
				grpc.Spec.CommonRouteSpec = http.Spec.CommonRouteSpec
				grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule("backend")}
				route = grpc
			}
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: http.Namespace, UID: "routes-namespace-uid"}}
			kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route, second).
				WithObjects(class, first, second, route, service, endpoints, namespace).Build()
			provider := &recordingProvider{}
			reconciler := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, provider).forRouteKind(kind)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
			reconcile := func(times int) {
				t.Helper()
				for range times {
					if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
						t.Fatal(err)
					}
				}
			}
			get := func() {
				t.Helper()
				if err := kube.Get(t.Context(), req.NamespacedName, route); err != nil {
					t.Fatal(err)
				}
			}
			updateParents := func(refs ...gatewayv1.ParentReference) {
				t.Helper()
				switch object := route.(type) {
				case *gatewayv1.HTTPRoute:
					object.Spec.ParentRefs = refs
				case *gatewayv1.GRPCRoute:
					object.Spec.ParentRefs = refs
				}
				route.SetGeneration(route.GetGeneration() + 1)
				if err := kube.Update(t.Context(), route); err != nil {
					t.Fatal(err)
				}
			}
			assertProgrammed := func(want map[gatewayv1.ObjectName]metav1.ConditionStatus) {
				t.Helper()
				_, statuses, _ := controller.RouteParents(route)
				if len(statuses) != len(want)+1 {
					t.Fatalf("parent statuses = %#v, expected %v and the mesh status", statuses, want)
				}
				for _, status := range statuses {
					if status.ParentRef.Name == meshRef.Name {
						if !reflect.DeepEqual(status, meshStatus) {
							t.Fatalf("mesh status changed: %#v", status)
						}
						continue
					}
					programmed := meta.FindStatusCondition(status.Conditions, cfg.Domain()+"/Programmed")
					accepted := meta.FindStatusCondition(status.Conditions, string(gatewayv1.RouteConditionAccepted))
					resolved := meta.FindStatusCondition(status.Conditions, string(gatewayv1.RouteConditionResolvedRefs))
					if programmed == nil || programmed.Status != want[status.ParentRef.Name] || programmed.ObservedGeneration != route.GetGeneration() ||
						accepted == nil || accepted.Status != metav1.ConditionTrue ||
						resolved == nil || resolved.Status != metav1.ConditionTrue {
						t.Fatalf("unexpected conditions for %v: %v", status.ParentRef, status.Conditions)
					}
				}
			}
			firstID := controller.RouteIdentity(cfg, first, route)
			secondID := controller.RouteIdentity(cfg, second, route)

			reconcile(2)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{firstRef.Name: metav1.ConditionTrue})
			if route.GetAnnotations()[cfg.RouteGatewayUIDAnnotation()] != string(first.UID) {
				t.Fatal("expected a persisted legacy singleton before adding the second parent")
			}

			updateParents(firstRef, secondRef, meshRef)
			reconcile(1)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{firstRef.Name: metav1.ConditionTrue, secondRef.Name: metav1.ConditionFalse})
			for _, spec := range provider.routeSpecs {
				if spec.Identity != firstID {
					t.Fatalf("unready Gateway was programmed: %v", spec.Identity)
				}
			}
			if err := kube.Get(t.Context(), client.ObjectKeyFromObject(second), second); err != nil {
				t.Fatal(err)
			}
			second.Status.Conditions = first.Status.Conditions
			if err := kube.Status().Update(t.Context(), second); err != nil {
				t.Fatal(err)
			}
			reconcile(2)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{firstRef.Name: metav1.ConditionTrue, secondRef.Name: metav1.ConditionTrue})
			bindings, err := controller.StoredRouteGatewayBindings(cfg, route)
			if err != nil || len(bindings) != 2 || len(route.GetFinalizers()) != 3 {
				t.Fatalf("expected two sealed checkpoints, got %v, %v, %v", bindings, route.GetFinalizers(), err)
			}
			if !slices.ContainsFunc(provider.routeSpecs, func(spec cloud.RouteSpec) bool { return spec.Identity == secondID }) {
				t.Fatalf("no provider ensure for the second Gateway: %v", provider.routeSpecs)
			}

			updateParents(secondRef, meshRef)
			reconcile(2)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{secondRef.Name: metav1.ConditionTrue})
			bindings, err = controller.StoredRouteGatewayBindings(cfg, route)
			if err != nil || len(bindings) != 1 || bindings[0].Identity != secondID ||
				!slices.Equal(provider.deletedRoutes, []cloud.Identity{firstID}) || len(route.GetFinalizers()) != 2 {
				t.Fatalf("removing one parent affected the wrong checkpoint: %v, %v, %v, %v", bindings, route.GetFinalizers(), provider.deletedRoutes, err)
			}

			updateParents(firstRef, secondRef, meshRef)
			reconcile(2)
			get()
			replacement := first.DeepCopy()
			replacement.UID = "replacement-gateway-uid"
			replacement.ResourceVersion = ""
			if err := kube.Delete(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			reconcile(3)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{firstRef.Name: metav1.ConditionFalse, secondRef.Name: metav1.ConditionTrue})
			bindings, err = controller.StoredRouteGatewayBindings(cfg, route)
			if err != nil || len(bindings) != 1 || bindings[0].Identity != secondID ||
				!slices.Equal(provider.deletedRoutes, []cloud.Identity{firstID, firstID}) {
				t.Fatalf("Gateway deletion affected the wrong checkpoint: %v, %v, %v", bindings, provider.deletedRoutes, err)
			}
			if err := kube.Get(t.Context(), client.ObjectKeyFromObject(first), first); err != nil {
				t.Fatal(err)
			}
			first.Finalizers = nil
			if err := kube.Update(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if err := kube.Create(t.Context(), replacement); err != nil {
				t.Fatal(err)
			}
			replacementID := controller.RouteIdentity(cfg, replacement, route)
			reconcile(2)
			get()
			assertProgrammed(map[gatewayv1.ObjectName]metav1.ConditionStatus{firstRef.Name: metav1.ConditionTrue, secondRef.Name: metav1.ConditionTrue})
			if err := kube.Delete(t.Context(), route); err != nil {
				t.Fatal(err)
			}
			reconcile(1)
			get()
			if len(route.GetFinalizers()) != 3 || len(provider.deletedRoutes) != 2 {
				t.Fatalf("deletion must retain both finalizers through the cleanup checkpoint: %v, %v", route.GetFinalizers(), provider.deletedRoutes)
			}
			reconcile(1)
			if err := kube.Get(t.Context(), req.NamespacedName, route); !apierrors.IsNotFound(err) {
				t.Fatalf("deleted route still exists: %v", err)
			}
			if len(provider.deletedRoutes) != 4 || !slices.Contains(provider.deletedRoutes[2:], replacementID) || !slices.Contains(provider.deletedRoutes[2:], secondID) {
				t.Fatalf("finalization did not remove both owned fragments: %v", provider.deletedRoutes)
			}
		})
	}
}

func TestReplacementGatewayWaitsForCleanupOfPreviousUID(t *testing.T) {
	cfg := testConfig()
	class, previous, route, service, endpoints := endpointRouteFixture(t)
	replacement := previous.DeepCopy()
	replacement.UID = "replacement-uid"
	sibling := previous.DeepCopy()
	sibling.Name, sibling.UID = "sibling", "sibling-uid"
	replacedRef := route.Spec.ParentRefs[0]
	siblingRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(sibling.Name)}
	route.Spec.ParentRefs = append(route.Spec.ParentRefs, siblingRef)
	plan := &routeStatusPlan{updates: []parentStatusUpdate{
		{parent: replacedRef, status: programmedRouteStatus()},
		{parent: siblingRef, status: programmedRouteStatus()},
	}}
	plan.apply(cfg, route)

	r := Reconciler{Config: cfg, Coordinator: &graph.Coordinator{}}
	if err := r.applyRouteBinding(route, previous); err != nil {
		t.Fatal(err)
	}
	legacyKey := cfg.RouteCleanupFailureAnnotation()
	route.Annotations[legacyKey] = controller.ProviderReasonInvalidConfiguration
	if err := r.applyRouteBinding(route, sibling); err != nil {
		t.Fatal(err)
	}
	previousKey := cfg.RouteGatewayCleanupFailureAnnotation(string(previous.UID))
	siblingKey := cfg.RouteGatewayCleanupFailureAnnotation(string(sibling.UID))
	if route.Annotations[legacyKey] != "" || route.Annotations[previousKey] != controller.ProviderReasonInvalidConfiguration || route.Annotations[siblingKey] != "" {
		t.Fatalf("legacy diagnostic did not migrate to its original Gateway: %v", route.Annotations)
	}

	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).
		WithObjects(class, replacement, sibling, route, service, endpoints).Build()
	r.Client, r.APIReader = kube, kube
	provider := &recordingProvider{}
	reconciler := newTestReconciler(r, provider)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	siblingFailure := controller.ProviderReasonQuotaExceeded
	if _, err := reconciler.patchRouteCleanupFailure(t.Context(), route, controller.RouteIdentity(cfg, sibling, route), siblingFailure); err != nil {
		t.Fatal(err)
	}

	check := func(previousFailure string) {
		t.Helper()
		if err := kube.Get(t.Context(), req.NamespacedName, route); err != nil {
			t.Fatal(err)
		}
		if route.Annotations[previousKey] != previousFailure || route.Annotations[siblingKey] != siblingFailure ||
			route.Annotations[cfg.RouteGatewayCleanupFailureAnnotation(string(replacement.UID))] != "" {
			t.Fatalf("cleanup diagnostics were changed for the wrong Gateway: %v", route.Annotations)
		}
		if len(route.Status.Parents) != 2 {
			t.Fatalf("expected two parents, got %v", route.Status.Parents)
		}
		for _, parent := range route.Status.Parents {
			programmed := meta.FindStatusCondition(parent.Conditions, cfg.Domain()+"/Programmed")
			switch parent.ParentRef.Name {
			case replacedRef.Name:
				if programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.Reason != "Pending" {
					t.Fatalf("replacement reported the old Gateway's status: %v", programmed)
				}
			case siblingRef.Name:
				if programmed == nil || programmed.Status != metav1.ConditionTrue {
					t.Fatalf("unaffected Gateway lost its programmed status: %v", programmed)
				}
			default:
				t.Fatalf("unexpected parent status: %v", parent)
			}
		}
	}

	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	check("")
	if len(provider.deletedRoutes) != 0 {
		t.Fatal("provider cleanup preceded the detaching checkpoint")
	}

	provider.routeDeleteErr = cloud.NewProviderError(cloud.ErrorCategoryOwnershipConflict, errors.New("previous Gateway cleanup is blocked"))
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	check(controller.ProviderReasonOwnershipConflict)

	provider.routeDeleteErr = nil
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	check("")
	bindings, err := controller.StoredRouteGatewayBindings(cfg, route)
	if err != nil || len(bindings) != 1 || bindings[0].Identity.GatewayUID != string(sibling.UID) {
		t.Fatalf("cleanup should remove only the previous UID, got %v, %v", bindings, err)
	}
}
