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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
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

func TestHTTPRouteRechecksSecondaryParentOwnershipAtGraphFences(t *testing.T) {
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
	if err := input.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("graph seal with newly managed parent = %v, want stale graph", err)
	}
	identity := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	repair, err := f.reconciler.ObserveRouteGraph(t.Context(), graph.Request{Identity: identity, Action: graph.Ensure}, f.gateway)
	if err != nil || len(repair.Nodes) != 1 || repair.Nodes[0].Disposition != cloud.GraphPreserve {
		t.Fatalf("Gateway repair should preserve the fragment for route reconciliation, got %v and error %v", repair.Nodes, err)
	}
	cleanup, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, identity)
	if err != nil || cleanup.disposition != routeCleanupRequired {
		t.Fatalf("multiple managed refs should require detaching the original fragment, got %v and error %v", cleanup.disposition, err)
	}
	if err := f.client.Delete(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.sealRouteCleanupSnapshot(t.Context(), cleanup); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("cleanup seal after the second class disappears = %v, want stale cleanup", err)
	}
}

func TestHTTPRouteRefusesSeveralManagedParentRefs(t *testing.T) {
	for _, sameGateway := range []bool{false, true} {
		name := "distinct Gateways"
		if sameGateway {
			name = "two sections of one Gateway"
		}
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			class, gateway, route, service, endpoints := endpointRouteFixture(t)
			other := gateway.DeepCopy()
			other.Name, other.UID = "other", "other-uid"
			secondRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(other.Name)}
			if sameGateway {
				gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "other", Protocol: gatewayv1.HTTPProtocolType, Port: 81})
				route.Spec.ParentRefs[0].SectionName = ptr.To(gateway.Spec.Listeners[0].Name)
				secondRef = gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("other"))}
			}
			route.Spec.ParentRefs = append(route.Spec.ParentRefs, secondRef)
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(class, gateway, other, route, service, endpoints).Build()
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
		})
	}
}
