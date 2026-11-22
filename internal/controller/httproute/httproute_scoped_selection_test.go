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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestHTTPRouteScopedStatusSelection(t *testing.T) {
	f, set, grpc := newRouteElectionFixture(t)
	r, route := f.reconciler, f.route
	neighbor := f.gateway.DeepCopy()
	neighbor.Name, neighbor.UID, neighbor.ResourceVersion = "neighbor", "neighbor-uid", ""
	neighborSet := set.DeepCopy()
	neighborSet.Name, neighborSet.UID, neighborSet.ResourceVersion = "neighbor-set", "neighbor-set-uid", ""
	neighborSet.Spec.ParentRef.Name = gatewayv1.ObjectName(neighbor.Name)
	f.create(t, neighbor, neighborSet)
	neighborRefs := []gatewayv1.ParentReference{
		{Name: gatewayv1.ObjectName(neighbor.Name)},
		{Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: gatewayv1.ObjectName(neighborSet.Name)},
	}
	route.Spec.ParentRefs = append(route.Spec.ParentRefs, neighborRefs...)
	if err := f.client.Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	neighborPlan := routeStatusPlan{gateway: neighbor, updates: []parentStatusUpdate{
		{parent: neighborRefs[0], status: pendingRouteStatus("neighbor pending")},
		{parent: neighborRefs[1], status: pendingRouteStatus("neighbor pending")},
	}}
	neighborPlan.apply(r.Config, route)
	if err := f.client.Status().Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}

	admission, err := r.liveRouteGraphAdmission(t.Context(), f.gateway, route)
	if err != nil {
		t.Fatal(err)
	}
	parents, err := r.parentsForGateway(t.Context(), r.APIReader, route, f.gateway)
	if err != nil {
		t.Fatal(err)
	}
	selection, _, err := r.selectRouteParents(t.Context(), route, parents, admission.listenerSets)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection) != 2 {
		t.Fatalf("Gateway selection has %d refs, want its Gateway and ListenerSet refs", len(selection))
	}
	scope := &httpRouteScope{route: route, parentGateway: f.gateway, parentSelection: selection, profileObserved: true}
	scope.setStatuses([]parentStatusUpdate{{parent: route.Spec.ParentRefs[0], status: programmedRouteStatus()}})
	if err := r.patchHTTPRoute(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	assertProgrammedReason := func(ref gatewayv1.ParentReference, reason string) {
		t.Helper()
		condition := findRouteParentCondition(route, r.Config.ControllerName, ref, r.Config.Domain()+"/Programmed")
		if condition == nil || condition.Reason != reason {
			t.Fatalf("parent %s Programmed = %+v, want reason %q", ref.Name, condition, reason)
		}
	}
	for _, ref := range route.Spec.ParentRefs[:2] {
		assertProgrammedReason(ref, "Programmed")
	}
	for _, ref := range neighborRefs {
		assertProgrammedReason(ref, "Pending")
	}

	f.create(t, grpc)
	if err := r.patchHTTPRoute(t.Context(), scope); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("patch after the Gateway election changed = %v, want stale input", err)
	}

	const cleanupReason = controller.ProviderReasonOwnershipConflict
	scope.setDeletingFailureStatus(controller.ProviderFailurePolicy{
		ConditionStatus: metav1.ConditionFalse, Reason: cleanupReason, Message: "cleanup failed",
	}, controller.RouteIdentity(r.Config, f.gateway, route))
	if err := r.patchHTTPRoute(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	for _, ref := range route.Spec.ParentRefs[:2] {
		assertProgrammedReason(ref, cleanupReason)
	}
	for _, ref := range neighborRefs {
		assertProgrammedReason(ref, "Pending")
	}
}

func TestHTTPRouteScopedSingletonRevalidatesNamespaceBeforeStatus(t *testing.T) {
	f := newRouteModelInputFixture(t)
	ctx, r := t.Context(), f.reconciler
	neighbor := f.gateway.DeepCopy()
	neighbor.Name, neighbor.UID, neighbor.ResourceVersion = "neighbor", "neighbor-uid", ""
	f.gateway.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
		From:     ptr.To(gatewayv1.NamespacesFromSelector),
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"routes": "allowed"}},
	}}
	neighborRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(neighbor.Name)}
	f.route.Spec.ParentRefs = append(f.route.Spec.ParentRefs, neighborRef)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.route.Namespace, UID: "ns-uid", Labels: map[string]string{"routes": "allowed"}}}
	f.create(t, neighbor, namespace)
	if err := f.client.Update(ctx, f.gateway); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Update(ctx, f.route); err != nil {
		t.Fatal(err)
	}
	statusFor := func(gateway *gatewayv1.Gateway) *httpRouteScope {
		t.Helper()
		parents, err := r.parentsForGateway(ctx, r.APIReader, f.route, gateway)
		if err != nil {
			t.Fatal(err)
		}
		selection, _, err := r.selectRouteParents(ctx, f.route, parents, nil)
		if err != nil || len(selection.listeners()) != 1 {
			t.Fatalf("initial selection for %s = %v, %v", gateway.Name, selection, err)
		}
		scope := &httpRouteScope{route: f.route, parentGateway: gateway, parentSelection: selection}
		scope.setStatuses(selection.statusUpdates(programmedRouteStatus()))
		return scope
	}
	firstPlan, neighborPlan := statusFor(f.gateway), statusFor(neighbor)

	namespace.Labels = nil
	if err := f.client.Update(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	if err := r.patchHTTPRoute(ctx, firstPlan); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("publish status after namespace authorization was revoked = %v, want stale input", err)
	}
	if err := r.patchHTTPRoute(ctx, neighborPlan); err != nil {
		t.Fatal(err)
	}
	if len(f.route.Status.Parents) != 1 {
		t.Fatalf("expected only the sibling's status, got %v", f.route.Status.Parents)
	}
	assertElectionParentAccepted(t, f.route, neighborRef, metav1.ConditionTrue)
}
