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
	parents, err := r.parentsForGateway(t.Context(), route, f.gateway)
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
