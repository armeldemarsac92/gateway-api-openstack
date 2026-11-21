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
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func routeCondition(route *transportRoute, typ string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return controller.Condition(typ, status, reason, message, route.GetGeneration())
}

func (p *routePlan) rejectBackend(route *transportRoute, diagnostic *model.Diagnostic) {
	if diagnostic == nil {
		p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "Backend references are resolved")
		return
	}
	p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
	if p.programmed.Reason != "NotSelected" {
		p.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
	}
}

func (r *Reconciler) currentParentCondition(route *transportRoute, ref gatewayv1.ParentReference, kind string) *metav1.Condition {
	for _, parent := range route.status.Parents {
		if parent.ControllerName != r.Config.ControllerName ||
			!controller.ParentRefsEqual(parent.ParentRef, route.GetNamespace(), ref, route.GetNamespace()) {
			continue
		}
		condition := meta.FindStatusCondition(parent.Conditions, kind)
		if condition != nil && condition.ObservedGeneration == route.GetGeneration() {
			return condition
		}
	}
	return nil
}

func (r *Reconciler) retainProgrammedCondition(route *transportRoute, plan *routePlan) bool {
	condition := r.currentParentCondition(route, plan.parents[0].ref, r.Config.Domain()+"/Programmed")
	if condition == nil {
		return false
	}
	plan.programmed = *condition
	return true
}

func (r *Reconciler) patchStatuses(ctx context.Context, route *transportRoute, plan routePlan) error {
	base := route.copyObject()
	previousParents := viewRoute(base).status.Parents
	parents := slices.DeleteFunc(route.status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
		return parent.ControllerName == r.Config.ControllerName
	})
	for _, parent := range plan.parents {
		status := gatewayv1.RouteParentStatus{ParentRef: parent.ref, ControllerName: r.Config.ControllerName}
		for _, previous := range previousParents {
			if previous.ControllerName == status.ControllerName && controller.ParentRefsEqual(previous.ParentRef, route.GetNamespace(), status.ParentRef, route.GetNamespace()) {
				status.Conditions = slices.Clone(previous.Conditions)
				break
			}
		}
		accepted := routeCondition(route, "Accepted", metav1.ConditionTrue, "Accepted", fmt.Sprintf("The %s listener accepts this route", r.protocol()))
		programmed := plan.programmed
		if parent.listener == nil {
			accepted = routeCondition(route, "Accepted", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
			programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
		}
		programmed.Type = r.Config.Domain() + "/Programmed"
		controller.SetCondition(&status.Conditions, accepted)
		controller.SetCondition(&status.Conditions, plan.resolved)
		controller.SetCondition(&status.Conditions, programmed)
		if plan.detach {
			meta.RemoveStatusCondition(&status.Conditions, r.forwardingTargetType())
		} else if plan.targetStatus.Type != "" {
			controller.SetCondition(&status.Conditions, plan.targetStatus)
		}
		parents = append(parents, status)
	}
	route.status.Parents = parents
	if len(previousParents) == len(route.status.Parents) && slices.EqualFunc(previousParents, route.status.Parents, func(previous, current gatewayv1.RouteParentStatus) bool {
		return previous.ControllerName == current.ControllerName &&
			controller.ParentRefsEqual(previous.ParentRef, route.GetNamespace(), current.ParentRef, route.GetNamespace()) &&
			slices.Equal(previous.Conditions, current.Conditions)
	}) {
		return nil
	}
	return r.Status().Patch(ctx, route.Object, controller.OptimisticMergeFrom(base))
}
