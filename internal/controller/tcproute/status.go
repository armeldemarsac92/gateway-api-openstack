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

package tcproute

import (
	"context"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func routeCondition(route *gatewayv1.TCPRoute, typ string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return controller.Condition(typ, status, reason, message, route.Generation)
}

func (p *routePlan) rejectBackend(route *gatewayv1.TCPRoute, diagnostic *model.Diagnostic) {
	if diagnostic == nil {
		p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "Backend references are resolved")
		return
	}
	p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
	if p.programmed.Reason != "NotSelected" {
		p.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
	}
}

func (r *Reconciler) retainProgrammedCondition(route *gatewayv1.TCPRoute, plan *routePlan) bool {
	for _, previous := range route.Status.Parents {
		if previous.ControllerName != r.Config.ControllerName ||
			!controller.ParentRefsEqual(previous.ParentRef, route.Namespace, plan.parents[0].ref, route.Namespace) {
			continue
		}
		condition := meta.FindStatusCondition(previous.Conditions, r.Config.Domain()+"/Programmed")
		if condition != nil && condition.ObservedGeneration == route.Generation {
			plan.programmed = *condition
			return true
		}
	}
	return false
}

func (r *Reconciler) patchStatuses(ctx context.Context, route *gatewayv1.TCPRoute, plan routePlan) error {
	base := route.DeepCopy()
	parents := slices.DeleteFunc(route.Status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
		return parent.ControllerName == r.Config.ControllerName
	})
	for _, parent := range plan.parents {
		status := gatewayv1.RouteParentStatus{ParentRef: parent.ref, ControllerName: r.Config.ControllerName}
		for _, previous := range base.Status.Parents {
			if previous.ControllerName == status.ControllerName && controller.ParentRefsEqual(previous.ParentRef, route.Namespace, status.ParentRef, route.Namespace) {
				status.Conditions = slices.Clone(previous.Conditions)
				break
			}
		}
		accepted := routeCondition(route, "Accepted", metav1.ConditionTrue, "Accepted", "The TCP listener accepts this route")
		programmed := plan.programmed
		if parent.listener == nil {
			accepted = routeCondition(route, "Accepted", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
			programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
		}
		programmed.Type = r.Config.Domain() + "/Programmed"
		controller.SetCondition(&status.Conditions, accepted)
		controller.SetCondition(&status.Conditions, plan.resolved)
		controller.SetCondition(&status.Conditions, programmed)
		parents = append(parents, status)
	}
	route.Status.Parents = parents
	if len(base.Status.Parents) == len(route.Status.Parents) && slices.EqualFunc(base.Status.Parents, route.Status.Parents, func(previous, current gatewayv1.RouteParentStatus) bool {
		return previous.ControllerName == current.ControllerName &&
			controller.ParentRefsEqual(previous.ParentRef, route.Namespace, current.ParentRef, route.Namespace) &&
			slices.Equal(previous.Conditions, current.Conditions)
	}) {
		return nil
	}
	return r.Status().Patch(ctx, route, controller.OptimisticMergeFrom(base))
}
