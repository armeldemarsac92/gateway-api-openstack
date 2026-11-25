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
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func routeCondition(route *transportRoute, typ string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return controller.Condition(typ, status, reason, message, route.GetGeneration())
}

func (r *Reconciler) programmedCondition(route *transportRoute) metav1.Condition {
	return routeCondition(route, "Programmed", metav1.ConditionTrue, "Programmed", fmt.Sprintf("Octavia programmed the %s forwarding configuration", r.protocol()))
}

func (p *routePlan) setResolvedRefs(route *transportRoute, diagnostic *model.Diagnostic) {
	if diagnostic == nil {
		p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "Backend references are resolved")
		return
	}
	p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
}

func (p *routePlan) rejectBackend(route *transportRoute, diagnostic *model.Diagnostic) {
	p.setResolvedRefs(route, diagnostic)
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
	condition := r.currentParentCondition(route, plan.parent.ref, r.Config.Domain()+"/Programmed")
	if condition == nil {
		return false
	}
	plan.programmed = *condition
	return true
}

func (r *Reconciler) keepParentStatuses(route *transportRoute, key client.ObjectKey) routePlan {
	var plan routePlan
	for _, previous := range route.status.Parents {
		if previous.ControllerName == r.Config.ControllerName && controller.IsGatewayParentRef(previous.ParentRef) && gatewayKey(route, previous.ParentRef) == key {
			plan.parents = append(plan.parents, routeParent{ref: previous.ParentRef})
		}
	}
	return plan
}

func parentWonPort(parent routeParent, ports []int) bool {
	return slices.ContainsFunc(parent.listeners, func(listener *gatewayv1.Listener) bool {
		return slices.Contains(ports, int(listener.Port))
	})
}

func (r *Reconciler) patchStatuses(ctx context.Context, route *transportRoute, plans ...routePlan) error {
	return r.updateStatuses(ctx, route, false, plans)
}

func (r *Reconciler) patchParentStatuses(ctx context.Context, route *transportRoute, plan routePlan) error {
	return r.updateStatuses(ctx, route, true, []routePlan{plan})
}

func (r *Reconciler) updateStatuses(ctx context.Context, route *transportRoute, partial bool, plans []routePlan) error {
	base := route.copyObject()
	previousParents := viewRoute(base).status.Parents
	parents := slices.DeleteFunc(route.status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
		if parent.ControllerName != r.Config.ControllerName {
			return false
		}
		return !partial || slices.ContainsFunc(plans[0].parents, func(current routeParent) bool {
			return controller.ParentRefsEqual(parent.ParentRef, route.GetNamespace(), current.ref, route.GetNamespace())
		})
	})
	var retained, added []gatewayv1.RouteParentStatus
	for _, plan := range plans {
		for _, parent := range plan.parents {
			var previous *gatewayv1.RouteParentStatus
			for i := range previousParents {
				entry := &previousParents[i]
				if entry.ControllerName == r.Config.ControllerName && controller.ParentRefsEqual(entry.ParentRef, route.GetNamespace(), parent.ref, route.GetNamespace()) {
					previous = entry
					break
				}
			}
			if plan.resolved.Type == "" {
				if previous != nil {
					retained = append(retained, *previous)
				}
				continue
			}
			status := r.parentStatus(route, plan, parent, previous)
			if previous != nil {
				retained = append(retained, status)
			} else {
				added = append(added, status)
			}
		}
	}
	remaining := max(0, controller.RouteParentStatusLimit-len(parents))
	parents = append(parents, retained[:min(remaining, len(retained))]...)
	remaining = max(0, controller.RouteParentStatusLimit-len(parents))
	parents = append(parents, added[:min(remaining, len(added))]...)
	route.status.Parents = parents
	if slices.EqualFunc(previousParents, route.status.Parents, func(previous, current gatewayv1.RouteParentStatus) bool {
		return previous.ControllerName == current.ControllerName &&
			controller.ParentRefsEqual(previous.ParentRef, route.GetNamespace(), current.ParentRef, route.GetNamespace()) &&
			slices.Equal(previous.Conditions, current.Conditions)
	}) {
		return nil
	}
	return r.Status().Patch(ctx, route.Object, controller.OptimisticMergeFrom(base))
}

func (r *Reconciler) parentStatus(route *transportRoute, plan routePlan, parent routeParent, previous *gatewayv1.RouteParentStatus) gatewayv1.RouteParentStatus {
	status := gatewayv1.RouteParentStatus{ParentRef: parent.ref, ControllerName: r.Config.ControllerName}
	if previous != nil {
		status.Conditions = slices.Clone(previous.Conditions)
	}
	message := fmt.Sprintf("The %s listener accepts this route", r.protocol())
	if len(parent.listeners) > 1 {
		message = fmt.Sprintf("The selected %s listeners accept this route", r.protocol())
	}
	accepted := routeCondition(route, "Accepted", metav1.ConditionTrue, "Accepted", message)
	programmed := plan.programmed
	won := parentWonPort(parent, plan.listenerPorts)
	if len(parent.listeners) == 0 {
		accepted = routeCondition(route, "Accepted", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
		programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, parent.rejected.Reason, parent.rejected.Message)
	} else if len(plan.listenerPorts) != 0 && !won {
		programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "NotSelected", fmt.Sprintf("An older %s takes precedence on each selected listener", r.kind()))
	}
	programmed.Type = r.Config.Domain() + "/Programmed"
	controller.SetCondition(&status.Conditions, accepted)
	controller.SetCondition(&status.Conditions, plan.resolved)
	controller.SetCondition(&status.Conditions, programmed)
	if plan.detach || !won && len(plan.listenerPorts) != 0 {
		meta.RemoveStatusCondition(&status.Conditions, r.forwardingTargetType())
	} else if won && plan.targetStatus.Type != "" {
		controller.SetCondition(&status.Conditions, plan.targetStatus)
	}
	return status
}
