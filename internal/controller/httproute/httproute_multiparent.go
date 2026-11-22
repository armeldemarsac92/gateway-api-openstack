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
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

type routeParentGroup struct {
	gateway *gatewayv1.Gateway
	parents []managedRouteParent
}

func groupManagedRouteParents(parents []managedRouteParent) []routeParentGroup {
	var groups []routeParentGroup
	indices := make(map[client.ObjectKey]int, len(parents))
	for _, parent := range parents {
		key := client.ObjectKeyFromObject(parent.gateway)
		i, exists := indices[key]
		if !exists {
			i = len(groups)
			indices[key] = i
			groups = append(groups, routeParentGroup{gateway: parent.gateway})
		}
		groups[i].parents = append(groups[i].parents, parent)
	}
	return groups
}

func (r *Reconciler) reconcileRouteParents(ctx context.Context, route *gatewayv1.HTTPRoute, groups []routeParentGroup) (ctrl.Result, error) {
	bindings, err := controller.StoredRouteGatewayBindings(r.Config, route)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(bindings) == 0 && controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) {
		return ctrl.Result{}, fmt.Errorf("HTTPRoute %s has the controller finalizer but no complete stored Gateway identity", client.ObjectKeyFromObject(route))
	}
	desired := make(map[client.ObjectKey]cloud.Identity, len(groups))
	plan := &routeStatusPlan{kind: routeStatusPlanPrune, validateOwnership: true}
	for _, group := range groups {
		desired[client.ObjectKeyFromObject(group.gateway)] = controller.RouteIdentity(r.Config, group.gateway, route)
		for _, parent := range group.parents {
			plan.updates = append(plan.updates, parentStatusUpdate{parent: parent.ref})
		}
	}
	_, _, _, err = r.patchHTTPRouteStatus(ctx, route, plan)
	if err != nil {
		return ctrl.Result{}, err
	}

	detaching := make(map[client.ObjectKey]struct{})
	replacements := make(map[client.ObjectKey]struct{})
	var cleanup []cloud.Identity
	for _, binding := range bindings {
		identity := binding.Identity
		key := client.ObjectKey{Namespace: identity.GatewayNamespace, Name: identity.GatewayName}
		wanted, found := desired[key]
		if wanted == identity && binding.Lifecycle != controller.RouteDetaching {
			continue
		}
		detaching[key] = struct{}{}
		if found && wanted.GatewayUID != identity.GatewayUID {
			replacements[key] = struct{}{}
		}
		cleanup = append(cleanup, identity)
	}

	for _, group := range groups {
		if _, replacing := replacements[client.ObjectKeyFromObject(group.gateway)]; !replacing {
			continue
		}
		scope := &httpRouteScope{route: route, parentGateway: group.gateway}
		status := pendingRouteStatus("OpenStack resources for the previous Gateway are being removed")
		updates := make([]parentStatusUpdate, 0, len(group.parents))
		for _, parent := range group.parents {
			updates = append(updates, parentStatusUpdate{parent: parent.ref, status: status})
		}
		scope.setStatuses(updates)
		if err := r.patchHTTPRoute(ctx, scope); err != nil {
			return ctrl.Result{}, err
		}
	}

	var result ctrl.Result
	var reconcileErr error
	for _, identity := range cleanup {
		progress, err := r.detachGatewayBinding(ctx, route, route.DeepCopy(), identity)
		if err != nil {
			scope := &httpRouteScope{route: route}
			progress, err = r.handleRouteFinalizationFailure(ctx, scope, identity, err)
			err = errors.Join(err, r.patchHTTPRoute(ctx, scope))
		}
		result = mergeRouteResult(result, progress)
		reconcileErr = errors.Join(reconcileErr, err)
	}

	for _, group := range groups {
		if _, blocked := detaching[client.ObjectKeyFromObject(group.gateway)]; blocked {
			// Cleanup updates the binding metadata and enqueues the route again.
			continue
		}
		scope := &httpRouteScope{route: route, parentGateway: group.gateway}
		progress, err := r.reconcileRouteParentGroup(ctx, scope, group.parents)
		reconcileErr = errors.Join(reconcileErr, err, r.patchHTTPRoute(ctx, scope))
		result = mergeRouteResult(result, progress)
	}
	return result, reconcileErr
}

func (r *Reconciler) reconcileRouteParentGroup(ctx context.Context, scope *httpRouteScope, parents []managedRouteParent) (ctrl.Result, error) {
	if !parentsShareGateway(parents) {
		return ctrl.Result{}, errHTTPRouteChanged
	}
	binding, err := controller.StoredRouteGatewayBinding(r.Config, scope.route, controller.RouteIdentity(r.Config, scope.parentGateway, scope.route))
	if err != nil {
		return ctrl.Result{}, err
	}
	if !managedParentsSupportInstalledVersion(parents) {
		return r.setRouteUnsupportedVersion(ctx, scope)
	}
	decision, err := r.validateCachedRouteDecision(ctx, scope.route, parents, cachedRouteDecision{
		storedIdentity: binding.Identity, bindingPresent: binding.Identity.RouteUID != "",
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	scope.parentSelection = decision.selection
	if decision.disposition == cachedRouteDetach {
		return r.setRouteStatusesAndDetach(ctx, scope, decision.statusUpdates)
	}
	return r.reconcileSelectedRouteGraph(ctx, scope, decision.parent)
}

func mergeRouteResult(a, b ctrl.Result) ctrl.Result {
	result := a
	result.Requeue = a.Requeue || b.Requeue
	if result.RequeueAfter == 0 || b.RequeueAfter > 0 && b.RequeueAfter < result.RequeueAfter {
		result.RequeueAfter = b.RequeueAfter
	}
	// Controller-runtime gives RequeueAfter priority. A binding or status
	// checkpoint needs an immediate retry even if a sibling is progressing.
	if result.Requeue {
		result.RequeueAfter = 0
	}
	return result
}
