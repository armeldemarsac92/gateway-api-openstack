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
	"fmt"
	"reflect"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

type routeStatusPlanKind int

const (
	routeStatusPlanReplace routeStatusPlanKind = iota
	routeStatusPlanProviderFailure
	routeStatusPlanDeletingFailure
	routeStatusPlanPrune
)

type routeStatusPlan struct {
	kind                routeStatusPlanKind
	updates             []parentStatusUpdate
	parent              gatewayv1.ParentReference
	policy              controller.ProviderFailurePolicy
	referencesEvaluated bool
	validateOwnership   bool
	gateway             *gatewayv1.Gateway
}

type routeProviderWarning struct {
	policy                 controller.ProviderFailurePolicy
	action                 string
	cleanupCheckpointSaved bool
	recordOnStatusChange   bool
}

type httpRouteScope struct {
	route         *gatewayv1.HTTPRoute
	statusPlan    *routeStatusPlan
	warning       *routeProviderWarning
	patchCause    error
	skipPatch     bool
	versionSkew   bool
	parentGateway *gatewayv1.Gateway
}

func (r *Reconciler) newHTTPRouteScope(
	ctx context.Context,
	req ctrl.Request,
) (*httpRouteScope, bool, error) {
	route := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.Client, req.NamespacedName, route); err != nil {
		return nil, false, client.IgnoreNotFound(err)
	}
	scope := &httpRouteScope{route: route}
	if route.DeletionTimestamp.IsZero() {
		return scope, true, nil
	}
	return scope, controller.RouteHasControllerBinding(r.Config, route), nil
}

func (s *httpRouteScope) setStatuses(updates []parentStatusUpdate) {
	s.statusPlan = &routeStatusPlan{
		kind:              routeStatusPlanReplace,
		updates:           append([]parentStatusUpdate(nil), updates...),
		validateOwnership: true,
		gateway:           s.parentGateway,
	}
}

func (s *httpRouteScope) setProviderFailureStatuses(
	updates []parentStatusUpdate,
	policy controller.ProviderFailurePolicy,
	referencesEvaluated bool,
) {
	s.statusPlan = &routeStatusPlan{
		kind:                routeStatusPlanProviderFailure,
		updates:             append([]parentStatusUpdate(nil), updates...),
		policy:              policy,
		referencesEvaluated: referencesEvaluated,
		validateOwnership:   true,
		gateway:             s.parentGateway,
	}
}

func (s *httpRouteScope) setDeletingFailureStatus(
	parent gatewayv1.ParentReference,
	policy controller.ProviderFailurePolicy,
) {
	s.statusPlan = &routeStatusPlan{
		kind:   routeStatusPlanDeletingFailure,
		parent: parent,
		policy: policy,
	}
}

func (s *httpRouteScope) queueWarning(warning routeProviderWarning) {
	s.warning = &warning
}

func (r *Reconciler) patchHTTPRoute(ctx context.Context, scope *httpRouteScope) error {
	if scope.skipPatch {
		return nil
	}
	applied, changed, transitioned := true, false, false
	if scope.statusPlan != nil {
		var err error
		applied, changed, transitioned, err = r.patchHTTPRouteStatus(ctx, scope.route, scope.statusPlan)
		if err != nil {
			return fmt.Errorf("patch HTTPRoute status: %w", err)
		}
	}
	if applied && scope.warning != nil &&
		(scope.warning.cleanupCheckpointSaved || transitioned || scope.warning.recordOnStatusChange && changed) {
		controller.RecordProviderWarning(r.Recorder, routeAPIObject(scope.route), scope.warning.policy, scope.warning.action)
	}
	return nil
}

func (r *Reconciler) patchHTTPRouteStatus(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	plan *routeStatusPlan,
) (applied, changed, transitioned bool, err error) {
	key := client.ObjectKeyFromObject(route)
	uid, generation := route.UID, route.Generation
	isDeleting := !route.DeletionTimestamp.IsZero()
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		applied, changed, transitioned = false, false, false
		current := &gatewayv1.HTTPRoute{}
		if err := r.readRoute(ctx, r.routePatchReader(), key, current); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !sameHTTPRouteRevision(current, uid, generation, isDeleting) {
			return errHTTPRouteChanged
		}
		if plan.validateOwnership {
			if err := r.validateRouteStatusOwnership(ctx, current, plan.updates, plan.gateway); err != nil {
				return err
			}
		}

		base := current.DeepCopy()
		transitioned = plan.apply(r.Config, current)
		applied = true
		if reflect.DeepEqual(base.Status, current.Status) {
			*route = *current
			return nil
		}
		if err := r.patchRoute(ctx, current, base, true); err != nil {
			return err
		}
		changed = true
		*route = *current
		log.FromContext(ctx).V(1).Info("Updated HTTPRoute status")
		return nil
	})
	return applied, changed, transitioned, err
}

func (r *Reconciler) routeStatusCheckpointNeeded(
	ctx context.Context,
	scope *httpRouteScope,
) (bool, error) {
	if scope.statusPlan == nil {
		return false, nil
	}
	current := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.routePatchReader(), client.ObjectKeyFromObject(scope.route), current); err != nil {
		return false, fmt.Errorf("read HTTPRoute status checkpoint: %w", err)
	}
	if !sameHTTPRouteRevision(
		current,
		scope.route.UID,
		scope.route.Generation,
		!scope.route.DeletionTimestamp.IsZero(),
	) {
		return false, errHTTPRouteChanged
	}
	if scope.statusPlan.validateOwnership {
		if err := r.validateRouteStatusOwnership(ctx, current, scope.statusPlan.updates, scope.statusPlan.gateway); err != nil {
			return false, err
		}
	}
	desired := current.DeepCopy()
	scope.statusPlan.apply(r.Config, desired)
	*scope.route = *current
	return !reflect.DeepEqual(current.Status, desired.Status), nil
}

func (r *Reconciler) routeProviderFailureCheckpointExists(
	ctx context.Context,
	scope *httpRouteScope,
	updates []parentStatusUpdate,
) (bool, error) {
	current := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.routePatchReader(), client.ObjectKeyFromObject(scope.route), current); err != nil {
		return false, fmt.Errorf("read HTTPRoute provider failure checkpoint: %w", err)
	}
	if !sameHTTPRouteRevision(
		current,
		scope.route.UID,
		scope.route.Generation,
		!scope.route.DeletionTimestamp.IsZero(),
	) {
		return false, errHTTPRouteChanged
	}
	*scope.route = *current
	return routeHasProviderFailureStatus(
		current,
		updates,
		r.Config.ControllerName,
		r.Config.Domain()+"/Programmed",
		scope.parentGateway != nil,
	), nil
}

func (r *Reconciler) routeCleanupStatusCheckpointNeeded(
	ctx context.Context,
	scope *httpRouteScope,
	updates []parentStatusUpdate,
) (bool, error) {
	if routeHasProviderFailureStatus(
		scope.route,
		updates,
		r.Config.ControllerName,
		r.Config.Domain()+"/Programmed",
		scope.parentGateway != nil,
	) {
		durable, err := r.routeProviderFailureCheckpointExists(ctx, scope, updates)
		if err != nil {
			return false, err
		}
		if durable {
			return false, nil
		}
	}
	scope.setStatuses(updates)
	return r.routeStatusCheckpointNeeded(ctx, scope)
}

func (r *Reconciler) validateRouteStatusOwnership(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	updates []parentStatusUpdate,
	gateway *gatewayv1.Gateway,
) error {
	parents, err := r.managedParentsWithReader(ctx, r.routePatchReader(), route)
	if err != nil {
		return err
	}
	for _, update := range updates {
		owned := false
		for _, parent := range parents {
			if controller.ParentRefsEqual(parent.ref, route.Namespace, update.parent, route.Namespace) &&
				(gateway == nil || gateway.UID == parent.gateway.UID && gateway.Generation == parent.gateway.Generation) {
				owned = true
				break
			}
		}
		if !owned {
			return errHTTPRouteChanged
		}
	}
	if gateway != nil {
		return nil
	}
	for _, parent := range parents {
		planned := false
		for _, update := range updates {
			if controller.ParentRefsEqual(parent.ref, route.Namespace, update.parent, route.Namespace) {
				planned = true
				break
			}
		}
		if !planned {
			return errHTTPRouteChanged
		}
	}
	return nil
}

func (r *Reconciler) routePatchReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (p *routeStatusPlan) apply(config controller.Config, route *gatewayv1.HTTPRoute) bool {
	existing := route.Status.Parents
	var retained []gatewayv1.RouteParentStatus
	if p.gateway != nil {
		for _, status := range route.Status.Parents {
			if status.ControllerName != config.ControllerName {
				continue
			}
			updated := false
			for _, update := range p.updates {
				if controller.ParentRefsEqual(status.ParentRef, route.Namespace, update.parent, route.Namespace) {
					updated = true
					break
				}
			}
			if !updated && controller.IsGatewayParentRef(status.ParentRef) {
				retained = append(retained, status)
			}
		}
	}
	var transitioned bool
	switch p.kind {
	case routeStatusPlanReplace:
		applyRouteParentStatuses(config, route, p.updates)
	case routeStatusPlanProviderFailure:
		transitioned = applyRouteProviderFailureStatuses(config, route, p.updates, p.policy, p.referencesEvaluated)
	case routeStatusPlanDeletingFailure:
		return applyDeletingRouteProviderFailure(config, route, p.parent, p.policy)
	case routeStatusPlanPrune:
		route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
			return status.ControllerName == config.ControllerName && !hasNonGatewayParentRef(route, status.ParentRef) &&
				!slices.ContainsFunc(p.updates, func(update parentStatusUpdate) bool {
					return controller.ParentRefsEqual(update.parent, route.Namespace, status.ParentRef, route.Namespace)
				})
		})
		return false
	}
	if p.gateway != nil {
		combined := append(retained, route.Status.Parents...)
		route.Status.Parents = retainRouteParentOrder(route.Namespace, combined, existing)
	}
	return transitioned
}

func retainRouteParentOrder(namespace string, current, previous []gatewayv1.RouteParentStatus) []gatewayv1.RouteParentStatus {
	ordered := make([]gatewayv1.RouteParentStatus, 0, len(current))
	for _, old := range previous {
		index := slices.IndexFunc(current, func(parent gatewayv1.RouteParentStatus) bool {
			return parent.ControllerName == old.ControllerName &&
				controller.ParentRefsEqual(parent.ParentRef, namespace, old.ParentRef, namespace)
		})
		if index >= 0 {
			ordered = append(ordered, current[index])
			current = slices.Delete(current, index, index+1)
		}
	}
	return append(ordered, current...)
}

func hasNonGatewayParentRef(route *gatewayv1.HTTPRoute, parent gatewayv1.ParentReference) bool {
	return !controller.IsGatewayParentRef(parent) && slices.ContainsFunc(route.Spec.ParentRefs, func(ref gatewayv1.ParentReference) bool {
		return controller.ParentRefsEqual(ref, route.Namespace, parent, route.Namespace)
	})
}

func applyRouteParentStatuses(config controller.Config, route *gatewayv1.HTTPRoute, updates []parentStatusUpdate) {
	existing := append([]gatewayv1.RouteParentStatus(nil), route.Status.Parents...)
	parents := make([]gatewayv1.RouteParentStatus, 0, len(existing)+len(updates))
	for _, parent := range existing {
		if parent.ControllerName != config.ControllerName || hasNonGatewayParentRef(route, parent.ParentRef) {
			parents = append(parents, parent)
		}
	}
	for index, update := range updates {
		if parentStatusUpdateAppearedEarlier(route.Namespace, updates, index) {
			continue
		}
		parentStatus := routeParentStatus(route, config.ControllerName, existing, update.parent)
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionAccepted), update.status.accepted, update.status.acceptedReason, update.status.acceptedMessage, route.Generation))
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionResolvedRefs), update.status.resolved, update.status.resolvedReason, update.status.resolvedMessage, route.Generation))
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(config.Domain()+"/Programmed", update.status.programmed, update.status.programmedReason, update.status.programmedMessage, route.Generation))
		parents = append(parents, parentStatus)
	}
	route.Status.Parents = parents
}

func applyRouteProviderFailureStatuses(
	config controller.Config,
	route *gatewayv1.HTTPRoute,
	updates []parentStatusUpdate,
	policy controller.ProviderFailurePolicy,
	referencesEvaluated bool,
) bool {
	existing := append([]gatewayv1.RouteParentStatus(nil), route.Status.Parents...)
	parents := make([]gatewayv1.RouteParentStatus, 0, len(existing)+len(updates))
	for _, parent := range existing {
		if parent.ControllerName != config.ControllerName || hasNonGatewayParentRef(route, parent.ParentRef) {
			parents = append(parents, parent)
		}
	}
	transitioned := false
	for index, update := range updates {
		if parentStatusUpdateAppearedEarlier(route.Namespace, updates, index) {
			continue
		}
		parentStatus := routeParentStatus(route, config.ControllerName, existing, update.parent)
		resolvedGeneration := controller.ConditionObservedGeneration(
			parentStatus.Conditions,
			string(gatewayv1.RouteConditionResolvedRefs),
			route.Generation,
			referencesEvaluated,
		)
		programmedGeneration := controller.ConditionObservedGeneration(
			parentStatus.Conditions,
			config.Domain()+"/Programmed",
			route.Generation,
			policy.AdvancesObservedGeneration,
		)
		transitioned = transitioned || controller.ConditionTransitioned(
			parentStatus.Conditions,
			config.Domain()+"/Programmed",
			update.status.programmed,
			update.status.programmedReason,
			update.status.programmedMessage,
			programmedGeneration,
		)
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionAccepted), update.status.accepted, update.status.acceptedReason, update.status.acceptedMessage, route.Generation))
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionResolvedRefs), update.status.resolved, update.status.resolvedReason, update.status.resolvedMessage, resolvedGeneration))
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(config.Domain()+"/Programmed", update.status.programmed, update.status.programmedReason, update.status.programmedMessage, programmedGeneration))
		parents = append(parents, parentStatus)
	}
	route.Status.Parents = parents
	return transitioned
}

func parentStatusUpdateAppearedEarlier(
	routeNamespace string,
	updates []parentStatusUpdate,
	index int,
) bool {
	for earlier := 0; earlier < index; earlier++ {
		if controller.ParentRefsEqual(
			updates[earlier].parent,
			routeNamespace,
			updates[index].parent,
			routeNamespace,
		) {
			return true
		}
	}
	return false
}

func applyDeletingRouteProviderFailure(
	config controller.Config,
	route *gatewayv1.HTTPRoute,
	parent gatewayv1.ParentReference,
	policy controller.ProviderFailurePolicy,
) bool {
	parentInSpec := false
	for _, candidate := range route.Spec.ParentRefs {
		if controller.ParentRefsEqual(candidate, route.Namespace, parent, route.Namespace) {
			parentInSpec = true
			break
		}
	}
	if !parentInSpec {
		return false
	}
	for index := range route.Status.Parents {
		candidate := &route.Status.Parents[index]
		if candidate.ControllerName != config.ControllerName ||
			!controller.ParentRefsEqual(candidate.ParentRef, route.Namespace, parent, route.Namespace) {
			continue
		}
		programmedGeneration := controller.ConditionObservedGeneration(
			candidate.Conditions,
			config.Domain()+"/Programmed",
			route.Generation,
			policy.AdvancesObservedGeneration,
		)
		transitioned := controller.ConditionTransitioned(
			candidate.Conditions,
			config.Domain()+"/Programmed",
			policy.ConditionStatus,
			policy.Reason,
			policy.Message,
			programmedGeneration,
		)
		controller.SetCondition(&candidate.Conditions, controller.Condition(
			config.Domain()+"/Programmed",
			policy.ConditionStatus,
			policy.Reason,
			policy.Message,
			programmedGeneration,
		))
		return transitioned
	}
	return false
}

func routeParentStatus(
	route *gatewayv1.HTTPRoute,
	controllerName gatewayv1.GatewayController,
	existing []gatewayv1.RouteParentStatus,
	parent gatewayv1.ParentReference,
) gatewayv1.RouteParentStatus {
	status := gatewayv1.RouteParentStatus{ParentRef: parent, ControllerName: controllerName}
	for _, candidate := range existing {
		if candidate.ControllerName != controllerName ||
			!controller.ParentRefsEqual(candidate.ParentRef, route.Namespace, parent, route.Namespace) {
			continue
		}
		status.Conditions = append([]metav1.Condition(nil), candidate.Conditions...)
		break
	}
	return status
}
