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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeStatusPlanKind int

const (
	routeStatusPlanReplace routeStatusPlanKind = iota
	routeStatusPlanProviderFailure
	routeStatusPlanDeletingFailure
	routeStatusPlanPrune
)

type routeStatusPlan struct {
	kind                       routeStatusPlanKind
	updates                    []parentStatusUpdate
	droppedRules               string
	corsFallback               bool
	policy                     controller.ProviderFailurePolicy
	referencesEvaluated        bool
	validateOwnership          bool
	selection                  routeSelection
	profileObserved            bool
	rejectedSections           []model.ListenerSection
	gateway                    *gatewayv1.Gateway
	cleanupIdentity            cloud.Identity
	persistenceRevision        *string
	servicePersistenceRevision *string
	xBackendTransportRevision  *string
	serviceImports             *routeServiceImportSnapshot
}

type routeProviderWarning struct {
	policy                 controller.ProviderFailurePolicy
	action                 string
	cleanupCheckpointSaved bool
	recordOnStatusChange   bool
}

type httpRouteScope struct {
	route            *gatewayv1.HTTPRoute
	statusPlan       *routeStatusPlan
	parentSelection  routeSelection
	parentGateway    *gatewayv1.Gateway
	rejectedSections []model.ListenerSection
	profileObserved  bool
	corsFallback     bool
	warning          *routeProviderWarning
	patchCause       error
	skipPatch        bool
	versionSkew      bool
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

func (s *httpRouteScope) completeParentUpdates(updates []parentStatusUpdate) []parentStatusUpdate {
	if len(s.parentSelection) > 1 && len(updates) == 1 {
		return s.parentSelection.withoutProfileConflicts(s.rejectedSections).statusUpdates(updates[0].status)
	}
	return updates
}

func (s *httpRouteScope) setStatuses(updates []parentStatusUpdate) {
	updates = s.completeParentUpdates(updates)
	s.statusPlan = &routeStatusPlan{
		kind:              routeStatusPlanReplace,
		updates:           append([]parentStatusUpdate(nil), updates...),
		selection:         s.parentSelection,
		validateOwnership: true,
		profileObserved:   s.profileObserved,
		corsFallback:      s.corsFallback,
		rejectedSections:  s.rejectedSections,
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
		updates:             append([]parentStatusUpdate(nil), s.completeParentUpdates(updates)...),
		selection:           s.parentSelection,
		policy:              policy,
		referencesEvaluated: referencesEvaluated,
		validateOwnership:   true,
		profileObserved:     s.profileObserved,
		corsFallback:        s.corsFallback,
		rejectedSections:    s.rejectedSections,
		gateway:             s.parentGateway,
	}
}

func (s *httpRouteScope) setDeletingFailureStatus(
	policy controller.ProviderFailurePolicy,
	identity cloud.Identity,
) {
	s.statusPlan = &routeStatusPlan{
		kind:            routeStatusPlanDeletingFailure,
		policy:          policy,
		cleanupIdentity: identity,
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
			if err := r.validateRouteStatusOwnership(ctx, current, plan); err != nil {
				return err
			}
		}
		if plan.serviceImports != nil {
			if err := plan.serviceImports.validate(ctx, r.routePatchReader()); err != nil {
				return err
			}
		}
		if plan.xBackendTransportRevision != nil {
			admitted := r.routeWithAdmittedRules(current)
			observations, err := r.liveXBackendRevisions(ctx, admitted)
			if err != nil {
				return err
			}
			if xBackendTransportRevision(admitted, observations) != *plan.xBackendTransportRevision ||
				plan.persistenceRevision != nil && xBackendPersistenceRevision(admitted, observations) != *plan.persistenceRevision {
				return errHTTPRouteChanged
			}
		}
		currentPlan := *plan
		_, currentPlan.droppedRules = r.projectedHTTPFilterRules(current)
		if plan.kind == routeStatusPlanDeletingFailure {
			updates, err := r.deletingFailureParentUpdates(ctx, current, plan.cleanupIdentity)
			if err != nil {
				return err
			}
			currentPlan.updates = updates
		}

		base := current.DeepCopy()
		transitioned = currentPlan.apply(r.Config, current)
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

func (r *Reconciler) routeCleanupStatusCheckpointNeeded(
	ctx context.Context,
	scope *httpRouteScope,
	updates []parentStatusUpdate,
) (bool, error) {
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
	scope.setStatuses(updates)
	plan := scope.statusPlan
	if err := r.validateRouteStatusOwnership(ctx, current, plan); err != nil {
		scope.statusPlan = nil
		return false, err
	}
	*scope.route = *current
	if routeHasProviderFailureStatus(
		current,
		plan.updates,
		r.Config.ControllerName,
		r.Config.Domain()+"/Programmed",
		plan.gateway != nil,
	) {
		scope.statusPlan = nil
		return false, nil
	}
	desired := current.DeepCopy()
	_, plan.droppedRules = r.projectedHTTPFilterRules(current)
	plan.apply(r.Config, desired)
	return !reflect.DeepEqual(current.Status, desired.Status), nil
}

func (r *Reconciler) deletingFailureParentUpdates(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	identity cloud.Identity,
) ([]parentStatusUpdate, error) {
	reader := r.routePatchReader()
	gatewayKey := client.ObjectKey{Namespace: identity.GatewayNamespace, Name: identity.GatewayName}
	var gateway gatewayv1.Gateway
	err := reader.Get(ctx, gatewayKey, &gateway)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	if err == nil && string(gateway.UID) != identity.GatewayUID {
		// The durable cleanup diagnostic belongs to the previous UID.
		return nil, nil
	}

	var updates []parentStatusUpdate
	for _, status := range route.Status.Parents {
		if status.ControllerName != r.Config.ControllerName {
			continue
		}
		ref := status.ParentRef
		targetsGateway := routeTargetsGateway(route, ref, gatewayKey.Namespace, gatewayKey.Name)
		declared := slices.ContainsFunc(route.Spec.ParentRefs, func(candidate gatewayv1.ParentReference) bool {
			return controller.ParentRefsEqual(candidate, route.Namespace, ref, route.Namespace)
		})
		// For an implicit parent, the recorded binding identifies a previously
		// reported Gateway even if it has since been removed from the cluster.
		implicit := r.Config.DefaultGatewaysEnabled() && route.Spec.UseDefaultGateways == gatewayv1.GatewayDefaultScopeAll &&
			targetsGateway && ref.SectionName == nil && ref.Port == nil
		if !declared && !implicit {
			continue
		}
		if !targetsGateway {
			setKey := model.ListenerSetRefKey(ref, route.Namespace)
			if model.HTTPProfileRouteKind(route) != model.HTTPRouteKind || setKey.Name == "" {
				continue
			}
			var set gatewayv1.ListenerSet
			if err := reader.Get(ctx, setKey, &set); err != nil {
				if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
					continue
				}
				return nil, err
			}
			if model.ListenerSetParentKey(&set) != gatewayKey {
				continue
			}
		}
		updates = append(updates, parentStatusUpdate{parent: ref})
	}
	return updates, nil
}

func (r *Reconciler) validateRouteStatusOwnership(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	plan *routeStatusPlan,
) error {
	parents, err := r.managedParentsWithReader(ctx, r.routePatchReader(), route)
	if err != nil {
		return err
	}
	if plan.gateway != nil {
		identity := controller.RouteIdentity(r.Config, plan.gateway, route)
		parents = managedParentsForGateway(parents, identity)
		if len(parents) == 0 || slices.ContainsFunc(parents, func(parent managedRouteParent) bool {
			return parent.gateway.Generation != plan.gateway.Generation
		}) {
			return errHTTPRouteChanged
		}
	}
	// A prune plan covers all managed parents. A scoped status plan covers only
	// the complete live group for its Gateway.
	for _, update := range plan.updates {
		if !slices.ContainsFunc(parents, func(parent managedRouteParent) bool {
			return controller.ParentRefsEqual(parent.ref, route.Namespace, update.parent, route.Namespace)
		}) {
			return errHTTPRouteChanged
		}
	}
	for _, parent := range parents {
		if !slices.ContainsFunc(plan.updates, func(update parentStatusUpdate) bool {
			return controller.ParentRefsEqual(parent.ref, route.Namespace, update.parent, route.Namespace)
		}) {
			return errHTTPRouteChanged
		}
	}
	if len(plan.selection) != 0 {
		expected := plan.selection[0].parent.gateway
		if !parentsShareGateway(parents) || client.ObjectKeyFromObject(parents[0].gateway) != client.ObjectKeyFromObject(expected) ||
			parents[0].gateway.UID != expected.UID || parents[0].gateway.Generation != expected.Generation {
			return errHTTPRouteChanged
		}
		current, _, _, err := r.observeRouteSelection(ctx, r.routePatchReader(), route, parents)
		if err != nil {
			return err
		}
		if !slices.EqualFunc(plan.selection, current, func(previous, current routeParentSelection) bool {
			if !controller.ParentRefsEqual(previous.parent.ref, route.Namespace, current.parent.ref, route.Namespace) ||
				!slices.EqualFunc(previous.listeners, current.listeners, func(a, b *gatewayv1.Listener) bool {
					return listenerSectionIdentity(previous.section(a.Name), *a) == listenerSectionIdentity(current.section(b.Name), *b)
				}) {
				return false
			}
			previousSet, currentSet := previous.parent.listenerSet, current.parent.listenerSet
			if (previousSet == nil) != (currentSet == nil) || previousSet != nil &&
				(previousSet.UID != currentSet.UID || previousSet.Generation != currentSet.Generation) {
				return false
			}
			return previous.rejection == nil && current.rejection == nil || previous.rejection != nil && current.rejection != nil &&
				previous.rejection.Error() == current.rejection.Error()
		}) {
			return errHTTPRouteChanged
		}
	}
	if plan.profileObserved {
		if len(plan.selection) == 0 {
			return errHTTPRouteChanged
		}
		admission, err := r.liveRouteGraphAdmission(ctx, parents[0].gateway, route)
		if err != nil {
			return err
		}
		if !slices.Equal(plan.rejectedSections, admission.rejectedSections[routeKey(route)]) {
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
	switch p.kind {
	case routeStatusPlanReplace, routeStatusPlanProviderFailure:
		return p.updateParentStatuses(config, route)
	case routeStatusPlanDeletingFailure:
		transitioned := false
		for _, update := range p.updates {
			if applyDeletingRouteProviderFailure(config, route, update.parent, p.policy) {
				transitioned = true
			}
		}
		return transitioned
	case routeStatusPlanPrune:
		route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
			return status.ControllerName == config.ControllerName && !hasNonGatewayParentRef(route, status.ParentRef) &&
				!slices.ContainsFunc(p.updates, func(update parentStatusUpdate) bool {
					return controller.ParentRefsEqual(update.parent, route.Namespace, status.ParentRef, route.Namespace)
				})
		})
		return false
	}
	return false
}

func hasNonGatewayParentRef(route *gatewayv1.HTTPRoute, parent gatewayv1.ParentReference) bool {
	return !controller.IsGatewayParentRef(parent) &&
		(model.HTTPProfileRouteKind(route) != model.HTTPRouteKind || model.ListenerSetRefKey(parent, route.Namespace).Name == "") &&
		slices.ContainsFunc(route.Spec.ParentRefs, func(ref gatewayv1.ParentReference) bool {
			return controller.ParentRefsEqual(ref, route.Namespace, parent, route.Namespace)
		})
}

func (p *routeStatusPlan) updateParentStatuses(config controller.Config, route *gatewayv1.HTTPRoute) bool {
	matches := func(parent gatewayv1.RouteParentStatus, ref gatewayv1.ParentReference) bool {
		return parent.ControllerName == config.ControllerName &&
			controller.ParentRefsEqual(parent.ParentRef, route.Namespace, ref, route.Namespace)
	}
	parents := make([]gatewayv1.RouteParentStatus, 0, len(route.Status.Parents))
	for _, parent := range route.Status.Parents {
		if p.gateway != nil || parent.ControllerName != config.ControllerName || hasNonGatewayParentRef(route, parent.ParentRef) ||
			slices.ContainsFunc(p.updates, func(update parentStatusUpdate) bool { return matches(parent, update.parent) }) {
			parents = append(parents, parent)
		}
	}
	transitioned := false
	requiresPolicyReadback := slices.ContainsFunc(route.Spec.Rules, func(rule gatewayv1.HTTPRouteRule) bool {
		return model.HasHTTPRequestPolicy(rule) || model.HasRequestMirrors(rule)
	})
	for index, update := range p.updates {
		if requiresPolicyReadback && !update.status.implementedRules && update.status.accepted == metav1.ConditionTrue && update.status.programmed != metav1.ConditionTrue {
			update.status.accepted = metav1.ConditionUnknown
			update.status.acceptedReason = string(gatewayv1.RouteReasonPending)
			update.status.acceptedMessage = update.status.programmedMessage
		}
		if parentStatusUpdateAppearedEarlier(route.Namespace, p.updates, index) {
			continue
		}
		existing := slices.IndexFunc(parents, func(parent gatewayv1.RouteParentStatus) bool { return matches(parent, update.parent) })
		if existing < 0 && len(parents) >= controller.RouteParentStatusLimit {
			continue
		}
		parentStatus := gatewayv1.RouteParentStatus{ParentRef: update.parent, ControllerName: config.ControllerName}
		if existing >= 0 {
			parentStatus.Conditions = slices.Clone(parents[existing].Conditions)
		}
		resolvedGeneration, programmedGeneration := route.Generation, route.Generation
		if p.kind == routeStatusPlanProviderFailure {
			resolvedGeneration = controller.ConditionObservedGeneration(
				parentStatus.Conditions, string(gatewayv1.RouteConditionResolvedRefs), route.Generation, p.referencesEvaluated,
			)
			programmedGeneration = controller.ConditionObservedGeneration(
				parentStatus.Conditions, config.Domain()+"/Programmed", route.Generation, p.policy.AdvancesObservedGeneration,
			)
			transitioned = transitioned || controller.ConditionTransitioned(
				parentStatus.Conditions, config.Domain()+"/Programmed",
				update.status.programmed, update.status.programmedReason, update.status.programmedMessage, programmedGeneration,
			)
		}
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionAccepted), update.status.accepted, update.status.acceptedReason, update.status.acceptedMessage, route.Generation))
		partial := string(gatewayv1.RouteConditionPartiallyInvalid)
		if (p.droppedRules != "" || p.corsFallback || update.status.partiallyInvalid != "") && update.status.accepted == metav1.ConditionTrue {
			message := p.droppedRules
			addDiagnostic := func(diagnostic string) {
				if message != "" {
					message += ". "
				}
				message += diagnostic
			}
			if p.corsFallback {
				addDiagnostic("CORS could not be applied to rules returning a fixed 500 response for an unresolved backend")
			}
			if update.status.partiallyInvalid != "" {
				addDiagnostic(update.status.partiallyInvalid)
			}
			controller.SetCondition(&parentStatus.Conditions, controller.Condition(partial, metav1.ConditionTrue,
				string(gatewayv1.RouteReasonUnsupportedValue), message, route.Generation))
		} else {
			meta.RemoveStatusCondition(&parentStatus.Conditions, partial)
		}
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(string(gatewayv1.RouteConditionResolvedRefs), update.status.resolved, update.status.resolvedReason, update.status.resolvedMessage, resolvedGeneration))
		controller.SetCondition(&parentStatus.Conditions, controller.Condition(config.Domain()+"/Programmed", update.status.programmed, update.status.programmedReason, update.status.programmedMessage, programmedGeneration))
		setXBackendRevision(&parentStatus, config.Domain()+servicePersistenceRevisionCondition, p.servicePersistenceRevision, update.status.programmed, route.Generation)
		setXBackendRevision(&parentStatus, config.Domain()+xBackendPersistenceRevisionCondition, p.persistenceRevision, update.status.programmed, route.Generation)
		setXBackendRevision(&parentStatus, config.Domain()+xBackendTransportRevisionCondition, p.xBackendTransportRevision, update.status.programmed, route.Generation)
		if existing >= 0 {
			parents[existing] = parentStatus
		} else {
			parents = append(parents, parentStatus)
		}
	}
	route.Status.Parents = parents
	return transitioned
}

func setXBackendRevision(parent *gatewayv1.RouteParentStatus, conditionType string, revision *string, programmed metav1.ConditionStatus, generation int64) {
	switch {
	case programmed != metav1.ConditionTrue:
		meta.RemoveStatusCondition(&parent.Conditions, conditionType)
	case revision != nil && *revision != "":
		controller.SetCondition(&parent.Conditions, controller.Condition(conditionType, metav1.ConditionTrue, "Observed", *revision, generation))
	case revision != nil:
		meta.RemoveStatusCondition(&parent.Conditions, conditionType)
	}
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
