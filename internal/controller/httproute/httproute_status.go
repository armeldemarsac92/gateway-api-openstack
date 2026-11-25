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
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeReconcileStatus struct {
	accepted          metav1.ConditionStatus
	acceptedReason    string
	acceptedMessage   string
	partiallyInvalid  string
	implementedRules  bool
	resolved          metav1.ConditionStatus
	resolvedReason    string
	resolvedMessage   string
	programmed        metav1.ConditionStatus
	programmedReason  string
	programmedMessage string
}

type parentStatusUpdate struct {
	parent gatewayv1.ParentReference
	status routeReconcileStatus
}

func (r *Reconciler) setRouteStatusesAndDetach(
	ctx context.Context,
	scope *httpRouteScope,
	updates []parentStatusUpdate,
) (ctrl.Result, error) {
	route := scope.route
	updates = scope.completeParentUpdates(updates)
	if err := r.applyUnsupportedRouteReferenceStatus(ctx, scope, updates); err != nil {
		return ctrl.Result{}, err
	}
	providerStatusPublished := routeHasProviderFailureStatus(route, updates, r.Config.ControllerName, r.Config.Domain()+"/Programmed", scope.parentGateway != nil)
	checkpointNeeded, err := r.routeCleanupStatusCheckpointNeeded(ctx, scope, updates)
	if err != nil {
		return ctrl.Result{}, err
	}
	if checkpointNeeded {
		return ctrl.Result{Requeue: true}, nil
	}
	var result ctrl.Result
	var cleanupErr error
	var identity cloud.Identity
	if scope.parentGateway == nil {
		result, cleanupErr = r.detachRoute(ctx, route)
	} else {
		identity = controller.RouteIdentity(r.Config, scope.parentGateway, route)
		binding, err := controller.StoredRouteGatewayBinding(r.Config, route, identity)
		if err != nil {
			return ctrl.Result{}, err
		}
		if binding.Identity == identity {
			result, cleanupErr = r.detachGatewayBinding(ctx, route, route.DeepCopy(), identity)
		}
	}
	if cleanupErr == nil {
		if providerStatusPublished {
			scope.setStatuses(updates)
			if route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
				// The detach checkpoint already verified the pre-detach election.
				scope.statusPlan.profileObserved = false
			}
		} else {
			// The rejection status was already durable before detaching.
			scope.statusPlan = nil
		}
		return result, nil
	}
	if errors.Is(cleanupErr, controller.ErrUnsupportedGatewayAPIVersion) {
		// Retry if the live cleanup check no longer confirms the reason for
		// taking forwarding down during the freeze.
		if scope.versionSkew || slices.ContainsFunc(updates, func(update parentStatusUpdate) bool {
			return update.status.resolvedReason == string(gatewayv1.RouteReasonRefNotPermitted)
		}) {
			scope.statusPlan = nil
			return ctrl.Result{}, errHTTPRouteChanged
		}
		bindings, bindingErr := controller.StoredRouteGatewayBindings(r.Config, route)
		if bindingErr != nil {
			return ctrl.Result{}, bindingErr
		}
		if len(bindings) == 0 {
			return ctrl.Result{}, cleanupErr
		}
		return r.setRouteUnsupportedVersion(ctx, scope)
	}
	policy, ok := controller.ProviderFailurePolicyFor(cleanupErr)
	if !ok {
		return ctrl.Result{}, cleanupErr
	}
	policy = controller.ProviderCleanupFailurePolicy(policy, "HTTPRoute")
	diagnosticTransitioned, diagnosticErr := r.patchRouteCleanupFailure(ctx, route, identity, policy.Reason)
	providerUpdates := routeProviderFailureUpdates(updates, policy)
	if len(providerUpdates) == 0 {
		scope.setStatuses(nil)
	} else {
		scope.setProviderFailureStatuses(providerUpdates, policy, true)
	}
	if diagnosticErr != nil {
		return ctrl.Result{}, errors.Join(diagnosticErr, controller.SafeProviderReconcileError(policy, cleanupErr))
	}
	scope.queueWarning(routeProviderWarning{
		policy:                 policy,
		action:                 "DeleteHTTPRoute",
		cleanupCheckpointSaved: diagnosticTransitioned,
		recordOnStatusChange:   len(providerUpdates) == 0,
	})
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, cleanupErr)
	}
	return controller.ProviderFinalizationFailureResult(policy, cleanupErr, string(route.UID))
}

func (r *Reconciler) applyUnsupportedRouteReferenceStatus(ctx context.Context, scope *httpRouteScope, updates []parentStatusUpdate) error {
	route := scope.route
	needsAuthorization := func(update parentStatusUpdate) bool {
		return update.status.acceptedReason == string(gatewayv1.RouteReasonUnsupportedValue) &&
			update.status.resolved == metav1.ConditionUnknown
	}
	if !slices.ContainsFunc(updates, needsAuthorization) {
		return nil
	}
	// A bound route must detach even if a reference check fails. Its binding
	// change enqueues the unbound route for independent status evaluation.
	bindings, err := controller.StoredRouteGatewayBindings(r.Config, route)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if scope.parentGateway == nil || controller.SameGatewayIdentity(binding.Identity, controller.RouteIdentity(r.Config, scope.parentGateway, route)) {
			return nil
		}
	}
	rejection, err := r.liveRouteBackendReferenceRejection(ctx, route)
	if err != nil || rejection == nil {
		return err
	}
	diagnostic := model.Diagnostic{Reason: string(gatewayv1.RouteReasonRefNotPermitted), Message: rejection.message}
	for i := range updates {
		if needsAuthorization(updates[i]) {
			updates[i].status = updates[i].status.withBackendDiagnostic(diagnostic)
		}
	}
	return nil
}

func routeHasProviderFailureStatus(
	route *gatewayv1.HTTPRoute,
	updates []parentStatusUpdate,
	controllerName gatewayv1.GatewayController,
	conditionType string,
	partial bool,
) bool {
	if len(updates) == 0 {
		return false
	}
	for _, update := range updates {
		found := false
		for _, parent := range route.Status.Parents {
			if parent.ControllerName != controllerName || !controller.ParentRefsEqual(parent.ParentRef, route.Namespace, update.parent, route.Namespace) {
				continue
			}
			programmed := meta.FindStatusCondition(parent.Conditions, conditionType)
			if programmed != nil && programmed.Status != metav1.ConditionTrue && isProviderFailureReason(programmed.Reason) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if partial {
		return true
	}
	for _, parent := range route.Status.Parents {
		if parent.ControllerName != controllerName || hasNonGatewayParentRef(route, parent.ParentRef) {
			continue
		}
		planned := false
		for _, update := range updates {
			if controller.ParentRefsEqual(parent.ParentRef, route.Namespace, update.parent, route.Namespace) {
				planned = true
				break
			}
		}
		programmed := meta.FindStatusCondition(parent.Conditions, conditionType)
		if !planned || programmed == nil || programmed.Status == metav1.ConditionTrue || !isProviderFailureReason(programmed.Reason) {
			return false
		}
	}
	return true
}

func isProviderFailureReason(reason string) bool {
	switch reason {
	case controller.ProviderReasonAuthenticationFailed, controller.ProviderReasonAuthorizationFailed, controller.ProviderReasonQuotaExceeded,
		controller.ProviderReasonRateLimited, controller.ProviderReasonRequestTimedOut, controller.ProviderReasonUnavailable,
		controller.ProviderReasonInvalidConfiguration, controller.ProviderReasonResourceFailed, controller.ProviderReasonOwnershipConflict:
		return true
	default:
		return false
	}
}

func routeProviderFailureUpdates(updates []parentStatusUpdate, policy controller.ProviderFailurePolicy) []parentStatusUpdate {
	result := make([]parentStatusUpdate, len(updates))
	for index := range updates {
		result[index] = updates[index]
		result[index].status.programmed = policy.ConditionStatus
		result[index].status.programmedReason = policy.Reason
		result[index].status.programmedMessage = policy.Message
	}
	return result
}

func (r *Reconciler) handleRouteProviderFailure(
	scope *httpRouteScope,
	parent gatewayv1.ParentReference,
	referencesEvaluated bool,
	diagnostic model.Diagnostic,
	providerErr error,
	action string,
) (ctrl.Result, error) {
	policy, ok := controller.ProviderFailurePolicyFor(providerErr)
	if !ok {
		return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "The HTTPRoute graph could not be reconciled"}, providerErr)
	}
	route := scope.route
	status := providerFailureRouteStatus(policy, referencesEvaluated).withBackendDiagnostic(diagnostic)
	scope.setProviderFailureStatuses(
		[]parentStatusUpdate{{parent: parent, status: status}},
		policy,
		referencesEvaluated,
	)
	scope.queueWarning(routeProviderWarning{policy: policy, action: action})
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, providerErr)
	}
	return controller.ProviderFailureResult(policy, providerErr, string(route.UID))
}

func (r *Reconciler) handleRouteFinalizationFailure(
	ctx context.Context,
	scope *httpRouteScope,
	identity cloud.Identity,
	providerErr error,
) (ctrl.Result, error) {
	policy, ok := controller.ProviderFailurePolicyFor(providerErr)
	if !ok {
		return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "The HTTPRoute graph could not be removed"}, providerErr)
	}
	route := scope.route
	policy = controller.ProviderCleanupFailurePolicy(policy, "HTTPRoute")
	diagnosticTransitioned, diagnosticErr := r.patchRouteCleanupFailure(ctx, route, identity, policy.Reason)
	scope.setDeletingFailureStatus(policy, identity)
	if diagnosticErr != nil {
		return ctrl.Result{}, errors.Join(
			controller.SafeProviderReconcileError(policy, providerErr),
			fmt.Errorf("publish HTTPRoute cleanup failure: %w", diagnosticErr),
		)
	}
	scope.queueWarning(routeProviderWarning{
		policy:                 policy,
		action:                 "DeleteHTTPRoute",
		cleanupCheckpointSaved: diagnosticTransitioned,
	})
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, providerErr)
	}
	return controller.ProviderFinalizationFailureResult(policy, providerErr, string(route.UID))
}

func providerFailureRouteStatus(policy controller.ProviderFailurePolicy, referencesEvaluated bool) routeReconcileStatus {
	status := programmedRouteStatus()
	if !referencesEvaluated {
		status.resolved = metav1.ConditionUnknown
		status.resolvedReason = string(gatewayv1.RouteReasonPending)
		status.resolvedMessage = "Backend references have not been evaluated"
	}
	status.programmed = policy.ConditionStatus
	status.programmedReason = policy.Reason
	status.programmedMessage = policy.Message
	return status
}

func statusForRouteBuildError(err error) routeReconcileStatus {
	if apierrors.IsNotFound(err) {
		return unresolvedRouteStatus(string(gatewayv1.RouteReasonBackendNotFound), err.Error())
	}
	var buildError *routeBuildError
	if errors.As(err, &buildError) {
		switch buildError.kind {
		case routeErrorInvalid, routeErrorIncompatibleFilters:
			return rejectedRouteStatus(string(buildError.kind), buildError.message)
		case routeErrorInvalidKind:
			return unresolvedRouteStatus(string(gatewayv1.RouteReasonInvalidKind), buildError.message)
		case routeErrorBackendNotFound:
			return unresolvedRouteStatus(string(gatewayv1.RouteReasonBackendNotFound), buildError.message)
		case routeErrorRefNotPermitted:
			return unresolvedRouteStatus(string(gatewayv1.RouteReasonRefNotPermitted), buildError.message)
		case routeErrorUnsupportedProtocol:
			return unresolvedRouteStatus(string(gatewayv1.RouteReasonUnsupportedProtocol), buildError.message)
		case routeErrorNoMatchingParent:
			return rejectedRouteStatus(string(gatewayv1.RouteReasonNoMatchingParent), buildError.message)
		case routeErrorNoMatchingHostname:
			return rejectedRouteStatus(string(gatewayv1.RouteReasonNoMatchingListenerHostname), buildError.message)
		case routeErrorNotAllowed:
			return rejectedRouteStatus(string(gatewayv1.RouteReasonNotAllowedByListeners), buildError.message)
		case routeErrorPending:
			return pendingRouteStatus(buildError.message)
		default:
			return rejectedRouteStatus(string(gatewayv1.RouteReasonUnsupportedValue), buildError.message)
		}
	}
	return rejectedRouteStatus(string(gatewayv1.RouteReasonUnsupportedValue), err.Error())
}

func programmedRouteStatus() routeReconcileStatus {
	return routeReconcileStatus{
		accepted:          metav1.ConditionTrue,
		acceptedReason:    string(gatewayv1.RouteReasonAccepted),
		acceptedMessage:   "Route is accepted by the Gateway",
		resolved:          metav1.ConditionTrue,
		resolvedReason:    string(gatewayv1.RouteReasonResolvedRefs),
		resolvedMessage:   "All backend references are resolved",
		programmed:        metav1.ConditionTrue,
		programmedReason:  "Programmed",
		programmedMessage: "Octavia configuration for the route is active",
	}
}

func pendingRouteStatus(message string) routeReconcileStatus {
	status := programmedRouteStatus()
	status.programmed = metav1.ConditionFalse
	status.programmedReason = "Pending"
	status.programmedMessage = message
	return status
}

func (s routeReconcileStatus) withBackendDiagnostic(diagnostic model.Diagnostic) routeReconcileStatus {
	if diagnostic.Reason != "" {
		s.resolved = metav1.ConditionFalse
		s.resolvedReason = diagnostic.Reason
		s.resolvedMessage = diagnostic.Message
	}
	return s
}

func (s routeReconcileStatus) withBackendTLSAncestor(slot backendtls.AncestorSlot) routeReconcileStatus {
	switch slot {
	case backendtls.AncestorSlotFull:
		s.programmed, s.programmedReason, s.programmedMessage =
			metav1.ConditionFalse, backendtls.AncestorLimitReason, backendtls.AncestorLimitMessage
	case backendtls.AncestorSlotPending:
		if s.programmed == metav1.ConditionTrue {
			s.programmed, s.programmedReason, s.programmedMessage =
				metav1.ConditionFalse, "Pending", "Waiting for BackendTLSPolicy ancestor status for this Gateway"
		}
	}
	return s
}

func (s routeReconcileStatus) withRetryBudgetAncestorLimit(full bool) routeReconcileStatus {
	if full && s.programmed == metav1.ConditionTrue {
		s.programmed, s.programmedReason, s.programmedMessage = metav1.ConditionFalse, "BackendRetryBudgetAncestorLimit",
			"An XBackendTrafficPolicy used by this route has no room for this Gateway in its ancestor status"
	}
	return s
}

func unsupportedGatewayAPIVersionRouteStatus() routeReconcileStatus {
	return routeReconcileStatus{
		accepted:          metav1.ConditionUnknown,
		acceptedReason:    string(gatewayv1.RouteReasonPending),
		acceptedMessage:   controller.UnsupportedGatewayAPIVersionMessage,
		resolved:          metav1.ConditionUnknown,
		resolvedReason:    string(gatewayv1.RouteReasonPending),
		resolvedMessage:   "Backend references were not evaluated",
		programmed:        metav1.ConditionFalse,
		programmedReason:  "Pending",
		programmedMessage: controller.UnsupportedGatewayAPIVersionMessage,
	}
}

func rejectedRouteStatus(reason, message string) routeReconcileStatus {
	return routeReconcileStatus{
		accepted:          metav1.ConditionFalse,
		acceptedReason:    reason,
		acceptedMessage:   message,
		resolved:          metav1.ConditionUnknown,
		resolvedReason:    string(gatewayv1.RouteReasonPending),
		resolvedMessage:   "Backend references were not evaluated because the route is unsupported",
		programmed:        metav1.ConditionFalse,
		programmedReason:  "Invalid",
		programmedMessage: message,
	}
}

// resolveRouteReferences evaluates object references without requiring ready endpoints.
func (r *Reconciler) resolveRouteReferences(ctx context.Context, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway, status *routeReconcileStatus) error {
	backends, err := r.validateRouteStructure(route)
	if err != nil {
		var rejection *routeBuildError
		if errors.As(err, &rejection) && rejection.kind == routeErrorInvalidKind {
			status.resolved = metav1.ConditionFalse
			status.resolvedReason = string(gatewayv1.RouteReasonInvalidKind)
			status.resolvedMessage = rejection.message
		}
		return nil
	}
	grantsByNamespace, rejection, err := r.observeRouteBackendPermissions(ctx, route)
	if err != nil {
		return err
	}
	status.resolved = metav1.ConditionFalse
	if rejection != nil {
		status.resolvedReason = string(gatewayv1.RouteReasonRefNotPermitted)
		status.resolvedMessage = rejection.message
		return nil
	}
	status.resolved = metav1.ConditionTrue
	status.resolvedReason = string(gatewayv1.RouteReasonResolvedRefs)
	status.resolvedMessage = "All backend references are resolved"
	rules, _ := r.projectedHTTPFilterRules(route)
	for _, rule := range rules {
		for _, backend := range rule.BackendRefs {
			if diagnostic := model.UnresolvedHTTPBackendExtension(backend); diagnostic != nil {
				*status = status.withBackendDiagnostic(*diagnostic)
			}
		}
	}
	if r.Config.HTTPRequestMirror && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind {
		if err := r.resolveRequestMirrorReferences(ctx, route, rules, grantsByNamespace, status); err != nil {
			return err
		}
	}
	grpc := model.HTTPProfileRouteKind(route) == model.GRPCRouteKind
	cookieCapability := r.xBackendCookieCapability(ctx)
	input := model.Input{Routes: []gatewayv1.HTTPRoute{*route}, BackendH2C: r.Config.BackendH2C}
	if grpc {
		for _, grants := range grantsByNamespace {
			input.ReferenceGrants = append(input.ReferenceGrants, grants...)
		}
	}
	for _, backend := range backends {
		if key, xbackend := model.XBackendReference(route.Namespace, backend); xbackend && r.Config.XBackendEnabled() && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind {
			if backend.Port != nil {
				*status = status.withBackendDiagnostic(model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "backendRef.port must be omitted for XBackend"})
				continue
			}
			object, err := graph.ReadXBackend(ctx, r.routePatchReader(), key)
			if err != nil {
				return err
			}
			var spec *gatewayx.BackendSpec
			if object != nil {
				spec = &object.Spec
			}
			profile, err := r.xBackendProfile(gateway)
			if err != nil {
				return err
			}
			pending := profile.MemberMode == ""
			if pending {
				// Check the spec and class now while topology is pending.
				profile.MemberMode = cloud.MemberModeEndpoint
			}
			if diagnostic := model.CheckXBackendTarget(key, spec, profile); diagnostic != nil {
				*status = status.withBackendDiagnostic(*diagnostic)
			} else if pending && (spec.Type == gatewayx.BackendTypeExternalHostname || spec.SessionPersistence != nil) && status.resolved == metav1.ConditionTrue {
				status.resolved, status.resolvedReason, status.resolvedMessage = metav1.ConditionUnknown, "Pending", "Gateway member mode is pending"
			} else if spec.SessionPersistence != nil && status.resolved != metav1.ConditionFalse {
				status.resolved, status.resolvedReason, status.resolvedMessage = cookieCapability.apply(status.resolved, status.resolvedReason, status.resolvedMessage)
			}
			continue
		}
		key, supported := model.BackendServiceReference(route.Namespace, backend)
		if !supported {
			*status = status.withBackendDiagnostic(model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonInvalidKind),
				Message: fmt.Sprintf("backendRef group %q and kind %q are unsupported",
					ptr.Deref(backend.Group, ""), ptr.Deref(backend.Kind, "Service")),
			})
			continue
		}
		var service corev1.Service
		err := r.routePatchReader().Get(ctx, key, &service)
		if apierrors.IsNotFound(err) || !service.DeletionTimestamp.IsZero() {
			*status = status.withBackendDiagnostic(model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: fmt.Sprintf("backend Service %s was not found", key),
			})
			continue
		}
		if err != nil {
			return fmt.Errorf("get backend Service: %w", err)
		}
		if grpc {
			input.Services = append(input.Services, service)
		} else if _, diagnostic := model.ResolveServicePort(&service, backend.Port, r.Config.BackendH2C); diagnostic != nil {
			*status = status.withBackendDiagnostic(*diagnostic)
		}
	}
	if grpc && r.Config.BackendTLSPolicy {
		if err := graph.CollectBackendTLSInputs(ctx, r.routePatchReader(), &input, nil, r.Config.ClusterTrustBundles); err != nil {
			return err
		}
	}
	if grpc {
		for _, rule := range route.Spec.Rules {
			for _, backend := range model.ResolveBackends(route, rule, input) {
				if backend.Diagnostic != nil {
					*status = status.withBackendDiagnostic(*backend.Diagnostic)
				}
			}
		}
	}
	return nil
}

func (r *Reconciler) resolveRequestMirrorReferences(ctx context.Context, route *gatewayv1.HTTPRoute, rules []gatewayv1.HTTPRouteRule, grants map[string][]gatewayv1.ReferenceGrant, status *routeReconcileStatus) error {
	services := map[types.NamespacedName]*corev1.Service{}
	for _, rule := range rules {
		for _, ref := range model.RequestMirrorReferences(rule) {
			key, supported := model.BackendServiceReference(route.Namespace, ref)
			if !supported {
				*status = status.withBackendDiagnostic(model.Diagnostic{Reason: string(gatewayv1.RouteReasonInvalidKind),
					Message: "RequestMirror backendRef must reference a core Service"})
				continue
			}
			if key.Namespace != route.Namespace {
				rows, observed := grants[key.Namespace]
				if !observed {
					if r.APIReader == nil {
						return controller.ErrAPIReaderRequired
					}
					var err error
					rows, err = graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
					if err != nil {
						return err
					}
					grants[key.Namespace] = rows
				}
				if denied := backendReferenceRejection(model.HTTPRouteKind, route.Namespace, key, rows); denied != nil {
					*status = status.withBackendDiagnostic(model.Diagnostic{Reason: string(gatewayv1.RouteReasonRefNotPermitted), Message: denied.message})
					continue
				}
			}
			service, observed := services[key]
			if !observed {
				service = &corev1.Service{}
				if err := r.routePatchReader().Get(ctx, key, service); err != nil {
					if !apierrors.IsNotFound(err) {
						return fmt.Errorf("get RequestMirror backend Service: %w", err)
					}
					service = nil
				}
				services[key] = service
			}
			switch {
			case service == nil || !service.DeletionTimestamp.IsZero():
				*status = status.withBackendDiagnostic(model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound),
					Message: fmt.Sprintf("RequestMirror backend Service %s was not found", key)})
			case service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeExternalName:
				*status = status.withBackendDiagnostic(model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue),
					Message: "RequestMirror backend Service must use ClusterIP or NodePort type"})
			default:
				_, diagnostic := model.ResolveServicePortForBackendTLS(service, ref.Port, r.Config.BackendH2C, r.Config.BackendTLSPolicy)
				if diagnostic != nil {
					*status = status.withBackendDiagnostic(*diagnostic)
				}
			}
		}
	}
	return nil
}

func backendProfileRejectionStatus(err error) routeReconcileStatus {
	status := statusForRouteBuildError(err)
	if status.resolvedReason == string(gatewayv1.RouteReasonRefNotPermitted) {
		return status
	}
	status.accepted = metav1.ConditionFalse
	status.acceptedReason = string(gatewayv1.RouteReasonUnsupportedValue)
	status.acceptedMessage = err.Error()
	return status
}

func unresolvedRouteStatus(reason, message string) routeReconcileStatus {
	status := programmedRouteStatus()
	status.resolved = metav1.ConditionFalse
	status.resolvedReason = reason
	status.resolvedMessage = message
	status.programmed = metav1.ConditionFalse
	status.programmedReason = "Invalid"
	status.programmedMessage = message
	return status
}

func (r *Reconciler) setRouteParentStatuses(ctx context.Context, route *gatewayv1.HTTPRoute, updates []parentStatusUpdate) error {
	plan := &routeStatusPlan{
		kind:    routeStatusPlanReplace,
		updates: append([]parentStatusUpdate(nil), updates...),
	}
	_, _, _, err := r.patchHTTPRouteStatus(ctx, route, plan)
	return err
}
