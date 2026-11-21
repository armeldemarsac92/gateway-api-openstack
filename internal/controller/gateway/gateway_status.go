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

package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) setGatewayFailure(gateway *gatewayv1.Gateway, validationErr error) {
	message := validationErr.Error()
	var detailed *gatewayValidationError
	errors.As(validationErr, &detailed)
	gatewayReason := gatewayv1.GatewayReasonListenersNotValid
	programmedReason := gatewayv1.GatewayReasonInvalid
	listenerSpecific := detailed == nil || detailed.gatewayReason == ""
	if detailed != nil {
		if detailed.gatewayReason != "" {
			gatewayReason = detailed.gatewayReason
		}
		if detailed.programmedReason != "" {
			programmedReason = detailed.programmedReason
		}
	}
	accepted := metav1.ConditionFalse
	if gatewayReason == gatewayv1.GatewayReasonAccepted {
		accepted = metav1.ConditionTrue
		if len(gateway.Spec.Listeners) > 1 {
			gatewayReason = gatewayv1.GatewayReasonListenersNotValid
		}
	}
	existingListeners := append([]gatewayv1.ListenerStatus(nil), gateway.Status.Listeners...)
	unsupportedMultipleL4 := len(gateway.Spec.Listeners) > 1 && !hasMultipleTransportListeners(gateway)
	gateway.Status.Addresses = nil
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionAccepted), accepted, string(gatewayReason), message, gateway.Generation))
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
	gateway.Status.Listeners = make([]gatewayv1.ListenerStatus, 0, len(gateway.Spec.Listeners))
	for _, listener := range gateway.Spec.Listeners {
		gateway.Status.Listeners = append(gateway.Status.Listeners, unprogrammedListenerStatus(
			listener, existingListeners, gateway.Generation, unsupportedMultipleL4, listenerSpecific, message,
		))
	}
}

func unprogrammedListenerStatus(
	listener gatewayv1.Listener,
	existing []gatewayv1.ListenerStatus,
	generation int64,
	multipleListeners bool,
	listenerSpecific bool,
	message string,
) gatewayv1.ListenerStatus {
	status := gatewayv1.ListenerStatus{
		Name:       listener.Name,
		Conditions: existingListenerConditionsFrom(existing, listener.Name),
	}
	kind := listenerRouteKind(listener.Protocol)
	l4InMultipleListeners := isL4Listener(listener.Protocol) && multipleListeners
	protocolSupported := kind != "" && !l4InMultipleListeners
	if protocolSupported && listenerAllowsRouteKind(listener, kind) {
		status.SupportedKinds = supportedRouteKinds(kind)
	}
	acceptedStatus := metav1.ConditionFalse
	acceptedReason := gatewayv1.ListenerReasonUnsupportedValue
	acceptedMessage := message
	programmedReason := gatewayv1.ListenerReasonInvalid
	if !protocolSupported {
		acceptedReason = gatewayv1.ListenerReasonUnsupportedProtocol
		acceptedMessage = "Listener protocol is unsupported"
		if l4InMultipleListeners {
			acceptedMessage = fmt.Sprintf("%s requires a Gateway with only %s listeners on distinct ports", listener.Protocol, listener.Protocol)
		}
	} else if !listenerSpecific {
		acceptedStatus = metav1.ConditionTrue
		acceptedReason = gatewayv1.ListenerReasonAccepted
		acceptedMessage = "Listener configuration is accepted"
		programmedReason = gatewayv1.ListenerReasonPending
	}
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), acceptedStatus, string(acceptedReason), acceptedMessage, generation))
	resolvedStatus := metav1.ConditionTrue
	resolvedReason := gatewayv1.ListenerReasonResolvedRefs
	resolvedMessage := "Listener has no unresolved references"
	if listener.AllowedRoutes != nil && slices.ContainsFunc(listener.AllowedRoutes.Kinds, func(ref gatewayv1.RouteGroupKind) bool {
		return !protocolSupported || !isRouteGroupKind(ref, kind)
	}) {
		resolvedStatus = metav1.ConditionFalse
		resolvedReason = gatewayv1.ListenerReasonInvalidRouteKinds
		resolvedMessage = "One or more allowed route kinds are unsupported on this listener"
	}
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), resolvedStatus, string(resolvedReason), resolvedMessage, generation))
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionConflicted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonNoConflicts), "Listener has no conflicts", generation))
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, generation))
	return status
}

func setGatewayAcceptedListeners(gateway *gatewayv1.Gateway, accepted ...gatewayv1.ListenerStatus) {
	reason := gatewayv1.GatewayReasonAccepted
	message := "Gateway configuration is accepted"
	if len(gateway.Spec.Listeners) != len(accepted) {
		reason = gatewayv1.GatewayReasonListenersNotValid
		message = "The HTTP listener is accepted. Other listeners cannot be programmed on this Gateway"
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
		string(gatewayv1.GatewayConditionAccepted), metav1.ConditionTrue, string(reason), message, gateway.Generation,
	))
	listeners := make([]gatewayv1.ListenerStatus, 0, len(gateway.Spec.Listeners))
	for _, listener := range gateway.Spec.Listeners {
		index := slices.IndexFunc(accepted, func(status gatewayv1.ListenerStatus) bool { return status.Name == listener.Name })
		if index >= 0 {
			listeners = append(listeners, accepted[index])
		} else {
			listeners = append(listeners, unprogrammedListenerStatus(
				listener, gateway.Status.Listeners, gateway.Generation, true, true, "Listener cannot be programmed on this Gateway",
			))
		}
	}
	gateway.Status.Listeners = listeners
}

func supportedRouteKinds(kind gatewayv1.Kind) []gatewayv1.RouteGroupKind {
	group := gatewayv1.Group(gatewayv1.GroupName)
	return []gatewayv1.RouteGroupKind{{Group: &group, Kind: kind}}
}

func existingListenerConditions(gateway *gatewayv1.Gateway, listenerName gatewayv1.SectionName) []metav1.Condition {
	return existingListenerConditionsFrom(gateway.Status.Listeners, listenerName)
}

func existingListenerConditionsFrom(statuses []gatewayv1.ListenerStatus, listenerName gatewayv1.SectionName) []metav1.Condition {
	for _, status := range statuses {
		if status.Name == listenerName {
			return append([]metav1.Condition(nil), status.Conditions...)
		}
	}
	return nil
}

func listenerStatusesWithoutControllerConditions(gateway *gatewayv1.Gateway) []gatewayv1.ListenerStatus {
	statuses := make([]gatewayv1.ListenerStatus, 0, len(gateway.Status.Listeners))
	for _, existing := range gateway.Status.Listeners {
		conditions := make([]metav1.Condition, 0, len(existing.Conditions))
		for _, candidate := range existing.Conditions {
			if !isControllerOwnedListenerCondition(candidate.Type) {
				conditions = append(conditions, candidate)
			}
		}
		if len(conditions) != 0 {
			statuses = append(statuses, gatewayv1.ListenerStatus{Name: existing.Name, Conditions: conditions})
		}
	}
	return statuses
}

func isControllerOwnedListenerCondition(conditionType string) bool {
	switch conditionType {
	case string(gatewayv1.ListenerConditionAccepted),
		string(gatewayv1.ListenerConditionResolvedRefs),
		string(gatewayv1.ListenerConditionConflicted),
		string(gatewayv1.ListenerConditionProgrammed):
		return true
	default:
		return false
	}
}

func (r *Reconciler) setGatewayFailureReason(gateway *gatewayv1.Gateway, reason gatewayv1.GatewayConditionReason, message string) {
	gateway.Status.Addresses = nil
	gateway.Status.Listeners = listenerStatusesWithoutControllerConditions(gateway)
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionAccepted), metav1.ConditionFalse, string(reason), message, gateway.Generation))
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, string(gatewayv1.GatewayReasonInvalid), message, gateway.Generation))
}

func (r *Reconciler) setGatewayProgressing(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, message string) error {
	return r.setGatewayProvisioningStatus(ctx, gateway, listener, metav1.ConditionFalse, gatewayv1.GatewayReasonNoResources, gatewayv1.ListenerReasonPending, message, true)
}

func (r *Reconciler) handleGatewayProviderFailure(
	ctx context.Context,
	scope *gatewayScope,
	listener *gatewayv1.Listener,
	providerErr error,
	observation cloud.GatewayResult,
) (ctrl.Result, error) {
	policy, ok := controller.ProviderFailurePolicyFor(providerErr)
	if !ok {
		return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "The Gateway graph could not be reconciled"}, providerErr)
	}
	gateway := scope.gateway
	gatewayReason, listenerReason := gatewayProviderFailureReasons(policy.Category)
	programmedGeneration := controller.ConditionObservedGeneration(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
		gateway.Generation,
		policy.AdvancesObservedGeneration,
	)
	transitioned := controller.ConditionTransitioned(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
		policy.ConditionStatus,
		string(gatewayReason),
		policy.Message,
		programmedGeneration,
	)
	var scopedReadyPorts []int
	if policy.Category == cloud.ErrorCategoryResourceFailure && observation.Outcome.State == cloud.OutcomeProgressing &&
		controller.GatewayListenerScope(r.Config, gateway) != "" {
		scopedReadyPorts = observation.ScopedListenerReadyPorts
	}
	if err := r.setGatewayListenerStatuses(
		ctx,
		gateway,
		listener,
		policy.ConditionStatus,
		gatewayReason,
		listenerReason,
		policy.Message,
		policy.Message,
		policy.AdvancesObservedGeneration,
		scopedReadyPorts,
	); err != nil {
		return ctrl.Result{}, errors.Join(controller.SafeProviderReconcileError(policy, providerErr), err)
	}
	if address := observation.State.Address(); address != "" && len(scopedReadyPorts) != 0 {
		setGatewayAddress(gateway, address)
	}
	if transitioned {
		scope.queueWarning(policy, "EnsureGateway")
	}
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, providerErr)
	}
	return controller.ProviderFailureResult(policy, providerErr, string(gateway.UID))
}

func (r *Reconciler) handleGatewayDeleteFailure(
	scope *gatewayScope,
	providerErr error,
) (ctrl.Result, error) {
	policy, ok := controller.ProviderFailurePolicyFor(providerErr)
	if !ok {
		return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "The Gateway graph could not be removed"}, providerErr)
	}
	gateway := scope.gateway
	policy = controller.ProviderCleanupFailurePolicy(policy, "Gateway")
	gatewayReason, listenerReason := gatewayProviderFailureReasons(policy.Category)
	programmedGeneration := controller.ConditionObservedGeneration(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
		gateway.Generation,
		policy.AdvancesObservedGeneration,
	)
	transitioned := controller.ConditionTransitioned(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
		policy.ConditionStatus,
		string(gatewayReason),
		policy.Message,
		programmedGeneration,
	)
	r.setGatewayCleanupFailure(gateway, policy, gatewayReason, listenerReason)
	if transitioned {
		scope.queueWarning(policy, "DeleteGateway")
	}
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, providerErr)
	}
	return controller.ProviderFinalizationFailureResult(policy, providerErr, string(gateway.UID))
}

func gatewayProviderFailureReasons(category cloud.ErrorCategory) (gatewayv1.GatewayConditionReason, gatewayv1.ListenerConditionReason) {
	switch category {
	case cloud.ErrorCategoryQuota:
		return gatewayv1.GatewayReasonNoResources, gatewayv1.ListenerReasonPending
	case cloud.ErrorCategoryTerminalValidation:
		return gatewayv1.GatewayReasonInvalid, gatewayv1.ListenerReasonInvalid
	case cloud.ErrorCategoryResourceFailure:
		return gatewayv1.GatewayConditionReason("ResourceFailed"), gatewayv1.ListenerConditionReason("ResourceFailed")
	case cloud.ErrorCategoryOwnershipConflict:
		return gatewayv1.GatewayConditionReason("OwnershipConflict"), gatewayv1.ListenerConditionReason("OwnershipConflict")
	default:
		return gatewayv1.GatewayReasonPending, gatewayv1.ListenerReasonPending
	}
}

func (r *Reconciler) setGatewayCleanupFailure(
	gateway *gatewayv1.Gateway,
	policy controller.ProviderFailurePolicy,
	gatewayReason gatewayv1.GatewayConditionReason,
	listenerReason gatewayv1.ListenerConditionReason,
) {
	programmedGeneration := controller.ConditionObservedGeneration(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
		gateway.Generation,
		policy.AdvancesObservedGeneration,
	)
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
		string(gatewayv1.GatewayConditionProgrammed),
		policy.ConditionStatus,
		string(gatewayReason),
		policy.Message,
		programmedGeneration,
	))
	for index := range gateway.Status.Listeners {
		accepted := meta.FindStatusCondition(gateway.Status.Listeners[index].Conditions, string(gatewayv1.ListenerConditionAccepted))
		if accepted != nil && accepted.Status == metav1.ConditionFalse && accepted.Reason == string(gatewayv1.ListenerReasonUnsupportedProtocol) {
			continue
		}
		listenerGeneration := controller.ConditionObservedGeneration(
			gateway.Status.Listeners[index].Conditions,
			string(gatewayv1.ListenerConditionProgrammed),
			gateway.Generation,
			policy.AdvancesObservedGeneration,
		)
		controller.SetCondition(&gateway.Status.Listeners[index].Conditions, controller.Condition(
			string(gatewayv1.ListenerConditionProgrammed),
			policy.ConditionStatus,
			string(listenerReason),
			policy.Message,
			listenerGeneration,
		))
	}
}

func (r *Reconciler) setGatewayProvisioningStatus(
	ctx context.Context,
	gateway *gatewayv1.Gateway,
	listener *gatewayv1.Listener,
	programmed metav1.ConditionStatus,
	gatewayReason gatewayv1.GatewayConditionReason,
	listenerReason gatewayv1.ListenerConditionReason,
	message string,
	advanceProgrammedGeneration bool,
) error {
	return r.setGatewayListenerStatuses(ctx, gateway, listener, programmed, gatewayReason, listenerReason, message, message, advanceProgrammedGeneration, nil)
}

func (r *Reconciler) setGatewayProgrammed(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, gatewayState cloud.GatewayState) error {
	message := "Octavia load balancer and listener are active"
	if hasMultipleTransportListeners(gateway) {
		message = "Octavia load balancer and listeners are active"
	}
	if err := r.setGatewayListenerStatuses(ctx, gateway, listener, metav1.ConditionTrue, gatewayv1.GatewayReasonProgrammed,
		gatewayv1.ListenerReasonProgrammed, message, "Octavia listener is active", true, nil); err != nil {
		return err
	}
	setGatewayAddress(gateway, gatewayState.Address())
	return nil
}

func (r *Reconciler) setGatewayAddressUnusable(
	ctx context.Context,
	gateway *gatewayv1.Gateway,
	listener *gatewayv1.Listener,
	result cloud.GatewayResult,
	progress cloud.Outcome,
) error {
	readyPorts := result.ScopedListenerReadyPorts
	if controller.GatewayListenerScope(r.Config, gateway) == "" && progress.State == cloud.OutcomeReady {
		readyPorts = []int{int(listener.Port)}
	}
	if err := r.setGatewayListenerStatuses(ctx, gateway, listener, metav1.ConditionFalse, gatewayv1.GatewayReasonAddressNotUsable,
		gatewayv1.ListenerReasonPending, result.AddressIssue, "Waiting for Octavia listener readiness", true, readyPorts); err != nil {
		return err
	}
	if address := result.State.Address(); address != "" {
		setGatewayAddress(gateway, address)
	} else {
		gateway.Status.Addresses = nil
	}
	return nil
}

func setGatewayAddress(gateway *gatewayv1.Gateway, address string) {
	addressType := gatewayv1.IPAddressType
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Type: &addressType, Value: address}}
}

func (r *Reconciler) setGatewayListenerStatuses(
	ctx context.Context,
	gateway *gatewayv1.Gateway,
	primary *gatewayv1.Listener,
	programmed metav1.ConditionStatus,
	gatewayReason gatewayv1.GatewayConditionReason,
	listenerReason gatewayv1.ListenerConditionReason,
	gatewayMessage, listenerMessage string,
	advanceProgrammedGeneration bool,
	readyPorts []int,
) error {
	listeners := []*gatewayv1.Listener{primary}
	if hasMultipleTransportListeners(gateway) {
		listeners = make([]*gatewayv1.Listener, 0, len(gateway.Spec.Listeners))
		for index := range gateway.Spec.Listeners {
			listeners = append(listeners, &gateway.Spec.Listeners[index])
		}
	}
	counts, err := r.attachedRouteCounts(ctx, gateway, listeners)
	if err != nil {
		return err
	}
	statuses := make([]gatewayv1.ListenerStatus, 0, len(listeners))
	for _, listener := range listeners {
		status := gatewayv1.ListenerStatus{
			Name:           listener.Name,
			SupportedKinds: supportedRouteKinds(listenerRouteKind(listener.Protocol)),
			AttachedRoutes: counts[listener.Name],
			Conditions:     existingListenerConditions(gateway, listener.Name),
		}
		setAcceptedListenerConditions(&status.Conditions, gateway.Generation)
		generation := controller.ConditionObservedGeneration(status.Conditions, string(gatewayv1.ListenerConditionProgrammed), gateway.Generation, advanceProgrammedGeneration)
		portProgrammed, portReason, portMessage := programmed, listenerReason, listenerMessage
		if slices.Contains(readyPorts, int(listener.Port)) {
			portProgrammed, portReason, portMessage = metav1.ConditionTrue, gatewayv1.ListenerReasonProgrammed, "Octavia listener is active"
		}
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), portProgrammed, string(portReason), portMessage, generation))
		statuses = append(statuses, status)
	}
	generation := controller.ConditionObservedGeneration(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed), gateway.Generation, advanceProgrammedGeneration)
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), programmed, string(gatewayReason), gatewayMessage, generation))
	setGatewayAcceptedListeners(gateway, statuses...)
	return nil
}

func setAcceptedListenerConditions(conditions *[]metav1.Condition, generation int64) {
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionTrue, string(gatewayv1.ListenerReasonAccepted), "Listener is accepted", generation))
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), metav1.ConditionTrue, string(gatewayv1.ListenerReasonResolvedRefs), "Listener references are resolved", generation))
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionConflicted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonNoConflicts), "Listener has no conflicts", generation))
}

func (r *Reconciler) attachedRouteCounts(ctx context.Context, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener) (map[gatewayv1.SectionName]int32, error) {
	gatewayKey := controller.ObjectKeyString(client.ObjectKeyFromObject(gateway))
	counts := make(map[gatewayv1.SectionName]int32, len(listeners))
	namespaces := make(map[string][]corev1.Namespace)
	countRoute := func(object runtime.Object) error {
		route := object.(client.Object)
		if !route.GetDeletionTimestamp().IsZero() {
			return nil
		}
		refs, parents, _ := controller.RouteParents(route)
		routeNamespace := route.GetNamespace()
		for _, listener := range listeners {
			for _, parentStatus := range parents {
				if !routeParentStatusTargetsListener(parentStatus, routeNamespace, gateway, listener, r.Config.ControllerName) {
					continue
				}
				accepted := meta.FindStatusCondition(parentStatus.Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != route.GetGeneration() ||
					!slices.ContainsFunc(refs, func(ref gatewayv1.ParentReference) bool {
						return controller.ParentRefsEqual(ref, routeNamespace, parentStatus.ParentRef, routeNamespace)
					}) {
					continue
				}
				if _, observed := namespaces[routeNamespace]; !observed && model.ObservesRouteNamespace(listener) {
					if r.APIReader == nil {
						return controller.ErrAPIReaderRequired
					}
					namespace, err := graph.ReadRouteNamespace(ctx, r.APIReader, routeNamespace)
					if err != nil {
						return err
					}
					namespaces[routeNamespace] = nil
					if namespace != nil {
						namespaces[routeNamespace] = []corev1.Namespace{*namespace}
					}
				}
				if model.AllowsRouteNamespace(listener, gateway.Namespace, routeNamespace, namespaces[routeNamespace]) {
					counts[listener.Name]++
				}
				break
			}
		}
		return nil
	}
	var routes client.ObjectList
	index := controller.IndexL4RouteByStatusGateway
	switch listeners[0].Protocol {
	case gatewayv1.TCPProtocolType:
		routes = &gatewayv1.TCPRouteList{}
	case gatewayv1.UDPProtocolType:
		routes = &gatewayv1.UDPRouteList{}
	default:
		routes = &gatewayv1.HTTPRouteList{}
		index = controller.IndexHTTPRouteByStatusGateway
	}
	if err := r.List(ctx, routes, client.MatchingFields{index: gatewayKey}); err != nil {
		return nil, err
	}
	if err := meta.EachListItem(routes, countRoute); err != nil {
		return nil, err
	}
	return counts, nil
}

func routeParentStatusTargetsListener(
	parentStatus gatewayv1.RouteParentStatus,
	routeNamespace string,
	gateway *gatewayv1.Gateway,
	listener *gatewayv1.Listener,
	controllerName gatewayv1.GatewayController,
) bool {
	parentRef := parentStatus.ParentRef
	if parentStatus.ControllerName != controllerName || !controller.IsGatewayParentRef(parentRef) || string(parentRef.Name) != gateway.Name {
		return false
	}
	if parentRef.Namespace != nil {
		routeNamespace = string(*parentRef.Namespace)
	}
	if routeNamespace != gateway.Namespace {
		return false
	}
	if parentRef.SectionName != nil && *parentRef.SectionName != listener.Name {
		return false
	}
	return parentRef.Port == nil || *parentRef.Port == listener.Port
}
