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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
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
		if listeners, _ := SupportedListenersForConfig(r.Config, gateway); len(listeners) != len(gateway.Spec.Listeners) {
			gatewayReason = gatewayv1.GatewayReasonListenersNotValid
		}
	}
	existingListeners := append([]gatewayv1.ListenerStatus(nil), gateway.Status.Listeners...)
	unsupportedMultipleL4 := len(gateway.Spec.Listeners) > 1 && !hasMultipleTransportListeners(gateway)
	if accepted == metav1.ConditionFalse && gateway.Status.AttachedListenerSets != nil {
		gateway.Status.AttachedListenerSets = ptr.To(int32(0))
	}
	gateway.Status.Addresses = nil
	if r.Config.GatewayAddressRoutabilityEnabled() {
		meta.RemoveStatusCondition(&gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionAccepted), accepted, string(gatewayReason), message, gateway.Generation))
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
	gateway.Status.Listeners = make([]gatewayv1.ListenerStatus, 0, len(gateway.Spec.Listeners))
	for _, listener := range gateway.Spec.Listeners {
		status := r.unprogrammedListenerStatus(listener, existingListeners, gateway.Generation, unsupportedMultipleL4, listenerSpecific, message)
		if r.Config.TCPTLSTermination && !r.Config.TCPTLSMixedMode && mixedTLSModes(gateway.Spec.Listeners, listener) {
			status.SupportedKinds = nil
			message := "Terminate and Passthrough require separate TLS listener ports"
			controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonProtocolConflict), message, gateway.Generation))
			controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionConflicted), metav1.ConditionTrue, string(gatewayv1.ListenerReasonProtocolConflict), message, gateway.Generation))
		}
		gateway.Status.Listeners = append(gateway.Status.Listeners, status)
	}
}

func (r *Reconciler) unprogrammedListenerStatus(
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
	protocolSupported := kind != "" && listener.Protocol != gatewayv1.HTTPSProtocolType && (listener.Protocol != gatewayv1.TLSProtocolType || r.Config.TLSRoutesEnabled() || terminatesTLS(&listener)) && !l4InMultipleListeners
	if protocolSupported {
		status.SupportedKinds = listenerSupportedRouteKinds(r.Config, listener)
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
		return !protocolSupported || !listenerSupportsRouteKind(r.Config, listener, ref)
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

func (r *Reconciler) setGatewayAcceptedListeners(gateway *gatewayv1.Gateway, accepted ...gatewayv1.ListenerStatus) {
	reason := gatewayv1.GatewayReasonAccepted
	message := "Gateway configuration is accepted"
	if len(gateway.Spec.Listeners) != len(accepted) {
		reason = gatewayv1.GatewayReasonListenersNotValid
		message = "The HTTP listener is accepted. Other listeners cannot be programmed on this Gateway"
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
		string(gatewayv1.GatewayConditionAccepted), metav1.ConditionTrue, string(reason), message, gateway.Generation,
	))
	r.replaceGatewayListenerStatuses(gateway, accepted)
}

func (r *Reconciler) replaceGatewayListenerStatuses(gateway *gatewayv1.Gateway, accepted []gatewayv1.ListenerStatus) {
	listeners := make([]gatewayv1.ListenerStatus, 0, len(gateway.Spec.Listeners))
	for _, listener := range gateway.Spec.Listeners {
		index := slices.IndexFunc(accepted, func(status gatewayv1.ListenerStatus) bool { return status.Name == listener.Name })
		if index >= 0 {
			listeners = append(listeners, accepted[index])
		} else {
			listeners = append(listeners, r.unprogrammedListenerStatus(
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

func listenerSupportedRouteKinds(config controller.Config, listener gatewayv1.Listener) []gatewayv1.RouteGroupKind {
	if listener.Protocol == gatewayv1.TLSProtocolType && !tlsListenerModeEnabled(config, &listener) {
		return nil
	}
	var result []gatewayv1.RouteGroupKind
	kind := listenerRouteKind(listener.Protocol)
	if kind != "" && listenerAllowsRouteKind(listener, kind) {
		result = append(result, supportedRouteKinds(kind)...)
	}
	if listener.Protocol == gatewayv1.HTTPProtocolType && config.GRPCExactMethodEnabled() && listenerAllowsRouteKind(listener, model.GRPCRouteKind) {
		result = append(result, supportedRouteKinds(model.GRPCRouteKind)...)
	}
	return result
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
	if r.Config.GatewayAddressRoutabilityEnabled() {
		meta.RemoveStatusCondition(&gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	}
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
	if terminatesTLS(listener) && observation.Outcome.State == cloud.OutcomeProgressing && observation.Outcome.Message != "" {
		policy.Message = observation.Outcome.Message
	}
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
	scopedReadyPorts := scopedGatewayFailurePorts(r.Config, gateway, policy.Category, observation)
	options := gatewayListenerStatusOptions{readyPorts: scopedReadyPorts}
	var missing *octavia.MissingGatewayCapability
	if errors.As(providerErr, &missing) && missing.Capability == octavia.GatewayCapabilityTCPTLSMixedMode {
		options.rejectedMixedTLSPort = listener.Port
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
		options,
	); err != nil {
		return ctrl.Result{}, errors.Join(controller.SafeProviderReconcileError(policy, providerErr), err)
	}
	if options.rejectedMixedTLSPort != 0 {
		const message = "Octavia does not advertise same-port mixed TLS in this region"
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionAccepted), metav1.ConditionFalse,
			string(gatewayv1.GatewayReasonListenersNotValid), message, gateway.Generation))
		for i := range gateway.Status.Listeners {
			if !options.rejectsRouteAttachments(&gateway.Spec.Listeners[i]) {
				continue
			}
			conditions := &gateway.Status.Listeners[i].Conditions
			controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonProtocolConflict), message, gateway.Generation))
			controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionConflicted), metav1.ConditionTrue,
				string(gatewayv1.ListenerReasonProtocolConflict), message, gateway.Generation))
		}
	}
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		if observation.AddressAbsent {
			gateway.Status.Addresses = nil
		} else if address := r.observedGatewayAddress(gateway, observation.State); address != "" && len(scopedReadyPorts) != 0 {
			setGatewayAddress(gateway, address)
		}
	}
	if transitioned {
		scope.queueWarning(policy, "EnsureGateway")
	}
	if !policy.ReturnError {
		scope.patchCause = controller.SafeProviderReconcileError(policy, providerErr)
	}
	return controller.ProviderFailureResult(policy, providerErr, string(gateway.UID))
}

func scopedGatewayFailurePorts(config controller.Config, gateway *gatewayv1.Gateway, category cloud.ErrorCategory, observation cloud.GatewayResult) []int {
	if category == cloud.ErrorCategoryResourceFailure && observation.Outcome.State == cloud.OutcomeProgressing &&
		controller.GatewayListenerScope(config, gateway) != "" {
		return observation.ScopedListenerReadyPorts
	}
	return nil
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
	return r.setGatewayListenerStatuses(ctx, gateway, listener, programmed, gatewayReason, listenerReason, message, message, advanceProgrammedGeneration, gatewayListenerStatusOptions{})
}

func (r *Reconciler) setGatewayProgrammed(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, address string) error {
	message := "Octavia load balancer and listener are active"
	if r.requestsListenerScope(gateway) {
		message = "Octavia load balancer and listeners are active"
	}
	if err := r.setGatewayListenerStatuses(ctx, gateway, listener, metav1.ConditionTrue, gatewayv1.GatewayReasonProgrammed,
		gatewayv1.ListenerReasonProgrammed, message, "Octavia listener is active", true, gatewayListenerStatusOptions{}); err != nil {
		return err
	}
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		setGatewayAddress(gateway, address)
	}
	return nil
}

func (r *Reconciler) setGatewayAddressUnusable(
	ctx context.Context,
	gateway *gatewayv1.Gateway,
	listener *gatewayv1.Listener,
	result cloud.GatewayResult,
	progress cloud.Outcome,
	cluster clusterAddressAssignments,
) error {
	readyPorts := result.ScopedListenerReadyPorts
	if controller.GatewayListenerScope(r.Config, gateway) == "" && progress.State == cloud.OutcomeReady {
		readyPorts = []int{int(listener.Port)}
	}
	if r.Config.GatewayAddressRoutabilityEnabled() {
		serving := r.observeAddressAssignment(gateway, result, cluster) || len(cluster.addresses) != 0
		if !r.usesLegacyAddressAdmission(gateway) {
			if serving && progress.State == cloud.OutcomeReady {
				return r.setGatewayProgrammed(ctx, gateway, listener, "")
			}
			reason := gatewayv1.GatewayReasonAddressNotAssigned
			if serving {
				reason = gatewayv1.GatewayReasonNoResources
			}
			return r.setGatewayListenerStatuses(ctx, gateway, listener, metav1.ConditionFalse, reason,
				gatewayv1.ListenerReasonPending, result.AddressIssue, "Waiting for Octavia listener readiness", true, gatewayListenerStatusOptions{readyPorts: readyPorts})
		}
	}
	rejection := validateAddresses(gateway.Spec.Addresses)
	programmedReason := gatewayv1.GatewayReasonAddressNotUsable
	var rejectedCondition *metav1.Condition
	if rejection != nil {
		programmedReason = rejection.programmedReason
		if rejection.gatewayReason != gatewayv1.GatewayReasonAccepted {
			condition := controller.Condition(string(gatewayv1.GatewayConditionAccepted), metav1.ConditionFalse,
				string(rejection.gatewayReason), rejection.message, gateway.Generation)
			previous := meta.FindStatusCondition(gateway.Status.Conditions, condition.Type)
			if previous != nil && previous.Status == condition.Status {
				condition.LastTransitionTime = previous.LastTransitionTime
			}
			rejectedCondition = &condition
		}
	}
	if err := r.setGatewayListenerStatuses(ctx, gateway, listener, metav1.ConditionFalse, programmedReason,
		gatewayv1.ListenerReasonPending, result.AddressIssue, "Waiting for Octavia listener readiness", true, gatewayListenerStatusOptions{readyPorts: readyPorts}); err != nil {
		return err
	}
	if rejectedCondition != nil {
		controller.SetCondition(&gateway.Status.Conditions, *rejectedCondition)
	}
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		if address := r.observedGatewayAddress(gateway, result.State); address != "" {
			setGatewayAddress(gateway, address)
		} else if result.AddressAbsent {
			gateway.Status.Addresses = nil
		}
	}
	return nil
}

func (r *Reconciler) observedGatewayAddress(gateway *gatewayv1.Gateway, state cloud.GatewayState) string {
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return ""
	}
	if topology.Topology.ExternalNetworkID != "" {
		return state.FloatingIPAddress
	}
	return state.VIPAddress
}

func setGatewayAddress(gateway *gatewayv1.Gateway, address string) {
	addressType := gatewayv1.IPAddressType
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Type: &addressType, Value: address}}
}

type gatewayListenerStatusOptions struct {
	readyPorts           []int
	rejectedMixedTLSPort gatewayv1.PortNumber
}

func (options gatewayListenerStatusOptions) rejectsRouteAttachments(listener *gatewayv1.Listener) bool {
	return options.rejectedMixedTLSPort != 0 && listener.Protocol == gatewayv1.TLSProtocolType && listener.Port == options.rejectedMixedTLSPort
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
	options gatewayListenerStatusOptions,
) error {
	listeners, err := SupportedListenersForConfig(r.Config, gateway)
	if err != nil {
		listeners = []*gatewayv1.Listener{primary}
	}
	attachmentListeners := listeners
	if options.rejectedMixedTLSPort != 0 {
		// Route reporting rejects every admitted route on this mixed bind,
		// including parents whose rejection could not fit in route status.
		attachmentListeners = slices.DeleteFunc(slices.Clone(listeners), options.rejectsRouteAttachments)
	}
	var attached attachedGatewayRoutes
	if len(attachmentListeners) != 0 {
		attached, err = r.attachedRoutes(ctx, gateway, attachmentListeners)
		if err != nil {
			return err
		}
	}
	statuses := make([]gatewayv1.ListenerStatus, 0, len(listeners))
	for _, listener := range listeners {
		status := gatewayv1.ListenerStatus{
			Name:           listener.Name,
			SupportedKinds: listenerSupportedRouteKinds(r.Config, *listener),
			AttachedRoutes: attached.counts[listener.Name],
			Conditions:     existingListenerConditions(gateway, listener.Name),
		}
		setAcceptedListenerConditions(&status.Conditions, gateway.Generation)
		generation := controller.ConditionObservedGeneration(status.Conditions, string(gatewayv1.ListenerConditionProgrammed), gateway.Generation, advanceProgrammedGeneration)
		portProgrammed, portReason, portMessage := programmed, listenerReason, listenerMessage
		if slices.Contains(options.readyPorts, int(listener.Port)) {
			portProgrammed, portReason, portMessage = metav1.ConditionTrue, gatewayv1.ListenerReasonProgrammed, "Octavia listener is active"
		}
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), portProgrammed, string(portReason), portMessage, generation))
		statuses = append(statuses, status)
	}
	generation := controller.ConditionObservedGeneration(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed), gateway.Generation, advanceProgrammedGeneration)
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), programmed, string(gatewayReason), gatewayMessage, generation))
	r.setGatewayAcceptedListeners(gateway, statuses...)
	if r.Config.BackendTLSPolicy {
		conditionType := r.Config.Domain() + "/BackendTLSPoliciesReady"
		if attached.backendTLSAncestorFull {
			controller.SetCondition(&gateway.Status.Conditions, controller.Condition(conditionType, metav1.ConditionFalse,
				backendtls.AncestorLimitReason, "A BackendTLSPolicy used by an attached route has no room for this Gateway in its ancestor status", gateway.Generation))
		} else {
			meta.RemoveStatusCondition(&gateway.Status.Conditions, conditionType)
		}
	}
	return nil
}

func setAcceptedListenerConditions(conditions *[]metav1.Condition, generation int64) {
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionTrue, string(gatewayv1.ListenerReasonAccepted), "Listener is accepted", generation))
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), metav1.ConditionTrue, string(gatewayv1.ListenerReasonResolvedRefs), "Listener references are resolved", generation))
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionConflicted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonNoConflicts), "Listener has no conflicts", generation))
}

type attachedGatewayRoutes struct {
	counts                 map[gatewayv1.SectionName]int32
	backendTLSAncestorFull bool
}

// refreshTransportListenerStatuses records attachment through a route programming
// failure and retains existing listener readiness observations by name.
func (r *Reconciler) refreshTransportListenerStatuses(ctx context.Context, gateway *gatewayv1.Gateway) error {
	listeners, err := SupportedListenersForConfig(r.Config, gateway)
	if err != nil {
		return err
	}
	attached, err := r.attachedRoutes(ctx, gateway, listeners)
	if err != nil {
		return err
	}
	statuses := make([]gatewayv1.ListenerStatus, 0, len(listeners))
	for _, listener := range listeners {
		conditions := existingListenerConditions(gateway, listener.Name)
		if len(conditions) == 0 {
			setAcceptedListenerConditions(&conditions, gateway.Generation)
			controller.SetCondition(&conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse, string(gatewayv1.ListenerReasonPending), "Waiting to observe listener readiness", gateway.Generation))
		}
		statuses = append(statuses, gatewayv1.ListenerStatus{
			Name:           listener.Name,
			SupportedKinds: listenerSupportedRouteKinds(r.Config, *listener),
			AttachedRoutes: attached.counts[listener.Name],
			Conditions:     conditions,
		})
	}
	r.replaceGatewayListenerStatuses(gateway, statuses)
	return nil
}

func (r *Reconciler) attachedRoutes(ctx context.Context, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener) (attachedGatewayRoutes, error) {
	gatewayKey := controller.ObjectKeyString(client.ObjectKeyFromObject(gateway))
	attached := attachedGatewayRoutes{counts: make(map[gatewayv1.SectionName]int32, len(listeners))}
	namespaces := make(map[string][]corev1.Namespace)
	unreportedSNI := make(map[unreportedSNIAttachment]struct{})
	countRoute := func(object runtime.Object) error {
		route := object.(client.Object)
		if !route.GetDeletionTimestamp().IsZero() {
			return nil
		}
		spec, parents := controller.RouteSpecAndParentStatuses(route)
		if spec == nil {
			return nil
		}
		routeNamespace := route.GetNamespace()
		refs := model.EffectiveParentRefs(*spec, routeNamespace, gateway, r.Config.DefaultGatewaysEnabled())
		var hostnames []gatewayv1.Hostname
		var httpKind gatewayv1.Kind
		var routeKind gatewayv1.Kind
		switch typed := route.(type) {
		case *gatewayv1.HTTPRoute:
			hostnames, httpKind = typed.Spec.Hostnames, model.HTTPRouteKind
		case *gatewayv1.GRPCRoute:
			hostnames, httpKind = typed.Spec.Hostnames, model.GRPCRouteKind
		case *gatewayv1.TCPRoute:
			routeKind = "TCPRoute"
		case *gatewayv1.UDPRoute:
			routeKind = "UDPRoute"
		case *gatewayv1.TLSRoute:
			routeKind = tlsRouteKind
		}
		if httpKind != "" {
			routeKind = httpKind
		}
		candidates := parents
		var unreportedHTTP *gatewayv1.HTTPRoute
		transportRoute := routeKind == "TCPRoute" || routeKind == "UDPRoute" || routeKind == tlsRouteKind
		if (r.Config.DefaultGatewaysEnabled() || transportRoute) && len(parents) >= controller.RouteParentStatusLimit {
			candidates = slices.Clip(parents)
			for _, ref := range refs {
				if !parentRefTargetsGateway(routeNamespace, ref, gateway) || slices.ContainsFunc(parents, func(parent gatewayv1.RouteParentStatus) bool {
					return parent.ControllerName == r.Config.ControllerName && controller.ParentRefsEqual(parent.ParentRef, routeNamespace, ref, routeNamespace)
				}) {
					continue
				}
				candidates = append(candidates, gatewayv1.RouteParentStatus{ParentRef: ref, ControllerName: r.Config.ControllerName})
			}
			if len(candidates) > len(parents) {
				switch typed := route.(type) {
				case *gatewayv1.HTTPRoute:
					unreportedHTTP = typed
				case *gatewayv1.GRPCRoute:
					unreportedHTTP = model.ProjectGRPCRoute(typed)
				}
			}
		}
		counted := make(map[gatewayv1.SectionName]bool, len(listeners))
		for i, parentStatus := range candidates {
			reported := i < len(parents)
			if reported {
				accepted := meta.FindStatusCondition(parentStatus.Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != route.GetGeneration() ||
					!slices.ContainsFunc(refs, func(ref gatewayv1.ParentReference) bool {
						return controller.ParentRefsEqual(ref, routeNamespace, parentStatus.ParentRef, routeNamespace)
					}) {
					continue
				}
			}
			var matching []*gatewayv1.Listener
			for _, listener := range listeners {
				if !routeParentStatusTargetsListener(parentStatus, routeNamespace, gateway, listener, r.Config.ControllerName) || !listenerAllowsRouteKind(*listener, routeKind) {
					continue
				}
				if !reported && !listenerSupportsRouteKind(r.Config, *listener, gatewayv1.RouteGroupKind{Kind: routeKind}) {
					continue
				}
				if httpKind == model.GRPCRouteKind && (listener.Protocol != gatewayv1.HTTPProtocolType || listener.Filters != nil && len(listener.Filters.Requests) != 0) {
					continue
				}
				if httpKind != "" && len(model.IntersectHostnames(listener.Hostname, hostnames)) == 0 {
					continue
				}
				if tlsRoute, ok := route.(*gatewayv1.TLSRoute); ok {
					if !reported {
						entry := sniRouteStatus{route: tlsRoute, parents: []gatewayv1.ParentReference{parentStatus.ParentRef}}
						if err := r.admitSNIRoute(ctx, &gatewayMutationSnapshot{gateway: gateway, listener: listener}, &entry); err != nil {
							return err
						}
						if entry.acceptedReason != gatewayv1.RouteReasonAccepted {
							continue
						}
						if r.Config.BackendTLSPolicy && terminatesTLS(listener) {
							unreportedSNI[unreportedSNIAttachment{route: client.ObjectKeyFromObject(route), section: listener.Name}] = struct{}{}
						}
						matching = append(matching, listener)
						continue
					}
					names, err := intersectSNIHostnames(listener, tlsRoute, allowsSNISuffix(r.Config, listener))
					if err != nil || len(names) == 0 {
						continue
					}
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
				if !model.AllowsRouteNamespace(listener, gateway.Namespace, routeNamespace, namespaces[routeNamespace]) {
					continue
				}
				if !reported && unreportedHTTP != nil {
					if r.AdmitsHTTPRouteListener == nil {
						return fmt.Errorf("HTTPRoute admission is required to count a parent omitted from route status")
					}
					if !r.AdmitsHTTPRouteListener(unreportedHTTP, gateway, parentStatus.ParentRef, listener, namespaces[routeNamespace]) {
						continue
					}
				}
				matching = append(matching, listener)
			}
			if httpKind == model.GRPCRouteKind && len(matching) > 1 {
				if slices.ContainsFunc(matching[1:], func(listener *gatewayv1.Listener) bool {
					return !HTTPRouteSectionsSharePort(r.Config, gateway, matching[0], listener)
				}) {
					continue
				}
			}
			if r.Config.BackendTLSPolicy && len(matching) != 0 &&
				(httpKind != "" || slices.ContainsFunc(matching, terminatesTLS)) {
				programmedType := r.Config.Domain() + "/Programmed"
				attached.backendTLSAncestorFull = attached.backendTLSAncestorFull || slices.ContainsFunc(parentStatus.Conditions, func(condition metav1.Condition) bool {
					return condition.ObservedGeneration == route.GetGeneration() && condition.Status == metav1.ConditionFalse &&
						condition.Reason == backendtls.AncestorLimitReason &&
						(condition.Type == programmedType || httpKind == "" && condition.Type == string(gatewayv1.RouteConditionResolvedRefs))
				})
			}
			for _, listener := range matching {
				if !counted[listener.Name] {
					counted[listener.Name] = true
					attached.counts[listener.Name]++
				}
			}
		}
		return nil
	}
	var routes client.ObjectList
	index := controller.IndexL4RouteByStatusGateway
	switch listeners[0].Protocol {
	case gatewayv1.TLSProtocolType:
		routes, err := r.readLiveSNIRoutes(ctx)
		if err != nil {
			return attachedGatewayRoutes{}, err
		}
		for i := range routes {
			if err := countRoute(&routes[i]); err != nil {
				return attachedGatewayRoutes{}, err
			}
		}
		if !attached.backendTLSAncestorFull && len(unreportedSNI) != 0 {
			attached.backendTLSAncestorFull, err = r.observeUnreportedSNIAncestorLimit(ctx, gateway, listeners, routes, unreportedSNI)
		}
		return attached, err
	case gatewayv1.TCPProtocolType:
		routes = &gatewayv1.TCPRouteList{}
	case gatewayv1.UDPProtocolType:
		routes = &gatewayv1.UDPRouteList{}
	default:
		routes = &gatewayv1.HTTPRouteList{}
		index = controller.IndexHTTPRouteByStatusGateway
	}
	countIndexed := func(list client.ObjectList) error {
		queries := []client.MatchingFields{{index: gatewayKey}}
		if r.Config.DefaultGatewaysEnabled() {
			queries = append(queries, client.MatchingFields{controller.IndexRouteAtParentStatusLimit: "true"})
		} else if index == controller.IndexL4RouteByStatusGateway {
			queries = append(queries, client.MatchingFields{controller.IndexL4RouteByParentGateway: gatewayKey})
		}
		seen := make(map[client.ObjectKey]bool)
		for _, query := range queries {
			if err := r.List(ctx, list, query); err != nil {
				return err
			}
			if err := meta.EachListItem(list, func(object runtime.Object) error {
				key := client.ObjectKeyFromObject(object.(client.Object))
				if seen[key] {
					return nil
				}
				seen[key] = true
				return countRoute(object)
			}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := countIndexed(routes); err != nil {
		return attachedGatewayRoutes{}, err
	}
	if r.Config.GRPCExactMethodEnabled() && index == controller.IndexHTTPRouteByStatusGateway {
		grpc := &gatewayv1.GRPCRouteList{}
		if err := countIndexed(grpc); err != nil {
			return attachedGatewayRoutes{}, err
		}
	}
	return attached, nil
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

func (r *Reconciler) applyHTTPSListenerStatus(
	gateway *gatewayv1.Gateway,
	previous gatewayv1.GatewayStatus,
	issues map[gatewayv1.SectionName]httpsListenerStatusIssue,
	changed *graph.TLSAuthorizationChanged,
	keepGatewayProgrammed bool,
) {
	if len(issues) == 0 && changed == nil {
		return
	}
	for _, listener := range gateway.Spec.Listeners {
		statusIssue, invalid := issues[listener.Name]
		stale := changed != nil && slices.Contains(changed.Ports, int(listener.Port))
		if !invalid && !stale {
			continue
		}
		index := slices.IndexFunc(gateway.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == listener.Name })
		if index < 0 {
			index = len(gateway.Status.Listeners)
			gateway.Status.Listeners = append(gateway.Status.Listeners, gatewayv1.ListenerStatus{Name: listener.Name})
		}
		status := &gateway.Status.Listeners[index]
		programmedReason := gatewayv1.ListenerReasonPending
		message := "Waiting to verify the HTTPS frontend after its authorization changed"
		if invalid {
			issue := statusIssue.primary
			conditionStatus := metav1.ConditionFalse
			if issue.condition == gatewayv1.ListenerConditionConflicted {
				conditionStatus = metav1.ConditionTrue
				controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted),
					metav1.ConditionFalse, string(gatewayv1.ListenerReasonPortUnavailable), issue.message, gateway.Generation))
			}
			controller.SetCondition(&status.Conditions, controller.Condition(string(issue.condition), conditionStatus, string(issue.reason), issue.message, gateway.Generation))
			if statusIssue.resolvedRefs != nil {
				controller.SetCondition(&status.Conditions, *statusIssue.resolvedRefs)
			}
			programmedReason, message = gatewayv1.ListenerReasonInvalid, issue.message
		}
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed),
			metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
		preserveTransitionTimes(status.Conditions, existingListenerConditionsFrom(previous.Listeners, listener.Name))
	}
	programmedReason := gatewayv1.GatewayReasonPending
	message := "Waiting to verify one or more HTTPS frontends"
	if len(issues) != 0 {
		programmedReason, message = gatewayv1.GatewayReasonListenersNotValid, "One or more HTTP or HTTPS listeners cannot be programmed"
		accepted := metav1.ConditionFalse
		for _, listener := range gateway.Status.Listeners {
			if meta.IsStatusConditionTrue(listener.Conditions, string(gatewayv1.ListenerConditionAccepted)) {
				accepted = metav1.ConditionTrue
				break
			}
		}
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionAccepted),
			accepted, string(gatewayv1.GatewayReasonListenersNotValid), message, gateway.Generation))
	}
	if !keepGatewayProgrammed && !r.addressRejectionPreventsProgramming(gateway) {
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed),
			metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
	}
	preserveTransitionTimes(gateway.Status.Conditions, previous.Conditions)
}

func setFrontendFallbackStatus(gateway *gatewayv1.Gateway) {
	insecureMode := func(config gatewayv1.TLSConfig) bool {
		return config.Validation != nil && config.Validation.Mode == gatewayv1.AllowInsecureFallback
	}
	conditionType := string(gatewayv1.GatewayConditionInsecureFrontendValidationMode)
	if tls := gateway.Spec.TLS; tls != nil && tls.Frontend != nil &&
		(insecureMode(tls.Frontend.Default) || slices.ContainsFunc(tls.Frontend.PerPort, func(port gatewayv1.TLSPortConfig) bool {
			return insecureMode(port.TLS)
		})) {
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(conditionType, metav1.ConditionTrue,
			string(gatewayv1.GatewayReasonConfigurationChanged), "The Gateway is configured to allow HTTPS connections without a valid client certificate", gateway.Generation))
		return
	}
	meta.RemoveStatusCondition(&gateway.Status.Conditions, conditionType)
}

// A rejected CA or unsupported mode is a configuration result. Separately report
// whether Octavia has confirmed the affected listener is closed.
func (r *Reconciler) reportFrontendCAWithdrawal(gateway *gatewayv1.Gateway, issues map[gatewayv1.SectionName]httpsListenerStatusIssue, closedPorts []int) {
	for i := range gateway.Status.Listeners {
		status := &gateway.Status.Listeners[i]
		issue, invalid := issues[status.Name]
		if !invalid || issue.primary.reason != gatewayv1.ListenerReasonNoValidCACertificate && issue.primary.reason != gatewayv1.ListenerReasonUnsupportedValue {
			continue
		}
		index := slices.IndexFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool { return listener.Name == status.Name })
		if index < 0 {
			continue
		}
		listener := gateway.Spec.Listeners[index]
		if listener.Protocol != gatewayv1.HTTPSProtocolType || model.FrontendClientValidation(gateway.Spec.TLS, listener.Port) == nil {
			continue
		}
		programmed := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerConditionProgrammed))
		if programmed != nil && !slices.Contains(closedPorts, int(listener.Port)) {
			programmed.Reason = string(gatewayv1.ListenerReasonPending)
			programmed.Message = "Waiting for Octavia to confirm the HTTPS listener is closed"
		}
	}
}

// The HTTPS overlay refines provider status. Keep transition times from the
// persisted state when an intermediate condition was replaced in this pass.
func preserveTransitionTimes(current, previous []metav1.Condition) {
	for i := range current {
		existing := meta.FindStatusCondition(previous, current[i].Type)
		if existing != nil && existing.Status == current[i].Status {
			current[i].LastTransitionTime = existing.LastTransitionTime
		}
	}
}

// A TLS withdrawal observes the unaffected frontends before reading their
// routes. Retain a section's own prior proof only at this Gateway generation.
func preserveProgrammedListeners(gateway *gatewayv1.Gateway, previous gatewayv1.GatewayStatus, ports []int) {
	for _, listener := range gateway.Spec.Listeners {
		if !slices.Contains(ports, int(listener.Port)) {
			continue
		}
		old := programmedListenerAtGeneration(previous.Listeners, listener.Name, gateway.Generation)
		if old == nil {
			continue
		}
		for i := range gateway.Status.Listeners {
			status := &gateway.Status.Listeners[i]
			if status.Name == listener.Name {
				controller.SetCondition(&status.Conditions, *old)
				break
			}
		}
	}
}

func programmedListenerAtGeneration(statuses []gatewayv1.ListenerStatus, name gatewayv1.SectionName, generation int64) *metav1.Condition {
	programmed := meta.FindStatusCondition(existingListenerConditionsFrom(statuses, name), string(gatewayv1.ListenerConditionProgrammed))
	if programmed == nil || programmed.ObservedGeneration != generation || programmed.Status != metav1.ConditionTrue {
		return nil
	}
	return programmed
}
