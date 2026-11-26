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
	"iter"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

const (
	maxListenerRequestFilters = 16
	invalidListenerFilter     = gatewayv1.ListenerConditionReason("InvalidListenerFilter")
	invalidListenerFilterRef  = gatewayv1.ListenerConditionReason("InvalidListenerFilterRef")
)

func listenerHasRequestFilters(listener *gatewayv1.Listener) bool {
	return listener.Filters != nil && len(listener.Filters.Requests) != 0
}

func hasHTTPFamilyListenerFilters(config controller.Config, gateway *gatewayv1.Gateway) bool {
	listeners, _ := listenerSectionsWithoutTCPConflicts(config, gateway)
	return slices.ContainsFunc(listeners, func(listener *gatewayv1.Listener) bool {
		return listenerFamily(listener) == gatewayv1.HTTPProtocolType && listenerHasRequestFilters(listener)
	})
}

type listenerFilterObservation struct {
	unconfirmed map[gatewayv1.SectionName]bool
	unavailable map[gatewayv1.SectionName]int
	ports       map[int][]cloud.ListenerRequestFilterSection
	issues      map[gatewayv1.SectionName]httpsListenerIssue
}

func (r *Reconciler) observeListenerFilters(ctx context.Context, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener, sets ListenerSetProjection, topology classconfig.Resolved) (listenerFilterObservation, error) {
	result := listenerFilterObservation{}
	if !r.Config.GatewayListenerFiltersEnabled() || !slices.ContainsFunc(listeners, listenerHasRequestFilters) {
		return result, nil
	}
	result.ports = make(map[int][]cloud.ListenerRequestFilterSection)
	result.issues = make(map[gatewayv1.SectionName]httpsListenerIssue)
	result.unavailable = make(map[gatewayv1.SectionName]int)
	for _, listener := range listeners {
		if listenerHasRequestFilters(listener) {
			result.ports[int(listener.Port)] = nil
		}
	}
	tlsObserver := graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	var ancestor *backendtls.GatewayAncestor
	if r.Config.BackendTLSPolicy {
		ancestor = &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
	}
	for _, listener := range listeners {
		port := int(listener.Port)
		if _, filtered := result.ports[port]; !filtered || usesHTTPSProfile(r.Config, gateway) && httpsProfileIssue(r.Config, gateway, listener) != nil {
			continue
		}
		section := cloud.ListenerRequestFilterSection{Hostname: HTTPVirtualHostname(listener)}
		if listenerHasRequestFilters(listener) {
			requests, issue, err := r.resolveListenerFilterRequests(ctx, gateway, listener, topology, tlsObserver, ancestor)
			if ctx.Err() != nil {
				return listenerFilterObservation{}, ctx.Err()
			}
			if err != nil {
				if result.unconfirmed == nil {
					result.unconfirmed = make(map[gatewayv1.SectionName]bool)
				}
				result.unconfirmed[listener.Name] = true
			}
			section.Requests = requests
			if missing := slices.IndexFunc(requests, func(auth cloud.ListenerExternalAuth) bool { return len(auth.Members) == 0 }); missing >= 0 {
				result.unavailable[listener.Name] = missing
			}
			if issue != nil {
				result.issues[listener.Name] = *issue
				section.Reject = true
			}
		}
		result.ports[port] = append(result.ports[port], section)
	}
	for _, set := range sets.sets {
		for _, entry := range set.entries {
			port := int(entry.listener.Port)
			if _, filtered := result.ports[port]; filtered && entry.rejection == "" {
				result.ports[port] = append(result.ports[port], cloud.ListenerRequestFilterSection{Hostname: HTTPVirtualHostname(&entry.listener)})
			}
		}
	}
	for port, sections := range result.ports {
		if !slices.ContainsFunc(sections, func(section cloud.ListenerRequestFilterSection) bool {
			return section.Reject || len(section.Requests) != 0
		}) {
			delete(result.ports, port)
			continue
		}
		slices.SortFunc(sections, func(a, b cloud.ListenerRequestFilterSection) int { return strings.Compare(a.Hostname, b.Hostname) })
	}
	return result, nil
}

// Keep denials backed by a successful authorization read. A live TLS policy
// with pending CA validation still requires us to withdraw authorization.
func (observation listenerFilterObservation) confirmedRejections(listeners []*gatewayv1.Listener) listenerFilterObservation {
	rejected := listenerFilterObservation{
		ports:  make(map[int][]cloud.ListenerRequestFilterSection),
		issues: make(map[gatewayv1.SectionName]httpsListenerIssue),
	}
	for _, listener := range listeners {
		issue, found := observation.issues[listener.Name]
		if !found || observation.unconfirmed[listener.Name] {
			continue
		}
		port := int(listener.Port)
		rejected.ports[port] = append(rejected.ports[port], cloud.ListenerRequestFilterSection{
			Hostname: HTTPVirtualHostname(listener), Reject: true,
		})
		rejected.issues[listener.Name] = issue
	}
	return rejected
}

func (r *Reconciler) listenerFilterWithdrawalInput(ctx context.Context, snapshot *gatewayMutationSnapshot, spec cloud.GatewaySpec) (graph.GatewayInput, error) {
	observe := func(ctx context.Context) (listenerFilterObservation, error) {
		// ListenerSets do not supply authorized filter sections. Read only the
		// Gateway's own filter denials, independently of TLS and route assembly.
		filters, err := r.observeListenerFilters(ctx, snapshot.gateway, snapshot.listeners, ListenerSetProjection{}, snapshot.topology)
		return filters.confirmedRejections(snapshot.listeners), err
	}
	rejected, err := observe(ctx)
	if err != nil || len(rejected.ports) == 0 {
		return graph.GatewayInput{}, err
	}
	spec.RequestFilterSections = rejected.ports
	return graph.GatewayInput{
		Object: snapshot.gateway,
		Node:   cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawListenerFilters, Spec: spec},
		Validate: func(ctx context.Context) error {
			current, err := observe(ctx)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(current.ports, rejected.ports) {
				return errGatewayChanged
			}
			return r.sealGatewayMutationSnapshot(ctx, snapshot)
		},
	}, nil
}

func (r *Reconciler) resolveListenerFilterRequests(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, topology classconfig.Resolved, tlsObserver *graph.BackendTLSObserver, ancestor *backendtls.GatewayAncestor) ([]cloud.ListenerExternalAuth, *httpsListenerIssue, error) {
	if len(listener.Filters.Requests) > maxListenerRequestFilters {
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, invalidListenerFilter, "filters.requests must contain at most 16 entries"}, nil
	}
	requests := make([]cloud.ListenerExternalAuth, 0, len(listener.Filters.Requests))
	for index, filter := range listener.Filters.Requests {
		auth, issue, err := r.resolveListenerFilter(ctx, gateway, filter, topology, tlsObserver, ancestor)
		if err != nil {
			// A partial authorization inventory cannot keep a stale backend in use.
			issue = &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonPending, "Waiting to read the ExternalAuth backend configuration"}
		}
		if issue != nil {
			issue.message = fmt.Sprintf("filters.requests[%d]: %s", index, issue.message)
			return nil, issue, err
		}
		requests = append(requests, auth)
	}
	return requests, nil, nil
}

// GatewayOwnsListenerRequestFilters identifies filters handled by the opted-in
// HTTP listener profile. Gateway reconciliation checks grants and backend readiness.
func GatewayOwnsListenerRequestFilters(config controller.Config, listener *gatewayv1.Listener) bool {
	if !config.GatewayListenerFiltersEnabled() || listenerFamily(listener) != gatewayv1.HTTPProtocolType ||
		listener.Protocol == gatewayv1.HTTPSProtocolType && !config.OperatorHTTPS || !listenerHasRequestFilters(listener) ||
		len(listener.Filters.Requests) > maxListenerRequestFilters {
		return false
	}
	return !slices.ContainsFunc(listener.Filters.Requests, func(filter gatewayv1.ListenerFilter) bool {
		_, issue := normalizeListenerFilter(filter)
		return issue != nil
	})
}

func normalizeListenerFilter(filter gatewayv1.ListenerFilter) (*model.HTTPExternalAuth, *httpsListenerIssue) {
	invalid := func(message string) (*model.HTTPExternalAuth, *httpsListenerIssue) {
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, invalidListenerFilter, message}
	}
	switch filter.Type {
	case gatewayv1.ListenerFilterExtensionRef:
		if filter.ExtensionRef == nil || filter.ExternalAuth != nil {
			return invalid("ExtensionRef requires an extensionRef")
		}
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, invalidListenerFilterRef,
			fmt.Sprintf("ExtensionRef %s %s/%s is unsupported", filter.ExtensionRef.Group, filter.ExtensionRef.Kind, filter.ExtensionRef.Name)}
	case gatewayv1.ListenerFilterExternalAuth:
		if filter.ExternalAuth == nil || filter.ExtensionRef != nil {
			return invalid("ExternalAuth requires an externalAuth configuration")
		}
	default:
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue,
			fmt.Sprintf("Listener filter type %q is unsupported", filter.Type)}
	}
	auth, diagnostic := model.NormalizeListenerExternalAuth(filter.ExternalAuth)
	if diagnostic == nil {
		return auth, nil
	}
	switch diagnostic.Condition {
	case string(gatewayv1.RouteConditionResolvedRefs):
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, invalidListenerFilterRef, diagnostic.Message}
	case string(gatewayv1.RouteConditionAccepted):
		if diagnostic.Reason == "Invalid" {
			return invalid(diagnostic.Message)
		}
	}
	return nil, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue, diagnostic.Message}
}

func (r *Reconciler) resolveListenerFilter(ctx context.Context, gateway *gatewayv1.Gateway, filter gatewayv1.ListenerFilter, topology classconfig.Resolved, tlsObserver *graph.BackendTLSObserver, ancestor *backendtls.GatewayAncestor) (cloud.ListenerExternalAuth, *httpsListenerIssue, error) {
	auth, issue := normalizeListenerFilter(filter)
	if issue != nil {
		return cloud.ListenerExternalAuth{}, issue, nil
	}
	resolved, service, port, diagnostic, err := routebackend.ObserveExternalAuthTransport(ctx, r.APIReader, tlsObserver, ancestor, "Gateway", gateway.Namespace, *auth)
	if diagnostic == nil && err == nil {
		resolved, diagnostic, err = routebackend.ResolveExternalAuthMembers(ctx, r.APIReader, service, port, resolved, topology)
	}
	if diagnostic == nil {
		return resolved, nil, err
	}
	condition, reason := gatewayv1.ListenerConditionResolvedRefs, invalidListenerFilterRef
	switch diagnostic.Reason {
	case string(gatewayv1.RouteReasonBackendNotFound), string(gatewayv1.RouteReasonRefNotPermitted), string(gatewayv1.RouteReasonPending):
		reason = gatewayv1.ListenerConditionReason(diagnostic.Reason)
	}
	if diagnostic.Condition == string(gatewayv1.RouteConditionAccepted) {
		condition, reason = gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue
	}
	return resolved, &httpsListenerIssue{condition, reason, diagnostic.Message}, err
}

func (r *Reconciler) applyListenerFilterStatus(gateway *gatewayv1.Gateway, previous gatewayv1.GatewayStatus, filters listenerFilterObservation, result cloud.GatewayResult, tlsChanged *graph.TLSAuthorizationChanged) {
	invalid, pending := false, false
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if !listenerHasRequestFilters(listener) {
			continue
		}
		port := int(listener.Port)
		issue, found := filters.issues[listener.Name]
		if !found && slices.Contains(result.ListenerFilterUnavailablePorts, port) {
			issue = httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue, "Octavia does not provide listener_request_filters_v1"}
			found = true
		}
		if !found && slices.Contains(result.ListenerFilterTransportUnavailableSections[port], HTTPVirtualHostname(listener)) {
			issue = httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue,
				"Octavia listener ExternalAuth backend transport is unavailable"}
			found = true
		}
		if !found && slices.Contains(result.ListenerFilterBodyUnavailableSections[port], HTTPVirtualHostname(listener)) {
			issue = httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue,
				"Octavia listener ExternalAuth body forwarding is unavailable at the requested size"}
			found = true
		}
		var message string
		programmedReason := gatewayv1.ListenerReasonPending
		switch {
		case found:
			message = issue.message
			if issue.reason != gatewayv1.ListenerReasonPending {
				programmedReason = gatewayv1.ListenerReasonInvalid
			}
		case slices.Contains(result.ListenerFilterCapabilityPendingPorts, port):
			message = "Waiting to discover Octavia listener request filter support"
		default:
			if index, unavailable := filters.unavailable[listener.Name]; unavailable {
				message = fmt.Sprintf("filters.requests[%d]: ExternalAuth backend has no ready members", index)
			} else if slices.Contains(result.ListenerFilterRecoveringSections[port], HTTPVirtualHostname(listener)) {
				message = "Waiting for the Octavia listener authorization backend"
			}
		}
		if message == "" {
			continue
		}
		index := slices.IndexFunc(gateway.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == listener.Name })
		if index < 0 {
			index = len(gateway.Status.Listeners)
			gateway.Status.Listeners = append(gateway.Status.Listeners, gatewayv1.ListenerStatus{Name: listener.Name})
		}
		status := &gateway.Status.Listeners[index]
		if found {
			conditionStatus := metav1.ConditionFalse
			if issue.reason == gatewayv1.ListenerReasonPending {
				conditionStatus = metav1.ConditionUnknown
			}
			controller.SetCondition(&status.Conditions, controller.Condition(string(issue.condition), conditionStatus, string(issue.reason), issue.message, gateway.Generation))
			if issue.reason == invalidListenerFilterRef && strings.Contains(issue.message, "ExtensionRef") {
				controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonUnsupportedValue), issue.message, gateway.Generation))
			}
		}
		programmed := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerConditionProgrammed))
		waitingForTLS := tlsChanged != nil && slices.Contains(tlsChanged.Ports, port) && programmed != nil &&
			programmed.ObservedGeneration == gateway.Generation && programmed.Status == metav1.ConditionFalse &&
			programmed.Reason == string(gatewayv1.ListenerReasonPending)
		if !waitingForTLS {
			controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
		}
		invalid = invalid || programmedReason == gatewayv1.ListenerReasonInvalid
		pending = pending || programmedReason == gatewayv1.ListenerReasonPending
		preserveTransitionTimes(status.Conditions, existingListenerConditionsFrom(previous.Listeners, listener.Name))
	}
	if (invalid || pending) && !r.addressRejectionPreventsProgramming(gateway) {
		reason, message := gatewayv1.GatewayReasonPending, "Waiting for one or more listener authorization filters"
		current := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		if invalid || current != nil && current.Status == metav1.ConditionFalse && current.Reason == string(gatewayv1.GatewayReasonListenersNotValid) {
			reason, message = gatewayv1.GatewayReasonListenersNotValid, "One or more listener sections cannot be programmed"
		}
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, string(reason), message, gateway.Generation))
	}
	preserveTransitionTimes(gateway.Status.Conditions, previous.Conditions)
}

func (r *Reconciler) validateListenerFilterInputs(ctx context.Context, snapshot *gatewayMutationSnapshot, sets ListenerSetProjection, expected listenerFilterObservation) error {
	if len(expected.ports) == 0 {
		return nil
	}
	actual, err := r.observeListenerFilters(ctx, snapshot.gateway, snapshot.listeners, sets, snapshot.topology)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, actual) {
		return errGatewayChanged
	}
	return nil
}

// ListenerExternalAuthReferences enumerates native authorization dependencies.
func ListenerExternalAuthReferences(gateway *gatewayv1.Gateway) iter.Seq[types.NamespacedName] {
	return func(yield func(types.NamespacedName) bool) {
		for _, listener := range gateway.Spec.Listeners {
			if !listenerHasRequestFilters(&listener) {
				continue
			}
			for _, filter := range listener.Filters.Requests {
				if filter.Type != gatewayv1.ListenerFilterExternalAuth || filter.ExternalAuth == nil {
					continue
				}
				key, valid := model.BackendServiceReference(gateway.Namespace, filter.ExternalAuth.BackendRef)
				if valid && !yield(key) {
					return
				}
			}
		}
	}
}

type ListenerBackendTLSPolicyConsumer struct {
	Result     backendtls.PolicyResult
	Protocol   string
	Compatible bool
}

// ObserveListenerBackendTLSPolicy supplies native listener consumers to the
// shared BackendTLSPolicy status reconciler, including before a pool is ready.
func (r *Reconciler) ObserveListenerBackendTLSPolicy(ctx context.Context, policy *gatewayv1.BackendTLSPolicy) (map[types.NamespacedName][]ListenerBackendTLSPolicyConsumer, error) {
	targets := backendtls.ServiceKeys(policy)
	if !r.Config.GatewayListenerFiltersEnabled() || !r.Config.BackendTLSPolicy || len(targets) == 0 {
		return nil, nil
	}
	var gateways gatewayv1.GatewayList
	if err := r.APIReader.List(ctx, &gateways); err != nil {
		return nil, err
	}
	if gateways.Continue != "" {
		return nil, errors.New("incomplete Gateway list for listener ExternalAuth policy")
	}
	observer := graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	consumers := make(map[types.NamespacedName][]ListenerBackendTLSPolicyConsumer)
	for i := range gateways.Items {
		gateway := &gateways.Items[i]
		candidate := false
		for key := range ListenerExternalAuthReferences(gateway) {
			if slices.Contains(targets, key) {
				candidate = true
				break
			}
		}
		if !candidate || !gateway.DeletionTimestamp.IsZero() {
			continue
		}
		listeners, err := SupportedListenersForConfig(r.Config, gateway)
		if err != nil {
			continue
		}
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() ||
			!controller.GatewayClassSupportsInstalledVersion(&class) {
			continue
		}
		for _, listener := range listeners {
			if !GatewayOwnsListenerRequestFilters(r.Config, listener) ||
				usesHTTPSProfile(r.Config, gateway) && httpsProfileIssue(r.Config, gateway, listener) != nil {
				continue
			}
			for _, filter := range listener.Filters.Requests {
				auth, _ := normalizeListenerFilter(filter)
				key, _ := model.BackendServiceReference(gateway.Namespace, auth.BackendRef)
				if !slices.Contains(targets, key) {
					continue
				}
				service, port, issue, err := routebackend.ObserveExternalAuthService(ctx, r.APIReader, "Gateway", gateway.Namespace, *auth)
				if err != nil {
					return nil, err
				}
				if issue != nil || !backendtls.AppliesToServicePort(policy, service, port) {
					continue
				}
				selection, err := observer.ObserveWithPendingCA(ctx, service, port, nil)
				if err != nil {
					return nil, err
				}
				for _, result := range selection.Policies {
					if result.UID == policy.UID && result.Generation == policy.Generation {
						compatible := routebackend.ExternalAuthTransportDiagnostic(service, port, *auth, true) == nil
						key := client.ObjectKeyFromObject(gateway)
						consumers[key] = append(consumers[key], ListenerBackendTLSPolicyConsumer{result, auth.Config.Protocol, compatible})
					}
				}
			}
		}
	}
	return consumers, nil
}

func (r *Reconciler) gatewayRequestsForListenerAuthCA(ctx context.Context, object client.Object) []reconcile.Request {
	key := client.ObjectKeyFromObject(object)
	var policies gatewayv1.BackendTLSPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(key.Namespace), client.MatchingFields{
		controller.IndexBackendTLSPolicyByCA: controller.ObjectKeyString(key),
	}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list BackendTLSPolicies for listener authorizer CA", "ca", key)
		return nil
	}
	targets := make(map[types.NamespacedName]bool)
	for i := range policies.Items {
		for _, target := range backendtls.ServiceKeys(&policies.Items[i]) {
			targets[target] = true
		}
	}
	if len(targets) == 0 {
		return nil
	}
	return r.gatewaysWithListenerAuthDependency(ctx, func(key types.NamespacedName) bool { return targets[key] })
}

func (r *Reconciler) gatewayRequestsForListenerFilterDependency(ctx context.Context, dependency client.Object) []reconcile.Request {
	return r.gatewaysWithListenerAuthDependency(ctx, func(key types.NamespacedName) bool { return listenerAuthDependsOn(key, dependency) })
}

func (r *Reconciler) gatewaysWithListenerAuthDependency(ctx context.Context, matches func(types.NamespacedName) bool) []reconcile.Request {
	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for listener filter dependency")
		return nil
	}
	requests := make(map[types.NamespacedName]struct{})
	for i := range gateways.Items {
		gateway := &gateways.Items[i]
		for service := range ListenerExternalAuthReferences(gateway) {
			if matches(service) {
				requests[client.ObjectKeyFromObject(gateway)] = struct{}{}
				break
			}
		}
	}
	return controller.SortedRequests(requests)
}

func listenerAuthDependsOn(service types.NamespacedName, dependency client.Object) bool {
	switch object := dependency.(type) {
	case *corev1.Service:
		return service == client.ObjectKeyFromObject(object)
	case *discoveryv1.EndpointSlice:
		return service.Namespace == object.Namespace && service.Name == object.Labels[discoveryv1.LabelServiceName]
	case *gatewayv1.ReferenceGrant, *gatewayv1.BackendTLSPolicy:
		return service.Namespace == dependency.GetNamespace()
	case *corev1.Node:
		return true
	default:
		return false
	}
}
