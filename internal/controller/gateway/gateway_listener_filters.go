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
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

var listenerAuthPath = regexp.MustCompile("^(?:[-A-Za-z0-9/._~!$&'()*+,;=:@]|%[0-9a-fA-F]{2})+$")

func listenerHasRequestFilters(listener *gatewayv1.Listener) bool {
	return listener.Filters != nil && len(listener.Filters.Requests) != 0
}

func hasHTTPFamilyListenerFilters(gateway *gatewayv1.Gateway) bool {
	return slices.ContainsFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
		return listenerFamily(&listener) == gatewayv1.HTTPProtocolType && listenerHasRequestFilters(&listener)
	})
}

type listenerFilterObservation struct {
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
	for _, listener := range listeners {
		port := int(listener.Port)
		if _, filtered := result.ports[port]; !filtered || usesHTTPSProfile(r.Config, gateway) && httpsProfileIssue(r.Config, gateway, listener) != nil {
			continue
		}
		section := cloud.ListenerRequestFilterSection{Hostname: HTTPVirtualHostname(listener)}
		if listenerHasRequestFilters(listener) {
			requests, issue, err := r.resolveListenerFilterRequests(ctx, gateway, listener, topology)
			if err != nil {
				return listenerFilterObservation{}, err
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

func (r *Reconciler) resolveListenerFilterRequests(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, topology classconfig.Resolved) ([]cloud.ListenerExternalAuth, *httpsListenerIssue, error) {
	if len(listener.Filters.Requests) > maxListenerRequestFilters {
		return nil, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, invalidListenerFilter, "filters.requests must contain at most 16 entries"}, nil
	}
	requests := make([]cloud.ListenerExternalAuth, 0, len(listener.Filters.Requests))
	for index, filter := range listener.Filters.Requests {
		auth, issue, err := r.resolveListenerFilter(ctx, gateway, filter, topology)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			// A partial authorization inventory cannot keep a stale backend in use.
			issue = &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonPending, "Waiting to read the ExternalAuth backend configuration"}
		}
		if issue != nil {
			issue.message = fmt.Sprintf("filters.requests[%d]: %s", index, issue.message)
			return nil, issue, nil
		}
		requests = append(requests, auth)
	}
	return requests, nil, nil
}

// GatewayOwnsListenerRequestFilters identifies filters handled by the opted-in
// HTTP listener profile. Gateway reconciliation checks grants and backend readiness.
func GatewayOwnsListenerRequestFilters(config controller.Config, namespace string, listener *gatewayv1.Listener) bool {
	if !config.GatewayListenerFiltersEnabled() || listenerFamily(listener) != gatewayv1.HTTPProtocolType ||
		listener.Protocol == gatewayv1.HTTPSProtocolType && !config.OperatorHTTPS || !listenerHasRequestFilters(listener) ||
		len(listener.Filters.Requests) > maxListenerRequestFilters {
		return false
	}
	return !slices.ContainsFunc(listener.Filters.Requests, func(filter gatewayv1.ListenerFilter) bool {
		_, _, issue := normalizeListenerFilter(namespace, filter)
		return issue != nil
	})
}

func normalizeListenerFilter(namespace string, filter gatewayv1.ListenerFilter) (cloud.ListenerExternalAuth, types.NamespacedName, *httpsListenerIssue) {
	var auth cloud.ListenerExternalAuth
	invalid := func(reason gatewayv1.ListenerConditionReason, message string) (cloud.ListenerExternalAuth, types.NamespacedName, *httpsListenerIssue) {
		return auth, types.NamespacedName{}, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, reason, message}
	}
	unresolved := func(message string) (cloud.ListenerExternalAuth, types.NamespacedName, *httpsListenerIssue) {
		return auth, types.NamespacedName{}, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, invalidListenerFilterRef, message}
	}
	switch filter.Type {
	case gatewayv1.ListenerFilterExtensionRef:
		if filter.ExtensionRef == nil || filter.ExternalAuth != nil {
			return invalid(invalidListenerFilter, "ExtensionRef requires an extensionRef")
		}
		return unresolved(fmt.Sprintf("ExtensionRef %s %s/%s is unsupported", filter.ExtensionRef.Group, filter.ExtensionRef.Kind, filter.ExtensionRef.Name))
	case gatewayv1.ListenerFilterExternalAuth:
		if filter.ExternalAuth == nil || filter.ExtensionRef != nil {
			return invalid(invalidListenerFilter, "ExternalAuth requires an externalAuth configuration")
		}
	default:
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, fmt.Sprintf("Listener filter type %q is unsupported", filter.Type))
	}
	filterAuth := filter.ExternalAuth
	if filterAuth.ExternalAuthProtocol != gatewayv1.HTTPRouteExternalAuthHTTPProtocol {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, "ExternalAuth supports HTTP authorization")
	}
	if filterAuth.GRPCAuthConfig != nil || filterAuth.ForwardBody != nil && filterAuth.ForwardBody.MaxSize < 0 {
		return invalid(invalidListenerFilter, "Invalid HTTP ExternalAuth options")
	}
	if filterAuth.ForwardBody != nil && filterAuth.ForwardBody.MaxSize > 0 {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, "ExternalAuth request body forwarding is unsupported")
	}
	if options := filterAuth.HTTPAuthConfig; options != nil {
		if len(options.Path) > 1024 || options.Path != "" && (!strings.HasPrefix(options.Path, "/") || !listenerAuthPath.MatchString(options.Path)) {
			return invalid(invalidListenerFilter, "ExternalAuth HTTP path prefix is invalid")
		}
		auth.PathPrefix = options.Path
		if !validAuthHeaders(options.AllowedRequestHeaders) || !validAuthHeaders(options.AllowedResponseHeaders) {
			return invalid(invalidListenerFilter, "ExternalAuth header list contains an invalid name")
		}
		auth.AllowedRequestHeaders = normalizeAuthHeaders(options.AllowedRequestHeaders)
		auth.AllowedResponseHeaders = normalizeAuthHeaders(options.AllowedResponseHeaders)
	}
	key, supported := model.BackendServiceReference(namespace, filterAuth.BackendRef)
	if !supported || key.Name == "" {
		return unresolved("ExternalAuth backendRef must refer to a core Service")
	}
	return auth, key, nil
}

func (r *Reconciler) resolveListenerFilter(ctx context.Context, gateway *gatewayv1.Gateway, filter gatewayv1.ListenerFilter, topology classconfig.Resolved) (cloud.ListenerExternalAuth, *httpsListenerIssue, error) {
	auth, key, issue := normalizeListenerFilter(gateway.Namespace, filter)
	if issue != nil {
		return auth, issue, nil
	}
	unresolved := func(message string) (cloud.ListenerExternalAuth, *httpsListenerIssue, error) {
		return auth, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, invalidListenerFilterRef, message}, nil
	}
	filterAuth := filter.ExternalAuth
	if key.Namespace != gateway.Namespace {
		grants, err := graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
		if err != nil {
			return auth, nil, err
		}
		grants = slices.DeleteFunc(grants, func(grant gatewayv1.ReferenceGrant) bool { return !grant.DeletionTimestamp.IsZero() })
		if !model.BackendReferenceAllowedFor("Gateway", gateway.Namespace, key, grants) {
			return auth, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonRefNotPermitted,
				fmt.Sprintf("No ReferenceGrant authorizes this Gateway to use Service %s", key)}, nil
		}
	}
	var service corev1.Service
	if err := r.APIReader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			return auth, &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerConditionReason(gatewayv1.RouteReasonBackendNotFound),
				fmt.Sprintf("ExternalAuth backend Service %s was not found", key)}, nil
		}
		return auth, nil, err
	}
	if !service.DeletionTimestamp.IsZero() || service.UID == "" {
		return unresolved(fmt.Sprintf("ExternalAuth backend Service %s has no live identity", key))
	}
	port, diagnostic := model.ResolveServicePort(&service, filterAuth.BackendRef.Port, false)
	if diagnostic != nil {
		return unresolved(diagnostic.Message)
	}
	var policies gatewayv1.BackendTLSPolicyList
	if err := r.APIReader.List(ctx, &policies, client.InNamespace(service.Namespace)); err != nil {
		return auth, nil, err
	}
	if policies.Continue != "" {
		return auth, nil, fmt.Errorf("incomplete BackendTLSPolicy inventory for ExternalAuth backend Service %s", key)
	}
	if slices.ContainsFunc(policies.Items, func(policy gatewayv1.BackendTLSPolicy) bool {
		return policy.DeletionTimestamp.IsZero() && backendtls.AppliesToServicePort(&policy, &service, port)
	}) {
		return auth, &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue,
			"ExternalAuth backend TLS is unsupported"}, nil
	}
	members, diagnostic, err := routebackend.TransportMembers(ctx, r.APIReader, &service, filterAuth.BackendRef.Port, corev1.ProtocolTCP, topology, nil, false)
	if err != nil {
		return auth, nil, err
	}
	if diagnostic != nil && diagnostic.Reason != string(gatewayv1.RouteReasonPending) {
		return unresolved(diagnostic.Message)
	}
	auth.ServiceUID, auth.ServicePort = string(service.UID), int(port.Port)
	auth.MemberSubnetID = topology.Topology.MemberSubnetID
	auth.BackendSecurityGroups = topology.Topology.BackendSecurityGroupIDs
	auth.Members = slices.DeleteFunc(members, func(member cloud.Member) bool { return member.Draining })
	return auth, nil, nil
}

func normalizeAuthHeaders(headers []string) []string {
	if len(headers) == 0 {
		return nil
	}
	names := make([]string, len(headers))
	for i, name := range headers {
		names[i] = strings.ToLower(name)
		if names[i] == ":authority" {
			// Octavia accepts HTTP field names. Retain a targeted list
			// containing only the protected authority as a targeted list.
			names[i] = "host"
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func validAuthHeaders(headers []string) bool {
	return len(headers) <= 64 && !slices.ContainsFunc(headers, func(name string) bool {
		return name != ":authority" && (len(name) > 256 || !httpguts.ValidHeaderFieldName(name))
	})
}

func (r *Reconciler) applyListenerFilterStatus(gateway *gatewayv1.Gateway, previous gatewayv1.GatewayStatus, filters listenerFilterObservation, result cloud.GatewayResult) {
	invalid, pending := false, false
	for i := range gateway.Status.Listeners {
		status := &gateway.Status.Listeners[i]
		listenerIndex := slices.IndexFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool { return listener.Name == status.Name })
		if listenerIndex < 0 {
			continue
		}
		listener := &gateway.Spec.Listeners[listenerIndex]
		if !listenerHasRequestFilters(listener) {
			continue
		}
		port := int(listener.Port)
		issue, found := filters.issues[status.Name]
		if !found && slices.Contains(result.ListenerFilterUnavailablePorts, port) {
			issue = httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue, "Octavia does not provide listener_request_filters_v1"}
			found = true
		}
		var message string
		programmedReason := gatewayv1.ListenerReasonPending
		switch {
		case found:
			controller.SetCondition(&status.Conditions, controller.Condition(string(issue.condition), metav1.ConditionFalse, string(issue.reason), issue.message, gateway.Generation))
			if issue.reason == invalidListenerFilterRef && strings.Contains(issue.message, "ExtensionRef") {
				controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionAccepted), metav1.ConditionFalse, string(gatewayv1.ListenerReasonUnsupportedValue), issue.message, gateway.Generation))
			}
			message = issue.message
			if issue.reason != gatewayv1.ListenerReasonPending {
				programmedReason = gatewayv1.ListenerReasonInvalid
			}
		case slices.Contains(result.ListenerFilterCapabilityPendingPorts, port):
			message = "Waiting to discover Octavia listener request filter support"
		default:
			if index, unavailable := filters.unavailable[status.Name]; unavailable {
				message = fmt.Sprintf("filters.requests[%d]: ExternalAuth backend has no ready members", index)
			} else if slices.Contains(result.ListenerFilterRecoveringSections[port], HTTPVirtualHostname(listener)) {
				message = "Waiting for the Octavia listener authorization backend"
			}
		}
		if message != "" {
			controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, gateway.Generation))
			invalid = invalid || programmedReason == gatewayv1.ListenerReasonInvalid
			pending = pending || programmedReason == gatewayv1.ListenerReasonPending
		}
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

func (r *Reconciler) gatewayRequestsForListenerFilterDependency(ctx context.Context, dependency client.Object) []reconcile.Request {
	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for listener filter dependency")
		return nil
	}
	requests := make(map[types.NamespacedName]struct{})
	for i := range gateways.Items {
		gateway := &gateways.Items[i]
		for _, listener := range gateway.Spec.Listeners {
			if !listenerHasRequestFilters(&listener) {
				continue
			}
			for _, filter := range listener.Filters.Requests {
				if filter.Type != gatewayv1.ListenerFilterExternalAuth || filter.ExternalAuth == nil {
					continue
				}
				if key, valid := model.BackendServiceReference(gateway.Namespace, filter.ExternalAuth.BackendRef); valid && listenerAuthDependsOn(key, dependency) {
					requests[client.ObjectKeyFromObject(gateway)] = struct{}{}
				}
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
