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

package graph

import (
	"context"
	"errors"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

type protectedHTTPPredicate struct {
	capability string
	role       string
	label      string
	required   func(desiredPolicy) bool
	observed   func(observedRoutePolicy) bool
	settled    func(desiredPolicy, observedRoutePolicy) bool
}

var (
	exactHTTPHeaderPredicate = protectedHTTPPredicate{
		capability: octavia.GatewayCapabilityHTTPHeaderExact, role: openstackidentity.RoleRuleHTTPHeaderExact,
		label: "exact HTTP header", required: func(policy desiredPolicy) bool { return len(policy.headers) != 0 },
	}
	httpMethodPredicate = protectedHTTPPredicate{
		capability: octavia.GatewayCapabilityHTTPMethodMatch, role: openstackidentity.RoleRuleHTTPMethod,
		label: "HTTP method", required: func(policy desiredPolicy) bool { return policy.method != "" },
	}
	grpcContentTypePredicate = protectedHTTPPredicate{
		capability: octavia.GatewayCapabilityGRPCContentType,
		label:      "native gRPC Content-Type",
		required: func(policy desiredPolicy) bool {
			return policy.grpcContentTypeRule == grpcContentTypeRuleType
		},
		observed: func(policy observedRoutePolicy) bool {
			return !observedPolicyDisabled(policy) && slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
				return rule.base == openstackidentity.RoleRuleGRPCContentType && rule.rule.RuleType == string(grpcContentTypeRuleType)
			})
		},
	}
	responseHeaderModifierPolicy = protectedHTTPPredicate{
		capability: octavia.GatewayCapabilityResponseHeaderOperations,
		label:      "response header modification",
		required:   func(policy desiredPolicy) bool { return len(policy.responseHeaderOperations) != 0 },
		observed: func(policy observedRoutePolicy) bool {
			return len(policy.responseHeaderOperations) != 0 && !observedPolicyDisabled(policy)
		},
		settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
			return slices.Equal(wanted.responseHeaderOperations, observed.responseHeaderOperations)
		},
	}
)

type guardedHTTPListener struct {
	desired, unsettled, installed bool
	policyRoutes, frontendRoutes  map[int]bool
	policyIDs                     map[string]bool
}

// protectHTTPPredicateTraffic uses the same verified listener and policy inventory
// as the graph writer. A confirmed revocation closes the port before changing
// its predicates. A discovery error alone leaves an existing port serving. The
// second result reports whether a revoked listener was observed closed.
func (p *Provider) protectHTTPPredicateTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult, predicate protectedHTTPPredicate) ([]graphMutation, bool) {
	if desired.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return plan, false
	}
	observedListeners := actual.scopedListeners
	if !actual.scoped && actual.listener != nil {
		observedListeners = []listeners.Listener{*actual.listener}
	}
	portByID := make(map[string]int, len(observedListeners))
	for _, listener := range observedListeners {
		portByID[listener.ID] = listener.ProtocolPort
	}
	// A scalar listener carries the effective Gateway port even if an older
	// Octavia inventory omitted protocol_port.
	if !actual.scoped && actual.listener != nil && actual.listener.ProtocolPort == 0 {
		portByID[actual.listener.ID] = desired.spec.Gateway.Spec.ListenerPort
		observedListeners[0].ProtocolPort = desired.spec.Gateway.Spec.ListenerPort
	}
	states := guardedHTTPListeners(desired, actual, portByID, predicate)
	if len(states) == 0 {
		return plan, false
	}
	if predicate.observed != nil {
		// Creation and activation have their own capability checks. Reprobe
		// here only when readback shows previously installed operations.
		installed := false
		for _, state := range states {
			installed = installed || state.installed
		}
		if !installed {
			return plan, false
		}
	}
	policyListeners := make(map[string]string)
	for _, listener := range observedListeners {
		for policyID := range states[listener.ProtocolPort].policyIDs {
			policyListeners[policyID] = listener.ID
		}
	}
	mutationListener := func(step graphMutation) string {
		if guarded := policyListeners[step.route.id]; guarded != "" {
			return guarded
		}
		if guarded := policyListeners[step.route.parentID]; guarded != "" {
			return guarded
		}
		if step.listenerID == "" && !actual.scoped && actual.listener != nil {
			return actual.listener.ID
		}
		return step.listenerID
	}
	var discoveryErr error
	if actual.loadBalancer != nil {
		discoveryErr = p.verifyGatewayHTTPCapabilities(ctx, desired, actual, []string{predicate.capability})
	}
	var missing *octavia.MissingGatewayCapability
	confirmedMissing := errors.As(discoveryErr, &missing)
	frontendBlocked := make(map[string]bool)
	closedByID := make(map[string]bool, len(observedListeners))
	waiting := "Waiting for Octavia " + predicate.label + " policies to be verified"
	unavailable := "Octavia withdrew the " + predicate.label + " capability on this listener"
	// An explicit route withdrawal must remain possible during a discovery
	// outage. Close before tearing down its last known guarded policy.
	var withdrawalListener string
	if !confirmedMissing && discoveryErr != nil && len(plan) != 0 {
		first := plan[0]
		if first.kind == graphRouteMutation && first.withdrawHTTPPolicy && isHTTPPolicyMutation(first.route.kind) {
			listenerID := mutationListener(first)
			state := states[portByID[listenerID]]
			if state.policyIDs[first.route.id] || state.policyIDs[first.route.parentID] {
				if !state.desired || desired.spec.Routes[first.routeIndex].Disposition == cloud.GraphDelete {
					withdrawalListener = listenerID
				}
			}
		}
	}
	confirmedClosed := false
	for _, listener := range observedListeners {
		state, relevant := states[listener.ProtocolPort]
		if !relevant {
			continue
		}
		closed := actual.adminObserved[listener.ID] && !listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE"
		closedByID[listener.ID] = closed
		// New policies are gated separately. Close a listener only when
		// ownership inventory proves an installed dependency was withdrawn.
		if confirmedMissing && !state.installed {
			continue
		}
		frontendBlocked[listener.ID] = discoveryErr != nil || state.unsettled
		reason := unavailable
		if !confirmedMissing {
			if predicate.observed != nil && discoveryErr != nil && state.installed && state.desired {
				p.markGuardedHTTPListener(result, listener.ProtocolPort, state.policyRoutes, "Waiting for Octavia to advertise "+predicate.capability)
			}
			if listener.ID != withdrawalListener || closed {
				continue
			}
			reason = "Closing the affected listener before withdrawing an Octavia " + predicate.label + " policy"
		}
		p.markGuardedHTTPListener(result, listener.ProtocolPort, state.frontendRoutes, reason)
		if closed {
			confirmedClosed = confirmedClosed || confirmedMissing
		} else if listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR" {
			return []graphMutation{closeGuardedHTTPListener(listener, reason)}, confirmedClosed
		}
	}
	return slices.DeleteFunc(plan, func(step graphMutation) bool {
		listenerID := mutationListener(step)
		state := states[portByID[listenerID]]
		switch step.kind {
		case graphEnableListener:
			if frontendBlocked[listenerID] {
				p.markGuardedHTTPListener(result, portByID[listenerID], state.frontendRoutes, waiting)
				return true
			}
		case graphEnableLoadBalancer:
			for _, listener := range observedListeners {
				if frontendBlocked[listener.ID] && !closedByID[listener.ID] {
					p.markGuardedHTTPListener(result, listener.ProtocolPort, states[listener.ProtocolPort].frontendRoutes, waiting)
					return true
				}
			}
		case graphRouteMutation:
			// Restrictive target updates leave the guarded predicate in place.
			if step.withdrawHTTPPolicy && step.route.kind == routeMutationSetPolicyTarget {
				return false
			}
			if predicate.observed != nil && step.route.kind == routeMutationSetPolicyHeaders &&
				step.route.responseHeaderOperations != nil && len(step.route.responseHeaderOperations) == 0 {
				// A clear is accepted even when the regional capability is unavailable.
				return false
			}
			if discoveryErr == nil || closedByID[listenerID] || !state.installed || !isHTTPPolicyMutation(step.route.kind) {
				return false
			}
			p.markGuardedHTTPListener(result, portByID[listenerID], state.policyRoutes, waiting)
			return true
		}
		return false
	}), confirmedClosed
}

func closeGuardedHTTPListener(listener listeners.Listener, reason string) graphMutation {
	return graphMutation{kind: graphDisableListener, listenerID: listener.ID,
		scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort}, redactTLSError: listener.Protocol == terminatedHTTPS,
		withdrawHTTPPolicy: true, message: reason}
}

func (p *Provider) markGuardedHTTPListener(result *cloud.GatewayGraphResult, port int, routes map[int]bool, reason string) {
	result.Outcome = p.progressingOutcome(reason)
	for index := range routes {
		if index < len(result.Routes) {
			result.Routes[index].Result.Outcome = result.Outcome
			result.Routes[index].Result.ObservationDeferred = false
		}
	}
	result.Gateway.Outcome = result.Outcome
	result.Gateway.ScopedListenerReadyPorts = slices.DeleteFunc(result.Gateway.ScopedListenerReadyPorts, func(ready int) bool { return ready == port })
}

func guardedHTTPListeners(desired desiredGatewayGraph, actual observedGatewayGraph, portByID map[string]int, predicate protectedHTTPPredicate) map[int]guardedHTTPListener {
	states := make(map[int]guardedHTTPListener)
	if hasHTTPRouteParts(desired, actual) {
		parts, observedParts, owners := httpPlanningParts(desired, actual)
		for index, part := range parts.routes {
			var observed observedRouteGraph
			if index < len(observedParts.routes) {
				observed = observedParts.routes[index]
			}
			collectGuardedHTTPListener(states, desired, portByID, part, observed, parts.spec.Routes[index].Disposition, owners[index], predicate)
		}
	}
	if len(states) == 0 {
		return states
	}
	// Closing a physical listener affects ordinary HTTP policies on that
	// port too. Keep their progress independent from policies on other ports.
	addFrontendRoute := func(port, index int) {
		state, guarded := states[port]
		if !guarded {
			return
		}
		if state.frontendRoutes == nil {
			state.frontendRoutes = make(map[int]bool)
		}
		state.frontendRoutes[index] = true
		states[port] = state
	}
	for index, route := range desired.routes {
		if desired.spec.Routes[index].Disposition == cloud.GraphEnsure {
			port := scopedHTTPRoutePort(route)
			if port == 0 {
				port = desired.spec.Gateway.Spec.ListenerPort
			}
			addFrontendRoute(port, index)
		}
		if index < len(actual.routes) {
			for _, part := range actual.routes[index].parts() {
				for _, policy := range part.policies {
					if port, present := portByID[policy.policy.ListenerID]; present {
						addFrontendRoute(port, index)
					}
				}
			}
		}
	}
	return states
}

func collectGuardedHTTPListener(states map[int]guardedHTTPListener, gateway desiredGatewayGraph, portByID map[string]int,
	route desiredRoute, observed observedRouteGraph, disposition cloud.GraphDisposition, owner int, predicate protectedHTTPPredicate) {
	mark := func(port int, change func(*guardedHTTPListener)) {
		state := states[port]
		if state.policyRoutes == nil {
			state.policyRoutes = make(map[int]bool)
		}
		state.policyRoutes[owner] = true
		change(&state)
		states[port] = state
	}
	port := scopedHTTPRoutePort(route)
	if port == 0 {
		port = gateway.spec.Gateway.Spec.ListenerPort
	}
	wantedAtPort := make(map[string]bool)
	if disposition == cloud.GraphEnsure {
		for _, wanted := range route.policies {
			if !predicate.required(wanted) {
				continue
			}
			wantedAtPort[wanted.role] = true
			settled := slices.ContainsFunc(observed.policies, func(policy observedRoutePolicy) bool {
				return policy.role == wanted.role && portByID[policy.policy.ListenerID] == port &&
					policy.policy.Position == wanted.position && policy.policy.AdminStateUp && observedForwardPolicySettled(policy) &&
					(predicate.settled == nil || predicate.settled(wanted, policy)) &&
					observedHTTPPredicateMatches(policy, rulesFor(wanted, route.hostnameRuleValue)) &&
					(route.redirect && desiredPolicyActionMatches(wanted, policy.policy, "") ||
						!route.redirect && observedForwardPolicyMatches(route, observed, policy))
			})
			mark(port, func(state *guardedHTTPListener) {
				state.desired, state.unsettled = true, state.unsettled || !settled
			})
		}
	}
	for _, policy := range observed.policies {
		observedPort, present := portByID[policy.policy.ListenerID]
		if !present || !predicate.policyInstalled(policy) {
			continue
		}
		mark(observedPort, func(state *guardedHTTPListener) {
			state.installed = true
			if state.policyIDs == nil {
				state.policyIDs = make(map[string]bool)
			}
			state.policyIDs[policy.policy.ID] = true
			state.unsettled = state.unsettled || observedPort != port || !wantedAtPort[policy.role]
		})
	}
}

func (predicate protectedHTTPPredicate) policyInstalled(policy observedRoutePolicy) bool {
	if predicate.observed != nil {
		return predicate.observed(policy)
	}
	return slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
		return rule.base == predicate.role
	})
}
