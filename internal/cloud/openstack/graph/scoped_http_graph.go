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
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func (p *Provider) observeScopedHTTPGraph(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, owners []routeGraphOwner) (observedGatewayGraph, error) {
	for _, listener := range actual.scopedListeners {
		if listener.DefaultPoolID != "" {
			return actual, unprovenDefaultPool(&listener)
		}
	}
	var routeRecovery *Identity
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		routeRecovery = &desired.identity
	}
	poolInventory, err := p.listGatewayPoolInventory(ctx, actual.loadBalancer)
	if err != nil {
		return actual, err
	}
	actual.routes, err = p.observeRouteGraphsFromPools(ctx, owners, actual.loadBalancer, nil, actual.protocol, actual.scopedListeners, poolInventory, routeRecovery, actual.httpsIsolationPolicies)
	if err != nil {
		return actual, err
	}
	for _, listener := range actual.scopedListeners {
		if !actual.isHTTPSIsolated(listener) && slices.ContainsFunc(actual.routes, func(route observedRouteGraph) bool {
			return slices.ContainsFunc(route.parts(), func(part observedRouteGraph) bool {
				return slices.ContainsFunc(part.policies, func(policy observedRoutePolicy) bool {
					return policy.policy.ListenerID == listener.ID && policy.httpsListenerHostname != ""
				})
			})
		}) {
			if actual.httpsIsolationUnverified == nil {
				actual.httpsIsolationUnverified = make(map[string]bool)
			}
			actual.httpsIsolationUnverified[listener.ID] = true
		}
	}
	if actual.loadBalancer.ProvisioningStatus == "ACTIVE" && len(actual.httpsIsolationUnverified) != 0 {
		p.planScopedHTTPSWithdrawal(desired, &actual)
		if actual.withdrawTLS != nil {
			return actual, nil
		}
	}
	configured := scopedHTTPListeners(desired)
	for _, listener := range actual.scopedListeners {
		if actual.outcome.State == cloud.OutcomeProgressing {
			break
		}
		spec, present := configured[listener.ProtocolPort]
		retiring := !present || scopedHTTPProtocol(spec) != listener.Protocol
		actual.outcome, err = p.defaultPoolListenerOutcome(&listener, retiring || listener.Protocol == terminatedHTTPS || requestTransformListenerNeedsRecovery(desired, actual, listener))
		if err != nil {
			return actual, err
		}
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete && actual.outcome.State == cloud.OutcomeReady {
		actual.deletion, actual.outcome = p.httpGatewayDeletionPlan(actual.routes, actual.scopedListeners)
	}
	return actual, nil
}

func (route observedRouteGraph) validateScopedHTTPPoolChildren() error {
	// Stock Octavia omits the pool pointer until a monitor exists. Confirm it
	// against the complete monitor collection before changing any children.
	listedMonitorID := ""
	if route.monitor != nil {
		listedMonitorID = route.monitor.ID
	}
	if route.pool.MonitorID != listedMonitorID || (route.monitor != nil && listedMonitorID == "") {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("pool %s references monitor %q but the monitor list reports %q", route.pool.ID, route.pool.MonitorID, listedMonitorID))
	}
	return crossCheckCompleteChildInventory("pool", route.pool.ID, "member", route.pool.Members, route.members, func(member pools.Member) string { return member.ID })
}

// Octavia's pool.listeners contains defaults and listeners whose policies are
// enabled and have at least one rule. The listener and policy inventories also
// include disabled policies and prove their ownership before any deletion.
func validateScopedHTTPRouteRelationships(routes []observedRouteGraph) error {
	layout := make(map[string][]observedRoutePolicy)
	memberPools, rulePolicies, policyListeners := make(map[string]string), make(map[string]string), make(map[string]string)
	poolListeners := make(map[string][]pools.ListenerID)
	unprovenPoolListeners := make(map[string][]pools.ListenerID)
	for _, route := range routes {
		for _, part := range route.parts() {
			if part.pool != nil && part.pool.Protocol != cloud.ListenerProtocolHTTP {
				continue
			}
			for _, policy := range part.policies {
				id := policy.policy.ListenerID
				if previous := policyListeners[policy.policy.ID]; previous != "" {
					return fmt.Errorf("%w: policy %s appears in listeners %s and %s", cloud.ErrOwnershipConflict, policy.policy.ID, previous, id)
				}
				policyListeners[policy.policy.ID] = id
				for _, rule := range policy.rules {
					if previous := rulePolicies[rule.rule.ID]; previous != "" {
						return fmt.Errorf("%w: rule %s appears in policies %s and %s", cloud.ErrOwnershipConflict, rule.rule.ID, previous, policy.policy.ID)
					}
					rulePolicies[rule.rule.ID] = policy.policy.ID
				}
				layout[id] = append(layout[id], policy)
				switch {
				case policy.policy.AdminStateUp && len(policy.rules) != 0:
					addPolicyPoolListeners(poolListeners, policy, id)
				case policy.initialRules && (!policy.adminObserved || policy.policy.AdminStateUp && policy.initialRulesPartial):
					// An owned, incompletely read policy can still explain the
					// link. Keep its withdrawal eligible while checking all
					// other links against their proved parents.
					addPolicyPoolListeners(unprovenPoolListeners, policy, id)
				}
			}
		}
	}
	for _, route := range routes {
		for _, part := range route.parts() {
			if part.pool == nil || part.pool.Protocol != cloud.ListenerProtocolHTTP {
				continue
			}
			known := poolListeners[part.pool.ID]
			refs := part.pool.Listeners
			if unproven := unprovenPoolListeners[part.pool.ID]; len(unproven) != 0 {
				refs = slices.DeleteFunc(slices.Clone(refs), func(ref pools.ListenerID) bool {
					return slices.Contains(unproven, ref) && !slices.Contains(known, ref)
				})
			}
			if err := crossCheckCompleteChildInventory("HTTP pool", part.pool.ID, "listener", refs, known, func(ref pools.ListenerID) string { return ref.ID }); err != nil {
				return err
			}
			for _, member := range part.members {
				if previous := memberPools[member.ID]; previous != "" {
					return fmt.Errorf("%w: member %s appears in pools %s and %s", cloud.ErrOwnershipConflict, member.ID, previous, part.pool.ID)
				}
				memberPools[member.ID] = part.pool.ID
			}
		}
	}
	for _, policies := range layout {
		if err := validateObservedPolicyLayout([]observedRouteGraph{{policies: policies}}); err != nil {
			return err
		}
	}
	return nil
}

func addPolicyPoolListeners(links map[string][]pools.ListenerID, policy observedRoutePolicy, listenerID string) {
	edge := pools.ListenerID{ID: listenerID}
	add := func(poolID string) {
		if poolID != "" && !slices.Contains(links[poolID], edge) {
			links[poolID] = append(links[poolID], edge)
		}
	}
	add(policy.policy.RedirectPoolID)
	for _, target := range policy.weightedTargets {
		add(target.PoolID)
	}
}

func scopedHTTPListeners(desired desiredGatewayGraph) map[int]cloud.ScopedListener {
	ports := make(map[int]cloud.ScopedListener, len(desired.spec.Gateway.Spec.ScopedListeners))
	for _, listener := range desired.spec.Gateway.Spec.ScopedListeners {
		ports[listener.Port] = listener
	}
	return ports
}

func scopedHTTPProtocol(spec cloud.ScopedListener) string {
	if spec.TLS != nil {
		return terminatedHTTPS
	}
	return cloud.ListenerProtocolHTTP
}

func hasAuthorizedListener(spec cloud.ScopedListener) bool {
	return spec.TLS == nil || spec.TLS.CertificateRef != ""
}

func singleServerCertificate(listener listeners.Listener) bool {
	return listener.SniContainerRefs != nil && len(listener.SniContainerRefs) == 0
}

func (actual observedGatewayGraph) scopedHTTPListenerMatches(listener listeners.Listener, spec cloud.ScopedListener) bool {
	if listener.Protocol != scopedHTTPProtocol(spec) || !actual.httpsBindingMatches(listener, spec) {
		return false
	}
	return spec.TLS == nil || actual.adminObserved[listener.ID] && hasAuthorizedListener(spec) &&
		listener.DefaultPoolID == "" && singleServerCertificate(listener) && listener.DefaultTlsContainerRef == spec.TLS.CertificateRef &&
		actual.frontendCA[spec.Port].matches(listener, spec.TLS)
}

func httpsIsolationWithdrawal(listener listeners.Listener, spec cloud.ScopedListener) graphMutation {
	if spec.Port == 0 {
		spec.Port = listener.ProtocolPort
	}
	return graphMutation{kind: graphDisableListener, listenerID: listener.ID, scopedListener: spec,
		redactTLSError: listener.Protocol == terminatedHTTPS, message: "Closing an Octavia listener to withdraw its HTTPS port",
		withdrawClientCA: listener.Protocol == terminatedHTTPS && listener.ClientCATLSContainerRef != ""}
}

func (p *Provider) planScopedHTTPSWithdrawal(desired desiredGatewayGraph, actual *observedGatewayGraph) {
	disposition := desired.spec.Gateway.Disposition
	configured := scopedHTTPListeners(desired)
	var retry *graphMutation
	for _, listener := range actual.scopedListeners {
		spec, present := configured[listener.ProtocolPort]
		https := listener.Protocol == terminatedHTTPS
		adminObserved := actual.adminObserved[listener.ID]
		var withdraw bool
		switch disposition {
		case cloud.GraphEnsure, cloud.GraphWithdrawTLS:
			withdraw = https && (!present || !actual.scopedHTTPListenerMatches(listener, spec) || listener.ProvisioningStatus == "ERROR") ||
				!https && present && spec.TLS != nil
		case cloud.GraphDelete:
			withdraw = https
		}
		if ports := desired.spec.Gateway.TLSWithdrawalPorts; len(ports) != 0 {
			// The writer observed a change on this port. Close it even if
			// the interrupted authorization has since been restored.
			withdraw = slices.Contains(ports, listener.ProtocolPort) && (https || present)
		} else {
			withdraw = withdraw || actual.httpsIsolationUnverified[listener.ID]
		}
		if !withdraw {
			continue
		}
		step := httpsIsolationWithdrawal(listener, spec)
		switch listener.ProvisioningStatus {
		case "ACTIVE":
			if listener.AdminStateUp || !adminObserved {
				actual.withdrawTLS = &step
				return
			}
		case "ERROR":
			// A failed close may report the intended admin state before
			// Amphora applied it. Retry only after fresh closes on siblings.
			retry = &step
		default:
			actual.outcome = p.progressingOutcome("Waiting for Octavia listener before HTTPS withdrawal")
		}
	}
	actual.withdrawTLS = retry
}

// A withdrawal runs before route observation. Keep the existing status only
// for a sibling whose owned frontend is still enabled with its expected TLS.
func unchangedHTTPSWithdrawalSiblings(desired desiredGatewayGraph, actual observedGatewayGraph) []int {
	step := actual.withdrawTLS
	if step == nil {
		step = actual.httpsIsolationMutation
	}
	return unchangedHTTPListenerSiblings(desired, actual, step.listenerID)
}

func unchangedHTTPListenerSiblings(desired desiredGatewayGraph, actual observedGatewayGraph, affectedListenerID string) []int {
	if actual.loadBalancer == nil || !actual.loadBalancer.AdminStateUp || actual.state.VIPAddress == "" {
		return nil
	}
	configured := scopedHTTPListeners(desired)
	ports := make([]int, 0, len(actual.scopedListeners))
	for _, listener := range actual.scopedListeners {
		if listener.ID == affectedListenerID || !actual.adminObserved[listener.ID] ||
			!listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE" {
			continue
		}
		spec, present := configured[listener.ProtocolPort]
		// A withdrawal-only request lists every intended HTTPS port. An
		// unlisted plaintext frontend is outside that request's scope.
		if !present && desired.spec.Gateway.Disposition != cloud.GraphWithdrawTLS {
			continue
		}
		if actual.scopedHTTPListenerMatches(listener, spec) && !actual.httpsIsolationUnverified[listener.ID] {
			ports = append(ports, listener.ProtocolPort)
		}
	}
	return ports
}

func scopedHTTPRoutePort(route desiredRoute) int {
	if len(route.spec.ScopedListenerPorts) == 1 {
		return route.spec.ScopedListenerPorts[0]
	}
	return 0
}

func scopedHTTPGatewayResult(desired desiredGatewayGraph, actual observedGatewayGraph, pending cloud.Outcome) cloud.GatewayResult {
	if hasHTTPRouteParts(desired, actual) {
		desired, actual, _ = httpPlanningParts(desired, actual)
	}
	for _, spec := range desired.spec.Gateway.Spec.ScopedListeners {
		if issue := actual.frontendCA[spec.Port].issue; issue != "" {
			pending.Message = issue
			break
		}
	}
	result := scopedGatewayResult(desired.spec.Gateway.Spec, actual, pending)
	if actual.routes == nil && actual.loadBalancer != nil {
		result.Outcome, result.ScopedListenerReadyPorts = pending, []int{}
		return result
	}
	portsByID := make(map[string]int, len(actual.scopedListeners))
	configured := scopedHTTPListeners(desired)
	unverified := make(map[int]bool)
	for _, listener := range actual.scopedListeners {
		portsByID[listener.ID] = listener.ProtocolPort
		if spec, found := configured[listener.ProtocolPort]; !found || actual.httpsIsolationUnverified[listener.ID] ||
			!actual.scopedHTTPListenerMatches(listener, spec) || !actual.httpFallbackReady(listener, desired.fixedHTTPResponses) ||
			!actual.isolatedPoliciesReady(desired, listener) {
			unverified[listener.ProtocolPort] = true
		}
	}
	if len(unverified) != 0 {
		result.ScopedListenerReadyPorts = slices.DeleteFunc(result.ScopedListenerReadyPorts, func(port int) bool { return unverified[port] })
		result.Outcome = pending
	}
	held := make(map[int]bool)
	for i, observed := range actual.routes {
		node, route := desired.spec.Routes[i], desired.routes[i]
		for _, policy := range observed.policies {
			port := portsByID[policy.policy.ListenerID]
			target := scopedHTTPRoutePort(route)
			switch {
			case node.WaitForHTTPPolicyRemoval || node.Disposition == cloud.GraphDelete:
				held[port] = true
				result.Outcome = pending
				result.Outcome.Message = "Waiting for outdated HTTP policy removal"
			case node.Disposition == cloud.GraphEnsure && (target != port || policyNeedsHostnameUpdate(route, policy)):
				held[port], held[target] = true, true
				result.Outcome = pending
				result.Outcome.Message = "Waiting for updated HTTP policies"
			}
		}
	}
	if len(held) != 0 {
		result.HTTPPolicyTransition = true
		result.ScopedListenerReadyPorts = slices.DeleteFunc(result.ScopedListenerReadyPorts, func(port int) bool { return held[port] })
	}
	return result
}

func deferredScopedHTTPRoutes(desired desiredGatewayGraph, actual observedGatewayGraph, readyPorts []int) map[int]bool {
	if hasHTTPRouteParts(desired, actual) {
		partDesired, partActual, owners := httpPlanningParts(desired, actual)
		parts := deferredScopedHTTPRoutes(partDesired, partActual, readyPorts)
		deferred, held := make(map[int]bool), make(map[int]bool)
		for part, owner := range owners {
			held[owner] = held[owner] || !parts[part]
			deferred[owner] = !held[owner]
		}
		return deferred
	}
	deferred := make(map[int]bool)
	for i, observed := range actual.routes {
		route := desired.routes[i]
		port := scopedHTTPRoutePort(route)
		if desired.spec.Routes[i].Disposition != cloud.GraphEnsure || !slices.Contains(readyPorts, port) || route.targetsPending ||
			route.backendTransportNeedsWithdrawal(observed) ||
			!route.fixedResponse && observed.backendProvisioningIssue(false, true) != "" {
			continue
		}
		listener := actual.scopedListeners[slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool { return listener.ProtocolPort == port })]
		if slices.ContainsFunc(observed.policies, func(policy observedRoutePolicy) bool {
			return policy.policy.ListenerID != listener.ID || policy.pending()
		}) {
			continue
		}
		steps, _, err := buildRouteEnsurePlan(route, observed)
		_, withdrawalHold := httpMemberWithdrawal(observed)
		deferred[i] = err == nil && route.redirectIssue == nil && len(steps) == 0 && (!route.unavailable || withdrawalHold == "")
	}
	return deferred
}

// Retire a port only after its authoritative policy inventory is empty.
func scopedHTTPListenerPlan(desired desiredGatewayGraph, actual observedGatewayGraph) (map[int]listeners.Listener, map[string]int, map[string]bool, []graphMutation) {
	byPort, portByID := make(map[int]listeners.Listener), make(map[string]int)
	retiring := make(map[string]bool)
	occupied := make(map[int]bool, len(actual.scopedListeners))
	var changes []graphMutation
	configured := scopedHTTPListeners(desired)
	for _, listener := range actual.scopedListeners {
		port := listener.ProtocolPort
		occupied[port], portByID[listener.ID] = true, port
		spec, present := configured[port]
		if desired.spec.Gateway.Disposition != cloud.GraphEnsure {
			continue
		}
		if !actual.httpsBindingMatches(listener, spec) || !actual.isHTTPSIsolated(listener) && actual.httpsIsolationUnverified[listener.ID] {
			// Enrollment is retained while isolation or authority cannot be proved.
			continue
		}
		if !present || scopedHTTPProtocol(spec) != listener.Protocol {
			retiring[listener.ID] = true
			if len(listener.L7Policies) == 0 {
				changes = append(changes, graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(listener.ID), message: "Deleting retired Octavia HTTP listener"})
			}
			continue
		}
		adminObserved := actual.adminObserved[listener.ID]
		if actual.scopedHTTPListenerMatches(listener, spec) {
			byPort[port] = listener
			if desired.fixedHTTPResponses && listener.ProvisioningStatus == "ACTIVE" && actual.defaultHTTPResponseCodes[listener.ID] != 404 {
				changes = append(changes, graphMutation{kind: graphSetListenerNotFound, listenerID: listener.ID, message: "Setting the default HTTP response on the scoped listener"})
			}
		} else if spec.TLS != nil && hasAuthorizedListener(spec) && adminObserved && !listener.AdminStateUp &&
			listener.ProvisioningStatus == "ACTIVE" && singleServerCertificate(listener) && listener.DefaultPoolID == "" {
			ca := actual.frontendCA[port]
			if ca.canUpdate(listener, spec.TLS) {
				changes = append(changes, graphMutation{kind: graphUpdateListenerCertificate, listenerID: listener.ID, scopedListener: spec,
					updateClientAuth: !ca.matches(listener, spec.TLS), redactTLSError: true, message: "Updating TLS configuration on a disabled Octavia HTTPS listener"})
			}
		}
	}
	if actual.loadBalancer != nil {
		for _, listener := range desired.spec.Gateway.Spec.ScopedListeners {
			if !occupied[listener.Port] && hasAuthorizedListener(listener) && actual.frontendCA[listener.Port].canCreate(listener) &&
				(len(httpsVirtualHosts(listener)) == 0 || actual.httpsIsolationAvailable) {
				changes = append(changes, graphMutation{kind: graphCreateListener, scopedListener: listener,
					redactTLSError: listener.TLS != nil, message: "Creating Octavia HTTP-family listener"})
			}
		}
	}
	return byPort, portByID, retiring, changes
}

func buildScopedHTTPTrafficRestorationPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, error) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.loadBalancer == nil {
		return nil, nil
	}
	configured := scopedHTTPListeners(desired)
	verified := make([]listeners.Listener, 0, len(actual.scopedListeners))
	for _, listener := range actual.scopedListeners {
		spec, present := configured[listener.ProtocolPort]
		if !present || listener.Protocol != scopedHTTPProtocol(spec) {
			if actual.isHTTPSIsolated(listener) {
				continue
			}
			return nil, nil
		}
		if !hasAuthorizedListener(spec) {
			// An independently denied port does not hold authorized siblings.
			continue
		}
		if !actual.scopedHTTPListenerMatches(listener, spec) || !actual.httpFallbackReady(listener, desired.fixedHTTPResponses) ||
			!actual.isolatedPoliciesReady(desired, listener) || actual.httpsIsolationUnverified[listener.ID] ||
			listener.Protocol == terminatedHTTPS && listener.ProvisioningStatus != "ACTIVE" {
			continue
		}
		verified = append(verified, listener)
	}
	if len(verified) == 0 {
		return nil, nil
	}
	plan, err := buildGatewayAddressRestorationPlan(desired.spec.Gateway.Spec.ExternalNetworkID, actual)
	if err != nil {
		return nil, err
	}
	for _, listener := range verified {
		if !listener.AdminStateUp {
			plan = append(plan, graphMutation{kind: graphEnableListener, listenerID: listener.ID, redactTLSError: listener.Protocol == terminatedHTTPS,
				message: "Enabling Octavia listener after validating the Gateway graph"})
		}
	}
	if !actual.loadBalancer.AdminStateUp {
		plan = append(plan, graphMutation{kind: graphEnableLoadBalancer, message: "Enabled Octavia load balancer after validating the Gateway graph"})
	}
	return plan, nil
}
