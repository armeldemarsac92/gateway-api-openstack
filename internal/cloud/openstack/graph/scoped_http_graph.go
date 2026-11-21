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
	var err error
	actual.routes, err = p.observeRouteGraphsWithListeners(ctx, owners, actual.loadBalancer, nil, actual.protocol, actual.scopedListeners)
	if err != nil {
		return actual, err
	}
	configured := scopedHTTPListeners(desired)
	for _, listener := range actual.scopedListeners {
		if actual.outcome.State == cloud.OutcomeProgressing {
			break
		}
		spec, present := configured[listener.ProtocolPort]
		retiring := !present || scopedHTTPProtocol(spec) != listener.Protocol
		actual.outcome, err = p.defaultPoolListenerOutcome(&listener, retiring || listener.Protocol == terminatedHTTPS)
		if err != nil {
			return actual, err
		}
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete && actual.outcome.State == cloud.OutcomeReady {
		for _, route := range actual.routes {
			if slices.ContainsFunc(route.policies, observedRoutePolicy.pending) {
				actual.outcome = p.progressingOutcome("Waiting for HTTP policy provisioning before Gateway cleanup")
				return actual, nil
			}
			if issue := route.backendProvisioningIssue(true, true); issue != "" {
				actual.outcome = p.progressingOutcome(issue)
				return actual, nil
			}
			for _, policy := range route.policies {
				for _, rule := range policy.rules {
					actual.deletion.routes = append(actual.deletion.routes, gatewayDeletionStep{
						resource: gatewayDeletionL7Rule, resourceID: rule.rule.ID, parentID: policy.policy.ID,
					})
				}
				actual.deletion.routes = append(actual.deletion.routes, gatewayDeletionStep{resource: gatewayDeletionL7Policy, resourceID: policy.policy.ID})
			}
			if route.pool != nil {
				actual.deletion.pools = appendDefaultPoolDeletionSteps(actual.deletion.pools, route)
			}
		}
		for _, listener := range actual.scopedListeners {
			actual.deletion.listeners = append(actual.deletion.listeners, listenerDeletionStep(listener.ID))
		}
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
	for _, route := range routes {
		if route.pool == nil {
			continue
		}
		var referenced []pools.ListenerID
		for _, policy := range route.policies {
			id := policy.policy.ListenerID
			layout[id] = append(layout[id], policy)
			edge := pools.ListenerID{ID: id}
			if policy.policy.AdminStateUp && len(policy.rules) != 0 && !slices.Contains(referenced, edge) {
				referenced = append(referenced, edge)
			}
		}
		if err := crossCheckCompleteChildInventory("HTTP pool", route.pool.ID, "listener", route.pool.Listeners, referenced, func(ref pools.ListenerID) string { return ref.ID }); err != nil {
			return err
		}
	}
	for _, policies := range layout {
		if err := validateObservedPolicyLayout([]observedRouteGraph{{policies: policies}}); err != nil {
			return err
		}
	}
	return nil
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

func supportsSingleCertificate(listener listeners.Listener) bool {
	return listener.SniContainerRefs != nil && len(listener.SniContainerRefs) == 0 &&
		(listener.ClientAuthentication == "" || listener.ClientAuthentication == string(listeners.ClientAuthenticationNone)) &&
		listener.ClientCATLSContainerRef == "" && listener.ClientCRLContainerRef == ""
}

func scopedHTTPListenerMatches(listener listeners.Listener, spec cloud.ScopedListener, adminObserved bool) bool {
	if listener.Protocol != scopedHTTPProtocol(spec) {
		return false
	}
	return spec.TLS == nil || adminObserved && hasAuthorizedListener(spec) &&
		listener.DefaultPoolID == "" && supportsSingleCertificate(listener) && listener.DefaultTlsContainerRef == spec.TLS.CertificateRef
}

func (p *Provider) planScopedHTTPSWithdrawal(desired desiredGatewayGraph, actual *observedGatewayGraph) {
	disposition := desired.spec.Gateway.Disposition
	if disposition == cloud.GraphPreserve {
		return
	}
	configured := scopedHTTPListeners(desired)
	var retry *graphMutation
	for _, listener := range actual.scopedListeners {
		spec, present := configured[listener.ProtocolPort]
		https := listener.Protocol == terminatedHTTPS
		adminObserved := actual.adminObserved[listener.ID]
		var withdraw bool
		switch disposition {
		case cloud.GraphEnsure, cloud.GraphWithdrawTLS:
			withdraw = https && (!present || !scopedHTTPListenerMatches(listener, spec, adminObserved) || listener.ProvisioningStatus == "ERROR") ||
				!https && present && spec.TLS != nil
		case cloud.GraphDelete:
			withdraw = https
		}
		if !withdraw {
			continue
		}
		step := graphMutation{kind: graphDisableListener, listenerID: listener.ID, scopedListener: spec, redactTLSError: https,
			message: "Disabling Octavia listener to withdraw its HTTPS port"}
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
	if actual.loadBalancer == nil || !actual.loadBalancer.AdminStateUp || actual.state.VIPAddress == "" {
		return nil
	}
	configured := scopedHTTPListeners(desired)
	var ports []int
	for _, listener := range actual.scopedListeners {
		if listener.ID == actual.withdrawTLS.listenerID || !actual.adminObserved[listener.ID] ||
			!listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE" {
			continue
		}
		spec, present := configured[listener.ProtocolPort]
		// A withdrawal-only request lists every intended HTTPS port. An
		// unlisted plaintext frontend is outside that request's scope.
		if !present && desired.spec.Gateway.Disposition != cloud.GraphWithdrawTLS {
			continue
		}
		if scopedHTTPListenerMatches(listener, spec, true) {
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
		if spec, found := configured[listener.ProtocolPort]; !found || !scopedHTTPListenerMatches(listener, spec, actual.adminObserved[listener.ID]) {
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
			case node.Disposition == cloud.GraphEnsure && (target != port || policy.hasOutdatedHostname(route.hostnameRuleValue)):
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
	deferred := make(map[int]bool)
	for i, observed := range actual.routes {
		route := desired.routes[i]
		port := scopedHTTPRoutePort(route)
		if desired.spec.Routes[i].Disposition != cloud.GraphEnsure || !slices.Contains(readyPorts, port) || observed.backendProvisioningIssue(false, true) != "" {
			continue
		}
		listener := actual.scopedListeners[slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool { return listener.ProtocolPort == port })]
		if slices.ContainsFunc(observed.policies, func(policy observedRoutePolicy) bool {
			return policy.policy.ListenerID != listener.ID || policy.pending()
		}) {
			continue
		}
		steps, _, err := buildRouteEnsurePlan(route, observed)
		deferred[i] = err == nil && len(steps) == 0
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
		if !present || scopedHTTPProtocol(spec) != listener.Protocol {
			retiring[listener.ID] = true
			if len(listener.L7Policies) == 0 {
				changes = append(changes, graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(listener.ID), message: "Deleting retired Octavia HTTP listener"})
			}
			continue
		}
		adminObserved := actual.adminObserved[listener.ID]
		if scopedHTTPListenerMatches(listener, spec, adminObserved) {
			byPort[port] = listener
		} else if spec.TLS != nil && hasAuthorizedListener(spec) && adminObserved && !listener.AdminStateUp &&
			listener.ProvisioningStatus == "ACTIVE" && supportsSingleCertificate(listener) {
			changes = append(changes, graphMutation{kind: graphUpdateListenerCertificate, listenerID: listener.ID, scopedListener: spec,
				redactTLSError: true, message: "Updating the certificate on a disabled Octavia HTTPS listener"})
		}
	}
	if actual.loadBalancer != nil {
		for _, listener := range desired.spec.Gateway.Spec.ScopedListeners {
			if !occupied[listener.Port] && hasAuthorizedListener(listener) {
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
			return nil, nil
		}
		if !hasAuthorizedListener(spec) {
			// An independently denied port does not hold authorized siblings.
			continue
		}
		if !scopedHTTPListenerMatches(listener, spec, actual.adminObserved[listener.ID]) ||
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
