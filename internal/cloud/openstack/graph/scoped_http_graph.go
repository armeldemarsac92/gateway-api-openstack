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
	var err error
	actual.routes, err = p.observeRouteGraphsWithListeners(ctx, owners, actual.loadBalancer, nil, actual.protocol, actual.scopedListeners)
	if err != nil {
		return actual, err
	}
	configured := scopedHTTPPorts(desired)
	for _, listener := range actual.scopedListeners {
		if actual.outcome.State == cloud.OutcomeProgressing {
			break
		}
		actual.outcome, err = p.defaultPoolListenerOutcome(&listener, !configured[listener.ProtocolPort])
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

func scopedHTTPPorts(desired desiredGatewayGraph) map[int]bool {
	ports := make(map[int]bool, len(desired.spec.Gateway.Spec.ScopedListeners))
	for _, listener := range desired.spec.Gateway.Spec.ScopedListeners {
		ports[listener.Port] = true
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
	for _, listener := range actual.scopedListeners {
		portsByID[listener.ID] = listener.ProtocolPort
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
func scopedHTTPListenerPlan(desired desiredGatewayGraph, actual observedGatewayGraph) (map[int]listeners.Listener, map[string]int, []graphMutation) {
	byPort, portByID := make(map[int]listeners.Listener), make(map[string]int)
	var changes []graphMutation
	configured := scopedHTTPPorts(desired)
	for _, listener := range actual.scopedListeners {
		byPort[listener.ProtocolPort], portByID[listener.ID] = listener, listener.ProtocolPort
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure && !configured[listener.ProtocolPort] && len(listener.L7Policies) == 0 {
			changes = append(changes, graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(listener.ID), message: "Deleting retired Octavia HTTP listener"})
		}
	}
	if actual.loadBalancer != nil {
		for _, listener := range desired.spec.Gateway.Spec.ScopedListeners {
			if _, exists := byPort[listener.Port]; !exists {
				changes = append(changes, graphMutation{kind: graphCreateListener, scopedListener: listener, message: "Creating Octavia HTTP listener"})
			}
		}
	}
	return byPort, portByID, changes
}

func buildScopedHTTPTrafficRestorationPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, error) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.loadBalancer == nil ||
		len(actual.scopedListeners) != len(desired.spec.Gateway.Spec.ScopedListeners) {
		return nil, nil
	}
	configured := scopedHTTPPorts(desired)
	for _, listener := range actual.scopedListeners {
		if !configured[listener.ProtocolPort] {
			return nil, nil
		}
	}
	plan, err := buildGatewayAddressRestorationPlan(desired.spec.Gateway.Spec.ExternalNetworkID, actual)
	if err != nil {
		return nil, err
	}
	for _, listener := range actual.scopedListeners {
		if !listener.AdminStateUp {
			plan = append(plan, graphMutation{kind: graphEnableListener, listenerID: listener.ID, message: "Enabled Octavia HTTP listener after validating the Gateway graph"})
		}
	}
	if !actual.loadBalancer.AdminStateUp {
		plan = append(plan, graphMutation{kind: graphEnableLoadBalancer, message: "Enabled Octavia load balancer after validating the Gateway graph"})
	}
	return plan, nil
}
