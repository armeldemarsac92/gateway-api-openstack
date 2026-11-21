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
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func isTCPGraph(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	if actual.listener != nil {
		return actual.listener.Protocol == cloud.ListenerProtocolTCP
	}
	if desired.spec.Gateway.Disposition != cloud.GraphPreserve {
		return desired.spec.Gateway.Spec.ListenerProtocol == cloud.ListenerProtocolTCP
	}
	// A detaching TCPRoute retains its protocol and UID even if the listener
	// has disappeared. This permits cleanup of the proven, now unlinked pool.
	return slices.ContainsFunc(desired.spec.Routes, func(node cloud.RouteGraphNode) bool {
		return node.Disposition == cloud.GraphDelete && node.Spec.GatewayRequirements.ListenerProtocol == cloud.ListenerProtocolTCP
	})
}

func validateTCPListener(listener *listeners.Listener, loadBalancerID string) error {
	if listener.ID == "" || len(listener.L7Policies) != 0 || len(listener.Loadbalancers) != 1 ||
		listener.Loadbalancers[0].ID != loadBalancerID {
		return fmt.Errorf("%w: TCP listener %s has an unexpected identity, load balancer or L7 policies", cloud.ErrOwnershipConflict, listener.ID)
	}
	return nil
}

func validateTCPPool(pool pools.Pool, loadBalancerID string, listener *listeners.Listener) error {
	if pool.ID == "" || pool.Protocol != cloud.ListenerProtocolTCP || pool.TLSEnabled ||
		len(pool.Loadbalancers) != 1 || pool.Loadbalancers[0].ID != loadBalancerID {
		return fmt.Errorf("%w: TCP pool %s has an unexpected protocol, identity or load balancer", cloud.ErrOwnershipConflict, pool.ID)
	}
	for _, parent := range pool.Listeners {
		if listener == nil || parent.ID != listener.ID || listener.DefaultPoolID != pool.ID {
			return fmt.Errorf("%w: TCP pool %s has an unexpected listener %s", cloud.ErrOwnershipConflict, pool.ID, parent.ID)
		}
	}
	return nil
}

func (route observedRouteGraph) validateTCPPoolChildren() error {
	if route.pool.MonitorID != "" && (route.monitor == nil || route.monitor.ID != route.pool.MonitorID) {
		return fmt.Errorf("%w: TCP pool %s references unproven monitor %s", cloud.ErrOwnershipConflict, route.pool.ID, route.pool.MonitorID)
	}
	members := make(map[string]bool, len(route.members))
	for _, member := range route.members {
		if member.ID == "" || members[member.ID] {
			return fmt.Errorf("%w: TCP pool %s has a duplicate or missing member ID", cloud.ErrOwnershipConflict, route.pool.ID)
		}
		members[member.ID] = true
	}
	for _, reference := range route.pool.Members {
		if !members[reference.ID] {
			return fmt.Errorf("%w: TCP pool %s references unproven member %s", cloud.ErrOwnershipConflict, route.pool.ID, reference.ID)
		}
	}
	return nil
}

func defaultPoolOwner(listener *listeners.Listener, routes []observedRouteGraph) int {
	return slices.IndexFunc(routes, func(route observedRouteGraph) bool {
		return route.pool != nil && route.pool.ID == listener.DefaultPoolID
	})
}

func unprovenDefaultPool(listener *listeners.Listener) error {
	return fmt.Errorf("%w: Gateway listener %s references an unproven default pool %s", cloud.ErrOwnershipConflict, listener.ID, listener.DefaultPoolID)
}

func clearTCPListenerDefault() graphMutation {
	return graphMutation{kind: graphClearListenerDefaultPool, message: "Detaching the Octavia TCP listener default pool"}
}

func (p *Provider) tcpListenerOutcome(listener *listeners.Listener, allowRecovery bool) (cloud.Outcome, error) {
	switch {
	case listener.ProvisioningStatus == "ACTIVE" || allowRecovery && listener.ProvisioningStatus == "ERROR":
		return cloud.ReadyOutcome(), nil
	case strings.HasPrefix(listener.ProvisioningStatus, "PENDING_"):
		return p.progressingOutcome(fmt.Sprintf("Waiting for Octavia TCP listener %s to be ACTIVE (observed %q)", listener.ID, listener.ProvisioningStatus)), nil
	default:
		return cloud.Outcome{}, tcpListenerProvisioningFailure(listener)
	}
}

func tcpListenerProvisioningFailure(listener *listeners.Listener) error {
	return cloud.NewProviderError(cloud.ErrorCategoryResourceFailure,
		fmt.Errorf("octavia TCP listener %s provisioning status is %q", listener.ID, listener.ProvisioningStatus))
}

func (p *Provider) buildTCPGatewayDeletionPlan(ctx context.Context, identities []Identity, lb *octavia.LoadBalancer, listener *listeners.Listener) (gatewayDeletionPlan, error) {
	plan := gatewayDeletionPlan{tcpListener: listener}
	if listener != nil {
		if err := validateTCPListener(listener, lb.ID); err != nil {
			return gatewayDeletionPlan{}, err
		}
		outcome, err := p.tcpListenerOutcome(listener, true)
		if err != nil {
			return gatewayDeletionPlan{}, err
		}
		if outcome.State == cloud.OutcomeProgressing {
			plan.tcpPending = outcome.Message
			return plan, nil
		}
		plan.listeners = append(plan.listeners, gatewayDeletionStep{resource: gatewayDeletionListener, resourceID: listener.ID})
	}
	routes, err := p.observeRouteGraphs(ctx, identities, lb, listener, true)
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	if listener != nil && listener.DefaultPoolID != "" && defaultPoolOwner(listener, routes) == -1 {
		return gatewayDeletionPlan{}, unprovenDefaultPool(listener)
	}
	// With an ERROR listener, an empty readback does not prove that Amphora
	// applied the detach. Gateway deletion can remove the owned listener first.
	if listener != nil && listener.ProvisioningStatus == "ERROR" {
		return plan, nil
	}
	for _, route := range routes {
		if route.pool == nil {
			continue
		}
		if issue := route.backendProvisioningIssue(true, true); issue != "" && (listener == nil || listener.DefaultPoolID == "") {
			plan.tcpPending = issue
		}
		poolID := route.pool.ID
		if route.monitor != nil {
			plan.pools = append(plan.pools, gatewayDeletionStep{resource: gatewayDeletionMonitor, resourceID: route.monitor.ID, parentID: poolID})
		}
		for _, member := range route.members {
			plan.pools = append(plan.pools, gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: member.ID, parentID: poolID})
		}
		plan.pools = append(plan.pools, gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: poolID})
	}
	return plan, nil
}

// A TCP listener forwards through one pointer. Detach before changing its
// serving pool or tearing down a route, then attach only an observed, complete
// backend. A Preserve fragment provides ownership proof for the detach and
// grants no authority to change the pool.
func (p *Provider) buildTCPGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ReadyOutcome(),
		Gateway: cloud.GatewayReadyResult(actual.state),
		Routes:  observedRouteDeleteCompletions(desired, actual),
	}
	plan, err := buildGatewayPrerequisitePlan(desired, actual)
	if err != nil {
		return nil, result, err
	}
	restoration, err := buildGatewayTrafficRestorationPlan(desired, actual)
	if err != nil {
		return nil, result, err
	}
	listenerFailed := actual.listener != nil && actual.listener.ProvisioningStatus == "ERROR"
	gatewayReady := actual.loadBalancer != nil && actual.listener != nil && !listenerFailed && len(plan) == 0 && len(restoration) == 0
	if desired.spec.Gateway.Disposition == cloud.GraphEnsure && !gatewayReady {
		result.Gateway.Outcome = p.progressingOutcome("Waiting for the Gateway infrastructure")
		result.Outcome = result.Gateway.Outcome
	}
	defaultOwner := -1
	if actual.listener != nil && actual.listener.DefaultPoolID != "" {
		defaultOwner = defaultPoolOwner(actual.listener, actual.routes)
		if defaultOwner == -1 {
			return nil, result, unprovenDefaultPool(actual.listener)
		}
	}
	detach := false
	var cleanup, backend, attach []graphMutation
	routeResults := make([]cloud.RouteGraphResult, 0, len(desired.routes))
	for index, route := range desired.routes {
		observed := observedRouteGraph{}
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		routeResult := cloud.RouteReadyResult(routeStateFromObservation(observed))
		switch desired.spec.Routes[index].Disposition {
		case cloud.GraphPreserve:
			detach = detach || index == defaultOwner && desired.spec.Gateway.Disposition == cloud.GraphEnsure
		case cloud.GraphDelete:
			if index == defaultOwner {
				detach = true
				routeResult.Outcome = p.progressingOutcome("Waiting for the TCP listener to detach before cleanup")
			} else if issue := observed.backendProvisioningIssue(true, true); issue != "" {
				routeResult.Outcome = p.progressingOutcome(issue)
			} else if steps := buildRouteDeletionPlan(observed); len(steps) > 0 {
				routeResult.Outcome = p.progressingOutcome(steps[0].message())
				cleanup = append(cleanup, graphMutation{kind: graphRouteMutation, routeIndex: index, route: steps[0], message: steps[0].message()})
			}
		case cloud.GraphEnsure:
			if err := validateRouteGatewayIDs(route.spec.Gateway, actual.state); err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			steps, state, err := buildRouteEnsurePlan(route, observed)
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			routeResult.State = state
			issue := observed.backendProvisioningIssue(false, true)
			canServe := slices.ContainsFunc(route.members, func(member cloud.Member) bool { return !member.Draining })
			backendReady := observed.pool != nil && issue == "" && canServe && len(steps) == 0
			detach = detach || index == defaultOwner && !backendReady
			switch {
			case issue != "":
				routeResult.Outcome = p.progressingOutcome(issue)
			case actual.listener == nil || actual.loadBalancer == nil:
				routeResult.Outcome = p.progressingOutcome("Waiting for the Gateway infrastructure")
			case len(steps) != 0:
				routeResult.Outcome = p.progressingOutcome(steps[0].message())
				backend = append(backend, graphMutation{kind: graphRouteMutation, routeIndex: index, route: steps[0], message: steps[0].message()})
			case !backendReady:
				routeResult.Outcome = p.progressingOutcome("Waiting for a ready TCP backend member")
			case index != defaultOwner:
				message := "Attaching the selected Octavia TCP pool"
				routeResult.Outcome = p.progressingOutcome(message)
				attach = append(attach, graphMutation{kind: graphSetListenerDefaultPool, poolID: observed.pool.ID, message: message})
			case !gatewayReady:
				routeResult.Outcome = result.Gateway.Outcome
			}
		}
		routeResults = append(routeResults, cloud.RouteGraphResult{Identity: route.spec.Identity, Result: routeResult})
		if routeResult.Outcome.State == cloud.OutcomeProgressing {
			result.Outcome = routeResult.Outcome
		}
	}
	result.Routes = routeResults
	if detach {
		mutation := clearTCPListenerDefault()
		result.Outcome = p.progressingOutcome(mutation.message)
		return []graphMutation{mutation}, result, nil
	}
	if listenerFailed {
		// A route-only operation can revoke its verified default. An ERROR
		// listener with no default may still be forwarding, so hold its children
		// until a subsequent read shows ACTIVE or Gateway deletion removes it.
		result.Routes = nil
		return nil, result, tcpListenerProvisioningFailure(actual.listener)
	}
	plan = append(plan, cleanup...)
	plan = append(plan, backend...)
	plan = append(plan, attach...)
	plan = append(plan, restoration...)
	if len(plan) != 0 {
		result.Outcome = p.progressingOutcome(plan[0].message)
	}
	return plan, result, nil
}

func validateRouteGatewayIDs(recorded, observed cloud.GatewayState) error {
	if recorded.LoadBalancerID != "" && observed.LoadBalancerID != "" && recorded.LoadBalancerID != observed.LoadBalancerID ||
		recorded.ListenerID != "" && observed.ListenerID != "" && recorded.ListenerID != observed.ListenerID {
		return fmt.Errorf("%w: route Gateway IDs differ from the observed graph", cloud.ErrOwnershipConflict)
	}
	return nil
}
