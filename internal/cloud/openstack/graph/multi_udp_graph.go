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
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const udpPortTagKey = "udp-port"

var udpPortTagPrefix = openstackidentity.Tag(udpPortTagKey, "")

func udpPortTag(port int) string {
	return openstackidentity.Tag(udpPortTagKey, strconv.Itoa(port))
}

func isUDPPortTag(tag string) bool {
	return strings.HasPrefix(tag, udpPortTagPrefix) || tag == strings.TrimSuffix(udpPortTagPrefix, "=")
}

func hasScopedUDPListeners(items []listeners.Listener) bool {
	return slices.ContainsFunc(items, func(listener listeners.Listener) bool {
		return slices.ContainsFunc(listener.Tags, isUDPPortTag)
	})
}

func (p *Provider) managedUDPListeners(identity Identity, lb *octavia.LoadBalancer, items []listeners.Listener, anchor int) ([]listeners.Listener, error) {
	// Octavia exposes both views. A list entry alone cannot establish that a
	// listener belongs to this graph, including after an uncertain create.
	if lb.Listeners == nil {
		return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("load balancer %s omitted its UDP listener inventory", lb.ID))
	}
	ports := make(map[int]bool, len(items))
	ids := make(map[string]bool, len(items))
	legacy := false
	for index := range items {
		listener := &items[index]
		port := listener.ProtocolPort
		if listener.ProjectID != p.projectID || !identity.MatchesGateway(listener.Tags, roleListener) ||
			listener.Protocol != cloud.ListenerProtocolUDP || port < 1 || port > 65535 || ports[port] || ids[listener.ID] {
			return nil, fmt.Errorf("%w: invalid or duplicate UDP listener %s on load balancer %s", cloud.ErrOwnershipConflict, listener.ID, lb.ID)
		}
		if err := validateDefaultPoolListener(listener, lb.ID); err != nil {
			return nil, err
		}
		if len(lb.VIPSecurityGroupIDs) > 0 && len(listener.AllowedCIDRs) > 0 {
			return nil, fmt.Errorf("%w: custom VIP security groups cannot be combined with listener allowed CIDRs", cloud.ErrOwnershipConflict)
		}
		ports[port], ids[listener.ID] = true, true

		marked := false
		for _, tag := range listener.Tags {
			if isUDPPortTag(tag) {
				if tag != udpPortTag(port) || marked {
					return nil, fmt.Errorf("%w: UDP listener %s has inconsistent port tags", cloud.ErrOwnershipConflict, listener.ID)
				}
				marked = true
			}
		}
		if !marked {
			if legacy || port != anchor {
				return nil, fmt.Errorf("%w: UDP listener %s does not match the legacy binding", cloud.ErrOwnershipConflict, listener.ID)
			}
			legacy = true
		}
	}
	if len(lb.Listeners) != len(ids) {
		return nil, fmt.Errorf("%w: load balancer %s has duplicate UDP listener references", cloud.ErrOwnershipConflict, lb.ID)
	}
	slices.SortFunc(items, func(a, b listeners.Listener) int { return a.ProtocolPort - b.ProtocolPort })
	// Use a non-nil empty list to select multi-listener pool validation before
	// creating the first listener or after the last listener disappears.
	if items == nil {
		items = []listeners.Listener{}
	}
	return items, nil
}

// GatewayState retains its single ID for legacy callers, while the graph
// authorizes every frontend from the independently observed listener set.
func udpRepresentativeListenerID(managed []listeners.Listener, anchor int) string {
	for _, listener := range managed {
		if listener.ProtocolPort == anchor || !slices.ContainsFunc(listener.Tags, isUDPPortTag) {
			return listener.ID
		}
	}
	if len(managed) == 0 {
		return ""
	}
	return managed[0].ID
}

func (p *Provider) observeMultiUDPGraph(
	ctx context.Context,
	desired desiredGatewayGraph,
	actual observedGatewayGraph,
	owners []routeGraphOwner,
	items []listeners.Listener,
	parentPending bool,
) (observedGatewayGraph, error) {
	if actual.protocol != "" && actual.protocol != cloud.ListenerProtocolUDP {
		return actual, fmt.Errorf("%w: scoped UDP listeners differ from the Gateway protocol binding", cloud.ErrOwnershipConflict)
	}
	actual.protocol = cloud.ListenerProtocolUDP
	anchor := desired.spec.Gateway.Spec.ListenerPort
	managed, err := p.managedUDPListeners(desired.identity, actual.loadBalancer, items, anchor)
	if err != nil {
		return actual, err
	}
	actual.udpListeners = managed
	actual.state.ListenerID = udpRepresentativeListenerID(managed, anchor)
	for _, listener := range managed {
		if parentPending {
			continue
		}
		outcome, err := p.defaultPoolListenerOutcome(&listener, true)
		if err != nil {
			return actual, err
		}
		if outcome.State == cloud.OutcomeProgressing {
			actual.outcome = outcome
		}
	}
	if actual.outcome.State == cloud.OutcomeProgressing {
		return actual, nil
	}
	if actual.loadBalancer.Pools == nil {
		return actual, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("load balancer %s omitted its UDP pool inventory", actual.loadBalancer.ID))
	}
	actual.routes, err = p.observeRouteGraphsWithListeners(ctx, owners, actual.loadBalancer, nil, actual.protocol, managed)
	if err != nil {
		return actual, err
	}
	for index := range managed {
		listener := &managed[index]
		if listener.DefaultPoolID != "" && defaultPoolOwner(listener, actual.routes) == -1 {
			return actual, unprovenDefaultPool(listener)
		}
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		actual.deletion = multiUDPDeletionPlan(actual)
		if actual.deletion.defaultPoolPending != "" {
			actual.outcome = p.progressingOutcome(actual.deletion.defaultPoolPending)
		}
	}
	return actual, nil
}

func (p *Provider) validateMultiUDPDefaultPool(pool pools.Pool, lbID string, managed []listeners.Listener, protocol string) error {
	if err := validateDefaultPoolParent(pool, lbID, protocol); err != nil {
		return err
	}
	if pool.ProjectID != p.projectID {
		return fmt.Errorf("%w: default pool %s has an unproven project", cloud.ErrOwnershipConflict, pool.ID)
	}
	if pool.Listeners == nil {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("pool %s omitted its UDP listener inventory", pool.ID))
	}
	byID := make(map[string]listeners.Listener, len(managed))
	for _, listener := range managed {
		byID[listener.ID] = listener
	}
	reverse := make(map[string]bool, len(pool.Listeners))
	disagree := func(listenerID string) error {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("listener %s and pool %s disagree about their UDP default association", listenerID, pool.ID))
	}
	for _, ref := range pool.Listeners {
		listener, found := byID[ref.ID]
		if !found || reverse[ref.ID] || listener.Protocol != protocol {
			return fmt.Errorf("%w: pool %s has an unexpected listener relationship with %s", cloud.ErrOwnershipConflict, pool.ID, ref.ID)
		}
		if listener.DefaultPoolID != pool.ID {
			return disagree(ref.ID)
		}
		reverse[ref.ID] = true
	}
	for _, listener := range managed {
		if listener.DefaultPoolID == pool.ID {
			if listener.Protocol != protocol {
				return fmt.Errorf("%w: listener %s references a %s pool", cloud.ErrOwnershipConflict, listener.ID, protocol)
			}
			if !reverse[listener.ID] {
				return disagree(listener.ID)
			}
		}
	}
	return nil
}

func multiUDPDeletionPlan(actual observedGatewayGraph) gatewayDeletionPlan {
	var plan gatewayDeletionPlan
	for _, listener := range actual.udpListeners {
		if listener.DefaultPoolID != "" {
			// The graph planner detaches known edges before processing this
			// subtree. One fresh, settled observation follows each detach.
			return plan
		}
	}
	for _, listener := range actual.udpListeners {
		if listener.ProvisioningStatus == "ERROR" {
			// An empty default readback under ERROR does not prove that the
			// dataplane detached. Remove the failed frontend before its backend.
			plan.listeners = append(plan.listeners, udpListenerDeletionStep(listener.ID))
		}
	}
	if len(plan.listeners) != 0 {
		return plan
	}
	for _, route := range actual.routes {
		if route.pool == nil {
			continue
		}
		if issue := route.backendProvisioningIssue(true, true); issue != "" {
			plan.defaultPoolPending = issue
		}
		plan.pools = appendDefaultPoolDeletionSteps(plan.pools, route)
	}
	for _, listener := range actual.udpListeners {
		plan.listeners = append(plan.listeners, udpListenerDeletionStep(listener.ID))
	}
	return plan
}

func udpListenerDeletionStep(id string) gatewayDeletionStep {
	return gatewayDeletionStep{resource: gatewayDeletionListener, resourceID: id}
}

func udpGatewayResult(spec cloud.GatewaySpec, actual observedGatewayGraph, pending cloud.Outcome) cloud.GatewayResult {
	result := cloud.GatewayResult{State: actual.state, Outcome: pending, UDPReadyPorts: []int{}}
	if actual.loadBalancer == nil || !actual.loadBalancer.AdminStateUp || actual.udpListeners == nil {
		return result
	}
	if spec.ExternalNetworkID == "" && actual.floatingIP != nil ||
		spec.ExternalNetworkID != "" && (actual.floatingIP == nil || actual.floatingIP.FloatingNetworkID != spec.ExternalNetworkID || actual.floatingIP.PortID == "") {
		return result
	}
	ready := make(map[int]bool, len(actual.udpListeners))
	for _, listener := range actual.udpListeners {
		ready[listener.ProtocolPort] = listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE"
	}
	for _, listener := range spec.UDPListeners {
		if ready[listener.Port] {
			result.UDPReadyPorts = append(result.UDPReadyPorts, listener.Port)
		}
	}
	if len(result.UDPReadyPorts) == len(spec.UDPListeners) && len(actual.udpListeners) == len(spec.UDPListeners) {
		result.Outcome = cloud.ReadyOutcome()
	}
	return result
}

func (p *Provider) buildMultiUDPGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(actual.state)}
	ensureGateway := desired.spec.Gateway.Disposition == cloud.GraphEnsure
	spec := desired.spec.Gateway.Spec
	if ensureGateway {
		result.Gateway = udpGatewayResult(spec, actual, p.progressingOutcome("Waiting for the Gateway UDP listeners"))
		result.Outcome = result.Gateway.Outcome
	}
	gatewayReady := result.Gateway.Outcome.State == cloud.OutcomeReady
	prerequisites, err := buildGatewayPrerequisitePlan(desired, actual)
	if err != nil {
		return nil, result, err
	}
	found := make(map[int]listeners.Listener, len(actual.udpListeners))
	configured := make(map[int]bool, len(spec.UDPListeners))
	for _, listener := range actual.udpListeners {
		found[listener.ProtocolPort] = listener
	}
	for _, listener := range spec.UDPListeners {
		configured[listener.Port] = true
		if _, exists := found[listener.Port]; !exists && actual.loadBalancer != nil {
			prerequisites = append(prerequisites, graphMutation{kind: graphCreateListener, udpListener: listener, message: "Creating Octavia UDP listener"})
		}
	}

	var detach, remove, cleanup, backend, attach, restore []graphMutation
	keepEdge := make(map[int]bool, len(actual.udpListeners))
	readyByPool := make(map[string]bool, len(actual.routes))
	for index, route := range desired.routes {
		observed := observedRouteGraph{}
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		node := desired.spec.Routes[index]
		routeResult := cloud.RouteReadyResult(routeStateFromObservation(observed))
		switch node.Disposition {
		case cloud.GraphDelete:
			if observed.pool != nil && len(observed.pool.Listeners) != 0 {
				routeResult.Outcome = p.progressingOutcome("Waiting for UDP listeners to detach before cleanup")
			} else if issue := observed.backendProvisioningIssue(true, true); issue != "" {
				routeResult.Outcome = p.progressingOutcome(issue)
			} else if steps := buildRouteDeletionPlan(observed); len(steps) != 0 {
				step := steps[0]
				routeResult.Outcome = p.progressingOutcome(step.message())
				cleanup = append(cleanup, graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, message: step.message()})
			}
		case cloud.GraphEnsure:
			if err := validateMultiUDPRouteGatewayIDs(route.spec.Gateway, actual.state, actual.udpListeners); err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			steps, state, err := buildRouteEnsurePlan(route, observed)
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			routeResult.State = state
			issue := observed.backendProvisioningIssue(false, true)
			canServe := slices.ContainsFunc(route.members, func(member cloud.Member) bool { return !member.Draining })
			ready := observed.pool != nil && issue == "" && len(steps) == 0 && canServe &&
				hasActiveUDPBackend(route, observed, actual.state.VIPAddress)
			if ready {
				readyByPool[observed.pool.ID] = true
			}
			switch {
			case issue != "":
				routeResult.Outcome = p.progressingOutcome(issue)
			case actual.loadBalancer == nil:
				routeResult.Outcome = p.progressingOutcome("Waiting for the Gateway infrastructure")
			case len(steps) != 0:
				step := steps[0]
				routeResult.Outcome = p.progressingOutcome(step.message())
				backend = append(backend, graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, message: step.message()})
			case !ready:
				routeResult.Outcome = p.progressingOutcome("Waiting for a ready UDP backend member in the VIP IP family")
			}
			if ready {
				for _, port := range route.spec.UDPListenerPorts {
					listener, exists := found[port]
					switch {
					case !exists:
						routeResult.Outcome = p.progressingOutcome("Waiting for the selected UDP listener")
					case listener.DefaultPoolID == observed.pool.ID:
						keepEdge[port] = true
					case listener.DefaultPoolID == "":
						message := "Attaching the selected Octavia UDP pool"
						routeResult.Outcome = p.progressingOutcome(message)
						attach = append(attach, graphMutation{kind: graphSetListenerDefaultPool, listenerID: listener.ID, poolID: observed.pool.ID, message: message})
					default:
						routeResult.Outcome = p.progressingOutcome("Waiting for the previous UDP route to detach")
					}
				}
			}
			if routeResult.Outcome.State == cloud.OutcomeReady && !gatewayReady {
				routeResult.Outcome = result.Gateway.Outcome
			}
		}
		result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: node.Spec.Identity, Result: routeResult})
		if routeResult.Outcome.State == cloud.OutcomeProgressing {
			result.Outcome = routeResult.Outcome
		}
	}
	var failed *listeners.Listener
	for index := range actual.udpListeners {
		listener := &actual.udpListeners[index]
		obsolete := ensureGateway && !configured[listener.ProtocolPort]
		owner := defaultPoolOwner(listener, actual.routes)
		if owner != -1 {
			node := desired.spec.Routes[owner]
			withdraw := node.Disposition == cloud.GraphDelete ||
				node.Disposition == cloud.GraphEnsure && !keepEdge[listener.ProtocolPort] ||
				node.RetainUDPListenerPorts != nil && !slices.Contains(node.RetainUDPListenerPorts, listener.ProtocolPort)
			if obsolete || withdraw {
				step := clearListenerDefaultPool(cloud.ListenerProtocolUDP)
				step.listenerID = listener.ID
				detach = append(detach, step)
				result.Routes[owner].Result.Outcome = p.progressingOutcome("Waiting for the superseded UDP default to detach")
				continue
			}
		}
		switch {
		case obsolete:
			remove = append(remove, graphMutation{kind: graphDeleteChild, deletion: udpListenerDeletionStep(listener.ID), message: "Removing an obsolete Octavia UDP listener"})
		case listener.ProvisioningStatus == "ERROR":
			failed = listener
		case ensureGateway && !listener.AdminStateUp && (listener.DefaultPoolID == "" || readyByPool[listener.DefaultPoolID]):
			restore = append(restore, graphMutation{kind: graphEnableListener, listenerID: listener.ID, message: "Enabled Octavia UDP listener"})
		}
	}
	var plan []graphMutation
	switch {
	case len(detach) != 0:
		plan = detach
	case len(remove) != 0:
		plan = remove
	case failed != nil:
		// An ERROR frontend may still be forwarding to a pool whose detach
		// appears in the API. Wait for proof before modifying any backend.
		result.Routes = nil
		return nil, result, defaultPoolListenerProvisioningFailure(failed)
	default:
		if ensureGateway && actual.loadBalancer != nil && len(actual.udpListeners) != 0 &&
			!slices.ContainsFunc(actual.udpListeners, func(listener listeners.Listener) bool {
				return listener.DefaultPoolID != "" && !readyByPool[listener.DefaultPoolID]
			}) {
			address, err := buildGatewayAddressRestorationPlan(spec.ExternalNetworkID, actual)
			if err != nil {
				return nil, result, err
			}
			restore = append(restore, address...)
			if !actual.loadBalancer.AdminStateUp {
				restore = append(restore, graphMutation{kind: graphEnableLoadBalancer, message: "Enabled Octavia load balancer after validating the Gateway graph"})
			}
		}
		plan = append(prerequisites, cleanup...)
		plan = append(plan, backend...)
		plan = append(plan, attach...)
		plan = append(plan, restore...)
	}
	if len(plan) != 0 {
		result.Outcome = p.progressingOutcome(plan[0].message)
	}
	return plan, result, nil
}

func validateMultiUDPRouteGatewayIDs(recorded, observed cloud.GatewayState, managed []listeners.Listener) error {
	if recorded.ListenerID != "" && len(managed) != 0 && !slices.ContainsFunc(managed, func(listener listeners.Listener) bool {
		return listener.ID == recorded.ListenerID
	}) {
		return fmt.Errorf("%w: route Gateway listener ID is absent from the observed graph", cloud.ErrOwnershipConflict)
	}
	// A recorded listener may be any member of the proven Gateway set.
	recorded.ListenerID = ""
	return validateRouteGatewayIDs(recorded, observed)
}
