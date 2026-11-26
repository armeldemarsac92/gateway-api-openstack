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
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func usesDefaultPool(protocol string) bool {
	return protocol == cloud.ListenerProtocolTCP || protocol == cloud.ListenerProtocolUDP
}

func udpMembersInVIPFamily(members []cloud.Member, vipAddress string) []cloud.Member {
	family := udpIPFamily(vipAddress)
	if family == 0 {
		return nil
	}
	// Amphora's IPVS renderer omits real servers of the other address family.
	// Plan from the members it can actually forward to.
	eligible := make([]cloud.Member, 0, len(members))
	for _, member := range members {
		if udpIPFamily(member.Address) == family {
			eligible = append(eligible, member)
		}
	}
	return eligible
}

func udpIPFamily(address string) int {
	ip, err := netip.ParseAddr(address)
	if err != nil || ip.Is4In6() || ip.Zone() != "" || ip.IsUnspecified() {
		return 0
	}
	if ip.Is4() {
		return 4
	}
	return 6
}

func validateDefaultPoolListener(listener *listeners.Listener, loadBalancerID string) error {
	if listener.ID == "" || len(listener.L7Policies) != 0 || len(listener.Loadbalancers) != 1 ||
		listener.Loadbalancers[0].ID != loadBalancerID {
		return fmt.Errorf("%w: %s listener %s has an unexpected identity, load balancer or L7 policies", cloud.ErrOwnershipConflict, listener.Protocol, listener.ID)
	}
	return nil
}

func validateDefaultPool(pool pools.Pool, loadBalancerID string, listener *listeners.Listener, protocol string) error {
	if err := validateDefaultPoolParent(pool, loadBalancerID, protocol); err != nil {
		return err
	}
	for _, parent := range pool.Listeners {
		if listener == nil || listener.Protocol != protocol || len(pool.Listeners) != 1 || parent.ID != listener.ID || listener.DefaultPoolID != pool.ID {
			return fmt.Errorf("%w: %s pool %s has an unexpected listener relationship", cloud.ErrOwnershipConflict, protocol, pool.ID)
		}
	}
	if listener != nil && listener.DefaultPoolID == pool.ID && len(pool.Listeners) == 0 {
		if listener.Protocol != protocol {
			return fmt.Errorf("%w: listener %s references a %s pool", cloud.ErrOwnershipConflict, listener.ID, protocol)
		}
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("waiting for %s pool %s to report its listener %s", protocol, pool.ID, listener.ID))
	}
	return nil
}

func validateDefaultPoolParent(pool pools.Pool, loadBalancerID, protocol string) error {
	if pool.TLSEnabled && (protocol != cloud.ListenerProtocolTCP || !slices.Contains(pool.Tags, tcpDefaultPoolTLSTag)) {
		return fmt.Errorf("%w: %s pool %s has unexpected TLS settings", cloud.ErrOwnershipConflict, protocol, pool.ID)
	}
	return validateBoundPoolParent(pool, loadBalancerID, protocol)
}

func validateBoundPoolParent(pool pools.Pool, loadBalancerID, protocol string) error {
	if pool.ID == "" || pool.Protocol != protocol ||
		len(pool.Loadbalancers) != 1 || pool.Loadbalancers[0].ID != loadBalancerID {
		return fmt.Errorf("%w: %s pool %s has an unexpected protocol, identity or load balancer", cloud.ErrOwnershipConflict, protocol, pool.ID)
	}
	return nil
}

func (route observedRouteGraph) validateDefaultPoolChildren() error {
	if route.pool.MonitorID != "" && (route.monitor == nil || route.monitor.ID != route.pool.MonitorID) {
		return fmt.Errorf("%w: pool %s references unproven monitor %s", cloud.ErrOwnershipConflict, route.pool.ID, route.pool.MonitorID)
	}
	members := make(map[string]bool, len(route.members))
	for _, member := range route.members {
		if member.ID == "" || members[member.ID] {
			return fmt.Errorf("%w: pool %s has a duplicate or missing member ID", cloud.ErrOwnershipConflict, route.pool.ID)
		}
		members[member.ID] = true
	}
	return crossCheckChildInventory("pool", route.pool.ID, "member", route.pool.Members, route.members, func(member pools.Member) string { return member.ID })
}

func defaultPoolOwner(listener *listeners.Listener, routes []observedRouteGraph) int {
	return slices.IndexFunc(routes, func(route observedRouteGraph) bool {
		return route.pool != nil && route.pool.ID == listener.DefaultPoolID
	})
}

func unprovenDefaultPool(listener *listeners.Listener) error {
	return fmt.Errorf("%w: Gateway listener %s references an unproven default pool %s", cloud.ErrOwnershipConflict, listener.ID, listener.DefaultPoolID)
}

func clearListenerDefaultPool(protocol string) graphMutation {
	return graphMutation{kind: graphClearListenerDefaultPool, message: fmt.Sprintf("Detaching the Octavia %s listener default pool", protocol)}
}

func (p *Provider) defaultPoolListenerOutcome(listener *listeners.Listener, allowRecovery bool) (cloud.Outcome, error) {
	switch {
	case listener.ProvisioningStatus == "ACTIVE" || allowRecovery && listener.ProvisioningStatus == "ERROR":
		return cloud.ReadyOutcome(), nil
	case strings.HasPrefix(listener.ProvisioningStatus, "PENDING_"):
		return p.progressingOutcome(fmt.Sprintf("Waiting for Octavia %s listener %s to be ACTIVE (observed %q)", listener.Protocol, listener.ID, listener.ProvisioningStatus)), nil
	default:
		return cloud.Outcome{}, defaultPoolListenerProvisioningFailure(listener)
	}
}

func defaultPoolListenerProvisioningFailure(listener *listeners.Listener) error {
	return cloud.NewProviderError(cloud.ErrorCategoryResourceFailure,
		fmt.Errorf("octavia %s listener %s provisioning status is %q", listener.Protocol, listener.ID, listener.ProvisioningStatus))
}

func (p *Provider) buildDefaultPoolGatewayDeletionPlan(ctx context.Context, owners []routeGraphOwner, lb *octavia.LoadBalancer, listener *listeners.Listener, protocol string, inventory octavia.ListenerInventory) (gatewayDeletionPlan, error) {
	plan := gatewayDeletionPlan{defaultPoolListener: listener}
	if listener != nil {
		if err := validateDefaultPoolListener(listener, lb.ID); err != nil {
			return gatewayDeletionPlan{}, err
		}
		outcome, err := p.defaultPoolListenerOutcome(listener, true)
		if err != nil {
			return gatewayDeletionPlan{}, err
		}
		if outcome.State == cloud.OutcomeProgressing {
			plan.defaultPoolPending = outcome.Message
			return plan, nil
		}
		plan.listeners = append(plan.listeners, gatewayDeletionStep{resource: gatewayDeletionListener, resourceID: listener.ID})
	}
	var routes []observedRouteGraph
	var err error
	actual := observedGatewayGraph{protocol: protocol, loadBalancer: lb, listener: listener,
		tcpDefaultTargets: inventory.TCPDefaultTargets, tcpDefaultPoolTLS: inventory.TCPDefaultPoolTLS,
		defaultPoolsObserved: inventory.DefaultPoolsObserved}
	if protocol == cloud.ListenerProtocolTCP {
		routes, err = p.observeTCPRouteGraphs(ctx, &actual, owners)
	} else {
		routes, err = p.observeRouteGraphs(ctx, owners, lb, listener, protocol)
	}
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	if listener != nil && listener.DefaultPoolID != "" && defaultPoolOwner(listener, routes) == -1 {
		return gatewayDeletionPlan{}, unprovenDefaultPool(listener)
	}
	if listener != nil {
		readback := actual.tcpDefaultTargets[listener.ID]
		if readback.Invalid || readback.PublicationStatus == "PENDING_UPDATE" {
			plan.defaultPoolPending = errTCPWeightedReadback.Error()
			return plan, nil
		}
		if len(readback.Targets) != 0 {
			plan.tcpWeightedWithdraw = weightedTCPMutation(listener.ID, -1, nil, false)
			return plan, nil
		}
		if weightedTCPPoolInventory(routes) && !tcpWeightedDetachmentReadback(actual, *listener) {
			plan.defaultPoolPending = errTCPWeightedReadback.Error()
			return plan, nil
		}
	}
	// With an ERROR listener, an empty readback does not prove that Amphora
	// applied the detach. Gateway deletion can remove the owned listener first.
	if listener != nil && listener.ProvisioningStatus == "ERROR" {
		if owner := defaultPoolOwner(listener, routes); owner != -1 && routes[owner].udpDropPool() {
			plan.defaultUDPPacketDropPool = listener.DefaultPoolID
		}
		return plan, nil
	}
	for _, route := range routes {
		appendWeightedTCPDeletionSteps(&plan, route)
		if route.pool == nil {
			continue
		}
		if route.udpDropPool() && listener != nil && listener.DefaultPoolID == route.pool.ID {
			plan.defaultUDPPacketDropPool = route.pool.ID
		}
		if issue := route.backendProvisioningIssue(true, true); issue != "" && (listener == nil || listener.DefaultPoolID == "") {
			plan.defaultPoolPending = issue
		}
		plan.pools = appendDefaultPoolDeletionSteps(plan.pools, route)
	}
	return plan, nil
}

func appendDefaultPoolDeletionSteps(steps gatewayPoolDeletionPlan, route observedRouteGraph) gatewayPoolDeletionPlan {
	poolID := route.pool.ID
	if route.udpDropPool() {
		return append(steps, gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: poolID})
	}
	if route.monitor != nil {
		steps = append(steps, gatewayDeletionStep{resource: gatewayDeletionMonitor, resourceID: route.monitor.ID, parentID: poolID})
	}
	for _, member := range route.members {
		steps = append(steps, gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: member.ID, parentID: poolID})
	}
	return append(steps, gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: poolID})
}

// A TCP or UDP listener forwards through one pointer. Detach before changing its
// serving pool or tearing down a route, then attach only an observed, complete
// backend. A Preserve fragment provides ownership proof for the detach and
// grants no authority to change the pool.
func (p *Provider) buildDefaultPoolGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
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
		node := desired.spec.Routes[index]
		if len(route.spec.TCPWeightedBackends) != 0 || len(observed.tcpParts) != 0 {
			routeResult = p.weightedTCPRouteResult(route, observed, result.Gateway.Outcome)
			if weighted := route.tcpWeightedPlan; weighted.step != nil {
				backend = append(backend, *weighted.step)
			}
			if index == defaultOwner && len(route.spec.TCPWeightedBackends) != 0 {
				detach = true
			}
			routeResults = append(routeResults, cloud.RouteGraphResult{Identity: route.spec.Identity, Result: routeResult})
			if routeResult.Outcome.State == cloud.OutcomeProgressing {
				result.Outcome = routeResult.Outcome
			}
			continue
		}
		switch node.Disposition {
		case cloud.GraphPreserve:
			if index != defaultOwner || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
				break
			}
			if !slices.Contains(node.RetainListenerPorts, actual.listener.ProtocolPort) ||
				actual.listener.ProvisioningStatus != "ACTIVE" || !actual.listener.AdminStateUp || !actual.loadBalancer.AdminStateUp {
				detach = true
				break
			}
			ready, err := retainedTransportBackendReady(route, observed, actual)
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			detach = !ready || !actual.tcpDefaultPoolTLSMatches(actual.listener.ID, route)
			// An existing edge permits no Gateway traffic restoration while its
			// route checkpoint is pending.
			restoration = nil
		case cloud.GraphDelete:
			if index == defaultOwner {
				detach = true
				routeResult.Outcome = p.progressingOutcome(fmt.Sprintf("Waiting for the %s listener to detach before cleanup", actual.protocol))
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
			if pending := transportBatchProvisioningPending(route, observed); pending != "" {
				routeResult.Outcome = p.progressingOutcome(pending)
				break
			}
			canServe := defaultPoolCanServe(route)
			if actual.protocol == cloud.ListenerProtocolUDP && index == defaultOwner {
				if !canServe || !route.dropsAllUDP() && !hasActiveUDPBackend(route, observed, actual.state.VIPAddress) {
					// Revoke the proven forwarding pointer even if another
					// backend difference prevents planning further changes.
					detach = true
					routeResult.Outcome = p.progressingOutcome("Waiting for a ready UDP backend member in the VIP IP family")
					break
				}
			}
			steps, state, err := buildRouteEnsurePlan(route, observed)
			if transportPoolPending(err) {
				detach = detach || index == defaultOwner
				routeResult.Outcome = p.progressingOutcome(err.Error())
				break
			}
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			routeResult.State = state
			issue := observed.backendProvisioningIssue(false, true)
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
				message := fmt.Sprintf("Waiting for a ready %s backend member", actual.protocol)
				if actual.protocol == cloud.ListenerProtocolUDP {
					message += " in the VIP IP family"
				}
				routeResult.Outcome = p.progressingOutcome(message)
			case index != defaultOwner && !actual.canAttachTCPDefaultPoolTLS(actual.listener.ID, route):
				routeResult.Outcome = p.progressingOutcome("Waiting for Octavia TCP default pool TLS listener readback")
			case index != defaultOwner:
				message := fmt.Sprintf("Attaching the selected Octavia %s pool", actual.protocol)
				routeResult.Outcome = p.progressingOutcome(message)
				attach = append(attach, graphMutation{kind: graphSetListenerDefaultPool, poolID: observed.pool.ID, tcpDefaultPoolTLS: route.spec.BackendTLS != nil, message: message})
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
		mutation := clearListenerDefaultPool(actual.protocol)
		result.Outcome = p.progressingOutcome(mutation.message)
		return []graphMutation{mutation}, result, nil
	}
	if listenerFailed {
		// A route-only operation can revoke its verified default. An ERROR
		// listener with no default may still be forwarding, so hold its children
		// until a subsequent read shows ACTIVE or Gateway deletion removes it.
		result.Routes = nil
		return nil, result, defaultPoolListenerProvisioningFailure(actual.listener)
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

func retainedTransportBackendReady(route desiredRoute, observed observedRouteGraph, actual observedGatewayGraph) (bool, error) {
	if observed.pool == nil {
		return false, nil
	}
	if err := validateTransportRouteBinding(route, actual); err != nil {
		return false, err
	}
	changes, _, err := buildRouteEnsurePlan(route, observed)
	if transportPoolPending(err) {
		return false, nil
	}
	return err == nil && scopedRouteBackendReady(route, observed, actual.state.VIPAddress, changes,
		observed.backendProvisioningIssue(false, true)), err
}

func validateRouteGatewayIDs(recorded, observed cloud.GatewayState) error {
	if recorded.LoadBalancerID != "" && observed.LoadBalancerID != "" && recorded.LoadBalancerID != observed.LoadBalancerID ||
		recorded.ListenerID != "" && observed.ListenerID != "" && recorded.ListenerID != observed.ListenerID {
		return fmt.Errorf("%w: route Gateway IDs differ from the observed graph", cloud.ErrOwnershipConflict)
	}
	return nil
}

func transportPoolPending(err error) bool {
	return errors.Is(err, errTCPDefaultPoolTLS) || errors.Is(err, errWeightedTCPBackupUnknown) || errors.Is(err, errTCPRejectionUnavailable) ||
		errors.Is(err, errTCPRejectionReadback) || errors.Is(err, errUDPPacketDropReadback) ||
		errors.Is(err, errUDPPacketDropUnavailable) || errors.Is(err, errUDPPacketDropUnverified)
}

func hasActiveUDPBackend(route desiredRoute, observed observedRouteGraph, vipAddress string) bool {
	family := udpIPFamily(vipAddress)
	return slices.ContainsFunc(observed.members, func(member pools.Member) bool {
		return family != 0 && udpIPFamily(member.Address) == family &&
			member.AdminStateUp && member.ProvisioningStatus == "ACTIVE" &&
			(route.spec.MemberMode != cloud.MemberModeEndpoint || member.Weight > 0)
	})
}
