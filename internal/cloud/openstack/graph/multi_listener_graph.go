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
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

var scopedPortTagPrefixes = map[string]string{
	cloud.ListenerProtocolUDP:  openstackidentity.Tag("udp-port", ""),
	cloud.ListenerProtocolTCP:  openstackidentity.Tag("tcp-port", ""),
	cloud.ListenerProtocolHTTP: openstackidentity.Tag("http-port", ""),
}

const terminatedHTTPS = string(listeners.ProtocolTerminatedHTTPS)

var scopedHTTPSPortTagPrefix = openstackidentity.Tag("https-port", "")

func scopedPortTag(protocol string, port int) string {
	if protocol == terminatedHTTPS {
		return scopedHTTPSPortTagPrefix + openstackidentity.EncodeValue(strconv.Itoa(port))
	}
	return scopedPortTagPrefixes[protocol] + openstackidentity.EncodeValue(strconv.Itoa(port))
}

func scopedPortTagProtocol(tag string) string {
	if strings.HasPrefix(tag, scopedHTTPSPortTagPrefix) || tag == strings.TrimSuffix(scopedHTTPSPortTagPrefix, "=") {
		return cloud.ListenerProtocolHTTP
	}
	for protocol, prefix := range scopedPortTagPrefixes {
		if strings.HasPrefix(tag, prefix) || tag == strings.TrimSuffix(prefix, "=") {
			return protocol
		}
	}
	return ""
}

func scopedListenerProtocol(items []listeners.Listener) (string, error) {
	var protocol string
	for _, listener := range items {
		for _, tag := range listener.Tags {
			markedProtocol := scopedPortTagProtocol(tag)
			if markedProtocol == "" {
				continue
			}
			if protocol != "" && protocol != markedProtocol {
				return "", fmt.Errorf("%w: mixed scoped listener protocols", cloud.ErrOwnershipConflict)
			}
			protocol = markedProtocol
		}
	}
	return protocol, nil
}

func (p *Provider) managedScopedListeners(identity Identity, lb *octavia.LoadBalancer, items []listeners.Listener, protocol string, anchor int) ([]listeners.Listener, error) {
	// Octavia exposes both views. A list entry alone cannot establish that a
	// listener belongs to this graph, including after an uncertain create.
	if lb.Listeners == nil {
		return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("load balancer %s omitted its scoped listener inventory", lb.ID))
	}
	ports := make(map[int]bool, len(items))
	ids := make(map[string]bool, len(items))
	legacy := false
	for index := range items {
		listener := &items[index]
		port := listener.ProtocolPort
		protocolMatches := listener.Protocol == protocol || protocol == cloud.ListenerProtocolHTTP && listener.Protocol == terminatedHTTPS
		if listener.ProjectID != p.projectID || !identity.MatchesGateway(listener.Tags, roleListener) ||
			!protocolMatches || port < 1 || port > 65535 || ports[port] || ids[listener.ID] {
			return nil, fmt.Errorf("%w: invalid or duplicate scoped listener %s on load balancer %s", cloud.ErrOwnershipConflict, listener.ID, lb.ID)
		}
		if protocol == cloud.ListenerProtocolHTTP {
			if listener.ID == "" || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID {
				return nil, fmt.Errorf("%w: HTTP listener %s has an unexpected load balancer", cloud.ErrOwnershipConflict, listener.ID)
			}
		} else if err := validateDefaultPoolListener(listener, lb.ID); err != nil {
			return nil, err
		}
		if len(lb.VIPSecurityGroupIDs) > 0 && len(listener.AllowedCIDRs) > 0 {
			return nil, fmt.Errorf("%w: custom VIP security groups cannot be combined with listener allowed CIDRs", cloud.ErrOwnershipConflict)
		}
		ports[port], ids[listener.ID] = true, true

		marked := false
		for _, tag := range listener.Tags {
			if markedProtocol := scopedPortTagProtocol(tag); markedProtocol != "" {
				if markedProtocol != protocol || tag != scopedPortTag(listener.Protocol, port) || marked {
					return nil, fmt.Errorf("%w: scoped listener %s has inconsistent port tags", cloud.ErrOwnershipConflict, listener.ID)
				}
				marked = true
			}
		}
		if !marked {
			if legacy || port != anchor || listener.Protocol == terminatedHTTPS {
				return nil, fmt.Errorf("%w: scoped listener %s does not match the legacy binding", cloud.ErrOwnershipConflict, listener.ID)
			}
			legacy = true
		}
	}
	if len(lb.Listeners) != len(ids) {
		return nil, fmt.Errorf("%w: load balancer %s has duplicate scoped listener references", cloud.ErrOwnershipConflict, lb.ID)
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
func scopedRepresentativeListenerID(managed []listeners.Listener, anchor int) string {
	for _, listener := range managed {
		if listener.ProtocolPort == anchor || !slices.ContainsFunc(listener.Tags, func(tag string) bool { return scopedPortTagProtocol(tag) != "" }) {
			return listener.ID
		}
	}
	if len(managed) == 0 {
		return ""
	}
	return managed[0].ID
}

func (p *Provider) observeScopedListenerGraph(
	ctx context.Context,
	desired desiredGatewayGraph,
	actual observedGatewayGraph,
	owners []routeGraphOwner,
	items []listeners.Listener,
	parentPending bool,
) (observedGatewayGraph, error) {
	anchor := desired.spec.Gateway.Spec.ListenerPort
	managed, err := p.managedScopedListeners(desired.identity, actual.loadBalancer, items, actual.protocol, anchor)
	if err != nil {
		return actual, err
	}
	actual.scopedListeners = managed
	actual.state.ListenerID = scopedRepresentativeListenerID(managed, anchor)
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
	if actual.outcome.State == cloud.OutcomeProgressing &&
		(parentPending || !canReconcileTCPRejection(desired, actual) && !hasTCPWeightedReadback(actual)) {
		return actual, nil
	}
	if actual.loadBalancer.Pools == nil {
		return actual, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("load balancer %s omitted its scoped pool inventory", actual.loadBalancer.ID))
	}
	if actual.protocol == cloud.ListenerProtocolTCP {
		actual.routes, err = p.observeTCPRouteGraphs(ctx, &actual, owners)
	} else {
		actual.routes, err = p.observeRouteGraphsWithListeners(ctx, owners, actual.loadBalancer, nil, actual.protocol, managed)
	}
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
		actual.deletion = scopedListenerDeletionPlan(actual)
		if actual.deletion.defaultPoolPending != "" {
			actual.outcome = p.progressingOutcome(actual.deletion.defaultPoolPending)
		}
	}
	return actual, nil
}

func (p *Provider) validateScopedDefaultPool(pool pools.Pool, lbID string, managed []listeners.Listener, protocol string) error {
	if err := validateDefaultPoolParent(pool, lbID, protocol); err != nil {
		return err
	}
	if pool.ProjectID != p.projectID {
		return fmt.Errorf("%w: default pool %s has an unproven project", cloud.ErrOwnershipConflict, pool.ID)
	}
	if pool.Listeners == nil {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("pool %s omitted its scoped listener inventory", pool.ID))
	}
	byID := make(map[string]listeners.Listener, len(managed))
	for _, listener := range managed {
		byID[listener.ID] = listener
	}
	reverse := make(map[string]bool, len(pool.Listeners))
	disagree := func(listenerID string) error {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("listener %s and pool %s disagree about their default association", listenerID, pool.ID))
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

func scopedListenerDeletionPlan(actual observedGatewayGraph) gatewayDeletionPlan {
	var plan gatewayDeletionPlan
	for _, listener := range actual.scopedListeners {
		readback := actual.tcpDefaultTargets[listener.ID]
		if readback.Invalid || readback.PublicationStatus == "PENDING_UPDATE" {
			plan.defaultPoolPending = errTCPWeightedReadback.Error()
			return plan
		}
		if len(readback.Targets) != 0 {
			plan.tcpWeightedWithdraw = weightedTCPMutation(listener.ID, -1, nil, false)
			return plan
		}
		if weightedTCPPoolInventory(actual.routes) && !tcpWeightedDetachmentReadback(actual, listener) {
			plan.defaultPoolPending = errTCPWeightedReadback.Error()
			return plan
		}
		if listener.DefaultPoolID != "" {
			// The graph planner detaches known edges before processing this
			// subtree. One fresh, settled observation follows each detach.
			return plan
		}
	}
	for _, listener := range actual.scopedListeners {
		if listener.ProvisioningStatus == "ERROR" {
			// An empty default readback under ERROR does not prove that the
			// dataplane detached. Remove the failed frontend before its backend.
			plan.listeners = append(plan.listeners, listenerDeletionStep(listener.ID))
		}
	}
	if len(plan.listeners) != 0 {
		return plan
	}
	for _, route := range actual.routes {
		appendWeightedTCPDeletionSteps(&plan, route)
		for _, key := range slices.Sorted(maps.Keys(route.udpParts)) {
			part := route.udpParts[key]
			if issue := part.backendProvisioningIssue(true, true); issue != "" {
				plan.defaultPoolPending = issue
				return plan
			}
			plan.pools = appendDefaultPoolDeletionSteps(plan.pools, *part)
		}
		if route.pool == nil {
			continue
		}
		if issue := route.backendProvisioningIssue(true, true); issue != "" {
			plan.defaultPoolPending = issue
		}
		plan.pools = appendDefaultPoolDeletionSteps(plan.pools, route)
	}
	for _, listener := range actual.scopedListeners {
		plan.listeners = append(plan.listeners, listenerDeletionStep(listener.ID))
	}
	return plan
}

func listenerDeletionStep(id string) gatewayDeletionStep {
	return gatewayDeletionStep{resource: gatewayDeletionListener, resourceID: id}
}

func scopedGatewayResult(spec cloud.GatewaySpec, actual observedGatewayGraph, pending cloud.Outcome) cloud.GatewayResult {
	result := cloud.GatewayResult{State: actual.state, Outcome: pending, ScopedListenerReadyPorts: []int{}}
	if actual.loadBalancer == nil || !actual.loadBalancer.AdminStateUp || actual.scopedListeners == nil || actual.state.VIPAddress == "" {
		return result
	}
	if spec.ExternalNetworkID == "" && actual.floatingIP != nil ||
		spec.ExternalNetworkID != "" && actual.state.FloatingIPAddress == "" {
		return result
	}
	ready := make(map[int]bool, len(actual.scopedListeners))
	for _, listener := range actual.scopedListeners {
		ready[listener.ProtocolPort] = listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE"
	}
	for _, listener := range spec.ScopedListeners {
		if ready[listener.Port] {
			result.ScopedListenerReadyPorts = append(result.ScopedListenerReadyPorts, listener.Port)
		}
	}
	if len(result.ScopedListenerReadyPorts) == len(spec.ScopedListeners) && len(actual.scopedListeners) == len(spec.ScopedListeners) {
		result.Outcome = cloud.ReadyOutcome()
	}
	return result
}

// The shared parent can defer full route observation while a sibling listener
// is pending. Keep existing proof only for routes whose exact edges still
// reach an active, owned pool.
func (p *Provider) deferredScopedRouteObservations(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, readyPorts []int) map[int]bool {
	if len(readyPorts) == 0 || actual.loadBalancer.Pools == nil || len(desired.routes) == 0 {
		return nil
	}
	readyListeners := make(map[int]listeners.Listener, len(readyPorts))
	weighted := actual.protocol == cloud.ListenerProtocolTCP && hasTCPWeightedReadback(actual)
	pending := weighted && (actual.loadBalancer.ProvisioningStatus == "PENDING_UPDATE" || actual.loadBalancer.ProvisioningStatus == "ERROR")
	for _, listener := range actual.scopedListeners {
		switch {
		case strings.HasPrefix(listener.ProvisioningStatus, "PENDING_"), weighted && listener.ProvisioningStatus == "ERROR":
			pending = true
		case listener.ProvisioningStatus != "ACTIVE":
			return nil
		case slices.Contains(readyPorts, listener.ProtocolPort):
			readyListeners[listener.ProtocolPort] = listener
		}
	}
	if !pending || len(readyListeners) == 0 {
		return nil
	}
	inventory, err := p.listGatewayPoolInventory(ctx, actual.loadBalancer)
	if err != nil {
		return nil
	}
	poolsByID := make(map[string]pools.Pool, len(inventory.Items))
	for _, pool := range inventory.Items {
		poolsByID[pool.ID] = pool
	}
	completePools := len(actual.loadBalancer.Pools) == len(inventory.Items)
	deferred := make(map[int]bool)
	for index, route := range desired.routes {
		ports := route.spec.ScopedListenerPorts
		if desired.spec.Routes[index].Disposition != cloud.GraphEnsure || len(ports) == 0 {
			continue
		}
		if len(route.spec.TCPWeightedBackends) != 0 {
			deferred[index] = p.proveServingTCPWeightedRoute(ctx, desired, actual, route, readyListeners, inventory)
			continue
		}
		if route.spec.UDPPacketDrop != nil {
			deferred[index] = completePools && p.proveServingUDPDropRoute(ctx, desired, actual, route, readyListeners, inventory, poolsByID)
			continue
		}
		poolID := readyListeners[ports[0]].DefaultPoolID
		if poolID == "" || slices.ContainsFunc(ports, func(port int) bool {
			listener := readyListeners[port]
			return listener.DefaultPoolID != poolID || !actual.tcpDefaultPoolTLSMatches(listener.ID, route)
		}) {
			continue
		}
		deferred[index] = p.proveScopedDefaultPool(ctx, desired, actual, route, inventory, poolsByID[poolID])
	}
	return deferred
}

func (p *Provider) proveServingUDPDropRoute(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, route desiredRoute,
	readyListeners map[int]listeners.Listener, inventory octavia.PoolInventory, poolsByID map[string]pools.Pool,
) bool {
	proven := make(map[string]bool, len(route.spec.ScopedListenerPorts))
	for _, port := range route.spec.ScopedListenerPorts {
		listener := readyListeners[port]
		pool := poolsByID[listener.DefaultPoolID]
		role, ok := openstackidentity.ResourceRole(pool.Tags)
		if !ok || proven[pool.ID] {
			return false
		}
		part := route
		part.spec.ScopedListenerPorts = []int{port}
		if role != rolePool {
			part.backendKey = udpDropPartKey(port)
			if role != part.httpRole(rolePool, "") || !slices.Contains(pool.Tags, udpDropPoolTag) {
				return false
			}
		}
		if !p.proveScopedDefaultPool(ctx, desired, actual, part, inventory, pool) {
			return false
		}
		proven[pool.ID] = true
	}
	// An obsolete edge can still be serving while its route loses an election.
	// It must settle before a logical route can claim unchanged forwarding.
	if route.backendKey == "" {
		for _, pool := range inventory.Items {
			if proven[pool.ID] || len(pool.Listeners) == 0 && !slices.ContainsFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
				return listener.DefaultPoolID == pool.ID
			}) {
				continue
			}
			role, ok := readRouteResourceRole(pool.Tags, rolePool)
			if ok && (role.raw == rolePool || udpDropPartPort(role.backend) != 0) && route.identity.MatchesRoute(pool.Tags, role.raw) {
				return false
			}
		}
	}
	return true
}

func (p *Provider) proveScopedDefaultPool(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, route desiredRoute,
	inventory octavia.PoolInventory, pool pools.Pool,
) bool {
	if !pool.AdminStateUp || pool.ProvisioningStatus != "ACTIVE" || len(pool.Listeners) != len(route.spec.ScopedListenerPorts) ||
		!route.identity.MatchesRoute(pool.Tags, route.httpRole(rolePool, "")) ||
		p.validateScopedDefaultPool(pool, actual.loadBalancer.ID, actual.scopedListeners, actual.protocol) != nil {
		return false
	}
	observed := observedRouteGraph{
		pool: &pool, tcpRejection: inventory.TCPConnectionRejection[pool.ID], udpDrop: inventory.UDPPacketDrop[pool.ID],
		backendTLS: inventory.BackendTLS[pool.ID], backendH2C: inventory.BackendH2C[pool.ID],
	}
	if p.observeRoutePoolChildrenWithRole(ctx, routeGraphOwner{identity: route.identity}, &observed, route.backendKey) != nil ||
		observed.validateDefaultPoolChildren() != nil ||
		observed.udpDropPool() && (observed.monitor != nil || pool.Persistence != (pools.SessionPersistence{}) || p.validateUDPPoolCascade(observed) != nil) {
		return false
	}
	if actual.protocol == cloud.ListenerProtocolTCP && route.spec.BackendTLS != nil {
		route.tcpDefaultPoolTLS = p.observeTCPDefaultPoolTLS(ctx, route, observed, desired.tcpDefaultPoolTLSAvailable, desired.spec.Gateway.BackendCAOwned)
	}
	changes, _, err := buildRouteEnsurePlan(route, observed)
	return err == nil && scopedRouteBackendReady(route, observed, actual.state.VIPAddress, changes, observed.backendProvisioningIssue(false, true))
}

// Use the same convergence test when the whole graph is active and when a
// pending sibling permits a read-only observation of this route's backend.
func scopedRouteBackendReady(route desiredRoute, observed observedRouteGraph, vip string, changes []routeMutation, provisioningIssue string) bool {
	return observed.pool != nil && provisioningIssue == "" && len(changes) == 0 &&
		defaultPoolCanServe(route) &&
		(route.spec.GatewayRequirements.ListenerProtocol == cloud.ListenerProtocolTCP || route.dropsAllUDP() || hasActiveUDPBackend(route, observed, vip))
}

func (p *Provider) buildScopedListenerGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	partDesired, partActual, parts := udpDropPlanningParts(desired, actual)
	plan, result, err := p.buildScopedListenerPartPlan(partDesired, partActual)
	if len(parts) != 0 {
		combineUDPDropResults(&result, parts, len(desired.routes))
		for index := range plan {
			step := &plan[index]
			if step.kind == graphRouteMutation {
				step.backendKey = partDesired.routes[step.routeIndex].backendKey
				step.routeIndex = parts[step.routeIndex].owner
			}
		}
	}
	return plan, result, err
}

func (p *Provider) buildScopedListenerPartPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(actual.state)}
	ensureGateway := desired.spec.Gateway.Disposition == cloud.GraphEnsure
	spec := desired.spec.Gateway.Spec
	if ensureGateway {
		result.Gateway = scopedGatewayResult(spec, actual, p.progressingOutcome("Waiting for the Gateway listeners"))
		result.Outcome = result.Gateway.Outcome
	}
	gatewayReady := result.Gateway.Outcome.State == cloud.OutcomeReady
	prerequisites, err := buildGatewayPrerequisitePlan(desired, actual)
	if err != nil {
		return nil, result, err
	}
	found := make(map[int]listeners.Listener, len(actual.scopedListeners))
	configured := make(map[int]bool, len(spec.ScopedListeners))
	for _, listener := range actual.scopedListeners {
		found[listener.ProtocolPort] = listener
	}
	for _, listener := range spec.ScopedListeners {
		configured[listener.Port] = true
		if _, exists := found[listener.Port]; !exists && actual.loadBalancer != nil {
			prerequisites = append(prerequisites, graphMutation{kind: graphCreateListener, scopedListener: listener, message: "Creating Octavia listener"})
		}
	}

	var detach, remove, cleanup, backend, attach, restore []graphMutation
	keepEdge := make(map[int]bool, len(actual.scopedListeners))
	readyByPool := make(map[string]bool, len(actual.routes))
	for index, route := range desired.routes {
		observed := observedRouteGraph{}
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		node := desired.spec.Routes[index]
		routeResult := cloud.RouteReadyResult(routeStateFromObservation(observed))
		if len(route.spec.TCPWeightedBackends) != 0 || len(observed.tcpParts) != 0 {
			gateway := result.Gateway.Outcome
			if allTCPWeightedPortsReady(route, result.Gateway.ScopedListenerReadyPorts) {
				gateway = cloud.ReadyOutcome()
			}
			routeResult = p.weightedTCPRouteResult(route, observed, gateway)
			if weighted := route.tcpWeightedPlan; weighted.step != nil {
				backend = append(backend, *weighted.step)
			}
			result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: node.Spec.Identity, Result: routeResult})
			if routeResult.Outcome.State == cloud.OutcomeProgressing {
				result.Outcome = routeResult.Outcome
			}
			continue
		}
		switch node.Disposition {
		case cloud.GraphPreserve:
			if len(node.RetainListenerPorts) == 0 || observed.pool == nil {
				break
			}
			ready, err := retainedTransportBackendReady(route, observed, actual)
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			if ready && actual.loadBalancer.AdminStateUp {
				for _, port := range node.RetainListenerPorts {
					listener := found[port]
					keepEdge[port] = listener.DefaultPoolID == observed.pool.ID && listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE" && actual.tcpDefaultPoolTLSMatches(listener.ID, route)
				}
			}
		case cloud.GraphDelete:
			if observed.pool != nil && len(observed.pool.Listeners) != 0 {
				routeResult.Outcome = p.progressingOutcome("Waiting for listeners to detach before cleanup")
			} else if issue := observed.backendProvisioningIssue(true, true); issue != "" {
				routeResult.Outcome = p.progressingOutcome(issue)
			} else if steps := buildRouteDeletionPlan(observed); len(steps) != 0 {
				step := steps[0]
				routeResult.Outcome = p.progressingOutcome(step.message())
				cleanup = append(cleanup, graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, message: step.message()})
			}
		case cloud.GraphEnsure:
			if err := validateScopedRouteGatewayIDs(route.spec.Gateway, actual.state, actual.scopedListeners); err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			if pending := transportBatchProvisioningPending(route, observed); pending != "" {
				routeResult.Outcome = p.progressingOutcome(pending)
				for _, port := range route.spec.ScopedListenerPorts {
					keepEdge[port] = found[port].DefaultPoolID == observed.pool.ID
				}
				break
			}
			steps, state, err := buildRouteEnsurePlan(route, observed)
			if transportPoolPending(err) {
				routeResult.Outcome = p.progressingOutcome(err.Error())
				break
			}
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			routeResult.State = state
			issue := observed.backendProvisioningIssue(false, true)
			ready := scopedRouteBackendReady(route, observed, actual.state.VIPAddress, steps, issue)
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
				message := fmt.Sprintf("Waiting for a ready %s backend member", actual.protocol)
				if actual.protocol == cloud.ListenerProtocolUDP {
					message += " in the VIP IP family"
				}
				routeResult.Outcome = p.progressingOutcome(message)
			}
			if ready {
				for _, port := range route.spec.ScopedListenerPorts {
					listener, exists := found[port]
					switch {
					case !exists:
						routeResult.Outcome = p.progressingOutcome("Waiting for the selected listener")
					case listener.DefaultPoolID == observed.pool.ID && actual.tcpDefaultPoolTLSMatches(listener.ID, route):
						keepEdge[port] = true
					case listener.DefaultPoolID == "" && !actual.canAttachTCPDefaultPoolTLS(listener.ID, route):
						routeResult.Outcome = p.progressingOutcome("Waiting for Octavia TCP default pool TLS listener readback")
					case listener.DefaultPoolID == "":
						message := "Attaching the selected Octavia pool"
						routeResult.Outcome = p.progressingOutcome(message)
						attach = append(attach, graphMutation{kind: graphSetListenerDefaultPool, listenerID: listener.ID, poolID: observed.pool.ID, tcpDefaultPoolTLS: route.spec.BackendTLS != nil, message: message})
					default:
						routeResult.Outcome = p.progressingOutcome("Waiting for the previous route to detach")
					}
				}
			}
			if routeResult.Outcome.State == cloud.OutcomeReady && !gatewayReady &&
				(actual.protocol == cloud.ListenerProtocolUDP || slices.ContainsFunc(route.spec.ScopedListenerPorts, func(port int) bool {
					return !slices.Contains(result.Gateway.ScopedListenerReadyPorts, port)
				})) {
				routeResult.Outcome = result.Gateway.Outcome
			}
		}
		result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: node.Spec.Identity, Result: routeResult})
		if routeResult.Outcome.State == cloud.OutcomeProgressing {
			result.Outcome = routeResult.Outcome
		}
	}
	var failed *listeners.Listener
	for index := range actual.scopedListeners {
		listener := &actual.scopedListeners[index]
		obsolete := ensureGateway && !configured[listener.ProtocolPort]
		owner := defaultPoolOwner(listener, actual.routes)
		if owner != -1 {
			node := desired.spec.Routes[owner]
			withdraw := node.Disposition == cloud.GraphDelete ||
				node.Disposition == cloud.GraphEnsure && !keepEdge[listener.ProtocolPort] ||
				node.RetainListenerPorts != nil && !keepEdge[listener.ProtocolPort]
			if obsolete || withdraw {
				result.Routes[owner].Result.Outcome = p.progressingOutcome("Waiting for the superseded default to detach")
				if actual.routes[owner].udpDropPool() {
					// Guarded edges are withdrawn with their checked
					// pool subtree by the packet-drop reconciler.
					continue
				}
				step := clearListenerDefaultPool(actual.protocol)
				step.listenerID = listener.ID
				detach = append(detach, step)
				continue
			}
		}
		switch {
		case obsolete:
			remove = append(remove, graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(listener.ID), message: "Removing an obsolete Octavia listener"})
		case listener.ProvisioningStatus == "ERROR":
			failed = listener
		case ensureGateway && !listener.AdminStateUp && tcpWeightedListenerCanRestore(desired, actual, *listener) && (listener.DefaultPoolID == "" || readyByPool[listener.DefaultPoolID]):
			restore = append(restore, graphMutation{kind: graphEnableListener, listenerID: listener.ID, message: "Enabled Octavia listener"})
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
		if ensureGateway && actual.loadBalancer != nil && len(actual.scopedListeners) != 0 &&
			!slices.ContainsFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
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

func validateScopedRouteGatewayIDs(recorded, observed cloud.GatewayState, managed []listeners.Listener) error {
	if recorded.ListenerID != "" && len(managed) != 0 && !slices.ContainsFunc(managed, func(listener listeners.Listener) bool {
		return listener.ID == recorded.ListenerID
	}) {
		return fmt.Errorf("%w: route Gateway listener ID is absent from the observed graph", cloud.ErrOwnershipConflict)
	}
	// A recorded listener may be any member of the proven Gateway set.
	recorded.ListenerID = ""
	return validateRouteGatewayIDs(recorded, observed)
}
