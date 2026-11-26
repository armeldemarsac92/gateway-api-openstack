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
	"slices"
	"strings"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func (actual observedGatewayGraph) tcpListeners() []listeners.Listener {
	if actual.scoped {
		return actual.scopedListeners
	}
	if actual.listener == nil {
		return nil
	}
	return []listeners.Listener{*actual.listener}
}

func hasTCPWeightedTarget(desired desiredGatewayGraph) bool {
	return slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return len(route.spec.TCPWeightedBackends) != 0 })
}

func hasTCPWeightedReadback(actual observedGatewayGraph) bool {
	for _, readback := range actual.tcpDefaultTargets {
		if readback.Invalid || len(readback.Targets) != 0 || readback.PublicationStatus != "" {
			return true
		}
	}
	return weightedTCPPoolInventory(actual.routes)
}

func weightedTCPPoolInventory(routes []observedRouteGraph) bool {
	return slices.ContainsFunc(routes, func(route observedRouteGraph) bool { return len(route.tcpParts) != 0 })
}

func (p *Provider) observeTCPRouteGraphs(ctx context.Context, actual *observedGatewayGraph, owners []routeGraphOwner) ([]observedRouteGraph, error) {
	lb := actual.loadBalancer
	inventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return nil, err
	}
	if hasTCPWeightedReadback(*actual) || slices.ContainsFunc(inventory.Items, func(pool pools.Pool) bool {
		role, ok := readRouteResourceRole(pool.Tags, rolePool)
		return ok && strings.HasPrefix(role.backend, tcpWeightedPartPrefix)
	}) {
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, actual.tcpListeners(), func(listener listeners.Listener) string { return listener.ID }); err != nil {
			return nil, err
		}
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, inventory.Items, func(pool pools.Pool) string { return pool.ID }); err != nil {
			return nil, err
		}
	}
	routes, err := p.observeRouteGraphsFromPools(ctx, owners, lb, actual.listener, actual.protocol, actual.scopedListeners,
		inventory, nil, nil, actual.tcpDefaultTargets)
	if err == nil && weightedTCPPoolInventory(routes) {
		p.confirmOmittedTCPWeightedTargets(ctx, actual)
	}
	return routes, err
}

// An older version of the extension may omit the empty list after regional
// opt-out. Both inventories have been cross-checked, including explicit pool
// reverse links. Confirm opt-out through the regional API and require a settled
// listener and parent before using the omission as empty readback.
func (p *Provider) confirmOmittedTCPWeightedTargets(ctx context.Context, actual *observedGatewayGraph) {
	if !actual.defaultPoolsObserved || actual.loadBalancer.Provider != "amphora" || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return
	}
	var omitted []string
	for _, listener := range actual.tcpListeners() {
		readback := actual.tcpDefaultTargets[listener.ID]
		if !readback.Present && !readback.Invalid && readback.PublicationStatus == "" && listener.DefaultPoolID == "" &&
			listener.ProvisioningStatus == "ACTIVE" && p.eligibleTCPWeightedListener(listener) && !actual.tcpDefaultPoolTLS[listener.ID] {
			omitted = append(omitted, listener.ID)
		}
	}
	if len(omitted) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	advertised, err := p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityTCPWeightedDefaultPools)
	if err != nil || advertised {
		return
	}
	actual.tcpWeightedOmittedEmpty = make(map[string]bool, len(omitted))
	for _, id := range omitted {
		actual.tcpWeightedOmittedEmpty[id] = true
	}
}

func tcpWeightedDetachmentReadback(actual observedGatewayGraph, listener listeners.Listener) bool {
	readback := actual.tcpDefaultTargets[listener.ID]
	if !actual.defaultPoolsObserved || readback.Invalid || listener.DefaultPoolID == "" && actual.tcpDefaultPoolTLS[listener.ID] ||
		readback.PublicationStatus != "" && readback.PublicationStatus != "ACTIVE" {
		return false
	}
	// The scalar association has already been ownership-checked against its
	// reverse links. Its presence excludes weighted selection on this listener.
	return readback.Present || listener.DefaultPoolID != "" || actual.tcpWeightedOmittedEmpty[listener.ID]
}

func (p *Provider) validateTCPWeightedPool(pool pools.Pool, lbID string, owned []listeners.Listener, readback map[string]octavia.TCPDefaultTargetObservation, recovery bool) error {
	if err := validateDefaultPoolParent(pool, lbID, cloud.ListenerProtocolTCP); err != nil {
		return err
	}
	if pool.ProjectID != p.projectID || slices.ContainsFunc(owned, func(listener listeners.Listener) bool {
		return listener.DefaultPoolID == pool.ID
	}) {
		return fmt.Errorf("%w: weighted TCP pool %s has an unexpected project or scalar default association", cloud.ErrOwnershipConflict, pool.ID)
	}
	if pool.Listeners == nil {
		return errTCPWeightedReadback
	}
	byID := make(map[string]listeners.Listener, len(owned))
	for _, listener := range owned {
		byID[listener.ID] = listener
	}
	reverse := make(map[string]bool, len(pool.Listeners))
	for _, ref := range pool.Listeners {
		listener, found := byID[ref.ID]
		if !found || reverse[ref.ID] || listener.Protocol != cloud.ListenerProtocolTCP || listener.ProjectID != p.projectID {
			return fmt.Errorf("%w: unproven or duplicate listener %s on weighted TCP pool %s", cloud.ErrOwnershipConflict, ref.ID, pool.ID)
		}
		reverse[ref.ID] = true
	}
	for _, listener := range owned {
		targets := readback[listener.ID]
		forward := slices.ContainsFunc(targets.Targets, func(target octavia.TCPDefaultTarget) bool { return target.PoolID == pool.ID })
		if forward == reverse[listener.ID] {
			continue
		}
		// Recovery can still see a published edge that has been removed
		// from desired state. The API retains it until publication settles.
		if recovery && !forward && targets.Present && !targets.Invalid && listener.DefaultPoolID == "" &&
			(targets.PublicationStatus == "ERROR" || targets.PublicationStatus == "PENDING_UPDATE") {
			continue
		}
		return errTCPWeightedReadback
	}
	return nil
}

func validateTCPWeightedTargetOwnership(frontends []listeners.Listener, readback map[string]octavia.TCPDefaultTargetObservation, owners map[string]poolRouteOwner) error {
	for _, listener := range frontends {
		targets := readback[listener.ID].Targets
		if len(targets) == 0 {
			continue
		}
		if listener.DefaultPoolID != "" || listener.Protocol != cloud.ListenerProtocolTCP {
			return fmt.Errorf("%w: listener %s has competing TCP forwarding selectors", cloud.ErrOwnershipConflict, listener.ID)
		}
		routeIndex := -1
		for _, target := range targets {
			if target.Reject {
				continue
			}
			owner, found := owners[target.PoolID]
			if !found || !strings.HasPrefix(owner.backend, tcpWeightedPartPrefix) || routeIndex != -1 && routeIndex != owner.route {
				return fmt.Errorf("%w: listener %s has an unproven weighted TCP pool %s", cloud.ErrOwnershipConflict, listener.ID, target.PoolID)
			}
			routeIndex = owner.route
		}
	}
	return nil
}

// Failed parents and unrelated child reads may block staging. A restrictive
// update requires proven ownership. Verify each pool's live profile and members
// before retaining its share in degraded mode.
func (p *Provider) observeFailedTCPWeightedWithdrawal(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) (*graphMutation, error) {
	lb := actual.loadBalancer
	if lb == nil || lb.Provider != "amphora" || lb.ProvisioningStatus != "ACTIVE" && lb.ProvisioningStatus != "ERROR" ||
		actual.protocol != "" && actual.protocol != cloud.ListenerProtocolTCP || hasSNIProfile(lb.Tags) ||
		desired.spec.Gateway.Disposition != cloud.GraphEnsure && desired.spec.Gateway.Disposition != cloud.GraphDelete &&
			!slices.ContainsFunc(desired.spec.Routes, func(node cloud.RouteGraphNode) bool {
				return node.Disposition == cloud.GraphDelete && node.Spec.GatewayRequirements.ListenerProtocol == cloud.ListenerProtocolTCP
			}) {
		return nil, nil
	}
	inventory, err := p.observeGatewayListenerInventory(ctx, desired, actual)
	if err != nil {
		return nil, err
	}
	if err := validateRawTLSDefault(inventory); err != nil {
		return nil, err
	}
	if !hasTCPWeightedReadback(*actual) && !hasTCPWeightedTarget(desired) {
		return nil, nil
	}
	if !inventory.DefaultPoolsObserved {
		return nil, errTCPWeightedReadback
	}
	anchor := desired.spec.Gateway.Spec.ListenerPort
	if actual.scoped {
		actual.scopedListeners, err = p.managedScopedListeners(desired.identity, lb, inventory.Items, cloud.ListenerProtocolTCP, anchor)
	} else {
		actual.listener, err = p.managedGatewayListener(desired.identity, lb.ID, inventory.Items)
	}
	if err != nil {
		return nil, err
	}
	owned := actual.tcpListeners()
	actual.state.ListenerID = scopedRepresentativeListenerID(owned, anchor)
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, owned, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return nil, err
	}
	for _, listener := range owned {
		if !p.eligibleTCPWeightedListener(listener) {
			return nil, fmt.Errorf("%w: weighted TCP listener %s has an unproven project or protocol", cloud.ErrOwnershipConflict, listener.ID)
		}
		if err := validateDefaultPoolListener(&listener, lb.ID); err != nil {
			return nil, err
		}
		if actual.tcpDefaultTargets[listener.ID].Invalid && weightedTCPNeedsAdminWithdrawal(*actual, listener) {
			return &graphMutation{kind: graphDisableListener, listenerID: listener.ID, routeIndex: selectedTCPWeightedRoute(desired, *actual, listener.ProtocolPort),
				scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort}, message: "Closing the listener until its TCP default targets can be verified"}, nil
		}
	}
	poolInventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return nil, err
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, poolInventory.Items, func(pool pools.Pool) string { return pool.ID }); err != nil {
		return nil, err
	}
	routeGraphs := make([]observedRouteGraph, len(desired.routes))
	owners := make(map[string]poolRouteOwner)
	for _, pool := range poolInventory.Items {
		role, ok := readRouteResourceRole(pool.Tags, rolePool)
		if !ok || !strings.HasPrefix(role.backend, tcpWeightedPartPrefix) {
			continue
		}
		index := slices.IndexFunc(desired.routes, func(route desiredRoute) bool { return route.identity.MatchesRoute(pool.Tags, role.raw) })
		if index == -1 {
			return nil, fmt.Errorf("%w: unowned weighted TCP pool %s", cloud.ErrOwnershipConflict, pool.ID)
		}
		if err := p.validateTCPWeightedPool(pool, lb.ID, owned, actual.tcpDefaultTargets, true); err != nil {
			if errors.Is(err, errTCPWeightedReadback) {
				for _, listener := range owned {
					readback := actual.tcpDefaultTargets[listener.ID]
					if (!readback.Present || readback.Invalid) && weightedTCPNeedsAdminWithdrawal(*actual, listener) &&
						slices.ContainsFunc(pool.Listeners, func(ref pools.ListenerID) bool { return ref.ID == listener.ID }) {
						return &graphMutation{kind: graphDisableListener, listenerID: listener.ID, routeIndex: index,
							scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort}, message: "Closing the listener until its TCP default targets can be verified"}, nil
					}
				}
			}
			return nil, err
		}
		part := routeGraphs[index].part(role.backend)
		if part.pool != nil {
			return nil, fmt.Errorf("%w: duplicate weighted TCP pool role", cloud.ErrOwnershipConflict)
		}
		part.pool, part.backendTLS, part.backendH2C = &pool, poolInventory.BackendTLS[pool.ID], poolInventory.BackendH2C[pool.ID]
		part.tcpRejection = poolInventory.TCPConnectionRejection[pool.ID]
		owners[pool.ID] = poolRouteOwner{route: index, backend: role.backend}
	}
	if err := validateTCPWeightedTargetOwnership(owned, actual.tcpDefaultTargets, owners); err != nil {
		return nil, err
	}
	available := false
	if hasTCPWeightedTarget(desired) {
		available, _ = p.TCPWeightedDefaultPoolsAvailable(ctx)
	}
	view := *actual
	view.routes = routeGraphs
	plans := make([]map[string]tcpWeightedPoolPlan, len(desired.routes))
	for index, route := range desired.routes {
		plans[index] = make(map[string]tcpWeightedPoolPlan)
		for _, part := range route.tcpParts {
			observed := routeGraphs[index].tcpParts[part.backendKey]
			if observed == nil || len(observed.pool.Listeners) == 0 {
				continue
			}
			if p.observeRoutePoolChildrenWithRole(ctx, routeGraphOwner{identity: route.identity}, observed, part.backendKey) != nil ||
				observed.validateCompletePoolChildren() != nil {
				continue
			}
			plans[index][part.backendKey] = p.observeTCPWeightedPart(ctx, desired, part, *observed, available)
		}
	}
	for _, listener := range owned {
		if listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" ||
			listener.DefaultPoolID != "" {
			continue
		}
		if step := p.withdrawTCPWeightedTargets(desired, view, listener, plans, available); step != nil {
			return step, nil
		}
	}
	return nil, nil
}

func (p *Provider) proveServingTCPWeightedRoute(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph,
	route desiredRoute, readyListeners map[int]listeners.Listener, inventory octavia.PoolInventory,
) bool {
	var targets []octavia.TCPDefaultTarget
	for _, port := range route.spec.ScopedListenerPorts {
		listener, ready := readyListeners[port]
		readback := actual.tcpDefaultTargets[listener.ID]
		if !ready || !readback.Present || !tcpWeightedDetachmentReadback(actual, listener) || len(readback.Targets) != len(route.spec.TCPWeightedBackends) ||
			listener.DefaultPoolID != "" || actual.tcpDefaultPoolTLS[listener.ID] || targets != nil && !slices.Equal(targets, readback.Targets) {
			return false
		}
		targets = readback.Targets
	}
	available, err := p.TCPWeightedDefaultPoolsAvailable(ctx)
	if err != nil || !available {
		return false
	}
	for i, target := range targets {
		want := route.spec.TCPWeightedBackends[i]
		if target.Weight != want.Weight || target.Reject != (want.Key == "") {
			return false
		}
		if target.Reject {
			continue
		}
		poolIndex := slices.IndexFunc(inventory.Items, func(pool pools.Pool) bool { return pool.ID == target.PoolID })
		if poolIndex == -1 {
			return false
		}
		pool := &inventory.Items[poolIndex]
		key := tcpWeightedPartPrefix + want.Key
		if !route.identity.MatchesRoute(pool.Tags, tcpWeightedResourceRole(rolePool, key)) ||
			len(pool.Listeners) != len(route.spec.ScopedListenerPorts) ||
			p.validateTCPWeightedPool(*pool, actual.loadBalancer.ID, actual.scopedListeners, actual.tcpDefaultTargets, false) != nil {
			return false
		}
		observed := observedRouteGraph{pool: pool, backendTLS: inventory.BackendTLS[pool.ID], backendH2C: inventory.BackendH2C[pool.ID],
			tcpRejection: inventory.TCPConnectionRejection[pool.ID]}
		if p.observeRoutePoolChildrenWithRole(ctx, routeGraphOwner{identity: route.identity}, &observed, key) != nil ||
			observed.validateCompletePoolChildren() != nil {
			return false
		}
		index := slices.IndexFunc(route.tcpParts, func(part desiredRoute) bool { return part.backendKey == key })
		if index == -1 || p.observeTCPWeightedPart(ctx, desired, route.tcpParts[index], observed, available).id != pool.ID {
			return false
		}
	}
	return true
}

func (p *Provider) eligibleTCPWeightedListener(listener listeners.Listener) bool {
	return listener.ProjectID == p.projectID && listener.Protocol == cloud.ListenerProtocolTCP &&
		listener.DefaultTlsContainerRef == "" && len(listener.SniContainerRefs) == 0 && len(listener.L7Policies) == 0
}

func weightedTCPNeedsAdminWithdrawal(actual observedGatewayGraph, listener listeners.Listener) bool {
	return listener.ProvisioningStatus == "ERROR" || listener.ProvisioningStatus == "ACTIVE" &&
		(listener.AdminStateUp || !actual.adminObserved[listener.ID])
}
