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
	"net/http"
	"slices"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

// The tag also identifies pools staged before their first batch or observed
// through an API that no longer exposes the extension.
const udpDropPoolTag = "gateway-api-openstack.udp-packet-drop=v1"

var (
	errUDPPacketDropUnavailable = errors.New("waiting for regional Octavia UDP packet drop support")
	errUDPPacketDropUnverified  = errors.New("waiting to verify regional Octavia UDP packet drop support")
	errUDPPacketDropReadback    = errors.New("waiting for a dedicated Octavia UDP packet drop pool and complete readback")
)

func (r observedRouteGraph) udpDropPool() bool {
	return r.pool != nil && r.pool.Protocol == cloud.ListenerProtocolUDP &&
		(r.udpDrop.Present || slices.Contains(r.pool.Tags, udpDropPoolTag))
}

func (r desiredRoute) dropsAllUDP() bool {
	drop := r.spec.UDPPacketDrop
	return drop != nil && drop.DropsAll()
}

func (r desiredRoute) requiresUDPExtendedMemberWeights() bool {
	return r.spec.UDPPacketDrop != nil && slices.ContainsFunc(r.members, func(member cloud.Member) bool {
		return member.Weight > cloud.MaxOctaviaMemberWeight
	})
}

func (step routeMutation) hasExtendedMemberWeights() bool {
	return step.kind == routeMutationReplaceTransportMembers && slices.ContainsFunc(step.batchMembers, func(member pools.BatchUpdateMemberOpts) bool {
		return member.Weight != nil && *member.Weight > cloud.MaxOctaviaMemberWeight
	})
}

func hasUDPDropTarget(desired desiredGatewayGraph) bool {
	return slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return route.spec.UDPPacketDrop != nil })
}

func hasUDPDropReadback(observed observedGatewayGraph) bool {
	return slices.ContainsFunc(observed.routes, func(route observedRouteGraph) bool {
		return route.udpDropPool() || len(route.udpParts) != 0
	})
}

func (p *Provider) observeUDPPacketDropCapability(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if !hasUDPDropTarget(*desired) {
		return
	}
	var available bool
	var err error
	var extendedAvailable bool
	var extendedErr error
	if actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora" && udpIPFamily(actual.state.VIPAddress) == 4 {
		available, err = p.UDPPacketDropAvailable(ctx)
		if available && slices.ContainsFunc(desired.routes, desiredRoute.requiresUDPExtendedMemberWeights) {
			extendedAvailable, extendedErr = p.UDPExtendedMemberWeightsAvailable(ctx)
		}
	}
	for index := range desired.routes {
		route := &desired.routes[index]
		switch {
		case available && route.requiresUDPExtendedMemberWeights():
			route.udpDropUnverified = extendedErr != nil
			route.udpDropUnavailable = extendedErr == nil && !extendedAvailable
		case route.spec.UDPPacketDrop != nil:
			route.udpDropUnverified = err != nil
			route.udpDropUnavailable = err == nil && !available
		}
	}
}

func (p *Provider) verifyExtendedUDPBatch(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, route desiredRoute, batch routeMutation) error {
	if batch.udpDrop == nil || !route.requiresUDPExtendedMemberWeights() || !hasAmphoraUDPGraph(actual) || udpIPFamily(actual.state.VIPAddress) != 4 {
		return cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("extended UDP weights require a complete IPv4 Amphora discard batch"))
	}
	available, err := p.UDPExtendedMemberWeightsAvailable(ctx)
	if err != nil || !available {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.Join(errors.New("waiting for regional Octavia extended UDP member weights"), err))
	}
	// Discovery is a remote read. Recheck the sealed authorization after it,
	// immediately before sending the complete batch.
	if validate := desired.spec.ValidateMutation; validate != nil {
		return validate(ctx)
	}
	return nil
}

func hasAmphoraUDPGraph(actual observedGatewayGraph) bool {
	return actual.protocol == cloud.ListenerProtocolUDP && actual.loadBalancer != nil && actual.loadBalancer.Provider == "amphora"
}

// Legacy singleton route deletion may carry the protocol only on the
// detaching route. The live listener still has to prove it is UDP.
func pendingUDPGraph(desired desiredGatewayGraph, protocol string) bool {
	return protocol == cloud.ListenerProtocolUDP || protocol == "" && desired.spec.Gateway.Disposition == cloud.GraphPreserve &&
		slices.ContainsFunc(desired.spec.Routes, func(node cloud.RouteGraphNode) bool {
			return node.Disposition == cloud.GraphDelete && node.Spec.GatewayRequirements.ListenerProtocol == cloud.ListenerProtocolUDP
		})
}

func (p *Provider) observePendingUDPRoutes(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) error {
	routes, err := p.observeRouteGraphsWithListeners(ctx, desired.routeOwners(), actual.loadBalancer,
		actual.listener, actual.protocol, actual.scopedListeners)
	if err != nil {
		return err
	}
	for _, listener := range actual.scopedListeners {
		if listener.DefaultPoolID != "" && defaultPoolOwner(&listener, routes) == -1 {
			return unprovenDefaultPool(&listener)
		}
	}
	actual.routes = routes
	return nil
}

// Octavia can replace a lost UDP batch or remove its pool when the same pool
// owns the pending operation. The provider checks this under its render lock.
func (p *Provider) reconcileUDPPacketDrop(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, bool, error) {
	if !hasAmphoraUDPGraph(actual) {
		return cloud.GatewayGraphResult{}, false, nil
	}
	pending := actual.loadBalancer.ProvisioningStatus == "PENDING_UPDATE"
	if !pending && (actual.loadBalancer.ProvisioningStatus != "ACTIVE" || desired.spec.Gateway.Disposition == cloud.GraphDelete) {
		return cloud.GatewayGraphResult{}, false, nil
	}
	if len(actual.routes) == 0 && (pending || actual.scoped && actual.outcome.State == cloud.OutcomeProgressing) {
		if err := p.observePendingUDPRoutes(ctx, desired, &actual); err != nil {
			return cloud.GatewayGraphResult{}, false, nil
		}
	}
	partDesired, partActual, parts := udpDropPlanningParts(desired, actual)
	var submission udpDropBatch
	result, handled, err := p.reconcileUDPPacketDropParts(ctx, partDesired, partActual, parts, &submission)
	combineUDPDropResults(&result, parts, len(desired.routes))
	if submission.mutation.id != "" {
		index := submission.index
		if len(parts) != 0 {
			index = parts[index].owner
		}
		err = p.submitTransportBatch(ctx, desired, actual, index, submission.mutation, &result)
	}
	return result, handled, err
}

type udpDropBatch struct {
	mutation routeMutation
	index    int
}

func (p *Provider) reconcileUDPPacketDropParts(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, parts []udpDropPlanningPart, submission *udpDropBatch) (cloud.GatewayGraphResult, bool, error) {
	pending := actual.loadBalancer.ProvisioningStatus == "PENDING_UPDATE"
	indices := make([]int, len(actual.routes))
	priorities := make([]int, len(actual.routes))
	for index, observed := range actual.routes {
		indices[index] = index
		route, node := desired.routes[index], desired.spec.Routes[index]
		switch {
		case observed.pool == nil:
			priorities[index] = 2
		case node.Disposition == cloud.GraphDelete || route.udpDropUnavailable ||
			route.spec.UDPPacketDrop != nil && staleUDPDestinations(route, observed) ||
			observed.udpDropPool() && route.spec.UDPPacketDrop == nil:
			priorities[index] = 0
		case len(observed.pool.Listeners) == 0:
			priorities[index] = 2
		default:
			priorities[index] = 1
		}
	}
	slices.SortStableFunc(indices, func(a, b int) int { return priorities[a] - priorities[b] })
	waitingIndex := -1
	unsettledDetach := -1
	for _, index := range indices {
		if unsettledDetach >= 0 && priorities[index] != 0 {
			break
		}
		observed := actual.routes[index]
		if observed.pool == nil {
			continue
		}
		route, node := desired.routes[index], desired.spec.Routes[index]
		managed := observed.udpDropPool()
		wantDrop := desired.spec.Gateway.Disposition == cloud.GraphEnsure && node.Disposition == cloud.GraphEnsure && route.spec.UDPPacketDrop != nil
		retainDrop := node.Disposition == cloud.GraphPreserve && len(node.RetainListenerPorts) != 0 && route.spec.UDPPacketDrop != nil
		if desired.spec.Gateway.Disposition == cloud.GraphPreserve && node.Disposition != cloud.GraphDelete {
			continue
		}
		poolStatus := observed.pool.ProvisioningStatus
		pendingBatch := pending && managed && poolStatus == "PENDING_UPDATE"
		pendingDelete := pending && managed && poolStatus == "PENDING_DELETE"
		migration := udpNoMigration
		if len(parts) != 0 {
			migration = parts[index].migration
		}
		if !managed && node.RestrictUDPPacketDrop {
			// An uncheckpointed target cannot enroll a stock default.
			continue
		}
		if !managed && !wantDrop && migration == udpNoMigration || pending && !pendingBatch && !pendingDelete {
			continue
		}
		owned := servingPoolListeners(actual, observed.pool.ID)
		if err := validateTransportRouteBinding(route, actual); err != nil {
			return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
		}
		failed := poolStatus == "ERROR" ||
			slices.ContainsFunc(owned, func(listener listeners.Listener) bool { return listener.ProvisioningStatus == "ERROR" }) ||
			slices.ContainsFunc(observed.members, func(member pools.Member) bool { return member.ProvisioningStatus == "ERROR" })
		if !pending && !failed && (observed.backendProvisioningIssue(true, true) != "" ||
			slices.ContainsFunc(owned, func(listener listeners.Listener) bool { return listener.ProvisioningStatus != "ACTIVE" })) {
			continue
		}

		restrict := node.RestrictUDPPacketDrop && len(owned) == 1 && slices.Contains(node.RetainListenerPorts, owned[0].ProtocolPort)
		if retainDrop && !restrict && !pending && !failed && actual.addressReadError == nil && actual.loadBalancer.AdminStateUp &&
			len(owned) == 1 && owned[0].AdminStateUp && slices.Contains(node.RetainListenerPorts, owned[0].ProtocolPort) {
			ready, err := retainedTransportBackendReady(route, observed, actual)
			if err != nil {
				return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			if ready {
				err := p.validateUDPPacketDropSecurity(ctx, desired, route, owned[0].ProtocolPort)
				if err == nil {
					continue
				}
				return p.handleUDPPacketDropSecurityError(ctx, desired, actual, index, err)
			}
		}

		superseded := supersededDefaultPoolListener(route, owned, actual.scoped)
		switch {
		case pendingDelete || !wantDrop && !restrict || route.udpDropUnavailable || superseded != "",
			route.udpDropUnverified && staleUDPDestinations(route, observed):
			unprovenDetach := migration != udpNoMigration && slices.ContainsFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
				return listener.DefaultPoolID != observed.pool.ID && listener.ProvisioningStatus != "ACTIVE"
			})
			clearSharedDefault := migration == udpDetachSharedStockPool && len(owned) != 0
			if unprovenDetach {
				// A failed detach can already report no default. The
				// regional pool delete renders only its current listeners.
				clearSharedDefault = len(owned) != 0 && (staleUDPDestinations(route, observed) || superseded != "")
				if !clearSharedDefault {
					unsettledDetach = index
					continue
				}
			}
			if clearSharedDefault {
				if err := p.validateUDPPoolCascade(observed); err != nil {
					return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
				}
				step := clearListenerDefaultPool(actual.protocol)
				step.listenerID = superseded
				if step.listenerID == "" {
					step.listenerID = owned[0].ID
				}
				actual.outcome = p.progressingOutcome(step.message)
				result := p.pendingTransportBatchResult(ctx, desired, actual, index)
				err := p.executeUDPPacketDropMutation(ctx, desired, actual, index, step, &result)
				return result, true, err
			}
			return p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing the Octavia UDP packet drop pool")
		case route.udpDropUnverified:
			waitingIndex = index
			continue
		}
		invalidPool := !managed || observed.monitor != nil || observed.pool.Persistence != (pools.SessionPersistence{}) ||
			!observed.pool.AdminStateUp || observed.pool.LBMethod != string(pools.LBMethodRoundRobin)
		if invalidPool || len(owned) > 1 || observed.udpDrop.Invalid ||
			len(owned) == 0 && (observed.udpDrop.Present || len(observed.members) != 0) {
			return p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Replacing an incompatible Octavia UDP packet drop pool")
		}
		if len(owned) == 0 {
			if result, handled, err := p.attachEmptyUDPPacketDropPool(ctx, desired, actual, index); handled {
				return result, true, err
			}
			continue
		}
		batch, _, changed, readErr := desiredTransportBatch(route, observed, pendingBatch || failed)
		if readErr != nil || actual.addressReadError != nil && (changed || failed) {
			return p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing an unverified Octavia UDP default")
		}
		if restrict {
			retained := retainedUDPDestinations(route, observed)
			if !staleUDPDestinations(route, observed) && !failed && (!pendingBatch || changed) {
				// An existing, more restrictive target can wait for its
				// route checkpoint without admitting any new member.
				route.spec.Members = retained
				if err := p.validateUDPPacketDropSecurity(ctx, desired, route, owned[0].ProtocolPort); err != nil {
					return p.handleUDPPacketDropSecurityError(ctx, desired, actual, index, err)
				}
				continue
			}
			if len(retained) != len(route.members) ||
				udpDropShareGreater(observed.udpDrop.Value, *route.spec.UDPPacketDrop) {
				return p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing an Octavia UDP default that cannot restrict its authorized target")
			}
		}
		if !pendingBatch && !changed && !failed {
			continue
		}
		if err := p.validateUDPPacketDropSecurity(ctx, desired, route, owned[0].ProtocolPort); err != nil {
			return p.handleUDPPacketDropSecurityError(ctx, desired, actual, index, err)
		}
		if pendingBatch && !changed {
			delay := p.transportReplayDelay(observed.pool.ID, observed.udpDrop.UpdatedAt, time.Now())
			actual.outcome = cloud.ProgressingOutcome("Waiting for Octavia UDP batch activation", max(delay, transportReplayInterval, p.pollInterval))
			if delay != 0 {
				return p.waitForUDPPacketDrop(ctx, desired, actual, index)
			}
		} else {
			actual.outcome = p.progressingOutcome(batch.message())
		}
		*submission = udpDropBatch{mutation: batch, index: index}
		return p.pendingTransportBatchResult(ctx, desired, actual, index), true, nil
	}
	if unsettledDetach >= 0 {
		actual.outcome = p.progressingOutcome("Waiting to confirm Octavia detached the former UDP default")
		return p.waitForUDPPacketDrop(ctx, desired, actual, unsettledDetach)
	}
	// An active scoped Gateway can still reconcile its other listeners. The
	// scoped planner holds each unverified route at its existing attachment.
	if waitingIndex >= 0 && (pending || !actual.scoped || actual.outcome.State == cloud.OutcomeProgressing) {
		actual.outcome = p.progressingOutcome(errUDPPacketDropUnverified.Error())
		return p.waitForUDPPacketDrop(ctx, desired, actual, waitingIndex)
	}
	return cloud.GatewayGraphResult{}, false, nil
}

func retainedUDPDestinations(route desiredRoute, observed observedRouteGraph) []cloud.Member {
	active := make(map[string]string, len(observed.members))
	for _, member := range observed.members {
		if member.AdminStateUp && member.Weight > 0 && member.ProvisioningStatus != "PENDING_DELETE" {
			active[memberKey(member.Address, member.ProtocolPort)] = member.SubnetID
		}
	}
	return slices.DeleteFunc(slices.Clone(route.members), func(member cloud.Member) bool {
		subnet, found := active[memberKey(member.Address, member.Port)]
		return !found || subnet != route.memberSubnet(member)
	})
}

func udpDropShareGreater(a, b cloud.UDPPacketDrop) bool {
	return int64(a.Numerator)*int64(b.Denominator) > int64(b.Numerator)*int64(a.Denominator)
}

func staleUDPDestinations(route desiredRoute, observed observedRouteGraph) bool {
	wanted := route.spec.UDPPacketDrop
	current := observed.udpDrop.Value
	if !current.Valid() {
		current = cloud.UDPPacketDrop{Denominator: 1}
	}
	if len(observed.members) != 0 && udpDropShareGreater(*wanted, current) {
		return true
	}
	ready := make(map[string]string, len(route.members))
	for _, member := range route.members {
		if !member.Draining {
			ready[memberKey(member.Address, member.Port)] = route.memberSubnet(member)
		}
	}
	return slices.ContainsFunc(observed.members, func(member pools.Member) bool {
		subnet, wanted := ready[memberKey(member.Address, member.ProtocolPort)]
		_, unreported := observed.memberBatchUnreported[member.ID]
		return (!wanted || member.SubnetID != subnet) && (unreported || member.AdminStateUp && member.Weight > 0)
	})
}

func (p *Provider) handleUDPPacketDropSecurityError(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, err error) (cloud.GatewayGraphResult, bool, error) {
	route, observed := desired.routes[index], actual.routes[index]
	category, _ := cloud.ErrorCategoryOf(classifyOpenStackError(err))
	temporary := category == cloud.ErrorCategoryTimeout || category == cloud.ErrorCategoryRateLimit || category == cloud.ErrorCategoryRetryableService
	if route.spec.MemberMode == cloud.MemberModeNodePort && temporary && !staleUDPDestinations(route, observed) {
		actual.outcome = p.progressingOutcome("Waiting to verify OpenStack UDP NodePort security")
		return p.waitForUDPPacketDrop(ctx, desired, actual, index)
	}
	result, handled, deleteErr := p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing the Octavia UDP default after security validation failed")
	return result, handled, errors.Join(err, deleteErr)
}

func (p *Provider) waitForUDPPacketDrop(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int) (cloud.GatewayGraphResult, bool, error) {
	result := p.pendingTransportBatchResult(ctx, desired, actual, index)
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, true, err
		}
	}
	return result, true, nil
}

func (p *Provider) withdrawUDPPacketDrop(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, message string) (cloud.GatewayGraphResult, bool, error) {
	actual.outcome = p.progressingOutcome(message)
	result := p.pendingTransportBatchResult(ctx, desired, actual, index)
	err := p.deleteUDPPacketDropPool(ctx, desired, actual, index, actual.routes[index], &result)
	return result, true, err
}

// An empty default must be attached before the first batch. Only validate
// the selected listener and route because this operation cannot open a peer.
func (p *Provider) attachEmptyUDPPacketDropPool(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int) (cloud.GatewayGraphResult, bool, error) {
	route := desired.routes[index]
	pool := actual.routes[index].pool
	listener := udpDropListener(route, actual)
	if actual.addressReadError != nil || listener.ID == "" || listener.DefaultPoolID != "" ||
		listener.ProvisioningStatus != "ACTIVE" || pool.ProvisioningStatus != "ACTIVE" ||
		len(pool.Listeners) != 0 || len(pool.Members) != 0 || pool.Members == nil {
		return cloud.GatewayGraphResult{}, false, nil
	}
	if err := p.validateUDPPacketDropSecurity(ctx, desired, route, listener.ProtocolPort); err != nil {
		return cloud.GatewayGraphResult{}, true, err
	}
	step := graphMutation{kind: graphSetListenerDefaultPool, listenerID: listener.ID, poolID: pool.ID, message: "Attaching an empty Octavia UDP packet drop pool"}
	actual.outcome = p.progressingOutcome(step.message)
	result := p.pendingTransportBatchResult(ctx, desired, actual, index)
	err := p.executeUDPPacketDropMutation(ctx, desired, actual, index, step, &result)
	return result, true, err
}

func (p *Provider) validateUDPPacketDropSecurity(ctx context.Context, desired desiredGatewayGraph, route desiredRoute, port int) error {
	gateway := desired.spec.Gateway.Spec
	gateway.ListenerPort, gateway.ScopedListeners = port, nil
	return p.topology.ValidateGatewaySecurity(ctx, gateway, []cloud.RouteSpec{route.spec})
}

func udpDropListener(route desiredRoute, actual observedGatewayGraph) listeners.Listener {
	if !actual.scoped && actual.listener != nil {
		return *actual.listener
	}
	ports := route.spec.ScopedListenerPorts
	if len(ports) == 1 {
		for _, listener := range actual.scopedListeners {
			if listener.ProtocolPort == ports[0] {
				return listener
			}
		}
	}
	return listeners.Listener{}
}

// Octavia deletes the member subtree along with the pool. Require exact
// project ownership and a complete reverse list before any cascade.
func (p *Provider) validateUDPPoolCascade(route observedRouteGraph) error {
	if route.pool.ProjectID != p.projectID ||
		slices.ContainsFunc(route.members, func(member pools.Member) bool { return member.ProjectID != p.projectID }) ||
		route.monitor != nil && route.monitor.ProjectID != p.projectID {
		return fmt.Errorf("%w: UDP pool %s or a child has an unproven project", cloud.ErrOwnershipConflict, route.pool.ID)
	}
	return crossCheckCompleteChildInventory("pool", route.pool.ID, "member", route.pool.Members, route.members,
		func(member pools.Member) string { return member.ID })
}

func (p *Provider) deleteUDPPacketDropPool(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, observed observedRouteGraph, result *cloud.GatewayGraphResult) error {
	if err := p.validateUDPPoolCascade(observed); err != nil {
		return &cloud.GraphError{Identity: desired.routes[index].spec.Identity, Err: err}
	}
	step := graphMutation{kind: graphRouteMutation, routeIndex: index,
		route: routeMutation{kind: routeMutationDeletePool, id: observed.pool.ID}, message: "Deleting an Octavia UDP pool and its default edge"}
	return p.executeUDPPacketDropMutation(ctx, desired, actual, index, step, result)
}

func (p *Provider) executeUDPPacketDropMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, step graphMutation, result *cloud.GatewayGraphResult) error {
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return err
		}
	}
	if err := p.executeGraphMutation(ctx, desired, actual, step); err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
			return nil
		}
		return &cloud.GraphError{Identity: desired.routes[index].spec.Identity, Err: err}
	}
	result.Outcome.MutationSubmitted = true
	result.Routes[index].Result.Outcome.MutationSubmitted = true
	return nil
}
