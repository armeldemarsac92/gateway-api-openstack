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

func hasUDPDropTarget(desired desiredGatewayGraph) bool {
	return slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return route.spec.UDPPacketDrop != nil })
}

func hasUDPDropReadback(observed observedGatewayGraph) bool {
	return slices.ContainsFunc(observed.routes, observedRouteGraph.udpDropPool)
}

func (p *Provider) observeUDPPacketDropCapability(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if !hasUDPDropTarget(*desired) {
		return
	}
	var available bool
	var err error
	if actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora" && udpIPFamily(actual.state.VIPAddress) == 4 {
		available, err = p.UDPPacketDropAvailable(ctx)
	}
	for index := range desired.routes {
		route := &desired.routes[index]
		if route.spec.UDPPacketDrop != nil {
			route.udpDropUnverified = err != nil
			route.udpDropUnavailable = err == nil && !available
		}
	}
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
	actual.routes = routes
	return err
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
	waitingIndex := -1
	for index, observed := range actual.routes {
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
		if !managed && !wantDrop || pending && !pendingBatch && !pendingDelete {
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

		if retainDrop && !pending && !failed && actual.addressReadError == nil && actual.loadBalancer.AdminStateUp &&
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
				result, handled, deleteErr := p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing the Octavia UDP default after security validation failed")
				return result, handled, errors.Join(err, deleteErr)
			}
		}

		switch {
		case pendingDelete || !wantDrop || route.udpDropUnavailable || supersededDefaultPoolListener(route, owned, actual.scoped) != "",
			route.udpDropUnverified && staleUDPDestinations(route, observed):
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
		if !pendingBatch && !changed && !failed {
			continue
		}
		if err := p.validateUDPPacketDropSecurity(ctx, desired, route, owned[0].ProtocolPort); err != nil {
			result, handled, deleteErr := p.withdrawUDPPacketDrop(ctx, desired, actual, index, "Withdrawing the Octavia UDP default after security validation failed")
			return result, handled, errors.Join(err, deleteErr)
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
		result := p.pendingTransportBatchResult(ctx, desired, actual, index)
		err := p.submitTransportBatch(ctx, desired, actual, index, batch, &result)
		return result, true, err
	}
	// An active scoped Gateway can still reconcile its other listeners. The
	// scoped planner holds each unverified route at its existing attachment.
	if waitingIndex >= 0 && (pending || !actual.scoped || actual.outcome.State == cloud.OutcomeProgressing) {
		actual.outcome = p.progressingOutcome(errUDPPacketDropUnverified.Error())
		return p.waitForUDPPacketDrop(ctx, desired, actual, waitingIndex)
	}
	return cloud.GatewayGraphResult{}, false, nil
}

func staleUDPDestinations(route desiredRoute, observed observedRouteGraph) bool {
	wanted := route.spec.UDPPacketDrop
	current := observed.udpDrop.Value
	if !current.Valid() {
		current = cloud.UDPPacketDrop{Denominator: 1}
	}
	if len(observed.members) != 0 && int64(wanted.Numerator)*int64(current.Denominator) > int64(current.Numerator)*int64(wanted.Denominator) {
		return true
	}
	ready := make(map[string]struct{}, len(route.members))
	for _, member := range route.members {
		if !member.Draining {
			ready[memberKey(member.Address, member.Port)] = struct{}{}
		}
	}
	return slices.ContainsFunc(observed.members, func(member pools.Member) bool {
		_, wanted := ready[memberKey(member.Address, member.ProtocolPort)]
		_, unreported := observed.memberBatchUnreported[member.ID]
		return !wanted && (unreported || member.AdminStateUp && member.Weight > 0)
	})
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
	err := p.deleteUDPPacketDropPool(ctx, desired, actual, index, &result)
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

func (p *Provider) deleteUDPPacketDropPool(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, result *cloud.GatewayGraphResult) error {
	observed := actual.routes[index]
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
