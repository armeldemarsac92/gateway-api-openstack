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
	available := false
	if actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora" && udpIPFamily(actual.state.VIPAddress) == 4 {
		available, _ = p.UDPPacketDropAvailable(ctx)
	}
	for index := range desired.routes {
		route := &desired.routes[index]
		route.udpDropUnavailable = route.spec.UDPPacketDrop != nil && !available
	}
}

func canReconcileUDPPacketDrop(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	return actual.protocol == cloud.ListenerProtocolUDP && desired.spec.Gateway.Disposition != cloud.GraphDelete &&
		actual.loadBalancer != nil && actual.loadBalancer.Provider == "amphora"
}

// Update a serving pool atomically. A new guarded pool must first serve as
// an empty default, because Octavia derives the packet selector from its listener.
func (p *Provider) reconcileUDPPacketDrop(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, bool, error) {
	if !canReconcileUDPPacketDrop(desired, actual) || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return cloud.GatewayGraphResult{}, false, nil
	}
	for index, observed := range actual.routes {
		if observed.pool == nil {
			continue
		}
		route, node := desired.routes[index], desired.spec.Routes[index]
		managed := observed.udpDropPool()
		wantDrop := desired.spec.Gateway.Disposition == cloud.GraphEnsure && node.Disposition == cloud.GraphEnsure && route.spec.UDPPacketDrop != nil
		if desired.spec.Gateway.Disposition == cloud.GraphPreserve && node.Disposition != cloud.GraphDelete {
			continue
		}
		if !managed && !wantDrop || observed.backendProvisioningIssue(true, true) != "" {
			continue
		}
		if err := validateTransportRouteBinding(route, actual); err != nil {
			return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
		}
		owned := servingPoolListeners(actual, observed.pool.ID)
		if slices.ContainsFunc(owned, func(listener listeners.Listener) bool {
			return listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR"
		}) {
			continue
		}
		// Withdrawing a managed default requires DELETE pool. A listener
		// update with an empty default is refused by the UDP extension.
		invalidPool := !managed || observed.monitor != nil || observed.pool.Persistence != (pools.SessionPersistence{}) ||
			!observed.pool.AdminStateUp || observed.pool.LBMethod != string(pools.LBMethodRoundRobin)
		switch {
		case !wantDrop || route.udpDropUnavailable || invalidPool || len(owned) > 1 ||
			supersededDefaultPoolListener(route, owned, actual.scoped) != "" || observed.udpDrop.Invalid ||
			len(owned) == 0 && (observed.udpDrop.Present || len(observed.members) != 0):
			actual.outcome = p.progressingOutcome("Withdrawing the Octavia UDP packet drop pool")
			result := p.pendingTransportBatchResult(ctx, desired, actual, index)
			err := p.deleteUDPPacketDropPool(ctx, desired, actual, index, &result)
			return result, true, err
		case len(owned) == 0:
			listener := udpDropListener(route, actual)
			if actual.addressReadError != nil || listener.ID == "" || listener.DefaultPoolID != "" ||
				listener.ProvisioningStatus != "ACTIVE" || observed.pool.ProvisioningStatus != "ACTIVE" ||
				len(observed.pool.Listeners) != 0 || len(observed.pool.Members) != 0 || observed.pool.Members == nil {
				continue
			}
			if err := p.validateGraphTrafficSecurity(ctx, desired); err != nil {
				return cloud.GatewayGraphResult{}, true, err
			}
			step := graphMutation{kind: graphSetListenerDefaultPool, listenerID: listener.ID, poolID: observed.pool.ID, message: "Attaching an empty Octavia UDP packet drop pool"}
			actual.outcome = p.progressingOutcome(step.message)
			result := p.pendingTransportBatchResult(ctx, desired, actual, index)
			err := p.executeUDPPacketDropMutation(ctx, desired, actual, index, step, &result)
			return result, true, err
		}
		batch, _, changed, readErr := desiredTransportBatch(route, observed, false)
		failed := observed.backendProvisioningIssue(false, true) != "" || owned[0].ProvisioningStatus == "ERROR"
		if readErr != nil || failed || actual.addressReadError != nil && changed {
			actual.outcome = p.progressingOutcome("Withdrawing an unverified Octavia UDP default")
			result := p.pendingTransportBatchResult(ctx, desired, actual, index)
			err := p.deleteUDPPacketDropPool(ctx, desired, actual, index, &result)
			return result, true, err
		}
		if !changed {
			continue
		}
		if err := p.topology.ValidateGatewaySecurity(ctx, desired.spec.Gateway.Spec, []cloud.RouteSpec{route.spec}); err != nil {
			actual.outcome = p.progressingOutcome("Withdrawing the Octavia UDP default after security validation failed")
			result := p.pendingTransportBatchResult(ctx, desired, actual, index)
			deleteErr := p.deleteUDPPacketDropPool(ctx, desired, actual, index, &result)
			return result, true, errors.Join(err, deleteErr)
		}
		actual.outcome = p.progressingOutcome(batch.message())
		result := p.pendingTransportBatchResult(ctx, desired, actual, index)
		err := p.submitTransportBatch(ctx, desired, actual, index, batch, &result)
		return result, true, err
	}
	return cloud.GatewayGraphResult{}, false, nil
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
