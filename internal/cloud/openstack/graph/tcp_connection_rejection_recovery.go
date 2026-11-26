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
	"net/http"
	"slices"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const transportReplayInterval = 15 * time.Second

func canReconcileTCPRejection(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	return desired.protocol == cloud.ListenerProtocolTCP && desired.spec.Gateway.Disposition == cloud.GraphEnsure &&
		actual.loadBalancer != nil && actual.loadBalancer.Provider == "amphora"
}

// Update or withdraw a proven serving default before planning unrelated
// resources. The regular graph still stages pools that have no default edge.
func (p *Provider) reconcileServingTCPRejection(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, bool, error) {
	if !canReconcileTCPRejection(desired, actual) || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return cloud.GatewayGraphResult{}, false, nil
	}
	for index, observed := range actual.routes {
		if observed.pool == nil || desired.spec.Routes[index].Disposition != cloud.GraphEnsure {
			continue
		}
		route := desired.routes[index]
		if route.spec.BackendTLS != nil || observed.managedTCPDefaultPoolTLS() {
			continue
		}
		owned := servingPoolListeners(actual, observed.pool.ID)
		failed := observed.tcpRejection.Present && observed.pool.ProvisioningStatus == "ERROR" &&
			(tcpRejectionBatchRequired(route, observed) || slices.ContainsFunc(owned, func(listener listeners.Listener) bool { return listener.ProvisioningStatus == "ERROR" }))
		if !failed && (len(owned) == 0 || !tcpRejectionBatchRequired(route, observed)) {
			continue
		}
		if err := validateTransportRouteBinding(route, actual); err != nil {
			return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
		}
		if !failed && transportBatchProvisioningPending(route, observed) != "" {
			continue
		}
		step, _, changed, readErr := desiredTransportBatch(route, observed, false)
		if readErr == nil && !changed && !failed && !route.tcpRejectionUnavailable {
			continue
		}
		actual.outcome = p.progressingOutcome(step.message())
		if readErr != nil || route.tcpRejectionUnavailable {
			actual.outcome = p.progressingOutcome("Withdrawing an unverified Octavia TCP default")
		}
		result := p.pendingTransportBatchResult(ctx, desired, actual, index)
		if readErr != nil || route.tcpRejectionUnavailable {
			if len(owned) == 0 {
				continue
			}
			err := p.withdrawTCPRejectionDefault(ctx, desired, actual, index, owned[0].ID, &result)
			return result, true, err
		}
		if listener := supersededDefaultPoolListener(route, owned, actual.scoped); listener != "" {
			err := p.withdrawTCPRejectionDefault(ctx, desired, actual, index, listener, &result)
			return result, true, err
		}
		if len(owned) == 0 && !failed {
			continue
		}
		ready := observed.pool.AdminStateUp && observed.pool.LBMethod == string(pools.LBMethodRoundRobin) &&
			observed.pool.Persistence == (pools.SessionPersistence{}) && (failed || observed.backendProvisioningIssue(false, true) == "") &&
			!slices.ContainsFunc(owned, func(listener listeners.Listener) bool {
				return listener.ProvisioningStatus != "ACTIVE" && (!failed || listener.ProvisioningStatus != "ERROR")
			})
		if ready {
			gateway := desired.spec.Gateway.Spec
			backend := route.spec
			if len(backend.Members) == 0 {
				backend.BackendSecurityGroupIDs = nil
			}
			if actual.scoped && len(owned) > 0 {
				gateway.ScopedListeners = slices.DeleteFunc(slices.Clone(gateway.ScopedListeners), func(listener cloud.ScopedListener) bool {
					return !slices.ContainsFunc(owned, func(actual listeners.Listener) bool { return actual.ProtocolPort == listener.Port })
				})
			}
			err := p.topology.ValidateGatewaySecurity(ctx, gateway, []cloud.RouteSpec{backend})
			ready = err == nil
			if err != nil && len(owned) == 0 {
				return result, true, err
			}
		}
		if !ready {
			if len(owned) == 0 {
				continue
			}
			err := p.withdrawTCPRejectionDefault(ctx, desired, actual, index, owned[0].ID, &result)
			return result, true, err
		}
		err := p.submitTransportBatch(ctx, desired, actual, index, step, &result)
		return result, true, err
	}
	return cloud.GatewayGraphResult{}, false, nil
}

func validateTransportRouteBinding(route desiredRoute, actual observedGatewayGraph) error {
	if actual.scoped {
		return validateScopedRouteGatewayIDs(route.spec.Gateway, actual.state, actual.scopedListeners)
	}
	return validateRouteGatewayIDs(route.spec.Gateway, actual.state)
}

func supersededDefaultPoolListener(route desiredRoute, owned []listeners.Listener, scoped bool) string {
	if scoped {
		for _, listener := range owned {
			if !slices.Contains(route.spec.ScopedListenerPorts, listener.ProtocolPort) {
				return listener.ID
			}
		}
	}
	return ""
}

func (p *Provider) pendingTransportBatchResult(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int) cloud.GatewayGraphResult {
	result := p.pendingGraphResult(ctx, desired, actual)
	if actual.scoped {
		for i, observed := range actual.routes {
			if i == index || observed.pool == nil || desired.spec.Routes[i].Disposition != cloud.GraphEnsure {
				continue
			}
			route := desired.routes[i]
			owned := servingPoolListeners(actual, observed.pool.ID)
			if len(owned) == 0 || len(owned) != len(route.spec.ScopedListenerPorts) ||
				slices.ContainsFunc(owned, func(listener listeners.Listener) bool {
					return !listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE" ||
						!slices.Contains(route.spec.ScopedListenerPorts, listener.ProtocolPort) ||
						!actual.tcpDefaultPoolTLSMatches(listener.ID, route)
				}) {
				continue
			}
			steps, _, err := buildRouteEnsurePlan(route, observed)
			result.Routes[i].Result.ObservationDeferred = err == nil &&
				scopedRouteBackendReady(route, observed, actual.state.VIPAddress, steps, observed.backendProvisioningIssue(false, true))
		}
	}
	if index >= 0 && index < len(result.Routes) {
		result.Routes[index].Result.ObservationDeferred = false
	}
	return result
}

func (p *Provider) submitTransportBatch(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, batch routeMutation, result *cloud.GatewayGraphResult) error {
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return err
		}
	}
	step := graphMutation{kind: graphRouteMutation, routeIndex: index, route: batch, message: batch.message()}
	if err := p.executeGraphMutation(ctx, desired, actual, step); err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
			return nil
		}
		handled, withdrawalErr := p.withdrawAfterRejectedTransportBatch(ctx, desired, index, result, err)
		if handled {
			return withdrawalErr
		}
		return &cloud.GraphError{Identity: desired.routes[index].spec.Identity, Err: errors.Join(err, withdrawalErr)}
	}
	result.Outcome.MutationSubmitted = true
	result.Routes[index].Result.Outcome.MutationSubmitted = true
	return nil
}

// Octavia compares a private operation token and the normalized request body
// under its load balancer lock. Only replay a candidate with exact public
// staged readback. A busy response supplies no forwarding proof.
func (p *Provider) replayPendingTCPRejection(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, bool, error) {
	managed := canReconcileTCPRejection(desired, actual)
	if !managed || actual.loadBalancer.ProvisioningStatus != "PENDING_UPDATE" {
		return cloud.GatewayGraphResult{}, false, nil
	}
	var err error
	actual.routes, err = p.observeRouteGraphsWithListeners(ctx, desired.routeOwners(), actual.loadBalancer, actual.listener, actual.protocol, actual.scopedListeners)
	if err != nil {
		return cloud.GatewayGraphResult{}, false, nil
	}
	for index, observed := range actual.routes {
		if observed.pool == nil || observed.pool.ProvisioningStatus != "PENDING_UPDATE" || desired.spec.Routes[index].Disposition != cloud.GraphEnsure {
			continue
		}
		route := desired.routes[index]
		if route.spec.BackendTLS != nil || observed.managedTCPDefaultPoolTLS() {
			continue
		}
		// A pending clear lacks a positive fraction. The native full batch
		// stages every retained row, including unchanged members.
		if !observed.tcpRejection.Present || observed.tcpRejection.Value == nil && route.spec.TCPConnectionRejection == nil &&
			(len(observed.members) == 0 || slices.ContainsFunc(observed.members, func(member pools.Member) bool {
				switch member.ProvisioningStatus {
				case "PENDING_CREATE", "PENDING_UPDATE", "PENDING_DELETE":
					return false
				default:
					return true
				}
			})) {
			continue
		}
		if supersededDefaultPoolListener(route, servingPoolListeners(actual, observed.pool.ID), actual.scoped) != "" {
			continue
		}
		if err := validateTransportRouteBinding(route, actual); err != nil {
			return cloud.GatewayGraphResult{}, true, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
		}
		batch, _, changed, err := desiredTransportBatch(route, observed, true)
		if err != nil || changed {
			continue
		}
		delay := p.transportReplayDelay(observed.pool.ID, observed.tcpRejection.UpdatedAt, time.Now())
		actual.outcome = cloud.ProgressingOutcome("Waiting for Octavia TCP batch activation", max(delay, transportReplayInterval, p.pollInterval))
		result := p.pendingTransportBatchResult(ctx, desired, actual, index)
		if delay != 0 {
			return result, true, nil
		}
		err = p.submitTransportBatch(ctx, desired, actual, index, batch, &result)
		return result, true, err
	}
	return cloud.GatewayGraphResult{}, false, nil
}

// Store only retry timing. The pending payload is re-observed and fenced for
// every attempt, including after a restart or a lost response.
func (p *Provider) transportReplayDelay(pool string, updated, now time.Time) time.Duration {
	interval := max(transportReplayInterval, p.pollInterval)
	known := !updated.IsZero() && !updated.After(now)
	if known && now.Sub(updated) < interval {
		return interval - now.Sub(updated)
	}
	p.transportReplayMu.Lock()
	defer p.transportReplayMu.Unlock()
	if p.transportReplayAfter == nil {
		p.transportReplayAfter = make(map[string]time.Time)
	}
	for id, next := range p.transportReplayAfter {
		if id != pool && now.After(next.Add(interval)) {
			delete(p.transportReplayAfter, id)
		}
	}
	next, seen := p.transportReplayAfter[pool]
	if seen && now.Before(next) {
		return next.Sub(now)
	}
	p.transportReplayAfter[pool] = now.Add(interval)
	if !seen && !known {
		return interval
	}
	return 0
}
