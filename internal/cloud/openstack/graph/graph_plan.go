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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
)

// buildGatewayGraphPlan is free of I/O. No plan is executable unless every
// component passed validation, even when an earlier prerequisite needs repair.
func (p *Provider) buildGatewayGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(actual.state)}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		plan := gatewayGraphDeletionPlan(actual)
		if len(plan) != 0 {
			result.Outcome = p.progressingOutcome(plan[0].message)
			result.Gateway.Outcome = result.Outcome
		}
		for _, route := range desired.routes {
			result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: route.spec.Identity, Result: cloud.RouteResult{Outcome: result.Outcome}})
		}
		return plan, result, nil
	}
	if actual.scoped && usesDefaultPool(actual.protocol) {
		return p.buildScopedListenerGraphPlan(desired, actual)
	}
	if usesDefaultPool(actual.protocol) {
		return p.buildDefaultPoolGraphPlan(desired, actual)
	}
	result.Routes = observedRouteDeleteCompletions(desired, actual)
	plan, err := buildGatewayPrerequisitePlan(desired, actual)
	if err != nil {
		return nil, result, err
	}
	externalAddressReady := actual.floatingIP == nil && desired.spec.Gateway.Spec.ExternalNetworkID == "" ||
		actual.floatingIP != nil && actual.floatingIP.PortID != ""
	gatewayReady := actual.loadBalancer != nil && actual.listener != nil && actual.loadBalancer.AdminStateUp &&
		actual.listener.AdminStateUp && actual.listener.DefaultPoolID == "" && externalAddressReady && len(plan) == 0
	if !gatewayReady {
		result.Gateway.Outcome = p.progressingOutcome("Waiting for the Gateway infrastructure")
	}
	scopedHTTP := actual.scoped && actual.protocol == cloud.ListenerProtocolHTTP
	var listenerChanges []graphMutation
	var listenersByPort map[int]listeners.Listener
	var portByListenerID map[string]int
	var retiringListeners map[string]bool
	if scopedHTTP {
		listenersByPort, portByListenerID, retiringListeners, listenerChanges = scopedHTTPListenerPlan(desired, actual)
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure {
			result.Gateway = scopedHTTPGatewayResult(desired, actual, p.progressingOutcome("Waiting for the Gateway listeners"))
		}
	} else if message := httpPolicyTransition(desired, actual); message != "" {
		result.Gateway.Outcome = p.progressingOutcome(message)
		result.Gateway.HTTPPolicyTransition = true
	}
	var deleteDisables, deleteDisableRetries, deletions []graphMutation
	disables := make([]graphMutation, 0)
	ensures := make([]graphMutation, 0)
	routeResults := make([]cloud.RouteGraphResult, 0, len(desired.routes))
	canRestoreTraffic := true
	holdPolicyOrder := make(map[int]bool)
	policyPending := make(map[int]bool)
	for index, observed := range actual.routes {
		node := desired.spec.Routes[index]
		for _, policy := range observed.policies {
			port := portByListenerID[policy.policy.ListenerID]
			if policy.pending() {
				policyPending[port] = true
				holdPolicyOrder[port] = true
				canRestoreTraffic = false
			}
			if node.Disposition == cloud.GraphPreserve {
				holdPolicyOrder[port] = true
			} else if target := scopedHTTPRoutePort(desired.routes[index]); target != 0 && target != port {
				holdPolicyOrder[port], holdPolicyOrder[target] = true, true
			}
		}
		// A preserved fragment cannot authorize reopening its parent.
		if node.Disposition == cloud.GraphPreserve && (observed.pool != nil || len(observed.policies) != 0) {
			canRestoreTraffic = false
		}
	}
	for index, route := range desired.routes {
		observed := observedRouteGraph{}
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		routeResult := cloud.RouteReadyResult(routeStateFromObservation(observed))
		var routePlan []routeMutation
		var backendIssue, ensureHold string
		node := desired.spec.Routes[index]
		port := scopedHTTPRoutePort(route)
		var observedPort int
		if len(observed.policies) != 0 {
			observedPort = portByListenerID[observed.policies[0].policy.ListenerID]
		}
		var listenerID string
		frontendReady := gatewayReady
		if scopedHTTP {
			listenerID = listenersByPort[port].ID
			frontendReady = slices.Contains(result.Gateway.ScopedListenerReadyPorts, port)
		}
		if node.Disposition == cloud.GraphEnsure {
			state := actual.state
			if scopedHTTP {
				state.ListenerID = listenerID
			}
			if err := validateRouteGatewayIDs(route.spec.Gateway, state); err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			route.spec.Gateway = state
		}
		relocating := node.Disposition == cloud.GraphEnsure && observedPort != 0 &&
			(port != observedPort || retiringListeners[observed.policies[0].policy.ListenerID])
		withdrawing := node.Disposition == cloud.GraphDelete || relocating
		switch {
		case withdrawing:
			if relocating {
				// Octavia cannot move a policy to another listener. Remove its
				// original policies before creating any on the new port.
				routePlan = buildRouteDeletionPlan(observedRouteGraph{policies: observed.policies})
			} else {
				routePlan = buildRouteDeletionPlan(observed)
				backendIssue = observed.backendProvisioningIssue(true, scopedHTTP)
			}
		case node.Disposition == cloud.GraphEnsure:
			routePlan, routeResult.State, err = buildRouteEnsurePlan(route, observed)
			if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			if actual.loadBalancer == nil || !scopedHTTP && actual.listener == nil || scopedHTTP && listenerID == "" {
				// The desired route has been validated, but its resources cannot
				// be created until both prerequisites have been observed.
				routePlan = nil
			}
			if !frontendReady {
				routeResult.Outcome = p.progressingOutcome("Waiting for the Gateway infrastructure")
			}
			if slices.ContainsFunc(observed.policies, observedRoutePolicy.pending) {
				ensureHold = "Waiting for Octavia L7 policy or rule provisioning"
				routePlan = nil
			} else if backendIssue := observed.backendProvisioningIssue(false, scopedHTTP); backendIssue != "" {
				ensureHold = backendIssue
				canRestoreTraffic = false
				// A blocked earlier route can leave an empty slot in the policy
				// order. Octavia would clamp a subsequent insert to the end.
				holdPolicyOrder[port] = holdPolicyOrder[port] || !policyPositionsMatch(route.policies, observed.policies)
			}
		}
		if len(routePlan) != 0 {
			routeResult.Outcome = p.progressingOutcome(routePlan[0].message())
		}
		if ensureHold != "" {
			routeResult.Outcome = p.progressingOutcome(ensureHold)
		}
		for _, step := range routePlan {
			mutation := graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, listenerID: listenerID, message: step.message()}
			mutation.withdrawHTTPPolicy = scopedHTTP && withdrawing &&
				(step.kind == routeMutationDisablePolicy || step.kind == routeMutationDeleteRule || step.kind == routeMutationDeletePolicy)
			switch {
			case withdrawing && step.kind == routeMutationDisablePolicy:
				if step.retry {
					deleteDisableRetries = append(deleteDisableRetries, mutation)
				} else {
					deleteDisables = append(deleteDisables, mutation)
				}
			case withdrawing:
				if policyPending[observedPort] {
					routeResult.Outcome = p.progressingOutcome("Waiting for Octavia L7 policy or rule provisioning before cleanup")
				} else if backendIssue != "" && (step.kind == routeMutationDeleteMember || step.kind == routeMutationDeleteMonitor || step.kind == routeMutationDeletePool) {
					routeResult.Outcome = p.progressingOutcome(backendIssue)
				} else {
					deletions = append(deletions, mutation)
				}
			case step.kind == routeMutationDisablePolicy:
				disables = append(disables, mutation)
			case ensureHold != "":
				continue
			case holdPolicyOrder[port] && (step.kind == routeMutationMovePolicy || step.kind == routeMutationCreatePolicy || step.kind == routeMutationEnablePolicy):
				routeResult.Outcome = p.progressingOutcome("Waiting for the HTTPRoute policy order")
			default:
				ensures = append(ensures, mutation)
			}
		}
		routeResults = append(routeResults, cloud.RouteGraphResult{Identity: route.spec.Identity, Result: routeResult})
		if routeResult.Outcome.State == cloud.OutcomeProgressing {
			result.Outcome = routeResult.Outcome
		}
	}
	result.Routes = routeResults
	// A failed teardown for one route must never leave another revoked route
	// forwarding. Close all actionable policies, including fresh disables ahead
	// of retries for previously failed disables, before touching descendants.
	if scopedHTTP {
		// A replacement's dependencies must never hold a verified withdrawal.
		// Select withdrawal first even when an earlier prerequisite needs repair.
		for _, steps := range [][]graphMutation{deleteDisables, deleteDisableRetries, deletions} {
			for _, step := range steps {
				if step.withdrawHTTPPolicy {
					result.Outcome = p.progressingOutcome(step.message)
					return []graphMutation{step}, result, nil
				}
			}
		}
	}
	plan = append(plan, deleteDisables...)
	plan = append(plan, deleteDisableRetries...)
	plan = append(plan, deletions...)
	plan = append(plan, disables...)
	plan = append(plan, listenerChanges...)
	// Ensured routes are ranked, and each route's policies are ordered within
	// its rank. Each new call refreshes the list after Octavia shifts positions.
	// Filling this prefix first keeps inserts and moves within the current list.
	plan = append(plan, ensures...)
	if canRestoreTraffic {
		var restore []graphMutation
		var err error
		if scopedHTTP {
			restore, err = buildScopedHTTPTrafficRestorationPlan(desired, actual)
		} else {
			restore, err = buildGatewayTrafficRestorationPlan(desired, actual)
		}
		if err != nil {
			return nil, result, err
		}
		plan = append(plan, restore...)
	}
	if len(plan) != 0 {
		result.Outcome = p.progressingOutcome(plan[0].message)
	} else if result.Gateway.Outcome.State == cloud.OutcomeProgressing && desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		result.Outcome = result.Gateway.Outcome
	}
	return plan, result, nil
}

func httpPolicyTransition(desired desiredGatewayGraph, actual observedGatewayGraph) string {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.protocol != cloud.ListenerProtocolHTTP || actual.listener == nil {
		return ""
	}
	for index, node := range desired.spec.Routes {
		if node.WaitForHTTPPolicyRemoval || node.Disposition == cloud.GraphDelete {
			if index >= len(actual.routes) || len(actual.routes[index].policies) != 0 {
				return "Waiting for outdated HTTP policy removal"
			}
		}
		if node.Disposition == cloud.GraphEnsure && index < len(actual.routes) {
			for _, policy := range actual.routes[index].policies {
				if policy.hasOutdatedHostname(desired.routes[index].hostnameRuleValue) {
					return "Waiting for updated HTTP hostname rules"
				}
			}
		}
	}
	return ""
}

func observedRouteDeleteCompletions(desired desiredGatewayGraph, actual observedGatewayGraph) []cloud.RouteGraphResult {
	if desired.spec.Gateway.Disposition == cloud.GraphDelete || actual.loadBalancer != nil && len(actual.routes) != len(desired.routes) {
		return nil
	}
	var completed []cloud.RouteGraphResult
	for index, node := range desired.spec.Routes {
		if node.Disposition != cloud.GraphDelete {
			continue
		}
		if index < len(actual.routes) && (actual.routes[index].pool != nil || len(actual.routes[index].policies) != 0) {
			continue
		}
		// Inventory was either completely observed or the parent was absent.
		completed = append(completed, cloud.RouteGraphResult{Identity: node.Spec.Identity, Result: cloud.RouteReadyResult(cloud.RouteState{})})
	}
	return completed
}

func buildGatewayPrerequisitePlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, error) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil, nil
	}
	if actual.loadBalancer == nil {
		if actual.scoped && actual.protocol == cloud.ListenerProtocolHTTP &&
			!slices.ContainsFunc(desired.spec.Gateway.Spec.ScopedListeners, hasAuthorizedListener) {
			return nil, nil
		}
		return []graphMutation{{kind: graphCreateLoadBalancer, message: "Created Octavia load balancer"}}, nil
	}
	plan := make([]graphMutation, 0)
	networkID := desired.spec.Gateway.Spec.ExternalNetworkID
	if actual.floatingIP != nil && networkID != "" && actual.floatingIP.FloatingNetworkID != networkID {
		return nil, fmt.Errorf("%w: Gateway Floating IP network differs from its binding", cloud.ErrOwnershipConflict)
	}
	if actual.listener != nil && actual.listener.DefaultPoolID != "" && !usesDefaultPool(actual.protocol) {
		// Octavia may return a cleared field before Amphora applies it. Keep the
		// frontend closed across that pending update so readback alone cannot
		// mark the Gateway programmed before the data plane has changed.
		if actual.loadBalancer.AdminStateUp && actual.listener.AdminStateUp {
			return []graphMutation{{kind: graphDisableListener, message: "Disabling Octavia listener before clearing its default pool"}}, nil
		}
		return []graphMutation{{kind: graphClearListenerDefaultPool, message: "Clearing unexpected Octavia listener default pool"}}, nil
	}
	if networkID == "" && actual.floatingIP != nil {
		plan = append(plan, graphMutation{kind: graphDeleteFloatingIP, message: "Removed a no-longer-configured Floating IP"})
	}
	if actual.listener == nil && !actual.scoped {
		plan = append(plan, graphMutation{kind: graphCreateListener, message: "Created Octavia listener"})
	}
	return plan, nil
}

func buildGatewayTrafficRestorationPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, error) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.loadBalancer == nil || actual.listener == nil ||
		actual.listener.DefaultPoolID != "" && !usesDefaultPool(actual.protocol) {
		return nil, nil
	}
	plan, err := buildGatewayAddressRestorationPlan(desired.spec.Gateway.Spec.ExternalNetworkID, actual)
	if err != nil {
		return nil, err
	}
	if !actual.listener.AdminStateUp {
		plan = append(plan, graphMutation{kind: graphEnableListener, message: "Enabled Octavia listener after validating the Gateway graph"})
	}
	if !actual.loadBalancer.AdminStateUp {
		plan = append(plan, graphMutation{kind: graphEnableLoadBalancer, message: "Enabled Octavia load balancer after validating the Gateway graph"})
	}
	return plan, nil
}

// Restoring an external address restores traffic and requires a complete VIP association.
func buildGatewayAddressRestorationPlan(networkID string, actual observedGatewayGraph) ([]graphMutation, error) {
	if networkID == "" || actual.floatingIP != nil && actual.floatingIP.PortID != "" {
		return nil, nil
	}
	if actual.state.VIPPortID == "" || actual.state.VIPAddress == "" {
		return nil, fmt.Errorf("%w: Gateway load balancer has no complete VIP association", cloud.ErrOwnershipConflict)
	}
	mutation := graphMutation{kind: graphCreateFloatingIP, message: "Allocated Floating IP"}
	if actual.floatingIP != nil {
		mutation = graphMutation{kind: graphAssociateFloatingIP, message: "Restored Floating IP association"}
	}
	return []graphMutation{mutation}, nil
}

func gatewayGraphDeletionPlan(actual observedGatewayGraph) []graphMutation {
	if actual.scoped {
		for _, listener := range actual.scopedListeners {
			if listener.DefaultPoolID != "" {
				mutation := clearListenerDefaultPool(actual.protocol)
				mutation.listenerID = listener.ID
				return []graphMutation{mutation}
			}
		}
	}
	if listener := actual.deletion.defaultPoolListener; listener != nil && listener.DefaultPoolID != "" {
		return []graphMutation{clearListenerDefaultPool(listener.Protocol)}
	}
	// Removing a predicate from a serving L7 policy can widen its match.
	// Close all frontends first. The next pass waits for ACTIVE and revalidates
	// the graph before changing any of its L7 resources.
	if actual.loadBalancer != nil && actual.loadBalancer.AdminStateUp && len(actual.deletion.routes) > 0 {
		return []graphMutation{{kind: graphDisableLoadBalancer, message: "Disabling Octavia load balancer before removing routes"}}
	}
	plan := make([]graphMutation, 0)
	for _, step := range actual.deletion.orderedSteps() {
		plan = append(plan, graphMutation{kind: graphDeleteChild, deletion: step, message: fmt.Sprintf("Deleting controller-owned %s", step.resource)})
	}
	if actual.floatingIP != nil {
		plan = append(plan, graphMutation{kind: graphDeleteFloatingIP, message: "Deleting controller-owned Floating IP"})
	}
	if actual.loadBalancer != nil {
		plan = append(plan, graphMutation{kind: graphDeleteLoadBalancer, message: "Deleting Octavia load balancer"})
	}
	return plan
}

func routeStateFromObservation(actual observedRouteGraph) cloud.RouteState {
	state := cloud.RouteState{}
	if actual.pool != nil {
		state.PoolID = actual.pool.ID
	}
	if actual.monitor != nil {
		state.MonitorID = actual.monitor.ID
	}
	for _, member := range actual.members {
		state.MemberIDs = append(state.MemberIDs, member.ID)
	}
	for _, policy := range actual.policies {
		state.L7PolicyIDs = append(state.L7PolicyIDs, policy.policy.ID)
		for _, rule := range policy.rules {
			state.L7RuleIDs = append(state.L7RuleIDs, rule.rule.ID)
		}
	}
	return state
}

func (p *Provider) executeGraphMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) error {
	var err error
	spec := desired.spec.Gateway.Spec
	listenerID := step.listenerID
	if listenerID == "" {
		listenerID = actual.state.ListenerID
	}
	switch step.kind {
	case graphCreateLoadBalancer:
		tags, tagErr := desired.identity.GatewayTags(roleLoadBalancer)
		if tagErr != nil {
			return tagErr
		}
		created, createErr := p.octavia.CreateLoadBalancer(ctx, octavia.CreateLoadBalancerOpts{CreateOpts: loadbalancers.CreateOpts{
			Name: resourceName(desired.spec.Identity, roleLoadBalancer), Description: desired.identity.Description(roleLoadBalancer),
			VipSubnetID: spec.VIPSubnetID, VipAddress: spec.RequestedVIPAddress, Provider: spec.Provider, FlavorID: spec.FlavorID, AvailabilityZone: spec.AvailabilityZone,
			AdminStateUp: boolPointer(true), Tags: tags,
		}, VIPSecurityGroupIDs: spec.VIPSecurityGroupIDs})
		err = createErr
		if err == nil {
			err = p.validateLoadBalancerProject(created.ID, created.ProjectID)
		}
	case graphCreateFloatingIP:
		created, createErr := p.neutron.CreateFloatingIP(ctx, floatingips.CreateOpts{
			Description: desired.identity.GatewayDescription(roleFloatingIP), FloatingNetworkID: spec.ExternalNetworkID, PortID: actual.state.VIPPortID,
			FixedIP: actual.state.VIPAddress,
		})
		if createErr != nil {
			return fmt.Errorf("create Floating IP: %w", createErr)
		}
		return p.validateFloatingIPProject(created.ID, created.ProjectID, created.TenantID)
	case graphAssociateFloatingIP:
		return p.associateGatewayFloatingIP(ctx, actual)
	case graphDeleteFloatingIP:
		err = p.neutron.DeleteFloatingIP(ctx, actual.floatingIP.ID)
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("delete Floating IP: %w", err)
		}
		return nil
	case graphCreateListener:
		tags, tagErr := desired.identity.GatewayTags(roleListener)
		if tagErr != nil {
			return tagErr
		}
		port, name := spec.ListenerPort, spec.ListenerName
		protocol := cloud.NormalizeListenerProtocol(spec.ListenerProtocol)
		enabled := true
		certificateRef := ""
		if step.scopedListener.Port != 0 {
			port, name = step.scopedListener.Port, step.scopedListener.Name
			if tls := step.scopedListener.TLS; tls != nil {
				protocol, enabled, certificateRef = terminatedHTTPS, false, tls.CertificateRef
			}
			tags = append(tags, scopedPortTag(protocol, port))
		}
		_, err = p.octavia.CreateListener(ctx, listeners.CreateOpts{
			LoadbalancerID: actual.state.LoadBalancerID, Protocol: listeners.Protocol(protocol), ProtocolPort: port,
			Name: name, Description: desired.identity.Description(roleListener), AdminStateUp: boolPointer(enabled),
			DefaultTlsContainerRef: certificateRef, Tags: tags,
		})
	case graphUpdateListenerCertificate:
		ref := step.scopedListener.TLS.CertificateRef
		_, err = p.octavia.UpdateListener(ctx, listenerID, listeners.UpdateOpts{DefaultTlsContainerRef: &ref})
	case graphClearListenerDefaultPool, graphSetListenerDefaultPool:
		// Gophercloud encodes an explicit empty DefaultPoolID as JSON null.
		_, err = p.octavia.UpdateListener(ctx, listenerID, listeners.UpdateOpts{DefaultPoolID: &step.poolID})
	case graphRouteMutation:
		route := desired.routes[step.routeIndex]
		route.spec.Gateway = actual.state
		route.spec.Gateway.ListenerID = listenerID
		return p.executeRouteMutation(ctx, route, step.route)
	case graphEnableListener, graphDisableListener:
		enabled := step.kind == graphEnableListener
		_, err = p.octavia.UpdateListener(ctx, listenerID, listeners.UpdateOpts{AdminStateUp: boolPointer(enabled)})
	case graphEnableLoadBalancer, graphDisableLoadBalancer:
		return p.setLoadBalancerAdminState(ctx, actual.state.LoadBalancerID, step.kind == graphEnableLoadBalancer)
	case graphDeleteChild:
		return p.executeGatewayDeletionStep(ctx, step.deletion)
	case graphDeleteLoadBalancer:
		err = p.octavia.DeleteLoadBalancer(ctx, actual.state.LoadBalancerID)
		if isNotFound(err) {
			err = nil
		}
	default:
		return fmt.Errorf("unsupported graph mutation %d", step.kind)
	}
	if err != nil {
		err = classifyOctaviaMutationError(err)
		if step.redactTLSError {
			// Octavia faults may echo Barbican references. Surface the
			// classification without publishing the provider's raw body.
			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}
			category, ok := cloud.ErrorCategoryOf(classifyOpenStackError(err))
			if !ok {
				category = cloud.ErrorCategoryResourceFailure
			}
			return cloud.NewProviderError(category, errors.New(step.message))
		}
		return fmt.Errorf("reconcile Gateway graph: %w", err)
	}
	return nil
}
