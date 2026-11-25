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
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

type grpcAuthTransitionPlan struct {
	request   octavia.GRPCAuthTransitionBatch
	listener  listeners.Listener
	withdraws bool
	prepare   *graphMutation
}

type grpcAuthRecovery struct {
	listenerID string
	operation  octavia.GRPCAuthRecovery
}

func (p *Provider) observePendingGRPCAuth(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) error {
	inventory, err := p.observeGatewayListenerInventory(ctx, desired, actual)
	if err != nil {
		return err
	}
	if !actual.scoped {
		actual.listener, err = p.managedGatewayListener(desired.identity, actual.loadBalancer.ID, inventory.Items)
		if err != nil {
			return err
		}
		if actual.listener != nil {
			if desired.spec.Gateway.Disposition == cloud.GraphEnsure &&
				actual.listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort {
				return fmt.Errorf("%w: Gateway listener port differs from its binding", cloud.ErrOwnershipConflict)
			}
			actual.state.ListenerID = actual.listener.ID
		}
	}
	for _, listener := range actual.authListeners() {
		if listener.Protocol != cloud.ListenerProtocolHTTP {
			continue
		}
		operation, err := p.octavia.ObserveGRPCAuthRecovery(ctx, listener.ID)
		if isNotFound(err) || gophercloud.ResponseCodeIs(err, http.StatusNotImplemented) {
			continue
		}
		if err != nil {
			return err
		}
		actual.grpcAuthRecovery = &grpcAuthRecovery{listenerID: listener.ID, operation: operation}
		return nil
	}
	return nil
}

func (p *Provider) resumeGRPCAuthCompensation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, error) {
	actual.outcome = p.progressingOutcome("Waiting for Octavia gRPC authorization compensation")
	result := p.pendingGraphResult(ctx, desired, actual)
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return result, err
		}
	}
	recovery := actual.grpcAuthRecovery
	err := p.octavia.RecoverGRPCAuth(ctx, recovery.listenerID, recovery.operation)
	if isNotFound(err) || gophercloud.ResponseCodeIs(err, http.StatusConflict) {
		return result, nil
	}
	result.Outcome.MutationSubmitted = err == nil
	return result, err
}

// Prove the entire ordered listener table before changing the selected policies.
// A missing, shifted, or unreadable sibling keeps the existing listener guard.
func (p *Provider) planGRPCAuthTransition(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph,
	listener listeners.Listener, state guardedHTTPListener) (*grpcAuthTransitionPlan, bool) {
	withdrawals := len(state.obsoleteIDs)
	if !desired.fixedHTTPResponses || actual.addressReadError != nil || actual.httpPools == nil ||
		actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" || !actual.loadBalancer.AdminStateUp ||
		listener.Protocol != cloud.ListenerProtocolHTTP || listener.ProjectID != p.projectID || listener.ProvisioningStatus != "ACTIVE" ||
		!actual.adminObserved[listener.ID] || !listener.AdminStateUp || !actual.defaultPoolsObserved ||
		listener.DefaultPoolID != "" || listener.L7Policies == nil || !actual.httpFallbackReady(listener, true) ||
		withdrawals == 1 || withdrawals > octavia.MaxPolicyActivationBatch ||
		withdrawals > 0 && state.unsettledDesired || withdrawals == 0 && !state.unsettledDesired {
		return nil, false
	}
	if filters, present := actual.requestFilters[listener.ID]; present && (!filters.Readable || len(filters.Sections) != 0) {
		return nil, false
	}

	parts, observed, owners := httpPlanningParts(desired, actual)
	type rankedPolicy struct {
		part int
		rule desiredPolicy
	}
	ranks := make(map[int32]rankedPolicy)
	for index, part := range parts.routes {
		if parts.spec.Routes[index].Disposition != cloud.GraphEnsure || httpURLPolicyRoutePort(desired, part) != listener.ProtocolPort {
			continue
		}
		for _, policy := range part.policies {
			if _, duplicate := ranks[policy.position]; policy.position <= 0 || duplicate {
				return nil, false
			}
			ranks[policy.position] = rankedPolicy{index, policy}
		}
	}
	if len(ranks) != len(listener.L7Policies) || len(observed.routes) != len(owners) {
		return nil, false
	}

	references := make(map[string]bool, len(listener.L7Policies))
	for _, ref := range listener.L7Policies {
		if ref.ID == "" || references[ref.ID] {
			return nil, false
		}
		references[ref.ID] = true
	}
	poolParts := make(map[string]observedRouteGraph)
	for _, part := range observed.routes {
		if part.pool != nil {
			poolParts[part.pool.ID] = part
		}
	}
	authPools := make(map[string]observedAuthPool)
	for _, pool := range actual.httpPools.auth {
		authPools[pool.pool.ID] = pool
	}

	currentPolicies := make(map[string]observedRoutePolicy)
	changes := make(map[string]octavia.GRPCAuthTransitionPolicy)
	destinations := make(map[string]observedRouteGraph)
	newDestinations := make(map[string]bool)
	addDestination := func(id string) bool {
		if id == "" {
			return true
		}
		if part, ok := poolParts[id]; ok && plainActivationPool(part) {
			destinations[id] = part
			return true
		}
		if auth, ok := authPools[id]; ok && isGRPCAuthPool(auth.pool) && auth.pool.ProjectID == p.projectID &&
			validateBoundPoolParent(auth.pool, actual.loadBalancer.ID, cloud.ListenerProtocolHTTP) == nil {
			destinations[id] = observedRouteGraph{pool: &auth.pool, members: auth.members, backendTLS: auth.tls, backendH2C: auth.h2c}
			return true
		}
		return false
	}
	waiting := false
	var prepare *graphMutation
	consumers := make(map[string]int)
	selectedConsumers := make(map[string]int)
	for index, part := range observed.routes {
		for _, current := range part.policies {
			row := current.policy
			if current.externalAuth != nil {
				consumers[current.externalAuth.PoolID]++
			}
			if row.ListenerID != listener.ID {
				continue
			}
			rank, found := ranks[row.Position]
			if !found || !references[row.ID] || row.ProjectID != p.projectID || !row.AdminStateUp ||
				!observedForwardPolicySettled(current) || current.initialRulesPartial ||
				!parts.routes[index].identity.MatchesRoute(row.Tags, current.role) ||
				!current.externalAuthReported || !current.externalAuthReadable() || !plainGRPCAuthTransitionPolicy(current) {
				return nil, false
			}
			delete(ranks, row.Position)
			delete(references, row.ID)
			currentPolicies[row.ID] = current
			if !addDestination(row.RedirectPoolID) || current.externalAuth != nil && !addDestination(current.externalAuth.PoolID) {
				return nil, false
			}

			wantedPart, wanted := parts.routes[rank.part], rank.rule
			if index == rank.part && isolatedPolicyMatches(wantedPart, part, wanted, current) && !state.obsoleteIDs[row.ID] {
				continue
			}
			if owners[index] != owners[rank.part] || !current.versioned || !current.targetObserved ||
				wanted.httpsListenerHostname != "" || len(wanted.requestHeaderOperations) != 0 ||
				len(wanted.responseHeaderOperations) != 0 || wanted.requestCORS != nil || wanted.requestPolicy != nil ||
				wanted.requestURLRewrite != nil || wanted.redirect != nil || wanted.weightedPlan != nil ||
				len(wanted.requestMirrors) != 0 {
				return nil, false
			}

			change := octavia.GRPCAuthTransitionPolicy{}
			if withdrawals != 0 {
				auth := current.externalAuth
				if !state.obsoleteIDs[row.ID] || row.Action != string(l7policies.ActionRedirectToPool) || row.RedirectPoolID == "" ||
					!current.externalAuthTagged || auth == nil || auth.Protocol != cloud.ExternalAuthProtocolGRPC ||
					!wantedPart.rejectsWithoutForwarding() ||
					!slices.Equal(authPools[auth.PoolID].pool.Listeners, []pools.ListenerID{{ID: listener.ID}}) {
					return nil, false
				}
				selectedConsumers[auth.PoolID]++
			} else {
				forward := observed.routes[rank.part]
				if !current.fixedResponse || current.hasExternalAuth() || !slices.Contains(row.Tags, grpcAuthWithdrawalTag) ||
					wanted.externalAuth == nil || wanted.externalAuth.Protocol != cloud.ExternalAuthProtocolGRPC ||
					wantedPart.spec.GRPC || wantedPart.fixedResponse || wantedPart.unavailable ||
					wantedPart.spec.BackendH2C || wantedPart.spec.BackendTLS != nil {
					return nil, false
				}
				ready, originStep, valid := p.planGRPCAuthOrigin(actual, wantedPart, forward)
				if !valid {
					return nil, false
				}
				if originStep != nil && prepare == nil {
					prepare = &graphMutation{kind: graphRouteMutation, listenerID: listener.ID, routeIndex: owners[rank.part],
						backendKey: wantedPart.backendKey, route: *originStep, message: originStep.message()}
				}
				if !ready || wanted.resolvedExternalAuth == nil {
					waiting = true
				} else {
					authID := wanted.resolvedExternalAuth.PoolID
					if authPools[authID].pool.ID == "" || !addDestination(forward.pool.ID) || !addDestination(authID) {
						return nil, false
					}
					change.Origin, change.ExternalAuth = forward.pool.ID, wanted.resolvedExternalAuth
					newDestinations[change.Origin], newDestinations[authID] = true, true
				}
			}
			if !tagGRPCAuthTransition(&change, wantedPart, wanted, current, withdrawals != 0) {
				return nil, false
			}
			changes[row.ID] = change
		}
	}

	if len(references) != 0 || len(changes) == 0 || len(changes) > octavia.MaxPolicyActivationBatch ||
		withdrawals != 0 && len(changes) != withdrawals {
		return nil, false
	}
	for id, selected := range selectedConsumers {
		if consumers[id] != selected {
			return nil, false
		}
	}
	if p.verifyGatewayHTTPURLCapabilities(ctx, desired, actual, []string{octavia.GatewayCapabilityGRPCAuthTransition}) != nil {
		return nil, false
	}
	if waiting {
		// Independently stage origin and authorizer pools behind the 500s.
		return &grpcAuthTransitionPlan{listener: listener, prepare: prepare}, true
	}

	snapshot, err := p.octavia.ObserveGRPCAuthTransition(ctx, listener.ID, slices.Sorted(maps.Keys(newDestinations)))
	if err != nil || !activationLoadBalancerMatches(snapshot.LoadBalancer, actual.loadBalancer) ||
		!activationListenerMatches(snapshot.Listener, &listener, actual.defaultHTTPResponseCodes[listener.ID], actual.httpsVirtualHosts[listener.ID]) ||
		!grpcTransitionDestinationsMatch(snapshot.Pools, destinations) || len(snapshot.Policies) != len(currentPolicies) {
		return nil, false
	}
	batch := &grpcAuthTransitionPlan{listener: listener, withdraws: withdrawals != 0}
	batch.request.ExpectedGraphRevision = snapshot.Revision
	for index, item := range snapshot.Policies {
		row := item.Policy.Policy
		current, found := currentPolicies[row.ID]
		if !found || row.Position != int32(index+1) || !item.Policy.ExternalAuthReported || !policySnapshotMatchesInventory(item, current) {
			return nil, false
		}
		delete(currentPolicies, row.ID)
		batch.request.ExpectedOrder = append(batch.request.ExpectedOrder, row.ID)
		if change, selected := changes[row.ID]; selected {
			change.PolicyActivationRevision = octavia.PolicyActivationRevision{ID: row.ID, Revision: item.Revision}
			batch.request.Policies = append(batch.request.Policies, change)
		}
	}
	return batch, true
}

func independentOfWaitingGRPCAuth(desired desiredGatewayGraph, step graphMutation, waitingListeners map[string]bool, waitingPorts map[int]bool) bool {
	if step.listenerID != "" {
		return !waitingListeners[step.listenerID]
	}
	port := step.scopedListener.Port
	if step.kind == graphRouteMutation {
		port = httpURLPolicyRoutePort(desired, graphRoutePart(desired, step))
	}
	return port != 0 && !waitingPorts[port]
}

func (p *Provider) planGRPCAuthOrigin(actual observedGatewayGraph, wanted desiredRoute, current observedRouteGraph) (bool, *routeMutation, bool) {
	if current.pool != nil && (current.pool.ProjectID != p.projectID || !plainActivationPool(current) ||
		validateBoundPoolParent(*current.pool, actual.loadBalancer.ID, cloud.ListenerProtocolHTTP) != nil) {
		return false, nil, false
	}
	if current.backendProvisioningIssue(false, true) != "" {
		return false, nil, true
	}

	wanted.policies, current.policies = nil, nil
	steps, _, err := buildRouteEnsurePlan(wanted, current)
	if err != nil {
		return false, nil, false
	}
	if len(steps) == 0 {
		return current.pool != nil && current.pool.AdminStateUp, nil, true
	}
	if isHTTPPolicyMutation(steps[0].kind) {
		return false, nil, false
	}
	// A backend that is still shared with a serving policy cannot be
	// altered as part of preparing a restored authorization rule.
	if current.pool != nil {
		for _, route := range actual.routes {
			for _, part := range route.parts() {
				for _, policy := range part.policies {
					if policy.policy.RedirectPoolID == current.pool.ID ||
						slices.ContainsFunc(policy.weightedTargets, func(target octavia.WeightedHTTPTarget) bool {
							return target.PoolID == current.pool.ID
						}) {
						return false, nil, false
					}
				}
			}
		}
	}
	return false, &steps[0], true
}

func plainGRPCAuthTransitionPolicy(current observedRoutePolicy) bool {
	return !current.redirect && !current.requestRedirectPresent && !current.hasHTTPRequestPolicy() &&
		!current.hasRequestURLRewrite() && !current.hasRequestCORS() && !current.hasRequestMirrors() &&
		!current.requestMirrorsUnknown && len(current.requestHeaderOperations) == 0 && len(current.responseHeaderOperations) == 0 &&
		len(current.weightedTargets) == 0 && current.httpsListenerHostname == "" && current.policy.RedirectURL == "" &&
		current.policy.RedirectPrefix == "" && current.policy.RedirectHttpCode == 0
}

func tagGRPCAuthTransition(change *octavia.GRPCAuthTransitionPolicy, part desiredRoute, wanted desiredPolicy, current observedRoutePolicy, withdrawal bool) bool {
	rules := rulesFor(wanted, part.hostnameRuleValue)
	if len(rules) == 0 || !observedHTTPPredicateMatches(current, rules) {
		return false
	}
	tags, ok := transitionRouteTags(part.identity, current.policy.Tags, current.role, wanted.role)
	if !ok {
		return false
	}
	tags = slices.DeleteFunc(tags, func(tag string) bool { return tag == routeExternalAuthTag || tag == grpcAuthWithdrawalTag })
	if withdrawal {
		tags = append(tags, grpcAuthWithdrawalTag)
	} else {
		tags = append(tags, routeExternalAuthTag)
	}
	change.Tags = tags
	for _, rule := range rules {
		index := slices.IndexFunc(current.rules, func(observed observedRouteRule) bool {
			return observed.rule.RuleType == string(rule.ruleType) && observed.rule.Value == rule.value &&
				observed.rule.CompareType == string(rule.compareType)
		})
		if rule.invert || !isHTTPPathRule(rule.ruleType) && rule.ruleType != l7policies.TypeHostName || index < 0 {
			return false
		}
		old := current.rules[index]
		tags, ok := transitionRouteTags(part.identity, old.rule.Tags, old.role, rule.role)
		if !ok || old.rule.ProjectID != current.policy.ProjectID {
			return false
		}
		change.Rules = append(change.Rules, octavia.GRPCAuthTransitionRule{ID: old.rule.ID, Tags: tags})
	}
	return true
}

func transitionRouteTags(identity Identity, installed []string, role, wanted string) ([]string, bool) {
	old, err := identity.RouteTags(role)
	if err != nil || !identity.MatchesRoute(installed, role) {
		return nil, false
	}
	tags, err := identity.RouteTags(wanted)
	if err != nil {
		return nil, false
	}
	for _, tag := range installed {
		if !slices.Contains(old, tag) && !slices.Contains(tags, tag) {
			tags = append(tags, tag)
		}
	}
	return tags, true
}

func grpcTransitionDestinationsMatch(snapshot []octavia.PolicyActivationPoolInventory, destinations map[string]observedRouteGraph) bool {
	if len(snapshot) != len(destinations) {
		return false
	}
	for _, item := range snapshot {
		if item.Pool == nil || item.Members == nil {
			return false
		}
		row := item.Pool
		part, found := destinations[row.ID]
		if !found || !part.backendTLS.AdminObserved || !part.backendTLS.EnabledObserved || !part.backendTLS.H2CObserved ||
			!activationPoolCoreMatches(row, part.pool) || row.TLSEnabled == nil || *row.TLSEnabled != part.pool.TLSEnabled ||
			row.BackendH2C == nil || *row.BackendH2C != part.backendH2C || row.BackendTLSHost != part.backendTLS.Hostname ||
			row.BackendRequireH2 != part.backendTLS.RequireH2 || !row.MatchesBackendTLSVerification(part.backendTLS) ||
			!activationMembersMatch(item.Members, part) ||
			!activationMonitorMatches(row.HealthMonitor, part.monitor) {
			return false
		}
		if persistence := string(row.Persistence); persistence != "" && persistence != "null" {
			return false
		}
		delete(destinations, row.ID)
	}
	return true
}
