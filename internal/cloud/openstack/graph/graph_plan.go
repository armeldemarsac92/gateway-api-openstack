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
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/apierrors"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
)

// buildGatewayGraphPlan is free of I/O. No plan is executable unless every
// component passed validation, even when an earlier prerequisite needs repair.
func (p *Provider) buildGatewayGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	if isSNIProtocol(desired.protocol) {
		return p.buildSNIGraphPlan(desired, actual)
	}
	var plan []graphMutation
	var result cloud.GatewayGraphResult
	var err error
	if desired.spec.Gateway.Disposition != cloud.GraphDelete && hasHTTPRouteParts(desired, actual) {
		plan, result, err = p.buildHTTPRoutePartsGraphPlan(desired, actual)
	} else {
		plan, result, err = p.buildGatewayGraphPartPlan(desired, actual, nil)
	}
	if err == nil {
		plan = p.protectPlainHTTPIsolation(desired, actual, plan, &result)
		if len(plan) == 0 && actual.scoped && actual.protocol == cloud.ListenerProtocolHTTP {
			// Already closed frontends cannot forward. Repair their fallback
			// after actionable changes so unreadable state cannot starve a peer.
			if step := actual.nextFrontendFallbackClear(desired); step != nil {
				plan = []graphMutation{*step}
				result.Outcome = p.progressingOutcome(step.message)
			}
		}
	}
	return plan, result, err
}

func (p *Provider) buildGatewayGraphPartPlan(desired desiredGatewayGraph, actual observedGatewayGraph, handovers *httpPolicyHandovers) ([]graphMutation, cloud.GatewayGraphResult, error) {
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
		actual.listener.AdminStateUp && actual.listener.DefaultPoolID == "" && actual.httpFallbackReady(*actual.listener, desired.fixedHTTPResponses) && externalAddressReady && len(plan) == 0
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
	groupedHTTP := handovers != nil
	var staleMemberWithdrawal bool
	var deleteDisables, deleteDisableRetries, deletions, memberDisables, memberDisableRetries []graphMutation
	var staging, fixedStaging, fixedTargetWithdrawals []graphMutation
	var weightedWithdrawals, targetRetries, closedListenerTargets []graphMutation
	queueTargetWithdrawal := func(step graphMutation, observed observedRouteGraph, fresh *[]graphMutation) {
		if desired.httpRequestPolicies == httpRequestPolicyUnverified && step.route.weightedTargets != nil {
			// A target PUT retaining the timed policy can require the very
			// capability we could not read. Close only this predicate.
			for _, policy := range observed.policies {
				if policy.policy.ID == step.route.id && policy.hasHTTPRequestPolicy() {
					if !observedPolicyDisabled(policy) {
						step.route = routeMutation{kind: routeMutationDisablePolicy, id: step.route.id, withdrawHTTPPolicy: true}
						step.message = step.route.message()
						*fresh = append(*fresh, step)
					}
					return
				}
			}
		}
		listener := actual.policyListener(observed, step.route.id)
		switch {
		case listener != nil && actual.adminObserved[listener.ID] && !listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE":
			closedListenerTargets = append(closedListenerTargets, step)
		case step.route.retry || listener != nil && listener.ProvisioningStatus == "ERROR":
			targetRetries = append(targetRetries, step)
		default:
			*fresh = append(*fresh, step)
		}
	}
	disables := make([]graphMutation, 0)
	ensures := make([]graphMutation, 0)
	var zeroWeightPoolEnsures []graphMutation
	var persistencePoolRecoveries []graphMutation
	routeResults := make([]cloud.RouteGraphResult, 0, len(desired.routes))
	canRestoreTraffic := !groupedHTTP || len(handovers.protected) == 0
	holdPolicyOrder := make(map[int]bool)
	policyPending := make(map[int]bool)
	policyCount := make(map[int]int32)
	holdPolicyEnable := make(map[int]bool)
	initialRulesReadback := make(map[int]bool)
	type redirectDomainKey struct {
		port  int
		scope string
	}
	holdRedirectEnable := make(map[redirectDomainKey]bool)
	redirectDomains := make(map[redirectDomainKey]bool)
	for index, route := range desired.routes {
		if route.redirect && desired.spec.Routes[index].Disposition == cloud.GraphEnsure {
			redirectDomains[redirectDomainKey{scopedHTTPRoutePort(route), route.spec.HTTPSListenerHostname}] = true
		}
	}
	for index, observed := range actual.routes {
		node := desired.spec.Routes[index]
		if groupedHTTP && node.Disposition == cloud.GraphEnsure && !policyPositionsMatch(desired.routes[index].policies, observed.policies) {
			holdPolicyEnable[scopedHTTPRoutePort(desired.routes[index])] = true
		}
		for _, policy := range observed.policies {
			port := portByListenerID[policy.policy.ListenerID]
			policyCount[port]++
			provedHandover := groupedHTTP && handovers.headerHandoverPredicates[policy.policy.ID]
			domain := redirectDomainKey{port, policy.httpsListenerHostname}
			if redirectDomains[domain] && !policy.redirect && !provedHandover && (node.Disposition != cloud.GraphEnsure ||
				!observedForwardPolicyMatches(desired.routes[index], observed, policy)) {
				holdRedirectEnable[domain] = true
			}
			if policy.initialRuleReadbackIncomplete() {
				initialRulesReadback[port] = true
				holdPolicyOrder[port] = true
				canRestoreTraffic = false
			}
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
		if node.Disposition == cloud.GraphPreserve && !desired.routes[index].mirrorOnly &&
			(observed.pool != nil || len(observed.policies) != 0) {
			canRestoreTraffic = false
		}
	}
	for index, route := range desired.routes {
		observed := observedRouteGraph{}
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		routeResult := cloud.RouteReadyResult(routeStateFromObservation(observed))
		routeResult.BackendSessionPersistenceIssue = route.sessionPersistenceIssue()
		var routePlan []routeMutation
		var backendIssue, ensureHold string
		optionalPool := route.zeroWeightOnly || route.mirrorOnly
		withdrawBackendTransport := route.backendTransportNeedsWithdrawal(observed)
		node := desired.spec.Routes[index]
		port := scopedHTTPRoutePort(route)
		redirectDomain := redirectDomainKey{port, route.spec.HTTPSListenerHostname}
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
				if observed.weightedPoolHeld {
					backendIssue = "Waiting for weighted HTTP policies to release the pool"
				}
			}
			if len(routePlan) == 0 && slices.ContainsFunc(observed.policies, observedRoutePolicy.initialRuleReadbackIncomplete) {
				ensureHold = initialL7RulesReadbackMessage
			}
		case node.Disposition == cloud.GraphEnsure:
			routePlan, routeResult.State, err = buildRouteEnsurePlanWithActivations(route, observed, p.l7PolicyBatchActivation)
			if errors.Is(err, errInitialL7RulesUnverified) {
				ensureHold = initialL7RulesReadbackMessage
				holdPolicyOrder[port] = true
				canRestoreTraffic = false
			} else if errors.Is(err, errHTTPRequestPolicyUnverified) {
				ensureHold = err.Error()
			} else if errors.Is(err, errRequestMirrorsUnverified) {
				ensureHold = err.Error()
			} else if errors.Is(err, errXBackendCookieUnavailable) || errors.Is(err, errXBackendCookieDeadlineUnavailable) || errors.Is(err, errXBackendCookieUnverified) ||
				errors.Is(err, errServiceSessionPersistenceAncestor) || errors.Is(err, errXBackendHeaderPersistence) || errors.Is(err, errXBackendCookiePermanentUnavailable) || errors.Is(err, errXBackendCookiePermanentReadback) || errors.Is(err, errXBackendCookiePathUnavailable) {
				ensureHold = err.Error()
			} else if errors.Is(err, errBackendRetryBudgetAncestor) || errors.Is(err, errBackendRetryBudgetCapability) || errors.Is(err, errBackendRetryBudgetReadback) {
				ensureHold = err.Error()
			} else if err != nil {
				return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
			}
			if withdrawBackendTransport && len(routePlan) == 0 {
				_, transportHold := httpMemberWithdrawal(observed)
				if ensureHold == "" {
					ensureHold = transportHold
				}
				canRestoreTraffic = canRestoreTraffic && transportHold == ""
			}
			if route.h2cTLSConflict(observed) {
				routeResult.UnsupportedBackendProtocol = "Octavia pool uses TLS, which prevents cleartext HTTP/2"
				if ensureHold == "" {
					ensureHold = routeResult.UnsupportedBackendProtocol
				}
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
			} else if backendIssue := observed.backendProvisioningIssue(route.unavailable && len(observed.members) != 0, scopedHTTP || route.backendKey != ""); !route.fixedResponse && backendIssue != "" {
				ensureHold = backendIssue
				canRestoreTraffic = canRestoreTraffic && optionalPool
				// A blocked earlier route can leave an empty slot in the policy
				// order. Octavia would clamp a subsequent insert to the end.
				holdPolicyOrder[port] = holdPolicyOrder[port] || !policyPositionsMatch(route.policies, observed.policies)
				if groupedHTTP && slices.ContainsFunc(observed.policies, func(policy observedRoutePolicy) bool { return !policy.policy.AdminStateUp }) {
					holdPolicyEnable[port] = true
				}
			}
		}
		if groupedHTTP {
			if handovers.drain[index] {
				if steps, hold := httpMemberWithdrawal(observed); len(steps) != 0 || hold != "" {
					routePlan, ensureHold = steps, hold
				}
			}
			if len(routePlan) > 1 && routePlan[0].kind == routeMutationEnablePolicy {
				blocked := slices.IndexFunc(routePlan, func(step routeMutation) bool {
					return handovers.protected[step.id] || handovers.protected[step.parentID]
				})
				if blocked >= 0 {
					// Keep the blocked step for the ordinary handover guard and
					// discard everything after it in this activation prefix.
					routePlan = routePlan[:blocked+1]
				}
			}
			routePlan = slices.DeleteFunc(routePlan, func(step routeMutation) bool {
				if handovers.protected[step.id] || handovers.protected[step.parentID] {
					routeResult.Outcome = p.progressingOutcome("Waiting for the replacement HTTP policy before withdrawing the old match")
					return true
				}
				if withdrawing && observed.weightedPoolHeld &&
					(step.kind == routeMutationDeleteMember || step.kind == routeMutationDeleteMonitor || step.kind == routeMutationDeletePool) {
					routeResult.Outcome = p.progressingOutcome(backendIssue)
					return true
				}
				return withdrawing && len(observed.policies) != 0 &&
					(step.kind == routeMutationDeleteMember || step.kind == routeMutationDeleteMonitor || step.kind == routeMutationDeletePool)
			})
			if route.unavailable && ensureHold == "" &&
				(len(routePlan) == 0 || routePlan[0].kind != routeMutationSetPolicyTarget) {
				_, ensureHold = httpMemberWithdrawal(observed)
			}
			if stage, ok := handovers.stages[index]; ok && !withdrawing && (ensureHold == "" || route.fixedResponse) &&
				actual.loadBalancer != nil && (!scopedHTTP && actual.listener != nil || listenerID != "") && !policyPending[port] && !initialRulesReadback[port] {
				steps, err := stage.plan(route, observed)
				if errors.Is(err, errInitialL7RulesUnverified) {
					routeResult.Outcome = p.progressingOutcome(initialL7RulesReadbackMessage)
					holdPolicyOrder[port] = true
					canRestoreTraffic = false
				} else if err != nil {
					return nil, result, &cloud.GraphError{Identity: route.spec.Identity, Err: err}
				}
				if len(steps) != 0 && steps[0].kind != routeMutationDisableMember {
					step := steps[0]
					step.policy.position = stage.wanted.position
					mutation := graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, listenerID: listenerID, message: step.message()}
					if route.httpFixedPolicy() {
						mutation.withdrawHTTPPolicy = true
						fixedStaging = append(fixedStaging, mutation)
					} else {
						staging = append(staging, mutation)
					}
					routeResult.Outcome = p.progressingOutcome(step.message())
				}
			}
			for _, step := range weightedTargetWithdrawals(route, observed) {
				queueTargetWithdrawal(graphMutation{kind: graphRouteMutation, routeIndex: index,
					route: step, listenerID: listenerID, message: step.message(), withdrawHTTPPolicy: true}, observed, &weightedWithdrawals)
				routeResult.Outcome = p.progressingOutcome(step.message())
			}
		}
		if node.Disposition == cloud.GraphEnsure && route.redirect && !withdrawing && holdRedirectEnable[redirectDomain] {
			routeResult.Outcome = p.progressingOutcome("Waiting for verified forwarding predicates on the redirect listener")
			var withdrawal []routeMutation
			for _, policy := range observed.policies {
				if !policy.pending() && (policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR") {
					withdrawal = append(withdrawal, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID,
						retry: policy.policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: true})
				}
			}
			if len(withdrawal) > 0 {
				routePlan = withdrawal
			}
		}
		if len(routePlan) != 0 {
			routeResult.Outcome = p.progressingOutcome(routePlan[0].message())
		} else if route.backendTLS.issue != "" {
			routeResult.Outcome = p.progressingOutcome(route.backendTLS.issue)
		} else if node.Disposition == cloud.GraphEnsure && route.xBackendH2CPending {
			routeResult.Outcome = p.progressingOutcome("Waiting to verify regional h2c support")
		}
		if errors.Is(route.redirectIssue, errExactHeaderRedirectPrecedence) || errors.Is(route.redirectIssue, errHTTPMethodRedirectPrecedence) ||
			errors.Is(route.redirectIssue, errExactQueryParamRedirectPrecedence) {
			routeResult.UnsupportedMatch = route.redirectIssue.Error()
		} else if route.redirectIssue != nil {
			routeResult.Outcome = p.progressingOutcome(route.redirectIssue.Error())
		}
		if route.targetsPending {
			canRestoreTraffic = false
			if routeResult.Outcome.State == cloud.OutcomeReady {
				routeResult.Outcome = p.progressingOutcome("Waiting for owned Octavia weighted HTTP target pools")
			}
		}
		if ensureHold != "" {
			routeResult.Outcome = p.progressingOutcome(ensureHold)
			if groupedHTTP && (route.unavailable && !optionalPool || handovers.drain[index]) {
				canRestoreTraffic = false
			}
		}
		hasRetryBackoff := slices.ContainsFunc(route.policies, func(policy desiredPolicy) bool { return policy.requestPolicy.HasRetryBackoff() })
		if (desired.httpRequestPolicies.unavailable() || hasRetryBackoff) && node.Disposition == cloud.GraphEnsure && !relocating && frontendReady {
			policyListenerID := listenerID
			if !scopedHTTP && actual.listener != nil {
				policyListenerID = actual.listener.ID
			}
			routeResult.ImplementedRules = hasProgrammedHTTPRule(route, observed, policyListenerID)
		}
		if groupedHTTP && p.initialL7Rules && !staleMemberWithdrawal && ensureHold == "" {
			// Keep the normal backend order while a stale member needs to
			// be removed, even if a pool update must precede its deletion.
			staleMemberWithdrawal = slices.ContainsFunc(routePlan, func(step routeMutation) bool {
				return step.kind == routeMutationDeleteMember
			})
		}
		for stepIndex, step := range routePlan {
			if groupedHTTP && step.kind == routeMutationEnablePolicy {
				for _, policy := range observed.policies {
					if policy.policy.ID == step.id {
						step.policy.position = policy.policy.Position
						break
					}
				}
			}
			mutation := graphMutation{kind: graphRouteMutation, routeIndex: index, route: step, listenerID: listenerID, message: step.message()}
			withdrawPolicy := step.withdrawHTTPPolicy || withdrawing &&
				(step.kind == routeMutationDisablePolicy || step.kind == routeMutationDeleteRule || step.kind == routeMutationDeletePolicy)
			fixTarget := route.fixedResponse && step.kind == routeMutationSetPolicyTarget
			memberWithdrawal := step.kind == routeMutationDisableMember
			mutation.withdrawHTTPPolicy = withdrawBackendTransport && memberWithdrawal ||
				(scopedHTTP || groupedHTTP) && (withdrawPolicy || memberWithdrawal || fixTarget)
			switch {
			case fixTarget:
				if stepIndex == 0 && ensureHold == "" {
					queueTargetWithdrawal(mutation, observed, &fixedTargetWithdrawals)
				}
			case step.kind == routeMutationDisableMember:
				if observed.backendProvisioningIssue(true, true) != "" {
					continue
				}
				if step.retry {
					memberDisableRetries = append(memberDisableRetries, mutation)
				} else {
					memberDisables = append(memberDisables, mutation)
				}
			case withdrawPolicy && step.kind == routeMutationDisablePolicy:
				if step.retry {
					deleteDisableRetries = append(deleteDisableRetries, mutation)
				} else {
					deleteDisables = append(deleteDisables, mutation)
				}
			case withdrawing || step.withdrawHTTPPolicy:
				if policyPending[observedPort] {
					routeResult.Outcome = p.progressingOutcome("Waiting for Octavia L7 policy or rule provisioning before cleanup")
				} else if backendIssue != "" && (step.kind == routeMutationDeleteMember || step.kind == routeMutationDeleteMonitor || step.kind == routeMutationDeletePool) {
					routeResult.Outcome = p.progressingOutcome(backendIssue)
				} else {
					deletions = append(deletions, mutation)
				}
			case step.kind == routeMutationDisablePolicy:
				disables = append(disables, mutation)
			case groupedHTTP && step.kind == routeMutationUpdateMember && step.retry:
				// A granted backend can repair an owned member left ERROR
				// by an earlier disable or a failed re-enable. Other backend
				// changes still wait for the pending mutation to settle.
				if observed.backendProvisioningIssue(true, true) == "" {
					ensures = append(ensures, mutation)
				}
			case step.kind == routeMutationSetXBackendPersistence && step.retry:
				// Recovery of an ERROR pool may fail repeatedly. Finish
				// other routes' withdrawals and ready mutations first.
				if actual.loadBalancer != nil && actual.loadBalancer.ProvisioningStatus == "ACTIVE" {
					persistencePoolRecoveries = append(persistencePoolRecoveries, mutation)
				}
			case ensureHold != "":
				continue
			case route.redirect && step.kind == routeMutationEnablePolicy && (holdRedirectEnable[redirectDomain] || holdPolicyOrder[port]):
				routeResult.Outcome = p.progressingOutcome("Waiting for verified HTTP forwarding policies before enabling the redirect")
			case groupedHTTP && stepIndex != 0 && (routePlan[0].kind != routeMutationEnablePolicy || step.kind != routeMutationEnablePolicy):
				// Prepared activations share the same proved backend. Otherwise
				// a later change may depend on a readback of the first one.
				continue
			case !groupedHTTP && holdPolicyOrder[port] && (isHTTPPolicyPositionChange(step.kind) || step.kind == routeMutationEnablePolicy):
				routeResult.Outcome = p.progressingOutcome("Waiting for the HTTPRoute policy order")
			case groupedHTTP && (step.kind == routeMutationCreatePolicy && step.policy.position > policyCount[port]+1 ||
				step.kind == routeMutationMovePolicy && step.policy.position > policyCount[port]):
				routeResult.Outcome = p.progressingOutcome("Waiting for the HTTPRoute policy order")
			case optionalPool:
				zeroWeightPoolEnsures = append(zeroWeightPoolEnsures, mutation)
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
	if desired.protocol == cloud.ListenerProtocolHTTP {
		if step, ok := nextHTTPRequestPolicyWithdrawal(desired, actual); ok {
			result.Outcome = p.progressingOutcome(step.message)
			result.Routes[step.routeIndex].Result.Outcome = result.Outcome
			return []graphMutation{step}, result, nil
		}
	}
	// A failed teardown for one route must never leave another revoked route
	// forwarding. Close all actionable policies, including fresh disables ahead
	// of retries for previously failed disables, before touching descendants.
	if scopedHTTP || groupedHTTP || len(memberDisables) != 0 || len(memberDisableRetries) != 0 {
		// Prioritize verified withdrawals even when earlier prerequisites need
		// repair. Withdraw fresh members first, then let an equivalent fixed
		// response take over if member withdrawal requires a retry.
		slices.SortStableFunc(fixedStaging, func(a, b graphMutation) int { return cmp.Compare(a.route.policy.position, b.route.policy.position) })
		for _, steps := range [][]graphMutation{weightedWithdrawals, memberDisables, fixedTargetWithdrawals, deleteDisables, fixedStaging, targetRetries, memberDisableRetries, deleteDisableRetries, deletions, closedListenerTargets} {
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
	slices.SortStableFunc(staging, func(a, b graphMutation) int { return cmp.Compare(a.route.policy.position, b.route.policy.position) })
	plan = append(plan, staging...)
	plan = append(plan, disables...)
	plan = append(plan, listenerChanges...)
	// Ensured routes are ranked, and each route's policies are ordered within
	// its rank. Each new call refreshes the list after Octavia shifts positions.
	// Filling this prefix first keeps inserts and moves within the current list.
	if groupedHTTP {
		var preparedTLS []graphMutation
		ensures = slices.DeleteFunc(ensures, func(step graphMutation) bool {
			port := scopedHTTPRoutePort(desired.routes[step.routeIndex])
			if handovers.ports[port] && isHTTPPolicyMutation(step.route.kind) {
				return true
			}
			switch step.route.kind {
			case routeMutationCreatePolicy, routeMutationEnablePolicy:
				if holdPolicyOrder[port] {
					return true
				}
				listenerID := step.listenerID
				if !scopedHTTP && gatewayReady && actual.listener.ProvisioningStatus == "ACTIVE" {
					listenerID = actual.listener.ID
				}
				frontendReady := scopedHTTP && slices.Contains(result.Gateway.ScopedListenerReadyPorts, port) ||
					!scopedHTTP && gatewayReady
				if p.initialL7Rules && !staleMemberWithdrawal && frontendReady && listenerID != "" &&
					independentPreparedTLSPolicy(desired, actual, step, listenerID, port, policyCount[port]) {
					preparedTLS = append(preparedTLS, step)
					return true
				}
				return step.route.kind == routeMutationEnablePolicy && holdPolicyEnable[port]
			case routeMutationMovePolicy:
				return holdPolicyOrder[port]
			default:
				return false
			}
		})
		slices.SortStableFunc(preparedTLS, func(a, b graphMutation) int {
			return cmp.Compare(a.route.policy.position, b.route.policy.position)
		})
		plan = append(plan, preparedTLS...)
		orderHTTPBackendMutations(ensures)
	}
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
	plan = append(plan, zeroWeightPoolEnsures...)
	if len(plan) == 0 && scopedHTTP && desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		if step := actual.nextFrontendCAMutation(desired); step != nil {
			plan = append(plan, *step)
		}
	}
	plan = append(plan, persistencePoolRecoveries...)
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
	if hasHTTPRouteParts(desired, actual) {
		desired, actual, _ = httpPlanningParts(desired, actual)
	}
	for index, node := range desired.spec.Routes {
		if node.WaitForHTTPPolicyRemoval || node.Disposition == cloud.GraphDelete {
			if index >= len(actual.routes) || len(actual.routes[index].policies) != 0 {
				return "Waiting for outdated HTTP policy removal"
			}
		}
		if node.Disposition == cloud.GraphEnsure && index < len(actual.routes) {
			for _, policy := range actual.routes[index].policies {
				if policyNeedsHostnameUpdate(desired.routes[index], policy) {
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
		if index < len(actual.routes) && (actual.routes[index].pool != nil || len(actual.routes[index].policies) != 0 || len(actual.routes[index].httpParts) != 0) {
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
	if !actual.scoped && actual.listener != nil && actual.listener.DefaultPoolID == "" &&
		desired.fixedHTTPResponses && actual.listener.ProvisioningStatus == "ACTIVE" && actual.listener.Protocol == cloud.ListenerProtocolHTTP && actual.defaultHTTPResponseCodes[actual.listener.ID] != 404 {
		plan = append(plan, graphMutation{kind: graphSetListenerNotFound, listenerID: actual.listener.ID, message: "Setting the default HTTP response on the Gateway listener"})
	}
	if actual.listener == nil && !actual.scoped {
		plan = append(plan, graphMutation{kind: graphCreateListener, message: "Created Octavia listener"})
	}
	return plan, nil
}

func buildGatewayTrafficRestorationPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, error) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.loadBalancer == nil || actual.listener == nil ||
		actual.listener.DefaultPoolID != "" && !usesDefaultPool(actual.protocol) || !actual.httpFallbackReady(*actual.listener, desired.fixedHTTPResponses) {
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
				if owner := defaultPoolOwner(&listener, actual.routes); owner != -1 && actual.routes[owner].udpDropPool() {
					return []graphMutation{deleteDefaultUDPPacketDropPool(listener.DefaultPoolID)}
				}
				mutation := clearListenerDefaultPool(actual.protocol)
				mutation.listenerID = listener.ID
				return []graphMutation{mutation}
			}
		}
	}
	if listener := actual.deletion.defaultPoolListener; listener != nil && listener.DefaultPoolID != "" {
		if pool := actual.deletion.defaultUDPPacketDropPool; pool != "" {
			return []graphMutation{deleteDefaultUDPPacketDropPool(pool)}
		}
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

func deleteDefaultUDPPacketDropPool(poolID string) graphMutation {
	return graphMutation{kind: graphDeleteChild, deletion: gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: poolID},
		message: "Deleting the Octavia UDP packet drop pool and its default edge"}
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

type unavailableRouteCapability struct {
	error
	capability                 string
	unsupportedBackendProtocol bool
}

func (e *unavailableRouteCapability) Unwrap() error { return e.error }

// prepareNextGraphMutation can stage a sibling's owned backend during a
// route capability outage. A policy on another physical listener has an
// independent order and can also proceed when a predicate capability is unavailable.
func (p *Provider) prepareNextGraphMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) (graphMutation, bool, error) {
	first := plan[0]
	step, err := p.prepareGraphMutation(ctx, desired, actual, first)
	var blocked *unavailableRouteCapability
	if !errors.As(err, &blocked) {
		return step, err == nil, err
	}

	recordBlocked := func(step graphMutation, blocked *unavailableRouteCapability) {
		message := "Waiting for Octavia to advertise " + blocked.capability
		route := &result.Routes[step.routeIndex].Result
		route.Outcome = p.progressingOutcome(message)
		if blocked.unsupportedBackendProtocol && route.UnsupportedBackendProtocol == "" {
			route.UnsupportedBackendProtocol = message
		}
	}
	recordBlocked(first, blocked)
	blockedHTTPPolicy := blocked.capability == octavia.GatewayCapabilityHTTPHeaderExact || blocked.capability == octavia.GatewayCapabilityHTTPMethodMatch ||
		blocked.capability == octavia.GatewayCapabilityHTTPQueryParamExact || blocked.capability == octavia.GatewayCapabilityHTTPRequestRedirect ||
		blocked.capability == octavia.GatewayCapabilityRequestURLRewrite || blocked.capability == octavia.GatewayCapabilityRequestCORS ||
		blocked.capability == octavia.GatewayCapabilityTargetRequestFilters || blocked.capability == octavia.GatewayCapabilityTargetResponseHeaders ||
		blocked.capability == octavia.GatewayCapabilityHTTPRequestMirror ||
		blocked.capability == octavia.GatewayCapabilityGRPCRequestMirror ||
		blocked.capability == octavia.GatewayCapabilityRequestHeaderOperations && rewritePolicyWriteRequiresCapability(graphRoutePart(desired, first), first.route) ||
		blocked.capability == octavia.GatewayCapabilityResponseHeaderOperations || blocked.capability == octavia.GatewayCapabilityGRPCContentType ||
		blocked.capability == octavia.GatewayCapabilityGRPCBinaryMetadata || blocked.capability == octavia.GatewayCapabilityGRPCBinaryRequestMetadata
	result.Outcome = result.Routes[first.routeIndex].Result.Outcome
	seen := map[poolRouteOwner]bool{{route: first.routeIndex, backend: first.backendKey}: true}
	for _, sibling := range plan[1:] {
		if sibling.kind != graphRouteMutation {
			continue
		}
		owner := poolRouteOwner{route: sibling.routeIndex, backend: sibling.backendKey}
		if seen[owner] {
			continue
		}
		// Each pool can contribute only its first planned action. Subsequent
		// actions may rely on a write whose result has yet to be observed.
		seen[owner] = true
		independentListener := blockedHTTPPolicy && first.listenerID != "" && sibling.listenerID != "" && first.listenerID != sibling.listenerID
		switch sibling.route.kind {
		case routeMutationCreatePool, routeMutationUpdatePool, routeMutationSetBackendRetryBudget, routeMutationCreateMember, routeMutationUpdateMember:
		default:
			if !independentListener || !isHTTPPolicyMutation(sibling.route.kind) {
				continue
			}
		}
		prepared, err := p.prepareGraphMutation(ctx, desired, actual, sibling)
		if err == nil {
			return prepared, true, nil
		}
		if errors.As(err, &blocked) {
			recordBlocked(sibling, blocked)
		}
	}
	return graphMutation{}, false, nil
}

// prepareGraphMutation resolves provider prerequisites before the caller's
// live Kubernetes authorization check, which stays adjacent to the write.
func (p *Provider) prepareGraphMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (graphMutation, error) {
	if isSNIProtocol(desired.protocol) {
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure && !step.sniWithdrawal {
			if desired.sniBackendPoolDiscoveryPending && (step.kind == graphEnableListener || step.kind == graphEnableLoadBalancer) {
				for _, fragment := range actual.sni {
					if fragment.backendPools != nil && fragment.policy != nil && fragment.policy.AdminStateUp {
						return step, fmt.Errorf("waiting for weighted TLS SNI capability discovery before reopening the frontend")
					}
				}
			}
			return step, p.verifySNICapability(ctx, desired, actual)
		}
		return step, nil
	}
	if desired.protocol != cloud.ListenerProtocolHTTP {
		return step, nil
	}
	if step.kind == graphRouteMutation && step.route.policy.httpsListenerHostname != "" &&
		(step.route.kind == routeMutationCreatePolicy || step.route.kind == routeMutationEnablePolicy ||
			step.route.kind == routeMutationSetPolicyHTTPSListenerHostname) {
		port := scopedHTTPRoutePort(graphRoutePart(desired, step))
		spec := scopedHTTPListeners(desired)[port]
		i := slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
			return listener.ID == step.listenerID && listener.ProtocolPort == port
		})
		if i < 0 || !actual.scopedHTTPListenerMatches(actual.scopedListeners[i], spec) ||
			!slices.Contains(httpsVirtualHosts(spec), step.route.policy.httpsListenerHostname) {
			return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for verified HTTPS listener isolation"))
		}
		listener := actual.scopedListeners[i]
		if step.route.kind == routeMutationSetPolicyHTTPSListenerHostname && (listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE") {
			return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for a closed HTTPS listener before policy scope repair"))
		}
	}
	var required []string
	var mirrorCapabilities []string
	var reopeningPersistenceCapability string
	var authBodySize int32
	fixed := desired.fixedHTTPResponses
	switch step.kind {
	case graphCreateLoadBalancer:
		if desired.spec.Gateway.Spec.ScopedListeners != nil &&
			!slices.ContainsFunc(desired.spec.Gateway.Spec.ScopedListeners, func(spec cloud.ScopedListener) bool {
				return hasAuthorizedListener(spec) && len(httpsVirtualHosts(spec)) == 0
			}) {
			if !p.httpsListenerIsolation {
				return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("controller opt-in for Octavia HTTPS listener isolation is disabled"))
			}
			required = append(required, octavia.GatewayCapabilityHTTPSListenerIsolation)
		}
	case graphCreateListener, graphSetListenerNotFound, graphEnableListener, graphEnableLoadBalancer:
	case graphListenerFiltersMutation:
		auth := step.listenerFilters.protectedAuth
		if auth == nil {
			return step, nil
		}
		fixed = false
		required = append(required, routeAuthorizerTransportCapabilities(*auth)...)
	case graphRouteMutation:
		part := graphRoutePart(desired, step)
		if !part.backendTLS.denies() && step.route.kind == routeMutationSetBackendTLS && step.route.poolAdmin != nil && *step.route.poolAdmin {
			// Octavia reauthorizes TLS on reopen. The PUT leaves persistence
			// unchanged, so recheck its capabilities as well.
			switch {
			case part.authenticatedH2Cookie() || part.protectedInlinePersistence() && part.sessionCookieName != "":
				reopeningPersistenceCapability = octavia.GatewayCapabilityHTTPCookieName
				required = append(required, poolSessionCookieCapabilities(part.sessionCookieDeadline.Requirements().WithPath(part.sessionCookiePath))...)
			case part.protectedInlinePersistence() && part.sessionHeader != nil:
				reopeningPersistenceCapability = octavia.GatewayCapabilityHTTPHeaderPersistence
				required = append(required, reopeningPersistenceCapability)
			}
		}
		if step.route.policy.externalAuth != nil && (step.route.kind == routeMutationCreatePolicy || step.route.kind == routeMutationEnablePolicy) {
			if step.route.policy.resolvedExternalAuth == nil {
				return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for the HTTPRoute authorization pool"))
			}
			authBodySize = step.route.policy.externalAuth.ForwardBodyMaxSize
			required = append(required, routeAuthorizerCapabilities(*step.route.policy.externalAuth, step.route.policy.weighted)...)
		}
		fixed = fixed && routeNeedsHTTPCapability(part, step.route)
		if fixed && (part.weightedTargets != nil || step.route.weightedTargets != nil) {
			required = append(required, octavia.GatewayCapabilityWeightedHTTPTargets)
		}
		if requestMirrorWriteRequiresCapability(step.route) {
			mirrorCapabilities = requestMirrorCapabilities(part.spec.GRPC)
			required = append(required, mirrorCapabilities...)
		}
		if redirectPolicyWriteRequiresCapability(part, step.route) {
			required = append(required, octavia.GatewayCapabilityHTTPRequestRedirect)
		}
		if backendRequestFilterWriteRequiresCapability(part, step.route) {
			required = append(required, octavia.GatewayCapabilityTargetRequestFilters)
		}
		if backendResponseHeaderWriteRequiresCapability(part, step.route) {
			required = append(required, octavia.GatewayCapabilityTargetResponseHeaders)
			// Octavia exposes target response headers only with the regional
			// rule-level response header and weighted target capabilities.
			required = append(required, octavia.GatewayCapabilityResponseHeaderOperations)
		}
		if corsPolicyWriteRequiresCapability(part, step.route) {
			required = append(required, octavia.GatewayCapabilityRequestCORS)
		}
		requiresRewrite := rewritePolicyWriteRequiresCapability(part, step.route)
		if requiresRewrite {
			required = append(required, octavia.GatewayCapabilityRequestURLRewrite)
		}
		if step.route.reenableH2CPool || step.route.backendH2C != nil && *step.route.backendH2C ||
			step.route.kind == routeMutationCreatePool && !step.route.persistenceFallback && part.spec.BackendH2C && part.backendTLS.allowsMembers() {
			required = append(required, octavia.GatewayCapabilityBackendH2C)
		}
		if len(step.route.requestHeaderOperations) != 0 || requiresRewrite && len(step.route.policy.requestHeaderOperations) != 0 {
			required = append(required, octavia.GatewayCapabilityRequestHeaderOperations)
		}
		if len(step.route.responseHeaderOperations) != 0 || requiresRewrite && len(step.route.policy.responseHeaderOperations) != 0 {
			required = append(required, octavia.GatewayCapabilityResponseHeaderOperations)
		}
		binaryResponseMetadata := step.route.requiresGRPCBinaryResponseMetadata()
		if binaryResponseMetadata {
			required = append(required, octavia.GatewayCapabilityGRPCBinaryMetadata)
		}
		binaryRequestMetadata := step.route.requiresGRPCBinaryRequestMetadata()
		if binaryRequestMetadata {
			required = append(required, octavia.GatewayCapabilityGRPCBinaryRequestMetadata)
		}
		if step.route.requiresExactHeaders || len(step.route.policy.headers) != 0 && step.route.kind == routeMutationCreatePolicy ||
			step.route.rule.ruleType == httpHeaderExactRuleType {
			required = append(required, octavia.GatewayCapabilityHTTPHeaderExact)
		}
		if step.route.requiresExactQueryParams || step.route.policy.requiresExactQueryParams() && step.route.kind == routeMutationCreatePolicy ||
			step.route.rule.ruleType == httpQueryParamExactRuleType {
			required = append(required, octavia.GatewayCapabilityHTTPQueryParamExact)
		}
		if step.route.requiresHTTPMethod || step.route.policy.method != "" && step.route.kind == routeMutationCreatePolicy ||
			step.route.rule.ruleType == httpMethodRuleType {
			required = append(required, octavia.GatewayCapabilityHTTPMethodMatch)
		}
		if binaryRequestMetadata || binaryResponseMetadata || step.route.probeBeforeGRPCMigration || step.route.rule.ruleType == grpcContentTypeRuleType ||
			step.route.policy.grpcContentTypeRule == grpcContentTypeRuleType &&
				(step.route.kind == routeMutationCreatePolicy || step.route.kind == routeMutationEnablePolicy) {
			required = append(required, octavia.GatewayCapabilityGRPCContentType)
		}
	default:
		return step, nil
	}
	if fixed {
		required = append(required, octavia.GatewayCapabilityFixedHTTPResponses)
	}
	if len(required) == 0 {
		return p.preparePolicyCreation(ctx, actual, desired, step)
	}
	withdrawTarget := step.kind == graphRouteMutation && step.withdrawHTTPPolicy && step.route.kind == routeMutationSetPolicyTarget
	verify := p.verifyGatewayHTTPRequirements
	if slices.Contains(required, octavia.GatewayCapabilityHTTPRequestRedirect) || slices.Contains(required, octavia.GatewayCapabilityRequestURLRewrite) ||
		slices.Contains(required, octavia.GatewayCapabilityTargetRequestFilters) || slices.Contains(required, octavia.GatewayCapabilityTargetResponseHeaders) ||
		slices.Contains(required, octavia.GatewayCapabilityRequestCORS) ||
		len(mirrorCapabilities) != 0 {
		verify = p.verifyGatewayHTTPURLRequirements
	}
	err := verify(ctx, desired, actual, required, authBodySize)
	if err != nil && reopeningPersistenceCapability != "" {
		var missing *octavia.MissingGatewayCapability
		capability := reopeningPersistenceCapability
		if errors.As(err, &missing) {
			capability = missing.Capability
		}
		return step, &unavailableRouteCapability{error: err, capability: capability}
	}
	if err != nil && len(mirrorCapabilities) != 0 {
		var missingMirror *octavia.MissingGatewayCapability
		capability := mirrorCapabilities[len(mirrorCapabilities)-1]
		if errors.As(err, &missingMirror) {
			capability = missingMirror.Capability
		}
		if slices.Contains(mirrorCapabilities, capability) {
			return step, &unavailableRouteCapability{error: err, capability: capability}
		}
	}
	var missingResponseHeaders *octavia.MissingGatewayCapability
	if withdrawTarget && len(step.route.responseHeaderOperations) != 0 &&
		!backendResponseHeaderWriteRequiresCapability(graphRoutePart(desired, step), step.route) &&
		errors.As(err, &missingResponseHeaders) && isResponseHeaderCapability(missingResponseHeaders.Capability) {
		// Retain the installed operations if the region no longer accepts
		// new ones. The restrictive action still needs its own capability.
		step.route.responseHeaderOperations = nil
		required = slices.DeleteFunc(required, isResponseHeaderCapability)
		err = verify(ctx, desired, actual, required, authBodySize)
	}
	if err != nil {
		// Removing a withdrawal predicate would expose a lower-priority
		// route. Close its verified listener until the target is updated.
		if withdrawTarget && step.routeIndex < len(actual.routes) {
			listener := actual.policyListener(actual.routes[step.routeIndex], step.route.id)
			if listener != nil && (listener.ProvisioningStatus == "ERROR" || listener.ProvisioningStatus == "ACTIVE" && (listener.AdminStateUp || !actual.adminObserved[listener.ID])) {
				return graphMutation{kind: graphDisableListener, listenerID: listener.ID,
					scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort},
					redactTLSError: listener.Protocol == terminatedHTTPS, withdrawHTTPPolicy: true,
					message: "Closing the affected listener until Octavia can apply the HTTP target withdrawal"}, nil
			}
		}
		if step.kind == graphRouteMutation {
			var missing *octavia.MissingGatewayCapability
			explicitlyMissing := errors.As(err, &missing)
			if step.route.probeBeforeGRPCMigration && explicitlyMissing && missing.Capability == octavia.GatewayCapabilityGRPCContentType {
				// Regional opt-in requires the native predicate. An explicit
				// negative response authorizes withdrawing the old policy.
				return step, nil
			}
			err = &cloud.GraphError{Identity: desired.routes[step.routeIndex].spec.Identity, Err: err}
			if slices.Contains(required, octavia.GatewayCapabilityBackendH2C) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityBackendH2C) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityBackendH2C,
					unsupportedBackendProtocol: explicitlyMissing && graphRoutePart(desired, step).spec.BackendH2C}
			} else if slices.Contains(required, octavia.GatewayCapabilityRequestHeaderOperations) && !step.withdrawHTTPPolicy &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityRequestHeaderOperations) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityRequestHeaderOperations}
			} else if slices.Contains(required, octavia.GatewayCapabilityTargetResponseHeaders) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityTargetResponseHeaders) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityTargetResponseHeaders}
			} else if slices.Contains(required, octavia.GatewayCapabilityResponseHeaderOperations) && !step.withdrawHTTPPolicy &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityResponseHeaderOperations) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityResponseHeaderOperations}
			} else if slices.Contains(required, octavia.GatewayCapabilityGRPCBinaryMetadata) && !step.withdrawHTTPPolicy &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityGRPCBinaryMetadata) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityGRPCBinaryMetadata}
			} else if slices.Contains(required, octavia.GatewayCapabilityGRPCBinaryRequestMetadata) && !step.withdrawHTTPPolicy &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityGRPCBinaryRequestMetadata) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityGRPCBinaryRequestMetadata}
			} else if slices.Contains(required, octavia.GatewayCapabilityHTTPHeaderExact) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityHTTPHeaderExact) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityHTTPHeaderExact}
			} else if slices.Contains(required, octavia.GatewayCapabilityHTTPQueryParamExact) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityHTTPQueryParamExact) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityHTTPQueryParamExact}
			} else if slices.Contains(required, octavia.GatewayCapabilityRequestCORS) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityRequestCORS) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityRequestCORS}
			} else if slices.Contains(required, octavia.GatewayCapabilityTargetRequestFilters) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityTargetRequestFilters) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityTargetRequestFilters}
			} else if slices.Contains(required, octavia.GatewayCapabilityRequestURLRewrite) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityRequestURLRewrite) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityRequestURLRewrite}
			} else if slices.Contains(required, octavia.GatewayCapabilityHTTPRequestRedirect) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityHTTPRequestRedirect) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityHTTPRequestRedirect}
			} else if slices.Contains(required, octavia.GatewayCapabilityHTTPMethodMatch) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityHTTPMethodMatch) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityHTTPMethodMatch}
			} else if slices.Contains(required, octavia.GatewayCapabilityGRPCContentType) &&
				(!explicitlyMissing || missing.Capability == octavia.GatewayCapabilityGRPCContentType) {
				err = &unavailableRouteCapability{error: err, capability: octavia.GatewayCapabilityGRPCContentType}
			}
			return step, err
		}
		return step, err
	}
	return p.preparePolicyCreation(ctx, actual, desired, step)
}

func (step routeMutation) requiresGRPCBinaryRequestMetadata() bool {
	if step.policy.grpcContentTypeRule != grpcContentTypeRuleType {
		return false
	}
	if hasGRPCBinaryMetadataList(step.requestHeaderOperations) {
		return true
	}
	if step.weightedTargets != nil {
		return hasGRPCBinaryTargetRequestMetadata(*step.weightedTargets)
	}
	return step.kind == routeMutationEnablePolicy && hasGRPCBinaryTargetRequestMetadata(step.policy.weightedFilterTargets())
}

func (step routeMutation) requiresGRPCBinaryResponseMetadata() bool {
	if step.policy.grpcContentTypeRule != grpcContentTypeRuleType {
		return false
	}
	if hasGRPCBinaryMetadataList(step.responseHeaderOperations) {
		return true
	}
	if step.weightedTargets != nil {
		return hasGRPCBinaryTargetResponseMetadata(*step.weightedTargets)
	}
	return step.kind == routeMutationEnablePolicy && hasGRPCBinaryTargetResponseMetadata(step.policy.weightedFilterTargets())
}

func isResponseHeaderCapability(capability string) bool {
	return capability == octavia.GatewayCapabilityResponseHeaderOperations || capability == octavia.GatewayCapabilityGRPCBinaryMetadata
}

func (actual observedGatewayGraph) policyListener(route observedRouteGraph, id string) *listeners.Listener {
	for _, part := range route.parts() {
		for _, policy := range part.policies {
			if policy.policy.ID != id || !policy.adminObserved || policy.pending() || policy.policy.ListenerID == "" {
				continue
			}
			listenerID := policy.policy.ListenerID
			if !actual.scoped && actual.listener != nil && actual.listener.ID == listenerID {
				return actual.listener
			}
			for i := range actual.scopedListeners {
				if actual.scopedListeners[i].ID == listenerID {
					return &actual.scopedListeners[i]
				}
			}
			return nil
		}
	}
	return nil
}

func graphRoutePart(desired desiredGatewayGraph, step graphMutation) desiredRoute {
	route := desired.routes[step.routeIndex]
	for _, part := range route.httpParts {
		if part.backendKey == step.backendKey {
			return part
		}
	}
	return route
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
		if isSNIProtocol(desired.protocol) {
			tags = append(tags, sniListenerProfileTag(desired.protocol))
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
	case graphListenerFiltersMutation:
		err = p.executeListenerFilterMutation(ctx, desired, actual, step)
		if auth := step.listenerFilters.protectedAuth; auth == nil || auth.BackendTLS == nil || step.listenerFilters.claimCA {
			return err
		}
		step.redactTLSError = true
	case graphCreateListener:
		tags, tagErr := desired.identity.GatewayTags(roleListener)
		if tagErr != nil {
			return tagErr
		}
		port, name := spec.ListenerPort, spec.ListenerName
		protocol := cloud.NormalizeListenerProtocol(spec.ListenerProtocol)
		enabled := true
		if isSNIProtocol(desired.protocol) {
			protocol, enabled = cloud.ListenerProtocolTCP, false
			tags = append(tags, sniListenerProfileTag(desired.protocol))
		}
		certificateRef := desired.spec.Gateway.TCPTLSCertificateRef
		if step.scopedListener.Port != 0 {
			port, name = step.scopedListener.Port, step.scopedListener.Name
			if tls := step.scopedListener.TLS; tls != nil {
				protocol, enabled, certificateRef = terminatedHTTPS, false, tls.CertificateRef
			}
			tags = append(tags, scopedPortTag(protocol, port))
		}
		if len(spec.RequestFilterSections[port]) != 0 || desired.plainHTTPRuleLimits[port] != "" || slices.ContainsFunc(desired.routes, func(route desiredRoute) bool {
			return slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
				return httpURLPolicyRoutePort(desired, part) == port && slices.ContainsFunc(part.policies, func(policy desiredPolicy) bool {
					return requestRedirectPolicy.required(policy) || requestURLRewritePolicy.required(policy) || requestCORSPolicy.required(policy)
				})
			})
		}) {
			enabled = false
		}
		opts := listeners.CreateOpts{
			LoadbalancerID: actual.state.LoadBalancerID, Protocol: listeners.Protocol(protocol), ProtocolPort: port,
			Name: name, Description: desired.identity.Description(roleListener), AdminStateUp: boolPointer(enabled),
			DefaultTlsContainerRef: certificateRef, Tags: tags,
		}
		var frontendFallback bool
		if step.scopedListener.TLS != nil && step.scopedListener.TLS.ClientValidation != nil {
			opts.ClientAuthentication = listeners.ClientAuthenticationMandatory
			opts.ClientCATLSContainerRef = actual.frontendCA[port].ref
			frontendFallback = step.scopedListener.TLS.ClientValidation.InsecureFallback
			if frontendFallback {
				opts.Tags = append(opts.Tags, frontendFallbackTag)
			}
		}
		if cloud.IsTCPTLSListenerProtocol(desired.protocol) {
			_, err = p.octavia.CreateTCPTLSListener(ctx, opts, desired.protocol == cloud.ListenerProtocolTLSMixed)
			step.redactTLSError = true
		} else if protocol == terminatedHTTPS {
			hosts := httpsVirtualHosts(step.scopedListener)
			if len(hosts) != 0 {
				opts.Tags = append(opts.Tags, httpsIsolationTag)
			}
			_, err = p.octavia.CreateHTTPSListener(ctx, opts, hosts, desired.fixedHTTPResponses, frontendFallback)
		} else if desired.fixedHTTPResponses && protocol == cloud.ListenerProtocolHTTP {
			_, err = p.octavia.CreateListenerWithNotFound(ctx, opts)
		} else {
			_, err = p.octavia.CreateListener(ctx, opts)
		}
	case graphSetListenerNotFound:
		err = p.octavia.SetListenerNotFound(ctx, listenerID)
	case graphUpdateListenerCertificate:
		ref := desired.spec.Gateway.TCPTLSCertificateRef
		if step.scopedListener.TLS != nil {
			ref = step.scopedListener.TLS.CertificateRef
		}
		opts := listeners.UpdateOpts{DefaultTlsContainerRef: &ref}
		if desired.protocol == cloud.ListenerProtocolTLSMixed {
			// An older graph could have enrolled additional SNI references.
			// Replace the authorized set atomically while the frontend is closed.
			opts.SniContainerRefs = new([]string{})
		}
		if step.updateClientAuth {
			validation := step.scopedListener.TLS.ClientValidation
			fallback := validation != nil && validation.InsecureFallback
			if fallback {
				i := slices.IndexFunc(actual.scopedListeners, func(l listeners.Listener) bool { return l.ID == listenerID })
				if i < 0 {
					return fmt.Errorf("cannot prove ownership of Octavia HTTPS listener")
				}
				tags := actual.scopedListeners[i].Tags
				if !slices.Contains(tags, frontendFallbackTag) {
					tags = append(slices.Clone(tags), frontendFallbackTag)
					opts.Tags = &tags
				}
			}
			err = p.octavia.SetListenerClientAuthentication(ctx, listenerID, opts, actual.frontendCA[step.scopedListener.Port].ref, fallback)
		} else {
			_, err = p.octavia.UpdateListener(ctx, listenerID, opts)
		}
	case graphDisableFrontendClientAuthFallback:
		err = p.octavia.DisableListenerClientAuthFallback(ctx, listenerID)
	case graphPublishFrontendCA:
		node := desired.spec.Gateway
		if !node.BackendCAOwned {
			if node.ClaimBackendCA == nil {
				return errors.New("gateway CA ownership checkpoint is unavailable")
			}
			return node.ClaimBackendCA(ctx)
		}
		ca := step.scopedListener.TLS.ClientValidation
		_, err = p.backendCA.Ensure(ctx, desired.spec.Identity.GatewayUID, ca.ConfigMapUID, ca.PEM)
		return err
	case graphUpdateListenerVirtualHosts:
		err = p.octavia.SetListenerHTTPSVirtualHosts(ctx, listenerID, httpsVirtualHosts(step.scopedListener))
	case graphClearListenerDefaultPool, graphSetListenerDefaultPool:
		// Gophercloud encodes an explicit empty DefaultPoolID as JSON null.
		_, err = p.octavia.UpdateListener(ctx, listenerID, listeners.UpdateOpts{DefaultPoolID: &step.poolID})
	case graphRouteMutation:
		if step.route.kind == routeMutationClaimBackendCA {
			if desired.spec.Gateway.ClaimBackendCA == nil {
				return errors.New("backend CA ownership checkpoint is unavailable")
			}
			return desired.spec.Gateway.ClaimBackendCA(ctx)
		}
		route := graphRoutePart(desired, step)
		if actual.scoped && step.route.policy.httpsListenerHostname != "" && step.listenerID == "" {
			return fmt.Errorf("%w: scoped HTTP policy has no verified listener", cloud.ErrOwnershipConflict)
		}
		route.spec.Gateway = actual.state
		route.spec.Gateway.ListenerID = listenerID
		return p.executeRouteMutation(ctx, route, step.route)
	case graphSNIMutation:
		err = p.executeSNIMutation(ctx, desired, actual, step.sni)
	case graphEnableListener, graphDisableListener:
		enabled := step.kind == graphEnableListener
		step.redactTLSError = step.redactTLSError || cloud.IsTCPTLSListenerProtocol(desired.protocol)
		if step.withdrawClientCA && slices.ContainsFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
			return listener.ID == listenerID && listener.ProvisioningStatus == "ERROR" &&
				actual.adminObserved[listener.ID] && !listener.AdminStateUp
		}) {
			// An accepted shutdown with an ERROR readback did not prove that
			// Amphora removed the port. Do not retry the same failed update.
			return p.deleteUnclosableFrontend(ctx, desired, actual, listenerID)
		}
		_, err = p.octavia.UpdateListener(ctx, listenerID, listeners.UpdateOpts{AdminStateUp: boolPointer(enabled)})
		if step.withdrawClientCA && gophercloud.ResponseCodeIs(err, http.StatusBadRequest) {
			// Octavia validates the attached CA even for an admin-down PUT.
			// A listener DELETE can proceed without the old CA.
			return p.deleteUnclosableFrontend(ctx, desired, actual, listenerID)
		}
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
		if step.redactTLSError {
			return apierrors.RedactOctaviaMutation(err, step.message)
		}
		return fmt.Errorf("reconcile Gateway graph: %w", classifyOctaviaMutationError(err))
	}
	return nil
}
