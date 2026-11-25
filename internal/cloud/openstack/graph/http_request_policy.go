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
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

var errHTTPRequestPolicyUnverified = errors.New("waiting for active Octavia HTTP request policy readback")

func (observed observedRoutePolicy) hasHTTPRequestPolicy() bool {
	return observed.requestPolicyTagged || observed.requestPolicyPresent
}

func (wanted desiredPolicy) requestPolicyMatches(observed observedRoutePolicy) bool {
	if wanted.requestPolicy == nil {
		return !observed.hasHTTPRequestPolicy()
	}
	return observed.adminObserved && observed.requestPolicyTagged && wanted.requestPolicy.Equal(observed.requestPolicy)
}

func httpRequestPolicyPatch(wanted desiredPolicy, observed observedRoutePolicy) routeMutation {
	tags := slices.DeleteFunc(slices.Clone(observed.policy.Tags), func(tag string) bool {
		return tag == openstackidentity.HTTPRequestPolicyTag
	})
	if wanted.requestPolicy != nil {
		tags = append(tags, openstackidentity.HTTPRequestPolicyTag)
	}
	return routeMutation{
		kind: routeMutationSetHTTPRequestPolicy, id: observed.policy.ID,
		requestPolicyUpdate: &octavia.HTTPRequestPolicyUpdate{Value: wanted.requestPolicy},
		policyTags:          tags, withdrawHTTPPolicy: wanted.requestPolicy == nil,
	}
}

func (step *routeMutation) clearHTTPRequestPolicy(observed observedRoutePolicy) {
	step.requestPolicyUpdate = &octavia.HTTPRequestPolicyUpdate{}
	if step.policyTags == nil {
		switch {
		case step.requestCORSUpdate != nil:
			step.policyTags = slices.Clone(step.requestCORSUpdate.Tags)
		case step.requestURLRewriteUpdate != nil:
			step.policyTags = slices.Clone(step.requestURLRewriteUpdate.Tags)
		default:
			step.policyTags = slices.Clone(observed.policy.Tags)
		}
	}
	step.policyTags = slices.DeleteFunc(step.policyTags, func(tag string) bool {
		return tag == openstackidentity.HTTPRequestPolicyTag
	})
}

type httpRequestPolicyAvailability uint8

const (
	httpRequestPolicyAvailable httpRequestPolicyAvailability = iota
	httpRequestPolicyUnverified
	httpRequestPolicyUnavailable
)

type unavailableHTTPRequestPolicies struct {
	remainingRules map[int]bool
	err            error
}

// gateHTTPRequestPolicies removes forwarding actions only after an explicit
// negative capability response. A failed discovery leaves the graph intact.
func (p *Provider) gateHTTPRequestPolicies(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) unavailableHTTPRequestPolicies {
	var unavailable unavailableHTTPRequestPolicies
	if !p.httpRequestPolicy || actual.loadBalancer == nil || desired.protocol != cloud.ListenerProtocolHTTP {
		return unavailable
	}

	affected := make(map[int]bool)
	for index, route := range desired.routes {
		for _, policies := range route.policySets() {
			if slices.ContainsFunc(policies, func(policy desiredPolicy) bool { return policy.requestPolicy != nil }) {
				affected[index] = true
				break
			}
		}
	}
	if len(affected) == 0 {
		return unavailable
	}
	unavailable.err = p.verifyGatewayHTTPURLCapabilities(ctx, *desired, actual, []string{octavia.GatewayCapabilityHTTPRequestPolicy})
	if unavailable.err == nil {
		return unavailable
	}
	var missing *octavia.MissingGatewayCapability
	if !errors.As(unavailable.err, &missing) && !isNotFound(unavailable.err) {
		desired.httpRequestPolicies = httpRequestPolicyUnverified
		return unavailable
	}
	desired.httpRequestPolicies = httpRequestPolicyUnavailable
	unavailable.remainingRules = affected
	policiesByPort := make(map[int][]*desiredPolicy)
	keep := func(part *desiredRoute) {
		part.policies = slices.DeleteFunc(part.policies, func(policy desiredPolicy) bool {
			return policy.requestPolicy != nil
		})
		port := httpURLPolicyRoutePort(*desired, *part)
		for i := range part.policies {
			policiesByPort[port] = append(policiesByPort[port], &part.policies[i])
		}
	}
	for i := range desired.routes {
		route := &desired.routes[i]
		keep(route)
		for j := range route.httpParts {
			keep(&route.httpParts[j])
		}
		if affected[i] {
			unavailable.remainingRules[i] = slices.ContainsFunc(route.policySets(), func(policies []desiredPolicy) bool {
				return len(policies) != 0
			})
		}
	}
	// Octavia uses consecutive one-based positions on each physical listener.
	for _, policies := range policiesByPort {
		slices.SortFunc(policies, func(a, b *desiredPolicy) int { return int(a.position - b.position) })
		for i, policy := range policies {
			policy.position = int32(i + 1)
		}
	}
	return unavailable
}

func canWithdrawWithoutHTTPRequestPolicyDiscovery(desired desiredGatewayGraph, step graphMutation) bool {
	switch step.kind {
	case graphDisableListener, graphDisableLoadBalancer, graphClearListenerDefaultPool, graphDeleteChild, graphDeleteLoadBalancer:
		return true
	case graphRouteMutation:
		if !step.withdrawHTTPPolicy {
			return false
		}
		switch step.route.kind {
		case routeMutationDisablePolicy, routeMutationDeleteRule, routeMutationDeletePolicy, routeMutationDisableMember, routeMutationSetPolicyTarget:
			return true
		}
		// Finishing a staged fixed response closes access to an invalid backend.
		return graphRoutePart(desired, step).fixedResponse
	}
	return false
}

// Octavia may refuse to disable an earlier policy while a later request
// transform can still be reached. Work back from the last policy on each
// listener and observe the new state before withdrawing the next one.
func nextHTTPRequestPolicyWithdrawal(desired desiredGatewayGraph, actual observedGatewayGraph) (graphMutation, bool) {
	type candidate struct {
		policy observedRoutePolicy
		index  int
	}
	var candidates []candidate
	for index, route := range actual.routes {
		for _, policy := range route.policies {
			if policy.hasHTTPRequestPolicy() && !observedPolicyDisabled(policy) {
				candidates = append(candidates, candidate{policy: policy, index: index})
			}
		}
	}
	slices.SortStableFunc(candidates, func(a, b candidate) int {
		if order := cmp.Compare(a.policy.policy.ListenerID, b.policy.policy.ListenerID); order != 0 {
			return order
		}
		return cmp.Compare(b.policy.policy.Position, a.policy.policy.Position)
	})
	var blockedListener string
	for _, candidate := range candidates {
		policy := candidate.policy
		if policy.policy.ListenerID == blockedListener {
			continue
		}
		if policy.pending() || desired.spec.Routes[candidate.index].Disposition == cloud.GraphPreserve {
			blockedListener = policy.policy.ListenerID
			continue
		}
		step := routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID,
			retry: policy.policy.ProvisioningStatus == "ERROR" || !policy.adminObserved, withdrawHTTPPolicy: true}
		return graphMutation{kind: graphRouteMutation, routeIndex: candidate.index, route: step, listenerID: policy.policy.ListenerID,
			message: step.message(), withdrawHTTPPolicy: true}, true
	}
	return graphMutation{}, false
}

func hasProgrammedHTTPRule(route desiredRoute, observed observedRouteGraph, listenerID string) bool {
	if listenerID == "" {
		return false
	}
	var poolID string
	if !route.fixedResponse && !route.redirect && route.weightedTargets == nil {
		if !route.backendTLS.allowsMembers() || observed.pool == nil || !observed.pool.AdminStateUp ||
			observed.backendProvisioningIssue(false, true) != "" || route.backendTransportNeedsWithdrawal(observed) {
			return false
		}
		poolID = observed.pool.ID
	}
	for _, wanted := range route.policies {
		if wanted.targetsPending() {
			continue
		}
		for _, policy := range observed.policies {
			if wanted.role != policy.role || policy.policy.ListenerID != listenerID ||
				!policy.policy.AdminStateUp || !observedForwardPolicySettled(policy) || policy.initialRulesPartial {
				continue
			}
			// Regional withdrawal can still be shifting positions on this
			// listener. Verify the retained action and predicate as observed.
			wanted.position = policy.policy.Position
			match := route
			match.policies = []desiredPolicy{wanted}
			plan, disables, _ := buildRoutePolicyPlan(match, []observedRoutePolicy{policy}, poolID)
			if len(plan) == 0 && len(disables) == 0 {
				return true
			}
		}
	}
	return false
}

func (p *Provider) reportUnavailableHTTPRequestPolicies(unavailable unavailableHTTPRequestPolicies, result *cloud.GatewayGraphResult) {
	if unavailable.err != nil && unavailable.remainingRules == nil && result.Outcome.State == cloud.OutcomeReady {
		result.Outcome = p.progressingOutcome("Waiting to verify Octavia HTTP request policy capability")
	}
	for index, remaining := range unavailable.remainingRules {
		if index >= len(result.Routes) {
			continue
		}
		route := &result.Routes[index].Result
		message := "Octavia does not advertise " + octavia.GatewayCapabilityHTTPRequestPolicy
		if remaining {
			route.PartiallyInvalid = message
		} else {
			route.UnsupportedMatch = message
		}
	}
}
