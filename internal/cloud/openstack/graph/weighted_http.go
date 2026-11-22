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
	"errors"
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

const maxWeightedHTTPTargets = 16
const maxHTTPTargetWeight = 1_000_000

type desiredHTTPWeightedTarget struct {
	backendKey string
	weight     int
}

type weightedHTTPPolicyPlan struct {
	targets []octavia.WeightedHTTPTarget
	pending bool
}

func (policy desiredPolicy) targetsPending() bool {
	return policy.weightedPlan != nil && policy.weightedPlan.pending
}

func (p *Provider) buildDesiredWeightedHTTPParts(spec cloud.RouteSpec) ([]desiredRoute, map[int]bool, error) {
	if len(spec.WeightedHTTPRules) == 0 {
		return nil, nil, nil
	}
	invalid := func(err error) ([]desiredRoute, map[int]bool, error) {
		return nil, nil, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	if !p.weightedHTTPTargets {
		return invalid(errors.New("HTTP weighted targets require the Octavia weighted HTTP capability"))
	}
	var parts []desiredRoute
	used, seen := make(map[int]bool), make(map[int]bool)
	partSpec := spec
	partSpec.HTTPBackends, partSpec.HTTPRedirects, partSpec.FixedHTTPRules, partSpec.WeightedHTTPRules = nil, nil, nil, nil
	for _, rule := range spec.WeightedHTTPRules {
		if rule.RuleIndex < 0 || seen[rule.RuleIndex] || len(rule.Matches) == 0 || !validWeightedHTTPTargetCount(len(rule.Targets)) {
			return invalid(errors.New("weighted HTTP rules require a unique rule index, a match and 1 to 16 targets"))
		}
		seen[rule.RuleIndex] = true
		part, err := p.buildDesiredRoutePart(partSpec, openstackidentity.HTTPWeightedRuleKey(rule.RuleIndex), true)
		if err != nil {
			return nil, nil, err
		}
		part.unavailable = false
		var total int
		for _, target := range rule.Targets {
			if !validHTTPTargetWeight(target.Weight) {
				return invalid(errors.New("weighted HTTP targets require a weight from 0 to 1000000"))
			}
			total += target.Weight
			wanted := desiredHTTPWeightedTarget{weight: target.Weight}
			if target.BackendIndex != nil {
				index := *target.BackendIndex
				if index < 0 || index >= len(spec.HTTPBackends) || spec.HTTPBackends[index].InvalidReference {
					return invalid(errors.New("weighted HTTP targets must reference an authorized backend"))
				}
				backend := spec.HTTPBackends[index]
				wanted.backendKey = openstackidentity.HTTPBackendPoolKey(backend)
				used[index] = true
			}
			part.weightedTargets = append(part.weightedTargets, wanted)
		}
		if total == 0 {
			return invalid(errors.New("weighted HTTP rules require positive total target weight"))
		}
		if err := p.buildHTTPMatchPolicies(&part, rule.Matches); err != nil {
			return invalid(err)
		}
		parts = append(parts, part)
	}
	return parts, used, nil
}

func validWeightedHTTPTargetCount(count int) bool {
	return count > 0 && count <= maxWeightedHTTPTargets
}

func validHTTPTargetWeight(weight int) bool { return weight >= 0 && weight <= maxHTTPTargetWeight }

func fixedWeightedHTTPTarget(weight int) octavia.WeightedHTTPTarget {
	return octavia.WeightedHTTPTarget{Weight: weight, FixedResponse: &octavia.FixedHTTPResponse{StatusCode: http.StatusInternalServerError}}
}

func weightedTargetsEqual(a, b octavia.WeightedHTTPTarget) bool {
	return a.Weight == b.Weight && a.PoolID == b.PoolID &&
		(a.FixedResponse == nil && b.FixedResponse == nil ||
			a.FixedResponse != nil && b.FixedResponse != nil && *a.FixedResponse == *b.FixedResponse)
}

func ownedWeightedHTTPPolicyTargets(targets []octavia.WeightedHTTPTarget, owners map[string]poolRouteOwner, routeIndex int) bool {
	if !validWeightedHTTPTargetCount(len(targets)) {
		return false
	}
	var total int
	for _, target := range targets {
		if !validHTTPTargetWeight(target.Weight) {
			return false
		}
		total += target.Weight
		if target.PoolID == "" {
			if target.FixedResponse == nil || target.FixedResponse.StatusCode != http.StatusInternalServerError {
				return false
			}
			continue
		}
		owner, found := owners[target.PoolID]
		if target.FixedResponse != nil || !found || owner.route != routeIndex || owner.backend == "" {
			return false
		}
	}
	return total > 0
}

func servingWeightedListenerID(actual observedGatewayGraph, route desiredRoute) string {
	if actual.loadBalancer == nil || !actual.loadBalancer.AdminStateUp || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return ""
	}
	if !actual.scoped && actual.listener != nil && actual.listener.AdminStateUp && actual.listener.ProvisioningStatus == "ACTIVE" {
		return actual.listener.ID
	}
	for _, listener := range actual.scopedListeners {
		if listener.ProtocolPort == scopedHTTPRoutePort(route) && listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE" {
			return listener.ID
		}
	}
	return ""
}

// An already serving edge can survive endpoint churn on its own predicate.
// A new edge or a disabled policy requires a fully converged positive pool.
func resolveWeightedHTTPPart(part desiredRoute, route desiredRoute, observed observedRouteGraph, listenerID string) desiredRoute {
	part.policies = slices.Clone(part.policies)
	ready := make(map[string]bool)
	denied := deniedBackendTLSKeys(route)
	for _, target := range part.weightedTargets {
		if backend := observed.httpParts[target.backendKey]; target.weight > 0 && backend != nil && backend.pool != nil {
			if _, checked := ready[target.backendKey]; !checked {
				ready[target.backendKey] = weightedBackendReady(route, target.backendKey, *backend)
			}
		}
	}
	for i := range part.policies {
		policy := &part.policies[i]
		serving := servingWeightedPoolIDs(*policy, observed.httpParts[part.backendKey], listenerID)
		plan := &weightedHTTPPolicyPlan{}
		for _, target := range part.weightedTargets {
			resolved := octavia.WeightedHTTPTarget{Weight: target.weight}
			if target.backendKey != "" && !denied[target.backendKey] {
				backend := observed.httpParts[target.backendKey]
				if backend != nil && backend.pool != nil {
					resolved.PoolID = backend.pool.ID
				}
				if target.weight > 0 && !ready[target.backendKey] && !serving[resolved.PoolID] {
					plan.pending = true
				}
			}
			if resolved.PoolID == "" {
				resolved = fixedWeightedHTTPTarget(target.weight)
			}
			plan.targets = append(plan.targets, resolved)
		}
		policy.weightedPlan = plan
		part.targetsPending = part.targetsPending || plan.pending
	}
	return part
}

func servingWeightedPoolIDs(wanted desiredPolicy, actual *observedRouteGraph, listenerID string) map[string]bool {
	if actual == nil || listenerID == "" {
		return nil
	}
	for _, policy := range actual.policies {
		if policy.role != wanted.role || policy.policy.ListenerID != listenerID || !policy.policy.AdminStateUp ||
			policy.policy.ProvisioningStatus != "ACTIVE" ||
			slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool { return rule.rule.ProvisioningStatus != "ACTIVE" }) ||
			!observedHTTPPredicateMatches(policy, rulesFor(wanted, "")) {
			continue
		}
		pools := make(map[string]bool)
		for _, target := range policy.weightedTargets {
			if target.Weight > 0 && target.PoolID != "" {
				pools[target.PoolID] = true
			}
		}
		return pools
	}
	return nil
}

func weightedBackendReady(route desiredRoute, key string, observed observedRouteGraph) bool {
	index := slices.IndexFunc(route.httpParts, func(part desiredRoute) bool { return part.backendKey == key })
	if index == -1 || observed.backendProvisioningIssue(false, true) != "" {
		return false
	}
	backend := route.httpParts[index]
	if !backend.backendTLS.allowsMembers() || backend.backendTransportNeedsWithdrawal(observed) {
		return false
	}
	backend.policies, observed.policies = nil, nil
	steps, _, err := buildRouteEnsurePlan(backend, observed)
	if _, hold := httpMemberWithdrawal(observed); backend.unavailable && hold != "" {
		return false
	}
	return err == nil && len(steps) == 0
}

func (route desiredRoute) httpFixedPolicy() bool {
	return route.fixedResponse || route.weightedTargets != nil && !slices.ContainsFunc(route.weightedTargets, func(target desiredHTTPWeightedTarget) bool {
		return target.backendKey != ""
	})
}

func isObservedWeightedHTTPPolicy(policy observedRoutePolicy) bool {
	return policy.weightedTargets != nil
}

func observedWeightedPoolHolds(route observedRouteGraph) (map[string]bool, bool) {
	var held map[string]bool
	for _, part := range route.httpParts {
		for _, policy := range part.policies {
			if !isObservedWeightedHTTPPolicy(policy) {
				continue
			}
			// An unsettled reload can still serve a previously observed
			// target even when the list now has its replacement.
			if policy.policy.ProvisioningStatus != "ACTIVE" {
				return held, true
			}
			for _, target := range policy.weightedTargets {
				if target.PoolID != "" {
					if held == nil {
						held = make(map[string]bool)
					}
					held[target.PoolID] = true
				}
			}
		}
	}
	return held, false
}

func weightedBackendDemand(route desiredRoute) map[string]int {
	demand := make(map[string]int)
	for _, part := range route.httpParts {
		for _, target := range part.weightedTargets {
			if target.backendKey != "" {
				demand[target.backendKey] += target.weight
			}
		}
		if part.weightedTargets == nil && len(part.policies) != 0 && !part.fixedResponse && !part.redirect {
			demand[part.backendKey]++
		}
	}
	return demand
}

func policyWeightedBackendDemand(part desiredRoute) map[string]int {
	demand := make(map[string]int)
	for _, target := range part.weightedTargets {
		if target.backendKey != "" {
			demand[target.backendKey] += target.weight
		}
	}
	if part.weightedTargets == nil && !part.fixedResponse && !part.redirect && part.backendKey != "" {
		demand[part.backendKey] = 1
	}
	return demand
}

func observedWeightedPoolWeights(demand map[string]int, observed observedRouteGraph) map[string]int {
	weights := make(map[string]int, len(demand))
	for key, weight := range demand {
		if backend := observed.httpParts[key]; backend != nil && backend.pool != nil {
			weights[backend.pool.ID] = weight
		}
	}
	return weights
}

// Compare the observed match with the desired policy that will replace it,
// including policies whose rule index shifted. Route-wide authority still
// closes genuine revocations when an edit removed the predicate altogether.
func weightedPolicyAuthorizations(route desiredRoute, actual observedRouteGraph, routeDemand map[string]int) map[string]map[string]int {
	var out map[string]map[string]int
	denied := deniedBackendTLSKeys(route)
	for _, part := range actual.httpParts {
		for _, policy := range part.policies {
			if !isObservedWeightedHTTPPolicy(policy) {
				continue
			}
			demand := routeDemand
			if replacement := matchingWeightedPolicyReplacement(route, policy); replacement != nil {
				demand = policyWeightedBackendDemand(*replacement)
			}
			if out == nil {
				out = make(map[string]map[string]int)
			}
			weights := observedWeightedPoolWeights(demand, actual)
			for key := range denied {
				if backend := actual.httpParts[key]; backend != nil && backend.pool != nil {
					delete(weights, backend.pool.ID)
				}
			}
			out[policy.policy.ID] = weights
		}
	}
	return out
}

func deniedBackendTLSKeys(route desiredRoute) map[string]bool {
	denied := make(map[string]bool)
	for _, backend := range route.httpParts {
		if backend.backendTLS.denies() {
			denied[backend.backendKey] = true
		}
	}
	return denied
}

func matchingWeightedPolicyReplacement(route desiredRoute, policy observedRoutePolicy) *desiredRoute {
	var best *desiredRoute
	var position int32
	for _, part := range route.httpParts {
		for _, wanted := range part.policies {
			if !observedHTTPPredicateMatches(policy, rulesFor(wanted, part.hostnameRuleValue)) {
				continue
			}
			if policy.role == wanted.role {
				return &part
			}
			if best == nil || wanted.position < position {
				best, position = &part, wanted.position
			}
		}
	}
	return best
}

// Replace only withdrawn forwarding edges here. The ordinary plan installs
// the complete desired vector after validating new targets and backend access.
func weightedTargetWithdrawals(desired desiredRoute, observed observedRouteGraph) []routeMutation {
	var steps []routeMutation
	for _, policy := range observed.policies {
		allowed, found := desired.weightedAuthorizations[policy.policy.ID]
		if !found || policy.pending() {
			continue
		}
		var withdrawn []octavia.WeightedHTTPTarget
		zeroed := -1
		retrySafeTarget := false
		for i, target := range policy.weightedTargets {
			if target.PoolID == "" {
				retrySafeTarget = true
				continue
			}
			wanted, authorized := allowed[target.PoolID]
			if authorized && (wanted > 0 || target.Weight == 0) {
				retrySafeTarget = retrySafeTarget || wanted == 0
				continue
			}
			if withdrawn == nil {
				withdrawn = slices.Clone(policy.weightedTargets)
			}
			if authorized {
				withdrawn[i].Weight = 0
				zeroed = i
			} else {
				withdrawn[i] = fixedWeightedHTTPTarget(target.Weight)
			}
		}
		if zeroed != -1 && !slices.ContainsFunc(withdrawn, func(target octavia.WeightedHTTPTarget) bool { return target.Weight > 0 }) {
			// Octavia requires positive total weight. Keep this predicate
			// closed while its replacement has no prepared positive pool.
			withdrawn[zeroed] = fixedWeightedHTTPTarget(policy.weightedTargets[zeroed].Weight)
		}
		retry := withdrawn == nil || !policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR"
		if withdrawn == nil && retrySafeTarget && policy.policy.ProvisioningStatus == "ERROR" {
			// The DB can already show the replacement while Amphora still
			// serves the previous vector. Retry in place and keep the rules.
			withdrawn = policy.weightedTargets
		}
		if withdrawn != nil {
			if policy.hasRequestURLRewrite() && !observedPolicyDisabled(policy) {
				steps = append(steps, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID, retry: retry})
				continue
			}
			steps = append(steps, routeMutation{kind: routeMutationSetPolicyTarget, id: policy.policy.ID, weightedTargets: &withdrawn, retry: retry, withdrawHTTPPolicy: true})
		}
	}
	return steps
}
