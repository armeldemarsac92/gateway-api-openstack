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

// The reserved identity does not overlap any Kubernetes Service port.
var grpcFailurePoolKey = openstackidentity.HTTPBackendKey("", "grpc-unavailable", 0)

type desiredHTTPWeightedTarget struct {
	backendKey              string
	weight                  int
	requestHeaderOperations []cloud.HTTPHeaderOperation
	requestURLRewrite       *cloud.HTTPURLRewrite
	requestMirrors          []desiredHTTPMirror
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
		part, err := p.buildDesiredRoutePart(partSpec, scopeHTTPKey(spec, openstackidentity.HTTPWeightedRuleKey(rule.RuleIndex)), true, false)
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
			if len(target.RequestHeaderOperations) != 0 || target.URLRewrite != nil {
				if !p.backendRequestFilters || spec.GRPC || target.BackendIndex == nil || target.Weight == 0 {
					return invalid(errors.New("HTTP backend request filters require opt-in and a positive forwarding target"))
				}
				wanted.requestHeaderOperations, wanted.requestURLRewrite = target.RequestHeaderOperations, target.URLRewrite
			}
			if len(target.RequestMirrors) != 0 {
				if target.BackendIndex == nil || target.Weight == 0 {
					return invalid(errors.New("backendRef mirroring requires a positive forwarding target"))
				}
				wanted.requestMirrors, err = p.compileHTTPMirrors(spec, target.RequestMirrors)
				if err != nil {
					return invalid(err)
				}
			}
			if target.BackendIndex != nil {
				index := *target.BackendIndex
				if index < 0 || index >= len(spec.HTTPBackends) || spec.HTTPBackends[index].InvalidReference {
					return invalid(errors.New("weighted HTTP targets must reference an authorized backend"))
				}
				backend := spec.HTTPBackends[index]
				wanted.backendKey = scopeHTTPKey(spec, openstackidentity.HTTPBackendPoolKey(backend))
				used[index] = true
			} else if spec.GRPC && target.Weight > 0 {
				// Octavia fixed targets only support HTTP 404 and 500. An
				// empty pool returns HTTP 503, which gRPC maps to UNAVAILABLE.
				wanted.backendKey = grpcFailurePoolKey
			}
			part.weightedTargets = append(part.weightedTargets, wanted)
		}
		if total == 0 {
			if !spec.GRPC {
				return invalid(errors.New("weighted HTTP rules require positive total target weight"))
			}
			// Capture just this match without forwarding to any declared Service.
			part.weightedTargets = []desiredHTTPWeightedTarget{{backendKey: grpcFailurePoolKey, weight: 1}}
		}
		if err := p.buildHTTPMatchPolicies(&part, rule.Matches, spec.HTTPBackends); err != nil {
			return invalid(err)
		}
		parts = append(parts, part)
	}
	if spec.GRPC {
		failure, err := p.buildDesiredRoutePart(partSpec, grpcFailurePoolKey, true, false)
		if err != nil {
			return nil, nil, err
		}
		failure.healthMonitor = cloud.HealthMonitorConfig{}
		parts = append(parts, failure)
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
		slices.Equal(a.RequestHeaderOperations, b.RequestHeaderOperations) && a.RequestURLRewrite.Equal(b.RequestURLRewrite) &&
		slices.Equal(a.RequestMirrors, b.RequestMirrors) &&
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
	denied := deniedHTTPBackendKeys(route)
	allLocal := !slices.ContainsFunc(part.weightedTargets, func(target desiredHTTPWeightedTarget) bool {
		return target.weight > 0 && target.backendKey != "" && !denied[target.backendKey]
	})
	for _, target := range part.weightedTargets {
		if backend := observed.httpParts[target.backendKey]; target.weight > 0 && backend != nil && backend.pool != nil {
			if _, checked := ready[target.backendKey]; !checked {
				ready[target.backendKey] = weightedBackendReady(route, target.backendKey, *backend)
			}
		}
	}
	for i := range part.policies {
		policy := &part.policies[i]
		if allLocal {
			policy.requestPolicy = nil
		}
		if allLocal && policy.requestURLRewrite != nil && len(policy.resolvedRequestMirrors) == 0 {
			policy.localRewriteFallback = true
			policy.requestURLRewrite = nil
			policy.requestHeaderOperations = nil
		}
		serving := servingWeightedPoolIDs(*policy, observed.httpParts[part.backendKey], listenerID)
		current := observedMirrorPolicy(observed.httpParts[part.backendKey], policy.role)
		plan := &weightedHTTPPolicyPlan{pending: part.spec.GRPC && part.grpcFailurePoolID == ""}
		for i, target := range part.weightedTargets {
			resolved := octavia.WeightedHTTPTarget{Weight: target.weight}
			if !policy.localRewriteFallback && target.backendKey != "" && !denied[target.backendKey] {
				backend := observed.httpParts[target.backendKey]
				if backend != nil && backend.pool != nil {
					resolved.PoolID = backend.pool.ID
				}
				if target.weight > 0 && !ready[target.backendKey] && !serving[resolved.PoolID] {
					plan.pending = true
				}
			}
			if resolved.PoolID != "" {
				var currentMirrors []octavia.HTTPRequestMirror
				if i < len(current.weightedTargets) && current.weightedTargets[i].PoolID == resolved.PoolID {
					currentMirrors = current.weightedTargets[i].RequestMirrors
				}
				resolved.RequestMirrors = resolveHTTPMirrors(route, observed, target.requestMirrors, currentMirrors)
			}
			if resolved.PoolID == "" {
				if part.spec.GRPC {
					resolved.PoolID = part.grpcFailurePoolID
				} else {
					resolved = fixedWeightedHTTPTarget(target.weight)
				}
			} else if policy.targetRequestFilters != nil {
				filters := policy.targetRequestFilters[i]
				resolved.RequestHeaderOperations, resolved.RequestURLRewrite = filters.RequestHeaderOperations, filters.RequestURLRewrite
			}
			plan.targets = append(plan.targets, resolved)
		}
		if !plan.pending && !hasWeightedTargetRequestFilters(plan.targets) {
			policy.targetRequestFilters = nil
		}
		policy.weightedPlan = plan
		part.targetsPending = part.targetsPending || plan.pending
	}
	return part
}

func allWeightedTargetsFixed(targets []octavia.WeightedHTTPTarget) bool {
	return len(targets) != 0 && !slices.ContainsFunc(targets, func(target octavia.WeightedHTTPTarget) bool {
		return target.PoolID != "" || target.FixedResponse == nil
	})
}

func observedGRPCFailurePoolID(route desiredRoute, observed observedRouteGraph) string {
	if !route.spec.GRPC {
		return ""
	}
	failure := observed.httpParts[grpcFailurePoolKey]
	if failure == nil || failure.pool == nil || len(failure.members) != 0 ||
		!failure.pool.AdminStateUp || failure.pool.ProvisioningStatus != "ACTIVE" ||
		failure.pool.Protocol != cloud.ListenerProtocolHTTP || failure.pool.TLSEnabled {
		return ""
	}
	return failure.pool.ID
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
	if backend.backendTLS.stagesSystemCA() {
		// The active predicate renders the pool so Octavia can attest its
		// trust. It has no destinations until that observation succeeds.
		return observed.pool != nil && observed.pool.AdminStateUp && len(observed.members) == 0
	}
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
	for _, part := range route.parts() {
		for _, policy := range part.policies {
			if !isObservedWeightedHTTPPolicy(policy) && !policy.hasRequestMirrors() {
				continue
			}
			// An unsettled reload can still serve a previously observed
			// target even when the list now has its replacement.
			if policy.policy.ProvisioningStatus != "ACTIVE" {
				return held, true
			}
			for _, mirror := range policy.allRequestMirrors() {
				if held == nil {
					held = make(map[string]bool)
				}
				held[mirror.PoolID] = true
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
		for _, policy := range part.policies {
			for _, mirror := range policy.requestMirrors {
				demand[mirror.backendKey]++
			}
		}
		for _, target := range part.weightedTargets {
			for _, mirror := range target.requestMirrors {
				demand[mirror.backendKey]++
			}
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
func weightedPolicyAuthorizations(route desiredRoute, actual observedRouteGraph, portsByListener map[string]int) map[string]map[string]int {
	var out map[string]map[string]int
	denied := deniedHTTPBackendKeys(route)
	for _, part := range actual.httpParts {
		for _, policy := range part.policies {
			if !isObservedWeightedHTTPPolicy(policy) {
				continue
			}
			port := portsByListener[policy.policy.ListenerID]
			replacement := matchingWeightedPolicyReplacement(route, policy, port)
			demand := make(map[string]int)
			if replacement != nil {
				demand = policyWeightedBackendDemand(*replacement)
			} else {
				for _, wanted := range route.httpParts {
					if scopedHTTPRoutePort(wanted) == port {
						for key, weight := range policyWeightedBackendDemand(wanted) {
							demand[key] += weight
						}
					}
				}
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

func deniedHTTPBackendKeys(route desiredRoute) map[string]bool {
	denied := make(map[string]bool)
	for _, backend := range route.httpParts {
		issue := backend.cookieSupportIssue()
		if backend.backendTLS.denies() || issue >= cloud.SessionPersistenceUnverified {
			denied[backend.backendKey] = true
		}
	}
	return denied
}

func matchingWeightedPolicyReplacement(route desiredRoute, policy observedRoutePolicy, port int) *desiredRoute {
	var best *desiredRoute
	var position int32
	for _, part := range route.httpParts {
		if scopedHTTPRoutePort(part) != port {
			continue
		}
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
		wantedIndex := -1
		if policy.hasHTTPRequestPolicy() {
			wantedIndex = slices.IndexFunc(desired.policies, func(wanted desiredPolicy) bool { return wanted.role == policy.role })
			if wantedIndex < 0 || desired.policies[wantedIndex].requestPolicy == nil && !policy.requestPolicyReported ||
				!desired.policies[wantedIndex].requestPolicy.HasRetryBackoff() && policy.retryBackoffReadbackMissing() {
				// Other writes can be refused after regional withdrawal.
				// Retire only this owned action.
				steps = append(steps, obsoleteWeightedFilterPolicyStep(policy))
				continue
			}
		}
		if policy.hasBackendRequestFilters() && !slices.ContainsFunc(desired.policies, func(wanted desiredPolicy) bool {
			return wanted.role == policy.role && backendRequestFiltersPolicy.required(wanted)
		}) {
			// A legacy API can preserve hidden transformations on a target PUT.
			// Delete the old policy with its marker intact, before waiting for
			// new pools, and build a fresh policy from the complete vector.
			steps = append(steps, obsoleteWeightedFilterPolicyStep(policy))
			continue
		}
		if step, ok := localOnlyURLRewriteWithdrawal(desired, policy); ok {
			steps = append(steps, step)
			continue
		}
		var withdrawn []octavia.WeightedHTTPTarget
		zeroed := -1
		retrySafeTarget := false
		failureTarget := func(weight int) octavia.WeightedHTTPTarget {
			if desired.spec.GRPC {
				return octavia.WeightedHTTPTarget{Weight: weight, PoolID: desired.grpcFailurePoolID}
			}
			return fixedWeightedHTTPTarget(weight)
		}
		for i, target := range policy.weightedTargets {
			if target.PoolID == "" && !desired.spec.GRPC ||
				target.PoolID != "" && target.PoolID == desired.grpcFailurePoolID {
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
				withdrawn[i] = failureTarget(target.Weight)
			}
		}
		if zeroed != -1 && !slices.ContainsFunc(withdrawn, func(target octavia.WeightedHTTPTarget) bool { return target.Weight > 0 }) {
			// Octavia requires positive total weight. Keep this predicate
			// closed while its replacement has no prepared positive pool.
			withdrawn[zeroed] = failureTarget(policy.weightedTargets[zeroed].Weight)
		}
		retry := withdrawn == nil || !policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR"
		if withdrawn == nil && retrySafeTarget && policy.policy.ProvisioningStatus == "ERROR" {
			// The DB can already show the replacement while Amphora still
			// serves the previous vector. Retry in place and keep the rules.
			withdrawn = policy.weightedTargets
		}
		if withdrawn != nil {
			if policy.hasHTTPRequestPolicy() && !policy.requestPolicyReported || policy.retryBackoffReadbackMissing() ||
				policy.backendRequestFiltersTagged && !hasWeightedTargetRequestFilters(policy.weightedTargets) {
				// A grant changed, but the API cannot prove what else would
				// be written by replaying the remaining target slots.
				steps = append(steps, obsoleteWeightedFilterPolicyStep(policy))
				continue
			}
			if (policy.hasRequestURLRewrite() || policy.hasRequestCORS()) && !observedPolicyDisabled(policy) {
				steps = append(steps, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID, retry: retry})
				continue
			}
			if desired.spec.GRPC && desired.grpcFailurePoolID == "" {
				// An older policy may predate the reserved pool, or that pool
				// may need repair. Close the policy without sending RPCs to a
				// revoked backend or programming an HTTP-only error response.
				if policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR" {
					steps = append(steps, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID, retry: retry})
				}
				continue
			}
			step := routeMutation{kind: routeMutationSetPolicyTarget, id: policy.policy.ID, weightedTargets: &withdrawn,
				policyTags: backendRequestFilterTags(withdrawn, policy, nil), retry: retry, withdrawHTTPPolicy: true}
			if policy.hasHTTPRequestPolicy() && desired.policies[wantedIndex].requestPolicy == nil {
				step.clearHTTPRequestPolicy(policy)
			}
			steps = append(steps, step)
		}
	}
	return steps
}

func obsoleteWeightedFilterPolicyStep(policy observedRoutePolicy) routeMutation {
	step := buildRouteDeletionPlan(observedRouteGraph{policies: []observedRoutePolicy{policy}})[0]
	step.withdrawHTTPPolicy = true
	return step
}

// A rule with no positive, authorized pool can replace its targets and clear
// the stale rewrite in one restrictive write. Keep the match so requests can
// receive the 500 without exposing a lower priority route. An installed CORS
// filter requires a disabled policy before the target list can change.
func localOnlyURLRewriteWithdrawal(desired desiredRoute, observed observedRoutePolicy) (routeMutation, bool) {
	if observed.requestURLRewrite == nil || !observed.adminObserved || observed.pending() {
		return routeMutation{}, false
	}
	for _, wanted := range desired.policies {
		if !wanted.localRewriteFallback || wanted.role != observed.role || wanted.httpsListenerHostname != observed.httpsListenerHostname ||
			wanted.targetsPending() || !observedHTTPPredicateMatches(observed, rulesFor(wanted, desired.hostnameRuleValue)) {
			continue
		}
		if observed.hasRequestCORS() && !observedPolicyDisabled(observed) {
			return routeMutation{kind: routeMutationDisablePolicy, id: observed.policy.ID, retry: observed.policy.ProvisioningStatus == "ERROR"}, true
		}
		patch := policyURLRewritePatch(nil, observed)
		patch.KeepPolicyState = true
		step := routeMutation{kind: routeMutationSetPolicyTarget, id: observed.policy.ID, weightedTargets: &wanted.weightedPlan.targets,
			requestURLRewriteUpdate: patch, requestHeaderOperations: policyHeaderPatch(nil, observed.requestHeaderOperations, false),
			policyTags:         backendRequestFilterTags(wanted.weightedPlan.targets, observed, patch),
			withdrawHTTPPolicy: true, retry: observed.policy.ProvisioningStatus == "ERROR"}
		if observed.hasHTTPRequestPolicy() {
			step.clearHTTPRequestPolicy(observed)
		}
		return step, true
	}
	return routeMutation{}, false
}
