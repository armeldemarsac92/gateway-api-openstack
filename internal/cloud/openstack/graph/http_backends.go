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
	"slices"
	"sort"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

func hasHTTPRouteParts(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	return slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return len(route.httpParts) != 0 }) ||
		slices.ContainsFunc(actual.routes, func(route observedRouteGraph) bool { return len(route.httpParts) != 0 })
}

// Route parts reuse the route planner and retain one binding and result per
// HTTPRoute. Inventory keeps each proved forwarding or redirect part across
// replacements and deletions.
func httpPlanningParts(desired desiredGatewayGraph, actual observedGatewayGraph) (desiredGatewayGraph, observedGatewayGraph, []int) {
	partDesired, partActual := desired, actual
	partDesired.spec.Routes, partDesired.routes, partActual.routes = nil, nil, nil
	var owners []int
	for index, route := range desired.routes {
		node := desired.spec.Routes[index]
		var observed observedRouteGraph
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		heldPools, uncertainTargets := observedWeightedPoolHolds(observed)
		var authorizations map[string]map[string]int
		demand := weightedBackendDemand(route)
		if node.Disposition == cloud.GraphEnsure {
			authorizations = weightedPolicyAuthorizations(route, observed, demand)
		}
		appendPart := func(part desiredRoute, fragment observedRouteGraph, obsolete bool) {
			partNode := node
			if obsolete && partNode.Disposition == cloud.GraphEnsure {
				partNode.Disposition = cloud.GraphDelete
			}
			part.httpParts, fragment.httpParts = nil, nil
			part.weightedAuthorizations = authorizations
			if fragment.pool != nil {
				fragment.weightedPoolHeld = uncertainTargets || heldPools[fragment.pool.ID]
			}
			partDesired.spec.Routes = append(partDesired.spec.Routes, partNode)
			partDesired.routes = append(partDesired.routes, part)
			if actual.routes != nil {
				partActual.routes = append(partActual.routes, fragment)
			}
			owners = append(owners, index)
		}
		if len(route.httpParts) == 0 || observed.pool != nil || len(observed.policies) != 0 {
			appendPart(route, observed, len(route.httpParts) != 0)
		}
		for _, backend := range route.httpParts {
			var fragment observedRouteGraph
			if part := observed.httpParts[backend.backendKey]; part != nil {
				fragment = *part
			}
			if backend.weightedTargets != nil {
				backend = resolveWeightedHTTPPart(backend, route, observed, servingWeightedListenerID(actual, backend))
			} else if weight, referenced := demand[backend.backendKey]; referenced && weight == 0 && len(backend.policies) == 0 {
				backend.zeroWeightOnly = true
			}
			appendPart(backend, fragment, false)
		}
		keys := make([]string, 0, len(observed.httpParts))
		for key := range observed.httpParts {
			if !slices.ContainsFunc(route.httpParts, func(backend desiredRoute) bool { return backend.backendKey == key }) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			part := route
			part.backendKey = key
			part.redirect = strings.HasPrefix(key, "redirect/")
			appendPart(part, *observed.httpParts[key], true)
		}
	}
	return partDesired, partActual, owners
}

func (p *Provider) buildHTTPRoutePartsGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	partDesired, partActual, owners := httpPlanningParts(desired, actual)
	plan, result, err := p.buildGatewayGraphPartPlan(partDesired, partActual, planHTTPPolicyHandovers(desired, partDesired, partActual, owners))
	if err != nil {
		result.Routes = observedRouteDeleteCompletions(desired, actual)
		return nil, result, err
	}
	byOwner := make([]cloud.RouteGraphResult, len(desired.routes))
	for index, route := range desired.routes {
		byOwner[index] = cloud.RouteGraphResult{Identity: route.spec.Identity, Result: cloud.RouteReadyResult(cloud.RouteState{})}
	}
	for index, fragment := range result.Routes {
		part := partDesired.routes[index]
		combined := &byOwner[owners[index]].Result
		if fragment.Result.Outcome.State != cloud.OutcomeReady {
			combined.Outcome = fragment.Result.Outcome
		}
		if combined.UnsupportedMatch == "" {
			combined.UnsupportedMatch = fragment.Result.UnsupportedMatch
		}
		if combined.UnsupportedBackendProtocol == "" {
			combined.UnsupportedBackendProtocol = fragment.Result.UnsupportedBackendProtocol
		}
		old, state := &combined.State, fragment.Result.State
		if part.backendKey == "" {
			old.PoolID, old.MonitorID = state.PoolID, state.MonitorID
		} else {
			if state.PoolID != "" {
				old.PoolIDs = append(old.PoolIDs, state.PoolID)
			}
			if state.MonitorID != "" {
				old.MonitorIDs = append(old.MonitorIDs, state.MonitorID)
			}
		}
		old.MemberIDs = append(old.MemberIDs, state.MemberIDs...)
		old.L7PolicyIDs = append(old.L7PolicyIDs, state.L7PolicyIDs...)
		old.L7RuleIDs = append(old.L7RuleIDs, state.L7RuleIDs...)
	}
	result.Routes = byOwner
	for i := range plan {
		step := &plan[i]
		if step.kind == graphRouteMutation {
			step.backendKey = partDesired.routes[step.routeIndex].backendKey
			step.routeIndex = owners[step.routeIndex]
		}
	}
	return plan, result, nil
}

func policyNeedsHostnameUpdate(route desiredRoute, policy observedRoutePolicy) bool {
	if hasPlainHTTPExclusions(policy) || slices.ContainsFunc(route.policies, func(wanted desiredPolicy) bool { return wanted.httpListenerHostname != "" }) {
		return !slices.ContainsFunc(route.policies, func(wanted desiredPolicy) bool {
			return wanted.role == policy.role && plainHTTPHostRulesMatch(wanted, route.hostnameRuleValue, policy)
		})
	}
	if route.redirect {
		for _, wanted := range route.policies {
			if wanted.role == policy.role {
				return !desiredPolicyActionMatches(wanted, policy, "") ||
					!observedHTTPPredicateMatches(policy, rulesFor(wanted, ""))
			}
		}
		return true
	}
	if route.backendKey == "" {
		return policy.hasOutdatedHostname(route.hostnameRuleValue)
	}
	for _, wanted := range route.policies {
		if wanted.role == policy.role {
			return policy.hasOutdatedHostname(wanted.hostnameRuleValue)
		}
	}
	return true
}

func isHTTPPolicyMutation(kind routeMutationKind) bool {
	return kind >= routeMutationDisablePolicy && kind <= routeMutationEnablePolicy
}

func isHTTPPolicyPositionChange(kind routeMutationKind) bool {
	return kind == routeMutationMovePolicy || kind == routeMutationCreatePolicy
}

// Fill lower positions first and finish preparing disabled policies before
// enabling them. Each Octavia insert or move shifts the other positions.
func orderHTTPBackendMutations(steps []graphMutation) {
	sort.SliceStable(steps, func(i, j int) bool {
		a, b := steps[i].route, steps[j].route
		if a.kind == routeMutationEnablePolicy || b.kind == routeMutationEnablePolicy {
			if a.kind != b.kind {
				return b.kind == routeMutationEnablePolicy
			}
			return a.policy.position < b.policy.position
		}
		aOrders, bOrders := isHTTPPolicyPositionChange(a.kind), isHTTPPolicyPositionChange(b.kind)
		if aOrders != bOrders {
			return bOrders
		}
		return aOrders && a.policy.position < b.policy.position
	})
}

// httpMemberWithdrawal keeps members addressable while Octavia applies the
// disable in Amphora. A failed reload can restore ACTIVE on the pool and load
// balancer, while the single-member worker leaves the affected member ERROR.
// Retain confirmed disabled members on unavailable targets so a restored
// grant can re-enable the same owned member if its address and subnet match.
func httpMemberWithdrawal(observed observedRouteGraph) ([]routeMutation, string) {
	if observed.pool == nil {
		return nil, ""
	}
	var steps []routeMutation
	var hold string
	for _, member := range observed.members {
		switch member.ProvisioningStatus {
		case "ACTIVE", "ERROR":
			_, unreported := observed.memberAdminUnreported[member.ID]
			if unreported {
				hold = "Waiting for explicit Octavia member administrative state before HTTP withdrawal"
			}
			if member.AdminStateUp || unreported || member.ProvisioningStatus == "ERROR" {
				steps = append(steps, routeMutation{kind: routeMutationDisableMember, id: member.ID,
					parentID: observed.pool.ID, retry: unreported || member.ProvisioningStatus == "ERROR"})
			}
		default:
			hold = "Waiting for Octavia member provisioning before HTTP withdrawal"
		}
	}
	return steps, hold
}

// Switching a verified predicate in place closes the revoked backend even
// when Octavia cannot confirm a member disable. A target in ERROR still
// needs a successful reload before its readback proves traffic is closed.
func planFixedResponseDuringMemberWithdrawal(desired desiredRoute, observed observedRouteGraph) (routeMutation, bool) {
	for _, wanted := range desired.policies {
		for _, policy := range observed.policies {
			if policy.role != wanted.role || !policy.targetObserved || !policy.policy.AdminStateUp ||
				policy.urlRewriteTagged && !policy.urlRewriteReported ||
				policy.policy.ProvisioningStatus != "ACTIVE" && policy.policy.ProvisioningStatus != "ERROR" ||
				slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool { return rule.rule.ProvisioningStatus != "ACTIVE" }) ||
				!observedHTTPPredicateMatches(policy, rulesFor(wanted, desired.hostnameRuleValue)) {
				continue
			}
			if policy.fixedResponse && policy.policy.ProvisioningStatus == "ERROR" ||
				!policy.fixedResponse && policy.policy.Action == string(l7policies.ActionRedirectToPool) &&
					policy.policy.RedirectPoolID == observed.pool.ID {
				var operations []cloud.HTTPHeaderOperation
				if policy.requestHeaderOperations != nil {
					operations = []cloud.HTTPHeaderOperation{}
				}
				responsePatch := policyHeaderPatch(wanted.responseHeaderOperations, policy.responseHeaderOperations,
					policy.policy.ProvisioningStatus == "ERROR")
				return routeMutation{kind: routeMutationSetPolicyTarget, id: policy.policy.ID,
					requestHeaderOperations: operations, responseHeaderOperations: responsePatch,
					requestURLRewriteUpdate: policyURLRewritePatch(nil, policy)}, true
			}
		}
	}
	return routeMutation{}, false
}

type httpPolicyHandovers struct {
	protected                map[string]bool
	ports                    map[int]bool
	drain                    map[int]bool
	stages                   map[int]httpPolicyStage
	headerHandoverPredicates map[string]bool
}

type httpPolicyStage struct {
	wanted   desiredPolicy
	current  []observedRoutePolicy
	position int32
}

func (stage httpPolicyStage) plan(route desiredRoute, observed observedRouteGraph) ([]routeMutation, error) {
	route.policies = []desiredPolicy{stage.wanted}
	observed.policies = stage.current
	if route.fixedResponse {
		// A proved fixed response can take over while a separate member
		// withdrawal is blocked. Its policy has no pool dependency.
		observed.members = nil
	}
	steps, _, err := buildRouteEnsurePlan(route, observed)
	return steps, err
}

func (stage httpPolicyStage) ready(route desiredRoute, observed observedRouteGraph) bool {
	if stage.wanted.targetsPending() || len(stage.current) == 0 || route.backendTransportNeedsWithdrawal(observed) ||
		!route.fixedResponse && observed.backendProvisioningIssue(false, true) != "" {
		return false
	}
	current := stage.current[0]
	if current.policy.Position >= stage.position || !current.policy.AdminStateUp || current.policy.ProvisioningStatus != "ACTIVE" ||
		slices.ContainsFunc(current.rules, func(rule observedRouteRule) bool { return rule.rule.ProvisioningStatus != "ACTIVE" }) {
		return false
	}
	if route.unavailable && !route.fixedResponse {
		if steps, hold := httpMemberWithdrawal(observed); len(steps) != 0 || hold != "" {
			return false
		}
	}
	steps, err := stage.plan(route, observed)
	return err == nil && len(steps) == 0
}

func observedHTTPPredicateMatches(policy observedRoutePolicy, rules []desiredRule) bool {
	if len(policy.rules) != len(rules) {
		return false
	}
	for _, wanted := range rules {
		if !slices.ContainsFunc(policy.rules, func(observed observedRouteRule) bool {
			rule := observed.rule
			return rule.RuleType == string(wanted.ruleType) && rule.CompareType == string(wanted.compareType) &&
				wanted.matchesKey(rule.Key) && rule.Value == wanted.value && rule.AdminStateUp && rule.Invert == wanted.invert
		}) {
			return false
		}
	}
	return true
}

func observedForwardPolicySettled(policy observedRoutePolicy) bool {
	return policy.adminObserved && policy.policy.ProvisioningStatus == "ACTIVE" &&
		!slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
			return !rule.flagsObserved || rule.rule.ProvisioningStatus != "ACTIVE"
		})
}

func observedPolicyDisabled(policy observedRoutePolicy) bool {
	return policy.adminObserved && !policy.policy.AdminStateUp && policy.policy.ProvisioningStatus == "ACTIVE"
}

func observedForwardPolicyMatches(route desiredRoute, observed observedRouteGraph, policy observedRoutePolicy) bool {
	if !observedForwardPolicySettled(policy) {
		return false
	}
	return slices.ContainsFunc(route.policies, func(wanted desiredPolicy) bool {
		if wanted.role != policy.role || wanted.httpsListenerHostname != policy.httpsListenerHostname || !observedHTTPPredicateMatches(policy, rulesFor(wanted, route.hostnameRuleValue)) {
			return false
		}
		switch {
		case wanted.weightedPlan != nil:
			return !wanted.weightedPlan.pending && slices.EqualFunc(wanted.weightedPlan.targets, policy.weightedTargets, weightedTargetsEqual)
		case route.fixedResponse:
			return policy.fixedResponse
		default:
			return observed.pool != nil && desiredPolicyActionMatches(wanted, policy, observed.pool.ID)
		}
	})
}

func observedLegacyPrefixMatches(observed []observedRoutePolicy, regex desiredPolicy, hostname string) bool {
	if len(observed) != 2 || regex.compareType != l7policies.CompareTypeRegex ||
		observed[0].policy.ListenerID != observed[1].policy.ListenerID {
		return false
	}
	path := strings.TrimSuffix(strings.TrimPrefix(regex.path, "^"), "($|/)")
	pattern, valid := amphora.LiteralPathPrefixPattern(path)
	if !valid || pattern != regex.path || strings.HasSuffix(path, "/") {
		return false
	}
	for _, piece := range policiesFor(path, path+"/") {
		wanted := regex
		wanted.compareType, wanted.path = piece.compareType, piece.path
		if !slices.ContainsFunc(observed, func(policy observedRoutePolicy) bool {
			return policy.role == piece.role && observedHTTPPredicateMatches(policy, rulesFor(wanted, hostname))
		}) {
			return false
		}
	}
	return true
}

func observedGroupedPrefixMatches(observed observedRoutePolicy, legacy desiredRoute) bool {
	if len(legacy.policies) != 2 {
		return false
	}
	exact, prefix := legacy.policies[0], legacy.policies[1]
	if exact.role != rolePolicyExact || exact.compareType != l7policies.CompareTypeEqual ||
		prefix.role != rolePolicyPrefix || prefix.compareType != l7policies.CompareTypeStartWith ||
		prefix.path != exact.path+"/" || strings.HasSuffix(exact.path, "/") {
		return false
	}
	pattern, valid := amphora.LiteralPathPrefixPattern(exact.path)
	if !valid {
		return false
	}
	exact.compareType, exact.path = l7policies.CompareTypeRegex, pattern
	return observedHTTPPredicateMatches(observed, rulesFor(exact, legacy.hostnameRuleValue))
}

// Stage an equivalent policy immediately before the old one. Inserting the
// disabled policy leaves the old predicate effective while the new backend
// and rules converge. Withdraw an obsolete pool before handing its matches
// to a fixed response. Leave unrelated positions alone until the new policy
// is observed ACTIVE and enabled.
func planHTTPPolicyHandovers(original, desired desiredGatewayGraph, actual observedGatewayGraph, owners []int) *httpPolicyHandovers {
	handover := &httpPolicyHandovers{
		protected:                make(map[string]bool),
		ports:                    make(map[int]bool),
		drain:                    make(map[int]bool),
		stages:                   make(map[int]httpPolicyStage),
		headerHandoverPredicates: make(map[string]bool),
	}
	listenerPorts := make(map[string]int, len(actual.scopedListeners))
	configured := scopedHTTPListeners(desired)
	for _, listener := range actual.scopedListeners {
		if spec, exists := configured[listener.ProtocolPort]; exists && scopedHTTPProtocol(spec) == listener.Protocol {
			listenerPorts[listener.ID] = listener.ProtocolPort
		}
	}
	stageReplacement := func(source, target, port int, old []observedRoutePolicy, wantedPolicies []desiredPolicy) bool {
		prior, next := desired.routes[source], desired.routes[target]
		oldPool := actual.routes[source].pool
		// A scalar/grouped transition for a modifier keeps the same match.
		// During staging, its proved old predicate still protects redirects.
		provedHeaderHandover := (prior.backendKey == "") != (next.backendKey == "") &&
			!next.fixedResponse && next.weightedTargets == nil && oldPool != nil &&
			(slices.ContainsFunc(old, func(policy observedRoutePolicy) bool {
				return len(policy.requestHeaderOperations) != 0 || len(policy.responseHeaderOperations) != 0
			}) || hasHeaderOperationPolicies(wantedPolicies)) &&
			!slices.ContainsFunc(old, func(policy observedRoutePolicy) bool {
				return !observedForwardPolicySettled(policy) ||
					policy.policy.Action != string(l7policies.ActionRedirectToPool) ||
					policy.policy.RedirectPoolID != oldPool.ID
			})
		if provedHeaderHandover {
			for _, policy := range old {
				handover.headerHandoverPredicates[policy.policy.ID] = true
			}
		}
		// A scalar prefix uses two consecutive positions for one logical
		// match. Start before the first and preserve earlier policy order.
		position := old[0].policy.Position
		for _, policy := range old[1:] {
			position = min(position, policy.policy.Position)
		}
		replacement := actual.routes[target]
		staging := false
		for _, wanted := range wantedPolicies {
			stage := httpPolicyStage{wanted: wanted, position: position}
			stage.wanted.position = position
			for _, current := range replacement.policies {
				if current.role == wanted.role && current.policy.ListenerID == old[0].policy.ListenerID {
					stage.current = []observedRoutePolicy{current}
					if provedHeaderHandover && observedPolicyDisabled(current) {
						handover.headerHandoverPredicates[current.policy.ID] = true
					}
					stage.wanted.position = min(position, current.policy.Position)
					break
				}
			}
			if stage.ready(desired.routes[target], replacement) {
				continue
			}
			staging = true
			handover.ports[port] = true
			for _, policy := range old {
				handover.protected[policy.policy.ID] = true
			}
			if current, found := handover.stages[target]; !found || stage.position < current.position {
				handover.stages[target] = stage
			}
		}
		return staging
	}
	for index, observed := range actual.routes {
		owner := owners[index]
		if original.spec.Routes[owner].Disposition != cloud.GraphEnsure {
			continue
		}
		node := desired.spec.Routes[index]
		backend := desired.routes[index].backendKey
		if desired.routes[index].redirect {
			// Stock Amphora evaluates redirects ahead of forwarding regardless
			// of policy position. Action changes must retire the old policy.
			continue
		}
		if node.Disposition == cloud.GraphDelete && backend == "" && len(observed.policies) == 2 {
			port := listenerPorts[observed.policies[0].policy.ListenerID]
			if !actual.scoped || port != 0 {
				target, wanted := findHTTPPolicyReplacement(desired, owners, owner, port, observed.policies)
				if target >= 0 {
					staging := stageReplacement(index, target, port, observed.policies, wanted)
					replacement := desired.routes[target]
					handover.drain[index] = staging && (replacement.httpFixedPolicy() || replacement.weightedTargets != nil)
					continue
				}
			}
		}
		if node.Disposition == cloud.GraphDelete {
			route := original.routes[owner]
			headerPredecessor := backend == "" && slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
				return hasHeaderOperationPolicies(part.policies)
			})
			for _, policy := range observed.policies {
				// Rule cleanup keeps an explicitly disabled predecessor out
				// of forwarding throughout the header handover.
				if observedPolicyDisabled(policy) && (headerPredecessor ||
					backend != "" && len(route.httpParts) == 0 && (len(policy.requestHeaderOperations) != 0 || len(policy.responseHeaderOperations) != 0)) {
					handover.headerHandoverPredicates[policy.policy.ID] = true
				}
			}
		}
		fixedReplacements := 0
		fixedStaging, groupedDrain := false, false
		for _, policy := range observed.policies {
			if node.Disposition == cloud.GraphEnsure && slices.ContainsFunc(desired.routes[index].policies, func(wanted desiredPolicy) bool {
				return wanted.role == policy.role
			}) {
				continue
			}
			port := listenerPorts[policy.policy.ListenerID]
			if actual.scoped && port == 0 {
				continue
			}
			old := []observedRoutePolicy{policy}
			target, wanted := findHTTPPolicyReplacement(desired, owners, owner, port, old)
			if target < 0 {
				continue
			}
			staging := stageReplacement(index, target, port, old, wanted)
			if desired.routes[target].httpFixedPolicy() {
				fixedReplacements++
				fixedStaging = fixedStaging || staging
			} else if backend != "" || desired.routes[target].weightedTargets != nil {
				groupedDrain = true
			}
		}
		if node.Disposition == cloud.GraphDelete {
			// Keep an obsolete pool serving if it still covers another
			// predicate while a fixed replacement is staged.
			handover.drain[index] = fixedReplacements == 0 && groupedDrain ||
				fixedStaging && fixedReplacements == len(observed.policies)
		}
	}
	return handover
}

func hasHeaderOperationPolicies(policies []desiredPolicy) bool {
	return slices.ContainsFunc(policies, func(policy desiredPolicy) bool {
		return len(policy.requestHeaderOperations) != 0 || len(policy.responseHeaderOperations) != 0
	})
}

func findHTTPPolicyReplacement(desired desiredGatewayGraph, owners []int, owner, port int, observed []observedRoutePolicy) (int, []desiredPolicy) {
	target := -1
	var best desiredPolicy
	hadRewrite := slices.ContainsFunc(observed, observedRoutePolicy.hasRequestURLRewrite)
	for index, route := range desired.routes {
		if route.redirect || owners[index] != owner || desired.spec.Routes[index].Disposition != cloud.GraphEnsure || scopedHTTPRoutePort(route) != port {
			continue
		}
		if !hadRewrite && len(observed) == 1 && observed[0].versioned && route.backendKey == "" &&
			route.spec.HTTPSListenerHostname == observed[0].httpsListenerHostname && observedGroupedPrefixMatches(observed[0], route) {
			return index, route.policies
		}
		for _, wanted := range route.policies {
			// The rewrite guard closes the listener before dropping its old
			// policy. Do not hold removal for a non-rewrite replacement.
			if hadRewrite && wanted.requestURLRewrite == nil {
				continue
			}
			if slices.ContainsFunc(observed, func(policy observedRoutePolicy) bool {
				return policy.httpsListenerHostname != wanted.httpsListenerHostname
			}) {
				continue
			}
			matches := len(observed) == 1 && observedHTTPPredicateMatches(observed[0], rulesFor(wanted, route.hostnameRuleValue)) ||
				len(observed) == 2 && route.backendKey != "" && observedLegacyPrefixMatches(observed, wanted, route.hostnameRuleValue)
			if matches && (target < 0 || wanted.position < best.position) {
				target, best = index, wanted
			}
		}
	}
	if target < 0 {
		return target, nil
	}
	return target, []desiredPolicy{best}
}
