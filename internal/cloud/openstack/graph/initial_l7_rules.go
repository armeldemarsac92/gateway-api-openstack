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
	"strings"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

// The marker preserves the activation requirements across controller restarts
// and after the operator disables grouped creation.
const initialL7RulesTag = "gateway-api-openstack.l7policy-initial-rules=v1"

var errInitialL7RulesUnverified = errors.New("waiting for verified Octavia L7 policy rules")

const initialL7RulesReadbackMessage = "Waiting for the Octavia L7 policy and its exact rules"

// A ready authenticated backend can install and activate its own exact match
// while unrelated backends on the same listener are still being provisioned.
// Only a settled, correctly positioned inventory and distinct exact paths
// permit this. Creation still appends in priority order and activation still
// requires the normal policy, rule, and backend observations.
func independentPreparedTLSPolicy(desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation, listenerID string, port int, policyCount int32) bool {
	route := desired.routes[step.routeIndex]
	if route.backendTLS.phase != backendTLSVerified || route.spec.BackendTLS == nil ||
		len(route.spec.BackendTLS.SubjectAltNames) == 0 || route.unavailable {
		return false
	}
	backend := actual.routes[step.routeIndex]
	if backend.pool == nil || !backend.pool.AdminStateUp || backend.backendProvisioningIssue(false, true) != "" {
		return false
	}
	position := step.route.policy.position
	index := slices.IndexFunc(route.policies, func(policy desiredPolicy) bool { return policy.position == position })
	if index == -1 {
		return false
	}
	wanted := route.policies[index]
	if !positiveExactPath(wanted) || len(rulesFor(wanted, route.hostnameRuleValue)) < 2 {
		return false
	}
	if step.route.kind == routeMutationEnablePolicy && !slices.ContainsFunc(backend.policies, func(policy observedRoutePolicy) bool {
		return policy.policy.ID == step.route.id && policy.initialRules
	}) {
		return false
	}
	for i, sibling := range desired.routes {
		onListener := desired.spec.Routes[i].Disposition == cloud.GraphEnsure && scopedHTTPRoutePort(sibling) == port
		if onListener {
			for j, policy := range sibling.policies {
				if i == step.routeIndex && j == index {
					continue
				}
				if !positiveExactPath(policy) || policy.path == wanted.path {
					return false
				}
			}
		}
		observed := actual.routes[i]
		for _, policy := range observed.policies {
			if policy.policy.ListenerID != listenerID {
				continue
			}
			if !onListener || policy.policy.Position <= 0 || policy.policy.Position > policyCount ||
				!slices.ContainsFunc(sibling.policies, func(expected desiredPolicy) bool {
					return expected.position == policy.policy.Position && expected.role == policy.role
				}) || !observedForwardPolicyMatches(sibling, observed, policy) {
				return false
			}
		}
	}
	return true
}

func positiveExactPath(policy desiredPolicy) bool {
	return policy.redirect == nil && policy.compareType == l7policies.CompareTypeEqual && policy.path != "" && !policy.pathInvert
}

func (p *Provider) preparePolicyCreation(ctx context.Context, actual observedGatewayGraph, desired desiredGatewayGraph, step graphMutation) (graphMutation, error) {
	if step.kind != graphRouteMutation || step.route.kind != routeMutationCreatePolicy {
		return step, nil
	}
	listener := actual.listener
	if actual.scoped {
		i := slices.IndexFunc(actual.scopedListeners, func(item listeners.Listener) bool { return item.ID == step.listenerID })
		if i < 0 {
			return step, nil
		}
		listener = &actual.scopedListeners[i]
	}
	if listener == nil || listener.ID == "" || step.listenerID != "" && listener.ID != step.listenerID {
		return step, nil
	}
	// Even the stock POST must wait when the parent reports an unlisted
	// policy. A previous grouped POST may have committed before losing its
	// response. Old providers can omit the parent references.
	if listener.L7Policies != nil {
		var listed []l7policies.L7Policy
		for _, route := range actual.routes {
			for _, part := range route.parts() {
				for _, policy := range part.policies {
					if !actual.scoped || policy.policy.ListenerID == listener.ID {
						listed = append(listed, policy.policy)
					}
				}
			}
		}
		if err := crossCheckCompleteChildInventory("listener", listener.ID, "L7 policy", listener.L7Policies, listed,
			func(policy l7policies.L7Policy) string { return policy.ID }); err != nil {
			return step, err
		}
	}

	if !p.initialL7Rules || actual.loadBalancer == nil || strings.TrimSpace(actual.loadBalancer.Provider) == "" ||
		listener.ProvisioningStatus != "ACTIVE" ||
		(listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS) ||
		listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID {
		return step, nil
	}

	route := graphRoutePart(desired, step)
	rules := rulesFor(step.route.policy, route.hostnameRuleValue)
	if len(rules) < 2 {
		return step, nil
	}

	budget := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/3)
	}
	discovery, cancel := context.WithTimeout(ctx, budget)
	err := p.verifyGatewayHTTPCapabilities(discovery, desired, actual, []string{octavia.GatewayCapabilityL7PolicyInitialRules})
	cancel()
	if err != nil {
		return step, nil
	}

	// Before submitting (also after any lost response), prove absence against
	// an explicit collection and the listener's policy IDs from this reconcile.
	items, complete, err := p.octavia.ListPolicyObservations(ctx, l7policies.ListOpts{ListenerID: listener.ID, ProjectID: p.projectID})
	if err != nil {
		return step, fmt.Errorf("confirm absence of the Octavia policy before creating initial rules: %w", err)
	}
	if !complete {
		return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("waiting for complete Octavia L7 policy inventory on listener %s", listener.ID))
	}
	itemsByID := make([]l7policies.L7Policy, 0, len(items))
	for _, item := range items {
		if route.identity.MatchesRoute(item.Policy.Tags, step.route.policy.role) {
			return step, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				fmt.Errorf("found Octavia L7 policy %s after observing listener %s", item.Policy.ID, listener.ID))
		}
		itemsByID = append(itemsByID, item.Policy)
	}
	if err := crossCheckCompleteChildInventory("listener", listener.ID, "L7 policy", listener.L7Policies, itemsByID,
		func(item l7policies.L7Policy) string { return item.ID }); err != nil {
		return step, err
	}

	step.route.initialRules = make([]l7policies.CreateRuleOpts, 0, len(rules))
	for _, rule := range rules {
		opts, err := route.createRuleOpts(rule)
		if err != nil {
			return step, err
		}
		step.route.initialRules = append(step.route.initialRules, opts)
	}
	return step, nil
}

func (r desiredRoute) createRuleOpts(rule desiredRule) (l7policies.CreateRuleOpts, error) {
	tags, err := r.identity.RouteTags(rule.role)
	if err != nil {
		return l7policies.CreateRuleOpts{}, err
	}
	return l7policies.CreateRuleOpts{
		RuleType:     rule.ruleType,
		CompareType:  rule.compareType,
		Value:        rule.value,
		Key:          rule.key,
		Invert:       rule.invert,
		AdminStateUp: boolPointer(true),
		Tags:         tags,
	}, nil
}

func validInitialRuleIDs(rules []l7policies.Rule, count int) bool {
	if len(rules) != count {
		return false
	}
	seen := make(map[string]bool, count)
	for _, rule := range rules {
		if strings.TrimSpace(rule.ID) == "" || seen[rule.ID] {
			return false
		}
		seen[rule.ID] = true
	}
	return true
}

func (policy observedRoutePolicy) initialRuleClosureUnproven() bool {
	return policy.initialRules && !observedPolicyDisabled(policy)
}

func (policy observedRoutePolicy) initialRuleReadbackIncomplete() bool {
	return policy.initialRules && (policy.initialRulesPartial || !policy.adminObserved ||
		!policy.policy.AdminStateUp && slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
			return !rule.flagsObserved
		}))
}

func initialL7RulesReady(policy observedRoutePolicy, expected []desiredRule) bool {
	// The inventory has already checked route ownership and matched the
	// rule IDs in the policy response to the independently listed children.
	if policy.initialRulesPartial || !observedPolicyDisabled(policy) || len(policy.rules) != len(expected) {
		return false
	}
	for _, wanted := range expected {
		if !slices.ContainsFunc(policy.rules, func(observed observedRouteRule) bool {
			rule := observed.rule
			return observed.role == wanted.role && observed.flagsObserved && rule.ProvisioningStatus == "ACTIVE" &&
				rule.RuleType == string(wanted.ruleType) &&
				rule.CompareType == string(wanted.compareType) && rule.Key == wanted.key && rule.Value == wanted.value &&
				rule.Invert == wanted.invert && rule.AdminStateUp
		}) {
			return false
		}
	}
	return true
}
