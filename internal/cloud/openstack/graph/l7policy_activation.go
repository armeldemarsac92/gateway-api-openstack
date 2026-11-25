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
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

var errPolicyActivationSnapshotBusy = errors.New("waiting for an available Octavia L7 policy activation snapshot")

type l7PolicyActivationBatch struct {
	listenerID   string
	request      octavia.PolicyActivationBatch
	routeIndices []int
}

func (p *Provider) preparePolicyActivationBatch(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation, plan []graphMutation) (*l7PolicyActivationBatch, error) {
	if !p.l7PolicyBatchActivation || desired.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure ||
		!policyActivationStep(step) || step.route.id != plan[0].route.id || actual.loadBalancer == nil ||
		actual.loadBalancer.ProvisioningStatus != "ACTIVE" || !actual.loadBalancer.AdminStateUp {
		return nil, nil
	}

	listener := actual.policyListener(actual.routes[step.routeIndex], step.route.id)
	if listener == nil || listener.Protocol != cloud.ListenerProtocolHTTP || listener.DefaultPoolID != "" || !actual.defaultPoolsObserved ||
		listener.ProjectID != p.projectID || listener.ProvisioningStatus != "ACTIVE" || !listener.AdminStateUp || !actual.adminObserved[listener.ID] ||
		len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID {
		return nil, nil
	}

	// The batch snapshot verifies L7 destinations, not pre-routing
	// filters. The ordinary activation path still verifies ownership.
	if filters, present := actual.requestFilters[listener.ID]; present && (!filters.Readable || len(filters.Sections) != 0) {
		return nil, nil
	}

	batch := &l7PolicyActivationBatch{listenerID: listener.ID}
	selected := make(map[string]bool)
	// A batch starts at the first planned mutation and stays within the
	// activation prefix. Activations on other listeners have independent order.
	for _, candidate := range plan {
		if !policyActivationStep(candidate) {
			break
		}
		if candidate.listenerID != step.listenerID {
			continue
		}
		route := graphRoutePart(desired, candidate)
		observed := actual.routes[candidate.routeIndex]
		if candidate.backendKey != "" {
			part := observed.httpParts[candidate.backendKey]
			if part == nil {
				break
			}
			observed = *part
		}
		if desired.spec.Routes[candidate.routeIndex].Disposition != cloud.GraphEnsure || observed.pool == nil ||
			observed.pool.ProjectID != p.projectID || validateBoundPoolParent(*observed.pool, actual.loadBalancer.ID, cloud.ListenerProtocolHTTP) != nil ||
			!plainActivationPool(observed) ||
			!canBatchPolicyActivation(route, observed, candidate.route.id, listener.ID) || selected[candidate.route.id] {
			break
		}
		selected[candidate.route.id] = true
		batch.routeIndices = append(batch.routeIndices, candidate.routeIndex)
		if len(selected) == octavia.MaxPolicyActivationBatch {
			break
		}
	}
	if len(selected) < 2 {
		return nil, nil
	}

	// Derive the complete destination set from the policy inventory that
	// authorized the plan. This includes disabled and nonselected policies.
	inventory := make(map[string]observedRoutePolicy)
	destinations := make(map[string]observedRouteGraph)
	partsByPool := make(map[string]observedRouteGraph)
	for _, route := range actual.routes {
		for _, part := range route.parts() {
			if slices.ContainsFunc(part.policies, func(policy observedRoutePolicy) bool {
				return policy.policy.ListenerID == listener.ID && policy.hasRequestMirrors()
			}) {
				return nil, nil
			}
			if part.pool != nil {
				partsByPool[part.pool.ID] = part
			}
			for _, policy := range part.policies {
				if policy.policy.ListenerID == listener.ID {
					inventory[policy.policy.ID] = policy
				}
			}
		}
	}
	stale := cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
		errors.New("waiting for an Octavia L7 policy activation snapshot matching the observed graph"))
	for _, policy := range inventory {
		var targets []string
		if policy.policy.RedirectPoolID != "" {
			targets = append(targets, policy.policy.RedirectPoolID)
		}
		for _, target := range policy.weightedTargets {
			if target.PoolID != "" {
				targets = append(targets, target.PoolID)
			}
		}
		for _, target := range targets {
			part, ok := partsByPool[target]
			if !ok {
				return nil, stale
			}
			if !plainActivationPool(part) {
				return nil, nil
			}
			destinations[target] = part
		}
	}

	budget := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/3)
	}
	discovery, cancel := context.WithTimeout(ctx, budget)
	err := p.octavia.VerifyGatewayCapabilities(discovery, actual.loadBalancer.Provider, octavia.GatewayCapabilityL7PolicyBatchActivation)
	cancel()
	var missing *octavia.MissingGatewayCapability
	if errors.As(err, &missing) || gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("verify Octavia L7 policy activation capability in region %q: %w", p.region, err)
	}

	snapshot, err := p.octavia.ObservePolicyActivation(ctx, listener.ID)
	if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
		return nil, errPolicyActivationSnapshotBusy
	}
	if gophercloud.ResponseCodeIs(err, http.StatusNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !activationLoadBalancerMatches(snapshot.LoadBalancer, actual.loadBalancer) ||
		!activationListenerMatches(snapshot.Listener, listener, actual.defaultHTTPResponseCodes[listener.ID], actual.httpsVirtualHosts[listener.ID]) ||
		!activationDestinationsMatch(snapshot.Pools, destinations) ||
		len(snapshot.Policies) != len(inventory) || len(snapshot.Policies) != len(listener.L7Policies) {
		return nil, stale
	}
	for i, item := range snapshot.Policies {
		policy := item.Policy.Policy
		proven, ok := inventory[policy.ID]
		if !ok || policy.Position != int32(i+1) || policy.ProjectID != p.projectID ||
			!activationPolicyMatchesInventory(item, proven) {
			return nil, stale
		}
		delete(inventory, policy.ID)
		batch.request.ExpectedOrder = append(batch.request.ExpectedOrder, policy.ID)
		if selected[policy.ID] {
			batch.request.Policies = append(batch.request.Policies, octavia.PolicyActivationRevision{ID: policy.ID, Revision: item.Revision})
		}
	}
	if len(batch.request.Policies) != len(selected) {
		return nil, stale
	}
	batch.request.ExpectedGraphRevision = snapshot.Revision
	return batch, nil
}

func policyActivationStep(step graphMutation) bool {
	return step.kind == graphRouteMutation && step.route.kind == routeMutationEnablePolicy && !step.withdrawHTTPPolicy
}

func canBatchPolicyActivation(route desiredRoute, observed observedRouteGraph, policyID, listenerID string) bool {
	if route.redirect || route.unavailable || route.fixedResponse || route.targetsPending || route.weightedTargets != nil ||
		!observed.pool.AdminStateUp || observed.backendProvisioningIssue(false, true) != "" {
		return false
	}
	for _, policy := range observed.policies {
		if policy.policy.ID != policyID {
			continue
		}
		if !policy.initialRules || policy.policy.ListenerID != listenerID || policy.activationOptions != "" ||
			policy.urlRewriteTagged || policy.corsTagged || policy.backendRequestFiltersTagged ||
			policy.policy.Action != string(l7policies.ActionRedirectToPool) || policy.policy.RedirectPoolID != observed.pool.ID ||
			policy.policy.RedirectURL != "" || policy.policy.RedirectPrefix != "" || policy.policy.RedirectHttpCode != 0 {
			return false
		}
		for _, wanted := range route.policies {
			if wanted.role != policy.role || wanted.position != policy.policy.Position || wanted.redirect != nil ||
				wanted.weightedPlan != nil || wanted.weighted || wanted.httpsListenerHostname != "" || wanted.requestURLRewrite != nil ||
				wanted.requestCORS != nil || len(wanted.targetRequestFilters) != 0 ||
				len(wanted.requestHeaderOperations) != 0 || len(wanted.responseHeaderOperations) != 0 {
				continue
			}
			rules := rulesFor(wanted, route.hostnameRuleValue)
			return len(rules) == 2 && rules[0].ruleType == l7policies.TypePath && rules[1].ruleType == l7policies.TypeHostName &&
				!rules[0].invert && !rules[1].invert && initialL7RulesReady(policy, rules)
		}
		return false
	}
	return false
}

func activationPolicyMatchesInventory(snapshot octavia.PolicyActivationSnapshot, observed observedRoutePolicy) bool {
	current, proven := snapshot.Policy, observed.policy
	policy := current.Policy
	if policy.ListenerID != proven.ListenerID || policy.ProjectID != proven.ProjectID ||
		policy.Position != proven.Position || policy.Action == "" || policy.Action != proven.Action ||
		current.PoolReference == nil || policy.RedirectPoolID != proven.RedirectPoolID ||
		policy.RedirectURL != proven.RedirectURL || policy.RedirectPrefix != proven.RedirectPrefix ||
		policy.RedirectHttpCode != proven.RedirectHttpCode || current.ActivationOptions != observed.activationOptions ||
		current.AdminState == nil || !observed.adminObserved || *current.AdminState != proven.AdminStateUp ||
		policy.ProvisioningStatus != "ACTIVE" || policy.ProvisioningStatus != proven.ProvisioningStatus ||
		!sameActivationTags(policy.Tags, proven.Tags) || len(snapshot.Rules) != len(observed.rules) {
		return false
	}
	rules := make(map[string]observedRouteRule, len(observed.rules))
	for _, rule := range observed.rules {
		rules[rule.rule.ID] = rule
	}
	for _, row := range snapshot.Rules {
		previous, ok := rules[row.ID]
		rule := previous.rule
		if !ok || !previous.flagsObserved || row.AdminState == nil || row.Inverted == nil ||
			*row.AdminState != rule.AdminStateUp || *row.Inverted != rule.Invert ||
			row.ProjectID != proven.ProjectID || row.ProjectID != rule.ProjectID ||
			row.RuleType != rule.RuleType || row.CompareType != rule.CompareType || row.Key != rule.Key || row.Value != rule.Value ||
			row.ProvisioningStatus != "ACTIVE" || row.ProvisioningStatus != rule.ProvisioningStatus ||
			!sameActivationTags(row.Tags, rule.Tags) {
			return false
		}
		delete(rules, row.ID)
	}
	return true
}

func sameActivationTags(left, right []string) bool {
	return slices.Equal(left, right) || slices.Equal(slices.Sorted(slices.Values(left)), slices.Sorted(slices.Values(right)))
}
