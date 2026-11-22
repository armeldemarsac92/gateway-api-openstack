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
	"sort"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

var errHTTPFragmentCleanup = errors.New("HTTP backend fragments require complete Gateway recovery observation")

type gatewayDeletionResource string

const (
	gatewayDisableL7Policy  gatewayDeletionResource = "L7 policy disable"
	gatewayDeletionL7Rule   gatewayDeletionResource = "L7 rule"
	gatewayDeletionL7Policy gatewayDeletionResource = "L7 policy"
	gatewayDeletionListener gatewayDeletionResource = "listener"
	gatewayDeletionMonitor  gatewayDeletionResource = "health monitor"
	gatewayDeletionMember   gatewayDeletionResource = "member"
	gatewayDeletionPool     gatewayDeletionResource = "pool"
)

type gatewayDeletionStep struct {
	resource   gatewayDeletionResource
	resourceID string
	parentID   string
}

type gatewayListenerDeletionPlan []gatewayDeletionStep
type gatewayRouteDeletionPlan []gatewayDeletionStep
type gatewayPoolDeletionPlan []gatewayDeletionStep

// gatewayDeletionPlan separates the load balancer graph into dependency
// subplans. No step may run until all three subplans have been validated.
type gatewayDeletionPlan struct {
	listeners           gatewayListenerDeletionPlan
	routes              gatewayRouteDeletionPlan
	pools               gatewayPoolDeletionPlan
	defaultPoolListener *listeners.Listener
	defaultPoolPending  string
}

// orderedSteps flattens the validated subplans in reverse dependency order.
func (p gatewayDeletionPlan) orderedSteps() []gatewayDeletionStep {
	steps := make([]gatewayDeletionStep, 0, len(p.routes)+len(p.pools)+len(p.listeners))
	steps = append(steps, p.routes...)
	steps = append(steps, p.pools...)
	steps = append(steps, p.listeners...)
	return steps
}

// buildGatewayDeletionPlan validates the complete load balancer graph before
// returning any work. Callers must not execute a deletion step when this
// method returns an error.
func (p *Provider) buildGatewayDeletionPlan(ctx context.Context, identity Identity, lb *octavia.LoadBalancer, owners []routeGraphOwner, protocol string) (gatewayDeletionPlan, error) {
	inventory, markedProtocol, err := p.listGatewayListeners(ctx, lb)
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	if markedProtocol != "" {
		if err := requireListenerDefaultPoolInventory(lb.ID, inventory); err != nil {
			return gatewayDeletionPlan{}, err
		}
	}
	return p.buildGatewayDeletionPlanFromListeners(ctx, identity, lb, owners, protocol, inventory)
}

func (p *Provider) buildGatewayDeletionPlanFromListeners(ctx context.Context, identity Identity, lb *octavia.LoadBalancer, owners []routeGraphOwner, protocol string, inventory octavia.ListenerInventory) (gatewayDeletionPlan, error) {
	listenerList := inventory.Items
	var err error
	if protocol == "" && len(listenerList) == 0 {
		protocol, err = detachingProtocolWithoutListener(owners)
		if err != nil {
			return gatewayDeletionPlan{}, err
		}
	}
	defaultPool := usesDefaultPool(protocol)
	for _, listener := range listenerList {
		defaultPool = defaultPool || usesDefaultPool(listener.Protocol)
		if err := p.validateGatewayDeletionListener(identity, listener, lb.ID); err != nil {
			return gatewayDeletionPlan{}, err
		}
	}
	if defaultPool {
		if len(listenerList) > 1 {
			return gatewayDeletionPlan{}, fmt.Errorf("%w: multiple listeners before Gateway deletion", cloud.ErrOwnershipConflict)
		}
		var listener *listeners.Listener
		if len(listenerList) == 1 {
			listener = &listenerList[0]
			if !usesDefaultPool(listener.Protocol) || protocol != "" && protocol != listener.Protocol {
				return gatewayDeletionPlan{}, fmt.Errorf("%w: listener protocol %q differs from its binding before Gateway deletion", cloud.ErrOwnershipConflict, listener.Protocol)
			}
			protocol = listener.Protocol
		}
		return p.buildDefaultPoolGatewayDeletionPlan(ctx, owners, lb, listener, protocol)
	}
	if p.fixedHTTPResponses {
		// The previous controller can leave scalar policy roles on this HTTP Gateway.
		// Prove their targets and relationships before the first deletion as well.
		return p.recoverHTTPBackendGatewayDeletion(ctx, identity, lb, owners, inventory)
	}
	listenerPlan, routePlan, err := p.buildGatewayListenerDeletionPlans(ctx, identity, listenerList)
	if errors.Is(err, errHTTPFragmentCleanup) {
		return p.recoverHTTPBackendGatewayDeletion(ctx, identity, lb, owners, inventory)
	}
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	poolPlan, err := p.buildGatewayPoolDeletionPlan(ctx, identity, lb)
	if errors.Is(err, errHTTPFragmentCleanup) {
		return p.recoverHTTPBackendGatewayDeletion(ctx, identity, lb, owners, inventory)
	}
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	return gatewayDeletionPlan{
		listeners: listenerPlan,
		routes:    routePlan,
		pools:     poolPlan,
	}, nil
}

// buildGatewayListenerDeletionPlans observes listeners and their policy and
// rule subtrees in listener order. It returns listener deletion separately so
// the aggregate plan can place listeners after every route resource.
func (p *Provider) buildGatewayListenerDeletionPlans(
	ctx context.Context,
	identity Identity,
	listenerList []listeners.Listener,
) (gatewayListenerDeletionPlan, gatewayRouteDeletionPlan, error) {
	sort.Slice(listenerList, func(i, j int) bool { return listenerList[i].ID < listenerList[j].ID })

	listenerPlan := make(gatewayListenerDeletionPlan, 0, len(listenerList))
	var routePlan gatewayRouteDeletionPlan
	for _, listener := range listenerList {
		listenerRoutePlan, err := p.buildGatewayRouteDeletionPlan(ctx, identity, listener)
		if err != nil {
			return nil, nil, err
		}
		routePlan = append(routePlan, listenerRoutePlan...)
		listenerPlan = append(listenerPlan, gatewayDeletionStep{
			resource:   gatewayDeletionListener,
			resourceID: listener.ID,
		})
	}
	return listenerPlan, routePlan, nil
}

func (p *Provider) validateGatewayDeletionListener(identity Identity, listener listeners.Listener, loadBalancerID string) error {
	if err := p.validateOptionalProject("listener", listener.ID, listener.ProjectID); err != nil {
		return err
	}
	if !identity.MatchesGateway(listener.Tags, roleListener) {
		return fmt.Errorf("%w: listener %s", cloud.ErrOwnershipConflict, listener.ID)
	}
	if listener.Protocol != "" && !supportedListenerProtocol(listener.Protocol) {
		return fmt.Errorf("%w: unexpected listener protocol %q", cloud.ErrOwnershipConflict, listener.Protocol)
	}
	for _, parent := range listener.Loadbalancers {
		if parent.ID != loadBalancerID {
			return fmt.Errorf("%w: listener %s reports load balancer %s instead of %s", cloud.ErrOwnershipConflict, listener.ID, parent.ID, loadBalancerID)
		}
	}
	return nil
}

func (p *Provider) buildGatewayRouteDeletionPlan(ctx context.Context, identity Identity, listener listeners.Listener) (gatewayRouteDeletionPlan, error) {
	listenerID := listener.ID
	policyList, err := p.octavia.ListCompletePolicies(ctx, l7policies.ListOpts{
		ListenerID: listenerID,
		ProjectID:  p.projectID,
	})
	if err != nil {
		return nil, fmt.Errorf("list L7 policies beneath listener %s before Gateway deletion: %w", listenerID, err)
	}
	if err := crossCheckChildInventory("listener", listenerID, "L7 policy", listener.L7Policies, policyList, func(policy l7policies.L7Policy) string { return policy.ID }); err != nil {
		return nil, err
	}
	if slices.ContainsFunc(policyList, func(policy l7policies.L7Policy) bool {
		role, ok := readRouteResourceRole(policy.Tags, rolePolicyExact, rolePolicyPrefix)
		return ok && role.backend != ""
	}) {
		return nil, errHTTPFragmentCleanup
	}
	sort.Slice(policyList, func(i, j int) bool {
		if policyList[i].Position == policyList[j].Position {
			return policyList[i].ID < policyList[j].ID
		}
		return policyList[i].Position < policyList[j].Position
	})

	var plan gatewayRouteDeletionPlan
	for _, policy := range policyList {
		if err := p.validateOptionalProject("L7 policy", policy.ID, policy.ProjectID); err != nil {
			return nil, err
		}
		if !matchesAnyGatewayRole(identity, policy.Tags, rolePolicyExact, rolePolicyPrefix) {
			return nil, fmt.Errorf("%w: L7 policy %s", cloud.ErrOwnershipConflict, policy.ID)
		}
		if policy.ListenerID != "" && policy.ListenerID != listenerID {
			return nil, fmt.Errorf("%w: L7 policy %s reports listener %s instead of %s", cloud.ErrOwnershipConflict, policy.ID, policy.ListenerID, listenerID)
		}

		ruleList, err := p.octavia.ListCompleteRules(ctx, policy.ID, l7policies.ListRulesOpts{ProjectID: p.projectID})
		if err != nil {
			return nil, fmt.Errorf("list L7 rules beneath policy %s before Gateway deletion: %w", policy.ID, err)
		}
		if err := crossCheckChildInventory("L7 policy", policy.ID, "rule", policy.Rules, ruleList, func(rule l7policies.Rule) string { return rule.ID }); err != nil {
			return nil, err
		}
		sort.Slice(ruleList, func(i, j int) bool { return ruleList[i].ID < ruleList[j].ID })
		for _, rule := range ruleList {
			if err := p.validateOptionalProject("L7 rule", rule.ID, rule.ProjectID); err != nil {
				return nil, err
			}
			if !matchesAnyGatewayRole(identity, rule.Tags, roleRulePath, roleRuleHost) {
				return nil, fmt.Errorf("%w: L7 rule %s", cloud.ErrOwnershipConflict, rule.ID)
			}
			plan = append(plan, gatewayDeletionStep{
				resource:   gatewayDeletionL7Rule,
				resourceID: rule.ID,
				parentID:   policy.ID,
			})
		}
		plan = append(plan, gatewayDeletionStep{
			resource:   gatewayDeletionL7Policy,
			resourceID: policy.ID,
		})
	}
	return plan, nil
}

func (p *Provider) buildGatewayPoolDeletionPlan(ctx context.Context, identity Identity, lb *octavia.LoadBalancer) (gatewayPoolDeletionPlan, error) {
	poolList, err := p.listGatewayPools(ctx, lb)
	if err != nil {
		return nil, err
	}
	if containsHTTPFragmentPool(poolList) {
		return nil, errHTTPFragmentCleanup
	}
	sort.Slice(poolList, func(i, j int) bool { return poolList[i].ID < poolList[j].ID })

	var plan gatewayPoolDeletionPlan
	for _, pool := range poolList {
		if usesDefaultPool(pool.Protocol) {
			return nil, fmt.Errorf("%w: %s pool %s needs its bound protocol or owned listener for cleanup", cloud.ErrOwnershipConflict, pool.Protocol, pool.ID)
		}
		if err := p.validateGatewayDeletionPool(identity, pool, lb.ID); err != nil {
			return nil, err
		}
		subtree, err := p.buildGatewayPoolSubtreeDeletionPlan(ctx, identity, pool.ID)
		if err != nil {
			return nil, err
		}
		plan = append(plan, subtree...)
	}
	return plan, nil
}

func (p *Provider) validateGatewayDeletionPool(identity Identity, pool pools.Pool, loadBalancerID string) error {
	return p.validatePoolParent(pool, loadBalancerID, rolePool, identity)
}

func (p *Provider) validatePoolParent(pool pools.Pool, loadBalancerID, role string, identity Identity) error {
	if err := p.validateOptionalProject("pool", pool.ID, pool.ProjectID); err != nil {
		return err
	}
	if !identity.MatchesGateway(pool.Tags, role) {
		return fmt.Errorf("%w: pool %s", cloud.ErrOwnershipConflict, pool.ID)
	}
	for _, parent := range pool.Loadbalancers {
		if parent.ID != loadBalancerID {
			return fmt.Errorf("%w: pool %s reports load balancer %s instead of %s", cloud.ErrOwnershipConflict, pool.ID, parent.ID, loadBalancerID)
		}
	}
	return nil
}

func (p *Provider) buildGatewayPoolSubtreeDeletionPlan(ctx context.Context, identity Identity, poolID string) (gatewayPoolDeletionPlan, error) {
	monitorPlan, err := p.buildGatewayMonitorDeletionPlan(ctx, identity, poolID)
	if err != nil {
		return nil, err
	}
	memberPlan, err := p.buildGatewayMemberDeletionPlan(ctx, identity, poolID)
	if err != nil {
		return nil, err
	}
	plan := make(gatewayPoolDeletionPlan, 0, len(monitorPlan)+len(memberPlan)+1)
	plan = append(plan, monitorPlan...)
	plan = append(plan, memberPlan...)
	plan = append(plan, gatewayDeletionStep{
		resource:   gatewayDeletionPool,
		resourceID: poolID,
	})
	return plan, nil
}

func (p *Provider) buildGatewayMonitorDeletionPlan(ctx context.Context, identity Identity, poolID string) (gatewayPoolDeletionPlan, error) {
	monitorList, err := p.octavia.ListCompleteMonitors(ctx, monitors.ListOpts{PoolID: poolID, ProjectID: p.projectID})
	if err != nil {
		return nil, fmt.Errorf("list health monitors beneath pool %s before Gateway deletion: %w", poolID, err)
	}
	sort.Slice(monitorList, func(i, j int) bool { return monitorList[i].ID < monitorList[j].ID })

	plan := make(gatewayPoolDeletionPlan, 0, len(monitorList))
	for _, monitor := range monitorList {
		if err := p.validateOptionalProject("health monitor", monitor.ID, monitor.ProjectID); err != nil {
			return nil, err
		}
		if !identity.MatchesGateway(monitor.Tags, roleMonitor) {
			return nil, fmt.Errorf("%w: monitor %s", cloud.ErrOwnershipConflict, monitor.ID)
		}
		for _, parent := range monitor.Pools {
			if parent.ID != poolID {
				return nil, fmt.Errorf("%w: health monitor %s reports pool %s instead of %s", cloud.ErrOwnershipConflict, monitor.ID, parent.ID, poolID)
			}
		}
		plan = append(plan, gatewayDeletionStep{
			resource:   gatewayDeletionMonitor,
			resourceID: monitor.ID,
			parentID:   poolID,
		})
	}
	return plan, nil
}

func (p *Provider) buildGatewayMemberDeletionPlan(ctx context.Context, identity Identity, poolID string) (gatewayPoolDeletionPlan, error) {
	memberList, err := p.octavia.ListCompleteMembers(ctx, poolID, pools.ListMembersOpts{ProjectID: p.projectID})
	if err != nil {
		return nil, fmt.Errorf("list members beneath pool %s before Gateway deletion: %w", poolID, err)
	}
	sort.Slice(memberList, func(i, j int) bool {
		if memberList[i].Address == memberList[j].Address {
			if memberList[i].ProtocolPort == memberList[j].ProtocolPort {
				return memberList[i].ID < memberList[j].ID
			}
			return memberList[i].ProtocolPort < memberList[j].ProtocolPort
		}
		return memberList[i].Address < memberList[j].Address
	})

	plan := make(gatewayPoolDeletionPlan, 0, len(memberList))
	for _, member := range memberList {
		if err := p.validateOptionalProject("member", member.ID, member.ProjectID); err != nil {
			return nil, err
		}
		if !identity.MatchesGateway(member.Tags, roleMember) {
			return nil, fmt.Errorf("%w: member %s", cloud.ErrOwnershipConflict, member.ID)
		}
		if member.PoolID != "" && member.PoolID != poolID {
			return nil, fmt.Errorf("%w: member %s reports pool %s instead of %s", cloud.ErrOwnershipConflict, member.ID, member.PoolID, poolID)
		}
		plan = append(plan, gatewayDeletionStep{
			resource:   gatewayDeletionMember,
			resourceID: member.ID,
			parentID:   poolID,
		})
	}
	return plan, nil
}

func matchesAnyGatewayRole(identity Identity, tags []string, roles ...string) bool {
	for _, role := range roles {
		if identity.MatchesGateway(tags, role) {
			return true
		}
	}
	return false
}

// httpGatewayDeletionPlan removes fully observed HTTP descendants from every
// owned backend. The caller first shuts the parent frontend before changing
// any rules that might widen a serving policy.
func (p *Provider) httpGatewayDeletionPlan(routes []observedRouteGraph, listeners []listeners.Listener) (gatewayDeletionPlan, cloud.Outcome) {
	var deletion gatewayDeletionPlan
	var disables, retries gatewayRouteDeletionPlan
	for _, route := range routes {
		for _, part := range route.parts() {
			if slices.ContainsFunc(part.policies, observedRoutePolicy.pending) {
				return gatewayDeletionPlan{}, p.progressingOutcome("Waiting for HTTP policy provisioning before Gateway cleanup")
			}
			if issue := part.backendProvisioningIssue(true, true); issue != "" {
				return gatewayDeletionPlan{}, p.progressingOutcome(issue)
			}
			for _, policy := range part.policies {
				if policy.redirect || policy.initialRules {
					step := gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: policy.policy.ID}
					if policy.policy.ProvisioningStatus == "ERROR" {
						retries = append(retries, step)
					} else if policy.policy.AdminStateUp || policy.initialRuleClosureUnproven() {
						disables = append(disables, step)
					}
				}
				if policy.initialRulesPartial && !policy.initialRuleClosureUnproven() {
					return gatewayDeletionPlan{}, p.progressingOutcome(initialL7RulesReadbackMessage)
				}
				for _, rule := range policy.rules {
					deletion.routes = append(deletion.routes, gatewayDeletionStep{
						resource: gatewayDeletionL7Rule, resourceID: rule.rule.ID, parentID: policy.policy.ID,
					})
				}
				deletion.routes = append(deletion.routes, gatewayDeletionStep{resource: gatewayDeletionL7Policy, resourceID: policy.policy.ID})
			}
			if part.pool != nil {
				deletion.pools = appendDefaultPoolDeletionSteps(deletion.pools, part)
			}
		}
	}
	deletion.routes = append(append(disables, retries...), deletion.routes...)
	for _, listener := range listeners {
		deletion.listeners = append(deletion.listeners, listenerDeletionStep(listener.ID))
	}
	return deletion, cloud.ReadyOutcome()
}

func (p *Provider) recoverHTTPBackendGatewayDeletion(ctx context.Context, identity Identity, lb *octavia.LoadBalancer, owners []routeGraphOwner, inventory octavia.ListenerInventory) (gatewayDeletionPlan, error) {
	listenerList := inventory.Items
	if len(listenerList) > 1 {
		return gatewayDeletionPlan{}, fmt.Errorf("%w: unscoped HTTP backend recovery requires at most one listener", cloud.ErrOwnershipConflict)
	}
	var listener *listeners.Listener
	if len(listenerList) == 1 {
		listener = &listenerList[0]
		if err := requireListenerDefaultPoolInventory(lb.ID, inventory); err != nil {
			return gatewayDeletionPlan{}, err
		}
		if listener.Protocol != cloud.ListenerProtocolHTTP || listener.DefaultPoolID != "" {
			return gatewayDeletionPlan{}, fmt.Errorf("%w: HTTP backend recovery requires an owned HTTP listener without a default pool", cloud.ErrOwnershipConflict)
		}
	}
	poolInventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	routes, err := p.observeRouteGraphsFromPools(ctx, owners, lb, listener, cloud.ListenerProtocolHTTP, nil, poolInventory, &identity, nil)
	if err != nil {
		return gatewayDeletionPlan{}, err
	}
	if listener != nil {
		outcome, err := p.defaultPoolListenerOutcome(listener, false)
		if err != nil {
			return gatewayDeletionPlan{}, err
		}
		if outcome.State == cloud.OutcomeProgressing {
			return gatewayDeletionPlan{defaultPoolPending: outcome.Message}, nil
		}
	}
	deletion, outcome := p.httpGatewayDeletionPlan(routes, listenerList)
	if outcome.State == cloud.OutcomeProgressing {
		deletion.defaultPoolPending = outcome.Message
	}
	return deletion, nil
}
