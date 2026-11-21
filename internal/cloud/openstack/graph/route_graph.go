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
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const routeMonitorExpectedCodes = "200-399"

var errWeightedTCPBackupUnknown = errors.New("waiting for Octavia to report the backup flag for every weighted TCP member")

type observedRoutePolicy struct {
	policy          l7policies.L7Policy
	role            string
	rules           []observedRouteRule
	versioned       bool
	redirect        bool
	adminObserved   bool
	fixedResponse   bool
	targetObserved  bool
	weightedTargets []octavia.WeightedHTTPTarget
}

type observedRouteRule struct {
	rule          l7policies.Rule
	role          string
	base          string
	flagsObserved bool
}

type observedRouteGraph struct {
	pool                   *pools.Pool
	members                []pools.Member
	memberBackupUnreported map[string]struct{}
	memberAdminUnreported  map[string]struct{}
	monitor                *monitors.Monitor
	policies               []observedRoutePolicy
	httpParts              map[string]*observedRouteGraph
	weightedPoolHeld       bool
}

type routeGraphOwner struct {
	identity                   Identity
	cleanupProtocol            string
	recovery                   *openstackidentity.RouteOwnerProof
	expectsHTTPParts           bool
	requiresPolicyAbsenceProof bool
}

func (owner routeGraphOwner) matchesRoute(tags []string, role string) bool {
	if owner.recovery == nil {
		return owner.identity.MatchesRoute(tags, role)
	}
	proof, ok := owner.identity.RouteOwner(tags, role)
	return ok && proof == *owner.recovery
}

type routeResourceRole struct {
	raw, base, backend, match string
	redirect                  bool
	weighted                  bool
}

func readRouteResourceRole(tags []string, bases ...string) (routeResourceRole, bool) {
	raw, ok := openstackidentity.ResourceRole(tags)
	if !ok {
		return routeResourceRole{}, false
	}
	if slices.Contains(bases, raw) && raw != roleRuleRedirectGuard {
		return routeResourceRole{raw: raw, base: raw}, true
	}
	if base, match, cell, _, ok := openstackidentity.ParseHTTPRedirectRole(raw); ok {
		return routeResourceRole{raw: raw, base: base, backend: redirectPartKey(match), match: cell, redirect: true}, slices.Contains(bases, base)
	}
	base, backend, match, ok := openstackidentity.ParseHTTPFragmentRole(raw)
	return routeResourceRole{raw: raw, base: base, backend: backend, match: match, weighted: openstackidentity.IsHTTPWeightedPolicyRole(raw)}, ok && slices.Contains(bases, base)
}

type routeGraphInventory struct {
	owners           []routeGraphOwner
	graphs           []observedRouteGraph
	recoverGateway   *Identity
	recoverAllRoutes bool
	recovered        map[openstackidentity.RouteOwnerProof]int
}

func (inventory *routeGraphInventory) resolve(tags []string, bases ...string) (int, routeResourceRole, error) {
	role, ok := readRouteResourceRole(tags, bases...)
	if !ok {
		return -1, role, fmt.Errorf("%w: Gateway route resource has an invalid or ambiguous role", cloud.ErrOwnershipConflict)
	}
	for index, owner := range inventory.owners {
		if owner.matchesRoute(tags, role.raw) {
			return index, role, nil
		}
	}
	if inventory.recoverGateway != nil && (inventory.recoverAllRoutes || role.redirect) {
		if proof, ok := inventory.recoverGateway.RouteOwner(tags, role.raw); ok {
			if index, ok := inventory.recovered[proof]; ok {
				return index, role, nil
			}
			index := len(inventory.owners)
			inventory.owners = append(inventory.owners, routeGraphOwner{identity: *inventory.recoverGateway, recovery: &proof})
			inventory.graphs = append(inventory.graphs, observedRouteGraph{})
			inventory.recovered[proof] = index
			return index, role, nil
		}
	}
	return -1, role, fmt.Errorf("%w: Gateway contains an unaccounted or foreign route resource", cloud.ErrOwnershipConflict)
}

func (route *observedRouteGraph) part(backend string) *observedRouteGraph {
	if backend == "" {
		return route
	}
	if route.httpParts == nil {
		route.httpParts = make(map[string]*observedRouteGraph)
	}
	if route.httpParts[backend] == nil {
		route.httpParts[backend] = &observedRouteGraph{}
	}
	return route.httpParts[backend]
}

func (route observedRouteGraph) parts() []observedRouteGraph {
	parts := []observedRouteGraph{route}
	keys := make([]string, 0, len(route.httpParts))
	for key := range route.httpParts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, *route.httpParts[key])
	}
	return parts
}

type poolRouteOwner struct {
	route   int
	backend string
}

// A route retains its bound protocol after the listener is removed. With no
// listener or Gateway binding, competing transport claims cannot identify it.
func detachingProtocolWithoutListener(owners []routeGraphOwner) (string, error) {
	var protocol string
	for _, owner := range owners {
		if owner.cleanupProtocol == "" {
			continue
		}
		if protocol != "" && owner.cleanupProtocol != protocol {
			return "", &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: detaching routes claim different protocols without a Gateway listener", cloud.ErrOwnershipConflict)}
		}
		protocol = owner.cleanupProtocol
	}
	return protocol, nil
}

func hasOtherCleanupProtocol(owners []routeGraphOwner, protocol string) bool {
	return slices.ContainsFunc(owners, func(owner routeGraphOwner) bool {
		return owner.cleanupProtocol != "" && owner.cleanupProtocol != protocol
	})
}

func (policy observedRoutePolicy) pending() bool {
	unsettled := func(status string) bool {
		if policy.versioned {
			return status != "ACTIVE" && status != "ERROR"
		}
		return strings.HasPrefix(status, "PENDING_")
	}
	return unsettled(policy.policy.ProvisioningStatus) ||
		slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool { return unsettled(rule.rule.ProvisioningStatus) })
}

func (policy observedRoutePolicy) hasOutdatedHostname(value string) bool {
	for _, observed := range policy.rules {
		rule := observed.rule
		if observed.role == roleRuleHost || observed.base == roleRuleHost {
			return value == "" || rule.Value != value || rule.RuleType != string(l7policies.TypeHostName) ||
				rule.CompareType != string(l7policies.CompareTypeRegex) || rule.Invert || !rule.AdminStateUp ||
				rule.ProvisioningStatus == "ERROR" || rule.ProvisioningStatus == "PENDING_UPDATE" || rule.ProvisioningStatus == "PENDING_DELETE"
		}
	}
	// Policies are created disabled, before their path and hostname rules.
	return value != "" && policy.policy.AdminStateUp
}

func (route observedRouteGraph) backendProvisioningIssue(pendingOnly, requireActive bool) string {
	issue := func(resource, id, status string) string {
		// Older observations may omit the status. Current Octavia supplies it
		// for all three backend resource types.
		if status == "" && !requireActive || status == "ACTIVE" || pendingOnly && !strings.HasPrefix(status, "PENDING_") {
			return ""
		}
		return fmt.Sprintf("Octavia %s %s provisioning status is %s", resource, id, status)
	}
	if route.pool != nil {
		if message := issue("pool", route.pool.ID, route.pool.ProvisioningStatus); message != "" {
			return message
		}
	}
	for _, member := range route.members {
		if message := issue("member", member.ID, member.ProvisioningStatus); message != "" {
			return message
		}
	}
	if route.monitor != nil {
		return issue("health monitor", route.monitor.ID, route.monitor.ProvisioningStatus)
	}
	return ""
}

func policyPositionsMatch(wanted []desiredPolicy, actual []observedRoutePolicy) bool {
	if len(wanted) != len(actual) {
		return false
	}
	for _, desired := range wanted {
		if !slices.ContainsFunc(actual, func(policy observedRoutePolicy) bool {
			return policy.role == desired.role && policy.policy.Position == desired.position
		}) {
			return false
		}
	}
	return true
}

// observeRouteGraphs reads every child once and assigns it to an exact route
// identity. An unaccounted child blocks the entire graph before any mutation.
func (p *Provider) observeRouteGraphs(ctx context.Context, owners []routeGraphOwner, lb *octavia.LoadBalancer, listener *listeners.Listener, protocol string) ([]observedRouteGraph, error) {
	return p.observeRouteGraphsWithListeners(ctx, owners, lb, listener, protocol, nil)
}

func (p *Provider) observeRouteGraphsWithListeners(ctx context.Context, owners []routeGraphOwner, lb *octavia.LoadBalancer, listener *listeners.Listener, protocol string, scopedListeners []listeners.Listener) ([]observedRouteGraph, error) {
	poolItems, err := p.listGatewayPools(ctx, lb)
	if err != nil {
		return nil, err
	}
	return p.observeRouteGraphsFromPools(ctx, owners, lb, listener, protocol, scopedListeners, poolItems, nil)
}

func (p *Provider) observeRouteGraphsFromPools(ctx context.Context, owners []routeGraphOwner, lb *octavia.LoadBalancer, listener *listeners.Listener, protocol string, scopedListeners []listeners.Listener, poolItems []pools.Pool, recoverGateway *Identity) ([]observedRouteGraph, error) {
	inventory := routeGraphInventory{
		owners: slices.Clone(owners), graphs: make([]observedRouteGraph, len(owners)),
		recoverGateway: recoverGateway, recoverAllRoutes: len(owners) == 0 || p.fixedHTTPResponses,
	}
	if recoverGateway != nil {
		inventory.recovered = make(map[openstackidentity.RouteOwnerProof]int)
	}
	defaultPool := usesDefaultPool(protocol)
	scopedHTTP := protocol == cloud.ListenerProtocolHTTP && scopedListeners != nil
	strictHTTP := scopedHTTP || protocol == cloud.ListenerProtocolHTTP && p.fixedHTTPResponses || containsHTTPFragmentPool(poolItems) ||
		slices.ContainsFunc(owners, func(owner routeGraphOwner) bool { return owner.expectsHTTPParts }) || recoverGateway != nil
	if strictHTTP {
		frontends := scopedListeners
		if listener != nil {
			frontends = []listeners.Listener{*listener}
		}
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, frontends, func(listener listeners.Listener) string { return listener.ID }); err != nil {
			return nil, err
		}
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, poolItems, func(pool pools.Pool) string { return pool.ID }); err != nil {
			return nil, err
		}
	}
	sort.Slice(poolItems, func(i, j int) bool { return poolItems[i].ID < poolItems[j].ID })
	poolOwners := make(map[string]poolRouteOwner, len(poolItems))
	for index := range poolItems {
		pool := &poolItems[index]
		routeIndex, role, err := inventory.resolve(pool.Tags, rolePool)
		if err != nil {
			return nil, fmt.Errorf("validate pool %s: %w", pool.ID, err)
		}
		owner := inventory.owners[routeIndex]
		identity := owner.identity
		if err := p.validatePoolParent(*pool, lb.ID, role.raw, identity); err != nil {
			return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
		}
		poolProtocol := protocol
		if owner.cleanupProtocol != "" && owner.cleanupProtocol != protocol {
			// A replacement listener does not authorize modifying its graph
			// through an old route. Require the old pool's exact transport
			// and prove it is independent of the replacement listener.
			poolProtocol = owner.cleanupProtocol
			if !usesDefaultPool(poolProtocol) {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: fmt.Errorf("%w: pool %s belongs to a different listener protocol", cloud.ErrOwnershipConflict, pool.ID)}
			}
		}
		if _, duplicate := poolOwners[pool.ID]; duplicate {
			return nil, fmt.Errorf("%w: %s pool %s appears more than once in the Gateway inventory", cloud.ErrOwnershipConflict, pool.Protocol, pool.ID)
		}
		if usesDefaultPool(poolProtocol) {
			if role.backend != "" {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: fmt.Errorf("%w: HTTP backend roles cannot identify a transport pool", cloud.ErrOwnershipConflict)}
			}
			var err error
			if scopedListeners != nil {
				err = p.validateScopedDefaultPool(*pool, lb.ID, scopedListeners, poolProtocol)
			} else {
				err = validateDefaultPool(*pool, lb.ID, listener, poolProtocol)
			}
			if err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
		} else if strictHTTP {
			if err := validateDefaultPoolParent(*pool, lb.ID, cloud.ListenerProtocolHTTP); err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
			if pool.ProjectID != p.projectID {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: fmt.Errorf("%w: HTTP pool %s has an unproven project", cloud.ErrOwnershipConflict, pool.ID)}
			}
		} else if usesDefaultPool(pool.Protocol) {
			return nil, &cloud.GraphError{Identity: identity.Value(), Err: fmt.Errorf("%w: %s pool %s observed without a matching Gateway", cloud.ErrOwnershipConflict, pool.Protocol, pool.ID)}
		}
		graph := inventory.graphs[routeIndex].part(role.backend)
		if graph.pool != nil {
			return nil, &cloud.GraphError{Identity: identity.Value(), Err: fmt.Errorf("%w: found multiple pools for one route", cloud.ErrOwnershipConflict)}
		}
		graph.pool = pool
		poolOwners[pool.ID] = poolRouteOwner{route: routeIndex, backend: role.backend}
		if err := p.observeRoutePoolChildrenWithRole(ctx, owner, graph, role.backend); err != nil {
			return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
		}
		if strictHTTP && !usesDefaultPool(poolProtocol) {
			if err := graph.validateScopedHTTPPoolChildren(); err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
		} else if usesDefaultPool(poolProtocol) {
			if err := graph.validateDefaultPoolChildren(); err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
		}
	}
	if defaultPool {
		return inventory.graphs, nil
	}
	listenerByRoute := make(map[int]string)
	if scopedHTTP {
		for i := range scopedListeners {
			if err := p.observeListenerPolicies(ctx, &inventory, &scopedListeners[i], protocol, poolOwners, listenerByRoute, true); err != nil {
				return nil, err
			}
		}
	} else if listener != nil {
		if strictHTTP && (listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID) {
			return nil, fmt.Errorf("%w: HTTP listener %s has an unproven parent or project", cloud.ErrOwnershipConflict, listener.ID)
		}
		if err := p.observeListenerPolicies(ctx, &inventory, listener, protocol, poolOwners, listenerByRoute, strictHTTP); err != nil {
			return nil, err
		}
	}
	if !strictHTTP && containsObservedRedirect(inventory.graphs) {
		if listener == nil || listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID {
			return nil, fmt.Errorf("%w: HTTP redirect has an unproven listener parent or project", cloud.ErrOwnershipConflict)
		}
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, []listeners.Listener{*listener}, func(listener listeners.Listener) string { return listener.ID }); err != nil {
			return nil, err
		}
		if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, poolItems, func(pool pools.Pool) string { return pool.ID }); err != nil {
			return nil, err
		}
		strictHTTP = true
	}
	if strictHTTP {
		if err := validateScopedHTTPRouteRelationships(inventory.graphs); err != nil {
			return nil, err
		}
	}
	return inventory.graphs, nil
}

func containsHTTPFragmentPool(items []pools.Pool) bool {
	return slices.ContainsFunc(items, func(pool pools.Pool) bool {
		role, ok := readRouteResourceRole(pool.Tags, rolePool)
		return ok && role.backend != ""
	})
}

func (p *Provider) observeListenerPolicies(ctx context.Context, inventory *routeGraphInventory, listener *listeners.Listener, protocol string, poolOwners map[string]poolRouteOwner, listenerByRoute map[int]string, strict bool) error {
	items, complete, err := p.octavia.ListPolicyObservations(ctx, l7policies.ListOpts{ListenerID: listener.ID, ProjectID: p.projectID})
	if err != nil {
		return fmt.Errorf("list Gateway L7 policies: %w", err)
	}
	otherCleanup := hasOtherCleanupProtocol(inventory.owners, protocol)
	policyID := func(item octavia.L7PolicyObservation) string { return item.Policy.ID }
	if !strict && slices.ContainsFunc(items, func(item octavia.L7PolicyObservation) bool {
		role, ok := readRouteResourceRole(item.Policy.Tags, rolePolicyExact, rolePolicyPrefix)
		return ok && role.redirect
	}) {
		strict = true
	}
	refs := make([]octavia.L7PolicyObservation, 0, len(listener.L7Policies))
	if listener.L7Policies == nil {
		refs = nil
	}
	for _, ref := range listener.L7Policies {
		refs = append(refs, octavia.L7PolicyObservation{Policy: l7policies.L7Policy{ID: ref.ID}})
	}
	if strict {
		if err := crossCheckCompleteChildInventory("listener", listener.ID, "L7 policy", refs, items, policyID); err != nil {
			return err
		}
	} else if otherCleanup || slices.ContainsFunc(inventory.owners, func(owner routeGraphOwner) bool { return owner.requiresPolicyAbsenceProof }) {
		if err := crossCheckChildInventory("listener", listener.ID, "L7 policy", refs, items, policyID); err != nil {
			return err
		}
	}
	if p.fixedHTTPResponses && !complete {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for complete Octavia HTTP policy inventory"))
	}
	sort.Slice(items, func(i, j int) bool {
		left, right := items[i].Policy, items[j].Policy
		if left.Position == right.Position {
			return left.ID < right.ID
		}
		return left.Position < right.Position
	})
	for _, observation := range items {
		item := observation.Policy
		routeIndex, role, err := inventory.resolve(item.Tags, rolePolicyExact, rolePolicyPrefix)
		if err != nil {
			return fmt.Errorf("validate L7 policy %s: %w", item.ID, err)
		}
		owner := inventory.owners[routeIndex]
		if owner.cleanupProtocol != "" && owner.cleanupProtocol != protocol {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s belongs to a different listener protocol", cloud.ErrOwnershipConflict, item.ID)}
		}
		strictPolicy := strict || role.backend != "" || p.fixedHTTPResponses
		fixedResponse := item.Action == string(octavia.ActionFixedResponse)
		weighted := item.Action == string(octavia.ActionWeightedTargets)
		if strictPolicy && (item.ProjectID != p.projectID || item.ListenerID != listener.ID ||
			(!role.redirect && !fixedResponse && !weighted && item.RedirectPoolID == "")) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven project, listener or pool", cloud.ErrOwnershipConflict, item.ID)}
		}
		target := observation.Target
		switch {
		case role.redirect:
			if protocol != cloud.ListenerProtocolHTTP || listener.ProjectID != p.projectID ||
				(listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS) || item.RedirectPoolID != "" ||
				observation.ExtendedTargetFields || fixedResponse || weighted {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: HTTP redirect policy %s has an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
			}
			if observation.AdminState == nil || len(observation.PoolReference) == 0 || item.Action == "" ||
				(item.Action == string(l7policies.ActionRedirectPrefix) && (item.RedirectPrefix == "" || item.RedirectHttpCode == 0)) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
					fmt.Errorf("HTTP redirect policy %s has incomplete action or administrative state", item.ID))}
			}
		case role.weighted || weighted:
			if !p.weightedHTTPTargets || !role.weighted || !weighted || target == nil || target.FixedResponse != nil || target.AdminStateUp == nil ||
				string(target.RedirectPool) != "null" || item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" || item.RedirectHttpCode != 0 ||
				!ownedWeightedHTTPPolicyTargets(target.WeightedTargets, poolOwners, routeIndex) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven weighted HTTP target", cloud.ErrOwnershipConflict, item.ID)}
			}
		case fixedResponse:
			if !p.fixedHTTPResponses || role.backend == "" || target == nil || !target.IsFixedServerError() || len(target.WeightedTargets) != 0 ||
				item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" || item.RedirectHttpCode != 0 {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven fixed HTTP response", cloud.ErrOwnershipConflict, item.ID)}
			}
		case p.fixedHTTPResponses:
			if item.Action != string(l7policies.ActionRedirectToPool) || target == nil || target.FixedResponse != nil || target.AdminStateUp == nil || len(target.WeightedTargets) != 0 ||
				item.RedirectURL != "" || item.RedirectPrefix != "" {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven forwarding action", cloud.ErrOwnershipConflict, item.ID)}
			}
		}
		observed, err := p.observeRoutePolicy(ctx, owner, item, listener.ID, role)
		if err != nil {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: err}
		}
		observed.adminObserved = observation.AdminState != nil
		if strictPolicy || owner.requiresPolicyAbsenceProof {
			rules := make([]l7policies.Rule, 0, len(observed.rules))
			for _, rule := range observed.rules {
				rules = append(rules, rule.rule)
			}
			check := crossCheckChildInventory[l7policies.Rule]
			if strictPolicy {
				check = crossCheckCompleteChildInventory[l7policies.Rule]
			}
			if err := check("L7 policy", item.ID, "rule", item.Rules, rules, func(rule l7policies.Rule) string { return rule.ID }); err != nil {
				return err
			}
		}
		if poolOwner, found := poolOwners[item.RedirectPoolID]; found && poolOwner != (poolRouteOwner{route: routeIndex, backend: role.backend}) ||
			(otherCleanup || strictPolicy) && !found && item.RedirectPoolID != "" {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s redirects to an unproven pool %s", cloud.ErrOwnershipConflict, item.ID, item.RedirectPoolID)}
		}
		if previous := listenerByRoute[routeIndex]; strictPolicy && previous != "" && previous != listener.ID {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: HTTPRoute policies appear on multiple listeners", cloud.ErrOwnershipConflict)}
		}
		listenerByRoute[routeIndex] = listener.ID
		observed.fixedResponse = fixedResponse
		observed.targetObserved = p.fixedHTTPResponses && role.backend != "" && !role.redirect
		if weighted {
			observed.weightedTargets = target.WeightedTargets
		}
		graph := inventory.graphs[routeIndex].part(role.backend)
		for _, policy := range graph.policies {
			if policy.role == observed.role {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: duplicate %s policies %s and %s", cloud.ErrOwnershipConflict, observed.role, policy.policy.ID, item.ID)}
			}
		}
		graph.policies = append(graph.policies, observed)
	}
	return nil
}

func (p *Provider) observeRoutePoolChildren(ctx context.Context, identity Identity, graph *observedRouteGraph) error {
	return p.observeRoutePoolChildrenWithRole(ctx, routeGraphOwner{identity: identity}, graph, "")
}

func (p *Provider) observeRoutePoolChildrenWithRole(ctx context.Context, owner routeGraphOwner, graph *observedRouteGraph, backend string) error {
	var err error
	opts := pools.ListMembersOpts{ProjectID: p.projectID}
	switch {
	case graph.pool.Protocol == cloud.ListenerProtocolTCP:
		graph.members, graph.memberBackupUnreported, err = p.octavia.ListMembersWithBackup(ctx, graph.pool.ID, opts)
	case backend != "" || p.fixedHTTPResponses && graph.pool.Protocol == cloud.ListenerProtocolHTTP:
		graph.members, graph.memberAdminUnreported, err = p.octavia.ListMembersWithAdminState(ctx, graph.pool.ID, opts)
	default:
		graph.members, err = p.octavia.ListCompleteMembers(ctx, graph.pool.ID, opts)
	}
	if err != nil {
		return fmt.Errorf("list route pool members: %w", err)
	}
	sort.Slice(graph.members, func(i, j int) bool {
		if graph.members[i].Address == graph.members[j].Address {
			if graph.members[i].ProtocolPort == graph.members[j].ProtocolPort {
				return graph.members[i].ID < graph.members[j].ID
			}
			return graph.members[i].ProtocolPort < graph.members[j].ProtocolPort
		}
		return graph.members[i].Address < graph.members[j].Address
	})
	memberKeys := make(map[string]string, len(graph.members))
	for index := range graph.members {
		member := &graph.members[index]
		if err := p.validateOptionalProject("member", member.ID, member.ProjectID); err != nil {
			return err
		}
		if backend != "" && member.ProjectID != p.projectID || !owner.matchesRoute(member.Tags, httpResourceRole(roleMember, backend, "")) {
			return fmt.Errorf("%w: pool %s contains member %s not owned by the selected route", cloud.ErrOwnershipConflict, graph.pool.ID, member.ID)
		}
		if member.PoolID != "" && member.PoolID != graph.pool.ID {
			return fmt.Errorf("%w: member %s reports pool %s instead of %s", cloud.ErrOwnershipConflict, member.ID, member.PoolID, graph.pool.ID)
		}
		address, err := netip.ParseAddr(member.Address)
		if err != nil || member.ProtocolPort < 1 || member.ProtocolPort > 65535 {
			return fmt.Errorf("%w: member %s has invalid address or port %s:%d", cloud.ErrOwnershipConflict, member.ID, member.Address, member.ProtocolPort)
		}
		member.Address = address.String()
		key := memberKey(member.Address, member.ProtocolPort)
		if existingID := memberKeys[key]; existingID != "" {
			return fmt.Errorf("%w: pool %s has duplicate members %s and %s for %s", cloud.ErrOwnershipConflict, graph.pool.ID, existingID, member.ID, key)
		}
		memberKeys[key] = member.ID
	}

	monitorItems, err := p.octavia.ListCompleteMonitors(ctx, monitors.ListOpts{
		PoolID:    graph.pool.ID,
		ProjectID: p.projectID,
	})
	if err != nil {
		return fmt.Errorf("list route health monitors: %w", err)
	}
	sort.Slice(monitorItems, func(i, j int) bool { return monitorItems[i].ID < monitorItems[j].ID })
	for index := range monitorItems {
		monitor := &monitorItems[index]
		if err := p.validateOptionalProject("health monitor", monitor.ID, monitor.ProjectID); err != nil {
			return err
		}
		if backend != "" && (monitor.ProjectID != p.projectID || len(monitor.Pools) != 1) ||
			!owner.matchesRoute(monitor.Tags, httpResourceRole(roleMonitor, backend, "")) {
			return fmt.Errorf("%w: pool %s contains health monitor %s not owned by the selected route", cloud.ErrOwnershipConflict, graph.pool.ID, monitor.ID)
		}
		for _, parent := range monitor.Pools {
			if parent.ID != graph.pool.ID {
				return fmt.Errorf("%w: health monitor %s reports pool %s instead of %s", cloud.ErrOwnershipConflict, monitor.ID, parent.ID, graph.pool.ID)
			}
		}
		if graph.monitor != nil {
			return fmt.Errorf("%w: pool %s has multiple health monitors", cloud.ErrOwnershipConflict, graph.pool.ID)
		}
		graph.monitor = monitor
	}
	return nil
}

func (p *Provider) observeRoutePolicy(ctx context.Context, owner routeGraphOwner, item l7policies.L7Policy, listenerID string, role routeResourceRole) (observedRoutePolicy, error) {
	if err := p.validateOptionalProject("L7 policy", item.ID, item.ProjectID); err != nil {
		return observedRoutePolicy{}, err
	}
	if item.ListenerID != "" && item.ListenerID != listenerID {
		return observedRoutePolicy{}, fmt.Errorf("%w: L7 policy %s reports listener %s instead of %s", cloud.ErrOwnershipConflict, item.ID, item.ListenerID, listenerID)
	}
	ruleItems, err := p.octavia.ObserveRules(ctx, item.ID, l7policies.ListRulesOpts{ProjectID: p.projectID})
	if err != nil {
		return observedRoutePolicy{}, fmt.Errorf("list rules beneath L7 policy %s: %w", item.ID, err)
	}
	sort.Slice(ruleItems, func(i, j int) bool { return ruleItems[i].ID < ruleItems[j].ID })
	observedRules := make([]observedRouteRule, 0, len(ruleItems))
	ruleRoles := make(map[string]string, len(ruleItems))
	for _, observation := range ruleItems {
		rule := observation.Rule
		if err := p.validateOptionalProject("L7 rule", rule.ID, rule.ProjectID); err != nil {
			return observedRoutePolicy{}, err
		}
		ruleRole, ok := readRouteResourceRole(rule.Tags, roleRulePath, roleRuleHost, roleRuleRedirectGuard)
		if !ok || !owner.matchesRoute(rule.Tags, ruleRole.raw) || ruleRole.backend != role.backend ||
			ruleRole.match != role.match || ruleRole.redirect != role.redirect || ruleRole.weighted != role.weighted ||
			role.backend != "" && rule.ProjectID != p.projectID {
			return observedRoutePolicy{}, fmt.Errorf("%w: L7 rule %s beneath policy %s has an unproven project or route, backend or match identity", cloud.ErrOwnershipConflict, rule.ID, item.ID)
		}
		flagsObserved := observation.AdminState != nil && observation.Inverted != nil
		if role.redirect && (!flagsObserved || rule.RuleType == "" || rule.CompareType == "" || rule.Value == "") {
			return observedRoutePolicy{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				fmt.Errorf("HTTP redirect rule %s has incomplete predicate or administrative state", rule.ID))
		}
		if existingID := ruleRoles[ruleRole.raw]; existingID != "" {
			return observedRoutePolicy{}, fmt.Errorf("%w: policy %s has duplicate %s rules %s and %s", cloud.ErrOwnershipConflict, item.ID, ruleRole.raw, existingID, rule.ID)
		}
		ruleRoles[ruleRole.raw] = rule.ID
		observedRules = append(observedRules, observedRouteRule{rule: rule, role: ruleRole.raw, base: ruleRole.base, flagsObserved: flagsObserved})
	}
	return observedRoutePolicy{policy: item, role: role.raw, rules: observedRules, versioned: role.backend != "", redirect: role.redirect}, nil
}

type routeMutationKind uint8

const (
	routeMutationCreatePool routeMutationKind = iota
	routeMutationUpdatePool
	routeMutationDeleteMember
	routeMutationDisableMember
	routeMutationCreateMember
	routeMutationUpdateMember
	routeMutationCreateMonitor
	routeMutationUpdateMonitor
	routeMutationDisablePolicy
	routeMutationDeleteRule
	routeMutationDeletePolicy
	routeMutationCreatePolicy
	routeMutationUpdatePolicy
	routeMutationSetPolicyTarget
	routeMutationMovePolicy
	routeMutationCreateRule
	routeMutationUpdateRule
	routeMutationEnablePolicy
	routeMutationDeleteMonitor
	routeMutationDeletePool
)

type routeMutation struct {
	kind                 routeMutationKind
	id                   string
	parentID             string
	member               cloud.Member
	policy               desiredPolicy
	rule                 desiredRule
	retry                bool
	clearPoolPersistence bool
	clearMemberBackup    bool
	withdrawHTTPPolicy   bool
	weightedTargets      *[]octavia.WeightedHTTPTarget
}

func (s routeMutation) message() string {
	switch s.kind {
	case routeMutationCreatePool:
		return "Created Octavia pool"
	case routeMutationUpdatePool:
		return "Updated Octavia pool"
	case routeMutationDisableMember:
		return "Disabling an Octavia HTTP backend member"
	case routeMutationDeleteMember:
		return "Deleted stale Octavia member"
	case routeMutationCreateMember:
		return "Created Octavia member"
	case routeMutationUpdateMember:
		return "Updated Octavia member"
	case routeMutationCreateMonitor:
		return "Created Octavia health monitor"
	case routeMutationUpdateMonitor:
		return "Updated Octavia health monitor"
	case routeMutationDisablePolicy:
		return "Disabled Octavia L7 policy before changing rules"
	case routeMutationDeleteRule:
		return "Deleted Octavia L7 rule"
	case routeMutationDeletePolicy:
		return "Deleted Octavia L7 policy"
	case routeMutationCreatePolicy:
		return "Created Octavia L7 policy"
	case routeMutationUpdatePolicy:
		return "Updated Octavia L7 policy"
	case routeMutationSetPolicyTarget:
		return "Updated Octavia L7 policy target"
	case routeMutationMovePolicy:
		return "Updated Octavia L7 policy position"
	case routeMutationCreateRule:
		return "Created Octavia L7 rule"
	case routeMutationUpdateRule:
		return "Updated Octavia L7 rule"
	case routeMutationEnablePolicy:
		return "Enabled converged Octavia L7 policy"
	case routeMutationDeleteMonitor:
		return "Deleted Octavia health monitor"
	case routeMutationDeletePool:
		return "Deleted Octavia pool"
	default:
		return "Changed Octavia route graph"
	}
}

func buildRouteEnsurePlan(desired desiredRoute, actual observedRouteGraph) ([]routeMutation, cloud.RouteState, error) {
	nonPolicyPlan := make([]routeMutation, 0)
	state := cloud.RouteState{}
	poolID := ""
	switch {
	case desired.redirect:
		if actual.pool != nil {
			return nil, state, fmt.Errorf("%w: HTTP redirect has a pool", cloud.ErrOwnershipConflict)
		}
	case desired.weightedTargets != nil:
	case desired.fixedResponse:
		state = routeStateFromObservation(actual)
		if steps, hold := httpMemberWithdrawal(actual); len(steps) != 0 || hold != "" {
			if hold != "" || slices.ContainsFunc(steps, func(step routeMutation) bool { return step.retry }) {
				if step, ok := planFixedResponseDuringMemberWithdrawal(desired, actual); ok {
					return []routeMutation{step}, state, nil
				}
			}
			return steps, state, nil
		}
	case actual.pool == nil:
		nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationCreatePool})
	default:
		pool := actual.pool
		poolID = pool.ID
		state.PoolID = pool.ID
		if pool.Protocol != desired.protocol() {
			return nil, cloud.RouteState{}, fmt.Errorf("%w: pool %s has protocol %s", cloud.ErrOwnershipConflict, pool.ID, pool.Protocol)
		}
		if desired.unavailable {
			state = routeStateFromObservation(actual)
			if steps, hold := httpMemberWithdrawal(actual); len(steps) != 0 || hold != "" {
				return steps, state, nil
			}
		}
		clearPersistence := pool.Persistence != (pools.SessionPersistence{}) && desired.spec.MemberMode == cloud.MemberModeEndpoint &&
			slices.ContainsFunc(desired.members, func(member cloud.Member) bool { return !member.Draining && member.Weight > 0 })
		if pool.LBMethod != string(pools.LBMethodRoundRobin) || !pool.AdminStateUp || clearPersistence {
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationUpdatePool, id: pool.ID, clearPoolPersistence: clearPersistence})
		}

		desiredMembers := make(map[string]cloud.Member, len(desired.members))
		for _, member := range desired.members {
			desiredMembers[memberKey(member.Address, member.Port)] = member
		}
		for _, member := range actual.members {
			if desired.unavailable {
				continue
			}
			key := memberKey(member.Address, member.ProtocolPort)
			wanted, exists := desiredMembers[key]
			retry := desired.backendKey != "" && member.ProvisioningStatus == "ERROR"
			if !exists || member.SubnetID != desired.spec.MemberSubnetID {
				kind := routeMutationDeleteMember
				if retry {
					kind = routeMutationDisableMember
				}
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: kind, id: member.ID, parentID: pool.ID, retry: retry})
				continue
			}
			state.MemberIDs = append(state.MemberIDs, member.ID)
			delete(desiredMembers, key)
			weight := desiredMemberWeight(desired.spec.MemberMode, wanted)
			// HAProxy excludes backup members while any regular member is up.
			weightedTCP := desired.protocol() == cloud.ListenerProtocolTCP && wanted.Weight > 0 && !wanted.Draining
			if _, unreported := actual.memberBackupUnreported[member.ID]; weightedTCP && unreported {
				return nil, cloud.RouteState{}, errWeightedTCPBackupUnknown
			}
			clearBackup := weightedTCP && member.Backup
			if _, unreported := actual.memberAdminUnreported[member.ID]; unreported || !member.AdminStateUp || retry || weight != nil && member.Weight != *weight || clearBackup {
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{
					kind:              routeMutationUpdateMember,
					id:                member.ID,
					parentID:          pool.ID,
					member:            wanted,
					retry:             retry,
					clearMemberBackup: clearBackup,
				})
			}
		}
		missingMemberKeys := make([]string, 0, len(desiredMembers))
		for key := range desiredMembers {
			missingMemberKeys = append(missingMemberKeys, key)
		}
		sort.Strings(missingMemberKeys)
		for _, key := range missingMemberKeys {
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationCreateMember, parentID: pool.ID, member: desiredMembers[key]})
		}

		switch {
		case desired.healthMonitor == (cloud.HealthMonitorConfig{}):
			if actual.monitor != nil {
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationDeleteMonitor, id: actual.monitor.ID, parentID: pool.ID})
			}
		case actual.monitor == nil:
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationCreateMonitor, parentID: pool.ID})
		default:
			monitor := actual.monitor
			state.MonitorID = monitor.ID
			if monitor.Type != desired.monitorType() {
				return nil, cloud.RouteState{}, fmt.Errorf("%w: health monitor %s has type %s", cloud.ErrOwnershipConflict, monitor.ID, monitor.Type)
			}
			httpDrift := monitor.Type == monitors.TypeHTTP && (monitor.URLPath != desired.healthMonitor.Path ||
				monitor.HTTPMethod != http.MethodGet || monitor.ExpectedCodes != routeMonitorExpectedCodes)
			if monitor.Delay != desired.healthMonitor.Delay || monitor.Timeout != desired.healthMonitor.Timeout ||
				monitor.MaxRetries != desired.healthMonitor.MaxRetries || monitor.MaxRetriesDown != desired.healthMonitor.MaxRetriesDown ||
				httpDrift || !monitor.AdminStateUp {
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationUpdateMonitor, id: monitor.ID, parentID: pool.ID})
			}
		}
	}

	policyPlan, disablePlan, policyState := buildRoutePolicyPlan(desired, actual.policies, poolID)
	state.L7PolicyIDs = policyState.L7PolicyIDs
	state.L7RuleIDs = policyState.L7RuleIDs
	plan := append(disablePlan, nonPolicyPlan...)
	plan = append(plan, policyPlan...)
	if len(plan) != 0 {
		return plan, state, nil
	}
	for _, wantedPolicy := range desired.policies {
		if wantedPolicy.targetsPending() {
			continue
		}
		for _, policy := range actual.policies {
			if policy.role == wantedPolicy.role && !policy.policy.AdminStateUp {
				return []routeMutation{{kind: routeMutationEnablePolicy, id: policy.policy.ID}}, state, nil
			}
		}
	}
	return nil, state, nil
}

func buildRoutePolicyPlan(
	desired desiredRoute,
	actual []observedRoutePolicy,
	poolID string,
) ([]routeMutation, []routeMutation, cloud.RouteState) {
	plan := make([]routeMutation, 0)
	disablePlan := make([]routeMutation, 0)
	state := cloud.RouteState{}
	desiredPolicies := make(map[string]desiredPolicy, len(desired.policies))
	for _, policy := range desired.policies {
		desiredPolicies[policy.role] = policy
	}
	for _, policy := range actual {
		if _, wanted := desiredPolicies[policy.role]; wanted {
			continue
		}
		if policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR" {
			disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID,
				retry: policy.policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: desired.backendKey != ""})
		}
		for _, rule := range policy.rules {
			plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.rule.ID, parentID: policy.policy.ID, withdrawHTTPPolicy: desired.backendKey != ""})
		}
		plan = append(plan, routeMutation{kind: routeMutationDeletePolicy, id: policy.policy.ID, withdrawHTTPPolicy: desired.backendKey != ""})
	}

	actualPolicies := make(map[string]observedRoutePolicy, len(actual))
	for _, policy := range actual {
		actualPolicies[policy.role] = policy
	}
	for _, wantedPolicy := range desired.policies {
		observedPolicy, exists := actualPolicies[wantedPolicy.role]
		if wantedPolicy.targetsPending() {
			if exists {
				state.L7PolicyIDs = append(state.L7PolicyIDs, observedPolicy.policy.ID)
				for _, rule := range observedPolicy.rules {
					state.L7RuleIDs = append(state.L7RuleIDs, rule.rule.ID)
				}
			}
			continue
		}
		if !exists {
			step := routeMutation{kind: routeMutationCreatePolicy, parentID: poolID, policy: wantedPolicy}
			if wantedPolicy.weightedPlan != nil {
				step.weightedTargets = &wantedPolicy.weightedPlan.targets
			}
			plan = append(plan, step)
			continue
		}
		policy := observedPolicy.policy
		state.L7PolicyIDs = append(state.L7PolicyIDs, policy.ID)
		weighted := wantedPolicy.weightedPlan
		targetUpdate := weighted != nil && (!slices.EqualFunc(weighted.targets, observedPolicy.weightedTargets, weightedTargetsEqual) || policy.ProvisioningStatus == "ERROR") ||
			desired.fixedResponse && (!observedPolicy.fixedResponse || policy.ProvisioningStatus == "ERROR") ||
			!desired.fixedResponse && poolID != "" && (observedPolicy.fixedResponse ||
				observedPolicy.targetObserved && policy.ProvisioningStatus == "ERROR" && policy.RedirectPoolID == poolID)
		if targetUpdate {
			step := routeMutation{kind: routeMutationSetPolicyTarget, id: policy.ID, parentID: poolID}
			if weighted != nil {
				step.weightedTargets = &weighted.targets
			}
			plan = append(plan, step)
		}
		start := len(plan)
		if weighted == nil && !desired.fixedResponse && !observedPolicy.fixedResponse && !desiredPolicyActionMatches(wantedPolicy, policy, poolID) {
			plan = append(plan, routeMutation{kind: routeMutationUpdatePolicy, id: policy.ID, parentID: poolID, policy: wantedPolicy})
		}

		wantedRules := rulesFor(wantedPolicy, desired.hostnameRuleValue)
		actualRules := make(map[string]observedRouteRule, len(observedPolicy.rules))
		for _, rule := range observedPolicy.rules {
			actualRules[rule.role] = rule
		}
		for _, wantedRule := range wantedRules {
			observedRule, exists := actualRules[wantedRule.role]
			if !exists {
				plan = append(plan, routeMutation{kind: routeMutationCreateRule, parentID: policy.ID, rule: wantedRule})
				continue
			}
			rule := observedRule.rule
			state.L7RuleIDs = append(state.L7RuleIDs, rule.ID)
			if rule.RuleType != string(wantedRule.ruleType) || rule.CompareType != string(wantedRule.compareType) ||
				rule.Value != wantedRule.value || rule.Invert != wantedRule.invert || !rule.AdminStateUp || rule.ProvisioningStatus == "ERROR" {
				plan = append(plan, routeMutation{kind: routeMutationUpdateRule, id: rule.ID, parentID: policy.ID, rule: wantedRule})
			}
			delete(actualRules, wantedRule.role)
		}
		staleRules := make([]observedRouteRule, 0, len(actualRules))
		for _, rule := range actualRules {
			staleRules = append(staleRules, rule)
		}
		sort.Slice(staleRules, func(i, j int) bool { return staleRules[i].rule.ID < staleRules[j].rule.ID })
		for _, rule := range staleRules {
			plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.rule.ID, parentID: policy.ID})
		}
		// A failed policy update can leave Octavia's desired admin state in
		// the database even while Amphora still uses an earlier configuration.
		if policy.ProvisioningStatus == "ERROR" && !targetUpdate || len(plan) > start && policy.AdminStateUp {
			disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
				retry: desired.redirect && policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: desired.redirect})
		}
		// Moving a policy changes precedence only. Keep its current admin state
		// and predicates and let Octavia shift the other positions atomically.
		if policy.Position != wantedPolicy.position {
			plan = append(plan, routeMutation{kind: routeMutationMovePolicy, id: policy.ID, policy: wantedPolicy})
		}
	}
	return plan, disablePlan, state
}

func buildRouteDeletionPlan(actual observedRouteGraph) []routeMutation {
	plan := make([]routeMutation, 0)
	for _, policy := range actual.policies {
		if !policy.pending() && (policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR") {
			plan = append(plan, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID, retry: policy.policy.ProvisioningStatus == "ERROR"})
		}
	}
	for _, policy := range actual.policies {
		for _, rule := range policy.rules {
			plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.rule.ID, parentID: policy.policy.ID})
		}
	}
	for _, policy := range actual.policies {
		plan = append(plan, routeMutation{kind: routeMutationDeletePolicy, id: policy.policy.ID})
	}
	if actual.monitor != nil {
		plan = append(plan, routeMutation{kind: routeMutationDeleteMonitor, id: actual.monitor.ID, parentID: actual.pool.ID})
	}
	for _, member := range actual.members {
		plan = append(plan, routeMutation{kind: routeMutationDeleteMember, id: member.ID, parentID: actual.pool.ID})
	}
	if actual.pool != nil {
		plan = append(plan, routeMutation{kind: routeMutationDeletePool, id: actual.pool.ID})
	}
	return plan
}

func routeNeedsHTTPCapability(route desiredRoute, step routeMutation) bool {
	switch step.kind {
	case routeMutationCreatePolicy, routeMutationSetPolicyTarget, routeMutationEnablePolicy:
		return route.fixedResponse || route.weightedTargets != nil || step.weightedTargets != nil
	default:
		return false
	}
}

func (p *Provider) executeRouteMutation(ctx context.Context, desired desiredRoute, step routeMutation) error {
	var err error
	switch step.kind {
	case routeMutationCreatePool:
		tags, tagErr := desired.identity.RouteTags(desired.httpRole(rolePool, ""))
		if tagErr != nil {
			return tagErr
		}
		_, err = p.octavia.CreatePool(ctx, pools.CreateOpts{
			LBMethod:       pools.LBMethodRoundRobin,
			Protocol:       pools.Protocol(desired.protocol()),
			LoadbalancerID: desired.spec.Gateway.LoadBalancerID,
			Name:           resourceName(desired.spec.Identity, desired.httpRole(rolePool, "")),
			Description:    desired.identity.Description(desired.httpRole(rolePool, "")),
			AdminStateUp:   boolPointer(true),
			Tags:           tags,
		})
	case routeMutationUpdatePool:
		opts := pools.UpdateOpts{
			LBMethod:     pools.LBMethodRoundRobin,
			AdminStateUp: boolPointer(true),
		}
		if step.clearPoolPersistence {
			// Gophercloud encodes a non-nil empty persistence as a JSON null.
			opts.Persistence = &pools.SessionPersistence{}
		}
		_, err = p.octavia.UpdatePool(ctx, step.id, opts)
	case routeMutationDisableMember:
		_, err = p.octavia.UpdateMember(ctx, step.parentID, step.id, pools.UpdateMemberOpts{AdminStateUp: boolPointer(false)})
	case routeMutationDeleteMember:
		err = p.octavia.DeleteMember(ctx, step.parentID, step.id)
	case routeMutationCreateMember:
		tags, tagErr := desired.identity.RouteTags(desired.httpRole(roleMember, ""))
		if tagErr != nil {
			return tagErr
		}
		_, err = p.octavia.CreateMember(ctx, step.parentID, pools.CreateMemberOpts{
			Address:      step.member.Address,
			ProtocolPort: step.member.Port,
			Name:         resourceName(desired.spec.Identity, desired.httpRole(roleMember, "")),
			SubnetID:     desired.spec.MemberSubnetID,
			AdminStateUp: boolPointer(true),
			Tags:         tags,
			Weight:       desiredMemberWeight(desired.spec.MemberMode, step.member),
		})
	case routeMutationUpdateMember:
		opts := pools.UpdateMemberOpts{
			AdminStateUp: boolPointer(true),
			Weight:       desiredMemberWeight(desired.spec.MemberMode, step.member),
		}
		if step.clearMemberBackup {
			opts.Backup = boolPointer(false)
		}
		_, err = p.octavia.UpdateMember(ctx, step.parentID, step.id, opts)
	case routeMutationCreateMonitor:
		tags, tagErr := desired.identity.RouteTags(desired.httpRole(roleMonitor, ""))
		if tagErr != nil {
			return tagErr
		}
		opts := monitors.CreateOpts{
			PoolID:         step.parentID,
			Type:           desired.monitorType(),
			Delay:          desired.healthMonitor.Delay,
			Timeout:        desired.healthMonitor.Timeout,
			MaxRetries:     desired.healthMonitor.MaxRetries,
			MaxRetriesDown: desired.healthMonitor.MaxRetriesDown,
			Name:           resourceName(desired.spec.Identity, desired.httpRole(roleMonitor, "")),
			AdminStateUp:   boolPointer(true),
			Tags:           tags,
		}
		if opts.Type == monitors.TypeHTTP {
			opts.URLPath, opts.HTTPMethod, opts.ExpectedCodes = desired.healthMonitor.Path, http.MethodGet, routeMonitorExpectedCodes
		}
		_, err = p.octavia.CreateMonitor(ctx, opts)
	case routeMutationUpdateMonitor:
		opts := monitors.UpdateOpts{
			Delay:          desired.healthMonitor.Delay,
			Timeout:        desired.healthMonitor.Timeout,
			MaxRetries:     desired.healthMonitor.MaxRetries,
			MaxRetriesDown: desired.healthMonitor.MaxRetriesDown,
			AdminStateUp:   boolPointer(true),
		}
		if desired.monitorType() == monitors.TypeHTTP {
			opts.URLPath, opts.HTTPMethod, opts.ExpectedCodes = desired.healthMonitor.Path, http.MethodGet, routeMonitorExpectedCodes
		}
		_, err = p.octavia.UpdateMonitor(ctx, step.id, opts)
	case routeMutationDisablePolicy:
		_, err = p.octavia.UpdatePolicy(ctx, step.id, l7policies.UpdateOpts{
			AdminStateUp: boolPointer(false),
		})
	case routeMutationDeleteRule:
		err = p.octavia.DeleteRule(ctx, step.parentID, step.id)
	case routeMutationDeletePolicy:
		err = p.octavia.DeletePolicy(ctx, step.id)
	case routeMutationCreatePolicy:
		tags, tagErr := desired.identity.RouteTags(step.policy.role)
		if tagErr != nil {
			return tagErr
		}
		opts := l7policies.CreateOpts{
			Name:           resourceName(desired.spec.Identity, step.policy.role),
			ListenerID:     desired.spec.Gateway.ListenerID,
			Action:         l7policies.ActionRedirectToPool,
			Position:       step.policy.position,
			Description:    desired.identity.Description(step.policy.role),
			RedirectPoolID: step.parentID,
			AdminStateUp:   boolPointer(false),
			Tags:           tags,
		}
		if redirect := step.policy.redirect; redirect != nil {
			opts.Action, opts.RedirectPrefix, opts.RedirectHttpCode = l7policies.ActionRedirectPrefix, redirect.prefix, redirect.statusCode
		}
		if step.weightedTargets != nil {
			_, err = p.octavia.CreateWeightedPolicy(ctx, opts, *step.weightedTargets)
		} else if desired.fixedResponse {
			_, err = p.octavia.CreateFixedResponsePolicy(ctx, opts)
		} else {
			_, err = p.octavia.CreatePolicy(ctx, opts)
		}
	case routeMutationUpdatePolicy:
		opts := l7policies.UpdateOpts{AdminStateUp: boolPointer(false)}
		if redirect := step.policy.redirect; redirect != nil {
			opts.Action, opts.RedirectPrefix, opts.RedirectHttpCode = l7policies.ActionRedirectPrefix, &redirect.prefix, redirect.statusCode
		} else {
			opts.Action, opts.RedirectPoolID = l7policies.ActionRedirectToPool, &step.parentID
		}
		_, err = p.octavia.UpdatePolicy(ctx, step.id, opts)
	case routeMutationSetPolicyTarget:
		if step.weightedTargets != nil {
			_, err = p.octavia.SetPolicyWeightedTargets(ctx, step.id, *step.weightedTargets)
		} else if desired.fixedResponse {
			_, err = p.octavia.SetPolicyFixedResponse(ctx, step.id)
		} else {
			_, err = p.octavia.UpdatePolicy(ctx, step.id, l7policies.UpdateOpts{RedirectPoolID: &step.parentID})
		}
	case routeMutationMovePolicy:
		_, err = p.octavia.UpdatePolicy(ctx, step.id, l7policies.UpdateOpts{Position: step.policy.position})
	case routeMutationCreateRule:
		tags, tagErr := desired.identity.RouteTags(step.rule.role)
		if tagErr != nil {
			return tagErr
		}
		_, err = p.octavia.CreateRule(ctx, step.parentID, l7policies.CreateRuleOpts{
			RuleType:     step.rule.ruleType,
			CompareType:  step.rule.compareType,
			Value:        step.rule.value,
			Invert:       step.rule.invert,
			AdminStateUp: boolPointer(true),
			Tags:         tags,
		})
	case routeMutationUpdateRule:
		_, err = p.octavia.UpdateRule(ctx, step.parentID, step.id, l7policies.UpdateRuleOpts{
			RuleType:     step.rule.ruleType,
			CompareType:  step.rule.compareType,
			Value:        step.rule.value,
			Invert:       boolPointer(step.rule.invert),
			AdminStateUp: boolPointer(true),
		})
	case routeMutationEnablePolicy:
		_, err = p.octavia.UpdatePolicy(ctx, step.id, l7policies.UpdateOpts{
			AdminStateUp: boolPointer(true),
		})
	case routeMutationDeleteMonitor:
		err = p.octavia.DeleteMonitor(ctx, step.id)
	case routeMutationDeletePool:
		err = p.octavia.DeletePool(ctx, step.id)
	default:
		return fmt.Errorf("unsupported route mutation %d", step.kind)
	}
	if err != nil && isNotFound(err) && isRouteDeletion(step.kind) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: %w", step.message(), classifyOctaviaMutationError(err))
	}
	return nil
}

// Endpoint mode owns weight for readiness and explicit TCP and UDP traffic shares.
func desiredMemberWeight(mode cloud.MemberMode, member cloud.Member) *int {
	if mode != cloud.MemberModeEndpoint {
		return nil
	}
	weight := member.Weight
	switch {
	case member.Draining:
		weight = 0
	case weight == 0:
		weight = 1
	}
	return &weight
}

func isRouteDeletion(kind routeMutationKind) bool {
	switch kind {
	case routeMutationDeleteMember, routeMutationDeleteRule, routeMutationDeletePolicy,
		routeMutationDeleteMonitor, routeMutationDeletePool:
		return true
	default:
		return false
	}
}
