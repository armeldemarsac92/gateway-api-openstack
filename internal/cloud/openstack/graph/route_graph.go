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
	policy                   l7policies.L7Policy
	role                     string
	rules                    []observedRouteRule
	versioned                bool
	redirect                 bool
	requestRedirect          *octavia.RequestRedirect
	requestRedirectPresent   bool
	requestURLRewrite        *octavia.RequestURLRewrite
	requestPolicy            *cloud.HTTPRequestPolicy
	externalAuth             *octavia.PolicyExternalAuth
	externalAuthPresent      bool
	externalAuthReported     bool
	externalAuthTagged       bool
	requestMirrors           []octavia.HTTPRequestMirror
	requestMirrorsTagged     bool
	requestMirrorsReported   bool
	requestMirrorsUnknown    bool
	requestPolicyPresent     bool
	requestPolicyReported    bool
	requestPolicyTagged      bool
	retryBackoffTagged       bool
	urlRewritePresent        bool
	urlRewriteReported       bool
	urlRewriteTagged         bool
	requestCORS              *cloud.HTTPCORS
	corsPresent              bool
	corsReported             bool
	corsTagged               bool
	adminObserved            bool
	initialRules             bool
	initialRulesPartial      bool
	activationOptions        string
	fixedResponse            bool
	targetObserved           bool
	weightedTargets          []octavia.WeightedHTTPTarget
	requestHeaderOperations  []cloud.HTTPHeaderOperation
	responseHeaderOperations []cloud.HTTPHeaderOperation
	httpsListenerHostname    string

	backendRequestFiltersTagged  bool
	backendResponseHeadersTagged bool
}

type observedRouteRule struct {
	rule          l7policies.Rule
	role          string
	base          string
	flagsObserved bool
}

type observedRouteGraph struct {
	cookieDeadline         octavia.PoolCookieDeadline
	cookiePath             octavia.PoolCookiePath
	headerPersistence      octavia.PoolHeaderPersistence
	pool                   *pools.Pool
	backendH2C             bool
	backendTLS             octavia.PoolBackendTLS
	backendRetryBudget     octavia.PoolBackendRetryBudget
	tcpRejection           octavia.PoolTCPConnectionRejection
	udpDrop                octavia.PoolUDPPacketDrop
	members                []pools.Member
	memberBackupUnreported map[string]struct{}
	memberAdminUnreported  map[string]struct{}
	memberBatchUnreported  map[string]struct{}
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
	expectsBackendTLS          bool
	requiresPolicyAbsenceProof bool
	requiresHTTPHostIsolation  bool
	requestCORSPorts           []int
	routeExternalAuthPorts     []int
	backendResponseHeaderPorts []int
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
	if slices.Contains(bases, raw) && !openstackidentity.HTTPRuleNeedsPolicyIdentity(raw) {
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
	scopedPolicies   map[string]httpsPolicyInventory
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
		if policy.versioned || policy.initialRules {
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
	poolInventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return nil, err
	}
	return p.observeRouteGraphsFromPools(ctx, owners, lb, listener, protocol, scopedListeners, poolInventory, nil, nil)
}

func (p *Provider) observeRouteGraphsFromPools(ctx context.Context, owners []routeGraphOwner, lb *octavia.LoadBalancer, listener *listeners.Listener, protocol string, scopedListeners []listeners.Listener, poolInventory octavia.PoolInventory, recoverGateway *Identity, scopedPolicies map[string]httpsPolicyInventory) ([]observedRouteGraph, error) {
	var err error
	lb, poolInventory.Items, err = omitTelemetryPoolsFromRoutes(lb, poolInventory.Items)
	if err != nil {
		return nil, err
	}
	poolItems := poolInventory.Items
	inventory := routeGraphInventory{
		owners: slices.Clone(owners), graphs: make([]observedRouteGraph, len(owners)),
		recoverGateway: recoverGateway, recoverAllRoutes: len(owners) == 0 || p.fixedHTTPResponses,
		scopedPolicies: scopedPolicies,
	}
	if recoverGateway != nil {
		inventory.recovered = make(map[openstackidentity.RouteOwnerProof]int)
	}
	defaultPool := usesDefaultPool(protocol)
	scopedHTTP := protocol == cloud.ListenerProtocolHTTP && scopedListeners != nil
	strictHTTP := scopedHTTP || protocol == cloud.ListenerProtocolHTTP && p.fixedHTTPResponses || containsHTTPFragmentPool(poolItems) ||
		slices.ContainsFunc(owners, func(owner routeGraphOwner) bool {
			return owner.expectsHTTPParts || owner.expectsBackendTLS || owner.requiresHTTPHostIsolation
		}) ||
		slices.ContainsFunc(poolItems, func(pool pools.Pool) bool {
			return hasObservedBackendTLS(pool, poolInventory.BackendTLS[pool.ID])
		}) || recoverGateway != nil
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
			validateParent := validateDefaultPoolParent
			if owner.expectsBackendTLS || hasObservedBackendTLS(*pool, poolInventory.BackendTLS[pool.ID]) {
				validateParent = validateBoundPoolParent
			}
			if err := validateParent(*pool, lb.ID, cloud.ListenerProtocolHTTP); err != nil {
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
		graph.backendH2C = poolInventory.BackendH2C[pool.ID]
		graph.backendTLS = poolInventory.BackendTLS[pool.ID]
		graph.cookieDeadline = poolInventory.CookieDeadline[pool.ID]
		graph.cookiePath = poolInventory.CookiePath[pool.ID]
		graph.headerPersistence = poolInventory.HeaderPersistence[pool.ID]
		graph.backendRetryBudget = poolInventory.BackendRetryBudget[pool.ID]
		graph.tcpRejection = poolInventory.TCPConnectionRejection[pool.ID]
		graph.udpDrop = poolInventory.UDPPacketDrop[pool.ID]
		poolOwners[pool.ID] = poolRouteOwner{route: routeIndex, backend: role.backend}
		if err := p.observeRoutePoolChildrenWithRole(ctx, owner, graph, role.backend); err != nil {
			return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
		}
		if strictHTTP && !usesDefaultPool(poolProtocol) {
			if err := graph.validateScopedHTTPPoolChildren(); err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
		} else if usesDefaultPool(poolProtocol) {
			if graph.udpDropPool() {
				if err := p.validateUDPPoolCascade(*graph); err != nil {
					return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
				}
			}
			if err := graph.validateDefaultPoolChildren(); err != nil {
				return nil, &cloud.GraphError{Identity: identity.Value(), Err: err}
			}
		}
	}
	if defaultPool {
		return inventory.graphs, nil
	}
	var frontends []listeners.Listener
	if scopedHTTP {
		frontends = scopedListeners
	} else if listener != nil {
		if strictHTTP && (listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID) {
			return nil, fmt.Errorf("%w: HTTP listener %s has an unproven parent or project", cloud.ErrOwnershipConflict, listener.ID)
		}
		frontends = []listeners.Listener{*listener}
	}
	for i := range frontends {
		if err := p.observeListenerPolicies(ctx, &inventory, &frontends[i], protocol, poolOwners, strictHTTP); err != nil {
			var incomplete *httpFilterInventoryError
			if strictHTTP && errors.As(err, &incomplete) {
				// Pool ownership, membership and project were proved above.
				// Keep that readback available only for TLS withdrawal.
				incomplete.ownedPools = make([]observedRouteGraph, len(owners))
				for _, owner := range poolOwners {
					if owner.route >= len(owners) {
						continue
					}
					part := inventory.graphs[owner.route].part(owner.backend)
					*incomplete.ownedPools[owner.route].part(owner.backend) = observedRouteGraph{
						pool: part.pool, backendTLS: part.backendTLS, backendH2C: part.backendH2C, members: part.members,
					}
				}
			}
			return nil, err
		}
	}
	if !strictHTTP && (containsObservedRedirect(inventory.graphs) || containsObservedRequestURLRewrite(inventory.graphs) || containsObservedCORS(inventory.graphs)) {
		if listener == nil || listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID {
			return nil, fmt.Errorf("%w: HTTP policy has an unproven listener parent or project", cloud.ErrOwnershipConflict)
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

func reportsBackendResponseHeaders(row octavia.L7PolicyObservation) bool {
	return row.TargetResponseHeadersPresent || slices.Contains(row.Policy.Tags, openstackidentity.BackendResponseHeadersPolicyTag)
}

// A partial target vector can still expose explicit contradictions. Check
// readable ownership evidence before an incomplete inventory can close a port.
func (p *Provider) validateBackendResponseHeaderOwnership(inventory *routeGraphInventory, listener *listeners.Listener,
	protocol string, poolOwners map[string]poolRouteOwner, observation octavia.L7PolicyObservation,
) error {
	item := observation.Policy
	routeIndex, role, err := inventory.resolve(item.Tags, rolePolicyExact, rolePolicyPrefix)
	if err != nil {
		return fmt.Errorf("validate L7 policy %s: %w", item.ID, err)
	}
	owner := inventory.owners[routeIndex]
	if protocol != cloud.ListenerProtocolHTTP || listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
		owner.cleanupProtocol != "" && owner.cleanupProtocol != protocol ||
		!role.weighted || item.Action != "" && item.Action != string(octavia.ActionWeightedTargets) ||
		observation.RedirectRequestPresent || item.ProjectID != p.projectID || item.ListenerID != listener.ID ||
		item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" || item.RedirectHttpCode != 0 ||
		observation.Target != nil && observation.Target.FixedResponse != nil {
		return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s reports backend response headers on an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
	}
	// Pool IDs are available even when another slot cannot be decoded.
	for _, poolID := range observation.PoolIDs {
		if pool, found := poolOwners[poolID]; !found || pool.route != routeIndex || pool.backend == "" {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven response header target pool %s", cloud.ErrOwnershipConflict, item.ID, poolID)}
		}
	}
	return nil
}

func (p *Provider) observeListenerPolicies(ctx context.Context, inventory *routeGraphInventory, listener *listeners.Listener, protocol string, poolOwners map[string]poolRouteOwner, strict bool) error {
	cached, found := inventory.scopedPolicies[listener.ID]
	items, complete, err := cached.items, cached.complete, cached.err
	if !found {
		items, complete, err = p.octavia.ListPolicyObservations(ctx, l7policies.ListOpts{ListenerID: listener.ID, ProjectID: p.projectID})
	} else if !complete && err == nil {
		err = cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for complete Octavia HTTPS policy inventory"))
	}
	corsRelevant := protocol == cloud.ListenerProtocolHTTP &&
		(slices.ContainsFunc(inventory.owners, func(owner routeGraphOwner) bool {
			return slices.Contains(owner.requestCORSPorts, listener.ProtocolPort)
		}) || slices.ContainsFunc(items, func(row octavia.L7PolicyObservation) bool {
			return row.Policy.ProjectID == p.projectID && row.Policy.ListenerID == listener.ID &&
				(row.RequestCORSPresent || slices.Contains(row.Policy.Tags, openstackidentity.RequestCORSPolicyTag))
		}))
	authRelevant := protocol == cloud.ListenerProtocolHTTP &&
		(slices.ContainsFunc(inventory.owners, func(owner routeGraphOwner) bool {
			return slices.Contains(owner.routeExternalAuthPorts, listener.ProtocolPort)
		}) || slices.ContainsFunc(items, func(row octavia.L7PolicyObservation) bool {
			return row.Policy.ProjectID == p.projectID && row.Policy.ListenerID == listener.ID &&
				(row.ExternalAuthPresent || slices.Contains(row.Policy.Tags, routeExternalAuthTag))
		}))
	backendResponseHeadersRelevant := protocol == cloud.ListenerProtocolHTTP &&
		(slices.ContainsFunc(inventory.owners, func(owner routeGraphOwner) bool {
			return slices.Contains(owner.backendResponseHeaderPorts, listener.ProtocolPort)
		}) || slices.ContainsFunc(items, func(row octavia.L7PolicyObservation) bool {
			return row.Policy.ProjectID == p.projectID && row.Policy.ListenerID == listener.ID &&
				reportsBackendResponseHeaders(row)
		}))
	inventoryError := func(err error) error {
		if authRelevant && !errors.Is(err, cloud.ErrOwnershipConflict) {
			return incompleteRouteAuthInventory(listener.ID, "", err)
		}
		if backendResponseHeadersRelevant && !errors.Is(err, cloud.ErrOwnershipConflict) {
			return incompleteBackendResponseHeaderInventory(listener.ID, "", err)
		}
		if corsRelevant && !errors.Is(err, cloud.ErrOwnershipConflict) {
			return incompleteCORSInventory(listener.ID, "", err)
		}
		return err
	}
	for _, row := range items {
		if reportsBackendResponseHeaders(row) {
			if ownership := p.validateBackendResponseHeaderOwnership(inventory, listener, protocol, poolOwners, row); ownership != nil {
				return ownership
			}
		}
	}
	if err != nil {
		return inventoryError(fmt.Errorf("list Gateway L7 policies: %w", err))
	}
	otherCleanup := hasOtherCleanupProtocol(inventory.owners, protocol)
	policyID := func(item octavia.L7PolicyObservation) string { return item.Policy.ID }
	if !strict && slices.ContainsFunc(items, func(item octavia.L7PolicyObservation) bool {
		role, ok := readRouteResourceRole(item.Policy.Tags, rolePolicyExact, rolePolicyPrefix)
		return ok && (role.redirect || item.RequestURLRewritePresent || slices.Contains(item.Policy.Tags, openstackidentity.RequestURLRewritePolicyTag) ||
			item.HTTPRequestPolicyPresent || slices.Contains(item.Policy.Tags, openstackidentity.HTTPRequestPolicyTag) ||
			item.RequestCORSPresent || slices.Contains(item.Policy.Tags, openstackidentity.RequestCORSPolicyTag) ||
			item.ExternalAuthPresent || slices.Contains(item.Policy.Tags, routeExternalAuthTag) ||
			reportsBackendResponseHeaders(item))
	}) {
		strict = true
	}
	strictInventory := strict || slices.ContainsFunc(items, func(item octavia.L7PolicyObservation) bool {
		return slices.Contains(item.Policy.Tags, initialL7RulesTag)
	})
	refs := make([]octavia.L7PolicyObservation, 0, len(listener.L7Policies))
	if listener.L7Policies == nil {
		refs = nil
	}
	for _, ref := range listener.L7Policies {
		refs = append(refs, octavia.L7PolicyObservation{Policy: l7policies.L7Policy{ID: ref.ID}})
	}
	requiresAbsenceProof := slices.ContainsFunc(inventory.owners, func(owner routeGraphOwner) bool { return owner.requiresPolicyAbsenceProof })
	// A stock 204 has no list body. During cleanup only a populated parent
	// inventory, including an explicit empty array, can corroborate it.
	if strictInventory || !complete && requiresAbsenceProof {
		if err := crossCheckCompleteChildInventory("listener", listener.ID, "L7 policy", refs, items, policyID); err != nil {
			return inventoryError(err)
		}
	} else if otherCleanup || requiresAbsenceProof {
		if err := crossCheckChildInventory("listener", listener.ID, "L7 policy", refs, items, policyID); err != nil {
			return err
		}
	}
	if !complete && (p.fixedHTTPResponses || protocol == cloud.ListenerProtocolHTTP &&
		(p.httpRequestRedirect || p.httpRequestURLRewrite || p.httpCORS || p.httpRequestMirror || strictInventory || !listener.AdminStateUp)) {
		return inventoryError(cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for complete Octavia HTTP policy inventory")))
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
		initialRules := slices.Contains(item.Tags, initialL7RulesTag)
		strictPolicy := strict || initialRules || role.backend != "" || p.fixedHTTPResponses
		fixedResponse := item.Action == string(octavia.ActionFixedResponse)
		weighted := item.Action == string(octavia.ActionWeightedTargets)
		requestRedirect := item.Action == string(octavia.ActionRedirectRequest) || observation.RedirectRequestPresent
		target := observation.Target
		corsTag := slices.Contains(item.Tags, openstackidentity.RequestCORSPolicyTag)
		hasCORS := corsTag || observation.RequestCORSPresent
		hasRouteAuth := observation.ExternalAuthPresent || slices.Contains(item.Tags, routeExternalAuthTag)
		if hasRouteAuth {
			if protocol != cloud.ListenerProtocolHTTP || listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
				role.redirect || requestRedirect || item.ProjectID != p.projectID || item.ListenerID != listener.ID ||
				!slices.Contains(item.Tags, routeExternalAuthTag) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: unproven HTTPRoute authorization policy", cloud.ErrOwnershipConflict)}
			}
			if !observation.ExternalAuthReported || observation.ExternalAuthPresent && observation.ExternalAuth == nil ||
				observation.AdminState == nil || item.Position <= 0 ||
				item.Action != string(l7policies.ActionRedirectToPool) && item.Action != string(octavia.ActionWeightedTargets) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: incompleteRouteAuthInventory(listener.ID, item.ID, nil)}
			}
		}
		if strictPolicy && (item.ProjectID != p.projectID || item.ListenerID != listener.ID) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven project or listener", cloud.ErrOwnershipConflict, item.ID)}
		}
		missingWeightedCORSAction := hasCORS && role.weighted && item.Action == ""
		if hasCORS && (protocol != cloud.ListenerProtocolHTTP || listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
			role.redirect || requestRedirect || role.weighted && !weighted && !missingWeightedCORSAction ||
			!role.weighted && item.Action != string(l7policies.ActionRedirectToPool)) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s reports CORS on an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
		}
		if poolOwner, found := poolOwners[item.RedirectPoolID]; found && poolOwner != (poolRouteOwner{route: routeIndex, backend: role.backend}) ||
			(otherCleanup || strictPolicy) && !found && item.RedirectPoolID != "" {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s redirects to an unproven pool %s", cloud.ErrOwnershipConflict, item.ID, item.RedirectPoolID)}
		}
		backendResponseHeaderTag := slices.Contains(item.Tags, openstackidentity.BackendResponseHeadersPolicyTag)
		hasBackendResponseHeaders := reportsBackendResponseHeaders(observation)
		if hasBackendResponseHeaders && (!weighted || target == nil || target.AdminStateUp == nil || item.Position <= 0 ||
			string(observation.PoolReference) != "null" || !ownedWeightedHTTPPolicyTargets(target.WeightedTargets, poolOwners, routeIndex) ||
			slices.ContainsFunc(target.WeightedTargets, func(slot octavia.WeightedHTTPTarget) bool {
				return len(slot.ResponseHeaderOperations) != 0 && poolOwners[slot.PoolID].backend == grpcFailurePoolKey
			})) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: incompleteBackendResponseHeaderInventory(listener.ID, item.ID, nil)}
		}
		if hasCORS && role.weighted && (item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" ||
			item.RedirectHttpCode != 0 || target != nil && target.FixedResponse != nil ||
			slices.ContainsFunc(observation.PoolIDs, func(poolID string) bool {
				poolOwner, found := poolOwners[poolID]
				return !found || poolOwner.route != routeIndex || poolOwner.backend == ""
			})) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven CORS target pool", cloud.ErrOwnershipConflict, item.ID)}
		}
		if hasCORS && (missingWeightedCORSAction || item.Position <= 0 || observation.AdminState == nil ||
			observation.RequestCORSPresent && !observation.HasValidRequestCORS()) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: incompleteCORSInventory(listener.ID, item.ID, nil)}
		}
		if strictPolicy && !role.redirect && !fixedResponse && !weighted && item.RedirectPoolID == "" {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven pool", cloud.ErrOwnershipConflict, item.ID)}
		}
		mirrorTag := slices.Contains(item.Tags, openstackidentity.HTTPRequestMirrorTag)
		hasMirrors := mirrorTag || observation.RequestMirrorsErr != nil || len(observation.RequestMirrors) != 0 || observation.TargetMirrorsPresent
		if hasMirrors && (protocol != cloud.ListenerProtocolHTTP || listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
			role.redirect || requestRedirect || item.ProjectID != p.projectID || item.ListenerID != listener.ID) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s reports request mirrors on an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
		}
		if hasMirrors && weighted && target == nil {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				&httpFilterInventoryError{listenerID: listener.ID, policyID: item.ID, filter: "request mirrors"})}
		}
		backendFilterTag := slices.Contains(item.Tags, openstackidentity.BackendRequestFiltersPolicyTag)
		hasBackendFilters := backendFilterTag || observation.TargetRequestFiltersPresent
		if hasBackendFilters && item.ProjectID == p.projectID && item.ListenerID == listener.ID &&
			(!role.weighted || !weighted || target == nil || target.AdminStateUp == nil || len(target.WeightedTargets) == 0) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				&httpFilterInventoryError{listenerID: listener.ID, policyID: item.ID, filter: "backend request filter"})}
		}
		switch {
		case requestRedirect && !role.redirect:
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s reports a request redirect without its owning role", cloud.ErrOwnershipConflict, item.ID)}
		case role.redirect:
			if protocol != cloud.ListenerProtocolHTTP || listener.ProjectID != p.projectID ||
				(listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS) || item.RedirectPoolID != "" ||
				observation.ExtendedTargetFields || fixedResponse || weighted || requestRedirect && string(observation.PoolReference) != "null" {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: HTTP redirect policy %s has an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
			}
			if !initialRules && observation.AdminState == nil || len(observation.PoolReference) == 0 || item.Action == "" ||
				(item.Action == string(l7policies.ActionRedirectPrefix) && (item.RedirectPrefix == "" || item.RedirectHttpCode == 0)) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
					fmt.Errorf("HTTP redirect policy %s has incomplete action or administrative state", item.ID))}
			}
		case role.weighted || weighted:
			if !p.weightedHTTPTargets && !hasBackendFilters && !hasBackendResponseHeaders && !hasCORS && !hasMirrors && !hasRouteAuth || !role.weighted || !weighted || target == nil || target.FixedResponse != nil || !initialRules && target.AdminStateUp == nil ||
				string(target.RedirectPool) != "null" || item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" || item.RedirectHttpCode != 0 ||
				!ownedWeightedHTTPPolicyTargets(target.WeightedTargets, poolOwners, routeIndex) {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven weighted HTTP target", cloud.ErrOwnershipConflict, item.ID)}
			}
		case fixedResponse:
			if !p.fixedHTTPResponses || role.backend == "" || target == nil || !target.IsFixedServerError() ||
				!initialRules && target.AdminStateUp == nil || len(target.WeightedTargets) != 0 ||
				item.RedirectPoolID != "" || item.RedirectURL != "" || item.RedirectPrefix != "" || item.RedirectHttpCode != 0 {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven fixed HTTP response", cloud.ErrOwnershipConflict, item.ID)}
			}
		case p.fixedHTTPResponses:
			if item.Action != string(l7policies.ActionRedirectToPool) || target == nil || target.FixedResponse != nil || !initialRules && target.AdminStateUp == nil ||
				len(target.WeightedTargets) != 0 ||
				item.RedirectURL != "" || item.RedirectPrefix != "" {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s has an unproven forwarding action", cloud.ErrOwnershipConflict, item.ID)}
			}
		}
		observed, err := p.observeRoutePolicy(ctx, owner, item, listener.ID, role)
		if err != nil {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: inventoryError(err)}
		}
		if observation.RequestHeaderOperationsErr != nil {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(
				cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("L7 policy %s reports invalid request header operations: %w", item.ID, observation.RequestHeaderOperationsErr))}
		}
		if observation.ResponseHeaderOperationsErr != nil {
			if hasBackendResponseHeaders {
				return &cloud.GraphError{Identity: owner.identity.Value(), Err: incompleteBackendResponseHeaderInventory(listener.ID, item.ID, observation.ResponseHeaderOperationsErr)}
			}
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(
				cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("L7 policy %s reports invalid response header operations: %w", item.ID, observation.ResponseHeaderOperationsErr))}
		}
		if len(observation.ResponseHeaderOperations) != 0 && item.Action != string(l7policies.ActionRedirectToPool) &&
			!fixedResponse && !weighted {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: cloud.NewProviderError(
				cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("L7 policy %s reports response header operations on an incompatible action", item.ID))}
		}
		observed.requestHeaderOperations = observation.RequestHeaderOperations
		observed.responseHeaderOperations = observation.ResponseHeaderOperations
		observed.httpsListenerHostname = observation.HTTPSListenerHostname
		observed.adminObserved = observation.AdminState != nil
		observed.activationOptions = observation.ActivationOptions
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
				category, _ := cloud.ErrorCategoryOf(err)
				if !initialRules || corsRelevant || backendResponseHeadersRelevant || category != cloud.ErrorCategoryDependencyNotReady {
					return inventoryError(err)
				}
				observed.initialRulesPartial = true
			}
		}
		graph := inventory.graphs[routeIndex].part(role.backend)
		if strictPolicy && len(graph.policies) != 0 && graph.policies[0].policy.ListenerID != listener.ID {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: HTTPRoute fragment policies appear on multiple listeners", cloud.ErrOwnershipConflict)}
		}
		observed.requestRedirectPresent = requestRedirect
		if observation.HasValidRequestRedirect() {
			observed.requestRedirect = observation.RedirectRequest
		}
		observed.urlRewriteReported, observed.urlRewritePresent = observation.RequestURLRewriteReported, observation.RequestURLRewritePresent
		observed.urlRewriteTagged = slices.Contains(item.Tags, openstackidentity.RequestURLRewritePolicyTag)
		if observation.HasValidRequestURLRewrite() && (listener.Protocol == cloud.ListenerProtocolHTTP || listener.Protocol == terminatedHTTPS) {
			observed.requestURLRewrite = observation.RequestURLRewrite
		}
		observed.requestPolicyPresent = observation.HTTPRequestPolicyPresent
		observed.externalAuth, observed.externalAuthReported, observed.externalAuthPresent = observation.ExternalAuth, observation.ExternalAuthReported, observation.ExternalAuthPresent
		observed.externalAuthTagged = slices.Contains(item.Tags, routeExternalAuthTag)
		observed.requestPolicyReported = observation.HTTPRequestPolicyReported
		observed.requestPolicyTagged = slices.Contains(item.Tags, openstackidentity.HTTPRequestPolicyTag)
		observed.retryBackoffTagged = slices.Contains(item.Tags, openstackidentity.HTTPRetryBackoffTag)
		if observed.hasHTTPRequestPolicy() && (protocol != cloud.ListenerProtocolHTTP ||
			listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
			role.redirect || fixedResponse || !weighted && item.Action != string(l7policies.ActionRedirectToPool)) {
			return &cloud.GraphError{Identity: owner.identity.Value(), Err: fmt.Errorf("%w: L7 policy %s reports an HTTP request policy on an incompatible listener or action", cloud.ErrOwnershipConflict, item.ID)}
		}
		if observation.HasValidHTTPRequestPolicy() {
			observed.requestPolicy = observation.HTTPRequestPolicy
		}
		observed.requestCORS = observation.RequestCORS
		observed.corsPresent, observed.corsReported, observed.corsTagged = observation.RequestCORSPresent, observation.RequestCORSReported, corsTag
		observed.backendRequestFiltersTagged = backendFilterTag
		observed.backendResponseHeadersTagged = backendResponseHeaderTag
		observed.fixedResponse = fixedResponse
		observed.targetObserved = p.fixedHTTPResponses && role.backend != "" && !role.redirect
		if weighted {
			observed.weightedTargets = target.WeightedTargets
		}
		observed.requestMirrors = observation.RequestMirrors
		observed.requestMirrorsReported = observation.RequestMirrorsReported
		observed.requestMirrorsTagged = mirrorTag
		observed.requestMirrorsUnknown = observation.RequestMirrorsErr != nil ||
			hasMirrors && (!observed.requestMirrorsReported || !observed.adminObserved)
		for _, mirror := range observed.allRequestMirrors() {
			pool, owned := poolOwners[mirror.PoolID]
			if !owned || pool.route != routeIndex || pool.backend == "" {
				observed.requestMirrorsUnknown = true
			}
		}
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
	strictChildren := backend != "" || owner.expectsBackendTLS || hasObservedBackendTLS(*graph.pool, graph.backendTLS)
	opts := pools.ListMembersOpts{ProjectID: p.projectID}
	switch {
	case graph.pool.Protocol == cloud.ListenerProtocolTCP && graph.tcpRejection.Present || graph.udpDropPool():
		graph.members, graph.memberBackupUnreported, graph.memberBatchUnreported, err = p.octavia.ListMembersForTransportBatch(ctx, graph.pool.ID, opts)
	case graph.pool.Protocol == cloud.ListenerProtocolTCP:
		graph.members, graph.memberBackupUnreported, err = p.octavia.ListMembersWithBackup(ctx, graph.pool.ID, opts)
	case p.l7PolicyBatchActivation && graph.pool.Protocol == cloud.ListenerProtocolHTTP:
		inspectAdmin := strictChildren || p.fixedHTTPResponses || p.backendH2C || graph.backendH2C
		graph.members, graph.memberAdminUnreported, graph.memberBatchUnreported, err = p.octavia.ListMembersForPolicyActivation(ctx, graph.pool.ID, opts, inspectAdmin)
	case strictChildren || (p.fixedHTTPResponses || p.backendH2C || graph.backendH2C) && graph.pool.Protocol == cloud.ListenerProtocolHTTP:
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
		if strictChildren && member.ProjectID != p.projectID || !owner.matchesRoute(member.Tags, httpResourceRole(roleMember, backend, "")) {
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
		if strictChildren && (monitor.ProjectID != p.projectID || len(monitor.Pools) != 1) ||
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
	initialRules := slices.Contains(item.Tags, initialL7RulesTag)
	observedRules := make([]observedRouteRule, 0, len(ruleItems))
	ruleRoles := make(map[string]string, len(ruleItems))
	for _, observation := range ruleItems {
		rule := observation.Rule
		if err := p.validateOptionalProject("L7 rule", rule.ID, rule.ProjectID); err != nil {
			return observedRoutePolicy{}, err
		}
		ruleRole, ok := readRouteResourceRole(rule.Tags, roleRulePath, roleRuleHost, roleRuleRedirectGuard,
			openstackidentity.RoleRuleHTTPHostExclusion, openstackidentity.RoleRuleHTTPHeaderExact,
			openstackidentity.RoleRuleHTTPQueryParamExact, openstackidentity.RoleRuleHTTPMethod, openstackidentity.RoleRuleGRPCContentType)
		if !ok || !owner.matchesRoute(rule.Tags, ruleRole.raw) || ruleRole.backend != role.backend ||
			ruleRole.match != role.match || ruleRole.redirect != role.redirect || ruleRole.weighted != role.weighted ||
			(role.backend != "" || initialRules) && rule.ProjectID != p.projectID {
			return observedRoutePolicy{}, fmt.Errorf("%w: L7 rule %s beneath policy %s has an unproven project or route, backend or match identity", cloud.ErrOwnershipConflict, rule.ID, item.ID)
		}
		flagsObserved := observation.AdminState != nil && observation.Inverted != nil
		if !initialRules && (role.redirect || openstackidentity.HTTPRuleNeedsPolicyIdentity(ruleRole.base)) &&
			(!flagsObserved || rule.RuleType == "" || rule.CompareType == "" || rule.Value == "") {
			return observedRoutePolicy{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				fmt.Errorf("HTTP rule %s has incomplete predicate or administrative state", rule.ID))
		}
		if existingID := ruleRoles[ruleRole.raw]; existingID != "" {
			return observedRoutePolicy{}, fmt.Errorf("%w: policy %s has duplicate %s rules %s and %s", cloud.ErrOwnershipConflict, item.ID, ruleRole.raw, existingID, rule.ID)
		}
		ruleRoles[ruleRole.raw] = rule.ID
		observedRules = append(observedRules, observedRouteRule{rule: rule, role: ruleRole.raw, base: ruleRole.base, flagsObserved: flagsObserved})
	}
	return observedRoutePolicy{policy: item, role: role.raw, rules: observedRules, versioned: role.backend != "", redirect: role.redirect, initialRules: initialRules}, nil
}

type routeMutationKind uint8

const (
	routeMutationCreatePool routeMutationKind = iota
	routeMutationUpdatePool
	routeMutationSetBackendRetryBudget
	routeMutationSetXBackendPersistence
	routeMutationClaimBackendCA
	routeMutationPublishBackendCA
	routeMutationClearPoolMembers
	routeMutationReplaceTransportMembers
	routeMutationSetBackendTLS
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
	routeMutationSetPolicyHTTPSListenerHostname
	routeMutationSetPolicyTarget
	routeMutationSetPolicyHeaders
	routeMutationSetPolicyURLRewrite
	routeMutationSetPolicyCORS
	routeMutationSetHTTPRequestPolicy
	routeMutationSetRequestMirrors
	routeMutationMovePolicy
	routeMutationCreateRule
	routeMutationUpdateRule
	routeMutationEnablePolicy
	routeMutationDeleteMonitor
	routeMutationDeletePool
)

type routeMutation struct {
	kind                     routeMutationKind
	id                       string
	parentID                 string
	member                   cloud.Member
	memberTags               *[]string
	batchMembers             []pools.BatchUpdateMemberOpts
	udpDrop                  *cloud.UDPPacketDrop
	policy                   desiredPolicy
	initialRules             []l7policies.CreateRuleOpts
	rule                     desiredRule
	retry                    bool
	clearPoolPersistence     bool
	poolPersistence          *pools.SessionPersistence
	poolHeaderPersistence    *cloud.HTTPSessionHeaderPersistence
	poolCookieDeadline       *cloud.HTTPSessionCookieDeadline
	poolCookiePath           *string
	clearPoolCookiePermanent bool
	persistenceFallback      bool
	persistenceDrain         bool
	backendH2C               *bool
	poolAdmin                *bool
	poolALPNProtocols        *[]string
	poolTags                 *[]string
	backendTLS               *octavia.BackendTLSSettings
	backendRetryBudget       *octavia.BackendRetryBudgetUpdate
	clearTLSClientRef        bool
	reenableH2CPool          bool
	clearMemberBackup        bool
	withdrawHTTPPolicy       bool
	weightedTargets          *[]octavia.WeightedHTTPTarget
	requestHeaderOperations  []cloud.HTTPHeaderOperation
	responseHeaderOperations []cloud.HTTPHeaderOperation
	requestURLRewriteUpdate  *octavia.RequestURLRewriteUpdate
	requestCORSUpdate        *octavia.RequestCORSUpdate
	requestPolicyUpdate      *octavia.HTTPRequestPolicyUpdate
	requestMirrorsUpdate     *octavia.RequestMirrorsUpdate
	externalAuthUpdate       *octavia.PolicyExternalAuthUpdate
	policyTags               []string
	requiresExactHeaders     bool
	requiresExactQueryParams bool
	requiresHTTPMethod       bool
	probeBeforeGRPCMigration bool
}

func (s routeMutation) message() string {
	switch s.kind {
	case routeMutationCreatePool:
		return "Created Octavia pool"
	case routeMutationUpdatePool:
		return "Updated Octavia pool"
	case routeMutationSetBackendRetryBudget:
		return "Updated Octavia Service retry budget"
	case routeMutationSetXBackendPersistence:
		if s.retry {
			return "Recovering Octavia XBackend pool before withdrawing session traffic"
		}
		return "Updated Octavia XBackend session persistence"
	case routeMutationClaimBackendCA:
		return "Checkpointed Gateway ownership of backend CAs"
	case routeMutationPublishBackendCA:
		return "Publishing the backend CA in Barbican"
	case routeMutationClearPoolMembers:
		if s.persistenceDrain {
			return "Withdrawing Octavia XBackend members before changing session persistence"
		}
		return "Withdrawing Octavia backend members until TLS authorization is verified"
	case routeMutationReplaceTransportMembers:
		if s.udpDrop != nil {
			return "Updating Octavia UDP members and packet drop together"
		}
		return "Updating Octavia TCP members and connection rejection together"
	case routeMutationSetBackendTLS:
		return "Updating authenticated TLS on the Octavia pool"
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
	case routeMutationSetPolicyHTTPSListenerHostname:
		return "Updating the HTTPS listener scope on a disabled Octavia HTTP policy"
	case routeMutationSetPolicyHeaders:
		return "Updated Octavia L7 policy header operations"
	case routeMutationSetPolicyURLRewrite:
		return "Updated Octavia L7 policy request URL rewrite"
	case routeMutationSetHTTPRequestPolicy:
		return "Updated Octavia HTTP request timeout and retry policy"
	case routeMutationSetRequestMirrors:
		return "Updated Octavia HTTP request mirrors"
	case routeMutationSetPolicyCORS:
		return "Updated Octavia L7 policy CORS"
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

func (r desiredRoute) h2cTLSConflict(observed observedRouteGraph) bool {
	return !r.fixedResponse && r.spec.BackendH2C && observed.pool != nil && observed.pool.TLSEnabled
}

func (r desiredRoute) backendTransportNeedsWithdrawal(observed observedRouteGraph) bool {
	return !r.fixedResponse && observed.pool != nil &&
		(r.spec.BackendH2C != observed.backendH2C || r.h2cTLSConflict(observed))
}

func buildRouteEnsurePlan(desired desiredRoute, actual observedRouteGraph) ([]routeMutation, cloud.RouteState, error) {
	return buildRouteEnsurePlanWithActivations(desired, actual, false)
}

func buildRouteEnsurePlanWithActivations(desired desiredRoute, actual observedRouteGraph, batchActivations bool) ([]routeMutation, cloud.RouteState, error) {
	if desired.tcpRejectionUnavailable {
		return nil, routeStateFromObservation(actual), errTCPRejectionUnavailable
	}
	if desired.udpDropUnverified {
		return nil, routeStateFromObservation(actual), errUDPPacketDropUnverified
	}
	if desired.udpDropUnavailable || desired.spec.UDPPacketDrop == nil && actual.udpDropPool() {
		return nil, routeStateFromObservation(actual), errUDPPacketDropUnavailable
	}
	step, err := planXBackendPersistence(desired, actual)
	switch {
	case step != nil:
		return []routeMutation{*step}, routeStateFromObservation(actual), nil
	case err == nil && desired.spec.ServiceSessionPersistencePending:
		return nil, routeStateFromObservation(actual), errServiceSessionPersistenceAncestor
	case err == nil:
		return buildRouteEnsureBackendPlan(desired, actual, batchActivations)
	case errors.Is(err, errXBackendCookiePermanentReadback):
		// The sibling ACK gates admission. Existing members still need to
		// leave as their endpoint eligibility changes.
		steps, state, _ := buildRouteEnsureBackendPlan(desired, actual, batchActivations)
		steps = slices.DeleteFunc(steps, func(step routeMutation) bool {
			return step.kind != routeMutationDeleteMember && step.kind != routeMutationDisableMember && step.kind != routeMutationClearPoolMembers
		})
		if len(steps) != 0 {
			return steps, state, nil
		}
		return nil, state, err
	case !desired.persistenceFallback(actual):
		return nil, routeStateFromObservation(actual), err
	case actual.pool == nil:
		fallback := routeMutation{kind: routeMutationCreatePool, persistenceFallback: true}
		if desired.protectedInlinePersistence() {
			fallback.poolTags = new([]string{backendTLSRefusalTag})
		}
		return []routeMutation{fallback}, cloud.RouteState{}, nil
	}
	// Reconcile the exact policy against an independently observed empty
	// pool. Keep its existing transport while waiting for persistence
	// readback to admit members or resume authenticated transport.
	var steps []routeMutation
	var state cloud.RouteState
	var fallbackErr error
	if desired.protectedInlinePersistence() {
		// Keep the exact match on the empty observed pool, without allowing
		// a policy fallback to change its authenticated transport or state.
		state = routeStateFromObservation(actual)
		if actual.pool.AdminStateUp || hasObservedBackendTLS(*actual.pool, actual.backendTLS) {
			steps, state, fallbackErr = buildRouteEnsurePolicyPlan(desired, actual.policies, state.PoolID, state, nil, batchActivations)
		}
	} else if !actual.pool.AdminStateUp && (hasBackendTLSSettings(*actual.pool, actual.backendTLS) || hasObservedBackendTLS(*actual.pool, actual.backendTLS)) {
		state = routeStateFromObservation(actual)
		if desired.authenticatedH2Cookie() && actual.pool.Protocol == cloud.ListenerProtocolHTTP &&
			(slices.Contains(actual.pool.Tags, backendTLSManagedTag) || actual.backendTLS.Hostname != "") {
			// Amphora keeps the HTTP predicate for a closed protected pool.
			steps, state, fallbackErr = buildRouteEnsurePolicyPlan(desired, actual.policies, state.PoolID, state, nil, batchActivations)
		}
	} else {
		desired.members, desired.healthMonitor = nil, cloud.HealthMonitorConfig{}
		desired.sessionCookieName, desired.sessionHeader, desired.backendTLS = "", nil, backendTLSPlan{}
		desired.spec.BackendH2C = actual.backendH2C
		poolOnly := actual
		poolOnly.monitor = nil
		steps, state, fallbackErr = buildRouteEnsureBackendPlan(desired, poolOnly, batchActivations)
		if actual.monitor != nil {
			state.MonitorID = actual.monitor.ID
		}
	}
	if len(steps) != 0 || fallbackErr != nil {
		return steps, state, fallbackErr
	}
	return nil, state, err
}

func buildRouteEnsureBackendPlan(desired desiredRoute, actual observedRouteGraph, batchActivations bool) ([]routeMutation, cloud.RouteState, error) {
	budgetStep, budgetErr := planBackendRetryBudget(desired, actual)
	if step := desired.backendTLS.poolMutation(desired, actual); step != nil {
		state := routeStateFromObservation(actual)
		switch {
		case step.kind == routeMutationCreatePool && budgetErr != nil:
			return nil, state, budgetErr
		case step.poolAdmin != nil && *step.poolAdmin && desired.backendTLS.allowsMembers():
			if budgetErr != nil {
				return nil, state, budgetErr
			}
			if budgetStep != nil {
				return []routeMutation{*budgetStep}, state, nil
			}
		}
		return []routeMutation{*step}, state, nil
	}
	if !desired.backendTLS.allowsMembers() && !desired.backendTLS.denies() && !desired.backendTLS.stagesSystemCA() {
		return nil, routeStateFromObservation(actual), nil
	}
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
		if budgetStep != nil {
			return []routeMutation{*budgetStep}, state, nil
		}
		if budgetErr != nil {
			return nil, state, budgetErr
		}
	case actual.pool == nil:
		if budgetErr != nil {
			return nil, state, budgetErr
		}
		nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationCreatePool})
	default:
		pool := actual.pool
		poolID = pool.ID
		state.PoolID = pool.ID
		if pool.Protocol != desired.protocol() {
			return nil, cloud.RouteState{}, fmt.Errorf("%w: pool %s has protocol %s", cloud.ErrOwnershipConflict, pool.ID, pool.Protocol)
		}
		if desired.backendTLS.allowsMembers() && desired.backendTransportNeedsWithdrawal(actual) {
			if desired.protocol() != cloud.ListenerProtocolHTTP {
				return nil, state, fmt.Errorf("%w: non-HTTP pool %s reports h2c", cloud.ErrOwnershipConflict, pool.ID)
			}
			// Keep the predicate in place and wait for an independent read of
			// the new transport before planning any traffic restoration.
			state = routeStateFromObservation(actual)
			steps, hold := httpMemberWithdrawal(actual)
			if len(steps) != 0 || hold != "" || desired.h2cTLSConflict(actual) {
				return steps, state, nil
			}
			return []routeMutation{{kind: routeMutationUpdatePool, id: pool.ID, backendH2C: boolPointer(desired.spec.BackendH2C)}}, state, nil
		}
		if desired.unavailable {
			state = routeStateFromObservation(actual)
			if steps, hold := httpMemberWithdrawal(actual); len(steps) != 0 || hold != "" {
				return steps, state, nil
			}
		}
		if budgetErr != nil {
			return nil, routeStateFromObservation(actual), budgetErr
		}
		if budgetStep != nil {
			return []routeMutation{*budgetStep}, state, nil
		}
		if desired.backendTLS.denies() || desired.backendTLS.stagesSystemCA() {
			break
		}
		clearPersistence := desired.sessionCookieName == "" && desired.sessionHeader == nil && pool.Persistence != (pools.SessionPersistence{}) &&
			slices.ContainsFunc(desired.members, func(member cloud.Member) bool { return !member.Draining && member.Weight > 0 })
		enableCookiePool := desired.sessionCookieName != "" && !pool.AdminStateUp
		if pool.LBMethod != string(pools.LBMethodRoundRobin) || !pool.AdminStateUp && !enableCookiePool || clearPersistence {
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{
				kind: routeMutationUpdatePool, id: pool.ID, clearPoolPersistence: clearPersistence,
				poolAdmin:       boolPointer(!enableCookiePool),
				reenableH2CPool: actual.backendH2C && !pool.AdminStateUp,
			})
		}

		if tcpRejectionBatchRequired(desired, actual) || desired.spec.UDPPacketDrop != nil {
			batch, memberIDs, err := planTransportBatch(desired, actual)
			if err != nil {
				return nil, state, err
			}
			state.MemberIDs = memberIDs
			if batch != nil {
				nonPolicyPlan = append(nonPolicyPlan, *batch)
			}
		} else {
			desiredMembers := make(map[string]cloud.Member, len(desired.members))
			for _, member := range desired.members {
				desiredMembers[memberKey(member.Address, member.Port)] = member
			}
			weightTag := desired.nodePortWeightTag()
			for _, member := range actual.members {
				// The ordinary Endpoint path can retain disabled members. An
				// external pool must drop records withdrawn by DNS.
				if desired.unavailable && !desired.routedExternal {
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
				weight := desired.memberWeight(wanted)
				var tags *[]string
				if weightTag != "" {
					// Change the marker and the weight together so a restart or a
					// partial activation can only normalize members we took over.
					managed := slices.Contains(member.Tags, weightTag)
					switch {
					case wanted.Weight > 0 && !managed:
						tags = new(append(slices.Clone(member.Tags), weightTag))
					case wanted.Weight == 0 && managed:
						tags = new(slices.DeleteFunc(slices.Clone(member.Tags), func(tag string) bool { return tag == weightTag }))
					}
				}
				// HAProxy excludes backup members while any regular member is up.
				weightedTCP := desired.protocol() == cloud.ListenerProtocolTCP && wanted.Weight > 0 && !wanted.Draining
				if _, unreported := actual.memberBackupUnreported[member.ID]; weightedTCP && unreported {
					return nil, cloud.RouteState{}, errWeightedTCPBackupUnknown
				}
				clearBackup := weightedTCP && member.Backup
				if _, unreported := actual.memberAdminUnreported[member.ID]; unreported || !member.AdminStateUp || retry || tags != nil || weight != nil && member.Weight != *weight || clearBackup {
					nonPolicyPlan = append(nonPolicyPlan, routeMutation{
						kind:              routeMutationUpdateMember,
						id:                member.ID,
						parentID:          pool.ID,
						member:            wanted,
						memberTags:        tags,
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
		}

		switch {
		case desired.healthMonitor == (cloud.HealthMonitorConfig{}):
			if actual.monitor != nil {
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationDeleteMonitor, id: actual.monitor.ID, parentID: pool.ID})
			}
		case actual.monitor == nil:
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationCreateMonitor, parentID: pool.ID})
		case actual.monitor.Type != desired.monitorType():
			// The observer has already proved that this monitor belongs to
			// the pool. Octavia requires replacement to change its type.
			if !desired.spec.GRPC || actual.monitor.Type != monitors.TypeHTTP {
				return nil, cloud.RouteState{}, fmt.Errorf("%w: health monitor %s has type %s", cloud.ErrOwnershipConflict, actual.monitor.ID, actual.monitor.Type)
			}
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationDeleteMonitor, id: actual.monitor.ID, parentID: pool.ID})
		default:
			monitor := actual.monitor
			state.MonitorID = monitor.ID
			httpDrift := monitor.Type == monitors.TypeHTTP && (monitor.URLPath != desired.healthMonitor.Path ||
				monitor.HTTPMethod != http.MethodGet || monitor.ExpectedCodes != routeMonitorExpectedCodes)
			if monitor.Delay != desired.healthMonitor.Delay || monitor.Timeout != desired.healthMonitor.Timeout ||
				monitor.MaxRetries != desired.healthMonitor.MaxRetries || monitor.MaxRetriesDown != desired.healthMonitor.MaxRetriesDown ||
				httpDrift || !monitor.AdminStateUp {
				nonPolicyPlan = append(nonPolicyPlan, routeMutation{kind: routeMutationUpdateMonitor, id: monitor.ID, parentID: pool.ID})
			}
		}
		if enableCookiePool {
			nonPolicyPlan = append(nonPolicyPlan, routeMutation{
				kind: routeMutationUpdatePool, id: pool.ID, poolAdmin: boolPointer(true), reenableH2CPool: actual.backendH2C,
			})
		}
	}

	return buildRouteEnsurePolicyPlan(desired, actual.policies, poolID, state, nonPolicyPlan, batchActivations)
}

func buildRouteEnsurePolicyPlan(desired desiredRoute, policies []observedRoutePolicy, poolID string, state cloud.RouteState, nonPolicyPlan []routeMutation, batchActivations bool) ([]routeMutation, cloud.RouteState, error) {
	policyPlan, disablePlan, policyState := buildRoutePolicyPlan(desired, policies, poolID)
	state.L7PolicyIDs = policyState.L7PolicyIDs
	state.L7RuleIDs = policyState.L7RuleIDs
	plan := append(disablePlan, nonPolicyPlan...)
	plan = append(plan, policyPlan...)
	if len(plan) != 0 {
		return plan, state, nil
	}
	if slices.ContainsFunc(policies, func(policy observedRoutePolicy) bool { return policy.initialRulesPartial }) {
		return nil, state, errInitialL7RulesUnverified
	}
	for _, wantedPolicy := range desired.policies {
		if wantedPolicy.targetsPending() {
			continue
		}
		for _, policy := range policies {
			if policy.role == wantedPolicy.role && !policy.policy.AdminStateUp &&
				(wantedPolicy.requestCORS == nil || requestCORSPolicy.settled(wantedPolicy, policy)) &&
				wantedPolicy.requestPolicyMatches(policy) && wantedPolicy.requestMirrorsMatch(policy) && wantedPolicy.externalAuthMatches(policy) {
				if policy.initialRules && !initialL7RulesReady(policy, rulesFor(wantedPolicy, desired.hostnameRuleValue)) {
					if len(plan) != 0 {
						return plan, state, nil
					}
					return nil, state, errInitialL7RulesUnverified
				}
				if len(plan) != 0 && !policy.initialRules {
					return plan, state, nil
				}
				plan = append(plan, routeMutation{kind: routeMutationEnablePolicy, id: policy.policy.ID,
					policy: desiredPolicy{httpsListenerHostname: wantedPolicy.httpsListenerHostname,
						externalAuth: wantedPolicy.externalAuth, resolvedExternalAuth: wantedPolicy.resolvedExternalAuth,
						resolvedRequestMirrors: wantedPolicy.resolvedRequestMirrors, weightedPlan: wantedPolicy.weightedPlan,
						targetFilters:       wantedPolicy.targetFilters,
						grpcContentTypeRule: wantedPolicy.grpcContentTypeRule, requestURLRewrite: wantedPolicy.requestURLRewrite, requestCORS: wantedPolicy.requestCORS,
						requestPolicy:           wantedPolicy.requestPolicy,
						requestHeaderOperations: wantedPolicy.requestHeaderOperations, responseHeaderOperations: wantedPolicy.responseHeaderOperations},
					requestHeaderOperations:  wantedPolicy.requestHeaderOperations,
					responseHeaderOperations: wantedPolicy.responseHeaderOperations,
					requiresExactHeaders:     len(wantedPolicy.headers) != 0,
					requiresExactQueryParams: wantedPolicy.requiresExactQueryParams(), requiresHTTPMethod: wantedPolicy.method != ""})
				if !batchActivations || !policy.initialRules {
					return plan, state, nil
				}
			}
		}
	}
	if len(plan) == 0 {
		for _, wanted := range desired.policies {
			if len(wanted.resolvedRequestMirrors) != 0 || wanted.weightedPlan != nil && weightedTargetHasMirrors(wanted.weightedPlan.targets) {
				if !slices.ContainsFunc(policies, func(observed observedRoutePolicy) bool {
					return observed.role == wanted.role && observed.policy.AdminStateUp &&
						observedForwardPolicySettled(observed) && wanted.requestMirrorsMatch(observed)
				}) {
					return nil, state, errRequestMirrorsUnverified
				}
			}
			if wanted.requestPolicy == nil || wanted.targetsPending() {
				continue
			}
			if !slices.ContainsFunc(policies, func(observed observedRoutePolicy) bool {
				return observed.role == wanted.role && observed.policy.AdminStateUp &&
					observedForwardPolicySettled(observed) && wanted.requestPolicyMatches(observed)
			}) {
				return nil, state, errHTTPRequestPolicyUnverified
			}
		}
	}
	return plan, state, nil
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
		if policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR" || policy.closureUnverified() {
			disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID,
				retry: policy.policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: desired.backendKey != "" || policy.initialRules})
		}
		if policy.closureUnverified() || policy.initialRulesPartial {
			continue
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
		if exists && !wantedPolicy.externalAuthMatches(observedPolicy) {
			// Stage a changed authorization decision on a disabled policy. A
			// replacement carries the complete target and auth filter in one POST.
			if !observedPolicyDisabled(observedPolicy) {
				disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: observedPolicy.policy.ID,
					retry: true, withdrawHTTPPolicy: true})
				continue
			}
			if !observedPolicy.externalAuthReadable() ||
				wantedPolicy.externalAuth != nil && wantedPolicy.resolvedExternalAuth == nil && observedPolicy.externalAuth == nil {
				continue
			}
			// An ERROR pool can only be replaced after its last policy
			// reference is gone. This owned policy is already closed.
			for _, change := range buildRouteDeletionPlan(observedRouteGraph{policies: []observedRoutePolicy{observedPolicy}}) {
				change.withdrawHTTPPolicy = true
				plan = append(plan, change)
			}
			continue
		}
		if !exists && wantedPolicy.externalAuth != nil && wantedPolicy.resolvedExternalAuth == nil {
			continue
		}
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
			step := routeMutation{kind: routeMutationCreatePolicy, parentID: poolID, policy: wantedPolicy,
				requestHeaderOperations:  wantedPolicy.requestHeaderOperations,
				responseHeaderOperations: wantedPolicy.responseHeaderOperations}
			if wantedPolicy.weightedPlan != nil {
				step.weightedTargets = &wantedPolicy.weightedPlan.targets
			}
			plan = append(plan, step)
			continue
		}
		policy := observedPolicy.policy
		if observedPolicy.requestRedirectPresent && wantedPolicy.redirect != nil && wantedPolicy.redirect.request == nil {
			// Cleanup of the old extension works after regional opt-out. Recreate
			// the stock policy after observing that its former rules are gone.
			for _, step := range buildRouteDeletionPlan(observedRouteGraph{policies: []observedRoutePolicy{observedPolicy}}) {
				step.withdrawHTTPPolicy = true
				if step.kind == routeMutationDisablePolicy {
					disablePlan = append(disablePlan, step)
				} else {
					plan = append(plan, step)
				}
			}
			continue
		}
		if wantedPolicy.requestURLRewrite == nil && observedPolicy.urlRewriteTagged && !observedPolicy.urlRewriteReported ||
			wantedPolicy.requestCORS == nil && observedPolicy.corsTagged && !observedPolicy.corsReported ||
			wantedPolicy.requestPolicy == nil && !desired.fixedResponse && observedPolicy.requestPolicyTagged && !observedPolicy.requestPolicyReported ||
			!wantedPolicy.requestPolicy.HasRetryBackoff() && !desired.fixedResponse && observedPolicy.retryBackoffReadbackMissing() {
			// A stock API may hide the field. Remove the owned policy while
			// closed before rebuilding it without the extension.
			if !observedPolicyDisabled(observedPolicy) {
				disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
					retry: true, withdrawHTTPPolicy: true})
				continue
			}
			for _, step := range buildRouteDeletionPlan(observedRouteGraph{policies: []observedRoutePolicy{observedPolicy}}) {
				step.withdrawHTTPPolicy = true
				plan = append(plan, step)
			}
			continue
		}
		state.L7PolicyIDs = append(state.L7PolicyIDs, policy.ID)
		if observedPolicy.initialRulesPartial {
			continue
		}
		if wantedPolicy.requestCORS != nil && observedPolicy.corsTagged && !observedPolicy.corsReported {
			// The readback cannot distinguish unchanged CORS from a stock API
			// hiding the field. The listener guard holds until it can.
			continue
		}
		if wantedPolicy.requestPolicy.HasRetryBackoff() && !wantedPolicy.requestPolicyMatches(observedPolicy) {
			// A matching rule may have been serving with ordinary retries
			// before backoff was added, or the API may have lost the field.
			// Stage the exact value behind a confirmed administrative closure.
			// Replacing a verified generation must wait for capability discovery.
			if !observedPolicyDisabled(observedPolicy) {
				disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
					retry:              policy.ProvisioningStatus == "ERROR" || !observedPolicy.adminObserved,
					withdrawHTTPPolicy: !observedPolicy.hasConfirmedActiveRetryBackoff()})
				continue
			}
			if observedPolicy.retryBackoffReadbackMissing() {
				// An older response can omit this nested field while still
				// reporting the rest of the policy. Wait for provable readback.
				continue
			}
		}
		if wantedPolicy.requestPolicy != nil && (!observedPolicy.adminObserved ||
			observedPolicy.requestPolicyTagged && !observedPolicy.requestPolicyReported) {
			// A readback omitting a field we wrote cannot prove the policy.
			continue
		}
		if wantedPolicy.requestPolicy == nil && observedPolicy.hasHTTPRequestPolicy() &&
			(!desired.fixedResponse || observedPolicy.fixedResponse && policy.ProvisioningStatus != "ERROR") {
			plan = append(plan, httpRequestPolicyPatch(wantedPolicy, observedPolicy))
			continue
		}
		corsPatch := policyCORSPatch(wantedPolicy.requestCORS, observedPolicy)
		if corsPatch != nil && corsPatch.Value == nil {
			// Clear the old CORS field before changing the forwarding action.
			plan = append(plan, routeMutation{kind: routeMutationSetPolicyCORS, id: policy.ID,
				requestCORSUpdate: corsPatch, withdrawHTTPPolicy: true})
			continue
		}
		if wantedPolicy.httpsListenerHostname != "" && (observedPolicy.httpsListenerHostname != wantedPolicy.httpsListenerHostname ||
			!observedPolicy.initialRules && !observedPolicy.adminObserved) {
			plan = append(plan, routeMutation{kind: routeMutationSetPolicyHTTPSListenerHostname, id: policy.ID, policy: wantedPolicy})
			continue
		}
		policyPlanStart := len(plan)
		weighted := wantedPolicy.weightedPlan
		targetUpdate := weighted != nil && (!slices.EqualFunc(weighted.targets, observedPolicy.weightedTargets, weightedTargetsEqual) ||
			hasWeightedTargetRequestFilters(weighted.targets) != observedPolicy.backendRequestFiltersTagged ||
			hasWeightedTargetResponseHeaders(weighted.targets) != observedPolicy.backendResponseHeadersTagged || policy.ProvisioningStatus == "ERROR") ||
			desired.fixedResponse && (!observedPolicy.fixedResponse || policy.ProvisioningStatus == "ERROR") ||
			!desired.fixedResponse && poolID != "" && (observedPolicy.fixedResponse ||
				observedPolicy.targetObserved && policy.ProvisioningStatus == "ERROR" && policy.RedirectPoolID == poolID)
		requestPatch := policyHeaderPatch(wantedPolicy.requestHeaderOperations, observedPolicy.requestHeaderOperations, policy.ProvisioningStatus == "ERROR")
		responsePatch := policyHeaderPatch(wantedPolicy.responseHeaderOperations, observedPolicy.responseHeaderOperations, policy.ProvisioningStatus == "ERROR")
		headersUpdate := requestPatch != nil || responsePatch != nil
		rewritePatch := policyURLRewritePatch(wantedPolicy.requestURLRewrite, observedPolicy)
		mirrorPatch := !wantedPolicy.requestMirrorsMatch(observedPolicy)
		fixedMirrorPatch := mirrorPatch && len(wantedPolicy.resolvedRequestMirrors) != 0 &&
			(desired.fixedResponse || weighted != nil && allWeightedTargetsFixed(weighted.targets))
		if targetUpdate {
			step := routeMutation{kind: routeMutationSetPolicyTarget, id: policy.ID, parentID: poolID,
				policy:                  desiredPolicy{grpcContentTypeRule: wantedPolicy.grpcContentTypeRule},
				requestHeaderOperations: requestPatch, responseHeaderOperations: responsePatch, requestURLRewriteUpdate: rewritePatch}
			if weighted != nil {
				step.weightedTargets = &weighted.targets
				step.policyTags = weightedTargetFilterTags(weighted.targets, observedPolicy, rewritePatch)
			}
			if fixedMirrorPatch || weighted != nil && weightedTargetHasMirrors(weighted.targets) {
				setRequestMirrorUpdate(&step, wantedPolicy, observedPolicy)
			}
			if desired.fixedResponse && observedPolicy.hasHTTPRequestPolicy() {
				step.clearHTTPRequestPolicy(observedPolicy)
			}
			if len(wantedPolicy.requestHeaderOperations) != 0 {
				step.requestHeaderOperations = wantedPolicy.requestHeaderOperations
			}
			plan = append(plan, step)
		} else if fixedMirrorPatch {
			// Octavia must receive the fixed response's request-only
			// transformations in the same request as the copy that uses them.
			if rewritePatch != nil {
				rewritePatch.KeepPolicyState = true
			}
			step := routeMutation{kind: routeMutationSetRequestMirrors, id: policy.ID,
				policy:                  desiredPolicy{grpcContentTypeRule: wantedPolicy.grpcContentTypeRule},
				requestHeaderOperations: requestPatch, responseHeaderOperations: responsePatch, requestURLRewriteUpdate: rewritePatch}
			setRequestMirrorUpdate(&step, wantedPolicy, observedPolicy)
			plan = append(plan, step)
		} else if rewritePatch != nil {
			step := routeMutation{kind: routeMutationSetPolicyURLRewrite, id: policy.ID, requestURLRewriteUpdate: rewritePatch,
				withdrawHTTPPolicy: rewritePatch.Value == nil}
			if rewritePatch.Value != nil {
				step.requestHeaderOperations, step.responseHeaderOperations = requestPatch, responsePatch
			}
			plan = append(plan, step)
		} else if headersUpdate {
			plan = append(plan, routeMutation{kind: routeMutationSetPolicyHeaders, id: policy.ID,
				policy:                  desiredPolicy{grpcContentTypeRule: wantedPolicy.grpcContentTypeRule},
				requestHeaderOperations: requestPatch, responseHeaderOperations: responsePatch})
		}
		policyActionUpdate := weighted == nil && !desired.fixedResponse && !observedPolicy.fixedResponse && !desiredPolicyActionMatches(wantedPolicy, observedPolicy, poolID)
		if !policyActionUpdate && len(plan) == policyPlanStart && mirrorPatch {
			step := routeMutation{kind: routeMutationSetRequestMirrors, id: policy.ID, retry: policy.ProvisioningStatus == "ERROR"}
			setRequestMirrorUpdate(&step, wantedPolicy, observedPolicy)
			plan = append(plan, step)
		}
		if !policyActionUpdate && corsPatch != nil && len(plan) == policyPlanStart {
			plan = append(plan, routeMutation{kind: routeMutationSetPolicyCORS, id: policy.ID,
				requestCORSUpdate: corsPatch, policy: wantedPolicy})
		}
		start := len(plan)
		if policyActionUpdate {
			plan = append(plan, routeMutation{kind: routeMutationUpdatePolicy, id: policy.ID, parentID: poolID, policy: wantedPolicy})
		}
		policyUpdate := wantedPolicy.requestPolicy != nil && !wantedPolicy.requestPolicyMatches(observedPolicy)
		if policyUpdate && !targetUpdate && !policyActionUpdate && len(plan) == policyPlanStart {
			plan = append(plan, httpRequestPolicyPatch(wantedPolicy, observedPolicy))
			// Octavia updates the field and reloads the selected action atomically.
			// A timeout or retry edit does not need to disable live forwarding.
			start = len(plan)
		}

		rulePlanStart := len(plan)
		wantedRules := rulesFor(wantedPolicy, desired.hostnameRuleValue)
		actualRules := make(map[string]observedRouteRule, len(observedPolicy.rules))
		for _, rule := range observedPolicy.rules {
			actualRules[rule.role] = rule
		}
		missingRules := 0
		grpcContentTypeChanged := false
		for _, wantedRule := range wantedRules {
			observedRule, exists := actualRules[wantedRule.role]
			if !exists {
				grpcContentTypeChanged = grpcContentTypeChanged || wantedRule.ruleType == wantedPolicy.grpcContentTypeRule
				missingRules++
				plan = append(plan, routeMutation{kind: routeMutationCreateRule, parentID: policy.ID, rule: wantedRule})
				continue
			}
			rule := observedRule.rule
			state.L7RuleIDs = append(state.L7RuleIDs, rule.ID)
			replacingNativeMedia := rule.RuleType == string(grpcContentTypeRuleType) && wantedRule.ruleType != grpcContentTypeRuleType
			replacingLiteralPath := rule.RuleType == string(octavia.RuleTypeHTTPPathLiteral) && wantedRule.ruleType != octavia.RuleTypeHTTPPathLiteral
			unexpectedKey := rule.Key != "" && (wantedRule.ruleType == httpMethodRuleType ||
				wantedRule.ruleType == octavia.RuleTypeHTTPPathLiteral || observedPolicy.initialRules && wantedRule.key == "")
			if replacingNativeMedia || replacingLiteralPath || unexpectedKey {
				// Octavia permits deletion after regional withdrawal. A PUT that
				// changes the rule type still requires enrollment.
				grpcContentTypeChanged = grpcContentTypeChanged || replacingNativeMedia
				plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.ID, parentID: policy.ID})
				delete(actualRules, wantedRule.role)
				continue
			}
			if rule.RuleType != string(wantedRule.ruleType) || rule.CompareType != string(wantedRule.compareType) ||
				!wantedRule.matchesKey(rule.Key) || rule.Value != wantedRule.value || rule.Invert != wantedRule.invert || !rule.AdminStateUp || rule.ProvisioningStatus == "ERROR" {
				grpcContentTypeChanged = grpcContentTypeChanged || wantedRule.ruleType == wantedPolicy.grpcContentTypeRule
				plan = append(plan, routeMutation{kind: routeMutationUpdateRule, id: rule.ID, parentID: policy.ID, rule: wantedRule})
			}
			delete(actualRules, wantedRule.role)
		}
		staleRules := make([]observedRouteRule, 0, len(actualRules))
		for _, rule := range actualRules {
			staleRules = append(staleRules, rule)
		}
		sort.Slice(staleRules, func(i, j int) bool { return staleRules[i].rule.ID < staleRules[j].rule.ID })
		staleRulePlanStart := len(plan)
		for _, rule := range staleRules {
			plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.rule.ID, parentID: policy.ID})
		}
		if len(plan) > rulePlanStart && observedPolicy.initialRuleClosureUnproven() {
			disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
				retry: policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: true})
			plan = plan[:policyPlanStart]
			continue
		}
		if len(staleRules) != 0 && len(observedPolicy.rules)+missingRules > maxOctaviaL7RulesPerPolicy {
			// Make room only after Octavia proves the policy is disabled.
			// After each mutation the graph reads back the remaining rules.
			if !observedPolicyDisabled(observedPolicy) {
				disablePlan = append(disablePlan, routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
					retry: policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: true})
				plan = plan[:policyPlanStart]
				continue
			}
			plan = slices.Concat(plan[:rulePlanStart], plan[staleRulePlanStart:], plan[rulePlanStart:staleRulePlanStart])
		}
		// A failed policy update can leave Octavia's desired admin state in
		// the database even while Amphora still uses an earlier configuration.
		if grpcContentTypeChanged && !observedPolicyDisabled(observedPolicy) ||
			policy.ProvisioningStatus == "ERROR" && !targetUpdate && !headersUpdate && rewritePatch == nil && corsPatch == nil && !policyUpdate || len(plan) > start && policy.AdminStateUp {
			withdraw := desired.redirect || grpcContentTypeChanged
			disable := routeMutation{kind: routeMutationDisablePolicy, id: policy.ID,
				retry: withdraw && policy.ProvisioningStatus == "ERROR", withdrawHTTPPolicy: withdraw}
			if grpcContentTypeChanged && grpcBinaryRequestMetadataPolicy.required(wantedPolicy) {
				// This disable stages a replacement, so it requires the
				// listener and capability guard for new binary metadata.
				disable.withdrawHTTPPolicy = false
			}
			// A verified portable policy can keep serving while discovery
			// recovers. Predicate, target and scope drift still need withdrawal.
			if disable.withdrawHTTPPolicy && grpcContentTypeChanged && wantedPolicy.grpcContentTypeRule == grpcContentTypeRuleType &&
				!targetUpdate && !headersUpdate && !policyActionUpdate &&
				observedPolicy.httpsListenerHostname == wantedPolicy.httpsListenerHostname && policy.Position == wantedPolicy.position &&
				policy.AdminStateUp && observedForwardPolicySettled(observedPolicy) {
				portable := wantedPolicy
				portable.grpcContentTypeRule = l7policies.TypeHeader
				disable.probeBeforeGRPCMigration = observedHTTPPredicateMatches(observedPolicy, rulesFor(portable, desired.hostnameRuleValue))
			}
			disablePlan = append(disablePlan, disable)
		}
		// Moving a policy changes precedence only. Keep its current admin state
		// and predicates and let Octavia shift the other positions atomically.
		if policy.Position != wantedPolicy.position {
			plan = append(plan, routeMutation{kind: routeMutationMovePolicy, id: policy.ID, policy: wantedPolicy})
		}
		targets := wantedPolicy.weightedFilterTargets()
		if hasWeightedTargetRequestFilters(targets) || hasWeightedTargetResponseHeaders(targets) {
			for i := policyPlanStart; i < len(plan); i++ {
				plan[i].policy.targetFilters = targets
			}
		}
		if wantedPolicy.requestCORS != nil {
			for i := policyPlanStart; i < len(plan); i++ {
				plan[i].policy.requestCORS = wantedPolicy.requestCORS
			}
		}
		if wantedPolicy.requestURLRewrite != nil {
			for i := policyPlanStart; i < len(plan); i++ {
				plan[i].policy.requestURLRewrite = wantedPolicy.requestURLRewrite
				plan[i].policy.requestHeaderOperations = wantedPolicy.requestHeaderOperations
				plan[i].policy.responseHeaderOperations = wantedPolicy.responseHeaderOperations
			}
		}
	}
	return plan, disablePlan, state
}

func (policy observedRoutePolicy) closureUnverified() bool {
	return (policy.initialRules || policy.hasRequestURLRewrite() || policy.hasBackendRequestFilters() || policy.hasBackendResponseHeaders() || policy.hasRequestCORS() || policy.hasHTTPRequestPolicy() || policy.hasRequestMirrors()) && !observedPolicyDisabled(policy)
}

func buildRouteDeletionPlan(actual observedRouteGraph) []routeMutation {
	if actual.udpDropPool() {
		return []routeMutation{{kind: routeMutationDeletePool, id: actual.pool.ID}}
	}
	plan := make([]routeMutation, 0)
	for _, policy := range actual.policies {
		if !policy.pending() && (policy.policy.AdminStateUp || policy.policy.ProvisioningStatus == "ERROR" || policy.closureUnverified()) {
			plan = append(plan, routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID,
				retry: policy.policy.ProvisioningStatus == "ERROR" || (policy.hasRequestURLRewrite() || policy.hasBackendRequestFilters() || policy.hasBackendResponseHeaders() || policy.hasRequestCORS() || policy.hasHTTPRequestPolicy()) && !policy.adminObserved})
		}
	}
	safeToRemove := func(policy observedRoutePolicy) bool {
		return !policy.closureUnverified() && !policy.initialRulesPartial
	}
	for _, policy := range actual.policies {
		if safeToRemove(policy) {
			for _, rule := range policy.rules {
				plan = append(plan, routeMutation{kind: routeMutationDeleteRule, id: rule.rule.ID, parentID: policy.policy.ID})
			}
		}
	}
	for _, policy := range actual.policies {
		if safeToRemove(policy) {
			plan = append(plan, routeMutation{kind: routeMutationDeletePolicy, id: policy.policy.ID})
		}
	}
	if slices.ContainsFunc(actual.policies, func(policy observedRoutePolicy) bool { return !safeToRemove(policy) }) {
		return plan
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

// policyHeaderPatch omits already-converged directions and explicitly clears
// installed operations when the desired rule no longer has a filter.
func policyHeaderPatch(wanted, observed []cloud.HTTPHeaderOperation, retry bool) []cloud.HTTPHeaderOperation {
	if wanted == nil && observed == nil || !retry && slices.Equal(wanted, observed) {
		return nil
	}
	if wanted == nil {
		return []cloud.HTTPHeaderOperation{}
	}
	return wanted
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
		if desired.spec.UDPPacketDrop != nil {
			tags = append(tags, udpDropPoolTag)
		}
		opts := pools.CreateOpts{
			LBMethod:       pools.LBMethodRoundRobin,
			Protocol:       pools.Protocol(desired.protocol()),
			LoadbalancerID: desired.spec.Gateway.LoadBalancerID,
			Name:           resourceName(desired.spec.Identity, desired.httpRole(rolePool, "")),
			Description:    desired.identity.Description(desired.httpRole(rolePool, "")),
			AdminStateUp:   boolPointer(true),
			Tags:           tags,
		}
		if step.poolTags != nil {
			opts.Tags = append(opts.Tags, (*step.poolTags)...)
		}
		var cookieDeadline *cloud.HTTPSessionCookieDeadline
		var cookiePath *string
		if desired.sessionCookieName != "" && !step.persistenceFallback {
			opts.AdminStateUp = boolPointer(false)
			opts.Persistence = &pools.SessionPersistence{Type: httpCookiePersistence, CookieName: desired.sessionCookieName}
			opts.Tags = append(opts.Tags, xBackendCookieTag)
			if cookiePath = desired.sessionCookiePath; cookiePath != nil {
				opts.Tags = append(opts.Tags, xBackendCookiePathTag)
			}
			if cookieDeadline = desired.sessionCookieDeadline; cookieDeadline != nil {
				opts.Tags = append(opts.Tags, xBackendCookieDeadlineTag)
				if cookieDeadline.Permanent {
					opts.Tags = append(opts.Tags, xBackendCookiePermanentTag)
				}
			}
		}
		var budget *cloud.BackendRetryBudget
		if desired.backendRetryBudgetAvailable {
			budget = desired.spec.BackendRetryBudget
		}
		var hostname string
		var subjectAltNames []cloud.BackendTLSSubjectAltName
		requireH2 := step.backendTLS != nil && step.backendTLS.RequireH2 != nil && *step.backendTLS.RequireH2
		systemCA := step.backendTLS != nil && step.backendTLS.SystemCA != nil && *step.backendTLS.SystemCA
		if step.poolTags != nil && (desired.xBackend || desired.spec.GRPC && !desired.protectedInlinePersistence() ||
			systemCA || step.backendTLS != nil && (desired.protectedInlinePersistence() ||
			step.backendTLS.ClientCertificateReference != "" || len(step.backendTLS.SubjectAltNames) != 0)) {
			// Authenticated XBackends and extended Service TLS start closed.
			// Enabling traffic requires independent, ownership-checked readback.
			opts.AdminStateUp = boolPointer(false)
		}
		if step.backendTLS != nil {
			opts.TLSEnabled, opts.CATLSContainerRef = true, step.backendTLS.CAReference
			if desired.xBackend {
				opts.ALPNProtocols = []string{desired.xBackendALPN()}
			}
			opts.TLSContainerRef = step.backendTLS.ClientCertificateReference
			hostname = step.backendTLS.Hostname
			subjectAltNames = step.backendTLS.SubjectAltNames
		}
		_, err = p.octavia.CreatePool(ctx, octavia.CreatePoolOpts{CreateOpts: opts,
			CookieDeadline:     cookieDeadline,
			CookiePath:         cookiePath,
			BackendRetryBudget: budget,
			BackendH2C:         !step.persistenceFallback && desired.spec.BackendH2C && desired.backendTLS.allowsMembers(),
			BackendTLSHostname: hostname, BackendTLSSubjectAltNames: subjectAltNames, BackendTLSRequireH2: requireH2, BackendTLSSystemCA: systemCA})
	case routeMutationUpdatePool:
		opts := pools.UpdateOpts{LBMethod: pools.LBMethodRoundRobin}
		if step.backendH2C == nil {
			opts.AdminStateUp = boolPointer(true)
			if step.poolAdmin != nil {
				opts.AdminStateUp = step.poolAdmin
			}
		}
		if step.clearPoolPersistence {
			// Gophercloud encodes a non-nil empty persistence as a JSON null.
			opts.Persistence = &pools.SessionPersistence{}
		}
		_, err = p.octavia.UpdatePool(ctx, step.id, octavia.UpdatePoolOpts{UpdateOpts: opts, BackendH2C: step.backendH2C})
	case routeMutationSetBackendRetryBudget:
		_, err = p.octavia.UpdatePool(ctx, step.id, octavia.UpdatePoolOpts{BackendRetryBudget: step.backendRetryBudget})
	case routeMutationSetXBackendPersistence:
		opts := pools.UpdateOpts{Persistence: step.poolPersistence, Tags: step.poolTags, AdminStateUp: step.poolAdmin}
		_, err = p.octavia.UpdatePool(ctx, step.id, octavia.UpdatePoolOpts{UpdateOpts: opts,
			CookieDeadline: step.poolCookieDeadline, CookiePath: step.poolCookiePath, ClearCookiePermanent: step.clearPoolCookiePermanent,
			HeaderPersistence: step.poolHeaderPersistence})
	case routeMutationPublishBackendCA:
		return p.publishBackendCA(ctx, desired.spec)
	case routeMutationClearPoolMembers:
		err = p.octavia.ClearMembers(ctx, step.id)
	case routeMutationReplaceTransportMembers:
		if step.udpDrop != nil {
			err = p.octavia.ReplaceUDPMembersAndDrop(ctx, step.id, step.batchMembers, *step.udpDrop)
		} else {
			err = p.octavia.ReplaceTCPMembersAndRejection(ctx, step.id, step.batchMembers, desired.spec.TCPConnectionRejection)
		}
	case routeMutationSetBackendTLS:
		opts := pools.UpdateOpts{AdminStateUp: step.poolAdmin, Tags: step.poolTags, ALPNProtocols: step.poolALPNProtocols}
		_, err = p.octavia.UpdatePool(ctx, step.id, octavia.UpdatePoolOpts{UpdateOpts: opts, BackendH2C: step.backendH2C, BackendTLS: step.backendTLS,
			ClearTLSClientRef: step.clearTLSClientRef})
	case routeMutationDisableMember:
		_, err = p.octavia.UpdateMember(ctx, step.parentID, step.id, pools.UpdateMemberOpts{AdminStateUp: boolPointer(false)})
	case routeMutationDeleteMember:
		err = p.octavia.DeleteMember(ctx, step.parentID, step.id)
	case routeMutationCreateMember:
		tags, tagErr := desired.identity.RouteTags(desired.httpRole(roleMember, ""))
		if tagErr != nil {
			return tagErr
		}
		if weightTag := desired.nodePortWeightTag(); weightTag != "" && step.member.Weight > 0 {
			tags = append(tags, weightTag)
		}
		_, err = p.octavia.CreateMember(ctx, step.parentID, pools.CreateMemberOpts{
			Address:      step.member.Address,
			ProtocolPort: step.member.Port,
			Name:         resourceName(desired.spec.Identity, desired.httpRole(roleMember, "")),
			SubnetID:     desired.spec.MemberSubnetID,
			AdminStateUp: boolPointer(true),
			Tags:         tags,
			Weight:       desired.memberWeight(step.member),
		})
	case routeMutationUpdateMember:
		opts := pools.UpdateMemberOpts{
			AdminStateUp: boolPointer(true),
			Weight:       desired.memberWeight(step.member),
		}
		if step.memberTags != nil {
			opts.Tags = *step.memberTags
			if opts.Weight == nil {
				// Removing weight ownership restores Octavia's default.
				opts.Weight = new(1)
			}
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
		if step.policy.requestCORS != nil {
			tags = append(tags, openstackidentity.RequestCORSPolicyTag)
		}
		if step.policy.requestURLRewrite != nil {
			tags = append(tags, openstackidentity.RequestURLRewritePolicyTag)
		}
		if step.weightedTargets != nil && hasWeightedTargetRequestFilters(*step.weightedTargets) {
			tags = append(tags, openstackidentity.BackendRequestFiltersPolicyTag)
		}
		if step.weightedTargets != nil && hasWeightedTargetResponseHeaders(*step.weightedTargets) {
			tags = append(tags, openstackidentity.BackendResponseHeadersPolicyTag)
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
		create := octavia.PolicyCreateOpts{CreateOpts: opts, FixedServerError: desired.fixedResponse,
			RequestHeaderOperations: step.requestHeaderOperations, ResponseHeaderOperations: step.responseHeaderOperations,
			HTTPSListenerHostname: step.policy.httpsListenerHostname, RequestURLRewrite: step.policy.requestURLRewrite, InitialRules: step.initialRules, RequestCORS: step.policy.requestCORS}
		if len(step.initialRules) != 0 {
			create.Tags = append(create.Tags, initialL7RulesTag)
		}
		if redirect := step.policy.redirect; redirect != nil {
			create.Action, create.RedirectPrefix, create.RedirectHttpCode = l7policies.ActionRedirectPrefix, redirect.prefix, redirect.statusCode
			create.RedirectRequest = redirect.request
		}
		if step.weightedTargets != nil {
			create.WeightedTargets = *step.weightedTargets
		}
		create.RequestMirrors = step.policy.resolvedRequestMirrors
		if step.policy.externalAuth != nil {
			if step.policy.resolvedExternalAuth == nil {
				return errors.New("waiting for the HTTPRoute authorization pool")
			}
			create.ExternalAuth = step.policy.resolvedExternalAuth
			create.Tags = append(create.Tags, routeExternalAuthTag)
		}
		if len(create.RequestMirrors) != 0 || step.weightedTargets != nil && weightedTargetHasMirrors(*step.weightedTargets) {
			if create.RequestMirrors == nil {
				create.RequestMirrors = []octavia.HTTPRequestMirror{}
			}
			create.Tags = mirrorPolicyTags(create.Tags, true)
		}
		if step.policy.requestPolicy != nil {
			create.HTTPRequestPolicy = step.policy.requestPolicy
			create.Tags = httpRequestPolicyTags(create.Tags, create.HTTPRequestPolicy)
		}
		created, createErr := p.octavia.CreatePolicy(ctx, create)
		err = createErr
		if err == nil && len(step.initialRules) != 0 &&
			(created == nil || created.ID == "" || !validInitialRuleIDs(created.Rules, len(step.initialRules))) {
			err = errors.New("create response from Octavia is missing policy or initial L7 rule IDs")
		}
	case routeMutationUpdatePolicy:
		opts := l7policies.UpdateOpts{AdminStateUp: boolPointer(false)}
		if redirect := step.policy.redirect; redirect != nil {
			if redirect.request != nil {
				_, err = p.octavia.SetPolicyRequestRedirect(ctx, step.id, *redirect.request, redirect.statusCode)
				break
			}
			opts.Action, opts.RedirectPrefix, opts.RedirectHttpCode = l7policies.ActionRedirectPrefix, &redirect.prefix, redirect.statusCode
		} else {
			opts.Action, opts.RedirectPoolID = l7policies.ActionRedirectToPool, &step.parentID
		}
		_, err = p.octavia.UpdatePolicy(ctx, step.id, opts)
	case routeMutationSetPolicyHTTPSListenerHostname:
		_, err = p.octavia.SetPolicyHTTPSListenerHostname(ctx, step.id, step.policy.httpsListenerHostname)
	case routeMutationSetHTTPRequestPolicy:
		_, err = p.octavia.UpdatePolicyFilters(ctx, step.id, l7policies.UpdateOpts{},
			octavia.PolicyFilters{RequestPolicy: step.requestPolicyUpdate, Tags: step.policyTags})
	case routeMutationSetRequestMirrors:
		filters := octavia.PolicyFilters{RequestMirrors: step.requestMirrorsUpdate, Tags: step.policyTags,
			Request: step.requestHeaderOperations, Response: step.responseHeaderOperations, URLRewrite: step.requestURLRewriteUpdate}
		if step.weightedTargets != nil {
			_, err = p.octavia.UpdatePolicyWeightedSlots(ctx, step.id, *step.weightedTargets, filters)
		} else {
			_, err = p.octavia.UpdatePolicyFilters(ctx, step.id, l7policies.UpdateOpts{}, filters)
		}
	case routeMutationSetPolicyHeaders, routeMutationSetPolicyURLRewrite, routeMutationSetPolicyCORS:
		_, err = p.octavia.UpdatePolicyFilters(ctx, step.id, l7policies.UpdateOpts{},
			octavia.PolicyFilters{Request: step.requestHeaderOperations, Response: step.responseHeaderOperations, URLRewrite: step.requestURLRewriteUpdate, CORS: step.requestCORSUpdate})
	case routeMutationSetPolicyTarget:
		filters := octavia.PolicyFilters{Request: step.requestHeaderOperations, Response: step.responseHeaderOperations,
			URLRewrite: step.requestURLRewriteUpdate, CORS: step.requestCORSUpdate, RequestPolicy: step.requestPolicyUpdate,
			RequestMirrors: step.requestMirrorsUpdate, ExternalAuth: step.externalAuthUpdate, Tags: step.policyTags}
		if step.weightedTargets != nil {
			_, err = p.octavia.SetPolicyWeightedTargets(ctx, step.id, *step.weightedTargets, filters)
		} else if desired.fixedResponse {
			_, err = p.octavia.SetPolicyFixedResponse(ctx, step.id, filters)
		} else {
			_, err = p.octavia.UpdatePolicyFilters(ctx, step.id, l7policies.UpdateOpts{RedirectPoolID: &step.parentID}, filters)
		}
	case routeMutationMovePolicy:
		_, err = p.octavia.UpdatePolicy(ctx, step.id, l7policies.UpdateOpts{Position: step.policy.position})
	case routeMutationCreateRule:
		opts, tagErr := desired.createRuleOpts(step.rule)
		if tagErr != nil {
			return tagErr
		}
		_, err = p.octavia.CreateRule(ctx, step.parentID, opts)
	case routeMutationUpdateRule:
		// Octavia clears the inherited header key on a native gRPC update.
		var key *string
		if step.rule.key != "" {
			key = &step.rule.key
		}
		_, err = p.octavia.UpdateRule(ctx, step.parentID, step.id, l7policies.UpdateRuleOpts{
			RuleType:     step.rule.ruleType,
			CompareType:  step.rule.compareType,
			Value:        step.rule.value,
			Key:          key,
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
		if step.backendTLS != nil || step.kind == routeMutationSetBackendTLS || step.kind == routeMutationClearPoolMembers {
			return redactBackendTLSMutationError(step, err)
		}
		return fmt.Errorf("%s: %w", step.message(), classifyOctaviaMutationError(err))
	}
	return nil
}

func (r desiredRoute) nodePortWeightTag() string {
	if r.spec.MemberMode == cloud.MemberModeNodePort {
		switch r.protocol() {
		case cloud.ListenerProtocolTCP:
			return "gateway-api-openstack.tcp-nodeport-weight=v1"
		case cloud.ListenerProtocolUDP:
			return "gateway-api-openstack.udp-nodeport-weight=v1"
		}
	}
	return ""
}

// Endpoint mode always owns weight for readiness. A singleton TCP or UDP NodePort
// leaves weights alone unless the observed member carries a managed share.
func (r desiredRoute) memberWeight(member cloud.Member) *int {
	managesWeight := r.spec.MemberMode == cloud.MemberModeEndpoint || r.nodePortWeightTag() != "" && member.Weight > 0
	if !managesWeight {
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
