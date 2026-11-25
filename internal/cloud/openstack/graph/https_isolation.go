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
	"slices"
	"strings"
	"sync"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

var (
	httpsIsolationTag       = openstackidentity.Tag("https-listener-isolation", "v1")
	httpsIsolationTagPrefix = openstackidentity.Tag("https-listener-isolation", "")
)

func httpsEnrollmentTag(tag string) bool {
	return strings.HasPrefix(tag, httpsIsolationTagPrefix) || tag == strings.TrimSuffix(httpsIsolationTagPrefix, "=")
}

func hasHTTPSIsolationEnrollment(listener listeners.Listener) bool {
	var count int
	for _, tag := range listener.Tags {
		if !httpsEnrollmentTag(tag) {
			continue
		}
		if tag != httpsIsolationTag {
			return false
		}
		count++
	}
	return count == 1
}

func httpsVirtualHosts(spec cloud.ScopedListener) []string {
	if spec.TLS == nil {
		return nil
	}
	return spec.TLS.VirtualHosts
}

func (actual observedGatewayGraph) isHTTPSIsolated(listener listeners.Listener) bool {
	return slices.ContainsFunc(listener.Tags, httpsEnrollmentTag) || actual.httpsVirtualHosts[listener.ID] != nil
}

func (actual observedGatewayGraph) httpsBindingMatches(listener listeners.Listener, spec cloud.ScopedListener) bool {
	wanted := httpsVirtualHosts(spec)
	if len(wanted) == 0 {
		return !actual.isHTTPSIsolated(listener)
	}
	return actual.httpsIsolationAvailable && hasHTTPSIsolationEnrollment(listener) &&
		slices.Equal(actual.httpsVirtualHosts[listener.ID], wanted)
}

type httpsPolicyInventory struct {
	items    []octavia.L7PolicyObservation
	complete bool
	err      error
}

func (p *Provider) readHTTPSPolicyInventory(ctx context.Context, listener listeners.Listener) httpsPolicyInventory {
	items, complete, err := p.octavia.ListPolicyObservations(ctx, l7policies.ListOpts{ListenerID: listener.ID, ProjectID: p.projectID})
	if err != nil || !complete {
		return httpsPolicyInventory{err: err}
	}
	refs := make([]string, 0, len(listener.L7Policies))
	if listener.L7Policies == nil {
		refs = nil
	}
	for _, ref := range listener.L7Policies {
		refs = append(refs, ref.ID)
	}
	observed := make([]string, 0, len(items))
	for _, item := range items {
		observed = append(observed, item.Policy.ID)
	}
	err = crossCheckCompleteChildInventory("listener", listener.ID, "L7 policy", refs, observed, func(id string) string { return id })
	return httpsPolicyInventory{items: items, complete: err == nil, err: err}
}

type httpsPolicyBinding struct {
	owner Identity
	scope string
}

// Inspect enrollment and policy selectors before backend or address reads. An
// unreadable selector can be the only barrier between two logical listeners.
func (p *Provider) observeHTTPSIsolation(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, defaultPoolsObserved bool) {
	if !slices.ContainsFunc(desired.spec.Gateway.Spec.ScopedListeners, func(spec cloud.ScopedListener) bool {
		return len(httpsVirtualHosts(spec)) != 0
	}) && !slices.ContainsFunc(actual.scopedListeners, actual.isHTTPSIsolated) {
		return
	}
	actual.httpsIsolationAvailable = p.httpsListenerIsolation && p.verifyGatewayHTTPCapabilities(ctx, desired, *actual,
		[]string{octavia.GatewayCapabilityHTTPSListenerIsolation}) == nil
	node := desired.spec.Gateway
	if len(node.TLSWithdrawalPorts) != 0 && len(node.TLSWithdrawalPreviousHosts) == 0 {
		return
	}
	policySource := desired
	authority := desired.spec.HTTPSPolicyAuthority
	verifiedCleanup, verifiedWithdrawal := false, false
	if (node.Disposition == cloud.GraphPreserve || node.Disposition == cloud.GraphWithdrawTLS) &&
		authority != nil && authority.Identity == desired.spec.Identity && authority.Gateway.Disposition == cloud.GraphEnsure {
		if observed, err := p.buildDesiredGatewayGraph(*authority); err == nil {
			policySource = observed
			verifiedCleanup = node.Disposition == cloud.GraphPreserve
			verifiedWithdrawal = node.Disposition == cloud.GraphWithdrawTLS && len(node.TLSWithdrawalPorts) == 0
		}
	}
	if len(node.TLSWithdrawalPorts) != 0 && policySource.spec.Gateway.Disposition != cloud.GraphEnsure {
		return
	}
	actual.httpsHostTransition = make(map[string]bool)
	actual.httpsIsolationPolicies = make(map[string]httpsPolicyInventory)
	actual.httpsIsolationUnverified = make(map[string]bool)
	configured := scopedHTTPListeners(desired)
	if verifiedCleanup {
		configured = scopedHTTPListeners(policySource)
		p.observeFrontendCAs(ctx, policySource, actual)
	}
	if node.Disposition == cloud.GraphWithdrawTLS && policySource.spec.Gateway.Disposition == cloud.GraphEnsure {
		proofPorts := scopedHTTPListeners(policySource)
		for port, spec := range configured {
			if !spec.TLS.Equal(proofPorts[port].TLS) {
				return
			}
		}
	}
	wanted := make(map[int]map[string][]httpsPolicyBinding)
	var detaching []Identity
	for index, route := range policySource.routes {
		switch policySource.spec.Routes[index].Disposition {
		case cloud.GraphDelete:
			detaching = append(detaching, route.identity)
			continue
		case cloud.GraphEnsure:
		default:
			continue
		}
		add := func(part desiredRoute) {
			port := scopedHTTPRoutePort(part)
			if len(httpsVirtualHosts(configured[port])) == 0 {
				return
			}
			if wanted[port] == nil {
				wanted[port] = make(map[string][]httpsPolicyBinding)
			}
			for _, policy := range part.policies {
				wanted[port][policy.role] = append(wanted[port][policy.role], httpsPolicyBinding{route.identity, policy.httpsListenerHostname})
			}
		}
		add(route)
		for _, part := range route.httpParts {
			add(part)
		}
	}
	proveRoutes := sync.OnceValue(func() map[string]httpsLivePolicy {
		if err := p.observeListenerFilterPools(ctx, desired, actual); err != nil {
			return nil
		}
		routes, err := p.observeRouteGraphsFromPools(ctx, policySource.routeOwners(), actual.httpPools.loadBalancer, nil,
			cloud.ListenerProtocolHTTP, actual.scopedListeners, actual.httpPools.routes, nil, actual.httpsIsolationPolicies)
		if err != nil {
			return nil
		}
		if node.Disposition == cloud.GraphEnsure {
			actual.routes = routes
		}
		proof := *actual
		proof.routes = routes
		source := policySource
		source.spec.Routes = slices.Clone(source.spec.Routes)
		source.routes = slices.Clone(source.routes)
		for index := range source.routes {
			source.routes[index].httpParts = slices.Clone(source.routes[index].httpParts)
		}
		if p.planBackendTLS(ctx, &source, proof) != nil {
			return nil
		}
		return liveHTTPSPolicies(source, proof)
	})
	for _, listener := range actual.scopedListeners {
		port := listener.ProtocolPort
		prior := node.TLSWithdrawalPreviousHosts[port]
		if len(node.TLSWithdrawalPorts) != 0 && prior == nil {
			continue
		}
		spec := configured[port]
		isolated := actual.isHTTPSIsolated(listener)
		hosts := httpsVirtualHosts(spec)
		if !isolated && len(hosts) == 0 {
			continue
		}
		// Only explicit scoped authorization can reopen a listener. When
		// its model is preserved, still require the native selector proof.
		bindingsMatch := actual.httpsBindingMatches(listener, spec)
		if verifiedCleanup || verifiedWithdrawal {
			bindingsMatch = bindingsMatch && actual.httpsTLSMaterialMatches(listener, spec) &&
				actual.httpFallbackReady(listener, desired.fixedHTTPResponses)
		} else if node.Disposition == cloud.GraphPreserve {
			bindingsMatch = actual.httpsIsolationAvailable && hasHTTPSIsolationEnrollment(listener) &&
				len(actual.httpsVirtualHosts[listener.ID]) != 0
		}
		enrolled := hasHTTPSIsolationEnrollment(listener) && len(actual.httpsVirtualHosts[listener.ID]) != 0
		changeHosts := len(hosts) != 0 && hasAuthorizedListener(spec) && (!isolated || enrolled)
		removeFrontend := spec.TLS == nil && enrolled
		retire := desired.spec.Gateway.Disposition == cloud.GraphEnsure && actual.httpsIsolationAvailable &&
			!bindingsMatch && (changeHosts || removeFrontend)
		current := actual.httpsVirtualHosts[listener.ID]
		if verifiedCleanup && actual.httpsTLSMaterialMatches(listener, spec) && httpsHostSubset(hosts, current) {
			// This cleanup owns route resources. The Gateway writer owns
			// contraction, including withdrawal of another route's policy.
			actual.httpsCleanupDeferred = true
			actual.outcome = p.progressingOutcome("Waiting for scoped HTTPS withdrawal before route cleanup")
			return
		}
		if !bindingsMatch {
			actual.httpsIsolationUnverified[listener.ID] = true
		}
		parentActive := actual.loadBalancer.ProvisioningStatus == "ACTIVE"
		active := listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE"
		liveChange := prior != nil || !bindingsMatch && (retire || verifiedCleanup || verifiedWithdrawal)
		closed := actual.adminObserved[listener.ID] && !listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE"
		expanding := retire && httpsHostSubset(current, hosts)
		readDuringUpdate := node.Disposition == cloud.GraphEnsure && (bindingsMatch || expanding) && active &&
			independentHTTPSHosts(hosts) && actual.loadBalancer.ProvisioningStatus == "PENDING_UPDATE"
		inspectPolicies := bindingsMatch || liveChange || (retire && closed)
		if !parentActive && !readDuringUpdate || !inspectPolicies {
			continue
		}
		inventory := p.readHTTPSPolicyInventory(ctx, listener)
		actual.httpsIsolationPolicies[listener.ID] = inventory
		if !inventory.complete {
			actual.httpsIsolationUnverified[listener.ID] = true
			continue
		}
		hasDetaching := bindingsMatch && len(detaching) != 0 && slices.ContainsFunc(inventory.items, func(item octavia.L7PolicyObservation) bool {
			role, ok := readRouteResourceRole(item.Policy.Tags, rolePolicyExact, rolePolicyPrefix)
			return ok && slices.ContainsFunc(detaching, func(owner Identity) bool { return owner.MatchesRoute(item.Policy.Tags, role.raw) })
		})
		if defaultPoolsObserved && active && (parentActive || expanding && readDuringUpdate) &&
			(liveChange || hasDetaching || verifiedWithdrawal) {
			step, serving, safe := planLiveHTTPSHostTransition(desired, *actual, listener, spec, inventory,
				proveRoutes, prior, hasDetaching, verifiedWithdrawal)
			// Cleanup of a withdrawn backend may keep Octavia busy when its
			// grant returns. Prove the active subset against the expanded
			// authority, provided expansion requires no policy retirement.
			if safe && (parentActive || step != nil && step.kind == graphUpdateListenerVirtualHosts) {
				if serving {
					actual.httpsServingPorts = append(actual.httpsServingPorts, port)
				}
				if parentActive {
					actual.httpsHostTransition[listener.ID] = true
					if actual.httpsIsolationMutation == nil {
						actual.httpsIsolationMutation = step
					}
				}
				continue
			}
			actual.httpsIsolationUnverified[listener.ID] = true
		}
		if !bindingsMatch && (!actual.adminObserved[listener.ID] || listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE") {
			continue
		}
		if retire {
			if defaultPoolsObserved {
				p.planHTTPSPolicyRetirement(ctx, desired, actual, listener, spec, inventory.items)
			}
			continue
		}
		for _, item := range inventory.items {
			hostname := item.HTTPSListenerHostname
			if item.Policy.ProjectID != p.projectID || item.Policy.ListenerID != listener.ID ||
				item.AdminState == nil || !slices.Contains(actual.httpsVirtualHosts[listener.ID], hostname) {
				actual.httpsIsolationUnverified[listener.ID] = true
				break
			}
			// A withdrawal-only snapshot has no route owners. A valid section
			// name alone cannot show that the policy belongs to that section.
			role, ok := readRouteResourceRole(item.Policy.Tags, rolePolicyExact, rolePolicyPrefix)
			if !ok || !httpsPolicyBindingMatches(wanted[port][role.raw], item, role.raw) {
				actual.httpsIsolationUnverified[listener.ID] = true
				break
			}
		}
	}
}

func httpsPolicyBindingMatches(bindings []httpsPolicyBinding, item octavia.L7PolicyObservation, role string) bool {
	return slices.ContainsFunc(bindings, func(binding httpsPolicyBinding) bool {
		return binding.scope == item.HTTPSListenerHostname && binding.owner.MatchesRoute(item.Policy.Tags, role)
	})
}

func httpsHostSubset(subset, hosts []string) bool {
	return len(subset) != 0 && len(subset) < len(hosts) && !slices.ContainsFunc(subset, func(host string) bool {
		return !slices.Contains(hosts, host)
	})
}

// Native wildcard selectors match any valid DNS name ending in their suffix.
func independentHTTPSHosts(hosts []string) bool {
	for i, host := range hosts {
		if amphora.ValidateSNIHostname(host) != nil || i > 0 && hosts[i-1] >= host {
			return false
		}
		for _, peer := range hosts[:i] {
			if amphora.SNISuffixCovers(peer, host) || amphora.SNISuffixCovers(host, peer) {
				return false
			}
		}
	}
	return true
}

type httpsLivePolicy struct {
	observed   observedRoutePolicy
	owner      cloud.Identity
	authorized bool
	detaching  bool
}

// Reuse the normal policy model, including resolved weighted targets and
// fixed responses, to prove which current actions may remain exposed.
func liveHTTPSPolicies(desired desiredGatewayGraph, actual observedGatewayGraph) map[string]httpsLivePolicy {
	if len(desired.routes) != len(actual.routes) {
		return nil
	}
	parts, readback, owners := httpPlanningParts(desired, actual)
	result := make(map[string]httpsLivePolicy)
	for index, route := range parts.routes {
		observed := readback.routes[index]
		authorized := parts.spec.Routes[index].Disposition == cloud.GraphEnsure && route.redirectIssue == nil &&
			route.backendTLS.allowsMembers() && !route.backendTransportNeedsWithdrawal(observed)
		if authorized && route.unavailable && !route.fixedResponse {
			withdrawal, hold := httpMemberWithdrawal(observed)
			authorized = len(withdrawal) == 0 && hold == ""
		}
		for _, policy := range observed.policies {
			record := httpsLivePolicy{observed: policy, owner: route.spec.Identity,
				detaching: desired.spec.Routes[owners[index]].Disposition == cloud.GraphDelete}
			if authorized {
				listener := slices.IndexFunc(actual.scopedListeners, func(item listeners.Listener) bool {
					return item.ID == policy.policy.ListenerID && item.ProtocolPort == scopedHTTPRoutePort(route)
				})
				record.authorized = listener >= 0 && slices.ContainsFunc(route.policies, func(wanted desiredPolicy) bool {
					return isolatedPolicyMatches(route, observed, wanted, policy)
				})
			}
			result[policy.policy.ID] = record
		}
	}
	return result
}

// The native SNI/authority selector guards every policy. Registering a new
// disjoint scope exposes only a 404 until the ordinary planner enables it.
func planLiveHTTPSHostTransition(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener,
	spec cloud.ScopedListener, inventory httpsPolicyInventory, proveRoutes func() map[string]httpsLivePolicy,
	prior []string, hasDetaching, verifiedWithdrawal bool,
) (*graphMutation, bool, bool) {
	current, next := actual.httpsVirtualHosts[listener.ID], httpsVirtualHosts(spec)
	if !actual.httpsIsolationAvailable || !hasHTTPSIsolationEnrollment(listener) ||
		listener.Protocol != terminatedHTTPS || !actual.httpsTLSMaterialMatches(listener, spec) ||
		!actual.httpFallbackReady(listener, desired.fixedHTTPResponses) {
		return nil, false, false
	}
	settled := slices.Equal(current, next) && (hasDetaching || verifiedWithdrawal ||
		prior != nil && independentHTTPSHosts(prior) && (httpsHostSubset(prior, current) || httpsHostSubset(current, prior)))
	if prior != nil && !settled && !slices.Equal(prior, current) {
		return nil, false, false
	}
	shrinking := httpsHostSubset(next, current)
	expanding := httpsHostSubset(current, next)
	if !shrinking && !expanding && !settled || !independentHTTPSHosts(current) || !independentHTTPSHosts(next) {
		return nil, false, false
	}
	proof := proveRoutes()
	if proof == nil {
		return nil, false, false
	}
	var remove, activeRemoval string
	serving := false
	for _, item := range inventory.items {
		host, policy := item.HTTPSListenerHostname, item.Policy
		entry, known := proof[policy.ID]
		if !known || item.AdminState == nil || entry.observed.initialRulesPartial ||
			!observedForwardPolicySettled(entry.observed) || !slices.Contains(current, host) {
			return nil, false, false
		}
		if slices.Contains(next, host) && !entry.detaching {
			if policy.AdminStateUp {
				if !entry.authorized {
					return nil, false, false
				}
				serving = true
			}
			continue
		}
		if policy.AdminStateUp {
			if desired.spec.Gateway.Disposition == cloud.GraphPreserve && !slices.ContainsFunc(desired.spec.Routes, func(node cloud.RouteGraphNode) bool {
				return node.Disposition == cloud.GraphDelete && node.Spec.Identity == entry.owner
			}) {
				return nil, false, false
			}
			// The API can retire a single active policy per write.
			if activeRemoval != "" {
				return nil, false, false
			}
			activeRemoval = policy.ID
		} else if remove == "" || policy.ID < remove {
			remove = policy.ID
		}
	}
	if desired.spec.Gateway.Disposition == cloud.GraphPreserve {
		// The ordinary delete plan owns the detaching policy. It disables
		// it before deleting its predicates or any referenced backends.
		return nil, serving, true
	}
	if activeRemoval != "" {
		remove = activeRemoval
	}
	if remove != "" {
		return &graphMutation{kind: graphDeleteChild, listenerID: listener.ID, scopedListener: spec, redactTLSError: true,
			deletion: gatewayDeletionStep{resource: gatewayDeletionL7Policy, resourceID: remove},
			message:  "Withdrawing an unauthorized Octavia HTTPS hostname policy"}, serving, true
	}
	if settled || expanding && desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil, serving, true
	}
	return &graphMutation{kind: graphUpdateListenerVirtualHosts, listenerID: listener.ID, scopedListener: spec, redactTLSError: true,
		message: "Updating the disjoint hostname scopes of an active Octavia HTTPS listener"}, serving, true
}

func (p *Provider) readOwnedHTTPSPolicy(ctx context.Context, identity Identity, listenerID string, policy l7policies.L7Policy, role routeResourceRole) (observedRoutePolicy, bool) {
	owner, owned := identity.RouteOwner(policy.Tags, role.raw)
	if !owned || policy.ListenerID != listenerID || policy.ProjectID != p.projectID {
		return observedRoutePolicy{}, false
	}
	observed, err := p.observeRoutePolicy(ctx, routeGraphOwner{identity: identity, recovery: &owner}, policy, listenerID, role)
	if err != nil || observed.pending() {
		return observedRoutePolicy{}, false
	}
	rules := make([]l7policies.Rule, 0, len(observed.rules))
	for _, rule := range observed.rules {
		if rule.rule.ProjectID != p.projectID {
			return observedRoutePolicy{}, false
		}
		rules = append(rules, rule.rule)
	}
	err = crossCheckCompleteChildInventory("L7 policy", policy.ID, "rule", policy.Rules, rules, func(rule l7policies.Rule) string { return rule.ID })
	return observed, err == nil
}

// Change the native hostname inventory only after retiring every proved
// policy beneath this closed listener. Updating in place preserves enrollment
// even if the controller restarts or the opt-in is withdrawn mid-transition.
func (p *Provider) planHTTPSPolicyRetirement(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, listener listeners.Listener, spec cloud.ScopedListener, items []octavia.L7PolicyObservation) {
	if actual.httpsIsolationMutation != nil || listener.DefaultPoolID != "" {
		return
	}
	for _, item := range items {
		policy := item.Policy
		role, ok := readRouteResourceRole(policy.Tags, rolePolicyExact, rolePolicyPrefix)
		if !ok || policy.ProvisioningStatus != "ACTIVE" && policy.ProvisioningStatus != "ERROR" {
			continue
		}
		if _, owned := p.readOwnedHTTPSPolicy(ctx, desired.identity, listener.ID, policy, role); !owned {
			continue
		}
		actual.httpsIsolationMutation = &graphMutation{kind: graphDeleteChild, listenerID: listener.ID, scopedListener: spec, redactTLSError: true,
			deletion: gatewayDeletionStep{resource: gatewayDeletionL7Policy, resourceID: policy.ID}, message: "Retiring an Octavia HTTP policy from a closed HTTPS listener"}
		return
	}
	if len(items) != 0 {
		return
	}
	if !actual.isHTTPSIsolated(listener) {
		actual.httpsIsolationMutation = &graphMutation{kind: graphDeleteChild, listenerID: listener.ID, scopedListener: spec,
			deletion: listenerDeletionStep(listener.ID), message: "Replacing the closed Octavia listener with isolated HTTPS"}
		return
	}
	if listener.Protocol != terminatedHTTPS || !singleServerCertificate(listener) || listener.ClientCRLContainerRef != "" || listener.DefaultTlsContainerRef == "" {
		return
	}
	if spec.TLS == nil {
		actual.httpsIsolationMutation = &graphMutation{kind: graphDeleteChild, listenerID: listener.ID, scopedListener: spec, redactTLSError: true,
			deletion: listenerDeletionStep(listener.ID), message: "Removing the closed Octavia HTTPS listener"}
		return
	}
	actual.httpsIsolationMutation = &graphMutation{kind: graphUpdateListenerVirtualHosts, listenerID: listener.ID, scopedListener: spec, redactTLSError: true,
		message: "Updating the hostname inventory of the closed Octavia HTTPS listener"}
}

func (actual observedGatewayGraph) isolatedPoliciesReady(desired desiredGatewayGraph, listener listeners.Listener) bool {
	if !actual.isHTTPSIsolated(listener) {
		return true
	}
	if actual.httpsIsolationUnverified[listener.ID] || !actual.httpsIsolationPolicies[listener.ID].complete || len(actual.routes) != len(desired.routes) {
		return false
	}
	for index, route := range desired.routes {
		if scopedHTTPRoutePort(route) != listener.ProtocolPort || desired.spec.Routes[index].Disposition != cloud.GraphEnsure {
			continue
		}
		observed := actual.routes[index]
		if route.redirectIssue != nil || !policyPositionsMatch(route.policies, observed.policies) {
			return false
		}
		for _, policy := range route.policies {
			if !slices.ContainsFunc(observed.policies, func(current observedRoutePolicy) bool {
				return current.policy.ListenerID == listener.ID && current.policy.AdminStateUp &&
					isolatedPolicyMatches(route, observed, policy, current)
			}) {
				return false
			}
		}
	}
	return true
}

func isolatedPolicyMatches(route desiredRoute, observed observedRouteGraph, policy desiredPolicy, current observedRoutePolicy) bool {
	return current.role == policy.role && current.httpsListenerHostname == policy.httpsListenerHostname &&
		slices.Equal(current.requestHeaderOperations, policy.requestHeaderOperations) &&
		slices.Equal(current.responseHeaderOperations, policy.responseHeaderOperations) &&
		(policy.requestURLRewrite == nil && !current.hasRequestURLRewrite() || policy.requestURLRewrite != nil && requestURLRewritePolicy.settled(policy, current)) &&
		(policy.requestCORS == nil && !current.hasRequestCORS() || policy.requestCORS != nil && requestCORSPolicy.settled(policy, current)) &&
		(route.redirect && observedForwardPolicySettled(current) && desiredPolicyActionMatches(policy, current, "") &&
			observedHTTPPredicateMatches(current, rulesFor(policy, route.hostnameRuleValue)) ||
			!route.redirect && observedForwardPolicyMatches(route, observed, current))
}
