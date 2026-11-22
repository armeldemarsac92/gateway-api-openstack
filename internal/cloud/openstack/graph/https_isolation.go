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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
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
	actual.httpsIsolationPolicies = make(map[string]httpsPolicyInventory)
	actual.httpsIsolationUnverified = make(map[string]bool)
	configured := scopedHTTPListeners(desired)
	wanted := make(map[int]map[string][]httpsPolicyBinding)
	for index, route := range desired.routes {
		if desired.spec.Routes[index].Disposition != cloud.GraphEnsure {
			continue
		}
		port := scopedHTTPRoutePort(route)
		if len(httpsVirtualHosts(configured[port])) == 0 {
			continue
		}
		if wanted[port] == nil {
			wanted[port] = make(map[string][]httpsPolicyBinding)
		}
		for _, policies := range route.policySets() {
			for _, policy := range policies {
				wanted[port][policy.role] = append(wanted[port][policy.role], httpsPolicyBinding{route.identity, policy.httpsListenerHostname})
			}
		}
	}
	for _, listener := range actual.scopedListeners {
		spec := configured[listener.ProtocolPort]
		isolated := actual.isHTTPSIsolated(listener)
		hosts := httpsVirtualHosts(spec)
		if !isolated && len(hosts) == 0 {
			continue
		}
		// Only explicit scoped authorization can reopen a listener. When
		// its model is preserved, still require the native selector proof.
		bindingsMatch := actual.httpsBindingMatches(listener, spec)
		if desired.spec.Gateway.Disposition == cloud.GraphPreserve {
			bindingsMatch = actual.httpsIsolationAvailable && hasHTTPSIsolationEnrollment(listener) &&
				len(actual.httpsVirtualHosts[listener.ID]) != 0
		}
		enrolled := hasHTTPSIsolationEnrollment(listener) && len(actual.httpsVirtualHosts[listener.ID]) != 0
		changeHosts := len(hosts) != 0 && hasAuthorizedListener(spec) && (!isolated || enrolled)
		removeFrontend := spec.TLS == nil && enrolled
		retire := desired.spec.Gateway.Disposition == cloud.GraphEnsure && actual.httpsIsolationAvailable &&
			!bindingsMatch && (changeHosts || removeFrontend)
		if !bindingsMatch {
			actual.httpsIsolationUnverified[listener.ID] = true
		}
		if !bindingsMatch && !retire || actual.loadBalancer.ProvisioningStatus != "ACTIVE" ||
			!bindingsMatch && (!actual.adminObserved[listener.ID] || listener.AdminStateUp || listener.ProvisioningStatus != "ACTIVE") {
			continue
		}
		inventory := p.readHTTPSPolicyInventory(ctx, listener)
		actual.httpsIsolationPolicies[listener.ID] = inventory
		if !inventory.complete {
			actual.httpsIsolationUnverified[listener.ID] = true
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
			if !ok || !slices.ContainsFunc(wanted[listener.ProtocolPort][role.raw], func(binding httpsPolicyBinding) bool {
				return binding.scope == hostname && binding.owner.MatchesRoute(item.Policy.Tags, role.raw)
			}) {
				actual.httpsIsolationUnverified[listener.ID] = true
				break
			}
		}
	}
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
		owner, owned := desired.identity.RouteOwner(policy.Tags, role.raw)
		if !ok || !owned || policy.ListenerID != listener.ID || policy.ProjectID != p.projectID ||
			policy.ProvisioningStatus != "ACTIVE" && policy.ProvisioningStatus != "ERROR" {
			continue
		}
		observed, err := p.observeRoutePolicy(ctx, routeGraphOwner{identity: desired.identity, recovery: &owner}, policy, listener.ID, role)
		if err != nil || observed.pending() {
			continue
		}
		rules := make([]l7policies.Rule, 0, len(observed.rules))
		for _, rule := range observed.rules {
			if rule.rule.ProjectID != p.projectID {
				break
			}
			rules = append(rules, rule.rule)
		}
		if len(rules) != len(observed.rules) || crossCheckCompleteChildInventory("L7 policy", policy.ID, "rule", policy.Rules, rules,
			func(rule l7policies.Rule) string { return rule.ID }) != nil {
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
	if listener.Protocol != terminatedHTTPS || !supportsSingleCertificate(listener) || listener.DefaultTlsContainerRef == "" {
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
				return current.role == policy.role && current.policy.ListenerID == listener.ID &&
					current.httpsListenerHostname == policy.httpsListenerHostname && current.policy.AdminStateUp &&
					slices.Equal(current.requestHeaderOperations, policy.requestHeaderOperations) &&
					(route.redirect && observedForwardPolicySettled(current) && desiredPolicyActionMatches(policy, current.policy, "") &&
						observedHTTPPredicateMatches(current, rulesFor(policy, route.hostnameRuleValue)) ||
						!route.redirect && observedForwardPolicyMatches(route, observed, current))
			}) {
				return false
			}
		}
	}
	return true
}
