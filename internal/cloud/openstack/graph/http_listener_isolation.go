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
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func listenerScope(plain, tls string) string {
	if plain != "" {
		return plain
	}
	return tls
}

func validatePlainHTTPMatch(hosts []string, section, hostname string) error {
	if len(hosts) == 0 && section == "" || slices.Contains(hosts, section) && (section == "*" || hostname != "" && hostContains(section, hostname)) {
		return nil
	}
	return cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("HTTP match is outside its configured plaintext listener section"))
}

const maxOctaviaL7RulesPerPolicy = 50

var errPlainHTTPRuleLimit = errors.New("plaintext hostname isolation exceeds the Octavia limit of 50 L7 rules per policy")

func addPlainHTTPExclusions(hosts []string, policy *desiredPolicy, scalarHostname string) error {
	section := policy.httpListenerHostname
	if err := validatePlainHTTPMatch(hosts, section, policy.hostname); err != nil || section == "" {
		return err
	}
	domain := section
	if domain == "*" {
		domain = ""
	}
	var children []string
	for _, host := range hosts {
		if host != section && host != "*" && hostContains(domain, host) &&
			(hostContains(policy.hostname, host) || hostContains(host, policy.hostname)) {
			children = append(children, host)
		}
	}
	// Excluding a wildcard also excludes its descendants. Keep only the
	// outermost exclusions that intersect this route's effective hostname.
	for _, host := range children {
		if slices.ContainsFunc(children, func(other string) bool { return other != host && hostContains(other, host) }) {
			continue
		}
		value, err := amphora.HostnameRuleValue(host)
		if err != nil {
			return err
		}
		policy.httpHostExclusions = append(policy.httpHostExclusions, redirectGuard{
			ruleType: l7policies.TypeHostName, compareType: l7policies.CompareTypeRegex, value: value,
		})
	}
	if len(rulesFor(*policy, scalarHostname)) > maxOctaviaL7RulesPerPolicy {
		policy.httpHostExclusions = compactRedirectGuards(policy.httpHostExclusions)
	}
	if len(rulesFor(*policy, scalarHostname)) > maxOctaviaL7RulesPerPolicy {
		return fmt.Errorf("%w on section %q", errPlainHTTPRuleLimit, section)
	}
	return nil
}

func hasPlainHTTPExclusions(policy observedRoutePolicy) bool {
	return slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
		return rule.base == openstackidentity.RoleRuleHTTPHostExclusion
	})
}

func plainHTTPHostRulesMatch(wanted desiredPolicy, hostname string, observed observedRoutePolicy) bool {
	isHostRule := func(rule l7policies.RuleType) bool { return rule == l7policies.TypeHostName }
	var desiredHostRules []desiredRule
	for _, rule := range rulesFor(wanted, hostname) {
		if isHostRule(rule.ruleType) {
			desiredHostRules = append(desiredHostRules, rule)
		}
	}
	count := 0
	for _, rule := range observed.rules {
		if !isHostRule(l7policies.RuleType(rule.rule.RuleType)) && rule.base != roleRuleHost &&
			rule.base != openstackidentity.RoleRuleHTTPHostExclusion {
			continue
		}
		count++
		if !rule.flagsObserved || rule.rule.ProvisioningStatus != "ACTIVE" || !rule.rule.AdminStateUp || rule.rule.Key != "" ||
			!slices.ContainsFunc(desiredHostRules, func(expected desiredRule) bool {
				return expected.role == rule.role && string(expected.ruleType) == rule.rule.RuleType &&
					string(expected.compareType) == rule.rule.CompareType && expected.value == rule.rule.Value && expected.invert == rule.rule.Invert
			}) {
			return false
		}
	}
	return count == len(desiredHostRules)
}

func observedPlainHTTPExclusions(routes []observedRouteGraph) bool {
	return slices.ContainsFunc(routes, func(route observedRouteGraph) bool {
		return slices.ContainsFunc(route.parts(), func(part observedRouteGraph) bool {
			return slices.ContainsFunc(part.policies, hasPlainHTTPExclusions)
		})
	})
}

// Changing hostname predicates in place requires a closed port. Withdraw an
// obsolete policy by itself before restoring any traffic it used to match.
func (p *Provider) protectPlainHTTPIsolation(desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	if desired.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return plan
	}
	configured := desired.spec.Gateway.Spec.HTTPListenerHostnames
	if len(configured) == 0 && !observedPlainHTTPExclusions(actual.routes) {
		return plan
	}
	physicalPort := func(route desiredRoute) int {
		if port := scopedHTTPRoutePort(route); port != 0 {
			return port
		}
		return desired.spec.Gateway.Spec.ListenerPort
	}
	var frontends []listeners.Listener
	if actual.scoped {
		frontends = actual.scopedListeners
	} else if actual.listener != nil {
		frontends = []listeners.Listener{*actual.listener}
	}
	parts, observed := desired, actual
	owners := make([]int, len(desired.routes))
	for i := range owners {
		owners[i] = i
	}
	if hasHTTPRouteParts(desired, actual) {
		parts, observed, owners = httpPlanningParts(desired, actual)
	}
	blocked := make(map[string]bool)
	var obsoletePolicy *graphMutation
	var pendingObsoletePolicy bool
	for port, issue := range desired.plainHTTPRuleLimits {
		state := guardedHTTPListener{routes: make(map[int]bool)}
		for i, route := range desired.routes {
			if desired.spec.Routes[i].Disposition == cloud.GraphEnsure && physicalPort(route) == port {
				state.routes[i] = true
			}
		}
		p.markGuardedHTTPListener(result, port, state, issue)
		result.Gateway.HTTPPolicyTransition = true
	}
	for _, frontend := range frontends {
		if frontend.Protocol != cloud.ListenerProtocolHTTP {
			continue
		}
		if len(configured[frontend.ProtocolPort]) == 0 && !slices.ContainsFunc(observed.routes, func(route observedRouteGraph) bool {
			return slices.ContainsFunc(route.policies, func(policy observedRoutePolicy) bool {
				return policy.policy.ListenerID == frontend.ID && hasPlainHTTPExclusions(policy)
			})
		}) {
			continue
		}
		state := guardedHTTPListener{routes: make(map[int]bool)}
		for i, route := range observed.routes {
			wanted := parts.routes[i]
			port := physicalPort(wanted)
			for _, policy := range route.policies {
				if policy.policy.ListenerID != frontend.ID || observedPolicyDisabled(policy) {
					continue
				}
				atTarget := parts.spec.Routes[i].Disposition == cloud.GraphEnsure && port == frontend.ProtocolPort
				match := slices.IndexFunc(wanted.policies, func(expected desiredPolicy) bool { return expected.role == policy.role })
				if !atTarget || match < 0 {
					// Policy identity also changes with a route's path. Closing just
					// the obsolete policy leaves the other sections available.
					if policy.pending() {
						pendingObsoletePolicy = true
					} else if obsoletePolicy == nil {
						step := routeMutation{kind: routeMutationDisablePolicy, id: policy.policy.ID, retry: policy.policy.ProvisioningStatus == "ERROR"}
						obsoletePolicy = &graphMutation{kind: graphRouteMutation, routeIndex: owners[i], backendKey: wanted.backendKey,
							listenerID: frontend.ID, route: step, withdrawHTTPPolicy: true, message: step.message()}
					}
					continue
				}
				if !plainHTTPHostRulesMatch(wanted.policies[match], wanted.hostnameRuleValue, policy) {
					state.routes[owners[i]] = true
				}
			}
		}
		reason := desired.plainHTTPRuleLimits[frontend.ProtocolPort]
		if reason == "" {
			if len(state.routes) == 0 {
				continue
			}
			reason = "Waiting for plaintext HTTP listener hostname isolation"
			p.markGuardedHTTPListener(result, frontend.ProtocolPort, state, reason)
			result.Gateway.HTTPPolicyTransition = true
		}
		blocked[frontend.ID] = true
		if actual.adminObserved[frontend.ID] && !frontend.AdminStateUp && frontend.ProvisioningStatus == "ACTIVE" {
			continue
		}
		if frontend.ProvisioningStatus == "ACTIVE" || frontend.ProvisioningStatus == "ERROR" {
			return []graphMutation{closeGuardedHTTPListener(frontend, "Closing the HTTP listener before changing hostname isolation")}
		}
		// An uncertain prior close must settle before any policy changes.
		return nil
	}
	if obsoletePolicy != nil {
		result.Outcome = p.progressingOutcome(obsoletePolicy.message)
		result.Routes[obsoletePolicy.routeIndex].Result.Outcome = result.Outcome
		result.Routes[obsoletePolicy.routeIndex].Result.ObservationDeferred = false
		return []graphMutation{*obsoletePolicy}
	}
	if pendingObsoletePolicy {
		result.Outcome = p.progressingOutcome("Waiting for obsolete plaintext HTTP policy readback")
		return nil
	}
	return slices.DeleteFunc(plan, func(step graphMutation) bool {
		switch step.kind {
		case graphEnableListener:
			id := step.listenerID
			if id == "" && actual.listener != nil {
				id = actual.listener.ID
			}
			return blocked[id]
		case graphEnableLoadBalancer:
			return slices.ContainsFunc(frontends, func(frontend listeners.Listener) bool {
				return blocked[frontend.ID] && (!actual.adminObserved[frontend.ID] || frontend.AdminStateUp || frontend.ProvisioningStatus != "ACTIVE")
			})
		case graphRouteMutation:
			return desired.plainHTTPRuleLimits[physicalPort(desired.routes[step.routeIndex])] != ""
		default:
			return false
		}
	})
}
