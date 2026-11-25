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
	"maps"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

type httpRuleCapabilities uint8

const (
	longHostnameCapability httpRuleCapabilities = 1 << iota
	literalPathCapability
)

func requiredHTTPRuleCapability(kind l7policies.RuleType, compare l7policies.CompareType, value string) httpRuleCapabilities {
	switch {
	case kind == octavia.RuleTypeHTTPPathLiteral:
		return literalPathCapability
	case kind == l7policies.TypeHostName && compare == l7policies.CompareTypeRegex && amphora.IsLongHostnameRuleValue(value):
		return longHostnameCapability
	default:
		return 0
	}
}

func desiredHTTPRuleCapabilities(policy desiredPolicy, scalarHostname string) httpRuleCapabilities {
	var required httpRuleCapabilities
	for _, rule := range rulesFor(policy, scalarHostname) {
		required |= requiredHTTPRuleCapability(rule.ruleType, rule.compareType, rule.value)
	}
	return required
}

func observedHTTPRuleCapabilities(policy observedRoutePolicy) httpRuleCapabilities {
	var required httpRuleCapabilities
	for _, rule := range policy.rules {
		required |= requiredHTTPRuleCapability(l7policies.RuleType(rule.rule.RuleType), l7policies.CompareType(rule.rule.CompareType), rule.rule.Value)
	}
	return required
}

func observedScalarHTTPRuleCapabilities(actual observedGatewayGraph) bool {
	return !actual.scoped && slices.ContainsFunc(actual.routes, func(route observedRouteGraph) bool {
		return slices.ContainsFunc(route.policies, func(policy observedRoutePolicy) bool {
			return observedHTTPRuleCapabilities(policy) != 0
		})
	})
}

// Route-only cleanup cannot repair or reopen a preserved port. Include
// preserved sibling policies only when the deletion actually touches their port.
func httpRuleDependencies(desired desiredGatewayGraph, actual observedGatewayGraph, ports map[string]int) map[int]httpRuleCapabilities {
	dependencies := make(map[int]httpRuleCapabilities)
	maps.Copy(dependencies, desired.httpRuleCapabilityPorts)
	preserved := desired.spec.Gateway.Disposition == cloud.GraphPreserve
	deletingPorts := make(map[int]bool)
	for index, route := range actual.routes {
		for _, part := range route.parts() {
			for _, policy := range part.policies {
				if port, found := ports[policy.policy.ListenerID]; found {
					if required := observedHTTPRuleCapabilities(policy); required != 0 {
						dependencies[port] |= required
					}
					if preserved && index < len(desired.spec.Routes) && desired.spec.Routes[index].Disposition == cloud.GraphDelete {
						deletingPorts[port] = true
					}
				}
			}
		}
	}
	if preserved {
		maps.DeleteFunc(dependencies, func(port int, _ httpRuleCapabilities) bool { return !deletingPorts[port] })
	}
	return dependencies
}

func httpRuleListenerNeedsRecovery(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener) bool {
	if actual.protocol != cloud.ListenerProtocolHTTP || listener.ProvisioningStatus != "ERROR" ||
		actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return false
	}
	port := listener.ProtocolPort
	if !actual.scoped && port == 0 {
		port = desired.spec.Gateway.Spec.ListenerPort
	}
	return httpRuleDependencies(desired, actual, map[string]int{listener.ID: port})[port] != 0
}

func (p *Provider) discoverHTTPRuleCapabilities(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) []string {
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	discovery, cancel := boundedGatewayCapabilityDiscovery(ctx)
	defer cancel()
	capabilities, err := p.octavia.GatewayCapabilities(discovery, provider)
	if err != nil {
		return nil
	}
	return capabilities
}

// Block the whole port when an unavailable predicate can change which
// policy wins. A rule can depend on a capability even when inverted.
func (p *Provider) protectHTTPRuleCapabilities(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	if actual.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure && desired.spec.Gateway.Disposition != cloud.GraphPreserve {
		return plan
	}

	frontends, ports := httpGuardListeners(desired, actual)
	dependencies := httpRuleDependencies(desired, actual, ports)
	if len(dependencies) == 0 {
		return plan
	}

	var required httpRuleCapabilities
	for _, dependency := range dependencies {
		required |= dependency
	}
	var advertised []string
	if required&longHostnameCapability != 0 && p.longHTTPHostnames || required&literalPathCapability != 0 && p.literalHTTPPaths {
		advertised = p.discoverHTTPRuleCapabilities(ctx, desired, actual)
	}
	var unavailable httpRuleCapabilities
	reasons := make(map[httpRuleCapabilities]string)
	for _, feature := range []struct {
		bit        httpRuleCapabilities
		enabled    bool
		capability string
		optOut     string
	}{
		{longHostnameCapability, p.longHTTPHostnames, octavia.GatewayCapabilityHTTPLongHostname, "Controller opt-in for long Octavia HTTP hostname rules is disabled"},
		{literalPathCapability, p.literalHTTPPaths, octavia.GatewayCapabilityHTTPLiteralPath, "Controller opt-in for Octavia literal HTTP path rules is disabled"},
	} {
		if required&feature.bit == 0 {
			continue
		}
		switch {
		case !feature.enabled:
			unavailable |= feature.bit
			reasons[feature.bit] = feature.optOut
		case !slices.Contains(advertised, feature.capability):
			unavailable |= feature.bit
			reasons[feature.bit] = "Waiting for Octavia to advertise " + feature.capability
		}
	}
	// ERROR does not prove the previous close reached Amphora. Retry it
	// before the port can recover, even when discovery succeeds again.
	retryPorts := make(map[int]bool)
	for _, listener := range frontends {
		if dependencies[listener.ProtocolPort] != 0 && listener.ProvisioningStatus == "ERROR" {
			retryPorts[listener.ProtocolPort] = true
		}
	}
	blockedPorts := make(map[int]httpRuleCapabilities)
	portReasons := make(map[int]string)
	for port, dependency := range dependencies {
		blocked := dependency & unavailable
		if blocked != 0 {
			blockedPorts[port] = blocked
			var messages []string
			for _, bit := range []httpRuleCapabilities{longHostnameCapability, literalPathCapability} {
				if blocked&bit != 0 {
					messages = append(messages, reasons[bit])
				}
			}
			portReasons[port] = strings.Join(messages, ". ")
		} else if retryPorts[port] {
			blockedPorts[port] = dependency
			portReasons[port] = "Retrying closure of an Octavia listener with extended HTTP rules"
		}
	}
	if len(blockedPorts) == 0 {
		return plan
	}

	for port := range blockedPorts {
		affected := make(map[int]bool)
		for i, route := range desired.routes {
			if desired.spec.Routes[i].Disposition != cloud.GraphEnsure {
				continue
			}
			for spec := range route.spec.HTTPPortSpecs() {
				if httpURLPolicyRoutePort(desired, desiredRoute{spec: spec}) == port {
					affected[i] = true
				}
			}
		}
		p.markGuardedHTTPListener(result, port, affected, portReasons[port])
	}
	result.Gateway.HTTPPolicyTransition = true

	unverifiedClosed := make(map[int]bool)
	var retry *listeners.Listener
	for _, listener := range frontends {
		if blockedPorts[listener.ProtocolPort] == 0 || actual.adminObserved[listener.ID] &&
			!listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE" {
			continue
		}
		if listener.ProvisioningStatus == "ACTIVE" {
			return []graphMutation{closeGuardedHTTPListener(listener, portReasons[listener.ProtocolPort])}
		}
		unverifiedClosed[listener.ProtocolPort] = true
		if retry == nil && listener.ProvisioningStatus == "ERROR" {
			retry = &listener
		}
	}

	plan = slices.DeleteFunc(plan, func(step graphMutation) bool {
		port, present := observedHTTPMutationPort(actual, ports, step)
		if !present && actual.listener != nil && !actual.scoped {
			port = ports[actual.listener.ID]
			present = true
		}
		switch step.kind {
		case graphCreateListener:
			port = step.scopedListener.Port
			if port == 0 {
				port = desired.spec.Gateway.Spec.ListenerPort
			}
			return blockedPorts[port] != 0
		case graphEnableListener:
			return blockedPorts[port] != 0
		case graphEnableLoadBalancer:
			return len(unverifiedClosed) != 0
		case graphGRPCAuthTransition:
			return blockedPorts[port] != 0 && (unverifiedClosed[port] || !step.withdrawHTTPPolicy)
		case graphRouteMutation:
			if !present {
				port = httpURLPolicyRoutePort(desired, graphRoutePart(desired, step))
			}
			if blockedPorts[port] == 0 || !isHTTPPolicyMutation(step.route.kind) {
				return false
			}
			if unverifiedClosed[port] || step.route.kind == routeMutationEnablePolicy {
				return true
			}
			// The existing planner has already verified this restrictive target
			// change. Allow it and ordinary deletion behind a closed frontend.
			// Short replacement rules may also repair a retired dependency.
			targetWithdrawal := step.route.kind == routeMutationSetPolicyTarget && step.route.withdrawHTTPPolicy
			return desired.httpRuleCapabilityPorts[port]&blockedPorts[port] != 0 && !httpURLPolicyCleanup(step.route.kind) && !targetWithdrawal
		}
		return false
	})
	if retry == nil {
		return plan
	}
	// An ERROR close must not take the place of a withdrawal the planner
	// has already authorized on an independently actionable port.
	if len(plan) != 0 && plan[0].withdrawHTTPPolicy {
		step := plan[0]
		port, present := observedHTTPMutationPort(actual, ports, step)
		if present && slices.ContainsFunc(frontends, func(listener listeners.Listener) bool {
			return listener.ProtocolPort == port && listener.ProvisioningStatus == "ACTIVE"
		}) && (actual.addressReadError == nil || len(plan) == 1 && canWithdrawHTTPWithoutAddress(step, desired, actual)) &&
			(desired.httpRequestPolicies != httpRequestPolicyUnverified || canWithdrawWithoutHTTPRequestPolicyDiscovery(desired, step)) {
			return plan
		}
	}
	return []graphMutation{closeGuardedHTTPListener(*retry, portReasons[retry.ProtocolPort])}
}

func observedHTTPMutationPort(actual observedGatewayGraph, ports map[string]int, step graphMutation) (int, bool) {
	// A policy being relocated still belongs to its observed listener.
	if step.kind == graphRouteMutation && isHTTPPolicyMutation(step.route.kind) && step.routeIndex < len(actual.routes) {
		for _, part := range actual.routes[step.routeIndex].parts() {
			for _, policy := range part.policies {
				if policy.policy.ID != "" && (policy.policy.ID == step.route.id || policy.policy.ID == step.route.parentID) {
					port, present := ports[policy.policy.ListenerID]
					return port, present
				}
			}
		}
	}
	port, present := ports[step.listenerID]
	return port, present
}
