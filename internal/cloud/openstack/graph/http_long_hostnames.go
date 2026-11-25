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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func isLongHTTPHostnameRule(kind l7policies.RuleType, compare l7policies.CompareType, value string) bool {
	return kind == l7policies.TypeHostName && compare == l7policies.CompareTypeRegex && amphora.IsLongHostnameRuleValue(value)
}

func usesLongHTTPHostname(policy desiredPolicy, scalarHostname string) bool {
	return slices.ContainsFunc(rulesFor(policy, scalarHostname), func(rule desiredRule) bool {
		return isLongHTTPHostnameRule(rule.ruleType, rule.compareType, rule.value)
	})
}

func observedLongHTTPHostname(policy observedRoutePolicy) bool {
	return slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool {
		return isLongHTTPHostnameRule(l7policies.RuleType(rule.rule.RuleType), l7policies.CompareType(rule.rule.CompareType), rule.rule.Value)
	})
}

func observedScalarLongHTTPHostname(actual observedGatewayGraph) bool {
	return !actual.scoped && slices.ContainsFunc(actual.routes, func(route observedRouteGraph) bool {
		return slices.ContainsFunc(route.policies, observedLongHTTPHostname)
	})
}

// Route-only cleanup cannot repair or reopen a preserved port. Include
// preserved sibling policies only when the deletion actually touches their port.
func longHTTPHostnameDependencies(desired desiredGatewayGraph, actual observedGatewayGraph, ports map[string]int) map[int]bool {
	dependencies := make(map[int]bool)
	maps.Copy(dependencies, desired.longHTTPHostnamePorts)
	preserved := desired.spec.Gateway.Disposition == cloud.GraphPreserve
	deletingPorts := make(map[int]bool)
	for index, route := range actual.routes {
		for _, part := range route.parts() {
			for _, policy := range part.policies {
				if port, found := ports[policy.policy.ListenerID]; found {
					if observedLongHTTPHostname(policy) {
						dependencies[port] = true
					}
					if preserved && index < len(desired.spec.Routes) && desired.spec.Routes[index].Disposition == cloud.GraphDelete {
						deletingPorts[port] = true
					}
				}
			}
		}
	}
	if preserved {
		maps.DeleteFunc(dependencies, func(port int, _ bool) bool { return !deletingPorts[port] })
	}
	return dependencies
}

func longHTTPHostnameNeedsRecovery(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener) bool {
	if actual.protocol != cloud.ListenerProtocolHTTP || listener.ProvisioningStatus != "ERROR" ||
		actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return false
	}
	port := listener.ProtocolPort
	if !actual.scoped && port == 0 {
		port = desired.spec.Gateway.Spec.ListenerPort
	}
	return longHTTPHostnameDependencies(desired, actual, map[string]int{listener.ID: port})[port]
}

// Block the whole port when an unavailable hostname predicate can change
// which policy wins. This protects positive and inverted matches alike.
func (p *Provider) protectLongHTTPHostnames(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	if actual.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure && desired.spec.Gateway.Disposition != cloud.GraphPreserve {
		return plan
	}

	frontends, ports := httpGuardListeners(desired, actual)
	blockedPorts := longHTTPHostnameDependencies(desired, actual, ports)
	if len(blockedPorts) == 0 {
		return plan
	}

	reason := "Controller opt-in for long Octavia HTTP hostname rules is disabled"
	if p.longHTTPHostnames {
		capability := octavia.GatewayCapabilityHTTPLongHostname
		reason = "Waiting for Octavia to advertise " + capability
		if err := p.verifyGatewayHTTPURLCapabilities(ctx, desired, actual, []string{capability}); err == nil {
			// An ERROR readback does not prove the previous close reached Amphora.
			// Retry it before treating a rediscovered capability as a recovery.
			retryPorts := make(map[int]bool)
			for _, listener := range frontends {
				if blockedPorts[listener.ProtocolPort] && listener.ProvisioningStatus == "ERROR" {
					retryPorts[listener.ProtocolPort] = true
				}
			}
			blockedPorts = retryPorts
			if len(blockedPorts) == 0 {
				return plan
			}
			reason = "Retrying closure of an Octavia listener with long HTTP hostname rules"
		}
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
		p.markGuardedHTTPListener(result, port, affected, reason)
	}
	result.Gateway.HTTPPolicyTransition = true

	unverifiedClosed := make(map[int]bool)
	for _, listener := range frontends {
		if !blockedPorts[listener.ProtocolPort] || actual.adminObserved[listener.ID] &&
			!listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE" {
			continue
		}
		if listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR" {
			return []graphMutation{closeGuardedHTTPListener(listener, reason)}
		}
		unverifiedClosed[listener.ProtocolPort] = true
	}

	return slices.DeleteFunc(plan, func(step graphMutation) bool {
		port, present := ports[step.listenerID]
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
			return blockedPorts[port]
		case graphEnableListener:
			return blockedPorts[port]
		case graphEnableLoadBalancer:
			return len(unverifiedClosed) != 0
		case graphGRPCAuthTransition:
			return blockedPorts[port] && (unverifiedClosed[port] || !step.withdrawHTTPPolicy)
		case graphRouteMutation:
			if !present {
				port = httpURLPolicyRoutePort(desired, graphRoutePart(desired, step))
			}
			if !blockedPorts[port] || !isHTTPPolicyMutation(step.route.kind) {
				return false
			}
			if unverifiedClosed[port] || step.route.kind == routeMutationEnablePolicy {
				return true
			}
			// The existing planner has already verified this restrictive target
			// change. Allow it and ordinary deletion behind a closed frontend.
			// Short replacement rules may also repair a retired dependency.
			targetWithdrawal := step.route.kind == routeMutationSetPolicyTarget && step.route.withdrawHTTPPolicy
			return desired.longHTTPHostnamePorts[port] && !httpURLPolicyCleanup(step.route.kind) && !targetWithdrawal
		}
		return false
	})
}
