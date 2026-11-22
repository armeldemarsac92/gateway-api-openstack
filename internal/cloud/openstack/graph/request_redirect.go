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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

var requestRedirectPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityHTTPRequestRedirect,
	required:   func(policy desiredPolicy) bool { return policy.redirect != nil && policy.redirect.request != nil },
	observed:   func(policy observedRoutePolicy) bool { return policy.requestRedirectPresent },
	settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
		return wanted.httpsListenerHostname == observed.httpsListenerHostname
	},
}

// protectRequestRedirectTraffic keeps each affected physical listener closed
// until its required destinations, predicates and policy positions are verified.
// Withdrawal can proceed without the capability once the listener is closed.
func (p *Provider) protectRequestRedirectTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	if desired.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return plan
	}
	frontends := actual.scopedListeners
	if !actual.scoped && actual.listener != nil {
		frontends = []listeners.Listener{*actual.listener}
	}
	portByID := make(map[string]int, len(frontends))
	for _, listener := range frontends {
		portByID[listener.ID] = listener.ProtocolPort
	}
	states := guardedHTTPListeners(desired, actual, portByID, requestRedirectPolicy)
	if len(states) == 0 {
		return plan
	}
	available := false
	if p.httpRequestRedirect && actual.loadBalancer != nil {
		available = p.verifyGatewayHTTPCapabilities(ctx, desired, actual, []string{requestRedirectPolicy.capability}) == nil
	}
	blocked := make(map[string]bool)
	unverifiedClosed := make(map[string]bool)
	requiresWithdrawal := make(map[string]bool)
	installedOn := make(map[string]string)
	for _, listener := range frontends {
		state, relevant := states[listener.ProtocolPort]
		if !relevant {
			continue
		}
		for id := range state.policyIDs {
			installedOn[id] = listener.ID
		}
		if available && !state.unsettled {
			continue
		}
		reason := "Waiting for verified Octavia request redirects on this listener"
		if !available && state.desired {
			reason = "Waiting for Octavia to advertise " + requestRedirectPolicy.capability
		}
		for i, route := range desired.routes {
			if desired.spec.Routes[i].Disposition == cloud.GraphEnsure && requestRedirectRoutePort(desired, route) == listener.ProtocolPort {
				state.routes[i] = true
			}
		}
		p.markGuardedHTTPListener(result, listener.ProtocolPort, state, reason)
		result.Gateway.HTTPPolicyTransition = true
		blocked[listener.ID] = true
		requiresWithdrawal[listener.ID] = !available && state.installed
		if actual.adminObserved[listener.ID] && !listener.AdminStateUp && listener.ProvisioningStatus == "ACTIVE" {
			continue
		}
		unverifiedClosed[listener.ID] = true
		if listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR" {
			return []graphMutation{closeGuardedHTTPListener(listener, reason)}
		}
	}
	return slices.DeleteFunc(plan, func(step graphMutation) bool {
		listenerID := step.listenerID
		if listenerID == "" && !actual.scoped && actual.listener != nil {
			listenerID = actual.listener.ID
		}
		switch step.kind {
		case graphEnableListener:
			return blocked[listenerID]
		case graphEnableLoadBalancer:
			return len(unverifiedClosed) != 0
		case graphRouteMutation:
			if id := installedOn[step.route.id]; id != "" {
				listenerID = id
			}
			if id := installedOn[step.route.parentID]; id != "" {
				listenerID = id
			}
			if !isHTTPPolicyMutation(step.route.kind) {
				return false
			}
			if unverifiedClosed[listenerID] {
				return true
			}
			if requiresWithdrawal[listenerID] && !redirectPolicyCleanup(step.route.kind) {
				return true
			}
			return !available && redirectPolicyWriteRequiresCapability(graphRoutePart(desired, step), step.route)
		}
		return false
	})
}

func requestRedirectRoutePort(gateway desiredGatewayGraph, route desiredRoute) int {
	if port := scopedHTTPRoutePort(route); port != 0 {
		return port
	}
	return gateway.spec.Gateway.Spec.ListenerPort
}

func redirectPolicyWriteRequiresCapability(route desiredRoute, step routeMutation) bool {
	return isHTTPPolicyMutation(step.kind) && !redirectPolicyCleanup(step.kind) &&
		slices.ContainsFunc(route.policies, requestRedirectPolicy.required)
}

func redirectPolicyCleanup(kind routeMutationKind) bool {
	return kind == routeMutationDisablePolicy || kind == routeMutationDeleteRule || kind == routeMutationDeletePolicy
}
