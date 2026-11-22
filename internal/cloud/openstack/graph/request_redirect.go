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
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

var requestRedirectPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityHTTPRequestRedirect,
	label:      "request redirect",
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
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, requestRedirectPolicy, p.httpRequestRedirect, redirectPolicyWriteRequiresCapability)
}

func (p *Provider) protectRequestURLRewriteTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, requestURLRewritePolicy, p.httpRequestURLRewrite, rewritePolicyWriteRequiresCapability)
}

func (p *Provider) protectHTTPURLPolicyTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation,
	result *cloud.GatewayGraphResult, predicate protectedHTTPPredicate, enabled bool, writeRequiresCapability func(desiredRoute, routeMutation) bool) []graphMutation {
	disposition := desired.spec.Gateway.Disposition
	if actual.protocol != cloud.ListenerProtocolHTTP || disposition != cloud.GraphEnsure && disposition != cloud.GraphPreserve {
		return plan
	}
	frontends := actual.scopedListeners
	if !actual.scoped && actual.listener != nil {
		frontends = []listeners.Listener{*actual.listener}
	}
	portByID := make(map[string]int, len(frontends))
	for index, listener := range frontends {
		if !actual.scoped && listener.ProtocolPort == 0 {
			listener.ProtocolPort = desired.spec.Gateway.Spec.ListenerPort
			frontends[index].ProtocolPort = listener.ProtocolPort
		}
		portByID[listener.ID] = listener.ProtocolPort
	}
	states := guardedHTTPListeners(desired, actual, portByID, predicate)
	if len(states) == 0 {
		return plan
	}
	verified := make(map[string]error)
	unavailable := make(map[string]bool)
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
		waiting := "Octavia " + predicate.label + " mode is disabled"
		available := disposition == cloud.GraphEnsure && enabled && actual.loadBalancer != nil
		if available && state.desired {
			required := append([]string{predicate.capability}, slices.Sorted(maps.Keys(state.additionalCapabilities))...)
			key := strings.Join(required, ",")
			err, found := verified[key]
			if !found {
				err = p.verifyGatewayHTTPURLCapabilities(ctx, desired, actual, required)
				verified[key] = err
			}
			var missing *octavia.MissingGatewayCapability
			available = err == nil
			switch {
			case errors.As(err, &missing):
				waiting = "Octavia does not advertise " + missing.Capability
			case err != nil:
				waiting = "Could not verify Octavia capability " + strings.Join(required, ", ")
			}
		}
		unavailable[listener.ID] = !available
		if predicate.capability == octavia.GatewayCapabilityRequestURLRewrite && (!state.desired || available) &&
			listener.ProvisioningStatus == "ACTIVE" && localOnlyRewriteWithdrawalOnPort(plan, state) {
			continue
		}
		retryClosure := listener.ProvisioningStatus == "ERROR" &&
			(predicate.capability == octavia.GatewayCapabilityRequestURLRewrite || predicate.capability == octavia.GatewayCapabilityTargetRequestFilters)
		if available && !state.unsettled && !retryClosure {
			continue
		}
		reason := "Waiting for verified Octavia " + predicate.label + " policies on this listener"
		if !available && state.desired {
			reason = waiting
		}
		p.markGuardedHTTPListener(result, listener.ProtocolPort, state.frontendRoutes, reason)
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
			patch := step.route.requestURLRewriteUpdate
			clearRewrite := patch != nil && patch.Value == nil
			targetWithdrawal := step.route.kind == routeMutationSetPolicyTarget && step.route.withdrawHTTPPolicy
			if requiresWithdrawal[listenerID] && !httpURLPolicyCleanup(step.route.kind) && !clearRewrite && !targetWithdrawal {
				return true
			}
			return unavailable[listenerID] && writeRequiresCapability(graphRoutePart(desired, step), step.route)
		}
		return false
	})
}

func httpURLPolicyRoutePort(gateway desiredGatewayGraph, route desiredRoute) int {
	if port := scopedHTTPRoutePort(route); port != 0 {
		return port
	}
	return gateway.spec.Gateway.Spec.ListenerPort
}

func redirectPolicyWriteRequiresCapability(route desiredRoute, step routeMutation) bool {
	return isHTTPPolicyMutation(step.kind) && !httpURLPolicyCleanup(step.kind) &&
		slices.ContainsFunc(route.policies, requestRedirectPolicy.required)
}

func httpURLPolicyCleanup(kind routeMutationKind) bool {
	return kind == routeMutationDisablePolicy || kind == routeMutationDeleteRule || kind == routeMutationDeletePolicy
}

func (p *Provider) verifyGatewayHTTPURLCapabilities(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, required []string) error {
	// Discovery may stop responding during an outage. Preserve time in the
	// parent context to validate and submit a restrictive listener update.
	budget := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/3)
	}
	discovery, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return p.verifyGatewayHTTPCapabilities(discovery, desired, actual, required)
}
