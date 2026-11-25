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
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func compileWeightedTargetFilters(match cloud.HTTPPathMatch, targets []desiredHTTPWeightedTarget) ([]octavia.WeightedHTTPTarget, error) {
	var filters []octavia.WeightedHTTPTarget
	for i, target := range targets {
		if len(target.requestHeaderOperations) == 0 && len(target.responseHeaderOperations) == 0 && target.requestURLRewrite == nil {
			continue
		}
		headers, err := cloud.NormalizeHTTPHeaderOperations(target.requestHeaderOperations)
		if err != nil {
			return nil, err
		}
		if err := cloud.ValidateHTTPBackendRequestFilterComposition(match.RequestHeaderOperations, headers, match.URLRewrite, target.requestURLRewrite); err != nil {
			return nil, err
		}
		backendMatch := match
		backendMatch.URLRewrite = target.requestURLRewrite
		rewrite, err := compileRequestURLRewrite(backendMatch)
		if err != nil {
			return nil, err
		}
		response, err := cloud.NormalizeHTTPResponseHeaderOperations(target.responseHeaderOperations)
		if err != nil {
			return nil, err
		}
		if filters == nil {
			filters = make([]octavia.WeightedHTTPTarget, len(targets))
		}
		filters[i] = octavia.WeightedHTTPTarget{RequestHeaderOperations: headers, ResponseHeaderOperations: response, RequestURLRewrite: rewrite}
	}
	return filters, nil
}

// weightedFilterTargets retains declared filters until the effective target
// vector is known. Ineligible slots no longer require their own capabilities.
func (policy desiredPolicy) weightedFilterTargets() []octavia.WeightedHTTPTarget {
	if policy.weightedPlan != nil && !policy.weightedPlan.pending {
		return policy.weightedPlan.targets
	}
	return policy.targetFilters
}

func hasWeightedTargetRequestFilters(targets []octavia.WeightedHTTPTarget) bool {
	return slices.ContainsFunc(targets, octavia.WeightedHTTPTarget.HasRequestFilters)
}

var backendRequestFiltersPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityTargetRequestFilters,
	label:      "backend request filter",
	required: func(policy desiredPolicy) bool {
		return hasWeightedTargetRequestFilters(policy.weightedFilterTargets())
	},
	observed: observedRoutePolicy.hasBackendRequestFilters,
	settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
		return observed.backendRequestFiltersTagged && wanted.weightedPlan != nil && !wanted.weightedPlan.pending &&
			slices.EqualFunc(wanted.weightedPlan.targets, observed.weightedTargets, weightedTargetsEqual) &&
			slices.Equal(wanted.requestHeaderOperations, observed.requestHeaderOperations) &&
			wanted.requestURLRewrite.Equal(observed.requestURLRewrite)
	},
	additionalCapabilities: func(policy desiredPolicy) []string {
		required := []string{octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses}
		if len(policy.requestHeaderOperations) != 0 {
			required = append(required, octavia.GatewayCapabilityRequestHeaderOperations)
		}
		if policy.requestURLRewrite != nil {
			required = append(required, octavia.GatewayCapabilityRequestURLRewrite)
		}
		return required
	},
}

func (p *Provider) protectBackendRequestFilterTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, backendRequestFiltersPolicy, p.backendRequestFilters, backendRequestFilterWriteRequiresCapability)
}

func backendRequestFilterWriteRequiresCapability(_ desiredRoute, step routeMutation) bool {
	if !isHTTPPolicyMutation(step.kind) || httpURLPolicyCleanup(step.kind) {
		return false
	}
	if step.weightedTargets != nil {
		// A restrictive update can still carry transformations for surviving slots.
		return hasWeightedTargetRequestFilters(*step.weightedTargets)
	}
	return backendRequestFiltersPolicy.required(step.policy)
}

func (policy observedRoutePolicy) hasBackendRequestFilters() bool {
	return policy.backendRequestFiltersTagged || hasWeightedTargetRequestFilters(policy.weightedTargets)
}

func hasWeightedTargetResponseHeaders(targets []octavia.WeightedHTTPTarget) bool {
	return slices.ContainsFunc(targets, func(target octavia.WeightedHTTPTarget) bool {
		return len(target.ResponseHeaderOperations) != 0
	})
}

var backendResponseHeadersPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityTargetResponseHeaders,
	label:      "backend response header modification",
	required: func(policy desiredPolicy) bool {
		return hasWeightedTargetResponseHeaders(policy.weightedFilterTargets())
	},
	observed: observedRoutePolicy.hasBackendResponseHeaders,
	settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
		return observed.backendResponseHeadersTagged && wanted.weightedPlan != nil && !wanted.weightedPlan.pending &&
			slices.EqualFunc(wanted.weightedPlan.targets, observed.weightedTargets, weightedTargetsEqual) &&
			slices.Equal(wanted.responseHeaderOperations, observed.responseHeaderOperations)
	},
	additionalCapabilities: func(policy desiredPolicy) []string {
		required := []string{octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses,
			octavia.GatewayCapabilityResponseHeaderOperations}
		if policy.grpcContentTypeRule == grpcContentTypeRuleType {
			required = append(required, octavia.GatewayCapabilityGRPCContentType)
			if hasGRPCBinaryTargetResponseMetadata(policy.weightedFilterTargets()) {
				required = append(required, octavia.GatewayCapabilityGRPCBinaryMetadata)
			}
		}
		return required
	},
}

func (p *Provider) protectBackendResponseHeaderTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, backendResponseHeadersPolicy, p.responseHeaderModifier && p.weightedHTTPTargets, backendResponseHeaderWriteRequiresCapability)
}

func backendResponseHeaderWriteRequiresCapability(_ desiredRoute, step routeMutation) bool {
	if !isHTTPPolicyMutation(step.kind) || httpURLPolicyCleanup(step.kind) {
		return false
	}
	if step.weightedTargets != nil {
		return hasWeightedTargetResponseHeaders(*step.weightedTargets)
	}
	return backendResponseHeadersPolicy.required(step.policy)
}

func (policy observedRoutePolicy) hasBackendResponseHeaders() bool {
	return policy.backendResponseHeadersTagged || hasWeightedTargetResponseHeaders(policy.weightedTargets)
}

func weightedTargetFilterTags(targets []octavia.WeightedHTTPTarget, observed observedRoutePolicy, rewrite *octavia.RequestURLRewriteUpdate) []string {
	var add []string
	if !observed.backendRequestFiltersTagged && hasWeightedTargetRequestFilters(targets) {
		add = append(add, openstackidentity.BackendRequestFiltersPolicyTag)
	}
	if !observed.backendResponseHeadersTagged && hasWeightedTargetResponseHeaders(targets) {
		add = append(add, openstackidentity.BackendResponseHeadersPolicyTag)
	}
	if len(add) == 0 {
		return nil
	}
	tags := observed.policy.Tags
	if rewrite != nil {
		tags = rewrite.Tags
	}
	return append(slices.Clone(tags), add...)
}

func incompleteBackendResponseHeaderInventory(listenerID, policyID string, cause error) error {
	return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
		&httpFilterInventoryError{listenerID: listenerID, policyID: policyID, filter: "backend response header", cause: cause})
}

type httpFilterInventoryError struct {
	listenerID string
	policyID   string
	filter     string
	ownedPools []observedRouteGraph
	cause      error
}

func (err *httpFilterInventoryError) Error() string {
	if err.cause != nil {
		return fmt.Sprintf("incomplete Octavia %s inventory on listener %s: %v", err.filter, err.listenerID, err.cause)
	}
	return fmt.Sprintf("L7 policy %s has incomplete or malformed Octavia %s inventory", err.policyID, err.filter)
}

func (p *Provider) ownedHTTPFilterListener(actual observedGatewayGraph, listenerID string) *listeners.Listener {
	if listenerID == "" || actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return nil
	}
	listener := actual.listener
	if actual.scoped {
		i := slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool { return listener.ID == listenerID })
		if i == -1 {
			return nil
		}
		listener = &actual.scopedListeners[i]
	}
	if listener == nil || listener.ID != listenerID || listener.ProjectID != p.projectID ||
		len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID ||
		listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS {
		return nil
	}
	return listener
}

func requestTransformListenerNeedsRecovery(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener) bool {
	return rewriteListenerNeedsRecovery(desired, actual, listener) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, backendRequestFiltersPolicy) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, backendResponseHeadersPolicy) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, requestCORSPolicy) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, routeExternalAuthPolicy) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, routeExternalAuthGRPCPolicy)
}
