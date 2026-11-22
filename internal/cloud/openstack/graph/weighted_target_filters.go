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
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func compileWeightedTargetRequestFilters(match cloud.HTTPPathMatch, targets []desiredHTTPWeightedTarget) ([]octavia.WeightedHTTPTarget, error) {
	var filters []octavia.WeightedHTTPTarget
	for i, target := range targets {
		if len(target.requestHeaderOperations) == 0 && target.requestURLRewrite == nil {
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
		if filters == nil {
			filters = make([]octavia.WeightedHTTPTarget, len(targets))
		}
		filters[i] = octavia.WeightedHTTPTarget{RequestHeaderOperations: headers, RequestURLRewrite: rewrite}
	}
	return filters, nil
}

func hasWeightedTargetRequestFilters(targets []octavia.WeightedHTTPTarget) bool {
	return slices.ContainsFunc(targets, octavia.WeightedHTTPTarget.HasRequestFilters)
}

var backendRequestFiltersPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityTargetRequestFilters,
	label:      "backend request filter",
	required:   func(policy desiredPolicy) bool { return policy.targetRequestFilters != nil },
	observed:   observedRoutePolicy.hasBackendRequestFilters,
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

func backendRequestFilterTags(targets []octavia.WeightedHTTPTarget, observed observedRoutePolicy, rewrite *octavia.RequestURLRewriteUpdate) []string {
	if observed.backendRequestFiltersTagged || !hasWeightedTargetRequestFilters(targets) {
		return nil
	}
	tags := slices.Clone(observed.policy.Tags)
	if rewrite != nil {
		tags = slices.Clone(rewrite.Tags)
	}
	return append(tags, openstackidentity.BackendRequestFiltersPolicyTag)
}

type backendRequestFilterInventoryError struct {
	listenerID string
	policyID   string
}

func (err *backendRequestFilterInventoryError) Error() string {
	return fmt.Sprintf("L7 policy %s has incomplete or malformed Octavia backend request filter inventory", err.policyID)
}

func (p *Provider) backendFilterListenerWithInventoryError(actual observedGatewayGraph, err error) *listeners.Listener {
	var incomplete *backendRequestFilterInventoryError
	if !errors.As(err, &incomplete) || actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return nil
	}
	listener := actual.listener
	if actual.scoped {
		i := slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool { return listener.ID == incomplete.listenerID })
		if i == -1 {
			return nil
		}
		listener = &actual.scopedListeners[i]
	}
	if listener == nil || listener.ID != incomplete.listenerID || listener.ProjectID != p.projectID ||
		len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID ||
		listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS {
		return nil
	}
	return listener
}

func requestTransformListenerNeedsRecovery(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener) bool {
	return rewriteListenerNeedsRecovery(desired, actual, listener) ||
		requestPolicyListenerNeedsRecovery(desired, actual, listener, backendRequestFiltersPolicy)
}
