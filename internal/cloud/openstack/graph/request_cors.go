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
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

var requestCORSPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityRequestCORS,
	label:      "CORS",
	required:   func(policy desiredPolicy) bool { return policy.requestCORS != nil },
	observed:   observedRoutePolicy.hasRequestCORS,
	additionalCapabilities: func(policy desiredPolicy) []string {
		var required []string
		if policy.weighted {
			required = append(required, octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses)
		}
		if policy.requestURLRewrite != nil {
			required = append(required, octavia.GatewayCapabilityRequestURLRewrite)
		}
		if len(policy.targetRequestFilters) != 0 {
			required = append(required, octavia.GatewayCapabilityTargetRequestFilters)
		}
		if len(policy.requestHeaderOperations) != 0 {
			required = append(required, octavia.GatewayCapabilityRequestHeaderOperations)
		}
		if len(policy.responseHeaderOperations) != 0 {
			required = append(required, octavia.GatewayCapabilityResponseHeaderOperations)
		}
		return required
	},
	settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
		return observed.corsTagged && wanted.requestCORS.Equal(observed.requestCORS) &&
			wanted.requestURLRewrite.Equal(observed.requestURLRewrite) &&
			slices.Equal(wanted.requestHeaderOperations, observed.requestHeaderOperations) &&
			slices.Equal(wanted.responseHeaderOperations, observed.responseHeaderOperations)
	},
}

func (p *Provider) protectCORSTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, requestCORSPolicy, p.httpCORS, corsPolicyWriteRequiresCapability)
}

func corsPolicyWriteRequiresCapability(_ desiredRoute, step routeMutation) bool {
	if step.requestCORSUpdate != nil {
		return step.requestCORSUpdate.Value != nil
	}
	return isHTTPPolicyMutation(step.kind) && !httpURLPolicyCleanup(step.kind) &&
		(step.kind != routeMutationSetPolicyTarget || !step.withdrawHTTPPolicy) && step.policy.requestCORS != nil
}

func (policy observedRoutePolicy) hasRequestCORS() bool {
	return policy.corsPresent || policy.corsTagged
}

func containsObservedCORS(routes []observedRouteGraph) bool {
	return slices.ContainsFunc(routes, func(route observedRouteGraph) bool {
		return slices.ContainsFunc(route.parts(), func(part observedRouteGraph) bool {
			return slices.ContainsFunc(part.policies, observedRoutePolicy.hasRequestCORS)
		})
	})
}

func policyCORSPatch(wanted *cloud.HTTPCORS, observed observedRoutePolicy) *octavia.RequestCORSUpdate {
	if wanted == nil && !observed.hasRequestCORS() ||
		wanted != nil && observed.corsTagged && wanted.Equal(observed.requestCORS) && observed.policy.ProvisioningStatus != "ERROR" {
		return nil
	}
	tag := openstackidentity.RequestCORSPolicyTag
	tags := slices.Clone(observed.policy.Tags)
	if wanted == nil {
		tags = slices.DeleteFunc(tags, func(value string) bool { return value == tag })
	} else if !observed.corsTagged {
		tags = append(tags, tag)
	}
	return &octavia.RequestCORSUpdate{Value: wanted, Tags: tags}
}

func incompleteCORSInventory(listenerID, policyID string, cause error) error {
	return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
		&httpFilterInventoryError{listenerID: listenerID, policyID: policyID, filter: "CORS", cause: cause})
}
