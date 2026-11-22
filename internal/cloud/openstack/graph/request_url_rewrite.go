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
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

var requestURLRewritePolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityRequestURLRewrite,
	label:      "request URL rewrite",
	required:   func(policy desiredPolicy) bool { return policy.requestURLRewrite != nil },
	observed:   observedRoutePolicy.hasRequestURLRewrite,
	settled: func(wanted desiredPolicy, observed observedRoutePolicy) bool {
		return observed.urlRewriteTagged && observed.requestURLRewrite.Equal(wanted.requestURLRewrite) &&
			slices.Equal(wanted.requestHeaderOperations, observed.requestHeaderOperations) &&
			slices.Equal(wanted.responseHeaderOperations, observed.responseHeaderOperations)
	},
}

func compileRequestURLRewrite(match cloud.HTTPPathMatch) (*octavia.RequestURLRewrite, error) {
	if match.URLRewrite == nil {
		return nil, nil
	}
	filter := match.URLRewrite
	rewrite := &octavia.RequestURLRewrite{Hostname: filter.Hostname, ReplaceFullPath: filter.ReplaceFullPath}
	if filter.ReplacePrefixMatch != nil {
		if match.PathType != cloud.PathMatchPrefix {
			return nil, errors.New("URLRewrite prefix replacement requires a PathPrefix match")
		}
		rewrite.ReplacePrefix = &octavia.PathPrefixReplacement{Match: pathOf(match).value, Replacement: *filter.ReplacePrefixMatch}
	}
	if !rewrite.Valid() {
		return nil, errors.New("invalid HTTP request URL rewrite")
	}
	return rewrite, nil
}

func (policy observedRoutePolicy) hasRequestURLRewrite() bool {
	return policy.urlRewritePresent || policy.urlRewriteTagged
}

func policyURLRewritePatch(wanted *octavia.RequestURLRewrite, observed observedRoutePolicy) *octavia.RequestURLRewriteUpdate {
	tag := openstackidentity.RequestURLRewritePolicyTag
	if wanted == nil && !observed.hasRequestURLRewrite() ||
		wanted != nil && observed.urlRewriteTagged && observed.requestURLRewrite.Equal(wanted) && observed.policy.ProvisioningStatus != "ERROR" {
		return nil
	}
	tags := slices.Clone(observed.policy.Tags)
	if wanted == nil {
		tags = slices.DeleteFunc(tags, func(value string) bool { return value == tag })
	} else if !observed.urlRewriteTagged {
		tags = append(tags, tag)
	}
	return &octavia.RequestURLRewriteUpdate{Value: wanted, Tags: tags}
}

func rewritePolicyWriteRequiresCapability(route desiredRoute, step routeMutation) bool {
	if step.requestURLRewriteUpdate != nil {
		return step.requestURLRewriteUpdate.Value != nil
	}
	return isHTTPPolicyMutation(step.kind) && !httpURLPolicyCleanup(step.kind) &&
		(step.kind != routeMutationSetPolicyTarget || !step.withdrawHTTPPolicy) &&
		slices.ContainsFunc(route.policies, requestURLRewritePolicy.required)
}

func containsObservedRequestURLRewrite(routes []observedRouteGraph) bool {
	return slices.ContainsFunc(routes, func(route observedRouteGraph) bool {
		return slices.ContainsFunc(route.parts(), func(part observedRouteGraph) bool {
			return slices.ContainsFunc(part.policies, observedRoutePolicy.hasRequestURLRewrite)
		})
	})
}
