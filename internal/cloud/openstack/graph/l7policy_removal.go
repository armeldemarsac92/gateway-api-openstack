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
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

func (p *Provider) preparePolicyRemoval(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (*octavia.PolicyRemoval, error) {
	requireClosed := step.route.kind == routeMutationDisablePolicy
	if actual.protocol != cloud.ListenerProtocolHTTP ||
		desired.spec.Gateway.Disposition == cloud.GraphDelete || step.kind != graphRouteMutation ||
		(!requireClosed && step.route.kind != routeMutationDeleteRule) || desired.spec.Routes[step.routeIndex].Disposition != cloud.GraphDelete {
		return nil, nil
	}
	// Replace only the next planned mutation for this same retiring route.
	policyID := step.route.parentID
	if requireClosed {
		policyID = step.route.id
	}
	route := actual.routes[step.routeIndex]
	policies := route.policies
	if step.backendKey != "" {
		part := route.httpParts[step.backendKey]
		if part == nil {
			return nil, nil
		}
		policies = part.policies
	}
	index := slices.IndexFunc(policies, func(policy observedRoutePolicy) bool {
		return policy.policy.ID == policyID
	})
	if index < 0 {
		return nil, nil
	}
	policy := policies[index]
	if requireClosed {
		if !policy.policy.AdminStateUp || policy.initialRulesPartial {
			return nil, nil
		}
	} else if !policy.adminObserved || policy.policy.AdminStateUp {
		// Individual rule deletion changes the predicate. Require an explicit
		// observation because the SDK also decodes a missing bit as false.
		return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("waiting for the disabled state of retiring L7 policy %s", policy.policy.ID))
	}
	if !p.l7PolicyConditionalRemoval || actual.loadBalancer == nil ||
		actual.loadBalancer.ProjectID != p.projectID || actual.loadBalancer.ProvisioningStatus != "ACTIVE" ||
		policy.policy.ProjectID != p.projectID || policy.policy.Action == "" ||
		!observedForwardPolicySettled(policy) ||
		slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool { return rule.rule.ProjectID != p.projectID }) {
		return nil, nil
	}
	listener := actual.policyListener(route, policy.policy.ID)
	if listener == nil || listener.ProjectID != p.projectID || listener.ProvisioningStatus != "ACTIVE" ||
		listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
		len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID ||
		!slices.ContainsFunc(listener.L7Policies, func(ref l7policies.L7Policy) bool { return ref.ID == policy.policy.ID }) {
		return nil, nil
	}

	required := []string{octavia.GatewayCapabilityL7PolicyConditionalRemoval}
	if requireClosed {
		if !actual.adminObserved[listener.ID] || listener.AdminStateUp {
			return nil, nil
		}
		required = append(required, octavia.GatewayCapabilityL7PolicyClosedListenerRemoval)
	}
	discovery, cancel := boundedGatewayCapabilityDiscovery(ctx)
	provider := actual.loadBalancer.Provider
	err := p.octavia.VerifyGatewayCapabilities(discovery, provider, required...)
	cancel()
	var missing *octavia.MissingGatewayCapability
	// A legacy load balancer can report "octavia" in a region that enables
	// only "amphora". Its optional capability lookup then returns HTTP 400.
	legacyAliasDisabled := provider == "octavia" && gophercloud.ResponseCodeIs(err, http.StatusBadRequest)
	if legacyAliasDisabled || errors.As(err, &missing) || isNotFound(err) || gophercloud.ResponseCodeIs(err, http.StatusNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("verify Octavia L7 policy removal capability in region %q: %w", p.region, err)
	}

	snapshot, err := p.octavia.ObservePolicyRemoval(ctx, policy.policy.ID)
	switch {
	case gophercloud.ResponseCodeIs(err, http.StatusNotImplemented):
		return nil, nil
	case isNotFound(err), gophercloud.ResponseCodeIs(err, http.StatusConflict):
		return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, err)
	case err != nil:
		return nil, err
	}
	if snapshot.LoadBalancerID != actual.loadBalancer.ID || !policySnapshotMatchesInventory(snapshot.PolicySnapshot, policy) {
		return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("waiting for an Octavia L7 policy removal snapshot matching the observed graph"))
	}
	if requireClosed && !snapshot.ListenerClosureConfirmed {
		return nil, nil
	}
	return &octavia.PolicyRemoval{
		PolicyID: policyID, ExpectedRevision: snapshot.Revision, RequireListenerDisabled: requireClosed,
	}, nil
}
