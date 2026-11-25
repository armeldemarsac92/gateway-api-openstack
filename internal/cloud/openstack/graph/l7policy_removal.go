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
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

func (p *Provider) preparePolicyRemoval(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (string, error) {
	if actual.protocol != cloud.ListenerProtocolHTTP ||
		desired.spec.Gateway.Disposition == cloud.GraphDelete || step.kind != graphRouteMutation ||
		step.route.kind != routeMutationDeleteRule || desired.spec.Routes[step.routeIndex].Disposition != cloud.GraphDelete {
		return "", nil
	}
	// The selected rule deletion is the first executable mutation. Use the
	// complete policy owned by that same retiring route fragment.
	route := actual.routes[step.routeIndex]
	policies := route.policies
	if step.backendKey != "" {
		part := route.httpParts[step.backendKey]
		if part == nil {
			return "", nil
		}
		policies = part.policies
	}
	index := slices.IndexFunc(policies, func(policy observedRoutePolicy) bool {
		return policy.policy.ID == step.route.parentID
	})
	if index < 0 {
		return "", nil
	}
	policy := policies[index]
	// Both deletion paths change the predicate. A missing admin_state_up
	// decodes to false in the SDK, so the explicit observation matters.
	if !policy.adminObserved || policy.policy.AdminStateUp {
		return "", cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("waiting for the disabled state of retiring L7 policy %s", policy.policy.ID))
	}
	if !p.l7PolicyConditionalRemoval || actual.loadBalancer == nil ||
		actual.loadBalancer.ProjectID != p.projectID || actual.loadBalancer.ProvisioningStatus != "ACTIVE" ||
		policy.policy.ProjectID != p.projectID || policy.policy.Action == "" ||
		!observedPolicyDisabled(policy) || !observedForwardPolicySettled(policy) ||
		slices.ContainsFunc(policy.rules, func(rule observedRouteRule) bool { return rule.rule.ProjectID != p.projectID }) {
		return "", nil
	}
	listener := actual.policyListener(route, policy.policy.ID)
	if listener == nil || listener.ProjectID != p.projectID || listener.ProvisioningStatus != "ACTIVE" ||
		listener.Protocol != cloud.ListenerProtocolHTTP && listener.Protocol != terminatedHTTPS ||
		len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != actual.loadBalancer.ID ||
		!slices.ContainsFunc(listener.L7Policies, func(ref l7policies.L7Policy) bool { return ref.ID == policy.policy.ID }) {
		return "", nil
	}

	budget := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/3)
	}
	discovery, cancel := context.WithTimeout(ctx, budget)
	err := p.octavia.VerifyGatewayCapabilities(discovery, actual.loadBalancer.Provider, octavia.GatewayCapabilityL7PolicyConditionalRemoval)
	cancel()
	var missing *octavia.MissingGatewayCapability
	if errors.As(err, &missing) || isNotFound(err) || gophercloud.ResponseCodeIs(err, http.StatusNotImplemented) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("verify Octavia L7 policy removal capability in region %q: %w", p.region, err)
	}

	snapshot, err := p.octavia.ObservePolicyRemoval(ctx, policy.policy.ID)
	switch {
	case gophercloud.ResponseCodeIs(err, http.StatusNotImplemented):
		return "", nil
	case isNotFound(err), gophercloud.ResponseCodeIs(err, http.StatusConflict):
		return "", cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, err)
	case err != nil:
		return "", err
	}
	if snapshot.LoadBalancerID != actual.loadBalancer.ID || !policySnapshotMatchesInventory(snapshot.PolicySnapshot, policy) {
		return "", cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("waiting for an Octavia L7 policy removal snapshot matching the observed graph"))
	}
	return snapshot.Revision, nil
}
