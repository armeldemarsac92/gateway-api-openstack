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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package graph

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestHTTPRequestPolicyConvergesBeforeRouteReady(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	wanted, observed := &desired.policies[0], &actual.policies[0]
	wanted.requestPolicy = &cloud.HTTPRequestPolicy{RequestTimeoutMS: ptr.To(int64(500))}
	observed.adminObserved, observed.policy.ProvisioningStatus = true, "ACTIVE"
	observed.rules[0].flagsObserved, observed.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	observed.policy.Tags = []string{"ownership"}
	plan, _, err := buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationSetHTTPRequestPolicy, plan[0].kind)
	require.Equal(t, wanted.requestPolicy, plan[0].requestPolicyUpdate.Value)
	require.Equal(t, []string{"ownership", openstackidentity.HTTPRequestPolicyTag}, plan[0].policyTags)

	observed.policy.Tags = plan[0].policyTags
	observed.requestPolicyTagged = true
	_, _, err = buildRouteEnsurePlan(desired, actual)
	require.ErrorIs(t, err, errHTTPRequestPolicyUnverified)

	observed.requestPolicy, observed.requestPolicyReported, observed.requestPolicyPresent = wanted.requestPolicy, true, true
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Empty(t, plan)
	wanted.requestPolicy = nil
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationSetHTTPRequestPolicy, plan[0].kind)
	require.Nil(t, plan[0].requestPolicyUpdate.Value)
	require.True(t, plan[0].withdrawHTTPPolicy)
	require.Equal(t, []string{"ownership"}, plan[0].policyTags)

	observed.requestPolicy, observed.requestPolicyReported = nil, false
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Equal(t, routeMutationDisablePolicy, firstRouteMutation(plan).kind)
}

func TestHTTPRetryBackoffWaitsForExactReadbackBeforeActivation(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	wanted, observed := &desired.policies[0], &actual.policies[0]
	wanted.requestPolicy = &cloud.HTTPRequestPolicy{Retry: &cloud.HTTPRetry{BackoffMS: ptr.To(int64(0))}}
	observed.adminObserved, observed.policy.ProvisioningStatus = true, "ACTIVE"
	observed.rules[0].flagsObserved, observed.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	observed.requestPolicyTagged, observed.requestPolicyReported, observed.requestPolicyPresent = true, true, true
	observed.requestPolicy = &cloud.HTTPRequestPolicy{Retry: &cloud.HTTPRetry{}}

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Equal(t, routeMutationDisablePolicy, firstRouteMutation(plan).kind)

	observed.policy.AdminStateUp = false
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationSetHTTPRequestPolicy, plan[0].kind)
	require.Equal(t, wanted.requestPolicy, plan[0].requestPolicyUpdate.Value)
	require.Contains(t, plan[0].policyTags, openstackidentity.HTTPRetryBackoffTag)

	observed.retryBackoffTagged = true
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.ErrorIs(t, err, errHTTPRequestPolicyUnverified)
	require.Empty(t, plan)

	observed.requestPolicy.Retry.BackoffMS = ptr.To(int64(50))
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Equal(t, routeMutationSetHTTPRequestPolicy, firstRouteMutation(plan).kind)

	observed.requestPolicy = wanted.requestPolicy
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Equal(t, routeMutationEnablePolicy, firstRouteMutation(plan).kind)

	observed.policy.AdminStateUp = true
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Empty(t, plan)
}

func TestHTTPRequestPolicyRegionalGatePreservesOtherPolicies(t *testing.T) {
	capabilities := []string{octavia.GatewayCapabilityHTTPRequestPolicy}
	status := http.StatusOK
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora-regional/gateway_capabilities", req.URL.Path)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}), &gatewayTransitionRequests{})
	provider.httpRequestPolicy = true
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-regional"
	actual := observedGatewayGraph{loadBalancer: lb}
	desired := desiredGatewayGraph{protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{
		{httpParts: []desiredRoute{{policies: []desiredPolicy{
			{role: "timed", position: 1, requestPolicy: &cloud.HTTPRequestPolicy{RequestTimeoutMS: ptr.To(int64(500))}},
			{role: "unchanged-rule", position: 2},
		}}}},
		{policies: []desiredPolicy{{role: "neighbor", position: 3}}},
	}}
	got := provider.gateHTTPRequestPolicies(t.Context(), &desired, actual)
	require.NoError(t, got.err)
	require.Len(t, desired.routes[0].httpParts[0].policies, 2)
	status = http.StatusServiceUnavailable
	got = provider.gateHTTPRequestPolicies(t.Context(), &desired, actual)
	require.Error(t, got.err)
	require.Nil(t, got.remainingRules)
	require.Equal(t, httpRequestPolicyUnverified, desired.httpRequestPolicies)
	require.Len(t, desired.routes[0].httpParts[0].policies, 2)
	ready := cloud.RouteReadyResult(cloud.RouteState{})
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Result: ready}}}
	provider.reportUnavailableHTTPRequestPolicies(got, &result)
	require.Equal(t, cloud.OutcomeProgressing, result.Outcome.State)
	require.Equal(t, ready, result.Routes[0].Result)
	require.False(t, canWithdrawWithoutHTTPRequestPolicyDiscovery(desired, graphMutation{kind: graphEnableListener}))
	require.False(t, canWithdrawWithoutHTTPRequestPolicyDiscovery(desired, graphMutation{kind: graphRouteMutation, route: routeMutation{kind: routeMutationEnablePolicy}}))
	require.True(t, canWithdrawWithoutHTTPRequestPolicyDiscovery(desired, graphMutation{kind: graphRouteMutation,
		withdrawHTTPPolicy: true, route: routeMutation{kind: routeMutationDisableMember}}))

	status = http.StatusOK
	capabilities = []string{}
	got = provider.gateHTTPRequestPolicies(t.Context(), &desired, actual)
	require.ErrorAs(t, got.err, new(*octavia.MissingGatewayCapability))
	require.Equal(t, map[int]bool{0: true}, got.remainingRules)
	require.Equal(t, []desiredPolicy{{role: "unchanged-rule", position: 1}}, desired.routes[0].httpParts[0].policies)
	require.Equal(t, []desiredPolicy{{role: "neighbor", position: 2}}, desired.routes[1].policies)
}

func TestHTTPRetryBackoffEditPreservesActiveReadbackDuringDiscoveryFailure(t *testing.T) {
	status := http.StatusServiceUnavailable
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora-regional/gateway_capabilities", req.URL.Path)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{
			octavia.GatewayCapabilityHTTPRequestPolicy, octavia.GatewayCapabilityHTTPRetryBackoff,
		}})
	}), &gatewayTransitionRequests{})
	provider.httpRequestPolicy = true

	route, observed := convergedRoutePlanFixture()
	observed.pool.ProvisioningStatus = "ACTIVE"
	observed.members[0].ProvisioningStatus = "ACTIVE"
	observed.monitor.ProvisioningStatus = "ACTIVE"
	backoff := func(milliseconds int64) *cloud.HTTPRequestPolicy {
		return &cloud.HTTPRequestPolicy{Retry: &cloud.HTTPRetry{BackoffMS: &milliseconds}}
	}
	route.policies[0].requestPolicy = backoff(200)
	policy := &observed.policies[0]
	policy.adminObserved, policy.targetObserved = true, true
	policy.policy.ListenerID, policy.policy.ProvisioningStatus = "listener-1", "ACTIVE"
	policy.rules[0].flagsObserved, policy.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	policy.requestPolicyTagged, policy.retryBackoffTagged = true, true
	policy.requestPolicyPresent, policy.requestPolicyReported = true, true
	policy.requestPolicy = backoff(100)

	desired, actual := fixedHTTPPlanFixture(route, observed)
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.listener.ProvisioningStatus, actual.loadBalancer.Provider = "ACTIVE", "amphora-regional"
	planForDiscovery := func() ([]graphMutation, cloud.GatewayGraphResult) {
		t.Helper()
		wanted := desired
		discovery := provider.gateHTTPRequestPolicies(t.Context(), &wanted, actual)
		if status == http.StatusOK {
			require.NoError(t, discovery.err)
		} else {
			require.Error(t, discovery.err)
			require.Equal(t, httpRequestPolicyUnverified, wanted.httpRequestPolicies)
		}
		plan, result, err := provider.buildGatewayGraphPlan(wanted, actual)
		require.NoError(t, err)
		if wanted.httpRequestPolicies == httpRequestPolicyUnverified && len(plan) != 0 &&
			!canWithdrawWithoutHTTPRequestPolicyDiscovery(wanted, plan[0]) {
			plan = nil
		}
		return plan, result
	}

	plan, result := planForDiscovery()
	require.Empty(t, plan)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)

	// When the readback itself disappears, closing the action is urgent
	// even during the same capability discovery outage.
	policy.requestPolicy = nil
	plan, _ = planForDiscovery()
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationDisablePolicy, plan[0].route.kind)
	require.True(t, plan[0].withdrawHTTPPolicy)

	policy.requestPolicy = backoff(100)
	status = http.StatusOK
	plan, _ = planForDiscovery()
	require.NotEmpty(t, plan)
	require.Equal(t, routeMutationDisablePolicy, plan[0].route.kind)

	policy.policy.AdminStateUp = false
	plan, _ = planForDiscovery()
	require.NotEmpty(t, plan)
	require.Equal(t, routeMutationSetHTTPRequestPolicy, plan[0].route.kind)
	require.True(t, backoff(200).Equal(plan[0].route.requestPolicyUpdate.Value))

	policy.requestPolicy = backoff(200)
	plan, _ = planForDiscovery()
	require.NotEmpty(t, plan)
	require.Equal(t, routeMutationEnablePolicy, plan[0].route.kind)
}

func TestHTTPRetryBackoffCapabilityWithdrawalKeepsOrdinaryRetries(t *testing.T) {
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora-regional/gateway_capabilities", req.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityHTTPRequestPolicy}})
	}), &gatewayTransitionRequests{})
	provider.httpRequestPolicy = true

	route, observed := convergedRoutePlanFixture()
	observed.pool.ProvisioningStatus = "ACTIVE"
	observed.members[0].ProvisioningStatus = "ACTIVE"
	observed.monitor.ProvisioningStatus = "ACTIVE"
	ordinary := &route.policies[0]
	ordinary.requestPolicy = &cloud.HTTPRequestPolicy{RequestTimeoutMS: ptr.To(int64(500)),
		Retry: &cloud.HTTPRetry{Codes: []int{503}}}
	ordinaryObserved := &observed.policies[0]
	ordinaryObserved.adminObserved, ordinaryObserved.targetObserved = true, true
	ordinaryObserved.policy.ListenerID, ordinaryObserved.policy.ProvisioningStatus = "listener-1", "ACTIVE"
	ordinaryObserved.rules[0].flagsObserved, ordinaryObserved.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	ordinaryObserved.requestPolicyTagged, ordinaryObserved.requestPolicyPresent, ordinaryObserved.requestPolicyReported = true, true, true
	ordinaryObserved.requestPolicy = ordinary.requestPolicy

	backoff := *ordinary
	backoff.role, backoff.path, backoff.position = "backoff", "/backoff", 2
	backoff.requestPolicy = &cloud.HTTPRequestPolicy{Retry: &cloud.HTTPRetry{BackoffMS: ptr.To(int64(0))}}
	route.policies = append(route.policies, backoff)
	backoffObserved := *ordinaryObserved
	backoffObserved.role, backoffObserved.policy.ID, backoffObserved.policy.Position = backoff.role, "policy-backoff", 2
	// An older API may still report ordinary retry fields and hide only backoff.
	backoffObserved.retryBackoffTagged = true
	observed.policies = append(observed.policies, backoffObserved)

	desired, actual := fixedHTTPPlanFixture(route, observed)
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.listener.ProvisioningStatus, actual.loadBalancer.Provider = "ACTIVE", "amphora-regional"
	got := provider.gateHTTPRequestPolicies(t.Context(), &desired, actual)
	require.ErrorAs(t, got.err, new(*octavia.MissingGatewayCapability))
	require.Equal(t, octavia.GatewayCapabilityHTTPRetryBackoff, got.capability)
	require.Equal(t, map[int]bool{0: true}, got.remainingRules)
	require.Equal(t, []desiredPolicy{*ordinary}, desired.routes[0].policies)

	plan, result, err := provider.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationDisablePolicy, plan[0].route.kind)
	require.Equal(t, backoffObserved.policy.ID, plan[0].route.id)
	require.True(t, result.Routes[0].Result.ImplementedRules)
	provider.reportUnavailableHTTPRequestPolicies(got, &result)
	require.Equal(t, "Octavia does not advertise http_retry_backoff_v1", result.Routes[0].Result.PartiallyInvalid)

	// The role itself identifies an old ordinary action if capability is
	// lost before the explicit value was ever installed.
	actual.routes[0].policies[1].retryBackoffTagged = false
	plan, _, err = provider.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, backoffObserved.policy.ID, plan[0].route.id)
}

func TestRegionalHTTPRequestPolicyWithdrawalDrainsListenerInPositionOrder(t *testing.T) {
	route, observedRoute := convergedRoutePlanFixture()
	observedRoute.pool.ProvisioningStatus = "ACTIVE"
	observedRoute.members[0].ProvisioningStatus = "ACTIVE"
	observedRoute.monitor.ProvisioningStatus = "ACTIVE"
	survivor := &observedRoute.policies[0]
	survivor.adminObserved, survivor.targetObserved = true, true
	survivor.policy.ListenerID, survivor.policy.ProvisioningStatus = "listener-1", "ACTIVE"
	survivor.rules[0].flagsObserved, survivor.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	for _, timed := range []struct {
		id       string
		position int32
	}{{"earlier", 2}, {"later", 3}} {
		policy := *survivor
		policy.role, policy.policy.ID, policy.policy.Position = timed.id, timed.id, timed.position
		policy.requestPolicyTagged = true
		observedRoute.policies = append(observedRoute.policies, policy)
	}
	desired, actual := fixedHTTPPlanFixture(route, observedRoute)
	desired.protocol, desired.httpRequestPolicies = cloud.ListenerProtocolHTTP, httpRequestPolicyUnavailable
	actual.listener.ProvisioningStatus = "ACTIVE"
	for _, index := range []int{2, 1} {
		plan, result, err := (&Provider{}).buildGatewayGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, plan, 1)
		policy := &actual.routes[0].policies[index]
		require.Equal(t, policy.policy.ID, plan[0].route.id)
		require.Equal(t, routeMutationDisablePolicy, plan[0].route.kind)
		require.True(t, result.Routes[0].Result.ImplementedRules)
		require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
		policy.policy.AdminStateUp = false
	}
}

func TestHTTPRequestPolicyClearedWithFixedResponseAfterMemberWithdrawalFails(t *testing.T) {
	route, actual := convergedRoutePlanFixture()
	route.fixedResponse, route.unavailable = true, true
	actual.members[0].ProvisioningStatus = "ERROR"
	policy := &actual.policies[0]
	policy.targetObserved, policy.requestPolicyTagged = true, true
	policy.policy.ProvisioningStatus, policy.rules[0].rule.ProvisioningStatus = "ACTIVE", "ACTIVE"
	policy.policy.Tags = []string{"ownership", openstackidentity.HTTPRequestPolicyTag, openstackidentity.RequestCORSPolicyTag}
	policy.corsTagged, policy.corsReported = true, true
	steps, _, err := buildRouteEnsurePlan(route, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].kind)

	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPut, req.Method)
		require.Equal(t, "/lbaas/l7policies/policy-1", req.URL.Path)
		var body struct {
			Policy map[string]any `json:"l7policy"`
		}
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		require.Equal(t, "FIXED_RESPONSE", body.Policy["action"])
		require.Equal(t, map[string]any{"status_code": float64(http.StatusInternalServerError)}, body.Policy["fixed_response"])
		require.Contains(t, body.Policy, "http_request_policy")
		require.Nil(t, body.Policy["http_request_policy"])
		require.Contains(t, body.Policy, "request_cors")
		require.Nil(t, body.Policy["request_cors"])
		require.Equal(t, []any{"ownership"}, body.Policy["tags"])
		safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": policy.policy.ID}})
	}), &gatewayTransitionRequests{})
	require.NoError(t, provider.executeRouteMutation(t.Context(), route, steps[0]))
}

func TestHTTPRequestPolicyWeightedWithdrawalDoesNotChangeTheNeighbor(t *testing.T) {
	part := desiredRoute{
		policies: []desiredPolicy{{role: "neighbor"}},
		weightedAuthorizations: map[string]map[string]int{
			"with-policy":    {"old": 1},
			"without-policy": {"other": 1},
		},
	}
	actual := observedRouteGraph{policies: []observedRoutePolicy{
		{
			role: "affected", adminObserved: true, requestPolicyTagged: true,
			policy:          l7policies.L7Policy{ID: "with-policy", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			weightedTargets: []octavia.WeightedHTTPTarget{{PoolID: "old", Weight: 1}},
		},
		{
			role: "neighbor", adminObserved: true,
			policy:          l7policies.L7Policy{ID: "without-policy", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			weightedTargets: []octavia.WeightedHTTPTarget{{PoolID: "other", Weight: 1}},
		},
	}}
	steps := weightedTargetWithdrawals(part, actual)
	require.Len(t, steps, 1)
	require.Equal(t, routeMutationDisablePolicy, steps[0].kind)
	require.Equal(t, "with-policy", steps[0].id)
	require.True(t, steps[0].withdrawHTTPPolicy)
}
