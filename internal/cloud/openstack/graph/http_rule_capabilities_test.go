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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/stretchr/testify/require"
)

func TestLiteralPathAndLongHostnameRequireIndependentCapabilities(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Routes[0].Spec.Hostname = strings.Repeat("a.", 126) + "b"
	spec.Routes[0].Spec.PathType = cloud.PathMatchExact
	spec.Routes[0].Spec.PathValue = "/o'clock&books"
	port := spec.Gateway.Spec.ListenerPort
	const peerPort = 8080
	advertised := []string{octavia.GatewayCapabilityHTTPLongHostname}
	discoveries, writes := 0, 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			discoveries++
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/l7policies/policy/rules":
			var body struct {
				Rule l7policies.Rule `json:"rule"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, string(octavia.RuleTypeHTTPPathLiteral), body.Rule.RuleType)
			require.Equal(t, "EQUAL_TO", body.Rule.CompareType)
			require.Equal(t, spec.Routes[0].Spec.PathValue, body.Rule.Value)
			require.False(t, body.Rule.Invert)
			require.Empty(t, body.Rule.Key)
			writes++
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"rule": map[string]string{"id": "path-rule"}})
		default:
			t.Errorf("unexpected Octavia request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{
		LongHTTPHostnames: true, LiteralHTTPPaths: true,
	})
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	require.Equal(t, longHostnameCapability|literalPathCapability, desired.httpRuleCapabilityPorts[port])
	part := desired.routes[0]
	pathRule := rulesFor(part.policies[0], part.hostnameRuleValue)[0]
	create := graphMutation{kind: graphRouteMutation, listenerID: "literal-port",
		route: routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: pathRule}}
	enable := graphMutation{kind: graphEnableListener, listenerID: "literal-port"}
	enablePeer := graphMutation{kind: graphEnableListener, listenerID: "hostname-port"}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolHTTP, loadBalancer: lb, scoped: true,
		scopedListeners: []listeners.Listener{
			{ID: "literal-port", ProtocolPort: port, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "hostname-port", ProtocolPort: peerPort, ProvisioningStatus: "ACTIVE"},
		},
		adminObserved: map[string]bool{"literal-port": true, "hostname-port": true},
		routes: []observedRouteGraph{{policies: []observedRoutePolicy{{
			policy: l7policies.L7Policy{ID: "peer-policy", ListenerID: "hostname-port"},
			rules: []observedRouteRule{{rule: l7policies.Rule{
				RuleType: "HOST_NAME", CompareType: "REGEX", Value: part.hostnameRuleValue, Invert: true,
			}}},
		}}}},
	}
	guard := func(steps ...graphMutation) ([]graphMutation, cloud.GatewayGraphResult) {
		t.Helper()
		result := cloud.GatewayGraphResult{Routes: make([]cloud.RouteGraphResult, 1)}
		result.Gateway.ScopedListenerReadyPorts = []int{port, peerPort}
		return provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result), result
	}

	plan, result := guard(create, enable, enablePeer)
	require.True(t, isHTTPListenerWithdrawal(plan))
	require.Equal(t, "literal-port", plan[0].listenerID)
	require.Equal(t, []int{peerPort}, result.Gateway.ScopedListenerReadyPorts)
	require.Contains(t, result.Routes[0].Result.Outcome.Message, octavia.GatewayCapabilityHTTPLiteralPath)
	require.Equal(t, 1, discoveries)
	actual.scopedListeners[0].AdminStateUp = false
	plan, _ = guard(create, enable, enablePeer)
	require.Equal(t, []graphMutation{enablePeer}, plan)

	advertised = []string{octavia.GatewayCapabilityHTTPLiteralPath}
	plan, _ = guard(create, enable, enablePeer)
	require.Empty(t, plan, "the literal capability alone cannot restore either hostname-dependent port")
	advertised = append(advertised, octavia.GatewayCapabilityHTTPLongHostname)
	plan, _ = guard(create, enable, enablePeer)
	require.Equal(t, []graphMutation{create, enable, enablePeer}, plan)
	require.NoError(t, provider.executeGraphMutation(t.Context(), desired, actual, plan[0]))
	require.Equal(t, 1, writes)

	// After opt-out the desired rule can be replaced with stock encoding.
	// The observed inverted rule can be removed while the port stays closed.
	provider.literalHTTPPaths = false
	desired.httpRuleCapabilityPorts[port] = longHostnameCapability
	actual.routes[0].policies = append(actual.routes[0].policies, observedRoutePolicy{
		policy: l7policies.L7Policy{ID: "policy", ListenerID: "literal-port"},
		rules: []observedRouteRule{{rule: l7policies.Rule{
			RuleType: string(octavia.RuleTypeHTTPPathLiteral), CompareType: "STARTS_WITH", Value: pathRule.value, Invert: true,
		}}},
	})
	remove := graphMutation{kind: graphRouteMutation, listenerID: "literal-port", route: routeMutation{kind: routeMutationDeleteRule}}
	stock := graphMutation{kind: graphRouteMutation, listenerID: "literal-port", route: routeMutation{kind: routeMutationCreateRule}}
	plan, result = guard(remove, stock, enable, enablePeer)
	require.Equal(t, []graphMutation{remove, stock, enablePeer}, plan)
	require.Contains(t, result.Routes[0].Result.Outcome.Message, "opt-in")
}

func TestLiteralPathPrefixExcludesOneWholePredicateFromRedirect(t *testing.T) {
	prefix := "/p&'/" + strings.Repeat("x", 1018)
	positive := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/p&'", Precedence: 10}
	forwarding := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: prefix + "/", Precedence: 1}
	provider := &Provider{literalHTTPPaths: true}
	cells, err := provider.redirectCells(positive, []cloud.HTTPPathMatch{forwarding})
	require.NoError(t, err)
	require.Len(t, cells, 1)
	require.Equal(t, []redirectGuard{{ruleType: octavia.RuleTypeHTTPPathLiteral,
		compareType: l7policies.CompareTypeStartWith, value: prefix}}, cells[0].guards)
	policies, _, err := provider.httpPathPolicies(positive, true)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	policy := policies[0]
	policy.redirect = &desiredRedirectPolicy{guards: cells[0].guards}
	rules := rulesFor(policy, "")
	require.Len(t, rules, 2)
	require.Equal(t, octavia.RuleTypeHTTPPathLiteral, rules[0].ruleType)
	require.Equal(t, l7policies.CompareTypeStartWith, rules[0].compareType)
	require.False(t, rules[0].invert)
	require.Equal(t, octavia.RuleTypeHTTPPathLiteral, rules[1].ruleType)
	require.Equal(t, l7policies.CompareTypeStartWith, rules[1].compareType)
	require.Equal(t, prefix, rules[1].value)
	require.True(t, rules[1].invert)
	require.Equal(t, literalPathCapability, desiredHTTPRuleCapabilities(policy, ""))
	for path, redirects := range map[string]int{prefix: 0, prefix + "/": 0, prefix + "/child": 0, prefix + "neighbor": 1} {
		require.Equal(t, redirects, eligibleRedirectCells(t, cells, "example.test", path))
	}
}

func TestReconcileLiteralPathAdoptsReadbackAndRetriesFailedClosure(t *testing.T) {
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	path := "/o'clock"
	spec.Routes[0].Spec.PathValue = path
	requests := &gatewayTransitionRequests{}
	base := convergedRouteGraphHandler(t, requests, id, true, true)
	frontend := map[string]any{
		"id": "listener-1", "project_id": "project-a", "protocol": "HTTP",
		"protocol_port": spec.Gateway.Spec.ListenerPort, "provisioning_status": "ACTIVE",
		"admin_state_up": true, "tags": safetyGatewayTags(t, id, roleListener),
	}
	pathTags, hostTags := safetyRouteTags(t, id, roleRulePath), safetyRouteTags(t, id, roleRuleHost)
	advertised := []string{octavia.GatewayCapabilityHTTPLiteralPath}
	closes := 0
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{frontend}})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies/policy-1/rules":
			safetyWriteJSON(t, w, map[string]any{"rules": []any{
				map[string]any{"id": "rule-path", "project_id": "project-a", "type": string(octavia.RuleTypeHTTPPathLiteral),
					"compare_type": "EQUAL_TO", "value": path, "admin_state_up": true, "invert": false,
					"provisioning_status": "ACTIVE", "tags": pathTags},
				map[string]any{"id": "rule-host", "project_id": "project-a", "type": "HOST_NAME",
					"compare_type": "REGEX", "value": `^api\.example\.com(:[0-9]+)?$`, "admin_state_up": true,
					"invert": false, "provisioning_status": "ACTIVE", "tags": hostTags},
			}})
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/listeners/listener-1":
			requests.record(r)
			var body struct {
				Listener struct {
					Admin *bool `json:"admin_state_up"`
				} `json:"listener"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.NotNil(t, body.Listener.Admin)
			require.False(t, *body.Listener.Admin, "an ERROR acknowledgement cannot authorize reactivation")
			closes++
			frontend["admin_state_up"] = false
			frontend["provisioning_status"] = "ERROR"
			if closes == 2 {
				frontend["provisioning_status"] = "ACTIVE"
			}
			safetyWriteJSON(t, w, map[string]any{"listener": frontend})
		default:
			base.ServeHTTP(w, r)
		}
	}), requests)
	provider.literalHTTPPaths = true
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, cloud.OutcomeReady, result.Routes[0].Result.Outcome.State)
	require.Contains(t, result.Routes[0].Result.State.L7RuleIDs, "rule-path")
	require.Empty(t, requests.mutationPaths(), "an installed path-owned literal rule should be adopted")

	advertised = []string{}
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.True(t, result.Gateway.HTTPPolicyTransition)
	require.Equal(t, 1, closes)
	require.Contains(t, result.Routes[0].Result.Outcome.Message, octavia.GatewayCapabilityHTTPLiteralPath)

	advertised = []string{octavia.GatewayCapabilityHTTPLiteralPath}
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.True(t, result.Gateway.HTTPPolicyTransition)
	require.Equal(t, 2, closes)

	advertised = []string{}
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.True(t, result.Gateway.HTTPPolicyTransition)
	require.Contains(t, result.Gateway.Outcome.Message, octavia.GatewayCapabilityHTTPLiteralPath)
	require.Equal(t, []string{"/lbaas/listeners/listener-1", "/lbaas/listeners/listener-1"}, requests.mutationPaths(),
		"the closed listener cannot reopen without the capability")
}

func TestHTTPRuleCapabilitiesCloseActivePortBeforeRetryingFailedClosure(t *testing.T) {
	provider := &Provider{}
	desired := desiredGatewayGraph{
		spec: cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure}},
		httpRuleCapabilityPorts: map[int]httpRuleCapabilities{
			80: longHostnameCapability, 8080: literalPathCapability,
		},
	}
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolHTTP, scoped: true, loadBalancer: safetyObservedLoadBalancer(),
		scopedListeners: []listeners.Listener{
			{ID: "hostname", ProtocolPort: 80, ProvisioningStatus: "ERROR"},
			{ID: "path", ProtocolPort: 8080, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		},
		adminObserved: map[string]bool{"hostname": true, "path": true},
	}
	guard := func() []graphMutation {
		t.Helper()
		plan := []graphMutation{
			{kind: graphEnableListener, listenerID: "hostname"},
			{kind: graphEnableListener, listenerID: "path"},
			{kind: graphEnableLoadBalancer},
		}
		return provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, plan, &cloud.GatewayGraphResult{})
	}

	plan := guard()
	require.True(t, isHTTPListenerWithdrawal(plan))
	require.Equal(t, "path", plan[0].listenerID)

	actual.scopedListeners[1].AdminStateUp = false
	plan = guard()
	require.True(t, isHTTPListenerWithdrawal(plan))
	require.Equal(t, "hostname", plan[0].listenerID)
}

func TestHTTPRuleCapabilitiesAllowIndependentWeightedWithdrawalBeforeFailedClosureRetry(t *testing.T) {
	desired, actual := weightedPlanFixture()
	desired.spec.Gateway.Spec.ListenerPort = 80
	desired.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "guarded", Port: 80}, {Name: "weighted", Port: 8080}}
	desired.httpRuleCapabilityPorts = map[int]httpRuleCapabilities{80: literalPathCapability}
	desired.routes[0].spec.ScopedListenerPorts = []int{8080}
	parts := desired.routes[0].httpParts
	for i := range parts {
		parts[i].spec.ScopedListenerPorts = []int{8080}
	}
	parts[2].weightedTargets[1].backendKey = ""
	desired.routes[0].httpParts = []desiredRoute{parts[0], parts[2]}
	actual.scoped = true
	actual.listener.ProtocolPort, actual.listener.ProvisioningStatus = 8080, "ACTIVE"
	actual.scopedListeners = []listeners.Listener{
		{ID: "guarded", Protocol: "HTTP", ProtocolPort: 80, ProvisioningStatus: "ERROR"},
		*actual.listener,
	}
	actual.adminObserved = map[string]bool{"guarded": true, actual.listener.ID: true}
	actual.listener = nil

	provider := &Provider{}
	steps, result, err := provider.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	withdrawal := steps[0]
	require.Equal(t, routeMutationSetPolicyTarget, withdrawal.route.kind)
	require.Equal(t, "listener-1", withdrawal.listenerID)
	require.True(t, withdrawal.withdrawHTTPPolicy)
	require.NotNil(t, withdrawal.route.weightedTargets)
	require.Equal(t, fixedWeightedHTTPTarget(1), (*withdrawal.route.weightedTargets)[1])
	plan := provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result)
	require.Len(t, plan, 1)
	require.Equal(t, withdrawal.kind, plan[0].kind)
	require.Equal(t, withdrawal.listenerID, plan[0].listenerID)
	require.Equal(t, withdrawal.route.weightedTargets, plan[0].route.weightedTargets)

	// The actual policy port controls the withdrawal when it differs from
	// the desired listener, which may happen during a relocation.
	actual.routes[0].httpParts[parts[2].backendKey].policies[0].policy.ListenerID = "guarded"
	plan = provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, []graphMutation{withdrawal}, &result)
	require.True(t, isHTTPListenerWithdrawal(plan))
	require.Equal(t, "guarded", plan[0].listenerID)
}
