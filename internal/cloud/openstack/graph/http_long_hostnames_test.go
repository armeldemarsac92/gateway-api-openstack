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
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/stretchr/testify/require"
)

func TestLongHTTPHostnameRuleRequiresRegionalDiscovery(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	var advertised, discoveryFailed bool
	var discoveries, writes int
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			discoveries++
			if discoveryFailed {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			capabilities := []string{octavia.GatewayCapabilityL7PolicyInitialRules}
			if advertised {
				capabilities = append(capabilities, octavia.GatewayCapabilityHTTPLongHostname)
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case req.Method == http.MethodPost && req.URL.Path == "/lbaas/l7policies/policy/rules":
			var body struct {
				Rule l7policies.Rule `json:"rule"`
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Equal(t, longHostnameCapability, requiredHTTPRuleCapability(l7policies.RuleType(body.Rule.RuleType), l7policies.CompareType(body.Rule.CompareType), body.Rule.Key, body.Rule.Value))
			require.False(t, body.Rule.Invert)
			require.Empty(t, body.Rule.Key)
			writes++
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"rule": map[string]string{"id": "host"}})
		default:
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{LongHTTPHostnames: true})
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolHTTP, loadBalancer: lb,
		listener:      &listeners.Listener{ID: "listener", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		adminObserved: map[string]bool{"listener": true},
	}
	result := cloud.GatewayGraphResult{Routes: make([]cloud.RouteGraphResult, 1)}
	stock, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	enable := graphMutation{kind: graphEnableListener}
	require.Equal(t, []graphMutation{enable}, provider.protectHTTPRuleCapabilities(t.Context(), stock, actual, []graphMutation{enable}, &result))
	require.Zero(t, discoveries, "a short hostname needs no capability read")

	spec.Routes[0].Spec.Hostname = strings.Repeat("a.", 126) + "b"
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	part := desired.routes[0]
	rule := rulesFor(part.policies[0], part.hostnameRuleValue)[1]
	create := graphMutation{kind: graphRouteMutation, route: routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: rule}}
	plan := provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, []graphMutation{create, enable}, &result)
	require.True(t, isHTTPListenerWithdrawal(plan))
	require.Equal(t, "listener", plan[0].listenerID)
	require.Zero(t, writes)
	require.Equal(t, cloud.OutcomeProgressing, result.Gateway.Outcome.State)

	actual.listener.AdminStateUp = false
	require.Empty(t, provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, []graphMutation{create, enable}, &result))
	advertised = true
	plan = provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, []graphMutation{create, enable}, &result)
	require.Equal(t, []graphMutation{create, enable}, plan)
	require.NoError(t, provider.executeGraphMutation(t.Context(), desired, actual, plan[0]))
	require.Equal(t, 1, writes)

	discoveryFailed = true
	actual.routes = []observedRouteGraph{{policies: []observedRoutePolicy{{
		policy: l7policies.L7Policy{ID: "policy", ListenerID: "listener"},
		rules: []observedRouteRule{{rule: l7policies.Rule{
			RuleType: "HOST_NAME", CompareType: "REGEX", Value: rule.value, Invert: true,
		}}},
	}}}}
	// An installed inverse may outlive its desired rule during replacement or
	// opt-out. It must still be withdrawn behind the closed listener.
	remove := graphMutation{kind: graphRouteMutation, route: routeMutation{kind: routeMutationDeleteRule, parentID: "policy", id: "host"}}
	require.Equal(t, []graphMutation{remove}, provider.protectHTTPRuleCapabilities(t.Context(), stock, actual, []graphMutation{remove, enable}, &result))
	provider.longHTTPHostnames = false
	before := discoveries
	require.Equal(t, []graphMutation{remove}, provider.protectHTTPRuleCapabilities(t.Context(), stock, actual, []graphMutation{remove, enable}, &result))
	require.Equal(t, before, discoveries)
}

func TestLongHTTPHostnameIsolationKeepsOneInvertedRule(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	hostname := "*." + strings.Repeat("a.", 125) + "b"
	port := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.HTTPListenerHostnames = map[int][]string{port: {"*", hostname}}
	spec.Routes[0].Spec.Hostname = ""
	spec.Routes[0].Spec.HTTPListenerHostname = "*"
	stock := &Provider{projectID: "project-a"}
	_, err := stock.buildDesiredGatewayGraph(spec)
	require.ErrorIs(t, err, cloud.ErrTerminalValidation)

	provider := &Provider{projectID: "project-a", longHTTPHostnames: true}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	require.Equal(t, longHostnameCapability, desired.httpRuleCapabilityPorts[port])
	policy := desired.routes[0].policies[0]
	require.Len(t, policy.httpHostExclusions, 1)
	rules := rulesFor(policy, "")
	require.Len(t, rules, 2)
	require.True(t, rules[1].invert)
	require.Equal(t, longHostnameCapability, requiredHTTPRuleCapability(rules[1].ruleType, rules[1].compareType, rules[1].key, rules[1].value))
	suffix := strings.TrimPrefix(hostname, "*.")
	require.True(t, redirectGuardMatches(t, policy.httpHostExclusions[0], "x."+strings.ToUpper(suffix)+":8080", "/api"))
	require.False(t, redirectGuardMatches(t, policy.httpHostExclusions[0], suffix, "/api"))
}

func TestReconcileLongHTTPHostnameRetriesClosureAfterListenerError(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "scalar"
		if scoped {
			name = "scoped"
		}
		t.Run(name, func(t *testing.T) {
			id := safetyTestIdentity(t)
			spec := completeGraphSpec(id)
			spec.Routes = nil
			port := spec.Gateway.Spec.ListenerPort
			hostname := strings.Repeat("a.", 126) + "b"
			spec.Gateway.Spec.HTTPListenerHostnames = map[int][]string{port: {"*", hostname}}
			spec.Gateway.Spec.RetainLongHTTPListenerHostnames = true

			lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
			lb["pools"], lb["listeners"] = []any{}, []map[string]string{{"id": "long"}}
			tags := safetyGatewayTags(t, id, roleListener)
			if scoped {
				spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
				spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "long", Port: port}}
				tags = append(tags, scopedPortTag(cloud.ListenerProtocolHTTP, port))
			}
			frontend := map[string]any{"id": "long", "project_id": "project-a", "protocol": "HTTP", "protocol_port": port,
				"provisioning_status": "ACTIVE", "admin_state_up": true, "default_pool_id": nil, "l7policies": []any{},
				"tags": tags, "loadbalancers": []map[string]string{{"id": "load-balancer-1"}}}
			frontends := []any{frontend}
			if scoped {
				peer := maps.Clone(frontend)
				peer["id"], peer["protocol_port"] = "peer", 8080
				peer["tags"] = append(safetyGatewayTags(t, id, roleListener), scopedPortTag(cloud.ListenerProtocolHTTP, 8080))
				lb["listeners"] = append(lb["listeners"].([]map[string]string), map[string]string{"id": "peer"})
				frontends = append(frontends, peer)
				spec.Gateway.Spec.ScopedListeners = append(spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "peer", Port: 8080})
			}
			reads := map[string]map[string]any{
				"/lbaas/loadbalancers": {"loadbalancers": []any{lb}}, "/lbaas/loadbalancers/load-balancer-1": {"loadbalancer": lb},
				"/lbaas/listeners": {"listeners": frontends}, "/lbaas/pools": {"pools": []any{}},
				"/lbaas/l7policies": {"l7policies": []any{}},
			}
			closes := 0
			provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if data, ok := reads[req.URL.Path]; req.Method == http.MethodGet && ok {
					safetyWriteJSON(t, w, data)
					return
				}
				if req.Method != http.MethodPut || req.URL.Path != "/lbaas/listeners/long" {
					t.Errorf("unexpected Octavia request %s %s", req.Method, req.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var body struct {
					Listener struct {
						Admin *bool `json:"admin_state_up"`
					} `json:"listener"`
				}
				require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
				require.NotNil(t, body.Listener.Admin)
				require.False(t, *body.Listener.Admin)
				closes++
				frontend["admin_state_up"] = false
				frontend["provisioning_status"] = "ERROR"
				if closes == 2 {
					frontend["provisioning_status"] = "ACTIVE"
				}
				safetyWriteJSON(t, w, map[string]any{"listener": frontend})
			}), &gatewayTransitionRequests{})
			for i := 1; i <= 3; i++ {
				result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
				require.NoError(t, err)
				require.Equal(t, min(i, 2), closes)
				require.Equal(t, "load-balancer-1", result.Gateway.State.LoadBalancerID)
				require.Equal(t, cloud.OutcomeProgressing, result.Gateway.Outcome.State)
				if scoped {
					require.Equal(t, []int{8080}, result.Gateway.ScopedListenerReadyPorts)
				}
			}

			spec.Gateway.Spec.HTTPListenerHostnames = nil
			spec.Gateway.Spec.RetainLongHTTPListenerHostnames = false
			frontend["provisioning_status"] = "ERROR"
			_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
			require.ErrorIs(t, err, cloud.ErrResourceFailure)
			require.Equal(t, 2, closes)
		})
	}
}

func TestWeightedTargetWithdrawalProceedsWithLongHostnameListenerClosed(t *testing.T) {
	desired, actual := weightedPlanFixture()
	desired.spec.Gateway.Spec.ListenerPort = 80
	desired.httpRuleCapabilityPorts = map[int]httpRuleCapabilities{80: longHostnameCapability}
	actual.listener.ProtocolPort = 80
	actual.listener.ProvisioningStatus = "ACTIVE"
	actual.adminObserved = map[string]bool{actual.listener.ID: true}

	local, remote, weighted := desired.routes[0].httpParts[0], desired.routes[0].httpParts[1], desired.routes[0].httpParts[2]
	weighted.weightedTargets[1].backendKey = ""
	desired.routes[0].httpParts = []desiredRoute{local, weighted}

	provider := &Provider{}
	plan := func() ([]graphMutation, cloud.GatewayGraphResult) {
		t.Helper()
		steps, result, err := provider.buildGatewayGraphPlan(desired, actual)
		require.NoError(t, err)
		return steps, result
	}
	steps, result := plan()
	require.Len(t, steps, 1)
	withdrawal := steps[0]
	require.Equal(t, routeMutationSetPolicyTarget, withdrawal.route.kind)
	require.True(t, withdrawal.route.withdrawHTTPPolicy)
	require.True(t, isHTTPListenerWithdrawal(provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result)))

	actual.listener.AdminStateUp = false
	steps, result = plan()
	protected := provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result)
	require.Equal(t, []graphMutation{withdrawal}, protected)
	policy := &actual.routes[0].httpParts[weighted.backendKey].policies[0]
	policy.weightedTargets = *withdrawal.route.weightedTargets
	steps, result = plan()
	protected = provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result)
	require.NotEmpty(t, protected)
	require.Equal(t, remote.backendKey, protected[0].backendKey)
	require.Equal(t, routeMutationDeleteMember, protected[0].route.kind)

	// Reopening and adding a new rule on the affected port stay blocked
	// while a planned sibling listener action can proceed.
	actual.scoped = true
	actual.scopedListeners = []listeners.Listener{
		*actual.listener, {ID: "peer", ProtocolPort: 8080, Protocol: "HTTP", ProvisioningStatus: "ACTIVE"},
	}
	enable := graphMutation{kind: graphEnableListener, listenerID: actual.listener.ID}
	create := graphMutation{kind: graphRouteMutation, listenerID: actual.listener.ID, route: routeMutation{kind: routeMutationCreateRule}}
	sibling := graphMutation{kind: graphEnableListener, listenerID: "peer"}
	require.Equal(t, []graphMutation{sibling}, provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, []graphMutation{enable, create, sibling}, &result))
}

func TestDeletingShortRouteDoesNotClosePreservedLongHostnamePort(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.GatewaySpec{
		ListenerProtocol: cloud.ListenerProtocolHTTP, ListenerPort: 80, ScopedListeners: []cloud.ScopedListener{},
	}}
	spec.Routes[0].Disposition = cloud.GraphDelete
	peer := spec.Routes[0]
	peer.Disposition = cloud.GraphPreserve
	peer.Spec.Identity.RouteName, peer.Spec.Identity.RouteUID = "zz-peer", "peer"
	spec.Routes = append(spec.Routes, peer)
	provider := &Provider{projectID: "project-a"}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)

	rule, err := amphora.HostnameRuleValue(strings.Repeat("a.", 126)+"b", true)
	require.NoError(t, err)
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolHTTP, scoped: true, loadBalancer: safetyObservedLoadBalancer(),
		adminObserved: map[string]bool{"short": true, "long": true}, scopedListeners: []listeners.Listener{
			{ID: "short", ProtocolPort: 80, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "long", ProtocolPort: 8080, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		}, routes: []observedRouteGraph{{}, {}},
	}
	actual.routes[0].policies = []observedRoutePolicy{{adminObserved: true, policy: l7policies.L7Policy{ID: "detaching-policy", ListenerID: "short", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}}
	actual.routes[1].policies = []observedRoutePolicy{{adminObserved: true, policy: l7policies.L7Policy{ID: "preserved-policy", ListenerID: "long", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		rules: []observedRouteRule{{rule: l7policies.Rule{RuleType: "HOST_NAME", CompareType: "REGEX", Value: rule, Invert: true}}},
	}}
	plan := func() []graphMutation {
		t.Helper()
		steps, result, err := provider.buildGatewayGraphPlan(desired, actual)
		require.NoError(t, err)
		return provider.protectHTTPRuleCapabilities(t.Context(), desired, actual, steps, &result)
	}
	deletion := plan()
	require.Len(t, deletion, 1)
	require.Equal(t, routeMutationDisablePolicy, deletion[0].route.kind)
	require.Equal(t, "detaching-policy", deletion[0].route.id)

	// Moving the protected peer to the deleted route's port makes that
	// same physical listener part of the withdrawal.
	actual.routes[1].policies[0].policy.ListenerID = "short"
	closure := plan()
	require.True(t, isHTTPListenerWithdrawal(closure))
	require.Equal(t, "short", closure[0].listenerID)
	actual.scopedListeners[0].AdminStateUp = false
	require.Equal(t, deletion, plan())
}
