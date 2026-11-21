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
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func TestHTTPListenerWritesRequireEffectiveProviderCapabilities(t *testing.T) {
	for _, test := range []struct {
		name            string
		fixed, weighted bool
		capabilityReply string
		wantWrite       bool
	}{
		{name: "stock", wantWrite: true},
		{name: "fixed endpoint missing", fixed: true},
		{name: "explicit null", fixed: true, capabilityReply: `{"gateway_capabilities":null}`},
		{name: "fixed supported", fixed: true, capabilityReply: `{"gateway_capabilities":["fixed_http_responses_v1"]}`, wantWrite: true},
		{name: "weighted missing fixed", fixed: true, weighted: true, capabilityReply: `{"gateway_capabilities":["weighted_http_targets_v1"]}`},
		{name: "weighted missing weighted", fixed: true, weighted: true, capabilityReply: `{"gateway_capabilities":["fixed_http_responses_v1"]}`},
		{name: "weighted supported", fixed: true, weighted: true, capabilityReply: `{"gateway_capabilities":["fixed_http_responses_v1","weighted_http_targets_v1"]}`, wantWrite: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var probed, wrote bool
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
					probed = true
					if test.capabilityReply == "" {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(test.capabilityReply))
				case r.Method == http.MethodPost && r.URL.Path == "/lbaas/listeners":
					wrote = true
					var body struct {
						Listener struct {
							Fallback *struct {
								StatusCode int `json:"status_code"`
							} `json:"default_http_response"`
						} `json:"listener"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
						test.fixed && (body.Listener.Fallback == nil || body.Listener.Fallback.StatusCode != http.StatusNotFound) ||
						!test.fixed && body.Listener.Fallback != nil {
						t.Errorf("unexpected listener request %#v, error %v", body, err)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"listener":{"id":"http"}}`))
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a", Region: "selected-region"},
				ProviderConfig{FixedHTTPResponses: test.fixed, WeightedHTTPTargets: test.weighted})
			spec := completeGraphSpec(safetyTestIdentity(t))
			desired, err := p.buildDesiredGatewayGraph(spec)
			if err != nil {
				t.Fatal(err)
			}
			lb := safetyObservedLoadBalancer()
			lb.Provider = "amphora-region-alias"
			actual := observedGatewayGraph{loadBalancer: lb, state: cloud.GatewayState{LoadBalancerID: lb.ID}}
			step, err := p.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphCreateListener})
			if err == nil {
				err = p.executeGraphMutation(t.Context(), desired, actual, step)
			}
			if (err == nil) != test.wantWrite || wrote != test.wantWrite || probed != test.fixed {
				t.Fatalf("probed=%t wrote=%t error=%v, want probed=%t wrote=%t", probed, wrote, err, test.fixed, test.wantWrite)
			}
		})
	}
}
func TestLostHTTPActionCapabilityOnlyAllowsRestrictiveWithdrawal(t *testing.T) {
	calls := 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{FixedHTTPResponses: true, WeightedHTTPTargets: true})
	desired := desiredGatewayGraph{fixedHTTPResponses: true, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true,
		adminObserved: map[string]bool{"matched": true, "unrelated": true},
		scopedListeners: []listeners.Listener{
			{ID: "unrelated", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "matched", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		},
		routes: []observedRouteGraph{{policies: []observedRoutePolicy{{
			adminObserved: true,
			policy:        l7policies.L7Policy{ID: "weighted", ListenerID: "matched", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		}}}}}
	targets := []octavia.WeightedHTTPTarget{fixedWeightedHTTPTarget(1)}
	step := graphMutation{kind: graphRouteMutation, withdrawHTTPPolicy: true,
		route: routeMutation{kind: routeMutationSetPolicyTarget, id: "weighted", weightedTargets: &targets}}
	prepared, err := p.prepareGraphMutation(t.Context(), desired, actual, step)
	if err != nil || calls != 1 || prepared.kind != graphDisableListener || prepared.listenerID != "matched" {
		t.Fatalf("expected a stock close on the policy's listener, got %#v after %d reads, error %v", prepared, calls, err)
	}
	actual.scopedListeners[1].AdminStateUp = false
	if _, err := p.prepareGraphMutation(t.Context(), desired, actual, step); err == nil {
		t.Fatal("missing capability allowed an extension write after verified disable")
	}
}

func TestRequestHeaderPolicyDriftUsesEffectiveProviderAndCanClearAfterOptOut(t *testing.T) {
	const path = "/lbaas/providers/amphora-region-alias/gateway_capabilities"
	var advertise, clear bool
	var probes, writes int
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == path:
			probes++
			capabilities := []string{octavia.GatewayCapabilityFixedHTTPResponses}
			if advertise {
				// The header capability authorizes the write independently.
				capabilities = []string{octavia.GatewayCapabilityRequestHeaderOperations}
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/l7policies/policy-1":
			writes++
			var body map[string]map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			expected := []any{}
			if !clear {
				expected = []any{
					map[string]any{"operation": "SET", "name": "x-color", "value": ""},
					map[string]any{"operation": "REMOVE", "name": "x-old"},
				}
			}
			if !reflect.DeepEqual(body["l7policy"], map[string]any{"request_header_operations": expected}) {
				t.Errorf("policy update = %#v", body)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]any{"id": "policy-1"}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{RequestHeaderModifier: true})
	desired, observed := convergedRoutePlanFixture()
	watched := &observed.policies[0]
	watched.policy.ProvisioningStatus = "ACTIVE"
	watched.requestHeaderOperations = []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "drifted"}}
	operations := []cloud.HTTPHeaderOperation{
		{Operation: cloud.HTTPHeaderSet, Name: "x-color"},
		{Operation: cloud.HTTPHeaderRemove, Name: "x-old"},
	}
	desired.policies[0].requestHeaderOperations = operations
	gateway := desiredGatewayGraph{protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{desired}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	state := observedGatewayGraph{loadBalancer: lb}
	reconfigure := func() error {
		t.Helper()
		steps, _, err := buildRouteEnsurePlan(desired, observed)
		if err != nil || len(steps) != 1 || steps[0].kind != routeMutationSetPolicyHeaders {
			t.Fatalf("expected one in-place header update, got %#v, %v", steps, err)
		}
		step, err := p.prepareGraphMutation(t.Context(), gateway, state, graphMutation{
			kind: graphRouteMutation, route: steps[0],
		})
		if err != nil {
			return err
		}
		return p.executeRouteMutation(t.Context(), desired, step.route)
	}
	if err := reconfigure(); err == nil || probes != 1 || writes != 0 {
		t.Fatalf("missing header capability allowed a write, reads=%d writes=%d err=%v", probes, writes, err)
	}
	advertise = true
	if err := reconfigure(); err != nil || probes != 2 || writes != 1 {
		t.Fatalf("independent header patch failed, reads=%d writes=%d err=%v", probes, writes, err)
	}
	watched.requestHeaderOperations = operations
	if plan, _, err := buildRouteEnsurePlan(desired, observed); err != nil || len(plan) != 0 {
		t.Fatalf("stable request headers planned new mutations: %#v, %v", plan, err)
	}
	advertise, clear = false, true
	desired.policies[0].requestHeaderOperations = nil
	if err := reconfigure(); err != nil || probes != 2 || writes != 2 {
		t.Fatalf("could not withdraw operations after opt-out, reads=%d writes=%d err=%v", probes, writes, err)
	}
}

func TestMissingHeaderCapabilityDoesNotBlockSiblingMembers(t *testing.T) {
	type record = map[string]any
	filtered := safetyTestIdentity(t)
	siblingID := filtered.Value()
	siblingID.RouteName, siblingID.RouteUID = "sibling", "sibling-uid"
	sibling, err := NewIdentity(siblingID)
	if err != nil {
		t.Fatal(err)
	}
	spec := completeGraphSpec(filtered)
	filteredSpec := &spec.Routes[0].Spec
	filteredSpec.Hostname, filteredSpec.PathType, filteredSpec.PathValue, filteredSpec.Members = "", "", "", nil
	filteredSpec.HealthMonitor = cloud.HealthMonitorConfig{Path: "/healthz", Delay: 10, Timeout: 5, MaxRetries: 3, MaxRetriesDown: 3}
	match := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/filtered",
		RequestHeaderOperations: []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}}
	filteredSpec.HTTPBackends = []cloud.HTTPBackend{{Namespace: "application", Name: "filtered", Port: 80,
		Matches: []cloud.HTTPPathMatch{match}, Members: []cloud.Member{{Address: "10.0.0.10", Port: 30080}}}}
	siblingSpec := routeTransitionSpec(sibling)
	siblingSpec.Hostname, siblingSpec.PathValue, siblingSpec.Precedence = "", "/ordinary", 1
	siblingSpec.HealthMonitor = cloud.HealthMonitorConfig{Path: "/healthz", Delay: 10, Timeout: 5, MaxRetries: 3, MaxRetriesDown: 3}
	siblingSpec.Members = []cloud.Member{{Address: "10.0.0.20", Port: 30080}, {Address: "10.0.0.21", Port: 30080}}
	spec.Routes = append(spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: siblingSpec})
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, filtered, roleLoadBalancer), "ACTIVE")
	lb["listeners"] = []record{{"id": "listener-1"}}
	lb["pools"] = []record{{"id": "filtered-pool"}, {"id": "ordinary-pool"}}
	listener := record{"id": "listener-1", "project_id": "project-a", "provisioning_status": "ACTIVE",
		"protocol": "HTTP", "protocol_port": 80, "admin_state_up": true, "default_pool_id": nil,
		"loadbalancers": []record{{"id": lb["id"]}}, "l7policies": []record{{"id": "filtered"}, {"id": "ordinary"}},
		"tags": safetyGatewayTags(t, filtered, roleListener)}
	collections := map[string]record{
		"/lbaas/loadbalancers":                          {"loadbalancers": []record{lb}},
		"/lbaas/loadbalancers/load-balancer-1":          {"loadbalancer": lb},
		"/lbaas/listeners":                              {"listeners": []record{listener}},
		"/lbaas/providers/amphora/gateway_capabilities": {"gateway_capabilities": []string{octavia.GatewayCapabilityFixedHTTPResponses}},
	}
	addBackend := func(name, backendKey, path, address string, identity Identity, position int) {
		t.Helper()
		poolID, policyID, ruleID := name+"-pool", name, name+"-path"
		matchKey := ""
		if backendKey != "" {
			matchKey = openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, path)
		}
		tags := func(base, match string) []string {
			return safetyRouteTags(t, identity, httpResourceRole(base, backendKey, match))
		}
		pool := record{"id": poolID, "project_id": "project-a", "provisioning_status": "ACTIVE", "protocol": "HTTP",
			"lb_algorithm": "ROUND_ROBIN", "admin_state_up": true, "loadbalancers": []record{{"id": lb["id"]}},
			"healthmonitor_id": name + "-monitor", "listeners": []record{{"id": "listener-1"}},
			"members": []record{{"id": name + "-member"}}, "tags": tags(rolePool, "")}
		member := record{"id": name + "-member", "project_id": "project-a", "provisioning_status": "ACTIVE",
			"address": address, "protocol_port": 30080, "subnet_id": "member-subnet", "admin_state_up": true, "tags": tags(roleMember, "")}
		policy := record{"id": policyID, "project_id": "project-a", "listener_id": "listener-1",
			"position": position, "action": "REDIRECT_TO_POOL", "redirect_pool_id": poolID, "provisioning_status": "ACTIVE",
			"admin_state_up": true, "tags": tags(rolePolicyExact, matchKey), "rules": []record{{"id": ruleID}}}
		rule := record{"id": ruleID, "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
			"type": "PATH", "compare_type": "EQUAL_TO", "value": path, "invert": false, "tags": tags(roleRulePath, matchKey)}
		for collection, item := range map[string]record{"/lbaas/pools": pool, "/lbaas/l7policies": policy} {
			key := collection[strings.LastIndex(collection, "/")+1:]
			if collections[collection] == nil {
				collections[collection] = record{key: []record{}}
			}
			collections[collection][key] = append(collections[collection][key].([]record), item)
		}
		collections["/lbaas/pools/"+poolID+"/members"] = record{"members": []record{member}}
		collections["monitor/"+poolID] = record{"healthmonitors": []record{{
			"id": name + "-monitor", "project_id": "project-a", "provisioning_status": "ACTIVE",
			"type": "HTTP", "url_path": "/healthz", "delay": 10, "timeout": 5, "max_retries": 3,
			"max_retries_down": 3, "http_method": "GET", "expected_codes": "200-399", "admin_state_up": true,
			"pools": []record{{"id": poolID}}, "tags": tags(roleMonitor, ""),
		}}}
		collections["/lbaas/l7policies/"+policyID+"/rules"] = record{"rules": []record{rule}}
	}
	addBackend("filtered", openstackidentity.HTTPBackendKey("application", "filtered", 80), "/filtered", "10.0.0.10", filtered, 1)
	addBackend("ordinary", "", "/ordinary", "10.0.0.20", sibling, 2)
	var writes []string
	requests := &gatewayTransitionRequests{}
	probes := 0
	provider := newTestProvider(t, ServiceClients{
		ProjectID: "project-a",
		Network:   safetyServiceClient(routeTransitionEmptyFloatingIPHandler(t, requests)),
		LoadBalancer: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			if r.Method == http.MethodGet {
				switch path {
				case "/lbaas/providers/amphora/gateway_capabilities":
					probes++
				case "/lbaas/healthmonitors":
					safetyWriteJSON(t, w, collections["monitor/"+r.URL.Query().Get("pool_id")])
					return
				}
				if response, ok := collections[path]; ok {
					safetyWriteJSON(t, w, response)
					return
				}
			}
			if r.Method == http.MethodPost && path == "/lbaas/pools/ordinary-pool/members" {
				writes = append(writes, path)
				var payload struct{ Member record }
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				payload.Member["id"], payload.Member["project_id"], payload.Member["provisioning_status"] = "ordinary-new", "project-a", "ACTIVE"
				body := collections[path]
				body["members"] = append(body["members"].([]record), payload.Member)
				pool := collections["/lbaas/pools"]["pools"].([]record)[1]
				pool["members"] = append(pool["members"].([]record), record{"id": "ordinary-new"})
				w.WriteHeader(http.StatusCreated)
				safetyWriteJSON(t, w, record{"member": payload.Member})
				return
			}
			t.Errorf("unexpected request %s %s", r.Method, path)
			w.WriteHeader(http.StatusBadRequest)
		})),
	}, ProviderConfig{RequestHeaderModifier: true})
	fence := errors.New("snapshot changed")
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("blocked header work must preserve the sibling mutation fence: %v, %v", err, writes)
	}
	spec.ValidateMutation = nil
	for stage := range 2 {
		if stage == 1 {
			// The sibling's pool and members are ready. Its disabled policy
			// still has to wait for the higher-priority header operation.
			collections["/lbaas/l7policies"]["l7policies"].([]record)[1]["admin_state_up"] = false
			collections["/lbaas/pools"]["pools"].([]record)[1]["listeners"] = []record{}
		}
		result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
		blocked := slices.IndexFunc(result.Routes, func(route cloud.RouteGraphResult) bool {
			return route.Identity == filtered.Value()
		})
		if err != nil || blocked < 0 || len(writes) != 1 ||
			result.Routes[blocked].Result.Outcome.State != cloud.OutcomeProgressing ||
			!strings.Contains(result.Routes[blocked].Result.Outcome.Message, octavia.GatewayCapabilityRequestHeaderOperations) {
			t.Fatalf("missing header capability must block only policy work: result=%+v, writes=%v, err=%v", result, writes, err)
		}
	}
	if probes != 3 {
		t.Fatalf("expected one effective provider probe per snapshot, got %d", probes)
	}
}

func TestH2CPoolWritesRequireEffectiveProviderCapability(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		capabilities                   []string
		h2c, probe, write, unsupported bool
	}{
		{name: "stock pool", write: true},
		{name: "provider lacks h2c", capabilities: []string{}, h2c: true, probe: true, unsupported: true},
		{name: "provider discovery unavailable", h2c: true, probe: true},
		{name: "provider supports h2c", capabilities: []string{"backend_h2c_v1"}, h2c: true, probe: true, write: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var probed, wrote bool
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
					probed = true
					if tc.capabilities == nil {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": tc.capabilities})
				case r.Method == http.MethodPost && r.URL.Path == "/lbaas/pools":
					wrote = true
					var payload struct {
						Pool struct {
							Protocol   string `json:"protocol"`
							BackendH2C *bool  `json:"backend_h2c"`
						} `json:"pool"`
					}
					err := json.NewDecoder(r.Body).Decode(&payload)
					if err != nil || payload.Pool.Protocol != "HTTP" ||
						tc.h2c && (payload.Pool.BackendH2C == nil || !*payload.Pool.BackendH2C) ||
						!tc.h2c && payload.Pool.BackendH2C != nil {
						t.Errorf("unexpected pool request %#v, error %v", payload, err)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"pool":{"id":"pool"}}`))
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			cfg := ProviderConfig{BackendH2C: true}
			p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a", Region: "selected-region"}, cfg)
			spec := completeGraphSpec(safetyTestIdentity(t))
			spec.Routes[0].Spec.BackendH2C = tc.h2c
			desired, err := p.buildDesiredGatewayGraph(spec)
			if err != nil {
				t.Fatal(err)
			}
			lb := safetyObservedLoadBalancer()
			lb.Provider = "amphora-region-alias"
			actual := observedGatewayGraph{loadBalancer: lb, state: cloud.GatewayState{LoadBalancerID: lb.ID}}
			result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
			plan := []graphMutation{{kind: graphRouteMutation, route: routeMutation{kind: routeMutationCreatePool}}}
			step, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, actual, plan, &result)
			if err != nil {
				t.Fatal(err)
			}
			if prepared {
				if err := p.executeGraphMutation(t.Context(), desired, actual, step); err != nil {
					t.Fatal(err)
				}
			}
			if prepared != tc.write || wrote != tc.write || probed != tc.probe {
				t.Fatalf("probed=%t wrote=%t, want probed=%t wrote=%t", probed, wrote, tc.probe, tc.write)
			}
			route := result.Routes[0].Result
			if (route.UnsupportedBackendProtocol != "") != tc.unsupported ||
				!prepared && route.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("unexpected provider route outcome %#v", route)
			}
		})
	}
}

func TestH2CPoolTransportChangeWithdrawsOldMembersBeforeUpdate(t *testing.T) {
	route, observed := convergedRoutePlanFixture()
	observed.members[0].ProvisioningStatus = "ACTIVE"
	observed.pool.ProvisioningStatus = "ACTIVE"

	var writes int
	transport := true
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []map[string]any{{"id": "pool-1", "protocol": "HTTP", "backend_h2c": transport}}})
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/pools/pool-1":
			writes++
			var body map[string]map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["pool"]["backend_h2c"] != false || body["pool"]["admin_state_up"] != nil {
				t.Errorf("h2c must be cleared without enabling the pool: body = %#v, error %v", body, err)
			}
			transport = false
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "pool-1"}})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{BackendH2C: true})
	desired, actual := fixedHTTPPlanFixture(route, observed)
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.loadBalancer.Provider = "amphora"
	inventory, err := p.listGatewayPoolInventory(t.Context(), actual.loadBalancer)
	if err != nil || len(inventory.Items) != 1 || !inventory.BackendH2C["pool-1"] {
		t.Fatalf("pool inventory did not observe its transport: %#v, error %v", inventory, err)
	}
	actual.routes[0].backendH2C = inventory.BackendH2C["pool-1"]
	plan, _, err := p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationDisableMember {
		t.Fatalf("old protocol must stop serving before the update: %#v, error %v", plan, err)
	}

	actual.routes[0].members[0].AdminStateUp = false
	plan, _, err = p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationUpdatePool {
		t.Fatalf("withdrawn backend must update its pool before re-enabling members: %#v, error %v", plan, err)
	}
	step, err := p.prepareGraphMutation(t.Context(), desired, actual, plan[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := p.executeGraphMutation(t.Context(), desired, actual, step); err != nil || writes != 1 {
		t.Fatalf("pool update writes = %d, error %v", writes, err)
	}

	plan, _, err = p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].route.kind != routeMutationUpdatePool {
		t.Fatalf("unconfirmed write must leave members disabled: %#v, error %v", plan, err)
	}
	inventory, err = p.listGatewayPoolInventory(t.Context(), actual.loadBalancer)
	if err != nil || len(inventory.Items) != 1 || inventory.BackendH2C["pool-1"] {
		t.Fatalf("updated pool transport was not observed: %#v, error %v", inventory, err)
	}
	actual.routes[0].backendH2C = inventory.BackendH2C["pool-1"]
	actual.routes[0].pool.ProvisioningStatus = "PENDING_UPDATE"
	plan, result, err := p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 0 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("pending transport cannot restore members: plan=%#v result=%#v error=%v", plan, result.Routes, err)
	}
	actual.routes[0].pool.ProvisioningStatus = "ACTIVE"
	plan, _, err = p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationUpdateMember {
		t.Fatalf("observed transport should restore members: %#v, error %v", plan, err)
	}
}

func TestH2CUnavailableDoesNotBypassAnotherPoolsTransportUpdate(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		name := "separate routes"
		if grouped {
			name = "same route"
		}
		t.Run(name, func(t *testing.T) {
			a, aObserved := h2cPlanningPool("a", true)
			b, bObserved := h2cPlanningPool("b", true)
			stock, stockObserved := h2cPlanningPool("stock", false)
			desired, actual := fixedHTTPPlanFixture(a, aObserved)
			desired.protocol = cloud.ListenerProtocolHTTP
			actual.loadBalancer.Provider = "amphora"
			if grouped {
				a.backendKey, b.backendKey, stock.backendKey = "a", "b", "stock"
				desired.routes[0] = desiredRoute{httpParts: []desiredRoute{a, b, stock}}
				actual.routes[0] = observedRouteGraph{httpParts: map[string]*observedRouteGraph{"a": &aObserved, "b": &bObserved, "stock": &stockObserved}}
			} else {
				desired.routes = append(desired.routes, b, stock)
				for range 2 {
					desired.spec.Routes = append(desired.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure})
				}
				actual.routes = append(actual.routes, bObserved, stockObserved)
			}
			p := h2cUnenrolledProvider(t)
			plan, result, err := p.buildGatewayGraphPlan(desired, actual)
			if err != nil || len(plan) == 0 {
				t.Fatalf("plan = %#v, error %v", plan, err)
			}
			step, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, actual, plan, &result)
			if err != nil || !prepared || step.route.kind != routeMutationUpdateMember || step.route.parentID != "pool-stock" {
				t.Fatalf("only the stock member can be enabled: step=%#v, prepared=%t, err=%v", step.route, prepared, err)
			}
			for _, step := range plan {
				if step.route.kind == routeMutationUpdateMember && step.route.parentID != "pool-stock" {
					t.Fatalf("planning exposed a member enable before transport confirmation: %#v", step)
				}
			}
			if result.Routes[0].Result.UnsupportedBackendProtocol == "" ||
				!grouped && (result.Routes[1].Result.UnsupportedBackendProtocol == "" || result.Routes[2].Result.UnsupportedBackendProtocol != "") {
				t.Fatalf("missing h2c affected the wrong route(s): %#v", result.Routes)
			}
		})
	}
}

func TestH2CDisabledPoolReactivationRequiresEnrollment(t *testing.T) {
	route, observed := h2cPlanningPool("h2c", true)
	observed.backendH2C, observed.pool.AdminStateUp = true, false
	desired, actual := fixedHTTPPlanFixture(route, observed)
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.loadBalancer.Provider = "amphora"
	p := h2cUnenrolledProvider(t)
	plan, result, err := p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationUpdatePool {
		t.Fatalf("expected a pool enable: plan=%#v err=%v", plan, err)
	}
	_, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, actual, plan, &result)
	if prepared || err != nil || result.Routes[0].Result.UnsupportedBackendProtocol == "" {
		t.Fatalf("pool must stay disabled without provider enrollment: prepared=%t result=%#v err=%v", prepared, result.Routes[0], err)
	}
}

func TestH2CBackendWithTLSDriftOnlyWithdrawsItsPool(t *testing.T) {
	incompatible, observedTLS := h2cPlanningPool("tls", true)
	stock, observedStock := h2cPlanningPool("stock", false)
	incompatible.backendKey, stock.backendKey = "tls", "stock"
	observedTLS.pool.TLSEnabled, observedTLS.members[0].AdminStateUp = true, true
	desired, actual := fixedHTTPPlanFixture(
		desiredRoute{httpParts: []desiredRoute{incompatible, stock}},
		observedRouteGraph{httpParts: map[string]*observedRouteGraph{"tls": &observedTLS, "stock": &observedStock}},
	)

	p := &Provider{}
	plan, result, err := p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].route.kind != routeMutationDisableMember || plan[0].route.parentID != "pool-tls" {
		t.Fatalf("TLS drift must withdraw its own member: plan=%#v err=%v", plan, err)
	}
	if result.Routes[0].Result.UnsupportedBackendProtocol == "" {
		t.Fatalf("TLS drift was absent from the affected route: %#v", result.Routes)
	}

	observedTLS.members[0].AdminStateUp = false
	plan, result, err = p.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].route.kind != routeMutationUpdateMember || plan[0].route.parentID != "pool-stock" {
		t.Fatalf("TLS drift must leave other backend convergence possible: plan=%#v err=%v", plan, err)
	}
	if result.Routes[0].Result.UnsupportedBackendProtocol == "" || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("drained TLS backend must still report incompatible transport: %#v", result.Routes)
	}
}

func h2cPlanningPool(name string, h2c bool) (desiredRoute, observedRouteGraph) {
	route, observed := convergedRoutePlanFixture()
	route.spec.BackendH2C = h2c
	route.policies, observed.policies = nil, nil
	observed.pool.ID, observed.pool.ProvisioningStatus = "pool-"+name, "ACTIVE"
	observed.monitor.ProvisioningStatus = "ACTIVE"
	observed.members[0].ID, observed.members[0].ProvisioningStatus = "member-"+name, "ACTIVE"
	observed.members[0].AdminStateUp = false
	return route, observed
}

func h2cUnenrolledProvider(t *testing.T) *Provider {
	t.Helper()
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{}})
	}))
	return NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{BackendH2C: true})
}

func TestExactHTTPHeadersUseCapableProviderAndLiteralRuleRequest(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	route.HTTPBackends = []cloud.HTTPBackend{{
		Namespace: "workloads", Name: "backend", Port: 8080, Members: route.Members,
		Matches: []cloud.HTTPPathMatch{
			{Hostname: "example.test", PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 1, Headers: []cloud.HTTPHeaderMatch{{Name: "X-Version", Value: "Three"}}},
			{Hostname: "example.test", PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 0, Headers: []cloud.HTTPHeaderMatch{{Name: "X-Version", Value: "One, Two"}, {Name: "Color", Value: "orange"}}},
		},
	}}
	route.Members = nil
	if _, err := (&Provider{projectID: "project-a"}).buildDesiredGatewayGraph(spec); !errors.Is(err, cloud.ErrTerminalValidation) {
		t.Fatalf("missing controller opt-in returned %v", err)
	}

	var advertise bool
	var writes int
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			var capabilities []string
			if advertise {
				capabilities = []string{octavia.GatewayCapabilityHTTPHeaderExact}
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case request.Method == http.MethodPost && request.URL.Path == "/lbaas/l7policies/policy/rules":
			writes++
			var body struct {
				Rule struct {
					RuleType    string   `json:"type"`
					CompareType string   `json:"compare_type"`
					Key         string   `json:"key"`
					Value       string   `json:"value"`
					Invert      bool     `json:"invert"`
					AdminState  bool     `json:"admin_state_up"`
					Tags        []string `json:"tags"`
				} `json:"rule"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			rule := body.Rule
			role, tagged := openstackidentity.ResourceRole(rule.Tags)
			base, _, _, parsed := openstackidentity.ParseHTTPFragmentRole(role)
			if rule.RuleType != "HTTP_HEADER_EXACT" || rule.CompareType != "EQUAL_TO" || rule.Key != "x-version" ||
				rule.Value != "One, Two" || !rule.AdminState || rule.Invert || !tagged || !parsed || base != openstackidentity.RoleRuleHTTPHeaderExact {
				t.Fatalf("unexpected exact header predicate: %#v", rule)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"rule":{"id":"header"}}`))
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{HTTPHeaderExact: true})
	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	if len(part.policies) != 2 || part.policies[0].role == part.policies[1].role {
		t.Fatalf("expected two independently owned policies, got %#v", part.policies)
	}
	conjunction := part.policies[slices.IndexFunc(part.policies, func(policy desiredPolicy) bool { return len(policy.headers) == 2 })]
	single := part.policies[slices.IndexFunc(part.policies, func(policy desiredPolicy) bool { return len(policy.headers) == 1 })]
	predicates := rulesFor(conjunction, "")
	if conjunction.position != 1 || single.position != 2 || len(predicates) != 4 {
		t.Fatalf("incorrect policy precedence or predicates: positions %d %d, rules %#v", conjunction.position, single.position, predicates)
	}
	header := predicates[slices.IndexFunc(predicates, func(rule desiredRule) bool { return rule.key == "x-version" })]
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{loadBalancer: lb}
	create := graphMutation{kind: graphRouteMutation, backendKey: part.backendKey,
		route: routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: header}}
	for _, step := range []routeMutation{create.route,
		{kind: routeMutationCreatePolicy, policy: conjunction},
		{kind: routeMutationEnablePolicy, requiresExactHeaders: true},
	} {
		blocked := create
		blocked.route = step
		var missing *octavia.MissingGatewayCapability
		if _, err := p.prepareGraphMutation(t.Context(), desired, actual, blocked); !errors.As(err, &missing) {
			t.Fatalf("unadvertised capability allowed %v: %v", step.kind, err)
		}
	}
	if writes != 0 {
		t.Fatalf("provider received a write without advertising exact header matching: %d", writes)
	}
	advertise = true
	prepared, err := p.prepareGraphMutation(t.Context(), desired, actual, create)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.executeGraphMutation(t.Context(), desired, actual, prepared); err != nil || writes != 1 {
		t.Fatalf("exact rule writes = %d, error %v", writes, err)
	}
}

func TestExactHTTPHeaderDeletionSurvivesCapabilityWithdrawal(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{}})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{HTTPHeaderExact: true})
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	policy := observedRoutePolicy{
		adminObserved: true,
		policy:        l7policies.L7Policy{ID: "guarded", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		rules: []observedRouteRule{{base: openstackidentity.RoleRuleHTTPHeaderExact,
			rule: l7policies.Rule{ID: "header", ProvisioningStatus: "ACTIVE"}}},
	}
	actual := observedGatewayGraph{loadBalancer: lb, routes: []observedRouteGraph{{policies: []observedRoutePolicy{policy}}}}
	step, withdraw := p.missingExactHeaderWithdrawal(t.Context(), desired, actual)
	if !withdraw || !step.withdrawHTTPPolicy || step.route.kind != routeMutationDisablePolicy || step.route.id != "guarded" {
		t.Fatalf("missing capability did not close the guarded policy: %#v", step)
	}
	for _, deletion := range buildRouteDeletionPlan(actual.routes[0]) {
		if _, err := p.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphRouteMutation, route: deletion}); err != nil {
			t.Fatalf("cleanup unexpectedly requires a capability: %v", err)
		}
	}
}
