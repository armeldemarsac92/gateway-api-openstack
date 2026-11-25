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
	"fmt"
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
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
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

func TestHTTPBackendWithdrawalCombinesResponseHeaderChangesWhenAvailable(t *testing.T) {
	advertise := true
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Fatalf("unexpected capability request %s %s", r.Method, r.URL.Path)
		}
		capabilities := []string{octavia.GatewayCapabilityFixedHTTPResponses}
		if advertise {
			capabilities = append(capabilities, octavia.GatewayCapabilityResponseHeaderOperations)
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{FixedHTTPResponses: true})
	desired := desiredGatewayGraph{fixedHTTPResponses: true, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{fixedResponse: true}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	actual := observedGatewayGraph{loadBalancer: lb}
	operations := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}
	step := graphMutation{kind: graphRouteMutation, withdrawHTTPPolicy: true,
		route: routeMutation{kind: routeMutationSetPolicyTarget, id: "policy", responseHeaderOperations: operations}}
	check := func(want []cloud.HTTPHeaderOperation) {
		t.Helper()
		prepared, err := p.prepareGraphMutation(t.Context(), desired, actual, step)
		if err != nil || prepared.kind != graphRouteMutation || prepared.route.kind != routeMutationSetPolicyTarget ||
			!reflect.DeepEqual(prepared.route.responseHeaderOperations, want) {
			t.Fatalf("target withdrawal has wrong response operations: %#v, %v", prepared, err)
		}
	}

	check(operations)
	advertise = false
	check(nil)
	step.route.responseHeaderOperations = []cloud.HTTPHeaderOperation{}
	check(step.route.responseHeaderOperations)
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
			matchKey = openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, path, "")
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
			capabilities := []string{}
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

func TestHTTPPredicateCapabilityLossClosesOnlyItsListener(t *testing.T) {
	for _, predicate := range []protectedHTTPPredicate{exactHTTPHeaderPredicate, httpMethodPredicate, exactHTTPQueryParamPredicate} {
		t.Run(predicate.label, func(t *testing.T) {
			testHTTPPredicateCapabilityLoss(t, predicate)
		})
	}
}

func testHTTPPredicateCapabilityLoss(t *testing.T, predicate protectedHTTPPredicate) {
	t.Helper()
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	replyCode := http.StatusServiceUnavailable
	replyBody := ""
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		w.WriteHeader(replyCode)
		_, _ = w.Write([]byte(replyBody))
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{})
	wanted := desiredPolicy{role: "guarded", position: 1, path: "/", compareType: l7policies.CompareTypeEqual,
		method: "GET", headers: []cloud.HTTPHeaderMatch{{Name: "color", Value: "blue"}},
		queryParams: []cloud.HTTPQueryParamMatch{{Name: "color", Value: "blue"}}}
	part := desiredRoute{backendKey: "backend", policies: []desiredPolicy{wanted}}
	part.spec.ScopedListenerPorts = []int{80}
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{httpParts: []desiredRoute{part}}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	policy := observedRoutePolicy{adminObserved: true, role: wanted.role, policy: l7policies.L7Policy{
		ID: "guarded", ListenerID: "affected", AdminStateUp: true, ProvisioningStatus: "ACTIVE", Position: 1,
		Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: "backend",
	}}
	for _, rule := range rulesFor(wanted, "") {
		base := openstackidentity.RoleRulePath
		switch rule.ruleType {
		case httpHeaderExactRuleType:
			base = exactHTTPHeaderPredicate.role
		case httpMethodRuleType:
			base = httpMethodPredicate.role
		case httpQueryParamExactRuleType:
			base = exactHTTPQueryParamPredicate.role
		}
		policy.rules = append(policy.rules, observedRouteRule{base: base, flagsObserved: true, rule: l7policies.Rule{
			CompareType: string(rule.compareType), RuleType: string(rule.ruleType), Key: rule.key, Value: rule.value,
			AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		}})
	}
	partGraph := &observedRouteGraph{pool: &pools.Pool{ID: "backend"}, policies: []observedRoutePolicy{policy}}
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, scopedListeners: []listeners.Listener{
		{ID: "affected", ProtocolPort: 80, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		{ID: "unrelated", ProtocolPort: 8080, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
	}, adminObserved: map[string]bool{"affected": true, "unrelated": true},
		routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{"backend": partGraph}}}}

	observe := func(plan ...graphMutation) []graphMutation {
		result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
		steps, _ := p.protectHTTPPredicateTraffic(t.Context(), desired, actual, plan, &result, predicate)
		return steps
	}
	guardedRemoval := graphMutation{kind: graphRouteMutation, withdrawHTTPPolicy: true,
		route: routeMutation{kind: routeMutationDisablePolicy, id: "guarded"}}
	if steps := observe(guardedRemoval); len(steps) != 0 {
		t.Fatalf("temporary discovery failure would withdraw the last verified policy: %#v", steps)
	}
	replyCode = http.StatusOK
	for _, reply := range []string{`{}`, `{"gateway_capabilities":null}`} {
		replyBody = reply
		if steps := observe(guardedRemoval); len(steps) != 0 {
			t.Fatalf("an inconclusive discovery response %s should preserve the installed policy: %#v", reply, steps)
		}
	}

	// A new match needs both capabilities. With just one advertised, its
	// policy write is blocked without disabling the already active listener.
	other := exactHTTPHeaderPredicate.capability
	if predicate.capability == other {
		other = httpMethodPredicate.capability
	}
	replyBody = fmt.Sprintf(`{"gateway_capabilities":[%q]}`, other)
	partGraph.policies = nil
	create := graphMutation{kind: graphRouteMutation, listenerID: "affected", backendKey: part.backendKey,
		route: routeMutation{kind: routeMutationCreatePolicy, policy: wanted}}
	steps := observe(create)
	if len(steps) != 1 || steps[0].kind != graphRouteMutation {
		t.Fatalf("an unavailable capability for a new policy must leave the existing listener active: %#v", steps)
	}
	result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
	if _, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, actual, steps, &result); err != nil || prepared ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("only the new route should wait, got %#v, prepared %t, error %v", result, prepared, err)
	}
	partGraph.policies = []observedRoutePolicy{policy}

	replyBody = `{"gateway_capabilities":[]}`
	if steps := observe(guardedRemoval); len(steps) != 1 || steps[0].kind != graphDisableListener || steps[0].listenerID != "affected" {
		t.Fatalf("confirmed loss should close the affected listener before touching its policy: %#v", steps)
	}
	actual.scopedListeners[0].AdminStateUp = false
	restore := graphMutation{kind: graphEnableListener, listenerID: "affected"}
	unrelated := graphMutation{kind: graphEnableListener, listenerID: "unrelated"}
	if steps := observe(restore, unrelated); len(steps) != 1 || steps[0].listenerID != "unrelated" {
		t.Fatalf("closed affected listener must wait independently for the capability: %#v", steps)
	}
	replyBody = fmt.Sprintf(`{"gateway_capabilities":[%q]}`, predicate.capability)
	if steps := observe(restore); len(steps) != 1 || steps[0].kind != graphEnableListener {
		t.Fatalf("recovery did not allow the verified listener to reopen: %#v", steps)
	}
	partGraph.policies[0].policy.AdminStateUp = false
	if steps := observe(restore); len(steps) != 0 {
		t.Fatalf("a closed listener cannot reopen before its predicate is active: %#v", steps)
	}
	partGraph.policies[0].policy.AdminStateUp = true
	replyBody = `{"gateway_capabilities":[]}`
	desired.spec.Routes[0].Disposition = cloud.GraphDelete
	if steps := observe(guardedRemoval); len(steps) != 1 || steps[0].kind != graphRouteMutation {
		t.Fatalf("owned policy cleanup must continue after the listener is closed: %#v", steps)
	}
	partGraph.policies = nil
	if steps := observe(restore); len(steps) != 1 {
		t.Fatalf("stock listener could not reopen after exact policy cleanup: %#v", steps)
	}
}

func TestExactHTTPHeadersUnavailableDoNotBlockAnIndependentStockPolicy(t *testing.T) {
	calls := 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{}})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{HTTPHeaderExact: true})
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	desired := desiredGatewayGraph{protocol: cloud.ListenerProtocolHTTP, routes: make([]desiredRoute, 3)}
	result := cloud.GatewayGraphResult{Routes: make([]cloud.RouteGraphResult, 3)}
	blocked := graphMutation{kind: graphRouteMutation, listenerID: "header-port",
		route: routeMutation{kind: routeMutationEnablePolicy, requiresExactHeaders: true}}
	samePort := graphMutation{kind: graphRouteMutation, routeIndex: 1, listenerID: "header-port", route: routeMutation{kind: routeMutationEnablePolicy}}
	otherPort := graphMutation{kind: graphRouteMutation, routeIndex: 2, listenerID: "stock-port", route: routeMutation{kind: routeMutationEnablePolicy}}
	step, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, observedGatewayGraph{loadBalancer: lb},
		[]graphMutation{blocked, samePort, otherPort}, &result)
	if err != nil || !prepared || step.listenerID != otherPort.listenerID || calls != 1 {
		t.Fatalf("independent stock policy = %#v, prepared=%t reads=%d err=%v", step, prepared, calls, err)
	}
}

func TestHTTPMethodPolicyUsesRegionalCapabilityAndKeylessPublicRule(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	route.HTTPBackends = []cloud.HTTPBackend{{
		Namespace: "workloads", Name: "backend", Port: 8080, Members: route.Members,
		Matches: []cloud.HTTPPathMatch{
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Method: "GET", Precedence: 0},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Method: "POST", Precedence: 1},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 2},
		},
	}}
	route.Members = nil
	if _, err := (&Provider{projectID: "project-a", fixedHTTPResponses: true}).buildDesiredGatewayGraph(spec); !errors.Is(err, cloud.ErrTerminalValidation) {
		t.Fatalf("missing controller opt-in returned %v", err)
	}

	var advertise bool
	var writes int
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			capabilities := []string{octavia.GatewayCapabilityFixedHTTPResponses}
			if advertise {
				capabilities = append(capabilities, octavia.GatewayCapabilityHTTPMethodMatch)
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case request.Method == http.MethodPost && request.URL.Path == "/lbaas/l7policies/policy/rules":
			writes++
			var body struct {
				Rule struct {
					Type    string   `json:"type"`
					Compare string   `json:"compare_type"`
					Key     *string  `json:"key"`
					Value   string   `json:"value"`
					Invert  bool     `json:"invert"`
					Enabled bool     `json:"admin_state_up"`
					Tags    []string `json:"tags"`
				} `json:"rule"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			rule := body.Rule
			role, tagged := openstackidentity.ResourceRole(rule.Tags)
			base, _, _, parsed := openstackidentity.ParseHTTPFragmentRole(role)
			if rule.Type != "HTTP_METHOD" || rule.Compare != "EQUAL_TO" || rule.Key != nil ||
				rule.Value != "GET" || rule.Invert || !rule.Enabled || !tagged || !parsed || base != openstackidentity.RoleRuleHTTPMethod {
				t.Fatalf("unexpected method predicate: %#v", rule)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"rule":{"id":"method"}}`))
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{HTTPMethodMatch: true, FixedHTTPResponses: true})
	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	var get desiredPolicy
	roles := make(map[string]bool)
	for _, policy := range part.policies {
		roles[policy.role] = true
		if policy.method == "GET" {
			get = policy
		}
	}
	rules := rulesFor(get, "")
	if len(roles) != 3 || get.position != 1 || len(rules) != 1 || rules[0].ruleType != httpMethodRuleType {
		t.Fatalf("unexpected policy identities or predicates: %#v", part.policies)
	}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{loadBalancer: lb}
	mutation := graphMutation{kind: graphRouteMutation, backendKey: part.backendKey}
	for _, step := range []routeMutation{
		{kind: routeMutationCreatePolicy, policy: get},
		{kind: routeMutationCreateRule, parentID: "policy", rule: rules[0]},
		{kind: routeMutationEnablePolicy, requiresHTTPMethod: true},
	} {
		mutation.route = step
		_, err := p.prepareGraphMutation(t.Context(), desired, actual, mutation)
		var missing *octavia.MissingGatewayCapability
		if !errors.As(err, &missing) || missing.Capability != octavia.GatewayCapabilityHTTPMethodMatch {
			t.Fatalf("missing method capability was not reported for %v: %v", step.kind, err)
		}
	}
	advertise = true
	mutation.route = routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: rules[0]}
	prepared, err := p.prepareGraphMutation(t.Context(), desired, actual, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.executeGraphMutation(t.Context(), desired, actual, prepared); err != nil || writes != 1 {
		t.Fatalf("method rule writes = %d, error %v", writes, err)
	}
}

func TestHTTPMethodProfileRootPrefixKeepsAPathIndependentRule(t *testing.T) {
	for _, test := range []struct {
		name    string
		match   cloud.HTTPPathMatch
		grouped bool
		want    int
	}{
		{name: "legacy fallback", match: cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/"}, want: 1},
		{name: "grouped fallback", match: cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/"}, grouped: true, want: 1},
		{name: "hostname and headers", match: cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", Hostname: "example.test",
			Headers: []cloud.HTTPHeaderMatch{{Name: "color", Value: "blue"}}}, grouped: true, want: 3},
		{name: "exact root", match: cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/"}, grouped: true, want: 1},
		{name: "non-root prefix", match: cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop"}, grouped: true, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			stock, stockHost, err := (&Provider{}).httpPathPolicies(test.match, test.grouped)
			if err != nil {
				t.Fatal(err)
			}
			active, host, err := (&Provider{httpMethodMatch: true}).httpPathPolicies(test.match, test.grouped)
			if err != nil {
				t.Fatal(err)
			}
			stockRules, activeRules := rulesFor(stock[0], stockHost), rulesFor(active[0], host)
			if len(activeRules) != test.want || len(stockRules) != test.want ||
				active[0].role != stock[0].role || activeRules[0].role != stockRules[0].role ||
				!slices.Equal(activeRules[1:], stockRules[1:]) {
				t.Fatalf("route or rule identities changed between profiles: stock %#v, active %#v", stockRules, activeRules)
			}
			if test.match.PathType == cloud.PathMatchPrefix && test.match.PathValue == "/" {
				if stockRules[0].compareType != l7policies.CompareTypeStartWith || stockRules[0].invert ||
					activeRules[0].ruleType != l7policies.TypePath || activeRules[0].compareType != l7policies.CompareTypeRegex ||
					activeRules[0].value != "(^$)." || !activeRules[0].invert {
					t.Fatalf("wrong root-prefix predicate: stock %#v, active %#v", stockRules[0], activeRules[0])
				}
			} else if activeRules[0] != stockRules[0] {
				t.Fatalf("non-catch-all path changed between profiles: stock %#v, active %#v", stockRules[0], activeRules[0])
			}
		})
	}
}

func TestGRPCBinaryMetadataSelectsNativeContentTypeOnlyForLists(t *testing.T) {
	provider := NewProvider(ServiceClients{}, ProviderConfig{ResponseHeaderModifier: true})
	for _, scenario := range []struct {
		name  string
		grpc  bool
		value string
		want  l7policies.RuleType
	}{
		{name: "gRPC list", grpc: true, value: "YQ==, Yg", want: grpcContentTypeRuleType},
		{name: "gRPC single value", grpc: true, value: "YQ==", want: l7policies.TypeHeader},
		{name: "HTTP list literal", value: "YQ==, Yg"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			part := desiredRoute{spec: cloud.RouteSpec{GRPC: scenario.grpc}, identity: safetyTestIdentity(t)}
			match := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo",
				ResponseHeaderOperations: []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "Wire-Bin", Value: scenario.value}}}
			if err := provider.buildHTTPMatchPolicies(&part, []cloud.HTTPPathMatch{match}, nil); err != nil {
				t.Fatal(err)
			}
			if len(part.policies) != 1 || part.policies[0].grpcContentTypeRule != scenario.want {
				t.Fatalf("match should use Content-Type rule %q, got %+v", scenario.want, part.policies)
			}
		})
	}
}

func TestGRPCRequestMetadataScopesAndCapabilities(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		grpc    bool
		target  bool
		value   string
		matcher l7policies.RuleType
	}{
		{name: "rule binary list", grpc: true, value: "YQ==, Yg", matcher: grpcContentTypeRuleType},
		{name: "target binary list", grpc: true, target: true, value: "YQ==, Yg", matcher: grpcContentTypeRuleType},
		{name: "target single binary value", grpc: true, target: true, value: "YQ==", matcher: l7policies.TypeHeader},
		{name: "HTTP target literal", target: true, value: "YQ==, Yg"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			identity := safetyTestIdentity(t)
			spec := completeGraphSpec(identity)
			spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "rpc", Port: 80}}
			route := &spec.Routes[0].Spec
			route.GRPC, route.ScopedListenerPorts = scenario.grpc, []int{80}
			match := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo"}
			route.Hostname, route.PathType, route.PathValue = "", "", ""
			route.HTTPBackends = []cloud.HTTPBackend{{Namespace: "application", Name: "echo", Port: 8080,
				Members: route.Members, BackendH2C: scenario.grpc}}
			route.Members = nil
			ops := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderAdd, Name: "request-bin", Value: scenario.value}}
			underlying := octavia.GatewayCapabilityRequestHeaderOperations
			if scenario.target {
				underlying = octavia.GatewayCapabilityTargetRequestFilters
				route.WeightedHTTPRules = []cloud.HTTPWeightedRule{{
					Matches: []cloud.HTTPPathMatch{match},
					Targets: []cloud.HTTPWeightedTarget{{BackendIndex: new(0), Weight: 1, RequestHeaderOperations: ops}},
				}}
			} else {
				match.RequestHeaderOperations = ops
				route.HTTPBackends[0].Matches = []cloud.HTTPPathMatch{match}
			}
			cfg := ProviderConfig{BackendH2C: scenario.grpc, FixedHTTPResponses: true, WeightedHTTPTargets: scenario.target,
				BackendRequestFilters: scenario.target, RequestHeaderModifier: !scenario.target}
			var advertised []string
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
			}))
			provider := NewProvider(ServiceClients{ProjectID: "project-a", LoadBalancer: client}, cfg)
			desired, err := provider.buildDesiredGatewayGraph(spec)
			if err != nil {
				t.Fatal(err)
			}
			var part desiredRoute
			for _, candidate := range desired.routes[0].httpParts {
				if len(candidate.policies) != 0 {
					part = candidate
					break
				}
			}
			if len(part.policies) != 1 || part.policies[0].grpcContentTypeRule != scenario.matcher {
				t.Fatalf("want one policy using the %q matcher, got %#v", scenario.matcher, part.policies)
			}
			policy := part.policies[0]
			if scenario.target {
				if len(policy.requestHeaderOperations) != 0 || len(policy.targetRequestFilters) != 1 ||
					!slices.Equal(policy.targetRequestFilters[0].RequestHeaderOperations, ops) {
					t.Fatalf("target operations were moved to another scope: %#v", policy)
				}
			} else if !slices.Equal(policy.requestHeaderOperations, ops) || len(policy.targetRequestFilters) != 0 {
				t.Fatalf("rule operations were moved to another scope: %#v", policy)
			}

			lb := safetyObservedLoadBalancer()
			lb.Provider = "amphora"
			actual := observedGatewayGraph{loadBalancer: lb}
			mutation := routeMutation{kind: routeMutationEnablePolicy, policy: policy, requestHeaderOperations: policy.requestHeaderOperations}
			updateKind := routeMutationSetPolicyHeaders
			if scenario.target {
				updateKind = routeMutationSetPolicyTarget
			}
			for _, kind := range []routeMutationKind{routeMutationCreatePolicy, updateKind, routeMutationEnablePolicy} {
				mutation.kind = kind
				if kind != routeMutationEnablePolicy && scenario.target {
					mutation.weightedTargets = &policy.targetRequestFilters
				} else {
					mutation.weightedTargets = nil
				}
				step := graphMutation{kind: graphRouteMutation, backendKey: part.backendKey, route: mutation}
				checkMissing := func(capability string) {
					t.Helper()
					var unavailable *unavailableRouteCapability
					if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, step); !errors.As(err, &unavailable) || unavailable.capability != capability {
						t.Fatalf("expected %s to gate %v, got %v", capability, kind, err)
					}
				}
				advertised = []string{octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses}
				checkMissing(underlying)
				advertised = append(advertised, underlying)
				if scenario.matcher == grpcContentTypeRuleType {
					checkMissing(octavia.GatewayCapabilityGRPCBinaryRequestMetadata)
					advertised = append(advertised, octavia.GatewayCapabilityGRPCBinaryRequestMetadata)
					checkMissing(octavia.GatewayCapabilityGRPCContentType)
					advertised = append(advertised, octavia.GatewayCapabilityGRPCContentType)
				}
				if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, step); err != nil {
					t.Fatalf("unexpected capability requirement for %v: %v", kind, err)
				}
			}
		})
	}
}

func TestGRPCBinaryRequestMigrationKeepsFallbackOffTheWire(t *testing.T) {
	for _, scope := range []string{"rule", "target"} {
		t.Run(scope, func(t *testing.T) {
			spec := completeGraphSpec(safetyTestIdentity(t))
			spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Port: 80}, {Port: 8080}}
			main, observation := convergedRoutePlanFixture()
			main.backendKey, main.spec.GRPC, main.spec.BackendH2C = "backend", true, true
			main.spec.ScopedListenerPorts = []int{80}
			main.healthMonitor, observation.monitor, observation.backendH2C = cloud.HealthMonitorConfig{}, nil, true
			observation.pool.ProvisioningStatus, observation.members[0].ProvisioningStatus = "ACTIVE", "ACTIVE"
			legacy := &observation.policies[0]
			legacy.policy.ListenerID, legacy.policy.ProvisioningStatus, legacy.adminObserved = "rpc", "ACTIVE", true
			legacy.rules[0].flagsObserved, legacy.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
			legacy.rules = append(legacy.rules, observedRouteRule{
				role: openstackidentity.RoleRuleGRPCContentType, base: openstackidentity.RoleRuleGRPCContentType, flagsObserved: true,
				rule: l7policies.Rule{ID: "media", RuleType: string(l7policies.TypeHeader), Key: "content-type",
					CompareType: string(l7policies.CompareTypeRegex), Value: portableGRPCContentTypePattern, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			})
			fallback, fallbackGraph := convergedRoutePlanFixture()
			fallback.backendKey, fallback.spec.ScopedListenerPorts = "fallback", []int{80}
			fallback.policies[0].path, fallback.policies[0].compareType, fallback.policies[0].position = "/", l7policies.CompareTypeStartWith, 2
			fallbackGraph.pool.ID = "fallback-pool"
			fallbackPolicy := &fallbackGraph.policies[0]
			fallbackPolicy.policy.ID, fallbackPolicy.policy.ListenerID = "fallback-policy", "rpc"
			fallbackPolicy.policy.RedirectPoolID, fallbackPolicy.policy.Position = fallbackGraph.pool.ID, 2
			fallbackPolicy.policy.ProvisioningStatus, fallbackPolicy.adminObserved = "ACTIVE", true
			fallbackPolicy.rules[0].rule.Value, fallbackPolicy.rules[0].rule.CompareType = "/", string(l7policies.CompareTypeStartWith)
			fallbackPolicy.rules[0].flagsObserved, fallbackPolicy.rules[0].rule.ProvisioningStatus = true, "ACTIVE"

			lb := safetyObservedLoadBalancer()
			lb.Provider, lb.AdminStateUp, lb.ProvisioningStatus, lb.VipAddress = "amphora", true, "ACTIVE", "192.0.2.10"
			actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, scoped: true,
				scopedListeners: []listeners.Listener{
					{ID: "rpc", ProtocolPort: 80, Protocol: cloud.ListenerProtocolHTTP, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
					{ID: "independent", ProtocolPort: 8080, Protocol: cloud.ListenerProtocolHTTP, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
				}, adminObserved: map[string]bool{"rpc": true, "independent": true},
				routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{"backend": &observation, "fallback": &fallbackGraph}}}}
			desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{spec: cloud.RouteSpec{GRPC: true}, httpParts: []desiredRoute{main, fallback}}}}
			wanted := &desired.routes[0].httpParts[0].policies[0]
			path := wanted.path
			ops := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "trace-bin", Value: "YQ==,Yg=="}}
			wanted.grpcContentTypeRule = grpcContentTypeRuleType
			advertised := []string{octavia.GatewayCapabilityGRPCContentType}
			if scope == "target" {
				// A target operation affects the weighted action only. Keep
				// the forwarding pools observed so the match is ready to stage.
				backend := desired.routes[0].httpParts[0]
				backend.backendKey, backend.policies = "target", nil
				targetPool := observation
				targetPool.policies = nil
				weighted := &desired.routes[0].httpParts[0]
				weighted.weightedTargets = []desiredHTTPWeightedTarget{{backendKey: "target", weight: 1, requestHeaderOperations: ops}}
				wanted.weighted = true
				wanted.targetRequestFilters = []octavia.WeightedHTTPTarget{{RequestHeaderOperations: ops}}
				legacy.policy.Action, legacy.policy.RedirectPoolID = string(octavia.ActionWeightedTargets), ""
				legacy.weightedTargets = []octavia.WeightedHTTPTarget{{Weight: 1, PoolID: targetPool.pool.ID}}
				observation.pool, observation.members = nil, nil
				actual.routes[0].httpParts["target"] = &targetPool
				failure, empty := convergedRoutePlanFixture()
				failure.backendKey, failure.unavailable = grpcFailurePoolKey, true
				failure.members, failure.policies, failure.healthMonitor = nil, nil, cloud.HealthMonitorConfig{}
				empty.members, empty.policies, empty.monitor = nil, nil, nil
				empty.pool.ID, empty.pool.ProvisioningStatus = "unavailable", "ACTIVE"
				actual.routes[0].httpParts[grpcFailurePoolKey] = &empty
				desired.routes[0].httpParts = append(desired.routes[0].httpParts, backend, failure)
				advertised = append(advertised, octavia.GatewayCapabilityTargetRequestFilters,
					octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses)
			} else {
				wanted.requestHeaderOperations = ops
				advertised = append(advertised, octavia.GatewayCapabilityRequestHeaderOperations)
			}
			probes := 0
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				probes++
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
			}))
			provider := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{RequestHeaderModifier: scope == "rule", BackendRequestFilters: scope == "target"})
			raw, _, err := provider.buildGatewayGraphPlan(desired, actual)
			if err != nil || !slices.ContainsFunc(raw, func(step graphMutation) bool {
				return step.kind == graphRouteMutation && step.route.kind == routeMutationDisablePolicy && step.route.id == legacy.policy.ID
			}) {
				t.Fatalf("the legacy graph never attempted to disable its winning policy: %v %+v", err, raw)
			}
			guard := func(plan []graphMutation) []graphMutation {
				t.Helper()
				result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
				guarded, _ := provider.protectHTTPPredicateTraffic(t.Context(), desired, actual, plan, &result, grpcBinaryRequestMetadataPolicy)
				return guarded
			}
			reopen := []graphMutation{{kind: graphEnableListener, listenerID: "rpc"}, {kind: graphEnableListener, listenerID: "independent"}}
			for _, available := range []bool{false, true} {
				if available {
					advertised = append(advertised, octavia.GatewayCapabilityGRPCBinaryRequestMetadata)
				}
				before := probes
				plan := guard(slices.Clone(raw))
				if probes <= before || len(plan) != 1 || plan[0].kind != graphDisableListener || plan[0].listenerID != "rpc" ||
					!legacy.policy.AdminStateUp || !fallbackPolicy.policy.AdminStateUp || !available && !strings.Contains(plan[0].message, octavia.GatewayCapabilityGRPCBinaryRequestMetadata) {
					t.Fatalf("only a verified physical closure can protect %s from the fallback (available=%v): %#v", path, available, plan)
				}
				actual.scopedListeners[0].AdminStateUp = false
				plan = guard(slices.Clone(raw))
				canStage := slices.ContainsFunc(plan, func(step graphMutation) bool {
					return step.kind == graphRouteMutation && step.route.kind == routeMutationDisablePolicy && step.route.id == legacy.policy.ID
				})
				if canStage != available {
					t.Fatalf("the winning policy can change only after capability discovery (available=%v): %#v", available, plan)
				}
				plan = guard(slices.Clone(reopen))
				if len(plan) != 1 || plan[0].listenerID != "independent" {
					t.Fatalf("keep only the affected listener closed while its priority is unsettled: %#v", plan)
				}
				actual.scopedListeners[0].AdminStateUp = true
			}

			actual.scopedListeners[0].AdminStateUp = false
			media := &legacy.rules[1].rule
			media.RuleType, media.Key, media.CompareType, media.Value = string(grpcContentTypeRuleType), "", string(l7policies.CompareTypeEqual), "application/grpc"
			if scope == "target" {
				legacy.weightedTargets[0].RequestHeaderOperations = ops
			} else {
				legacy.requestHeaderOperations = ops
			}
			plan := guard(slices.Clone(reopen))
			if len(plan) != 2 {
				t.Fatalf("verified active priority must permit restoring traffic: %#v", plan)
			}

			// Removing the list remains possible even if enrollment is lost
			// again after the new policy activated.
			advertised = advertised[:len(advertised)-1]
			wanted = &desired.routes[0].httpParts[0].policies[0]
			wanted.requestHeaderOperations, wanted.targetRequestFilters = nil, nil
			cleanup := graphMutation{kind: graphRouteMutation, listenerID: "rpc", withdrawHTTPPolicy: true, backendKey: "backend",
				route: routeMutation{kind: routeMutationDisablePolicy, id: legacy.policy.ID, withdrawHTTPPolicy: true}}
			if plan := guard([]graphMutation{cleanup}); len(plan) != 1 || plan[0].route.kind != routeMutationDisablePolicy {
				t.Fatalf("capability withdrawal must not prevent owned policy cleanup on the closed listener: %#v", plan)
			}
		})
	}
}

func TestGRPCTargetBinaryRequestWithdrawalUsesObservedMatcher(t *testing.T) {
	part, observation := convergedRoutePlanFixture()
	part.backendKey, part.spec.GRPC, part.grpcFailurePoolID = "backend", true, "unavailable"
	part.spec.ScopedListenerPorts = []int{80}
	policy := &observation.policies[0]
	policy.policy.ListenerID, policy.policy.AdminStateUp, policy.adminObserved = "affected", true, true
	policy.backendRequestFiltersTagged = true
	policy.rules = append(policy.rules, observedRouteRule{base: openstackidentity.RoleRuleGRPCContentType,
		rule: l7policies.Rule{RuleType: string(grpcContentTypeRuleType)}})
	ops := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "request-bin", Value: "YQ==, Yg"}}
	policy.weightedTargets = []octavia.WeightedHTTPTarget{
		{PoolID: "local", Weight: 75, RequestHeaderOperations: ops},
		{PoolID: "remote", Weight: 25, RequestHeaderOperations: ops},
	}
	part.policies[0].targetRequestFilters = []octavia.WeightedHTTPTarget{{RequestHeaderOperations: ops}, {}}
	part.policies[0].grpcContentTypeRule = grpcContentTypeRuleType
	part.weightedAuthorizations = map[string]map[string]int{policy.policy.ID: {"local": 75}}
	steps := weightedTargetWithdrawals(part, observation)
	if len(steps) != 1 || !steps[0].withdrawHTTPPolicy || steps[0].weightedTargets == nil ||
		!steps[0].requiresGRPCBinaryRequestMetadata() || !slices.EqualFunc(*steps[0].weightedTargets, []octavia.WeightedHTTPTarget{
		{PoolID: "local", Weight: 75, RequestHeaderOperations: ops}, {PoolID: "unavailable", Weight: 25},
	}, weightedTargetsEqual) {
		t.Fatalf("target grant withdrawal did not preserve the other scope and its capability gate: %#v", steps)
	}

	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Port: 80}, {Port: 8080}}
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{httpParts: []desiredRoute{part}}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, scopedListeners: []listeners.Listener{
		{ID: "affected", ProtocolPort: 80, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		{ID: "unrelated", ProtocolPort: 8080, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
	}, adminObserved: map[string]bool{"affected": true, "unrelated": true},
		routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{"backend": &observation}}}}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityGRPCContentType}})
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{})
	result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
	plan, _ := provider.protectHTTPPredicateTraffic(t.Context(), desired, actual, nil, &result, grpcBinaryRequestMetadataPolicy)
	if len(plan) != 1 || plan[0].kind != graphDisableListener || plan[0].listenerID != "affected" ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("the restarted controller did not withdraw only the affected listener: %#v, %#v", plan, result)
	}

	part.policies[0].targetRequestFilters = nil
	steps = weightedTargetWithdrawals(part, observation)
	if len(steps) != 1 || steps[0].kind != routeMutationDisablePolicy || !steps[0].withdrawHTTPPolicy {
		t.Fatalf("removing the last target filter must retire the tagged policy: %#v", steps)
	}
}

func TestResponseHeaderPolicyActivationAndWithdrawalAcrossRestart(t *testing.T) {
	t.Run("HTTP headers", func(t *testing.T) { testResponseHeaderPolicyActivationAndWithdrawal(t, false) })
	t.Run("gRPC binary list", func(t *testing.T) { testResponseHeaderPolicyActivationAndWithdrawal(t, true) })
}

func testResponseHeaderPolicyActivationAndWithdrawal(t *testing.T, grpcBinary bool) {
	t.Helper()
	part, observation := convergedRoutePlanFixture()
	part.backendKey = "backend"
	part.spec.ScopedListenerPorts = []int{80}
	wanted := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}
	policy := &observation.policies[0]
	policy.policy.ListenerID, policy.policy.ProvisioningStatus = "affected", "ACTIVE"
	policy.policy.AdminStateUp, policy.adminObserved = false, true
	policy.rules[0].rule.ProvisioningStatus, policy.rules[0].flagsObserved = "ACTIVE", true
	predicate := responseHeaderModifierPolicy
	if grpcBinary {
		predicate = grpcBinaryResponseMetadataPolicy
		wanted = []cloud.HTTPHeaderOperation{
			{Operation: cloud.HTTPHeaderSet, Name: "wire-bin", Value: "YQ==, Yg"},
			{Operation: cloud.HTTPHeaderAdd, Name: "more-bin", Value: "Yw, ZA=="},
		}
		part.policies[0].grpcContentTypeRule = grpcContentTypeRuleType
		policy.rules = append(policy.rules, observedRouteRule{
			role: openstackidentity.RoleRuleGRPCContentType, base: openstackidentity.RoleRuleGRPCContentType,
			flagsObserved: true,
			rule: l7policies.Rule{ID: "media-rule", RuleType: string(grpcContentTypeRuleType),
				CompareType: string(l7policies.CompareTypeEqual), Value: "application/grpc", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		})
	}
	part.policies[0].responseHeaderOperations = wanted

	spec := completeGraphSpec(safetyTestIdentity(t))
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{{httpParts: []desiredRoute{part}}}}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, scopedListeners: []listeners.Listener{
		{ID: "affected", ProtocolPort: 80, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		{ID: "unrelated", ProtocolPort: 8080, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
	}, adminObserved: map[string]bool{"affected": true, "unrelated": true},
		routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{"backend": &observation}}}}

	advertise := false
	probes, writes := 0, 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			probes++
			capabilities := []string{}
			if grpcBinary {
				capabilities = append(capabilities, octavia.GatewayCapabilityResponseHeaderOperations, octavia.GatewayCapabilityGRPCContentType)
			}
			if advertise {
				capabilities = append(capabilities, predicate.capability)
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/l7policies/policy-1":
			writes++
			var body struct {
				Policy map[string]json.RawMessage `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.Policy) != 1 || string(body.Policy["response_header_operations"]) != "[]" {
				t.Fatalf("withdrawal did not send an independent clear: %s", body.Policy)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": "policy-1"}})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{ResponseHeaderModifier: true})
	next := func(want routeMutationKind) graphMutation {
		t.Helper()
		steps, _, err := buildRouteEnsurePlan(part, observation)
		if err != nil || len(steps) < 1 || steps[0].kind != want {
			t.Fatalf("expected %v first, got %+v, error %v", want, steps, err)
		}
		return graphMutation{kind: graphRouteMutation, listenerID: "affected", backendKey: "backend", route: steps[0]}
	}
	noReadback := next(routeMutationSetPolicyHeaders)
	_, err := provider.prepareGraphMutation(t.Context(), desired, actual, noReadback)
	var unavailable *unavailableRouteCapability
	if !errors.As(err, &unavailable) || unavailable.capability != predicate.capability || writes != 0 {
		t.Fatalf("plan wrote without response capability: %v, %d", err, writes)
	}
	policy.responseHeaderOperations = wanted
	enable := next(routeMutationEnablePolicy)
	if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, enable); !errors.As(err, &unavailable) {
		t.Fatalf("activation must verify capability after policy readback: %v", err)
	}
	if grpcBinary {
		// An older provider still accepts a single binary value on a gRPC
		// policy and the same comma-containing literal on an HTTP policy.
		simple := noReadback
		simple.route.responseHeaderOperations = []cloud.HTTPHeaderOperation{wanted[0]}
		simple.route.responseHeaderOperations[0].Value = "YQ=="
		if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, simple); err != nil {
			t.Fatalf("older provider rejected a single binary value: %v", err)
		}
		ordinary := noReadback
		ordinary.route.policy.grpcContentTypeRule = ""
		if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, ordinary); err != nil {
			t.Fatalf("older provider rejected an ordinary HTTP operation: %v", err)
		}
	}
	advertise = true
	if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, enable); err != nil {
		t.Fatalf("advertised capability did not permit activation: %v", err)
	}

	// The replacement controller starts with API observations and no process state.
	policy.policy.AdminStateUp = true
	advertise = false
	provider = NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{ResponseHeaderModifier: true})
	result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
	steps, _ := provider.protectHTTPPredicateTraffic(t.Context(), desired, actual, nil, &result, predicate)
	if len(steps) != 1 || steps[0].kind != graphDisableListener || steps[0].listenerID != "affected" ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("a restarted controller did not withdraw the affected listener: %+v, %+v", steps, result)
	}
	actual.scopedListeners[0].AdminStateUp = false
	part.policies[0].responseHeaderOperations = nil
	provider = NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{})
	clear := next(routeMutationSetPolicyHeaders)
	steps, _ = provider.protectHTTPPredicateTraffic(t.Context(), desired, actual, []graphMutation{clear}, &result, predicate)
	if len(steps) != 1 {
		t.Fatalf("clear was lost while capability was unavailable: %+v", steps)
	}
	prepared, err := provider.prepareGraphMutation(t.Context(), desired, actual, steps[0])
	if err != nil {
		t.Fatal(err)
	}
	wantProbes := 5
	if grpcBinary {
		wantProbes += 2
	}
	if err := provider.executeRouteMutation(t.Context(), part, prepared.route); err != nil || writes != 1 || probes != wantProbes {
		t.Fatalf("withdrawal failed without capability and opt-in: writes=%d probes=%d error=%v", writes, probes, err)
	}
	policy.responseHeaderOperations = []cloud.HTTPHeaderOperation{}
	if plan, _, err := buildRouteEnsurePlan(part, observation); err != nil || len(plan) != 0 {
		t.Fatalf("a verified clear did not converge: %+v, %v", plan, err)
	}
}

// Exercise controller restarts against the same Octavia inventory.
func TestRegionalGRPCContentTypePolicyTransition(t *testing.T) {
	t.Run("with metadata", func(t *testing.T) { testRegionalGRPCContentTypePolicyTransition(t, true) })
	t.Run("without metadata", func(t *testing.T) { testRegionalGRPCContentTypePolicyTransition(t, false) })
}

func testRegionalGRPCContentTypePolicyTransition(t *testing.T, metadata bool) {
	t.Helper()
	type record = map[string]any
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "grpc", Port: 80}, {Name: "http", Port: 8080}}
	route := &spec.Routes[0].Spec
	route.ScopedListenerPorts = []int{80}
	match := cloud.HTTPPathMatch{Hostname: route.Hostname, PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 1}
	if metadata {
		match.Headers = []cloud.HTTPHeaderMatch{{Name: "x-version", Value: "v2"}}
	}
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	route.GRPC = true
	route.HTTPBackends = []cloud.HTTPBackend{{Namespace: "application", Name: "echo", Port: 8080,
		Matches: []cloud.HTTPPathMatch{match}, Members: route.Members, BackendH2C: true}}
	route.Members = nil
	for _, port := range []int{80, 8080} {
		id := identity.Value()
		id.RouteName = fmt.Sprintf("http-%d", port)
		id.RouteUID = id.RouteName
		spec.Routes = append(spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.RouteSpec{
			Identity: id, GatewayRequirements: route.GatewayRequirements, ScopedListenerPorts: []int{port},
			MemberSubnetID: route.MemberSubnetID, HealthMonitor: route.HealthMonitor,
			FixedHTTPRules: []cloud.HTTPFixedRule{{Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/http"}}}},
		}})
	}
	cfg := ProviderConfig{BackendH2C: true, FixedHTTPResponses: true, HTTPHeaderExact: metadata}
	initial := NewProvider(ServiceClients{ProjectID: "project-a"}, cfg)
	desired, err := initial.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	wanted := part.policies[0]
	policyRefs, rules := []record{}, []record{}
	var mediaRule record
	for _, predicate := range rulesFor(wanted, "") {
		rule := record{"id": string(predicate.ruleType), "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
			"type": predicate.ruleType, "compare_type": predicate.compareType, "key": predicate.key, "value": predicate.value,
			"invert": predicate.invert, "tags": safetyRouteTags(t, identity, predicate.role)}
		if predicate.ruleType == l7policies.TypeHeader {
			rule["id"] = "media-rule"
			mediaRule = rule
		}
		policyRefs = append(policyRefs, record{"id": rule["id"]})
		rules = append(rules, rule)
	}
	if len(rules) != 3+len(match.Headers) || mediaRule == nil || mediaRule["key"] != "content-type" {
		t.Fatalf("portable policy must include path, hostname, Content-Type and any metadata: %v", rules)
	}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "ACTIVE")
	grpcListeners := []record{{"id": "listener-1"}}
	lb["listeners"], lb["pools"] = []record{{"id": "listener-1"}, {"id": "http-listener"}}, []record{{"id": "pool-1"}}
	listener := record{"id": "listener-1", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "HTTP", "protocol_port": 80, "default_pool_id": nil, "default_http_response": record{"status_code": 404},
		"loadbalancers": []record{{"id": lb["id"]}}, "l7policies": []record{{"id": "policy"}}, "tags": safetyGatewayTags(t, identity, roleListener)}
	httpListener := record{"id": "http-listener", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "HTTP", "protocol_port": 8080, "default_pool_id": nil, "default_http_response": record{"status_code": 404},
		"loadbalancers": listener["loadbalancers"], "l7policies": []record{},
		"tags": append(safetyGatewayTags(t, identity, roleListener), scopedPortTag(cloud.ListenerProtocolHTTP, 8080))}
	pool := record{"id": "pool-1", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "HTTP", "backend_h2c": true, "lb_algorithm": "ROUND_ROBIN", "healthmonitor_id": "monitor",
		"loadbalancers": listener["loadbalancers"], "listeners": grpcListeners, "members": []record{{"id": "member"}},
		"tags": safetyRouteTags(t, identity, part.httpRole(rolePool, ""))}
	member := record{"id": "member", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"address": "10.0.0.10", "protocol_port": 30080, "subnet_id": "member-subnet", "tags": safetyRouteTags(t, identity, part.httpRole(roleMember, ""))}
	monitor := record{"id": "monitor", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"type": "TCP", "delay": 10, "timeout": 5, "max_retries": 3, "max_retries_down": 3, "pools": lb["pools"],
		"tags": safetyRouteTags(t, identity, part.httpRole(roleMonitor, ""))}
	policy := record{"id": "policy", "project_id": "project-a", "listener_id": "listener-1", "action": "REDIRECT_TO_POOL",
		"redirect_pool_id": "pool-1", "position": wanted.position, "admin_state_up": true, "provisioning_status": "ACTIVE",
		"rules": policyRefs, "tags": safetyRouteTags(t, identity, wanted.role)}
	const capabilityPath = "/lbaas/providers/amphora/gateway_capabilities"
	const policyPath = "/lbaas/l7policies/policy"
	const rulesPath = policyPath + "/rules"
	const rulePath = rulesPath + "/media-rule"
	const listenerPath = "/lbaas/listeners/listener-1"
	reads := map[string]record{
		"/lbaas/loadbalancers": {"loadbalancers": []record{lb}}, "/lbaas/loadbalancers/load-balancer-1": {"loadbalancer": lb},
		"/lbaas/listeners": {"listeners": []record{listener, httpListener}}, "/lbaas/pools": {"pools": []record{pool}},
		"/lbaas/healthmonitors": {"healthmonitors": []record{monitor}}, "/lbaas/pools/pool-1/members": {"members": []record{member}},
	}
	// Converged ordinary HTTP routes on the same and on a different port.
	httpPolicies := make(map[string][]record)
	for i, frontend := range []record{listener, httpListener} {
		sibling := desired.routes[i+1].httpParts[0]
		predicate := sibling.policies[0]
		id := sibling.spec.Identity.RouteName
		listenerID := frontend["id"].(string)
		rule := rulesFor(predicate, "")[0]
		httpPolicies[listenerID] = []record{{
			"id": id, "project_id": "project-a", "listener_id": listenerID, "action": "FIXED_RESPONSE", "redirect_pool_id": nil,
			"fixed_response": record{"status_code": 500}, "position": predicate.position, "admin_state_up": true,
			"provisioning_status": "ACTIVE", "rules": []record{{"id": id + "-path"}},
			"tags": safetyRouteTags(t, sibling.identity, predicate.role),
		}}
		reads["/lbaas/l7policies/"+id+"/rules"] = record{"rules": []record{{
			"id": id + "-path", "project_id": "project-a", "type": rule.ruleType, "compare_type": rule.compareType,
			"value": rule.value, "admin_state_up": true, "invert": false, "provisioning_status": "ACTIVE", "tags": safetyRouteTags(t, sibling.identity, rule.role),
		}}}
		frontend["l7policies"] = append(frontend["l7policies"].([]record), record{"id": id})
	}
	var advertised, discoveryOutage, networkOutage bool
	present := true
	var probes int
	var writes []string
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		path := request.URL.Path
		if request.Method == http.MethodGet {
			if path == rulesPath {
				safetyWriteJSON(t, w, record{"rules": rules})
				return
			}
			if path == capabilityPath {
				probes++
				if discoveryOutage {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				capabilities := []string{octavia.GatewayCapabilityFixedHTTPResponses, octavia.GatewayCapabilityHTTPHeaderExact, octavia.GatewayCapabilityBackendH2C}
				if advertised {
					capabilities = append(capabilities, octavia.GatewayCapabilityGRPCContentType)
				}
				safetyWriteJSON(t, w, record{"gateway_capabilities": capabilities})
				return
			}
			if path == "/lbaas/l7policies" {
				listenerID := request.URL.Query().Get("listener_id")
				policies := slices.Clone(httpPolicies[listenerID])
				if present && listenerID == "listener-1" {
					policies = append(policies, policy)
				}
				safetyWriteJSON(t, w, record{"l7policies": policies})
				return
			}
			if body, ok := reads[path]; ok {
				safetyWriteJSON(t, w, body)
				return
			}
		}
		if request.Method == http.MethodDelete && path == rulePath {
			if policy["admin_state_up"] != false || policy["provisioning_status"] != "ACTIVE" || mediaRule["type"] != "GRPC_CONTENT_TYPE" {
				t.Fatalf("unexpected native rule deletion with policy %v and rule %v", policy, mediaRule)
			}
			writes = append(writes, path)
			removeMedia := func(r record) bool { return r["id"] == mediaRule["id"] }
			policyRefs = slices.DeleteFunc(policyRefs, removeMedia)
			rules = slices.DeleteFunc(rules, removeMedia)
			policy["rules"] = policyRefs
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if request.Method != http.MethodPut && (path != rulesPath || request.Method != http.MethodPost) {
			t.Fatalf("unexpected %s %s", request.Method, path)
		}
		writes = append(writes, path)
		var patch map[string]record
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Fatal(err)
		}
		var resource record
		var kind string
		switch path {
		case policyPath, listenerPath:
			resource, kind = policy, "l7policy"
			if path == listenerPath {
				resource, kind = listener, "listener"
			}
			if len(patch[kind]) != 1 || patch[kind]["admin_state_up"] == nil {
				t.Fatalf("expected only an administrative state change on %s: %v", kind, patch)
			}
		case rulePath:
			resource, kind = mediaRule, "rule"
			rule := patch[kind]
			if !advertised {
				w.WriteHeader(http.StatusNotImplemented)
				return
			}
			if policy["admin_state_up"] != false || rule["invert"] != false || rule["admin_state_up"] != true || rule["tags"] != nil {
				t.Fatalf("unexpected Content-Type update: %v with policy state %v", patch, policy["admin_state_up"])
			}
			if _, keyed := rule["key"]; rule["type"] != "GRPC_CONTENT_TYPE" || rule["compare_type"] != "EQUAL_TO" ||
				rule["value"] != "application/grpc" || keyed {
				t.Fatalf("invalid native predicate: %v", rule)
			}
			delete(resource, "key")
		case rulesPath:
			rule := patch["rule"]
			if len(rules) != 2+len(match.Headers) || policy["admin_state_up"] != false || policy["provisioning_status"] != "ACTIVE" ||
				rule["type"] != "HEADER" || rule["compare_type"] != "REGEX" || rule["key"] != "content-type" ||
				rule["value"] != portableGRPCContentTypePattern || rule["admin_state_up"] != true || rule["invert"] == true {
				t.Fatalf("invalid portable rule creation: %v, policy: %v", rule, policy)
			}
			rule["id"], rule["project_id"], rule["provisioning_status"] = "portable-media-rule", "project-a", "ACTIVE"
			rule["invert"] = false
			rules = append(rules, rule)
			policyRefs = append(policyRefs, record{"id": rule["id"]})
			policy["rules"] = policyRefs
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, record{"rule": rule})
			return
		default:
			t.Fatalf("unexpected mutation %s", path)
		}
		for key, value := range patch[kind] {
			resource[key] = value
		}
		if path == policyPath {
			pool["listeners"] = []record{}
			if policy["admin_state_up"] == true {
				pool["listeners"] = grpcListeners
			}
			policy["provisioning_status"] = "ACTIVE"
		}
		safetyWriteJSON(t, w, record{kind: resource})
	}))

	network := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/floatingips" {
			t.Fatalf("unexpected Neutron request %s %s", request.Method, request.URL.Path)
		}
		if networkOutage {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		safetyWriteJSON(t, w, record{"floatingips": []record{}})
	}))
	run := func() (cloud.GatewayGraphResult, error) {
		writes = nil
		p := newTestProvider(t, ServiceClients{LoadBalancer: client, Network: network, ProjectID: "project-a", Region: "regional-endpoint"}, cfg)
		return p.ReconcileGatewayGraph(t.Context(), spec)
	}
	reconcile := func(write string) cloud.GatewayGraphResult {
		t.Helper()
		result, err := run()
		var want []string
		if write != "" {
			want = []string{write}
		}
		if err != nil || !slices.Equal(writes, want) {
			t.Fatalf("expected mutation %q, got %v with error %v and result %+v", write, writes, err, result)
		}
		return result
	}
	if result := reconcile(""); result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("default portable policy did not converge: %+v", result)
	}
	cfg.GRPCContentType = true
	discoveryOutage = true
	probes = 0
	if result := reconcile(""); probes == 0 || policy["admin_state_up"] != true || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("indeterminate discovery must preserve the verified portable policy: %+v", result)
	}
	discoveryOutage = false
	reconcile(policyPath)
	if result := reconcile(""); policy["admin_state_up"] != false ||
		result.Routes[0].Result.Outcome.Message != "Waiting for Octavia to advertise grpc_content_type_v1" {
		t.Fatalf("missing native capability must leave the old policy closed and report the prerequisite: %+v", result)
	}
	advertised = true
	reconcile(rulePath)
	advertised = false
	if result := reconcile(""); policy["admin_state_up"] != false || listener["admin_state_up"] != true ||
		result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("a staged native rule must wait for enrollment without closing the serving listener: %+v", result)
	}
	advertised = true
	reconcile(policyPath)
	if result := reconcile(""); result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("native policy did not converge: %+v", result)
	}
	advertised = false
	networkOutage, discoveryOutage = true, true
	if _, err := run(); err == nil || !strings.Contains(err.Error(), "discover Gateway Floating IPs") || len(writes) != 0 {
		t.Fatalf("address and discovery failures must leave the verified listener serving: writes %v, error %v", writes, err)
	}
	discoveryOutage = false
	fence := errors.New("Kubernetes snapshot changed")
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := run(); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("listener closure must revalidate after discovery: writes %v, error %v", writes, err)
	}
	spec.ValidateMutation = nil
	assertRevoked := func(result cloud.GatewayGraphResult) {
		t.Helper()
		if listener["admin_state_up"] != false || listener["provisioning_status"] != "ACTIVE" ||
			result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || result.Routes[0].Result.ObservationDeferred ||
			result.Routes[1].Result.Outcome.State != cloud.OutcomeProgressing || result.Routes[1].Result.ObservationDeferred ||
			result.Routes[2].Result.Outcome.State != cloud.OutcomeReady || !slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{8080}) {
			t.Fatalf("revocation must close only the affected listener and mark all its routes progressing: %+v", result)
		}
	}
	assertRevoked(reconcile(listenerPath))
	for range 2 {
		result := reconcile("")
		assertRevoked(result)
		if message := result.Routes[0].Result.Outcome.Message; message != "Octavia withdrew the native gRPC Content-Type capability on this listener" {
			t.Fatalf("confirmed revocation must reach the route status during the Floating IP outage: %s", message)
		}
	}
	networkOutage = false
	assertRevoked(reconcile(""))
	cfg.GRPCContentType = false
	reconcile(policyPath)
	reconcile(rulePath)
	reconcile(rulesPath)
	reconcile(policyPath)
	reconcile(listenerPath)
	if result := reconcile(""); result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("portable rollback did not converge: %+v", result)
	}

	cfg.GRPCContentType = true
	present = false
	listener["l7policies"], pool["listeners"] = []record{{"id": "http-80"}}, []record{}
	if result := reconcile(""); result.Routes[0].Result.Outcome.Message != "Waiting for Octavia to advertise grpc_content_type_v1" ||
		listener["admin_state_up"] != true {
		t.Fatalf("fresh native policy creation must wait for enrollment without closing the listener: %+v", result)
	}
}

func TestExactHTTPQueryParameterReconcilesRegionalRuleReadback(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	const name, value = "ANIMAL", "é whale&+%2F"
	match := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", QueryParams: []cloud.HTTPQueryParamMatch{{Name: name, Value: value}}}
	route.HTTPBackends = []cloud.HTTPBackend{{Namespace: "workloads", Name: "backend", Port: 8080, Members: route.Members, Matches: []cloud.HTTPPathMatch{match}}}
	route.Members = nil
	for _, p := range []*Provider{
		{projectID: "project-a", fixedHTTPResponses: true},
		{projectID: "project-a", httpQueryParamExact: true},
	} {
		if _, err := p.buildDesiredGatewayGraph(spec); !errors.Is(err, cloud.ErrTerminalValidation) {
			t.Fatalf("missing opt-in or fixed fallback returned %v", err)
		}
	}

	const rulesURL = "/lbaas/l7policies/policy/rules"
	capabilities := []string{"fixed_http_responses_v1"}
	var rules []octavia.RuleObservation
	var queryWrites int
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/lbaas/providers/amphora-region-alias/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case req.Method == http.MethodGet && req.URL.Path == rulesURL:
			safetyWriteJSON(t, w, map[string]any{"rules": rules})
		case req.Method == http.MethodPost && req.URL.Path == rulesURL,
			req.Method == http.MethodPut && req.URL.Path == rulesURL+"/query":
			var body struct {
				Rule octavia.RuleObservation `json:"rule"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			rule := body.Rule
			rule.ID, rule.ProjectID, rule.ProvisioningStatus = "path", "project-a", "ACTIVE"
			if rule.RuleType == "HTTP_QUERY_PARAM_EXACT" {
				queryWrites++
				rule.ID = "query"
				if rule.CompareType != "EQUAL_TO" || rule.Key != name || rule.Value != value ||
					rule.AdminState == nil || !*rule.AdminState || rule.Inverted != nil && *rule.Inverted {
					t.Fatalf("unexpected query predicate: %#v", rule)
				}
			}
			if rule.Inverted == nil {
				rule.Inverted = new(false)
			}
			if req.Method == http.MethodPost {
				rules = append(rules, rule)
				w.WriteHeader(http.StatusCreated)
			} else {
				rule.Tags = rules[1].Tags
				rules[1] = rule
			}
			safetyWriteJSON(t, w, map[string]any{"rule": rule})
		default:
			t.Errorf("unexpected %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{HTTPQueryParamExact: true, FixedHTTPResponses: true})
	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	policy := part.policies[0]
	predicates := rulesFor(policy, "")
	if len(predicates) != 2 || predicates[1].ruleType != httpQueryParamExactRuleType {
		t.Fatalf("missing query conjunction: %#v", predicates)
	}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region-alias"
	actual := observedGatewayGraph{loadBalancer: lb}
	writeRule := func(mutation routeMutation) {
		t.Helper()
		step, err := p.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphRouteMutation, backendKey: part.backendKey, route: mutation})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.executeGraphMutation(t.Context(), desired, actual, step); err != nil {
			t.Fatal(err)
		}
	}
	queryCreate := routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: predicates[1]}
	for _, mutation := range []routeMutation{
		{kind: routeMutationCreatePolicy, policy: policy},
		queryCreate,
		{kind: routeMutationEnablePolicy, requiresExactQueryParams: true},
	} {
		var missing *octavia.MissingGatewayCapability
		_, err := p.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphRouteMutation, backendKey: part.backendKey, route: mutation})
		if !errors.As(err, &missing) || missing.Capability != "http_query_param_exact_v1" {
			t.Fatalf("mutation %v bypassed query capability discovery: %v", mutation.kind, err)
		}
	}
	capabilities = append(capabilities, "http_query_param_exact_v1")
	writeRule(routeMutation{kind: routeMutationCreateRule, parentID: "policy", rule: predicates[0]})
	writeRule(queryCreate)

	rawPolicy := l7policies.L7Policy{ID: "policy", ProjectID: "project-a", ListenerID: "listener",
		Position: policy.position, ProvisioningStatus: "ACTIVE", AdminStateUp: true,
		Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: "pool"}
	tags, err := part.identity.RouteTags(policy.role)
	if err != nil {
		t.Fatal(err)
	}
	policyRole, ok := readRouteResourceRole(tags, rolePolicyPrefix)
	if !ok {
		t.Fatalf("unrecognized policy identity %q", policy.role)
	}
	observe := func() observedRoutePolicy {
		t.Helper()
		observed, err := p.observeRoutePolicy(t.Context(), routeGraphOwner{identity: part.identity}, rawPolicy, "listener", policyRole)
		if err != nil {
			t.Fatal(err)
		}
		return observed
	}
	if observed := observe(); queryWrites != 1 || !observedHTTPPredicateMatches(observed, predicates) {
		t.Fatalf("query write did not round trip: %#v", observed)
	}
	rules[1].Key = "animal"
	rules[1].Value = "stale"
	plan, disable, _ := buildRoutePolicyPlan(part, []observedRoutePolicy{observe()}, "pool")
	if len(disable) != 1 || disable[0].kind != routeMutationDisablePolicy || len(plan) != 1 || plan[0].kind != routeMutationUpdateRule {
		t.Fatalf("query drift must disable the live policy before repair: %v %v", disable, plan)
	}
	rawPolicy.AdminStateUp = false // The next pass sees the completed disable.
	writeRule(plan[0])
	if observed := observe(); queryWrites != 2 || !observedHTTPPredicateMatches(observed, predicates) {
		t.Fatalf("query update did not round trip: %#v", observed)
	}
	rawPolicy.AdminStateUp = true
	for _, params := range [][]cloud.HTTPQueryParamMatch{nil, {{Name: name, Value: "dolphin"}}} {
		route.HTTPBackends[0].Matches[0].QueryParams = params
		replacement, err := p.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		part := replacement.routes[0].httpParts[0]
		plan, disable, _ := buildRoutePolicyPlan(part, []observedRoutePolicy{observe()}, "pool")
		if len(disable) != 1 || disable[0].kind != routeMutationDisablePolicy ||
			!slices.ContainsFunc(plan, func(step routeMutation) bool { return step.kind == routeMutationDeleteRule && step.id == "query" }) ||
			!slices.ContainsFunc(plan, func(step routeMutation) bool {
				return step.kind == routeMutationCreatePolicy && step.policy.role != policy.role
			}) {
			t.Fatalf("query change %v must retire the old policy without broadening it: %v %v", params, disable, plan)
		}
	}
}
