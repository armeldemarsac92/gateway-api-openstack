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
