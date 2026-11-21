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
	"reflect"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
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
	actual := observedGatewayGraph{loadBalancer: lb, routes: []observedRouteGraph{{policies: []observedRoutePolicy{{
		adminObserved: true,
		policy:        l7policies.L7Policy{ID: "weighted", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
	}}}}}
	targets := []octavia.WeightedHTTPTarget{fixedWeightedHTTPTarget(1)}
	step := graphMutation{kind: graphRouteMutation, withdrawHTTPPolicy: true,
		route: routeMutation{kind: routeMutationSetPolicyTarget, id: "weighted", weightedTargets: &targets}}
	prepared, err := p.prepareGraphMutation(t.Context(), desired, actual, step)
	if err != nil || calls != 1 || prepared.route.kind != routeMutationDisablePolicy || prepared.route.id != "weighted" || prepared.route.weightedTargets != nil {
		t.Fatalf("expected only a stock disable, got %#v after %d reads, error %v", prepared.route, calls, err)
	}
	actual.routes[0].policies[0].policy.AdminStateUp = false
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
