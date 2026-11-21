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

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestSNIPassthroughListenerRequiresDiscoveryAndUsesNativeTCP(t *testing.T) {
	for _, capability := range []bool{false, true} {
		var discovered, wrote bool
		client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
				discovered = true
				if capability {
					safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{"tls_sni_passthrough_v1"}})
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			case r.Method == http.MethodPost && r.URL.Path == "/lbaas/listeners":
				wrote = true
				var body struct {
					Listener map[string]any `json:"listener"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.Listener["protocol"] != "TCP" || body.Listener["default_pool_id"] != nil || body.Listener["admin_state_up"] != false {
					t.Errorf("unsafe passthrough listener request: %v", body.Listener)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"listener":{"id":"tcp"}}`))
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		provider := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{TLSSNIPassthrough: true})
		spec := completeGraphSpec(safetyTestIdentity(t))
		spec.Routes = nil
		spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolTLSSNI
		desired, err := provider.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		lb := safetyObservedLoadBalancer()
		lb.Provider = "amphora"
		actual := observedGatewayGraph{loadBalancer: lb, state: cloud.GatewayState{LoadBalancerID: lb.ID}}
		step, err := provider.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphCreateListener})
		if err == nil {
			err = provider.executeGraphMutation(t.Context(), desired, actual, step)
		}
		if !discovered || wrote != capability || (err == nil) != capability {
			t.Fatalf("discovered=%t wrote=%t err=%v; capability=%t", discovered, wrote, err, capability)
		}
	}
}

func TestSNIGraphWithdrawsObsoletePolicyWithoutUnprogrammingSibling(t *testing.T) {
	gateway := safetyTestIdentity(t)
	routeA, routeB := gateway.Value(), gateway.Value()
	routeA.RouteNamespace, routeA.RouteName, routeA.RouteUID = "edge", "a", "route-a"
	routeB.RouteNamespace, routeB.RouteName, routeB.RouteUID = "edge", "b", "route-b"
	targets, err := buildSNITargets(gateway.Value(), []cloud.TLSSNIRoute{
		{Identity: routeA, Hostname: "a.example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}},
		{Identity: routeB, Hostname: "b.example.test", ServiceUID: "service-b", Members: []cloud.Member{{Address: "192.0.2.11", Port: 443}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	observed := func(route desiredSNIFragment, id string) *observedSNIFragment {
		return &observedSNIFragment{
			pool:    &pools.Pool{ID: "pool-" + id, ProvisioningStatus: "ACTIVE", AdminStateUp: true, LBMethod: "ROUND_ROBIN"},
			members: []pools.Member{{ID: "member-" + id, Address: route.route.Members[0].Address, ProtocolPort: 443, Weight: 1, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}},
			policy:  &l7policies.L7Policy{ID: "policy-" + id, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
			rule:    &l7policies.Rule{ID: "rule-" + id, Value: route.route.Hostname, RuleType: "TLS_SNI", CompareType: "EQUAL_TO", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		}
	}
	desired := desiredGatewayGraph{spec: completeGraphSpec(gateway), sni: targets[1:]}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:      map[sniFragmentKey]*observedSNIFragment{targets[0].key: observed(targets[0], "a"), targets[1].key: observed(targets[1], "b")},
	}
	actual.loadBalancer.AdminStateUp = true
	provider := &Provider{}
	steps, result, err := provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy || steps[0].deletion.resourceID != "policy-a" {
		t.Fatalf("the first withdrawal must close only policy A, plan=%#v err=%v", steps, err)
	}
	if len(result.Routes) != 1 || result.Routes[0].Identity.RouteUID != "route-b" || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("B remains proven during withdrawal of A, got %#v", result.Routes)
	}
}

func TestSNIPassthroughRejectsOverlongDNSLabel(t *testing.T) {
	gateway := safetyTestIdentity(t)
	route := gateway.Value()
	route.RouteNamespace, route.RouteName, route.RouteUID = "edge", "a", "route-a"
	_, err := buildSNITargets(gateway.Value(), []cloud.TLSSNIRoute{
		{Identity: route, Hostname: strings.Repeat("a", 64) + ".example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}},
	})
	if err == nil {
		t.Fatal("accepted a DNS label that the Octavia SNI extension refuses")
	}
}
