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

func TestSNIPolicyWithdrawalDuringNeutronFailureAndDisabledCleanup(t *testing.T) {
	routeID := safetyTestIdentity(t)
	spec := completeGraphSpec(routeID)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolTLSSNI
	spec.Gateway.Spec.ExternalNetworkID = ""
	targets, err := buildSNITargets(spec.Identity, []cloud.TLSSNIRoute{{
		Identity: routeID.Value(), Hostname: "a.example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.30", Port: 443}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tags := func(role string) []string {
		t.Helper()
		tags, err := targets[0].tags(role)
		if err != nil {
			t.Fatal(err)
		}
		return tags
	}
	ids := func(id string) []map[string]string { return []map[string]string{{"id": id}} }
	lb := map[string]any{"id": "lb", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"provider": "amphora", "vip_subnet_id": spec.Gateway.Spec.VIPSubnetID, "vip_port_id": "vip-port", "vip_address": "192.0.2.10",
		"listeners": ids("listener"), "pools": ids("pool"), "tags": append(safetyGatewayTags(t, routeID, roleLoadBalancer), sniProfileTag)}
	listener := map[string]any{"id": "listener", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "TCP", "protocol_port": spec.Gateway.Spec.ListenerPort, "default_pool_id": nil, "loadbalancers": ids("lb"),
		"l7policies": ids("policy-a"), "tags": append(safetyGatewayTags(t, routeID, roleListener), sniProfileTag)}
	pool := map[string]any{"id": "pool", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "TCP", "lb_algorithm": "ROUND_ROBIN", "loadbalancers": ids("lb"), "listeners": ids("listener"),
		"members": ids("member"), "tags": tags(roleSNIPool)}
	member := map[string]any{"id": "member", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"address": "192.0.2.30", "protocol_port": 443, "weight": 1, "tags": tags(roleSNIMember)}
	policy := map[string]any{"id": "policy-a", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"position": 1, "action": "REDIRECT_TO_POOL", "listener_id": "listener", "redirect_pool_id": "pool", "rules": ids("rule"), "tags": tags(roleSNIPolicy)}
	rule := map[string]any{"id": "rule", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"type": "TLS_SNI", "value": "a.example.test", "compare_type": "EQUAL_TO", "invert": false, "tags": tags(roleSNIRule)}
	reads := map[string]map[string]any{
		"/lbaas/loadbalancers": {"loadbalancers": []any{lb}}, "/lbaas/loadbalancers/lb": {"loadbalancer": lb},
		"/lbaas/listeners": {"listeners": []any{listener}}, "/lbaas/pools": {"pools": []any{pool}},
		"/lbaas/pools/pool/members": {"members": []any{member}}, "/lbaas/l7policies": {"l7policies": []any{policy}},
		"/lbaas/l7policies/policy-a/rules": {"rules": []any{rule}},
	}
	var writes []string
	neutronFailed := true
	clients := ServiceClients{
		ProjectID: "project-a",
		Network: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/floatingips" {
				t.Errorf("unexpected Neutron request: %s %s", r.Method, r.URL.Path)
			}
			if neutronFailed {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			safetyWriteJSON(t, w, map[string]any{"floatingips": []any{}})
		})),
		LoadBalancer: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if body, found := reads[r.URL.Path]; r.Method == http.MethodGet && found {
				safetyWriteJSON(t, w, body)
				return
			}
			writes = append(writes, r.Method+" "+r.URL.Path)
			if r.Method != http.MethodPut || r.URL.Path != "/lbaas/l7policies/policy-a" {
				t.Errorf("unexpected Octavia request: %s", writes[len(writes)-1])
				http.Error(w, "unexpected", http.StatusBadRequest)
				return
			}
			var body struct {
				Policy map[string]any `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Policy) != 1 || body.Policy["admin_state_up"] != false {
				t.Errorf("withdrawal must only close the owned SNI policy: %#v, %v", body.Policy, err)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy": policy})
		})),
	}
	t.Run("withdrawal remains writable with Neutron down", func(t *testing.T) {
		provider := newTestProvider(t, clients, ProviderConfig{TLSSNIPassthrough: true})
		stale := errors.New("authorization changed")
		spec.ValidateMutation = func(context.Context) error { return stale }
		if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, stale) || len(writes) != 0 {
			t.Fatalf("the withdrawal must still pass the mutation fence: err=%v writes=%v", err, writes)
		}
		spec.ValidateMutation = nil
		if result, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || result.Outcome.State != cloud.OutcomeProgressing || len(writes) != 1 {
			t.Fatalf("the verified withdrawal should run without the optional address inventory: err=%v result=%v writes=%v", err, result, writes)
		}
		spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{targets[0].route}
		if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "discover Gateway Floating IPs") || len(writes) != 1 {
			t.Fatalf("serving state still needs address inventory: err=%v writes=%v", err, writes)
		}
		spec.Gateway.TLSSNIRoutes = nil
	})
	t.Run("disabled controller still cleans verified TLS graph", func(t *testing.T) {
		writes, neutronFailed = nil, false
		provider := newTestProvider(t, clients, ProviderConfig{})
		if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err == nil || len(writes) != 0 {
			t.Fatalf("feature off must reject Ensure: err=%v writes=%v", err, writes)
		}
		cleanup := spec
		cleanup.Gateway.Disposition = cloud.GraphDelete
		if result, err := provider.ReconcileGatewayGraph(t.Context(), cleanup); err != nil || result.Outcome.State != cloud.OutcomeProgressing || len(writes) != 1 {
			t.Fatalf("feature off must allow verified GraphDelete: err=%v result=%v writes=%v", err, result, writes)
		}
		policy["project_id"] = "foreign-project"
		if _, err := provider.ReconcileGatewayGraph(t.Context(), cleanup); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != 1 {
			t.Fatalf("feature off must never delete a foreign fragment: err=%v writes=%v", err, writes)
		}
	})
}

func TestSNIPendingLoadBalancerDefersOnlyDesiredRouteObservations(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ExternalNetworkID = ""
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolTLSSNI,
		sni: []desiredSNIFragment{{route: cloud.TLSSNIRoute{Identity: identity.Value()}}}}
	lb := safetyObservedLoadBalancer()
	lb.ProvisioningStatus, lb.AdminStateUp = "PENDING_UPDATE", true
	actual := observedGatewayGraph{loadBalancer: lb, outcome: cloud.ProgressingOutcome("Updating", 1),
		listener: &listeners.Listener{ID: "listener", AdminStateUp: true, ProvisioningStatus: "PENDING_UPDATE"}}
	p := &Provider{}
	if got := p.pendingGraphResult(t.Context(), desired, actual).Routes; len(got) != 1 || !got[0].Result.ObservationDeferred {
		t.Fatalf("expected one desired route eligible to retain an existing proof while a policy update is pending, got %#v", got)
	}
	lb.ProvisioningStatus = "PENDING_CREATE"
	if got := p.pendingGraphResult(t.Context(), desired, actual).Routes; len(got) != 0 && got[0].Result.ObservationDeferred {
		t.Fatalf("a pending new loadbalancer is not an eligible serving sibling, got %#v", got)
	}
}
