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
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

func TestSNISuffixRequiresBothCapabilitiesAndWritesWildcardValue(t *testing.T) {
	for _, capabilities := range [][]string{
		{"tls_sni_passthrough_v1"}, {"tls_sni_suffix_v1"}, {"tls_sni_passthrough_v1", "tls_sni_suffix_v1"},
	} {
		var discovered, wrote bool
		client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
				discovered = true
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
			case r.Method == http.MethodPost && r.URL.Path == "/lbaas/l7policies/policy/rules":
				wrote = true
				var body struct {
					Rule struct {
						Type, Value string
						CompareType string `json:"compare_type"`
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Rule.Type != "TLS_SNI" ||
					body.Rule.CompareType != "ENDS_WITH" || body.Rule.Value != "*.example.test" {
					t.Errorf("invalid Octavia SNI suffix request: %#v, %v", body, err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"rule":{"id":"rule"}}`))
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		cfg := ProviderConfig{TLSSNIPassthrough: true}
		identity := safetyTestIdentity(t)
		spec := completeGraphSpec(identity)
		spec.Routes = nil
		spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolTLSSNI
		spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{{
			Identity: identity.Value(), Hostname: "*.example.test", ServiceUID: "service-a",
			Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}},
		}}
		clients := ServiceClients{LoadBalancer: client, ProjectID: "project-a", Region: "region-a"}
		if _, err := NewProvider(clients, cfg).buildDesiredGatewayGraph(spec); err == nil {
			t.Fatal("accepted a wildcard with the suffix feature off")
		}
		cfg.TLSSNISuffix = true
		provider := NewProvider(clients, cfg)
		desired, err := provider.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		step := graphMutation{kind: graphSNIMutation, sni: sniMutation{kind: sniCreateRule, parentID: "policy", target: desired.sni[0]}}
		actual := observedGatewayGraph{}
		step, err = provider.prepareGraphMutation(t.Context(), desired, actual, step)
		if err == nil {
			err = provider.executeGraphMutation(t.Context(), desired, actual, step)
		}
		wantWrite := len(capabilities) == 2
		if !discovered || wrote != wantWrite || (err == nil) != wantWrite {
			t.Fatalf("caps=%v discovered=%t wrote=%t err=%v", capabilities, discovered, wrote, err)
		}
		if len(capabilities) == 1 && capabilities[0] == "tls_sni_passthrough_v1" {
			actual.sni = map[sniFragmentKey]*observedSNIFragment{desired.sni[0].key: activeSNIFragment(desired.sni[0], "observed")}
			spec.Gateway.TLSSNIRoutes[0].Hostname = "exact.example.test"
			exact, err := provider.buildDesiredGatewayGraph(spec)
			if err != nil {
				t.Fatal(err)
			}
			if err := provider.verifySNICapability(t.Context(), exact, actual); err == nil {
				t.Fatal("an observed serving suffix still needs the suffix capability")
			}
			actual.sni[desired.sni[0].key].policy.AdminStateUp = false
			if err := provider.verifySNICapability(t.Context(), exact, actual); err != nil {
				t.Fatalf("the suffix flag must not block an exact-only graph: %v", err)
			}
		}
	}
}

func TestSNIGraphOrdersExactAndNestedSuffixTargets(t *testing.T) {
	identity := safetyTestIdentity(t).Value()
	var routes []cloud.TLSSNIRoute
	for _, hostname := range []string{"*.example.test", "vip.shop.example.test", "*.shop.example.test"} {
		routes = append(routes, cloud.TLSSNIRoute{Identity: identity, Hostname: hostname, ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}})
	}
	targets, err := buildSNITargets(identity, routes)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"vip.shop.example.test", "*.shop.example.test", "*.example.test"}
	for i, target := range targets {
		if target.route.Hostname != want[i] {
			t.Fatalf("target %d = %s, want %s", i, target.route.Hostname, want[i])
		}
	}
}

func TestSNISuffixStagesReplacementBeforeRetiringProtectedSelector(t *testing.T) {
	identity := safetyTestIdentity(t)
	narrow := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "*.shop.example.test",
		ServiceUID: "narrow-service", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}}
	broad := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "*.example.test",
		ServiceUID: "broad-service", Members: []cloud.Member{{Address: "192.0.2.11", Port: 443}}}
	broad.Identity.RouteName, broad.Identity.RouteUID = "broad", "broad-uid"
	exact := broad
	exact.Hostname, exact.Identity.RouteName, exact.Identity.RouteUID = "vip.shop.example.test", "exact", "exact-uid"
	deny := cloud.TLSSNIRoute{Identity: narrow.Identity, Hostname: narrow.Hostname, Deny: true}
	compile := func(narrowTarget cloud.TLSSNIRoute) []desiredSNIFragment {
		t.Helper()
		targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{narrowTarget, broad, exact})
		if err != nil {
			t.Fatal(err)
		}
		return targets
	}
	serving, restricted := compile(narrow), compile(deny)
	desired := desiredGatewayGraph{spec: completeGraphSpec(identity), sni: restricted}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni: map[sniFragmentKey]*observedSNIFragment{
			serving[0].key: activeSNIFragment(serving[0], "exact"),
			serving[1].key: activeSNIFragment(serving[1], "narrow"),
			serving[2].key: activeSNIFragment(serving[2], "broad"),
		},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	loadBalancerClient := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/pools/pool-narrow/members":
			var body struct {
				Members []json.RawMessage
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Members == nil || len(body.Members) != 0 {
				t.Errorf("revocation must atomically remove all members: %#v, %v", body, err)
			}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/pools":
			http.Error(w, "Quota exceeded for pool.", http.StatusForbidden)
		default:
			t.Errorf("unexpected Octavia write %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: loadBalancerClient, ProjectID: "project-a"}, ProviderConfig{})
	next := func() graphMutation {
		t.Helper()
		steps, result, err := provider.buildSNIGraphPlan(desired, actual)
		if err != nil || len(steps) != 1 {
			t.Fatalf("expected a single graph mutation, got %#v, %v", steps, err)
		}
		for _, route := range result.Routes {
			if route.Identity == narrow.Identity {
				if len(desired.sni) > 1 && desired.sni[1].route.Deny {
					t.Fatal("an empty denial pool may not produce forwarding evidence")
				}
				continue
			}
			if route.Result.Outcome.State != cloud.OutcomeReady {
				t.Fatalf("authorized sibling must retain forwarding evidence: %#v", route)
			}
		}
		return steps[0]
	}
	assertMutation := func(kind sniMutationKind, target desiredSNIFragment) {
		t.Helper()
		if got := next(); got.kind != graphSNIMutation || got.sni.kind != kind || got.sni.target.key != target.key {
			t.Fatalf("expected SNI mutation %d for %s, got %#v", kind, target.route.Hostname, got)
		}
	}
	assertDisable := func(id string) {
		t.Helper()
		if got := next(); got.kind != graphDeleteChild || got.deletion.resource != gatewayDisableL7Policy || got.deletion.resourceID != "policy-"+id {
			t.Fatalf("expected only policy %s to close, got %#v", id, got)
		}
	}
	oldNarrow := actual.sni[serving[1].key]
	oldNarrow.policy.Position = 2
	if got := next(); got.kind != graphSNIMutation || got.sni.kind != sniClearMembers || !got.sniWithdrawal {
		t.Fatalf("must withdraw the revoked members before allocation: %#v", got)
	} else if err := provider.executeSNIMutation(t.Context(), desired, actual, got.sni); err != nil {
		t.Fatal(err)
	}
	oldNarrow.members = nil
	quota := next()
	if quota.sni.kind != sniCreatePool {
		t.Fatalf("expected to stage a replacement for the now-empty selector: %#v", quota)
	}
	if err := provider.executeSNIMutation(t.Context(), desired, actual, quota.sni); err == nil {
		t.Fatal("the fake Octavia project must reject the new pool")
	}
	if oldNarrow.forwardingHostname() != narrow.Hostname || len(oldNarrow.members) != 0 {
		t.Fatal("quota rejection must leave a matching empty pool in front of the broad route")
	}
	// If the grant returns before the new denial is provisioned, the original
	// selector can regain its authorized members without exposing a fallback.
	desired.sni = serving
	assertMutation(sniCreateMember, serving[1])
	desired.sni = restricted

	assertMutation(sniCreatePool, restricted[1])
	newDeny := activeSNIFragment(restricted[1], "deny")
	newDeny.policy.Position = 1
	actual.sni[restricted[1].key] = &observedSNIFragment{pool: newDeny.pool}
	assertMutation(sniCreatePolicy, restricted[1])
	newDeny.policy.AdminStateUp = false
	actual.sni[restricted[1].key].policy = newDeny.policy
	assertMutation(sniCreateRule, restricted[1])
	actual.sni[restricted[1].key].rule = newDeny.rule
	assertMutation(sniEnablePolicy, restricted[1])
	newDeny.policy.AdminStateUp = true
	// The confirmed position-1 guard also protects the name when a stale
	// lower-priority member is still present. The obsolete policy can close.
	oldNarrow.members = activeSNIFragment(serving[1], "narrow").members
	assertDisable("narrow")

	delete(actual.sni, serving[1].key)
	desired.sni = serving
	assertMutation(sniCreatePool, serving[1])
	newServing := activeSNIFragment(serving[1], "replacement")
	newServing.policy.AdminStateUp = false
	actual.sni[serving[1].key] = newServing
	assertMutation(sniEnablePolicy, serving[1])
	newServing.policy.AdminStateUp = true
	assertDisable("deny")

	// Opting back into exact-only operation withdraws the old broader rule
	// while the observed narrow selector still covers its names.
	desired.sni = serving[:1]
	assertDisable("broad")
}

func TestSNISuffixWithdrawsFormerOwnerBeforeStagingSuccessor(t *testing.T) {
	identity := safetyTestIdentity(t)
	previous := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "*.shop.example.test",
		ServiceUID: "former-service", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}}
	successor := previous
	successor.Identity.RouteUID = "successor-uid"
	successor.ServiceUID = "successor-service"
	successor.Members = []cloud.Member{{Address: "192.0.2.11", Port: 443}}
	broad := previous
	broad.Identity.RouteName, broad.Identity.RouteUID = "broad", "broad-uid"
	broad.Hostname = "*.example.test"
	broad.ServiceUID = "broad-service"
	broad.Members = []cloud.Member{{Address: "192.0.2.12", Port: 443}}
	compile := func(narrow cloud.TLSSNIRoute) []desiredSNIFragment {
		t.Helper()
		targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{narrow, broad})
		if err != nil {
			t.Fatal(err)
		}
		return targets
	}
	before, after := compile(previous), compile(successor)
	formerTarget, successorTarget := before[0], after[0]
	former := activeSNIFragment(formerTarget, "former")
	formerMembers := former.members
	desired := desiredGatewayGraph{spec: completeGraphSpec(identity), sni: after}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni: map[sniFragmentKey]*observedSNIFragment{
			formerTarget.key: former,
			after[1].key:     activeSNIFragment(after[1], "broad"),
		},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/lbaas/pools" {
			t.Errorf("unexpected Octavia write %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		http.Error(w, "Quota exceeded for pool.", http.StatusForbidden)
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	next := func(wantNarrow cloud.OutcomeState) graphMutation {
		t.Helper()
		steps, result, err := provider.buildSNIGraphPlan(desired, actual)
		if err != nil || len(steps) != 1 || len(result.Routes) != 2 ||
			result.Routes[0].Identity != desired.sni[0].route.Identity || result.Routes[0].Result.Outcome.State != wantNarrow ||
			result.Routes[1].Identity != broad.Identity || result.Routes[1].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("expected one mutation and an unaffected covering route: plan=%v routes=%v err=%v", steps, result.Routes, err)
		}
		return steps[0]
	}
	if clear := next(cloud.OutcomeProgressing); clear.kind != graphSNIMutation || clear.sni.kind != sniClearMembers ||
		clear.sni.parentID != former.pool.ID || !clear.isRestrictiveSNI() {
		t.Fatalf("withdraw the former destinations before allocating the successor: %#v", clear)
	}
	former.members = nil
	quota := next(cloud.OutcomeProgressing)
	if quota.kind != graphSNIMutation || quota.sni.kind != sniCreatePool || quota.sni.target.key != successorTarget.key {
		t.Fatalf("allocate the successor after observing the withdrawal: %#v", quota)
	}
	if err := provider.executeSNIMutation(t.Context(), desired, actual, quota.sni); err == nil {
		t.Fatal("expected Octavia to reject the pool allocation")
	}
	if retry := next(cloud.OutcomeProgressing); retry.sni.kind != sniCreatePool || retry.sni.target.key != successorTarget.key ||
		former.forwardingHostname() != previous.Hostname || len(former.members) != 0 {
		t.Fatalf("the former selector must stay active and empty across an allocation failure: %#v", retry)
	}

	// When the previous owner regains the claim, reuse the empty pool.
	desired.sni = before
	if restore := next(cloud.OutcomeProgressing); restore.kind != graphSNIMutation || restore.sni.kind != sniCreateMember ||
		restore.sni.target.key != formerTarget.key || restore.sni.parentID != former.pool.ID {
		t.Fatalf("restore the authorized destination in the original pool: %#v", restore)
	}
	former.members = formerMembers
	if steps, result, err := provider.buildSNIGraphPlan(desired, actual); err != nil || len(steps) != 0 || len(result.Routes) != 2 ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("the restored owner must become ready: plan=%v routes=%v err=%v", steps, result.Routes, err)
	}

	// Once the successor is staged, observe its activation before cleanup.
	desired.sni = after
	former.members = nil
	replacement := activeSNIFragment(successorTarget, "successor")
	replacement.policy.AdminStateUp = false
	actual.sni[successorTarget.key] = replacement
	if enable := next(cloud.OutcomeProgressing); enable.kind != graphSNIMutation || enable.sni.kind != sniEnablePolicy || enable.sni.target.key != successorTarget.key {
		t.Fatalf("enable the successor while retaining the former selector: %#v", enable)
	}
	replacement.policy.AdminStateUp = true
	if disable := next(cloud.OutcomeProgressing); disable.kind != graphDeleteChild || disable.deletion.resource != gatewayDisableL7Policy || disable.deletion.resourceID != former.policy.ID {
		t.Fatalf("retire the former selector after the successor is active: %#v", disable)
	}
	former.policy.AdminStateUp = false
	if cleanup := next(cloud.OutcomeReady); cleanup.deletion.resource != gatewayDeletionL7Rule || cleanup.deletion.resourceID != former.rule.ID {
		t.Fatalf("clean up the former fragment while the successor is ready: %#v", cleanup)
	}
	former.rule = nil
	if cleanup := next(cloud.OutcomeReady); cleanup.deletion.resource != gatewayDeletionL7Policy || cleanup.deletion.resourceID != former.policy.ID {
		t.Fatalf("delete the former policy after removing its rule: %#v", cleanup)
	}
	former.policy = nil
	if cleanup := next(cloud.OutcomeReady); cleanup.deletion.resource != gatewayDeletionPool || cleanup.deletion.resourceID != former.pool.ID {
		t.Fatalf("delete the former pool after removing its policy: %#v", cleanup)
	}
	delete(actual.sni, formerTarget.key)
	if steps, result, err := provider.buildSNIGraphPlan(desired, actual); err != nil || len(steps) != 0 || result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("the handoff must settle after cleanup: plan=%v outcome=%v err=%v", steps, result.Outcome, err)
	}
}

func TestSNISuffixSuspendsFallbacksBeforeAllocatingMissingDenial(t *testing.T) {
	identity := safetyTestIdentity(t)
	makeTarget := func(name, hostname string, deny bool) cloud.TLSSNIRoute {
		route := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: hostname, Deny: deny}
		route.Identity.RouteName, route.Identity.RouteUID = name, name+"-uid"
		if !deny {
			route.ServiceUID, route.Members = name+"-svc", []cloud.Member{{Address: "192.0.2.12", Port: 443}}
		}
		return route
	}
	targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{
		makeTarget("exact", "vip.shop.example.test", false),
		makeTarget("denial", "*.shop.example.test", true),
		makeTarget("regional", "*.example.test", false),
		makeTarget("wide", "*.test", false),
	})
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredGatewayGraph{spec: completeGraphSpec(identity), sni: targets}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni: map[sniFragmentKey]*observedSNIFragment{
			targets[0].key: activeSNIFragment(targets[0], "exact"),
			targets[2].key: activeSNIFragment(targets[2], "regional"),
			targets[3].key: activeSNIFragment(targets[3], "wide"),
		},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	for _, index := range []int{3, 2} {
		steps, result, err := (&Provider{}).buildSNIGraphPlan(desired, actual)
		fragment := actual.sni[targets[index].key]
		if err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy ||
			steps[0].deletion.resourceID != fragment.policy.ID || !steps[0].sniWithdrawal {
			t.Fatalf("expected to suspend %s before any allocation, got %#v, %v", fragment.policy.ID, steps, err)
		}
		if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("the exact sibling must retain forwarding evidence: %#v", result.Routes)
		}
		fragment.policy.AdminStateUp = false
	}
	steps, _, err := (&Provider{}).buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].kind != graphSNIMutation ||
		steps[0].sni.kind != sniCreatePool || !steps[0].sni.target.route.Deny {
		t.Fatalf("only stage the denial once all of its fallbacks are suspended, got %#v, %v", steps, err)
	}
	actual.sni[targets[1].key] = activeSNIFragment(targets[1], "denial")
	steps, _, err = (&Provider{}).buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].sni.kind != sniEnablePolicy || steps[0].sni.parentID != "policy-regional" {
		t.Fatalf("restore the covering policies after observing the denial, got %#v, %v", steps, err)
	}
}

func TestSNISuffixReadinessWaitsForObsoleteNarrowerMatch(t *testing.T) {
	identity := safetyTestIdentity(t)
	makeTarget := func(name, hostname string) cloud.TLSSNIRoute {
		route := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: hostname, ServiceUID: name + "-svc",
			Members: []cloud.Member{{Address: "192.0.2.12", Port: 443}}}
		route.Identity.RouteName, route.Identity.RouteUID = name, name+"-uid"
		return route
	}
	targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{
		makeTarget("obsolete", "old.shop.example.test"),
		makeTarget("disjoint", "vip.example.test"),
		makeTarget("regional", "*.shop.example.test"),
		makeTarget("wide", "*.example.test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredGatewayGraph{spec: completeGraphSpec(identity), sni: targets[1:]}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:      make(map[sniFragmentKey]*observedSNIFragment),
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	for _, target := range targets {
		actual.sni[target.key] = activeSNIFragment(target, target.route.Identity.RouteName)
	}
	check := func(wantRegional bool) {
		t.Helper()
		ready := readySNIRoutes(desired, actual)
		if ready[targets[2].key.uid] != wantRegional || !ready[targets[1].key.uid] || !ready[targets[3].key.uid] {
			t.Fatalf("only the suffix inheriting the obsolete hostname must wait: %v", ready)
		}
	}
	check(false)
	obsolete := actual.sni[targets[0].key]
	obsolete.policy.AdminStateUp, obsolete.policy.ProvisioningStatus = false, "PENDING_UPDATE"
	check(false)
	obsolete.policy.ProvisioningStatus = "ACTIVE"
	check(true)
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
	desired := desiredGatewayGraph{spec: completeGraphSpec(gateway), sni: targets[1:]}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:      map[sniFragmentKey]*observedSNIFragment{targets[0].key: activeSNIFragment(targets[0], "a"), targets[1].key: activeSNIFragment(targets[1], "b")},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	provider := &Provider{}
	steps, result, err := provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy || steps[0].deletion.resourceID != "policy-a" {
		t.Fatalf("the first withdrawal must close only policy A, plan=%#v err=%v", steps, err)
	}
	if len(result.Routes) != 1 || result.Routes[0].Identity.RouteUID != "route-b" || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("B remains proven during withdrawal of A, got %#v", result.Routes)
	}

	retired := actual.sni[targets[0].key]
	retired.rule.Value = strings.ToUpper(targets[1].route.Hostname)
	steps, result, err = provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("B is pending while A also selects B's name, plan=%v routes=%v err=%v", steps, result.Routes, err)
	}
	retired.policy.AdminStateUp = false
	retired.policy.ProvisioningStatus = "PENDING_UPDATE"
	steps, result, err = provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 0 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("B waits for A's closure to settle, plan=%v routes=%v err=%v", steps, result.Routes, err)
	}
	retired.policy.ProvisioningStatus = "ACTIVE"
	steps, result, err = provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDeletionL7Rule ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("B is ready once A's policy closure is observed, plan=%v routes=%v err=%v", steps, result.Routes, err)
	}
}

func activeSNIFragment(route desiredSNIFragment, id string) *observedSNIFragment {
	fragment := &observedSNIFragment{
		pool:   &pools.Pool{ID: "pool-" + id, ProvisioningStatus: "ACTIVE", AdminStateUp: true, LBMethod: "ROUND_ROBIN"},
		policy: &l7policies.L7Policy{ID: "policy-" + id, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		rule: &l7policies.Rule{ID: "rule-" + id, Value: route.route.Hostname, RuleType: "TLS_SNI",
			CompareType: sniCompareType(route.route.Hostname), ProvisioningStatus: "ACTIVE", AdminStateUp: true},
	}
	for _, member := range route.route.Members {
		fragment.members = append(fragment.members, pools.Member{
			ID: "member-" + id, Address: member.Address, ProtocolPort: member.Port,
			Weight: 1, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		})
	}
	return fragment
}

func TestSNIGraphProgramsAllHostnamesBeforeReportingRouteReady(t *testing.T) {
	identity := safetyTestIdentity(t)
	first := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "a.example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}}
	second := first
	second.Hostname = "b.example.test"
	targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if targets[0].targetTag == targets[1].targetTag {
		t.Fatal("each hostname needs its own Octavia target")
	}
	desired := desiredGatewayGraph{spec: completeGraphSpec(identity), sni: targets}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(),
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:      map[sniFragmentKey]*observedSNIFragment{targets[0].key: activeSNIFragment(targets[0], "a")},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	check := func(state cloud.OutcomeState) []graphMutation {
		t.Helper()
		steps, result, err := (&Provider{}).buildSNIGraphPlan(desired, actual)
		if err != nil || len(result.Routes) != 1 || result.Routes[0].Identity != first.Identity ||
			result.Routes[0].Result.Outcome.State != state || result.Routes[0].Result.ObservationDeferred {
			t.Fatalf("expected one %s result for both hostnames, got %#v, %v", state, result.Routes, err)
		}
		return steps
	}
	if steps := check(cloud.OutcomeProgressing); len(steps) != 1 || steps[0].sni.kind != sniCreatePool || steps[0].sni.target.route.Hostname != second.Hostname {
		t.Fatalf("the existing target must stay active while creating the second pool: %#v", steps)
	}
	b := activeSNIFragment(targets[1], "b")
	actual.sni[targets[1].key] = &observedSNIFragment{pool: b.pool}
	if steps := check(cloud.OutcomeProgressing); len(steps) != 1 || steps[0].sni.kind != sniCreateMember || steps[0].sni.parentID != "pool-b" {
		t.Fatalf("readback must resume from the existing second pool: %#v", steps)
	}
	b.rule.ProvisioningStatus = "PENDING_UPDATE"
	actual.sni[targets[1].key] = b
	if steps := check(cloud.OutcomeProgressing); len(steps) != 0 {
		t.Fatalf("pending rule must finish before further writes: %#v", steps)
	}
	b.rule.ProvisioningStatus = "ACTIVE"
	if steps := check(cloud.OutcomeReady); len(steps) != 0 {
		t.Fatalf("settled route should be a no-op: %#v", steps)
	}
	desired.sni = targets[:1]
	if steps := check(cloud.OutcomeProgressing); len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy ||
		steps[0].deletion.resourceID != "policy-b" {
		t.Fatalf("the route should withdraw only the retired hostname before it is ready: %#v", steps)
	}
	replacement := second
	replacement.Hostname = "c.example.test"
	desired.sni, err = buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{first, replacement})
	if err != nil {
		t.Fatal(err)
	}
	if steps := check(cloud.OutcomeProgressing); len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy {
		t.Fatalf("the old target must be closed before creating its replacement: %#v", steps)
	}
}

func TestSNIPassthroughRejectsConflictingHostnamesAndFragments(t *testing.T) {
	identity := safetyTestIdentity(t).Value()
	first := cloud.TLSSNIRoute{Identity: identity, Hostname: "a.example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}}
	for _, test := range []struct {
		name   string
		change func(*cloud.TLSSNIRoute)
		want   error
	}{
		{"duplicate hostname", func(route *cloud.TLSSNIRoute) {
			route.Hostname = first.Hostname
			route.Identity.RouteUID = "another-route"
		}, cloud.ErrTerminalValidation},
		{"same UID with another name", func(route *cloud.TLSSNIRoute) { route.Identity.RouteName = "another-name" }, cloud.ErrOwnershipConflict},
		{"different Service", func(route *cloud.TLSSNIRoute) { route.ServiceUID = "another-service" }, cloud.ErrTerminalValidation},
		{"different member", func(route *cloud.TLSSNIRoute) { route.Members = []cloud.Member{{Address: "192.0.2.11", Port: 443}} }, cloud.ErrTerminalValidation},
	} {
		t.Run(test.name, func(t *testing.T) {
			second := first
			second.Hostname = "b.example.test"
			test.change(&second)
			if _, err := buildSNITargets(identity, []cloud.TLSSNIRoute{first, second}); !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
		})
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
	var expectReplay map[string]any
	neutronFailed := true
	capabilityReadFailed := false
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
			if r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities" {
				if capabilityReadFailed {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{"tcp_tls_termination_v1", "tls_sni_passthrough_v1", "tls_sni_suffix_v1"}})
				return
			}
			if body, found := reads[r.URL.Path]; r.Method == http.MethodGet && found {
				safetyWriteJSON(t, w, body)
				return
			}
			writes = append(writes, r.Method+" "+r.URL.Path)
			if expectReplay != nil && r.Method == http.MethodPut && r.URL.Path == "/lbaas/pools/pool/members" {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !reflect.DeepEqual(body, expectReplay) {
					t.Errorf("replay does not match the original batch: %#v, %v", body, err)
				}
				w.WriteHeader(http.StatusAccepted)
				return
			}
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
	t.Run("pending LB and listener require complete active route readback", func(t *testing.T) {
		lb["provisioning_status"], listener["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE"
		t.Cleanup(func() {
			lb["provisioning_status"], listener["provisioning_status"], policy["provisioning_status"] = "ACTIVE", "ACTIVE", "ACTIVE"
		})
		pending := spec
		pending.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{targets[0].route}
		provider := newTestProvider(t, clients, ProviderConfig{TLSSNIPassthrough: true})
		check := func(wantDeferred bool) {
			t.Helper()
			got, err := provider.ReconcileGatewayGraph(t.Context(), pending)
			if err != nil || len(writes) != 0 || len(got.Routes) != 1 || got.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
				got.Routes[0].Result.ObservationDeferred != wantDeferred {
				t.Fatalf("pending route result=%#v writes=%v err=%v, want deferred=%t", got.Routes, writes, err, wantDeferred)
			}
		}
		check(true)
		second := targets[0].route
		second.Hostname = "b.example.test"
		pending.Gateway.TLSSNIRoutes = append(pending.Gateway.TLSSNIRoutes, second)
		check(false)
		pending.Gateway.TLSSNIRoutes = pending.Gateway.TLSSNIRoutes[:1]
		policy["provisioning_status"] = "PENDING_UPDATE"
		check(false)
	})
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
	t.Run("restart replays the staged batch during cleanup", func(t *testing.T) {
		writes = nil
		originalPool, originalMember := maps.Clone(pool), maps.Clone(member)
		originalListenerPolicies, originalStatus := listener["l7policies"], lb["provisioning_status"]
		t.Cleanup(func() {
			maps.Copy(pool, originalPool)
			delete(pool, "tcp_connection_rejection")
			delete(pool, "updated_at")
			maps.Copy(member, originalMember)
			delete(member, "backup")
			lb["provisioning_status"], listener["l7policies"] = originalStatus, originalListenerPolicies
			reads["/lbaas/l7policies"]["l7policies"] = []any{policy}
			expectReplay, writes = nil, nil
		})
		lb["provisioning_status"], pool["provisioning_status"], member["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE", "PENDING_CREATE"
		pool["listeners"], listener["l7policies"], reads["/lbaas/l7policies"]["l7policies"] = []any{}, []any{}, []any{}
		pool["tags"] = append(tags(roleSNIPool), sniWeightedPoolTag)
		pool["updated_at"] = time.Now().Add(-time.Minute).Format(time.RFC3339)
		fraction := map[string]any{"reject_weight": float64(1), "total_weight": float64(3)}
		pool["tcp_connection_rejection"] = fraction
		originalTags := append(tags(roleSNIMember), "controller-batch-version-observed")
		slices.Sort(originalTags)
		observedTags := slices.Clone(originalTags)
		slices.Reverse(observedTags)
		member["backup"], member["tags"], member["weight"] = false, observedTags, 3
		wireTags := make([]any, len(originalTags))
		for i, tag := range originalTags {
			wireTags[i] = tag
		}
		expectReplay = map[string]any{"tcp_connection_rejection": fraction, "members": []any{map[string]any{
			"address": "192.0.2.30", "protocol_port": float64(443), "admin_state_up": true, "backup": false, "weight": float64(3), "tags": wireTags,
		}}}
		cleanup := spec
		cleanup.Gateway.Disposition = cloud.GraphDelete
		cleanup.Identity.ControllerVersion, cleanup.Gateway.Spec.Identity.ControllerVersion = "new-version", "new-version"
		fenced := false
		cleanup.ValidateMutation = func(context.Context) error { fenced = true; return nil }
		provider := newTestProvider(t, clients, ProviderConfig{})
		result, err := provider.ReconcileGatewayGraph(t.Context(), cleanup)
		if err != nil || !fenced || !result.Outcome.MutationSubmitted || !slices.Equal(writes, []string{"PUT /lbaas/pools/pool/members"}) {
			t.Fatalf("cleanup must replay the detached original batch first: fenced=%t writes=%v outcome=%#v err=%v", fenced, writes, result.Outcome, err)
		}
	})
	t.Run("unattributable member allows policy withdrawal only", func(t *testing.T) {
		writes, neutronFailed = nil, false
		originalTags := member["tags"]
		member["tags"] = []string{"foreign-member"}
		t.Cleanup(func() {
			member["tags"], policy["admin_state_up"], pool["listeners"] = originalTags, true, ids("listener")
			writes = nil
		})
		cleanup := spec
		cleanup.Gateway.Disposition = cloud.GraphDelete
		provider := newTestProvider(t, clients, ProviderConfig{})
		if result, err := provider.ReconcileGatewayGraph(t.Context(), cleanup); err != nil || result.Outcome.State != cloud.OutcomeProgressing || len(writes) != 1 {
			t.Fatalf("the owned policy can be withdrawn: result=%#v writes=%v err=%v", result.Outcome, writes, err)
		}
		policy["admin_state_up"], pool["listeners"] = false, []any{}
		if _, err := provider.ReconcileGatewayGraph(t.Context(), cleanup); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != 1 {
			t.Fatalf("cleanup must preserve unattributable members and their pool: writes=%v err=%v", writes, err)
		}
	})
	t.Run("disabled controller still cleans verified suffix rules", func(t *testing.T) {
		writes, neutronFailed = nil, false
		rule["value"], rule["compare_type"] = "*.example.test", "ENDS_WITH"
		t.Cleanup(func() { rule["value"], rule["compare_type"] = "a.example.test", "EQUAL_TO" })
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
	t.Run("terminated SNI reuses the owned forwarding graph", func(t *testing.T) {
		writes, neutronFailed = nil, false
		const certificate = "https://barbican.example/v1/secrets/approved"
		listenerTags, lbTags := listener["tags"], lb["tags"]
		listener["tags"] = append(safetyGatewayTags(t, routeID, roleListener), tcpTLSProfileTag)
		lb["tags"] = append(safetyGatewayTags(t, routeID, roleLoadBalancer), tcpTLSProfileTag)
		listener["tcp_tls_termination"], listener["default_tls_container_ref"], listener["sni_container_refs"] = true, certificate, []string{}
		pool["tls_enabled"], policy["project_id"] = false, "project-a"
		t.Cleanup(func() {
			listener["tags"], lb["tags"] = listenerTags, lbTags
			delete(listener, "tcp_tls_termination")
			delete(listener, "default_tls_container_ref")
			delete(listener, "sni_container_refs")
			delete(pool, "tls_enabled")
		})
		ensure := spec
		ensure.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolTLSTerminate
		ensure.Gateway.TCPTLSCertificateRef = certificate
		ensure.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{targets[0].route}
		provider := newTestProvider(t, clients, ProviderConfig{TCPTLSTermination: true})
		result, err := provider.ReconcileGatewayGraph(t.Context(), ensure)
		if err != nil || len(writes) != 0 || len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("terminated TLSRoute not ready, result=%#v writes=%v err=%v", result, writes, err)
		}
		observation, found, err := provider.GetGateway(t.Context(), ensure.Identity)
		if err != nil || !found || observation.Outcome.State != cloud.OutcomeProgressing || observation.State.ListenerID != "listener" {
			t.Fatalf("identity-only TCP TLS observation = %#v found=%t err=%v", observation, found, err)
		}
		capabilityReadFailed = true
		t.Cleanup(func() { capabilityReadFailed = false })
		if _, err := provider.ReconcileGatewayGraph(t.Context(), ensure); err != nil || len(writes) != 0 {
			t.Fatalf("an indeterminate capability read must preserve the verified terminating frontend: err=%v writes=%v", err, writes)
		}
		ensure.Gateway.TLSSNIRoutes = nil
		result, err = provider.ReconcileGatewayGraph(t.Context(), ensure)
		if err != nil || len(writes) != 1 || result.Outcome.State != cloud.OutcomeProgressing {
			t.Fatalf("terminated TLSRoute withdrawal = %#v writes=%v err=%v", result, writes, err)
		}
	})
	t.Run("same UID with different ownership tags is a conflict", func(t *testing.T) {
		writes, neutronFailed = nil, false
		policy["project_id"] = "project-a"
		conflicting := targets[0].route
		conflicting.Identity.RouteName = "conflicting-name"
		conflicts, err := buildSNITargets(spec.Identity, []cloud.TLSSNIRoute{conflicting})
		if err != nil {
			t.Fatal(err)
		}
		for role, resource := range map[string]map[string]any{roleSNIPool: pool, roleSNIMember: member, roleSNIPolicy: policy, roleSNIRule: rule} {
			resource["tags"], err = conflicts[0].tags(role)
			if err != nil {
				t.Fatal(err)
			}
		}
		spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{targets[0].route}
		provider := newTestProvider(t, clients, ProviderConfig{TLSSNIPassthrough: true})
		if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != 0 {
			t.Fatalf("a conflicting route UID cannot authorize a mutation: err=%v writes=%v", err, writes)
		}
	})
	t.Run("unproven listener binding cannot authorize weighted frontend withdrawal", func(t *testing.T) {
		writes, neutronFailed = nil, false
		binding := listener["protocol_port"]
		listener["protocol_port"] = spec.Gateway.Spec.ListenerPort + 1
		t.Cleanup(func() { listener["protocol_port"] = binding })
		ensure := spec
		ensure.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{largeSNIPoolsTestRoute(routeID)}
		provider := newTestProvider(t, clients, ProviderConfig{TLSSNIPassthrough: true})
		if _, err := provider.ReconcileGatewayGraph(t.Context(), ensure); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != 0 {
			t.Fatalf("an unproven listener binding cannot authorize a mutation: err=%v writes=%v", err, writes)
		}
	})
}

func TestSNIPendingLoadBalancerWithoutFragmentReadbackCannotDefer(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ExternalNetworkID = ""
	first := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "a.example.test", ServiceUID: "service-a", Members: []cloud.Member{{Address: "192.0.2.10", Port: 443}}}
	second := first
	second.Hostname = "b.example.test"
	targets, err := buildSNITargets(spec.Identity, []cloud.TLSSNIRoute{first, second})
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolTLSSNI, sni: targets}
	lb := safetyObservedLoadBalancer()
	lb.ProvisioningStatus, lb.AdminStateUp = "PENDING_UPDATE", true
	actual := observedGatewayGraph{loadBalancer: lb, outcome: cloud.ProgressingOutcome("Updating", 1),
		listener: &listeners.Listener{ID: "listener", AdminStateUp: true, ProvisioningStatus: "PENDING_UPDATE"}}
	p := &Provider{}
	if got := p.pendingGraphResult(t.Context(), desired, actual).Routes; len(got) != 1 || got[0].Result.ObservationDeferred {
		t.Fatalf("pending frontend without a route inventory cannot retain an existing proof, got %#v", got)
	}
	actual.sni = map[sniFragmentKey]*observedSNIFragment{targets[0].key: activeSNIFragment(targets[0], "a"), targets[1].key: activeSNIFragment(targets[1], "b")}
	lb.ProvisioningStatus = "PENDING_CREATE"
	if got := p.pendingGraphResult(t.Context(), desired, actual).Routes; len(got) != 1 || got[0].Result.ObservationDeferred {
		t.Fatalf("a pending new loadbalancer is not an eligible serving sibling, got %#v", got)
	}
}

func TestTCPTLSTerminationPublicFrontendLifecycle(t *testing.T) {
	t.Run("termination", func(t *testing.T) {
		testTCPTLSFrontendLifecycle(t, cloud.ListenerProtocolTLSTerminate, "tcp_tls_termination_v1")
	})
	t.Run("mixed", func(t *testing.T) {
		testTCPTLSFrontendLifecycle(t, cloud.ListenerProtocolTLSMixed, "tcp_tls_mixed_mode_v1")
	})
}

func testTCPTLSFrontendLifecycle(t *testing.T, protocol, requiredCapability string) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol = protocol
	mixed := protocol == cloud.ListenerProtocolTLSMixed
	profileTag := sniListenerProfileTag(protocol)
	const first, replacement = "https://barbican.example/v1/secrets/first", "https://barbican.example/v1/secrets/replacement"
	spec.Gateway.TCPTLSCertificateRef = first
	ids := func(id string) []map[string]string { return []map[string]string{{"id": id}} }
	lb := map[string]any{"id": "lb", "project_id": "project-a", "provider": "amphora",
		"provisioning_status": "ACTIVE", "admin_state_up": true, "listeners": []any{}, "pools": []any{},
		"vip_subnet_id": spec.Gateway.Spec.VIPSubnetID, "vip_port_id": "vip-port", "vip_address": "192.0.2.10",
		"tags": append(safetyGatewayTags(t, identity, roleLoadBalancer), profileTag)}
	var listener map[string]any
	poolRows, policyRows, ruleRows := []any{}, []any{}, []any{}
	capability, addressFailure := false, false
	replacementProfile := ""
	var writes []string
	clients := ServiceClients{
		ProjectID: "project-a",
		Network: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/floatingips" {
				t.Errorf("unexpected Neutron request: %s %s", r.Method, r.URL.Path)
			}
			if addressFailure {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			items := []any{}
			if replacementProfile != "" {
				items = append(items, map[string]any{
					"id": "fip", "project_id": "project-a", "description": identity.GatewayDescription(roleFloatingIP),
					"port_id": "vip-port", "fixed_ip_address": "192.0.2.10", "floating_ip_address": "203.0.113.10",
					"floating_network_id": spec.Gateway.Spec.ExternalNetworkID,
				})
			}
			safetyWriteJSON(t, w, map[string]any{"floatingips": items})
		})),
		LoadBalancer: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method + " " + r.URL.Path {
			case "GET /lbaas/loadbalancers":
				safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
			case "GET /lbaas/loadbalancers/lb":
				safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
			case "GET /lbaas/providers/amphora/gateway_capabilities":
				capabilities := []string{"tls_sni_passthrough_v1", "tls_sni_suffix_v1"}
				if capability {
					capabilities = append(capabilities, requiredCapability)
					if replacementProfile != "" && mixed {
						capabilities = append(capabilities, "tcp_tls_termination_v1")
					}
				}
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
			case "GET /lbaas/listeners":
				items := []any{}
				if listener != nil {
					items = append(items, listener)
				}
				safetyWriteJSON(t, w, map[string]any{"listeners": items})
			case "GET /lbaas/pools":
				safetyWriteJSON(t, w, map[string]any{"pools": poolRows})
			case "GET /lbaas/l7policies":
				safetyWriteJSON(t, w, map[string]any{"l7policies": policyRows})
			case "GET /lbaas/l7policies/policy/rules":
				safetyWriteJSON(t, w, map[string]any{"rules": ruleRows})
			case "GET /lbaas/pools/pool/members":
				safetyWriteJSON(t, w, map[string]any{"members": []any{}})
			case "POST /lbaas/pools":
				var body struct {
					Pool map[string]any `json:"pool"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Pool["protocol"] != "TCP" || body.Pool["tls_enabled"] != false {
					t.Errorf("TCP TLS route requires an explicit plaintext TCP pool, got %#v, %v", body.Pool, err)
				}
				writes = append(writes, "pool")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "pool"}})
			case "DELETE /lbaas/listeners/listener", "DELETE /lbaas/listeners/replacement-listener":
				if replacementProfile == "" || listener["admin_state_up"] != false {
					t.Fatal("old TLS listener must be closed before replacement")
				}
				listener, lb["listeners"] = nil, []any{}
				writes = append(writes, "delete-listener")
				w.WriteHeader(http.StatusNoContent)
			case "POST /lbaas/listeners", "PUT /lbaas/listeners/listener", "PUT /lbaas/listeners/replacement-listener":
				var body struct {
					Listener map[string]any `json:"listener"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				change := body.Listener
				if r.Method == http.MethodPost {
					profile := protocol
					if replacementProfile != "" {
						profile = replacementProfile
					}
					var wantTermination, wantMixed, wantCertificate any
					if cloud.IsTCPTLSListenerProtocol(profile) {
						wantTermination, wantCertificate = true, first
					}
					if profile == cloud.ListenerProtocolTLSMixed {
						wantMixed = true
					}
					if change["tcp_tls_termination"] != wantTermination || change["tcp_tls_mixed_mode"] != wantMixed ||
						change["default_tls_container_ref"] != wantCertificate || change["admin_state_up"] != false ||
						change["protocol"] != "TCP" || change["loadbalancer_id"] != "lb" || change["default_pool_id"] != nil {
						t.Errorf("unexpected %s listener create: %#v", profile, change)
					}
					listener = change
					listener["id"], listener["project_id"], listener["provisioning_status"] = "listener", "project-a", "ACTIVE"
					listener["loadbalancers"], listener["l7policies"], listener["default_pool_id"] = ids("lb"), []any{}, nil
					if replacementProfile != "" {
						listener["id"] = "replacement-listener"
					}
					listener["sni_container_refs"] = []string{}
					lb["listeners"] = ids(listener["id"].(string))
					writes = append(writes, "create")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
				} else {
					rotation := change["default_tls_container_ref"] != nil
					updatesCertificateSet := mixed && rotation && len(change) == 2 && change["sni_container_refs"] != nil
					if len(change) != 1 && !updatesCertificateSet {
						t.Errorf("unexpected TCP TLS listener mutation: %#v", change)
					}
					switch {
					case change["admin_state_up"] == false:
						writes = append(writes, "disable")
					case change["admin_state_up"] == true:
						writes = append(writes, "enable")
					case change["default_tls_container_ref"] == replacement && listener["admin_state_up"] == false:
						writes = append(writes, "rotate")
					default:
						t.Errorf("unexpected TCP TLS listener mutation: %#v", change)
					}
					for key, value := range change {
						listener[key] = value
					}
				}
				safetyWriteJSON(t, w, map[string]any{"listener": listener})
			default:
				t.Errorf("unexpected Octavia request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		})),
	}
	provider := newTestProvider(t, clients, ProviderConfig{TCPTLSTermination: true, TCPTLSMixedMode: mixed})
	check := func(p *Provider, spec cloud.GatewayGraphSpec, want cloud.OutcomeState, mutation string, wantErr error) {
		t.Helper()
		writes = nil
		result, err := p.ReconcileGatewayGraph(t.Context(), spec)
		if !errors.Is(err, wantErr) || (want != "" && result.Outcome.State != want) ||
			want == cloud.OutcomeReady && result.Gateway.Outcome.State != want ||
			(len(writes) != 0) != (mutation != "") || len(writes) > 1 || len(writes) > 0 && writes[0] != mutation {
			t.Fatalf("wanted outcome=%q mutation=%q error=%v, got result=%#v writes=%v err=%v", want, mutation, wantErr, result, writes, err)
		}
	}
	check(provider, spec, "", "", cloud.ErrDependencyNotReady)
	capability = true
	check(provider, spec, "", "create", nil)
	check(provider, spec, "", "enable", nil)
	check(provider, spec, cloud.OutcomeReady, "", nil)
	observation, found, err := provider.GetGateway(t.Context(), spec.Identity)
	if err != nil || !found || observation.Outcome.State != cloud.OutcomeProgressing || observation.State.ListenerID != "listener" {
		t.Fatalf("empty listener identity-only TCP TLS observation = %#v found=%t err=%v", observation, found, err)
	}
	delete(listener, "admin_state_up")
	check(provider, spec, cloud.OutcomeProgressing, "disable", nil)
	check(provider, spec, "", "enable", nil)
	routed := spec
	routed.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{{
		Identity: identity.Value(), Hostname: "tls.example.com", ServiceUID: "tcp-backend",
		Members: []cloud.Member{{Address: "192.0.2.101", Port: 3000}},
	}}
	check(provider, routed, cloud.OutcomeProgressing, "pool", nil)
	if mixed {
		targets, err := buildSNITargets(identity.Value(), routed.Gateway.TLSSNIRoutes)
		if err != nil {
			t.Fatal(err)
		}
		poolTags, _ := targets[0].tags(roleSNIPool)
		policyTags, _ := targets[0].tags(roleSNIPolicy)
		poolRows = []any{map[string]any{"id": "pool", "project_id": "project-a", "protocol": "TCP", "admin_state_up": true,
			"provisioning_status": "ACTIVE", "loadbalancers": ids("lb"), "listeners": []any{}, "members": []any{},
			"healthmonitor_id": nil, "tls_enabled": false, "tags": poolTags}}
		policyRows = []any{map[string]any{"id": "policy", "project_id": "project-a", "listener_id": "listener",
			"redirect_pool_id": "pool", "action": "REDIRECT_TO_POOL", "admin_state_up": true,
			"provisioning_status": "ACTIVE", "rules": []any{}, "tags": policyTags}}
		lb["pools"], listener["l7policies"] = ids("pool"), ids("policy")
		// A missing policy mode never authorizes forwarding. Close only
		// the owned mixed frontend and wait for coherent policy readback.
		check(provider, routed, cloud.OutcomeProgressing, "disable", nil)
		check(provider, routed, cloud.OutcomeProgressing, "", nil)
		ruleTags, _ := targets[0].tags(roleSNIRule)
		ruleRows = []any{map[string]any{"id": "rule", "project_id": "project-a", "type": "TLS_SNI",
			"compare_type": "EQUAL_TO", "value": "tls.example.com", "admin_state_up": true, "invert": false,
			"provisioning_status": "ACTIVE", "tags": ruleTags}}
		policy := policyRows[0].(map[string]any)
		policy["rules"], policy["tcp_tls_passthrough"] = ids("rule"), false
		poolRows[0].(map[string]any)["listeners"] = ids("listener")
		lb["pools"], listener["l7policies"] = []any{}, []any{}
		poolRows, policyRows = []any{}, []any{}
		check(provider, spec, "", "enable", nil)
	}

	// The whole frontend closes before certificate rotation, even during an
	// unrelated Neutron failure. It cannot reopen before coherent readback.
	addressFailure = true
	spec.Gateway.TCPTLSCertificateRef = ""
	check(provider, spec, "", "disable", nil)
	spec.Gateway.TCPTLSCertificateRef = replacement
	check(provider, spec, "", "rotate", nil)
	addressFailure = false
	check(provider, spec, "", "enable", nil)
	capability, addressFailure = false, true
	check(provider, spec, "", "disable", cloud.ErrDependencyNotReady)
	capability, addressFailure = true, false
	check(provider, spec, "", "enable", nil)
	delete(listener, "tcp_tls_termination")
	check(provider, spec, "", "disable", nil)
	check(provider, spec, "", "", nil)
	listener["tcp_tls_termination"] = true
	check(provider, spec, "", "enable", nil)
	if mixed {
		delete(listener, "tcp_tls_mixed_mode")
		check(provider, spec, "", "disable", nil)
		check(provider, spec, "", "", nil)
		listener["tcp_tls_mixed_mode"] = true
		check(provider, spec, "", "enable", nil)
	}
	delete(listener, "sni_container_refs")
	check(provider, spec, "", "disable", nil)
	listener["sni_container_refs"] = []string{}
	check(provider, spec, "", "enable", nil)
	restrict := spec
	restrict.Gateway.Disposition = cloud.GraphWithdrawTLS
	capability = false
	check(provider, restrict, "", "disable", cloud.ErrDependencyNotReady)
	capability = true
	check(provider, spec, "", "enable", nil)

	// A restarted controller with termination disabled can still withdraw a
	// verified binding. A separate passthrough profile is not eligible.
	spec.Gateway.Disposition, spec.Gateway.TCPTLSCertificateRef = cloud.GraphWithdrawTLS, ""
	optedOut := newTestProvider(t, clients, ProviderConfig{})
	listener["tags"] = append(safetyGatewayTags(t, identity, roleListener), sniProfileTag)
	if _, found, err := provider.GetGateway(t.Context(), spec.Identity); found || !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("identity-only read accepted a different listener profile: found=%t err=%v", found, err)
	}
	check(optedOut, spec, "", "", cloud.ErrOwnershipConflict)
	listener["tags"] = append(safetyGatewayTags(t, identity, roleListener), profileTag)
	check(optedOut, spec, "", "disable", nil)
	listener["provisioning_status"] = "ERROR"
	check(optedOut, spec, cloud.OutcomeProgressing, "disable", nil)
	listener["provisioning_status"] = "PENDING_UPDATE"
	check(optedOut, spec, cloud.OutcomeProgressing, "", nil)
	listener["provisioning_status"] = "ACTIVE"
	check(optedOut, spec, cloud.OutcomeReady, "", nil)

	if mixed {
		spec.Gateway.Spec.ExternalNetworkID = "external-network"
		passthrough := newTestProvider(t, clients, ProviderConfig{TLSSNIPassthrough: true})
		terminating := newTestProvider(t, clients, ProviderConfig{TCPTLSTermination: true})
		current := optedOut
		for _, profile := range []struct {
			name     string
			provider *Provider
		}{
			{cloud.ListenerProtocolTLSSNI, passthrough},
			{cloud.ListenerProtocolTLSMixed, provider},
			{cloud.ListenerProtocolTLSTerminate, terminating},
			{cloud.ListenerProtocolTLSMixed, provider},
		} {
			replacementProfile = profile.name
			cleanup := spec
			cleanup.Gateway.Disposition = cloud.GraphDelete
			cleanup.Gateway.RetainSNIAddress = true
			if listener["admin_state_up"] == true {
				check(current, cleanup, cloud.OutcomeProgressing, "disable", nil)
			}
			check(current, cleanup, cloud.OutcomeProgressing, "delete-listener", nil)
			check(current, cleanup, cloud.OutcomeReady, "", nil)

			spec.Gateway.Disposition = cloud.GraphEnsure
			spec.Gateway.Spec.ListenerProtocol = profile.name
			spec.Gateway.TCPTLSCertificateRef = ""
			if cloud.IsTCPTLSListenerProtocol(profile.name) {
				spec.Gateway.TCPTLSCertificateRef = first
			}
			check(profile.provider, spec, cloud.OutcomeProgressing, "create", nil)
			check(profile.provider, spec, cloud.OutcomeProgressing, "enable", nil)
			result, err := profile.provider.ReconcileGatewayGraph(t.Context(), spec)
			state := result.Gateway.State
			if err != nil || result.Outcome.State != cloud.OutcomeReady || state.LoadBalancerID != "lb" || state.ListenerID != "replacement-listener" ||
				state.VIPPortID != "vip-port" || state.VIPAddress != "192.0.2.10" || state.FloatingIPID != "fip" || state.FloatingIPAddress != "203.0.113.10" {
				t.Fatalf("%s replacement did not retain the Gateway addresses: %#v, %v", profile.name, result, err)
			}
			observed, found, err := profile.provider.GetGateway(t.Context(), spec.Identity)
			if err != nil || !found || observed.State.ListenerID != "replacement-listener" {
				t.Fatalf("%s replacement listener could not be observed: %#v, %t, %v", profile.name, observed, found, err)
			}
			current = profile.provider
		}
	}
}

func TestMixedSNIPoliciesAndWithdrawal(t *testing.T) {
	identity := safetyTestIdentity(t)
	terminate := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "tls.example.com", ServiceUID: "tcp-backend", ServicePort: 3000,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 3000}}}
	passthrough := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "*.example.com", Passthrough: true, ServiceUID: "tcp-backend", ServicePort: 8443,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 8443}}}
	passthrough.Identity.RouteUID, passthrough.Identity.RouteName = "pass-uid", "pass"
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol, spec.Gateway.Spec.ExternalNetworkID = cloud.ListenerProtocolTLSMixed, ""
	spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{passthrough, terminate}
	spec.Gateway.TCPTLSCertificateRef = "https://barbican.example/v1/secrets/server"

	policyModes := map[string]bool{}
	closedPool := ""
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /lbaas/l7policies":
			var body struct {
				Policy map[string]any `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			mode, present := body.Policy["tcp_tls_passthrough"].(bool)
			poolID, _ := body.Policy["redirect_pool_id"].(string)
			if !present || poolID == "" || body.Policy["action"] != "REDIRECT_TO_POOL" || body.Policy["admin_state_up"] != false {
				t.Fatalf("unsafe mixed TLS policy creation: %#v", body.Policy)
			}
			policyModes[poolID] = mode
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": "policy"}})
		case "PUT /lbaas/pools/pool-exact":
			var body struct {
				Pool map[string]any `json:"pool"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Pool["admin_state_up"] != false {
				t.Fatalf("retired exact selector must retain a closed pool, got %#v, %v", body.Pool, err)
			}
			closedPool = r.URL.Path
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "pool-exact"}})
		default:
			t.Fatalf("unexpected Octavia request: %s %s", r.Method, r.URL.Path)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{TCPTLSTermination: true, TCPTLSMixedMode: true})
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	exact, broad := desired.sni[0], desired.sni[1]
	if exact.route.Hostname != terminate.Hostname || broad.route.Hostname != passthrough.Hostname {
		t.Fatalf("exact terminating selector must precede passthrough suffix: %#v", desired.sni)
	}
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolTLSMixed, loadBalancer: safetyObservedLoadBalancer(),
		state:                    cloud.GatewayState{ListenerID: "listener", VIPAddress: "192.0.2.10"},
		listener:                 &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		rawTLSCapabilityVerified: true, rawTLSVerified: true,
		sni: map[sniFragmentKey]*observedSNIFragment{
			exact.key: activeSNIFragment(exact, "exact"),
			broad.key: activeSNIFragment(broad, "broad"),
		},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	for _, target := range desired.sni {
		fragment := actual.sni[target.key]
		fragment.passthrough = new(target.route.Passthrough)
		fragment.backendTLS.AdminObserved = true
		if err := provider.executeSNIMutation(t.Context(), desired, actual, sniMutation{
			kind: sniCreatePolicy, target: target, parentID: fragment.pool.ID,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(policyModes) != 2 || policyModes["pool-exact"] || !policyModes["pool-broad"] {
		t.Fatalf("Octavia received wrong policy modes: %#v", policyModes)
	}

	// Removing the terminating hostname must close its destinations while
	// keeping its selector in front of the remaining passthrough wildcard.
	desired.sni = []desiredSNIFragment{broad}
	steps, _, err := provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 1 || steps[0].kind != graphSNIMutation || steps[0].sni.kind != sniDisableBackendTLSPool {
		t.Fatalf("expected one restrictive pool mutation, got %#v, %v", steps, err)
	}
	if err := provider.executeGraphMutation(t.Context(), desired, actual, steps[0]); err != nil || closedPool != "/lbaas/pools/pool-exact" {
		t.Fatalf("withdrawal did not close only the former target: %s, %v", closedPool, err)
	}
	actual.sni[exact.key].pool.AdminStateUp = false
	steps, result, err := provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 0 || len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("the closed exact selector must shield the ready wildcard: %#v, %#v, %v", steps, result, err)
	}
}

func TestMixedSNISuffixWithdrawalPreservesExactReadback(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol, spec.Gateway.Spec.ExternalNetworkID = cloud.ListenerProtocolTLSMixed, ""
	exact := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "tls.example.com", ServiceUID: "tcp-backend", ServicePort: 3000,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 3000}}}
	wildcard := cloud.TLSSNIRoute{Identity: identity.Value(), Hostname: "*.example.com", Passthrough: true, ServiceUID: "tcp-backend", ServicePort: 8443,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 8443}}}
	wildcard.Identity.RouteUID, wildcard.Identity.RouteName = "suffix-uid", "suffix"
	targets, err := buildSNITargets(identity.Value(), []cloud.TLSSNIRoute{wildcard, exact})
	if err != nil {
		t.Fatal(err)
	}
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolTLSMixed, loadBalancer: safetyObservedLoadBalancer(),
		state:                    cloud.GatewayState{ListenerID: "listener", VIPAddress: "192.0.2.10"},
		listener:                 &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		rawTLSCapabilityVerified: true, rawTLSVerified: true,
		sni: map[sniFragmentKey]*observedSNIFragment{
			targets[0].key: activeSNIFragment(targets[0], "exact"),
			targets[1].key: activeSNIFragment(targets[1], "wildcard"),
		},
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp, actual.loadBalancer.Provider = "ACTIVE", true, "amphora"
	observedWildcard := actual.sni[targets[1].key]
	observedWildcard.passthrough = new(true)
	discoveries := 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			t.Fatalf("unexpected Octavia call: %s %s", r.Method, r.URL.Path)
		}
		discoveries++
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{"tcp_tls_mixed_mode_v1"}})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{TCPTLSTermination: true, TCPTLSMixedMode: true, TLSSNISuffix: true})
	// The wildcard has already disappeared from desired state. Active
	// readback still requires explicit suffix capability during withdrawal.
	desired := desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolTLSMixed, sni: targets[:1]}
	provider.isolateUnavailableSNISuffix(t.Context(), &desired, actual)
	steps, result, err := provider.buildSNIGraphPlan(desired, actual)
	if discoveries != 1 || err != nil || len(steps) != 1 || steps[0].deletion.resource != gatewayDisableL7Policy ||
		steps[0].deletion.resourceID != observedWildcard.policy.ID {
		t.Fatalf("only the wildcard policy should be disabled: %#v, %v", steps, err)
	}
	if len(result.Routes) != 1 || result.Routes[0].Identity != exact.Identity || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("the exact policy should remain independently ready: %#v", result.Routes)
	}
	observedWildcard.policy.AdminStateUp = false
	steps, _, err = provider.buildSNIGraphPlan(desired, actual)
	if err != nil || len(steps) != 0 {
		t.Fatalf("no frontend downtime is required after the wildcard closes: %#v, %v", steps, err)
	}
	provider.tlsSNISuffix = false
	wildcardKey := targets[1].key
	desired = desiredGatewayGraph{spec: spec, protocol: cloud.ListenerProtocolTLSMixed, sni: targets}
	provider.isolateUnavailableSNISuffix(t.Context(), &desired, actual)
	if discoveries != 1 || len(desired.sni) != 1 || !desired.sniPaused[wildcardKey] {
		t.Fatalf("a disabled suffix option must pause the wildcard without capability discovery: %#v", desired.sni)
	}
}
