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
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

const batchActivationEndpoint = "/lbaas/listeners/listener-1/l7policy_activation"

func TestReconcileGatewayGraphBatchesPreparedL7Policies(t *testing.T) {
	f := preparedHTTPPolicies(t)
	f.spec.ValidateMutation = func(_ context.Context) error {
		if f.requests.count(http.MethodGet, batchActivationEndpoint) != 1 {
			t.Fatal("Kubernetes validation ran before the Octavia activation snapshot")
		}
		return nil
	}
	result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeProgressing || !result.Outcome.MutationSubmitted {
		t.Fatalf("batch submission = %v, %v", result.Outcome, err)
	}
	for _, route := range result.Routes {
		if route.Result.Outcome.State != cloud.OutcomeProgressing || !route.Result.Outcome.MutationSubmitted {
			t.Fatalf("route reported %+v after submission", route.Result.Outcome)
		}
	}
	want := []string{"policy-1", "policy-2"}
	if f.batch.ExpectedGraphRevision != f.revision("graph") || !slices.Equal(f.batch.ExpectedOrder, want) ||
		!slices.Equal(f.batch.Policies, []octavia.PolicyActivationRevision{
			{ID: want[0], Revision: f.revision(want[0])},
			{ID: want[1], Revision: f.revision(want[1])},
		}) {
		t.Fatalf("activation body = %+v", f.batch)
	}
	result, err = f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("activation readback = %v, %v", result.Outcome, err)
	}
	for _, route := range result.Routes {
		if route.Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("route is pending after activation readback: %+v", route.Result.Outcome)
		}
	}
	if writes := f.requests.mutationPaths(); !slices.Equal(writes, []string{batchActivationEndpoint}) {
		t.Fatalf("writes = %v", writes)
	}
}

func TestReconcileGatewayGraphHoldsInvalidL7ActivationSnapshot(t *testing.T) {
	for _, name := range []string{"outdated predicate", "foreign policy", "missing rules", "incomplete inventory", "changed option"} {
		t.Run(name, func(t *testing.T) {
			f := preparedHTTPPolicies(t)
			f.changeSnapshot = func(snapshot []map[string]any) []map[string]any {
				policy := snapshot[1]["policy"].(map[string]any)
				switch name {
				case "outdated predicate":
					policy["rules"].([]map[string]any)[0]["value"] = "/old"
				case "foreign policy":
					policy["project_id"] = "foreign-project"
				case "missing rules":
					delete(policy, "rules")
				case "incomplete inventory":
					return snapshot[:1]
				case "changed option":
					policy["request_header_operations"] = []map[string]string{{"op": "set", "name": "x-route", "value": "old"}}
				}
				return snapshot
			}
			result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
			category, _ := cloud.ErrorCategoryOf(err)
			if category != cloud.ErrorCategoryDependencyNotReady || result.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("stale snapshot = %v, %v", result.Outcome, err)
			}
			if writes := f.requests.mutationPaths(); len(writes) != 0 {
				t.Fatalf("wrote against an unproved snapshot: %v", writes)
			}
		})
	}
}

func TestReconcileGatewayGraphBatchesTwoPreparedL7PoliciesOnOneBackend(t *testing.T) {
	f := prepareHTTPPolicies(t, true)
	result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeProgressing || !result.Outcome.MutationSubmitted {
		t.Fatalf("activation for the shared backend = %v, %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
	}
	if len(f.batch.Policies) != 2 || !slices.Equal(f.requests.mutationPaths(), []string{batchActivationEndpoint}) {
		t.Fatalf("batch = %+v, writes = %v", f.batch, f.requests.mutationPaths())
	}
	result, err = f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("shared backend readback = %v, %v", result.Outcome, err)
	}
}

func TestReconcileGatewayGraphBatchesPathsAndExactHeaderWithoutHostname(t *testing.T) {
	matches := []cloud.HTTPPathMatch{
		{PathType: cloud.PathMatchExact, PathValue: "/fallback"},
		{PathType: cloud.PathMatchExact, PathValue: "/api", Precedence: 1, Headers: []cloud.HTTPHeaderMatch{{Name: "version", Value: "two"}}},
	}
	f := prepareHTTPPolicies(t, true, matches...)
	result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || !result.Outcome.MutationSubmitted || len(f.batch.Policies) != 2 || !slices.Equal(f.requests.mutationPaths(), []string{batchActivationEndpoint}) {
		t.Fatalf("activation without a hostname: outcome %v, error %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
	}
	result, err = f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("activation readback: outcome %v, error %v", result.Outcome, err)
	}
	t.Run("exact header capability unavailable", func(t *testing.T) {
		f := prepareHTTPPolicies(t, true, matches...)
		f.exactHeaders = false
		result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
		if err != nil || result.Outcome.State != cloud.OutcomeProgressing || !slices.Equal(f.requests.mutationPaths(), []string{"/lbaas/listeners/listener-1"}) {
			t.Fatalf("missing exact header capability: outcome %v, error %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
		}
	})
	t.Run("regional capabilities withdrawn after listener check", func(t *testing.T) {
		f := prepareHTTPPolicies(t, true, matches...)
		f.withdrawAfterDiscovery = true
		result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
		if err != nil || result.Outcome.State != cloud.OutcomeProgressing || len(f.requests.mutationPaths()) != 0 {
			t.Fatalf("withdrawal during activation: outcome %v, error %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
		}
		_, err = f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
		if err != nil || !slices.Equal(f.requests.mutationPaths(), []string{"/lbaas/listeners/listener-1"}) {
			t.Fatalf("listener withdrawal: error %v, writes %v", err, f.requests.mutationPaths())
		}
	})
}

func TestReconcileGatewayGraphHoldsL7ActivationAfterGraphDrift(t *testing.T) {
	for _, name := range []string{"reassigned listener", "updated member", "missing member flags"} {
		t.Run(name, func(t *testing.T) {
			f := preparedHTTPPolicies(t)
			f.changeGraph = func(graph map[string]any) {
				listener := graph["listener"].(map[string]any)
				members := graph["pools"].([]map[string]any)[0]["members"].([]map[string]any)
				switch name {
				case "reassigned listener":
					listener["tags"] = []string{"different-owner"}
				case "updated member":
					members[0]["address"] = "10.0.0.99"
				case "missing member flags":
					delete(members[0], "backup")
				}
			}
			result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
			category, _ := cloud.ErrorCategoryOf(err)
			if category != cloud.ErrorCategoryDependencyNotReady || result.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("graph drift = %v, %v", result.Outcome, err)
			}
			if writes := f.requests.mutationPaths(); len(writes) != 0 {
				t.Fatalf("wrote against a changed graph: %v", writes)
			}
		})
	}
}

func TestReconcileGatewayGraphReobservesL7ActivationAfterConflict(t *testing.T) {
	f := preparedHTTPPolicies(t)
	f.postStatus = http.StatusConflict
	result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeProgressing || result.Outcome.MutationSubmitted {
		t.Fatalf("conflict = %v, %v", result.Outcome, err)
	}
	if writes := f.requests.mutationPaths(); !slices.Equal(writes, []string{batchActivationEndpoint}) {
		t.Fatalf("conflict fell back to individual writes: %v", writes)
	}
	f.postStatus = http.StatusNoContent
	f.revisionSalt = 1
	if _, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec); err != nil {
		t.Fatal(err)
	}
	if f.batch.ExpectedGraphRevision != f.revision("graph") || f.batch.Policies[0].Revision != f.revision("policy-1") || f.requests.count(http.MethodGet, batchActivationEndpoint) != 2 {
		t.Fatalf("conflict retry reused stale evidence: %+v", f.batch)
	}
}

func TestReconcileGatewayGraphFencesL7ActivationWhenGrantChanges(t *testing.T) {
	f := preparedHTTPPolicies(t)
	errGrantRevoked := errors.New("the ReferenceGrant changed")
	f.spec.ValidateMutation = func(_ context.Context) error {
		if f.requests.count(http.MethodGet, batchActivationEndpoint) == 1 {
			return errGrantRevoked
		}
		return nil
	}
	if _, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, errGrantRevoked) {
		t.Fatalf("grant revalidation = %v", err)
	}
	if writes := f.requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("activation crossed the grant fence: %v", writes)
	}
}

func TestReconcileGatewayGraphWithoutL7BatchKeepsSinglePolicyActivation(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		f := preparedHTTPPolicies(t)
		f.provider.l7PolicyBatchActivation = false
		assertSinglePolicyActivation(t, f)
		if got := f.requests.count(http.MethodGet, batchActivationEndpoint); got != 0 {
			t.Fatalf("batch snapshot requests with opt-in disabled = %d", got)
		}
	})
	t.Run("capability absent", func(t *testing.T) {
		f := preparedHTTPPolicies(t)
		f.advertised = false
		assertSinglePolicyActivation(t, f)
		if got := f.requests.count(http.MethodGet, batchActivationEndpoint); got != 0 {
			t.Fatalf("batch snapshot requests without provider support = %d", got)
		}
	})
	t.Run("stock provider discovery endpoint absent", func(t *testing.T) {
		f := preparedHTTPPolicies(t)
		f.discoveryStatus = http.StatusNotFound
		assertSinglePolicyActivation(t, f)
	})
	t.Run("endpoint explicitly unsupported", func(t *testing.T) {
		f := preparedHTTPPolicies(t)
		f.getStatus = http.StatusNotImplemented
		assertSinglePolicyActivation(t, f)
	})
}

func TestReconcileGatewayGraphHoldsActivationAfterDiscoveryError(t *testing.T) {
	for _, test := range []struct {
		status   int
		category cloud.ErrorCategory
	}{
		{http.StatusForbidden, cloud.ErrorCategoryAuthorization},
		{http.StatusServiceUnavailable, cloud.ErrorCategoryRetryableService},
		{http.StatusConflict, ""},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			f := preparedHTTPPolicies(t)
			f.discoveryStatus = test.status
			_, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
			category, _ := cloud.ErrorCategoryOf(err)
			if err == nil || category != test.category || len(f.requests.mutationPaths()) != 0 {
				t.Fatalf("discovery HTTP %d returned %v and wrote %v", test.status, err, f.requests.mutationPaths())
			}
		})
	}
}

func assertSinglePolicyActivation(t *testing.T, f *preparedHTTPPolicyFixture) {
	t.Helper()
	result, err := f.provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err != nil || result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("individual activation = %v, %v", result.Outcome, err)
	}
	if writes := f.requests.mutationPaths(); !slices.Equal(writes, []string{"/lbaas/l7policies/policy-1"}) {
		t.Fatalf("writes = %v", writes)
	}
}

type preparedHTTPPolicyFixture struct {
	spec                   cloud.GatewayGraphSpec
	provider               *Provider
	requests               gatewayTransitionRequests
	batch                  octavia.PolicyActivationBatch
	advertised             bool
	exactHeaders           bool
	withdrawAfterDiscovery bool
	discoveryStatus        int
	getStatus              int
	postStatus             int
	revisionSalt           int
	changeSnapshot         func([]map[string]any) []map[string]any
	changeGraph            func(map[string]any)
}

func (f *preparedHTTPPolicyFixture) revision(id string) string {
	return fmt.Sprintf("%064x", int(id[len(id)-1])+f.revisionSalt)
}

func preparedHTTPPolicies(t *testing.T) *preparedHTTPPolicyFixture {
	t.Helper()
	return prepareHTTPPolicies(t, false)
}

func prepareHTTPPolicies(t *testing.T, sharedBackend bool, matches ...cloud.HTTPPathMatch) *preparedHTTPPolicyFixture {
	t.Helper()
	id := safetyTestIdentity(t)
	f := &preparedHTTPPolicyFixture{spec: completeGraphSpec(id), advertised: true, getStatus: http.StatusOK, postStatus: http.StatusNoContent}
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "http", Port: 80}}
	if !sharedBackend {
		second := f.spec.Routes[0]
		second.Spec.Identity.RouteUID, second.Spec.Identity.RouteName = "route-second", "second"
		f.spec.Routes = append(f.spec.Routes, second)
	}
	for i := range f.spec.Routes {
		route := &f.spec.Routes[i].Spec
		route.ScopedListenerPorts = []int{80}
		route.HTTPBackends = []cloud.HTTPBackend{
			{Namespace: "backends", Name: fmt.Sprintf("backend-%d", i+1), Port: 80, Members: route.Members,
				Matches: []cloud.HTTPPathMatch{{Hostname: route.Hostname, PathType: route.PathType, PathValue: fmt.Sprintf("/api-%d", i+1), Precedence: i}}},
		}
		if sharedBackend {
			route.HTTPBackends[0].Matches = append(route.HTTPBackends[0].Matches,
				cloud.HTTPPathMatch{Hostname: route.Hostname, PathType: route.PathType, PathValue: "/api-2", Precedence: 1})
		}
		if len(matches) != 0 {
			route.HTTPBackends[0].Matches = matches
			f.exactHeaders = slices.ContainsFunc(matches, func(match cloud.HTTPPathMatch) bool { return len(match.Headers) != 0 })
		}
		route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	}

	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["listeners"], lb["pools"] = []map[string]string{{"id": "listener-1"}}, []map[string]string{}
	listener := map[string]any{
		"id": "listener-1", "protocol": "HTTP", "protocol_port": 80, "project_id": "project-a",
		"admin_state_up": true, "provisioning_status": "ACTIVE", "tags": safetyGatewayTags(t, id, roleListener),
		"default_pool_id": nil, "loadbalancers": []map[string]string{{"id": "load-balancer-1"}},
		"l7policies": []map[string]string{{"id": "policy-1"}, {"id": "policy-2"}},
	}
	var policies, pools []map[string]any
	rules, members, monitors := map[string][]map[string]any{}, map[string][]map[string]any{}, map[string][]map[string]any{}
	enable := func(policyID string) {
		for _, policy := range policies {
			if policy["id"] != policyID {
				continue
			}
			policy["admin_state_up"] = true
			for _, pool := range pools {
				if pool["id"] == policy["redirect_pool_id"] {
					pool["listeners"] = []map[string]string{{"id": "listener-1"}}
				}
			}
		}
	}
	f.provider = completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.record(r)
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/lbaas/loadbalancers":
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
		case r.Method == http.MethodGet && path == "/lbaas/loadbalancers/load-balancer-1":
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
		case r.Method == http.MethodGet && path == "/lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{listener}})
		case r.Method == http.MethodGet && path == "/lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": pools})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/members"):
			safetyWriteJSON(t, w, map[string]any{"members": members[strings.Split(path, "/")[3]]})
		case r.Method == http.MethodGet && path == "/lbaas/healthmonitors":
			safetyWriteJSON(t, w, map[string]any{"healthmonitors": monitors[r.URL.Query().Get("pool_id")]})
		case r.Method == http.MethodGet && path == "/lbaas/l7policies":
			safetyWriteJSON(t, w, map[string]any{"l7policies": policies})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/rules"):
			safetyWriteJSON(t, w, map[string]any{"rules": rules[strings.Split(path, "/")[3]]})
		case r.Method == http.MethodGet && path == "/lbaas/providers/amphora/gateway_capabilities":
			if f.discoveryStatus != 0 {
				w.WriteHeader(f.discoveryStatus)
				return
			}
			capabilities := []string{}
			if f.advertised {
				capabilities = append(capabilities, octavia.GatewayCapabilityL7PolicyBatchActivation)
			}
			if f.exactHeaders {
				capabilities = append(capabilities, octavia.GatewayCapabilityHTTPHeaderExact)
			}
			if f.withdrawAfterDiscovery {
				f.exactHeaders, f.advertised = false, false
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case r.Method == http.MethodGet && path == batchActivationEndpoint:
			if f.getStatus != http.StatusOK {
				w.WriteHeader(f.getStatus)
				return
			}
			var items []map[string]any
			for _, row := range policies {
				policy := maps.Clone(row)
				id := row["id"].(string)
				policyRules := make([]map[string]any, 0, len(rules[id]))
				for _, rule := range rules[id] {
					policyRules = append(policyRules, maps.Clone(rule))
				}
				policy["rules"] = policyRules
				items = append(items, map[string]any{"policy": policy, "revision": f.revision(id)})
			}
			if f.changeSnapshot != nil {
				items = f.changeSnapshot(items)
			}
			var destinations []map[string]any
			for _, row := range pools {
				pool := maps.Clone(row)
				id := pool["id"].(string)
				pool["healthmonitor"] = monitors[id][0]
				var poolMembers []map[string]any
				for _, member := range members[id] {
					row := maps.Clone(member)
					row["pool_id"] = id
					poolMembers = append(poolMembers, row)
				}
				destinations = append(destinations, map[string]any{"pool": pool, "members": poolMembers})
			}
			graph := map[string]any{
				"graph_revision": f.revision("graph"), "loadbalancer": maps.Clone(lb), "listener": maps.Clone(listener),
				"pools": destinations, "policies": items,
			}
			if f.changeGraph != nil {
				f.changeGraph(graph)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy_activation": graph})
		case r.Method == http.MethodPost && path == batchActivationEndpoint:
			var body struct {
				Batch octavia.PolicyActivationBatch `json:"l7policy_activation"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			f.batch = body.Batch
			if f.postStatus == http.StatusNoContent {
				for _, item := range f.batch.Policies {
					enable(item.ID)
				}
			}
			w.WriteHeader(f.postStatus)
		case r.Method == http.MethodPut && path == "/lbaas/listeners/listener-1":
			var body struct{ Listener map[string]any }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Listener["admin_state_up"] != false {
				t.Fatalf("expected listener withdrawal, got %v, %v", body, err)
			}
			listener["admin_state_up"] = false
			safetyWriteJSON(t, w, map[string]any{"listener": listener})
		case r.Method == http.MethodPut && strings.HasPrefix(path, "/lbaas/l7policies/"):
			id := strings.TrimPrefix(path, "/lbaas/l7policies/")
			enable(id)
			safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": id}})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL)
		}
	}), &f.requests)
	f.provider.l7PolicyBatchActivation = true
	f.provider.httpHeaderExact = f.exactHeaders
	desired, err := f.provider.buildDesiredGatewayGraph(f.spec)
	if err != nil {
		t.Fatal(err)
	}
	for i, route := range desired.routes {
		part := route.httpParts[0]
		poolID := fmt.Sprintf("pool-%d", i+1)
		lb["pools"] = append(lb["pools"].([]map[string]string), map[string]string{"id": poolID})
		memberID, monitorID := fmt.Sprintf("member-%d", i+1), fmt.Sprintf("monitor-%d", i+1)
		pools = append(pools, map[string]any{
			"id": poolID, "project_id": "project-a", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN",
			"admin_state_up": true, "provisioning_status": "ACTIVE", "healthmonitor_id": monitorID, "tls_enabled": false, "backend_h2c": false,
			"tags": safetyRouteTags(t, part.identity, part.httpRole(rolePool, "")), "loadbalancers": []map[string]string{{"id": "load-balancer-1"}},
			"members": []map[string]string{{"id": memberID}}, "listeners": []any{},
		})
		member := part.members[0]
		members[poolID] = []map[string]any{{
			"id": memberID, "project_id": "project-a", "address": member.Address, "protocol_port": member.Port,
			"subnet_id": "member-subnet", "admin_state_up": true, "provisioning_status": "ACTIVE", "weight": 1, "backup": false,
			"tags": safetyRouteTags(t, part.identity, part.httpRole(roleMember, "")),
		}}
		monitors[poolID] = []map[string]any{{
			"id": monitorID, "project_id": "project-a", "type": "HTTP", "delay": 10, "timeout": 5,
			"max_retries": 3, "max_retries_down": 3, "url_path": "/healthz", "http_method": "GET", "expected_codes": "200-399",
			"admin_state_up": true, "provisioning_status": "ACTIVE",
			"tags": safetyRouteTags(t, part.identity, part.httpRole(roleMonitor, "")), "pools": []map[string]string{{"id": poolID}},
		}}
		for _, wanted := range part.policies {
			policyID := fmt.Sprintf("policy-%d", wanted.position)
			var ruleRefs []map[string]string
			for j, rule := range rulesFor(wanted, part.hostnameRuleValue) {
				ruleID := fmt.Sprintf("rule-%d-%d", wanted.position, j+1)
				rules[policyID] = append(rules[policyID], map[string]any{
					"id": ruleID, "project_id": "project-a", "type": rule.ruleType, "compare_type": rule.compareType,
					"key": rule.key, "value": rule.value, "invert": rule.invert, "admin_state_up": true, "provisioning_status": "ACTIVE",
					"tags": safetyRouteTags(t, part.identity, rule.role),
				})
				ruleRefs = append(ruleRefs, map[string]string{"id": ruleID})
			}
			policies = append(policies, map[string]any{
				"id": policyID, "project_id": "project-a", "listener_id": "listener-1", "position": wanted.position,
				"action": "REDIRECT_TO_POOL", "redirect_pool_id": poolID, "admin_state_up": false, "provisioning_status": "ACTIVE",
				"tags": append(safetyRouteTags(t, part.identity, wanted.role), initialL7RulesTag), "rules": ruleRefs,
			})
		}
	}
	return f
}
