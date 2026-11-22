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
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func TestInitialL7RulesRecoverLostCreateResponseBeforeActivation(t *testing.T) {
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	requests := &gatewayTransitionRequests{}
	base := convergedRouteGraphHandler(t, requests, identity, true, true)
	listener := map[string]any{
		"id": "listener-1", "protocol": "HTTP", "protocol_port": 80, "project_id": "project-a",
		"admin_state_up": true, "provisioning_status": "ACTIVE", "tags": safetyGatewayTags(t, identity, roleListener),
		"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "l7policies": []map[string]string{},
	}
	var policy map[string]any
	var rules []map[string]any
	exposePolicy, enablePolicy := true, true
	exposeRules := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
			requests.record(r)
			safetyWriteJSON(t, w, map[string][]string{"gateway_capabilities": {octavia.GatewayCapabilityL7PolicyInitialRules}})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/listeners":
			requests.record(r)
			safetyWriteJSON(t, w, map[string][]any{"listeners": {listener}})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies":
			requests.record(r)
			items := []map[string]any{}
			if policy != nil && exposePolicy {
				items = append(items, policy)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policies": items})
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/l7policies":
			requests.record(r)
			if policy != nil {
				t.Fatal("a lost response caused a duplicate policy")
			}
			var body struct {
				Policy map[string]any `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			policy = body.Policy
			initial, ok := policy["rules"].([]any)
			if !ok || len(initial) != 2 || policy["admin_state_up"] != false ||
				policy["action"] != "REDIRECT_TO_POOL" || policy["redirect_pool_id"] != "pool-1" || policy["position"] != float64(1) {
				t.Fatalf("grouped POST = %v", policy)
			}
			policyTags := append(safetyRouteTags(t, identity, rolePolicyExact), initialL7RulesTag)
			assertInitialL7Tags(t, policy["tags"], policyTags)
			want := map[string]struct{ role, compare, value string }{
				"PATH":      {roleRulePath, "EQUAL_TO", "/api"},
				"HOST_NAME": {roleRuleHost, "REGEX", `^api\.example\.com(:[0-9]+)?$`},
			}
			refs := make([]map[string]string, 0, len(initial))
			for _, entry := range initial {
				rule := entry.(map[string]any)
				ruleType := rule["type"].(string)
				expected, ok := want[ruleType]
				if !ok || rule["compare_type"] != expected.compare || rule["value"] != expected.value ||
					rule["invert"] != false || rule["admin_state_up"] != true {
					t.Fatalf("initial rule = %v", rule)
				}
				delete(want, ruleType)
				assertInitialL7Tags(t, rule["tags"], safetyRouteTags(t, identity, expected.role))
				id := "rule-" + ruleType
				rule["id"], rule["project_id"], rule["provisioning_status"] = id, "project-a", "ACTIVE"
				rules = append(rules, rule)
				refs = append(refs, map[string]string{"id": id})
			}
			policy["id"], policy["project_id"], policy["provisioning_status"], policy["rules"] = "policy-1", "project-a", "ACTIVE", refs
			listener["l7policies"] = []map[string]string{{"id": "policy-1"}}
			http.Error(w, "response lost after commit", http.StatusBadGateway)
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies/policy-1/rules":
			requests.record(r)
			safetyWriteJSON(t, w, map[string]any{"rules": rules[:exposeRules]})
		case r.Method == http.MethodPut && r.URL.Path == "/lbaas/l7policies/policy-1":
			requests.record(r)
			var body struct {
				Policy map[string]bool `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Policy) != 1 {
				t.Fatalf("update policy = %v, error = %v", body.Policy, err)
			}
			if state, found := body.Policy["admin_state_up"]; !found || state != enablePolicy {
				t.Fatalf("policy admin state = %v, want %t", body.Policy, enablePolicy)
			}
			if enablePolicy {
				policy["admin_state_up"] = true
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy": policy})
		case r.Method == http.MethodDelete && r.URL.Path == "/lbaas/l7policies/policy-1/rules/rule-HOST_NAME":
			requests.record(r)
			if policy["admin_state_up"] != false || policy["provisioning_status"] != "ACTIVE" {
				t.Fatalf("host predicate removed without confirmed policy closure: %v", policy)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			base.ServeHTTP(w, r)
		}
	})
	first := completeGraphProvider(t, handler, requests)
	first.initialL7Rules = true
	if _, err := first.ReconcileGatewayGraph(t.Context(), spec); err == nil {
		t.Fatal("expected the ambiguous create response")
	}
	// A fresh controller also preserves the proof requirement with the flag off.
	restarted := completeGraphProvider(t, handler, requests)
	exposePolicy = false
	for _, enabled := range []bool{true, false} {
		restarted.initialL7Rules = enabled
		if _, err := restarted.ReconcileGatewayGraph(t.Context(), spec); err == nil {
			t.Fatal("listener references an unlisted policy after the lost reply")
		}
	}
	exposePolicy = true
	exposeRules = 1
	_, _ = restarted.ReconcileGatewayGraph(t.Context(), spec)
	exposeRules = 2
	delete(rules[0], "invert")
	_, _ = restarted.ReconcileGatewayGraph(t.Context(), spec)
	if writes := requests.mutationPaths(); !slices.Equal(writes, []string{"/lbaas/l7policies"}) {
		t.Fatalf("writes before complete, active readback = %v", writes)
	}
	rules[0]["invert"] = false
	result, err := restarted.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("complete disabled batch reconcile = %v, %v", result.Outcome, err)
	}
	result, err = restarted.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("active batch reconcile = %v, %v", result.Outcome, err)
	}
	if writes := requests.mutationPaths(); !slices.Equal(writes, []string{"/lbaas/l7policies", "/lbaas/l7policies/policy-1"}) {
		t.Fatalf("policy creation and activation writes = %v", writes)
	}

	delete(policy, "admin_state_up")
	enablePolicy = false
	spec.Routes[0].Disposition = cloud.GraphDelete
	for range 2 {
		if _, err := restarted.ReconcileGatewayGraph(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
	}
	path := "/lbaas/l7policies/policy-1"
	if requests.count(http.MethodPut, path) != 3 || len(requests.mutationPaths()) != 4 {
		t.Fatalf("teardown without closure = %v", requests.mutationPaths())
	}
	policy["admin_state_up"] = false
	if _, err := restarted.ReconcileGatewayGraph(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if requests.count(http.MethodDelete, path+"/rules/rule-HOST_NAME") != 1 {
		t.Fatalf("teardown after closure = %v", requests.mutationPaths())
	}
}

func TestInitialL7RulesRequireExplicitCapabilityOnScopedHTTPS(t *testing.T) {
	for _, scenario := range []struct {
		name                                                            string
		enabled, advertised, stockEndpoint, singleRule, requestRedirect bool
	}{
		{name: "opted out", advertised: true},
		{name: "stock provider", enabled: true, stockEndpoint: true},
		{name: "regional opt-out", enabled: true},
		{name: "single rule", enabled: true, advertised: true, singleRule: true},
		{name: "regional opt-in", enabled: true, advertised: true},
		{name: "request-derived redirect", enabled: true, advertised: true, requestRedirect: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var probes, inventories, posts int
			batch := scenario.enabled && scenario.advertised && !scenario.singleRule
			wantAction := "REDIRECT_PREFIX"
			if scenario.requestRedirect {
				wantAction = "REDIRECT_REQUEST"
			}
			provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora-regional/gateway_capabilities":
					probes++
					if scenario.stockEndpoint {
						http.NotFound(w, r)
						return
					}
					capabilities := []string{}
					if scenario.advertised {
						capabilities = append(capabilities, octavia.GatewayCapabilityL7PolicyInitialRules)
					}
					if scenario.requestRedirect {
						capabilities = append(capabilities, octavia.GatewayCapabilityHTTPRequestRedirect)
					}
					safetyWriteJSON(t, w, map[string][]string{"gateway_capabilities": capabilities})
				case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies":
					inventories++
					safetyWriteJSON(t, w, map[string][]any{"l7policies": {}})
				case r.Method == http.MethodPost && r.URL.Path == "/lbaas/l7policies":
					posts++
					var body struct {
						Policy map[string]any `json:"l7policy"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					nested, hasRules := body.Policy["rules"].([]any)
					if hasRules != batch || batch && len(nested) < 2 || body.Policy["admin_state_up"] != false ||
						body.Policy["listener_id"] != "https-listener" || body.Policy["action"] != wantAction ||
						body.Policy["position"] != float64(1) {
						t.Fatalf("redirect request = %v", body.Policy)
					}
					if scenario.requestRedirect {
						target, ok := body.Policy["redirect_request"].(map[string]any)
						if !ok || target["scheme"] != "https" || target["port"] != float64(8443) {
							t.Fatalf("request-derived target = %v", body.Policy)
						}
					}
					refs := make([]map[string]string, 0, len(nested))
					for _, rule := range nested {
						refs = append(refs, map[string]string{"id": rule.(map[string]any)["type"].(string)})
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]any{"id": "id", "rules": refs}})
				default:
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
			}), &gatewayTransitionRequests{})
			provider.initialL7Rules = scenario.enabled
			spec := redirectGraphSpec(t)
			if scenario.requestRedirect {
				provider.httpRequestRedirect = true
				spec.Routes[0].Spec.HTTPRedirects[0].Prefix = ""
				spec.Routes[0].Spec.HTTPRedirects[0].Request = &cloud.HTTPRedirectRequest{Scheme: "https", Port: 8443}
			}
			desired, err := provider.buildDesiredGatewayGraph(spec)
			if err != nil {
				t.Fatal(err)
			}
			part := desired.routes[0].httpParts[0]
			wanted := part.policies[0]
			wanted.position = 1
			if scenario.singleRule {
				wanted.hostnameRuleValue = ""
			}
			lb := safetyObservedLoadBalancer()
			lb.Provider = "amphora-regional"
			actual := observedGatewayGraph{loadBalancer: lb, scoped: true, scopedListeners: []listeners.Listener{{
				ID: "https-listener", Protocol: terminatedHTTPS, ProjectID: "project-a", ProvisioningStatus: "ACTIVE",
				Loadbalancers: []listeners.LoadBalancerID{{ID: lb.ID}}, L7Policies: []l7policies.L7Policy{},
			}}}
			if scenario.stockEndpoint {
				actual.scopedListeners[0].L7Policies = nil
			}
			step, err := provider.prepareGraphMutation(t.Context(), desired, actual, graphMutation{
				kind: graphRouteMutation, routeIndex: 0, backendKey: part.backendKey, listenerID: "https-listener",
				route: routeMutation{kind: routeMutationCreatePolicy, policy: wanted},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := provider.executeGraphMutation(t.Context(), desired, actual, step); err != nil {
				t.Fatal(err)
			}
			wantProbes := 0
			if scenario.enabled && !scenario.singleRule {
				wantProbes++
			}
			if scenario.requestRedirect {
				wantProbes++
			}
			if probes != wantProbes || (inventories == 1) != batch || posts != 1 {
				t.Fatalf("capability probes = %d, inventory reads = %d, writes = %d", probes, inventories, posts)
			}
		})
	}
}

func TestInitialL7RulesPreserveWeightedTargetsAndRequestFilters(t *testing.T) {
	identity := safetyTestIdentity(t)
	desired := desiredRoute{identity: identity}
	desired.spec.Identity = identity.Value()
	desired.spec.Gateway.ListenerID = "listener"
	backendPath := "/selected"
	targets := []octavia.WeightedHTTPTarget{
		{Weight: 3, PoolID: "pool", RequestURLRewrite: &octavia.RequestURLRewrite{ReplaceFullPath: &backendPath}},
		fixedWeightedHTTPTarget(1),
	}
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/lbaas/l7policies" {
			t.Fatalf("unexpected weighted action request %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Policy map[string]json.RawMessage `json:"l7policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		got := body.Policy
		expectedTargets := `[{"weight":3,"pool_id":"pool","request_url_rewrite":{"replace_full_path":"/selected"}},{"weight":1,"fixed_response":{"status_code":500}}]`
		var tags []string
		if err := json.Unmarshal(got["tags"], &tags); err != nil ||
			!slices.Contains(tags, initialL7RulesTag) || !slices.Contains(tags, openstackidentity.BackendRequestFiltersPolicyTag) ||
			!slices.Contains(tags, openstackidentity.RequestURLRewritePolicyTag) || !slices.Contains(tags, openstackidentity.RequestCORSPolicyTag) {
			t.Fatalf("grouped creation lost a request filter or rule marker: %s", got["tags"])
		}
		if string(got["action"]) != `"WEIGHTED_TARGETS"` || string(got["admin_state_up"]) != "false" ||
			string(got["targets"]) != expectedTargets || string(got["request_cors"]) != `{"allow_origins":["https://client.example"]}` ||
			string(got["request_url_rewrite"]) != `{"hostname":"upstream.example.test"}` || len(got["rules"]) == 0 {
			t.Fatalf("weighted policy with initial rules = %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]any{"id": "weighted", "rules": []map[string]string{{"id": "one"}, {"id": "two"}}}})
	}), &gatewayTransitionRequests{})
	path := desiredPolicy{path: "/api", compareType: l7policies.CompareTypeEqual, role: rolePolicyExact,
		requestURLRewrite: &octavia.RequestURLRewrite{Hostname: "upstream.example.test"},
		requestCORS:       &cloud.HTTPCORS{AllowOrigins: []string{"https://client.example"}}}
	step := routeMutation{kind: routeMutationCreatePolicy, policy: path, weightedTargets: &targets}
	for _, rule := range rulesFor(path, "^example[.]test$") {
		opts, err := desired.createRuleOpts(rule)
		if err != nil {
			t.Fatal(err)
		}
		step.initialRules = append(step.initialRules, opts)
	}
	if err := provider.executeRouteMutation(t.Context(), desired, step); err != nil {
		t.Fatal(err)
	}
}

func TestInitialL7RulesHoldOnlyTheAffectedPort(t *testing.T) {
	held, heldGraph := convergedRoutePlanFixture()
	samePort, samePortGraph := convergedRoutePlanFixture()
	revoked, revokedGraph := convergedRoutePlanFixture()
	held.spec.ScopedListenerPorts, samePort.spec.ScopedListenerPorts, revoked.spec.ScopedListenerPorts = []int{80}, []int{80}, []int{8080}
	grouped := &heldGraph.policies[0]
	grouped.initialRules, grouped.adminObserved = true, true
	grouped.policy.ListenerID, grouped.policy.AdminStateUp, grouped.policy.ProvisioningStatus = "listener-80", false, "ACTIVE"
	grouped.rules[0].rule.ProvisioningStatus = "ACTIVE"
	// A rule was returned without its invert flag.
	grouped.rules[0].flagsObserved = false
	heldGraph.pool.ProvisioningStatus, heldGraph.members[0].ProvisioningStatus, heldGraph.monitor.ProvisioningStatus = "ACTIVE", "ACTIVE", "ACTIVE"

	samePort.policies[0].position, samePortGraph.policies[0].policy.Position = 2, 2
	samePortGraph.policies[0].policy.ID, samePortGraph.policies[0].policy.ListenerID = "same-port", "listener-80"
	samePortGraph.policies[0].policy.AdminStateUp = false
	samePortGraph.pool.ProvisioningStatus, samePortGraph.members[0].ProvisioningStatus, samePortGraph.monitor.ProvisioningStatus = "ACTIVE", "ACTIVE", "ACTIVE"
	revokedGraph.policies[0].policy.ID, revokedGraph.policies[0].policy.ListenerID = "revoked", "listener-8080"

	spec := cloud.GatewayGraphSpec{
		Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure,
			Spec: cloud.GatewaySpec{ScopedListeners: []cloud.ScopedListener{{Name: "first", Port: 80}, {Name: "other", Port: 8080}}}},
		Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}, {Disposition: cloud.GraphEnsure}, {Disposition: cloud.GraphDelete}},
	}
	desired := desiredGatewayGraph{spec: spec, routes: []desiredRoute{held, samePort, revoked}, protocol: cloud.ListenerProtocolHTTP}
	actual := observedGatewayGraph{
		loadBalancer: safetyObservedLoadBalancer(), scoped: true, protocol: cloud.ListenerProtocolHTTP,
		scopedListeners: []listeners.Listener{
			{ID: "listener-80", Protocol: cloud.ListenerProtocolHTTP, ProtocolPort: 80, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "listener-8080", Protocol: cloud.ListenerProtocolHTTP, ProtocolPort: 8080, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		},
		adminObserved: map[string]bool{"listener-80": true, "listener-8080": true},
		routes:        []observedRouteGraph{heldGraph, samePortGraph, revokedGraph},
	}
	actual.loadBalancer.AdminStateUp = true
	provider := &Provider{}
	plan, result, err := provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].route.kind != routeMutationDisablePolicy || plan[0].route.id != "revoked" {
		t.Fatalf("expected independent withdrawal, plan = %v, error = %v", plan, err)
	}
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("unverified route = %v", result.Routes[0].Result.Outcome)
	}

	desired.spec.Routes[2].Disposition = cloud.GraphPreserve
	plan, _, err = provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || slices.ContainsFunc(plan, func(step graphMutation) bool { return step.route.kind == routeMutationEnablePolicy }) {
		t.Fatalf("same-listener opening while readback is incomplete, plan = %v, error = %v", plan, err)
	}
	grouped.rules[0].flagsObserved = true
	plan, _, err = provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationEnablePolicy || plan[0].route.id != grouped.policy.ID {
		t.Fatalf("matching readback should restore policy order, plan = %v, error = %v", plan, err)
	}
}

func TestInitialL7RulesLeaveLegacySiblingCleanupEligible(t *testing.T) {
	identity := safetyTestIdentity(t)
	markedTags := append(safetyRouteTags(t, identity, rolePolicyExact), initialL7RulesTag)
	legacyTags := safetyRouteTags(t, identity, rolePolicyPrefix)
	rows := []map[string]any{
		{"id": "marked", "project_id": "project-a", "listener_id": "listener", "position": 1, "redirect_pool_id": "pool",
			"admin_state_up": false, "provisioning_status": "ACTIVE", "rules": []any{}, "tags": markedTags},
		{"id": "legacy", "position": 2, "admin_state_up": false, "provisioning_status": "ACTIVE", "tags": legacyTags},
	}
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/lbaas/l7policies":
			safetyWriteJSON(t, w, map[string]any{"l7policies": rows})
		case "/lbaas/l7policies/marked/rules", "/lbaas/l7policies/legacy/rules":
			safetyWriteJSON(t, w, map[string][]any{"rules": {}})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}), &gatewayTransitionRequests{})
	listener := &listeners.Listener{ID: "listener", L7Policies: []l7policies.L7Policy{{ID: "marked"}, {ID: "legacy"}}}
	inventory := routeGraphInventory{owners: []routeGraphOwner{{identity: identity}}, graphs: []observedRouteGraph{{}}}
	if err := provider.observeListenerPolicies(t.Context(), &inventory, listener, cloud.ListenerProtocolHTTP,
		map[string]poolRouteOwner{"pool": {}}, false); err != nil {
		t.Fatal(err)
	}
	plan := buildRouteDeletionPlan(inventory.graphs[0])
	if !slices.ContainsFunc(plan, func(step routeMutation) bool { return step.kind == routeMutationDeletePolicy && step.id == "legacy" }) {
		t.Fatalf("legacy sibling cleanup = %v", plan)
	}
}

func TestInitialL7RulesRebuildUnexpectedPathKeyWhileClosed(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	policy := &actual.policies[0]
	policy.initialRules = true
	policy.policy.AdminStateUp, policy.policy.ProvisioningStatus = false, "ACTIVE"
	path := &policy.rules[0]
	path.flagsObserved, path.rule.ProvisioningStatus, path.rule.Key = true, "ACTIVE", "stale"
	policy.policy.ListenerID = "listener"
	actual.pool.Listeners = []pools.ListenerID{{ID: "listener"}}
	if err := validateScopedHTTPRouteRelationships([]observedRouteGraph{actual}); err != nil {
		t.Fatalf("the owned pool link must allow withdrawal of an unknown policy state: %v", err)
	}
	check := func(kind routeMutationKind) {
		t.Helper()
		plan, _, err := buildRouteEnsurePlan(desired, actual)
		if err != nil || len(plan) != 1 || plan[0].kind != kind {
			t.Fatalf("plan = %v, error = %v, want %v", plan, err, kind)
		}
	}
	check(routeMutationDisablePolicy)
	policy.adminObserved = true
	check(routeMutationDeleteRule)
	policy.rules = nil
	check(routeMutationCreateRule)
	path.rule.Key = ""
	policy.rules = []observedRouteRule{*path}
	check(routeMutationEnablePolicy)

	// Complete rule readback cannot activate a policy whose tagged CORS
	// settings the region has stopped reporting.
	desired.policies[0].requestCORS = &cloud.HTTPCORS{AllowOrigins: []string{"https://client.example"}}
	policy.corsTagged = true
	if plan, _, err := buildRouteEnsurePlan(desired, actual); err != nil || len(plan) != 0 {
		t.Fatalf("grouped policy with missing CORS readback: plan %v, error %v", plan, err)
	}
	policy.requestCORS = desired.policies[0].requestCORS
	policy.corsReported, policy.corsPresent = true, true
	check(routeMutationEnablePolicy)
}

func assertInitialL7Tags(t *testing.T, raw any, want []string) {
	t.Helper()
	tags := raw.([]any)
	if len(tags) != len(want) {
		t.Fatalf("tags = %v, want %v", tags, want)
	}
	for _, tag := range want {
		if !slices.Contains(tags, any(tag)) {
			t.Fatalf("tags = %v, missing %s", tags, tag)
		}
	}
}
