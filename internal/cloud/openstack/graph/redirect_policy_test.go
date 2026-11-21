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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func redirectGraphSpec(t *testing.T) cloud.GatewayGraphSpec {
	t.Helper()
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathValue, route.PathType, route.Precedence = "", "", "", 4
	route.Members, route.MemberSubnetID, route.HealthMonitor = nil, "", cloud.HealthMonitorConfig{}
	route.HTTPRedirects = []cloud.HTTPRedirect{{
		Prefix: "https://moved.example.test:8443", StatusCode: 301,
		Matches: []cloud.HTTPPathMatch{{Hostname: "*.example.test", PathType: cloud.PathMatchPrefix, PathValue: "/app", Precedence: 4}},
	}}
	return spec
}

func TestStockRedirectUsesPolicyOnlyPublicAPI(t *testing.T) {
	spec := redirectGraphSpec(t)
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/lbaas/l7policies" {
			t.Fatalf("unexpected cloud request %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Policy map[string]any `json:"l7policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		got := request.Policy
		if got["action"] != "REDIRECT_PREFIX" || got["redirect_prefix"] != "https://moved.example.test:8443" ||
			got["redirect_http_code"] != float64(301) || got["admin_state_up"] != false ||
			got["redirect_pool_id"] != nil || got["listener_id"] != "listener" {
			t.Fatalf("stock redirect request = %v", got)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"l7policy":{"id":"redirect"}}`))
	}), &gatewayTransitionRequests{})
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	partDesired, _, _ := httpPlanningParts(desired, observedGatewayGraph{})
	if len(partDesired.routes) != 1 {
		t.Fatalf("redirect-only route has %d parts", len(partDesired.routes))
	}
	route := partDesired.routes[0]
	route.spec.Gateway.ListenerID = "listener"
	plan, _, err := buildRouteEnsurePlan(route, observedRouteGraph{})
	if err != nil || len(plan) != 1 || plan[0].kind != routeMutationCreatePolicy {
		t.Fatalf("new redirect plans only a policy, plan %v, error %v", plan, err)
	}
	if err := provider.executeRouteMutation(t.Context(), route, plan[0]); err != nil {
		t.Fatal(err)
	}
}

func TestStockRedirectProtectsHigherPriorityForwardingOnTheSamePort(t *testing.T) {
	spec := redirectGraphSpec(t)
	forward := completeGraphSpec(safetyTestIdentity(t)).Routes[0]
	forward.Spec.Identity.RouteName, forward.Spec.Identity.RouteUID = "forward", "forward"
	forward.Spec.Hostname, forward.Spec.PathType, forward.Spec.PathValue, forward.Spec.Precedence =
		"api.example.test", cloud.PathMatchExact, "/app/private", 1
	spec.Routes = append(spec.Routes, forward)

	provider := &Provider{projectID: "project-a"}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	route := desired.routes[1].httpParts[0]
	if !route.redirect || len(route.policies) != 2 {
		t.Fatalf("redirect physical policies = %v, want one outside and one inside the forwarding host", route.policies)
	}
	inversions := map[l7policies.RuleType]int{}
	for i, policy := range route.policies {
		if policy.position != int32(i+2) || policy.precedence != 4 {
			t.Fatalf("physical policy position = %d, rank = %d", policy.position, policy.precedence)
		}
		rules := rulesFor(policy, "")
		if len(rules) != 3 || rules[0].ruleType != l7policies.TypePath || rules[0].invert ||
			rules[1].ruleType != l7policies.TypeHostName || rules[1].invert || !rules[2].invert {
			t.Fatalf("redirect rules = %v, want positive path and host plus one exclusion", rules)
		}
		inversions[rules[2].ruleType]++
	}
	if inversions[l7policies.TypePath] != 1 || inversions[l7policies.TypeHostName] != 1 || route.policies[0].role == route.policies[1].role {
		t.Fatalf("guards = %v, policy roles = %s and %s", inversions, route.policies[0].role, route.policies[1].role)
	}
	guard := rulesFor(route.policies[0], "")[2]
	wire := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/lbaas/l7policies/redirect/rules" {
			t.Fatalf("unexpected wire request %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Rule map[string]any `json:"rule"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if got := request.Rule; got["invert"] != true || got["type"] != string(guard.ruleType) ||
			got["compare_type"] != string(guard.compareType) || got["value"] != guard.value {
			t.Fatalf("guard request = %v", got)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"rule":{"id":"guard"}}`))
	}), &gatewayTransitionRequests{})
	if err := wire.executeRouteMutation(t.Context(), route, routeMutation{
		kind: routeMutationCreateRule, parentID: "redirect", rule: guard,
	}); err != nil {
		t.Fatal(err)
	}

	spec.Routes[1].Spec.Precedence = 4
	if _, err := provider.buildDesiredGatewayGraph(spec); err == nil {
		t.Fatal("accepted a collision between distinct logical match ranks")
	}
	anchor := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "redirect", Port: anchor}, {Name: "forward", Port: anchor + 1}}
	spec.Routes[0].Spec.ScopedListenerPorts = []int{anchor}
	spec.Routes[1].Spec.ScopedListenerPorts = []int{anchor + 1}
	desired, err = provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, desiredRoute := range desired.routes {
		for _, part := range desiredRoute.httpParts {
			if part.redirect && (len(part.policies) != 1 || len(rulesFor(part.policies[0], "")) != 2) {
				t.Fatalf("other listener's higher-priority forwarding affected redirect: %v", part.policies)
			}
		}
	}
}

func TestStockRedirectStagesAndRepairsPoliciesFromReadback(t *testing.T) {
	spec := redirectGraphSpec(t)
	forward := completeGraphSpec(safetyTestIdentity(t)).Routes[0]
	forward.Spec.Identity.RouteName, forward.Spec.Identity.RouteUID = "forward", "forward"
	forward.Spec.Hostname, forward.Spec.PathType, forward.Spec.PathValue, forward.Spec.Precedence =
		"*.example.test", cloud.PathMatchExact, "/app/private", 1
	spec.Routes = append(spec.Routes, forward)
	provider := &Provider{projectID: "project-a"}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[1].httpParts[0]
	wanted := part.policies[0]
	actual := observedRouteGraph{policies: []observedRoutePolicy{{
		role: wanted.role, versioned: true, redirect: true,
		policy: l7policies.L7Policy{ID: "redirect", Position: wanted.position, Action: "REDIRECT_PREFIX",
			RedirectPrefix: wanted.redirect.prefix, RedirectHttpCode: wanted.redirect.statusCode,
			ProvisioningStatus: "ACTIVE"},
	}}}
	current := &actual.policies[0]
	expectFirst := func(want routeMutationKind) routeMutation {
		t.Helper()
		plan, state, err := buildRouteEnsurePlan(part, actual)
		if err != nil || len(plan) == 0 || plan[0].kind != want || state.PoolID != "" || len(state.MemberIDs) != 0 {
			t.Fatalf("plan = %v, pool/member state = %v, err = %v, want first mutation %v", plan, state, err, want)
		}
		return plan[0]
	}
	for i, wantedRule := range rulesFor(wanted, "") {
		step := expectFirst(routeMutationCreateRule)
		if step.rule != wantedRule {
			t.Fatalf("next rule = %v, want %v", step.rule, wantedRule)
		}
		current.rules = append(current.rules, observedRouteRule{role: wantedRule.role, rule: l7policies.Rule{
			ID: fmt.Sprintf("rule-%d", i), RuleType: string(wantedRule.ruleType), CompareType: string(wantedRule.compareType),
			Value: wantedRule.value, Invert: wantedRule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		}})
	}
	expectFirst(routeMutationEnablePolicy)
	current.policy.AdminStateUp = true
	if plan, state, err := buildRouteEnsurePlan(part, actual); err != nil || len(plan) != 0 || len(state.L7RuleIDs) != 3 {
		t.Fatalf("converged restart = plan %v, state %v, err %v", plan, state, err)
	}

	current.rules[2].rule.Invert = false
	expectFirst(routeMutationDisablePolicy)
	current.policy.AdminStateUp, current.policy.ProvisioningStatus = false, "ERROR"
	if step := expectFirst(routeMutationDisablePolicy); !step.retry {
		t.Fatal("failed policy reload should retry disabling")
	}
	current.policy.ProvisioningStatus = "ACTIVE"
	if step := expectFirst(routeMutationUpdateRule); !step.rule.invert {
		t.Fatal("repaired guard must be inverted")
	}
	current.rules[2].rule.Invert = true

	current.policy.AdminStateUp = true
	current.policy.Action, current.policy.RedirectPrefix = "REDIRECT_TO_URL", ""
	expectFirst(routeMutationDisablePolicy)
	current.policy.AdminStateUp = false
	if step := expectFirst(routeMutationUpdatePolicy); step.policy.redirect.prefix != wanted.redirect.prefix {
		t.Fatalf("wrong redirect target in repair %v", step)
	}

	current.policy.AdminStateUp = true
	if plan := buildRouteDeletionPlan(actual); len(plan) != 5 || plan[0].kind != routeMutationDisablePolicy ||
		plan[1].kind != routeMutationDeleteRule || plan[4].kind != routeMutationDeletePolicy {
		t.Fatalf("route withdrawal plan = %v", plan)
	}
	if plan, _ := provider.httpGatewayDeletionPlan([]observedRouteGraph{actual}, nil); len(plan.routes) != 5 ||
		plan.routes[0].resource != gatewayDisableL7Policy || plan.routes[4].resource != gatewayDeletionL7Policy {
		t.Fatalf("Gateway withdrawal plan = %v", plan.routes)
	}
}

func TestStockRedirectRejectsIncompleteOrForeignRuleReadback(t *testing.T) {
	spec := redirectGraphSpec(t)
	identity := safetyTestIdentity(t)
	provider := &Provider{projectID: "project-a"}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	wanted := desired.routes[0].httpParts[0].policies[0]
	policyTags := safetyRouteTags(t, identity, wanted.role)
	ruleTags := safetyRouteTags(t, identity, rulesFor(wanted, "")[0].role)
	listener := &listeners.Listener{ID: "listener", Protocol: cloud.ListenerProtocolHTTP, ProjectID: "project-a",
		L7Policies: []l7policies.L7Policy{{ID: "policy"}}}
	for _, test := range []struct {
		name     string
		invert   any
		ruleTags []string
		want     error
	}{
		{"omitted inversion flag", nil, ruleTags, cloud.ErrDependencyNotReady},
		{"foreign rule identity", false, slices.DeleteFunc(slices.Clone(ruleTags), func(tag string) bool {
			return slices.Contains(policyTags, tag)
		}), cloud.ErrOwnershipConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/lbaas/l7policies":
					_ = json.NewEncoder(w).Encode(map[string]any{"l7policies": []map[string]any{{
						"id": "policy", "listener_id": listener.ID, "project_id": "project-a", "position": 1,
						"tags": policyTags, "action": "REDIRECT_PREFIX", "redirect_pool_id": nil, "admin_state_up": false,
						"redirect_prefix": "https://moved.example.test:8443", "redirect_http_code": 301,
						"rules": []map[string]string{{"id": "rule"}},
					}}})
				case "/lbaas/l7policies/policy/rules":
					_ = json.NewEncoder(w).Encode(map[string]any{"rules": []map[string]any{{
						"id": "rule", "project_id": "project-a", "tags": test.ruleTags,
						"admin_state_up": true, "invert": test.invert, "type": "PATH", "compare_type": "REGEX", "value": "^/app(/|$)",
					}}})
				default:
					t.Fatalf("unexpected provider request %s", r.URL.Path)
				}
			}), &gatewayTransitionRequests{})
			inventory := routeGraphInventory{owners: []routeGraphOwner{{identity: identity}}, graphs: []observedRouteGraph{{}}}
			err := provider.observeListenerPolicies(t.Context(), &inventory, listener, cloud.ListenerProtocolHTTP, nil, make(map[int]string), true)
			if !errors.Is(err, test.want) {
				t.Fatalf("observe redirect = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStockRedirectWaitsForPreservedForwarding(t *testing.T) {
	spec := redirectGraphSpec(t)
	preserved := completeGraphSpec(safetyTestIdentity(t)).Routes[0]
	preserved.Spec.Identity.RouteName, preserved.Spec.Identity.RouteUID = "preserved", "preserved"
	preserved.Disposition = cloud.GraphPreserve
	spec.Routes = append(spec.Routes, preserved)

	provider := &Provider{projectID: "project-a"}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	actual := observedGatewayGraph{
		protocol:     cloud.ListenerProtocolHTTP,
		loadBalancer: &octavia.LoadBalancer{},
		listener:     &listeners.Listener{ID: "listener", AdminStateUp: true},
		routes:       make([]observedRouteGraph, 2),
	}
	actual.loadBalancer.AdminStateUp = true
	var redirectIndex, preservedIndex int
	for i, route := range desired.routes {
		if len(route.httpParts) == 0 {
			preservedIndex = i
			actual.routes[i].policies = []observedRoutePolicy{{policy: l7policies.L7Policy{
				ID: "preserved", ListenerID: "listener", Position: 2,
				ProvisioningStatus: "ACTIVE", AdminStateUp: true, Action: "REDIRECT_TO_POOL",
			}}}
			continue
		}
		redirectIndex = i
		part := route.httpParts[0]
		wanted := part.policies[0]
		observation := observedRoutePolicy{
			role: wanted.role, versioned: true, redirect: true,
			policy: l7policies.L7Policy{
				ID: "redirect", ListenerID: "listener", Position: 1,
				ProvisioningStatus: "ACTIVE", Action: "REDIRECT_PREFIX",
				RedirectPrefix: wanted.redirect.prefix, RedirectHttpCode: wanted.redirect.statusCode,
			},
		}
		for i, wantedRule := range rulesFor(wanted, "") {
			observation.rules = append(observation.rules, observedRouteRule{role: wantedRule.role, rule: l7policies.Rule{
				ID: fmt.Sprintf("rule-%d", i), RuleType: string(wantedRule.ruleType), CompareType: string(wantedRule.compareType),
				Value: wantedRule.value, Invert: wantedRule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
			}})
		}
		actual.routes[i].part(part.backendKey).policies = []observedRoutePolicy{observation}
	}

	plan, result, err := provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 0 || result.Routes[redirectIndex].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("activation over preserved forwarding = plan %v, result %v, error %v", plan, result, err)
	}
	// A previously active redirect is just as authoritative in HAProxy as a
	// newly enabled one. Once its forwarding peer cannot be proved, it must
	// stop serving until the peer's predicate and precedence are known.
	part := desired.routes[redirectIndex].httpParts[0]
	active := &actual.routes[redirectIndex].part(part.backendKey).policies[0].policy
	active.AdminStateUp = true
	plan, result, err = provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) == 0 || plan[0].route.kind != routeMutationDisablePolicy ||
		result.Routes[redirectIndex].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("active redirect over unproved forward: plan %v, result %v, error %v", plan, result, err)
	}
	active.AdminStateUp = false
	actual.routes[preservedIndex].policies = nil
	plan, _, err = provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].route.kind != routeMutationEnablePolicy {
		t.Fatalf("activation after forwarding withdrawal = plan %v, error %v", plan, err)
	}
}

func TestStockRedirectKeepsPolicyIdentityWhenAnUnrelatedRuleIsInserted(t *testing.T) {
	spec := redirectGraphSpec(t)
	provider := &Provider{projectID: "project-a"}
	before, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	existing := before.routes[0].httpParts[0].policies[0]
	moved := spec.Routes[0].Spec.HTTPRedirects[0]
	moved.RuleIndex = 1
	spec.Routes[0].Spec.HTTPRedirects = []cloud.HTTPRedirect{
		{Prefix: "https://another.example.test", StatusCode: 302, Matches: []cloud.HTTPPathMatch{{
			Hostname: "elsewhere.example.test", PathType: cloud.PathMatchExact, PathValue: "/unrelated", Precedence: 3,
		}}},
		moved,
	}
	after, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(after.routes[0].httpParts, func(part desiredRoute) bool {
		return slices.ContainsFunc(part.policies, func(policy desiredPolicy) bool { return policy.role == existing.role })
	})
	if index == -1 {
		t.Fatal("an unchanged redirect got a new policy identity")
	}
	replacement := after.routes[0].httpParts[index]
	if !slices.Equal(rulesFor(replacement.policies[0], ""), rulesFor(existing, "")) {
		t.Fatal("an unchanged redirect got new rule identities")
	}
	actual := observedRoutePolicy{
		role: existing.role, versioned: true, redirect: true,
		policy: l7policies.L7Policy{
			ID: "redirect", AdminStateUp: true, Position: existing.position, ProvisioningStatus: "ACTIVE",
			Action: "REDIRECT_PREFIX", RedirectPrefix: existing.redirect.prefix, RedirectHttpCode: existing.redirect.statusCode,
		},
	}
	for i, rule := range rulesFor(existing, "") {
		actual.rules = append(actual.rules, observedRouteRule{role: rule.role, rule: l7policies.Rule{
			ID: fmt.Sprintf("rule-%d", i), RuleType: string(rule.ruleType), CompareType: string(rule.compareType),
			Value: rule.value, Invert: rule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		}})
	}
	plan, disables, _ := buildRoutePolicyPlan(replacement, []observedRoutePolicy{actual}, "")
	if len(disables) != 0 || len(plan) != 1 || plan[0].kind != routeMutationMovePolicy {
		t.Fatalf("position change: plan %v, disables %v", plan, disables)
	}
	actual.policy.Position = replacement.policies[0].position
	plan, disables, _ = buildRoutePolicyPlan(replacement, []observedRoutePolicy{actual}, "")
	if len(disables) != 0 || len(plan) != 0 {
		t.Fatalf("after automatic Octavia renumbering: plan %v, disables %v", plan, disables)
	}
	duplicate := moved.Matches[0]
	duplicate.Precedence = 3
	spec.Routes[0].Spec.HTTPRedirects[0].Matches[0] = duplicate
	// Identical predicates in different HTTPRoute rules are legal. Only the
	// highest-priority action is reachable, so it owns the stable policy.
	for range 2 {
		winners, err := provider.buildDesiredGatewayGraph(spec)
		if err != nil || len(winners.routes[0].httpParts) != 1 {
			t.Fatalf("two ranked rules for the same match: graph %v, error %v", winners, err)
		}
		policy := winners.routes[0].httpParts[0].policies[0]
		if policy.redirect.prefix != "https://another.example.test" || policy.role != existing.role {
			t.Fatalf("wrong winning redirect %v with role %s", policy.redirect, policy.role)
		}
		slices.Reverse(spec.Routes[0].Spec.HTTPRedirects)
	}
}

func TestStockRedirectCleanupWaitsForReferencedPolicyMissingFromList(t *testing.T) {
	identity := safetyTestIdentity(t)
	for _, disposition := range []cloud.GraphDisposition{cloud.GraphPreserve, cloud.GraphDelete} {
		t.Run(string(disposition), func(t *testing.T) {
			spec := redirectGraphSpec(t)
			spec.Gateway.Disposition, spec.Routes[0].Disposition = disposition, cloud.GraphDelete
			requests := &gatewayTransitionRequests{}
			base := convergedRouteGraphHandler(t, requests, identity, true, true)
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/lbaas/listeners":
					requests.record(request)
					safetyWriteJSON(t, w, map[string]any{"listeners": []any{map[string]any{
						"id": "listener-1", "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
						"tags":       safetyGatewayTags(t, identity, roleListener),
						"l7policies": []map[string]string{{"id": "missing-redirect"}},
					}}})
				case "/lbaas/l7policies":
					requests.record(request)
					safetyWriteJSON(t, w, map[string]any{"l7policies": []any{}})
				case "/lbaas/pools":
					requests.record(request)
					safetyWriteJSON(t, w, map[string]any{"pools": []any{}})
				default:
					base.ServeHTTP(w, request)
				}
			})
			result, err := completeGraphProvider(t, handler, requests).ReconcileGatewayGraph(t.Context(), spec)
			if !errors.Is(err, cloud.ErrDependencyNotReady) || len(requests.mutationPaths()) != 0 || len(result.Routes) != 0 {
				t.Fatalf("incomplete policy inventory authorized cleanup: result %v, error %v, writes %v", result, err, requests.mutationPaths())
			}
		})
	}
}

func TestStockRedirectGuardsHigherFixedAndWeightedMatches(t *testing.T) {
	spec := redirectGraphSpec(t)
	route := &spec.Routes[0].Spec
	backend := completeGraphSpec(safetyTestIdentity(t)).Routes[0].Spec
	route.MemberSubnetID, route.HealthMonitor = backend.MemberSubnetID, backend.HealthMonitor
	route.FixedHTTPRules = []cloud.HTTPFixedRule{{
		RuleIndex: 1,
		Matches:   []cloud.HTTPPathMatch{{Hostname: "*.example.test", PathType: cloud.PathMatchExact, PathValue: "/app/missing", Precedence: 1}},
	}}
	route.WeightedHTTPRules = []cloud.HTTPWeightedRule{{
		RuleIndex: 2,
		Matches: []cloud.HTTPPathMatch{
			{Hostname: "*.example.test", PathType: cloud.PathMatchExact, PathValue: "/app/split", Precedence: 2},
			{Hostname: "*.example.test", PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 5},
		},
		Targets: []cloud.HTTPWeightedTarget{{Weight: 1}},
	}}
	p := &Provider{projectID: "project-a", fixedHTTPResponses: true, weightedHTTPTargets: true}
	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	var redirect desiredPolicy
	for _, part := range desired.routes[0].httpParts {
		if part.redirect {
			if len(part.policies) != 1 {
				t.Fatalf("redirect has %d policies, want one", len(part.policies))
			}
			redirect = part.policies[0]
		}
	}
	if redirect.redirect == nil || redirect.position != 3 {
		t.Fatalf("redirect policy belongs after the two exact matches: %#v", redirect)
	}
	guards := redirect.redirect.guards
	if len(guards) != 2 || guards[0].value != "/app/missing" || guards[1].value != "/app/split" {
		t.Fatalf("redirect must exclude both higher actions and allow the lower match: %#v", guards)
	}
}
