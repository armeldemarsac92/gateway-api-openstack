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
	"errors"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func TestInvalidHTTPBackendChangesOnlyItsPolicyTarget(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	desired.fixedResponse, desired.unavailable = true, true
	desired.members = nil
	actual.members[0].ProvisioningStatus = "ACTIVE"

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].kind != routeMutationDisableMember {
		t.Fatalf("withdraw enabled member first: plan = %#v, error = %v", plan, err)
	}

	actual.members[0].AdminStateUp = false
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	want := []routeMutation{{kind: routeMutationSetPolicyTarget, id: "policy-1"}}
	if err != nil || !reflect.DeepEqual(plan, want) {
		t.Fatalf("change the enabled policy in place: plan = %#v, error = %v", plan, err)
	}

	policy := &actual.policies[0]
	policy.policy.Action, policy.policy.RedirectPoolID = "FIXED_RESPONSE", ""
	policy.fixedResponse = true
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	if err != nil || len(plan) != 0 {
		t.Fatalf("verified 500 policy should be stable: plan = %#v, error = %v", plan, err)
	}

	desired.fixedResponse, desired.unavailable = false, false
	desired.members = []cloud.Member{{Address: "10.0.0.10", Port: 30080}}
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	want[0].parentID = "pool-1"
	if err != nil || len(plan) != 2 || plan[0].kind != routeMutationUpdateMember || !reflect.DeepEqual(plan[1], want[0]) {
		t.Fatalf("prepare the original pool and restore it in place: plan = %#v, error = %v", plan, err)
	}

	actual.members[0].AdminStateUp = true
	policy.fixedResponse, policy.targetObserved = false, true
	policy.policy.Action, policy.policy.RedirectPoolID = string(l7policies.ActionRedirectToPool), "pool-1"
	policy.policy.ProvisioningStatus = "ERROR"
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	if err != nil || !reflect.DeepEqual(plan, want) {
		t.Fatalf("retry the forwarding target in place after a failed reload: plan = %#v, error = %v", plan, err)
	}
}

func TestInvalidHTTPBackendSwitchesPolicyWhenMemberDisableFails(t *testing.T) {
	route, actual := convergedRoutePlanFixture()
	route.fixedResponse, route.unavailable, route.backendKey = true, true, "backend"
	route.members = nil
	actual.members[0].ProvisioningStatus, actual.members[0].AdminStateUp = "ERROR", false
	policy := &actual.policies[0]
	policy.targetObserved = true
	policy.policy.ProvisioningStatus, policy.rules[0].rule.ProvisioningStatus = "ACTIVE", "ACTIVE"
	desired, observed := fixedHTTPPlanFixture(desiredRoute{httpParts: []desiredRoute{route}},
		observedRouteGraph{httpParts: map[string]*observedRouteGraph{route.backendKey: &actual}})

	check := func(want routeMutationKind, id string) routeMutation {
		t.Helper()
		plan, _, err := (&Provider{}).buildGatewayGraphPlan(desired, observed)
		if err != nil || len(plan) != 1 || plan[0].route.kind != want || plan[0].route.id != id || !plan[0].withdrawHTTPPolicy {
			t.Fatalf("want priority for %v on %s, plan = %#v, error = %v", want, id, plan, err)
		}
		return plan[0].route
	}
	policy.rules[0].rule.Value = "/unexpected"
	check(routeMutationDisableMember, actual.members[0].ID)
	policy.rules[0].rule.Value = route.policies[0].path
	policy.responseHeaderOperations = []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderRemove, Name: "x-old"}}
	route.policies[0].responseHeaderOperations = []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}
	change := check(routeMutationSetPolicyTarget, policy.policy.ID)
	if !reflect.DeepEqual(change.responseHeaderOperations, route.policies[0].responseHeaderOperations) {
		t.Fatalf("fixed target omitted the response header replacement: %#v", change)
	}
	route.policies[0].responseHeaderOperations = nil
	clear := check(routeMutationSetPolicyTarget, policy.policy.ID)
	if clear.responseHeaderOperations == nil || len(clear.responseHeaderOperations) != 0 {
		t.Fatalf("fixed target omitted the response header clear: %#v", clear)
	}
	policy.fixedResponse, policy.policy.ProvisioningStatus = true, "ERROR"
	policy.policy.Action, policy.policy.RedirectPoolID = "FIXED_RESPONSE", ""
	check(routeMutationSetPolicyTarget, policy.policy.ID)
	policy.policy.ProvisioningStatus = "ACTIVE"
	check(routeMutationDisableMember, actual.members[0].ID)
}

func TestInvalidHTTPBackendWithdrawsLegacyPoolBeforeStaging(t *testing.T) {
	for _, test := range []struct {
		name             string
		old, replacement []desiredPolicy
	}{
		{name: "exact", old: policiesFor("/api", ""), replacement: policiesFor("/api", "")},
		{name: "prefix pair", old: policiesFor("/api", "/api/"), replacement: []desiredPolicy{{
			role: rolePolicyPrefix, path: "^/api($|/)", compareType: l7policies.CompareTypeRegex,
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, actual := convergedRoutePlanFixture()
			actual.members[0].ProvisioningStatus = "ACTIVE"
			actual.policies = nil
			for i, wanted := range test.old {
				policy := observedRoutePolicy{role: wanted.role, policy: l7policies.L7Policy{
					ID: wanted.role, ListenerID: "listener-1", Position: int32(i + 1), AdminStateUp: true,
					Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: actual.pool.ID, ProvisioningStatus: "ACTIVE",
				}}
				for _, rule := range rulesFor(wanted, "") {
					policy.rules = append(policy.rules, observedRouteRule{role: rule.role, rule: l7policies.Rule{
						RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value,
						AdminStateUp: true, ProvisioningStatus: "ACTIVE",
					}})
				}
				actual.policies = append(actual.policies, policy)
			}
			replacement := desiredRoute{fixedResponse: true, unavailable: true, backendKey: "replacement", policies: test.replacement}
			desired, observed := fixedHTTPPlanFixture(desiredRoute{httpParts: []desiredRoute{replacement}}, actual)
			check := func(want routeMutationKind, backend string) graphMutation {
				t.Helper()
				plan, _, err := (&Provider{}).buildGatewayGraphPlan(desired, observed)
				if err != nil || len(plan) != 1 || plan[0].route.kind != want || plan[0].backendKey != backend || !plan[0].withdrawHTTPPolicy {
					t.Fatalf("want priority for %v on backend %q, plan = %#v, error = %v", want, backend, plan, err)
				}
				return plan[0]
			}
			if step := check(routeMutationDisableMember, ""); step.route.id != actual.members[0].ID || step.route.parentID != actual.pool.ID {
				t.Fatalf("legacy withdrawal selected a different member: %#v", step)
			}
			actual.members[0].AdminStateUp = false
			actual.members[0].ProvisioningStatus = "ERROR"
			check(routeMutationCreatePolicy, replacement.backendKey)
			actual.members[0].ProvisioningStatus = "ACTIVE"
			observed.routes[0].memberAdminUnreported = map[string]struct{}{actual.members[0].ID: {}}
			check(routeMutationCreatePolicy, replacement.backendKey)
			observed.routes[0].memberAdminUnreported = nil
			check(routeMutationCreatePolicy, replacement.backendKey)

			// On a failed member withdrawal, the staged policy must remain
			// disabled until its identical predicate has been read back.
			actual.members[0].ProvisioningStatus = "ERROR"
			for i := range observed.routes[0].policies {
				observed.routes[0].policies[i].policy.Position++
			}
			wanted := replacement.policies[0]
			staged := observedRoutePolicy{role: wanted.role, fixedResponse: true, targetObserved: true, policy: l7policies.L7Policy{
				ID: "fixed-policy", ListenerID: "listener-1", Position: 1, Action: "FIXED_RESPONSE", ProvisioningStatus: "ACTIVE",
			}}
			newBackend := &observedRouteGraph{policies: []observedRoutePolicy{staged}}
			observed.routes[0].httpParts = map[string]*observedRouteGraph{replacement.backendKey: newBackend}
			check(routeMutationCreateRule, replacement.backendKey)
			for _, rule := range rulesFor(wanted, "") {
				newBackend.policies[0].rules = append(newBackend.policies[0].rules, observedRouteRule{role: rule.role, rule: l7policies.Rule{
					RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value,
					AdminStateUp: true, ProvisioningStatus: "ACTIVE",
				}})
			}
			check(routeMutationEnablePolicy, replacement.backendKey)
			newBackend.policies[0].policy.AdminStateUp = true
			check(routeMutationDisablePolicy, "")
		})
	}
}

func TestFixedHTTPBackendHandoverKeepsSharedPoolServing(t *testing.T) {
	kept, shared := convergedRoutePlanFixture()
	kept.backendKey = "shared"
	replaced := shared.policies[0]
	replaced.role, replaced.policy.ID = "replaced-role", "replaced-policy"
	replaced.rules = []observedRouteRule{{role: roleRulePath, rule: l7policies.Rule{
		RuleType: string(l7policies.TypePath), CompareType: string(l7policies.CompareTypeEqual),
		Value: "/revoked", AdminStateUp: true, ProvisioningStatus: "ACTIVE",
	}}}
	replaced.policy.Position, replaced.policy.ProvisioningStatus = 2, "ACTIVE"
	shared.policies = append(shared.policies, replaced)
	shared.members[0].ProvisioningStatus = "ACTIVE"
	fixed := desiredRoute{fixedResponse: true, unavailable: true, backendKey: "replacement", policies: []desiredPolicy{{
		role: "replacement-role", path: "/revoked", compareType: l7policies.CompareTypeEqual, position: 2,
	}}}
	desired, observed := fixedHTTPPlanFixture(desiredRoute{httpParts: []desiredRoute{kept, fixed}},
		observedRouteGraph{httpParts: map[string]*observedRouteGraph{kept.backendKey: &shared}})

	plan, _, err := (&Provider{}).buildGatewayGraphPlan(desired, observed)
	if err != nil || len(plan) != 1 || plan[0].backendKey != fixed.backendKey ||
		plan[0].route.kind != routeMutationCreatePolicy || plan[0].route.policy.position != 2 || !plan[0].withdrawHTTPPolicy {
		t.Fatalf("want fixed policy before the revoked match on an otherwise healthy pool, plan = %#v, error = %v", plan, err)
	}
}

func fixedHTTPPlanFixture(route desiredRoute, observed observedRouteGraph) (desiredGatewayGraph, observedGatewayGraph) {
	desired := desiredGatewayGraph{
		spec: cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure},
			Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}}},
		routes: []desiredRoute{route}, fixedHTTPResponses: true,
	}
	actual := observedGatewayGraph{
		loadBalancer: &octavia.LoadBalancer{LoadBalancer: loadbalancers.LoadBalancer{AdminStateUp: true}},
		listener:     &listeners.Listener{ID: "listener-1", AdminStateUp: true, Protocol: cloud.ListenerProtocolHTTP},
		protocol:     cloud.ListenerProtocolHTTP, defaultHTTPResponseCodes: map[string]int{"listener-1": http.StatusNotFound},
		routes: []observedRouteGraph{observed},
	}
	return desired, actual
}

func TestInvalidHTTPBackendWithoutPoolCreatesOnlyItsPolicy(t *testing.T) {
	desired, _ := convergedRoutePlanFixture()
	desired.fixedResponse, desired.unavailable = true, true
	desired.members = nil

	plan, _, err := buildRouteEnsurePlan(desired, observedRouteGraph{})
	if err != nil || len(plan) != 1 || plan[0].kind != routeMutationCreatePolicy || plan[0].parentID != "" {
		t.Fatalf("create a disabled fixed policy without a pool: plan = %#v, error = %v", plan, err)
	}
}

func TestObserveFixedPolicyRequiresVerifiedTargetAndOwner(t *testing.T) {
	identity := safetyTestIdentity(t)
	backend := openstackidentity.HTTPBackendKey("default", "web", 80)
	match := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/api", "")
	role := openstackidentity.HTTPFragmentRole(rolePolicyExact, backend, match)
	for _, test := range []struct {
		name, field    string
		value          any
		change, remove bool
	}{
		{name: "verified"},
		{name: "status omitted", field: "fixed_response", remove: true},
		{name: "wrong status", field: "fixed_response", value: map[string]int{"status_code": 404}, change: true},
		{name: "pool omitted", field: "redirect_pool_id", remove: true},
		{name: "pool present", field: "redirect_pool_id", value: "foreign-pool", change: true},
		{name: "unknown action", field: "action", value: "UNKNOWN", change: true},
		{name: "foreign project", field: "project_id", value: "foreign-project", change: true},
		{name: "unknown administrative state", field: "admin_state_up", remove: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := map[string]any{
				"id": "policy-1", "project_id": "project-a", "listener_id": "listener-1", "action": "FIXED_RESPONSE",
				"redirect_pool_id": nil, "fixed_response": map[string]int{"status_code": 500},
				"admin_state_up": false, "position": 1, "provisioning_status": "ACTIVE", "rules": []any{},
				"tags": safetyRouteTags(t, identity, role),
			}
			if test.remove {
				delete(policy, test.field)
			} else if test.change {
				policy[test.field] = test.value
			}
			provider := &Provider{projectID: "project-a", fixedHTTPResponses: true, octavia: octavia.NewService(safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/lbaas/l7policies":
					safetyWriteJSON(t, w, map[string]any{"l7policies": []any{policy}})
				case "/lbaas/l7policies/policy-1/rules":
					safetyWriteJSON(t, w, map[string]any{"rules": []any{}})
				default:
					t.Errorf("unexpected request to %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})))}
			inventory := routeGraphInventory{owners: []routeGraphOwner{{identity: identity}}, graphs: make([]observedRouteGraph, 1)}
			listener := &listeners.Listener{ID: "listener-1", L7Policies: []l7policies.L7Policy{{ID: "policy-1"}}}
			err := provider.observeListenerPolicies(t.Context(), &inventory, listener, cloud.ListenerProtocolHTTP, nil, map[int]string{}, true)
			if test.change || test.remove {
				if !errors.Is(err, cloud.ErrOwnershipConflict) {
					t.Fatalf("expected ambiguous or conflicting target to block the graph, got %v", err)
				}
				return
			}
			if err != nil || !inventory.graphs[0].part(backend).policies[0].fixedResponse {
				t.Fatalf("expected verified no-pool fixed policy, got %v", err)
			}
		})
	}
}

func TestBuildRouteEnsurePlanDisablesPolicyBeforeChangingRules(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	desired.hostnameRuleValue = `^api\.example\.test(:[0-9]+)?$`

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	if len(plan) == 0 || !reflect.DeepEqual(plan[0], routeMutation{kind: routeMutationDisablePolicy, id: "policy-1"}) {
		t.Fatalf("first route mutation = %#v, want policy disable", firstRouteMutation(plan))
	}
}

func TestBuildRouteEnsurePlanEnablesOnlyConvergedPolicy(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.policies[0].policy.AdminStateUp = false

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := []routeMutation{{kind: routeMutationEnablePolicy, id: "policy-1"}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("route plan = %#v, want %#v", plan, want)
	}
}

func TestBuildRouteEnsurePlanReturnsDeterministicConvergedState(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()

	plan, state, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	if len(plan) != 0 {
		t.Fatalf("converged route plan = %#v, want no mutations", plan)
	}
	want := cloud.RouteState{
		PoolID:      "pool-1",
		MemberIDs:   []string{"member-1"},
		MonitorID:   "monitor-1",
		L7PolicyIDs: []string{"policy-1"},
		L7RuleIDs:   []string{"rule-path"},
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("converged route state = %#v, want %#v", state, want)
	}
}

func TestBuildRouteEnsurePlanRepairsDisabledMember(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.members[0].AdminStateUp = false

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := []routeMutation{{
		kind:     routeMutationUpdateMember,
		id:       "member-1",
		parentID: "pool-1",
		member:   desired.members[0],
	}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("route plan = %#v, want %#v", plan, want)
	}
}

func TestBuildRouteEnsurePlanRecreatesDeletedMember(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.members = nil

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := routeMutation{
		kind:     routeMutationCreateMember,
		parentID: "pool-1",
		member:   cloud.Member{Address: "10.0.0.10", Port: 30080},
	}
	if !reflect.DeepEqual(plan, []routeMutation{want}) {
		t.Fatalf("route mutations = %#v, want only %#v", plan, want)
	}
}

func TestBuildRouteEnsurePlanRecreatesDeletedMonitor(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.monitor = nil

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := routeMutation{kind: routeMutationCreateMonitor, parentID: "pool-1"}
	if !reflect.DeepEqual(plan, []routeMutation{want}) {
		t.Fatalf("route mutations = %#v, want only %#v", plan, want)
	}
}

func TestBuildRouteEnsurePlanRecreatesDeletedPolicy(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.policies = nil

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := routeMutation{
		kind:     routeMutationCreatePolicy,
		parentID: "pool-1",
		policy:   desired.policies[0],
	}
	if !reflect.DeepEqual(plan, []routeMutation{want}) {
		t.Fatalf("route mutations = %#v, want only %#v", plan, want)
	}
}

func TestBuildRouteEnsurePlanDisablesPolicyBeforeRecreatingDeletedRule(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.policies[0].rules = nil

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := []routeMutation{
		{kind: routeMutationDisablePolicy, id: "policy-1"},
		{
			kind:     routeMutationCreateRule,
			parentID: "policy-1",
			rule: desiredRule{
				role:        roleRulePath,
				ruleType:    l7policies.TypePath,
				compareType: l7policies.CompareTypeEqual,
				value:       "/api",
			},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("route mutations = %#v, want %#v", plan, want)
	}
}

func TestGRPCPoliciesRequireNativeContentType(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	route.GRPC = true
	route.HTTPBackends = []cloud.HTTPBackend{{
		Namespace: "workloads", Name: "echo", Port: 8080, Members: route.Members, BackendH2C: true,
		Matches: []cloud.HTTPPathMatch{
			{PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 0},
			{PathType: cloud.PathMatchPrefix, PathValue: "/example.Echo", Precedence: 1},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 2},
		},
	}}
	route.Members = nil
	p := &Provider{projectID: "project-a", backendH2C: true, fixedHTTPResponses: true}
	graph, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := graph.routes[0].httpParts[0]
	var ranks [3]bool
	for _, policy := range part.policies {
		ranks[policy.precedence] = true
		rules := rulesFor(policy, "")
		index := slices.IndexFunc(rules, func(rule desiredRule) bool { return rule.key == "content-type" })
		if index == -1 || rules[index].ruleType != l7policies.TypeHeader ||
			rules[index].compareType != l7policies.CompareTypeRegex || rules[index].invert {
			t.Fatalf("policy %s requires a positive Octavia HEADER REGEX, got %#v", policy.role, rules)
		}
		guard := rules[index]
		tags := safetyRouteTags(t, part.identity, guard.role)
		role, ok := readRouteResourceRole(tags, openstackidentity.RoleRuleGRPCContentType)
		if !ok || role.match != policy.matchKey || role.backend != policy.backendKey {
			t.Fatalf("policy %s has an unrecognized guard owner: %#v", policy.role, role)
		}
		expression, err := regexp.CompilePOSIX(guard.value)
		if err != nil {
			t.Fatalf("policy %s requires a POSIX extended regular expression: %v", policy.role, err)
		}
		for _, test := range []struct {
			name, contentType string
			match             bool
		}{
			{name: "gRPC request", contentType: "application/grpc", match: true},
			{name: "gRPC with protobuf codec", contentType: "application/grpc+proto", match: true},
			{name: "mixed case and JSON codec with parameters", contentType: "Application/GRPC+JSON \t; charset=utf-8", match: true},
			{name: "custom codec", contentType: "application/grpc+x.v2-!$%*+^_`|~", match: true},
			{name: "ordinary GET /health"},
			{name: "ordinary POST with JSON", contentType: "application/json"},
			{name: "gRPC-Web", contentType: "application/grpc-web+proto"},
			{name: "empty codec", contentType: "application/grpc+; charset=utf-8"},
			{name: "separator in codec", contentType: "application/grpc+custom/json"},
			{name: "quoted codec", contentType: `application/grpc+"proto"`},
			{name: "space in codec", contentType: "application/grpc+custom json"},
		} {
			if got := expression.MatchString(test.contentType); got != test.match {
				t.Errorf("policy %s for %s (%q) matched = %t, want %t", policy.role, test.name, test.contentType, got, test.match)
			}
		}
	}
	if ranks != [3]bool{true, true, true} {
		t.Fatalf("missing exact, service or default policies: %v", ranks)
	}
}

func TestGRPCRouteTCPMonitorReplacesOwnedHTTPMonitor(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	desired.spec.GRPC = true
	plan, _, err := buildRouteEnsurePlan(desired, actual)
	want := []routeMutation{{kind: routeMutationDeleteMonitor, id: "monitor-1", parentID: "pool-1"}}
	if err != nil || !reflect.DeepEqual(plan, want) {
		t.Fatalf("existing gRPC monitor: plan %#v, error %v, want %#v", plan, err, want)
	}

	actual.monitor = nil
	plan, _, err = buildRouteEnsurePlan(desired, actual)
	want = []routeMutation{{kind: routeMutationCreateMonitor, parentID: "pool-1"}}
	if err != nil || !reflect.DeepEqual(plan, want) || desired.monitorType() != monitors.TypeTCP {
		t.Fatalf("replacement gRPC monitor: plan %#v, error %v, type %q, want a TCP monitor creation", plan, err, desired.monitorType())
	}
}

func TestBuildRouteEnsurePlanDisablesPolicyBeforeRecreatingDeletedPool(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	actual.pool = nil
	actual.members = nil
	actual.monitor = nil

	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatalf("buildRouteEnsurePlan() error = %v", err)
	}
	want := []routeMutation{
		{kind: routeMutationDisablePolicy, id: "policy-1"},
		{kind: routeMutationCreatePool},
	}
	if len(plan) < len(want) || !reflect.DeepEqual(plan[:len(want)], want) {
		t.Fatalf("first route mutations = %#v, want %#v", plan, want)
	}
}

func TestBuildRouteDeletionPlanDisablesPolicyBeforeDeletingRules(t *testing.T) {
	_, actual := convergedRoutePlanFixture()
	plan := buildRouteDeletionPlan(actual)
	if len(plan) < 2 {
		t.Fatalf("route deletion plan = %#v, want policy disable and rule deletion", plan)
	}
	if !reflect.DeepEqual(plan[0], routeMutation{kind: routeMutationDisablePolicy, id: "policy-1"}) {
		t.Fatalf("first route deletion = %#v, want policy disable", plan[0])
	}
	if !reflect.DeepEqual(plan[1], routeMutation{kind: routeMutationDeleteRule, id: "rule-path", parentID: "policy-1"}) {
		t.Fatalf("second route deletion = %#v, want path rule deletion", plan[1])
	}
}

func convergedRoutePlanFixture() (desiredRoute, observedRouteGraph) {
	policy := desiredPolicy{
		role:        rolePolicyExact,
		position:    1,
		compareType: l7policies.CompareTypeEqual,
		path:        "/api",
	}
	desired := desiredRoute{
		spec: cloud.RouteSpec{
			MemberSubnetID: "member-subnet",
			PathType:       cloud.PathMatchExact,
			PathValue:      "/api",
		},
		members:       []cloud.Member{{Address: "10.0.0.10", Port: 30080}},
		healthMonitor: cloud.HealthMonitorConfig{Path: "/healthz", Delay: 10, Timeout: 5, MaxRetries: 3, MaxRetriesDown: 3},
		policies:      []desiredPolicy{policy},
	}
	actual := observedRouteGraph{
		pool: &pools.Pool{
			ID:           "pool-1",
			Protocol:     string(pools.ProtocolHTTP),
			LBMethod:     string(pools.LBMethodRoundRobin),
			AdminStateUp: true,
		},
		members: []pools.Member{{
			ID: "member-1", Address: "10.0.0.10", ProtocolPort: 30080, SubnetID: "member-subnet", AdminStateUp: true,
		}},
		monitor: &monitors.Monitor{
			ID:             "monitor-1",
			Type:           monitors.TypeHTTP,
			Delay:          desired.healthMonitor.Delay,
			Timeout:        desired.healthMonitor.Timeout,
			MaxRetries:     desired.healthMonitor.MaxRetries,
			MaxRetriesDown: desired.healthMonitor.MaxRetriesDown,
			URLPath:        desired.healthMonitor.Path,
			HTTPMethod:     http.MethodGet,
			ExpectedCodes:  routeMonitorExpectedCodes,
			AdminStateUp:   true,
		},
		policies: []observedRoutePolicy{{
			role: rolePolicyExact,
			policy: l7policies.L7Policy{
				ID: "policy-1", Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: "pool-1",
				Position: 1, AdminStateUp: true,
			},
			rules: []observedRouteRule{{
				role: roleRulePath,
				rule: l7policies.Rule{
					ID: "rule-path", RuleType: string(l7policies.TypePath),
					CompareType: string(l7policies.CompareTypeEqual), Value: "/api", AdminStateUp: true,
				},
			}},
		}},
	}
	return desired, actual
}

func firstRouteMutation(plan []routeMutation) routeMutation {
	if len(plan) == 0 {
		return routeMutation{}
	}
	return plan[0]
}

func TestMemberProviderIdentityDoesNotChangeOctaviaMembership(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	desired.members[0].ProviderID = "openstack:///11111111-1111-1111-1111-111111111111"
	plan, _, err := buildRouteEnsurePlan(desired, actual)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 0 {
		t.Fatalf("provider identity changed a converged member: %#v", plan)
	}
}

func TestHeaderDriftKeepsTheForwardingPredicateProved(t *testing.T) {
	route, actual := convergedRoutePlanFixture()
	route.policies[0].requestHeaderOperations = []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}
	policy := &actual.policies[0]
	policy.adminObserved, policy.policy.ProvisioningStatus = true, "ACTIVE"
	for i := range policy.rules {
		policy.rules[i].flagsObserved, policy.rules[i].rule.ProvisioningStatus = true, "ACTIVE"
	}
	if !observedForwardPolicyMatches(route, actual, *policy) {
		t.Fatal("header-only drift must not make a neighboring redirect withdraw a proved forwarding predicate")
	}
	plan, disables, _ := buildRoutePolicyPlan(route, actual.policies, actual.pool.ID)
	if len(disables) != 0 || len(plan) != 1 || plan[0].kind != routeMutationSetPolicyHeaders {
		t.Fatalf("expected an independent header repair: plan=%#v disables=%#v", plan, disables)
	}
}

func TestFirstHeaderModifierStagesOnlyItsOwnRoute(t *testing.T) {
	ordinary, old := convergedRoutePlanFixture()
	replacement, prepared := convergedRoutePlanFixture()
	old.policies[0].policy.ListenerID = "listener-1"
	old.policies[0].policy.ProvisioningStatus, old.policies[0].adminObserved = "ACTIVE", true
	old.policies[0].rules[0].rule.ProvisioningStatus, old.policies[0].rules[0].flagsObserved = "ACTIVE", true
	replacement.backendKey = "preview"
	replacement.policies[0].role = "preview-path"
	replacement.policies[0].requestHeaderOperations = []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"}}
	prepared.pool.ID, prepared.pool.ProvisioningStatus = "preview-pool", "ACTIVE"
	prepared.members[0].ProvisioningStatus, prepared.monitor.ProvisioningStatus = "ACTIVE", "ACTIVE"
	prepared.policies = nil
	old.httpParts = map[string]*observedRouteGraph{replacement.backendKey: &prepared}
	desired, actual := fixedHTTPPlanFixture(desiredRoute{spec: ordinary.spec, httpParts: []desiredRoute{replacement}}, old)
	desired.fixedHTTPResponses = false
	actual.protocol = cloud.ListenerProtocolHTTP

	redirect, err := (&Provider{projectID: "project-a"}).buildDesiredGatewayGraph(redirectGraphSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	redirectPart := redirect.routes[0].httpParts[0]
	wanted := redirectPart.policies[0]
	redirectPolicy := observedRoutePolicy{
		role: wanted.role, versioned: true, redirect: true, adminObserved: true,
		policy: l7policies.L7Policy{ID: "redirect", ListenerID: "listener-1", Position: 2, AdminStateUp: true,
			ProvisioningStatus: "ACTIVE", Action: "REDIRECT_PREFIX", RedirectPrefix: wanted.redirect.prefix, RedirectHttpCode: wanted.redirect.statusCode},
	}
	for _, rule := range rulesFor(wanted, "") {
		redirectPolicy.rules = append(redirectPolicy.rules, observedRouteRule{role: rule.role, flagsObserved: true,
			rule: l7policies.Rule{RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value,
				AdminStateUp: true, Invert: rule.invert, ProvisioningStatus: "ACTIVE"}})
	}
	desired.routes = append(desired.routes, redirect.routes[0])
	desired.spec.Routes = append(desired.spec.Routes, redirect.spec.Routes[0])
	actual.routes = append(actual.routes, observedRouteGraph{httpParts: map[string]*observedRouteGraph{
		redirectPart.backendKey: {policies: []observedRoutePolicy{redirectPolicy}},
	}})

	planned := func(kind routeMutationKind, id string) graphMutation {
		t.Helper()
		plan, _, err := (&Provider{}).buildGatewayGraphPlan(desired, actual)
		if err != nil || len(plan) == 0 || plan[0].route.kind != kind || plan[0].route.id != id {
			t.Fatalf("wrong handover mutation after readback: %+v, %v", plan, err)
		}
		for _, step := range plan {
			if step.kind == graphRouteMutation && (step.route.id == "redirect" ||
				step.route.id == "policy-1" && kind != routeMutationDisablePolicy) {
				t.Fatalf("the old forward or unrelated redirect would stop before the replacement is ready: %+v", step)
			}
		}
		return plan[0]
	}
	stage := planned(routeMutationCreatePolicy, "")
	if stage.backendKey != "preview" || stage.route.policy.position != 1 || len(stage.route.requestHeaderOperations) != 1 {
		t.Fatalf("stage the filtered policy ahead of the existing predicate: %+v", stage)
	}
	old.policies[0].policy.Position++
	actual.routes[1].httpParts[redirectPart.backendKey].policies[0].policy.Position++
	prepared.policies = []observedRoutePolicy{{
		role: replacement.policies[0].role, adminObserved: true,
		requestHeaderOperations: replacement.policies[0].requestHeaderOperations,
		policy: l7policies.L7Policy{ID: "preview-policy", ListenerID: "listener-1", Position: 1,
			Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: prepared.pool.ID, ProvisioningStatus: "ACTIVE"},
	}}
	planned(routeMutationCreateRule, "")
	for _, rule := range rulesFor(replacement.policies[0], "") {
		prepared.policies[0].rules = append(prepared.policies[0].rules, observedRouteRule{role: rule.role, flagsObserved: true,
			rule: l7policies.Rule{RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value,
				AdminStateUp: true, ProvisioningStatus: "ACTIVE"}})
	}
	planned(routeMutationEnablePolicy, "preview-policy")
	prepared.policies[0].policy.AdminStateUp = true
	planned(routeMutationDisablePolicy, "policy-1")
}
