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
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"k8s.io/utils/ptr"
)

func TestWeightedHTTPRuleRetainsPolicyRoleWhenGrantChanges(t *testing.T) {
	identity := safetyTestIdentity(t).Value()
	desired, _ := convergedRoutePlanFixture()
	spec := desired.spec
	spec.Identity = identity
	spec.HealthMonitor = desired.healthMonitor
	spec.GatewayRequirements.Provider = "amphora"
	spec.GatewayRequirements.VIPSubnetID = "vip-subnet"
	spec.GatewayRequirements.ListenerPort = 80
	spec.Hostname, spec.PathType, spec.PathValue, spec.Members = "", "", "", nil
	index := 0
	spec.HTTPBackends = []cloud.HTTPBackend{{Namespace: "apps", Name: "api", Port: 80, Members: desired.members}}
	spec.WeightedHTTPRules = []cloud.HTTPWeightedRule{{
		RuleIndex: 2, Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/api"}},
		Targets: []cloud.HTTPWeightedTarget{{BackendIndex: &index, Weight: 3}, {Weight: 1}},
	}}
	provider := &Provider{projectID: identity.OpenStackProjectID, fixedHTTPResponses: true, weightedHTTPTargets: true}
	granted, err := provider.buildDesiredRoute(spec)
	if err != nil || len(granted.httpParts) != 2 {
		t.Fatalf("want one policy fragment and one target-only pool, got %#v, %v", granted.httpParts, err)
	}
	policyRole := func(route desiredRoute) string {
		t.Helper()
		index := slices.IndexFunc(route.httpParts, func(part desiredRoute) bool { return len(part.policies) != 0 })
		if index == -1 || len(route.httpParts[index].policies) != 1 {
			t.Fatalf("want one weighted policy in %#v", route.httpParts)
		}
		return route.httpParts[index].policies[0].role
	}
	spec.HTTPBackends = nil
	spec.WeightedHTTPRules[0].Targets[0].BackendIndex = nil
	revoked, err := provider.buildDesiredRoute(spec)
	if err != nil || len(revoked.httpParts) != 1 {
		t.Fatalf("want the weighted policy without any pool, got %#v, %v", revoked.httpParts, err)
	}
	if grantedRole, revokedRole := policyRole(granted), policyRole(revoked); grantedRole != revokedRole {
		t.Fatalf("grant changed policy role from %q to %q", grantedRole, revokedRole)
	}
}

func TestWeightedHTTPFailedWithdrawalCanRetryWithoutFloatingIPInventory(t *testing.T) {
	identity := safetyTestIdentity(t)
	gatewayID := gatewayIdentity(identity.Value())
	gateway := cloud.GatewaySpec{Identity: gatewayID, Provider: "amphora", VIPSubnetID: "vip-subnet", ExternalNetworkID: "external",
		ListenerPort: 80, ListenerProtocol: cloud.ListenerProtocolHTTP}
	match := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/api"}
	key, predicate := openstackidentity.HTTPWeightedRuleKey(0), openstackidentity.HTTPMatchKey("", match.PathType, match.PathValue, "")
	route := cloud.RouteSpec{Identity: identity.Value(), GatewayRequirements: gatewayRequirements(gateway), MemberSubnetID: "member-subnet",
		HealthMonitor:     cloud.HealthMonitorConfig{Path: "/ready", Delay: 10, Timeout: 5, MaxRetries: 3, MaxRetriesDown: 3},
		WeightedHTTPRules: []cloud.HTTPWeightedRule{{Matches: []cloud.HTTPPathMatch{match}, Targets: []cloud.HTTPWeightedTarget{{Weight: 1}}}},
	}
	spec := cloud.GatewayGraphSpec{Identity: gatewayID, Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: gateway},
		Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure, Spec: route}}}
	lb := map[string]any{"id": "lb", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true, "provider": "amphora",
		"vip_subnet_id": "vip-subnet", "vip_address": "192.0.2.10", "vip_port_id": "vip-port", "pools": []any{},
		"listeners": []map[string]string{{"id": "listener"}}, "tags": safetyGatewayTags(t, identity, roleLoadBalancer)}
	listener := map[string]any{"id": "listener", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"protocol": "HTTP", "protocol_port": 80, "default_pool_id": nil, "default_http_response": map[string]int{"status_code": 404},
		"loadbalancers": []map[string]string{{"id": "lb"}}, "l7policies": []map[string]string{{"id": "policy"}}, "tags": safetyGatewayTags(t, identity, roleListener)}
	policy := map[string]any{"id": "policy", "project_id": "project-a", "listener_id": "listener", "action": "WEIGHTED_TARGETS", "provisioning_status": "ERROR",
		"admin_state_up": true, "position": 1, "redirect_pool_id": nil, "targets": []octavia.WeightedHTTPTarget{fixedWeightedHTTPTarget(1)},
		"rules": []map[string]string{{"id": "path"}}, "tags": safetyRouteTags(t, identity, openstackidentity.HTTPWeightedPolicyRole(rolePolicyExact, key, predicate))}
	rule := map[string]any{"id": "path", "project_id": "project-a", "provisioning_status": "ACTIVE", "admin_state_up": true,
		"type": "PATH", "compare_type": "EQUAL_TO", "value": match.PathValue,
		"tags": safetyRouteTags(t, identity, openstackidentity.HTTPWeightedPolicyRole(roleRulePath, key, predicate))}
	reads := map[string]map[string]any{
		"/lbaas/loadbalancers": {"loadbalancers": []any{lb}}, "/lbaas/loadbalancers/lb": {"loadbalancer": lb},
		"/lbaas/listeners": {"listeners": []any{listener}}, "/lbaas/pools": {"pools": []any{}},
		"/lbaas/l7policies": {"l7policies": []any{policy}}, "/lbaas/l7policies/policy/rules": {"rules": []any{rule}},
		"/lbaas/providers/amphora/gateway_capabilities": {"gateway_capabilities": []string{octavia.GatewayCapabilityFixedHTTPResponses, octavia.GatewayCapabilityWeightedHTTPTargets}},
	}
	var writes []string
	provider := newTestProvider(t, ServiceClients{
		ProjectID: "project-a",
		Network: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet || request.URL.Path != "/floatingips" {
				t.Errorf("unexpected Neutron request: %s %s", request.Method, request.URL.Path)
			}
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		})),
		LoadBalancer: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if body, found := reads[request.URL.Path]; request.Method == http.MethodGet && found {
				safetyWriteJSON(t, w, body)
				return
			}
			writes = append(writes, request.Method+" "+request.URL.Path)
			if request.Method != http.MethodPut || request.URL.Path != "/lbaas/l7policies/policy" {
				t.Errorf("unexpected Octavia request: %s", writes[len(writes)-1])
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			var body struct {
				Policy map[string]json.RawMessage `json:"l7policy"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Policy) != 2 ||
				string(body.Policy["action"]) != `"WEIGHTED_TARGETS"` || string(body.Policy["targets"]) != `[{"weight":1,"fixed_response":{"status_code":500}}]` {
				t.Errorf("withdrawal must only replay the safe target vector: %#v (%v)", body.Policy, err)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy": policy})
		})),
	}, ProviderConfig{WeightedHTTPTargets: true})
	fence := errors.New("backend authorization changed")
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("the address failure must preserve the mutation fence, got %v, writes %v", err, writes)
	}
	spec.ValidateMutation = nil
	if result, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || !result.Outcome.MutationSubmitted ||
		result.Gateway.AddressAbsent || len(writes) != 1 {
		t.Fatalf("the verified target reduction can retry without claiming the address absent, got %v, result %#v, writes %v", err, result, writes)
	}
	policy["provisioning_status"] = "ACTIVE"
	index := 0
	spec.Routes[0].Spec.WeightedHTTPRules[0].Targets[0].BackendIndex = &index
	spec.Routes[0].Spec.HTTPBackends = []cloud.HTTPBackend{{Namespace: "apps", Name: "future", Port: 80, Unavailable: true}}
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "discover Gateway Floating IPs") || len(writes) != 1 {
		t.Fatalf("new forwarding still requires address inventory, got %v, writes %v", err, writes)
	}
	spec.Routes[0].Spec.WeightedHTTPRules = nil
	spec.Routes[0].Spec.HTTPBackends[0].Matches = []cloud.HTTPPathMatch{match}
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "discover Gateway Floating IPs") || len(writes) != 1 {
		t.Fatalf("an ordinary policy also requires address inventory, got %v, writes %v", err, writes)
	}
	spec.Routes[0].Spec = route
	spec.Routes[0].Spec.WeightedHTTPRules[0].Targets[0].BackendIndex = nil
	policy["project_id"] = "other-project"
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != 1 {
		t.Fatalf("incomplete policy ownership still blocks graph mutations, got %v, writes %v", err, writes)
	}
}

func weightedPlanFixture() (desiredGatewayGraph, observedGatewayGraph) {
	local, localPool := convergedRoutePlanFixture()
	remote, remotePool := convergedRoutePlanFixture()
	local.backendKey, remote.backendKey = "local", "remote"
	local.policies, remote.policies = nil, nil
	for _, pool := range []*observedRouteGraph{&localPool, &remotePool} {
		pool.policies, pool.monitor = nil, nil
		pool.pool.ProvisioningStatus, pool.members[0].ProvisioningStatus = "ACTIVE", "ACTIVE"
	}
	local.healthMonitor, remote.healthMonitor = cloud.HealthMonitorConfig{}, cloud.HealthMonitorConfig{}
	remotePool.pool.ID, remotePool.members[0].ID = "pool-remote", "member-remote"
	weighted, policyGraph := convergedRoutePlanFixture()
	weighted.backendKey = "weighted-rule"
	weighted.weightedTargets = []desiredHTTPWeightedTarget{{backendKey: local.backendKey, weight: 3}, {backendKey: remote.backendKey, weight: 1}}
	policy := &policyGraph.policies[0]
	policy.policy.Action, policy.policy.RedirectPoolID = string(octavia.ActionWeightedTargets), ""
	policy.policy.ListenerID = "listener-1"
	policy.policy.ProvisioningStatus, policy.rules[0].rule.ProvisioningStatus = "ACTIVE", "ACTIVE"
	policy.weightedTargets = []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: localPool.pool.ID}, {Weight: 1, PoolID: remotePool.pool.ID}}
	policyGraph.pool, policyGraph.members, policyGraph.monitor = nil, nil, nil
	return fixedHTTPPlanFixture(desiredRoute{httpParts: []desiredRoute{local, remote, weighted}},
		observedRouteGraph{httpParts: map[string]*observedRouteGraph{
			local.backendKey: &localPool, remote.backendKey: &remotePool, weighted.backendKey: &policyGraph,
		}})

}

func weightedPlan(t *testing.T, desired desiredGatewayGraph, actual observedGatewayGraph) []graphMutation {
	t.Helper()
	steps, _, err := (&Provider{}).buildGatewayGraphPlan(desired, actual)
	if err != nil {
		t.Fatal(err)
	}
	return steps
}

func TestWeightedHTTPGrantWithdrawalPrecedesPoolCleanup(t *testing.T) {
	desired, actual := weightedPlanFixture()
	parts := desired.routes[0].httpParts
	local, remote, weighted := parts[0], parts[1], parts[2]
	graphs := actual.routes[0].httpParts
	remotePool, policy := graphs[remote.backendKey], &graphs[weighted.backendKey].policies[0]
	plan := func() []graphMutation {
		return weightedPlan(t, desired, actual)
	}
	if steps := plan(); len(steps) != 0 {
		t.Fatalf("the existing weighted graph requires no writes, got %#v", steps)
	}
	// Losing the grant changes one target with neither a new policy nor a
	// reassignment of the failed target's request share.
	withdrawn := []octavia.WeightedHTTPTarget{policy.weightedTargets[0], fixedWeightedHTTPTarget(1)}
	desired.routes[0].httpParts = []desiredRoute{local, weighted}
	weighted.weightedTargets[1].backendKey = ""
	checkWithdrawal := func() {
		t.Helper()
		steps := plan()
		if len(steps) != 1 || steps[0].route.kind != routeMutationSetPolicyTarget || steps[0].route.id != policy.policy.ID ||
			!steps[0].withdrawHTTPPolicy || steps[0].route.weightedTargets == nil ||
			!slices.EqualFunc(*steps[0].route.weightedTargets, withdrawn, weightedTargetsEqual) {
			t.Fatalf("want in-place 3:1 target withdrawal for the existing policy, got %#v", steps)
		}
	}
	checkWithdrawal()
	policy.weightedTargets = withdrawn
	policy.policy.ProvisioningStatus = "ERROR"
	checkWithdrawal()
	future := remote
	future.backendKey = "future"
	weighted.weightedTargets = append(weighted.weightedTargets, desiredHTTPWeightedTarget{backendKey: future.backendKey, weight: 1})
	desired.routes[0].httpParts = []desiredRoute{local, future, weighted}
	checkWithdrawal()
	weighted.weightedTargets = weighted.weightedTargets[:2]
	desired.routes[0].httpParts = []desiredRoute{local, weighted}
	policy.policy.ProvisioningStatus = "ACTIVE"
	if steps := plan(); len(steps) < 1 || steps[0].backendKey != remote.backendKey || steps[0].route.kind != routeMutationDeleteMember {
		t.Fatalf("want the revoked member pruned after ACTIVE readback, got %#v", steps)
	}
	remotePool.members = nil
	if steps := plan(); len(steps) != 1 || steps[0].backendKey != remote.backendKey || steps[0].route.kind != routeMutationDeletePool {
		t.Fatalf("want the revoked pool deleted after its members, got %#v", steps)
	}
	delete(actual.routes[0].httpParts, remote.backendKey)
	if steps := plan(); len(steps) != 0 {
		t.Fatalf("the revoked graph requires no writes after cleanup, got %#v", steps)
	}

	weighted.weightedTargets[1].backendKey = remote.backendKey
	desired.routes[0].httpParts = []desiredRoute{local, remote, weighted}
	if steps := plan(); len(steps) != 1 || steps[0].backendKey != remote.backendKey || steps[0].route.kind != routeMutationCreatePool {
		t.Fatalf("restore the target pool before forwarding its share, got %#v", steps)
	}
	remotePool.pool.ID = "pool-restored"
	actual.routes[0].httpParts[remote.backendKey] = remotePool
	if steps := plan(); len(steps) != 1 || steps[0].backendKey != remote.backendKey || steps[0].route.kind != routeMutationCreateMember {
		t.Fatalf("restore the target's members before forwarding its share, got %#v", steps)
	}
	remotePool.members = []pools.Member{{ID: "member-restored", Address: remote.members[0].Address,
		ProtocolPort: remote.members[0].Port, SubnetID: remote.spec.MemberSubnetID, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
	restored := []octavia.WeightedHTTPTarget{withdrawn[0], {Weight: 1, PoolID: remotePool.pool.ID}}
	if steps := plan(); len(steps) != 1 || steps[0].route.kind != routeMutationSetPolicyTarget || steps[0].route.id != policy.policy.ID ||
		steps[0].route.weightedTargets == nil || !slices.EqualFunc(*steps[0].route.weightedTargets, restored, weightedTargetsEqual) {
		t.Fatalf("restore the second share on the same policy, got %#v", steps)
	}
	policy.weightedTargets = restored
	local.members = append(slices.Clone(local.members), cloud.Member{Address: "10.0.0.11", Port: local.members[0].Port})
	desired.routes[0].httpParts[0] = local
	if steps := plan(); len(steps) != 1 || steps[0].backendKey != local.backendKey || steps[0].route.kind != routeMutationCreateMember {
		t.Fatalf("scaling one Service must preserve the policy's 3:1 destinations, got %#v", steps)
	}
}

func TestWeightedHTTPOwnedPoolMustConvergeBeforeEnablingEachPolicy(t *testing.T) {
	desired, actual := weightedPlanFixture()
	parts := desired.routes[0].httpParts
	remote, weighted := parts[1], &parts[2]
	graphs := actual.routes[0].httpParts
	remotePool, policies := graphs[remote.backendKey], graphs[weighted.backendKey]
	remotePool.pool.ProvisioningStatus = "ERROR"
	remotePool.members[0].Address = "10.0.0.55"
	secondary := weighted.policies[0]
	secondary.role, secondary.path, secondary.position = "second", "/other", 2
	weighted.policies = append(weighted.policies, secondary)
	secondObserved := policies.policies[0]
	secondObserved.role, secondObserved.policy.ID, secondObserved.policy.Position = secondary.role, "second-policy", 2
	secondObserved.policy.AdminStateUp = false
	secondObserved.rules = slices.Clone(secondObserved.rules)
	secondObserved.rules[0].rule.Value = secondary.path
	policies.policies = append(policies.policies, secondObserved)
	if steps := weightedPlan(t, desired, actual); slices.ContainsFunc(steps, func(step graphMutation) bool {
		return step.route.kind == routeMutationEnablePolicy
	}) {
		t.Fatalf("the serving sibling cannot prove that a disabled policy may open, got %#v", steps)
	}
}

func TestWeightedHTTPZeroWeightDoesNotHoldPositiveTargets(t *testing.T) {
	desired, actual := weightedPlanFixture()
	parts := desired.routes[0].httpParts
	remote, weighted := parts[1], &parts[2]
	graphs := actual.routes[0].httpParts
	remotePool, policy := graphs[remote.backendKey], &graphs[weighted.backendKey].policies[0]
	remotePool.pool.ProvisioningStatus = "ERROR"
	weighted.weightedTargets[1].weight = 0
	policy.weightedTargets[1].Weight = 0
	policy.policy.AdminStateUp = false
	if steps := weightedPlan(t, desired, actual); len(steps) == 0 || steps[0].route.kind != routeMutationEnablePolicy {
		t.Fatalf("a zero-weight pool cannot hold a prepared positive sibling, got %#v", steps)
	}
	policyGraph := graphs[weighted.backendKey]
	delete(graphs, weighted.backendKey)
	delete(graphs, remote.backendKey)
	initial := weightedPlan(t, desired, actual)
	if len(initial) == 0 || initial[0].route.kind != routeMutationCreatePolicy || initial[0].route.weightedTargets == nil ||
		!weightedTargetsEqual((*initial[0].route.weightedTargets)[1], fixedWeightedHTTPTarget(0)) {
		t.Fatalf("prepare the positive share even before creating a zero-weight destination, got %#v", initial)
	}
	graphs[weighted.backendKey], graphs[remote.backendKey] = policyGraph, remotePool

	policy.policy.AdminStateUp, policy.weightedTargets[1].Weight = true, 1
	future := remote
	future.backendKey = "future"
	weighted.weightedTargets = append(weighted.weightedTargets, desiredHTTPWeightedTarget{backendKey: future.backendKey, weight: 1})
	desired.routes[0].httpParts = append(parts, future)
	steps := weightedPlan(t, desired, actual)
	want := []octavia.WeightedHTTPTarget{policy.weightedTargets[0], {PoolID: remotePool.pool.ID, Weight: 0}}
	if len(steps) == 0 || steps[0].route.kind != routeMutationSetPolicyTarget || !steps[0].withdrawHTTPPolicy ||
		steps[0].route.weightedTargets == nil || !slices.EqualFunc(*steps[0].route.weightedTargets, want, weightedTargetsEqual) {
		t.Fatalf("stop the remote share while waiting for a new destination, got %#v", steps)
	}
}

func TestWeightedHTTPWithdrawalPrioritizesServingPolicy(t *testing.T) {
	desired, actual := weightedPlanFixture()
	parts := desired.routes[0].httpParts
	local, weighted := parts[0], parts[2]
	second := weighted.policies[0]
	second.role, second.path, second.position = "second", "/second", 2
	weighted.policies = append(weighted.policies, second)
	weighted.weightedTargets[1].backendKey = ""
	desired.routes[0].httpParts = []desiredRoute{local, weighted}
	graph := actual.routes[0].httpParts[weighted.backendKey]
	other := graph.policies[0]
	other.role, other.policy.ID, other.policy.Position = second.role, "policy-2", 2
	other.rules = slices.Clone(other.rules)
	other.rules[0].rule.ID, other.rules[0].rule.Value = "rule-2", second.path
	graph.policies = append(graph.policies, other)
	first := &graph.policies[0]
	for _, state := range []string{"disabled", "retry with stored replacement"} {
		if state == "disabled" {
			first.policy.AdminStateUp = false
		} else {
			first.policy.AdminStateUp, first.policy.ProvisioningStatus = true, "ERROR"
			first.weightedTargets = []octavia.WeightedHTTPTarget{first.weightedTargets[0], fixedWeightedHTTPTarget(1)}
		}
		steps := weightedPlan(t, desired, actual)
		if len(steps) != 1 || steps[0].route.id != other.policy.ID || steps[0].route.weightedTargets == nil {
			t.Fatalf("first policy %s: want second serving policy updated first, got %#v", state, steps)
		}
	}
}

func TestWeightedHTTPRuleReorderKeepsAuthorizedPredicateServing(t *testing.T) {
	desired, actual := weightedPlanFixture()
	parts := desired.routes[0].httpParts
	local, remote, first := parts[0], parts[1], parts[2]
	graphs := actual.routes[0].httpParts
	third, thirdPool := convergedRoutePlanFixture()
	third.backendKey, third.policies, third.healthMonitor = "third", nil, cloud.HealthMonitorConfig{}
	thirdPool.policies, thirdPool.monitor = nil, nil
	thirdPool.pool.ID, thirdPool.pool.ProvisioningStatus = "pool-third", "ACTIVE"
	thirdPool.members[0].ID, thirdPool.members[0].ProvisioningStatus = "member-third", "ACTIVE"
	graphs[third.backendKey] = &thirdPool
	second := first
	second.backendKey = "second"
	second.policies = slices.Clone(first.policies)
	second.policies[0].role, second.policies[0].path, second.policies[0].position = "second-original", "/other", 2
	second.weightedTargets = []desiredHTTPWeightedTarget{{backendKey: local.backendKey, weight: 3}, {backendKey: third.backendKey, weight: 1}}
	secondPolicy := graphs[first.backendKey].policies[0]
	secondPolicy.role = second.policies[0].role
	secondPolicy.policy.ID, secondPolicy.policy.Position = "second-policy", 2
	secondPolicy.rules = slices.Clone(secondPolicy.rules)
	secondPolicy.rules[0].rule.Value, secondPolicy.rules[0].rule.ID = "/other", "second-path"
	secondPolicy.weightedTargets = []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: graphs[local.backendKey].pool.ID}, {Weight: 1, PoolID: thirdPool.pool.ID}}
	graphs[second.backendKey] = &observedRouteGraph{policies: []observedRoutePolicy{secondPolicy}}
	first.policies[0], second.policies[0] = second.policies[0], first.policies[0]
	first.policies[0].role, second.policies[0].role = "first-new", "second-new"
	first.policies[0].position, second.policies[0].position = 1, 2
	first.weightedTargets, second.weightedTargets = second.weightedTargets, first.weightedTargets
	desired.routes[0].httpParts = []desiredRoute{local, remote, third, first, second}
	if steps := weightedPlan(t, desired, actual); len(steps) == 0 || steps[0].route.kind != routeMutationCreatePolicy {
		t.Fatalf("stage an equivalent policy before changing the old target, got %#v", steps)
	}
	// A simultaneous grant revocation still withdraws the old predicate's real edge.
	second.weightedTargets[1].backendKey = ""
	desired.routes[0].httpParts = []desiredRoute{local, third, first, second}
	steps := weightedPlan(t, desired, actual)
	if len(steps) == 0 || steps[0].route.id != "policy-1" || steps[0].route.kind != routeMutationSetPolicyTarget ||
		steps[0].route.weightedTargets == nil || (*steps[0].route.weightedTargets)[1].FixedResponse == nil {
		t.Fatalf("withdraw the revoked target on its original predicate before staging, got %#v", steps)
	}
}

func TestObserveWeightedHTTPPolicyRequiresOwnedPoolEdges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner poolRouteOwner
		want  bool
	}{
		{name: "owned pool", owner: poolRouteOwner{route: 0, backend: "service"}, want: true},
		{name: "other route", owner: poolRouteOwner{route: 1, backend: "service"}},
		{name: "scalar pool", owner: poolRouteOwner{route: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity := safetyTestIdentity(t)
			key := openstackidentity.HTTPWeightedRuleKey(2)
			match := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/api", "")
			policy := map[string]any{
				"id": "policy-1", "project_id": "project-a", "listener_id": "listener-1", "action": "WEIGHTED_TARGETS",
				"redirect_pool_id": nil, "targets": []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: "pool-1"}, fixedWeightedHTTPTarget(1)},
				"admin_state_up": true, "position": 1, "provisioning_status": "ACTIVE",
				"rules": []map[string]string{{"id": "rule-1"}},
				"tags":  safetyRouteTags(t, identity, openstackidentity.HTTPWeightedPolicyRole(rolePolicyExact, key, match)),
			}
			rule := map[string]any{"id": "rule-1", "project_id": "project-a", "tags": safetyRouteTags(t, identity, openstackidentity.HTTPWeightedPolicyRole(roleRulePath, key, match))}
			provider := &Provider{projectID: "project-a", fixedHTTPResponses: true, weightedHTTPTargets: true,
				octavia: octavia.NewService(safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					switch req.URL.Path {
					case "/lbaas/l7policies":
						safetyWriteJSON(t, w, map[string]any{"l7policies": []any{policy}})
					case "/lbaas/l7policies/policy-1/rules":
						safetyWriteJSON(t, w, map[string]any{"rules": []any{rule}})
					default:
						t.Errorf("unexpected request to %s", req.URL.Path)
						w.WriteHeader(http.StatusNotFound)
					}
				})))}
			inventory := routeGraphInventory{owners: []routeGraphOwner{{identity: identity}}, graphs: make([]observedRouteGraph, 1)}
			listener := &listeners.Listener{ID: "listener-1", L7Policies: []l7policies.L7Policy{{ID: "policy-1"}}}
			err := provider.observeListenerPolicies(t.Context(), &inventory, listener, cloud.ListenerProtocolHTTP,
				map[string]poolRouteOwner{"pool-1": tc.owner}, map[int]string{}, true)
			if !tc.want {
				if !errors.Is(err, cloud.ErrOwnershipConflict) {
					t.Fatalf("weighted pool edge must be proved, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			pool := &pools.Pool{ID: "pool-1", Protocol: cloud.ListenerProtocolHTTP, Listeners: []pools.ListenerID{{ID: listener.ID}}}
			inventory.graphs[0].part(tc.owner.backend).pool = pool
			if err := validateScopedHTTPRouteRelationships(inventory.graphs); err != nil {
				t.Fatalf("weighted target must account for the pool's listener edge: %v", err)
			}
		})
	}
}

func TestWeightedTargetRequestFiltersRequireRegionalCapabilityAndReadback(t *testing.T) {
	desired, actual := weightedPlanFixture()
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.listener.ProtocolPort, actual.listener.ProvisioningStatus = 80, "ACTIVE"
	actual.loadBalancer.Provider, actual.loadBalancer.ProvisioningStatus = "amphora-regional", "ACTIVE"
	actual.adminObserved = map[string]bool{actual.listener.ID: true}
	desired.spec.Gateway.Spec.ListenerPort = 80
	part := &desired.routes[0].httpParts[2]
	headers := []cloud.HTTPHeaderOperation{{Name: "backend", Operation: cloud.HTTPHeaderSet, Value: "local"}}
	part.weightedTargets[0].requestHeaderOperations = headers
	part.weightedTargets[0].requestURLRewrite = &cloud.HTTPURLRewrite{ReplacePrefixMatch: ptr.To("/new")}
	filters, err := compileWeightedTargetRequestFilters(cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/old"}, part.weightedTargets)
	if err != nil {
		t.Fatal(err)
	}
	part.policies[0].targetRequestFilters = filters
	policy := &actual.routes[0].httpParts[part.backendKey].policies[0]
	policy.policy.Position = part.policies[0].position
	policy.adminObserved = true
	policy.rules[0].flagsObserved = true
	resolved := resolveWeightedHTTPPart(*part, desired.routes[0], actual.routes[0], "listener-1")
	targets := resolved.policies[0].weightedPlan.targets
	if len(targets) != 2 || !slices.Equal(targets[0].RequestHeaderOperations, headers) ||
		targets[0].RequestURLRewrite == nil || targets[0].RequestURLRewrite.ReplacePrefix == nil ||
		*targets[0].RequestURLRewrite.ReplacePrefix != (octavia.PathPrefixReplacement{Match: "/old", Replacement: "/new"}) ||
		targets[1].HasRequestFilters() {
		t.Fatalf("target filters leaked or were compiled on the wrong match: %+v", targets)
	}
	var capabilities = []string{octavia.GatewayCapabilityFixedHTTPResponses, octavia.GatewayCapabilityWeightedHTTPTargets}
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora-regional/gateway_capabilities" {
			t.Errorf("unexpected discovery request: %s %s", r.Method, r.URL)
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}), &gatewayTransitionRequests{})
	provider.backendRequestFilters, provider.weightedHTTPTargets = true, true
	open := graphMutation{kind: graphEnableListener, listenerID: actual.listener.ID}
	guard := func(plan ...graphMutation) []graphMutation {
		return provider.protectBackendRequestFilterTraffic(t.Context(), desired, actual, plan, &cloud.GatewayGraphResult{})
	}
	if got := guard(open); len(got) != 1 || got[0].kind != graphDisableListener {
		t.Fatalf("missing regional capability did not close the listener: %+v", got)
	}
	actual.listener.AdminStateUp = false
	if got := guard(open); len(got) != 0 {
		t.Fatalf("regional capability is required before opening: %+v", got)
	}
	capabilities = append(capabilities, octavia.GatewayCapabilityTargetRequestFilters)
	policy.backendRequestFiltersTagged = true
	if got := guard(open); len(got) != 0 {
		t.Fatalf("tag without filter readback reopened the listener: %+v", got)
	}
	leaked := slices.Clone(targets)
	leaked[1].RequestHeaderOperations = headers
	policy.weightedTargets = leaked
	if got := guard(open); len(got) != 0 {
		t.Fatalf("transformation on another pool reopened the listener: %+v", got)
	}
	policy.weightedTargets = targets
	if got := guard(open); len(got) != 1 || got[0].kind != graphEnableListener {
		t.Fatalf("verified target vector did not permit reopening: %+v", got)
	}
	capabilities = capabilities[:2]
	actual.listener.AdminStateUp = true
	if got := guard(open); len(got) != 1 || got[0].kind != graphDisableListener {
		t.Fatalf("regional capability withdrawal did not close the listener: %+v", got)
	}
}
