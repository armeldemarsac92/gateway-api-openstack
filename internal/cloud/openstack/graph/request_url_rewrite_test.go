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
	"net/http"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"k8s.io/utils/ptr"
)

func TestRequestURLRewriteRequiresRegionalCapabilityAndVerifiedReadback(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	port := spec.Gateway.Spec.ListenerPort
	route.ScopedListenerPorts = []int{port}
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Port: port, Name: "dependent"}, {Port: port + 1, Name: "independent"}}
	headers := []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderRemove, Name: "x-obsolete"}}
	route.HTTPBackends = []cloud.HTTPBackend{{Namespace: "apps", Name: "api", Port: 8080, Members: route.Members,
		Matches: []cloud.HTTPPathMatch{{Hostname: "public.example.test", PathType: cloud.PathMatchPrefix, PathValue: "/incoming/",
			URLRewrite:              &cloud.HTTPURLRewrite{Hostname: "backend.example.test", ReplacePrefixMatch: ptr.To("/replacement")},
			RequestHeaderOperations: headers}}}}
	route.Members, route.PathType, route.PathValue, route.Hostname = nil, "", "", ""

	var capabilities []string
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora-regional/gateway_capabilities" {
			t.Fatalf("unexpected discovery request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string][]string{"gateway_capabilities": capabilities}); err != nil {
			t.Fatal(err)
		}
	}), &gatewayTransitionRequests{})
	provider.httpRequestURLRewrite, provider.requestHeaderModifier = true, true
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	wanted := part.policies[0]
	if wanted.hostname != "public.example.test" || wanted.path != "^/incoming($|/)" ||
		wanted.requestURLRewrite == nil || wanted.requestURLRewrite.ReplacePrefix == nil ||
		*wanted.requestURLRewrite.ReplacePrefix != (octavia.PathPrefixReplacement{Match: "/incoming", Replacement: "/replacement"}) {
		t.Fatalf("policy changed original predicates or lost its rewrite: %+v", wanted)
	}
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-regional"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, protocol: cloud.ListenerProtocolHTTP,
		adminObserved: map[string]bool{"dependent": true, "independent": true}, scopedListeners: []listeners.Listener{
			{ID: "dependent", ProtocolPort: port, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "independent", ProtocolPort: port + 1, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		}, routes: []observedRouteGraph{{}}}
	observed := actual.routes[0].part(part.backendKey)
	observed.pool = &pools.Pool{ID: "backend-pool"}
	create := graphMutation{kind: graphRouteMutation, routeIndex: 0, backendKey: part.backendKey, listenerID: "dependent",
		route: routeMutation{kind: routeMutationCreatePolicy, policy: wanted, requestHeaderOperations: headers}}
	open := graphMutation{kind: graphEnableListener, listenerID: "dependent"}
	openIndependent := graphMutation{kind: graphEnableListener, listenerID: "independent"}
	guard := func(plan ...graphMutation) []graphMutation {
		result := cloud.GatewayGraphResult{Gateway: cloud.GatewayResult{ScopedListenerReadyPorts: []int{port, port + 1}},
			Routes: []cloud.RouteGraphResult{{Result: cloud.RouteReadyResult(cloud.RouteState{})}}}
		return provider.protectRequestURLRewriteTraffic(t.Context(), desired, actual, plan, &result)
	}

	capabilities = []string{}
	if got := guard(create, open, openIndependent); len(got) != 1 || got[0].kind != graphDisableListener || got[0].listenerID != "dependent" {
		t.Fatalf("did not close the affected port before staging: %+v", got)
	}
	actual.scopedListeners[0].AdminStateUp = false
	actual.scopedListeners[0].ProvisioningStatus = "PENDING_UPDATE"
	capabilities = []string{octavia.GatewayCapabilityRequestURLRewrite}
	if got := guard(create, open, openIndependent); len(got) != 1 || got[0].listenerID != "independent" {
		t.Fatalf("pending closure allowed a dependent write or blocked the independent port: %+v", got)
	}
	actual.scopedListeners[0].ProvisioningStatus = "ACTIVE"
	if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, create); err == nil {
		t.Fatal("rewrite capability authorized the independent header modifier")
	} else if missing := new(octavia.MissingGatewayCapability); !errors.As(err, &missing) || missing.Capability != octavia.GatewayCapabilityRequestHeaderOperations {
		t.Fatalf("prepare with missing header capability: %v", err)
	}
	capabilities = append(capabilities, octavia.GatewayCapabilityRequestHeaderOperations)
	if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, create); err != nil {
		t.Fatalf("both regional capabilities did not authorize staging: %v", err)
	}
	if got := guard(create, open, openIndependent); len(got) != 2 || got[0].route.kind != routeMutationCreatePolicy || got[1].listenerID != "independent" {
		t.Fatalf("expected staging but not reopening while unverified: %+v", got)
	}
	policy := observedRoutePolicy{role: wanted.role, adminObserved: true, urlRewriteTagged: true, urlRewritePresent: true, urlRewriteReported: true,
		policy: l7policies.L7Policy{ID: "policy", ListenerID: "dependent", Action: string(l7policies.ActionRedirectToPool),
			RedirectPoolID: "backend-pool", Position: wanted.position, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
			Tags: []string{openstackidentity.RequestURLRewritePolicyTag, "ownership"}}}
	for _, rule := range rulesFor(wanted, "") {
		policy.rules = append(policy.rules, observedRouteRule{role: rule.role, flagsObserved: true,
			rule: l7policies.Rule{RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value, Key: rule.key,
				Invert: rule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}})
	}
	observed.policies = []observedRoutePolicy{policy}
	if got := guard(open); len(got) != 0 {
		t.Fatalf("malformed readback reopened the port: %+v", got)
	}
	observed.policies[0].requestURLRewrite = wanted.requestURLRewrite
	if got := guard(open); len(got) != 0 {
		t.Fatalf("missing header modifier readback reopened the port: %+v", got)
	}
	observed.policies[0].requestHeaderOperations = headers
	if got := guard(open); len(got) != 1 || got[0].kind != graphEnableListener {
		t.Fatalf("verified readback did not reopen the port: %+v", got)
	}

	capabilities = []string{octavia.GatewayCapabilityRequestURLRewrite}
	actual.scopedListeners[0].AdminStateUp = true
	if got := guard(openIndependent); len(got) != 1 || got[0].kind != graphDisableListener || got[0].listenerID != "dependent" {
		t.Fatalf("request header capability withdrawal did not close only its port: %+v", got)
	}
	actual.scopedListeners[0].AdminStateUp = false
	if got := guard(create, open, openIndependent); len(got) != 1 || got[0].listenerID != "independent" {
		t.Fatalf("request header capability withdrawal blocked an independent port: %+v", got)
	}
	ruleStep := create
	ruleStep.route.kind, ruleStep.route.requestHeaderOperations = routeMutationCreateRule, nil
	if _, err := provider.prepareGraphMutation(t.Context(), desired, actual, ruleStep); err == nil {
		t.Fatal("a mutation of the combined policy only checked its rewrite capability")
	}

	capabilities = []string{octavia.GatewayCapabilityRequestHeaderOperations}
	actual.scopedListeners[0].AdminStateUp = true
	if got := guard(); len(got) != 1 || got[0].kind != graphDisableListener {
		t.Fatalf("regional withdrawal did not close the active port: %+v", got)
	}
	stock := part
	stock.policies = slices.Clone(part.policies)
	stock.policies[0].requestURLRewrite = nil
	plan, _, _ := buildRoutePolicyPlan(stock, observed.policies, "backend-pool")
	if len(plan) == 0 || plan[0].kind != routeMutationSetPolicyURLRewrite ||
		plan[0].requestURLRewriteUpdate == nil || plan[0].requestURLRewriteUpdate.Value != nil ||
		!slices.Equal(plan[0].requestURLRewriteUpdate.Tags, []string{"ownership"}) {
		t.Fatalf("extended API rollback did not produce a null update: %+v", plan)
	}
	observed.policies[0].urlRewritePresent, observed.policies[0].urlRewriteReported = false, false
	observed.policies[0].requestURLRewrite = nil
	plan, disables, _ := buildRoutePolicyPlan(stock, observed.policies, "backend-pool")
	if len(plan) != 0 || len(disables) != 1 || disables[0].kind != routeMutationDisablePolicy {
		t.Fatalf("stock API omission did not require disabling the tagged policy: %+v %+v", plan, disables)
	}
	observed.policies[0].policy.AdminStateUp = false
	plan, disables, _ = buildRoutePolicyPlan(stock, observed.policies, "backend-pool")
	if len(plan) <= 1 || plan[len(plan)-1].kind != routeMutationDeletePolicy || len(disables) != 0 {
		t.Fatalf("disabled tagged policy did not have a stock-compatible deletion plan: %+v", plan)
	}
	desired.spec.Routes[0].Disposition = cloud.GraphDelete
	actual.scopedListeners[0].AdminStateUp = false
	if got := guard(graphMutation{kind: graphRouteMutation, listenerID: "dependent", withdrawHTTPPolicy: true, route: plan[0]}, open); len(got) != 1 || got[0].kind != graphRouteMutation {
		t.Fatalf("route removal did not proceed with the affected port still closed: %+v", got)
	}
}

func TestRequestURLRewriteRollbackRemovesExactGroupedPolicyBeforeScalarReplacement(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Routes[0].Spec.Hostname = ""
	spec.Routes[0].Spec.PathType, spec.Routes[0].Spec.PathValue = cloud.PathMatchExact, "/one"
	candidate := spec
	candidate.Routes = slices.Clone(spec.Routes)
	route := &candidate.Routes[0].Spec
	route.HTTPBackends = []cloud.HTTPBackend{{Namespace: "apps", Name: "api", Port: 8080, Members: route.Members,
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/one",
			URLRewrite: &cloud.HTTPURLRewrite{ReplaceFullPath: ptr.To("/replacement")}}}}}
	route.Members, route.PathType, route.PathValue = nil, "", ""
	p := &Provider{projectID: "project-a", httpRequestURLRewrite: true}
	prior, err := p.buildDesiredGatewayGraph(candidate)
	if err != nil {
		t.Fatal(err)
	}
	p.httpRequestURLRewrite = false
	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	port := spec.Gateway.Spec.ListenerPort
	listener := listeners.Listener{ID: "listener", Protocol: "HTTP", ProtocolPort: port, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolHTTP, listener: &listener, loadBalancer: safetyObservedLoadBalancer(),
		adminObserved: map[string]bool{listener.ID: true}, routes: []observedRouteGraph{{}}}
	part := prior.routes[0].httpParts[0]
	old := actual.routes[0].part(part.backendKey)
	old.pool = &pools.Pool{ID: "old-backend", ProvisioningStatus: "ACTIVE"}
	old.policies = []observedRoutePolicy{observedPolicyForRewriteTest(part.policies[0], listener.ID, old.pool.ID)}
	plan := func() []graphMutation {
		t.Helper()
		steps, result, err := p.buildGatewayGraphPlan(desired, actual)
		if err != nil {
			t.Fatal(err)
		}
		return p.protectRequestURLRewriteTraffic(t.Context(), desired, actual, steps, &result)
	}
	if steps := plan(); len(steps) != 1 || steps[0].kind != graphDisableListener {
		t.Fatalf("active rewrite was not closed before rollback: %+v", steps)
	}
	listener.AdminStateUp = false
	for _, want := range []routeMutationKind{routeMutationDisablePolicy, routeMutationDeleteRule, routeMutationDeletePolicy} {
		steps := plan()
		if len(steps) == 0 || steps[0].kind != graphRouteMutation || steps[0].route.kind != want || steps[0].backendKey != part.backendKey {
			t.Fatalf("rollback must retire old policy before its scalar replacement, want %v, got %+v", want, steps)
		}
		switch want {
		case routeMutationDisablePolicy:
			old.policies[0].policy.AdminStateUp = false
		case routeMutationDeleteRule:
			old.policies[0].rules = nil
		}
	}
}

func TestRequestURLRewriteRetriesErrorOnOwnedPort(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		spec := completeGraphSpec(safetyTestIdentity(t))
		port := spec.Gateway.Spec.ListenerPort
		if scoped {
			spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "rewrite", Port: port}, {Name: "other", Port: port + 1}}
		}
		p := &Provider{projectID: "project-a"}
		spec.Routes[0].Disposition = cloud.GraphDelete
		if scoped {
			spec.Routes[0].Spec.ScopedListenerPorts = []int{port}
		}
		desired, err := p.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		listener := listeners.Listener{ID: "listener", Protocol: "HTTP", ProtocolPort: port, ProvisioningStatus: "ERROR"}
		other := listeners.Listener{ID: "other", Protocol: "HTTP", ProtocolPort: port + 1, ProvisioningStatus: "ERROR"}
		actual := observedGatewayGraph{protocol: cloud.ListenerProtocolHTTP, loadBalancer: safetyObservedLoadBalancer(),
			adminObserved: map[string]bool{listener.ID: true}, scoped: scoped, routes: []observedRouteGraph{{}}}
		if scoped {
			actual.scopedListeners = []listeners.Listener{listener, other}
		} else {
			actual.listener = &listener
		}
		actual.routes[0].part("retired").policies = []observedRoutePolicy{{urlRewriteTagged: true,
			policy: l7policies.L7Policy{ID: "owned", ListenerID: listener.ID}}}
		actual.loadBalancer.ProvisioningStatus = "PENDING_UPDATE"
		if rewriteListenerNeedsRecovery(desired, actual, listener) {
			t.Fatalf("ERROR recovery must wait for the parent load balancer, scoped=%t", scoped)
		}
		actual.loadBalancer.ProvisioningStatus = "ACTIVE"
		if !rewriteListenerNeedsRecovery(desired, actual, listener) || rewriteListenerNeedsRecovery(desired, actual, other) {
			t.Fatalf("ERROR recovery did not isolate the selected HTTP port, scoped=%t", scoped)
		}
		steps := p.protectRequestURLRewriteTraffic(t.Context(), desired, actual, []graphMutation{{kind: graphEnableListener, listenerID: other.ID}}, &cloud.GatewayGraphResult{})
		if len(steps) != 1 || steps[0].kind != graphDisableListener || steps[0].listenerID != listener.ID {
			t.Fatalf("ERROR closure must be retried before reopening, scoped=%t, plan=%+v", scoped, steps)
		}
	}
}

func TestRequestURLRewriteWithdrawsToOnlyLocalWeightedResponses(t *testing.T) {
	const listenerID, poolID = "listener", "old-backend"
	route := desiredRoute{backendKey: "weighted", weightedTargets: []desiredHTTPWeightedTarget{{weight: 2}, {weight: 1}, {backendKey: "zero", weight: 0}},
		policies: []desiredPolicy{{role: "policy", position: 1, path: "/weighted", compareType: l7policies.CompareTypeEqual,
			requestURLRewrite: &octavia.RequestURLRewrite{Hostname: "backend.example.test"}}}}
	old := observedPolicyForRewriteTest(route.policies[0], listenerID, "")
	old.policy.Action = string(octavia.ActionWeightedTargets)
	old.weightedTargets = []octavia.WeightedHTTPTarget{{PoolID: poolID, Weight: 2}, {PoolID: "other-backend", Weight: 1}}
	resolved := resolveWeightedHTTPPart(route, desiredRoute{}, observedRouteGraph{}, listenerID)
	wanted := resolved.policies[0]
	if wanted.requestURLRewrite != nil || wanted.targetsPending() || len(wanted.weightedPlan.targets) != 3 {
		t.Fatalf("local-only rule retains provider rewrites or pending pools: %+v", wanted)
	}
	for _, target := range wanted.weightedPlan.targets {
		if target.PoolID != "" || target.FixedResponse == nil || target.FixedResponse.StatusCode != 500 {
			t.Fatalf("unexpected nonlocal target: %+v", target)
		}
	}
	steps := weightedTargetWithdrawals(resolved, observedRouteGraph{policies: []observedRoutePolicy{old}})
	if len(steps) != 0 {
		t.Fatalf("unauthorized observation permitted a policy write: %+v", steps)
	}
	resolved.weightedAuthorizations = map[string]map[string]int{old.policy.ID: {}}
	steps = weightedTargetWithdrawals(resolved, observedRouteGraph{policies: []observedRoutePolicy{old}})
	if len(steps) != 1 || steps[0].kind != routeMutationSetPolicyTarget ||
		rewritePolicyWriteRequiresCapability(resolved, steps[0]) || steps[0].requestURLRewriteUpdate == nil ||
		steps[0].requestURLRewriteUpdate.Value != nil || !steps[0].requestURLRewriteUpdate.KeepPolicyState {
		t.Fatalf("did not choose an atomic local-only withdrawal without rewrite capability: %+v", steps)
	}
	gatewayStep := graphMutation{kind: graphRouteMutation, route: steps[0], listenerID: listenerID}
	if !localOnlyRewriteWithdrawalOnPort([]graphMutation{gatewayStep}, guardedHTTPListener{obsoleteIDs: map[string]bool{old.policy.ID: true}}) {
		t.Fatal("local-only restrictive update could not proceed without closing the port")
	}

	old.corsTagged = true
	steps = weightedTargetWithdrawals(resolved, observedRouteGraph{policies: []observedRoutePolicy{old}})
	if len(steps) != 1 || steps[0].kind != routeMutationDisablePolicy || steps[0].weightedTargets != nil {
		t.Fatalf("CORS requires observing a disabled policy before changing targets: %+v", steps)
	}
	old.policy.AdminStateUp = false
	old.policy.ProvisioningStatus = "PENDING_UPDATE"
	if steps = weightedTargetWithdrawals(resolved, observedRouteGraph{policies: []observedRoutePolicy{old}}); len(steps) != 0 {
		t.Fatalf("target update started before the disable settled: %+v", steps)
	}
	old.policy.ProvisioningStatus = "ACTIVE"
	steps = weightedTargetWithdrawals(resolved, observedRouteGraph{policies: []observedRoutePolicy{old}})
	if len(steps) != 1 || steps[0].kind != routeMutationSetPolicyTarget || steps[0].weightedTargets == nil ||
		steps[0].requestURLRewriteUpdate == nil || steps[0].requestURLRewriteUpdate.Value != nil {
		t.Fatalf("settled disable did not permit restrictive withdrawal: %+v", steps)
	}
}

func observedPolicyForRewriteTest(wanted desiredPolicy, listenerID, poolID string) observedRoutePolicy {
	observed := observedRoutePolicy{role: wanted.role, adminObserved: true, urlRewriteTagged: true, urlRewritePresent: true,
		urlRewriteReported: true, requestURLRewrite: wanted.requestURLRewrite, versioned: true,
		policy: l7policies.L7Policy{ID: "policy", ListenerID: listenerID, RedirectPoolID: poolID, Action: string(l7policies.ActionRedirectToPool),
			Position: wanted.position, AdminStateUp: true, ProvisioningStatus: "ACTIVE", Tags: []string{openstackidentity.RequestURLRewritePolicyTag, "ownership"}}}
	for _, rule := range rulesFor(wanted, "") {
		observed.rules = append(observed.rules, observedRouteRule{role: rule.role, flagsObserved: true,
			rule: l7policies.Rule{ID: "old-rule", RuleType: string(rule.ruleType), CompareType: string(rule.compareType),
				Value: rule.value, Key: rule.key, Invert: rule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}})
	}
	return observed
}
