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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"k8s.io/utils/ptr"
)

func TestRequestRedirectPolicyWireAndReadback(t *testing.T) {
	spec := redirectGraphSpec(t)
	redirect := &spec.Routes[0].Spec.HTTPRedirects[0]
	redirect.Prefix, redirect.StatusCode = "", 303
	redirect.Request = &cloud.HTTPRedirectRequest{Scheme: "https", Port: 8443, ReplacePrefixMatch: ptr.To("")}
	identity := safetyTestIdentity(t)
	listener := listeners.Listener{ID: "listener", Protocol: cloud.ListenerProtocolHTTP, ProjectID: "project-a",
		L7Policies: []l7policies.L7Policy{{ID: "policy"}}}
	var policy map[string]any
	var rules []map[string]any
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/l7policies":
			var body struct {
				Policy map[string]any `json:"l7policy"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			got := body.Policy
			request, ok := got["redirect_request"].(map[string]any)
			if got["action"] != "REDIRECT_REQUEST" || got["redirect_http_code"] != float64(303) || got["admin_state_up"] != false ||
				got["redirect_pool_id"] != nil || got["redirect_prefix"] != nil || !ok ||
				request["scheme"] != "https" || request["port"] != float64(8443) || request["hostname"] != nil {
				t.Fatalf("request redirect creation = %v", got)
			}
			prefix, ok := request["replace_prefix"].(map[string]any)
			if !ok || prefix["match"] != "/app" || prefix["replacement"] != "" {
				t.Fatalf("prefix replacement = %v", request)
			}
			policy = got
			policy["id"], policy["project_id"], policy["provisioning_status"], policy["redirect_pool_id"] = "policy", "project-a", "ACTIVE", nil
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"l7policy": policy})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies":
			_ = json.NewEncoder(w).Encode(map[string]any{"l7policies": []map[string]any{policy}})
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/l7policies/policy/rules":
			_ = json.NewEncoder(w).Encode(map[string]any{"rules": rules})
		default:
			t.Fatalf("unexpected provider request %s %s", r.Method, r.URL.Path)
		}
	}), &gatewayTransitionRequests{})
	provider.httpRequestRedirect = true
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	part.spec.Gateway.ListenerID = listener.ID
	wanted := part.policies[0]
	if err := provider.executeRouteMutation(t.Context(), part, routeMutation{kind: routeMutationCreatePolicy, policy: wanted}); err != nil {
		t.Fatal(err)
	}

	var refs []map[string]string
	for _, rule := range rulesFor(wanted, "") {
		id := "rule-" + string(rule.ruleType)
		rules = append(rules, map[string]any{"id": id, "project_id": "project-a", "provisioning_status": "ACTIVE",
			"tags": safetyRouteTags(t, identity, rule.role), "admin_state_up": true, "invert": rule.invert,
			"type": rule.ruleType, "compare_type": rule.compareType, "value": rule.value})
		refs = append(refs, map[string]string{"id": id})
	}
	policy["rules"], policy["admin_state_up"] = refs, true
	observe := func() observedRouteGraph {
		t.Helper()
		inventory := routeGraphInventory{owners: []routeGraphOwner{{identity: identity}}, graphs: []observedRouteGraph{{}}}
		if err := provider.observeListenerPolicies(t.Context(), &inventory, &listener, cloud.ListenerProtocolHTTP, nil, make(map[int]string), true); err != nil {
			t.Fatal(err)
		}
		return *inventory.graphs[0].part(part.backendKey)
	}
	current := observe()
	if plan, state, err := buildRouteEnsurePlan(part, current); err != nil || len(plan) != 0 || !slices.Equal(state.L7PolicyIDs, []string{"policy"}) {
		t.Fatalf("restart plan = %v, state = %v, error = %v", plan, state, err)
	}

	// A response with a missing replacement does not prove an empty replacement.
	delete(policy["redirect_request"].(map[string]any)["replace_prefix"].(map[string]any), "replacement")
	current = observe()
	if plan, _, err := buildRouteEnsurePlan(part, current); err != nil || len(plan) == 0 || plan[0].kind != routeMutationDisablePolicy {
		t.Fatalf("incomplete readback = %v, error = %v", plan, err)
	}

	// The stock conversion proceeds via removal once the provider reports closure.
	stock := &spec.Routes[0].Spec.HTTPRedirects[0]
	stock.Prefix, stock.Request = "https://moved.example.test", nil
	provider.httpRequestRedirect = false
	desired, err = provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	policy["admin_state_up"] = false
	if plan, _, err := buildRouteEnsurePlan(desired.routes[0].httpParts[0], observe()); err != nil || len(plan) == 0 ||
		plan[0].kind != routeMutationDeleteRule || !plan[0].withdrawHTTPPolicy {
		t.Fatalf("stock conversion = %v, error = %v", plan, err)
	}
}

func TestRequestRedirectCapabilityClosesOnlyTheDependentListener(t *testing.T) {
	capability := `{"gateway_capabilities":[]}`
	probes := 0
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora-region/gateway_capabilities" {
			t.Fatalf("unexpected provider request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		probes++
		_, _ = w.Write([]byte(capability))
	}), &gatewayTransitionRequests{})
	provider.httpRequestRedirect = true
	spec := redirectGraphSpec(t)
	port := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "dependent", Port: port}, {Name: "independent", Port: port + 1}}
	spec.Routes[0].Spec.ScopedListenerPorts = []int{port}
	redirect := &spec.Routes[0].Spec.HTTPRedirects[0]
	redirect.Prefix, redirect.Request = "", &cloud.HTTPRedirectRequest{Scheme: "http", Port: 80}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	part := desired.routes[0].httpParts[0]
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-region"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, protocol: cloud.ListenerProtocolHTTP,
		adminObserved: map[string]bool{"dependent": true, "independent": true}, scopedListeners: []listeners.Listener{
			{ID: "dependent", ProtocolPort: port, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
			{ID: "independent", ProtocolPort: port + 1, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		}, routes: []observedRouteGraph{{}}}
	plan := func() []graphMutation {
		t.Helper()
		result := cloud.GatewayGraphResult{Gateway: cloud.GatewayResult{ScopedListenerReadyPorts: []int{port, port + 1}},
			Routes: []cloud.RouteGraphResult{{Result: cloud.RouteReadyResult(cloud.RouteState{})}}}
		steps := provider.protectRequestRedirectTraffic(t.Context(), desired, actual, []graphMutation{
			{kind: graphRouteMutation, routeIndex: 0, backendKey: part.backendKey, listenerID: "dependent",
				route: routeMutation{kind: routeMutationCreatePolicy}},
			{kind: graphEnableListener, listenerID: "independent"},
		}, &result)
		if !slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{port + 1}) ||
			result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
			t.Fatalf("redirect outage affected independent status or left redirect ready: %v", result)
		}
		return steps
	}
	if got := plan(); len(got) != 1 || got[0].kind != graphDisableListener || got[0].listenerID != "dependent" {
		t.Fatalf("request redirect closure = %v", got)
	}
	actual.scopedListeners[0].AdminStateUp = false
	if got := plan(); len(got) != 1 || got[0].listenerID != "independent" {
		t.Fatalf("capability outage must leave independent writes eligible: %v", got)
	}
	capability = `{"gateway_capabilities":["http_request_redirect_v1"]}`
	if got := plan(); len(got) != 2 || got[0].route.kind != routeMutationCreatePolicy {
		t.Fatalf("regional enrollment must allow staging on the closed listener: %v", got)
	}

	wanted := part.policies[0]
	current := observedRoutePolicy{role: wanted.role, redirect: true, adminObserved: true, requestRedirectPresent: true,
		requestRedirect: wanted.redirect.request, policy: l7policies.L7Policy{
			ID: "policy", ListenerID: "dependent", Action: string(octavia.ActionRedirectRequest), Position: wanted.position,
			AdminStateUp: true, ProvisioningStatus: "ACTIVE", RedirectHttpCode: wanted.redirect.statusCode,
		}}
	for _, rule := range rulesFor(wanted, "") {
		current.rules = append(current.rules, observedRouteRule{role: rule.role, flagsObserved: true, rule: l7policies.Rule{
			AdminStateUp: true, ProvisioningStatus: "ACTIVE", RuleType: string(rule.ruleType),
			CompareType: string(rule.compareType), Key: rule.key, Value: rule.value, Invert: rule.invert,
		}})
	}
	actual.routes[0].part(part.backendKey).policies = []observedRoutePolicy{current}
	restore := []graphMutation{{kind: graphEnableListener, listenerID: "dependent"}}
	result := cloud.GatewayGraphResult{}
	if got := provider.protectRequestRedirectTraffic(t.Context(), desired, actual, restore, &result); len(got) != 1 {
		t.Fatalf("verified destination and predicates must permit reopening: %v", got)
	}

	observedProbes := probes
	provider.httpRequestRedirect = false
	spec.Routes[0].Disposition = cloud.GraphDelete
	desired, err = provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	withdraw := graphMutation{kind: graphRouteMutation, listenerID: "dependent", backendKey: part.backendKey,
		route: routeMutation{kind: routeMutationDisablePolicy, id: "policy"}, withdrawHTTPPolicy: true}
	if got := provider.protectRequestRedirectTraffic(t.Context(), desired, actual, []graphMutation{withdraw}, &result); len(got) != 1 || got[0].route.kind != routeMutationDisablePolicy || probes != observedProbes {
		t.Fatalf("opt-out must permit withdrawal without regional discovery: %v, probes=%d", got, probes)
	}
}

func TestRequestRedirectShadowingAndUnsupportedGuard(t *testing.T) {
	spec := redirectGraphSpec(t)
	route := &spec.Routes[0].Spec
	redirect := &route.HTTPRedirects[0]
	redirect.Prefix, redirect.Request = "", &cloud.HTTPRedirectRequest{Scheme: "http", Port: 80}
	forward := completeGraphSpec(safetyTestIdentity(t)).Routes[0]
	forward.Spec.Identity.RouteName, forward.Spec.Identity.RouteUID = "forward", "forward"
	forward.Spec.Hostname, forward.Spec.PathType, forward.Spec.PathValue, forward.Spec.Precedence =
		"*.example.test", cloud.PathMatchPrefix, "/app", 1
	spec.Routes = append(spec.Routes, forward)

	provider := &Provider{projectID: "project-a", httpRequestRedirect: true, httpMethodMatch: true, fixedHTTPResponses: true}
	check := func(want error) {
		t.Helper()
		desired, err := provider.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		for _, route := range desired.routes {
			for _, part := range route.httpParts {
				if part.redirect {
					if len(part.policies) != 0 || part.redirectIssue != want {
						t.Fatalf("unexpected redirect policies or issue: %v, %v", part.policies, part.redirectIssue)
					}
					return
				}
			}
		}
		t.Fatal("redirect match disappeared")
	}
	check(nil)
	f := &spec.Routes[1].Spec
	f.HTTPBackends = []cloud.HTTPBackend{{Namespace: "apps", Name: "api", Port: 80, Members: f.Members,
		Matches: []cloud.HTTPPathMatch{{Hostname: f.Hostname, PathType: f.PathType, PathValue: f.PathValue, Precedence: f.Precedence, Method: "GET"}}}}
	f.Hostname, f.PathType, f.PathValue, f.Members = "", "", "", nil
	check(errHTTPMethodRedirectPrecedence)
}
