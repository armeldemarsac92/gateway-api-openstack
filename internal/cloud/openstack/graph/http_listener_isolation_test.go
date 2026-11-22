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
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func TestPlainHTTPListenerIsolationReservesHostnamesBeforeRouteMatching(t *testing.T) {
	provider := &Provider{projectID: "project-a"}
	spec := completeGraphSpec(safetyTestIdentity(t))
	port := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.HTTPListenerHostnames = map[int][]string{port: {"*", "*.bar.com", "*.foo.bar.com", "foo.bar.com"}}
	fallback := spec.Routes[0]
	fallback.Spec.HTTPListenerHostname, fallback.Spec.Hostname = "*", ""
	fallback.Spec.PathType, fallback.Spec.PathValue = cloud.PathMatchPrefix, "/"
	wildcard := fallback
	wildcard.Spec.Identity.RouteName, wildcard.Spec.Identity.RouteUID = "wildcard", "wildcard"
	wildcard.Spec.HTTPListenerHostname, wildcard.Spec.Hostname = "*.bar.com", "*.bar.com"
	wildcard.Spec.PathType, wildcard.Spec.PathValue = cloud.PathMatchExact, "/wildcard"
	nested := wildcard
	nested.Spec.Identity.RouteName, nested.Spec.Identity.RouteUID = "nested", "nested"
	nested.Spec.HTTPListenerHostname, nested.Spec.Hostname = "*.foo.bar.com", "*.foo.bar.com"
	nested.Spec.PathValue = "/nested"
	exact := wildcard
	exact.Spec.Identity.RouteName, exact.Spec.Identity.RouteUID = "exact", "exact"
	exact.Spec.HTTPListenerHostname, exact.Spec.Hostname = "foo.bar.com", "foo.bar.com"
	exact.Spec.PathValue = "/exact"
	wildcard.Spec.HTTPBackends = []cloud.HTTPBackend{{
		Namespace: "default", Name: "wildcard", Port: 80, Members: wildcard.Spec.Members,
		Matches: []cloud.HTTPPathMatch{
			{HTTPListenerHostname: "*.bar.com", Hostname: "*.bar.com", PathType: cloud.PathMatchExact, PathValue: "/wildcard"},
			{HTTPListenerHostname: "*.bar.com", Hostname: "*.bar.com", PathType: cloud.PathMatchExact, PathValue: "/second", Precedence: 2},
		},
	}}
	wildcard.Spec.Hostname, wildcard.Spec.PathType, wildcard.Spec.PathValue = "", "", ""
	wildcard.Spec.Members = nil
	redirect := redirectGraphSpec(t).Routes[0]
	redirect.Spec.Identity.RouteName, redirect.Spec.Identity.RouteUID = "redirect", "redirect"
	redirect.Spec.HTTPRedirects[0].Matches = []cloud.HTTPPathMatch{{
		HTTPListenerHostname: "*.bar.com", Hostname: "*.bar.com", PathType: cloud.PathMatchExact,
		PathValue: "/move", Precedence: 1,
	}}
	spec.Routes = []cloud.RouteGraphNode{fallback, wildcard, nested, exact, redirect}

	probe := func(host, path, want string) {
		t.Helper()
		desired, err := provider.buildDesiredGatewayGraph(spec)
		if err != nil {
			t.Fatal(err)
		}
		var matches []string
		for _, route := range desired.routes {
			for _, policies := range route.policySets() {
				for _, policy := range policies {
					matched := true
					for _, rule := range rulesFor(policy, route.hostnameRuleValue) {
						value := path
						if rule.ruleType == l7policies.TypeHostName {
							value = strings.ToLower(host)
						}
						var accepts bool
						switch rule.compareType {
						case l7policies.CompareTypeEqual:
							accepts = rule.value == value
						case l7policies.CompareTypeStartWith:
							accepts = strings.HasPrefix(value, rule.value)
						case l7policies.CompareTypeRegex:
							accepts = regexp.MustCompile(rule.value).MatchString(value)
						default:
							t.Fatalf("unexpected test rule: %#v", rule)
						}
						matched = matched && accepts != rule.invert
					}
					if matched {
						matches = append(matches, route.spec.Identity.RouteName)
					}
				}
			}
		}
		var expected []string
		if want != "" {
			expected = []string{want}
		}
		if !slices.Equal(matches, expected) {
			t.Fatalf("Host %q Path %q matches %v, want %v", host, path, matches, expected)
		}
	}
	probe("FOO.BAR.COM:8080", "/exact", "exact")
	probe("foo.bar.com", "/wildcard", "")
	probe("deep.foo.bar.com", "/nested", "nested")
	probe("deep.foo.bar.com", "/wildcard", "")
	probe("deep.sub.bar.com", "/wildcard", "wildcard")
	probe("deep.sub.bar.com", "/second", "wildcard")
	probe("foo.bar.com", "/second", "")
	probe("deep.sub.bar.com", "/wrong-path", "")
	probe("bar.com", "/", fallback.Spec.Identity.RouteName)
	probe("foo.bar.com", "/move", "")
	probe("deep.sub.bar.com", "/move", "redirect")

	spec.Routes = slices.Delete(spec.Routes, 3, 4)
	probe("foo.bar.com", "/wildcard", "")
	spec.Gateway.Spec.HTTPListenerHostnames[port] = []string{"*", "*.bar.com", "*.foo.bar.com"}
	probe("foo.bar.com", "/wildcard", "wildcard")
}

func TestPlainHTTPListenerIsolationClosesFrontendBeforeChangingRules(t *testing.T) {
	provider := &Provider{projectID: "project-a"}
	spec := completeGraphSpec(safetyTestIdentity(t))
	port := spec.Gateway.Spec.ListenerPort
	route := &spec.Routes[0].Spec
	route.Hostname = "*.bar.com"
	prev, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Gateway.Spec.HTTPListenerHostnames = map[int][]string{port: {"*.bar.com", "foo.bar.com"}}
	route.HTTPListenerHostname = "*.bar.com"
	desired, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	frontend := &listeners.Listener{ID: "listener", Protocol: cloud.ListenerProtocolHTTP, ProtocolPort: port, ProvisioningStatus: "ACTIVE", AdminStateUp: true}
	policy := observedRoutePolicy{
		adminObserved: true, role: prev.routes[0].policies[0].role,
		policy: l7policies.L7Policy{ID: "policy", ListenerID: frontend.ID, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
	}
	oldRules := rulesFor(prev.routes[0].policies[0], prev.routes[0].hostnameRuleValue)
	for _, rule := range oldRules {
		policy.rules = append(policy.rules, observedRouteRule{role: rule.role, flagsObserved: true, rule: l7policies.Rule{
			RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		}})
	}
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolHTTP, listener: frontend,
		loadBalancer: &octavia.LoadBalancer{}, adminObserved: map[string]bool{frontend.ID: true},
		routes: []observedRouteGraph{{policies: []observedRoutePolicy{policy}}},
	}
	plan, result, err := provider.buildGatewayGraphPlan(desired, actual)
	if err != nil || len(plan) != 1 || plan[0].kind != graphDisableListener || plan[0].listenerID != frontend.ID ||
		!plan[0].withdrawHTTPPolicy || !result.Gateway.HTTPPolicyTransition {
		t.Fatalf("after adding a more-specific listener, plan = %#v, result = %#v, error = %v", plan, result, err)
	}

	// Readback supplies the actual inverted rule after the port is closed.
	frontend.AdminStateUp = false
	guards := desired.routes[0].policies[0].httpHostExclusions
	if len(guards) != 1 {
		t.Fatalf("want one hostname exclusion, got %#v", guards)
	}
	guard := guards[0]
	role := policyRuleRole(desired.routes[0].policies[0], openstackidentity.RoleRuleHTTPHostExclusion, redirectGuardKey(guard))
	actual.routes[0].policies[0].rules = append(policy.rules, observedRouteRule{
		base: openstackidentity.RoleRuleHTTPHostExclusion, role: role, flagsObserved: true,
		rule: l7policies.Rule{RuleType: string(guard.ruleType), CompareType: string(guard.compareType), Value: guard.value, Invert: true, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
	})
	enable := []graphMutation{{kind: graphEnableListener, listenerID: frontend.ID}}
	if plan := provider.protectPlainHTTPIsolation(desired, actual, enable, &result); len(plan) != 1 || plan[0].kind != graphEnableListener {
		t.Fatalf("verified exclusion must allow reopening, plan %#v", plan)
	}

	frontend.AdminStateUp = true
	if plan := provider.protectPlainHTTPIsolation(prev, actual, enable, &result); len(plan) != 1 || plan[0].kind != graphDisableListener {
		t.Fatalf("removing the listener must reconcile the obsolete exclusion, plan %#v", plan)
	}
}

func TestPlainHTTPRedirectPathChangeWithdrawsOnlyItsObsoletePolicy(t *testing.T) {
	provider := &Provider{projectID: "project-a"}
	spec := redirectGraphSpec(t)
	port := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.HTTPListenerHostnames = map[int][]string{port: {"*.example.test", "exact.example.test"}}
	match := &spec.Routes[0].Spec.HTTPRedirects[0].Matches[0]
	match.HTTPListenerHostname = "*.example.test"
	match.PathType = cloud.PathMatchExact
	before, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	original := before.routes[0].httpParts[0]
	wanted := original.policies[0]
	frontend := &listeners.Listener{ID: "listener", Protocol: cloud.ListenerProtocolHTTP, ProtocolPort: port,
		ProvisioningStatus: "ACTIVE", AdminStateUp: true}
	observation := observedRoutePolicy{
		role: wanted.role, adminObserved: true, redirect: true, versioned: true,
		policy: l7policies.L7Policy{ID: "obsolete", ListenerID: frontend.ID, Position: 1, AdminStateUp: true,
			ProvisioningStatus: "ACTIVE", Action: string(l7policies.ActionRedirectPrefix),
			RedirectPrefix: wanted.redirect.prefix, RedirectHttpCode: wanted.redirect.statusCode},
	}
	for _, rule := range rulesFor(wanted, "") {
		observation.rules = append(observation.rules, observedPolicyRule(rule))
	}
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolHTTP, listener: frontend, loadBalancer: &octavia.LoadBalancer{},
		adminObserved: map[string]bool{frontend.ID: true},
		routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{
			original.backendKey: {policies: []observedRoutePolicy{observation}},
		}}},
	}
	match.PathValue = "/new"
	after, err := provider.buildDesiredGatewayGraph(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := provider.buildGatewayGraphPlan(after, actual)
	if err != nil || len(plan) != 1 || plan[0].kind != graphRouteMutation || plan[0].route.kind != routeMutationDisablePolicy ||
		plan[0].route.id != observation.policy.ID || !plan[0].withdrawHTTPPolicy {
		t.Fatalf("route-only edit must withdraw its obsolete redirect, plan %#v, error %v", plan, err)
	}
}

func TestPlainHTTPHostnameReplacementAtOctaviaRuleLimit(t *testing.T) {
	wanted := desiredPolicy{role: rolePolicyExact, position: 1, path: "/path", compareType: l7policies.CompareTypeEqual}
	for i := 1; i < maxOctaviaL7RulesPerPolicy; i++ {
		wanted.httpHostExclusions = append(wanted.httpHostExclusions, redirectGuard{
			ruleType: l7policies.TypeHostName, compareType: l7policies.CompareTypeRegex, value: fmt.Sprintf("^host-%d[.]example[.]test$", i),
		})
	}
	desired := desiredRoute{policies: []desiredPolicy{wanted}}
	observation := observedRoutePolicy{
		role: wanted.role, adminObserved: true,
		policy: l7policies.L7Policy{ID: "policy", Position: 1, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
			Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: "pool"},
	}
	for _, rule := range rulesFor(wanted, "") {
		observation.rules = append(observation.rules, observedPolicyRule(rule))
	}
	last := &observation.rules[len(observation.rules)-1]
	last.role, last.rule.ID, last.rule.Value = "obsolete", "obsolete", "^retired[.]example[.]test$"

	plan, disable, _ := buildRoutePolicyPlan(desired, []observedRoutePolicy{observation}, "pool")
	if len(plan) != 0 || len(disable) != 1 || disable[0].kind != routeMutationDisablePolicy {
		t.Fatalf("active full policy must be disabled, plan %#v, disables %#v", plan, disable)
	}
	observation.policy.AdminStateUp = false
	observation.policy.ProvisioningStatus = "ERROR"
	plan, disable, _ = buildRoutePolicyPlan(desired, []observedRoutePolicy{observation}, "pool")
	if len(plan) != 0 || len(disable) != 1 || !disable[0].retry {
		t.Fatalf("uncertain disable must be retried, plan %#v, disables %#v", plan, disable)
	}
	observation.policy.ProvisioningStatus = "ACTIVE"
	plan, disable, _ = buildRoutePolicyPlan(desired, []observedRoutePolicy{observation}, "pool")
	if len(disable) != 0 || len(plan) != 2 || plan[0].kind != routeMutationDeleteRule || plan[0].id != "obsolete" ||
		plan[1].kind != routeMutationCreateRule {
		t.Fatalf("disable readback must permit deletion before creation, plan %#v, disables %#v", plan, disable)
	}
	observation.rules = observation.rules[:len(observation.rules)-1]
	plan, _, _ = buildRoutePolicyPlan(desired, []observedRoutePolicy{observation}, "pool")
	if len(plan) != 1 || plan[0].kind != routeMutationCreateRule || !plan[0].rule.invert {
		t.Fatalf("once space is observed, retry must create the replacement exclusion, plan %#v", plan)
	}
}

func observedPolicyRule(rule desiredRule) observedRouteRule {
	return observedRouteRule{role: rule.role, flagsObserved: true, rule: l7policies.Rule{
		RuleType: string(rule.ruleType), CompareType: string(rule.compareType), Value: rule.value,
		Invert: rule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE",
	}}
}
