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
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

// Evaluate the path and hostname predicates emitted for redirects. HAProxy checks
// the same wire cases independently in the qualification fixture.
func redirectGuardMatches(t *testing.T, rule redirectGuard, host, path string) bool {
	t.Helper()
	value := path
	if rule.ruleType == l7policies.TypeHostName {
		value = strings.ToLower(host)
	}
	switch rule.compareType {
	case l7policies.CompareTypeEqual:
		return value == rule.value
	case l7policies.CompareTypeStartWith:
		if rule.ruleType == octavia.RuleTypeHTTPPathLiteral {
			return value == rule.value || strings.HasPrefix(value, rule.value+"/")
		}
		return strings.HasPrefix(value, rule.value)
	case l7policies.CompareTypeRegex:
		return regexp.MustCompile(rule.value).MatchString(value)
	default:
		t.Fatalf("unexpected guard %v", rule)
		return false
	}
}

func eligibleRedirectCells(t *testing.T, cells []redirectCell, host, path string) int {
	t.Helper()
	count := 0
	for _, cell := range cells {
		if cell.hostname != "" {
			hostRule, err := amphora.HostnameRuleValue(cell.hostname, false)
			if err != nil {
				t.Fatal(err)
			}
			if !regexp.MustCompile(hostRule).MatchString(strings.ToLower(host)) {
				continue
			}
		}
		eligible := true
		for _, guard := range cell.guards {
			if redirectGuardMatches(t, guard, host, path) {
				eligible = false
				break
			}
		}
		if eligible {
			count++
		}
	}
	return count
}

func TestRedirectExcludesOnlyHigherPriorityForwardMatches(t *testing.T) {
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 5}
	forwards := []cloud.HTTPPathMatch{
		{PathType: cloud.PathMatchExact, PathValue: "/shop/health", Precedence: 3},
		{Hostname: "*.tenant.example", PathType: cloud.PathMatchPrefix, PathValue: "/shop/live", Precedence: 2},
		{Hostname: "api.tenant.example", PathType: cloud.PathMatchExact, PathValue: "/shop/item", Precedence: 1},
		{Hostname: "static.example", PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 0},
		{Hostname: "other.example", PathType: cloud.PathMatchExact, PathValue: "/unrelated", Precedence: 4},
		{PathType: cloud.PathMatchExact, PathValue: "/shop/later", Precedence: 6},
	}
	cells, err := (&Provider{}).redirectCells(redirect, forwards)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		host, path string
		redirected int
	}{
		{"one.example", "/shop/item", 1}, {"one.example", "/shop/health", 0}, {"one.example", "/shop/health/", 1},
		{"API.TENANT.EXAMPLE:8443", "/shop/item", 0}, {"www.tenant.example", "/shop/item", 1},
		{"deep.www.tenant.example", "/shop/live/x", 0}, {"www.tenant.example", "/shop/lively", 1},
		{"tenant.example", "/shop/live", 1}, {"static.example", "/shop", 0}, {"other.example", "/shop/later", 1},
	} {
		if n := eligibleRedirectCells(t, cells, tt.host, tt.path); n != tt.redirected {
			t.Errorf("redirect cells for %s%s = %d, want %d", tt.host, tt.path, n, tt.redirected)
		}
	}
	allShadowed, err := (&Provider{}).redirectCells(cloud.HTTPPathMatch{Hostname: "api.tenant.example", PathType: cloud.PathMatchExact, PathValue: "/shop/live/x", Precedence: 5}, forwards)
	if err != nil || len(allShadowed) != 0 {
		t.Errorf("shadowed exact match produced %d cells, error %v", len(allShadowed), err)
	}
}

func TestRedirectExcludesLongForwardHostname(t *testing.T) {
	hostname := strings.Repeat("a.", 126) + "b"
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 1}
	forward := cloud.HTTPPathMatch{Hostname: hostname, PathType: cloud.PathMatchPrefix, PathValue: "/"}
	cells, err := (&Provider{longHTTPHostnames: true}).redirectCells(redirect, []cloud.HTTPPathMatch{forward})
	if err != nil || len(cells) != 1 || len(cells[0].guards) != 1 {
		t.Fatalf("expected one inverted hostname exclusion, got %+v: %v", cells, err)
	}
	if eligibleRedirectCells(t, cells, strings.ToUpper(hostname)+":8080", "/") != 0 ||
		eligibleRedirectCells(t, cells, "unrelated.example", "/") != 1 {
		t.Fatal("the higher-priority hostname was not excluded from the redirect")
	}
}

func TestManyForwardExceptionsFitStockRuleLimit(t *testing.T) {
	var forwards []cloud.HTTPPathMatch
	for i := 0; i < 70; i++ {
		forwards = append(forwards, cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: fmt.Sprintf("/item/%d", i), Precedence: i})
	}
	cells, err := (&Provider{}).redirectCells(cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 70}, forwards)
	if err != nil || len(cells) != 1 {
		t.Fatalf("got %d cells, error %v", len(cells), err)
	}
	if len(cells[0].guards)+1 > 50 {
		t.Fatalf("policy needs %d rules", len(cells[0].guards)+1)
	}
	for _, f := range forwards {
		if eligibleRedirectCells(t, cells, "example.test", f.PathValue) != 0 {
			t.Fatalf("redirect included %s", f.PathValue)
		}
	}
	for _, path := range []string{"/item/70", "/item/1/next"} {
		if eligibleRedirectCells(t, cells, "example.test", path) != 1 {
			t.Errorf("overbroad exception for %s", path)
		}
	}
}

func TestRedirectForwardExceptionsUseTheirPhysicalListener(t *testing.T) {
	scalar := cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.RouteSpec{
		GatewayRequirements: cloud.GatewayRequirements{ListenerPort: 80}, ScopedListenerPorts: []int{8080},
		PathType: cloud.PathMatchExact, PathValue: "/app", Precedence: 1,
	}}
	group := cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.RouteSpec{
		GatewayRequirements: cloud.GatewayRequirements{ListenerPort: 80},
		HTTPBackends:        []cloud.HTTPBackend{{Unavailable: true, Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchPrefix, PathValue: "/gone", Precedence: 2}}}},
	}}
	preserved, transport := scalar, scalar
	preserved.Disposition = cloud.GraphPreserve
	transport.Spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolTCP
	actual := redirectForwardingByPort(cloud.GatewayGraphSpec{Routes: []cloud.RouteGraphNode{scalar, group, preserved, transport}})
	if len(actual) != 2 || len(actual[8080]) != 1 || actual[8080][0].PathValue != "/app" || len(actual[80]) != 1 || actual[80][0].PathValue != "/gone" {
		t.Fatalf("unexpected forward exclusions by port: %v", actual)
	}
}

func TestRedirectLongRepeatedPathExceptions(t *testing.T) {
	var forwards []cloud.HTTPPathMatch
	for i := 0; i < 55; i++ {
		forwards = append(forwards, cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: fmt.Sprintf("/%03d%s", i, strings.Repeat("x", 246)), Precedence: i})
	}
	cells, err := (&Provider{}).redirectCells(cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 55}, forwards)
	if err != nil || len(cells) != 1 {
		t.Fatalf("compact long literals: %d cells, error %v", len(cells), err)
	}
	for _, f := range forwards {
		if eligibleRedirectCells(t, cells, "example.test", f.PathValue) != 0 {
			t.Fatalf("redirect included %s", f.PathValue)
		}
	}
	if eligibleRedirectCells(t, cells, "example.test", forwards[0].PathValue+"/") != 1 {
		t.Fatal("exact path exception matched its descendant")
	}
}

func TestRedirectReportsControllerEncodingLimit(t *testing.T) {
	var forwards []cloud.HTTPPathMatch
	for i := 0; i < 55; i++ {
		var path strings.Builder
		path.WriteByte('/')
		for j := 0; j < 3; j++ {
			_, _ = fmt.Fprintf(&path, "%x", sha256.Sum256([]byte(fmt.Sprintf("%d/%d", i, j))))
		}
		forwards = append(forwards, cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: path.String(), Precedence: i})
	}
	cells, err := (&Provider{}).redirectCells(cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 55}, forwards)
	if err == nil || len(cells) > 0 || !strings.Contains(err.Error(), "controller encoding") {
		t.Fatalf("got %d cells, error %v", len(cells), err)
	}
}

func TestRedirectRequiresProvablePrecedenceWithExactHTTPHeaders(t *testing.T) {
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 2}
	header := cloud.HTTPHeaderMatch{Name: "color", Value: "blue sky"}
	forward := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/shop/item",
		Precedence: 1, Headers: []cloud.HTTPHeaderMatch{header}}
	if cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward}); err == nil || len(cells) != 0 {
		t.Fatalf("overlapping forward header would let the redirect take precedence: %v, %v", cells, err)
	}
	redirect.Headers = []cloud.HTTPHeaderMatch{header}
	cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward})
	if err != nil || len(cells) != 1 || len(cells[0].guards) != 1 {
		t.Fatalf("redirect with the same header needs a path guard: %v, %v", cells, err)
	}
}

func TestRedirectMethodPrecedenceRespectsDisjointAndSharedMethods(t *testing.T) {
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 2}
	forward := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/shop/item", Precedence: 1, Method: "POST"}
	if cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward}); !errors.Is(err, errHTTPMethodRedirectPrecedence) || len(cells) != 0 {
		t.Fatalf("an unrestricted redirect must preserve the overlapping forward: %v, %v", cells, err)
	}
	redirect.Method = "GET"
	if cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward}); err != nil || len(cells) != 1 || len(cells[0].guards) != 0 {
		t.Fatalf("disjoint methods need no path guard: %v, %v", cells, err)
	}
	redirect.Method = "POST"
	if cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward}); err != nil || len(cells) != 1 || len(cells[0].guards) != 1 {
		t.Fatalf("a shared method needs a path guard: %v, %v", cells, err)
	}
}

func TestRedirectGuardsRespectExactQueryParameterOverlap(t *testing.T) {
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 1,
		QueryParams: []cloud.HTTPQueryParamMatch{{Name: "animal", Value: "whale"}}}
	for _, test := range []struct {
		param            cloud.HTTPQueryParamMatch
		policies, guards int
	}{
		{param: cloud.HTTPQueryParamMatch{Name: "animal", Value: "whale"}, policies: 1, guards: 1},
		{param: cloud.HTTPQueryParamMatch{Name: "animal", Value: "dolphin"}, policies: 1},
		{param: cloud.HTTPQueryParamMatch{Name: "ANIMAL", Value: "whale"}, policies: 2, guards: 1},
	} {
		forward := cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/shop/health",
			QueryParams: []cloud.HTTPQueryParamMatch{test.param}}
		cells, err := (&Provider{}).redirectCells(redirect, []cloud.HTTPPathMatch{forward})
		if err != nil || len(cells) != test.policies {
			t.Fatalf("query param %v: redirect cells = %v, error = %v", test.param, cells, err)
		}
		for _, cell := range cells {
			if len(cell.guards) != test.guards {
				t.Fatalf("query param %v: redirect cell = %v, want %d guards", test.param, cell, test.guards)
			}
		}
	}
}

func TestRedirectQueryExceptionsReturnAnExplicitErrorAtExpansionLimit(t *testing.T) {
	redirect := cloud.HTTPPathMatch{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 8}
	var forwards []cloud.HTTPPathMatch
	for i := range 8 {
		forward := redirect
		forward.Precedence = i
		forward.QueryParams = []cloud.HTTPQueryParamMatch{
			{Name: fmt.Sprintf("preview%d", i), Value: "on"}, {Name: fmt.Sprintf("color%d", i), Value: "blue"},
		}
		forwards = append(forwards, forward)
	}
	if cells, err := (&Provider{}).redirectCells(redirect, forwards); len(cells) != 0 || !errors.Is(err, errExactQueryParamRedirectPrecedence) {
		t.Fatalf("oversized complement returned %d policies, error %v", len(cells), err)
	}
}
