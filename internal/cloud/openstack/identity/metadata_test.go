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

package identity

import (
	"reflect"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestIdentityMatchesProjectButNotControllerVersion(t *testing.T) {
	value := cloud.Identity{
		OpenStackProjectID: "project-a",
		ClusterID:          "cluster-a",
		Controller:         "example.com/openstack",
		ControllerVersion:  "v0.1.0",
		GatewayNamespace:   "default",
		GatewayName:        "edge",
		GatewayUID:         "gateway-uid",
	}
	identity, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := identity.GatewayTags(RoleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	if !identity.MatchesGateway(tags, RoleLoadBalancer) {
		t.Fatal("identity did not match its complete resource tags")
	}

	upgradedValue := value
	upgradedValue.ControllerVersion = "v0.1.1"
	upgraded, err := NewIdentity(upgradedValue)
	if err != nil {
		t.Fatal(err)
	}
	if !upgraded.MatchesGateway(tags, RoleLoadBalancer) {
		t.Fatal("controller upgrade changed immutable resource ownership")
	}
	otherProjectValue := value
	otherProjectValue.OpenStackProjectID = "project-b"
	otherProject, err := NewIdentity(otherProjectValue)
	if err != nil {
		t.Fatal(err)
	}
	if otherProject.MatchesGateway(tags, RoleLoadBalancer) {
		t.Fatal("identity accepted resource tags from another OpenStack project")
	}
	discovery, err := identity.GatewayDiscoveryTags(RoleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	otherProjectDiscovery, err := otherProject.GatewayDiscoveryTags(RoleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(discovery, otherProjectDiscovery) {
		t.Fatalf("project identity leaked into discovery tags: %#v != %#v", discovery, otherProjectDiscovery)
	}
}

func TestIdentityHashesLongTagValuesAndRejectsConflicts(t *testing.T) {
	value := cloud.Identity{
		OpenStackProjectID: strings.Repeat("p", 300),
		ClusterID:          strings.Repeat("c", 300),
		Controller:         "example.com/" + strings.Repeat("controller", 30),
		ControllerVersion:  strings.Repeat("version", 100),
		GatewayNamespace:   "default",
		GatewayName:        strings.Repeat("gateway", 40),
		GatewayUID:         "gateway-uid",
	}
	identity, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := identity.GatewayTags(RoleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range tags {
		if len(tag) > maxTagLength {
			t.Fatalf("identity tag length = %d, want at most %d: %q", len(tag), maxTagLength, tag)
		}
	}
	if !identity.MatchesGateway(tags, RoleLoadBalancer) {
		t.Fatal("identity did not match tags containing hashed values")
	}
	conflicting := append([]string(nil), tags...)
	conflicting = append(conflicting, tag("uid", "another-gateway-uid"))
	if identity.MatchesGateway(conflicting, RoleLoadBalancer) {
		t.Fatal("identity accepted conflicting values for a reserved identity tag")
	}
	if got := len(identity.Description(RoleFloatingIP)); got > maxTagLength {
		t.Fatalf("description length = %d, want at most %d", got, maxTagLength)
	}
}

func TestGatewayDescriptionIgnoresRouteFields(t *testing.T) {
	value := cloud.Identity{
		OpenStackProjectID: "project-a",
		ClusterID:          "cluster-a",
		Controller:         "example.com/openstack",
		ControllerVersion:  "v0.1.0",
		GatewayNamespace:   "default",
		GatewayName:        "edge",
		GatewayUID:         "gateway-uid",
	}
	gatewayIdentity, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	value.RouteNamespace = "default"
	value.RouteName = "api"
	value.RouteUID = "route-uid"
	routeIdentity, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	description := gatewayIdentity.GatewayDescription(RoleFloatingIP)
	if !routeIdentity.MatchesGatewayDescription(description, RoleFloatingIP) {
		t.Fatal("route-scoped identity did not recognize its Gateway-scoped Floating IP description")
	}
}

func TestHTTPRouteOwnershipProofAcrossPoolsAndRules(t *testing.T) {
	gatewayID := cloud.Identity{OpenStackProjectID: "project", ClusterID: "cluster", Controller: "gateway.example.test",
		GatewayNamespace: "infra", GatewayName: "private", GatewayUID: "gateway-uid"}
	gateway, err := NewIdentity(gatewayID)
	if err != nil {
		t.Fatal(err)
	}
	value := gatewayID
	value.RouteNamespace, value.RouteName, value.RouteUID = "apps", strings.Repeat("long-name.", 24)+"app", "route-uid"
	route, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	backend := HTTPBackendKey("apps", "users", 8080)
	match := HTTPMatchKey("service.test", cloud.PathMatchExact, "/users")
	poolRole, pathRole := HTTPFragmentRole(RolePool, backend, ""), HTTPFragmentRole(RoleRulePath, backend, match)
	proof := func(role string) RouteOwnerProof {
		t.Helper()
		tags, tagErr := route.RouteTags(role)
		if tagErr != nil {
			t.Fatal(tagErr)
		}
		owner, ok := gateway.RouteOwner(tags, role)
		if !ok {
			t.Fatalf("missing proof for %q", role)
		}
		if _, ok := gateway.RouteOwner(append(tags, tag("route-uid", "other")), role); ok {
			t.Fatal("duplicate owner tag produced a route proof")
		}
		return owner
	}
	if proof(poolRole) != proof(pathRole) {
		t.Fatal("pool and path identify different owners")
	}
	if base, b, m, ok := ParseHTTPFragmentRole(pathRole); !ok || base != RoleRulePath || b != backend || m != match {
		t.Fatalf("cannot recover the original path role: %q", pathRole)
	}
	rootWithRouteTags, err := route.RouteTags(RoleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gateway.RouteOwner(rootWithRouteTags, RoleLoadBalancer); ok {
		t.Fatal("Gateway resource role produced a route proof")
	}
}

func TestHTTPPolicyDescriptionFitsOctaviaAndPreservesMatchIdentity(t *testing.T) {
	value := cloud.Identity{OpenStackProjectID: "project", ClusterID: "cluster", Controller: "gateway.example.test",
		GatewayNamespace: "infra", GatewayName: "edge", GatewayUID: "gateway-uid",
		RouteNamespace: "apps", RouteName: "catalog", RouteUID: "route-uid", ControllerVersion: strings.Repeat("release", 40)}
	id, err := NewIdentity(value)
	if err != nil {
		t.Fatal(err)
	}
	backend := HTTPBackendKey("apps", "catalog", 80)
	roleFor := func(path string) string {
		return HTTPFragmentRole(RolePolicyExact, backend, HTTPMatchKey("", cloud.PathMatchExact, path))
	}
	role := roleFor("/one")
	description := id.Description(role)
	if len(description) > 255 {
		t.Fatalf("Octavia L7Policy description is %d bytes, limit 255", len(description))
	}
	if !id.MatchesDescription(description, role) || id.MatchesDescription(description, roleFor("/two")) {
		t.Fatal("policy description must identify its original owner and match")
	}
}

func TestHTTPRedirectDescriptionsFitOctaviaAndPreserveMatchIdentity(t *testing.T) {
	id, err := NewIdentity(cloud.Identity{
		OpenStackProjectID: "project", ClusterID: "cluster", Controller: "gateway.example.test",
		GatewayNamespace: "infra", GatewayName: "edge", GatewayUID: "gateway-uid",
		RouteNamespace: "apps", RouteName: "redirects", RouteUID: "route-uid",
		ControllerVersion: "0665660e81b64da8a5d456d573e1932d23a8dffc",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		base  string
		guard string
	}{
		{base: RolePolicyExact},
		{base: RolePolicyPrefix},
		{base: RoleRuleRedirectGuard, guard: HTTPRedirectKey("PATH", "EQUAL_TO", "/forward")},
	} {
		t.Run(test.base, func(t *testing.T) {
			roleFor := func(path string) string {
				match := HTTPMatchKey("", cloud.PathMatchPrefix, path)
				cell := HTTPRedirectKey(match, "", test.base)
				return HTTPRedirectRole(test.base, match, cell, test.guard)
			}
			role := roleFor("/hostname-redirect")
			other := roleFor("/host-and-status")
			description := id.Description(role)
			if len(description) > 255 {
				t.Errorf("Octavia description is %d bytes, limit 255", len(description))
			}
			_, displayRole, _ := strings.Cut(description, "; role=")
			wantDisplay := encode(test.base) + "; controller-version=" + encode(id.Value().ControllerVersion)
			if displayRole != wantDisplay {
				t.Errorf("description display role and version = %q, want %q", displayRole, wantDisplay)
			}
			if !id.MatchesDescription(description, role) || id.MatchesDescription(description, other) {
				t.Error("description must distinguish redirects with the same display role")
			}
			tags, err := id.RouteTags(role)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := ResourceRole(tags); !ok || got != role {
				t.Errorf("role tag = %q, want full redirect role %q", got, role)
			}
		})
	}
}
