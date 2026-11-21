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

package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/audit"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/clients"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
)

func TestScanReturnsMatchedGraphUsingReadOnlyPaginatedQueries(t *testing.T) {
	identity := safetyTestIdentity(t)
	fixture := newAuditScanFixture(t, identity)
	fixture.paginate = true

	provider := fixture.provider()
	inventory, err := provider.Scan(context.Background(), auditTestScope(identity.Value()), auditTestRecords(identity.Value()))
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(inventory.Resources) != 8 {
		t.Fatalf("Scan() returned %d resources, want 8: %#v; requests: %#v", len(inventory.Resources), inventory.Resources, fixture.recordedRequests())
	}
	wantParents := map[string]string{
		"floating-ip-1": "load-balancer-1",
		"listener-1":    "load-balancer-1",
		"pool-1":        "load-balancer-1",
		"member-1":      "pool-1",
		"monitor-1":     "pool-1",
		"policy-1":      "listener-1",
		"rule-1":        "policy-1",
	}
	for _, finding := range inventory.Resources {
		if finding.Disposition != audit.DispositionMatched || finding.Reason != auditReasonExactBinding {
			t.Errorf("resource %s disposition = %q, reason = %q, want matched ExactBinding", finding.ID, finding.Disposition, finding.Reason)
		}
		if want := wantParents[finding.ID]; finding.ParentID != want {
			t.Errorf("resource %s parent = %q, want %q", finding.ID, finding.ParentID, want)
		}
		if len(finding.Objects) != 1 {
			t.Errorf("resource %s objects = %#v, want one object", finding.ID, finding.Objects)
		}
	}
	if len(inventory.Limitations) == 0 {
		t.Error("Scan() returned no documented limitations")
	}
	for _, limitation := range inventory.Limitations {
		if limitation == "" {
			t.Errorf("Scan() limitations contain an empty entry: %#v", inventory.Limitations)
		}
	}

	requests := fixture.recordedRequests()
	if len(requests) == 0 {
		t.Fatal("Scan() made no OpenStack requests")
	}
	for _, request := range requests {
		if request.method != http.MethodGet {
			t.Errorf("Scan() sent mutating request %s %s", request.method, request.path)
		}
		if got := request.query.Get("project_id"); got != identity.Value().OpenStackProjectID {
			t.Errorf("%s query project_id = %q, want %q", request.path, got, identity.Value().OpenStackProjectID)
		}
	}
	if fixture.mutatingRequests != 0 {
		t.Fatalf("Scan() sent %d mutating requests, want 0", fixture.mutatingRequests)
	}

	wantScopeTags := openstackidentity.ScopeTags(identity.Value().ClusterID, identity.Value().Controller)
	slices.Sort(wantScopeTags)
	for _, path := range []string{"/lbaas/loadbalancers", "/lbaas/listeners", "/lbaas/pools", "/lbaas/healthmonitors"} {
		request, ok := fixture.findRequest(path, func(query url.Values) bool {
			return query.Get("tags") != "" && query.Get("page") == ""
		})
		if !ok {
			t.Errorf("Scan() did not send a scope tag query to %s", path)
			continue
		}
		gotTags := append([]string(nil), request.query["tags"]...)
		slices.Sort(gotTags)
		if !slices.Equal(gotTags, wantScopeTags) {
			t.Errorf("%s scope tags = %#v, want %#v", path, gotTags, wantScopeTags)
		}
	}
	for path, key := range map[string]string{
		"/lbaas/listeners":      "loadbalancer_id",
		"/lbaas/pools":          "loadbalancer_id",
		"/lbaas/healthmonitors": "pool_id",
	} {
		if _, ok := fixture.findRequest(path, func(query url.Values) bool {
			return query.Get(key) != "" && query.Get("page") == ""
		}); !ok {
			t.Errorf("Scan() did not send a parent-scoped query to %s", path)
		}
	}

	wantPagedPaths := []string{
		"/floatingips",
		"/lbaas/healthmonitors",
		"/lbaas/l7policies",
		"/lbaas/l7policies/policy-1/rules",
		"/lbaas/listeners",
		"/lbaas/loadbalancers",
		"/lbaas/pools",
		"/lbaas/pools/pool-1/members",
	}
	for _, path := range wantPagedPaths {
		if _, ok := fixture.findRequest(path, func(query url.Values) bool { return query.Get("page") == "2" }); !ok {
			t.Errorf("Scan() did not follow the next page for %s", path)
		}
	}

	encoded, err := json.Marshal(inventory)
	if err != nil {
		t.Fatalf("json.Marshal(inventory) error = %v", err)
	}
	for _, sensitive := range []string{
		"raw-private-tag",
		"raw-private-description",
		"198.51.100.44",
		"203.0.113.44",
		identity.Value().OpenStackProjectID,
		strings.Join(safetyGatewayTags(t, identity, roleLoadBalancer), ","),
		identity.GatewayDescription(roleFloatingIP),
	} {
		if strings.Contains(string(encoded), sensitive) {
			t.Errorf("serialized inventory contains sensitive provider data %q: %s", sensitive, encoded)
		}
	}
}

func TestScanWeightedPolicyRequiresEveryPoolToBelongToItsRouteAndListener(t *testing.T) {
	for _, test := range []struct {
		name, poolCase string
		policy, rule   audit.Disposition
	}{
		{"owned pools including zero", "", audit.DispositionMatched, audit.DispositionMatched},
		{"pool replaced by fixed 500", "fixed", audit.DispositionMatched, audit.DispositionMatched},
		{"zero weight pool missing", "missing", audit.DispositionUnresolved, audit.DispositionUnresolved},
		{"zero weight pool from sibling route", "sibling", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"sibling pool alongside fixed 404", "sibling-fixed-404", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"sibling pool after malformed slot", "sibling-malformed", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"a rule without a project inherits the policy conflict", "sibling-rule-no-project", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"a stock action with a sibling target still conflicts", "sibling-stock-action", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"pool from other load balancer", "other-lb", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"pool without backend fragment", "legacy", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"pool has conflicting duplicate identity", "duplicate-pool", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"pool from wrong project with another target missing", "wrong-project", audit.DispositionOwnershipConflict, audit.DispositionOwnershipConflict},
		{"unsupported fixed 404 target", "fixed-404", audit.DispositionUnresolved, audit.DispositionUnresolved},
		{"malformed target", "malformed", audit.DispositionUnresolved, audit.DispositionUnresolved},
		{"204 after weighted page leaves inventory incomplete", "incomplete", audit.DispositionUnresolved, audit.DispositionUnresolved},
		{"unknown action", "unknown", audit.DispositionUnresolved, audit.DispositionUnresolved},
		{"target changed on another page", "changed", audit.DispositionUnresolved, audit.DispositionUnresolved},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := safetyTestIdentity(t)
			fixture := newAuditScanFixture(t, route)
			fixture.paginate = true
			fixture.members, fixture.monitors = nil, nil
			records := auditTestRecords(route.Value())
			policy := fixture.policies[0]
			ruleKey := openstackidentity.HTTPWeightedRuleKey(0)
			matchKey := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/weighted")
			policy["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPWeightedPolicyRole(rolePolicyExact, ruleKey, matchKey))
			policy["action"] = "WEIGHTED_TARGETS"
			policy["redirect_pool_id"] = nil
			policy["admin_state_up"] = true
			fixture.rules[0]["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPWeightedPolicyRole(roleRulePath, ruleKey, matchKey))
			a := openstackidentity.HTTPBackendKey("default", "first", 80)
			b := openstackidentity.HTTPBackendKey("default", "second", 80)
			fixture.pools[0]["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPFragmentRole(rolePool, a, ""))
			second := maps.Clone(fixture.pools[0])
			second["id"] = "pool-2"
			second["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPFragmentRole(rolePool, b, ""))
			fixture.pools = append(fixture.pools, second)
			targets := []map[string]any{{"weight": 3, "pool_id": "pool-1"}, {"weight": 0, "pool_id": "pool-2"}}
			switch test.poolCase {
			case "fixed", "fixed-404":
				code := http.StatusInternalServerError
				if test.poolCase == "fixed-404" {
					code = http.StatusNotFound
				}
				targets[1] = map[string]any{"weight": 1, "fixed_response": map[string]any{"status_code": code}}
				fixture.pools = fixture.pools[:1]
			case "missing":
				fixture.pools = fixture.pools[:1]
			case "sibling", "sibling-fixed-404", "sibling-malformed", "sibling-rule-no-project", "sibling-stock-action", "other-lb":
				siblingID := route.Value()
				siblingID.RouteName, siblingID.RouteUID = "sibling", "sibling-uid"
				if test.poolCase == "other-lb" {
					lb := maps.Clone(fixture.loadBalancers[0])
					lb["id"], lb["vip_port_id"] = "load-balancer-2", "vip-port-2"
					// Keep the pool's ownership tags the same so this case exercises the actual LB edge.
					fixture.loadBalancers = append(fixture.loadBalancers, lb)
					second["loadbalancers"] = []any{map[string]any{"id": "load-balancer-2"}}
				} else {
					sibling, err := openstackidentity.NewIdentity(siblingID)
					if err != nil {
						t.Fatal(err)
					}
					records = append(records, auditTestRecords(siblingID)[1])
					second["tags"] = safetyRouteTags(t, sibling, openstackidentity.HTTPFragmentRole(rolePool, b, ""))
				}
				switch test.poolCase {
				case "sibling-fixed-404":
					targets = append(targets, map[string]any{"weight": 1, "fixed_response": map[string]any{"status_code": 404}})
				case "sibling-malformed":
					delete(targets[0], "weight")
				case "sibling-rule-no-project":
					delete(fixture.rules[0], "project_id")
				case "sibling-stock-action":
					policy["action"], policy["redirect_pool_id"] = "REDIRECT_TO_POOL", "pool-1"
					policy["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPFragmentRole(rolePolicyExact, a, matchKey))
					fixture.rules[0]["tags"] = safetyRouteTags(t, route, openstackidentity.HTTPFragmentRole(roleRulePath, a, matchKey))
				}
			case "legacy":
				second["tags"] = safetyRouteTags(t, route, rolePool)
			case "duplicate-pool":
				duplicate := maps.Clone(second)
				duplicate["id"] = "duplicate"
				fixture.pools = append(fixture.pools, duplicate)
			case "wrong-project":
				targets[0]["pool_id"] = "missing"
				second["project_id"] = "other-project"
			case "malformed":
				delete(targets[1], "weight")
			case "incomplete":
				fixture.overridePolicyPage = func(w http.ResponseWriter, req *http.Request) bool {
					if req.URL.Query().Get("page") != "2" {
						return false
					}
					w.WriteHeader(http.StatusNoContent)
					return true
				}
			case "unknown":
				policy["action"] = "PRIVATE_UNKNOWN"
			case "changed":
				changed := maps.Clone(policy)
				changed["targets"] = []map[string]any{targets[0], {"weight": 1, "fixed_response": map[string]any{"status_code": 500}}}
				fixture.policies = append(fixture.policies, changed)
			}
			policy["targets"] = targets
			if test.poolCase != "changed" && test.poolCase != "incomplete" {
				// The managed weighted policy is on the second page.
				fixture.policies = append([]map[string]any{{"id": "unmanaged-policy"}}, fixture.policies...)
			}
			result, err := fixture.provider().Scan(t.Context(), auditTestScope(route.Value()), records)
			if err != nil {
				t.Fatal(err)
			}
			for id, want := range map[string]audit.Disposition{"policy-1": test.policy, "rule-1": test.rule} {
				got := auditFindingByID(t, result.Resources, id)
				if got.Disposition != want {
					t.Errorf("%s = %s/%s, want %s", id, got.Disposition, got.Reason, want)
				}
			}
			if test.poolCase == "other-lb" {
				got := auditFindingByID(t, result.Resources, "policy-1")
				if got.Reason != auditReasonParentMismatch {
					t.Errorf("policy reason = %s, want %s", got.Reason, auditReasonParentMismatch)
				}
			}
			if test.poolCase == "changed" {
				got := auditFindingByID(t, result.Resources, "policy-1")
				if got.Reason != auditReasonObservationChanged {
					t.Errorf("policy reason = %s, want %s", got.Reason, auditReasonObservationChanged)
				}
			}
			if fixture.mutatingRequests != 0 {
				t.Errorf("weighted audit sent %d mutating requests", fixture.mutatingRequests)
			}
			if _, ok := fixture.findRequest("/lbaas/l7policies", func(query url.Values) bool { return query.Get("page") == "2" }); !ok {
				t.Error("weighted audit did not follow the L7 policy page link")
			}
			policyRequests := 0
			for _, request := range fixture.recordedRequests() {
				if request.path == "/lbaas/l7policies" {
					policyRequests++
				}
			}
			if policyRequests != 2 {
				t.Errorf("L7 policy requests = %d, want one read per page", policyRequests)
			}
		})
	}
}

func TestScanDoesNotMatchRedirectPolicyWithPrivateTargets(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields map[string]any
		want   audit.Disposition
	}{
		{"stock empty targets", map[string]any{"targets": []any{}, "fixed_response": nil}, audit.DispositionMatched},
		{"private fixed slot", map[string]any{"targets": []any{map[string]any{"weight": 1, "fixed_response": map[string]any{"status_code": 500}}}}, audit.DispositionUnresolved},
		{"malformed targets", map[string]any{"targets": "invalid"}, audit.DispositionUnresolved},
		{"malformed fixed response", map[string]any{"fixed_response": "invalid"}, audit.DispositionUnresolved},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := safetyTestIdentity(t)
			fixture := newAuditScanFixture(t, identity)
			maps.Copy(fixture.policies[0], test.fields)
			inventory, err := fixture.provider().Scan(t.Context(), auditTestScope(identity.Value()), auditTestRecords(identity.Value()))
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"policy-1", "rule-1"} {
				finding := auditFindingByID(t, inventory.Resources, id)
				if finding.Disposition != test.want {
					t.Errorf("%s = %s/%s, want %s", id, finding.Disposition, finding.Reason, test.want)
				}
			}
		})
	}
}

func TestScanRequiresExplicitPolicyCollection(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    map[string]any
		wantErr bool
	}{
		{"empty array", map[string]any{"l7policies": []any{}}, false},
		{"no content", nil, false},
		{"missing collection", map[string]any{}, true},
		{"null collection", map[string]any{"l7policies": nil}, true},
		{"null row", map[string]any{"l7policies": []any{nil}}, true},
		{"row without ID", map[string]any{"l7policies": []any{map[string]any{"action": "WEIGHTED_TARGETS"}}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := safetyTestIdentity(t)
			fixture := newAuditScanFixture(t, identity)
			fixture.overridePolicyPage = func(w http.ResponseWriter, _ *http.Request) bool {
				if test.body == nil {
					w.WriteHeader(http.StatusNoContent)
				} else {
					safetyWriteJSON(t, w, test.body)
				}
				return true
			}
			inventory, err := fixture.provider().Scan(t.Context(), auditTestScope(identity.Value()), auditTestRecords(identity.Value()))
			if (err != nil) != test.wantErr {
				t.Fatalf("Scan() returned %d resources and error %v, want error = %t", len(inventory.Resources), err, test.wantErr)
			}
			for _, finding := range inventory.Resources {
				if finding.Type == auditTypeL7Policy || finding.Type == auditTypeL7Rule {
					t.Errorf("Scan() reported unexpected resource %s", finding.ID)
				}
			}
		})
	}
}

func TestScanClassifiesOwnershipFindings(t *testing.T) {
	current := safetyTestIdentity(t)
	currentGateway := auditGatewayIdentity(current.Value())

	tests := []struct {
		name             string
		configure        func(*testing.T, *auditScanFixture)
		records          []audit.OwnershipRecord
		wantDisposition  audit.Disposition
		wantReason       string
		wantFindingCount int
	}{
		{
			name: "orphan candidate",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.keepOnlyLoadBalancers()
			},
			wantDisposition:  audit.DispositionOrphanCandidate,
			wantReason:       auditReasonNoBinding,
			wantFindingCount: 1,
		},
		{
			name: "stale Gateway UID",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.keepOnlyLoadBalancers()
				stale := current.Value()
				stale.GatewayUID = "deleted-gateway-uid"
				fixture.loadBalancers[0]["tags"] = auditGatewayTagsFor(t, stale, roleLoadBalancer)
			},
			records: []audit.OwnershipRecord{{
				Identity: currentGateway,
				Objects:  []audit.ObjectReference{auditGatewayReference(currentGateway)},
			}},
			wantDisposition:  audit.DispositionStaleUID,
			wantReason:       auditReasonStaleUID,
			wantFindingCount: 1,
		},
		{
			name: "wrong load balancer provider",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.keepOnlyLoadBalancers()
				fixture.loadBalancers[0]["provider"] = "ovn"
			},
			records: []audit.OwnershipRecord{{
				Identity: currentGateway,
				Objects:  []audit.ObjectReference{auditGatewayReference(currentGateway)},
			}},
			wantDisposition:  audit.DispositionOwnershipConflict,
			wantReason:       auditReasonInvalidIdentity,
			wantFindingCount: 1,
		},
		{
			name: "resource reports another project",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.keepOnlyLoadBalancers()
				fixture.loadBalancers[0]["project_id"] = "foreign-project"
			},
			wantDisposition:  audit.DispositionOwnershipConflict,
			wantReason:       auditReasonProjectMismatch,
			wantFindingCount: 1,
		},
		{
			name: "descendant lost scope identity",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.pools = nil
				fixture.members = nil
				fixture.monitors = nil
				fixture.policies = nil
				fixture.rules = nil
				fixture.floatingIPs = nil
				fixture.listeners[0]["tags"] = []string{"owned-by=another-controller"}
			},
			records: []audit.OwnershipRecord{{
				Identity: currentGateway,
				Objects:  []audit.ObjectReference{auditGatewayReference(currentGateway)},
			}},
			wantDisposition:  audit.DispositionOwnershipConflict,
			wantReason:       auditReasonInvalidIdentity,
			wantFindingCount: 2,
		},
		{
			name: "duplicate logical identity",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.keepOnlyLoadBalancers()
				duplicate := auditCloneMap(fixture.loadBalancers[0])
				duplicate["id"] = "load-balancer-2"
				duplicate["vip_port_id"] = "vip-port-2"
				fixture.loadBalancers = append(fixture.loadBalancers, duplicate)
			},
			wantDisposition:  audit.DispositionOwnershipConflict,
			wantReason:       auditReasonDuplicateIdentity,
			wantFindingCount: 2,
		},
		{
			name: "detached Floating IP",
			configure: func(t *testing.T, fixture *auditScanFixture) {
				fixture.loadBalancers = nil
				fixture.listeners = nil
				fixture.pools = nil
				fixture.members = nil
				fixture.monitors = nil
				fixture.policies = nil
				fixture.rules = nil
				fixture.floatingIPs[0]["port_id"] = ""
			},
			records: []audit.OwnershipRecord{{
				Identity: currentGateway,
				Objects:  []audit.ObjectReference{auditGatewayReference(currentGateway)},
			}},
			wantDisposition:  audit.DispositionUnresolved,
			wantReason:       auditReasonDetachedFloatingIP,
			wantFindingCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAuditScanFixture(t, current)
			test.configure(t, fixture)
			inventory, err := fixture.provider().Scan(context.Background(), auditTestScope(current.Value()), test.records)
			if err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if len(inventory.Resources) != test.wantFindingCount {
				t.Fatalf("Scan() returned %d findings, want %d: %#v", len(inventory.Resources), test.wantFindingCount, inventory.Resources)
			}
			matched := 0
			for _, finding := range inventory.Resources {
				if finding.Disposition == test.wantDisposition && finding.Reason == test.wantReason {
					matched++
				}
			}
			wantMatches := 1
			if test.wantReason == auditReasonDuplicateIdentity {
				wantMatches = 2
			}
			if matched != wantMatches {
				t.Errorf("Scan() returned %d findings with %s/%s, want %d: %#v", matched, test.wantDisposition, test.wantReason, wantMatches, inventory.Resources)
			}
			if fixture.mutatingRequests != 0 {
				t.Errorf("Scan() sent %d mutating requests, want 0", fixture.mutatingRequests)
			}
		})
	}
}

func TestScanSortsFindingsDeterministically(t *testing.T) {
	base := safetyTestIdentity(t)
	identityA := auditIdentityForGateway(t, base.Value(), "gateway-a", "gateway-a-uid")
	identityB := auditIdentityForGateway(t, base.Value(), "gateway-b", "gateway-b-uid")
	fixture := newAuditScanFixture(t, base)
	fixture.keepOnlyLoadBalancers()
	fixture.loadBalancers = []map[string]any{
		auditLoadBalancer(t, "load-balancer-b", "vip-port-b", identityB),
		auditLoadBalancer(t, "load-balancer-a", "vip-port-a", identityA),
	}

	first, err := fixture.provider().Scan(context.Background(), auditTestScope(base.Value()), nil)
	if err != nil {
		t.Fatalf("first Scan() error = %v", err)
	}
	if got := auditFindingIDs(first.Resources); !slices.Equal(got, []string{"load-balancer-a", "load-balancer-b"}) {
		t.Fatalf("first Scan() order = %#v, want sorted IDs", got)
	}

	slices.Reverse(fixture.loadBalancers)
	second, err := fixture.provider().Scan(context.Background(), auditTestScope(base.Value()), nil)
	if err != nil {
		t.Fatalf("second Scan() error = %v", err)
	}
	if !reflect.DeepEqual(second, first) {
		t.Errorf("Scan() result changed with server response order\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func TestScanOmitsUnattributedDetachedFloatingIP(t *testing.T) {
	current := safetyTestIdentity(t)
	foreignValue := current.Value()
	foreignValue.ClusterID = "another-cluster"
	foreignValue.Controller = "other.example/gateway-controller"
	foreignValue.GatewayName = "other-gateway"
	foreignValue.GatewayUID = "other-gateway-uid"
	foreignValue.RouteNamespace = ""
	foreignValue.RouteName = ""
	foreignValue.RouteUID = ""
	foreign, err := openstackidentity.NewIdentity(foreignValue)
	if err != nil {
		t.Fatalf("openstackidentity.NewIdentity() error = %v", err)
	}
	fixture := newAuditScanFixture(t, current)
	fixture.loadBalancers = nil
	fixture.listeners = nil
	fixture.pools = nil
	fixture.members = nil
	fixture.monitors = nil
	fixture.policies = nil
	fixture.rules = nil
	fixture.floatingIPs = []map[string]any{{
		"id": "floating-ip-foreign", "project_id": current.Value().OpenStackProjectID,
		"port_id": "", "description": foreign.GatewayDescription(roleFloatingIP),
	}}

	inventory, err := fixture.provider().Scan(
		context.Background(),
		auditTestScope(current.Value()),
		[]audit.OwnershipRecord{{
			Identity: auditGatewayIdentity(current.Value()),
			Objects:  []audit.ObjectReference{auditGatewayReference(current.Value())},
		}},
	)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(inventory.Resources) != 0 {
		t.Fatalf("Scan() resources = %#v, want the unattributed Floating IP omitted", inventory.Resources)
	}
}

func TestScanRejectsProjectMismatchBeforeRequest(t *testing.T) {
	requests := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests++
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	})
	provider := NewScanner(clients.ServiceClients{
		LoadBalancer: safetyServiceClient(handler),
		Network:      safetyServiceClient(handler),
		ProjectID:    "project-a",
	}, 0)

	_, err := provider.Scan(context.Background(), audit.Scope{
		ClusterID:          "test-cluster",
		ControllerName:     "example.com/openstack-gateway-controller",
		OpenStackProjectID: "project-b",
	}, nil)
	if !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("Scan() error = %v, want ownership conflict", err)
	}
	for _, projectID := range []string{"project-a", "project-b"} {
		if strings.Contains(err.Error(), projectID) {
			t.Errorf("Scan() error exposes OpenStack project ID %q: %v", projectID, err)
		}
	}
	if requests != 0 {
		t.Errorf("Scan() made %d requests before rejecting the project mismatch", requests)
	}
}

type auditRecordedRequest struct {
	method string
	path   string
	query  url.Values
}

func TestScanDistinguishesMissingRedirectPoolRelationship(t *testing.T) {
	identity := safetyTestIdentity(t)
	match := openstackidentity.HTTPMatchKey("example.test", cloud.PathMatchPrefix, "/")
	cell := openstackidentity.HTTPRedirectKey(match, "example.test", "prefix")
	role := openstackidentity.HTTPRedirectRole(rolePolicyPrefix, match, cell, "")
	for _, scenario := range []struct {
		name      string
		reported  bool
		noContent bool
		want      audit.Disposition
	}{
		{name: "omitted pool relationship", want: audit.DispositionUnresolved},
		{name: "explicit null", reported: true, want: audit.DispositionMatched},
		{name: "empty 204 listing", noContent: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newAuditScanFixture(t, identity)
			fixture.noPolicyContent = scenario.noContent
			fixture.paginate = true
			fixture.policies[0]["action"] = "REDIRECT_PREFIX"
			fixture.policies[0]["tags"] = safetyRouteTags(t, identity, role)
			delete(fixture.policies[0], "redirect_pool_id")
			if scenario.reported {
				fixture.policies[0]["redirect_pool_id"] = nil
			}
			fixture.rules = nil
			inventory, err := fixture.provider().Scan(t.Context(), auditTestScope(identity.Value()), auditTestRecords(identity.Value()))
			if err != nil {
				t.Fatal(err)
			}
			if scenario.noContent {
				if slices.ContainsFunc(inventory.Resources, func(finding audit.ResourceFinding) bool { return finding.ID == "policy-1" }) {
					t.Fatal("empty 204 listing reported a policy")
				}
				return
			}
			got := auditFindingByID(t, inventory.Resources, "policy-1")
			if got.Disposition != scenario.want || !scenario.reported && got.Reason != auditReasonMissingBackendPool {
				t.Fatalf("policy disposition = %s (%s), want %s", got.Disposition, got.Reason, scenario.want)
			}
		})
	}
}

func TestScanMatchesPoolFreeFixedPolicyWithExplicitTarget(t *testing.T) {
	identity := safetyTestIdentity(t)
	match := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/omitted")
	fixed := openstackidentity.HTTPFixedRuleKey(0)
	for _, statusCode := range []int{500, 302} {
		fixture := newAuditScanFixture(t, identity)
		fixture.policies[0]["action"] = "FIXED_RESPONSE"
		fixture.policies[0]["admin_state_up"] = true
		fixture.policies[0]["redirect_pool_id"] = nil
		fixture.policies[0]["fixed_response"] = map[string]any{"status_code": statusCode}
		fixture.policies[0]["tags"] = safetyRouteTags(t, identity, openstackidentity.HTTPFragmentRole(rolePolicyExact, fixed, match))
		fixture.rules[0]["tags"] = safetyRouteTags(t, identity, openstackidentity.HTTPFragmentRole(roleRulePath, fixed, match))
		inventory, err := fixture.provider().Scan(t.Context(), auditTestScope(identity.Value()), auditTestRecords(identity.Value()))
		if err != nil {
			t.Fatal(err)
		}
		want := audit.DispositionMatched
		if statusCode == 302 {
			want = audit.DispositionUnresolved
		}
		for _, id := range []string{"policy-1", "rule-1"} {
			got := auditFindingByID(t, inventory.Resources, id)
			if got.Disposition != want {
				t.Fatalf("%s with fixed code %d: got %s (%s), want %s", id, statusCode, got.Disposition, got.Reason, want)
			}
		}
	}
}

type auditScanFixture struct {
	t                  *testing.T
	loadBalancers      []map[string]any
	listeners          []map[string]any
	pools              []map[string]any
	members            []map[string]any
	monitors           []map[string]any
	policies           []map[string]any
	rules              []map[string]any
	floatingIPs        []map[string]any
	paginate           bool
	noPolicyContent    bool
	overridePolicyPage func(http.ResponseWriter, *http.Request) bool
	mutex              sync.Mutex
	requests           []auditRecordedRequest
	mutatingRequests   int
}

func newAuditScanFixture(t *testing.T, identity openstackidentity.Identity) *auditScanFixture {
	t.Helper()
	loadBalancerTags := append(safetyGatewayTags(t, identity, roleLoadBalancer), "raw-private-tag=customer-a")
	listenerTags := safetyGatewayTags(t, identity, roleListener)
	poolTags := safetyRouteTags(t, identity, rolePool)
	memberTags := safetyRouteTags(t, identity, roleMember)
	monitorTags := safetyRouteTags(t, identity, roleMonitor)
	policyTags := safetyRouteTags(t, identity, rolePolicyExact)
	ruleTags := safetyRouteTags(t, identity, roleRulePath)

	return &auditScanFixture{
		t: t,
		loadBalancers: []map[string]any{{
			"id": "load-balancer-1", "project_id": identity.Value().OpenStackProjectID,
			"provider": "amphora", "provisioning_status": "ACTIVE",
			"vip_port_id": "vip-port-1", "vip_address": "198.51.100.44", "tags": loadBalancerTags,
		}},
		listeners: []map[string]any{{
			"id": "listener-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "tags": listenerTags,
			"loadbalancers": []any{map[string]any{"id": "load-balancer-1"}},
		}},
		pools: []map[string]any{{
			"id": "pool-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "tags": poolTags,
			"loadbalancers": []any{map[string]any{"id": "load-balancer-1"}},
		}},
		members: []map[string]any{{
			"id": "member-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "pool_id": "pool-1", "tags": memberTags,
			"address": "192.0.2.10",
		}},
		monitors: []map[string]any{{
			"id": "monitor-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "tags": monitorTags,
			"pools": []any{map[string]any{"id": "pool-1"}},
		}},
		policies: []map[string]any{{
			"id": "policy-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "listener_id": "listener-1",
			"action": "REDIRECT_TO_POOL", "redirect_pool_id": "pool-1", "tags": policyTags,
		}},
		rules: []map[string]any{{
			"id": "rule-1", "project_id": identity.Value().OpenStackProjectID,
			"provisioning_status": "ACTIVE", "policy_id": "policy-1", "tags": ruleTags,
		}},
		floatingIPs: []map[string]any{{
			"id": "floating-ip-1", "project_id": identity.Value().OpenStackProjectID,
			"port_id": "vip-port-1", "floating_ip_address": "203.0.113.44",
			"description": identity.GatewayDescription(roleFloatingIP) + "; operator-note=raw-private-description",
		}},
	}
}

func (f *auditScanFixture) provider() *Scanner {
	handler := http.HandlerFunc(f.serveHTTP)
	return NewScanner(clients.ServiceClients{
		LoadBalancer: safetyServiceClient(handler),
		Network:      safetyServiceClient(handler),
		ProjectID:    "project-a",
	}, 0)
}

func (f *auditScanFixture) serveHTTP(w http.ResponseWriter, request *http.Request) {
	f.record(request)
	if request.Method != http.MethodGet {
		f.mutex.Lock()
		f.mutatingRequests++
		f.mutex.Unlock()
		http.Error(w, "audit must be read only", http.StatusMethodNotAllowed)
		return
	}

	var key string
	var items []map[string]any
	switch {
	case request.URL.Path == "/lbaas/loadbalancers":
		key, items = "loadbalancers", auditFilterTags(f.loadBalancers, request.URL.Query()["tags"])
	case request.URL.Path == "/lbaas/listeners":
		key = "listeners"
		items = auditFilterByParent(f.listeners, "loadbalancers", request.URL.Query().Get("loadbalancer_id"))
		items = auditFilterTags(items, request.URL.Query()["tags"])
	case request.URL.Path == "/lbaas/pools":
		key = "pools"
		items = auditFilterByParent(f.pools, "loadbalancers", request.URL.Query().Get("loadbalancer_id"))
		items = auditFilterTags(items, request.URL.Query()["tags"])
	case request.URL.Path == "/lbaas/l7policies":
		if f.noPolicyContent {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if f.overridePolicyPage != nil && f.overridePolicyPage(w, request) {
			return
		}
		key, items = "l7policies", f.policies
	case strings.HasPrefix(request.URL.Path, "/lbaas/pools/") && strings.HasSuffix(request.URL.Path, "/members"):
		key = "members"
		poolID := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/lbaas/pools/"), "/members")
		items = auditFilterScalar(f.members, "pool_id", poolID)
	case request.URL.Path == "/lbaas/healthmonitors":
		key = "healthmonitors"
		items = auditFilterByParent(f.monitors, "pools", request.URL.Query().Get("pool_id"))
		items = auditFilterTags(items, request.URL.Query()["tags"])
	case strings.HasPrefix(request.URL.Path, "/lbaas/l7policies/") && strings.HasSuffix(request.URL.Path, "/rules"):
		key = "rules"
		policyID := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/lbaas/l7policies/"), "/rules")
		items = auditFilterScalar(f.rules, "policy_id", policyID)
	case request.URL.Path == "/floatingips":
		key, items = "floatingips", f.floatingIPs
	default:
		f.t.Errorf("unexpected audit request: %s %s", request.Method, request.URL.RequestURI())
		http.Error(w, "unexpected request", http.StatusNotFound)
		return
	}
	f.writeCollection(w, request, key, items)
}

func (f *auditScanFixture) writeCollection(w http.ResponseWriter, request *http.Request, key string, items []map[string]any) {
	nextItems := []map[string]any{}
	if items == nil {
		items = []map[string]any{}
	}
	if f.paginate && key == "l7policies" && len(items) > 1 {
		items, nextItems = items[:1], items[1:]
	}
	if f.paginate && request.URL.Query().Get("page") == "" {
		query := request.URL.Query()
		query.Set("page", "2")
		next := "http://openstack.test" + request.URL.Path + "?" + query.Encode()
		safetyWriteJSON(f.t, w, map[string]any{
			key:            items,
			key + "_links": []any{map[string]any{"rel": "next", "href": next}},
		})
		return
	}
	if f.paginate {
		items = nextItems
	}
	safetyWriteJSON(f.t, w, map[string]any{key: items})
}

func (f *auditScanFixture) record(request *http.Request) {
	query := make(url.Values, len(request.URL.Query()))
	for key, values := range request.URL.Query() {
		query[key] = append([]string(nil), values...)
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.requests = append(f.requests, auditRecordedRequest{method: request.Method, path: request.URL.Path, query: query})
}

func (f *auditScanFixture) recordedRequests() []auditRecordedRequest {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return append([]auditRecordedRequest(nil), f.requests...)
}

func (f *auditScanFixture) findRequest(path string, match func(url.Values) bool) (auditRecordedRequest, bool) {
	for _, request := range f.recordedRequests() {
		if request.path == path && match(request.query) {
			return request, true
		}
	}
	return auditRecordedRequest{}, false
}

func (f *auditScanFixture) keepOnlyLoadBalancers() {
	f.listeners = nil
	f.pools = nil
	f.members = nil
	f.monitors = nil
	f.policies = nil
	f.rules = nil
	f.floatingIPs = nil
}

func auditTestScope(identity cloud.Identity) audit.Scope {
	return audit.Scope{
		ClusterID:          identity.ClusterID,
		ControllerName:     identity.Controller,
		OpenStackProjectID: identity.OpenStackProjectID,
	}
}

func auditTestRecords(identity cloud.Identity) []audit.OwnershipRecord {
	gatewayIdentity := auditGatewayIdentity(identity)
	return []audit.OwnershipRecord{
		{Identity: gatewayIdentity, Objects: []audit.ObjectReference{auditGatewayReference(gatewayIdentity)}},
		{Identity: identity, Objects: []audit.ObjectReference{auditRouteReference(identity)}},
	}
}

func auditGatewayIdentity(identity cloud.Identity) cloud.Identity {
	identity.RouteNamespace = ""
	identity.RouteName = ""
	identity.RouteUID = ""
	return identity
}

func auditGatewayReference(identity cloud.Identity) audit.ObjectReference {
	return audit.ObjectReference{
		APIVersion: "gateway.networking.k8s.io/v1", Kind: "Gateway",
		Namespace: identity.GatewayNamespace, Name: identity.GatewayName, UID: identity.GatewayUID,
	}
}

func auditRouteReference(identity cloud.Identity) audit.ObjectReference {
	return audit.ObjectReference{
		APIVersion: "gateway.networking.k8s.io/v1", Kind: "HTTPRoute",
		Namespace: identity.RouteNamespace, Name: identity.RouteName, UID: identity.RouteUID,
	}
}

func auditIdentityForGateway(t *testing.T, base cloud.Identity, name, uid string) openstackidentity.Identity {
	t.Helper()
	base.GatewayName = name
	base.GatewayUID = uid
	base.RouteNamespace = ""
	base.RouteName = ""
	base.RouteUID = ""
	identity, err := openstackidentity.NewIdentity(base)
	if err != nil {
		t.Fatalf("openstackidentity.NewIdentity() error = %v", err)
	}
	return identity
}

func auditLoadBalancer(t *testing.T, id, portID string, identity openstackidentity.Identity) map[string]any {
	t.Helper()
	return map[string]any{
		"id": id, "project_id": identity.Value().OpenStackProjectID,
		"provider": "amphora", "provisioning_status": "ACTIVE",
		"vip_port_id": portID, "tags": safetyGatewayTags(t, identity, roleLoadBalancer),
	}
}

func auditGatewayTagsFor(t *testing.T, identity cloud.Identity, role string) []string {
	t.Helper()
	providerIdentity, err := openstackidentity.NewIdentity(identity)
	if err != nil {
		t.Fatalf("openstackidentity.NewIdentity() error = %v", err)
	}
	return safetyGatewayTags(t, providerIdentity, role)
}

func auditFindingIDs(findings []audit.ResourceFinding) []string {
	ids := make([]string, 0, len(findings))
	for _, finding := range findings {
		ids = append(ids, finding.ID)
	}
	return ids
}

func auditFilterTags(items []map[string]any, query []string) []map[string]any {
	if len(query) == 0 {
		return items
	}
	wanted := make([]string, 0, len(query))
	for _, value := range query {
		wanted = append(wanted, strings.Split(value, ",")...)
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		tags, _ := item["tags"].([]string)
		if auditContainsStrings(tags, wanted) {
			result = append(result, item)
		}
	}
	return result
}

func auditContainsStrings(values, wanted []string) bool {
	for _, wantedValue := range wanted {
		if !slices.Contains(values, wantedValue) {
			return false
		}
	}
	return true
}

func auditFilterByParent(items []map[string]any, field, parentID string) []map[string]any {
	if parentID == "" {
		return items
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		parents, _ := item[field].([]any)
		for _, value := range parents {
			parent, _ := value.(map[string]any)
			if parent["id"] == parentID {
				result = append(result, item)
				break
			}
		}
	}
	return result
}

func auditFilterScalar(items []map[string]any, field, value string) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if fmt.Sprint(item[field]) == value {
			result = append(result, item)
		}
	}
	return result
}

func auditCloneMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
