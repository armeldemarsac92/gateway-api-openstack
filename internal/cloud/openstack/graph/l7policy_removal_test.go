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
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

const (
	retiringPolicyPath          = "/lbaas/l7policies/policy-1"
	conditionalRemovalPath      = retiringPolicyPath + "/conditional_removal"
	conditionalRemovalDiscovery = "/lbaas/providers/amphora/gateway_capabilities"
)

func TestReconcileGatewayGraphConditionallyRemovesDisabledL7Policy(t *testing.T) {
	f := newPolicyRemovalFixture(t)
	f.policy["admin_state_up"] = true
	ready, err := f.reconcile()
	if err != nil || ready.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		!slices.Equal(f.requests.mutationPaths(), []string{retiringPolicyPath}) ||
		f.requests.count(http.MethodGet, conditionalRemovalDiscovery) != 0 {
		t.Fatalf("policy must first be disabled by the ordinary plan: %v, %v, writes %v", ready, err, f.requests.mutationPaths())
	}

	f.spec.ValidateMutation = func(_ context.Context) error {
		if f.requests.count(http.MethodGet, conditionalRemovalPath) != 1 ||
			f.requests.count(http.MethodPost, conditionalRemovalPath) != 0 {
			t.Fatal("Kubernetes validation did not run between the removal snapshot and write")
		}
		return nil
	}
	submitted, err := f.reconcile()
	if err != nil || submitted.Outcome.State != cloud.OutcomeProgressing || !submitted.Outcome.MutationSubmitted ||
		submitted.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || !submitted.Routes[0].Result.Outcome.MutationSubmitted ||
		submitted.Routes[1].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("accepted removal result = %+v, %v", submitted, err)
	}
	f.spec.ValidateMutation = nil
	complete, err := f.reconcile()
	if err != nil || complete.Routes[0].Result.Outcome.State != cloud.OutcomeReady || complete.Routes[1].Result.Outcome.State != cloud.OutcomeReady ||
		f.requests.count(http.MethodGet, "/lbaas/l7policies") != 3 || f.neighbor["admin_state_up"] != true {
		t.Fatalf("cleanup did not confirm absence alongside a serving neighbor: %+v, %v", complete, err)
	}
	if got := f.requests.mutationPaths(); !slices.Equal(got, []string{retiringPolicyPath, conditionalRemovalPath}) {
		t.Fatalf("route retirement wrote %v", got)
	}
}

func TestReconcileGatewayGraphReobservesConflictingL7Removal(t *testing.T) {
	f := newPolicyRemovalFixture(t)
	f.postStatus = http.StatusConflict
	result, err := f.reconcile()
	if err != nil || result.Outcome.MutationSubmitted || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("conflict = %+v, %v", result, err)
	}
	foreign := maps.Clone(f.rules[0])
	foreign["id"], foreign["tags"] = "foreign-rule", f.neighbor["tags"]
	f.rules = append(f.rules, foreign)
	f.policy["rules"] = append(f.policy["rules"].([]map[string]string), map[string]string{"id": "foreign-rule"})
	_, err = f.reconcile()
	if !errors.Is(err, cloud.ErrOwnershipConflict) || !slices.Equal(f.requests.mutationPaths(), []string{conditionalRemovalPath}) {
		t.Fatalf("retry after concurrent change = %v, writes %v", err, f.requests.mutationPaths())
	}
}

func TestRetiringRouteWaitsForPolicyStateAndInventory(t *testing.T) {
	t.Run("admin state unknown", func(t *testing.T) {
		f := newPolicyRemovalFixture(t)
		delete(f.policy, "admin_state_up")
		result, err := f.reconcile()
		category, _ := cloud.ErrorCategoryOf(err)
		if category != cloud.ErrorCategoryDependencyNotReady || result.Outcome.State == cloud.OutcomeReady || len(f.requests.mutationPaths()) != 0 {
			t.Fatalf("missing admin state: outcome %v, error %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
		}
	})

	t.Run("stock no-content readback", func(t *testing.T) {
		f := newPolicyRemovalFixture(t)
		submitted, err := f.reconcile()
		if err != nil || !submitted.Routes[0].Result.Outcome.MutationSubmitted {
			t.Fatalf("submit removal: %+v, %v", submitted.Outcome, err)
		}
		f.noPolicyContent = true
		delete(f.listener, "l7policies")
		result, err := f.reconcile()
		category, _ := cloud.ErrorCategoryOf(err)
		if category != cloud.ErrorCategoryDependencyNotReady || result.Outcome.State == cloud.OutcomeReady {
			t.Fatalf("no policy inventory: outcome %v, error %v", result.Outcome, err)
		}
		f.listener["l7policies"] = []map[string]string{}
		result, err = f.reconcile()
		if err != nil || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || !slices.Equal(f.requests.mutationPaths(), []string{conditionalRemovalPath}) {
			t.Fatalf("explicitly empty inventory: outcome %v, error %v, writes %v", result.Outcome, err, f.requests.mutationPaths())
		}
	})
}

func TestReconcileGatewayGraphHoldsL7RemovalOnStaleSnapshotOrFence(t *testing.T) {
	t.Run("snapshot includes an unobserved rule", func(t *testing.T) {
		f := newPolicyRemovalFixture(t)
		f.changeSnapshot = func(policy map[string]any) {
			foreign := maps.Clone(f.rules[0])
			foreign["id"], foreign["tags"] = "foreign-rule", f.neighbor["tags"]
			policy["rules"] = append(slices.Clone(f.rules), foreign)
		}
		result, err := f.reconcile()
		category, _ := cloud.ErrorCategoryOf(err)
		if category != cloud.ErrorCategoryDependencyNotReady || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
			len(f.requests.mutationPaths()) != 0 {
			t.Fatalf("stale removal snapshot = %+v, %v, writes %v", result, err, f.requests.mutationPaths())
		}
	})
	t.Run("Kubernetes binding changed", func(t *testing.T) {
		f := newPolicyRemovalFixture(t)
		fenced := errors.New("stale Kubernetes binding")
		f.spec.ValidateMutation = func(_ context.Context) error {
			if f.requests.count(http.MethodGet, conditionalRemovalPath) != 1 {
				t.Fatal("Kubernetes validation preceded the removal snapshot")
			}
			return fenced
		}
		if _, err := f.reconcile(); !errors.Is(err, fenced) || len(f.requests.mutationPaths()) != 0 {
			t.Fatalf("stale binding = %v, writes %v", err, f.requests.mutationPaths())
		}
	})
}

func TestReconcileGatewayGraphFallsBackToDeletingIndividualL7Rules(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*policyRemovalFixture)
		want  []string
	}{
		{"disabled", func(f *policyRemovalFixture) { f.provider.l7PolicyConditionalRemoval = false }, []string{retiringPolicyPath + "/rules/rule-host"}},
		{"capability missing", func(f *policyRemovalFixture) { f.advertised = false }, []string{retiringPolicyPath + "/rules/rule-host"}},
		{"endpoint unsupported", func(f *policyRemovalFixture) { f.getStatus = http.StatusNotImplemented }, []string{retiringPolicyPath + "/rules/rule-host"}},
		{"writer unsupported", func(f *policyRemovalFixture) { f.postStatus = http.StatusNotImplemented }, []string{conditionalRemovalPath, retiringPolicyPath + "/rules/rule-host"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPolicyRemovalFixture(t)
			tc.setup(f)
			fences := 0
			f.spec.ValidateMutation = func(_ context.Context) error { fences++; return nil }
			result, err := f.reconcile()
			if err != nil || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
				!slices.Equal(f.requests.mutationPaths(), tc.want) || fences != len(tc.want) {
				t.Fatalf("fallback = %+v, %v, writes %v, fences %d", result, err, f.requests.mutationPaths(), fences)
			}
		})
	}
}

func TestReconcileGatewayGraphReadsBackLostL7RemovalResponse(t *testing.T) {
	f := newPolicyRemovalFixture(t)
	f.postStatus = http.StatusBadGateway
	submitted, err := f.reconcile()
	if err == nil || submitted.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("lost removal response = %+v, %v", submitted, err)
	}
	complete, err := f.reconcile()
	if err != nil || complete.Routes[0].Result.Outcome.State != cloud.OutcomeReady ||
		!slices.Equal(f.requests.mutationPaths(), []string{conditionalRemovalPath}) {
		t.Fatalf("lost removal was not recovered by readback: %+v, %v, writes %v", complete, err, f.requests.mutationPaths())
	}
}

type policyRemovalFixture struct {
	spec                  cloud.GatewayGraphSpec
	provider              *Provider
	requests              gatewayTransitionRequests
	policy, neighbor      map[string]any
	listener              map[string]any
	noPolicyContent       bool
	rules                 []map[string]any
	advertised            bool
	getStatus, postStatus int
	changeSnapshot        func(map[string]any)
}

func (f *policyRemovalFixture) reconcile() (cloud.GatewayGraphResult, error) {
	return f.provider.ReconcileGatewayGraph(context.Background(), f.spec)
}

func newPolicyRemovalFixture(t *testing.T) *policyRemovalFixture {
	t.Helper()
	identity := safetyTestIdentity(t)
	peer := identity.Value()
	peer.RouteUID, peer.RouteName = "peer", "peer"
	neighbor, err := NewIdentity(peer)
	if err != nil {
		t.Fatal(err)
	}
	f := &policyRemovalFixture{
		spec: completeGraphSpec(identity), advertised: true, getStatus: http.StatusOK, postStatus: http.StatusAccepted,
	}
	f.spec.Gateway.Disposition = cloud.GraphPreserve
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.spec.Routes = append(f.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{Identity: peer}})

	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "ACTIVE")
	listener := map[string]any{
		"id": "listener-1", "project_id": "project-a", "protocol": "HTTP", "protocol_port": 80,
		"provisioning_status": "ACTIVE", "admin_state_up": true,
		"tags": safetyGatewayTags(t, identity, roleListener), "loadbalancers": []map[string]string{{"id": "load-balancer-1"}},
		"l7policies": []map[string]string{{"id": "policy-1"}, {"id": "policy-2"}},
	}
	f.listener = listener
	f.policy = map[string]any{
		"id": "policy-1", "project_id": "project-a", "listener_id": "listener-1", "position": 1,
		"action": "REJECT", "redirect_pool_id": nil, "admin_state_up": false, "provisioning_status": "ACTIVE",
		"tags": safetyRouteTags(t, identity, rolePolicyExact), "rules": []map[string]string{{"id": "rule-host"}, {"id": "rule-path"}},
	}
	f.neighbor = maps.Clone(f.policy)
	f.neighbor["id"], f.neighbor["position"], f.neighbor["admin_state_up"] = "policy-2", 2, true
	f.neighbor["tags"], f.neighbor["rules"] = safetyRouteTags(t, neighbor, rolePolicyExact), []map[string]string{}
	f.rules = []map[string]any{
		{"id": "rule-host", "project_id": "project-a", "type": "HOST_NAME", "compare_type": "EQUAL_TO", "value": "api.example.com",
			"admin_state_up": true, "invert": false, "provisioning_status": "ACTIVE", "tags": safetyRouteTags(t, identity, roleRuleHost)},
		{"id": "rule-path", "project_id": "project-a", "type": "PATH", "compare_type": "EQUAL_TO", "value": "/api",
			"admin_state_up": true, "invert": false, "provisioning_status": "ACTIVE", "tags": safetyRouteTags(t, identity, roleRulePath)},
	}
	revision := strings.Repeat("ab", 32)
	f.provider = completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.record(r)
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/loadbalancers":
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
		case "GET /lbaas/loadbalancers/load-balancer-1":
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
		case "GET /lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{listener}})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{}})
		case "GET /lbaas/l7policies":
			if f.noPolicyContent {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			policies := []any{f.neighbor}
			if f.policy != nil {
				policies = append(policies, f.policy)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policies": policies})
		case "GET " + retiringPolicyPath + "/rules":
			safetyWriteJSON(t, w, map[string]any{"rules": f.rules})
		case "GET /lbaas/l7policies/policy-2/rules":
			safetyWriteJSON(t, w, map[string]any{"rules": []any{}})
		case "PUT " + retiringPolicyPath:
			f.policy["admin_state_up"] = false
			safetyWriteJSON(t, w, map[string]any{"l7policy": f.policy})
		case "GET " + conditionalRemovalDiscovery:
			caps := []string{}
			if f.advertised {
				caps = append(caps, octavia.GatewayCapabilityL7PolicyConditionalRemoval)
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": caps})
		case "GET " + conditionalRemovalPath:
			if f.getStatus != http.StatusOK {
				w.WriteHeader(f.getStatus)
				return
			}
			snapshot := maps.Clone(f.policy)
			snapshot["rules"] = f.rules
			if f.changeSnapshot != nil {
				f.changeSnapshot(snapshot)
			}
			safetyWriteJSON(t, w, map[string]any{"l7policy_removal": map[string]any{
				"loadbalancer_id": lb["id"], "revision": revision, "policy": snapshot,
			}})
		case "POST " + conditionalRemovalPath:
			var body map[string]map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
				body["l7policy_removal"]["expected_revision"] != revision || f.policy["admin_state_up"] != false {
				t.Fatalf("unexpected removal payload %v, policy %v, decode error %v", body, f.policy, err)
			}
			if f.postStatus == http.StatusAccepted || f.postStatus == http.StatusBadGateway {
				f.policy, f.rules = nil, nil
				f.neighbor["position"] = 1
				listener["l7policies"] = []map[string]string{{"id": "policy-2"}}
			}
			w.WriteHeader(f.postStatus)
		case "DELETE " + retiringPolicyPath + "/rules/rule-host":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}), &f.requests)
	f.provider.l7PolicyConditionalRemoval = true
	return f
}
