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
)

const (
	tcpLBs       = "/v2/lbaas/loadbalancers"
	tcpLB        = tcpLBs + "/gateway"
	tcpListeners = "/v2/lbaas/listeners"
	tcpListener  = tcpListeners + "/listener"
	tcpPools     = "/v2/lbaas/pools"
	tcpPool      = tcpPools + "/pool"
	tcpMembers   = tcpPool + "/members"
	tcpMember    = tcpMembers + "/member"
	tcpMonitors  = "/v2/lbaas/healthmonitors"
	tcpMonitor   = tcpMonitors + "/monitor"
)

type tcpGraphFixture struct {
	t                      *testing.T
	spec                   cloud.GatewayGraphSpec
	inventory              map[string]map[string]any
	writes                 []string
	loseNextListenerUpdate bool
	hiddenChild            string
}

func newTCPGraphFixture(t *testing.T) *tcpGraphFixture {
	t.Helper()
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolTCP
	route := &spec.Routes[0].Spec
	route.Gateway = cloud.GatewayState{}
	route.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolTCP
	route.Hostname, route.PathType, route.PathValue = "", "", ""
	return &tcpGraphFixture{t: t, spec: spec, inventory: map[string]map[string]any{}}
}

func (f *tcpGraphFixture) provider() *Provider {
	return newTestProvider(f.t, hibiscusServiceClients(http.HandlerFunc(f.serveHTTP)), ProviderConfig{})
}

func (f *tcpGraphFixture) reconcile(provider *Provider) cloud.GatewayGraphResult {
	f.t.Helper()
	before := len(f.writes)
	result, err := provider.ReconcileGatewayGraph(f.t.Context(), f.spec)
	if err != nil || len(f.writes) > before+1 {
		f.t.Fatalf("reconcile = %#v, %v, with writes %v", result, err, f.writes[before:])
	}
	return result
}

func (f *tcpGraphFixture) serveHTTP(w http.ResponseWriter, req *http.Request) {
	type collection struct{ name, parentKey, parentID, responseParent string }
	collections := map[string]collection{
		tcpLBs:       {"loadbalancer", "", "", ""},
		tcpListeners: {"listener", "loadbalancer_id", "gateway", "loadbalancers"},
		tcpPools:     {"pool", "loadbalancer_id", "gateway", "loadbalancers"},
		tcpMembers:   {"member", "", "", ""},
		tcpMonitors:  {"healthmonitor", "pool_id", "pool", "pools"},
	}
	resources := map[string]string{tcpLBs: tcpLB, tcpListeners: tcpListener, tcpPools: tcpPool, tcpMembers: tcpMember, tcpMonitors: tcpMonitor}
	reject := func() {
		f.t.Errorf("unexpected Octavia request %s %s", req.Method, req.URL.RequestURI())
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
	resource := req.URL.Path
	if resource == "/v2.0/floatingips" && req.Method == http.MethodGet {
		safetyWriteJSON(f.t, w, map[string]any{"floatingips": []any{}})
		return
	}
	for listPath, entryPath := range resources {
		if resource != listPath && resource != entryPath {
			continue
		}
		kind := collections[listPath]
		entry := f.inventory[entryPath]
		switch req.Method {
		case http.MethodGet:
			if resource == listPath {
				if !safetyRequireQuery(f.t, w, req, "project_id", hibiscusProject) ||
					kind.parentKey != "" && !safetyRequireQuery(f.t, w, req, kind.parentKey, kind.parentID) {
					return
				}
				items := []map[string]any{}
				if entry != nil && entryPath != f.hiddenChild {
					items = append(items, entry)
				}
				safetyWriteJSON(f.t, w, map[string]any{kind.name + "s": items})
			} else if entry == nil {
				w.WriteHeader(http.StatusNotFound)
			} else {
				if resource == tcpLB {
					for key, child := range map[string]string{"listeners": tcpListener, "pools": tcpPool} {
						references := []map[string]any{}
						if item := f.inventory[child]; item != nil {
							references = append(references, map[string]any{"id": item["id"]})
						}
						entry[key] = references
					}
				}
				safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
			}
		case http.MethodPost:
			f.writes = append(f.writes, req.Method+" "+resource)
			var body map[string]map[string]any
			if resource != listPath || entry != nil || json.NewDecoder(req.Body).Decode(&body) != nil || body[kind.name] == nil {
				reject()
				return
			}
			data := body[kind.name]
			_, linked := data["listener_id"]
			_, defaulted := data["default_pool_id"]
			if (listPath == tcpListeners || listPath == tcpPools) && (data["protocol"] != "TCP" || linked || defaulted) {
				f.t.Errorf("expected an unattached TCP resource, got %v", data)
			}
			_, hasURL := data["url_path"]
			_, hasMethod := data["http_method"]
			_, hasCodes := data["expected_codes"]
			if listPath == tcpMonitors && (data["type"] != "TCP" || hasURL || hasMethod || hasCodes) {
				f.t.Errorf("expected a native TCP monitor, got %v", data)
			}
			if kind.responseParent != "" {
				data[kind.responseParent] = []map[string]any{{"id": data[kind.parentKey]}}
			}
			data["id"], data["project_id"], data["provisioning_status"] = entryPath[len(listPath)+1:], hibiscusProject, "ACTIVE"
			switch listPath {
			case tcpLBs:
				data["vip_address"], data["vip_port_id"] = "127.0.0.1", "vip-port"
			case tcpListeners:
				data["default_pool_id"] = nil
			}
			f.inventory[entryPath] = data
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(f.t, w, map[string]any{kind.name: data})
		case http.MethodPut:
			f.writes = append(f.writes, req.Method+" "+resource)
			var body map[string]map[string]any
			if resource != tcpListener || entry == nil || json.NewDecoder(req.Body).Decode(&body) != nil {
				reject()
				return
			}
			payload := body[kind.name]
			target, ok := payload["default_pool_id"]
			monitorRequired := f.spec.Routes[0].Spec.HealthMonitor != (cloud.HealthMonitorConfig{})
			if !ok || len(payload) != 1 || target != nil && (target != "pool" || f.inventory[tcpMember]["provisioning_status"] != "ACTIVE" ||
				f.inventory[tcpPool]["provisioning_status"] != "ACTIVE" || monitorRequired && f.inventory[tcpMonitor]["provisioning_status"] != "ACTIVE") {
				reject()
				return
			}
			entry["default_pool_id"], entry["provisioning_status"] = target, "PENDING_UPDATE"
			if target == nil {
				f.inventory[tcpPool]["listeners"] = nil
			} else {
				f.inventory[tcpPool]["listeners"] = []map[string]any{{"id": "listener"}}
			}
			if f.loseNextListenerUpdate {
				f.loseNextListenerUpdate = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
		case http.MethodDelete:
			f.writes = append(f.writes, req.Method+" "+resource)
			if resource != entryPath || entry == nil || f.inventory[tcpListener]["default_pool_id"] != nil {
				reject()
				return
			}
			delete(f.inventory, entryPath)
			w.WriteHeader(http.StatusNoContent)
		default:
			reject()
		}
		return
	}
	reject()
}

func TestTCPGraphDefaultPoolLifecycle(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	for _, path := range []string{tcpLBs, tcpListeners, tcpPools, tcpMembers, tcpMonitors} {
		before := len(f.writes)
		result := f.reconcile(provider)
		if result.Outcome.State != cloud.OutcomeProgressing || !slices.Equal(f.writes[before:], []string{"POST " + path}) {
			t.Fatalf("reconcile = %#v, writes = %v, want POST %s", result, f.writes[before:], path)
		}
	}
	f.inventory[tcpMember]["provisioning_status"] = "PENDING_CREATE"
	result := f.reconcile(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != 5 {
		t.Fatalf("pending backend = %#v, writes %v", result, f.writes)
	}
	f.inventory[tcpMember]["provisioning_status"] = "ACTIVE"
	result = f.reconcile(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || f.inventory[tcpListener]["default_pool_id"] != "pool" || len(f.writes) != 6 {
		t.Fatalf("attachment = %#v, writes %v", result, f.writes)
	}
	result = f.reconcile(f.provider())
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != 6 {
		t.Fatalf("pending attachment after restart = %#v, writes %v", result, f.writes)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	result = f.reconcile(f.provider())
	if result.Outcome.State != cloud.OutcomeReady || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != 6 {
		t.Fatalf("settled attachment after restart = %#v, writes %v", result, f.writes)
	}
	route := &f.spec.Routes[0]
	route.Disposition = cloud.GraphPreserve
	route.Spec = cloud.RouteSpec{Identity: route.Spec.Identity}
	result = f.reconcile(provider)
	if result.Outcome.State != cloud.OutcomeProgressing || f.inventory[tcpListener]["default_pool_id"] != nil || len(f.writes) != 7 {
		t.Fatalf("revocation = %#v, writes %v", result, f.writes)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	result = f.reconcile(f.provider())
	if result.Outcome.State != cloud.OutcomeReady || f.inventory[tcpPool] == nil || len(f.writes) != 7 {
		t.Fatalf("preserved route = %#v, writes %v", result, f.writes)
	}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	route.Disposition = cloud.GraphDelete
	for range 4 {
		result = f.reconcile(provider)
	}
	want := []string{"DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool}
	if result.Outcome.State != cloud.OutcomeReady || !slices.Equal(f.writes[7:], want) {
		t.Fatalf("route deletion = %#v, writes %v, want %v", result, f.writes[7:], want)
	}
}

func TestTCPGraphCanAttachWithoutMonitor(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.spec.Routes[0].Spec.HealthMonitor = cloud.HealthMonitorConfig{}
	provider := f.provider()
	for range 5 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(provider); result.Outcome.State != cloud.OutcomeReady || f.inventory[tcpMonitor] != nil || f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("TCP listener without a configured monitor = %#v, writes %v", result, f.writes)
	}
}

func TestTCPGraphDetachesServingPoolBeforeChangingMembers(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	for range 6 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.spec.Routes[0].Spec.Members[0].Port++
	if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		f.inventory[tcpListener]["default_pool_id"] != nil || !slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) {
		t.Fatalf("serving pool changed before detach = %#v, writes %v", result, f.writes[6:])
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		!slices.Equal(f.writes[7:], []string{"DELETE " + tcpMember}) {
		t.Fatalf("detached pool did not start converging = %#v, writes %v", result, f.writes[7:])
	}
}

func TestTCPGraphGatewayCleanupProvesOwnershipBeforeDetaching(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		corrupt func(*tcpGraphFixture)
	}{
		{name: "unproven default", corrupt: func(f *tcpGraphFixture) { f.inventory[tcpListener]["default_pool_id"] = "unproven" }},
		{name: "foreign project", corrupt: func(f *tcpGraphFixture) { f.inventory[tcpMember]["project_id"] = "other-project" }},
		{name: "stale route UID", corrupt: func(f *tcpGraphFixture) { f.spec.Routes[0].Spec.Identity.RouteUID = "new-route-uid" }},
		{name: "unproven member", corrupt: func(f *tcpGraphFixture) {
			f.inventory[tcpPool]["members"] = []map[string]string{{"id": "hidden-member"}}
		}},
		{name: "owned graph"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newTCPGraphFixture(t)
			provider := f.provider()
			for range 6 {
				f.reconcile(provider)
			}
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
			f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete}
			f.spec.Routes[0] = cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{Identity: f.spec.Routes[0].Spec.Identity}}
			if scenario.corrupt != nil {
				scenario.corrupt(f)
				_, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
				if !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != 6 {
					t.Fatalf("unproven default = %v, writes %v", err, f.writes)
				}
				return
			}
			f.loseNextListenerUpdate = true
			_, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
			if err == nil || f.inventory[tcpListener]["default_pool_id"] != nil || !slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) {
				t.Fatalf("lost detach response = %v, writes %v", err, f.writes)
			}
			provider = f.provider()
			if result := f.reconcile(provider); result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != 7 {
				t.Fatalf("pending detach after restart = %#v, writes %v", result, f.writes)
			}
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
			for range 5 {
				f.reconcile(provider)
			}
			want := []string{"DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool, "DELETE " + tcpListener, "DELETE " + tcpLB}
			if result := f.reconcile(provider); result.Outcome.State != cloud.OutcomeReady || !slices.Equal(f.writes[7:], want) {
				t.Fatalf("Gateway cleanup = %#v, writes %v, want %v", result, f.writes[7:], want)
			}
		})
	}
}

func TestTCPRouteCleanupAfterListenerDisappears(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	for range 5 {
		f.reconcile(provider)
	}
	// The listener disappeared after the backend objects were created. On
	// restart only the durably detaching TCPRoute can identify its transport.
	delete(f.inventory, tcpListener)
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	node := &f.spec.Routes[0]
	node.Disposition = cloud.GraphDelete
	node.Spec = cloud.RouteSpec{Identity: node.Spec.Identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolTCP}}
	provider = f.provider()
	for range 3 {
		f.reconcile(provider)
	}
	result := f.reconcile(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || !slices.Equal(f.writes[5:], []string{"DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool}) || f.inventory[tcpLB] == nil {
		t.Fatalf("route cleanup after listener loss = %#v, writes %v", result, f.writes)
	}
}

func TestTCPGraphListenerErrorFailsEnsureAndAllowsOwnedRevocation(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	for range 6 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ERROR"
	if result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrResourceFailure) ||
		result.Gateway.Outcome.State == cloud.OutcomeReady || len(f.writes) != 6 {
		t.Fatalf("ensure with a failed listener = %#v, %v, writes %v", result, err, f.writes[6:])
	}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeProgressing ||
		f.inventory[tcpListener]["default_pool_id"] != nil || !slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) {
		t.Fatalf("revocation with a failed listener = %#v, writes %v", result, f.writes[6:])
	}
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != 7 {
		t.Fatalf("pending detach on restart = %#v, writes %v", result, f.writes[6:])
	}
	f.inventory[tcpListener]["provisioning_status"] = "ERROR"
	if _, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrResourceFailure) || len(f.writes) != 7 {
		t.Fatalf("failed detach should hold descendants = %v, writes %v", err, f.writes[6:])
	}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolTCP}}
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeProgressing ||
		!slices.Equal(f.writes[7:], []string{"DELETE " + tcpListener}) {
		t.Fatalf("Gateway cleanup should remove the failed listener first = %#v, writes %v", result, f.writes[7:])
	}
	for range 4 {
		f.reconcile(f.provider())
	}
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeReady || len(f.inventory) != 0 {
		t.Fatalf("cleanup did not converge after removing the failed listener = %#v, inventory %v", result, f.inventory)
	}
}

func TestTCPGraphBlocksCleanupWhileParentStillReferencesUnlistedChildren(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	for range 5 {
		f.reconcile(provider)
	}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.hiddenChild = tcpPool
	if result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrDependencyNotReady) ||
		len(result.Routes) != 0 || len(f.writes) != 5 {
		t.Fatalf("route cleanup with an unlisted pool = %#v, %v, writes %v", result, err, f.writes[5:])
	}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolTCP}}
	for _, child := range []string{tcpPool, tcpListener} {
		f.hiddenChild = child
		if result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrDependencyNotReady) ||
			result.Outcome.State == cloud.OutcomeReady || len(f.writes) != 5 {
			t.Fatalf("Gateway cleanup with unlisted %s = %#v, %v, writes %v", child, result, err, f.writes[5:])
		}
	}
}
