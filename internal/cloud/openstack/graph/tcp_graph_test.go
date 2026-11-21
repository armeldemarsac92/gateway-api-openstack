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
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
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
	loseNextListenerCreate bool
	hiddenChild            string
	unreferencedChild      string
	extraListedMembers     []map[string]any
	rejectMonitorDelay     float64
}

func newTCPGraphFixture(t *testing.T) *tcpGraphFixture {
	return newTransportGraphFixture(t, "TCP")
}

func newTransportGraphFixture(t *testing.T, protocol string) *tcpGraphFixture {
	t.Helper()
	identity := safetyTestIdentity(t)
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ListenerProtocol = protocol
	route := &spec.Routes[0].Spec
	route.Gateway = cloud.GatewayState{}
	route.GatewayRequirements.ListenerProtocol = protocol
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
	resource := req.URL.Path
	memberCollection := tcpMembers
	if subpath, found := strings.CutPrefix(resource, tcpPools+"/"); found {
		poolID, child, _ := strings.Cut(subpath, "/")
		if child == "members" || strings.HasPrefix(child, "members/") {
			memberCollection = tcpPools + "/" + poolID + "/members"
		}
	}
	collections := map[string]collection{
		tcpLBs:           {"loadbalancer", "", "", ""},
		tcpListeners:     {"listener", "loadbalancer_id", "gateway", "loadbalancers"},
		tcpPools:         {"pool", "loadbalancer_id", "gateway", "loadbalancers"},
		memberCollection: {"member", "", "", ""},
		tcpMonitors:      {"healthmonitor", "pool_id", req.URL.Query().Get("pool_id"), "pools"},
	}
	resources := map[string]string{tcpLBs: tcpLB, tcpListeners: tcpListener, tcpPools: tcpPool, memberCollection: memberCollection + "/member", tcpMonitors: tcpMonitor}
	reject := func() {
		f.t.Errorf("unexpected Octavia request %s %s", req.Method, req.URL.RequestURI())
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
	if resource == "/v2.0/floatingips" && req.Method == http.MethodGet {
		safetyWriteJSON(f.t, w, map[string]any{"floatingips": []any{}})
		return
	}
	if resource == "/v2/lbaas/l7policies" && req.Method == http.MethodGet && f.inventory[tcpListener]["protocol"] == "HTTP" {
		if safetyRequireQuery(f.t, w, req, "project_id", hibiscusProject) && safetyRequireQuery(f.t, w, req, "listener_id", "listener") {
			safetyWriteJSON(f.t, w, map[string]any{"l7policies": []any{}})
		}
		return
	}
	for listPath, entryPath := range resources {
		if id, found := strings.CutPrefix(resource, listPath+"/"); found && !strings.Contains(id, "/") {
			entryPath = resource
		}
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
				for _, child := range f.children(listPath) {
					if listPath+"/"+child["id"].(string) != f.hiddenChild &&
						(listPath != tcpMonitors || child["pool_id"] == kind.parentID) {
						items = append(items, child)
					}
				}
				if listPath == memberCollection {
					items = append(items, f.extraListedMembers...)
				}
				safetyWriteJSON(f.t, w, map[string]any{kind.name + "s": items})
			} else if entry == nil {
				w.WriteHeader(http.StatusNotFound)
			} else {
				if resource == tcpLB {
					for key, collection := range map[string]string{"listeners": tcpListeners, "pools": tcpPools} {
						references := []map[string]any{}
						for _, item := range f.children(collection) {
							if collection+"/"+item["id"].(string) != f.unreferencedChild {
								references = append(references, map[string]any{"id": item["id"]})
							}
						}
						entry[key] = references
					}
				}
				safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
			}
		case http.MethodPost:
			f.writes = append(f.writes, req.Method+" "+resource)
			var body map[string]map[string]any
			if resource != listPath || json.NewDecoder(req.Body).Decode(&body) != nil || body[kind.name] == nil {
				reject()
				return
			}
			data := body[kind.name]
			if listPath == tcpListeners && data["protocol_port"] != float64(f.spec.Gateway.Spec.ListenerPort) {
				entryPath = tcpListener + "-" + strconv.Itoa(int(data["protocol_port"].(float64)))
			}
			if listPath == tcpPools && len(f.spec.Routes) > 1 &&
				slices.Contains(pilotTags(data["tags"]), openstackidentity.Tag("route-uid", f.spec.Routes[1].Spec.Identity.RouteUID)) {
				entryPath = tcpPool + "-secondary"
			}
			if listPath == tcpMonitors && data["pool_id"] == "pool-secondary" {
				entryPath = tcpMonitor + "-secondary"
			}
			if f.inventory[entryPath] != nil {
				reject()
				return
			}
			_, linked := data["listener_id"]
			_, defaulted := data["default_pool_id"]
			protocol := f.spec.Gateway.Spec.ListenerProtocol
			if (listPath == tcpListeners || listPath == tcpPools) && (data["protocol"] != protocol || linked || defaulted) {
				f.t.Errorf("expected an unattached %s resource, got %v", protocol, data)
			}
			if listPath == tcpPools && data["lb_algorithm"] != "ROUND_ROBIN" {
				f.t.Errorf("expected a ROUND_ROBIN pool, got %v", data)
			}
			_, hasURL := data["url_path"]
			_, hasMethod := data["http_method"]
			_, hasCodes := data["expected_codes"]
			if listPath == tcpMonitors {
				monitorType := "TCP"
				if protocol == "UDP" {
					monitorType = "UDP-CONNECT"
				}
				if data["type"] != monitorType || hasURL || hasMethod || hasCodes {
					f.t.Errorf("expected a native %s monitor, got %v", monitorType, data)
				}
				if data["delay"] == f.rejectMonitorDelay {
					http.Error(w, `{"faultstring":"The request delay value is below the configured UDP-CONNECT interval"}`, http.StatusBadRequest)
					return
				}
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
			case tcpPools:
				data["listeners"] = []map[string]any{}
			case memberCollection:
				data["backup"] = false
			}
			f.inventory[entryPath] = data
			if listPath == tcpListeners && f.loseNextListenerCreate {
				f.loseNextListenerCreate = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(f.t, w, map[string]any{kind.name: data})
		case http.MethodPut:
			f.writes = append(f.writes, req.Method+" "+resource)
			var body map[string]map[string]any
			if entry == nil || json.NewDecoder(req.Body).Decode(&body) != nil {
				reject()
				return
			}
			payload := body[kind.name]
			if resource == tcpPool {
				persistence, found := payload["session_persistence"]
				if f.inventory[tcpListener]["default_pool_id"] != nil || !found || persistence != nil {
					reject()
					return
				}
				entry["session_persistence"], entry["provisioning_status"] = nil, "PENDING_UPDATE"
				safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
				return
			}
			if listPath == memberCollection && resource != listPath {
				weight, ok := payload["weight"].(float64)
				pool := f.inventory[strings.TrimSuffix(memberCollection, "/members")]
				attached := slices.ContainsFunc(f.children(tcpListeners), func(listener map[string]any) bool {
					return listener["default_pool_id"] == pool["id"]
				})
				if attached || !ok || weight < 0 || weight > 256 || payload["admin_state_up"] != true {
					reject()
					return
				}
				if backup, found := payload["backup"]; found {
					entry["backup"] = backup
				}
				entry["weight"], entry["admin_state_up"], entry["provisioning_status"] = weight, true, "PENDING_UPDATE"
				pool["provisioning_status"] = "PENDING_UPDATE"
				safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
				return
			}
			target, ok := payload["default_pool_id"]
			if listPath != tcpListeners || !ok || len(payload) != 1 || target != nil && !f.poolReady(target.(string)) {
				reject()
				return
			}
			entry["default_pool_id"], entry["provisioning_status"] = target, "PENDING_UPDATE"
			for _, pool := range f.children(tcpPools) {
				references := []map[string]any{}
				for _, listener := range f.children(tcpListeners) {
					if listener["default_pool_id"] == pool["id"] {
						references = append(references, map[string]any{"id": listener["id"]})
					}
				}
				pool["listeners"] = references
			}
			if f.loseNextListenerUpdate {
				f.loseNextListenerUpdate = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			safetyWriteJSON(f.t, w, map[string]any{kind.name: entry})
		case http.MethodDelete:
			f.writes = append(f.writes, req.Method+" "+resource)
			poolID := entry["pool_id"]
			switch listPath {
			case tcpPools:
				poolID = entry["id"]
			case memberCollection:
				poolID = strings.TrimSuffix(strings.TrimPrefix(memberCollection, tcpPools+"/"), "/members")
			}
			if resource != entryPath || entry == nil || entry["default_pool_id"] != nil ||
				poolID != nil && slices.ContainsFunc(f.children(tcpListeners), func(listener map[string]any) bool { return listener["default_pool_id"] == poolID }) {
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

func (f *tcpGraphFixture) poolReady(id string) bool {
	routeIndex, monitorPath := 0, tcpMonitor
	if id == "pool-secondary" {
		routeIndex, monitorPath = 1, tcpMonitor+"-secondary"
	}
	poolPath := tcpPools + "/" + id
	monitorRequired := f.spec.Routes[routeIndex].Spec.HealthMonitor != (cloud.HealthMonitorConfig{})
	return f.inventory[poolPath]["provisioning_status"] == "ACTIVE" &&
		f.inventory[poolPath+"/members/member"]["provisioning_status"] == "ACTIVE" &&
		(!monitorRequired || f.inventory[monitorPath]["provisioning_status"] == "ACTIVE")
}

func (f *tcpGraphFixture) children(collection string) []map[string]any {
	var children []map[string]any
	for path, item := range f.inventory {
		if id, found := strings.CutPrefix(path, collection+"/"); found && !strings.Contains(id, "/") {
			children = append(children, item)
		}
	}
	slices.SortFunc(children, func(a, b map[string]any) int {
		return strings.Compare(a["id"].(string), b["id"].(string))
	})
	return children
}

func TestMultiUDPGraphUpgradesAndRemovesOneSharedListener(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	provider := f.provider()
	for range 6 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.enableMultiUDP()
	f.loseNextListenerCreate = true
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || f.inventory[tcpListener+"-1812"] == nil {
		t.Fatalf("expected the Octavia listener to exist after losing its create response, got %v", err)
	}
	provider = f.provider()
	settled := f.settleTransportGraph(provider)
	if settled.Routes[0].Result.State.PoolID != "pool" || len(f.children(tcpListeners)) != 3 {
		t.Fatalf("expected three listeners with the original pool, got %#v, %v", settled, f.inventory)
	}
	for _, listener := range f.children(tcpListeners) {
		port := int(listener["protocol_port"].(float64))
		marked := slices.Contains(pilotTags(listener["tags"]), scopedPortTag(cloud.ListenerProtocolUDP, port))
		if listener["default_pool_id"] != "pool" || marked == (listener["id"] == "listener") {
			t.Fatalf("incorrect pool or port marker for listener %v", listener)
		}
	}
	before := len(f.writes)
	f.spec.Gateway.Spec.ScopedListeners = f.spec.Gateway.Spec.ScopedListeners[1:2]
	f.spec.Routes[0].Spec.ScopedListenerPorts = []int{5353}
	f.settleTransportGraph(provider)
	sibling := f.inventory[tcpListener+"-5353"]
	if sibling == nil || sibling["default_pool_id"] != "pool" || len(f.children(tcpListeners)) != 1 {
		t.Fatalf("retained listener stopped serving, inventory %v", f.inventory)
	}
	if got := f.writes[before:]; !slices.Equal(got, []string{"PUT " + tcpListener, "PUT " + tcpListener + "-1812", "DELETE " + tcpListener, "DELETE " + tcpListener + "-1812"}) {
		t.Fatalf("unexpected writes while removing listeners: %v", got)
	}
}

func (f *tcpGraphFixture) enableMultiUDP() {
	anchor := f.spec.Gateway.Spec.ListenerPort
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "anchor", Port: anchor}, {Name: "dns", Port: 5353}, {Name: "radius", Port: 1812},
	}
	f.spec.Routes[0].Spec.ScopedListenerPorts = []int{anchor, 1812, 5353}
	slices.Sort(f.spec.Routes[0].Spec.ScopedListenerPorts)
}

func (f *tcpGraphFixture) settleTransportGraph(provider *Provider) cloud.GatewayGraphResult {
	f.t.Helper()
	for range 20 {
		for _, listener := range f.children(tcpListeners) {
			if status, _ := listener["provisioning_status"].(string); strings.HasPrefix(status, "PENDING_") {
				listener["provisioning_status"] = "ACTIVE"
			}
		}
		result := f.reconcile(provider)
		if result.Outcome.State == cloud.OutcomeReady {
			return result
		}
	}
	f.t.Fatalf("%s graph did not converge, writes %v", f.spec.Gateway.Spec.ListenerProtocol, f.writes)
	return cloud.GatewayGraphResult{}
}

func TestMultiUDPGraphChangesOneWinnerAndPreservesOtherRouteDuringCleanup(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	first := &f.spec.Routes[0]
	first.Spec.ScopedListenerPorts = []int{f.spec.Gateway.Spec.ListenerPort, 5353}
	second := *first
	second.Spec.Identity.RouteName, second.Spec.Identity.RouteUID = "other", "other-uid"
	second.Spec.Precedence++
	second.Spec.ScopedListenerPorts = []int{1812}
	f.spec.Routes = append(f.spec.Routes, second)
	middle := tcpListener + "-1812"
	before := len(f.writes)
	result := f.settleTransportGraph(provider)
	want := []string{"PUT " + middle, "POST " + tcpPools, "POST " + tcpPool + "-secondary/members",
		"POST " + tcpMonitors, "PUT " + middle}
	if got := f.writes[before:]; !slices.Equal(got, want) || result.Routes[1].Result.State.PoolID != "pool-secondary" {
		t.Fatalf("wrong handoff: writes %v, result %#v", got, result)
	}
	if f.inventory[middle]["default_pool_id"] != "pool-secondary" ||
		f.inventory[tcpListener]["default_pool_id"] != "pool" ||
		f.inventory[tcpListener+"-5353"]["default_pool_id"] != "pool" {
		t.Fatalf("incorrect forwarding graph: %v", f.children(tcpListeners))
	}

	// A restarted route-only deletion has no desired listener inventory.
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.spec.Routes[1].Disposition = cloud.GraphPreserve
	before = len(f.writes)
	provider = f.provider()
	f.reconcile(provider)
	pending := f.reconcile(provider)
	if len(f.writes) != before+1 || pending.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("cleanup did not wait for settlement: writes %v, result %#v", f.writes[before:], pending)
	}
	f.settleTransportGraph(provider)
	want = []string{"PUT " + tcpListener, "PUT " + tcpListener + "-5353", "DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool}
	if got := f.writes[before:]; !slices.Equal(got, want) || f.inventory[middle]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("route-only deletion disturbed the other route: writes %v, listeners %v", got, f.children(tcpListeners))
	}
	f.udpCleanupGateway(cloud.GraphDelete)
	f.settleTransportGraph(f.provider())
	if len(f.inventory) != 0 {
		t.Fatalf("whole Gateway deletion leaked resources: %v", f.inventory)
	}
}

func TestMultiUDPGraphBlocksCleanupAcrossInconsistentAssociations(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	listener := f.inventory[tcpListener+"-1812"]
	pool := f.inventory[tcpPool]
	before := len(f.writes)
	assertBlocked := func(want cloud.ErrorCategory) {
		t.Helper()
		_, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
		if category, _ := cloud.ErrorCategoryOf(err); category != want || len(f.writes) != before {
			t.Fatalf("unsafe readback = %v, new writes %v, want %s", err, f.writes[before:], want)
		}
	}

	listener["default_pool_id"] = nil
	assertBlocked(cloud.ErrorCategoryDependencyNotReady)
	listener["default_pool_id"] = "pool"
	reverse := pool["listeners"]
	pool["listeners"] = nil
	assertBlocked(cloud.ErrorCategoryDependencyNotReady)
	pool["listeners"] = []map[string]any{{"id": "foreign-listener"}}
	assertBlocked(cloud.ErrorCategoryOwnershipConflict)
	pool["listeners"] = reverse

	tags := listener["tags"]
	listener["tags"] = append(pilotTags(tags), scopedPortTag(cloud.ListenerProtocolUDP, 1812))
	assertBlocked(cloud.ErrorCategoryOwnershipConflict)
	listener["tags"] = tags

	f.unreferencedChild = tcpListener + "-1812"
	assertBlocked(cloud.ErrorCategoryDependencyNotReady)
	f.unreferencedChild = ""
	listener["project_id"] = "foreign-project"
	assertBlocked(cloud.ErrorCategoryOwnershipConflict)
}

func TestMultiUDPGraphKeepsPoolWhenDetachReportsError(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	before := len(f.writes)
	for _, listener := range f.children(tcpListeners) {
		f.reconcile(provider)
		for _, observed := range f.children(tcpListeners) {
			observed["provisioning_status"] = "ACTIVE"
		}
		if listener["default_pool_id"] != nil && listener["id"] == "listener" {
			t.Fatal("expected the original listener to detach first")
		}
	}
	f.inventory[tcpListener]["provisioning_status"] = "ERROR"
	_, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec)
	want := []string{"PUT " + tcpListener, "PUT " + tcpListener + "-1812", "PUT " + tcpListener + "-5353"}
	if category, _ := cloud.ErrorCategoryOf(err); category != cloud.ErrorCategoryResourceFailure || !slices.Equal(f.writes[before:], want) {
		t.Fatalf("cleanup after failed detach = %v, writes %v, want %v", err, f.writes[before:], want)
	}
}

func (f *tcpGraphFixture) udpCleanupGateway(disposition cloud.GraphDisposition) {
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: disposition, Spec: cloud.GatewaySpec{
		ListenerProtocol: "UDP", ListenerPort: f.spec.Gateway.Spec.ListenerPort, ScopedListeners: []cloud.ScopedListener{},
	}}
}

func TestMultiUDPDetachedPoolNeedsExplicitEmptyReverseList(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	for _, listener := range f.children(tcpListeners) {
		listener["default_pool_id"] = nil
	}
	pool := f.inventory[tcpPool]
	before := len(f.writes)
	delete(pool, "listeners")
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || len(f.writes) != before {
		t.Fatalf("missing reverse inventory permitted pool cleanup: %v, writes %v", err, f.writes[before:])
	}
	pool["listeners"] = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || len(f.writes) != before {
		t.Fatalf("null reverse inventory permitted pool cleanup: %v, writes %v", err, f.writes[before:])
	}
	pool["listeners"] = []map[string]any{}
	// An empty reverse list alone cannot prove what a listener forwards to.
	listener := f.inventory[tcpListener+"-5353"]
	delete(listener, "default_pool_id")
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || len(f.writes) != before {
		t.Fatalf("missing forward association permitted pool cleanup: %v, writes %v", err, f.writes[before:])
	}
	listener["default_pool_id"] = nil
	f.settleTransportGraph(provider)
	if f.inventory[tcpPool] != nil {
		t.Fatal("route-owned pool survived authorized cleanup after explicit detach")
	}
}

func TestMultiUDPCleanupRequiresFrozenPortForUnmarkedListener(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	provider := f.provider()
	f.settleTransportGraph(provider)
	f.enableMultiUDP()
	f.settleTransportGraph(provider)
	if slices.ContainsFunc(pilotTags(f.inventory[tcpListener]["tags"]), func(tag string) bool { return scopedPortTagProtocol(tag) == cloud.ListenerProtocolUDP }) {
		t.Fatal("expected the original single-port listener to retain its legacy tags")
	}
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	original := f.spec.Gateway.Spec.ListenerPort
	before := len(f.writes)
	f.spec.Gateway.Spec.ListenerPort = 0
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || len(f.writes) != before {
		t.Fatalf("missing frozen port allowed adoption: %v, writes %v", err, f.writes[before:])
	}
	// After both marked listeners disappear, the sole unmarked listener still
	// needs the enrolled Gateway's port and the pool's complete reverse list.
	delete(f.inventory, tcpListener+"-1812")
	delete(f.inventory, tcpListener+"-5353")
	f.inventory[tcpPool]["listeners"] = []map[string]any{{"id": "listener"}}
	f.spec.Gateway.Spec.ListenerPort = original + 1
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); err == nil || len(f.writes) != before {
		t.Fatalf("last unmarked listener on the wrong port was adopted: %v, writes %v", err, f.writes[before:])
	}
	f.spec.Gateway.Spec.ListenerPort = original
	f.settleTransportGraph(provider)
	if f.inventory[tcpPool] != nil {
		t.Fatal("fully owned orphaned route pool survived cleanup")
	}
}

func TestMultiUDPWithdrawsUnelectedDefaultsBeforeReportingObserved(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	route := &f.spec.Routes[0]
	route.Spec.ScopedListenerPorts = []int{f.spec.Gateway.Spec.ListenerPort}
	changing := f.reconcile(provider)
	if changing.Routes[0].Result.Outcome.State == cloud.OutcomeReady {
		t.Fatal("route marked observed while the reverse list still contains superseded ports")
	}
	f.settleTransportGraph(provider)
	route.Disposition = cloud.GraphPreserve
	route.RetainScopedListenerPorts = []int{}
	before := len(f.writes)
	f.settleTransportGraph(provider)
	if !slices.Equal(f.writes[before:], []string{"PUT " + tcpListener}) || f.inventory[tcpPool] == nil {
		t.Fatalf("revoked route must detach only its remaining default, preserving the pool: %v", f.writes[before:])
	}
}

func TestMultiUDPReportsServingPortWhenSiblingIsPending(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	provider := f.provider()
	f.settleTransportGraph(provider)
	f.inventory[tcpListener+"-1812"]["provisioning_status"] = "PENDING_UPDATE"
	result := f.reconcile(provider)
	if result.Gateway.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{f.spec.Gateway.Spec.ListenerPort, 5353}) {
		t.Fatalf("partial UDP listener readiness = %#v", result.Gateway)
	}
}

func TestDefaultPoolLifecycle(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol, func(t *testing.T) { testDefaultPoolLifecycle(t, protocol) })
	}
}

func testDefaultPoolLifecycle(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
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

func TestDefaultPoolCanAttachWithoutMonitor(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol, func(t *testing.T) { testDefaultPoolCanAttachWithoutMonitor(t, protocol) })
	}
}

func testDefaultPoolCanAttachWithoutMonitor(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
	f.spec.Routes[0].Spec.HealthMonitor = cloud.HealthMonitorConfig{}
	provider := f.provider()
	for range 5 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(provider); result.Outcome.State != cloud.OutcomeReady || f.inventory[tcpMonitor] != nil || f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("%s listener without a configured monitor = %#v, writes %v", protocol, result, f.writes)
	}
}

func TestDefaultPoolDetachesServingPoolBeforeChangingMembers(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol, func(t *testing.T) { testDefaultPoolDetachesServingPoolBeforeChangingMembers(t, protocol) })
	}
}

func testDefaultPoolDetachesServingPoolBeforeChangingMembers(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
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
		wantErr error
	}{
		{name: "unproven default", wantErr: cloud.ErrOwnershipConflict, corrupt: func(f *tcpGraphFixture) { f.inventory[tcpListener]["default_pool_id"] = "unproven" }},
		{name: "foreign project", wantErr: cloud.ErrOwnershipConflict, corrupt: func(f *tcpGraphFixture) { f.inventory[tcpMember]["project_id"] = "other-project" }},
		{name: "stale route UID", wantErr: cloud.ErrOwnershipConflict, corrupt: func(f *tcpGraphFixture) { f.spec.Routes[0].Spec.Identity.RouteUID = "new-route-uid" }},
		{name: "unlisted member", wantErr: cloud.ErrDependencyNotReady, corrupt: func(f *tcpGraphFixture) {
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
				if !errors.Is(err, scenario.wantErr) || len(f.writes) != 6 {
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

func TestDefaultPoolRouteCleanupAfterListenerDisappears(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol, func(t *testing.T) { testDefaultPoolRouteCleanupAfterListenerDisappears(t, protocol) })
	}
}

func testDefaultPoolRouteCleanupAfterListenerDisappears(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
	provider := f.provider()
	for range 5 {
		f.reconcile(provider)
	}
	// The listener disappeared after the backend objects were created. On
	// restart only the durably detaching route can identify its transport.
	delete(f.inventory, tcpListener)
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	node := &f.spec.Routes[0]
	node.Disposition = cloud.GraphDelete
	node.Spec = cloud.RouteSpec{Identity: node.Spec.Identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: protocol}}
	provider = f.provider()
	for range 3 {
		f.reconcile(provider)
	}
	result := f.reconcile(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || !slices.Equal(f.writes[5:], []string{"DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool}) || f.inventory[tcpLB] == nil {
		t.Fatalf("route cleanup after listener loss = %#v, writes %v", result, f.writes)
	}
}

func TestUDPGraphRejectsUnprovenPoolAndConflictingTransports(t *testing.T) {
	for _, test := range []struct {
		name    string
		corrupt func(*tcpGraphFixture)
	}{
		{"TCP pool", func(f *tcpGraphFixture) { f.inventory[tcpPool]["protocol"] = "TCP" }},
		{"foreign listener", func(f *tcpGraphFixture) { f.inventory[tcpPool]["listeners"] = []map[string]any{{"id": "foreign"}} }},
		{"TCP listener", func(f *tcpGraphFixture) { f.inventory[tcpListener]["protocol"] = "TCP" }},
		{"conflicting detachment", func(f *tcpGraphFixture) {
			delete(f.inventory, tcpListener)
			f.inventory[tcpPool]["listeners"] = nil
			f.spec.Routes[0].Disposition = cloud.GraphDelete
			identity := f.spec.Routes[0].Spec.Identity
			identity.RouteName, identity.RouteUID = "other-route", "other-route-uid"
			f.spec.Routes = append(f.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{
				Identity: identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: "TCP"},
			}})
			f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTransportGraphFixture(t, "UDP")
			provider := f.provider()
			for range 6 {
				f.reconcile(provider)
			}
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
			test.corrupt(f)
			if _, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != 6 {
				t.Fatalf("unproven UDP graph = %v, writes %v", err, f.writes)
			}
		})
	}
}

func TestUDPRouteCleanupAfterListenerReplacement(t *testing.T) {
	for _, protocol := range []string{"TCP", "HTTP"} {
		t.Run(protocol, func(t *testing.T) {
			f := newTransportGraphFixture(t, "UDP")
			for range 5 {
				f.reconcile(f.provider())
			}
			f.inventory[tcpListener]["protocol"] = protocol
			f.spec.Gateway.Spec.ListenerProtocol = protocol
			f.spec.Routes[0].Disposition = cloud.GraphDelete
			replacement := f.inventory[tcpListener]
			f.inventory[tcpPool]["listeners"] = []map[string]any{{"id": "listener"}}
			replacement["default_pool_id"] = "pool"
			if _, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != 5 {
				t.Fatalf("replacement listener has a cross-protocol relationship: %v, writes %v", err, f.writes[5:])
			}
			replacement["default_pool_id"] = nil
			f.inventory[tcpPool]["listeners"] = nil
			if protocol == "HTTP" {
				replacement["l7policies"] = []map[string]any{{"id": "unlisted"}}
				if _, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrDependencyNotReady) || len(f.writes) != 5 {
					t.Fatalf("replacement listener still references an unlisted L7 policy: %v, writes %v", err, f.writes[5:])
				}
				delete(replacement, "l7policies")
			}
			if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("cleanup under ensured replacement gateway = %#v", result)
			}
			f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
			for range 2 {
				f.reconcile(f.provider())
			}
			want := []string{"DELETE " + tcpMonitor, "DELETE " + tcpMember, "DELETE " + tcpPool}
			if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady ||
				!slices.Equal(f.writes[5:], want) || f.inventory[tcpListener]["protocol"] != protocol {
				t.Fatalf("cleanup leaves replacement listener in place: %#v, writes %v", result, f.writes[5:])
			}
		})
	}
}

func TestUDPRouteCleanupPreservesReplacementBackend(t *testing.T) {
	f := newTransportGraphFixture(t, "TCP")
	for range 6 {
		f.reconcile(f.provider())
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	oldRoute := f.spec.Routes[0].Spec.Identity
	oldRoute.RouteName, oldRoute.RouteUID = "old-route", "old-route-uid"
	f.spec.Routes = append(f.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{
		Identity: oldRoute, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: "UDP"},
	}})
	for _, disposition := range []cloud.GraphDisposition{cloud.GraphEnsure, cloud.GraphPreserve} {
		f.spec.Gateway.Disposition = disposition
		if disposition == cloud.GraphPreserve {
			f.spec.Routes[0].Disposition = cloud.GraphPreserve
		}
		result := f.reconcile(f.provider())
		if result.Outcome.State != cloud.OutcomeReady || len(f.writes) != 6 || f.inventory[tcpListener]["default_pool_id"] != "pool" {
			t.Fatalf("cleanup changed the serving replacement: %#v, writes %v", result, f.writes[6:])
		}
		var complete bool
		for _, route := range result.Routes {
			if route.Identity == oldRoute {
				complete = route.Result.Outcome.State == cloud.OutcomeReady
			}
		}
		if !complete {
			t.Fatalf("old route did not complete: %#v", result.Routes)
		}
	}
}

func TestUDPGraphDetachesWithoutMembersInTheVIPFamily(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	provider := f.provider()
	for range 6 {
		f.reconcile(provider)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.spec.Routes[0].Spec.Members[0].Address = "2001:db8::44"
	f.inventory[tcpMonitor]["type"] = "TCP"
	if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) || f.inventory[tcpListener]["default_pool_id"] != nil {
		t.Fatalf("no same-family backend, result %#v, writes %v", result, f.writes[6:])
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.inventory[tcpMonitor]["type"] = "UDP-CONNECT"
	f.reconcile(f.provider())
	if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[7:], []string{"DELETE " + tcpMember}) || f.inventory[tcpListener]["default_pool_id"] != nil {
		t.Fatalf("an IPv6 member behind an IPv4 VIP was accepted: %#v, writes %v", result, f.writes[7:])
	}
}

func TestTransportGraphReconcilesEndpointWeightThroughThePublicAPI(t *testing.T) {
	for _, protocol := range []string{cloud.ListenerProtocolTCP, cloud.ListenerProtocolUDP} {
		t.Run(protocol, func(t *testing.T) {
			testTransportGraphReconcilesEndpointWeight(t, protocol)
		})
	}
}

func testTransportGraphReconcilesEndpointWeight(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
	route := &f.spec.Routes[0].Spec
	route.MemberMode = cloud.MemberModeEndpoint
	member := &route.Members[0]
	member.Weight = 256
	for range 6 {
		f.reconcile(f.provider())
	}
	observed := f.inventory[tcpMember]
	if observed["weight"] != float64(256) {
		t.Fatalf("created member weight = %v, want 256", observed["weight"])
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	assertWriteFree := func(want cloud.OutcomeState) {
		t.Helper()
		before := len(f.writes)
		if result := f.reconcile(f.provider()); result.Outcome.State != want || len(f.writes) != before {
			t.Fatalf("rediscovery = %v, writes %v, want %v without writes", result.Outcome.State, f.writes[before:], want)
		}
	}
	assertWriteFree(cloud.OutcomeReady)
	for _, change := range []struct {
		name          string
		desiredWeight int
		observedDrift bool
		want          float64
	}{
		{name: "change weight", desiredWeight: 7, want: 7},
		{name: "recover drift at zero", desiredWeight: 7, observedDrift: true, want: 7},
		{name: "remove explicit weight", want: 1},
	} {
		member.Weight = change.desiredWeight
		if change.observedDrift {
			observed["weight"] = float64(0)
		}
		before := len(f.writes)
		f.reconcile(f.provider())
		if f.inventory[tcpListener]["default_pool_id"] != nil || !slices.Equal(f.writes[before:], []string{"PUT " + tcpListener}) {
			t.Fatalf("%s: serving pool was not detached before weight update, writes %v", change.name, f.writes[before:])
		}
		assertWriteFree(cloud.OutcomeProgressing)
		f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
		f.reconcile(f.provider())
		if observed["weight"] != change.want || f.inventory[tcpListener]["default_pool_id"] != nil {
			t.Fatalf("%s: detached member weight = %v, want %v", change.name, observed["weight"], change.want)
		}
		observed["provisioning_status"] = "ACTIVE"
		assertWriteFree(cloud.OutcomeProgressing)
		f.inventory[tcpPool]["provisioning_status"] = "ACTIVE"
		f.reconcile(f.provider())
		f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
		assertWriteFree(cloud.OutcomeReady)
		want := []string{"PUT " + tcpListener, "PUT " + tcpMember, "PUT " + tcpListener}
		if !slices.Equal(f.writes[before:], want) || f.inventory[tcpListener]["default_pool_id"] != "pool" {
			t.Fatalf("%s: weight transition writes = %v, want %v and an attached pool", change.name, f.writes[before:], want)
		}
	}
	member.Weight, member.Draining = 7, true
	before := len(f.writes)
	f.reconcile(f.provider())
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.reconcile(f.provider())
	observed["provisioning_status"], f.inventory[tcpPool]["provisioning_status"] = "ACTIVE", "ACTIVE"
	assertWriteFree(cloud.OutcomeProgressing)
	if observed["weight"] != float64(0) || f.inventory[tcpListener]["default_pool_id"] != nil ||
		!slices.Equal(f.writes[before:], []string{"PUT " + tcpListener, "PUT " + tcpMember}) {
		t.Fatalf("draining member weight = %v, default pool = %v, writes %v", observed["weight"], f.inventory[tcpListener]["default_pool_id"], f.writes[before:])
	}
}

func TestTransportGraphRepairsBackupOnlyForWeightedTCPEndpoints(t *testing.T) {
	for _, test := range []struct {
		name       string
		protocol   string
		weights    [2]int
		wantRepair bool
	}{
		{"weighted TCP", cloud.ListenerProtocolTCP, [2]int{7, 3}, true},
		{"unweighted TCP", cloud.ListenerProtocolTCP, [2]int{0, 0}, false},
		{"weighted UDP", cloud.ListenerProtocolUDP, [2]int{7, 3}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTransportGraphFixture(t, test.protocol)
			route := &f.spec.Routes[0].Spec
			route.MemberMode = cloud.MemberModeEndpoint
			route.Members[0].Weight = test.weights[0]
			for range 6 {
				f.reconcile(f.provider())
			}
			listener, pool := f.inventory[tcpListener], f.inventory[tcpPool]
			listener["provisioning_status"] = "ACTIVE"
			secondEndpoint := route.Members[0]
			secondEndpoint.Address, secondEndpoint.Weight = "192.0.2.180", test.weights[1]
			route.Members = append(route.Members, secondEndpoint)
			second := maps.Clone(f.inventory[tcpMember])
			second["id"], second["address"], second["weight"], second["backup"] = "member-two", secondEndpoint.Address, float64(max(1, secondEndpoint.Weight)), true
			const secondPath = tcpMembers + "/member-two"
			f.inventory[secondPath] = second
			before := len(f.writes)
			result := f.reconcile(f.provider())
			if !test.wantRepair {
				if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before || second["backup"] != true {
					t.Fatalf("backup flag changed the existing %s contract: outcome %v, writes %v", test.name, result.Outcome.State, f.writes[before:])
				}
				second["backup"] = nil
				if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before {
					t.Fatalf("null backup changed the existing %s contract: outcome %v, writes %v", test.name, result.Outcome.State, f.writes[before:])
				}
				delete(second, "backup")
				if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before {
					t.Fatalf("omitted backup changed the existing %s contract: outcome %v, writes %v", test.name, result.Outcome.State, f.writes[before:])
				}
				return
			}
			if result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || listener["default_pool_id"] != nil ||
				!slices.Equal(f.writes[before:], []string{"PUT " + tcpListener}) {
				t.Fatalf("a backup member cannot serve the desired 70:30 share: outcome %v, pool %v, writes %v",
					result.Routes[0].Result.Outcome.State, listener["default_pool_id"], f.writes[before:])
			}
			listener["provisioning_status"] = "ACTIVE"
			f.reconcile(f.provider())
			if second["backup"] != false || second["weight"] != float64(3) || listener["default_pool_id"] != nil ||
				!slices.Equal(f.writes[before+1:], []string{"PUT " + secondPath}) {
				t.Fatalf("expected repair confined to the detached backup member: backup %v, weight %v, writes %v",
					second["backup"], second["weight"], f.writes[before+1:])
			}
			if result := f.reconcile(f.provider()); result.Outcome.State == cloud.OutcomeReady || len(f.writes) != before+2 {
				t.Fatalf("pending member update changed forwarding: outcome %v, writes %v", result.Outcome.State, f.writes[before:])
			}
			// A fresh provider must hold the pool if readback still shows backup=true.
			second["backup"], second["provisioning_status"], pool["provisioning_status"] = true, "ACTIVE", "ACTIVE"
			f.reconcile(f.provider())
			if listener["default_pool_id"] != nil || !slices.Equal(f.writes[before+2:], []string{"PUT " + secondPath}) {
				t.Fatalf("listener reattached without confirmed backup readback: writes %v", f.writes[before+2:])
			}
			second["provisioning_status"], pool["provisioning_status"] = "ACTIVE", "ACTIVE"
			for _, form := range []string{"omitted", "null"} {
				if form == "omitted" {
					delete(second, "backup")
				} else {
					second["backup"] = nil
				}
				if result := f.reconcile(f.provider()); listener["default_pool_id"] != nil ||
					result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != before+3 {
					t.Fatalf("%s backup is not confirmed: outcome %v, pool %v, writes %v",
						form, result.Outcome.State, listener["default_pool_id"], f.writes[before+3:])
				}
			}
			second["backup"] = false
			f.reconcile(f.provider())
			listener["provisioning_status"] = "ACTIVE"
			if result := f.reconcile(f.provider()); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady ||
				!slices.Equal(f.writes[before+3:], []string{"PUT " + tcpListener}) || listener["default_pool_id"] != "pool" {
				t.Fatalf("confirmed repair did not restore the serving pool: outcome %v, writes %v", result.Outcome.State, f.writes[before+3:])
			}
		})
	}
}

func TestWeightedTransportGraphClearsSourceIPPersistenceBeforeAttaching(t *testing.T) {
	for _, protocol := range []string{cloud.ListenerProtocolTCP, cloud.ListenerProtocolUDP} {
		t.Run(protocol, func(t *testing.T) {
			testWeightedTransportGraphClearsSourceIPPersistence(t, protocol)
		})
	}
}

func testWeightedTransportGraphClearsSourceIPPersistence(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
	route := &f.spec.Routes[0].Spec
	route.MemberMode = cloud.MemberModeEndpoint
	for range 6 {
		f.reconcile(f.provider())
	}
	listener, pool := f.inventory[tcpListener], f.inventory[tcpPool]
	listener["provisioning_status"] = "ACTIVE"
	sticky := map[string]any{"type": "SOURCE_IP"}
	pool["session_persistence"] = sticky
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeReady || len(f.writes) != 6 {
		t.Fatalf("legacy endpoint pool changed: %v, writes %v", result.Outcome.State, f.writes)
	}

	// An explicit weight of 1 is also used when weighted backend shares are equal.
	route.Members[0].Weight = 1
	f.reconcile(f.provider())
	if listener["default_pool_id"] != nil || !slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) {
		t.Fatalf("pool with source persistence did not detach first, writes %v", f.writes[6:])
	}
	listener["provisioning_status"] = "ACTIVE"
	f.reconcile(f.provider())
	if pool["session_persistence"] != nil || !slices.Equal(f.writes[7:], []string{"PUT " + tcpPool}) {
		t.Fatalf("pool update did not explicitly clear persistence, writes %v", f.writes[7:])
	}
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != 8 {
		t.Fatalf("pending pool update was treated as ready: %v, writes %v", result.Outcome.State, f.writes[6:])
	}
	pool["provisioning_status"], pool["session_persistence"] = "ACTIVE", sticky
	f.reconcile(f.provider())
	if listener["default_pool_id"] != nil || !slices.Equal(f.writes[8:], []string{"PUT " + tcpPool}) {
		t.Fatalf("sticky pool was reattached without clear readback, writes %v", f.writes[8:])
	}
	pool["provisioning_status"] = "ACTIVE"
	f.reconcile(f.provider())
	listener["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeReady || listener["default_pool_id"] != "pool" ||
		!slices.Equal(f.writes[9:], []string{"PUT " + tcpListener}) {
		t.Fatalf("cleared pool did not converge: %v, writes %v", result.Outcome.State, f.writes[9:])
	}
}

func TestUDPGraphBlocksMutationWhenPoolMemberInventoryDisagrees(t *testing.T) {
	for _, test := range []struct {
		name       string
		references any
		extra      bool
		wantErr    error
	}{
		{name: "omitted"},
		{name: "null", references: json.RawMessage(`null`)},
		{name: "matching", references: []map[string]string{{"id": "member"}}},
		{name: "explicit empty", references: []map[string]string{}, wantErr: cloud.ErrDependencyNotReady},
		{name: "detailed list has extra member", references: []map[string]string{{"id": "member"}}, extra: true, wantErr: cloud.ErrDependencyNotReady},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTransportGraphFixture(t, cloud.ListenerProtocolUDP)
			for range 6 {
				f.reconcile(f.provider())
			}
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
			if test.references != nil {
				f.inventory[tcpPool]["members"] = test.references
			}
			if test.extra {
				extra := maps.Clone(f.inventory[tcpMember])
				extra["id"], extra["address"] = "extra", "10.0.0.11"
				f.extraListedMembers = []map[string]any{extra}
			}
			// The desired destination changed and would cause a detach if observation were trusted.
			f.spec.Routes[0].Spec.Members[0].Port++
			result, err := f.provider().ReconcileGatewayGraph(t.Context(), f.spec)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) || len(f.writes) != 6 {
					t.Fatalf("inconsistent inventory = %v, writes %v", err, f.writes[6:])
				}
				return
			}
			if err != nil || result.Outcome.State != cloud.OutcomeProgressing || !slices.Equal(f.writes[6:], []string{"PUT " + tcpListener}) {
				t.Fatalf("valid inventory = %v, %v, writes %v", result.Outcome.State, err, f.writes[6:])
			}
		})
	}
}

func TestUDPGraphReportsProviderMonitorDelayRejection(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	route := &f.spec.Routes[0].Spec
	route.HealthMonitor.Delay, route.HealthMonitor.Timeout = 7, 2
	f.rejectMonitorDelay = 7
	provider := f.provider()
	for range 4 {
		f.reconcile(provider)
	}
	result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
	var component *cloud.GraphError
	if !errors.Is(err, cloud.ErrTerminalValidation) || !errors.As(err, &component) || component.Identity != route.Identity ||
		result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || f.inventory[tcpListener]["default_pool_id"] != nil ||
		!slices.Equal(f.writes[4:], []string{"POST " + tcpMonitors}) {
		t.Fatalf("rejected UDP monitor = %v, result %#v, writes %v", err, result, f.writes)
	}
}

func TestDefaultPoolListenerErrorFailsEnsureAndAllowsOwnedRevocation(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol, func(t *testing.T) { testDefaultPoolListenerErrorFailsEnsureAndAllowsOwnedRevocation(t, protocol) })
	}
}

func testDefaultPoolListenerErrorFailsEnsureAndAllowsOwnedRevocation(t *testing.T, protocol string) {
	f := newTransportGraphFixture(t, protocol)
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
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.GatewaySpec{ListenerProtocol: protocol}}
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

func TestMultiTCPGraphKeepsSharedPoolOnListenerRemoval(t *testing.T) {
	f := newTCPGraphFixture(t)
	provider := f.provider()
	f.settleTransportGraph(provider)

	anchor := f.spec.Gateway.Spec.ListenerPort
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "one", Port: anchor}, {Name: "two", Port: 9311}, {Name: "three", Port: 9312}, {Name: "four", Port: 9313},
	}
	f.spec.Routes[0].Spec.ScopedListenerPorts = []int{anchor, 9311, 9312, 9313}
	before := len(f.writes)
	f.settleTransportGraph(provider)
	if slices.Contains(f.writes[before:], "PUT "+tcpListener) || slices.Contains(f.writes[before:], "DELETE "+tcpListener) {
		t.Fatalf("enrolling TCP siblings changed the original listener: %v", f.writes[before:])
	}
	if len(f.children(tcpListeners)) != 4 {
		t.Fatalf("expected four TCP listeners, got %d", len(f.children(tcpListeners)))
	}
	for _, listener := range f.children(tcpListeners) {
		if listener["default_pool_id"] != "pool" {
			t.Fatalf("listener %s does not reach the shared TCP pool", listener["id"])
		}
	}

	// A stale reverse association must stop frontend deletion, even after
	// the listener no longer reports a default pool.
	f.spec.Gateway.Spec.ScopedListeners = f.spec.Gateway.Spec.ScopedListeners[:3]
	f.spec.Routes[0].Spec.ScopedListenerPorts = f.spec.Routes[0].Spec.ScopedListenerPorts[:3]
	removed := tcpListener + "-9313"
	f.inventory[removed]["default_pool_id"] = nil
	before = len(f.writes)
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrDependencyNotReady) || len(f.writes) != before {
		t.Fatalf("mismatched readback authorized a write: %v, writes %v", err, f.writes[before:])
	}

	f.inventory[removed]["default_pool_id"] = "pool"
	f.settleTransportGraph(provider)
	if want := []string{"PUT " + removed, "DELETE " + removed}; !slices.Equal(f.writes[before:], want) {
		t.Fatalf("removing one TCP listener changed sibling traffic: writes %v, want %v", f.writes[before:], want)
	}
	for _, listener := range f.children(tcpListeners) {
		if listener["default_pool_id"] != "pool" {
			t.Fatalf("sibling listener %s lost the shared pool", listener["id"])
		}
	}
}
