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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gophercloud/gophercloud/v2"
)

func TestMultiUDPDeferredObservationRequiresServingRouteEdges(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	ports := []int{f.spec.Gateway.Spec.ListenerPort, 5353}
	f.spec.Routes[0].Spec.ScopedListenerPorts = ports
	provider := f.provider()
	f.settleTransportGraph(provider)
	writes := len(f.writes)

	sibling := f.inventory[tcpListener+"-1812"]
	sibling["provisioning_status"] = "PENDING_UPDATE"
	result := f.reconcile(provider)
	if len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		!result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("pending unrelated sibling deferred no proven UDP route: %+v", result.Routes)
	}

	pool := f.inventory[tcpPool]
	pool["provisioning_status"] = "ERROR"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("failed pool deferred a UDP route with obsolete forwarding proof: %+v", result.Routes)
	}
	pool["provisioning_status"] = "ACTIVE"

	member := f.inventory[tcpMember]
	member["provisioning_status"] = "ERROR"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("failed member deferred a UDP route with obsolete forwarding proof: %+v", result.Routes)
	}
	member["provisioning_status"] = "ACTIVE"
	port := member["protocol_port"]
	member["protocol_port"] = float64(30081)
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatal("retained forwarding status after the observed member moved to another port")
	}
	member["protocol_port"] = port

	// A formerly winning port remains attached to the pool until the graph
	// can perform the withdrawal. That stale edge cannot support deferral.
	f.spec.Routes[0].Spec.ScopedListenerPorts = ports[:1]
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("superseded default deferred a UDP route: %+v", result.Routes)
	}
	f.spec.Routes[0].Spec.ScopedListenerPorts = ports

	winning := f.inventory[tcpListener+"-5353"]
	winning["provisioning_status"] = "PENDING_UPDATE"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("pending winning listener deferred a UDP route: %+v", result.Routes)
	}
	winning["provisioning_status"] = "ACTIVE"

	sibling["provisioning_status"] = "ERROR"
	result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err == nil || result.Gateway.Outcome.State != cloud.OutcomeProgressing || !slices.Equal(result.Gateway.ScopedListenerReadyPorts, ports) ||
		result.Gateway.State.Address() == "" || len(result.Routes) != 0 || len(f.writes) != writes {
		t.Fatalf("UDP sibling ERROR returned result %+v, error %v, writes %v", result, err, f.writes[writes:])
	}
}

func newUDPPacketDropFixture(t *testing.T) *tcpGraphFixture {
	t.Helper()
	f := newTransportGraphFixture(t, cloud.ListenerProtocolUDP)
	f.udpDropAPI, f.udpDropAdvertised = true, true
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "affected", Port: f.spec.Gateway.Spec.ListenerPort}, {Name: "independent", Port: 9311},
	}
	first := &f.spec.Routes[0]
	first.Spec.ScopedListenerPorts = []int{f.spec.Gateway.Spec.ListenerPort}
	first.Spec.MemberMode = cloud.MemberModeEndpoint
	first.Spec.Members = []cloud.Member{{Address: "10.1.2.3", Port: 8080, Weight: 3}, {Address: "10.1.2.4", Port: 8080, Weight: 1}}
	second := *first
	second.Spec.Identity.RouteName, second.Spec.Identity.RouteUID = "independent", "independent-uid"
	second.Spec.Precedence++
	second.Spec.ScopedListenerPorts = []int{9311}
	second.Spec.Members = []cloud.Member{{Address: "10.1.2.5", Port: 8080, Weight: 1}}
	f.spec.Routes = append(f.spec.Routes, second)
	return f
}

func newUDPPacketDropProvider(f *tcpGraphFixture) *Provider {
	return newTestProvider(f.t, hibiscusServiceClients(http.HandlerFunc(f.serveHTTP)), ProviderConfig{UDPPacketDrop: true})
}

func newScopedUDPPacketDropGraph(t *testing.T, guarded bool) *tcpGraphFixture {
	t.Helper()
	f := newUDPPacketDropFixture(t)
	f.distinctUDPPoolIDs = true
	f.spec.Gateway.Spec.ScopedListeners = append(f.spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "additional", Port: 9353})
	route := &f.spec.Routes[0].Spec
	route.ScopedListenerPorts = append(route.ScopedListenerPorts, 9353)
	slices.Sort(route.ScopedListenerPorts)
	if !guarded {
		f.settleTransportGraph(newUDPPacketDropProvider(f))
		return f
	}
	route.UDPPacketDrop, route.HealthMonitor = &cloud.UDPPacketDrop{Denominator: 1}, cloud.HealthMonitorConfig{}
	settleScopedUDPPacketDropGraph(t, f)
	return f
}

func settleScopedUDPPacketDropGraph(t *testing.T, f *tcpGraphFixture) {
	t.Helper()
	provider := newUDPPacketDropProvider(f)
	for range 20 {
		for _, item := range append(f.children(tcpListeners), f.children(tcpPools)...) {
			if item["provisioning_status"] == "PENDING_UPDATE" {
				item["provisioning_status"] = "ACTIVE"
			}
		}
		if result := f.reconcile(provider); result.Outcome.State == cloud.OutcomeReady {
			return
		}
	}
	t.Fatalf("UDP graph did not settle: %v", f.writes)
}

func TestUDPPacketDropWithdrawsAnOrdinaryPeersSupersededDefault(t *testing.T) {
	f := newScopedUDPPacketDropGraph(t, true)
	primary, additional := f.inventory[tcpListener], f.inventory[tcpListener+"-9353"]
	control := f.inventory[tcpListener+"-9311"]
	primaryID, additionalID := primary["default_pool_id"], additional["default_pool_id"]
	peer := &f.spec.Routes[1]
	peer.Disposition, peer.RetainListenerPorts = cloud.GraphPreserve, []int{}
	peer.Spec = cloud.RouteSpec{Identity: peer.Spec.Identity}
	before := len(f.writes)
	f.reconcile(newUDPPacketDropProvider(f))
	if !slices.Equal(f.writes[before:], []string{"PUT " + tcpListener + "-9311"}) || control["default_pool_id"] != nil ||
		primary["default_pool_id"] != primaryID || additional["default_pool_id"] != additionalID {
		t.Fatalf("withdrawal changed the wrong graph edges: %v", f.writes[before:])
	}
}

func TestUDPPacketDropRestrictsAnExistingPoolBeforeRouteCheckpoint(t *testing.T) {
	f := newScopedUDPPacketDropGraph(t, true)
	node := &f.spec.Routes[0]
	route := &node.Spec
	granted := slices.Clone(route.Members)
	primary, additional, sibling := f.inventory[tcpListener], f.inventory[tcpListener+"-9353"], f.inventory[tcpListener+"-9311"]
	primaryID, additionalID, siblingID := primary["default_pool_id"], additional["default_pool_id"], sibling["default_pool_id"]
	paths := []string{tcpPools + "/" + primaryID.(string), tcpPools + "/" + additionalID.(string)}
	node.Disposition, node.RetainListenerPorts = cloud.GraphPreserve, slices.Clone(route.ScopedListenerPorts)
	node.RestrictUDPPacketDrop = true
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	retained := route.Members[0]
	retained.Weight = 1
	route.Members = []cloud.Member{retained}
	before := len(f.writes)
	provider := newUDPPacketDropProvider(f)
	fenceErr := errors.New("authorization changed")
	f.spec.ValidateMutation = func(context.Context) error { return fenceErr }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, fenceErr) || len(f.writes) != before {
		t.Fatalf("the uncheckpointed restriction bypassed the authorization fence: %v, writes %v", err, f.writes[before:])
	}
	f.spec.ValidateMutation = nil
	for _, path := range paths {
		f.reconcile(provider)
		if pool := f.inventory[path]; pool != nil {
			pool["provisioning_status"] = "ACTIVE"
		}
	}
	want := []string{"PUT " + paths[0] + "/members", "PUT " + paths[1] + "/members"}
	if !slices.Equal(f.writes[before:], want) || primary["default_pool_id"] != primaryID ||
		additional["default_pool_id"] != additionalID || sibling["default_pool_id"] != siblingID {
		t.Fatalf("withdrawal before the route checkpoint did not batch both guarded pools: %v", f.writes[before:])
	}
	for _, path := range paths {
		members := f.children(path + "/members")
		if len(members) != 1 || members[0]["id"] != "member" || members[0]["address"] != retained.Address {
			t.Fatalf("retained member changed identity or an unauthorized member still serves: %v", members)
		}
	}
	route.UDPPacketDrop, route.Members = &cloud.UDPPacketDrop{Denominator: 1}, granted
	before = len(f.writes)
	f.reconcile(provider)
	if len(f.writes) != before || primary["default_pool_id"] != primaryID || additional["default_pool_id"] != additionalID ||
		sibling["default_pool_id"] != siblingID {
		t.Fatalf("grant restoration changed forwarding before the route checkpoint: %v", f.writes[before:])
	}
	node.Disposition, node.RetainListenerPorts = cloud.GraphEnsure, nil
	node.RestrictUDPPacketDrop = false
	settleScopedUDPPacketDropGraph(t, f)
	if !slices.Equal(f.writes[before:], want) || primary["default_pool_id"] != primaryID || additional["default_pool_id"] != additionalID {
		t.Fatalf("route checkpoint did not restore both guarded pools: %v", f.writes[before:])
	}

	// The same snapshot can revoke an active member while granting access to a
	// new one. The gateway must stop the revoked member without admitting the new one.
	node.Disposition, node.RetainListenerPorts, node.RestrictUDPPacketDrop = cloud.GraphPreserve, slices.Clone(route.ScopedListenerPorts), true
	route.Members = []cloud.Member{retained, {Address: "10.1.2.6", Port: 8080, Weight: 1}}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	before = len(f.writes)
	f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"DELETE " + paths[0]}) || additional["default_pool_id"] != additionalID ||
		sibling["default_pool_id"] != siblingID {
		t.Fatalf("a mixed authorization change mutated the wrong pool: %v", f.writes[before:])
	}
}

func TestUDPPacketDropKeepsSharedStockPoolAfterFailedDetach(t *testing.T) {
	f := newScopedUDPPacketDropGraph(t, false)
	node := &f.spec.Routes[0]
	route := &node.Spec
	first, second, control := f.inventory[tcpListener], f.inventory[tcpListener+"-9353"], f.inventory[tcpListener+"-9311"]
	stockID, controlID := first["default_pool_id"], control["default_pool_id"]
	route.ScopedListenerPorts = []int{9353}
	route.UDPPacketDrop, route.HealthMonitor = &cloud.UDPPacketDrop{Denominator: 1}, cloud.HealthMonitorConfig{}
	f.reconcile(newUDPPacketDropProvider(f))
	if first["default_pool_id"] != nil || second["default_pool_id"] != stockID {
		t.Fatal("migration did not start by detaching the former winning port")
	}
	first["provisioning_status"] = "ERROR"
	before := len(f.writes)
	f.reconcile(newUDPPacketDropProvider(f))
	if len(f.writes) != before || f.inventory[tcpPool] == nil || second["default_pool_id"] != stockID || control["default_pool_id"] != controlID {
		t.Fatalf("an unverified detach let the shared stock pool change: %v", f.writes[before:])
	}
	first["provisioning_status"] = "ACTIVE"
	settleScopedUDPPacketDropGraph(t, f)
	if first["default_pool_id"] != nil || second["default_pool_id"] == nil || second["default_pool_id"] == stockID || control["default_pool_id"] != controlID {
		t.Fatalf("migration did not recover after the old listener settled: %v", f.writes[before:])
	}
}

func TestUDPPacketDropDefersOnlyAfterReadbackOfEveryWinningPort(t *testing.T) {
	f := newScopedUDPPacketDropGraph(t, true)
	f.inventory[tcpLB]["provisioning_status"] = "PENDING_UPDATE"
	f.inventory[tcpListener+"-9311"]["provisioning_status"] = "PENDING_UPDATE"
	provider := newUDPPacketDropProvider(f)
	before := len(f.writes)
	if result := f.reconcile(provider); !result.Routes[0].Result.ObservationDeferred ||
		result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || len(f.writes) != before {
		t.Fatalf("the pending sibling did not retain the established route observation: %+v", result.Routes)
	}
	additionalID := f.inventory[tcpListener+"-9353"]["default_pool_id"].(string)
	f.inventory[tcpPools+"/"+additionalID]["udp_packet_drop"] = map[string]any{"numerator": 1, "denominator": 4}
	if result := f.reconcile(provider); result.Routes[0].Result.ObservationDeferred || len(f.writes) != before {
		t.Fatalf("one drifted pool incorrectly deferred the route observation: %+v", result.Routes)
	}
}

func TestUDPPacketDropUsesAuthorizedNodePorts(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	f.spec.Routes[0].Spec.MemberMode = "" // The default normalizes to NodePort.
	f.spec.Routes[0].Spec.Members = []cloud.Member{
		{Address: "192.0.2.44", Port: 32000, Weight: 3},
		{Address: "192.0.2.44", Port: 32001, Weight: 1},
	}
	f.spec.Routes[1].Spec.MemberMode = cloud.MemberModeNodePort
	f.spec.Routes[1].Spec.Members = []cloud.Member{{Address: "192.0.2.44", Port: 32002, Weight: 1}}
	testUDPPacketDropAtomicReplacement(t, f)
}

func TestUDPPacketDropRejectsIneligibleNodePortAddresses(t *testing.T) {
	for _, address := range []string{"2001:db8::1", "::ffff:192.0.2.44", "127.0.0.1"} {
		t.Run(address, func(t *testing.T) {
			f := newUDPPacketDropFixture(t)
			route := &f.spec.Routes[0].Spec
			route.MemberMode = cloud.MemberModeNodePort
			route.HealthMonitor = cloud.HealthMonitorConfig{}
			route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
			route.Members = []cloud.Member{{Address: address, Port: 32000}}
			client := safetyServiceClient(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				t.Errorf("cloud request for ineligible UDP member: %s %s", request.Method, request.URL)
			}))
			provider := newTestProvider(t, ServiceClients{LoadBalancer: client, Network: client, ProjectID: hibiscusProject}, ProviderConfig{UDPPacketDrop: true})
			if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrTerminalValidation) {
				t.Fatalf("error = %v, want terminal validation", err)
			}
		})
	}
}

func TestUDPPacketDropRetainsOnlyTheObservedWeightedPool(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	provider := newUDPPacketDropProvider(f)
	f.settleTransportGraph(provider)
	node := &f.spec.Routes[0]
	node.Spec.HealthMonitor = cloud.HealthMonitorConfig{}
	node.Spec.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	node.Spec.Members = []cloud.Member{{Address: "10.1.2.3", Port: 8080, Weight: 1}}
	for range 4 {
		f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
		f.reconcile(provider)
	}
	pool := f.inventory[tcpPool]
	pool["provisioning_status"] = "ACTIVE"
	f.settleTransportGraph(provider)

	node.Disposition = cloud.GraphPreserve
	node.RetainListenerPorts = node.Spec.ScopedListenerPorts
	before := len(f.writes)
	f.reconcile(provider)
	if len(f.writes) != before || f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("the active weighted UDP pool was not retained: writes %v", f.writes[before:])
	}
	pool["udp_packet_drop"] = map[string]any{"numerator": 0, "denominator": 1}
	f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) ||
		f.inventory[tcpListener]["default_pool_id"] != nil || f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("the drifted drop profile did not withdraw only its own pool: writes %v", f.writes[before:])
	}
}

func TestUDPPacketDropAtomicallyReplacesAServingPoolAndRollsBack(t *testing.T) {
	testUDPPacketDropAtomicReplacement(t, newUDPPacketDropFixture(t))
}

func TestUDPPacketDropOneRouteOnTwoPorts(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	f.distinctUDPPoolIDs = true
	const additional = 9353
	f.spec.Gateway.Spec.ScopedListeners = append(f.spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "additional", Port: additional})
	route := &f.spec.Routes[0].Spec
	route.ScopedListenerPorts = append(route.ScopedListenerPorts, additional)
	slices.Sort(route.ScopedListenerPorts)
	provider := newUDPPacketDropProvider(f)
	f.settleTransportGraph(provider)
	first, second, control := f.inventory[tcpListener], f.inventory[tcpListener+"-9353"], f.inventory[tcpListener+"-9311"]
	stock := first["default_pool_id"]
	if stock == nil || second["default_pool_id"] != stock || control["default_pool_id"] == stock {
		t.Fatalf("expected a stock pool shared by the route's two ports and an independent sibling")
	}
	stockMonitor, granted := route.HealthMonitor, slices.Clone(route.Members)
	controlID := control["default_pool_id"]
	checkListeners := func() {
		t.Helper()
		if f.inventory[tcpListener]["id"] != first["id"] || f.inventory[tcpListener+"-9353"]["id"] != second["id"] ||
			control["default_pool_id"] != controlID || control["provisioning_status"] != "ACTIVE" {
			t.Fatal("the migration changed a listener identity or the independent sibling")
		}
	}
	settle := func(provider *Provider) {
		t.Helper()
		for range 20 {
			for _, item := range append(f.children(tcpListeners), f.children(tcpPools)...) {
				if item["provisioning_status"] == "PENDING_UPDATE" {
					item["provisioning_status"] = "ACTIVE"
				}
			}
			result := f.reconcile(provider)
			checkListeners()
			if len(result.Routes) != 2 || result.Routes[0].Identity != route.Identity {
				t.Fatalf("physical pool parts must have one route result: %+v", result.Routes)
			}
			if result.Routes[0].Result.Outcome.State == cloud.OutcomeReady {
				return
			}
		}
		t.Fatalf("UDP route did not settle, writes %v", f.writes)
	}
	route.UDPPacketDrop, route.HealthMonitor = &cloud.UDPPacketDrop{Denominator: 1}, cloud.HealthMonitorConfig{}
	before := len(f.writes)
	settle(provider)
	wantDetach := []string{"PUT " + tcpListener, "PUT " + tcpListener + "-9353", "DELETE " + tcpPool}
	if got := f.writes[before:]; !slices.Equal(got[:min(len(wantDetach), len(got))], wantDetach) {
		t.Fatalf("shared stock defaults must detach before pool replacement: %v", got)
	}
	firstID, secondID := first["default_pool_id"], second["default_pool_id"]
	if firstID == nil || secondID == nil || firstID == secondID || firstID == controlID || secondID == controlID {
		t.Fatalf("expected two dedicated pool IDs and an independent sibling: %v, %v, %v", firstID, secondID, controlID)
	}
	paths := []string{tcpPools + "/" + firstID.(string), tcpPools + "/" + secondID.(string)}
	retained := granted[0]
	retained.Weight = 1
	for _, phase := range []struct {
		share   cloud.UDPPacketDrop
		members []cloud.Member
	}{
		{share: cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, members: []cloud.Member{retained}},
		{share: cloud.UDPPacketDrop{Denominator: 1}, members: granted},
	} {
		route.UDPPacketDrop, route.Members = &phase.share, phase.members
		before := len(f.writes)
		provider = newUDPPacketDropProvider(f)
		if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || result.Routes[0].Result.ObservationDeferred {
			t.Fatalf("reported convergence with only the first port updated: %+v", result.Routes[0])
		}
		settle(provider)
		writes := slices.Clone(f.writes[before:])
		slices.Sort(writes)
		want := []string{"PUT " + paths[0] + "/members", "PUT " + paths[1] + "/members"}
		slices.Sort(want)
		if !slices.Equal(writes, want) || first["default_pool_id"] != firstID || second["default_pool_id"] != secondID {
			t.Fatalf("wanted one atomic batch per unchanged default: writes %v", writes)
		}
		for _, path := range paths {
			pool := f.inventory[path]
			fraction, _ := pool["udp_packet_drop"].(map[string]any)
			members := f.children(path + "/members")
			if fraction["numerator"] != float64(phase.share.Numerator) || fraction["denominator"] != float64(phase.share.Denominator) || len(members) != len(phase.members) {
				t.Fatalf("pool %s readback is %v with members %v", path, fraction, members)
			}
			for _, member := range phase.members {
				if !slices.ContainsFunc(members, func(item map[string]any) bool {
					return item["address"] == member.Address && item["weight"] == float64(member.Weight)
				}) {
					t.Fatalf("pool %s missing member %+v", path, member)
				}
			}
		}
	}
	// A rejected batch on the second port must withdraw that pool only.
	share := cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	route.UDPPacketDrop, route.Members = &share, []cloud.Member{retained}
	f.reconcile(provider)
	f.inventory[paths[0]]["provisioning_status"] = "ACTIVE"
	f.tcpRejectionRejectCode = http.StatusBadRequest
	before = len(f.writes)
	if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[before:], []string{"DELETE " + paths[1]}) ||
		first["default_pool_id"] != firstID || second["default_pool_id"] != nil {
		t.Fatalf("rejected batch must remove the affected pool only: %+v, writes %v", result, f.writes[before:])
	}
	f.tcpRejectionRejectCode = 0
	route.UDPPacketDrop, route.Members = &cloud.UDPPacketDrop{Denominator: 1}, granted
	settle(newUDPPacketDropProvider(f))
	if first["default_pool_id"] != firstID || second["default_pool_id"] == secondID || second["default_pool_id"] == nil {
		t.Fatal("the affected port did not recover with a replacement pool")
	}
	paths[1] = tcpPools + "/" + second["default_pool_id"].(string)

	// The same route can lose one port at the election without changing the
	// surviving default or the independent route, and win it again later.
	ports := route.ScopedListenerPorts
	route.ScopedListenerPorts = ports[:1]
	before = len(f.writes)
	settle(provider)
	if !slices.Equal(f.writes[before:], []string{"DELETE " + paths[1]}) ||
		first["default_pool_id"] != firstID || second["default_pool_id"] != nil {
		t.Fatalf("election withdrawal touched more than its port: %v", f.writes[before:])
	}
	route.ScopedListenerPorts = ports
	settle(newUDPPacketDropProvider(f))
	if first["default_pool_id"] != firstID || second["default_pool_id"] == nil {
		t.Fatal("adding an elected port did not keep the surviving pool")
	}
	paths[1] = tcpPools + "/" + second["default_pool_id"].(string)

	route.UDPPacketDrop, route.HealthMonitor = nil, stockMonitor
	settle(f.provider())
	if first["default_pool_id"] == nil || first["default_pool_id"] != second["default_pool_id"] || f.inventory[tcpMonitor] == nil ||
		f.inventory[paths[0]] != nil || f.inventory[paths[1]] != nil {
		t.Fatal("disabling the feature did not restore the shared stock pool and remove its dedicated predecessors")
	}
}

func TestUDPPacketDropAdoptsOnlyAProvenLegacyDefault(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	f.distinctUDPPoolIDs = true
	const addedPort = 9353
	f.spec.Gateway.Spec.ScopedListeners = append(f.spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "additional", Port: addedPort})
	route := &f.spec.Routes[0].Spec
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	settle := func() {
		t.Helper()
		provider := newUDPPacketDropProvider(f)
		for range 20 {
			for _, item := range append(f.children(tcpPools), f.children(tcpListeners)...) {
				if item["provisioning_status"] == "PENDING_UPDATE" {
					item["provisioning_status"] = "ACTIVE"
				}
			}
			if result := f.reconcile(provider); result.Outcome.State == cloud.OutcomeReady {
				return
			}
		}
		t.Fatalf("UDP graph did not settle: %v", f.writes)
	}
	settle()

	first, second := f.inventory[tcpListener], f.inventory[tcpListener+"-9353"]
	poolID := first["default_pool_id"]
	path := tcpPools + "/" + poolID.(string)
	identity, err := newUDPPacketDropProvider(f).identity(route.Identity)
	if err != nil {
		t.Fatal(err)
	}
	// Seed the controller's original scalar role and its original member tags.
	tags, err := identity.RouteTags(rolePool)
	if err != nil {
		t.Fatal(err)
	}
	f.inventory[path]["tags"] = append(tags, udpDropPoolTag)
	tags, err = identity.RouteTags(roleMember)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range f.children(path + "/members") {
		member["tags"] = tags
	}

	route.ScopedListenerPorts = append(route.ScopedListenerPorts, addedPort)
	slices.Sort(route.ScopedListenerPorts)
	settle()
	if first["default_pool_id"] != poolID || second["default_pool_id"] == nil || second["default_pool_id"] == poolID {
		t.Fatalf("proven legacy default was not kept on its original port: %v / %v", first, second)
	}
	witness := f.inventory[tcpListener+"-9311"]
	witness["provisioning_status"] = "PENDING_UPDATE"
	if result := f.reconcile(newUDPPacketDropProvider(f)); !result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("a proved legacy default and a port-specific default did not defer together: %+v", result.Routes[0])
	}
	witness["provisioning_status"] = "ACTIVE"

	newPool := tcpPools + "/" + second["default_pool_id"].(string)
	before := len(f.writes)
	tags = pilotTags(f.inventory[newPool]["tags"])
	wrongPortRole := udpDropResourceRole(rolePool, udpDropPartKey(route.ScopedListenerPorts[0]))
	replacement, err := identity.RouteTags(wrongPortRole)
	if err != nil {
		t.Fatal(err)
	}
	f.inventory[newPool]["tags"] = append(replacement, udpDropPoolTag)
	provider := newUDPPacketDropProvider(f)
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != before {
		t.Fatalf("wrong-port pool role allowed a mutation: %v, writes %v", err, f.writes[before:])
	}
	f.inventory[newPool]["tags"] = tags
	member := f.children(newPool + "/members")[0]
	tags = pilotTags(member["tags"])
	member["tags"] = append(slices.Clone(tags), openstackidentity.Tag("role", roleMember))
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != before {
		t.Fatalf("ambiguous descendant role allowed a mutation: %v, writes %v", err, f.writes[before:])
	}
	member["tags"] = tags

	f.udpCleanupGateway(cloud.GraphPreserve)
	f.spec.Routes[0].Disposition, f.spec.Routes[1].Disposition = cloud.GraphDelete, cloud.GraphPreserve
	settle()
	if f.inventory[path] != nil || f.inventory[newPool] != nil ||
		f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("restarted cleanup did not remove exactly the two owned pools: %v", f.writes[before:])
	}
}

func testUDPPacketDropAtomicReplacement(t *testing.T, f *tcpGraphFixture) {
	t.Helper()
	route := &f.spec.Routes[0].Spec
	provider := newUDPPacketDropProvider(f)
	f.settleTransportGraph(provider)
	anchor, sibling := f.inventory[tcpListener], f.inventory[tcpListener+"-9311"]
	checkSibling := func() {
		t.Helper()
		if sibling["default_pool_id"] != "pool-secondary" || sibling["provisioning_status"] != "ACTIVE" {
			t.Fatalf("independent UDP listener changed: %+v", sibling)
		}
	}

	stockMonitor, granted := route.HealthMonitor, slices.Clone(route.Members)
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	start := len(f.writes)
	originalTags := f.inventory[tcpMember]["tags"]
	f.inventory[tcpMember]["tags"] = []string{"operator-owned"}
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(f.writes) != start {
		t.Fatalf("migration accepted a foreign member: %v, writes %v", err, f.writes[start:])
	}
	f.inventory[tcpMember]["tags"] = originalTags
	fenceErr := errors.New("authorization changed")
	f.spec.ValidateMutation = func(context.Context) error { return fenceErr }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, fenceErr) || len(f.writes) != start {
		t.Fatalf("migration bypassed the authorization fence: %v, writes %v", err, f.writes[start:])
	}
	f.spec.ValidateMutation = nil
	f.reconcile(provider) // replace the serving stock pool
	anchor["provisioning_status"] = "ACTIVE"
	f.reconcile(provider) // create an empty, marked pool
	f.reconcile(provider) // attach its empty default
	if len(f.children(tcpMembers)) != 0 || anchor["default_pool_id"] != "pool" {
		t.Fatalf("first managed batch requires an empty default, writes %v", f.writes[start:])
	}
	anchor["provisioning_status"] = "ACTIVE"
	f.reconcile(provider) // atomically enroll and add both authorized members
	want := []string{"DELETE " + tcpPool, "POST " + tcpPools, "PUT " + tcpListener, "PUT " + tcpMembers}
	if !slices.Equal(f.writes[start:], want) {
		t.Fatalf("pool enrollment writes %v, want %v", f.writes[start:], want)
	}
	pool := f.inventory[tcpPool]
	pool["provisioning_status"] = "ACTIVE"
	f.settleTransportGraph(provider)
	checkSibling()

	retained := granted[0]
	retained.Weight = 1
	for _, phase := range []struct {
		name    string
		drop    cloud.UDPPacketDrop
		members []cloud.Member
	}{
		{name: "partial withdrawal", drop: cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, members: []cloud.Member{retained}},
		{name: "all traffic unavailable", drop: cloud.UDPPacketDrop{Numerator: 1, Denominator: 1}},
		{name: "restore the original weights", drop: cloud.UDPPacketDrop{Denominator: 1}, members: granted},
	} {
		route.UDPPacketDrop, route.Members = &phase.drop, phase.members
		before := len(f.writes)
		result := f.reconcile(provider)
		if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
			anchor["default_pool_id"] != "pool" {
			t.Fatalf("%s must use one atomic batch with the default intact: %+v, writes %v", phase.name, result, f.writes[before:])
		}
		checkSibling()
		body := f.tcpRejectionWrites[len(f.tcpRejectionWrites)-1]
		var drop cloud.UDPPacketDrop
		var members []cloud.Member
		var wireMembers []struct {
			Address string
			Port    int `json:"protocol_port"`
			Weight  int
		}
		if err := json.Unmarshal(body["udp_packet_drop"], &drop); err != nil || drop != phase.drop {
			t.Fatalf("%s encoded share %v, error %v", phase.name, drop, err)
		}
		if err := json.Unmarshal(body["members"], &wireMembers); err != nil || wireMembers == nil {
			t.Fatalf("%s encoded member set %s, error %v", phase.name, body["members"], err)
		}
		for _, member := range wireMembers {
			members = append(members, cloud.Member{Address: member.Address, Port: member.Port, Weight: member.Weight})
		}
		if !slices.Equal(members, phase.members) {
			t.Fatalf("%s encoded members %v, want %v", phase.name, members, phase.members)
		}
		pending := f.reconcile(provider)
		if pending.Routes[0].Result.Outcome.State == cloud.OutcomeReady || len(f.writes) != before+1 {
			t.Fatalf("%s became ready without active readback: %+v", phase.name, pending)
		}
		pool["provisioning_status"] = "ACTIVE"
		result = f.reconcile(provider)
		if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before+1 {
			t.Fatalf("%s did not settle with exact active readback: %+v, writes %v", phase.name, result, f.writes[before:])
		}
	}

	f.udpDropAdvertised = false
	f.inventory[tcpLB]["provisioning_status"], pool["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE"
	delete(pool, "udp_packet_drop") // a downgraded endpoint can omit the extension
	before := len(f.writes)
	if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) || anchor["default_pool_id"] != nil {
		t.Fatalf("lost capability must withdraw only the managed default: %+v, writes %v", result, f.writes[before:])
	}
	route.UDPPacketDrop, route.HealthMonitor = nil, stockMonitor
	f.inventory[tcpLB]["provisioning_status"] = "ACTIVE"
	f.settleTransportGraph(f.provider())
	checkSibling()
	if anchor["default_pool_id"] != "pool" || f.inventory[tcpPool]["udp_packet_drop"] != nil ||
		slices.Contains(pilotTags(f.inventory[tcpPool]["tags"]), udpDropPoolTag) || f.inventory[tcpMonitor] == nil {
		t.Fatalf("stock fallback did not restore the ordinary pool and monitor: %+v", f.inventory[tcpPool])
	}
}

func TestUDPPacketDropSupersedesALostOrFailedBatch(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	provider := newUDPPacketDropProvider(f)
	route := &f.spec.Routes[0].Spec
	granted := slices.Clone(route.Members)
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	for range 12 {
		if f.inventory[tcpPool]["udp_packet_drop"] != nil {
			break
		}
		if f.inventory[tcpListener]["provisioning_status"] == "PENDING_UPDATE" {
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
		}
		f.reconcile(provider)
	}
	pool, lb := f.inventory[tcpPool], f.inventory[tcpLB]
	if pool["udp_packet_drop"] == nil {
		t.Fatal("initial guarded batch was not staged")
	}
	lb["provisioning_status"], pool["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE"
	f.inventory[tcpMembers+"/member-batch-1"]["provisioning_status"] = "PENDING_UPDATE"
	route.Members = []cloud.Member{{Address: granted[0].Address, Port: granted[0].Port, Weight: 1}}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	apply := func(wanted cloud.UDPPacketDrop, members int) {
		t.Helper()
		before := len(f.writes)
		result := f.reconcile(provider)
		if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) ||
			!result.Outcome.MutationSubmitted || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
			t.Fatalf("expected a replacement batch, result %+v writes %v", result, f.writes[before:])
		}
		body := f.tcpRejectionWrites[len(f.tcpRejectionWrites)-1]
		var got cloud.UDPPacketDrop
		if err := json.Unmarshal(body["udp_packet_drop"], &got); err != nil || got != wanted || len(f.children(tcpMembers)) != members {
			t.Fatalf("replacement used obsolete intent %v, members %v, error %v", got, f.children(tcpMembers), err)
		}
	}
	f.udpDropRenderBusy = true
	before := len(f.tcpRejectionWrites)
	result := f.reconcile(provider)
	if result.Outcome.MutationSubmitted || result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || len(f.tcpRejectionWrites) != before {
		t.Fatalf("render conflict provided forwarding proof or staged a second batch: %+v", result)
	}
	f.udpDropRenderBusy = false
	fence := errors.New("grant changed again")
	f.spec.ValidateMutation = func(context.Context) error { return fence }
	blocked := len(f.writes)
	if _, err := provider.ReconcileGatewayGraph(t.Context(), f.spec); !errors.Is(err, fence) || len(f.writes) != blocked {
		t.Fatalf("pending replacement bypassed the authorization fence: %v", err)
	}
	f.spec.ValidateMutation = nil
	apply(*route.UDPPacketDrop, 1) // revoke while the first RPC is lost

	route.UDPPacketDrop, route.Members = &cloud.UDPPacketDrop{Denominator: 1}, granted
	apply(*route.UDPPacketDrop, 2) // restore while the revocation RPC is lost

	lb["provisioning_status"], pool["provisioning_status"] = "ACTIVE", "ERROR"
	f.inventory[tcpMembers+"/member-batch-1"]["provisioning_status"] = "PENDING_DELETE"
	f.inventory[tcpMember]["provisioning_status"] = "PENDING_UPDATE"
	apply(*route.UDPPacketDrop, 2) // an abandoned delete must not block failed-pool recovery

	lb["provisioning_status"], pool["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE"
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.spec.Routes[0].Spec = cloud.RouteSpec{Identity: route.Identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolUDP}}
	f.spec.Routes[1].Disposition = cloud.GraphPreserve
	f.udpCleanupGateway(cloud.GraphPreserve)
	f.udpDropAdvertised = false
	before = len(f.writes)
	f.udpDropRenderBusy = true
	f.reconcile(provider)
	f.udpDropRenderBusy = false
	f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool, "DELETE " + tcpPool}) ||
		f.inventory[tcpPool] != nil || f.inventory[tcpListener]["default_pool_id"] != nil {
		t.Fatalf("cleanup must retry the targeted delete without replaying a superseded batch: writes %v", f.writes[before:])
	}
}

func TestUDPPacketDropHoldsOnDiscoveryFailureUnlessADestinationWasRevoked(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	route := &f.spec.Routes[0].Spec
	provider := newUDPPacketDropProvider(f)
	f.settleTransportGraph(provider)
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	pool := f.inventory[tcpPool]
	pool["udp_packet_drop"] = map[string]any{"numerator": 0, "denominator": 1}
	delete(f.inventory, tcpMonitor)
	before := len(f.writes)
	f.udpDiscoveryUnavailable = true
	result := f.reconcile(provider)
	if len(f.writes) != before || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		result.Routes[0].Result.ObservationDeferred || f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("transient discovery failure changed the serving pool: %+v, writes %v", result, f.writes[before:])
	}
	route.Members = route.Members[:1]
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	result = f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) ||
		result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("a discovery failure delayed quarantine of a revoked target: %+v, writes %v", result, f.writes[before:])
	}
}

func TestUDPPacketDropDiscoveryFailureAllowsUnrelatedListenerRemoval(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	gateway := &f.spec.Gateway.Spec
	gateway.ScopedListeners = append(gateway.ScopedListeners, cloud.ScopedListener{Name: "retiring", Port: 9312})
	ordinary := &f.spec.Routes[1].Spec
	ordinary.ScopedListenerPorts = []int{9311, 9312}
	guarded := &f.spec.Routes[0].Spec
	guarded.HealthMonitor = cloud.HealthMonitorConfig{}
	provider := newUDPPacketDropProvider(f)
	f.settleTransportGraph(provider)
	guarded.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	guardedPool := f.inventory[tcpPool]
	guardedPool["tags"] = append(pilotTags(guardedPool["tags"]), udpDropPoolTag)
	guardedPool["udp_packet_drop"] = map[string]any{"numerator": 0, "denominator": 1}

	retiring := tcpListener + "-9312"
	if f.inventory[retiring]["default_pool_id"] != "pool-secondary" {
		t.Fatal("the ordinary pool must initially serve both of its listeners")
	}
	gateway.ScopedListeners = gateway.ScopedListeners[:2]
	ordinary.ScopedListenerPorts = ordinary.ScopedListenerPorts[:1]
	f.udpDiscoveryUnavailable = true
	before := len(f.writes)
	f.reconcile(provider)
	if f.inventory[retiring]["default_pool_id"] != nil || !slices.Equal(f.writes[before:], []string{"PUT " + retiring}) {
		t.Fatalf("discovery failure kept the obsolete ordinary listener attached: writes %v", f.writes[before:])
	}
	f.inventory[retiring]["provisioning_status"] = "ACTIVE"
	f.reconcile(provider)
	result := f.reconcile(provider)
	if f.inventory[retiring] != nil || !slices.Equal(f.writes[before:], []string{"PUT " + retiring, "DELETE " + retiring}) {
		t.Fatalf("ordinary listener cleanup during discovery failure: writes %v", f.writes[before:])
	}
	if f.inventory[tcpListener]["default_pool_id"] != "pool" || f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || result.Routes[0].Result.ObservationDeferred ||
		result.Routes[1].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("independent listeners or route outcomes during discovery failure: %+v, writes %v", result.Routes, f.writes[before:])
	}
	f.udpDiscoveryUnavailable = false
	result = f.settleTransportGraph(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before+2 {
		t.Fatalf("guarded route did not recover from discovery without a write: %+v, writes %v", result.Routes[0], f.writes[before:])
	}
}

func TestUDPPacketDropValidatesOnlyItsListenerSecurity(t *testing.T) {
	t.Run("withdrawn destination", func(t *testing.T) { testUDPPacketDropSecurityOutage(t, true) })
	t.Run("increased drop with identical destinations", func(t *testing.T) { testUDPPacketDropSecurityOutage(t, false) })
}

func testUDPPacketDropSecurityOutage(t *testing.T, withdrawDestination bool) {
	t.Helper()
	f := newUDPPacketDropFixture(t)
	route := &f.spec.Routes[0].Spec
	route.MemberMode = cloud.MemberModeNodePort
	route.Members = []cloud.Member{
		{Address: "192.0.2.44", Port: 32000, Weight: 3},
		{Address: "192.0.2.44", Port: 32001, Weight: 1},
	}
	f.settleTransportGraph(newUDPPacketDropProvider(f))
	gateway := &f.spec.Gateway.Spec
	const group = "96d7a899-4f5d-4cf5-9842-5cc66ad91575"
	gateway.VIPSecurityGroupIDs = []string{group}
	f.inventory[tcpLB]["vip_sg_ids"] = gateway.VIPSecurityGroupIDs
	for i := range f.spec.Routes {
		f.spec.Routes[i].Spec.GatewayRequirements.VIPSecurityGroupIDs = gateway.VIPSecurityGroupIDs
	}
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
	neutronUnavailable := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/", "/v2/":
			safetyWriteJSON(t, w, map[string]any{"versions": []any{map[string]string{"id": "v2.29", "status": "CURRENT"}}})
		case "/v2/lbaas/providers":
			safetyWriteJSON(t, w, map[string]any{"providers": []any{map[string]string{"name": "amphora"}}})
		case "/v2.0/subnets/" + gateway.VIPSubnetID:
			safetyWriteJSON(t, w, map[string]any{"subnet": map[string]any{
				"id": gateway.VIPSubnetID, "network_id": "private", "project_id": hibiscusProject, "cidr": "192.0.2.0/24", "ip_version": 4,
			}})
		case "/v2.0/security-groups/" + group:
			if neutronUnavailable {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			safetyWriteJSON(t, w, map[string]any{"security_group": map[string]any{
				"id": group, "project_id": hibiscusProject, "security_group_rules": []any{map[string]any{
					"direction": "ingress", "ethertype": "IPv4", "protocol": "udp", "port_range_min": gateway.ListenerPort, "port_range_max": gateway.ListenerPort,
				}},
			}})
		default:
			f.serveHTTP(w, req)
		}
	})
	clients := hibiscusServiceClients(handler)
	clients.LoadBalancer.Microversion = "2.29"
	provider := newTestProvider(t, clients, ProviderConfig{UDPPacketDrop: true})
	before := len(f.writes)
	for range 4 {
		f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
		f.reconcile(provider)
	}
	want := []string{"DELETE " + tcpPool, "POST " + tcpPools, "PUT " + tcpListener, "PUT " + tcpMembers}
	if !slices.Equal(f.writes[before:], want) || f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("sibling ingress denied the authorized listener's transition: wrote %v, want %v", f.writes[before:], want)
	}
	f.inventory[tcpPool]["provisioning_status"] = "ACTIVE"
	before = len(f.writes)
	neutronUnavailable = true
	node := &f.spec.Routes[0]
	node.Disposition, node.RetainListenerPorts = cloud.GraphPreserve, route.ScopedListenerPorts
	checkHeld := func() {
		t.Helper()
		result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
		if err != nil || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || len(f.writes) != before ||
			f.inventory[tcpListener]["default_pool_id"] != "pool" {
			t.Fatalf("temporary security outage disrupted the authorized pool: %v, writes %v", err, f.writes[before:])
		}
	}
	checkHeld() // Keep the exact serving set while the route is held.
	node.Disposition, node.RetainListenerPorts = cloud.GraphEnsure, nil
	serving := slices.Clone(route.Members)
	newWorker := cloud.Member{Address: "192.0.2.45", Port: 32000, Weight: 1}
	route.Members = append(route.Members, newWorker)
	checkHeld() // Defer the new worker until Neutron validates it.
	route.Members = serving
	if withdrawDestination {
		route.Members = []cloud.Member{serving[0], newWorker}
		route.Members[0].Weight = 1
	}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}
	result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if !gophercloud.ResponseCodeIs(err, http.StatusServiceUnavailable) || result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) || f.inventory[tcpListener]["default_pool_id"] != nil ||
		f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("temporary security outage left stale UDP forwarding active: %v, writes %v", err, f.writes[before:])
	}
}

func TestUDPPacketDropDeletesAPendingLegacyRouteWithOnlyItsRouteBinding(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	f.spec.Gateway.Spec.ScopedListeners = nil
	f.spec.Routes = f.spec.Routes[:1]
	f.spec.Routes[0].Spec.ScopedListenerPorts = nil
	f.settleTransportGraph(newUDPPacketDropProvider(f))
	f.inventory[tcpLB]["provisioning_status"] = "PENDING_UPDATE"
	f.inventory[tcpPool]["provisioning_status"] = "PENDING_UPDATE"
	f.inventory[tcpPool]["udp_packet_drop"] = map[string]any{"numerator": 0, "denominator": 1}
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	route := &f.spec.Routes[0]
	route.Disposition = cloud.GraphDelete
	route.Spec = cloud.RouteSpec{Identity: route.Spec.Identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolUDP}}
	before := len(f.writes)
	f.reconcile(f.provider())
	if !slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) ||
		f.inventory[tcpPool] != nil || f.inventory[tcpListener]["default_pool_id"] != nil {
		t.Fatalf("legacy UDP binding did not authorize pending guarded pool deletion: %v", f.writes[before:])
	}
}
