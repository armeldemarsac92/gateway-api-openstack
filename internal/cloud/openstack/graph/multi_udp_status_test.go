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

func TestUDPPacketDropAtomicallyReplacesAServingPoolAndRollsBack(t *testing.T) {
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
	route := &f.spec.Routes[0].Spec
	provider := newTestProvider(t, hibiscusServiceClients(http.HandlerFunc(f.serveHTTP)), ProviderConfig{UDPPacketDrop: true})
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

	for _, phase := range []struct {
		name    string
		drop    cloud.UDPPacketDrop
		members []cloud.Member
	}{
		{name: "partial withdrawal", drop: cloud.UDPPacketDrop{Numerator: 1, Denominator: 4}, members: []cloud.Member{{Address: "10.1.2.3", Port: 8080, Weight: 1}}},
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
	delete(pool, "udp_packet_drop") // a downgraded endpoint can omit the extension
	before := len(f.writes)
	if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady ||
		!slices.Equal(f.writes[before:], []string{"DELETE " + tcpPool}) || anchor["default_pool_id"] != nil {
		t.Fatalf("lost capability must withdraw only the managed default: %+v, writes %v", result, f.writes[before:])
	}
	route.UDPPacketDrop, route.HealthMonitor = nil, stockMonitor
	f.settleTransportGraph(f.provider())
	checkSibling()
	if anchor["default_pool_id"] != "pool" || f.inventory[tcpPool]["udp_packet_drop"] != nil ||
		slices.Contains(pilotTags(f.inventory[tcpPool]["tags"]), udpDropPoolTag) || f.inventory[tcpMonitor] == nil {
		t.Fatalf("stock fallback did not restore the ordinary pool and monitor: %+v", f.inventory[tcpPool])
	}
}
