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

func TestUDPPacketDropAtomicallyReplacesAServingPoolAndRollsBack(t *testing.T) {
	f := newUDPPacketDropFixture(t)
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

func TestUDPPacketDropValidatesOnlyItsListenerSecurity(t *testing.T) {
	f := newUDPPacketDropFixture(t)
	f.settleTransportGraph(newUDPPacketDropProvider(f))
	gateway := &f.spec.Gateway.Spec
	const group = "96d7a899-4f5d-4cf5-9842-5cc66ad91575"
	gateway.VIPSecurityGroupIDs = []string{group}
	f.inventory[tcpLB]["vip_sg_ids"] = gateway.VIPSecurityGroupIDs
	for i := range f.spec.Routes {
		f.spec.Routes[i].Spec.GatewayRequirements.VIPSecurityGroupIDs = gateway.VIPSecurityGroupIDs
	}
	route := &f.spec.Routes[0].Spec
	route.HealthMonitor = cloud.HealthMonitorConfig{}
	route.UDPPacketDrop = &cloud.UDPPacketDrop{Denominator: 1}
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
