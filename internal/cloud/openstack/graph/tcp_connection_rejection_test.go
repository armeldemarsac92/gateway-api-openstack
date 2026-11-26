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
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func (f *tcpGraphFixture) rejectionProvider(optIn bool) *Provider {
	return newTestProvider(f.t, hibiscusServiceClients(http.HandlerFunc(f.serveHTTP)), ProviderConfig{TCPConnectionRejection: optIn})
}

func (f *tcpGraphFixture) serveTransportBatch(w http.ResponseWriter, request *http.Request) {
	field, advertised := "tcp_connection_rejection", f.tcpRejectionAdvertised
	if f.udpDropAPI {
		field, advertised = "udp_packet_drop", f.udpDropAdvertised
	}
	var fields map[string]json.RawMessage
	var members []map[string]any
	var rejection map[string]any
	if json.NewDecoder(request.Body).Decode(&fields) != nil || len(fields) != 2 ||
		json.Unmarshal(fields["members"], &members) != nil || members == nil ||
		json.Unmarshal(fields[field], &rejection) != nil {
		f.t.Errorf("invalid transport batch request: %s", request.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if f.tcpRejectionRejectCode != 0 {
		if f.tcpRejectionRemovePool {
			for _, member := range f.children(request.URL.Path) {
				delete(f.inventory, request.URL.Path+"/"+member["id"].(string))
			}
			delete(f.inventory, strings.TrimSuffix(request.URL.Path, "/members"))
			delete(f.inventory, tcpMonitor)
			f.inventory[tcpListener]["default_pool_id"] = nil
		}
		w.WriteHeader(f.tcpRejectionRejectCode)
		return
	}
	if rejection != nil && !advertised {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if f.udpDropAPI && (rejection == nil || len(f.inventory[strings.TrimSuffix(request.URL.Path, "/members")]["listeners"].([]map[string]any)) != 1) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if f.udpDropAPI && f.udpDropRenderBusy {
		f.writes = append(f.writes, "PUT "+request.URL.Path)
		w.WriteHeader(http.StatusConflict)
		return
	}
	if !f.udpDropAPI && f.inventory[tcpLB]["provisioning_status"] == "PENDING_UPDATE" {
		previous := f.tcpRejectionWrites[len(f.tcpRejectionWrites)-1]
		f.writes = append(f.writes, "PUT "+request.URL.Path)
		if string(fields["members"]) != string(previous["members"]) || string(fields[field]) != string(previous[field]) {
			w.WriteHeader(http.StatusConflict)
		} else {
			f.tcpRejectionWrites = append(f.tcpRejectionWrites, fields)
			w.WriteHeader(http.StatusAccepted)
		}
		return
	}
	f.writes = append(f.writes, "PUT "+request.URL.Path)
	f.tcpRejectionWrites = append(f.tcpRejectionWrites, fields)
	collection := request.URL.Path
	pool := f.inventory[strings.TrimSuffix(collection, "/members")]
	if rejection == nil {
		pool[field] = nil
	} else {
		pool[field] = rejection
	}
	existing := f.children(collection)
	for _, old := range existing {
		delete(f.inventory, collection+"/"+old["id"].(string))
	}
	for i, member := range members {
		id := "member"
		if i > 0 {
			id += "-batch-" + strconv.Itoa(i)
		}
		member["id"], member["project_id"], member["provisioning_status"] = id, hibiscusProject, "ACTIVE"
		f.inventory[collection+"/"+id] = member
	}
	pool["provisioning_status"] = "PENDING_UPDATE"
	w.WriteHeader(http.StatusAccepted)
}

func TestTCPConnectionRejectionReplacesACompleteServingBatch(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
	anchor := f.spec.Gateway.Spec.ListenerPort
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "affected", Port: anchor}, {Name: "independent", Port: 9311}}
	first := &f.spec.Routes[0]
	first.Spec.MemberMode = cloud.MemberModeNodePort
	first.Spec.ScopedListenerPorts = []int{anchor}
	backup := cloud.Member{Address: "192.0.2.45", Port: 32001, Weight: 1}
	first.Spec.Members[0].Weight = 3
	first.Spec.Members = append(first.Spec.Members, backup)
	second := *first
	second.Spec.Identity.RouteName, second.Spec.Identity.RouteUID = "independent", "independent-uid"
	second.Spec.Precedence++
	second.Spec.ScopedListenerPorts = []int{9311}
	second.Spec.Members = []cloud.Member{{Address: "192.0.2.46", Port: 32002, Weight: 1}}
	f.spec.Routes = append(f.spec.Routes, second)
	route := &f.spec.Routes[0].Spec
	provider := f.rejectionProvider(true)
	f.settleTransportGraph(provider)
	primary, sibling := f.inventory[tcpListener], f.inventory[tcpListener+"-9311"]
	pool := f.inventory[tcpPool]
	f.inventory[tcpMember]["tags"] = append(pilotTags(f.inventory[tcpMember]["tags"]), "operator-metadata=preserved")

	apply := func(rejection *cloud.TCPConnectionRejection, members []cloud.Member, active *Provider, wantFraction string) {
		t.Helper()
		route.TCPConnectionRejection, route.Members = rejection, members
		before := len(f.writes)
		result := f.reconcile(active)
		if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) ||
			result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
			primary["default_pool_id"] != "pool" || sibling["default_pool_id"] != "pool-secondary" {
			t.Fatalf("serving transition must use one atomic write, got %v, result %+v", f.writes[before:], result)
		}
		body := f.tcpRejectionWrites[len(f.tcpRejectionWrites)-1]
		var got []struct {
			Address string
			Weight  int
			Tags    []string
			Backup  bool
		}
		if string(body["tcp_connection_rejection"]) != wantFraction || json.Unmarshal(body["members"], &got) != nil || got == nil || len(got) != len(members) {
			t.Fatalf("batch payload %s / %s", body["tcp_connection_rejection"], body["members"])
		}
		for i, wanted := range members {
			if got[i].Address != wanted.Address || got[i].Weight != wanted.Weight || got[i].Backup ||
				!slices.Contains(got[i].Tags, "gateway-api-openstack.tcp-nodeport-weight=v1") {
				t.Fatalf("unexpected encoded member %+v, want %v", got[i], wanted)
			}
		}
		if result := f.reconcile(active); result.Routes[0].Result.Outcome.State == cloud.OutcomeReady || len(f.writes) != before+1 {
			t.Fatalf("PENDING_UPDATE allowed another write or certified traffic: %+v", result)
		}
		pool["provisioning_status"] = "ACTIVE"
		if result := f.reconcile(active); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(f.writes) != before+1 {
			t.Fatalf("exact ACTIVE readback did not settle: %+v, writes %v", result, f.writes[before:])
		}
	}

	primaryMember := route.Members[0]
	primaryMember.Weight = 1
	apply(&cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}, []cloud.Member{primaryMember}, provider, `{"reject_weight":1,"total_weight":4}`)
	if !slices.Contains(pilotTags(f.inventory[tcpMember]["tags"]), "operator-metadata=preserved") {
		t.Fatal("batch removed operator metadata from the surviving member")
	}
	apply(&cloud.TCPConnectionRejection{RejectWeight: 4, TotalWeight: 4}, nil, provider, `{"reject_weight":4,"total_weight":4}`)
	primaryMember.Weight = 3
	apply(nil, []cloud.Member{primaryMember, backup}, provider, "null")

	primaryMember.Weight = 1
	apply(&cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}, []cloud.Member{primaryMember}, provider, `{"reject_weight":1,"total_weight":4}`)
	f.tcpRejectionAdvertised = false
	primaryMember.Weight = 3
	apply(nil, []cloud.Member{primaryMember, backup}, f.rejectionProvider(false), "null")
}

func TestTransportBatchClearsSubnetForRoutedServiceImport(t *testing.T) {
	for _, protocol := range []string{cloud.ListenerProtocolTCP, cloud.ListenerProtocolUDP} {
		t.Run(protocol, func(t *testing.T) {
			desired := desiredRoute{spec: cloud.RouteSpec{
				MemberSubnetID:      "local-subnet",
				GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: protocol},
			}}
			existing := pools.Member{ID: "member", SubnetID: "local-subnet"}
			member := cloud.Member{Address: "10.2.3.4", Port: 8180, Weight: 3, RoutedExternal: true}
			for _, routed := range []bool{true, false} {
				member.RoutedExternal = routed
				opts, err := transportBatchMember(desired, member, existing)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := opts.ToBatchMemberUpdateMap()
				if err != nil {
					t.Fatal(err)
				}
				var want any
				if !routed {
					want = desired.spec.MemberSubnetID
				}
				if got, exists := wire["subnet_id"]; !exists || got != want {
					t.Fatalf("member subnet = %v (present %t), want %v", got, exists, want)
				}
			}
		})
	}
}

func TestTCPConnectionRejectionWithdrawsAnUnprovableDefault(t *testing.T) {
	for _, missing := range []string{"capability", "controller opt-in", "pool field", "member inventory", "successful batch", "batch subnet", "batch request limit"} {
		t.Run(missing, func(t *testing.T) {
			f := newTCPGraphFixture(t)
			f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
			provider := f.rejectionProvider(true)
			f.settleTransportGraph(provider)
			switch missing {
			case "capability":
				f.tcpRejectionAdvertised = false
			case "controller opt-in":
				provider = f.rejectionProvider(false)
			case "pool field":
				delete(f.inventory[tcpPool], "tcp_connection_rejection")
			case "member inventory":
				delete(f.inventory[tcpMember], "weight")
			case "successful batch":
				f.tcpRejectionRejectCode = http.StatusBadRequest
			case "batch subnet":
				f.tcpRejectionRejectCode = http.StatusNotFound
			case "batch request limit":
				f.tcpRejectionRejectCode = http.StatusRequestEntityTooLarge
			}
			before := len(f.writes)
			f.spec.Routes[0].Spec.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}
			result := f.reconcile(provider)
			if f.inventory[tcpListener]["default_pool_id"] != nil || len(f.tcpRejectionWrites) != 0 ||
				!slices.Equal(f.writes[before:], []string{"PUT " + tcpListener}) || result.Routes[0].Result.Outcome.State == cloud.OutcomeReady {
				t.Fatalf("unprovable target = %+v, writes %v", result, f.writes[before:])
			}
		})
	}
}

func TestTCPConnectionRejectionPrioritizesServingRevocation(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
	anchor := f.spec.Gateway.Spec.ListenerPort
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "affected", Port: anchor}, {Name: "independent", Port: 9311}}
	first := &f.spec.Routes[0]
	first.Spec.ScopedListenerPorts = []int{anchor}
	independent := *first
	independent.Spec.Identity.RouteName, independent.Spec.Identity.RouteUID = "independent", "independent-uid"
	independent.Spec.Precedence++
	independent.Spec.ScopedListenerPorts = []int{9311}
	f.spec.Routes = append(f.spec.Routes, independent)
	provider := f.rejectionProvider(true)
	f.settleTransportGraph(provider)
	f.spec.Gateway.Spec.ScopedListeners = append(f.spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "new", Port: 9312})
	f.spec.Routes[1].Spec.BackendSecurityGroupIDs = []string{"96d7a899-4f5d-4cf5-9842-5cc66ad91575"}
	f.tcpRejectionNetworkDown = true
	first = &f.spec.Routes[0]
	first.Spec.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}
	before := len(f.writes)
	f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) ||
		f.inventory[tcpListener]["default_pool_id"] != "pool" ||
		f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" {
		t.Fatalf("a sibling dependency or new listener delayed the revocation: writes %v", f.writes[before:])
	}

	f.inventory[tcpPool]["provisioning_status"] = "ACTIVE"
	first.Spec.TCPConnectionRejection.RejectWeight = 2
	first.Spec.BackendSecurityGroupIDs = []string{"96d7a899-4f5d-4cf5-9842-5cc66ad91575"}
	before = len(f.writes)
	result := f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"PUT " + tcpListener}) ||
		f.inventory[tcpListener]["default_pool_id"] != nil ||
		f.inventory[tcpListener+"-9311"]["default_pool_id"] != "pool-secondary" ||
		result.Routes[0].Result.Outcome.State == cloud.OutcomeReady {
		t.Fatalf("uncertain affected dependency did not close only the affected default: %+v, writes %v", result, f.writes[before:])
	}
}

func TestTCPConnectionRejectionReobservesAnActuallyMissingPool(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
	provider := f.rejectionProvider(true)
	f.settleTransportGraph(provider)
	f.spec.Routes[0].Spec.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}
	f.tcpRejectionRejectCode, f.tcpRejectionRemovePool = http.StatusNotFound, true
	before := len(f.writes)
	result := f.reconcile(provider)
	if len(f.writes) != before || f.inventory[tcpPool] != nil || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("pool disappearance was not handled by fresh observation: %+v, writes %v", result, f.writes[before:])
	}
	f.tcpRejectionRejectCode, f.tcpRejectionRemovePool = 0, false
	f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"POST " + tcpPools}) {
		t.Fatalf("missing pool did not return to ordinary staging: writes %v", f.writes[before:])
	}
}

func TestTCPConnectionRejectionReplaysOnlyAnExactAgedPendingBatch(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(strconv.FormatBool(clear), func(t *testing.T) {
			f := newTCPGraphFixture(t)
			f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
			provider := f.rejectionProvider(true)
			f.settleTransportGraph(provider)
			route := &f.spec.Routes[0].Spec
			route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}
			f.reconcile(provider)
			if clear {
				f.inventory[tcpPool]["provisioning_status"] = "ACTIVE"
				route.TCPConnectionRejection = nil
				f.reconcile(provider)
			}
			// The persisted manifest contains a staged member and can still
			// expose an obsolete row that the worker has not deleted.
			member := f.inventory[tcpMember]
			member["provisioning_status"] = "PENDING_UPDATE"
			old := maps.Clone(member)
			old["id"], old["address"], old["provisioning_status"] = "retired", "192.0.2.99", "PENDING_DELETE"
			f.inventory[tcpMembers+"/retired"] = old
			f.inventory[tcpLB]["provisioning_status"] = "PENDING_UPDATE"
			f.inventory[tcpListener]["provisioning_status"] = "PENDING_UPDATE"
			pool := f.inventory[tcpPool]
			pool["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			before := len(f.writes)
			if result := f.reconcile(provider); len(f.writes) != before || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("ordinary pending was retried immediately: %+v, writes %v", result, f.writes[before:])
			}
			pool["updated_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
			route.Members[0].Port++
			if result := f.reconcile(provider); len(f.writes) != before || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
				t.Fatalf("different desired payload was replayed: %+v, writes %v", result, f.writes[before:])
			}
			route.Members[0].Port--
			acceptedBefore := len(f.tcpRejectionWrites)
			result := f.reconcile(provider)
			if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) ||
				len(f.tcpRejectionWrites) != acceptedBefore+1 ||
				result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
				result.Routes[0].Result.ObservationDeferred || result.Outcome.RequeueAfter < transportReplayInterval {
				t.Fatalf("aged batch was not replayed exactly or certified too early: %+v, writes %v", result, f.writes[before:])
			}
			f.reconcile(provider)
			if len(f.writes) != before+1 {
				t.Fatalf("repeated event skipped replay backoff: writes %v", f.writes[before:])
			}
		})
	}
}

func TestTCPConnectionRejectionRecoversFailedActiveLoadBalancer(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
	provider := f.rejectionProvider(true)
	f.settleTransportGraph(provider)
	route := &f.spec.Routes[0].Spec
	route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}
	f.reconcile(provider)
	pool := f.inventory[tcpPool]
	pool["provisioning_status"] = "ERROR"
	f.inventory[tcpListener]["provisioning_status"] = "ERROR"
	old := maps.Clone(f.inventory[tcpMember])
	old["id"], old["address"], old["provisioning_status"] = "retired", "192.0.2.99", "PENDING_DELETE"
	f.inventory[tcpMembers+"/retired"] = old
	route.TCPConnectionRejection = nil
	f.tcpRejectionAdvertised = false
	before := len(f.writes)
	provider = f.rejectionProvider(false)
	result := f.reconcile(provider)
	if !slices.Equal(f.writes[before:], []string{"PUT " + tcpMembers}) ||
		string(f.tcpRejectionWrites[len(f.tcpRejectionWrites)-1]["tcp_connection_rejection"]) != "null" ||
		result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing || f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("failed batch did not recover with the current clear target: %+v, writes %v", result, f.writes[before:])
	}
	pool["provisioning_status"], f.inventory[tcpListener]["provisioning_status"] = "ACTIVE", "ACTIVE"
	if result := f.reconcile(provider); result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("recovery did not settle with confirmed readback: %+v", result)
	}
}

func TestTCPConnectionRejectionStagesAnEmptyRejectingPool(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.tcpRejectionAPI, f.tcpRejectionAdvertised = true, true
	f.spec.Routes[0].Spec.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 1}
	f.spec.Routes[0].Spec.Members = nil
	provider := f.rejectionProvider(true)
	for range 4 {
		f.reconcile(provider)
	}
	if f.inventory[tcpListener]["default_pool_id"] != nil || len(f.tcpRejectionWrites) != 1 ||
		string(f.tcpRejectionWrites[0]["members"]) != "[]" {
		t.Fatalf("new rejecting pool attached before confirmation or used a fake member, writes %v", f.writes)
	}
	f.inventory[tcpPool]["provisioning_status"] = "ACTIVE"
	result := f.settleTransportGraph(provider)
	if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || f.inventory[tcpListener]["default_pool_id"] != "pool" ||
		len(f.children(tcpMembers)) != 0 || slices.Contains(f.writes, "POST "+tcpMembers) {
		t.Fatalf("full rejection did not settle with an empty pool, result %+v, writes %v", result, f.writes)
	}
}
