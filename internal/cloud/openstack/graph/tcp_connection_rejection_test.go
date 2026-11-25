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
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func (f *tcpGraphFixture) rejectionProvider(optIn bool) *Provider {
	return newTestProvider(f.t, hibiscusServiceClients(http.HandlerFunc(f.serveHTTP)), ProviderConfig{TCPConnectionRejection: optIn})
}

func (f *tcpGraphFixture) serveTCPRejectionBatch(w http.ResponseWriter, request *http.Request) {
	var fields map[string]json.RawMessage
	var members []map[string]any
	var rejection map[string]any
	if json.NewDecoder(request.Body).Decode(&fields) != nil || len(fields) != 2 ||
		json.Unmarshal(fields["members"], &members) != nil || members == nil ||
		json.Unmarshal(fields["tcp_connection_rejection"], &rejection) != nil {
		f.t.Errorf("invalid atomic TCP request: %s", request.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if rejection != nil && (!f.tcpRejectionAdvertised || f.tcpRejectionRejectWrite) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.writes = append(f.writes, "PUT "+request.URL.Path)
	f.tcpRejectionWrites = append(f.tcpRejectionWrites, fields)
	collection := request.URL.Path
	pool := f.inventory[strings.TrimSuffix(collection, "/members")]
	if rejection == nil {
		pool["tcp_connection_rejection"] = nil
	} else {
		pool["tcp_connection_rejection"] = rejection
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

func TestTCPConnectionRejectionWithdrawsAnUnprovableDefault(t *testing.T) {
	for _, missing := range []string{"capability", "controller opt-in", "pool field", "member inventory", "successful batch"} {
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
				f.tcpRejectionRejectWrite = true
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
