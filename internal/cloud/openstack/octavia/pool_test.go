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

package octavia

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func TestListMembersWithAdminStateRequiresExplicitReadback(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/lbaas/pools/owned-pool/members" {
			t.Fatalf("unexpected Octavia operation %s %s", req.Method, req.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"members":[{"id":"disabled","admin_state_up":false},{"id":"omitted"},{"id":"null","admin_state_up":null}]}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	members, unknown, err := service.ListMembersWithAdminState(t.Context(), "owned-pool", pools.ListMembersOpts{})
	_, omitted := unknown["omitted"]
	_, null := unknown["null"]
	if err != nil || len(members) != 3 || len(unknown) != 2 || !omitted || !null || members[0].AdminStateUp {
		t.Fatalf("members = %#v, unknown state = %v, err = %v", members, unknown, err)
	}
}

func TestPoolInventoryRequiresExplicitTLSReadback(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/lbaas/pools" {
			t.Fatalf("unexpected Octavia operation %s %s", req.Method, req.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Query().Get("marker") {
		case "":
			_, _ = w.Write([]byte(`{"pools":[
				{"id":"plain","backend_tls_hostname":null,"ca_tls_container_ref":null,"tls_container_ref":null,"tls_enabled":false,"admin_state_up":true}
			],"pools_links":[{"rel":"next","href":"http://octavia.test/lbaas/pools?marker=plain"}]}`))
			return
		case "plain":
		default:
			t.Fatalf("unexpected pool marker %q", req.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"pools":[
			{"id":"missing-boolean","backend_tls_hostname":null,"ca_tls_container_ref":null,"tls_container_ref":null,"admin_state_up":true},
			{"id":"missing-nullable","backend_tls_hostname":null,"ca_tls_container_ref":null,"tls_enabled":false,"admin_state_up":true},
			{"id":"protected","backend_tls_hostname":"backend.example.test","ca_tls_container_ref":"http://barbican.test/example","tls_container_ref":"http://barbican.test/client","tls_enabled":true,"admin_state_up":false,"alpn_protocols":["http/1.1"],
				"backend_tls_subject_alt_names":[{"type":"Hostname","hostname":"*.backend.example.test"},{"uri":"spiffe://id.example.test/workload%2fa","type":"URI"}]},
			{"id":"cleared","backend_tls_subject_alt_names":[],"alpn_protocols":null},
			{"id":"null","backend_tls_subject_alt_names":null,"alpn_protocols":"http/1.1"},
			{"id":"wrong-case","backend_tls_subject_alt_names":[{"Type":"URI","uri":"spiffe://id.example.test/workload%2fa"}]},
			{"id":"extra","backend_tls_subject_alt_names":[{"type":"URI","uri":"spiffe://id.example.test/workload%2fa","hostname":""}]},
			{"id":"empty-identity","backend_tls_subject_alt_names":[{"type":"URI","uri":""}]},
			{"id":"duplicate","backend_tls_subject_alt_names":[{"type":"URI","uri":"spiffe://other.example.test/workload","uri":"spiffe://id.example.test/workload%2fa"}]}
		]}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	inventory, err := service.ListPoolInventory(t.Context(), pools.ListOpts{})
	if err != nil || len(inventory.Items) != 10 {
		t.Fatalf("list pools: %v", err)
	}
	if !inventory.BackendTLS["plain"].Complete() || !inventory.Items[0].AdminStateUp || inventory.Items[0].TLSEnabled ||
		inventory.BackendTLS["missing-boolean"].Complete() || inventory.BackendTLS["missing-nullable"].Complete() {
		t.Fatal("an omitted TLS field was treated as an explicit plain setting")
	}
	if protected := inventory.Items[3]; !protected.TLSEnabled || protected.AdminStateUp || protected.CATLSContainerRef == "" || protected.TLSContainerRef != "http://barbican.test/client" ||
		!inventory.BackendTLS["protected"].Complete() || inventory.BackendTLS["protected"].Hostname != "backend.example.test" {
		t.Fatal("the protected pool settings were not preserved")
	}
	if alpn := inventory.Items[3].ALPNProtocols; !inventory.BackendTLS["protected"].ALPNObserved || len(alpn) != 1 || alpn[0] != "http/1.1" ||
		!inventory.BackendTLS["cleared"].ALPNObserved || inventory.BackendTLS["plain"].ALPNObserved || inventory.BackendTLS["null"].ALPNObserved {
		t.Fatal("pool ALPN readback must report explicit settings while isolating omissions and malformed values")
	}
	if legacy, cleared := inventory.BackendTLS["plain"], inventory.BackendTLS["cleared"]; legacy.SubjectAltNamesObserved ||
		!cleared.SubjectAltNamesObserved || cleared.SubjectAltNamesInvalid || len(cleared.SubjectAltNames) != 0 {
		t.Fatalf("did not distinguish omitted SAN readback from explicit empty: legacy=%+v, cleared=%+v", legacy, cleared)
	}
	protectedTLS := inventory.BackendTLS["protected"]
	if !protectedTLS.SubjectAltNamesObserved || protectedTLS.SubjectAltNamesInvalid || len(protectedTLS.SubjectAltNames) != 2 ||
		protectedTLS.SubjectAltNames[0].Hostname != "*.backend.example.test" || protectedTLS.SubjectAltNames[1].URI != "spiffe://id.example.test/workload%2fa" {
		t.Fatalf("did not preserve the typed SANs: %+v", protectedTLS)
	}
	for _, id := range []string{"null", "wrong-case", "extra", "empty-identity", "duplicate"} {
		if tls := inventory.BackendTLS[id]; !tls.SubjectAltNamesObserved || !tls.SubjectAltNamesInvalid || len(tls.SubjectAltNames) != 0 {
			t.Errorf("pool %q should have unusable SAN readback: %+v", id, tls)
		}
	}
}

func TestPoolSystemCAFields(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pools":[
			{"id":"legacy"},
			{"id":"empty","backend_tls_system_ca":true,"backend_tls_system_ca_sha256":null},
			{"id":"applied","backend_tls_system_ca":true,"backend_tls_system_ca_sha256":"` + strings.Repeat("a", 64) + `"},
			{"id":"malformed","backend_tls_system_ca":true,"backend_tls_system_ca_sha256":"` + strings.Repeat("A", 64) + `"},
			{"id":"wrong-mode","backend_tls_system_ca":"true","backend_tls_system_ca_sha256":"` + strings.Repeat("a", 64) + `"},
			{"id":"cleared","backend_tls_system_ca":false,"backend_tls_system_ca_sha256":null}
		]}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	list, err := service.ListPoolInventory(t.Context(), pools.ListOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, obs := range list.Items {
		tls := list.BackendTLS[obs.ID]
		if tls.SystemCAApplied() != (obs.ID == "applied") {
			t.Errorf("incorrect proof classification for %s", obs.ID)
		}
	}
	if list.BackendTLS["legacy"].SystemCAObserved || !list.BackendTLS["cleared"].SystemCAObserved || !list.BackendTLS["cleared"].SystemCADigestObserved {
		t.Fatal("legacy fields and an explicit clear must be distinguishable")
	}
	create := CreatePoolOpts{CreateOpts: pools.CreateOpts{Protocol: pools.ProtocolHTTP, LBMethod: pools.LBMethodRoundRobin, LoadbalancerID: "lb"},
		BackendTLSHostname: "backend.example.test", BackendTLSSystemCA: true}
	body, err := create.ToPoolCreateMap()
	if err != nil || body["pool"].(map[string]any)["backend_tls_system_ca"] != true {
		t.Fatalf("create did not send system trust: %v, %v", body, err)
	}
	update := UpdatePoolOpts{BackendTLS: &BackendTLSSettings{Hostname: "backend.example.test", SystemCA: new(true)}}
	body, err = update.ToPoolUpdateMap()
	if err != nil {
		t.Fatal(err)
	}
	wire := body["pool"].(map[string]any)
	if ref, found := wire["ca_tls_container_ref"]; !found || ref != nil || wire["backend_tls_system_ca"] != true {
		t.Fatalf("system trust must explicitly remove the CA reference: %v", wire)
	}
	if _, found := wire["backend_tls_system_ca_sha256"]; found {
		t.Fatalf("update sent a read only digest: %v", wire)
	}
}
