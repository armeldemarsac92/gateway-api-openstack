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
			{"id":"protected","backend_tls_hostname":"backend.example.test","ca_tls_container_ref":"http://barbican.test/example","tls_container_ref":"http://barbican.test/client","tls_enabled":true,"admin_state_up":false,
				"backend_tls_subject_alt_names":[{"type":"Hostname","hostname":"*.backend.example.test"},{"uri":"spiffe://id.example.test/workload%2fa","type":"URI"}]},
			{"id":"cleared","backend_tls_subject_alt_names":[]},
			{"id":"null","backend_tls_subject_alt_names":null},
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
