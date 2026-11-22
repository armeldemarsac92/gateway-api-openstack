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
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
)

func TestCAUnusedRequiresCompleteProjectTrustInventory(t *testing.T) {
	const revision = "108a9849-f944-4d1c-b779-3b73ae0c1c68"
	const reference = "https://barbican.example/v1/secrets/" + revision
	for _, tc := range []struct {
		name            string
		pool, listener  map[string]any
		unused, wantErr bool
	}{
		{name: "detached", pool: map[string]any{"ca_tls_container_ref": nil}, listener: map[string]any{"client_ca_tls_container_ref": nil}, unused: true},
		{name: "another pool uses secret UUID", pool: map[string]any{"ca_tls_container_ref": revision}},
		{name: "frontend uses the same PEM", pool: map[string]any{"ca_tls_container_ref": nil}, listener: map[string]any{"client_ca_tls_container_ref": "https://internal.example/v1/secrets/" + revision}},
		{name: "missing pool field", pool: map[string]any{}, wantErr: true},
		{name: "missing listener field", pool: map[string]any{"ca_tls_container_ref": nil}, listener: map[string]any{}, wantErr: true},
		{name: "previous pool still serving", pool: map[string]any{"ca_tls_container_ref": nil, "provisioning_status": "PENDING_UPDATE"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != "GET" || req.URL.Query().Get("project_id") != "tenant" {
					t.Fatalf("unexpected inventory request %s %s", req.Method, req.URL.Path)
				}
				var fields map[string]any
				var collection string
				switch req.URL.Path {
				case "/lbaas/pools":
					collection, fields = "pools", tc.pool
				case "/lbaas/listeners":
					collection, fields = "listeners", tc.listener
				default:
					t.Fatalf("unexpected inventory path %s", req.URL.Path)
				}
				if fields == nil {
					t.Fatal("unexpected inventory read")
				}
				row := map[string]any{"id": "item", "project_id": "tenant", "provisioning_status": "ACTIVE"}
				for k, v := range fields {
					row[k] = v
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]any{collection: []map[string]any{row}}); err != nil {
					t.Fatal(err)
				}
			})
			api := NewService(&gophercloud.ServiceClient{
				ProviderClient: &gophercloud.ProviderClient{
					HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}},
				},
				Endpoint: "http://octavia.test/",
			})
			unused, err := api.CAUnused(t.Context(), "tenant", reference)
			if unused != tc.unused || (err != nil) != tc.wantErr {
				t.Fatalf("CAUnused = %v, %v", unused, err)
			}
		})
	}
}
