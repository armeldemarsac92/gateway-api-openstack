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
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func TestListenerNotFoundResponseRoundTrip(t *testing.T) {
	var created, changed bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"listeners":[{"id":"eligible","default_http_response":{"status_code":404}},{"id":"native","default_http_response":null},{"id":"stock"}]}`))
			return
		}
		var request struct {
			Listener struct {
				Default struct {
					StatusCode int `json:"status_code"`
				} `json:"default_http_response"`
				Port int `json:"protocol_port"`
			} `json:"listener"`
		}
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil || request.Listener.Default.StatusCode != http.StatusNotFound {
			t.Fatalf("default response in %s: %#v, %v", req.Method, request.Listener, err)
		}
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/lbaas/listeners" && request.Listener.Port == 80:
			created = true
			w.WriteHeader(http.StatusCreated)
		case req.Method == http.MethodPut && req.URL.Path == "/lbaas/listeners/eligible":
			changed = true
		default:
			t.Fatalf("unexpected Octavia request %s %s", req.Method, req.URL.Path)
		}
		_, _ = w.Write([]byte(`{"listener":{"id":"eligible"}}`))
	})
	s := NewService(&gophercloud.ServiceClient{ProviderClient: &gophercloud.ProviderClient{
		HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}}, Endpoint: "http://octavia.test/"})
	if _, err := s.CreateListenerWithNotFound(t.Context(), listeners.CreateOpts{Protocol: listeners.ProtocolHTTP, ProtocolPort: 80, LoadbalancerID: "lb"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetListenerNotFound(t.Context(), "eligible"); err != nil {
		t.Fatal(err)
	}
	inventory, err := s.ListListenerInventory(t.Context(), listeners.ListOpts{})
	if err != nil || !created || !changed || inventory.DefaultHTTPResponseCodes["eligible"] != 404 || len(inventory.DefaultHTTPResponseCodes) != 1 {
		t.Fatalf("create = %v, update = %v, readback = %v, err = %v", created, changed, inventory.DefaultHTTPResponseCodes, err)
	}
}
