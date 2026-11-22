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
	"slices"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func TestIsolatedHTTPSListenerIncludesVirtualHosts(t *testing.T) {
	hosts := []string{"*", "*.example.test", "shop.example.test"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"listeners":[{"id":"shared","https_virtual_hosts":["*","*.example.test","shop.example.test"]},{"id":"dedicated"},{"id":"invalid","admin_state_up":true,"https_virtual_hosts":false}]}`))
			return
		}
		if req.Method == http.MethodPut {
			var change struct {
				Listener map[string][]string `json:"listener"`
			}
			if err := json.NewDecoder(req.Body).Decode(&change); err != nil || len(change.Listener) != 1 ||
				!slices.Equal(change.Listener["https_virtual_hosts"], hosts[:2]) {
				t.Fatalf("section change = %#v, err = %v", change, err)
			}
			_, _ = w.Write([]byte(`{"listener":{"id":"shared"}}`))
			return
		}
		var body struct {
			Listener struct {
				VirtualHosts []string           `json:"https_virtual_hosts"`
				DefaultTLS   string             `json:"default_tls_container_ref"`
				Fallback     *FixedHTTPResponse `json:"default_http_response"`
				Enabled      *bool              `json:"admin_state_up"`
			} `json:"listener"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		item := body.Listener
		if !slices.Equal(item.VirtualHosts, hosts) || item.DefaultTLS != "certificate" ||
			item.Enabled == nil || *item.Enabled || item.Fallback == nil || item.Fallback.StatusCode != 404 {
			t.Fatalf("wrong shared listener parameters: %+v", item)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"listener":{"id":"shared"}}`))
	})
	service := NewService(&gophercloud.ServiceClient{ProviderClient: &gophercloud.ProviderClient{
		HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}}, Endpoint: "http://octavia.test/"})
	disabled := false
	_, err := service.CreateIsolatedHTTPSListener(t.Context(), listeners.CreateOpts{
		LoadbalancerID: "lb", Protocol: listeners.ProtocolTerminatedHTTPS, ProtocolPort: 443,
		DefaultTlsContainerRef: "certificate", AdminStateUp: &disabled,
	}, hosts, true)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := service.ListListenerInventory(t.Context(), listeners.ListOpts{})
	if err != nil || !slices.Equal(observed.HTTPSVirtualHosts["shared"], hosts) || observed.HTTPSVirtualHosts["dedicated"] != nil {
		t.Fatalf("virtual hosts = %#v, err = %v", observed.HTTPSVirtualHosts, err)
	}
	if observed.HTTPSVirtualHosts["invalid"] == nil || len(observed.HTTPSVirtualHosts["invalid"]) != 0 || !observed.AdminStatesObserved["invalid"] {
		t.Fatalf("invalid enrollment lost its identity or appeared valid: %+v", observed)
	}
	if err := service.SetListenerHTTPSVirtualHosts(t.Context(), "shared", hosts[:2]); err != nil {
		t.Fatal(err)
	}
}

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
