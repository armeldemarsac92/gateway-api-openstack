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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
)

func TestOctaviaRequiresCompleteChildCollections(t *testing.T) {
	for _, collection := range []struct {
		field string
		read  func(context.Context, *Service) (int, error)
	}{
		{"l7policies", func(ctx context.Context, s *Service) (int, error) {
			items, err := s.ListCompletePolicies(ctx, l7policies.ListOpts{})
			return len(items), err
		}},
		{"rules", func(ctx context.Context, s *Service) (int, error) {
			items, err := s.ListCompleteRules(ctx, "policy", l7policies.ListRulesOpts{})
			return len(items), err
		}},
		{"healthmonitors", func(ctx context.Context, s *Service) (int, error) {
			items, err := s.ListCompleteMonitors(ctx, monitors.ListOpts{})
			return len(items), err
		}},
	} {
		for _, scenario := range []struct {
			name       string
			pages      []map[string]any
			wantCount  int
			incomplete bool
		}{
			{name: "empty", pages: []map[string]any{{collection.field: []any{}}}},
			{name: "missing", pages: []map[string]any{{}}, incomplete: true},
			{name: "null", pages: []map[string]any{{collection.field: nil}}, incomplete: true},
			{name: "empty first page", pages: []map[string]any{{collection.field: []any{}}, {collection.field: []any{map[string]string{"id": "child"}}}}, wantCount: 1},
			{name: "incomplete later page", pages: []map[string]any{{collection.field: []any{map[string]string{"id": "child"}}}, {}}, incomplete: true},
		} {
			t.Run(collection.field+"/"+scenario.name, func(t *testing.T) {
				reads := 0
				handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if reads >= len(scenario.pages) {
						t.Fatalf("unexpected extra read of %s", req.URL)
					}
					page := scenario.pages[reads]
					reads++
					if reads < len(scenario.pages) {
						page[collection.field+"_links"] = []map[string]string{{"rel": "next", "href": "http://octavia.test" + req.URL.Path + "?marker=next"}}
					}
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(page); err != nil {
						t.Fatal(err)
					}
				})
				service := NewService(&gophercloud.ServiceClient{
					ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
					Endpoint:       "http://octavia.test/",
				})
				count, err := collection.read(t.Context(), service)
				if reads != len(scenario.pages) || count != scenario.wantCount || errors.Is(err, cloud.ErrDependencyNotReady) != scenario.incomplete || err != nil && !scenario.incomplete {
					t.Fatalf("read %d pages, %d children, error %v", reads, count, err)
				}
			})
		}
	}
}
