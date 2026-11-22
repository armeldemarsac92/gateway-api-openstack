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
	"reflect"
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

func TestWeightedPolicyWire(t *testing.T) {
	const pool = "b9b600af-3eb4-455c-aab5-d87c28c9c301"
	targets := []WeightedHTTPTarget{{Weight: 3, PoolID: pool}, {Weight: 1, FixedResponse: &FixedHTTPResponse{StatusCode: 500}}, {Weight: 0, PoolID: pool}}
	raw := `[{"pool_id":"` + pool + `","weight":3},{"weight":1,"fixed_response":{"status_code":500}},{"weight":0,"pool_id":"` + pool + `"}]`
	var wantTargets any
	if err := json.Unmarshal([]byte(raw), &wantTargets); err != nil {
		t.Fatal(err)
	}
	var methods []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		methods = append(methods, req.Method)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"l7policies":[{"id":"policy","action":"WEIGHTED_TARGETS","redirect_pool_id":null,"admin_state_up":true,"targets":` + raw + `}]}`))
			return
		}
		var body struct {
			Policy map[string]any `json:"l7policy"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"action": "WEIGHTED_TARGETS", "targets": wantTargets}
		if req.Method == http.MethodPost {
			want["listener_id"], want["admin_state_up"] = "listener", false
			w.WriteHeader(http.StatusCreated)
		}
		if !reflect.DeepEqual(body.Policy, want) {
			t.Fatalf("policy = %#v, want %#v", body.Policy, want)
		}
		_, _ = w.Write([]byte(`{"l7policy":{"id":"policy"}}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	disabled := false
	if _, err := service.CreatePolicy(t.Context(), PolicyCreateOpts{CreateOpts: l7policies.CreateOpts{ListenerID: "listener", AdminStateUp: &disabled}, WeightedTargets: targets}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetPolicyWeightedTargets(t.Context(), "policy", targets, nil); err != nil {
		t.Fatal(err)
	}
	observed, complete, err := service.ListPolicyObservations(t.Context(), l7policies.ListOpts{})
	if err != nil || !complete || len(observed) != 1 || observed[0].Target == nil || !reflect.DeepEqual(observed[0].Target.WeightedTargets, targets) {
		t.Fatalf("observed targets = %#v, error = %v", observed, err)
	}
	if !reflect.DeepEqual(methods, []string{"POST", "PUT", "GET"}) {
		t.Fatal(methods)
	}
}

func TestWeightedPolicyReadsOnlyExplicitTargetFields(t *testing.T) {
	const pool = "b9b600af-3eb4-455c-aab5-d87c28c9c301"
	for _, body := range []string{
		`{"pool_id":"` + pool + `"}`,
		`{"weight":0,"pool_id":"` + pool + `","fixed_response":null}`,
		`{"weight":1,"pool_id":"` + pool + `","unexpected":true}`,
		`{"weight":1,"fixed_response":{"status_code":500,"location":"/"}}`,
		`{"weight":1,"fixed_response":{"status_code":200}}`,
	} {
		var target WeightedHTTPTarget
		if err := json.Unmarshal([]byte(body), &target); err == nil {
			t.Errorf("accepted invalid weighted target %s", body)
		}
	}
}

func TestHTTPPolicyScopeAppliesToEachAction(t *testing.T) {
	disabled := false
	base := l7policies.CreateOpts{ListenerID: "shared", AdminStateUp: &disabled, Action: l7policies.ActionRedirectToPool, RedirectPoolID: "pool"}
	for _, opts := range []PolicyCreateOpts{
		{CreateOpts: base, HTTPSListenerHostname: "*"},
		{CreateOpts: l7policies.CreateOpts{ListenerID: "shared", Action: l7policies.ActionRedirectPrefix, RedirectPrefix: "https://example.test"}, HTTPSListenerHostname: "www.example.test"},
		{CreateOpts: base, HTTPSListenerHostname: "*.example.test", FixedServerError: true},
		{CreateOpts: base, HTTPSListenerHostname: "www.example.test", WeightedTargets: []WeightedHTTPTarget{{Weight: 1, PoolID: "pool"}}},
	} {
		body, err := opts.ToL7PolicyCreateMap()
		if err != nil {
			t.Fatal(err)
		}
		fields := body["l7policy"].(map[string]any)
		if fields["https_listener_hostname"] != opts.HTTPSListenerHostname {
			t.Fatalf("policy scope = %v, want %s", fields, opts.HTTPSListenerHostname)
		}
	}
	change, err := policyHTTPSHostname("*").ToL7PolicyUpdateMap()
	if err != nil || !reflect.DeepEqual(change["l7policy"], map[string]any{"https_listener_hostname": "*", "admin_state_up": false}) {
		t.Fatalf("scope repair = %v, err = %v", change, err)
	}
	scoped, err := readPolicyObservation(json.RawMessage(`{"id":"scoped","https_listener_hostname":"*"}`))
	if err != nil || scoped.HTTPSListenerHostname != "*" {
		t.Fatalf("scope readback = %q, err = %v", scoped.HTTPSListenerHostname, err)
	}
	plain, err := readPolicyObservation(json.RawMessage(`{"id":"plain"}`))
	if err != nil || plain.HTTPSListenerHostname != "" {
		t.Fatalf("plain scope = %q, err = %v", plain.HTTPSListenerHostname, err)
	}
}

func TestAuditReadbackTreatsFormattedEmptyTargetsAsInactive(t *testing.T) {
	row := json.RawMessage(`{"id":"policy","action":"REDIRECT_TO_POOL","targets":[
	]}`)
	observation, err := readPolicyObservation(row)
	if err != nil || observation.ExtendedTargetFields {
		t.Fatalf("empty JSON array activated private target handling: %v, %v", observation.ExtendedTargetFields, err)
	}
}
