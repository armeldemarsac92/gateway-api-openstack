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
	"slices"
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
	if _, err := service.SetPolicyWeightedTargets(t.Context(), "policy", targets, PolicyFilters{}); err != nil {
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
	unreadable, err := readPolicyObservation(json.RawMessage(`{"id":"invalid","https_listener_hostname":false}`))
	if err != nil || unreadable.HTTPSListenerHostname != "" || unreadable.Policy.ID != "invalid" {
		t.Fatalf("unreadable policy lost its identity or appeared authorized: %+v, err = %v", unreadable, err)
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

func TestResponseHeaderPolicyWire(t *testing.T) {
	const path = "/lbaas/l7policies"
	records := []json.RawMessage{
		json.RawMessage(`{"id":"configured","response_header_operations":[{"operation":"SET","name":"X-Color","value":"blue"},{"operation":"ADD","name":"x-extra","value":""},{"operation":"REMOVE","name":"x-old"}]}`),
		json.RawMessage(`{"id":"cleared","response_header_operations":[]}`),
		json.RawMessage(`{"id":"stock"}`),
		json.RawMessage(`{"id":"malformed","response_header_operations":null}`),
	}
	response := []cloud.HTTPHeaderOperation{
		{Operation: cloud.HTTPHeaderSet, Name: "x-color", Value: "blue"},
		{Operation: cloud.HTTPHeaderAdd, Name: "x-extra"},
		{Operation: cloud.HTTPHeaderRemove, Name: "x-old"},
	}
	const responseJSON = `[{"name":"x-color","operation":"SET","value":"blue"},{"name":"x-extra","operation":"ADD","value":""},{"name":"x-old","operation":"REMOVE"}]`
	const requestJSON = `[{"name":"x-request","operation":"REMOVE"}]`
	call := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			if r.URL.Path != path || call != 3 {
				t.Fatalf("unexpected policy read on call %d: %s", call, r.URL)
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"l7policies": records}); err != nil {
				t.Fatal(err)
			}
			return
		}
		var body struct {
			Policy map[string]json.RawMessage `json:"l7policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		switch call {
		case 0:
			if r.Method != http.MethodPost || r.URL.Path != path ||
				string(body.Policy[responseHeaderOperationsField]) != responseJSON ||
				string(body.Policy[requestHeaderOperationsField]) != requestJSON ||
				string(body.Policy["admin_state_up"]) != "false" {
				t.Fatalf("unexpected policy create %s %s: %v", r.Method, r.URL, body.Policy)
			}
			w.WriteHeader(http.StatusCreated)
		case 1:
			if r.Method != http.MethodPut || r.URL.Path != path+"/configured" ||
				string(body.Policy["action"]) != `"FIXED_RESPONSE"` ||
				string(body.Policy[responseHeaderOperationsField]) != responseJSON ||
				string(body.Policy[requestHeaderOperationsField]) != "[]" {
				t.Fatalf("unexpected fixed response update %s %s: %v", r.Method, r.URL, body.Policy)
			}
		case 2:
			if r.Method != http.MethodPut || r.URL.Path != path+"/configured" ||
				string(body.Policy[responseHeaderOperationsField]) != "[]" || len(body.Policy) != 1 {
				t.Fatalf("unexpected independent response clear %s %s: %v", r.Method, r.URL, body.Policy)
			}
		default:
			t.Fatalf("unexpected policy write %s %s", r.Method, r.URL)
		}
		call++
		_, _ = w.Write([]byte(`{"l7policy":{"id":"configured"}}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	disabled := false
	if _, err := service.CreatePolicy(t.Context(), PolicyCreateOpts{
		CreateOpts:               l7policies.CreateOpts{ListenerID: "listener", Action: l7policies.ActionRedirectToPool, RedirectPoolID: "pool", AdminStateUp: &disabled},
		RequestHeaderOperations:  []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderRemove, Name: "x-request"}},
		ResponseHeaderOperations: response,
	}); err != nil {
		t.Fatal(err)
	}
	clear := []cloud.HTTPHeaderOperation{}
	if _, err := service.SetPolicyFixedResponse(t.Context(), "configured", PolicyFilters{Request: clear, Response: response}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdatePolicyFilters(t.Context(), "configured", l7policies.UpdateOpts{}, PolicyFilters{Response: clear}); err != nil {
		t.Fatal(err)
	}
	observed, complete, err := service.ListPolicyObservations(t.Context(), l7policies.ListOpts{})
	if err != nil || !complete || len(observed) != len(records) ||
		!reflect.DeepEqual(observed[0].ResponseHeaderOperations, response) ||
		!reflect.DeepEqual(observed[1].ResponseHeaderOperations, clear) ||
		observed[2].ResponseHeaderOperations != nil || observed[2].ResponseHeaderOperationsErr != nil ||
		observed[3].ResponseHeaderOperationsErr == nil {
		t.Fatalf("incorrect response operation readback: %#v, error %v", observed, err)
	}
}

func TestRequestRedirectReadbackValidatesOptionalFieldsAndPath(t *testing.T) {
	for _, scenario := range []struct {
		name, fields string
		valid        bool
	}{
		{"omitted hostname and path", "", true},
		{"empty replacement", `,"replace_prefix":{"match":"/it's","replacement":""}`, true},
		{"mixed case DNS hostname", `,"hostname":"Moved.Example.test"`, true},
		{"empty full path", `,"replace_full_path":""`, true},
		{"empty explicit hostname", `,"hostname":""`, false},
		{"null hostname", `,"hostname":null`, false},
		{"null prefix replacement", `,"replace_prefix":null`, false},
		{"null full replacement", `,"replace_full_path":null`, false},
		{"invalid path prefix", `,"replace_prefix":{"match":"/a//b","replacement":"/changed"}`, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			payload := `{"id":"policy","action":"REDIRECT_REQUEST","redirect_pool_id":null,"redirect_http_code":308,"admin_state_up":true,"redirect_request":{"scheme":"https","port":443` + scenario.fields + `}}`
			observation, err := readPolicyObservation(json.RawMessage(payload))
			if err != nil || !observation.RedirectRequestPresent || observation.HasValidRequestRedirect() != scenario.valid {
				t.Fatalf("request redirect readback = %+v, %v, valid=%t", observation, err, scenario.valid)
			}
		})
	}
}

func TestRequestURLRewritePolicyWire(t *testing.T) {
	const wantRewrite = `{"hostname":"backend.example.test","replace_prefix":{"match":"/incoming","replacement":""}}`
	rewrite := &RequestURLRewrite{Hostname: "backend.example.test", ReplacePrefix: &PathPrefixReplacement{Match: "/incoming"}}
	disabled := false
	create := PolicyCreateOpts{
		CreateOpts:              l7policies.CreateOpts{ListenerID: "http", Action: l7policies.ActionRedirectToPool, RedirectPoolID: "pool", AdminStateUp: &disabled},
		RequestURLRewrite:       rewrite,
		RequestHeaderOperations: []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderRemove, Name: "x-example"}},
	}
	check := func(body map[string]any, err error, expected map[string]string) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		for field, want := range expected {
			value, present := body["l7policy"].(map[string]any)[field]
			if !present {
				t.Fatalf("%s was omitted", field)
			}
			raw, err := json.Marshal(value)
			if err != nil || string(raw) != want {
				t.Fatalf("%s = %s, %v, want %s", field, raw, err, want)
			}
		}
	}
	body, err := create.ToL7PolicyCreateMap()
	check(body, err, map[string]string{
		"action": `"REDIRECT_TO_POOL"`, "redirect_pool_id": `"pool"`, "admin_state_up": "false",
		requestURLRewriteField: wantRewrite, requestHeaderOperationsField: `[{"name":"x-example","operation":"REMOVE"}]`,
	})
	create.WeightedTargets = []WeightedHTTPTarget{{PoolID: "pool-a", Weight: 2}, {PoolID: "pool-b", Weight: 1}}
	body, err = create.ToL7PolicyCreateMap()
	check(body, err, map[string]string{"action": `"WEIGHTED_TARGETS"`, requestURLRewriteField: wantRewrite})
	body, err = (policyWithFilters{update: l7policies.UpdateOpts{}, filters: PolicyFilters{URLRewrite: &RequestURLRewriteUpdate{}}}).ToL7PolicyUpdateMap()
	check(body, err, map[string]string{requestURLRewriteField: "null", "admin_state_up": "false"})
	create.WeightedTargets = nil
	create.FixedServerError = true
	if _, err := create.ToL7PolicyCreateMap(); err == nil {
		t.Fatal("a fixed response accepted a forwarding filter")
	}

	for _, tc := range []struct {
		value                    string
		reported, present, valid bool
	}{
		{wantRewrite, true, true, true},
		{`{"replace_prefix":{"match":"/incoming","replacement":null}}`, true, true, false},
		{"null", true, false, false},
		{},
	} {
		row := `{"id":"policy","action":"REDIRECT_TO_POOL","redirect_pool_id":"pool","admin_state_up":false`
		if tc.value != "" {
			row += `,"request_url_rewrite":` + tc.value
		}
		got, err := readPolicyObservation(json.RawMessage(row + "}"))
		if err != nil || got.RequestURLRewriteReported != tc.reported || got.RequestURLRewritePresent != tc.present ||
			got.HasValidRequestURLRewrite() != tc.valid || tc.valid && !got.RequestURLRewrite.Equal(rewrite) {
			t.Errorf("rewrite readback %s = %+v, %v", tc.value, got, err)
		}
	}
}

func TestRequestURLRewriteRejectsContradictoryForwardingTargets(t *testing.T) {
	const row = `{"id":"policy","action":"REDIRECT_TO_POOL","redirect_pool_id":"pool","admin_state_up":true,"request_url_rewrite":{"hostname":"backend.example.test"}`
	for _, conflicting := range []string{
		`,"fixed_response":{"status_code":500}`,
		`,"targets":[{"pool_id":"another","weight":1}]`,
		`,"targets":[]`,
	} {
		got, err := readPolicyObservation(json.RawMessage(row + conflicting + "}"))
		if err != nil || !got.RequestURLRewritePresent || got.HasValidRequestURLRewrite() {
			t.Errorf("accepted a conflicting forwarding target %s: %+v, %v", conflicting, got, err)
		}
	}
}

func TestWeightedTargetRequestFiltersWireAndInventory(t *testing.T) {
	const vector = `[{"weight":10,"pool_id":"one","request_header_operations":[{"name":"backend","operation":"SET","value":"one"},{"name":"x-remove","operation":"REMOVE"}],"request_url_rewrite":{"hostname":"one.example.test","replace_prefix":{"match":"/old","replacement":"/new"}}},{"weight":10,"pool_id":"two"},{"weight":1,"fixed_response":{"status_code":500}}]`
	var targets []WeightedHTTPTarget
	if err := json.Unmarshal([]byte(vector), &targets); err != nil {
		t.Fatal(err)
	}
	if !targets[0].HasRequestFilters() || targets[1].HasRequestFilters() || targets[2].HasRequestFilters() {
		t.Fatalf("unexpected readback: %+v", targets)
	}
	wire, err := json.Marshal(targets)
	if err != nil || string(wire) != vector {
		t.Fatalf("mismatched provider payload: %s, %v", wire, err)
	}
	malformed := []string{
		`{"weight":1,"fixed_response":{"status_code":500},"request_header_operations":[]}`,
		`{"weight":1,"pool_id":"one","request_header_operations":[{"operation":"REMOVE","name":"x","value":""}]}`,
		`{"weight":1,"pool_id":"one","request_url_rewrite":{"replace_prefix":{"match":"/old"}}}`,
	}
	for _, target := range malformed {
		row := `{"id":"policy","action":"WEIGHTED_TARGETS","targets":[` + target + `,{"weight":10,"pool_id":"two"}]}`
		got, err := readPolicyObservation(json.RawMessage(row))
		if err != nil || got.Target != nil || !got.TargetRequestFiltersPresent || !slices.Contains(got.PoolIDs, "two") {
			t.Fatalf("malformed target lost its known dependencies or was accepted: %+v, %v", got, err)
		}
	}
}

func TestMixedTCPPolicyListPreservesExplicitModes(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/l7policies" {
			t.Fatalf("unexpected policy request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"l7policies":[
			{"id":"terminate","tcp_tls_passthrough":false},
			{"id":"passthrough","tcp_tls_passthrough":true},
			{"id":"unverified"}
		]}`))
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	policies, complete, err := service.ListPolicyObservations(t.Context(), l7policies.ListOpts{ListenerID: "mixed"})
	if err != nil || !complete || len(policies) != 3 {
		t.Fatalf("unexpected Octavia mixed policy inventory: %#v, complete=%t, %v", policies, complete, err)
	}
	terminate, passthrough := policies[0].TCPTLSPassthrough, policies[1].TCPTLSPassthrough
	if terminate == nil || *terminate || passthrough == nil || !*passthrough || policies[2].TCPTLSPassthrough != nil {
		t.Fatalf("explicit false, true and missing policy modes must remain distinct: %#v", policies)
	}
}
