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
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

func TestHTTPCORSPolicyReconciliation(t *testing.T) {
	type record = map[string]any
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	port := spec.Gateway.Spec.ListenerPort
	spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "cors", Port: port}, {Name: "ordinary", Port: port + 1}}
	route := &spec.Routes[0].Spec
	route.Members, route.Hostname, route.PathType, route.PathValue = nil, "", "", ""
	route.ScopedListenerPorts = []int{port}
	route.Gateway.ListenerID = ""
	route.WeightedHTTPRules = []cloud.HTTPWeightedRule{{Targets: []cloud.HTTPWeightedTarget{{Weight: 3}, {Weight: 1}},
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/cors", CORS: &cloud.HTTPCORS{}}}}}
	refs := func(id string) []map[string]string { return []map[string]string{{"id": id}} }
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["listeners"], lb["pools"] = append(refs("cors"), refs("ordinary")...), []record{}
	newListener := func(name string, port int) record {
		return record{"id": name, "project_id": "project-a", "protocol_port": port, "protocol": "HTTP",
			"provisioning_status": "ACTIVE", "admin_state_up": true, "default_pool_id": nil,
			"loadbalancers": refs(lb["id"].(string)), "l7policies": []record{},
			"default_http_response": record{"status_code": 404},
			"tags":                  append(safetyGatewayTags(t, id, roleListener), scopedPortTag(cloud.ListenerProtocolHTTP, port))}
	}
	listener, ordinary := newListener("cors", port), newListener("ordinary", port+1)
	const policyPath = "/lbaas/l7policies/policy"
	const rulePath = policyPath + "/rules"
	const listenerPath = "/lbaas/listeners/cors"
	const capabilityPath = "/lbaas/providers/amphora/gateway_capabilities"
	reads := map[string]record{
		"/lbaas/loadbalancers":                      {"loadbalancers": []record{lb}},
		"/lbaas/loadbalancers/" + lb["id"].(string): {"loadbalancer": lb},
		"/lbaas/listeners":                          {"listeners": []record{listener, ordinary}},
		"/lbaas/pools":                              {"pools": []record{}},
	}
	var policy record
	rules := []record{}
	advertised, hideCORS, hideRules := true, false, false
	var writes []string
	var patches []record
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path
		if req.Method == http.MethodGet {
			switch path {
			case capabilityPath:
				capabilities := []string{octavia.GatewayCapabilityFixedHTTPResponses, octavia.GatewayCapabilityWeightedHTTPTargets}
				if advertised {
					capabilities = append(capabilities, octavia.GatewayCapabilityRequestCORS)
				}
				safetyWriteJSON(t, w, record{"gateway_capabilities": capabilities})
				return
			case "/lbaas/l7policies":
				rows := []record{}
				if policy != nil && req.URL.Query().Get("listener_id") == "cors" {
					observed := maps.Clone(policy)
					if hideCORS {
						delete(observed, "request_cors")
					}
					rows = append(rows, observed)
				}
				safetyWriteJSON(t, w, record{"l7policies": rows})
				return
			case rulePath:
				if hideRules {
					safetyWriteJSON(t, w, record{"rules": []record{}})
				} else {
					safetyWriteJSON(t, w, record{"rules": rules})
				}
				return
			}
			if body, ok := reads[path]; ok {
				safetyWriteJSON(t, w, body)
				return
			}
		}
		writes = append(writes, req.Method+" "+path)
		var body map[string]record
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		var resource record
		var kind string
		switch {
		case req.Method == http.MethodPut && path == listenerPath:
			resource, kind = listener, "listener"
		case req.Method == http.MethodPost && path == "/lbaas/l7policies":
			if listener["admin_state_up"] != false || !advertised {
				t.Fatalf("created CORS policy while listener was open or capability absent: %v", body)
			}
			resource, kind = record{"id": "policy", "project_id": "project-a", "provisioning_status": "ACTIVE",
				"redirect_pool_id": nil, "rules": []record{}}, "l7policy"
			policy = resource
			listener["l7policies"] = refs("policy")
			w.WriteHeader(http.StatusCreated)
		case req.Method == http.MethodPost && path == rulePath:
			resource, kind = record{"id": "predicate", "project_id": "project-a", "provisioning_status": "ACTIVE", "invert": false}, "rule"
			rules = append(rules, resource)
			policy["rules"] = refs("predicate")
			w.WriteHeader(http.StatusCreated)
		case req.Method == http.MethodPut && path == policyPath:
			resource, kind = policy, "l7policy"
		default:
			t.Fatalf("unexpected request %s %s", req.Method, path)
		}
		patch := body[kind]
		patches = append(patches, patch)
		maps.Copy(resource, patch)
		if cors, fields := patch["request_cors"]; fields && cors == nil {
			delete(resource, "request_cors")
		}
		safetyWriteJSON(t, w, record{kind: resource})
	})

	reconcile := func(want string, requireIncomplete bool) cloud.GatewayGraphResult {
		t.Helper()
		writes, patches = nil, nil
		p := completeGraphProvider(t, handler, &gatewayTransitionRequests{})
		p.httpCORS, p.fixedHTTPResponses, p.weightedHTTPTargets = true, true, true
		result, err := p.ReconcileGatewayGraph(t.Context(), spec)
		var incomplete *httpFilterInventoryError
		if requireIncomplete && (!errors.As(err, &incomplete) || incomplete.filter != "CORS") || !requireIncomplete && err != nil {
			t.Fatalf("reconcile for %q: %v", want, err)
		}
		var expected []string
		if want != "" {
			expected = []string{want}
		}
		if !slices.Equal(writes, expected) {
			t.Fatalf("wanted %v, got %v (%+v)", expected, writes, result.Outcome)
		}
		return result
	}
	pending := func(outcome cloud.Outcome) {
		t.Helper()
		if outcome.State != cloud.OutcomeProgressing {
			t.Fatalf("expected pending while CORS is unverified, got %+v", outcome)
		}
	}
	ready := func(result cloud.GatewayGraphResult) {
		t.Helper()
		if result.Gateway.Outcome.State != cloud.OutcomeReady || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("verified CORS graph did not become ready: %+v", result)
		}
	}

	pending(reconcile("PUT "+listenerPath, false).Gateway.Outcome)
	reconcile("POST /lbaas/l7policies", false)
	cors, ok := policy["request_cors"].(map[string]any)
	if !ok || len(cors) != 0 || policy["action"] != "WEIGHTED_TARGETS" || len(policy["targets"].([]any)) != 2 {
		t.Fatalf("restrictive CORS object or weighted targets lost: %v", policy)
	}
	if _, ok := policy["redirect_pool_id"]; !ok || policy["redirect_pool_id"] != nil {
		t.Fatalf("weighted policy acquired a redirect pool: %v", policy)
	}
	reconcile("POST "+rulePath, false)
	reconcile("PUT "+policyPath, false)
	if _, sent := patches[0]["request_cors"]; sent {
		t.Fatalf("an unrelated administrative update overwrote request_cors: %v", patches[0])
	}
	reconcile("PUT "+listenerPath, false)
	ready(reconcile("", false))

	spec.Routes[0].Spec.WeightedHTTPRules[0].Matches[0].CORS = &cloud.HTTPCORS{
		AllowOrigins: []string{"https://*.bar.com:443"}, AllowCredentials: new(true),
		AllowMethods: []string{"*"}, AllowHeaders: []string{"*"}, ExposeHeaders: []string{"X-Trace"}, MaxAge: 3600,
	}
	reconcile("PUT "+listenerPath, false)
	reconcile("PUT "+policyPath, false)
	updated, ok := patches[0]["request_cors"].(map[string]any)
	if !ok || len(updated) != 6 || updated["max_age"] != float64(3600) ||
		updated["allow_credentials"] != true || policy["admin_state_up"] != false {
		t.Fatalf("expected all settings on the closed policy: %v", patches[0])
	}
	reconcile("PUT "+policyPath, false)
	reconcile("PUT "+listenerPath, false)
	ready(reconcile("", false))

	advertised = false
	pending(reconcile("PUT "+listenerPath, false).Gateway.Outcome)
	pending(reconcile("", false).Routes[0].Result.Outcome)
	if listener["admin_state_up"] != false || ordinary["admin_state_up"] != true || policy["request_cors"] == nil {
		t.Fatalf("capability withdrawal erased CORS or affected the sibling: %v %v", listener, policy)
	}
	advertised, hideCORS = true, true
	pending(reconcile("", false).Gateway.Outcome)
	hideCORS = false
	reconcile("PUT "+listenerPath, false)
	ready(reconcile("", false))

	hideRules = true
	pending(reconcile("PUT "+listenerPath, true).Gateway.Outcome)
	pending(reconcile("", true).Routes[0].Result.Outcome)
	hideRules = false
	reconcile("PUT "+listenerPath, false)
	ready(reconcile("", false))

	advertised = false
	spec.Routes[0].Spec.WeightedHTTPRules[0].Matches[0].CORS = nil
	reconcile("PUT "+listenerPath, false)
	reconcile("PUT "+policyPath, false)
	if sent, found := patches[0]["request_cors"]; !found || sent != nil ||
		slices.ContainsFunc(policy["tags"].([]any), func(tag any) bool { return tag == openstackidentity.RequestCORSPolicyTag }) {
		t.Fatalf("withdrawal did not explicitly clear only CORS and its tag: %v", patches[0])
	}
	reconcile("PUT "+policyPath, false)
	reconcile("PUT "+listenerPath, false)
	ready(reconcile("", false))
}
