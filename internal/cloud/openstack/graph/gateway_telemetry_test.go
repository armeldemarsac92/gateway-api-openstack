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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/stretchr/testify/require"
)

func TestTelemetryGatewayReconcileUpdatesOnlyTraceExportsAndOwnedCollector(t *testing.T) {
	id := safetyTestIdentity(t)
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["listeners"] = []map[string]string{{"id": "http"}, {"id": "https"}, {"id": "tcp"}}
	lb["pools"] = []any{}
	frontend := func(name, protocol string, port int) map[string]any {
		return map[string]any{"id": name, "project_id": "project-a", "protocol": protocol, "protocol_port": port,
			"provisioning_status": "ACTIVE", "admin_state_up": true, "default_pool_id": nil,
			"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "tags": safetyGatewayTags(t, id, roleListener)}
	}
	frontends := map[string]map[string]any{
		"http": frontend("http", "HTTP", 80), "https": frontend("https", "TERMINATED_HTTPS", 443),
		"tcp": frontend("tcp", "TCP", 5443),
	}
	spec := cloud.GatewayGraphSpec{
		Identity: gatewayIdentity(id.Value()),
		Gateway: cloud.GatewayGraphNode{
			Disposition: cloud.GraphReconcileTelemetry,
			Telemetry: &cloud.GatewayTelemetry{
				Listeners:  []cloud.TelemetryListener{{Port: 80, Protocol: "HTTP"}, {Port: 443, Protocol: "TERMINATED_HTTPS"}},
				ServiceUID: "service-uid", ServicePort: 4318, ServiceName: "apps/edge", SpanName: "gateway.request",
				Members: []cloud.Member{{Address: "192.0.2.51", Port: 4318}}, MemberSubnetID: "member-subnet",
			},
		},
		ValidateMutation: func(context.Context) error { return nil },
	}
	const poolID = "975c99aa-e26a-4818-8391-b704f0712d1f"
	var pool, member map[string]any
	var calls []string
	capable := true
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.Method + " " + r.URL.Path
		if r.Method != http.MethodGet {
			calls = append(calls, request)
		}
		switch request {
		case "GET /lbaas/loadbalancers":
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
		case "GET /lbaas/loadbalancers/load-balancer-1":
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
		case "GET /lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{frontends["http"], frontends["https"], frontends["tcp"]}})
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			capabilities := []string{}
			if capable {
				capabilities = append(capabilities, octavia.GatewayCapabilityListenerOTLPTracing)
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case "GET /lbaas/pools":
			rows := []any{}
			if pool != nil {
				rows = append(rows, pool)
			}
			safetyWriteJSON(t, w, map[string]any{"pools": rows})
		case "GET /lbaas/pools/" + poolID + "/members":
			rows := []any{}
			if member != nil {
				rows = append(rows, member)
			}
			safetyWriteJSON(t, w, map[string]any{"members": rows})
		case "POST /lbaas/pools":
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			pool = body.Pool
			require.Equal(t, "HTTP", pool["protocol"])
			require.Equal(t, "load-balancer-1", pool["loadbalancer_id"])
			require.NotContains(t, pool, "listener_id")
			pool["id"], pool["project_id"], pool["provisioning_status"] = poolID, "project-a", "ACTIVE"
			pool["members"], pool["listeners"], pool["loadbalancers"] = []any{}, []any{}, []map[string]string{{"id": "load-balancer-1"}}
			lb["pools"] = []map[string]string{{"id": poolID}}
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		case "POST /lbaas/pools/" + poolID + "/members":
			var body struct{ Member map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			member = body.Member
			require.Equal(t, "192.0.2.51", member["address"])
			require.Equal(t, float64(4318), member["protocol_port"])
			member["id"], member["project_id"], member["provisioning_status"] = "collector", "project-a", "ACTIVE"
			pool["members"] = []map[string]string{{"id": "collector"}}
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"member": member})
		case "PUT /lbaas/listeners/http", "PUT /lbaas/listeners/https":
			var body struct{ Listener map[string]json.RawMessage }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Len(t, body.Listener, 2)
			require.Contains(t, body.Listener, "otlp_tracing")
			if spec.Gateway.Telemetry == nil || !capable {
				require.JSONEq(t, "null", string(body.Listener["otlp_tracing"]))
			} else {
				require.JSONEq(t, `{"collector_pool_id":"975c99aa-e26a-4818-8391-b704f0712d1f","service_name":"apps/edge","span_name":"gateway.request"}`, string(body.Listener["otlp_tracing"]))
			}
			listener := frontends[r.URL.Path[len("/lbaas/listeners/"):]]
			listener["otlp_tracing"] = body.Listener["otlp_tracing"]
			listener["tags"] = body.Listener["tags"]
			w.WriteHeader(http.StatusAccepted)
			safetyWriteJSON(t, w, map[string]any{"listener": listener})
		case "DELETE /lbaas/pools/" + poolID + "/members/collector":
			member, pool["members"] = nil, []any{}
			w.WriteHeader(http.StatusNoContent)
		case "DELETE /lbaas/pools/" + poolID:
			pool, lb["pools"] = nil, []any{}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected Octavia request %s", request)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	run := func() cloud.GatewayGraphResult {
		result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
		require.NoError(t, err)
		return result
	}
	stale := errors.New("stale collector authority")
	spec.ValidateMutation = func(context.Context) error { return stale }
	_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorIs(t, err, stale)
	require.Empty(t, calls)

	spec.ValidateMutation = func(context.Context) error { return nil }
	for range 4 {
		require.True(t, run().Outcome.MutationSubmitted)
	}
	require.Equal(t, cloud.OutcomeReady, run().Outcome.State)
	require.Equal(t, []string{"POST /lbaas/pools", "POST /lbaas/pools/" + poolID + "/members", "PUT /lbaas/listeners/http", "PUT /lbaas/listeners/https"}, calls)
	require.Equal(t, true, frontends["http"]["admin_state_up"])
	require.Equal(t, true, frontends["https"]["admin_state_up"])
	require.NotContains(t, frontends["tcp"], "otlp_tracing")

	capable = false
	calls = nil
	for range 4 {
		require.True(t, run().Outcome.MutationSubmitted)
	}
	settled := run()
	require.Equal(t, cloud.OutcomeReady, settled.Outcome.State)
	require.Contains(t, settled.TelemetryUnsupported, octavia.GatewayCapabilityListenerOTLPTracing)
	require.Equal(t, []string{"PUT /lbaas/listeners/http", "PUT /lbaas/listeners/https",
		"DELETE /lbaas/pools/" + poolID + "/members/collector", "DELETE /lbaas/pools/" + poolID}, calls)
}
