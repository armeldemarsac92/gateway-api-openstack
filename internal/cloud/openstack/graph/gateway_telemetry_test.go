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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
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
	samplingCapable := false
	exporterState := "ACTIVE"
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
			if samplingCapable {
				capabilities = append(capabilities, octavia.GatewayCapabilityListenerOTLPSampling)
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
			if spec.Gateway.Telemetry == nil || !capable || spec.Gateway.Telemetry.Sampling != nil && !samplingCapable {
				require.JSONEq(t, "null", string(body.Listener["otlp_tracing"]))
			} else if spec.Gateway.Telemetry.Sampling != nil {
				require.JSONEq(t, `{"collector_pool_id":"975c99aa-e26a-4818-8391-b704f0712d1f","service_name":"apps/edge","span_name":"gateway.request","sampling":{"base":{"numerator":0,"denominator":100},"parent":{"numerator":1,"denominator":1}}}`, string(body.Listener["otlp_tracing"]))
			} else {
				require.JSONEq(t, `{"collector_pool_id":"975c99aa-e26a-4818-8391-b704f0712d1f","service_name":"apps/edge","span_name":"gateway.request"}`, string(body.Listener["otlp_tracing"]))
			}
			listener := frontends[r.URL.Path[len("/lbaas/listeners/"):]]
			listener["otlp_tracing"] = body.Listener["otlp_tracing"]
			listener["tags"] = body.Listener["tags"]
			listener["otlp_tracing_status"] = exporterState
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

	sampling := &cloud.TelemetrySampling{Base: cloud.TelemetryRate{Denominator: 100}, Parent: &cloud.TelemetryRate{Numerator: 1, Denominator: 1}}
	samplingCapable = true
	for _, desired := range []*cloud.TelemetrySampling{sampling, nil} {
		spec.Gateway.Telemetry.Sampling = desired
		calls = nil
		for range 2 {
			require.True(t, run().Outcome.MutationSubmitted)
		}
		require.Equal(t, cloud.OutcomeReady, run().Outcome.State)
		require.Equal(t, []string{"PUT /lbaas/listeners/http", "PUT /lbaas/listeners/https"}, calls)
	}

	calls = nil
	frontends["http"]["otlp_tracing_status"] = "PENDING_UPDATE"
	require.Equal(t, cloud.OutcomeProgressing, run().Outcome.State)
	delete(frontends["http"], "otlp_tracing_status")
	require.Equal(t, cloud.OutcomeProgressing, run().Outcome.State)
	require.Empty(t, calls)

	exporterState = "ERROR"
	frontends["http"]["otlp_tracing_status"] = exporterState
	require.True(t, run().Outcome.MutationSubmitted, "an identical export is reprogrammed after an exporter error")
	retry := run()
	require.Equal(t, cloud.OutcomeProgressing, retry.Outcome.State)
	require.False(t, retry.Outcome.MutationSubmitted)
	require.Greater(t, retry.Outcome.RequeueAfter, provider.pollInterval)
	require.Equal(t, []string{"PUT /lbaas/listeners/http"}, calls)
	exporterState = "ACTIVE"
	frontends["http"]["otlp_tracing_status"] = exporterState
	frontends["http"]["provisioning_status"] = "ERROR"
	require.Equal(t, cloud.OutcomeReady, run().Outcome.State, "export readiness is independent of the application listener")
	frontends["http"]["provisioning_status"] = "ACTIVE"

	foreign := map[string]any{"collector_pool_id": "fab28eae-2876-4dbe-a51a-bc4520788de3",
		"service_name": "operator.collector", "span_name": "foreign"}
	frontends["https"]["otlp_tracing"] = foreign
	frontends["tcp"]["protocol"] = "HTTP"
	frontends["tcp"]["tags"] = []string{"unknown-owner"}
	frontends["tcp"]["default_pool_id"] = map[string]string{"unreadable": poolID}
	frontends["tcp"]["otlp_tracing"] = map[string]any{"collector_pool_id": poolID,
		"service_name": "operator.shared", "span_name": "foreign neighbor"}
	frontends["http"]["otlp_tracing"] = map[string]any{"collector_pool_id": poolID,
		"service_name": "apps/edge", "span_name": "gateway.request", "sampling": map[string]any{"base": map[string]int{"denominator": 100}}}
	capable = false
	calls = nil
	require.True(t, run().Outcome.MutationSubmitted)
	require.Equal(t, []string{"PUT /lbaas/listeners/http"}, calls)
	require.Equal(t, cloud.OutcomeProgressing, run().Outcome.State)
	require.Equal(t, foreign, frontends["https"]["otlp_tracing"])
	require.Equal(t, []string{"PUT /lbaas/listeners/http"}, calls)

	frontends["tcp"]["protocol"] = "TCP"
	frontends["tcp"]["default_pool_id"] = nil
	delete(frontends["tcp"], "otlp_tracing")
	frontends["http"]["otlp_tracing_status"] = "PENDING_UPDATE"
	require.Equal(t, cloud.OutcomeProgressing, run().Outcome.State, "a pending null is insufficient to delete the pool")
	frontends["http"]["otlp_tracing_status"] = "ACTIVE"
	for range 2 {
		require.True(t, run().Outcome.MutationSubmitted)
	}
	settled := run()
	require.Equal(t, cloud.OutcomeReady, settled.Outcome.State)
	require.Contains(t, settled.TelemetryUnsupported, octavia.GatewayCapabilityListenerOTLPTracing)
	require.True(t, settled.TelemetryCapabilityUnavailable)
	require.Equal(t, []string{"PUT /lbaas/listeners/http",
		"DELETE /lbaas/pools/" + poolID + "/members/collector", "DELETE /lbaas/pools/" + poolID}, calls)

	capable, samplingCapable = true, false
	spec.Gateway.Telemetry.Sampling = sampling
	calls = nil
	settled = run()
	require.Contains(t, settled.TelemetryUnsupported, octavia.GatewayCapabilityListenerOTLPSampling)
	require.False(t, settled.TelemetryCapabilityUnavailable, "the v1 tracing capability can still report the policy's ancestor")
	require.Empty(t, calls, "a regional v1 provider must not receive the sampling object")
}

func TestTelemetryCollectorTLSUsesExplicitHTTP11(t *testing.T) {
	identity := safetyTestIdentity(t)
	wanted := &cloud.GatewayTelemetry{ServiceUID: "collector", ServicePort: 4318, BackendTLS: newGraphBackendTLS(t)}
	var sent struct{ Pool pools.Pool }
	var writes []string
	handle := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/octavia/v2.0/lbaas/pools", request.URL.Path)
		require.NoError(t, json.NewDecoder(request.Body).Decode(&sent))
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "collector"}})
	})
	clients := scalarBackendTLSClients(t, identity.Value().OpenStackProjectID, handle, &writes)
	provider := newTestProvider(t, clients, ProviderConfig{BackendTLSPolicy: true, BackendCA: backendca.New(identity.Value().ClusterID, clients)})
	graph := telemetryGraph{identity: identity, loadBalancer: &octavia.LoadBalancer{}, spec: cloud.GatewayGraphSpec{
		Identity: identity.Value(), ValidateMutation: func(context.Context) error { return nil },
	}}
	graph.loadBalancer.ID = "gateway"
	for range 2 {
		result, err := provider.createTelemetryPool(t.Context(), graph, "collector", wanted)
		require.NoError(t, err)
		require.True(t, result.Outcome.MutationSubmitted)
	}
	require.Equal(t, []string{"http/1.1"}, sent.Pool.ALPNProtocols)
	require.True(t, sent.Pool.TLSEnabled)
	require.NotEmpty(t, sent.Pool.CATLSContainerRef)
	tls := octavia.PoolBackendTLS{Hostname: wanted.BackendTLS.Hostname, HostnameObserved: true, CAObserved: true,
		ClientRefObserved: true, EnabledObserved: true, AdminObserved: true}
	pool := telemetryPool{observedAuthPool: observedAuthPool{pool: sent.Pool}, tls: tls}
	require.True(t, telemetryPoolTLSMatches(pool, wanted.BackendTLS, sent.Pool.CATLSContainerRef, ""))
	pool.pool.ALPNProtocols = nil
	require.False(t, telemetryPoolTLSMatches(pool, wanted.BackendTLS, sent.Pool.CATLSContainerRef, ""))
	pool.pool.ALPNProtocols = []string{"h2", "http/1.1"}
	require.False(t, telemetryPoolTLSMatches(pool, wanted.BackendTLS, sent.Pool.CATLSContainerRef, ""))
}
