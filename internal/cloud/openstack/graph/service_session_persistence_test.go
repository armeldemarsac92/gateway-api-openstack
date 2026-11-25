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
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestRouteSessionPersistencePoolWithdrawal(t *testing.T) {
	capabilities := []string{octavia.GatewayCapabilityHTTPCookieName}
	lbClient := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", r.Method+" "+r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: lbClient, ProjectID: "project-a"}, ProviderConfig{RouteSessionPersistence: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode = cloud.MemberModeEndpoint
	members := []cloud.Member{{Address: "192.0.2.20", Port: 8080}, {Address: "192.0.2.21", Port: 8080}}
	persisted := cloud.HTTPBackend{
		Namespace: "apps", Name: "checkout", Port: 8080, Members: members, SessionPersistenceRule: "name/checkout",
		SessionPersistenceServiceUID: "checkout-uid",
		ServiceSessionPersistence:    &cloud.ServiceSessionPersistence{CookieName: "checkout_session"},
		Matches:                      []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/checkout"}},
	}
	ordinary := cloud.HTTPBackend{
		Namespace: "apps", Name: "checkout", Port: 8080, Members: members,
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/ordinary", Precedence: 1}},
	}
	route.HTTPBackends = []cloud.HTTPBackend{persisted, ordinary}
	build := func() desiredGatewayGraph {
		graph, err := provider.buildDesiredGatewayGraph(spec)
		require.NoError(t, err)
		return graph
	}
	desired := build()
	partKey, plainKey := openstackidentity.HTTPBackendPoolKey(persisted), openstackidentity.HTTPBackendPoolKey(ordinary)
	require.NotEqual(t, partKey, plainKey)
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
	affinity := &observedRouteGraph{pool: &pools.Pool{
		ID: "affinity", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", AdminStateUp: true, Tags: []string{xBackendCookieTag},
		Persistence: pools.SessionPersistence{Type: httpCookiePersistence, CookieName: "checkout_session"},
	}, backendTLS: octavia.PoolBackendTLS{AdminObserved: true}, members: []pools.Member{{ID: "first", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}}
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{
		httpParts: map[string]*observedRouteGraph{partKey: affinity},
	}}}
	require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, desired.routes[0].sessionPersistenceIssue())
	available, err := provider.XBackendCookieAvailable(t.Context(), cloud.HTTPSessionCookieNamed)
	require.NoError(t, err)
	require.False(t, available)
	for _, part := range desired.routes[0].httpParts {
		if part.backendKey == plainKey {
			require.Empty(t, part.sessionCookieName)
			require.Equal(t, members, part.members)
		}
	}

	capabilities = []string{}
	desired = build()
	withdraw := provider.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, partKey, withdraw.backendKey)
	require.Equal(t, routeMutationClearPoolMembers, withdraw.route.kind)
	require.Equal(t, cloud.SessionPersistenceUnsupported, desired.routes[0].sessionPersistenceIssue())

	ordinary.Matches = append(ordinary.Matches, persisted.Matches...)
	route.HTTPBackends = []cloud.HTTPBackend{ordinary}
	desired = build()
	withdraw = provider.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, partKey, withdraw.backendKey)
	affinity.members = nil
	withdraw = provider.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, partKey, withdraw.backendKey)
	require.Equal(t, &pools.SessionPersistence{}, withdraw.route.poolPersistence)
	parts, _, _ := httpPlanningParts(desired, actual)
	require.Len(t, parts.routes, 2)
	require.Equal(t, plainKey, parts.routes[0].backendKey)
	require.Len(t, parts.routes[0].policies, 2)
	require.Equal(t, cloud.GraphEnsure, parts.spec.Routes[0].Disposition)
	require.Equal(t, partKey, parts.routes[1].backendKey)
	require.Equal(t, cloud.GraphDelete, parts.spec.Routes[1].Disposition)
}

func TestServiceSessionPersistenceSharesGRPCPoolWithRetryBudget(t *testing.T) {
	var updates []map[string]json.RawMessage
	lbClient := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{
				octavia.GatewayCapabilityHTTPHeaderPersistence, octavia.GatewayCapabilityBackendRetryBudget,
			}})
		case "PUT /lbaas/pools/reports":
			var request struct {
				Pool map[string]json.RawMessage `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			updates = append(updates, request.Pool)
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "reports"}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: lbClient, ProjectID: "project-a"}, ProviderConfig{
		BackendH2C: true, XBackendSessionPersistence: true, BackendRetryBudget: true, FixedHTTPResponses: true, WeightedHTTPTargets: true,
	})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.GRPC, route.MemberMode = true, cloud.MemberModeEndpoint
	header := &cloud.HTTPSessionHeaderPersistence{Name: "x-grpc-affinity", TimeoutMS: ptr.To(int64(20000))}
	budget := &cloud.BackendRetryBudget{
		ServiceUID: "reports-uid", BudgetPercent: 20, BudgetIntervalMS: 10000, MinRetryCount: 10, MinRetryIntervalMS: 1000,
	}
	route.HTTPBackends = []cloud.HTTPBackend{
		{Namespace: "apps", Name: "reports", Port: 8080, BackendH2C: true, Members: []cloud.Member{{Address: "192.0.2.20", Port: 8080}},
			ServiceSessionPersistence: &cloud.ServiceSessionPersistence{Header: header}, BackendRetryBudget: budget},
		{Namespace: "apps", Name: "other", Port: 8080, BackendH2C: true, Members: []cloud.Member{{Address: "192.0.2.21", Port: 8080}}},
	}
	route.WeightedHTTPRules = []cloud.HTTPWeightedRule{{
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/example.Reports/List"}},
		Targets: []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {BackendIndex: ptr.To(1), Weight: 1}},
	}}
	for _, name := range []string{"x-grpc-affinity-bin", "x+affinity", "user-agent", "grpc-timeout"} {
		header.Name = name
		_, err := provider.buildDesiredGatewayGraph(spec)
		require.ErrorIs(t, err, cloud.ErrTerminalValidation, name)
	}
	header.Name = "x-grpc-affinity"
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
	current := &observedRouteGraph{pool: &pools.Pool{
		ID: "reports", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", LBMethod: string(pools.LBMethodRoundRobin), AdminStateUp: true,
	}, backendH2C: true, backendTLS: octavia.PoolBackendTLS{AdminObserved: true}}
	part := &desired.routes[0].httpParts[0]
	for i := range desired.routes[0].httpParts {
		candidate := &desired.routes[0].httpParts[i]
		if candidate.sessionHeader != nil {
			part = candidate
		}
	}
	require.NotNil(t, part.sessionHeader)
	require.Equal(t, 3, weightedBackendDemand(desired.routes[0])[part.backendKey])
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{
		httpParts: map[string]*observedRouteGraph{part.backendKey: current},
	}}}
	require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
	provider.observeBackendRetryBudgetCapability(t.Context(), &desired, actual)
	require.Equal(t, cloud.SessionPersistenceHeaderReadbackUnverified, part.sessionPersistenceIssue())

	apply := func(expected routeMutationKind) {
		t.Helper()
		steps, _, err := buildRouteEnsurePlan(*part, *current)
		require.NoError(t, err)
		require.NotEmpty(t, steps)
		require.Equal(t, expected, steps[0].kind)
		require.NoError(t, provider.executeRouteMutation(t.Context(), *part, steps[0]))
	}
	apply(routeMutationSetXBackendPersistence)
	require.JSONEq(t, `{"type":"HTTP_HEADER","header_name":"x-grpc-affinity","header_absolute_timeout_ms":20000}`, string(updates[0]["session_persistence"]))
	require.NotContains(t, updates[0], "backend_retry_budget")
	current.pool.Tags = []string{xBackendHeaderTag}
	current.pool.Persistence.Type = cloud.HTTPHeaderPersistenceType
	current.headerPersistence = octavia.PoolHeaderPersistence{Observed: true, Value: header}
	require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, part.sessionPersistenceIssue())
	apply(routeMutationSetBackendRetryBudget)
	require.Contains(t, updates[1], "backend_retry_budget")
	require.NotContains(t, updates[1], "session_persistence")

	// Policy withdrawal clears affinity on the same pool before returning traffic.
	part.spec.ServiceSessionPersistence, part.sessionHeader = nil, nil
	current.backendRetryBudget = octavia.PoolBackendRetryBudget{Present: true, Value: budget}
	current.members = []pools.Member{{ID: "member", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
	withdraw := provider.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, routeMutationClearPoolMembers, withdraw.route.kind)
	require.Equal(t, part.backendKey, withdraw.backendKey)
	require.Equal(t, 3, weightedBackendDemand(desired.routes[0])[part.backendKey])
	current.members = nil
	apply(routeMutationSetXBackendPersistence)
	require.Equal(t, "null", string(updates[2]["session_persistence"]))
	require.NotContains(t, updates[2], "backend_retry_budget")
}

func TestServiceSessionPersistenceCreatesNamedCookieOnScalarHTTPPool(t *testing.T) {
	capabilities := []string{octavia.GatewayCapabilityHTTPCookieName, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout}
	var written json.RawMessage
	lbClient := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case "POST /lbaas/pools":
			var request struct {
				Pool map[string]json.RawMessage `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			written = request.Pool["session_persistence"]
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "checkout"}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	provider := NewProvider(ServiceClients{LoadBalancer: lbClient, ProjectID: "project-a"}, ProviderConfig{XBackendSessionPersistence: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.MemberMode = cloud.MemberModeEndpoint
	route.Members = []cloud.Member{{Address: "192.0.2.20", Port: 8080}}
	route.ServiceSessionPersistence = &cloud.ServiceSessionPersistence{
		CookieName: "checkout", CookiePath: ptr.To("/apps"),
		CookieDeadline: &cloud.HTTPSessionCookieDeadline{Scope: strings.Repeat("a", 64), TimeoutMS: 20000},
	}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	part := &desired.routes[0]
	require.Empty(t, part.httpParts)
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{}}}
	require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistencePathUnsupported, part.sessionPersistenceIssue())
	steps, _, err := buildRouteEnsurePlan(*part, actual.routes[0])
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.True(t, steps[0].persistenceFallback)

	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePath)
	require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
	steps, _, err = buildRouteEnsurePlan(*part, actual.routes[0])
	require.NoError(t, err)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationCreatePool, steps[0].kind)
	require.False(t, steps[0].persistenceFallback)
	require.NoError(t, provider.executeRouteMutation(t.Context(), *part, steps[0]))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"checkout","cookie_path":"/apps","cookie_absolute_timeout_ms":20000,"cookie_scope":"`+
		strings.Repeat("a", 64)+`"}`, string(written))
}
