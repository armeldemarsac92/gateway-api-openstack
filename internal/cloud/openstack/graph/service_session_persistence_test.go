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
	"slices"
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

func TestRouteSessionPersistenceRequiresTLSAndAffinityReadback(t *testing.T) {
	for _, tc := range []struct {
		name, capability string
		grpc             bool
		persistence      cloud.ServiceSessionPersistence
	}{
		{name: "HTTP Cookie", capability: octavia.GatewayCapabilityHTTPCookieName,
			persistence: cloud.ServiceSessionPersistence{CookieName: "checkout_session"}},
		{name: "gRPC Header", capability: octavia.GatewayCapabilityHTTPHeaderPersistence, grpc: true,
			persistence: cloud.ServiceSessionPersistence{Header: &cloud.HTTPSessionHeaderPersistence{Name: "x-session"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tlsCapabilities := []string{octavia.GatewayCapabilityBackendTLSHostname, octavia.GatewayCapabilityBackendTLSSystemCA}
			advertised := []string{tc.capability}
			var created struct{ Pool map[string]any }
			lbClient := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "GET /lbaas/providers/amphora/gateway_capabilities":
					safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
				case "POST /lbaas/pools":
					require.NoError(t, json.NewDecoder(r.Body).Decode(&created))
					w.WriteHeader(http.StatusCreated)
					safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "protected"}})
				default:
					t.Errorf("unexpected Octavia operation %s %s", r.Method, r.URL.Path)
				}
			}))
			provider := NewProvider(ServiceClients{LoadBalancer: lbClient, ProjectID: "project-a"},
				ProviderConfig{BackendTLSPolicy: true, RouteSessionPersistence: true})
			spec := completeGraphSpec(safetyTestIdentity(t))
			route := &spec.Routes[0].Spec
			route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
			route.MemberMode, route.GRPC, route.BackendTLSObserved = cloud.MemberModeEndpoint, tc.grpc, true
			tls := &cloud.BackendTLSValidation{Trust: cloud.BackendTLSTrustSystem, Hostname: "backend.example.test"}
			backend := cloud.HTTPBackend{
				Namespace: "apps", Name: "checkout", Port: 443, BackendTLS: tls,
				SessionPersistenceRule: "name/checkout", SessionPersistenceServiceUID: "checkout-uid",
				ServiceSessionPersistence: &tc.persistence, Members: []cloud.Member{{Address: "192.0.2.20", Port: 8443}},
				Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/checkout"}},
			}
			route.HTTPBackends = []cloud.HTTPBackend{backend}
			desired, err := provider.buildDesiredGatewayGraph(spec)
			require.NoError(t, err)
			key := openstackidentity.HTTPBackendPoolKey(backend)
			part := &desired.routes[0].httpParts[0]
			lb := safetyObservedLoadBalancer()
			lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
			current := &observedRouteGraph{}
			actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{
				httpParts: map[string]*observedRouteGraph{key: current},
			}}}
			prepare := func() *graphMutation {
				t.Helper()
				return provider.prepareBackendTLS(t.Context(), &desired, actual)
			}
			plan := func() routeMutation {
				t.Helper()
				steps, _, err := buildRouteEnsurePlan(*part, *current)
				require.NoError(t, err)
				require.NotEmpty(t, steps)
				return steps[0]
			}
			// Selecting BackendTLSPolicy closes the previous cleartext pool
			// before transport changes. A sibling can keep its own pool.
			current.pool = &pools.Pool{ID: "protected", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", AdminStateUp: true}
			current.backendTLS.AdminObserved = true
			current.members = []pools.Member{{ID: "old-clear", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
			withdraw := prepare()
			require.NotNil(t, withdraw)
			require.Equal(t, key, withdraw.backendKey)
			require.Equal(t, routeMutationSetBackendTLS, withdraw.route.kind)
			require.Equal(t, ptr.To(false), withdraw.route.poolAdmin)
			*current = observedRouteGraph{}

			// Stock Octavia drops a match when its pool is disabled. Keep a
			// new, verified empty fallback enabled until TLS can be staged.
			require.Nil(t, prepare())
			require.Equal(t, backendTLSBlocked, part.backendTLS.phase)
			require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
			fallback := plan()
			require.Equal(t, routeMutationCreatePool, fallback.kind)
			require.Equal(t, []string{backendTLSRefusalTag}, *fallback.poolTags)
			require.NoError(t, provider.executeRouteMutation(t.Context(), *part, fallback))
			require.Equal(t, true, created.Pool["admin_state_up"])
			require.Contains(t, created.Pool["tags"], backendTLSRefusalTag)
			require.NotContains(t, created.Pool, "backend_tls_hostname")
			require.NotContains(t, created.Pool, "session_persistence")
			current.pool = &pools.Pool{ID: "protected", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", AdminStateUp: true, Tags: *fallback.poolTags}
			current.backendTLS = octavia.PoolBackendTLS{AdminObserved: true, EnabledObserved: true, CAObserved: true, ClientRefObserved: true}
			require.Nil(t, prepare())
			policy := plan()
			require.Equal(t, routeMutationCreatePolicy, policy.kind)
			require.Equal(t, current.pool.ID, policy.parentID)

			// Removing affinity and policy together changes the pool key.
			// Only a fresh inventory permits retiring the empty stock pool.
			plain := backend
			plain.SessionPersistenceRule, plain.SessionPersistenceServiceUID = "", ""
			plain.BackendTLS, plain.ServiceSessionPersistence = nil, nil
			replacement := spec
			replacement.Routes = slices.Clone(spec.Routes)
			replacement.Routes[0].Spec.HTTPBackends = []cloud.HTTPBackend{plain}
			for _, observed := range []bool{false, true} {
				replacement.Routes[0].Disposition = cloud.GraphEnsure
				replacement.Routes[0].Spec.BackendTLSObserved = observed
				next, err := provider.buildDesiredGatewayGraph(replacement)
				require.NoError(t, err)
				require.Nil(t, provider.prepareBackendTLS(t.Context(), &next, actual))
				parts, _, _ := httpPlanningParts(next, actual)
				old := slices.IndexFunc(parts.routes, func(part desiredRoute) bool { return part.backendKey == key })
				require.NotEqual(t, -1, old)
				if observed {
					require.Equal(t, cloud.GraphDelete, parts.spec.Routes[old].Disposition)
				} else {
					require.Equal(t, cloud.GraphPreserve, parts.spec.Routes[old].Disposition)
				}
			}

			// On restart the policy informer can be disabled. The same rule
			// still selects the stock refusal pool and cannot add a member.
			part.spec.BackendTLS, part.spec.BackendTLSObserved = nil, false
			provider.backendTLSPolicy = false
			require.Nil(t, prepare())
			require.Equal(t, backendTLSBlocked, part.backendTLS.phase)
			require.Equal(t, routeMutationCreatePolicy, plan().kind)
			part.spec.BackendTLSObserved, provider.backendTLSPolicy = true, true
			require.Nil(t, prepare())
			require.Equal(t, backendTLSRelease, part.backendTLS.phase)
			release := plan()
			require.Equal(t, routeMutationSetBackendTLS, release.kind)
			require.NotContains(t, *release.poolTags, backendTLSRefusalTag)
			current.pool.Tags = *release.poolTags
			part.backendTLS = backendTLSPlan{} // the next reconciliation rebuilds desired
			require.Nil(t, prepare())
			require.Equal(t, backendTLSUnmanaged, part.backendTLS.phase)
			part.spec.BackendTLS = tls

			current.pool.Tags = []string{backendTLSManagedTag}
			withdraw = prepare()
			require.NotNil(t, withdraw)
			require.Equal(t, routeMutationSetBackendTLS, withdraw.route.kind)
			require.Equal(t, ptr.To(false), withdraw.route.poolAdmin)
			*current = observedRouteGraph{}

			advertised = append(tlsCapabilities, tc.capability)
			if tc.grpc {
				require.Nil(t, prepare())
				require.Equal(t, backendTLSBlocked, part.backendTLS.phase)
				advertised = append(advertised, octavia.GatewayCapabilityBackendTLSH2ALPN)
			}
			require.Nil(t, prepare())
			require.Equal(t, backendTLSConfigure, part.backendTLS.phase)
			require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
			create := plan()
			require.Equal(t, routeMutationCreatePool, create.kind)
			require.NoError(t, provider.executeRouteMutation(t.Context(), *part, create))
			require.Equal(t, false, created.Pool["admin_state_up"])
			require.Equal(t, true, created.Pool["tls_enabled"])
			require.Equal(t, true, created.Pool["backend_tls_system_ca"])
			require.Equal(t, tls.Hostname, created.Pool["backend_tls_hostname"])
			require.NotContains(t, created.Pool, "backend_h2c")
			require.NotContains(t, created.Pool, "session_persistence")
			if tc.grpc {
				require.Equal(t, true, created.Pool["backend_tls_require_h2"])
			} else {
				require.NotContains(t, created.Pool, "backend_tls_require_h2")
			}

			// TLS and affinity are acknowledged separately while the pool is closed.
			current.pool = &pools.Pool{ID: "protected", Protocol: "HTTP", ProvisioningStatus: "ACTIVE",
				TLSEnabled: true, LBMethod: string(pools.LBMethodRoundRobin), Tags: *create.poolTags}
			current.backendTLS = octavia.PoolBackendTLS{Hostname: tls.Hostname, HostnameObserved: true, CAObserved: true, ClientRefObserved: true,
				EnabledObserved: true, AdminObserved: true, RequireH2: tc.grpc, RequireH2Observed: true,
				SystemCA: true, SystemCAObserved: true, SystemCADigestObserved: true}
			require.Nil(t, prepare())
			require.True(t, part.backendTLS.stagesSystemCA())
			require.Nil(t, provider.prepareXBackendPersistence(t.Context(), &desired, actual))
			affinity := plan()
			require.Equal(t, routeMutationSetXBackendPersistence, affinity.kind)
			require.Nil(t, affinity.poolAdmin)
			current.pool.Tags = *affinity.poolTags
			if tc.grpc {
				require.Equal(t, tc.persistence.Header, affinity.poolHeaderPersistence)
				current.pool.Persistence = pools.SessionPersistence{Type: cloud.HTTPHeaderPersistenceType}
				require.Equal(t, routeMutationSetXBackendPersistence, plan().kind)
				current.headerPersistence = octavia.PoolHeaderPersistence{Value: tc.persistence.Header, Observed: true}
			} else {
				require.Equal(t, &pools.SessionPersistence{Type: httpCookiePersistence, CookieName: tc.persistence.CookieName}, affinity.poolPersistence)
				require.Equal(t, routeMutationSetXBackendPersistence, plan().kind)
				current.pool.Persistence = *affinity.poolPersistence
			}
			open := plan()
			require.Equal(t, routeMutationSetBackendTLS, open.kind)
			require.Equal(t, ptr.To(true), open.poolAdmin)
			activation := graphMutation{kind: graphRouteMutation, backendKey: key, route: open}
			_, err = provider.prepareGraphMutation(t.Context(), desired, actual, activation)
			require.NoError(t, err)

			advertised = tlsCapabilities
			if tc.grpc {
				advertised = append(advertised, octavia.GatewayCapabilityBackendTLSH2ALPN)
			}
			_, err = provider.prepareGraphMutation(t.Context(), desired, actual, activation)
			var missing *unavailableRouteCapability
			require.ErrorAs(t, err, &missing)
			require.Equal(t, tc.capability, missing.capability)
			withdraw = provider.prepareXBackendPersistence(t.Context(), &desired, actual)
			require.NotNil(t, withdraw)
			require.Equal(t, routeMutationSetXBackendPersistence, withdraw.route.kind)
			require.Equal(t, &pools.SessionPersistence{}, withdraw.route.poolPersistence)

			// A previously serving pool must close before removing its affinity.
			current.pool.AdminStateUp = true
			current.backendTLS.SystemCASHA256 = strings.Repeat("a", 64)
			current.members = []pools.Member{{ID: "serving", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
			require.Nil(t, prepare())
			withdraw = provider.prepareXBackendPersistence(t.Context(), &desired, actual)
			require.NotNil(t, withdraw)
			require.Equal(t, key, withdraw.backendKey)
			require.Equal(t, routeMutationSetBackendTLS, withdraw.route.kind)
			require.Equal(t, ptr.To(false), withdraw.route.poolAdmin)
			current.pool.AdminStateUp = false
			withdraw = provider.prepareXBackendPersistence(t.Context(), &desired, actual)
			require.NotNil(t, withdraw)
			require.Equal(t, routeMutationClearPoolMembers, withdraw.route.kind)
			current.members = nil
			current.pool.Persistence = pools.SessionPersistence{}
			current.headerPersistence = octavia.PoolHeaderPersistence{Observed: true}
			next := plan()
			require.Equal(t, routeMutationCreatePolicy, next.kind)
			require.Equal(t, current.pool.ID, next.parentID)
			require.Nil(t, next.poolAdmin)
		})
	}
}

func TestInlineTLSWithdrawalPreservesServingOrdinaryPoolDuringBundleOutage(t *testing.T) {
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode, route.BackendTLSObserved = cloud.MemberModeEndpoint, true
	ordinary := cloud.HTTPBackend{Namespace: "apps", Name: "checkout", Port: 80,
		BackendTLS: &cloud.BackendTLSValidation{Denied: true}, Unavailable: true,
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/ordinary"}}}
	inline := ordinary
	inline.SessionPersistenceRule, inline.SessionPersistenceServiceUID = "name/inline", "checkout-service"
	inline.Matches = []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/inline", Precedence: 1}}
	route.HTTPBackends = []cloud.HTTPBackend{ordinary, inline}
	provider := NewProvider(ServiceClients{ProjectID: "project-a"}, ProviderConfig{BackendTLSPolicy: true, RouteSessionPersistence: true})
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	ordinaryKey, inlineKey := openstackidentity.HTTPBackendPoolKey(ordinary), openstackidentity.HTTPBackendPoolKey(inline)
	lb := safetyObservedLoadBalancer()
	lb.ProvisioningStatus = "ACTIVE"
	clear := &observedRouteGraph{pool: &pools.Pool{ID: "inline", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		backendTLS: octavia.PoolBackendTLS{AdminObserved: true}, members: []pools.Member{{ID: "clear-member"}}}
	verified := &observedRouteGraph{pool: &pools.Pool{ID: "ordinary", AdminStateUp: true, TLSEnabled: true, CATLSContainerRef: "ca",
		Tags: []string{backendTLSManagedTag}, ProvisioningStatus: "ACTIVE"},
		backendTLS: octavia.PoolBackendTLS{AdminObserved: true, Hostname: "checkout.example.test"}, members: []pools.Member{{ID: "tls-member"}}}
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{
		httpParts: map[string]*observedRouteGraph{ordinaryKey: verified, inlineKey: clear},
	}}}
	step := inlineBackendTLSWithdrawal(&desired, actual)
	require.NotNil(t, step)
	require.Equal(t, inlineKey, step.backendKey)
	require.Equal(t, ptr.To(false), step.route.poolAdmin)
	// A rename creates a different desired key while the old pool can
	// still serve the original predicate on this HTTPRoute.
	route.HTTPBackends[1].SessionPersistenceRule = "name/renamed"
	desired, err = provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	require.NotEqual(t, inlineKey, openstackidentity.HTTPBackendPoolKey(route.HTTPBackends[1]))
	step = inlineBackendTLSWithdrawal(&desired, actual)
	require.NotNil(t, step)
	require.Equal(t, inlineKey, step.backendKey)
	require.Equal(t, clear.pool.ID, step.route.id)
	require.Equal(t, ptr.To(false), step.route.poolAdmin)
	clear.pool.AdminStateUp, clear.pool.Tags = false, *step.route.poolTags
	step = inlineBackendTLSWithdrawal(&desired, actual)
	require.NotNil(t, step)
	require.Equal(t, inlineKey, step.backendKey)
	require.Equal(t, routeMutationClearPoolMembers, step.route.kind)
	clear.members = nil
	require.Nil(t, inlineBackendTLSWithdrawal(&desired, actual))
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
