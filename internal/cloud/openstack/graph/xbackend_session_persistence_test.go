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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestXBackendCookieOctaviaPoolLifecycle(t *testing.T) {
	var pool map[string]any
	var writes []map[string]json.RawMessage
	capabilityResponse := http.StatusOK
	capabilities := []string{octavia.GatewayCapabilityHTTPCookieName}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			if capabilityResponse != http.StatusOK {
				w.WriteHeader(capabilityResponse)
				return
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{pool}})
		case "POST /lbaas/pools", "PUT /lbaas/pools/pool":
			var request struct {
				Pool map[string]json.RawMessage `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			writes = append(writes, request.Pool)
			if r.Method == http.MethodPost {
				pool = map[string]any{"id": "pool", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN", "provisioning_status": "ACTIVE"}
			}
			for _, field := range []string{"session_persistence", "admin_state_up", "tags"} {
				if value := request.Pool[field]; value != nil {
					pool[field] = value
				}
			}
			if r.Method == http.MethodPost {
				w.WriteHeader(http.StatusCreated)
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"pool": pool}))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{XBackendSessionPersistence: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode = cloud.MemberModeEndpoint
	route.HTTPBackends = []cloud.HTTPBackend{{
		Kind: cloud.HTTPBackendKindXBackend, Namespace: "apps", Name: "reports", Port: 8080,
		SessionCookieName: "reports_session",
		Members:           []cloud.Member{{Address: "192.0.2.20", Port: 8080}},
		Matches:           []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/reports"}},
	}}
	desired, err := p.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{}}}
	wanted := &desired.routes[0].httpParts[0]
	wanted.healthMonitor = cloud.HealthMonitorConfig{}
	wanted.spec.Gateway.LoadBalancerID = lb.ID
	require.Nil(t, p.prepareXBackendCookies(t.Context(), &desired, actual))
	require.Equal(t, xBackendCookieAvailable, wanted.sessionCookieCapability)

	converge := func(actual observedRouteGraph) routeMutation {
		t.Helper()
		steps, _, err := buildRouteEnsurePlan(*wanted, actual)
		require.NoError(t, err)
		require.NotEmpty(t, steps)
		return steps[0]
	}
	execute := func(step routeMutation) {
		t.Helper()
		require.NoError(t, p.executeRouteMutation(t.Context(), *wanted, step))
	}
	observe := func() observedRouteGraph {
		t.Helper()
		inventory, err := p.octavia.ListPoolInventory(t.Context(), pools.ListOpts{})
		require.NoError(t, err)
		require.Len(t, inventory.Items, 1)
		return observedRouteGraph{pool: &inventory.Items[0], backendTLS: inventory.BackendTLS["pool"]}
	}
	create := converge(observedRouteGraph{})
	require.Equal(t, routeMutationCreatePool, create.kind)
	execute(create)
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_session"}`, string(writes[0]["session_persistence"]))
	require.Equal(t, "false", string(writes[0]["admin_state_up"]))
	persisted := observe()
	require.False(t, persisted.pool.AdminStateUp)
	require.Equal(t, "reports_session", persisted.pool.Persistence.CookieName)
	require.False(t, plainActivationPool(persisted))
	require.Equal(t, routeMutationCreateMember, converge(persisted).kind)
	persisted.members = []pools.Member{{ID: "member", Address: "192.0.2.20", ProtocolPort: 8080,
		SubnetID: wanted.spec.MemberSubnetID, AdminStateUp: true, ProvisioningStatus: "ACTIVE", Weight: 1}}
	enable := converge(persisted)
	require.Equal(t, routeMutationUpdatePool, enable.kind)
	execute(enable)
	require.Equal(t, "true", string(writes[1]["admin_state_up"]))
	require.NotContains(t, writes[1], "session_persistence")

	wanted.sessionCookieName = "reports_renamed"
	active := observe()
	active.members = persisted.members
	actual.routes[0].httpParts = map[string]*observedRouteGraph{wanted.backendKey: &active}
	drain := p.prepareXBackendCookies(t.Context(), &desired, actual)
	require.NotNil(t, drain)
	require.Equal(t, routeMutationClearPoolMembers, drain.route.kind)
	active.members = nil
	update := converge(active)
	require.Equal(t, routeMutationSetXBackendCookie, update.kind)
	execute(update)
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`, string(writes[2]["session_persistence"]))
	require.Equal(t, "reports_renamed", observe().pool.Persistence.CookieName)

	active = observe()
	capabilityResponse = http.StatusServiceUnavailable
	require.Nil(t, p.prepareXBackendCookies(t.Context(), &desired, actual))
	_, _, err = buildRouteEnsurePlan(*wanted, active)
	require.ErrorIs(t, err, errXBackendCookieUnverified)
	require.Len(t, writes, 3)
	capabilityResponse = http.StatusOK
	capabilities = []string{}
	withdraw := p.prepareXBackendCookies(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	execute(withdraw.route)
	require.Equal(t, "null", string(writes[3]["session_persistence"]))
	active = observe()
	require.Empty(t, active.pool.Persistence)
	_, _, err = buildRouteEnsurePlan(*wanted, active)
	require.ErrorIs(t, err, errXBackendCookieUnavailable)

	wanted.sessionCookieName = ""
	step := converge(active)
	require.Equal(t, routeMutationSetXBackendCookie, step.kind)
	execute(step)
	require.NotContains(t, observe().pool.Tags, xBackendCookieTag)
	require.Equal(t, routeMutationCreateMember, converge(observe()).kind)
}

func TestXBackendCookieWeightedPolicyFollowsPoolReadback(t *testing.T) {
	desired, actual := weightedPlanFixture()
	part := &desired.routes[0].httpParts[1]
	part.xBackend, part.sessionCookieName, part.sessionCookieCapability = true, "reports_session", xBackendCookieAvailable
	backend := actual.routes[0].httpParts[part.backendKey]
	backend.backendTLS.AdminObserved = true
	weighted := desired.routes[0].httpParts[2]
	resolved := resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.True(t, resolved.policies[0].targetsPending())
	backend.pool.Tags = append(backend.pool.Tags, xBackendCookieTag)
	backend.pool.Persistence = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: part.sessionCookieName}
	resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.False(t, resolved.policies[0].targetsPending())
	require.Equal(t, []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: "pool-1"}, {Weight: 1, PoolID: "pool-remote"}},
		resolved.policies[0].weightedPlan.targets)
	part.sessionCookieCapability = xBackendCookieUnavailable
	resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.False(t, resolved.policies[0].targetsPending())
	require.Equal(t, []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: "pool-1"}, fixedWeightedHTTPTarget(1)},
		resolved.policies[0].weightedPlan.targets)
}

func TestXBackendCookieAndServerOnlyBothGatePoolOpening(t *testing.T) {
	desired, observed := convergedRoutePlanFixture()
	desired.xBackend, desired.sessionCookieName, desired.sessionCookieCapability = true, "reports_session", xBackendCookieAvailable
	desired.spec.BackendTLS = newGraphBackendTLS(t)
	desired.backendTLS = backendTLSPlan{phase: backendTLSVerified}
	observed.pool.TLSEnabled, observed.pool.AdminStateUp, observed.pool.ProvisioningStatus = true, false, "ACTIVE"
	observed.backendTLS.AdminObserved = true
	observed.members = nil
	plan, _, err := buildRouteEnsurePlan(desired, observed)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationSetXBackendCookie, plan[0].kind)
	require.Nil(t, plan[0].poolAdmin)

	observed.pool.Persistence = *plan[0].poolPersistence
	observed.pool.Tags = append(*plan[0].poolTags, backendTLSManagedTag, backendTLSRevisionTag(desired.spec.BackendTLS))
	plan, _, err = buildRouteEnsurePlan(desired, observed)
	require.NoError(t, err)
	require.Equal(t, routeMutationSetBackendTLS, plan[0].kind)
	require.Equal(t, new(true), plan[0].poolAdmin)
}
