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
	"k8s.io/utils/ptr"
)

func TestDeniedXBackendCreatesFixedResponseAfterHeaderWithdrawal(t *testing.T) {
	p := NewProvider(ServiceClients{ProjectID: "project-a"}, ProviderConfig{FixedHTTPResponses: true, XBackendSessionPersistence: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode = cloud.MemberModeEndpoint
	route.HTTPBackends = []cloud.HTTPBackend{{Kind: cloud.HTTPBackendKindXBackend, Namespace: "apps", Name: "reports", Port: 8080,
		InvalidReference: true, Unavailable: true, SessionHeader: &cloud.HTTPSessionHeaderPersistence{Name: "X-Session"},
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/reports"}},
	}}
	desired, err := p.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	part := desired.routes[0].httpParts[0]
	require.True(t, part.fixedResponse)
	part.sessionHeaderCapability = xBackendCookieUnavailable
	plan, _, err := buildRouteEnsurePlan(part, observedRouteGraph{})
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationCreatePolicy, plan[0].kind)
	require.Empty(t, plan[0].parentID)
	previous := observedRouteGraph{pool: &pools.Pool{ID: "former-pool", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", Tags: []string{xBackendHeaderTag},
		Persistence: pools.SessionPersistence{Type: cloud.HTTPHeaderPersistenceType}}, backendTLS: octavia.PoolBackendTLS{AdminObserved: true},
		headerPersistence: octavia.PoolHeaderPersistence{Observed: true, Value: &cloud.HTTPSessionHeaderPersistence{Name: "x-session"}}}
	plan, _, err = buildRouteEnsurePlan(part, previous)
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationSetXBackendPersistence, plan[0].kind)
	require.Equal(t, &pools.SessionPersistence{}, plan[0].poolPersistence)
}

func TestXBackendHeaderOctaviaPoolLifecycle(t *testing.T) {
	pool := map[string]any{"id": "pool", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN",
		"provisioning_status": "ACTIVE", "admin_state_up": true, "session_persistence": nil, "backend_h2c": true}
	var enrolled json.RawMessage
	capabilities := []string{octavia.GatewayCapabilityHTTPHeaderPersistence}
	capabilityResponse := http.StatusOK
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/effective-amphora/gateway_capabilities":
			if capabilityResponse != http.StatusOK {
				w.WriteHeader(capabilityResponse)
				return
			}
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{pool}})
		case "PUT /lbaas/pools/pool":
			var request struct{ Pool map[string]json.RawMessage }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			enrolled = request.Pool["session_persistence"]
			for field, value := range request.Pool {
				pool[field] = value
			}
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{XBackendSessionPersistence: true, BackendH2C: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode = cloud.MemberModeEndpoint
	route.HTTPBackends = []cloud.HTTPBackend{{
		Kind: cloud.HTTPBackendKindXBackend, Namespace: "apps", Name: "reports", Port: 8080, BackendH2C: true,
		SessionHeader: &cloud.HTTPSessionHeaderPersistence{Name: "X-Reports-Session", TimeoutMS: ptr.To(int64(0))},
		Members:       []cloud.Member{{Address: "192.0.2.20", Port: 8080}},
		Matches:       []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/reports"}},
	}}
	desired, err := p.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	wanted := &desired.routes[0].httpParts[0]
	wanted.healthMonitor = cloud.HealthMonitorConfig{}
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "effective-amphora", "ACTIVE"
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{
		httpParts: make(map[string]*observedRouteGraph),
	}}}
	observe := func() *observedRouteGraph {
		t.Helper()
		inventory, err := p.octavia.ListPoolInventory(t.Context(), pools.ListOpts{})
		require.NoError(t, err)
		require.Len(t, inventory.Items, 1)
		observed := &observedRouteGraph{pool: &inventory.Items[0], backendH2C: inventory.BackendH2C["pool"],
			backendTLS: inventory.BackendTLS["pool"], headerPersistence: inventory.HeaderPersistence["pool"]}
		actual.routes[0].httpParts[wanted.backendKey] = observed
		return observed
	}
	apply := func(step *routeMutation) {
		t.Helper()
		require.NotNil(t, step)
		require.NoError(t, p.executeRouteMutation(t.Context(), *wanted, *step))
	}
	current := observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceHeaderReadbackUnverified, wanted.sessionPersistenceIssue())
	steps, _, err := buildRouteEnsurePlan(*wanted, *current)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	apply(&steps[0])
	require.JSONEq(t, `{"type":"HTTP_HEADER","header_name":"x-reports-session","header_absolute_timeout_ms":0}`, string(enrolled))

	// A partial readback cannot authorize members for an explicit zero timeout.
	pool["session_persistence"] = json.RawMessage(`{"type":"HTTP_HEADER","header_name":"X-Reports-Session"}`)
	current = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceHeaderReadbackUnverified, wanted.sessionPersistenceIssue())
	step, err := planXBackendPersistence(*wanted, *current)
	require.NoError(t, err)
	require.Equal(t, routeMutationSetXBackendPersistence, step.kind)
	apply(step)
	current = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, wanted.sessionPersistenceIssue())
	steps, _, err = buildRouteEnsurePlan(*wanted, *current)
	require.NoError(t, err)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationCreateMember, steps[0].kind)

	// An inconclusive discovery drains the pool but retains its enrolled policy.
	member := pools.Member{ID: "member", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}
	current.members = []pools.Member{member}
	capabilityResponse = http.StatusServiceUnavailable
	withdraw := p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, routeMutationClearPoolMembers, withdraw.route.kind)
	require.Equal(t, cloud.SessionPersistenceHeaderUnverified, wanted.sessionPersistenceIssue())
	current.members = nil
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	step, err = planXBackendPersistence(*wanted, *current)
	require.ErrorIs(t, err, errXBackendHeaderPersistence)
	require.Nil(t, step)

	// An explicit revocation clears persistence using the stock API.
	capabilityResponse, capabilities = http.StatusOK, []string{}
	withdraw = p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, cloud.SessionPersistenceHeaderUnsupported, wanted.sessionPersistenceIssue())
	apply(&withdraw.route)
	require.Equal(t, "null", string(enrolled))
	current = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Contains(t, current.pool.Tags, xBackendHeaderTag)

	capabilities = []string{octavia.GatewayCapabilityHTTPHeaderPersistence}
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	step, err = planXBackendPersistence(*wanted, *current)
	require.NoError(t, err)
	apply(step)
	require.JSONEq(t, `{"type":"HTTP_HEADER","header_name":"x-reports-session","header_absolute_timeout_ms":0}`, string(enrolled))
	current = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, wanted.sessionPersistenceIssue())
	wanted.sessionHeader = &cloud.HTTPSessionHeaderPersistence{Name: wanted.sessionHeader.Name}
	step, err = planXBackendPersistence(*wanted, *current)
	require.NoError(t, err)
	apply(step)
	require.JSONEq(t, `{"type":"HTTP_HEADER","header_name":"x-reports-session"}`, string(enrolled))
	current = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, wanted.sessionPersistenceIssue())

	wanted.sessionHeader = nil
	step, err = planXBackendPersistence(*wanted, *current)
	require.NoError(t, err)
	apply(step)
	require.Equal(t, "null", string(enrolled))
	step, err = planXBackendPersistence(*wanted, *observe())
	require.NoError(t, err)
	apply(step)
	require.NotContains(t, observe().pool.Tags, xBackendHeaderTag)
}

func TestXBackendHeaderWeightedTargetKeepsItsShare(t *testing.T) {
	desired, actual := weightedPlanFixture()
	part, rule := &desired.routes[0].httpParts[1], desired.routes[0].httpParts[2]
	part.xBackend, part.sessionHeaderCapability = true, xBackendCookieAvailable
	part.sessionHeader = &cloud.HTTPSessionHeaderPersistence{Name: "x-session"}
	backend := actual.routes[0].httpParts[part.backendKey]
	backend.backendTLS.AdminObserved = true
	backend.pool.Tags = append(backend.pool.Tags, xBackendHeaderTag)
	backend.pool.Persistence.Type = cloud.HTTPHeaderPersistenceType
	backend.headerPersistence = octavia.PoolHeaderPersistence{Observed: true, Value: part.sessionHeader}
	resolve := func() []octavia.WeightedHTTPTarget {
		return resolveWeightedHTTPPart(rule, desired.routes[0], actual.routes[0], "").policies[0].weightedPlan.targets
	}
	sibling := octavia.WeightedHTTPTarget{Weight: 3, PoolID: "pool-1"}
	require.Equal(t, []octavia.WeightedHTTPTarget{sibling, {Weight: 1, PoolID: backend.pool.ID}}, resolve())
	part.sessionHeaderCapability = xBackendCookieUnavailable
	require.Equal(t, []octavia.WeightedHTTPTarget{sibling, fixedWeightedHTTPTarget(1)}, resolve())

	backend.pool.ProvisioningStatus = "ERROR"
	require.NotEmpty(t, backend.members)
	recovery, err := planXBackendPersistence(*part, *backend)
	require.NoError(t, err)
	require.NotNil(t, recovery)
	require.True(t, recovery.retry)
	require.Equal(t, ptr.To(false), recovery.poolAdmin)
	require.Equal(t, &pools.SessionPersistence{}, recovery.poolPersistence)
	require.Contains(t, *recovery.poolTags, backendTLSManagedTag)
}
