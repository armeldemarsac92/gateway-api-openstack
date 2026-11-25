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
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestXBackendAuthenticatedH2CookieGatesPoolMutations(t *testing.T) {
	for _, mode := range []struct{ name, clientRef string }{
		{name: "ServerOnly"},
		{name: "ClientAndServer", clientRef: "https://barbican.example/v1/secrets/reports-client"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			testXBackendAuthenticatedH2CookieGatesPoolMutations(t, mode.clientRef)
		})
	}
}

func testXBackendAuthenticatedH2CookieGatesPoolMutations(t *testing.T, clientRef string) {
	t.Helper()
	var request struct{ Pool map[string]any }
	var advertised []string
	p := &Provider{octavia: octavia.NewService(safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities" {
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
			return
		}
		require.Equal(t, "POST /lbaas/pools", r.Method+" "+r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "reports"}})
	})))}
	spec := completeGraphSpec(safetyTestIdentity(t)).Routes[0].Spec
	p.projectID = spec.Identity.OpenStackProjectID
	spec.Hostname, spec.PathType, spec.PathValue, spec.Members = "", "", "", nil
	spec.MemberMode = cloud.MemberModeEndpoint
	tls := newGraphBackendTLS(t)
	tls.RequireH2 = true
	spec.HTTPBackends = []cloud.HTTPBackend{{
		Kind: cloud.HTTPBackendKindXBackend, Namespace: "apps", Name: "reports", Port: 8443,
		SessionCookieName: "reports_session", BackendTLS: tls, XBackendClientCertificateReference: clientRef,
		Members: []cloud.Member{{Address: "192.0.2.20", Port: 8443}},
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/reports"}},
	}}
	route, err := p.buildDesiredRoute(spec)
	require.NoError(t, err)
	part := route.httpParts[0]
	part.sessionCookieCapability = xBackendCookieAvailable
	settings := octavia.BackendTLSSettings{Hostname: tls.Hostname, CAReference: "https://barbican.example/v1/secrets/reports-ca",
		ClientCertificateReference: clientRef, RequireH2: new(true)}
	converge := func(observed observedRouteGraph) []routeMutation {
		t.Helper()
		steps, _, err := buildRouteEnsurePlan(part, observed)
		require.NoError(t, err)
		return steps
	}
	create := func() {
		t.Helper()
		request.Pool = nil
		step := converge(observedRouteGraph{})[0]
		require.Equal(t, routeMutationCreatePool, step.kind)
		require.NoError(t, p.executeRouteMutation(t.Context(), part, step))
	}

	part.backendTLS.phase = backendTLSBlocked
	create()
	require.Equal(t, true, request.Pool["admin_state_up"])
	require.NotContains(t, request.Pool, "session_persistence")
	require.NotContains(t, request.Pool, "tls_enabled")

	part.backendTLS = backendTLSPlan{phase: backendTLSConfigure, settings: settings}
	part.sessionCookieCapability = xBackendCookieUnavailable
	create()
	require.NotContains(t, request.Pool, "session_persistence")
	require.NotContains(t, request.Pool, "tls_enabled")

	part.sessionCookieCapability = xBackendCookieAvailable
	create()
	require.Equal(t, false, request.Pool["admin_state_up"])
	require.Equal(t, true, request.Pool["backend_tls_require_h2"])
	require.Equal(t, []any{"h2"}, request.Pool["alpn_protocols"])
	require.Equal(t, settings.Hostname, request.Pool["backend_tls_hostname"])
	require.Equal(t, settings.CAReference, request.Pool["ca_tls_container_ref"])
	require.Equal(t, map[string]any{"type": "HTTP_COOKIE", "cookie_name": "reports_session"}, request.Pool["session_persistence"])
	if clientRef == "" {
		require.NotContains(t, request.Pool, "tls_container_ref")
	} else {
		require.Equal(t, clientRef, request.Pool["tls_container_ref"])
	}

	// An established pool might confirm TLS and persistence on different reads.
	observed := observedRouteGraph{pool: &pools.Pool{ID: "reports", Protocol: "HTTP", ProvisioningStatus: "ACTIVE",
		TLSEnabled: true, ALPNProtocols: []string{"h2"}, Tags: []string{backendTLSManagedTag, backendTLSRevisionTag(tls)}},
		backendTLS: octavia.PoolBackendTLS{AdminObserved: true}}
	part.backendTLS.phase = backendTLSVerified
	part.sessionCookieCapability = xBackendCookieUnavailable
	for _, hostname := range []string{"", "reports.example.test"} {
		part.policies[0].httpsListenerHostname = hostname
		steps := converge(observed)
		require.Len(t, steps, 1)
		require.Equal(t, routeMutationCreatePolicy, steps[0].kind)
		require.Equal(t, observed.pool.ID, steps[0].parentID)
		require.Equal(t, hostname, steps[0].policy.httpsListenerHostname)
	}
	part.policies[0].httpsListenerHostname = ""
	part.sessionCookieCapability = xBackendCookieAvailable
	part.backendTLS.phase = backendTLSBlocked
	require.Equal(t, routeMutationCreatePolicy, converge(observed)[0].kind)
	part.backendTLS.phase = backendTLSVerified
	steps := converge(observed)
	require.Equal(t, routeMutationSetXBackendPersistence, steps[0].kind)
	require.Nil(t, steps[0].poolAdmin)
	observed.pool.Persistence = *steps[0].poolPersistence
	observed.pool.Tags = *steps[0].poolTags
	step := converge(observed)[0]
	require.Equal(t, routeMutationSetBackendTLS, step.kind)
	require.Equal(t, new(true), step.poolAdmin)

	// The cookie capability can disappear between planning and reopening.
	route.httpParts[0] = part
	graph := desiredGatewayGraph{protocol: cloud.ListenerProtocolHTTP, routes: []desiredRoute{route, {}}}
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer()}
	actual.loadBalancer.Provider = "amphora"
	activation := graphMutation{kind: graphRouteMutation, backendKey: part.backendKey, route: step}
	sibling := graphMutation{kind: graphRouteMutation, routeIndex: 1, route: routeMutation{kind: routeMutationCreateMember}}
	result := cloud.GatewayGraphResult{Routes: make([]cloud.RouteGraphResult, 2)}
	advertised = []string{octavia.GatewayCapabilityBackendTLSHostname, octavia.GatewayCapabilityBackendTLSH2ALPN}
	next, ok, err := p.prepareNextGraphMutation(t.Context(), graph, actual, []graphMutation{activation, sibling}, &result)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sibling, next)
	advertised = append(advertised, octavia.GatewayCapabilityHTTPCookieName)
	next, ok, err = p.prepareNextGraphMutation(t.Context(), graph, actual, []graphMutation{activation, sibling}, &result)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, activation, next)
}

func TestXBackendCookieOctaviaPoolLifecycle(t *testing.T) {
	t.Run("HTTP11", func(t *testing.T) { testXBackendCookieOctaviaPoolLifecycle(t, false) })
	t.Run("H2C", func(t *testing.T) { testXBackendCookieOctaviaPoolLifecycle(t, true) })
}

func testXBackendCookieOctaviaPoolLifecycle(t *testing.T, h2c bool) {
	t.Helper()
	var pool map[string]any
	var writes []map[string]json.RawMessage
	var siblingEnabled bool
	capabilityResponse := http.StatusOK
	namedCapabilities := []string{octavia.GatewayCapabilityHTTPCookieName}
	if h2c {
		namedCapabilities = append(namedCapabilities, octavia.GatewayCapabilityBackendH2C)
	}
	capabilities := slices.Clone(namedCapabilities)
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
		case "PUT /lbaas/pools/ordinary":
			var request struct {
				Pool struct {
					AdminStateUp bool `json:"admin_state_up"`
				} `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			siblingEnabled = request.Pool.AdminStateUp
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "ordinary"}})
		case "POST /lbaas/pools", "PUT /lbaas/pools/pool":
			var request struct {
				Pool map[string]json.RawMessage `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			writes = append(writes, maps.Clone(request.Pool))
			if r.Method == http.MethodPost {
				pool = map[string]any{"id": "pool", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN", "provisioning_status": "ACTIVE"}
			}
			if r.Method == http.MethodPut && request.Pool["session_persistence"] != nil {
				// Octavia preserves a custom Path when a same-type PUT omits it.
				var before, after map[string]json.RawMessage
				previous, err := json.Marshal(pool["session_persistence"])
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(previous, &before))
				require.NoError(t, json.Unmarshal(request.Pool["session_persistence"], &after))
				if before["cookie_path"] != nil && after["type"] != nil && after["cookie_path"] == nil {
					after["cookie_path"] = before["cookie_path"]
					request.Pool["session_persistence"], err = json.Marshal(after)
					require.NoError(t, err)
				}
			}
			for _, field := range []string{"session_persistence", "admin_state_up", "backend_h2c", "tags"} {
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
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{XBackendSessionPersistence: true, BackendH2C: h2c})
	spec := completeGraphSpec(safetyTestIdentity(t))
	route := &spec.Routes[0].Spec
	route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
	route.MemberMode = cloud.MemberModeEndpoint
	route.HTTPBackends = []cloud.HTTPBackend{{
		Kind: cloud.HTTPBackendKindXBackend, Namespace: "apps", Name: "reports", Port: 8080,
		SessionCookieName: "reports_session", BackendH2C: h2c,
		Members: []cloud.Member{{Address: "192.0.2.20", Port: 8080}},
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/reports"}},
	}, {
		Namespace: "apps", Name: "ordinary", Port: 80,
		Members: []cloud.Member{{Address: "192.0.2.30", Port: 8080}},
		Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: "/ordinary", Precedence: 1}},
	}}
	desired, err := p.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	lb := safetyObservedLoadBalancer()
	lb.Provider, lb.ProvisioningStatus = "amphora", "ACTIVE"
	actual := observedGatewayGraph{loadBalancer: lb, protocol: cloud.ListenerProtocolHTTP, routes: []observedRouteGraph{{}}}
	parts := desired.routes[0].httpParts
	require.Len(t, parts, 2)
	cookieIndex := slices.IndexFunc(parts, func(part desiredRoute) bool { return part.xBackend })
	require.GreaterOrEqual(t, cookieIndex, 0)
	wanted, sibling := &parts[cookieIndex], &parts[1-cookieIndex]
	wanted.healthMonitor = cloud.HealthMonitorConfig{}
	wanted.spec.Gateway.LoadBalancerID = lb.ID
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
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
		return observedRouteGraph{pool: &inventory.Items[0], backendH2C: inventory.BackendH2C["pool"], backendTLS: inventory.BackendTLS["pool"],
			cookieDeadline: inventory.CookieDeadline["pool"], cookiePath: inventory.CookiePath["pool"]}
	}
	create := converge(observedRouteGraph{})
	require.Equal(t, routeMutationCreatePool, create.kind)
	execute(create)
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_session"}`, string(writes[0]["session_persistence"]))
	require.Equal(t, "false", string(writes[0]["admin_state_up"]))
	if h2c {
		require.Equal(t, "true", string(writes[0]["backend_h2c"]))
	} else {
		require.NotContains(t, writes[0], "backend_h2c")
	}
	persisted := observe()
	require.Equal(t, h2c, persisted.backendH2C)
	require.False(t, persisted.pool.AdminStateUp)
	require.Equal(t, "reports_session", persisted.pool.Persistence.CookieName)
	require.False(t, plainActivationPool(persisted))
	require.Equal(t, routeMutationCreateMember, converge(persisted).kind)
	persisted.members = []pools.Member{{ID: "member", Address: "192.0.2.20", ProtocolPort: 8080,
		SubnetID: wanted.spec.MemberSubnetID, AdminStateUp: true, ProvisioningStatus: "ACTIVE", Weight: 1}}
	enable := converge(persisted)
	require.Equal(t, routeMutationUpdatePool, enable.kind)
	plan := []graphMutation{{kind: graphRouteMutation, backendKey: wanted.backendKey, route: enable}}
	siblingEnable := graphMutation{kind: graphRouteMutation, backendKey: sibling.backendKey,
		route: routeMutation{kind: routeMutationUpdatePool, id: "ordinary", poolAdmin: boolPointer(true)}}
	applyNext := func(plan []graphMutation) (bool, cloud.RouteResult) {
		t.Helper()
		result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{{}}}
		step, prepared, err := p.prepareNextGraphMutation(t.Context(), desired, actual, plan, &result)
		require.NoError(t, err)
		if prepared {
			require.NoError(t, p.executeGraphMutation(t.Context(), desired, actual, step))
		}
		return prepared, result.Routes[0].Result
	}
	if h2c {
		// The cookie capability stays enrolled while the regional transport
		// capability is withdrawn after the closed pool's members converge.
		capabilities = []string{octavia.GatewayCapabilityHTTPCookieName}
		applied, result := applyNext(plan)
		require.False(t, applied)
		require.Contains(t, result.UnsupportedBackendProtocol, octavia.GatewayCapabilityBackendH2C)
		require.Len(t, writes, 1)
		require.False(t, observe().pool.AdminStateUp)

		applied, _ = applyNext(append(plan, siblingEnable))
		require.True(t, applied)
		require.True(t, siblingEnabled)
		require.Len(t, writes, 1)
		capabilities = slices.Clone(namedCapabilities)
	}
	applied, result := applyNext(plan)
	require.True(t, applied)
	require.Empty(t, result.UnsupportedBackendProtocol)
	require.Equal(t, "true", string(writes[1]["admin_state_up"]))
	require.NotContains(t, writes[1], "session_persistence")

	wanted.sessionCookieName = "reports_renamed"
	active := observe()
	active.members = persisted.members
	actual.routes[0].httpParts = map[string]*observedRouteGraph{wanted.backendKey: &active}
	drain := p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, drain)
	require.Equal(t, routeMutationClearPoolMembers, drain.route.kind)
	active.members = nil
	update := converge(active)
	require.Equal(t, routeMutationSetXBackendPersistence, update.kind)
	execute(update)
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`, string(writes[2]["session_persistence"]))
	require.Equal(t, "reports_renamed", observe().pool.Persistence.CookieName)

	// The named capability alone cannot authorize a newly requested deadline.
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{Scope: strings.Repeat("a", 64)}
	active = observe()
	actual.routes[0].httpParts[wanted.backendKey] = &active
	require.NotNil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceDeadlineUnsupported, wanted.sessionPersistenceIssue())
	available, err := p.XBackendCookieAvailable(t.Context(), cloud.HTTPSessionCookieNamed)
	require.NoError(t, err)
	require.True(t, available)

	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout)
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceDeadlineReadbackUnverified, wanted.sessionPersistenceIssue())
	execute(converge(active))
	zero := observe()
	require.Equal(t, wanted.sessionCookieDeadline, zero.cookieDeadline.Value)
	require.Contains(t, zero.pool.Tags, xBackendCookieDeadlineTag)
	require.Equal(t, routeMutationCreateMember, converge(zero).kind)

	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1234, Scope: wanted.sessionCookieDeadline.Scope}
	zero.members = persisted.members
	actual.routes[0].httpParts[wanted.backendKey] = &zero
	require.Equal(t, routeMutationClearPoolMembers, p.prepareXBackendPersistence(t.Context(), &desired, actual).route.kind)
	zero.members = nil
	execute(converge(zero))
	require.NotContains(t, string(writes[len(writes)-1]["session_persistence"]), "cookie_permanent")
	bounded := observe()
	require.Equal(t, wanted.sessionCookieDeadline, bounded.cookieDeadline.Value)
	require.Equal(t, routeMutationCreateMember, converge(bounded).kind)

	// A provider that advertised the extension can still ignore it or return
	// incomplete data. Both must keep the policy from admitting members.
	correct := pool["session_persistence"]
	for _, persistence := range []string{
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`,
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":"1234","cookie_scope":"bad"}`,
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":5,"cookie_scope":"` + wanted.sessionCookieDeadline.Scope + `"}`,
	} {
		pool["session_persistence"] = json.RawMessage(persistence)
		ignored := observe()
		actual.routes[0].httpParts[wanted.backendKey] = &ignored
		require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
		require.Equal(t, cloud.SessionPersistenceDeadlineReadbackUnverified, wanted.sessionPersistenceIssue())
		require.Equal(t, routeMutationSetXBackendPersistence, converge(ignored).kind)
	}
	pool["session_persistence"] = correct

	// An API rollback leaves the ownership tags but omits the deadline fields.
	// Use only the stock persistence clear and restore the name separately.
	deadline := wanted.sessionCookieDeadline
	wanted.sessionCookieDeadline = nil
	bounded.members = persisted.members
	require.Equal(t, routeMutationClearPoolMembers, converge(bounded).kind)
	pool["session_persistence"] = json.RawMessage(`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`)
	legacyNamed := observe()
	require.Nil(t, legacyNamed.cookieDeadline.Value)
	execute(converge(legacyNamed))
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	execute(converge(observe()))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`, string(writes[len(writes)-1]["session_persistence"]))
	active = observe()
	require.Nil(t, active.cookieDeadline.Value)
	require.NotContains(t, active.pool.Tags, xBackendCookieDeadlineTag)
	require.Equal(t, routeMutationCreateMember, converge(active).kind)

	wanted.sessionCookieDeadline = deadline
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(active))
	active = observe()
	actual.routes[0].httpParts[wanted.backendKey] = &active
	capabilityResponse = http.StatusServiceUnavailable
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	_, err = planXBackendCookie(*wanted, active)
	require.ErrorIs(t, err, errXBackendCookieUnverified)
	capabilityResponse = http.StatusOK
	capabilities = slices.Clone(namedCapabilities)
	withdraw := p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, withdraw)
	require.Equal(t, cloud.SessionPersistenceDeadlineUnsupported, wanted.sessionPersistenceIssue())
	execute(withdraw.route)
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	active = observe()
	require.Empty(t, active.pool.Persistence)
	_, err = planXBackendCookie(*wanted, active)
	require.ErrorIs(t, err, errXBackendCookieDeadlineUnavailable)

	wanted.sessionCookieName, wanted.sessionCookieDeadline = "", nil
	step := converge(active)
	require.Equal(t, routeMutationSetXBackendPersistence, step.kind)
	execute(step)
	require.NotContains(t, observe().pool.Tags, xBackendCookieTag)
	require.Equal(t, routeMutationCreateMember, converge(observe()).kind)

	// The first bounded pool also starts closed and sends the full contract.
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout)
	wanted.sessionCookieName, wanted.sessionCookieDeadline = "reports_renamed", deadline
	actual.routes[0].httpParts = nil
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(observedRouteGraph{}))
	request := writes[len(writes)-1]
	require.Equal(t, "false", string(request["admin_state_up"]))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1234,"cookie_scope":"`+deadline.Scope+`"}`,
		string(request["session_persistence"]))

	// Opt in to Permanent on the same scope and require provider readback of the mode.
	active = observe()
	actual.routes[0].httpParts = map[string]*observedRouteGraph{wanted.backendKey: &active}
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: deadline.Scope, Permanent: true}
	require.NotNil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistencePermanentUnsupported, wanted.sessionPersistenceIssue())
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePermanent)
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(active))
	permanent := observe()
	require.Equal(t, wanted.sessionCookieDeadline, permanent.cookieDeadline.Value)
	require.Contains(t, permanent.pool.Tags, xBackendCookiePermanentTag)
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1500,"cookie_scope":"`+deadline.Scope+`","cookie_permanent":true}`,
		string(writes[len(writes)-1]["session_persistence"]))

	correct = pool["session_persistence"]
	pool["session_persistence"] = json.RawMessage(`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1500,"cookie_scope":"` + deadline.Scope + `","cookie_permanent":"true"}`)
	require.True(t, observe().cookieDeadline.Invalid)
	pool["session_persistence"] = correct
	actual.routes[0].httpParts[wanted.backendKey] = &permanent
	capabilityResponse = http.StatusServiceUnavailable
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistencePermanentUnverified, wanted.sessionPersistenceIssue())
	capabilityResponse = http.StatusOK
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, wanted.sessionPersistenceIssue())

	// Losing Permanent support still allows an explicit downgrade to a timed Session cookie.
	capabilities = capabilities[:len(capabilities)-1]
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: deadline.Scope}
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(permanent))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1500,"cookie_scope":"`+deadline.Scope+`","cookie_permanent":false}`,
		string(writes[len(writes)-1]["session_persistence"]))
	require.NotContains(t, observe().pool.Tags, xBackendCookiePermanentTag)

	// An older API can hide the mode while the ownership tag still exists.
	pool["tags"] = permanent.pool.Tags
	pool["session_persistence"] = json.RawMessage(`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1500,"cookie_scope":"` + deadline.Scope + `"}`)
	legacy := observe()
	actual.routes[0].httpParts[wanted.backendKey] = &legacy
	legacyClear := p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, legacyClear)
	execute(legacyClear.route)
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	execute(converge(observe()))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":1500,"cookie_scope":"`+deadline.Scope+`"}`,
		string(writes[len(writes)-1]["session_persistence"]))

	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePermanent)
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{Scope: deadline.Scope, Permanent: true}
	actual.routes[0].httpParts = nil
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(observedRouteGraph{}))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_absolute_timeout_ms":0,"cookie_scope":"`+deadline.Scope+`","cookie_permanent":true}`,
		string(writes[len(writes)-1]["session_persistence"]))
	permanent = observe()
	require.Equal(t, wanted.sessionCookieDeadline, permanent.cookieDeadline.Value)

	// A name-only API also accepts this withdrawal from a Permanent cookie.
	capabilities = []string{octavia.GatewayCapabilityHTTPCookieName}
	wanted.sessionCookieDeadline = nil
	pool["session_persistence"] = json.RawMessage(`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`)
	legacyNamed = observe()
	actual.routes[0].httpParts = map[string]*observedRouteGraph{wanted.backendKey: &legacyNamed}
	clear := p.prepareXBackendPersistence(t.Context(), &desired, actual)
	require.NotNil(t, clear)
	execute(clear.route)
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	execute(converge(observe()))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`, string(writes[len(writes)-1]["session_persistence"]))

	// A custom path reuses the owned pool and requires its own capability.
	path := `reports "\?#,%`
	wanted.sessionCookiePath = &path
	active = observe()
	active.members = persisted.members
	actual.routes[0].httpParts[wanted.backendKey] = &active
	require.Equal(t, routeMutationClearPoolMembers, p.prepareXBackendPersistence(t.Context(), &desired, actual).route.kind)
	require.Equal(t, cloud.SessionPersistencePathUnsupported, wanted.sessionPersistenceIssue())
	active.members = nil
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePath)
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistencePathReadbackUnverified, wanted.sessionPersistenceIssue())
	execute(converge(active))
	correct = pool["session_persistence"]
	require.Equal(t, &path, observe().cookiePath.Value)
	require.Contains(t, observe().pool.Tags, xBackendCookiePathTag)

	for _, persistence := range []string{
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`,
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_path":null}`,
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_path":42}`,
		`{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_path":"/"}`,
	} {
		pool["session_persistence"] = json.RawMessage(persistence)
		active = observe()
		active.members = persisted.members
		require.Equal(t, routeMutationClearPoolMembers, p.prepareXBackendPersistence(t.Context(), &desired, actual).route.kind)
		require.Equal(t, cloud.SessionPersistencePathReadbackUnverified, wanted.sessionPersistenceIssue())
	}
	pool["session_persistence"] = correct
	active = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, cloud.SessionPersistenceAvailable, wanted.sessionPersistenceIssue())
	require.Equal(t, routeMutationCreateMember, converge(active).kind)

	// An explicit empty string has its own readback and survives capability recovery.
	path = ""
	execute(converge(active))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_path":""}`, string(writes[len(writes)-1]["session_persistence"]))
	active = observe()
	require.Equal(t, &path, active.cookiePath.Value)
	active.members = persisted.members
	capabilities = []string{octavia.GatewayCapabilityHTTPCookieName}
	require.Equal(t, routeMutationClearPoolMembers, p.prepareXBackendPersistence(t.Context(), &desired, actual).route.kind)
	require.Equal(t, cloud.SessionPersistencePathUnsupported, wanted.sessionPersistenceIssue())
	active.members = nil
	execute(p.prepareXBackendPersistence(t.Context(), &desired, actual).route)
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	_, err = planXBackendCookie(*wanted, observe())
	require.ErrorIs(t, err, errXBackendCookiePathUnavailable)
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePath)
	active = observe()
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(active))
	require.Equal(t, &path, observe().cookiePath.Value)

	// Reverting to the default path clears the old value before restoring the stock shape.
	wanted.sessionCookiePath = nil
	capabilities = []string{octavia.GatewayCapabilityHTTPCookieName}
	active = observe()
	active.members = persisted.members
	require.Equal(t, routeMutationClearPoolMembers, p.prepareXBackendPersistence(t.Context(), &desired, actual).route.kind)
	active.members = nil
	execute(p.prepareXBackendPersistence(t.Context(), &desired, actual).route)
	require.Equal(t, "null", string(writes[len(writes)-1]["session_persistence"]))
	execute(converge(observe()))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed"}`, string(writes[len(writes)-1]["session_persistence"]))
	active = observe()
	require.Nil(t, active.cookiePath.Value)
	require.NotContains(t, active.pool.Tags, xBackendCookiePathTag)
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	require.Equal(t, routeMutationCreateMember, converge(active).kind)

	// A timed Session needs both capabilities, and Permanent adds its own.
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePath)
	available, err = p.XBackendCookieAvailable(t.Context(), cloud.HTTPSessionCookieTimed|cloud.HTTPSessionCookiePath)
	require.NoError(t, err)
	require.False(t, available)
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout)
	available, err = p.XBackendCookieAvailable(t.Context(), cloud.HTTPSessionCookieTimed|cloud.HTTPSessionCookiePath)
	require.NoError(t, err)
	require.True(t, available)

	// POST carries the same path alongside a Permanent lifetime.
	path = "/reports"
	wanted.sessionCookiePath = &path
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: deadline.Scope, Permanent: true}
	actual.routes[0].httpParts = nil
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	_, err = planXBackendCookie(*wanted, observedRouteGraph{})
	require.ErrorIs(t, err, errXBackendCookiePathUnavailable)
	capabilities = append(capabilities, octavia.GatewayCapabilityHTTPCookiePermanent)
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	execute(converge(observedRouteGraph{}))
	require.JSONEq(t, `{"type":"HTTP_COOKIE","cookie_name":"reports_renamed","cookie_path":"/reports","cookie_absolute_timeout_ms":1500,"cookie_scope":"`+deadline.Scope+`","cookie_permanent":true}`,
		string(writes[len(writes)-1]["session_persistence"]))
	active = observe()
	require.False(t, active.pool.AdminStateUp)
	require.Equal(t, wanted.sessionCookieDeadline, active.cookieDeadline.Value)
	require.Equal(t, &path, active.cookiePath.Value)
}

func TestXBackendPermanentCookieWaitsForBothRoutesWithoutBlockingOtherPools(t *testing.T) {
	desired, actual := weightedPlanFixture()
	actual.loadBalancer.Provider, actual.loadBalancer.ProvisioningStatus = "amphora", "ACTIVE"
	actual.listener.ProvisioningStatus = "ACTIVE"
	wanted := &desired.routes[0].httpParts[1]
	wanted.xBackend, wanted.sessionCookieName = true, "reports_session"
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: strings.Repeat("a", 64), Permanent: true}
	current := actual.routes[0].httpParts[wanted.backendKey]
	current.backendTLS.AdminObserved = true
	current.pool.Persistence = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: wanted.sessionCookieName}
	current.pool.Tags = append(current.pool.Tags, xBackendCookieTag, xBackendCookieDeadlineTag, xBackendCookiePermanentTag)
	current.cookieDeadline.Value = wanted.sessionCookieDeadline

	sibling := *wanted
	sibling.backendKey = "sibling-reports"
	sibling.spec.Identity.RouteName = "sibling"
	sibling.policies = []desiredPolicy{{path: "/sibling"}}
	desired.routes = append(desired.routes, desiredRoute{spec: sibling.spec, httpParts: []desiredRoute{sibling}})
	desired.spec.Routes = append(desired.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure})
	siblingPool := *current.pool
	siblingPool.ID = "pool-sibling"
	siblingObserved := observedRouteGraph{pool: &siblingPool, backendTLS: current.backendTLS}
	actual.routes = append(actual.routes, observedRouteGraph{httpParts: map[string]*observedRouteGraph{sibling.backendKey: &siblingObserved}})

	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", r.Method+" "+r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{
			octavia.GatewayCapabilityHTTPCookieName, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout,
			octavia.GatewayCapabilityHTTPCookiePermanent,
		}})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{XBackendSessionPersistence: true})
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	steps, result, err := p.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, result.Routes, 2)
	for _, route := range result.Routes {
		require.Equal(t, cloud.SessionPersistencePermanentReadbackUnverified, route.Result.BackendSessionPersistenceIssue)
		require.Equal(t, cloud.OutcomeProgressing, route.Result.Outcome.State)
	}
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	policy := &actual.routes[0].httpParts[desired.routes[0].httpParts[2].backendKey].policies[0]
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], fixedWeightedHTTPTarget(1)}, *steps[0].route.weightedTargets)
	policy.weightedTargets = *steps[0].route.weightedTargets

	step, err := planXBackendCookie(*wanted, *current)
	require.ErrorIs(t, err, errXBackendCookiePermanentReadback)
	require.Nil(t, step)
	siblingObserved.cookieDeadline.Value = sibling.sessionCookieDeadline
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	for _, route := range desired.routes {
		require.Equal(t, cloud.SessionPersistenceAvailable, route.sessionPersistenceIssue())
	}
	steps, _, err = p.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], {Weight: 1, PoolID: current.pool.ID}}, *steps[0].route.weightedTargets)
}

func TestXBackendPermanentCookiePeerACKStillWithdrawsStaleDirectMembers(t *testing.T) {
	wanted, current := convergedRoutePlanFixture()
	wanted.xBackend, wanted.sessionCookieName = true, "reports_session"
	wanted.sessionCookieCapability, wanted.cookieReadbackPending = xBackendCookieAvailable, true
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: strings.Repeat("a", 64), Permanent: true}
	wanted.members[0].Address = "10.0.0.11"
	current.backendTLS.AdminObserved = true
	current.pool.ProvisioningStatus, current.members[0].ProvisioningStatus = "ACTIVE", "ACTIVE"
	current.pool.Persistence = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: wanted.sessionCookieName}
	current.pool.Tags = []string{xBackendCookieTag, xBackendCookieDeadlineTag, xBackendCookiePermanentTag}
	current.cookieDeadline.Value = wanted.sessionCookieDeadline

	steps, _, err := buildRouteEnsurePlan(wanted, current)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, routeMutationDeleteMember, steps[0].kind)
	require.Equal(t, current.members[0].ID, steps[0].id)
	current.members = nil
	steps, _, err = buildRouteEnsurePlan(wanted, current)
	require.ErrorIs(t, err, errXBackendCookiePermanentReadback)
	require.Empty(t, steps)
}

func TestXBackendPermanentCookieErrorWithdrawsModeBeforeDrainingMembers(t *testing.T) {
	wanted, current := convergedRoutePlanFixture()
	wanted.xBackend, wanted.xBackendPlaintext, wanted.sessionCookieName = true, true, "reports_session"
	wanted.sessionCookieCapability = xBackendCookieAvailable
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 1500, Scope: strings.Repeat("a", 64)}
	permanent := *wanted.sessionCookieDeadline
	permanent.Permanent = true
	current.cookieDeadline.Value = &permanent
	current.pool.ProvisioningStatus = "ERROR"
	current.pool.Persistence = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: wanted.sessionCookieName}
	current.pool.Tags = []string{xBackendCookieTag, xBackendCookieDeadlineTag, xBackendCookiePermanentTag}
	current.backendTLS.AdminObserved = true

	for _, test := range []struct {
		name        string
		edit        func(*desiredRoute)
		persistence any
	}{
		{name: "timed Session", persistence: map[string]any{"cookie_permanent": false}},
		{name: "remove persistence", edit: func(route *desiredRoute) {
			route.sessionCookieName, route.sessionCookieDeadline = "", nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := wanted
			if test.edit != nil {
				test.edit(&route)
			}
			step, err := planXBackendCookie(route, current)
			require.NoError(t, err)
			require.NotNil(t, step)
			require.True(t, step.retry)
			opts := octavia.UpdatePoolOpts{UpdateOpts: pools.UpdateOpts{AdminStateUp: step.poolAdmin, Tags: step.poolTags, Persistence: step.poolPersistence},
				ClearCookiePermanent: step.clearPoolCookiePermanent}
			out, err := opts.ToPoolUpdateMap()
			require.NoError(t, err)
			fields := out["pool"].(map[string]any)
			require.Equal(t, test.persistence, fields["session_persistence"])
			require.Contains(t, fields, "session_persistence")
			require.Equal(t, false, fields["admin_state_up"])
			require.Contains(t, *step.poolTags, backendTLSManagedTag)
			require.NotContains(t, *step.poolTags, xBackendCookiePermanentTag)
		})
	}
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
	part.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 3000, Scope: strings.Repeat("b", 64)}
	backend.pool.Tags = append(backend.pool.Tags, xBackendCookieDeadlineTag)
	resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.True(t, resolved.policies[0].targetsPending())
	backend.cookieDeadline.Value = part.sessionCookieDeadline
	path := "/reports"
	part.sessionCookiePath = &path
	resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.True(t, resolved.policies[0].targetsPending())
	backend.pool.Tags = append(backend.pool.Tags, xBackendCookiePathTag)
	backend.cookiePath.Value = &path
	resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
	require.False(t, resolved.policies[0].targetsPending())
	require.Equal(t, []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: "pool-1"}, {Weight: 1, PoolID: "pool-remote"}},
		resolved.policies[0].weightedPlan.targets)
	for _, capability := range []xBackendCookieCapability{xBackendCookieUnavailable, xBackendCookieUnverified} {
		part.sessionCookieCapability = capability
		resolved = resolveWeightedHTTPPart(weighted, desired.routes[0], actual.routes[0], "")
		require.False(t, resolved.policies[0].targetsPending())
		require.Equal(t, []octavia.WeightedHTTPTarget{{Weight: 3, PoolID: "pool-1"}, fixedWeightedHTTPTarget(1)},
			resolved.policies[0].weightedPlan.targets)
	}
}

func TestXBackendCookieStockFallbackProgramsTheExactPolicyWithoutMembers(t *testing.T) {
	wanted, observed := convergedRoutePlanFixture()
	wanted.identity = safetyTestIdentity(t)
	wanted.spec.Identity = wanted.identity.Value()
	wanted.xBackend, wanted.sessionCookieName = true, "reports_session"
	wanted.spec.Gateway.LoadBalancerID = "lb"
	var poolRequest map[string]json.RawMessage
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST /lbaas/pools", r.Method+" "+r.URL.Path)
		var request struct {
			Pool map[string]json.RawMessage `json:"pool"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		poolRequest = request.Pool
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"pool":{"id":"pool-1"}}`))
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{})
	for _, capability := range []xBackendCookieCapability{xBackendCookieUnavailable, xBackendCookieUnverified} {
		wanted.sessionCookieCapability = capability
		// In the real TLS case the positive TLS plan remains blocked behind
		// cookie discovery. The placeholder uses only the stock pool API.
		wanted.backendTLS = backendTLSPlan{phase: backendTLSClaim}
		steps, _, err := buildRouteEnsurePlan(wanted, observedRouteGraph{})
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, routeMutationCreatePool, steps[0].kind)
		require.NoError(t, p.executeRouteMutation(t.Context(), wanted, steps[0]))
		require.Equal(t, "true", string(poolRequest["admin_state_up"]))
		require.NotContains(t, poolRequest, "session_persistence")
		require.NotContains(t, poolRequest, "backend_tls_hostname")

		empty := observed
		empty.members, empty.monitor, empty.policies = nil, nil, nil
		empty.pool.ProvisioningStatus, empty.backendTLS.AdminObserved = "ACTIVE", true
		steps, _, err = buildRouteEnsurePlan(wanted, empty)
		require.NoError(t, err)
		require.NotEmpty(t, steps)
		require.Equal(t, routeMutationCreatePolicy, steps[0].kind)
		require.Equal(t, "/api", steps[0].policy.path)
		require.Equal(t, "pool-1", steps[0].parentID)

		empty.policies = observed.policies
		empty.policies[0].policy.AdminStateUp = false
		steps, _, err = buildRouteEnsurePlan(wanted, empty)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, routeMutationEnablePolicy, steps[0].kind)
		empty.policies[0].policy.AdminStateUp = true
		steps, _, err = buildRouteEnsurePlan(wanted, empty)
		require.Error(t, err)
		require.Empty(t, steps)
	}
}

func TestXBackendCookieErrorPoolRecoversAfterHealthyWorkAndWithdrawal(t *testing.T) {
	desired, actual := weightedPlanFixture()
	actual.loadBalancer.ProvisioningStatus = "ACTIVE"
	parts := desired.routes[0].httpParts
	healthy, wanted, rule := &parts[0], &parts[1], &parts[2]
	wanted.xBackend, wanted.sessionCookieName, wanted.sessionCookieCapability = true, "new_session", xBackendCookieAvailable
	graphs := actual.routes[0].httpParts
	failed := graphs[wanted.backendKey]
	failed.pool.ProvisioningStatus, failed.backendTLS.AdminObserved = "ERROR", true
	failed.pool.Persistence = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: "old_session"}
	recover, err := planXBackendCookie(*wanted, *failed)
	require.NoError(t, err)
	require.NotNil(t, recover)
	require.True(t, recover.retry)
	require.Equal(t, new(true), recover.poolAdmin)
	require.Nil(t, recover.poolPersistence)

	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT /lbaas/pools/pool-remote", r.Method+" "+r.URL.Path)
		var request struct {
			Pool map[string]bool `json:"pool"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, map[string]bool{"admin_state_up": true}, request.Pool)
		safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": failed.pool.ID}})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{})
	require.NoError(t, p.executeRouteMutation(t.Context(), *wanted, *recover))
	healthyPool := graphs[healthy.backendKey]
	healthyPool.members = nil
	steps := weightedPlan(t, desired, actual)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationCreateMember, steps[0].route.kind)

	wanted.sessionCookieCapability = xBackendCookieUnavailable
	policy := &graphs[rule.backendKey].policies[0]
	steps = weightedPlan(t, desired, actual)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], fixedWeightedHTTPTarget(1)}, *steps[0].route.weightedTargets)

	failed.pool.ProvisioningStatus = "ACTIVE"
	drain, err := planXBackendCookie(*wanted, *failed)
	require.NoError(t, err)
	require.Equal(t, routeMutationClearPoolMembers, drain.kind)
	require.True(t, drain.persistenceDrain)

	// An ERROR pool cannot drain until Octavia recovers it. Close the
	// pool as part of the repair when the old path still has members.
	failed.pool.ProvisioningStatus = "ERROR"
	wanted.sessionCookieCapability = xBackendCookieAvailable
	path := "/reports"
	wanted.sessionCookiePath = &path
	recover, err = planXBackendCookie(*wanted, *failed)
	require.NoError(t, err)
	require.True(t, recover.retry)
	require.Equal(t, new(false), recover.poolAdmin)
	require.Equal(t, &pools.SessionPersistence{}, recover.poolPersistence)
	require.Contains(t, *recover.poolTags, backendTLSManagedTag)
}

func TestXBackendCookieDeadlineRecoversWhenPoolReadbackStillMatches(t *testing.T) {
	desired, actual := weightedPlanFixture()
	actual.loadBalancer.Provider, actual.loadBalancer.ProvisioningStatus = "amphora", "ACTIVE"
	actual.listener.ProvisioningStatus = "ACTIVE"
	parts := desired.routes[0].httpParts
	healthy, wanted, rule := &parts[0], &parts[1], &parts[2]
	wanted.xBackend, wanted.sessionCookieName = true, "reports_session"
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 60000, Scope: strings.Repeat("a", 64)}
	graphs := actual.routes[0].httpParts
	failed := graphs[wanted.backendKey]

	status, retries := "ERROR", 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{
				octavia.GatewayCapabilityHTTPCookieName, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout,
			}})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{map[string]any{
				"id": "pool-remote", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN",
				"admin_state_up": true, "provisioning_status": status,
				"tags": []string{xBackendCookieTag, xBackendCookieDeadlineTag},
				"session_persistence": map[string]any{
					"type": "HTTP_COOKIE", "cookie_name": "reports_session",
					"cookie_absolute_timeout_ms": 60000, "cookie_scope": wanted.sessionCookieDeadline.Scope,
				},
			}}})
		case "PUT /lbaas/pools/pool-remote":
			var request struct {
				Pool map[string]bool `json:"pool"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, map[string]bool{"admin_state_up": true}, request.Pool)
			retries++
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "pool-remote", "provisioning_status": "PENDING_UPDATE"}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{XBackendSessionPersistence: true})
	read := func() {
		t.Helper()
		inventory, err := p.octavia.ListPoolInventory(t.Context(), pools.ListOpts{})
		require.NoError(t, err)
		require.Len(t, inventory.Items, 1)
		failed.pool = &inventory.Items[0]
		failed.backendTLS = inventory.BackendTLS[failed.pool.ID]
		failed.cookieDeadline = inventory.CookieDeadline[failed.pool.ID]
		require.Equal(t, wanted.sessionCookieDeadline, failed.cookieDeadline.Value)
		require.Equal(t, status, failed.pool.ProvisioningStatus)
		require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	}
	plan := func() ([]graphMutation, cloud.RouteResult) {
		t.Helper()
		steps, result, err := p.buildGatewayGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, result.Routes, 1)
		return steps, result.Routes[0].Result
	}

	read()
	steps, result := plan()
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	policy := &graphs[rule.backendKey].policies[0]
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], fixedWeightedHTTPTarget(1)}, *steps[0].route.weightedTargets)
	policy.weightedTargets = *steps[0].route.weightedTargets
	require.Equal(t, cloud.OutcomeProgressing, result.Outcome.State)
	require.Equal(t, cloud.SessionPersistenceDeadlineReadbackUnverified, result.BackendSessionPersistenceIssue)

	healthyPool := graphs[healthy.backendKey]
	members := healthyPool.members
	healthyPool.members = nil
	steps, _ = plan()
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationCreateMember, steps[0].route.kind)
	healthyPool.members = members

	steps, _ = plan()
	require.NotEmpty(t, steps)
	recovery := steps[0].route
	require.Equal(t, routeMutationSetXBackendPersistence, recovery.kind)
	require.True(t, recovery.retry)
	require.NoError(t, p.executeRouteMutation(t.Context(), *wanted, recovery))
	require.Equal(t, 1, retries)

	status = "PENDING_UPDATE"
	read()
	steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeProgressing, result.Outcome.State)
	require.Equal(t, cloud.SessionPersistenceDeadlineReadbackUnverified, result.BackendSessionPersistenceIssue)

	status = "ACTIVE"
	read()
	steps, result = plan()
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], {Weight: 1, PoolID: failed.pool.ID}}, *steps[0].route.weightedTargets)
	policy.weightedTargets = *steps[0].route.weightedTargets
	steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeReady, result.Outcome.State, "%+v", result.Outcome)
	require.Equal(t, cloud.SessionPersistenceAvailable, result.BackendSessionPersistenceIssue)
}

func TestXBackendCookieDeadlineUnavailableDoesNotBlockOtherRoutes(t *testing.T) {
	desired, actual := weightedPlanFixture()
	actual.loadBalancer.Provider, actual.loadBalancer.ProvisioningStatus = "amphora", "ACTIVE"
	actual.listener.ProvisioningStatus = "ACTIVE"
	wanted := &desired.routes[0].httpParts[1]
	wanted.xBackend, wanted.sessionCookieName = true, "reports_session"
	wanted.sessionCookieDeadline = &cloud.HTTPSessionCookieDeadline{TimeoutMS: 60000, Scope: strings.Repeat("b", 64)}
	named := actual.routes[0].httpParts[wanted.backendKey]
	named.members = nil

	peer, peerObserved := convergedRoutePlanFixture()
	peer.spec.Identity.RouteName = "healthy-peer"
	peer.policies[0].path, peer.policies[0].position = "/peer", 2
	desired.routes = append(desired.routes, peer)
	desired.spec.Routes = append(desired.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure})
	actual.routes = append(actual.routes, observedRouteGraph{})

	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", r.Method+" "+r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityHTTPCookieName}})
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client}, ProviderConfig{XBackendSessionPersistence: true})
	require.Nil(t, p.prepareXBackendPersistence(t.Context(), &desired, actual))
	steps, result, err := p.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, result.Routes, 2)
	require.Equal(t, cloud.SessionPersistenceDeadlineUnsupported, result.Routes[0].Result.BackendSessionPersistenceIssue)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationSetPolicyTarget, steps[0].route.kind)
	policy := &actual.routes[0].httpParts[desired.routes[0].httpParts[2].backendKey].policies[0]
	require.Equal(t, []octavia.WeightedHTTPTarget{policy.weightedTargets[0], fixedWeightedHTTPTarget(1)}, *steps[0].route.weightedTargets)
	policy.weightedTargets = *steps[0].route.weightedTargets

	steps, result, err = p.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.NotEmpty(t, steps)
	require.Equal(t, 1, steps[0].routeIndex)
	require.Equal(t, routeMutationCreatePool, steps[0].route.kind)
	require.Equal(t, cloud.SessionPersistenceDeadlineUnsupported, result.Routes[0].Result.BackendSessionPersistenceIssue)

	peerObserved.pool.ID, peerObserved.pool.ProvisioningStatus = "pool-peer", "ACTIVE"
	peerPolicy := &peerObserved.policies[0]
	peerPolicy.policy.ID, peerPolicy.policy.RedirectPoolID = "policy-peer", peerObserved.pool.ID
	peerPolicy.policy.ListenerID, peerPolicy.policy.Position = actual.listener.ID, peer.policies[0].position
	peerPolicy.rules[0].rule.Value = peer.policies[0].path
	actual.routes[1] = peerObserved
	steps, result, err = p.buildGatewayGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Empty(t, steps)
	require.Equal(t, cloud.SessionPersistenceDeadlineUnsupported, result.Routes[0].Result.BackendSessionPersistenceIssue)
	require.Equal(t, cloud.SessionPersistenceAvailable, result.Routes[1].Result.BackendSessionPersistenceIssue)
	require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)
}

func TestXBackendCookieBusyParentSkipsCapabilityObservation(t *testing.T) {
	desired, actual := weightedPlanFixture()
	part := &desired.routes[0].httpParts[1]
	part.xBackend, part.sessionCookieName = true, "reports_session"
	actual.routes, actual.scoped = nil, false
	actual.outcome = cloud.ProgressingOutcome("Octavia load balancer is PENDING_UPDATE", 1)
	p := &Provider{}
	result := p.pendingGraphResult(t.Context(), desired, actual)
	require.True(t, result.Routes[0].Result.ObservationDeferred)
	require.Equal(t, cloud.SessionPersistenceNotObserved, result.Routes[0].Result.BackendSessionPersistenceIssue)

	path := "/reports"
	part.sessionCookiePath = &path
	result = p.pendingGraphResult(t.Context(), desired, actual)
	require.True(t, result.Routes[0].Result.ObservationDeferred)
	require.Equal(t, cloud.SessionPersistenceNotObserved, result.Routes[0].Result.BackendSessionPersistenceIssue)
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
	require.Equal(t, routeMutationSetXBackendPersistence, plan[0].kind)
	require.Nil(t, plan[0].poolAdmin)

	observed.pool.Persistence = *plan[0].poolPersistence
	observed.pool.Tags = append(*plan[0].poolTags, backendTLSManagedTag, backendTLSRevisionTag(desired.spec.BackendTLS))
	plan, _, err = buildRouteEnsurePlan(desired, observed)
	require.NoError(t, err)
	require.Equal(t, routeMutationSetBackendTLS, plan[0].kind)
	require.Equal(t, new(true), plan[0].poolAdmin)
}
