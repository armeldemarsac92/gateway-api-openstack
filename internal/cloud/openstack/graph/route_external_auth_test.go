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
	"net/http"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestRouteExternalAuthRequiresAReadyAuthorizationPool(t *testing.T) {
	auth := cloud.ListenerExternalAuth{ServiceUID: "reviews-uid", ServicePort: 8080, MemberSubnetID: "subnet",
		Members: []cloud.Member{{Address: "192.0.2.25", Port: 9000}}, PathPrefix: "/check"}
	require.NoError(t, validateRouteAuthorizer(auth))
	name := listenerAuthPoolName(80, auth)
	wanted := map[string]*cloud.ListenerExternalAuth{name: &auth}
	actual := observedGatewayGraph{}
	(&Provider{}).planRouteAuthPools(t.Context(), desiredGatewayGraph{}, &actual, wanted, nil, nil, true)
	require.Empty(t, actual.routeAuthPools)
	require.Equal(t, name, actual.routeAuthPoolMutation.listenerFilters.createPool)

	pool := observedAuthPool{pool: pools.Pool{ID: "check-pool", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}, admin: true}
	actual = observedGatewayGraph{}
	(&Provider{}).planRouteAuthPools(t.Context(), desiredGatewayGraph{}, &actual, wanted, map[string]observedAuthPool{name: pool}, nil, true)
	require.Empty(t, actual.routeAuthPools)
	require.Equal(t, auth.Members[0], *actual.routeAuthPoolMutation.listenerFilters.createMember)
	pool.members = []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port,
		SubnetID: auth.MemberSubnetID, ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	actual = observedGatewayGraph{}
	(&Provider{}).planRouteAuthPools(t.Context(), desiredGatewayGraph{}, &actual, wanted, map[string]observedAuthPool{name: pool}, nil, true)
	require.Nil(t, actual.routeAuthPoolMutation)
	require.Equal(t, pool.pool.ID, actual.routeAuthPools[name])

	pool.pool.ProvisioningStatus = "ERROR"
	pool.members = nil
	owned := map[string]observedAuthPool{name: pool}
	actual = observedGatewayGraph{}
	(&Provider{}).planRouteAuthPools(t.Context(), desiredGatewayGraph{}, &actual, wanted, owned, map[string]bool{pool.pool.ID: true}, true)
	require.Nil(t, actual.routeAuthPoolMutation)
	(&Provider{}).planRouteAuthPools(t.Context(), desiredGatewayGraph{}, &actual, wanted, owned, nil, true)
	require.NotNil(t, actual.routeAuthPoolMutation)
	require.Equal(t, gatewayDeletionPool, actual.routeAuthPoolMutation.deletion.resource)
	require.Equal(t, pool.pool.ID, actual.routeAuthPoolMutation.deletion.resourceID)
}

func TestRouteExternalAuthChangesOnlyThroughAClosedPolicy(t *testing.T) {
	auth := &cloud.ListenerExternalAuth{PathPrefix: "/review", AllowedRequestHeaders: []string{"x-account"}}
	wanted := desiredPolicy{role: rolePolicyExact, path: "/reports", position: 1, externalAuth: auth}
	route := desiredRoute{policies: []desiredPolicy{wanted}}
	plan, closePlan, _ := buildRoutePolicyPlan(route, nil, "origin")
	require.Empty(t, plan)
	require.Empty(t, closePlan)

	wanted.resolvedExternalAuth = policyAuthorizer("b5f68fe4-1d6d-4bba-8be6-cdd310ae6896", *auth)
	route.policies[0] = wanted
	plan, closePlan, _ = buildRoutePolicyPlan(route, nil, "origin")
	require.Empty(t, closePlan)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationCreatePolicy, plan[0].kind)
	require.Same(t, wanted.resolvedExternalAuth, plan[0].policy.resolvedExternalAuth)

	observed := observedRoutePolicy{role: wanted.role, adminObserved: true, policy: l7policies.L7Policy{
		ID: "reports-policy", Action: string(l7policies.ActionRedirectToPool), RedirectPoolID: "origin", ProvisioningStatus: "ACTIVE",
		Position: 1, AdminStateUp: true}}
	plan, closePlan, _ = buildRoutePolicyPlan(route, []observedRoutePolicy{observed}, "origin")
	require.Empty(t, plan)
	require.Len(t, closePlan, 1)
	require.Equal(t, routeMutationDisablePolicy, closePlan[0].kind)

	observed.policy.AdminStateUp = false
	plan, closePlan, _ = buildRoutePolicyPlan(route, []observedRoutePolicy{observed}, "origin")
	require.Empty(t, closePlan)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationDeletePolicy, plan[0].kind)
	observed.externalAuthTagged, observed.externalAuthPresent, observed.externalAuthReported = true, true, true
	observed.externalAuth = policyAuthorizer(wanted.resolvedExternalAuth.PoolID, *auth)
	require.True(t, wanted.externalAuthMatches(observed))
	route.policies[0].resolvedExternalAuth = nil
	plan, closePlan, _ = buildRoutePolicyPlan(route, []observedRoutePolicy{observed}, "origin")
	require.Empty(t, closePlan)
	require.Len(t, plan, 1)
	require.Equal(t, routeMutationDeletePolicy, plan[0].kind)
	require.Equal(t, observed.policy.ID, plan[0].id)
	route.policies[0] = wanted
	observed.externalAuth = nil
	require.False(t, wanted.externalAuthMatches(observed))
	plan, closePlan, _ = buildRoutePolicyPlan(route, []observedRoutePolicy{observed}, "origin")
	require.Empty(t, plan)
	require.Empty(t, closePlan)
}

func TestRouteExternalAuthOptOutStagesLocalResponseWithListenerClosed(t *testing.T) {
	wanted := desiredPolicy{role: rolePolicyExact, path: "/reports", position: 1}
	fixed := desiredRoute{fixedResponse: true, backendKey: "local-rejection", policies: []desiredPolicy{wanted}}
	installed := observedRoutePolicy{role: wanted.role, externalAuthTagged: true, policy: l7policies.L7Policy{
		ID: "protected", ListenerID: "listener-1", Position: 1, ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	desired, actual := fixedHTTPPlanFixture(desiredRoute{httpParts: []desiredRoute{fixed}}, observedRouteGraph{policies: []observedRoutePolicy{installed}})
	desired.spec.Gateway.Spec.ListenerPort = 80
	actual.listener.ProtocolPort, actual.listener.ProvisioningStatus = 80, "ACTIVE"
	actual.adminObserved = map[string]bool{actual.listener.ID: true}

	create := graphMutation{kind: graphRouteMutation, routeIndex: 0, listenerID: actual.listener.ID, backendKey: fixed.backendKey,
		route: routeMutation{kind: routeMutationCreatePolicy, policy: wanted}}
	filter := func() []graphMutation {
		return (&Provider{}).protectRouteAuthTraffic(t.Context(), desired, actual, []graphMutation{create}, &cloud.GatewayGraphResult{})
	}
	plan := filter()
	require.Len(t, plan, 1)
	require.Equal(t, graphDisableListener, plan[0].kind)
	actual.listener.AdminStateUp = false
	require.Equal(t, []graphMutation{create}, filter())
	desired.routes[0].httpParts[0].fixedResponse = false
	require.Empty(t, filter())
	desired.routes[0].httpParts[0].fixedResponse = true
	desired.routes[0].httpParts[0].policies[0].requestMirrors = []desiredHTTPMirror{{}}
	require.Empty(t, filter())

	desired.routes[0].httpParts[0].policies[0].requestMirrors = nil
	actual.routes[0].policies[0].externalAuth = policyAuthorizer("b5f68fe4-1d6d-4bba-8be6-cdd310ae6896",
		cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC})
	actual.listener.AdminStateUp = true
	plan = filter()
	require.Len(t, plan, 1)
	require.Equal(t, graphDisableListener, plan[0].kind)
	actual.listener.AdminStateUp = false
	require.Equal(t, []graphMutation{create}, filter())
}

func TestGRPCAuthorizerStagesAndVerifiesIndependentTransportPools(t *testing.T) {
	auth := cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC, ServiceUID: "authorize-uid", ServicePort: 8080, MemberSubnetID: "subnet",
		Members: []cloud.Member{{Address: "192.0.2.25", Port: 9080}}}
	clearName := routeAuthorizerPoolName(80, auth, "")
	require.NotEqual(t, listenerAuthPoolName(80, auth), clearName)
	tlsAuth := auth
	tlsAuth.BackendTLS = &cloud.BackendTLSValidation{Trust: cloud.BackendTLSTrustSystem, Hostname: "authorize.example.test", RequireH2: true}
	tlsName := routeAuthorizerPoolName(80, tlsAuth, "")
	require.NotEqual(t, clearName, tlsName)

	capabilities := grpcAuthorizerCapabilities(nil)
	var poolCreate map[string]any
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/pools":
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			poolCreate = body.Pool
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "authorizer-pool"}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuthGRPC: true, FixedHTTPResponses: true, BackendTLSPolicy: true})
	provider.poolALPN = true
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer()}
	actual.loadBalancer.Provider = "amphora"
	desired := desiredGatewayGraph{identity: safetyTestIdentity(t)}
	ready, step := provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &auth, clearName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.Equal(t, "HTTP", poolCreate["protocol"])
	require.Equal(t, true, poolCreate["backend_h2c"])
	require.Contains(t, poolCreate["tags"], grpcAuthPoolTag)

	pool := observedAuthPool{pool: pools.Pool{ID: "authorizer-pool", Protocol: "HTTP", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		admin: true, h2c: true, tls: octavia.PoolBackendTLS{H2CObserved: true, EnabledObserved: true}}
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.Same(t, &auth, step.listenerFilters.grpcAuth)
	require.Equal(t, auth.Members[0], *step.listenerFilters.createMember)
	pool.members = []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port, SubnetID: auth.MemberSubnetID,
		ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	pool.tls.H2CObserved = false
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)

	capabilities = grpcAuthorizerCapabilities(tlsAuth.BackendTLS)
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.NotContains(t, poolCreate, "backend_h2c")
	require.Equal(t, true, poolCreate["tls_enabled"])
	require.Equal(t, true, poolCreate["backend_tls_require_h2"])
	require.Equal(t, true, poolCreate["backend_tls_system_ca"])
	require.Equal(t, []any{"h2"}, poolCreate["alpn_protocols"])
	require.Equal(t, tlsAuth.BackendTLS.Hostname, poolCreate["backend_tls_hostname"])
	pool.h2c, pool.pool.TLSEnabled = false, true
	pool.pool.ALPNProtocols = []string{"h2"}
	pool.pool.Tags = []string{grpcAuthPoolTag, backendTLSSystemCATag, backendTLSRevisionTag(tlsAuth.BackendTLS)}
	pool.tls = octavia.PoolBackendTLS{Hostname: tlsAuth.BackendTLS.Hostname, HostnameObserved: true,
		CAObserved: true, ClientRefObserved: true, AdminObserved: true, EnabledObserved: true,
		RequireH2: true, RequireH2Observed: true, ALPNObserved: true, SystemCA: true, SystemCAObserved: true}
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)
	pool.tls.SystemCADigestObserved, pool.tls.SystemCASHA256 = true, "verified"
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	pool.tls.RequireH2 = false
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)

	const firstCert = "https://barbican.example/v1/secrets/client-1"
	const secondCert = "https://barbican.example/v1/secrets/client-2"
	mtlsName := routeAuthorizerPoolName(80, tlsAuth, firstCert)
	require.NotEqual(t, tlsName, mtlsName)
	require.NotEqual(t, mtlsName, routeAuthorizerPoolName(80, tlsAuth, secondCert))
	require.Equal(t, clearName, routeAuthorizerPoolName(80, auth, firstCert))

	desired.spec.Gateway.BackendClientCertificate.Reference = firstCert
	var claimed bool
	desired.spec.Gateway.ClaimBackendCA = func(context.Context) error { claimed = true; return nil }
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.True(t, step.listenerFilters.claimCA)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.True(t, claimed)
	desired.spec.Gateway.BackendCAOwned = true
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.Equal(t, firstCert, poolCreate["tls_container_ref"])
	require.Contains(t, poolCreate["tags"], backendTLSClientTag)

	pool.pool.TLSContainerRef = firstCert
	pool.pool.Tags = append(pool.pool.Tags, backendTLSClientTag)
	pool.tls.RequireH2 = true
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	desired.spec.Gateway.BackendClientCertificate = cloud.BackendClientCertificate{Reference: secondCert}
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)

	capabilities = []string{octavia.GatewayCapabilityPolicyExternalAuthGRPC, octavia.GatewayCapabilityFixedHTTPResponses}
	ready, step = provider.planGRPCAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.Nil(t, step)
}

func TestRouteAuthorizerWeightedCapabilityAppliesOnlyToWeightedPolicyWrites(t *testing.T) {
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": grpcAuthorizerCapabilities(nil)})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuthGRPC: true, FixedHTTPResponses: true, WeightedHTTPTargets: true})
	available, err := provider.RouteExternalAuthGRPCAvailable(t.Context(), nil, true)
	require.NoError(t, err)
	require.False(t, available)
	available, err = provider.RouteExternalAuthGRPCAvailable(t.Context(), nil, false)
	require.NoError(t, err)
	require.True(t, available)

	desired, actual := fixedHTTPPlanFixture(desiredRoute{fixedResponse: true}, observedRouteGraph{})
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.loadBalancer.Provider = "amphora"
	step := graphMutation{kind: graphRouteMutation, route: routeMutation{kind: routeMutationCreatePolicy}}
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, step)
	require.NoError(t, err)
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphEnableListener})
	require.NoError(t, err)

	desired.routes[0].fixedResponse = false
	desired.routes[0].weightedTargets = []desiredHTTPWeightedTarget{{weight: 1}}
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, step)
	var missing *octavia.MissingGatewayCapability
	require.ErrorAs(t, err, &missing)
	require.Equal(t, octavia.GatewayCapabilityWeightedHTTPTargets, missing.Capability)
}
