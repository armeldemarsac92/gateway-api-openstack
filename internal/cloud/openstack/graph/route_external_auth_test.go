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
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
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

func TestGRPCAuthTransitionPreservesOperatorTags(t *testing.T) {
	identity := safetyTestIdentity(t)
	match := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/protected", "")
	backend := openstackidentity.HTTPFragmentRole(rolePolicyExact, openstackidentity.HTTPBackendKey("app", "origin", 80), match)
	fixed := openstackidentity.HTTPFragmentRole(rolePolicyExact, openstackidentity.HTTPFixedRuleKey(1), match)
	tags := append(safetyRouteTags(t, identity, backend), "team-payments")

	withdrawn, ok := transitionRouteTags(identity, tags, backend, fixed)
	require.True(t, ok)
	require.True(t, identity.MatchesRoute(withdrawn, fixed))
	require.Contains(t, withdrawn, "team-payments")
	restored, ok := transitionRouteTags(identity, withdrawn, fixed, backend)
	require.True(t, ok)
	require.ElementsMatch(t, tags, restored)

	other := identity.Value()
	other.RouteUID += "-other"
	foreign, err := NewIdentity(other)
	require.NoError(t, err)
	_, ok = transitionRouteTags(identity, safetyRouteTags(t, foreign, backend), backend, fixed)
	require.False(t, ok)
}

func TestGRPCAuthorizerWithdrawalOnSharedListener(t *testing.T) {
	for _, test := range []struct {
		name      string
		constrain func(*desiredRoute, *observedRoutePolicy, *observedAuthPool)
	}{
		{name: "active mTLS pool"},
		{name: "pool referenced from another listener", constrain: func(_ *desiredRoute, _ *observedRoutePolicy, pool *observedAuthPool) {
			pool.pool.Listeners = append(pool.pool.Listeners, pools.ListenerID{ID: "listener-2"})
		}},
		{name: "changed path", constrain: func(fixed *desiredRoute, _ *observedRoutePolicy, _ *observedAuthPool) {
			fixed.policies[0].path = "/different"
		}},
		{name: "changed rank", constrain: func(fixed *desiredRoute, _ *observedRoutePolicy, _ *observedAuthPool) {
			fixed.policies[0].position++
		}},
		{name: "different route owner", constrain: func(_ *desiredRoute, old *observedRoutePolicy, _ *observedAuthPool) {
			old.policy.Tags = []string{routeExternalAuthTag}
		}},
		{name: "live policy is not confirmed", constrain: func(_ *desiredRoute, old *observedRoutePolicy, _ *observedAuthPool) {
			old.policy.ProvisioningStatus = "ERROR"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const poolID = "b5f68fe4-1d6d-4bba-8be6-cdd310ae6896"
			identity := safetyTestIdentity(t)
			originKey, fixedKey := openstackidentity.HTTPBackendKey("application", "origin", 80), openstackidentity.HTTPFixedRuleKey(1)
			matchKey := openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/secure", "")
			oldRole := openstackidentity.HTTPFragmentRole(rolePolicyExact, originKey, matchKey)
			fixedRole := openstackidentity.HTTPFragmentRole(rolePolicyExact, fixedKey, matchKey)
			auth := cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC, ServiceUID: "authorizer", ServicePort: 443}
			wanted := desiredPolicy{role: fixedRole, path: "/secure", compareType: l7policies.CompareTypeEqual, position: 2}
			fixed := desiredRoute{identity: identity, backendKey: fixedKey, fixedResponse: true, policies: []desiredPolicy{wanted}}
			tags := safetyRouteTags(t, identity, oldRole)
			old := observedRoutePolicy{role: oldRole, adminObserved: true, versioned: true, targetObserved: true,
				externalAuthPresent: true, externalAuthTagged: true, externalAuthReported: true, externalAuth: policyAuthorizer(poolID, auth),
				policy: l7policies.L7Policy{ID: "protected", ListenerID: "listener-1", ProjectID: "project-a",
					Position: 2, ProvisioningStatus: "ACTIVE", AdminStateUp: true, Action: string(l7policies.ActionRedirectToPool),
					RedirectPoolID: "origin", Tags: append(slices.Clone(tags), routeExternalAuthTag)},
				rules: []observedRouteRule{{role: openstackidentity.HTTPFragmentRole(roleRulePath, originKey, matchKey), flagsObserved: true,
					rule: l7policies.Rule{ID: "secure-path", RuleType: string(l7policies.TypePath), CompareType: string(l7policies.CompareTypeEqual),
						Value: "/secure", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}}}
			authPool := observedAuthPool{pool: pools.Pool{ID: poolID, ProjectID: "project-a", Tags: []string{grpcAuthPoolTag},
				AdminStateUp: true, ProvisioningStatus: "ACTIVE", Listeners: []pools.ListenerID{{ID: "listener-1"}},
				TLSContainerRef: "https://barbican.example/v1/secrets/client"},
				admin: true, members: []pools.Member{{ID: "authorizer-member", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}}
			if test.constrain != nil {
				test.constrain(&fixed, &old, &authPool)
			}
			public := desiredRoute{identity: identity, backendKey: "public",
				policies: []desiredPolicy{{role: rolePolicyExact, path: "/public", compareType: l7policies.CompareTypeEqual, position: 1}}}
			publicPolicy := old
			publicPolicy.role = rolePolicyExact
			publicPolicy.policy.ID, publicPolicy.policy.Position = "public", 1
			publicPolicy.policy.ProvisioningStatus = "ACTIVE"
			publicPolicy.policy.Tags = safetyRouteTags(t, identity, rolePolicyExact)
			publicPolicy.externalAuth, publicPolicy.externalAuthPresent, publicPolicy.externalAuthTagged = nil, false, false
			publicPolicy.rules = slices.Clone(old.rules)
			publicPolicy.rules[0].rule.Value = "/public"
			desired, actual := fixedHTTPPlanFixture(desiredRoute{identity: identity, httpParts: []desiredRoute{public, fixed}},
				observedRouteGraph{httpParts: map[string]*observedRouteGraph{
					originKey: {policies: []observedRoutePolicy{old}},
					"public":  {policies: []observedRoutePolicy{publicPolicy}, pool: &pools.Pool{ID: "origin"}},
				}})
			desired.protocol, desired.spec.Gateway.Spec.ListenerPort = cloud.ListenerProtocolHTTP, 80
			actual.loadBalancer.Provider, actual.loadBalancer.ProvisioningStatus = "amphora", "ACTIVE"
			actual.listener.ProtocolPort, actual.listener.ProvisioningStatus = 80, "ACTIVE"
			actual.listener.L7Policies = []l7policies.L7Policy{{ID: publicPolicy.policy.ID}, {ID: old.policy.ID}}
			actual.adminObserved = map[string]bool{actual.listener.ID: true}
			actual.httpPools = &listenerFilterPoolInventory{auth: map[string]observedAuthPool{"authorizer": authPool}}

			var writes []string
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
					safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": routeAuthorizerTransportCapabilities(cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC})})
					return
				}
				writes = append(writes, r.Method+" "+r.URL.Path)
				var body struct {
					L7Policy json.RawMessage `json:"l7policy"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				want, err := json.Marshal(map[string]any{"action": "FIXED_RESPONSE", "external_auth": nil,
					"fixed_response": map[string]int{"status_code": 500}, "tags": append(slices.Clone(tags), grpcAuthWithdrawalTag)})
				require.NoError(t, err)
				require.JSONEq(t, string(want), string(body.L7Policy))
				safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]any{"id": old.policy.ID}})
			}))
			provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
				ProviderConfig{RouteExternalAuthGRPC: true, FixedHTTPResponses: true})
			result := &cloud.GatewayGraphResult{Gateway: cloud.GatewayResult{ScopedListenerServingPorts: []int{80}}}
			staging := graphMutation{kind: graphRouteMutation, listenerID: actual.listener.ID, backendKey: fixedKey,
				route: routeMutation{kind: routeMutationCreatePolicy, policy: wanted}}
			plan := provider.protectRouteAuthTraffic(t.Context(), desired, actual, []graphMutation{staging}, result)
			require.Len(t, plan, 1)
			if test.constrain != nil {
				require.Equal(t, graphDisableListener, plan[0].kind)
				require.Empty(t, result.Gateway.ScopedListenerServingPorts)
				require.Empty(t, writes)
				return
			}
			require.Equal(t, graphRouteMutation, plan[0].kind)
			require.Equal(t, routeMutationSetPolicyTarget, plan[0].route.kind)
			require.Equal(t, []int{80}, result.Gateway.ScopedListenerServingPorts)
			prepared, err := provider.prepareGraphMutation(t.Context(), desired, actual, plan[0])
			require.NoError(t, err)
			require.NoError(t, provider.executeGraphMutation(t.Context(), desired, actual, prepared))
			require.Equal(t, []string{"PUT /lbaas/l7policies/protected"}, writes)

			// An ordinary policy in flight invalidates the whole-table proof.
			actual.routes[0].httpParts["public"].policies[0].policy.ProvisioningStatus = "PENDING_UPDATE"
			plan = provider.protectRouteAuthTraffic(t.Context(), desired, actual, []graphMutation{staging}, result)
			require.Len(t, plan, 1)
			require.Equal(t, graphDisableListener, plan[0].kind)
			actual.routes[0].httpParts["public"].policies[0].policy.ProvisioningStatus = "ACTIVE"

			// A failed reload can show the new database values while Amphora
			// still routes to the previously active authorizer.
			old.policy.Action, old.policy.RedirectPoolID = string(octavia.ActionFixedResponse), ""
			old.externalAuth, old.externalAuthPresent, old.externalAuthTagged, old.fixedResponse = nil, false, false, true
			old.policy.Tags = append(slices.Clone(tags), grpcAuthWithdrawalTag)
			old.policy.ProvisioningStatus = "ERROR"
			actual.routes[0].httpParts[originKey].policies[0] = old
			plan = provider.protectRouteAuthTraffic(t.Context(), desired, actual, []graphMutation{staging}, result)
			require.Len(t, plan, 1)
			require.Equal(t, graphDisableListener, plan[0].kind)

			old.policy.ProvisioningStatus = "ACTIVE"
			actual.routes[0].httpParts[originKey].policies[0] = old
			require.Equal(t, []graphMutation{staging}, provider.protectRouteAuthTraffic(t.Context(), desired, actual, []graphMutation{staging}, result))
			parts, observed, owners := httpPlanningParts(desired, actual)
			require.True(t, planHTTPPolicyHandovers(desired, parts, observed, owners).protected[old.policy.ID])

			// Restore the grant. The fixed barrier cannot become a forwarding
			// policy before the old authorization pool has a ready replacement.
			forward := wanted
			forward.role, forward.externalAuth = oldRole, &auth
			desired.routes[0].httpParts[1] = desiredRoute{identity: identity, backendKey: originKey, policies: []desiredPolicy{forward}}
			plan = provider.protectRouteAuthTraffic(t.Context(), desired, actual, nil, result)
			require.Len(t, plan, 1)
			require.Equal(t, graphDisableListener, plan[0].kind)
			old.policy.ProvisioningStatus, old.policy.AdminStateUp = "ACTIVE", false
			steps, _, _ := buildRoutePolicyPlan(desired.routes[0].httpParts[1], []observedRoutePolicy{old}, "origin")
			require.Empty(t, steps)
			forward.resolvedExternalAuth = policyAuthorizer(poolID, auth)
			desired.routes[0].httpParts[1].policies[0] = forward
			steps, _, _ = buildRoutePolicyPlan(desired.routes[0].httpParts[1], nil, "origin")
			require.Len(t, steps, 1)
			require.Equal(t, routeMutationCreatePolicy, steps[0].kind)
			require.Equal(t, forward.resolvedExternalAuth, steps[0].policy.resolvedExternalAuth)
		})
	}
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

	capabilities := routeAuthorizerTransportCapabilities(cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC})
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
	ready, step := provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, clearName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.Equal(t, "HTTP", poolCreate["protocol"])
	require.Equal(t, true, poolCreate["backend_h2c"])
	require.Contains(t, poolCreate["tags"], grpcAuthPoolTag)

	pool := observedAuthPool{pool: pools.Pool{ID: "authorizer-pool", Protocol: "HTTP", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		admin: true, h2c: true, tls: octavia.PoolBackendTLS{H2CObserved: true, EnabledObserved: true}}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.Same(t, &auth, step.listenerFilters.protectedAuth)
	require.Equal(t, auth.Members[0], *step.listenerFilters.createMember)
	pool.members = []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port, SubnetID: auth.MemberSubnetID,
		ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	pool.tls.H2CObserved = false
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, clearName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)

	capabilities = routeAuthorizerTransportCapabilities(tlsAuth)
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, observedAuthPool{}, false)
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
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)
	pool.tls.SystemCADigestObserved, pool.tls.SystemCASHA256 = true, "verified"
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	pool.tls.RequireH2 = false
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, pool, false)
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
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.True(t, step.listenerFilters.claimCA)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.True(t, claimed)
	desired.spec.Gateway.BackendCAOwned = true
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.Equal(t, firstCert, poolCreate["tls_container_ref"])
	require.Contains(t, poolCreate["tags"], backendTLSClientTag)

	pool.pool.TLSContainerRef = firstCert
	pool.pool.Tags = append(pool.pool.Tags, backendTLSClientTag)
	pool.tls.RequireH2 = true
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, pool, false)
	require.True(t, ready)
	require.Nil(t, step)
	desired.spec.Gateway.BackendClientCertificate = cloud.BackendClientCertificate{Reference: secondCert}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, mtlsName, pool, false)
	require.False(t, ready)
	require.Nil(t, step)

	capabilities = []string{octavia.GatewayCapabilityPolicyExternalAuthGRPC, octavia.GatewayCapabilityFixedHTTPResponses}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &tlsAuth, tlsName, observedAuthPool{}, false)
	require.False(t, ready)
	require.Nil(t, step)
}

func TestHTTPAuthorizerUsesIsolatedTLSHTTP11Pool(t *testing.T) {
	auth := cloud.ListenerExternalAuth{ServiceUID: "authorize-uid", ServicePort: 8443, MemberSubnetID: "subnet",
		PathPrefix: "/check", Members: []cloud.Member{{Address: "192.0.2.25", Port: 8443}}, BackendTLS: &cloud.BackendTLSValidation{
			Trust: cloud.BackendTLSTrustSystem, Hostname: "authorize.example.test",
			SubjectAltNames: []cloud.BackendTLSSubjectAltName{{Type: cloud.BackendTLSSANHostname, Hostname: "authorize.example.test"}},
		}}
	require.NoError(t, validateRouteAuthorizer(auth))
	plainName := listenerAuthPoolName(80, auth)
	name := routeAuthorizerPoolName(80, auth, "")
	require.NotEqual(t, plainName, name)
	grpc := auth
	grpc.Protocol = cloud.ExternalAuthProtocolGRPC
	require.NotEqual(t, routeAuthorizerPoolName(80, grpc, ""), name)

	advertised := []string{
		octavia.GatewayCapabilityPolicyExternalAuth,
		octavia.GatewayCapabilityFixedHTTPResponses,
		octavia.GatewayCapabilityBackendTLSHostname,
		octavia.GatewayCapabilityBackendTLSSubjectAltNames,
		octavia.GatewayCapabilityBackendTLSSystemCA,
	}
	var poolCreate map[string]any
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": advertised})
		case r.Method == http.MethodPost && r.URL.Path == "/lbaas/pools":
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			poolCreate = body.Pool
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "http-tls-pool"}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuth: true, FixedHTTPResponses: true, BackendTLSPolicy: true})
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer()}
	actual.loadBalancer.Provider = "amphora"
	desired := desiredGatewayGraph{identity: safetyTestIdentity(t)}
	ready, step := provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, name, observedAuthPool{}, false)
	require.False(t, ready)
	require.Nil(t, step)
	capable, err := provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.NoError(t, err)
	require.False(t, capable)

	advertised = append(advertised, octavia.GatewayCapabilityPolicyExternalAuthTLS)
	capable, err = provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.NoError(t, err)
	require.True(t, capable)
	auth.ForwardBodyMaxSize = 512
	capable, err = provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.NoError(t, err)
	require.False(t, capable)
	auth.ForwardBodyMaxSize = 0

	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, name, observedAuthPool{}, false)
	require.False(t, ready)
	require.NotNil(t, step)
	require.NoError(t, provider.executeListenerFilterMutation(t.Context(), desired, actual, *step))
	require.Equal(t, "HTTP", poolCreate["protocol"])
	require.Equal(t, true, poolCreate["tls_enabled"])
	require.Equal(t, true, poolCreate["backend_tls_system_ca"])
	require.Equal(t, auth.BackendTLS.Hostname, poolCreate["backend_tls_hostname"])
	require.Contains(t, poolCreate["tags"], httpAuthTLSPoolTag)
	require.NotContains(t, poolCreate["tags"], grpcAuthPoolTag)
	require.NotContains(t, poolCreate["tags"], listenerAuthPoolTag)
	require.NotContains(t, poolCreate, "backend_h2c")
	require.NotContains(t, poolCreate, "backend_tls_require_h2")
	require.NotContains(t, poolCreate, "alpn_protocols")
	require.Empty(t, policyAuthorizer("http-tls-pool", auth).Protocol)

	pool := observedAuthPool{pool: pools.Pool{ID: "http-tls-pool", Protocol: "HTTP", AdminStateUp: true, ProvisioningStatus: "ACTIVE",
		TLSEnabled: true, ALPNProtocols: []string{"h2", "http/1.1", "http/1.0"}, Listeners: []pools.ListenerID{{ID: "listener-1"}},
		Tags: []string{httpAuthTLSPoolTag, backendTLSManagedTag, backendTLSSystemCATag, backendTLSRevisionTag(auth.BackendTLS)}},
		admin: true, members: []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port,
			SubnetID: auth.MemberSubnetID, ProvisioningStatus: "ACTIVE", AdminStateUp: true}},
		tls: octavia.PoolBackendTLS{Hostname: auth.BackendTLS.Hostname, HostnameObserved: true, CAObserved: true, ClientRefObserved: true,
			AdminObserved: true, EnabledObserved: true, H2CObserved: true, RequireH2Observed: true, ALPNObserved: true,
			SystemCA: true, SystemCAObserved: true, SubjectAltNames: auth.BackendTLS.SubjectAltNames, SubjectAltNamesObserved: true}}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, name, pool, false)
	require.False(t, ready)
	require.Nil(t, step)
	pool.tls.SystemCADigestObserved, pool.tls.SystemCASHA256 = true, strings.Repeat("a", 64)
	desired.spec.Gateway.Spec.ListenerPort = 80
	desired.spec.Routes = []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}}
	desired.routes = []desiredRoute{{httpParts: []desiredRoute{{backendKey: "origin",
		policies: []desiredPolicy{{role: rolePolicyExact, position: 1, externalAuth: &auth}}}}}}
	provider.planRouteAuthPools(t.Context(), desired, &actual, map[string]*cloud.ListenerExternalAuth{name: &auth},
		map[string]observedAuthPool{name: pool}, nil, true)
	require.Nil(t, actual.routeAuthPoolMutation)
	parts, _, _ := httpPlanningParts(desired, actual)
	require.Len(t, parts.routes, 1)
	require.Equal(t, policyAuthorizer(pool.pool.ID, auth), parts.routes[0].policies[0].resolvedExternalAuth)
	pool.pool.ALPNProtocols = []string{"h2"}
	ready, step = provider.planProtectedAuthorizerPool(t.Context(), desired, actual, &auth, name, pool, false)
	require.False(t, ready)
	require.Nil(t, step)
}

func TestHTTPAuthorizerRestagesSystemCAAfterClientIdentityReturns(t *testing.T) {
	const ref = "https://barbican.example/v1/secrets/authorizer-client"
	auth := cloud.ListenerExternalAuth{ServiceUID: "authorize-uid", ServicePort: 8443, MemberSubnetID: "subnet",
		Members: []cloud.Member{{Address: "192.0.2.25", Port: 8443}}, BackendTLS: &cloud.BackendTLSValidation{
			Trust: cloud.BackendTLSTrustSystem, Hostname: "authorize.example.test",
		}}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": routeAuthorizerTransportCapabilities(auth)})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuth: true, FixedHTTPResponses: true, BackendTLSPolicy: true})
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer()}
	actual.loadBalancer.Provider = "amphora"
	desired := desiredGatewayGraph{}
	desired.spec.Gateway.BackendCAOwned = true
	desired.spec.Gateway.BackendClientCertificate = cloud.BackendClientCertificate{Reference: ref, Denied: true}
	pool := observedAuthPool{pool: pools.Pool{ID: "http-tls-pool", Protocol: "HTTP", ProvisioningStatus: "ACTIVE", Listeners: []pools.ListenerID{},
		TLSEnabled: true, TLSContainerRef: ref, ALPNProtocols: []string{"h2", "http/1.1", "http/1.0"},
		Tags: []string{httpAuthTLSPoolTag, backendTLSManagedTag, backendTLSClientTag, backendTLSSystemCATag, backendTLSRevisionTag(auth.BackendTLS)}},
		admin: true, tls: octavia.PoolBackendTLS{Hostname: auth.BackendTLS.Hostname, HostnameObserved: true,
			CAObserved: true, ClientRefObserved: true, AdminObserved: true, EnabledObserved: true, H2CObserved: true,
			RequireH2Observed: true, ALPNObserved: true, SystemCA: true, SystemCAObserved: true, SystemCADigestObserved: true}}
	name := routeAuthorizerPoolName(80, auth, ref)
	plan := func(referenced map[string]bool) *graphMutation {
		t.Helper()
		actual.routeAuthPoolMutation = nil
		provider.planRouteAuthPools(t.Context(), desired, &actual, map[string]*cloud.ListenerExternalAuth{name: &auth},
			map[string]observedAuthPool{name: pool}, referenced, true)
		return actual.routeAuthPoolMutation
	}
	require.Nil(t, plan(nil))
	require.Empty(t, actual.routeAuthPools)

	desired.spec.Gateway.BackendClientCertificate.Denied = false
	pool.members = []pools.Member{{ID: "former-member", ProvisioningStatus: "ACTIVE"}}
	require.Nil(t, plan(map[string]bool{pool.pool.ID: true}))
	pool.pool.Listeners = nil
	require.Nil(t, plan(nil))
	pool.pool.Listeners = []pools.ListenerID{}
	step := plan(nil)
	require.NotNil(t, step)
	require.Equal(t, gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: "former-member", parentID: pool.pool.ID}, step.deletion)

	pool.members = nil
	step = plan(nil)
	require.NotNil(t, step)
	require.True(t, step.listenerFilters.enablePool)
	require.Equal(t, pool.pool.ID, step.listenerFilters.poolID)
	require.Empty(t, actual.routeAuthPools)

	pool.pool.AdminStateUp = true
	require.Nil(t, plan(nil))
	require.Empty(t, actual.routeAuthPools)
	pool.tls.SystemCASHA256 = strings.Repeat("a", 64)
	step = plan(nil)
	require.NotNil(t, step)
	require.Equal(t, auth.Members[0], *step.listenerFilters.createMember)
	require.Empty(t, actual.routeAuthPools)
	pool.members = []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port, SubnetID: auth.MemberSubnetID,
		ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	require.Nil(t, plan(nil))
	require.Equal(t, pool.pool.ID, actual.routeAuthPools[name])
}

func TestHTTPAuthorizerTLSPoolBelongsOnlyToRoutePolicies(t *testing.T) {
	const poolID, listenerID = "http-tls-pool", "listener-1"
	identity := safetyTestIdentity(t)
	poolTags, err := identity.GatewayTags(rolePool)
	require.NoError(t, err)
	lb := safetyObservedLoadBalancer()
	pool := pools.Pool{ID: poolID, ProjectID: "project-a", Name: httpAuthTLSPoolNamePrefix + "fixture",
		Protocol: "HTTP", TLSEnabled: true, Tags: append(poolTags, httpAuthTLSPoolTag),
		Loadbalancers: []pools.LoadBalancerID{{ID: lb.ID}}, Listeners: []pools.ListenerID{{ID: listenerID}}, Members: []pools.Member{}}
	l7Policy := l7policies.L7Policy{ID: "protected", ListenerID: listenerID, ProjectID: pool.ProjectID,
		Tags: append(safetyRouteTags(t, identity, rolePolicyExact), routeExternalAuthTag)}
	row := octavia.L7PolicyObservation{Policy: l7Policy, ExternalAuth: &octavia.PolicyExternalAuth{PoolID: poolID},
		ExternalAuthReported: true, ExternalAuthPresent: true}
	actual := observedGatewayGraph{loadBalancer: lb, listener: &listeners.Listener{ID: listenerID, L7Policies: []l7policies.L7Policy{l7Policy}},
		httpsIsolationPolicies: map[string]httpsPolicyInventory{listenerID: {items: []octavia.L7PolicyObservation{row}, complete: true}}}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/pools/"+poolID+"/members", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"members": []any{}})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: pool.ProjectID}, ProviderConfig{})
	desired := desiredGatewayGraph{identity: identity}
	inventory := octavia.PoolInventory{Items: []pools.Pool{pool}}
	owned, err := provider.observeListenerAuthPools(t.Context(), desired, &actual, inventory)
	require.NoError(t, err)
	require.Equal(t, poolID, owned[pool.Name].pool.ID)

	row.ExternalAuth.Protocol = cloud.ExternalAuthProtocolGRPC
	_, err = provider.observeListenerAuthPools(t.Context(), desired, &actual, inventory)
	require.ErrorIs(t, err, cloud.ErrOwnershipConflict)

	row.ExternalAuth.Protocol = ""
	inventory.Items[0].Tags = append(inventory.Items[0].Tags, listenerAuthPoolTag)
	_, err = provider.observeListenerAuthPools(t.Context(), desired, &actual, inventory)
	require.ErrorIs(t, err, cloud.ErrOwnershipConflict)
}

func TestRouteAuthorizerRegionalBodyLimit(t *testing.T) {
	offer := map[string]any{"gateway_capabilities": append(routeAuthorizerTransportCapabilities(cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC}), octavia.GatewayCapabilityPolicyExternalAuthForwardBody)}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, offer)
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuthGRPC: true, FixedHTTPResponses: true})
	auth := cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC, ForwardBodyMaxSize: 1024}
	check := func(allowed bool) {
		t.Helper()
		available, err := provider.RouteExternalAuthAvailable(t.Context(), auth, false)
		require.NoError(t, err)
		require.Equal(t, allowed, available)
	}
	check(false)
	offer["l7policy_external_auth_max_body_bytes"] = 1024
	check(true)
	auth.ForwardBodyMaxSize++
	check(false)
	desired, actual := fixedHTTPPlanFixture(desiredRoute{fixedResponse: true}, observedRouteGraph{})
	desired.protocol = cloud.ListenerProtocolHTTP
	actual.loadBalancer.Provider = "amphora"
	policy := desiredPolicy{externalAuth: &auth, resolvedExternalAuth: policyAuthorizer("12345678-1234-4000-8000-123456789abc", auth)}
	step := graphMutation{kind: graphRouteMutation, route: routeMutation{kind: routeMutationCreatePolicy, policy: policy}}
	_, writeErr := provider.prepareGraphMutation(t.Context(), desired, actual, step)
	var missing *octavia.MissingGatewayCapability
	require.ErrorAs(t, writeErr, &missing)
	require.Equal(t, octavia.GatewayCapabilityPolicyExternalAuthForwardBody, missing.Capability)
	offer["l7policy_external_auth_max_body_bytes"] = "unreadable"
	_, err := provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.Error(t, err)
	auth.ForwardBodyMaxSize = 0
	check(true)
}

func TestRouteAuthorizerWeightedCapabilityAppliesOnlyToWeightedPolicyWrites(t *testing.T) {
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": routeAuthorizerTransportCapabilities(cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC})})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
		ProviderConfig{RouteExternalAuthGRPC: true, FixedHTTPResponses: true, WeightedHTTPTargets: true})
	auth := cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC}
	available, err := provider.RouteExternalAuthAvailable(t.Context(), auth, true)
	require.NoError(t, err)
	require.False(t, available)
	available, err = provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.NoError(t, err)
	require.True(t, available)
	auth.ForwardBodyMaxSize = 1024
	available, err = provider.RouteExternalAuthAvailable(t.Context(), auth, false)
	require.NoError(t, err)
	require.False(t, available)

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
