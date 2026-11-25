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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
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
	actual.planRouteAuthPools(wanted, nil, nil, true)
	require.Empty(t, actual.routeAuthPools)
	require.Equal(t, name, actual.routeAuthPoolMutation.listenerFilters.createPool)

	pool := observedAuthPool{pool: pools.Pool{ID: "check-pool", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}, admin: true}
	actual = observedGatewayGraph{}
	actual.planRouteAuthPools(wanted, map[string]observedAuthPool{name: pool}, nil, true)
	require.Empty(t, actual.routeAuthPools)
	require.Equal(t, auth.Members[0], *actual.routeAuthPoolMutation.listenerFilters.createMember)
	pool.members = []pools.Member{{Address: auth.Members[0].Address, ProtocolPort: auth.Members[0].Port,
		SubnetID: auth.MemberSubnetID, ProvisioningStatus: "ACTIVE", AdminStateUp: true}}
	actual = observedGatewayGraph{}
	actual.planRouteAuthPools(wanted, map[string]observedAuthPool{name: pool}, nil, true)
	require.Nil(t, actual.routeAuthPoolMutation)
	require.Equal(t, pool.pool.ID, actual.routeAuthPools[name])

	pool.pool.ProvisioningStatus = "ERROR"
	pool.members = nil
	owned := map[string]observedAuthPool{name: pool}
	actual = observedGatewayGraph{}
	actual.planRouteAuthPools(wanted, owned, map[string]bool{pool.pool.ID: true}, true)
	require.Nil(t, actual.routeAuthPoolMutation)
	actual.planRouteAuthPools(wanted, owned, nil, true)
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
}
