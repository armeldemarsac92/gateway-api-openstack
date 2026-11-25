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
	"sync/atomic"
	"testing"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

func TestActiveRequestMirrorsAccountForPoolListenerReferences(t *testing.T) {
	listener := pools.ListenerID{ID: "ingress"}
	route := observedRouteGraph{
		policies: []observedRoutePolicy{{
			policy: l7policies.L7Policy{ID: "policy", ListenerID: listener.ID, Position: 1, AdminStateUp: true},
			rules:  []observedRouteRule{{}},
			requestMirrors: []octavia.HTTPRequestMirror{
				{PoolID: "route-copy", Numerator: 1, Denominator: 1},
				{PoolID: "zero-copy", Numerator: 0, Denominator: 100},
			},
			weightedTargets: []octavia.WeightedHTTPTarget{{
				PoolID: "primary", Weight: 1,
				RequestMirrors: []octavia.HTTPRequestMirror{
					{PoolID: "backend-copy", Numerator: 1, Denominator: 4},
				},
			}},
		}},
	}
	for _, id := range []string{"primary", "route-copy", "backend-copy", "zero-copy"} {
		refs := []pools.ListenerID{listener}
		if id == "zero-copy" {
			refs = []pools.ListenerID{}
		}
		route.part(id).pool = &pools.Pool{ID: id, Protocol: cloud.ListenerProtocolHTTP, Listeners: refs}
	}
	require.NoError(t, validateScopedHTTPRouteRelationships([]observedRouteGraph{route}))

	route.policies[0].requestMirrors = nil
	require.ErrorIs(t, validateScopedHTTPRouteRelationships([]observedRouteGraph{route}), cloud.ErrDependencyNotReady)
	route.part("route-copy").pool.Listeners = []pools.ListenerID{}
	require.NoError(t, validateScopedHTTPRouteRelationships([]observedRouteGraph{route}))
}

func TestRequestMirrorWithdrawalPreservesPrimaryAndOtherFilters(t *testing.T) {
	stable := desiredHTTPMirror{backendKey: "allowed", numerator: 2, denominator: 7, phase: cloud.HTTPMirrorAfterHeaders}
	revoked := octavia.HTTPRequestMirror{PoolID: "d3b2b6ee-3469-48a1-b60f-6c59a66b1c82", Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorBeforeTransforms}
	allowed := stable.forPool("e1cab506-0bcb-4705-8f0e-22f16e5b9cf7")
	observed := observedRoutePolicy{
		role: "match", adminObserved: true, requestMirrorsReported: true, requestMirrorsTagged: true,
		policy: l7policies.L7Policy{ID: "policy", ProvisioningStatus: "ACTIVE", AdminStateUp: true,
			Tags: []string{"ownership", openstackidentity.HTTPRequestMirrorTag}},
		requestMirrors: []octavia.HTTPRequestMirror{allowed, revoked, allowed},
		weightedTargets: []octavia.WeightedHTTPTarget{
			{Weight: 3, PoolID: "primary"},
			{Weight: 1, PoolID: "secondary", RequestMirrors: []octavia.HTTPRequestMirror{revoked, allowed},
				RequestHeaderOperations: []cloud.HTTPHeaderOperation{{Name: "x-request", Operation: cloud.HTTPHeaderSet, Value: "selected"}}},
		},
	}
	wanted := desiredPolicy{role: observed.role, requestMirrors: []desiredHTTPMirror{stable, stable}}
	part := desiredRoute{backendKey: "weighted", policies: []desiredPolicy{wanted},
		weightedTargets: []desiredHTTPWeightedTarget{
			{weight: 3, backendKey: "primary"},
			{weight: 1, backendKey: "secondary", requestMirrors: []desiredHTTPMirror{stable}},
		},
	}
	route := desiredRoute{httpParts: []desiredRoute{part, {backendKey: "allowed"}, {backendKey: "primary"}, {backendKey: "secondary"}}}
	inventory := observedRouteGraph{httpParts: map[string]*observedRouteGraph{
		"allowed":   {pool: &pools.Pool{ID: allowed.PoolID}},
		"primary":   {pool: &pools.Pool{ID: "primary"}},
		"secondary": {pool: &pools.Pool{ID: "secondary"}},
		"weighted":  {policies: []observedRoutePolicy{observed}},
	}}
	desired := desiredGatewayGraph{routes: []desiredRoute{route}, spec: cloud.GatewayGraphSpec{Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}}}}
	actual := observedGatewayGraph{routes: []observedRouteGraph{inventory}}

	step, found := nextRequestMirrorWithdrawal(desired, actual)
	require.True(t, found)
	require.Equal(t, "weighted", step.backendKey)
	require.True(t, step.withdrawHTTPPolicy)
	require.False(t, requestMirrorWriteRequiresCapability(step.route))
	require.Equal(t, []octavia.HTTPRequestMirror{allowed, allowed}, step.route.requestMirrorsUpdate.Value)
	targets := *step.route.weightedTargets
	require.Equal(t, observed.weightedTargets[0], targets[0])
	require.Equal(t, observed.weightedTargets[1].Weight, targets[1].Weight)
	require.Equal(t, observed.weightedTargets[1].RequestHeaderOperations, targets[1].RequestHeaderOperations)
	require.Equal(t, []octavia.HTTPRequestMirror{allowed}, targets[1].RequestMirrors)

	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPut, req.Method)
		require.Equal(t, "/lbaas/l7policies/policy", req.URL.Path)
		var payload struct {
			Policy map[string]json.RawMessage `json:"l7policy"`
		}
		require.NoError(t, json.NewDecoder(req.Body).Decode(&payload))
		require.Len(t, payload.Policy, 2)
		require.NotContains(t, payload.Policy, "action")
		require.NotContains(t, payload.Policy, "request_header_operations")
		require.NotContains(t, payload.Policy, "admin_state_up")
		require.NotContains(t, payload.Policy, "request_url_rewrite")
		require.JSONEq(t, `[{"pool_id":"e1cab506-0bcb-4705-8f0e-22f16e5b9cf7","numerator":2,"denominator":7,"phase":"after_headers"},{"pool_id":"e1cab506-0bcb-4705-8f0e-22f16e5b9cf7","numerator":2,"denominator":7,"phase":"after_headers"}]`, string(payload.Policy["request_mirrors"]))
		var readback []octavia.WeightedHTTPTarget
		require.NoError(t, json.Unmarshal(payload.Policy["targets"], &readback))
		require.Equal(t, targets, readback)
		w.WriteHeader(http.StatusOK)
		safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": "policy"}})
	}), &gatewayTransitionRequests{})
	require.NoError(t, provider.executeRouteMutation(t.Context(), part, step.route))

	policy := &inventory.httpParts["weighted"].policies[0]
	policy.requestMirrors, policy.weightedTargets = step.route.requestMirrorsUpdate.Value, targets
	_, found = nextRequestMirrorWithdrawal(desired, actual)
	require.False(t, found)
	policy.policy.ProvisioningStatus = "ERROR"
	step, found = nextRequestMirrorWithdrawal(desired, actual)
	require.True(t, found)
	require.Equal(t, routeMutationDisablePolicy, step.route.kind)
}

func TestRequestMirrorRegionalDiscoveryUsesEffectiveProvider(t *testing.T) {
	status := http.StatusOK
	capabilities := []string{octavia.GatewayCapabilityHTTPRequestMirror}
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora-regional/gateway_capabilities", req.URL.Path)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}), &gatewayTransitionRequests{})
	provider.httpRequestMirror = true
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-regional"
	actual := observedGatewayGraph{loadBalancer: lb}
	build := func() desiredGatewayGraph {
		return desiredGatewayGraph{spec: cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure}},
			routes: []desiredRoute{{httpParts: []desiredRoute{{policies: []desiredPolicy{{requestMirrors: []desiredHTTPMirror{{backendKey: "mirror"}}}}}}}}}
	}
	desired := build()
	provider.gateRequestMirrors(t.Context(), &desired, actual)
	require.Equal(t, requestMirrorAvailable, desired.routes[0].mirrorAvailability)
	capabilities = []string{}
	provider.gateRequestMirrors(t.Context(), &desired, actual)
	require.Equal(t, requestMirrorMissing, desired.routes[0].mirrorAvailability)
	status = http.StatusServiceUnavailable
	desired = build()
	provider.gateRequestMirrors(t.Context(), &desired, actual)
	require.Equal(t, requestMirrorUnverified, desired.routes[0].mirrorAvailability)
}

func TestGRPCRequestMirrorRequiresRegionalCapabilityForCopiesOnly(t *testing.T) {
	var capabilities atomic.Value
	capabilities.Store([]string{octavia.GatewayCapabilityHTTPRequestMirror, octavia.GatewayCapabilityGRPCContentType})
	provider := completeGraphProvider(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora-regional/gateway_capabilities", req.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities.Load()})
	}), &gatewayTransitionRequests{})
	provider.httpRequestMirror = true
	lb := safetyObservedLoadBalancer()
	lb.Provider = "amphora-regional"
	actual := observedGatewayGraph{loadBalancer: lb}
	copyPolicy := desiredPolicy{requestMirrors: []desiredHTTPMirror{{backendKey: "copy"}}}
	grpcSpec := cloud.RouteSpec{GRPC: true}
	desired := desiredGatewayGraph{
		protocol: cloud.ListenerProtocolHTTP,
		spec:     cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure}},
		routes: []desiredRoute{
			{httpParts: []desiredRoute{{backendKey: "primary", policies: []desiredPolicy{copyPolicy}}}},
			{spec: grpcSpec, httpParts: []desiredRoute{{spec: grpcSpec, backendKey: "primary", policies: []desiredPolicy{copyPolicy}}}},
			{spec: grpcSpec},
		},
	}
	provider.gateRequestMirrors(t.Context(), &desired, actual)
	require.Equal(t, requestMirrorAvailable, desired.routes[0].mirrorAvailability)
	require.Equal(t, requestMirrorMissing, desired.routes[1].mirrorAvailability)
	require.Equal(t, octavia.GatewayCapabilityGRPCRequestMirror, desired.routes[1].mirrorUnavailableCapability)
	require.Equal(t, requestMirrorAvailable, desired.routes[2].mirrorAvailability)

	mirrorWrite := graphMutation{kind: graphRouteMutation, backendKey: "primary", route: routeMutation{
		kind: routeMutationSetRequestMirrors, requestMirrorsUpdate: &octavia.RequestMirrorsUpdate{
			Value: []octavia.HTTPRequestMirror{{PoolID: "shadow", Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite}},
		},
	}}
	_, err := provider.prepareGraphMutation(t.Context(), desired, actual, mirrorWrite)
	require.NoError(t, err)
	mirrorWrite.routeIndex = 1
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, mirrorWrite)
	var blocked *unavailableRouteCapability
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, octavia.GatewayCapabilityGRPCRequestMirror, blocked.capability)

	capabilities.Store([]string{octavia.GatewayCapabilityHTTPRequestMirror, octavia.GatewayCapabilityGRPCRequestMirror})
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, mirrorWrite)
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, octavia.GatewayCapabilityGRPCContentType, blocked.capability)
	capabilities.Store(requestMirrorCapabilities(true))
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, mirrorWrite)
	require.NoError(t, err)

	// Capability withdrawal must permit deleting the installed copies.
	capabilities.Store([]string{})
	mirrorWrite.route.withdrawHTTPPolicy = true
	mirrorWrite.route.requestMirrorsUpdate = &octavia.RequestMirrorsUpdate{}
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, mirrorWrite)
	require.NoError(t, err)
}

func TestGRPCRequestMirrorWithdrawsOnLossOfStrictH2Readback(t *testing.T) {
	const shadowID = "shadow-pool"
	tls := &cloud.BackendTLSValidation{Hostname: "shadow.example"}
	identity := safetyTestIdentity(t).Value()
	provider := &Provider{projectID: identity.OpenStackProjectID, backendH2C: true, httpRequestMirror: true}
	fixture, _ := convergedRoutePlanFixture()
	spec := cloud.RouteSpec{
		Identity: identity, GRPC: true, MemberMode: cloud.MemberModeEndpoint, MemberSubnetID: "subnet",
		HealthMonitor:       fixture.healthMonitor,
		GatewayRequirements: cloud.GatewayRequirements{Provider: "amphora", VIPSubnetID: "vip-subnet", ListenerPort: 80},
		HTTPBackends: []cloud.HTTPBackend{
			{Namespace: "apps", Name: "primary", Port: 80, BackendH2C: true, Matches: []cloud.HTTPPathMatch{{
				PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo",
				RequestMirrors: []cloud.HTTPMirrorTarget{{BackendIndex: 1, Numerator: 2, Denominator: 3, Phase: cloud.HTTPMirrorBeforeTransforms}},
			}}},
			{Namespace: "apps", Name: "shadow", Port: 443, BackendTLS: tls},
		},
	}
	route, err := provider.buildDesiredRoute(spec)
	require.NoError(t, err)
	require.Len(t, route.httpParts, 2)
	i := slices.IndexFunc(route.httpParts, func(part desiredRoute) bool { return len(part.policies) != 0 })
	require.NotEqual(t, -1, i)
	primary, shadow := route.httpParts[i], route.httpParts[1-i]
	primaryPolicy := primary.policies[0]
	require.Equal(t, l7policies.TypeHeader, primaryPolicy.grpcContentTypeRule)
	require.Equal(t, []desiredHTTPMirror{{backendKey: shadow.backendKey, numerator: 2, denominator: 3,
		phase: cloud.HTTPMirrorBeforeTransforms}}, primaryPolicy.requestMirrors)
	desired := desiredGatewayGraph{
		spec:   cloud.GatewayGraphSpec{Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}}},
		routes: []desiredRoute{route},
	}
	live := observedRoutePolicy{
		role: primaryPolicy.role, requestMirrorsTagged: true, requestMirrorsReported: true,
		policy:         l7policies.L7Policy{ID: "primary-policy", AdminStateUp: true, ProvisioningStatus: "ACTIVE", RedirectPoolID: "primary-pool"},
		requestMirrors: []octavia.HTTPRequestMirror{primaryPolicy.requestMirrors[0].forPool(shadowID)},
	}
	shadowReadback := observedRouteGraph{
		pool: &pools.Pool{ID: shadowID, TLSEnabled: true, Tags: []string{backendTLSRevisionTag(tls)}},
		backendTLS: octavia.PoolBackendTLS{
			Hostname: tls.Hostname, HostnameObserved: true, CAObserved: true, ClientRefObserved: true,
			RequireH2: true, RequireH2Observed: true, EnabledObserved: true, AdminObserved: true,
		},
	}
	actual := observedGatewayGraph{routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{
		primary.backendKey: {policies: []observedRoutePolicy{live}}, shadow.backendKey: &shadowReadback,
	}}}}
	_, needed := nextRequestMirrorWithdrawal(desired, actual)
	require.False(t, needed)

	shadowReadback.backendTLS.RequireH2 = false
	step, needed := nextRequestMirrorWithdrawal(desired, actual)
	require.True(t, needed)
	require.True(t, step.withdrawHTTPPolicy)
	require.Equal(t, routeMutationSetRequestMirrors, step.route.kind)
	require.Equal(t, live.policy.ID, step.route.id)
	require.NotNil(t, step.route.requestMirrorsUpdate)
	require.Empty(t, step.route.requestMirrorsUpdate.Value)

	spec.HTTPBackends[1].BackendTLS = nil
	_, err = provider.buildDesiredRoute(spec)
	require.ErrorContains(t, err, "using h2c or authenticated HTTP/2")
}

func TestRequestMirrorConvergesIndependentlyOfRequestHeaders(t *testing.T) {
	desired, actual := convergedRoutePlanFixture()
	wanted, observed := &desired.policies[0], &actual.policies[0]
	mirror := octavia.HTTPRequestMirror{PoolID: "e1cab506-0bcb-4705-8f0e-22f16e5b9cf7", Numerator: 0, Denominator: 87, Phase: cloud.HTTPMirrorAfterRewrite}
	wanted.resolvedRequestMirrors = []octavia.HTTPRequestMirror{mirror}
	observed.adminObserved, observed.policy.ProvisioningStatus = true, "ACTIVE"
	observed.rules[0].flagsObserved, observed.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	steps, _, err := buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, routeMutationSetRequestMirrors, steps[0].kind)
	require.Equal(t, []octavia.HTTPRequestMirror{mirror}, steps[0].requestMirrorsUpdate.Value)
	require.Empty(t, steps[0].requestHeaderOperations)
	require.True(t, requestMirrorWriteRequiresCapability(steps[0]))
	observed.requestMirrors, observed.requestMirrorsReported, observed.requestMirrorsTagged = []octavia.HTTPRequestMirror{mirror}, true, true
	steps, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Empty(t, steps)
	wanted.requestHeaderOperations = []cloud.HTTPHeaderOperation{{Name: "x-request", Operation: cloud.HTTPHeaderSet, Value: "revised"}}
	steps, _, err = buildRouteEnsurePlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, routeMutationSetPolicyHeaders, steps[0].kind)
	require.Nil(t, steps[0].requestMirrorsUpdate)
}

func TestLastBackendRequestMirrorClearsInPlaceWithExplicitReadback(t *testing.T) {
	policy := observedRoutePolicy{
		policy: l7policies.L7Policy{ID: "weighted", AdminStateUp: true, ProvisioningStatus: "ACTIVE",
			Tags: []string{"owner", openstackidentity.HTTPRequestMirrorTag}},
		adminObserved: true, requestMirrorsReported: true, requestMirrorsTagged: true,
		weightedTargets: []octavia.WeightedHTTPTarget{{PoolID: "primary", Weight: 7,
			RequestMirrors: []octavia.HTTPRequestMirror{{PoolID: "revoked", Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite}}}},
	}
	withdraw, ok := requestMirrorWithdrawal(desiredRoute{}, observedRouteGraph{}, desiredRoute{}, desiredPolicy{}, policy)
	require.True(t, ok)
	require.Equal(t, routeMutationSetRequestMirrors, withdraw.kind)
	require.Nil(t, withdraw.requestMirrorsUpdate)
	require.Equal(t, []octavia.WeightedHTTPTarget{{PoolID: "primary", Weight: 7}}, *withdraw.weightedTargets)
	require.Nil(t, withdraw.policyTags)

	policy.weightedTargets = *withdraw.weightedTargets
	cleanTags, ok := requestMirrorWithdrawal(desiredRoute{}, observedRouteGraph{}, desiredRoute{}, desiredPolicy{}, policy)
	require.True(t, ok)
	require.Equal(t, routeMutationSetRequestMirrors, cleanTags.kind)
	require.Equal(t, []string{"owner"}, cleanTags.policyTags)
	require.Nil(t, cleanTags.weightedTargets)

	policy.requestMirrorsReported, policy.requestMirrorsUnknown = false, true
	unverified, ok := requestMirrorWithdrawal(desiredRoute{}, observedRouteGraph{}, desiredRoute{}, desiredPolicy{}, policy)
	require.True(t, ok)
	require.Equal(t, routeMutationDisablePolicy, unverified.kind)
}

func TestMirrorOnlyPoolDoesNotBlockListenerRestoration(t *testing.T) {
	for _, test := range []struct {
		name         string
		availability requestMirrorAvailability
		status       string
	}{
		{"regional capability withdrawn", requestMirrorMissing, "ACTIVE"},
		{"mirror pool in error", requestMirrorAvailable, "ERROR"},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary, observedPrimary := convergedRoutePlanFixture()
			primary.backendKey = "primary"
			observedPrimary.pool.ProvisioningStatus = "ACTIVE"
			observedPrimary.members[0].ProvisioningStatus = "ACTIVE"
			observedPrimary.monitor.ProvisioningStatus = "ACTIVE"
			primary.policies[0].requestMirrors = []desiredHTTPMirror{{backendKey: "shadow",
				numerator: 1, denominator: 1, phase: cloud.HTTPMirrorAfterRewrite}}
			shadow := desiredRoute{backendKey: "shadow", unavailable: true}
			desired := desiredGatewayGraph{spec: cloud.GatewayGraphSpec{
				Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure},
				Routes:  []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure}},
			}, routes: []desiredRoute{{httpParts: []desiredRoute{primary, shadow}, mirrorAvailability: test.availability}}}
			actual := observedGatewayGraph{loadBalancer: &octavia.LoadBalancer{}, listener: &listeners.Listener{ID: "listener"},
				routes: []observedRouteGraph{{httpParts: map[string]*observedRouteGraph{
					"primary": &observedPrimary,
					"shadow":  {pool: &pools.Pool{ID: "shadow-pool", Protocol: "HTTP", ProvisioningStatus: test.status}},
				}}}}
			plan, _, err := (&Provider{}).buildGatewayGraphPlan(desired, actual)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(plan, func(step graphMutation) bool { return step.kind == graphEnableListener }), "%#v", plan)
		})
	}
}

func TestRuleRequestMirrorUsesTransformsWithFixedPrimaryResponse(t *testing.T) {
	for _, weighted := range []bool{false, true} {
		name := "fixed"
		if weighted {
			name = "all weighted targets fixed"
		}
		t.Run(name, func(t *testing.T) {
			identity := safetyTestIdentity(t).Value()
			p := &Provider{projectID: identity.OpenStackProjectID, fixedHTTPResponses: true, weightedHTTPTargets: true,
				httpRequestMirror: true, requestHeaderModifier: true, httpRequestURLRewrite: true}
			path := "/mirrored"
			operations := []cloud.HTTPHeaderOperation{{Name: "x-private", Operation: cloud.HTTPHeaderRemove}}
			match := cloud.HTTPPathMatch{
				PathType: cloud.PathMatchExact, PathValue: "/original", URLRewrite: &cloud.HTTPURLRewrite{ReplaceFullPath: &path},
				RequestHeaderOperations: operations,
				RequestMirrors:          []cloud.HTTPMirrorTarget{{BackendIndex: 0, Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite}},
			}
			fixture, shadow := convergedRoutePlanFixture()
			spec := cloud.RouteSpec{Identity: identity, MemberSubnetID: fixture.spec.MemberSubnetID, HealthMonitor: fixture.healthMonitor,
				GatewayRequirements: cloud.GatewayRequirements{Provider: "amphora", VIPSubnetID: "vip-subnet", ListenerPort: 80},
				HTTPBackends:        []cloud.HTTPBackend{{Namespace: "apps", Name: "shadow", Port: 80, Members: fixture.members}}}
			if weighted {
				spec.WeightedHTTPRules = []cloud.HTTPWeightedRule{{Matches: []cloud.HTTPPathMatch{match},
					Targets: []cloud.HTTPWeightedTarget{{Weight: 3}, {Weight: 1}}}}
			} else {
				spec.FixedHTTPRules = []cloud.HTTPFixedRule{{Matches: []cloud.HTTPPathMatch{match}}}
			}
			route, err := p.buildDesiredRoute(spec)
			require.NoError(t, err)
			i := slices.IndexFunc(route.httpParts, func(part desiredRoute) bool { return len(part.policies) != 0 })
			require.NotEqual(t, -1, i)
			compiled := route.httpParts[i].policies[0]
			require.Equal(t, operations, compiled.requestHeaderOperations)
			require.Equal(t, path, *compiled.requestURLRewrite.ReplaceFullPath)
			mirror := compiled.requestMirrors[0]
			const poolID = "e1cab506-0bcb-4705-8f0e-22f16e5b9cf7"
			shadow.pool.ID, shadow.pool.ProvisioningStatus = poolID, "ACTIVE"
			shadow.members[0].ProvisioningStatus = "ACTIVE"
			shadow.monitor.ProvisioningStatus = "ACTIVE"
			shadow.policies = nil
			inventory := observedRouteGraph{httpParts: map[string]*observedRouteGraph{
				mirror.backendKey: &shadow,
			}}
			graph := desiredGatewayGraph{spec: cloud.GatewayGraphSpec{Routes: []cloud.RouteGraphNode{{Spec: spec, Disposition: cloud.GraphEnsure}}},
				routes: []desiredRoute{route}}
			plan, _, _ := httpPlanningParts(graph, observedGatewayGraph{routes: []observedRouteGraph{inventory}})
			i = slices.IndexFunc(plan.routes, func(part desiredRoute) bool { return len(part.policies) != 0 })
			require.NotEqual(t, -1, i)
			part := plan.routes[i]
			wanted := part.policies[0]
			require.Equal(t, []octavia.HTTPRequestMirror{mirror.forPool(poolID)}, wanted.resolvedRequestMirrors)
			require.Equal(t, operations, wanted.requestHeaderOperations)
			require.Equal(t, path, *wanted.requestURLRewrite.ReplaceFullPath)
			if weighted {
				require.True(t, allWeightedTargetsFixed(wanted.weightedPlan.targets))
			}

			observed := observedRoutePolicy{
				role: wanted.role, adminObserved: true, fixedResponse: !weighted,
				policy: l7policies.L7Policy{ID: "local-response", Position: wanted.position, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
			}
			if weighted {
				observed.weightedTargets = wanted.weightedPlan.targets
			}
			steps, _, err := buildRoutePolicyPlanForMirrorTest(part, observed)
			require.NoError(t, err)
			step := firstRouteMutation(steps)
			require.Equal(t, routeMutationSetRequestMirrors, step.kind)
			require.Equal(t, operations, step.requestHeaderOperations)
			require.True(t, step.requestURLRewriteUpdate.KeepPolicyState)
			require.Equal(t, path, *step.requestURLRewriteUpdate.Value.ReplaceFullPath)
			require.Equal(t, wanted.resolvedRequestMirrors, step.requestMirrorsUpdate.Value)

			observed.requestMirrors, observed.requestMirrorsReported, observed.requestMirrorsTagged = wanted.resolvedRequestMirrors, true, true
			observed.requestHeaderOperations, observed.requestURLRewrite, observed.urlRewriteTagged = operations, wanted.requestURLRewrite, true
			withdraw, found := requestMirrorWithdrawal(route, inventory, desiredRoute{}, desiredPolicy{}, observed)
			require.True(t, found)
			require.Equal(t, routeMutationSetRequestMirrors, withdraw.kind)
			require.Equal(t, []cloud.HTTPHeaderOperation{}, withdraw.requestHeaderOperations)
			require.True(t, withdraw.requestURLRewriteUpdate.KeepPolicyState)
			require.Nil(t, withdraw.requestURLRewriteUpdate.Value)
			require.Empty(t, withdraw.requestMirrorsUpdate.Value)
		})
	}
}

func buildRoutePolicyPlanForMirrorTest(part desiredRoute, observed observedRoutePolicy) ([]routeMutation, cloud.RouteState, error) {
	for _, rule := range rulesFor(part.policies[0], part.hostnameRuleValue) {
		observed.rules = append(observed.rules, observedRouteRule{role: rule.role, flagsObserved: true,
			rule: l7policies.Rule{RuleType: string(rule.ruleType), CompareType: string(rule.compareType),
				Value: rule.value, Key: rule.key, Invert: rule.invert, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}})
	}
	return buildRouteEnsurePlan(part, observedRouteGraph{policies: []observedRoutePolicy{observed}})
}
