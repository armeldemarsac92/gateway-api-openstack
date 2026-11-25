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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

const independentSNITestCA = "https://barbican.example/v1/secrets/ca"

func verifiedIndependentSNITestTLS() backendTLSPlan {
	return backendTLSPlan{phase: backendTLSVerified, settings: octavia.BackendTLSSettings{CAReference: independentSNITestCA}}
}

func independentSNIPoolsTestRoute(id Identity) cloud.TLSSNIRoute {
	ca := []byte("fixture CA")
	endpoint := []cloud.Member{{Address: "192.0.2.10", Port: 8443}}
	return cloud.TLSSNIRoute{
		Identity: id.Value(), Hostname: "app.example.test",
		BackendPools: []cloud.TLSSNIBackendPool{
			{Weight: 7, ServiceUID: "ordinary", ServicePort: 443, Members: endpoint},
			{Weight: 2, ServiceUID: "authenticated", ServicePort: 443, Members: endpoint,
				BackendTLS: &cloud.BackendTLSValidation{Hostname: "backend.example.test", CAConfigMapUID: "ca",
					CADigest: fmt.Sprintf("%x", sha256.Sum256(ca)), CAPEM: ca}},
			{Weight: 1},
		},
	}
}

func activeIndependentSNIFragment(target desiredSNIFragment) *observedSNIFragment {
	fragment := activeSNIFragment(target, "weighted")
	fragment.pool = nil
	fragment.policy.Action = string(octavia.ActionWeightedTargets)
	fragment.policy.RedirectPoolID = ""
	fragment.backendPools = make(map[int]*observedSNIFragment)
	for slot, backend := range target.route.BackendPools {
		leafTarget := target.backendPool(slot)
		leaf := activeSNIFragment(leafTarget, fmt.Sprintf("backend-%d", slot))
		leaf.policy, leaf.rule = nil, nil
		leaf.pool.AdminStateUp = backend.ServiceUID != ""
		leaf.backendTLS = octavia.PoolBackendTLS{AdminObserved: true, EnabledObserved: true}
		leaf.pool.Tags = append(leaf.pool.Tags, openstackidentity.SNIPoolSlotTag(slot))
		if backend.ServiceUID != "" {
			leaf.pool.Tags = append(leaf.pool.Tags, sniBackendServiceTag(leafTarget.route))
		}
		if backend.BackendTLS != nil {
			leaf.pool.Tags = append(leaf.pool.Tags, backendTLSManagedTag, backendTLSRevisionTag(backend.BackendTLS))
			leaf.pool.TLSEnabled = true
			leaf.pool.CATLSContainerRef = independentSNITestCA
			leaf.backendTLS.Hostname = backend.BackendTLS.Hostname
			leaf.backendTLS.HostnameObserved, leaf.backendTLS.CAObserved, leaf.backendTLS.ClientRefObserved = true, true, true
		}
		fragment.backendPools[slot] = leaf
		fragment.backendTargets = append(fragment.backendTargets, octavia.WeightedTCPPolicyTarget{PoolID: leaf.pool.ID, Weight: backend.Weight})
	}
	return fragment
}

func largeSNIPoolsTestRoute(id Identity) cloud.TLSSNIRoute {
	return cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "app.example.test", BackendPools: []cloud.TLSSNIBackendPool{
		{Weight: 257, ServiceUID: "stable", ServicePort: 443, Members: []cloud.Member{
			{Address: "192.0.2.10", Port: 8443}, {Address: "192.0.2.11", Port: 8443},
		}},
		{Weight: 128, ServiceUID: "canary", ServicePort: 443, Members: []cloud.Member{{Address: "192.0.2.12", Port: 8443}}},
	}}
}

func TestSNIExactWeightedPoolsRequireRegionalTransportCapabilities(t *testing.T) {
	weighted := octavia.GatewayCapabilityTLSSNIWeightedPools
	passthrough := octavia.GatewayCapabilityTLSSNIPassthrough
	weightedPassthrough := octavia.GatewayCapabilityTLSSNIPassthroughWeightedPools
	for _, tc := range []struct {
		name         string
		protocol     string
		capabilities []string
		available    bool
	}{
		{"terminate with existing weighted pools", cloud.ListenerProtocolTLSTerminate, []string{weighted}, true},
		{"mixed terminate keeps existing capability", cloud.ListenerProtocolTLSMixed, []string{weighted}, true},
		{"passthrough with all three", cloud.ListenerProtocolTLSSNI, []string{weighted, passthrough, weightedPassthrough}, true},
		{"passthrough with legacy capabilities", cloud.ListenerProtocolTLSSNI, []string{weighted, passthrough}, false},
		{"passthrough without base passthrough", cloud.ListenerProtocolTLSSNI, []string{weighted, weightedPassthrough}, false},
		{"passthrough without base weighted", cloud.ListenerProtocolTLSSNI, []string{passthrough, weightedPassthrough}, false},
		{"passthrough with indeterminate discovery", cloud.ListenerProtocolTLSSNI, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := safetyTestIdentity(t)
			targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{largeSNIPoolsTestRoute(id)})
			require.NoError(t, err)
			desired, actual := weightedSNITestGraph(id, targets)
			desired.protocol, actual.protocol = tc.protocol, tc.protocol
			actual.rawTLSVerified, actual.rawTLSCapabilityVerified = true, true
			actual.loadBalancer.Provider = "amphora"
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", req.Method+" "+req.URL.Path)
				if tc.capabilities == nil {
					http.Error(w, "discovery unavailable", http.StatusServiceUnavailable)
					return
				}
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": tc.capabilities})
			}))
			provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
			require.NoError(t, provider.prepareSNIRouteBackendTLS(t.Context(), &desired, actual))
			require.Equal(t, !tc.available, desired.sni[0].weightedBlocked)
			steps, result, err := provider.buildSNIGraphPlan(desired, actual)
			require.NoError(t, err)
			if tc.available {
				require.Len(t, steps, 1)
				require.Equal(t, sniCreatePool, steps[0].sni.kind)
			} else {
				require.Empty(t, steps)
				require.Contains(t, result.Routes[0].Result.Outcome.Message, "Exact TLSRoute backend shares")
			}
		})
	}
}

func TestSNIExactWeightedPolicyWirePreservesMixedPassthrough(t *testing.T) {
	id := safetyTestIdentity(t)
	route := largeSNIPoolsTestRoute(id)
	route.Passthrough = true
	route.BackendPools[1] = cloud.TLSSNIBackendPool{Weight: 128}
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, targets)
	desired.protocol, actual.protocol = cloud.ListenerProtocolTLSMixed, cloud.ListenerProtocolTLSMixed
	actual.state.ListenerID = "listener"
	fragment := activeIndependentSNIFragment(targets[0])
	fragment.policy, fragment.rule, fragment.backendTargets = nil, nil, nil
	step, ok := nextSNIFragmentMutation(targets[0], fragment)
	require.True(t, ok)
	require.Equal(t, sniCreatePolicy, step.sni.kind)

	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "POST /lbaas/l7policies", req.Method+" "+req.URL.Path)
		var body map[string]map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		policy := body["l7policy"]
		require.Equal(t, string(octavia.ActionWeightedTargets), policy["action"])
		require.Equal(t, true, policy["tcp_tls_passthrough"])
		require.Equal(t, false, policy["admin_state_up"])
		require.NotContains(t, policy, "redirect_pool_id")
		require.Equal(t, []any{
			map[string]any{"pool_id": "pool-backend-0", "weight": float64(257)},
			map[string]any{"pool_id": "pool-backend-1", "weight": float64(128)},
		}, policy["targets"])
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": "weighted"}})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))
	readback := activeIndependentSNIFragment(targets[0])
	readback.passthrough = new(true)
	require.True(t, readback.ready(targets[0]))
	readback.passthrough = new(false)
	require.False(t, readback.ready(targets[0]), "a terminating policy cannot prove the passthrough target")
}

func TestSNIExactWeightedDiscoveryPreservesProvenTrafficAndWithdrawsRevocation(t *testing.T) {
	id := safetyTestIdentity(t)
	route := largeSNIPoolsTestRoute(id)
	old, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, old)
	actual.loadBalancer.Provider = "amphora"
	previous := activeIndependentSNIFragment(old[0])
	actual.sni[old[0].key] = previous
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", req.Method+" "+req.URL.Path)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	plan := func(targets []desiredSNIFragment) []graphMutation {
		t.Helper()
		desired.sni = slices.Clone(targets)
		require.NoError(t, provider.prepareSNIRouteBackendTLS(t.Context(), &desired, actual))
		require.True(t, desired.sniBackendPoolDiscoveryPending)
		require.True(t, desired.sni[0].weightedBlocked)
		steps, _, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		return steps
	}
	require.Empty(t, plan(old))
	cause := provider.verifySNICapability(t.Context(), desired, actual)
	result, err := provider.handleSNICapabilityError(t.Context(), desired, actual, cloud.GatewayGraphResult{}, cause)
	require.NoError(t, err)
	require.Equal(t, cloud.OutcomeProgressing, result.Gateway.Outcome.State)
	_, err = provider.prepareGraphMutation(t.Context(), desired, actual, graphMutation{kind: graphEnableListener})
	require.ErrorContains(t, err, "before reopening the frontend")

	route.BackendPools[1] = cloud.TLSSNIBackendPool{Weight: 128}
	revoked, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	steps := plan(revoked)
	require.Len(t, steps, 1)
	require.Equal(t, previous.backendPools[1].pool.ID, steps[0].sni.parentID)
	require.Equal(t, sniDisableBackendTLSPool, steps[0].sni.kind)
	previous.backendPools[1].pool.AdminStateUp = false
	require.Empty(t, plan(revoked))
	require.True(t, previous.policy.AdminStateUp)
	require.True(t, previous.backendPools[0].pool.AdminStateUp)
}

func TestSNIExactWeightedReplacementStagesBehindAuthorizedFlatPool(t *testing.T) {
	id := safetyTestIdentity(t)
	route := largeSNIPoolsTestRoute(id)
	flat := route
	flat.BackendPools = nil
	for _, backend := range route.BackendPools {
		flat.BackendServices = append(flat.BackendServices, cloud.TLSBackendService{UID: backend.ServiceUID, Port: backend.ServicePort})
		for _, member := range backend.Members {
			member.Weight = 1
			flat.Members = append(flat.Members, member)
		}
	}
	slices.Reverse(flat.BackendServices)
	old, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{flat})
	require.NoError(t, err)
	wanted, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, wanted)
	desired.sniPassthroughBackendPoolsCapable = true
	predecessor := activeSNIFragment(old[0], "flat")
	predecessor.pool.Tags = append(predecessor.pool.Tags, sniWeightedPoolTag)
	predecessor.backendTLS = octavia.PoolBackendTLS{AdminObserved: true, EnabledObserved: true}
	actual.sni[old[0].key] = predecessor
	provider := &Provider{}
	next := func() graphMutation {
		t.Helper()
		steps, _, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		return steps[0]
	}
	stage := next()
	require.Equal(t, graphSNIMutation, stage.kind)
	require.Equal(t, sniCreatePool, stage.sni.kind)
	require.True(t, stage.sni.preferPolicy)
	require.True(t, predecessor.policy.AdminStateUp)

	safeReplacement := activeIndependentSNIFragment(wanted[0])
	safeReplacement.policy.ID = "replacement"
	actual.sni[wanted[0].key] = safeReplacement
	cutover := next()
	require.Equal(t, gatewayDisableL7Policy, cutover.deletion.resource)
	require.Equal(t, predecessor.policy.ID, cutover.deletion.resourceID)

	route.BackendPools[1] = cloud.TLSSNIBackendPool{Weight: 128}
	desired.sni, err = buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	delete(actual.sni, wanted[0].key)
	require.Equal(t, predecessor.policy.ID, next().deletion.resourceID, "the flat policy with a revoked destination must close immediately")
}

func TestSNIExactWeightedLargeLeafUsesLeastConnections(t *testing.T) {
	id := safetyTestIdentity(t)
	route := largeSNIPoolsTestRoute(id)
	route.BackendPools[0].Members = nil
	for i := range cloud.MaxTCPRoundRobinMembers + 1 {
		route.BackendPools[0].Members = append(route.BackendPools[0].Members, cloud.Member{
			Address: fmt.Sprintf("10.1.%d.%d", i/256, i%256), Port: 8443,
		})
	}
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, targets)
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "POST /lbaas/pools", req.Method+" "+req.URL.Path)
		var body map[string]map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		require.Equal(t, string(pools.LBMethodLeastConnections), body["pool"]["lb_algorithm"])
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "large"}})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	step, ok := nextSNIFragmentMutation(targets[0], nil)
	require.True(t, ok)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))

	readback := activeIndependentSNIFragment(targets[0])
	require.False(t, readback.ready(targets[0]))
	readback.backendPools[0].pool.LBMethod = string(pools.LBMethodLeastConnections)
	require.True(t, readback.ready(targets[0]))
	require.Equal(t, string(pools.LBMethodRoundRobin), readback.backendPools[1].pool.LBMethod)

	smaller := route
	smaller.BackendPools = slices.Clone(route.BackendPools)
	smaller.BackendPools[0].Members = route.BackendPools[0].Members[1:]
	old, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{smaller})
	require.NoError(t, err)
	previous := activeIndependentSNIFragment(old[0])
	require.True(t, targets[0].permitsBackendPool(previous.backendPools[0]), "adding an authorized endpoint leaves the prior round-robin pool safe during staging")
}

func TestSNIExactWeightedPoolsWithdrawBeforeReplacingOneRevokedShare(t *testing.T) {
	for _, tc := range []struct {
		name       string
		capability bool
		scalar     bool
	}{
		{"native replacement", true, false},
		{"legacy exact replacement after opt-out", false, true},
		{"capability unavailable without exact replacement", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := safetyTestIdentity(t)
			route := largeSNIPoolsTestRoute(id)
			route.Passthrough = true
			peer := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "peer.example.test", ServiceUID: "peer",
				Members: []cloud.Member{{Address: "192.0.2.25", Port: 443}}}
			peer.Identity.RouteUID = "peer"
			old, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route, peer})
			require.NoError(t, err)
			revised := route
			revised.BackendPools = slices.Clone(route.BackendPools)
			revised.BackendPools[1] = cloud.TLSSNIBackendPool{Weight: 128}
			if tc.capability || tc.scalar {
				revised.BackendServiceMembers, revised.BackendPools = revised.BackendPools, nil
				revised.BackendServices = []cloud.TLSBackendService{{UID: "stable", Port: 443}}
				revised.Members = slices.Clone(route.BackendPools[0].Members)
				for i := range revised.Members {
					revised.Members[i].Weight = 1
				}
				revised.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 128, TotalWeight: 385}
			}
			wanted, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{revised, peer})
			require.NoError(t, err)
			desired, actual := weightedSNITestGraph(id, wanted)
			desired.protocol, actual.protocol = cloud.ListenerProtocolTLSMixed, cloud.ListenerProtocolTLSMixed
			actual.rawTLSVerified, actual.rawTLSCapabilityVerified = true, true
			actual.loadBalancer.Provider = "amphora"
			previous, neighbor := activeIndependentSNIFragment(old[0]), activeSNIFragment(old[1], "neighbor")
			previous.passthrough = new(true)
			actual.sni[old[0].key], actual.sni[old[1].key] = previous, neighbor

			capabilities := []string{octavia.GatewayCapabilityTLSSNIWeightedPools, octavia.GatewayCapabilityTLSSNIPassthrough,
				octavia.GatewayCapabilityTCPConnectionRejection}
			if tc.capability {
				capabilities = append(capabilities, octavia.GatewayCapabilityTLSSNIPassthroughWeightedPools)
			}
			client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, "GET /lbaas/providers/amphora/gateway_capabilities", req.Method+" "+req.URL.Path)
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
			}))
			provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"},
				ProviderConfig{TCPConnectionRejection: tc.capability || tc.scalar})
			require.NoError(t, provider.prepareSNIRouteBackendTLS(t.Context(), &desired, actual))
			require.Equal(t, !tc.scalar, desired.sni[0].usesBackendPools())
			steps, result, err := provider.buildSNIGraphPlan(desired, actual)
			require.NoError(t, err)
			require.Len(t, steps, 1)
			require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)
			require.Equal(t, sniDisableBackendTLSPool, steps[0].sni.kind)
			require.Equal(t, previous.backendPools[1].pool.ID, steps[0].sni.parentID)
			previous.backendPools[1].pool.AdminStateUp = false

			steps, result, err = provider.buildSNIGraphPlan(desired, actual)
			require.NoError(t, err)
			require.Len(t, steps, 1)
			if !tc.capability && !tc.scalar {
				require.Equal(t, gatewayDisableL7Policy, steps[0].deletion.resource)
				require.Equal(t, previous.policy.ID, steps[0].deletion.resourceID)
				return
			}
			require.Equal(t, sniCreatePool, steps[0].sni.kind)
			require.True(t, steps[0].sni.preferPolicy)
			require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)
			require.True(t, previous.backendPools[0].pool.AdminStateUp)
			require.True(t, previous.policy.AdminStateUp)
		})
	}
}

func TestSNIIndependentBackendPoolsStageStrictTransportAndExactTargets(t *testing.T) {
	id := safetyTestIdentity(t)
	route := independentSNIPoolsTestRoute(id)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	target := targets[0]
	target.backendPoolPlans = []backendTLSPlan{{}, {phase: backendTLSConfigure,
		settings: octavia.BackendTLSSettings{CAReference: independentSNITestCA}}, {}}

	var calls []string
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls = append(calls, req.URL.Path)
		var body map[string]map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		switch req.URL.Path {
		case "/lbaas/pools":
			pool := body["pool"]
			require.Equal(t, false, pool["admin_state_up"])
			require.Equal(t, true, pool["tls_enabled"])
			require.Equal(t, route.BackendPools[1].BackendTLS.Hostname, pool["backend_tls_hostname"])
			require.Equal(t, target.backendPoolPlans[1].settings.CAReference, pool["ca_tls_container_ref"])
			require.Contains(t, pool["tags"], openstackidentity.SNIPoolSlotTag(1))
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]string{"id": "created"}})
		case "/lbaas/l7policies":
			policy := body["l7policy"]
			require.Equal(t, string(octavia.ActionWeightedTargets), policy["action"])
			require.Equal(t, false, policy["admin_state_up"])
			require.Equal(t, false, policy["tcp_tls_passthrough"])
			require.Equal(t, []any{
				map[string]any{"pool_id": "pool-backend-0", "weight": float64(7)},
				map[string]any{"pool_id": "pool-backend-1", "weight": float64(2)},
				map[string]any{"pool_id": "pool-backend-2", "weight": float64(1)},
			}, policy["targets"])
			require.NotContains(t, policy, "redirect_pool_id")
			safetyWriteJSON(t, w, map[string]any{"l7policy": map[string]string{"id": "weighted"}})
		default:
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	desired, actual := weightedSNITestGraph(id, targets)
	actual.protocol = cloud.ListenerProtocolTLSMixed
	actual.state.ListenerID, actual.state.LoadBalancerID = "listener", "lb"

	fragment := activeIndependentSNIFragment(target)
	fragment.policy, fragment.rule, fragment.backendTargets = nil, nil, nil
	delete(fragment.backendPools, 1)
	step, ok := nextSNIFragmentMutation(target, fragment)
	require.True(t, ok)
	require.Equal(t, sniCreatePool, step.sni.kind)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))

	target.backendPoolPlans[1].phase = backendTLSVerified
	readback := activeIndependentSNIFragment(target)
	fragment.backendPools[1] = readback.backendPools[1]
	step, ok = nextSNIFragmentMutation(target, fragment)
	require.True(t, ok)
	require.Equal(t, sniCreatePolicy, step.sni.kind)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))
	require.Equal(t, []string{"/lbaas/pools", "/lbaas/l7policies"}, calls)

	require.True(t, readback.ready(target))
	readback.backendTargets[1].Weight = 1
	require.False(t, readback.ready(target), "the staged targets must match their observed weights")
}

func TestSNIIndependentBackendPoolsWithdrawOnlyUnavailableShare(t *testing.T) {
	id := safetyTestIdentity(t)
	route := independentSNIPoolsTestRoute(id)
	peer := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "peer.example.test", ServiceUID: "peer",
		Members: []cloud.Member{{Address: "192.0.2.25", Port: 443}}}
	peer.Identity.RouteUID = "peer"
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route, peer})
	require.NoError(t, err)
	targets[0].backendPoolPlans = []backendTLSPlan{{}, verifiedIndependentSNITestTLS(), {}}
	desired, actual := weightedSNITestGraph(id, targets)
	desired.protocol = cloud.ListenerProtocolTLSTerminate
	desired.sniBackendPoolsCapable = true
	actual.protocol, actual.rawTLSVerified, actual.rawTLSCapabilityVerified = desired.protocol, true, true
	weighted, sibling := activeIndependentSNIFragment(targets[0]), activeSNIFragment(targets[1], "sibling")
	actual.sni[targets[0].key], actual.sni[targets[1].key] = weighted, sibling
	provider := &Provider{}
	targets[0].backendPoolPlans[1].phase = backendTLSBlocked
	steps, result, err := provider.buildSNIGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, sniDisableBackendTLSPool, steps[0].sni.kind)
	require.Equal(t, weighted.backendPools[1].pool.ID, steps[0].sni.parentID)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
	require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)

	weighted.backendPools[1].pool.AdminStateUp = false
	steps, result, err = provider.buildSNIGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Empty(t, steps, "the owned disabled pool still reserves its share")
	require.Equal(t, cloud.OutcomeReady, result.Gateway.Outcome.State)
	require.True(t, weighted.policy.AdminStateUp)
	require.True(t, weighted.backendPools[0].pool.AdminStateUp)

	targets[0].backendPoolPlans[1].phase = backendTLSVerified
	steps, _, err = provider.buildSNIGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, sniEnableBackendTLSPool, steps[0].sni.kind)
	require.Equal(t, weighted.backendPools[1].pool.ID, steps[0].sni.parentID)
}

func TestSNIIndependentBackendPoolsRejectMalformedOrForeignTargetReadback(t *testing.T) {
	id := safetyTestIdentity(t)
	route := independentSNIPoolsTestRoute(id)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	weighted := activeIndependentSNIFragment(targets[0])
	target := octavia.L7PolicyTarget{AdminStateUp: new(true)}
	row := octavia.L7PolicyObservation{
		Policy: *weighted.policy, Target: &target, PoolReference: json.RawMessage("null"),
		WeightedTCPPolicyTargets: []octavia.WeightedTCPPolicyTarget{
			{PoolID: weighted.backendPools[0].pool.ID, Weight: 7},
			{PoolID: "pool-from-another-fragment", Weight: 2},
			{PoolID: weighted.backendPools[2].pool.ID, Weight: 1},
		},
	}
	weighted.observeBackendTargets(row)
	require.True(t, weighted.targetsUnverified)
	require.False(t, weighted.matches(targets[0]))
}

func TestSNIIndependentBackendPoolsWithdrawBeforeCoveringPolicyCanInherit(t *testing.T) {
	id := safetyTestIdentity(t)
	route := independentSNIPoolsTestRoute(id)
	cover := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "*.example.test", ServiceUID: "cover",
		Members: []cloud.Member{{Address: "192.0.2.25", Port: 443}}}
	cover.Identity.RouteUID = "cover"
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route, cover})
	require.NoError(t, err)
	targets[0].weightedBlocked = true
	desired, actual := weightedSNITestGraph(id, targets)
	desired.protocol = cloud.ListenerProtocolTLSTerminate
	actual.protocol, actual.rawTLSVerified, actual.rawTLSCapabilityVerified = desired.protocol, true, true
	weighted, wildcard := activeIndependentSNIFragment(targets[0]), activeSNIFragment(targets[1], "cover")
	actual.sni[targets[0].key], actual.sni[targets[1].key] = weighted, wildcard

	provider := &Provider{}
	steps, _, err := provider.buildSNIGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, gatewayDisableL7Policy, steps[0].deletion.resource)
	require.Equal(t, wildcard.policy.ID, steps[0].deletion.resourceID)

	wildcard.policy.AdminStateUp = false
	steps, _, err = provider.buildSNIGraphPlan(desired, actual)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, gatewayDisableL7Policy, steps[0].deletion.resource)
	require.Equal(t, weighted.policy.ID, steps[0].deletion.resourceID)
}

func TestSNIIndependentBackendTransitionKeepsAuthorizedSharesAcrossFailedStage(t *testing.T) {
	id := safetyTestIdentity(t)
	old := independentSNIPoolsTestRoute(id)
	old.BackendPools[2] = cloud.TLSSNIBackendPool{Weight: 1, ServiceUID: "revoked", ServicePort: 443,
		Members: []cloud.Member{{Address: "192.0.2.16", Port: 8443}}}
	// The originally protected Service remains in the replacement. Its
	// observed CA drifts while a revoked Service's slot disappears.
	revised := old
	revised.BackendPools = slices.Clone(old.BackendPools)
	revised.BackendPools[2] = revised.BackendPools[0]
	revised.BackendPools[0] = cloud.TLSSNIBackendPool{Weight: 1}
	current, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{old})
	require.NoError(t, err)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{revised})
	require.NoError(t, err)
	targets[0].backendPoolPlans = []backendTLSPlan{{}, verifiedIndependentSNITestTLS(), {}}
	desired, actual := weightedSNITestGraph(id, targets)
	desired.protocol, desired.sniBackendPoolsCapable = cloud.ListenerProtocolTLSTerminate, true
	actual.protocol, actual.rawTLSVerified, actual.rawTLSCapabilityVerified = desired.protocol, true, true
	actual.state.ListenerID, actual.state.LoadBalancerID = "listener", "lb"
	predecessor := activeIndependentSNIFragment(current[0])
	actual.sni[current[0].key] = predecessor
	predecessor.backendPools[1].pool.CATLSContainerRef = "https://barbican.example/v1/secrets/wrong-ca"
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "POST /lbaas/pools", req.Method+" "+req.URL.Path)
		http.Error(w, "quota", http.StatusForbidden)
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	plan := func() graphMutation {
		t.Helper()
		steps, _, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		return steps[0]
	}
	withdraw := plan()
	require.Equal(t, sniDisableBackendTLSPool, withdraw.sni.kind)
	require.Equal(t, predecessor.backendPools[1].pool.ID, withdraw.sni.parentID)
	predecessor.backendPools[1].pool.AdminStateUp = false
	withdrawRevoked := plan()
	require.Equal(t, sniDisableBackendTLSPool, withdrawRevoked.sni.kind)
	require.Equal(t, predecessor.backendPools[2].pool.ID, withdrawRevoked.sni.parentID)
	predecessor.backendPools[2].pool.AdminStateUp = false
	stage := plan()
	require.Equal(t, sniCreatePool, stage.sni.kind)
	require.Error(t, provider.executeSNIMutation(t.Context(), desired, actual, stage.sni))
	require.Equal(t, sniCreatePool, plan().sni.kind, "an unsuccessful create keeps staging behind the old selector")
	require.True(t, predecessor.backendPools[0].pool.AdminStateUp, "the healthy Service moved to another slot")
	require.True(t, predecessor.policy.AdminStateUp)
	replacement := activeIndependentSNIFragment(targets[0])
	replacement.policy.ID = "replacement-policy"
	replacement.policy.AdminStateUp = false
	detached := *replacement
	detached.policy, detached.rule, detached.backendTargets = nil, nil, nil
	actual.sni[targets[0].key] = &detached
	createPolicy := plan()
	require.Equal(t, sniCreatePolicy, createPolicy.sni.kind)
	require.True(t, createPolicy.sni.preferPolicy, "the successor must win when it is enabled")
	actual.sni[targets[0].key] = replacement
	activate := plan()
	require.Equal(t, sniEnablePolicy, activate.sni.kind)
	require.Equal(t, replacement.policy.ID, activate.sni.parentID)
	replacement.policy.AdminStateUp = true
	complete := plan()
	require.Equal(t, gatewayDisableL7Policy, complete.deletion.resource)
	require.Equal(t, predecessor.policy.ID, complete.deletion.resourceID)
}

func TestSNIIndependentBackendWitnessSurvivesPolicyDeletionAndCapabilityLoss(t *testing.T) {
	id := safetyTestIdentity(t)
	original := independentSNIPoolsTestRoute(id)
	alias := cloud.TLSSNIRoute{
		Identity: id.Value(), Hostname: "alias.example.test",
		ServiceUID: original.BackendPools[1].ServiceUID, ServicePort: original.BackendPools[1].ServicePort,
		Members: original.BackendPools[1].Members,
	}
	alias.Identity.RouteUID, alias.Identity.RouteName = "different-route", "alias"
	current, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{original})
	require.NoError(t, err)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{alias})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, targets)
	desired.protocol, desired.spec.Gateway.SNIPoliciesObserved = cloud.ListenerProtocolTLSTerminate, true
	actual.protocol, actual.rawTLSVerified, actual.rawTLSCapabilityVerified = desired.protocol, true, true
	actual.loadBalancer.Provider = "amphora"
	old := activeIndependentSNIFragment(current[0])
	old.policy, old.rule, old.backendTargets = nil, nil, nil
	for _, leaf := range old.backendPools {
		leaf.pool.AdminStateUp = false
	}
	actual.sni[current[0].key] = old
	capabilities := append(tcpBackendTLSCapabilities(), octavia.GatewayCapabilityTLSSNIWeightedPools)
	var writes []string
	clients := scalarBackendTLSClients(t, id.Value().OpenStackProjectID, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodGet, req.Method)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
	}), &writes)
	provider := newTestProvider(t, clients, ProviderConfig{BackendCA: backendca.New(id.Value().ClusterID, clients), BackendTLSPolicy: true, TCPTLSTermination: true})
	plan := func() (desiredGatewayGraph, []graphMutation) {
		t.Helper()
		wanted := desired
		wanted.sni = slices.Clone(targets)
		require.NoError(t, provider.prepareSNIRouteBackendTLS(t.Context(), &wanted, actual))
		steps, _, err := provider.buildSNIGraphPlan(wanted, actual)
		require.NoError(t, err)
		return wanted, steps
	}
	for _, slot := range []int{0, 2} {
		_, steps := plan()
		require.Len(t, steps, 1)
		require.Equal(t, gatewayDeletionPool, steps[0].deletion.resource)
		require.Equal(t, old.backendPools[slot].pool.ID, steps[0].deletion.resourceID)
		delete(old.backendPools, slot)
	}
	_, steps := plan()
	require.Len(t, steps, 1)
	require.Equal(t, sniCreatePool, steps[0].sni.kind, "a verified plaintext transition may stage, but the old pool still witnesses the Service port")
	capabilities = []string{octavia.GatewayCapabilityTCPTLSTermination, octavia.GatewayCapabilityTLSSNIWeightedPools}
	prepared, steps := plan()
	require.Empty(t, steps, "capability loss after policy deletion must never stage or enable plaintext")
	require.Empty(t, prepared.sni)
	require.True(t, prepared.sniHeldClosed[current[0].key])
	require.Empty(t, writes)
}

func TestSNIIndependentBackendRefusesExplicitHTTPMatchKey(t *testing.T) {
	id := safetyTestIdentity(t)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{independentSNIPoolsTestRoute(id)})
	require.NoError(t, err)
	targets[0].backendPoolPlans = []backendTLSPlan{{}, verifiedIndependentSNITestTLS(), {}}
	fragment := activeIndependentSNIFragment(targets[0])
	require.True(t, fragment.ready(targets[0]))
	fragment.observeBackendRule(octavia.RuleObservation{Rule: *fragment.rule, KeyValue: json.RawMessage(`""`)})
	require.False(t, fragment.ready(targets[0]))
}

func TestSNIIgnoreAnotherServicesEndpointWhenPreservingNativeLeafDuringFlattening(t *testing.T) {
	id := safetyTestIdentity(t)
	old := independentSNIPoolsTestRoute(id)
	shared := old.BackendPools[0].Members[0]
	second := cloud.Member{Address: "192.0.2.21", Port: shared.Port}
	replaced := cloud.Member{Address: "192.0.2.22", Port: shared.Port}
	old.BackendPools[2] = cloud.TLSSNIBackendPool{Weight: 1, ServiceUID: "second", ServicePort: 443, Members: []cloud.Member{second}}
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{old})
	require.NoError(t, err)
	predecessor := activeIndependentSNIFragment(targets[0])
	firstBackend := cloud.TLSBackendService{UID: old.BackendPools[0].ServiceUID, Port: 443}
	secondBackend := cloud.TLSBackendService{UID: old.BackendPools[2].ServiceUID, Port: 443}
	flattened := cloud.TLSSNIRoute{
		Identity: old.Identity, Hostname: old.Hostname,
		BackendServices: []cloud.TLSBackendService{firstBackend, secondBackend},
		BackendServiceMembers: []cloud.TLSSNIBackendPool{
			{ServiceUID: firstBackend.UID, ServicePort: firstBackend.Port, Members: []cloud.Member{replaced}},
			{ServiceUID: secondBackend.UID, ServicePort: secondBackend.Port, Members: []cloud.Member{shared, second}},
		},
		Members: []cloud.Member{
			{Address: shared.Address, Port: shared.Port, Weight: 1},
			{Address: second.Address, Port: second.Port, Weight: 1},
			{Address: replaced.Address, Port: replaced.Port, Weight: 1},
		},
	}
	targets, err = buildSNITargets(id.Value(), []cloud.TLSSNIRoute{flattened})
	require.NoError(t, err)
	require.False(t, targets[0].permitsBackendPool(predecessor.backendPools[0]),
		"the former endpoint now belongs only to the second Service")
	require.True(t, targets[0].permitsBackendPool(predecessor.backendPools[2]),
		"the second Service's previously authorized endpoint remains available")
	require.False(t, targets[0].permitsBackendPool(predecessor.backendPools[1]), "the protected destination was withdrawn")
	targets[0].route.Members = slices.DeleteFunc(slices.Clone(targets[0].route.Members), func(member cloud.Member) bool {
		return member.Address == second.Address
	})
	require.False(t, targets[0].permitsBackendPool(predecessor.backendPools[2]),
		"provenance must not authorize a destination missing from the current forwarding graph")
}
