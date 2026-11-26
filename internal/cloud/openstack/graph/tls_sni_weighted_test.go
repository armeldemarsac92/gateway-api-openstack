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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestSNIWeightedMembersAreStagedBeforePolicyActivation(t *testing.T) {
	id := safetyTestIdentity(t)
	route := weightedSNITestRoute(id)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	target := targets[0]
	fragment := activeWeightedSNIFragment(target, "weighted")
	fragment.policy, fragment.rule, fragment.members = nil, nil, nil
	var requests []string
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /lbaas/pools/pool-weighted/members":
			var body struct{ Member struct{ Weight int } }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, 2, body.Member.Weight)
			requests = append(requests, "member")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"member": map[string]string{"id": "created"}})
		case "PUT /lbaas/pools/pool-weighted/members":
			var body struct {
				Members []struct {
					Address string
					Weight  int
					Tags    []string
				}
				Rejection cloud.TCPConnectionRejection `json:"tcp_connection_rejection"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 3}, body.Rejection)
			require.Len(t, body.Members, 2)
			tags, err := target.tags(roleSNIMember)
			require.NoError(t, err)
			slices.Sort(tags)
			require.Equal(t, []int{2, 6}, []int{body.Members[0].Weight, body.Members[1].Weight})
			for _, member := range body.Members {
				require.Equal(t, tags, member.Tags)
			}
			requests = append(requests, "batch")
			http.Error(w, "member quota", http.StatusForbidden)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	desired, actual := weightedSNITestGraph(id, targets)
	actual.sni[target.key] = fragment
	step, ok := nextSNIFragmentMutation(target, fragment)
	require.True(t, ok)
	require.Equal(t, sniCreateMember, step.sni.kind)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))

	route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 3}
	partial, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	require.NotEqual(t, target.key, partial[0].key)
	target = partial[0]
	_, ok = nextSNIFragmentMutation(target, fragment)
	require.False(t, ok, "omitted rejection readback leaves the pool detached")
	fragment.tcpRejection.Present = true // The extended API reports an explicit null on an empty pool.
	step, ok = nextSNIFragmentMutation(target, fragment)
	require.True(t, ok)
	require.Equal(t, sniReplaceTCPMembers, step.sni.kind)
	require.Error(t, provider.executeSNIMutation(t.Context(), desired, actual, step.sni))
	require.Equal(t, []string{"member", "batch"}, requests)
	step, ok = nextSNIFragmentMutation(target, fragment)
	require.True(t, ok)
	require.Equal(t, sniReplaceTCPMembers, step.sni.kind, "a rejected batch must leave the target detached")

	fragment = activeWeightedSNIFragment(target, "weighted")
	require.True(t, fragment.matches(target))
	fragment.members[0].Weight = 1
	require.False(t, fragment.matches(target))
}

func TestSNIServiceImportOmitsMemberSubnetAndPreservesWeightedRejection(t *testing.T) {
	id := safetyTestIdentity(t)
	route := weightedSNITestRoute(id)
	route.BackendSubnetID = "class-member-subnet"
	route.BackendServices[1].UID = "service-import"
	route.Members[1].RoutedExternal = true
	route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 5}
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	target := targets[0]
	fragment := activeWeightedSNIFragment(target, "import")
	fragment.members[0].SubnetID = route.BackendSubnetID
	require.True(t, fragment.matches(target))
	fragment.members[1].SubnetID = route.BackendSubnetID
	require.False(t, fragment.matches(target), "an imported VIP must be confirmed without a member subnet")

	var writes []string
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		writes = append(writes, req.Method)
		require.Equal(t, "/lbaas/pools/pool-import/members", req.URL.Path)
		var body struct {
			Member    map[string]any
			Members   []map[string]any
			Rejection cloud.TCPConnectionRejection `json:"tcp_connection_rejection"`
		}
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		if req.Method == http.MethodPost {
			require.Equal(t, route.Members[1].Address, body.Member["address"])
			require.Equal(t, float64(route.Members[1].Port), body.Member["protocol_port"])
			require.NotContains(t, body.Member, "subnet_id")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"member": map[string]string{"id": "created"}})
			return
		}
		require.Equal(t, http.MethodPut, req.Method)
		require.Equal(t, *route.TCPConnectionRejection, body.Rejection)
		require.Len(t, body.Members, 2)
		require.Equal(t, route.BackendSubnetID, body.Members[0]["subnet_id"])
		require.NotContains(t, body.Members[1], "subnet_id")
		require.Equal(t, float64(2), body.Members[0]["weight"])
		require.Equal(t, float64(6), body.Members[1]["weight"])
		w.WriteHeader(http.StatusAccepted)
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	desired, actual := weightedSNITestGraph(id, targets)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, sniMutation{
		kind: sniReplaceTCPMembers, target: target, parentID: fragment.pool.ID,
	}))
	native := route
	native.UseBackendPools([]cloud.TLSSNIBackendPool{
		{Weight: 3, ServiceUID: "service-import", ServicePort: 443, Members: []cloud.Member{{Address: route.Members[1].Address, Port: 443, RoutedExternal: true}}},
		{Weight: 2},
	})
	targets, err = buildSNITargets(id.Value(), []cloud.TLSSNIRoute{native})
	require.NoError(t, err)
	leaf := targets[0].backendPool(0)
	require.NoError(t, provider.executeSNIMutation(t.Context(), desired, actual, sniMutation{
		kind: sniCreateMember, target: leaf, parentID: fragment.pool.ID, member: leaf.route.Members[0], poolSlot: new(0),
	}))
	require.Equal(t, []string{http.MethodPut, http.MethodPost}, writes)
}

func TestSNIFailedDetachedBatchDoesNotBlockWithdrawalOrCleanup(t *testing.T) {
	id := safetyTestIdentity(t)
	route := weightedSNITestRoute(id)
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	target := targets[0]
	desired, actual := weightedSNITestGraph(id, nil)
	failed := activeWeightedSNIFragment(target, "failed")
	failed.pool.ProvisioningStatus = "ERROR"
	failed.pool.Listeners = []pools.ListenerID{}
	failed.policy, failed.rule = nil, nil
	failed.members[0].ProvisioningStatus = "PENDING_CREATE"
	actual.sni[target.key] = failed
	obsolete := activeWeightedSNIFragment(target, "obsolete")
	stale := target.key
	stale.target = "retired"
	actual.sni[stale] = obsolete
	provider := &Provider{}
	next := func() graphMutation {
		t.Helper()
		steps, _, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		return steps[0]
	}
	require.Equal(t, obsolete.policy.ID, next().deletion.resourceID)
	delete(actual.sni, stale)
	cleanup := next().deletion
	require.Equal(t, gatewayDeletionPool, cleanup.resource)
	require.Equal(t, failed.pool.ID, cleanup.resourceID)
}

func TestSNIPartialBatchPreservesGuardAndWildcardBeforeRetry(t *testing.T) {
	id := safetyTestIdentity(t)
	route := weightedSNITestRoute(id)
	route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 3}
	fallback := cloud.TLSSNIRoute{Identity: route.Identity, Hostname: "*.example.test", ServiceUID: "wildcard",
		Members: []cloud.Member{{Address: "192.0.2.42", Port: 443}}}
	fallback.Identity.RouteName, fallback.Identity.RouteUID = "wildcard", "wildcard-uid"
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route, fallback})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, targets)
	actual.loadBalancer.Provider = "amphora"
	staged := activeWeightedSNIFragment(targets[0], "weighted")
	staged.policy, staged.rule, staged.members, staged.tcpRejection.Value = nil, nil, nil, nil
	actual.sni[targets[0].key] = staged
	covering := activeSNIFragment(targets[1], "covering")
	covering.policy.AdminStateUp = false
	actual.sni[targets[1].key] = covering
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityTCPConnectionRejection}})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{TCPConnectionRejection: true})
	require.NoError(t, provider.prepareWeightedSNI(t.Context(), &desired, actual))
	require.False(t, desired.sni[0].weightedBlocked)
	next := func() graphMutation {
		t.Helper()
		steps, _, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		return steps[0]
	}
	guard := next().sni.target
	require.True(t, guard.route.Deny)
	actual.sni[guard.key] = activeSNIFragment(guard, "denial")
	resume := next().sni
	require.Equal(t, sniEnablePolicy, resume.kind)
	require.Equal(t, covering.policy.ID, resume.parentID)
	covering.policy.AdminStateUp = true
	for range 2 {
		require.Equal(t, sniReplaceTCPMembers, next().sni.kind, "batch retries can leave the verified guard and wildcard in place")
	}

	broken := activeWeightedSNIFragment(targets[0], "wrong-fraction")
	broken.policy, broken.rule = nil, nil
	broken.tcpRejection.Value = &cloud.TCPConnectionRejection{RejectWeight: 2, TotalWeight: 3}
	actual.sni[targets[0].key] = broken
	desired.sni = targets
	require.NoError(t, provider.prepareWeightedSNI(t.Context(), &desired, actual))
	require.False(t, desired.sni[0].weightedBlocked, "a complete, different fraction authorizes rebuilding the detached pool")
	require.Equal(t, broken.pool.ID, next().deletion.resourceID)
}

func TestSNIWeightedGrantLossRetiresRevokedPoolBeforeReplacement(t *testing.T) {
	id := safetyTestIdentity(t)
	route := weightedSNITestRoute(id)
	old, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route})
	require.NoError(t, err)
	route.BackendServices = route.BackendServices[:1]
	route.Members = route.Members[:1]
	route.TCPConnectionRejection = &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 3}
	broad, sibling := route, route
	broad.Identity.RouteName, broad.Identity.RouteUID, broad.Hostname = "wildcard", "wildcard-uid", "*.example.test"
	sibling.Identity.RouteName, sibling.Identity.RouteUID, sibling.Hostname = "sibling", "sibling-uid", "safe.other.test"
	for _, target := range []*cloud.TLSSNIRoute{&broad, &sibling} {
		target.BackendServices, target.TCPConnectionRejection = nil, nil
		target.ServiceUID, target.Members = "single-service", []cloud.Member{{Address: "192.0.2.50", Port: 443}}
	}
	targets, err := buildSNITargets(id.Value(), []cloud.TLSSNIRoute{route, broad, sibling})
	require.NoError(t, err)
	desired, actual := weightedSNITestGraph(id, targets)
	revoked := activeWeightedSNIFragment(old[0], "revoked")
	actual.sni[old[0].key] = revoked
	actual.sni[targets[1].key] = activeSNIFragment(targets[1], "sibling")
	wildcard := activeSNIFragment(targets[2], "wildcard")
	actual.sni[targets[2].key] = wildcard
	provider := &Provider{} // Rejection capability has been lost.
	require.NoError(t, provider.prepareWeightedSNI(t.Context(), &desired, actual))
	require.True(t, desired.sni[0].weightedBlocked)
	check := func() graphMutation {
		t.Helper()
		steps, result, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
		require.Equal(t, sibling.Identity, result.Routes[1].Identity)
		require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)
		return steps[0]
	}
	require.Equal(t, wildcard.policy.ID, check().deletion.resourceID)
	wildcard.policy.AdminStateUp = false
	require.Equal(t, revoked.policy.ID, check().deletion.resourceID)
	revoked.policy.AdminStateUp = false
	guard := check()
	require.Equal(t, sniCreatePool, guard.sni.kind)
	require.True(t, guard.sni.target.route.Deny)
	actual.sni[guard.sni.target.key] = activeSNIFragment(guard.sni.target, "denial")
	delete(actual.sni, old[0].key)
	require.Equal(t, sniEnablePolicy, check().sni.kind, "a verified guard lets the covering suffix resume")

	current := activeWeightedSNIFragment(targets[0], "unverified")
	actual.sni[targets[0].key] = current
	wildcard.policy.AdminStateUp = true
	current.tcpRejection.Invalid = true
	require.False(t, current.matches(targets[0]))
	require.Equal(t, wildcard.policy.ID, check().deletion.resourceID)
}

func weightedSNITestRoute(id Identity) cloud.TLSSNIRoute {
	return cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "app.example.test",
		BackendServices: []cloud.TLSBackendService{{UID: "service-a", Port: 443}, {UID: "service-b", Port: 443}},
		Members:         []cloud.Member{{Address: "192.0.2.10", Port: 443, Weight: 2}, {Address: "192.0.2.11", Port: 443, Weight: 6}}}
}

func activeWeightedSNIFragment(target desiredSNIFragment, id string) *observedSNIFragment {
	fragment := activeSNIFragment(target, id)
	fragment.pool.Tags = append(fragment.pool.Tags, sniWeightedPoolTag)
	fragment.backendTLS.AdminObserved = true
	for i, member := range target.route.Members {
		fragment.members[i].Weight = member.Weight
	}
	if target.route.TCPConnectionRejection != nil {
		fragment.tcpRejection = octavia.PoolTCPConnectionRejection{Present: true, Value: target.route.TCPConnectionRejection}
	}
	return fragment
}

func weightedSNITestGraph(id Identity, targets []desiredSNIFragment) (desiredGatewayGraph, observedGatewayGraph) {
	desired := desiredGatewayGraph{spec: completeGraphSpec(id), sni: targets, protocol: cloud.ListenerProtocolTLSSNI}
	desired.spec.Gateway.Spec.ExternalNetworkID = ""
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer(), protocol: desired.protocol,
		listener: &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:      make(map[sniFragmentKey]*observedSNIFragment),
	}
	actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.AdminStateUp = "ACTIVE", true
	return desired, actual
}
