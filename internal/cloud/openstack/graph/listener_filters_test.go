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
	"net/http"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestListenerFiltersCloseBeforeReprogrammingAndUseOrderedPools(t *testing.T) {
	capability := true
	discoveryStatus := http.StatusOK
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		if discoveryStatus != http.StatusOK {
			w.WriteHeader(discoveryStatus)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if capability {
			_, _ = w.Write([]byte(`{"gateway_capabilities":["listener_request_filters_v1"]}`))
		} else {
			_, _ = w.Write([]byte(`{"gateway_capabilities":[]}`))
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	spec := completeGraphSpec(safetyTestIdentity(t))
	authFirst := cloud.ListenerExternalAuth{ServiceUID: "one", ServicePort: 8081, PathPrefix: "/first"}
	authSecond := cloud.ListenerExternalAuth{ServiceUID: "two", ServicePort: 8082, PathPrefix: "/second"}
	spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{
		80: {{Hostname: "*"}, {Hostname: "protected.example.test", Requests: []cloud.ListenerExternalAuth{authSecond, authFirst}}},
	}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	loadBalancer := safetyObservedLoadBalancer()
	loadBalancer.ProvisioningStatus, loadBalancer.Provider = "ACTIVE", "amphora"
	actual := observedGatewayGraph{loadBalancer: loadBalancer, scoped: true, adminObserved: map[string]bool{"filtered": true, "plain": true},
		scopedListeners: []listeners.Listener{
			{ID: "filtered", ProtocolPort: 80, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
			{ID: "plain", ProtocolPort: 81, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		}}
	pool := func(id string) observedAuthPool {
		return observedAuthPool{admin: true, pool: pools.Pool{ID: id, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
	}
	owned := map[string]observedAuthPool{listenerAuthPoolName(80, authFirst): pool("pool-1"), listenerAuthPoolName(80, authSecond): pool("pool-2")}

	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphDisableListener, actual.listenerFilterMutation.kind)
	require.Equal(t, "filtered", actual.listenerFilterMutation.listenerID)
	require.False(t, actual.unverifiedListenerFilters[81])

	actual.listenerFilterMutation = nil
	actual.scopedListeners[0].AdminStateUp = false
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	sections := actual.listenerFilterMutation.listenerFilters.sections
	require.Len(t, sections, 2)
	require.Empty(t, sections[0].Requests)
	require.Equal(t, []octavia.ListenerRequestFilter{
		{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "pool-2", PathPrefix: "/second"}},
		{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "pool-1", PathPrefix: "/first"}},
	}, sections[1].Requests)

	actual.listenerFilterMutation = nil
	actual.requestFilters = map[string]octavia.ListenerRequestFilterObservation{"filtered": {Readable: true, Sections: sections}}
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.Nil(t, actual.listenerFilterMutation)
	require.False(t, actual.unverifiedListenerFilters[80])

	capability = false
	actual.scopedListeners[0].AdminStateUp = true
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.Equal(t, graphDisableListener, actual.listenerFilterMutation.kind)
	require.True(t, actual.unavailableListenerFilters[80])
	require.False(t, actual.unavailableListenerFilters[81])

	actual.listenerFilterMutation = nil
	discoveryStatus = http.StatusServiceUnavailable
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.Equal(t, graphDisableListener, actual.listenerFilterMutation.kind)
	require.True(t, actual.pendingListenerFilterCapability[80])
	require.Empty(t, actual.unavailableListenerFilters)
}

func TestListenerFiltersPreservedDuringRouteDetach(t *testing.T) {
	id := safetyTestIdentity(t)
	auth := cloud.ListenerExternalAuth{ServiceUID: "authorization", ServicePort: 8080}
	name := listenerAuthPoolName(443, auth)
	loadBalancer := safetyObservedLoadBalancer()
	loadBalancer.ProvisioningStatus, loadBalancer.Provider, loadBalancer.AdminStateUp = "ACTIVE", "amphora", true
	loadBalancer.Pools = []pools.Pool{{ID: "authorization-pool"}}
	loadBalancer.Listeners = []listeners.Listener{{ID: "filtered"}, {ID: "plain"}}

	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		switch r.URL.Path {
		case "/lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{map[string]any{
				"id": "authorization-pool", "name": name, "protocol": "HTTP", "project_id": "project-a",
				"provisioning_status": "ACTIVE", "admin_state_up": true,
				"tags":          append(safetyGatewayTags(t, id, rolePool), listenerAuthPoolTag),
				"loadbalancers": []map[string]string{{"id": loadBalancer.ID}},
				"listeners":     []map[string]string{{"id": "filtered"}},
				"members":       []any{},
			}}})
		case "/lbaas/pools/authorization-pool/members":
			safetyWriteJSON(t, w, map[string]any{"members": []any{}})
		default:
			t.Fatalf("unexpected observation during route detach: %s", r.URL)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	desired := desiredGatewayGraph{identity: id, spec: cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}}}
	actual := observedGatewayGraph{
		loadBalancer: loadBalancer, adminObserved: map[string]bool{"filtered": true},
		scopedListeners: []listeners.Listener{{ID: "filtered", ProtocolPort: 443, ProvisioningStatus: "ACTIVE", AdminStateUp: true}},
		requestFilters: map[string]octavia.ListenerRequestFilterObservation{"filtered": {Readable: true, Sections: []octavia.ListenerRequestFilterSection{{
			Hostname: "protected.example.test", Requests: []octavia.ListenerRequestFilter{{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "authorization-pool"}}},
		}}}},
	}
	require.NoError(t, provider.observeListenerFilterGraph(t.Context(), desired, &actual))
	require.Nil(t, actual.listenerFilterMutation)
	require.Contains(t, actual.httpPools.auth, name)
	require.Empty(t, actual.httpPools.routes.Items)
	require.Empty(t, actual.httpPools.loadBalancer.Pools)

	snapshot := *loadBalancer
	snapshot.Pools = []pools.Pool{{ID: "authorization-pool"}}
	require.True(t, activationLoadBalancerMatches(&snapshot, actual.loadBalancer))
}

func TestListenerFiltersCanClearAfterCapabilityWithdrawal(t *testing.T) {
	provider := newTestProvider(t, ServiceClients{LoadBalancer: safetyServiceClient(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		t.Fatalf("clearing a previously observed filter does not need capability discovery: %s", request.URL)
	})), ProjectID: "project-a"}, ProviderConfig{})
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Routes = nil
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Port: 80, Name: "filtered"}, {Port: 81, Name: "sibling"}}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	loadBalancer := safetyObservedLoadBalancer()
	loadBalancer.ProvisioningStatus, loadBalancer.AdminStateUp = "ACTIVE", true
	actual := observedGatewayGraph{
		loadBalancer: loadBalancer, adminObserved: map[string]bool{"filtered": true, "sibling": true},
		scopedListeners: []listeners.Listener{
			{ID: "filtered", Protocol: "HTTP", ProtocolPort: 80, ProvisioningStatus: "ACTIVE"},
			{ID: "sibling", Protocol: "HTTP", ProtocolPort: 81, ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		},
		requestFilters: map[string]octavia.ListenerRequestFilterObservation{"filtered": {Readable: true, Sections: []octavia.ListenerRequestFilterSection{{
			Hostname: "*", Requests: []octavia.ListenerRequestFilter{{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "authorization-pool"}}},
		}}}},
	}
	owned := map[string]observedAuthPool{"former-authorizer": {pool: pools.Pool{ID: "authorization-pool", ProvisioningStatus: "ACTIVE"}}}

	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	require.Empty(t, actual.listenerFilterMutation.listenerFilters.sections)
	require.False(t, actual.unverifiedListenerFilters[81])

	actual.listenerFilterMutation = nil
	actual.requestFilters["filtered"] = octavia.ListenerRequestFilterObservation{Readable: true}
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphDeleteChild, actual.listenerFilterMutation.kind)
	require.Equal(t, "authorization-pool", actual.listenerFilterMutation.deletion.resourceID)
	require.False(t, actual.unverifiedListenerFilters[80])

	restoration, err := buildScopedHTTPTrafficRestorationPlan(desired, actual)
	require.NoError(t, err)
	require.Contains(t, restoration, graphMutation{kind: graphEnableListener, listenerID: "filtered",
		message: "Enabling Octavia listener after validating the Gateway graph"})
}

func TestListenerFiltersRepairOwnedPoolsWithSectionLocalRejection(t *testing.T) {
	for _, status := range []string{"ACTIVE", "ERROR"} {
		t.Run(status, func(t *testing.T) {
			provider := newTestProvider(t, ServiceClients{LoadBalancer: safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", request.URL.Path)
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityListenerRequestFilters}})
			})), ProjectID: "project-a"}, ProviderConfig{})
			auth := cloud.ListenerExternalAuth{ServiceUID: "authorization", ServicePort: 8080}
			spec := completeGraphSpec(safetyTestIdentity(t))
			spec.Routes = nil
			spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Port: 80, Name: "filtered"}}
			spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{
				80: {{Hostname: "*"}, {Hostname: "protected.example.test", Requests: []cloud.ListenerExternalAuth{auth}}},
			}
			desired, err := provider.buildDesiredGatewayGraph(spec)
			require.NoError(t, err)
			loadBalancer := safetyObservedLoadBalancer()
			loadBalancer.ProvisioningStatus, loadBalancer.Provider, loadBalancer.AdminStateUp = "ACTIVE", "amphora", true
			actual := observedGatewayGraph{
				loadBalancer: loadBalancer, adminObserved: map[string]bool{"filtered": true},
				scopedListeners: []listeners.Listener{{ID: "filtered", Protocol: "HTTP", ProtocolPort: 80, ProvisioningStatus: "ACTIVE"}},
				requestFilters: map[string]octavia.ListenerRequestFilterObservation{"filtered": {Readable: true, Sections: []octavia.ListenerRequestFilterSection{
					{Hostname: "*"},
					{Hostname: "protected.example.test", Requests: []octavia.ListenerRequestFilter{{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "authorization-pool"}}}},
				}}},
			}
			owned := map[string]observedAuthPool{listenerAuthPoolName(80, auth): {
				admin: true, pool: pools.Pool{ID: "authorization-pool", ProvisioningStatus: status},
			}}
			provider.planListenerFilters(t.Context(), desired, &actual, owned)
			require.NotNil(t, actual.listenerFilterMutation)
			require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
			want := []octavia.ListenerRequestFilterSection{{Hostname: "*"}, {Hostname: "protected.example.test", Reject: true}}
			require.Equal(t, want, actual.listenerFilterMutation.listenerFilters.sections)

			actual.listenerFilterMutation = nil
			actual.requestFilters["filtered"] = octavia.ListenerRequestFilterObservation{Readable: true, Sections: want}
			provider.planListenerFilters(t.Context(), desired, &actual, owned)
			require.Nil(t, actual.listenerFilterMutation)
			require.False(t, actual.unverifiedListenerFilters[80])
			require.Equal(t, []string{"protected.example.test"}, actual.recoveringListenerFilterSections[80])
			restoration, err := buildScopedHTTPTrafficRestorationPlan(desired, actual)
			require.NoError(t, err)
			require.Contains(t, restoration, graphMutation{kind: graphEnableListener, listenerID: "filtered",
				message: "Enabling Octavia listener after validating the Gateway graph"})

			actual.scopedListeners[0].AdminStateUp = true
			provider.planListenerFilters(t.Context(), desired, &actual, owned)
			require.NotNil(t, actual.listenerFilterMutation)
			if status == "ERROR" {
				require.Equal(t, graphDeleteChild, actual.listenerFilterMutation.kind)
				require.Equal(t, "authorization-pool", actual.listenerFilterMutation.deletion.resourceID)
			} else {
				require.True(t, actual.listenerFilterMutation.listenerFilters.enablePool)
				require.Equal(t, "authorization-pool", actual.listenerFilterMutation.listenerFilters.poolID)
			}
		})
	}
}
