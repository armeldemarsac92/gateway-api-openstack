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
	"errors"
	"fmt"
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
	bodyCapability := "l7policy_external_auth_forward_body_v1"
	bodyLimit := 4096
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
			_, _ = fmt.Fprintf(w, `{"gateway_capabilities":["listener_request_filters_v1",%q],"l7policy_external_auth_max_body_bytes":%d}`, bodyCapability, bodyLimit)
		} else {
			_, _ = w.Write([]byte(`{"gateway_capabilities":[]}`))
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	spec := completeGraphSpec(safetyTestIdentity(t))
	authFirst := cloud.ListenerExternalAuth{ServiceUID: "one", ServicePort: 8081, PathPrefix: "/first"}
	authSecond := cloud.ListenerExternalAuth{ServiceUID: "two", ServicePort: 8082, PathPrefix: "/second", ForwardBodyMaxSize: 4096}
	spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{
		80: {{Hostname: "*", Requests: []cloud.ListenerExternalAuth{authFirst}}, {Hostname: "protected.example.test", Requests: []cloud.ListenerExternalAuth{authSecond, authFirst}}},
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
	for _, profile := range []struct {
		capability string
		limit      int
	}{
		{"l7policy_external_auth_forward_body_v1", 4096},
		{octavia.GatewayCapabilityListenerExternalAuthForwardBody, 4095},
	} {
		bodyCapability, bodyLimit = profile.capability, profile.limit
		provider.planListenerFilters(t.Context(), desired, &actual, owned)
		require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
		sections := actual.listenerFilterMutation.listenerFilters.sections
		require.Len(t, sections[0].Requests, 1)
		require.True(t, sections[1].Reject)
		require.Empty(t, sections[1].Requests)
		require.Equal(t, []string{"protected.example.test"}, actual.unavailableListenerBodySections[80])
		actual.listenerFilterMutation = nil
	}
	bodyLimit = 4096
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	sections := actual.listenerFilterMutation.listenerFilters.sections
	require.Len(t, sections, 2)
	require.Len(t, sections[0].Requests, 1)
	require.Equal(t, []octavia.ListenerRequestFilter{
		{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "pool-2", PathPrefix: "/second", ForwardBodyMaxSize: 4096}},
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

func TestHTTPSListenerBodyRequiresItsOwnCapability(t *testing.T) {
	const port = 443
	offers := []string{octavia.GatewayCapabilityListenerRequestFilters, octavia.GatewayCapabilityListenerHTTPSExternalAuthForwardBody}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/lbaas/providers/amphora/gateway_capabilities", r.URL.Path)
		safetyWriteJSON(t, w, map[string]any{
			"gateway_capabilities":                  offers,
			"l7policy_external_auth_max_body_bytes": 4096,
		})
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Routes = nil
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "plain", Port: 80},
		{Name: "tls", Port: port, TLS: &cloud.ListenerTLS{
			CertificateRef: "cert", VirtualHosts: []string{"protected.example", "public.example"},
		}},
	}
	auth := cloud.ListenerExternalAuth{ServiceUID: "authorizer", ServicePort: 8080, ForwardBodyMaxSize: 4096}
	protected := cloud.ListenerRequestFilterSection{Hostname: "protected.example", Requests: []cloud.ListenerExternalAuth{auth}}
	spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{
		80:   {protected},
		port: {protected, {Hostname: "public.example"}},
	}
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	lb := safetyObservedLoadBalancer()
	lb.ProvisioningStatus, lb.Provider = "ACTIVE", "amphora"
	actual := observedGatewayGraph{loadBalancer: lb, scoped: true, adminObserved: map[string]bool{"tls": true},
		scopedListeners: []listeners.Listener{{ID: "tls", Protocol: cloud.ListenerProtocolTerminatedHTTPS,
			ProtocolPort: port, ProvisioningStatus: "ACTIVE"}},
	}
	owned := map[string]observedAuthPool{listenerAuthPoolName(port, auth): {
		admin: true, pool: pools.Pool{ID: "auth-pool", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
	}}

	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	installed := actual.listenerFilterMutation.listenerFilters.sections
	require.Equal(t, []octavia.ListenerRequestFilterSection{
		{Hostname: "protected.example", Requests: []octavia.ListenerRequestFilter{
			{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "auth-pool", ForwardBodyMaxSize: 4096}},
		}},
		{Hostname: "public.example"},
	}, installed)
	require.Equal(t, map[int][]string{80: {"protected.example"}}, actual.unavailableListenerBodySections)

	// An older region still offers cleartext forwarding. It must withdraw
	// the TLS binding without dropping the adjacent hostname from readback.
	offers[1] = octavia.GatewayCapabilityListenerExternalAuthForwardBody
	actual.listenerFilterMutation = nil
	actual.scopedListeners[0].AdminStateUp = true
	actual.requestFilters = map[string]octavia.ListenerRequestFilterObservation{"tls": {Readable: true, Sections: installed}}
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphDisableListener, actual.listenerFilterMutation.kind)
	require.Equal(t, map[int][]string{port: {"protected.example"}}, actual.unavailableListenerBodySections)

	actual.listenerFilterMutation = nil
	actual.scopedListeners[0].AdminStateUp = false
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	require.Equal(t, []octavia.ListenerRequestFilterSection{
		{Hostname: "protected.example", Reject: true},
		{Hostname: "public.example"},
	}, actual.listenerFilterMutation.listenerFilters.sections)

	// When replacing a cleartext frontend, clear its existing binding
	// before applying the HTTPS filters to the new frontend.
	offers[1] = octavia.GatewayCapabilityListenerHTTPSExternalAuthForwardBody
	actual.scopedListeners[0].Protocol = cloud.ListenerProtocolHTTP
	actual.listenerFilterMutation = nil
	provider.planListenerFilters(t.Context(), desired, &actual, owned)
	require.NotNil(t, actual.listenerFilterMutation)
	require.Equal(t, graphListenerFiltersMutation, actual.listenerFilterMutation.kind)
	require.Empty(t, actual.listenerFilterMutation.listenerFilters.sections)
}

func TestListenerBodyWithdrawalValidatesOnlyEffectiveAuthorizers(t *testing.T) {
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	spec.Routes = nil
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "http", Port: 80}}
	protected := cloud.ListenerExternalAuth{
		ServiceUID: "protected", ServicePort: 8080, ForwardBodyMaxSize: 4096,
		BackendSecurityGroups: []string{"protected-nodes"},
	}
	public := cloud.ListenerExternalAuth{ServiceUID: "public", ServicePort: 8081}
	sections := []cloud.ListenerRequestFilterSection{
		{Hostname: "protected.example.test", Requests: []cloud.ListenerExternalAuth{protected}},
		{Hostname: "public.example.test", Requests: []cloud.ListenerExternalAuth{public}},
	}
	spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{80: sections}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["listeners"] = []map[string]string{{"id": "listener-1"}}
	lb["pools"] = []map[string]string{{"id": "protected"}, {"id": "public"}}
	publicSection := octavia.ListenerRequestFilterSection{
		Hostname: "public.example.test", Requests: []octavia.ListenerRequestFilter{
			{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "public"}},
		},
	}
	listener := map[string]any{
		"id": "listener-1", "project_id": "project-a", "protocol": "HTTP", "protocol_port": 80,
		"admin_state_up": false, "provisioning_status": "ACTIVE", "default_pool_id": nil,
		"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "l7policies": []any{},
		"tags": append(safetyGatewayTags(t, id, roleListener), scopedPortTag("HTTP", 80)),
		"request_filter_sections": []octavia.ListenerRequestFilterSection{
			{Hostname: "protected.example.test", Requests: []octavia.ListenerRequestFilter{
				{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "protected", ForwardBodyMaxSize: 4096}},
			}}, publicSection,
		},
	}
	pool := func(poolID string, auth cloud.ListenerExternalAuth) map[string]any {
		return map[string]any{
			"id": poolID, "name": listenerAuthPoolName(80, auth), "project_id": "project-a",
			"protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN", "admin_state_up": true, "provisioning_status": "ACTIVE",
			"loadbalancers": []map[string]string{{"id": "load-balancer-1"}},
			"listeners":     []map[string]string{{"id": "listener-1"}}, "members": []any{},
			"tags": append(safetyGatewayTags(t, id, rolePool), listenerAuthPoolTag),
		}
	}
	authPools := []any{pool("protected", protected), pool("public", public)}
	limit, filterUpdates := 4096, 0
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/loadbalancers":
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
		case "GET /lbaas/loadbalancers/load-balancer-1":
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
		case "GET /lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{listener}})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": authPools})
		case "GET /lbaas/pools/protected/members", "GET /lbaas/pools/public/members":
			safetyWriteJSON(t, w, map[string]any{"members": []any{}})
		case "GET /lbaas/l7policies":
			safetyWriteJSON(t, w, map[string]any{"l7policies": []any{}})
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{
				"gateway_capabilities":                  []string{octavia.GatewayCapabilityListenerRequestFilters, octavia.GatewayCapabilityListenerExternalAuthForwardBody},
				"l7policy_external_auth_max_body_bytes": limit,
			})
		case "PUT /lbaas/listeners/listener-1":
			var body struct {
				Listener struct {
					Sections []octavia.ListenerRequestFilterSection `json:"request_filter_sections"`
				} `json:"listener"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, []octavia.ListenerRequestFilterSection{
				{Hostname: "protected.example.test", Reject: true}, publicSection,
			}, body.Listener.Sections)
			filterUpdates++
			safetyWriteJSON(t, w, map[string]any{"listener": listener})
		default:
			t.Fatalf("unexpected Octavia request: %s %s", r.Method, r.URL)
		}
	}))
	var securityReads []string
	network := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/floatingips":
			safetyWriteJSON(t, w, map[string]any{"floatingips": []any{}})
		case "/security-groups/protected-nodes", "/security-groups/public-nodes":
			securityReads = append(securityReads, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Fatalf("unexpected Neutron request: %s %s", r.Method, r.URL)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, Network: network, ProjectID: "project-a"}, ProviderConfig{})
	_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorContains(t, err, "get security group")
	require.Equal(t, []string{"/security-groups/protected-nodes"}, securityReads)
	require.Zero(t, filterUpdates)

	limit = 2048
	securityReads = nil
	sections[1].Requests[0].BackendSecurityGroups = []string{"public-nodes"}
	_, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorContains(t, err, "get security group")
	require.Equal(t, []string{"/security-groups/public-nodes"}, securityReads)
	require.Zero(t, filterUpdates)

	sections[1].Requests[0].BackendSecurityGroups = nil
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.True(t, result.Outcome.MutationSubmitted)
	require.Equal(t, 1, filterUpdates)
	require.Equal(t, []string{"protected.example.test"}, result.Gateway.ListenerFilterBodyUnavailableSections[80])
}

func TestListenerFilterWithdrawalOnlyClosesAndReplacesDeniedSection(t *testing.T) {
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	spec.Routes = nil
	spec.Gateway.Disposition = cloud.GraphWithdrawListenerFilters
	spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{}
	spec.Gateway.Spec.RequestFilterSections = map[int][]cloud.ListenerRequestFilterSection{
		80: {{Hostname: "protected.example.test", Reject: true}},
	}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["listeners"] = []map[string]string{{"id": "filtered"}, {"id": "sibling"}}
	lb["pools"] = []map[string]string{{"id": "backend-tls-pool"}}
	newListener := func(name string, port int) map[string]any {
		return map[string]any{
			"id": name, "project_id": "project-a", "protocol": "HTTP", "protocol_port": port,
			"admin_state_up": true, "provisioning_status": "ACTIVE", "default_pool_id": nil,
			"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "l7policies": []any{},
			"tags": append(safetyGatewayTags(t, id, roleListener), scopedPortTag("HTTP", port)),
		}
	}
	filtered, sibling := newListener("filtered", 80), newListener("sibling", 81)
	ordinary := octavia.ListenerRequestFilterSection{Hostname: "*", Requests: []octavia.ListenerRequestFilter{
		{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "retained-authorizer", PathPrefix: "/check"}},
	}}
	filtered["request_filter_sections"] = []octavia.ListenerRequestFilterSection{
		ordinary, {Hostname: "protected.example.test", Requests: []octavia.ListenerRequestFilter{
			{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "revoked-authorizer", ForwardBodyMaxSize: 4096}},
		}},
	}
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/loadbalancers":
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
		case "GET /lbaas/loadbalancers/load-balancer-1":
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
		case "GET /lbaas/listeners":
			safetyWriteJSON(t, w, map[string]any{"listeners": []any{filtered, sibling}})
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityListenerRequestFilters}})
		case "PUT /lbaas/listeners/filtered":
			var body struct {
				Listener map[string]json.RawMessage
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Len(t, body.Listener, 1)
			if filtered["admin_state_up"] == true {
				require.JSONEq(t, "false", string(body.Listener["admin_state_up"]))
				filtered["admin_state_up"] = false
			} else {
				var sections []octavia.ListenerRequestFilterSection
				require.NoError(t, json.Unmarshal(body.Listener["request_filter_sections"], &sections))
				require.Equal(t, []octavia.ListenerRequestFilterSection{ordinary, {Hostname: "protected.example.test", Reject: true}}, sections)
				filtered["request_filter_sections"] = sections
			}
			safetyWriteJSON(t, w, map[string]any{"listener": filtered})
		default:
			t.Fatalf("withdrawal touched an unrelated resource: %s %s", r.Method, r.URL)
		}
	}))
	provider := newTestProvider(t, ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{})
	stale := errors.New("listener filter authority changed")
	spec.ValidateMutation = func(context.Context) error { return stale }
	_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorIs(t, err, stale)
	require.Equal(t, true, filtered["admin_state_up"])

	spec.ValidateMutation = func(context.Context) error { return nil }
	for range 2 {
		result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
		require.NoError(t, err)
		require.True(t, result.Outcome.MutationSubmitted)
	}
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, cloud.OutcomeReady, result.Outcome.State)
	require.False(t, result.Outcome.MutationSubmitted)
	require.Equal(t, false, filtered["admin_state_up"])
	require.Equal(t, true, sibling["admin_state_up"])
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
			Hostname: "*", Requests: []octavia.ListenerRequestFilter{{Type: "ExternalAuth", HTTP: octavia.ListenerHTTPExternalAuth{PoolID: "authorization-pool", ForwardBodyMaxSize: 4096}}},
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
