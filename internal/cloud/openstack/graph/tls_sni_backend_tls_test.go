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
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestTLSRouteBackendTLSStagesNativeAuthenticationBeforeActivation(t *testing.T) {
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol, spec.Gateway.Spec.ExternalNetworkID = cloud.ListenerProtocolTLSTerminate, ""
	spec.Gateway.TCPTLSCertificateRef = "https://barbican.example/v1/secrets/frontend"
	spec.Gateway.SNIPoliciesObserved = true
	tls := newGraphBackendTLS(t)
	strict := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "tls.example.test", ServiceUID: "strict-service", ServicePort: 8443,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 8443}}, BackendTLS: tls}
	peer := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "*.example.test", ServiceUID: strict.ServiceUID, ServicePort: 8080,
		Members: []cloud.Member{{Address: "192.0.2.20", Port: 8443}}}
	peer.Identity.RouteName, peer.Identity.RouteUID = "peer", "peer-uid"
	spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{strict, peer}
	capabilities := tcpBackendTLSCapabilities()
	var writes []string
	rejectPool := false
	var pool map[string]any
	octaviaAPI := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/v2.0")
		switch r.Method + " " + path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case "GET /lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{pool}})
		case "POST /lbaas/pools":
			if rejectPool {
				http.Error(w, "pool quota", http.StatusForbidden)
				return
			}
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			pool = body.Pool
			require.Equal(t, "TCP", pool["protocol"])
			require.Equal(t, true, pool["tls_enabled"])
			require.Equal(t, tls.Hostname, pool["backend_tls_hostname"])
			require.NotEmpty(t, pool["ca_tls_container_ref"])
			require.Equal(t, false, pool["admin_state_up"])
			require.Nil(t, pool["listener_id"])
			require.Nil(t, pool["backend_tls_require_h2"])
			require.Nil(t, pool["backend_tls_subject_alt_names"])
			pool["id"], pool["provisioning_status"], pool["tls_container_ref"] = "strict-pool", "ACTIVE", nil
			writes = append(writes, "create-tls")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		case "PUT /lbaas/pools/strict-pool":
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Len(t, body.Pool, 1)
			require.IsType(t, false, body.Pool["admin_state_up"])
			pool["admin_state_up"] = body.Pool["admin_state_up"]
			writes = append(writes, "pool-admin")
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		default:
			t.Errorf("unexpected Octavia request %s %s", r.Method, path)
			http.NotFound(w, r)
		}
	})
	clients := scalarBackendTLSClients(t, id.Value().OpenStackProjectID, octaviaAPI, &writes)
	store := backendca.New(id.Value().ClusterID, clients)
	provider := newTestProvider(t, clients, ProviderConfig{BackendCA: store, BackendTLSPolicy: true, TCPTLSTermination: true})
	spec.Gateway.ClaimBackendCA = func(context.Context) error {
		writes = append(writes, "claim")
		spec.Gateway.BackendCAOwned = true
		return nil
	}
	compiled, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	strictTarget, peerTarget := compiled.sni[0], compiled.sni[1]
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolTLSTerminate, loadBalancer: safetyObservedLoadBalancer(),
		listener:                 &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		sni:                      map[sniFragmentKey]*observedSNIFragment{peerTarget.key: activeSNIFragment(peerTarget, "peer")},
		rawTLSCapabilityVerified: true, rawTLSVerified: true,
	}
	actual.loadBalancer.AdminStateUp, actual.loadBalancer.ProvisioningStatus, actual.loadBalancer.Provider = true, "ACTIVE", "amphora"
	plan := func() (desiredGatewayGraph, []graphMutation, cloud.GatewayGraphResult) {
		t.Helper()
		desired, err := provider.buildDesiredGatewayGraph(spec)
		require.NoError(t, err)
		require.NoError(t, provider.prepareSNIRouteBackendTLS(t.Context(), &desired, actual))
		steps, result, err := provider.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		return desired, steps, result
	}
	apply := func(expected sniMutationKind) {
		t.Helper()
		desired, steps, _ := plan()
		require.Len(t, steps, 1)
		require.Equal(t, expected, steps[0].sni.kind)
		require.NoError(t, provider.executeGraphMutation(t.Context(), desired, actual, steps[0]))
	}
	// Before claiming or allocating, close the active fallback for this new
	// name. A subsequent CA publication or pool quota failure leaves it closed.
	_, steps, result := plan()
	require.Len(t, steps, 1)
	require.Equal(t, gatewayDisableL7Policy, steps[0].deletion.resource)
	require.Equal(t, "policy-peer", steps[0].deletion.resourceID)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[1].Result.Outcome.State)
	peerFragment := actual.sni[peerTarget.key]
	peerFragment.policy.AdminStateUp = false
	apply(sniClaimBackendCA)
	apply(sniPublishBackendCA)
	rejectPool = true
	desired, steps, _ := plan()
	require.Len(t, steps, 1)
	require.Equal(t, sniCreatePool, steps[0].sni.kind)
	require.Error(t, provider.executeGraphMutation(t.Context(), desired, actual, steps[0]))
	require.False(t, peerFragment.policy.AdminStateUp)
	rejectPool = false
	apply(sniCreatePool)
	require.Equal(t, []string{"claim", "publish", "create-tls"}, writes)
	protected := activeSNIFragment(strictTarget, "strict")
	protected.policy.AdminStateUp = false
	actual.sni[strictTarget.key] = protected
	readPool := func() {
		t.Helper()
		inventory, err := provider.octavia.ListPoolInventory(t.Context(), pools.ListOpts{})
		require.NoError(t, err)
		require.Len(t, inventory.Items, 1)
		protected.pool = &inventory.Items[0]
		protected.backendTLS = inventory.BackendTLS[protected.pool.ID]
	}
	readPool()
	apply(sniEnableBackendTLSPool)
	readPool()
	_, steps, result = plan()
	require.Len(t, steps, 1)
	require.Equal(t, sniEnablePolicy, steps[0].sni.kind)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
	protected.policy.AdminStateUp = true
	_, steps, _ = plan()
	require.Len(t, steps, 1)
	require.Equal(t, sniEnablePolicy, steps[0].sni.kind)
	require.Equal(t, "policy-peer", steps[0].sni.parentID)
	peerFragment.policy.AdminStateUp = true
	_, steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeReady, result.Routes[0].Result.Outcome.State)
	require.Equal(t, cloud.OutcomeReady, result.Routes[1].Result.Outcome.State)

	// Losing only the new regional capability closes the strict pool. Its
	// selector and the broader route stay in place, with no plaintext create.
	capabilities = []string{octavia.GatewayCapabilityTCPTLSTermination, octavia.GatewayCapabilityBackendTLSHostname}
	apply(sniDisableBackendTLSPool)
	readPool()
	_, steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeReady, result.Routes[0].Result.Outcome.State, "closed exact pool shields the wildcard")
	require.False(t, protected.pool.AdminStateUp)
	require.True(t, protected.policy.AdminStateUp)
	require.Equal(t, []string{"claim", "publish", "create-tls", "pool-admin", "pool-admin"}, writes)

	// Losing the Kubernetes CA or permission keeps the closed protected
	// selector as the denial. No replacement pool or wildcard closure is needed.
	spec.Gateway.TLSSNIRoutes[0] = cloud.TLSSNIRoute{Identity: strict.Identity, Hostname: strict.Hostname, Deny: true}
	_, steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeReady, result.Routes[0].Result.Outcome.State)

	// Gateway client-certificate requests cannot be silently ignored.
	spec.Gateway.TLSSNIRoutes[0] = strict
	capabilities = tcpBackendTLSCapabilities()
	spec.Gateway.BackendClientCertificate.Reference = "https://barbican.example/v1/secrets/client"
	_, steps, result = plan()
	require.Empty(t, steps)
	require.Equal(t, cloud.OutcomeReady, result.Routes[0].Result.Outcome.State)
	spec.Gateway.BackendClientCertificate.Reference = ""
	_, steps, _ = plan()
	require.Len(t, steps, 1)
	require.Equal(t, sniEnableBackendTLSPool, steps[0].sni.kind)

	// On opt-out neither an alias on the original route nor another route
	// referencing the same port may reopen it in plaintext. A different port
	// of the same Service stays independently eligible.
	provider.backendTLSPolicy, spec.Gateway.SNIPoliciesObserved = false, false
	strict.BackendTLS = nil
	alias := strict
	alias.Hostname = "new.other.test"
	shared := alias
	shared.Identity.RouteName, shared.Identity.RouteUID = "shared", "shared-uid"
	shared.Hostname = "shared.other.test"
	clear := peer
	spec.Gateway.TLSSNIRoutes = []cloud.TLSSNIRoute{strict, alias, shared, clear}
	desired, steps, _ = plan()
	require.Empty(t, steps)
	require.Len(t, desired.sni, 1)
	require.Equal(t, clear.Hostname, desired.sni[0].route.Hostname)
	require.True(t, desired.sniHeldClosed[strictTarget.key])
	require.False(t, protected.pool.AdminStateUp)
}

func TestTLSRouteBackendTLSRenamePreservesProtectionAcrossRestart(t *testing.T) {
	id := safetyTestIdentity(t)
	spec := completeGraphSpec(id)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol, spec.Gateway.Spec.ExternalNetworkID = cloud.ListenerProtocolTLSTerminate, ""
	spec.Gateway.SNIPoliciesObserved = true
	original := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "old.example.test", ServiceUID: "strict-service", ServicePort: 8443,
		Members: []cloud.Member{{Address: "192.0.2.10", Port: 8443}}, BackendTLS: newGraphBackendTLS(t)}
	alias := original
	alias.Hostname = "new.example.test"
	fallback := cloud.TLSSNIRoute{Identity: id.Value(), Hostname: "*.example.test", ServiceUID: "plain-service", ServicePort: 8080,
		Members: []cloud.Member{{Address: "192.0.2.20", Port: 8080}}}
	fallback.Identity.RouteName, fallback.Identity.RouteUID = "fallback", "fallback-uid"

	provider := &Provider{projectID: id.Value().OpenStackProjectID, tcpTLSTermination: true, backendTLSPolicy: true}
	compile := func(p *Provider, routes ...cloud.TLSSNIRoute) desiredGatewayGraph {
		t.Helper()
		spec.Gateway.TLSSNIRoutes = routes
		desired, err := p.buildDesiredGatewayGraph(spec)
		require.NoError(t, err)
		return desired
	}
	originalTarget := compile(provider, original).sni[0]
	desired := compile(provider, alias, fallback)
	target, fallbackTarget := &desired.sni[0], desired.sni[1]
	protected := func(target desiredSNIFragment, id string) *observedSNIFragment {
		t.Helper()
		fragment := activeSNIFragment(target, id)
		fragment.pool.AdminStateUp, fragment.pool.TLSEnabled = false, true
		fragment.pool.Tags = []string{backendTLSManagedTag, backendTLSRevisionTag(target.route.BackendTLS), sniBackendServiceTag(target.route)}
		fragment.backendTLS.AdminObserved = true
		return fragment
	}
	old := protected(originalTarget, "old")
	old.policy, old.rule, old.members = nil, nil, nil
	broader := activeSNIFragment(fallbackTarget, "fallback")
	broader.policy.AdminStateUp = false // The strict claim has already suspended fallback.
	actual := observedGatewayGraph{
		protocol: cloud.ListenerProtocolTLSTerminate, loadBalancer: safetyObservedLoadBalancer(),
		listener:                 &listeners.Listener{ID: "listener", ProvisioningStatus: "ACTIVE", AdminStateUp: true},
		rawTLSCapabilityVerified: true, rawTLSVerified: true,
		sni: map[sniFragmentKey]*observedSNIFragment{originalTarget.key: old, fallbackTarget.key: broader},
	}
	plan := func(p *Provider, desired desiredGatewayGraph) graphMutation {
		t.Helper()
		steps, _, err := p.buildSNIGraphPlan(desired, actual)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		return steps[0]
	}

	target.backendTLS.phase = backendTLSConfigure
	step := plan(provider, desired)
	require.Equal(t, graphSNIMutation, step.kind)
	require.Equal(t, sniCreatePool, step.sni.kind)
	require.Equal(t, alias, step.sni.target.route)

	restarted := &Provider{projectID: id.Value().OpenStackProjectID, tcpTLSTermination: true}
	spec.Gateway.SNIPoliciesObserved = false
	checkRestart := func(unprotected cloud.TLSSNIRoute) {
		t.Helper()
		unprotected.BackendTLS = nil
		afterRestart := compile(restarted, unprotected, fallback)
		require.NoError(t, restarted.prepareSNIRouteBackendTLS(t.Context(), &afterRestart, actual))
		require.True(t, afterRestart.sniBlockedRoutes[target.key.uid])
		step := plan(restarted, afterRestart)
		require.Equal(t, graphSNIMutation, step.kind)
		require.Equal(t, sniCreatePool, step.sni.kind)
		require.True(t, step.sni.target.route.Deny)
		require.Equal(t, alias.Hostname, step.sni.target.route.Hostname)
	}
	checkRestart(alias)

	successor := protected(*target, "new")
	actual.sni[target.key] = &observedSNIFragment{pool: successor.pool, backendTLS: successor.backendTLS}
	target.backendTLS.phase = backendTLSVerified
	step = plan(provider, desired)
	require.Equal(t, graphSNIMutation, step.kind)
	require.Equal(t, sniCreateMember, step.sni.kind)

	successor.policy.AdminStateUp = false
	actual.sni[target.key] = successor
	step = plan(provider, desired)
	require.Equal(t, gatewayDeletionPool, step.deletion.resource)
	require.Equal(t, old.pool.ID, step.deletion.resourceID)
	delete(actual.sni, originalTarget.key)
	step = plan(provider, desired)
	require.Equal(t, graphSNIMutation, step.kind)
	require.Equal(t, sniEnableBackendTLSPool, step.sni.kind)
	checkRestart(alias)

	otherBackend := alias
	otherBackend.ServiceUID, otherBackend.ServicePort, otherBackend.Members = fallback.ServiceUID, fallback.ServicePort, fallback.Members
	checkRestart(otherBackend)

	withoutAlias := compile(restarted, fallback)
	step = plan(restarted, withoutAlias)
	require.Equal(t, gatewayDeletionL7Rule, step.deletion.resource)
	require.Equal(t, successor.rule.ID, step.deletion.resourceID)
	successor.policy, successor.rule, successor.members = nil, nil, nil
	step = plan(restarted, withoutAlias)
	require.Equal(t, gatewayDeletionPool, step.deletion.resource)
	require.Equal(t, successor.pool.ID, step.deletion.resourceID)
}
