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
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/stretchr/testify/require"
)

func TestFrontendClientValidationPublishesRotatesAndWithdrawsOnlyItsListener(t *testing.T) {
	id := safetyTestIdentity(t)
	project := id.Value().OpenStackProjectID
	spec := completeGraphSpec(id)
	spec.Routes = nil
	spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
	ca := newGraphBackendTLS(t)
	trust := &cloud.FrontendClientValidation{ConfigMapUID: ca.CAConfigMapUID, Digest: ca.CADigest, PEM: ca.CAPEM}
	tls := &cloud.ListenerTLS{CertificateRef: "server-certificate", ClientValidation: trust}
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "http", Port: 80}, {Name: "https", Port: 443, TLS: tls},
	}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["pools"] = []any{}
	newListener := func(listenerID, protocol string, port int) map[string]any {
		return map[string]any{
			"id": listenerID, "project_id": project, "protocol": protocol, "protocol_port": port,
			"admin_state_up": true, "provisioning_status": "ACTIVE", "default_pool_id": nil,
			"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "l7policies": []any{},
			"tags":               append(safetyGatewayTags(t, id, roleListener), scopedPortTag(protocol, port)),
			"sni_container_refs": []string{}, "client_ca_tls_container_ref": nil,
		}
	}
	plain := newListener("listener-http", "HTTP", 80)
	var secure, peer, policy map[string]any
	var rejectCARead bool
	var writes []string
	octavia := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/v2.0")
		if r.Method == http.MethodGet {
			switch {
			case strings.HasPrefix(path, "/lbaas/loadbalancers"):
				lb["listeners"] = []map[string]string{{"id": "listener-http"}}
				if secure != nil {
					lb["listeners"] = append(lb["listeners"].([]map[string]string), map[string]string{"id": "listener-https"})
				}
				if peer != nil {
					lb["listeners"] = append(lb["listeners"].([]map[string]string), map[string]string{"id": "listener-peer"})
				}
				if path == "/lbaas/loadbalancers" {
					safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{lb}})
				} else {
					safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
				}
				return
			case path == "/lbaas/listeners":
				items := []any{plain}
				if secure != nil {
					items = append(items, secure)
				}
				if peer != nil {
					items = append(items, peer)
				}
				safetyWriteJSON(t, w, map[string]any{"listeners": items})
				return
			case path == "/lbaas/pools":
				safetyWriteJSON(t, w, map[string]any{"pools": []any{}})
				return
			case path == "/lbaas/providers/amphora/gateway_capabilities":
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityHTTPSListenerIsolation}})
				return
			case path == "/lbaas/l7policies":
				items := []any{}
				if policy != nil && r.URL.Query().Get("listener_id") == "listener-https" {
					items = append(items, policy)
				}
				safetyWriteJSON(t, w, map[string]any{"l7policies": items})
				return
			case path == "/lbaas/l7policies/owned-policy/rules":
				safetyWriteJSON(t, w, map[string]any{"rules": []any{}})
				return
			case strings.HasSuffix(path, "/floatingips"):
				safetyWriteJSON(t, w, map[string]any{"floatingips": []any{}})
				return
			}
		}
		switch {
		case r.Method == http.MethodPost && path == "/lbaas/listeners":
			var body struct{ Listener map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, false, body.Listener["admin_state_up"])
			require.Equal(t, "server-certificate", body.Listener["default_tls_container_ref"])
			require.Equal(t, "MANDATORY", body.Listener["client_authentication"])
			require.NotEmpty(t, body.Listener["client_ca_tls_container_ref"])
			secure = newListener("listener-https", terminatedHTTPS, 443)
			for key, value := range body.Listener {
				secure[key] = value
			}
			writes = append(writes, "create")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"listener": secure})
			return
		case r.Method == http.MethodPut && path == "/lbaas/listeners/listener-https":
			var body struct{ Listener map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			switch body.Listener["admin_state_up"] {
			case false:
				require.Len(t, body.Listener, 1)
				if rejectCARead {
					writes = append(writes, "reject-close")
					http.Error(w, "Could not retrieve certificate", http.StatusBadRequest)
					return
				}
				writes = append(writes, "close")
			case true:
				writes = append(writes, "open")
			default:
				require.Equal(t, false, secure["admin_state_up"])
				require.Equal(t, "server-certificate", body.Listener["default_tls_container_ref"])
				if tls.ClientValidation == nil {
					require.Equal(t, "NONE", body.Listener["client_authentication"])
					require.Contains(t, body.Listener, "client_ca_tls_container_ref")
					require.Nil(t, body.Listener["client_ca_tls_container_ref"])
					writes = append(writes, "clear")
				} else {
					require.Equal(t, "MANDATORY", body.Listener["client_authentication"])
					if secure["client_authentication"] == "OPTIONAL" {
						require.Equal(t, secure["client_ca_tls_container_ref"], body.Listener["client_ca_tls_container_ref"])
					} else {
						require.NotEqual(t, secure["client_ca_tls_container_ref"], body.Listener["client_ca_tls_container_ref"])
					}
					writes = append(writes, "change")
				}
			}
			for key, value := range body.Listener {
				secure[key] = value
			}
			safetyWriteJSON(t, w, map[string]any{"listener": secure})
			return
		case r.Method == http.MethodDelete && path == "/lbaas/listeners/listener-https":
			writes = append(writes, "delete")
			lb["provisioning_status"], secure["provisioning_status"] = "PENDING_UPDATE", "PENDING_DELETE"
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("unexpected Octavia or Neutron request %s %s", r.Method, path)
		http.NotFound(w, r)
	})
	services := scalarBackendTLSClients(t, project, octavia, &writes)
	provider := newTestProvider(t, services, ProviderConfig{BackendCA: backendca.New(id.Value().ClusterID, services), HTTPSListenerIsolation: true})
	spec.Gateway.ClaimBackendCA = func(context.Context) error {
		spec.Gateway.BackendCAOwned = true
		writes = append(writes, "claim")
		return nil
	}
	reconcile := func(expected ...string) cloud.GatewayGraphResult {
		t.Helper()
		var result cloud.GatewayGraphResult
		for _, write := range expected {
			writes = nil
			var err error
			result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
			require.NoError(t, err)
			require.Equal(t, []string{write}, writes)
		}
		if len(expected) == 0 {
			writes = nil
			var err error
			result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
			require.NoError(t, err)
			require.Empty(t, writes)
		}
		require.Equal(t, true, plain["admin_state_up"])
		return result
	}

	reconcile("claim", "publish", "create", "open")
	require.Equal(t, []int{80, 443}, reconcile().Gateway.ScopedListenerReadyPorts)
	secure["client_authentication"] = "OPTIONAL"
	reconcile("close", "change", "open")

	// An unrelated denied HTTPS port stays unprogrammed throughout rotation.
	spec.Gateway.Spec.ScopedListeners = append(spec.Gateway.Spec.ScopedListeners,
		cloud.ScopedListener{Name: "denied", Port: 10443, TLS: &cloud.ListenerTLS{}})
	next := newGraphBackendTLS(t)
	tls.ClientValidation = &cloud.FrontendClientValidation{ConfigMapUID: ca.CAConfigMapUID, Digest: next.CADigest, PEM: next.CAPEM}
	fenced := errors.New("client CA changed")
	spec.ValidateMutation = func(context.Context) error { return fenced }
	writes = nil
	_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorIs(t, err, fenced)
	require.Empty(t, writes)
	spec.ValidateMutation = nil
	reconcile("close", "publish", "change", "open", "retire")
	rotated := reconcile()
	require.Equal(t, []int{80, 443}, rotated.Gateway.ScopedListenerReadyPorts)
	require.Equal(t, []int{10443}, rotated.Gateway.ScopedTLSClosedPorts)
	require.Equal(t, cloud.OutcomeProgressing, rotated.Gateway.Outcome.State)

	gatewaySpec := spec.Gateway.Spec
	peer = newListener("listener-peer", terminatedHTTPS, 8443)
	peer["tags"] = append(peer["tags"].([]string), httpsIsolationTag)
	peer["default_tls_container_ref"] = "peer-certificate"
	peer["https_virtual_hosts"] = []string{"peer.example.test"}
	peer["l7policies"] = []map[string]string{{"id": "peer-policy"}}
	spec.Gateway.Disposition = cloud.GraphWithdrawTLS
	spec.Gateway.TLSWithdrawalPorts = []int{443}
	spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
		{Name: "https", Port: 443, TLS: &cloud.ListenerTLS{}},
		{Name: "peer", Port: 8443, TLS: &cloud.ListenerTLS{CertificateRef: "peer-certificate", VirtualHosts: []string{"peer.example.test"}}},
	}
	rejectCARead = true
	secure["l7policies"] = []map[string]string{{"id": "owned-policy"}}
	policy = map[string]any{"id": "owned-policy", "listener_id": "listener-https", "project_id": project,
		"provisioning_status": "ACTIVE", "admin_state_up": true, "rules": []any{}}

	// A foreign child blocks the cascade. A stale Kubernetes snapshot blocks
	// it even after the provider proves every descendant belongs to this Gateway.
	writes = nil
	_, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorIs(t, err, cloud.ErrOwnershipConflict)
	require.Equal(t, []string{"reject-close"}, writes)
	policy["tags"] = safetyRouteTags(t, id, rolePolicyExact)
	var fences int
	spec.ValidateMutation = func(context.Context) error {
		fences++
		if fences == 2 {
			return fenced
		}
		return nil
	}
	writes = nil
	_, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	require.ErrorIs(t, err, fenced)
	require.Equal(t, []string{"reject-close"}, writes)
	spec.ValidateMutation = nil

	writes = nil
	withdrawn, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, []string{"reject-close", "delete"}, writes)
	require.True(t, withdrawn.Outcome.MutationSubmitted)
	require.Equal(t, []int{80, 8443}, withdrawn.Gateway.ScopedListenerDeferredPorts)
	require.Empty(t, withdrawn.Gateway.ScopedTLSClosedPorts)
	waiting := reconcile()
	require.Equal(t, cloud.OutcomeProgressing, waiting.Outcome.State)
	require.Empty(t, waiting.Gateway.ScopedTLSClosedPorts)
	secure, policy = nil, nil
	lb["provisioning_status"] = "ACTIVE"
	settled := reconcile()
	require.Equal(t, []int{443}, settled.Gateway.ScopedTLSClosedPorts)
	require.Equal(t, cloud.OutcomeReady, settled.Outcome.State)
	require.Equal(t, true, peer["admin_state_up"])
	peer = nil
	rejectCARead = false

	spec.Gateway.Disposition, spec.Gateway.Spec = cloud.GraphEnsure, gatewaySpec
	spec.Gateway.TLSWithdrawalPorts = nil
	reconcile("create", "open")
	// A failed asynchronous disable must not be mistaken for settled closure.
	secure["admin_state_up"], secure["provisioning_status"] = false, "ERROR"
	require.NotContains(t, reconcile("delete").Gateway.ScopedTLSClosedPorts, 443)
	secure = nil
	lb["provisioning_status"] = "ACTIVE"
	reconcile("create", "open")
	tls.ClientValidation = nil
	reconcile("close", "clear", "open", "retire")
	require.Equal(t, []int{80, 443}, reconcile().Gateway.ScopedListenerReadyPorts)
}
