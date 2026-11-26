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
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/stretchr/testify/require"
)

func newAuthenticatedTCPGraphFixture(t *testing.T) (*tcpGraphFixture, *Provider) {
	t.Helper()
	f := newTCPGraphFixture(t)
	f.tcpDefaultPoolTLSAPI, f.tcpDefaultPoolTLSAdvertised = true, true
	anchor := f.spec.Gateway.Spec.ListenerPort
	f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "authenticated", Port: anchor}, {Name: "plain", Port: 9001}}
	strict := &f.spec.Routes[0].Spec
	strict.ScopedListenerPorts = []int{anchor}
	strict.MemberMode = cloud.MemberModeEndpoint
	strict.BackendTLS = newGraphBackendTLS(t)
	strict.BackendTLS.Trust = cloud.BackendTLSTrustReference
	strict.Members[0].Weight = 1
	peer := *strict
	peer.Identity.RouteUID, peer.Identity.RouteName = "tcp-plain", "tcp-plain"
	peer.BackendTLS, peer.ScopedListenerPorts = nil, []int{9001}
	f.spec.Routes = append(f.spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: peer})

	f.spec.Gateway.ClaimBackendCA = func(context.Context) error {
		f.writes = append(f.writes, "claim")
		f.spec.Gateway.BackendCAOwned = true
		return nil
	}
	clients := scalarBackendTLSClients(t, f.spec.Identity.OpenStackProjectID, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/neutron")
		if path, ok := strings.CutPrefix(r.URL.Path, "/v2.0/lbaas/"); ok {
			r.URL.Path = "/v2/lbaas/" + path
		}
		f.serveHTTP(w, r)
	}), &f.writes)
	p := newTestProvider(t, clients, ProviderConfig{
		BackendCA: backendca.New(f.spec.Identity.ClusterID, clients), BackendTLSPolicy: true, TCPDefaultPoolTLS: true,
	})
	return f, p
}

func TestTCPDefaultPoolTLSStagesAndRotatesWithoutChangingNeighbor(t *testing.T) {
	f, p := newAuthenticatedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	require.Equal(t, true, f.inventory[tcpListener]["tcp_default_pool_tls"])
	require.Equal(t, "pool", f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, f.spec.Routes[0].Spec.BackendTLS.Hostname, f.inventory[tcpPool]["backend_tls_hostname"])
	require.Nil(t, f.inventory[tcpPool]["tls_container_ref"])
	require.Equal(t, []string{"h2", "http/1.1", "http/1.0"}, f.inventory[tcpPool]["alpn_protocols"])
	settled := len(f.writes)
	require.Equal(t, cloud.OutcomeReady, f.reconcile(p).Routes[0].Result.Outcome.State)
	require.Empty(t, f.writes[settled:])
	plainListener := tcpListener + "-9001"
	require.Equal(t, "pool-secondary", f.inventory[plainListener]["default_pool_id"])
	require.Equal(t, false, f.inventory[plainListener]["tcp_default_pool_tls"])

	// A client-identity change requires detachment before the old pool is
	// closed and replaced. The attached plaintext neighbor is unchanged.
	start := len(f.writes)
	const client = "https://barbican.example/v1/secrets/enrolled-client"
	f.spec.Routes[0].Spec.TCPXBackendClientCertificateReference = client
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
	require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, false, f.inventory[tcpListener]["tcp_default_pool_tls"])
	f.settleTransportGraph(p)
	require.Equal(t, true, f.inventory[tcpListener]["tcp_default_pool_tls"])
	require.Equal(t, client, f.inventory[tcpPool]["tls_container_ref"])
	for _, write := range f.writes[start:] {
		require.NotContains(t, write, "secondary")
		require.NotContains(t, write, plainListener)
	}

	f.tcpDefaultPoolTLSAdvertised = false
	start = len(f.writes)
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
	require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, false, f.inventory[tcpListener]["tcp_default_pool_tls"])
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener, "PUT " + tcpPool}, f.writes[start:])
	require.Equal(t, false, f.inventory[tcpPool]["admin_state_up"])
	require.Equal(t, "pool-secondary", f.inventory[plainListener]["default_pool_id"])

	f.tcpDefaultPoolTLSAdvertised = true
	delete(f.inventory[tcpListener], "tcp_default_pool_tls")
	f.reconcile(p) // The verified pool may reopen while still detached.
	start = len(f.writes)
	f.reconcile(p)
	require.Empty(t, f.writes[start:])
	require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
	f.inventory[tcpListener]["tcp_default_pool_tls"] = false
	f.settleTransportGraph(p)
	require.Equal(t, true, f.inventory[tcpListener]["tcp_default_pool_tls"])
}

func TestTCPDefaultPoolTLSRecoversEnrolledListenerWithNoPool(t *testing.T) {
	f, p := newAuthenticatedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	listener := f.inventory[tcpListener]
	listener["default_pool_id"] = nil
	f.inventory[tcpPool]["listeners"] = []map[string]any{}

	before := len(f.writes)
	listener["tcp_default_pool_tls"] = "true"
	result := f.reconcile(p)
	require.Equal(t, cloud.OutcomeProgressing, result.Routes[0].Result.Outcome.State)
	require.Empty(t, f.writes[before:])
	require.Nil(t, listener["default_pool_id"])

	listener["tcp_default_pool_tls"] = true
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Equal(t, "pool", listener["default_pool_id"])
	require.Equal(t, true, listener["tcp_default_pool_tls"])
}

func TestTCPDefaultPoolTLSWithdrawsOnlyItsDefaultOnFailedLoadBalancer(t *testing.T) {
	for _, changed := range []string{"capability", "client identity", "route authorization"} {
		t.Run(changed, func(t *testing.T) {
			f, p := newAuthenticatedTCPGraphFixture(t)
			f.settleTransportGraph(p)
			f.inventory[tcpLB]["provisioning_status"] = "ERROR"
			before := len(f.writes)
			_, err := p.ReconcileGatewayGraph(t.Context(), f.spec)
			require.Error(t, err)
			require.Empty(t, f.writes[before:])
			require.Equal(t, "pool", f.inventory[tcpListener]["default_pool_id"])

			switch changed {
			case "capability":
				f.tcpDefaultPoolTLSAdvertised = false
			case "client identity":
				f.spec.Routes[0].Spec.TCPXBackendClientCertificateReference = "https://barbican.example/v1/secrets/next-client"
			case "route authorization":
				f.spec.Gateway.Disposition = cloud.GraphPreserve
				f.spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{}
				route := f.spec.Routes[0].Spec
				f.spec.Routes[0] = cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{
					Identity: route.Identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolTCP},
				}}
				f.spec.Routes[1].Disposition = cloud.GraphPreserve
			}
			links := f.inventory[tcpPool]["listeners"]
			f.inventory[tcpPool]["listeners"] = []map[string]any{}
			_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
			require.Error(t, err)
			require.Empty(t, f.writes[before:])
			f.inventory[tcpPool]["listeners"] = links

			stale := errors.New("Kubernetes inputs changed")
			f.spec.ValidateMutation = func(context.Context) error { return stale }
			_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
			require.ErrorIs(t, err, stale)
			require.Empty(t, f.writes[before:])

			f.spec.ValidateMutation = nil
			result := f.reconcile(p)
			require.Equal(t, cloud.OutcomeProgressing, result.Gateway.Outcome.State)
			require.True(t, result.Routes[0].Result.Outcome.MutationSubmitted)
			require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
			require.Equal(t, false, f.inventory[tcpListener]["tcp_default_pool_tls"])
			require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
			f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
			_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
			require.Error(t, err)
			require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
		})
	}
}

func TestTCPDefaultPoolTLSRevalidatesServingStatusDuringSiblingUpdate(t *testing.T) {
	f, p := newAuthenticatedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	f.inventory[tcpLB]["provisioning_status"] = "PENDING_UPDATE"
	f.inventory[tcpListener+"-9001"]["provisioning_status"] = "PENDING_UPDATE"
	before := len(f.writes)
	validations := 0
	f.spec.ValidateMutation = func(context.Context) error {
		validations++
		return nil
	}
	result := f.reconcile(p)
	require.True(t, result.Routes[0].Result.ObservationDeferred)
	require.False(t, result.Routes[1].Result.ObservationDeferred)
	require.Equal(t, 1, validations)

	stale := errors.New("Kubernetes inputs changed")
	f.spec.ValidateMutation = func(context.Context) error { return stale }
	_, err := p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.ErrorIs(t, err, stale)

	f.spec.ValidateMutation = nil
	f.spec.Routes[0].Spec.TCPXBackendClientCertificateReference = "https://barbican.example/v1/secrets/next-client"
	result = f.reconcile(p)
	require.False(t, result.Routes[0].Result.ObservationDeferred)
	require.Equal(t, "pool", f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
	require.Empty(t, f.writes[before:])
}
