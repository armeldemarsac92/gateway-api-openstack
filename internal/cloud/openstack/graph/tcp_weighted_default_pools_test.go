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
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/stretchr/testify/require"
)

const (
	weightedClearKey = "000000000000000000000001"
	weightedTLSKey   = "000000000000000000000002"
)

func newWeightedTCPGraphFixture(t *testing.T) (*tcpGraphFixture, *Provider) {
	f, p := newAuthenticatedTCPGraphFixture(t)
	enableWeightedTCPGraphFixture(f, p)
	return f, p
}

func enableWeightedTCPGraphFixture(f *tcpGraphFixture, p *Provider) {
	f.tcpWeightedAPI, f.tcpWeightedAdvertised, p.tcpWeightedDefaultPools = true, true, true
	spec := &f.spec.Routes[0].Spec
	spec.TCPWeightedBackends = []cloud.TCPWeightedBackend{
		{Key: weightedClearKey, Weight: 3, Members: spec.Members},
		{Key: weightedTLSKey, Weight: 1, Members: spec.Members, BackendTLS: spec.BackendTLS,
			ClientCertificateReference: "https://barbican.example/v1/secrets/enrolled-client"},
	}
	spec.Members, spec.BackendTLS = nil, nil
	spec.HealthMonitor = cloud.HealthMonitorConfig{}
}

func weightedFixtureTargets(t *testing.T, f *tcpGraphFixture) []map[string]any {
	t.Helper()
	rows, ok := f.inventory[tcpListener]["tcp_default_targets"].([]any)
	require.True(t, ok)
	targets := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		targets = append(targets, row.(map[string]any))
	}
	return targets
}

func TestTCPWeightedDefaultPoolsWithdrawAndRestoreOnlyTheInvalidShare(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	targets := weightedFixtureTargets(t, f)
	require.Len(t, targets, 2)
	clear, authenticated := targets[0], targets[1]
	clearID, tlsID := clear["pool_id"].(string), authenticated["pool_id"].(string)
	require.NotEqual(t, clearID, tlsID)
	require.Equal(t, float64(3), clear["weight"])
	require.Equal(t, float64(1), authenticated["weight"])
	require.Equal(t, true, f.inventory[tcpPools+"/"+tlsID]["tls_enabled"])
	require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, false, f.inventory[tcpListener]["tcp_default_pool_tls"])

	spec := &f.spec.Routes[0]
	valid := spec.Spec.TCPWeightedBackends[1]
	spec.Spec.TCPWeightedBackends[1] = cloud.TCPWeightedBackend{Weight: 1}
	// A sibling can reconcile before this route checkpoints its new inputs.
	spec.Disposition = cloud.GraphPreserve
	spec.RetainListenerPorts = spec.Spec.ScopedListenerPorts
	before := len(f.writes)
	result := f.reconcile(p)
	require.Equal(t, spec.Spec.Identity, result.Routes[1].Identity)
	require.True(t, result.Routes[1].Result.Outcome.MutationSubmitted)
	require.True(t, result.Routes[0].Result.ObservationDeferred)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Equal(t, []map[string]any{clear, {"reject": true, "weight": float64(1)}}, weightedFixtureTargets(t, f))
	require.NotNil(t, f.inventory[tcpPools+"/"+tlsID])

	spec.Disposition, spec.RetainListenerPorts = cloud.GraphEnsure, nil
	f.settleTransportGraph(p)
	require.Nil(t, f.inventory[tcpPools+"/"+tlsID])
	require.Equal(t, clear, weightedFixtureTargets(t, f)[0])

	spec.Spec.TCPWeightedBackends[1] = valid
	result = f.settleTransportGraph(p)
	restored := weightedFixtureTargets(t, f)
	require.Equal(t, clear, restored[0])
	require.Equal(t, float64(1), restored[1]["weight"])
	restoredID := restored[1]["pool_id"].(string)
	require.NotEqual(t, tlsID, restoredID)
	require.Equal(t, valid.ClientCertificateReference, f.inventory[tcpPools+"/"+restoredID]["tls_container_ref"])
	require.ElementsMatch(t, []string{clearID, restoredID}, result.Routes[0].Result.State.PoolIDs)
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
	for _, write := range f.writes[before:] {
		require.NotContains(t, write, "secondary")
		require.NotContains(t, write, tcpListener+"-9001")
	}
	before = len(f.writes)
	require.Equal(t, cloud.OutcomeReady, f.reconcile(p).Outcome.State)
	require.Empty(t, f.writes[before:])
}

func TestTCPWeightedDefaultPoolsPreserveDifferentTLSIdentitiesOnOneSocket(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.spec.Routes[0].Spec.TCPWeightedBackends[0] = f.spec.Routes[0].Spec.TCPWeightedBackends[1]
	first := &f.spec.Routes[0].Spec.TCPWeightedBackends[0]
	first.Key, first.Weight, first.ClientCertificateReference = weightedClearKey, 3, "https://barbican.example/v1/secrets/first-client"
	tls := *first.BackendTLS
	tls.Hostname = "alternate.example.test"
	first.BackendTLS = &tls
	f.settleTransportGraph(p)
	targets := weightedFixtureTargets(t, f)
	firstID := targets[0]["pool_id"].(string)
	secondID := targets[1]["pool_id"].(string)
	require.NotEqual(t, firstID, secondID)
	require.Equal(t, f.inventory[tcpPools+"/"+firstID+"/members/member"]["address"], f.inventory[tcpPools+"/"+secondID+"/members/member"]["address"])
	require.Equal(t, f.inventory[tcpPools+"/"+firstID+"/members/member"]["protocol_port"], f.inventory[tcpPools+"/"+secondID+"/members/member"]["protocol_port"])
	for i, target := range targets {
		pool := f.inventory[tcpPools+"/"+target["pool_id"].(string)]
		spec := f.spec.Routes[0].Spec.TCPWeightedBackends[i]
		require.Equal(t, spec.BackendTLS.Hostname, pool["backend_tls_hostname"])
		require.Equal(t, spec.ClientCertificateReference, pool["tls_container_ref"])
	}

	// ERROR allows an authorized withdrawal, without opening the new identity
	// or changing a previously verified sibling.
	f.inventory[tcpLB]["provisioning_status"] = "ERROR"
	first.ClientCertificateReference = "https://barbican.example/v1/secrets/rotated-client"
	start := len(f.writes)
	stale := errors.New("the Kubernetes authorizations changed")
	f.spec.ValidateMutation = func(context.Context) error { return stale }
	_, err := p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.ErrorIs(t, err, stale)
	require.Empty(t, f.writes[start:])
	f.spec.ValidateMutation = nil
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
	require.Equal(t, []map[string]any{{"reject": true, "weight": float64(3)}, targets[1]}, weightedFixtureTargets(t, f))
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.Error(t, err)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
}

func TestTCPWeightedDefaultPoolsRequireOwnedCompleteReadback(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	listener := f.inventory[tcpListener]
	targets := listener["tcp_default_targets"]
	id := weightedFixtureTargets(t, f)[1]["pool_id"].(string)
	pool := f.inventory[tcpPools+"/"+id]
	start := len(f.writes)
	links := pool["listeners"]
	pool["listeners"] = []any{}
	_, err := p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.Error(t, err)
	require.Empty(t, f.writes[start:])
	pool["listeners"] = links

	tags := pool["tags"]
	pool["tags"] = []any{}
	_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.ErrorIs(t, err, cloud.ErrOwnershipConflict)
	require.Empty(t, f.writes[start:])
	pool["tags"] = tags

	delete(listener, "tcp_default_targets")
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
	require.Equal(t, false, listener["admin_state_up"])
	listener["provisioning_status"] = "ACTIVE"
	_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.Error(t, err)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])

	listener["tcp_default_targets"] = "unreadable"
	_, err = p.ReconcileGatewayGraph(t.Context(), f.spec)
	require.Error(t, err)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[start:])
	listener["tcp_default_targets"] = targets
	f.settleTransportGraph(p)
	require.Equal(t, true, listener["admin_state_up"])
}

func TestTCPWeightedDefaultPoolsTransitionFromAndBackToScalar(t *testing.T) {
	f, p := newAuthenticatedTCPGraphFixture(t)
	f.tcpWeightedAPI, f.tcpWeightedAdvertised = true, true
	f.settleTransportGraph(p)
	scalar := f.spec.Routes[0].Spec
	require.Equal(t, "pool", f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, true, f.inventory[tcpListener]["tcp_default_pool_tls"])

	enableWeightedTCPGraphFixture(f, p)
	before := len(f.writes)
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Nil(t, f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, false, f.inventory[tcpListener]["tcp_default_pool_tls"])
	require.Equal(t, []map[string]any{{"reject": true, "weight": float64(3)}, {"reject": true, "weight": float64(1)}}, weightedFixtureTargets(t, f))
	require.NotNil(t, f.inventory[tcpPool])
	f.settleTransportGraph(p)
	require.Nil(t, f.inventory[tcpPool])

	p.tcpWeightedDefaultPools = false
	f.spec.Routes[0].Spec = scalar
	f.settleTransportGraph(p)
	require.Empty(t, weightedFixtureTargets(t, f))
	require.Equal(t, "pool", f.inventory[tcpListener]["default_pool_id"])
	require.Equal(t, true, f.inventory[tcpListener]["tcp_default_pool_tls"])
	require.Len(t, f.children(tcpPools), 2)
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
}

func TestTCPWeightedDefaultPoolsReplaysFailedWithdrawalWithoutChangingWitness(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	clear, tls := weightedFixtureTargets(t, f)[0], weightedFixtureTargets(t, f)[1]
	retired := f.inventory[tcpPools+"/"+tls["pool_id"].(string)]
	lastPublishedLinks := retired["listeners"]
	f.spec.Routes[0].Spec.TCPWeightedBackends[1] = cloud.TCPWeightedBackend{Weight: 1}
	f.reconcile(p)
	parked := []map[string]any{clear, {"reject": true, "weight": float64(1)}}
	require.Equal(t, parked, weightedFixtureTargets(t, f))

	listener := f.inventory[tcpListener]
	listener["provisioning_status"] = "ERROR"
	listener["tcp_default_targets_status"] = "ERROR"
	listener["tcp_default_targets_retryable"] = true
	// Desired state already reads as parked. The former pool remains linked
	// until Amphora confirms withdrawal, even if capability is switched off.
	retired["listeners"] = lastPublishedLinks
	f.tcpWeightedAdvertised = false
	f.inventory[tcpLB]["provisioning_status"] = "ERROR"
	before := len(f.writes)
	result := f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Equal(t, parked, weightedFixtureTargets(t, f))
	require.True(t, result.Routes[0].Result.Outcome.MutationSubmitted)
	require.True(t, result.Routes[1].Result.ObservationDeferred)
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
	require.NotEmpty(t, retired)
}

func TestTCPWeightedDefaultPoolsNeverReplaysFailedAdmission(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	listener := f.inventory[tcpListener]
	listener["provisioning_status"] = "ERROR"
	listener["tcp_default_targets_status"] = "ERROR"
	listener["tcp_default_targets_retryable"] = false
	before := len(f.writes)
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Empty(t, weightedFixtureTargets(t, f))
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
}

func TestTCPWeightedDefaultPoolsWithdrawsOldEndpointOnFailedLoadBalancer(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	clear := weightedFixtureTargets(t, f)[0]
	tls := &f.spec.Routes[0].Spec.TCPWeightedBackends[1]
	tls.Members = slices.Clone(tls.Members)
	tls.Members[0].Address = "192.0.2.45"
	f.inventory[tcpLB]["provisioning_status"] = "ERROR"
	before := len(f.writes)
	result := f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Equal(t, []map[string]any{clear, {"reject": true, "weight": float64(1)}}, weightedFixtureTargets(t, f))
	require.True(t, result.Routes[1].Result.ObservationDeferred)
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
}

func TestTCPWeightedDefaultPoolsRemoveRetiredPoolsAfterRegionalOptOut(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.tcpWeightedAdvertised = false
	f.reconcile(p)
	require.Empty(t, weightedFixtureTargets(t, f))
	listener := f.inventory[tcpListener]

	// Desired state alone, while provisioning is still pending, grants no
	// permission to delete the previously selected pools.
	delete(listener, "tcp_default_targets")
	before := len(f.writes)
	f.reconcile(p)
	require.Empty(t, f.writes[before:])

	listener["provisioning_status"] = "ACTIVE"
	f.tcpWeightedAdvertised = true
	f.reconcile(p)
	require.Empty(t, f.writes[before:])
	f.tcpWeightedAdvertised = false
	f.settleTransportGraph(p)
	require.Len(t, f.children(tcpPools), 1)
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
}

func TestTCPWeightedDefaultPoolsDeleteGatewayWithScalarSibling(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete}
	for i, node := range f.spec.Routes {
		f.spec.Routes[i] = cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{
			Identity:            node.Spec.Identity,
			GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolTCP},
		}}
	}

	f.reconcile(p)
	require.Empty(t, weightedFixtureTargets(t, f))
	listener := f.inventory[tcpListener]
	listener["provisioning_status"] = "ERROR"
	listener["tcp_default_targets_status"] = "ERROR"
	listener["tcp_default_targets_retryable"] = true
	before := len(f.writes)
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Empty(t, weightedFixtureTargets(t, f))
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])

	listener["tcp_default_targets_status"] = "ACTIVE"
	listener["tcp_default_targets_retryable"] = false
	f.settleTransportGraph(p)
	require.Nil(t, f.inventory[tcpLB])
	require.Empty(t, f.children(tcpListeners))
	require.Empty(t, f.children(tcpPools))
}

func TestTCPWeightedDefaultPoolsReplaysFailedRouteOnlyClear(t *testing.T) {
	f, p := newWeightedTCPGraphFixture(t)
	f.settleTransportGraph(p)
	poolID := weightedFixtureTargets(t, f)[1]["pool_id"].(string)
	retired := f.inventory[tcpPools+"/"+poolID]
	lastPublishedLinks := retired["listeners"]

	f.spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}
	for i, node := range f.spec.Routes {
		f.spec.Routes[i] = cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{
			Identity:            node.Spec.Identity,
			GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolTCP},
		}}
	}
	f.spec.Routes[0].Disposition = cloud.GraphDelete
	f.reconcile(p)
	require.Empty(t, weightedFixtureTargets(t, f))
	retired["listeners"] = lastPublishedLinks
	listener := f.inventory[tcpListener]
	listener["provisioning_status"] = "ERROR"
	listener["tcp_default_targets_status"] = "ERROR"
	listener["tcp_default_targets_retryable"] = true
	f.inventory[tcpLB]["provisioning_status"] = "ERROR"
	before := len(f.writes)
	f.reconcile(p)
	require.Equal(t, []string{"PUT " + tcpListener}, f.writes[before:])
	require.Empty(t, weightedFixtureTargets(t, f))
	require.Equal(t, "pool-secondary", f.inventory[tcpListener+"-9001"]["default_pool_id"])
}
