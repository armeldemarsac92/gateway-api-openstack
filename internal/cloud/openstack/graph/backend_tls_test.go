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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/clients"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2/openstack/keymanager/v1/secrets"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"
)

func TestBackendTLSScalarQuarantinesBeforePublishingAndReopening(t *testing.T) {
	id := safetyTestIdentity(t)
	project := id.Value().OpenStackProjectID
	spec := completeGraphSpec(id)
	spec.Gateway.Spec.ListenerProtocol, spec.Routes[0].Spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolHTTP, cloud.ListenerProtocolHTTP
	spec.Routes[0].Spec.BackendTLSObserved = true
	tls := newGraphBackendTLS(t)
	spec.Routes[0].Spec.BackendTLS = tls
	const poolURL = "/lbaas/pools/pool-1"
	const sanField = "backend_tls_subject_alt_names"
	memberIDs := []map[string]string{{"id": "member-1"}}
	pool := map[string]any{
		"id": "pool-1", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN", "project_id": project,
		"provisioning_status": "ACTIVE", "admin_state_up": true, "backend_tls_hostname": nil, "ca_tls_container_ref": nil, "tls_container_ref": nil, "tls_enabled": false,
		"healthmonitor_id": "monitor-1", "listeners": []map[string]string{{"id": "listener-1"}},
		"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "tags": safetyRouteTags(t, id, rolePool),
	}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["pools"], lb["listeners"] = []map[string]string{{"id": "pool-1"}}, []map[string]string{{"id": "listener-1"}}
	listener := map[string]any{
		"id": "listener-1", "project_id": project, "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
		"provisioning_status": "ACTIVE", "default_pool_id": nil, "client_ca_tls_container_ref": nil, "loadbalancers": pool["loadbalancers"],
		"l7policies": []map[string]string{{"id": "policy-1"}}, "tags": safetyGatewayTags(t, id, roleListener),
	}
	policy := map[string]any{
		"id": "policy-1", "project_id": project, "listener_id": "listener-1", "action": "REDIRECT_TO_POOL", "redirect_pool_id": "pool-1",
		"position": 1, "admin_state_up": true, "provisioning_status": "ACTIVE",
		"rules": []map[string]string{{"id": "rule-path"}, {"id": "rule-host"}}, "tags": safetyRouteTags(t, id, rolePolicyExact),
	}
	member := map[string]any{
		"id": "member-1", "project_id": project, "provisioning_status": "ACTIVE", "admin_state_up": true,
		"address": "10.0.0.10", "protocol_port": 30080, "subnet_id": "member-subnet", "tags": safetyRouteTags(t, id, roleMember),
	}
	monitor := map[string]any{
		"id": "monitor-1", "project_id": project, "provisioning_status": "ACTIVE", "type": "HTTP", "pools": lb["pools"],
		"delay": 10, "timeout": 5, "max_retries": 3, "max_retries_down": 3, "url_path": "/healthz",
		"http_method": "GET", "expected_codes": "200-399", "admin_state_up": true, "tags": safetyRouteTags(t, id, roleMonitor),
	}
	rules := []map[string]any{
		{"id": "rule-path", "type": "PATH", "compare_type": "EQUAL_TO", "value": "/api", "admin_state_up": true,
			"provisioning_status": "ACTIVE", "tags": safetyRouteTags(t, id, roleRulePath)},
		{"id": "rule-host", "type": "HOST_NAME", "compare_type": "REGEX", "value": `^api\.example\.com(:[0-9]+)?$`, "admin_state_up": true,
			"provisioning_status": "ACTIVE", "tags": safetyRouteTags(t, id, roleRuleHost)},
	}
	reads := map[string]map[string]any{
		"/lbaas/loadbalancers": {"loadbalancers": []any{lb}}, "/lbaas/loadbalancers/load-balancer-1": {"loadbalancer": lb},
		"/lbaas/listeners": {"listeners": []any{listener}}, "/lbaas/pools": {"pools": []any{pool}},
		"/lbaas/l7policies": {"l7policies": []any{policy}}, "/lbaas/l7policies/policy-1/rules": {"rules": rules},
		"/lbaas/healthmonitors": {"healthmonitors": []any{monitor}},
	}
	capabilities := []string{octavia.GatewayCapabilityBackendTLSHostname}
	const filterListenerURL = "/lbaas/listeners/filter-listener"
	var filterListener map[string]any
	var writes []string
	handle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/v2.0")
		if r.Method == http.MethodGet {
			pool["members"] = memberIDs
			if path == "/lbaas/l7policies" && r.URL.Query().Get("listener_id") == "filter-listener" {
				path = "/filter-policies"
			}
			if path == "/lbaas/healthmonitors" && r.URL.Query().Get("pool_id") == "peer-pool" {
				path = "/peer-monitor"
			}
			if body := reads[path]; body != nil {
				safetyWriteJSON(t, w, body)
				return
			}
			switch {
			case strings.HasSuffix(path, "/floatingips"):
				safetyWriteJSON(t, w, map[string]any{"floatingips": []any{}})
				return
			case path == "/lbaas/providers/amphora/gateway_capabilities":
				safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
				return
			case path == poolURL+"/members":
				members := []any{}
				if len(memberIDs) != 0 {
					members = append(members, member)
				}
				safetyWriteJSON(t, w, map[string]any{"members": members})
				return
			}
		}
		switch {
		case r.Method == http.MethodPut && path == filterListenerURL:
			var body struct{ Listener map[string]any }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Listener) != 1 || body.Listener["admin_state_up"] != false {
				t.Fatalf("filter listener must only be closed: request %v, error %v", body, err)
			}
			filterListener["admin_state_up"] = false
			writes = append(writes, "close-filter-listener")
			safetyWriteJSON(t, w, map[string]any{"listener": filterListener})
		case r.Method == http.MethodPut && path == poolURL+"/members":
			var body struct{ Members []json.RawMessage }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Members == nil || len(body.Members) != 0 {
				t.Fatal("backend quarantine must remove every old member in one request")
			}
			memberIDs = []map[string]string{}
			writes = append(writes, "drain")
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPut && path == poolURL:
			var body struct{ Pool map[string]any }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal("could not decode Octavia pool update")
			}
			require.NotContains(t, body.Pool, "backend_tls_require_h2")
			if value, changed := body.Pool["tls_container_ref"]; changed && len(body.Pool) == 1 {
				require.Nil(t, value)
				require.Equal(t, false, pool["admin_state_up"])
				require.Empty(t, memberIDs)
				pool["tls_container_ref"] = nil
				writes = append(writes, "forget-client")
				safetyWriteJSON(t, w, map[string]any{"pool": pool})
				return
			}
			if _, changed := body.Pool["backend_tls_hostname"]; changed {
				if len(memberIDs) != 0 || body.Pool["tls_enabled"] != (body.Pool["backend_tls_hostname"] != nil) {
					t.Fatal("TLS settings must change together on an empty pool")
				}
				if body.Pool["backend_tls_hostname"] == nil && (pool["admin_state_up"] != false || body.Pool["ca_tls_container_ref"] != nil) {
					t.Fatal("releasing TLS requires an already disabled pool and null CA reference")
				}
				if _, changed := body.Pool[sanField]; changed {
					require.Equal(t, false, pool["admin_state_up"])
				}
				for field, value := range body.Pool {
					pool[field] = value
				}
				if body.Pool["admin_state_up"] == true {
					writes = append(writes, "enable")
				} else {
					writes = append(writes, "configure")
				}
			} else {
				for field, value := range body.Pool {
					pool[field] = value
				}
				switch body.Pool["admin_state_up"] {
				case true:
					writes = append(writes, "enable")
				case false:
					writes = append(writes, "disable")
				default:
					writes = append(writes, "mark")
				}
			}
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		case r.Method == http.MethodPost && path == poolURL+"/members":
			if want := spec.Routes[0].Spec.BackendTLS; want != nil && (pool["backend_tls_hostname"] != want.Hostname || pool["tls_enabled"] != true ||
				pool["ca_tls_container_ref"] == nil || pool["admin_state_up"] != true) {
				t.Fatal("member exposed without the active authenticated pool")
			}
			if tls := spec.Routes[0].Spec.BackendTLS; tls != nil && len(tls.SubjectAltNames) != 0 {
				want, err := json.Marshal(tls.SubjectAltNames)
				require.NoError(t, err)
				observed, err := json.Marshal(pool[sanField])
				require.NoError(t, err)
				require.JSONEq(t, string(want), string(observed))
			}
			if client := spec.Gateway.BackendClientCertificate; client.Denied || client.Reference != "" && pool["tls_container_ref"] != client.Reference {
				t.Fatal("member exposed without the authorized backend client identity")
			}
			memberIDs = []map[string]string{{"id": "member-1"}}
			writes = append(writes, "member")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			safetyWriteJSON(t, w, map[string]any{"member": member})
		default:
			t.Errorf("unexpected Octavia or Neutron request %s %s", r.Method, path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	serviceClients := scalarBackendTLSClients(t, project, handle, &writes)
	store := backendca.New(id.Value().ClusterID, serviceClients)
	provider := newTestProvider(t, serviceClients, ProviderConfig{BackendCA: store, BackendTLSPolicy: true})
	spec.Gateway.ClaimBackendCA = func(context.Context) error {
		spec.Gateway.BackendCAOwned = true
		writes = append(writes, "claim")
		return nil
	}
	reconcile := func(expectedWrites ...string) {
		t.Helper()
		for _, expected := range expectedWrites {
			writes = nil
			if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || !slices.Equal(writes, []string{expected}) {
				t.Fatalf("expected only %q, got %v and %v", expected, writes, err)
			}
		}
	}
	fence := errors.New("policy snapshot changed")
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("the stale request reached a write: %v, %v", writes, err)
	}
	spec.ValidateMutation = nil
	reconcile("disable")
	if !slices.Contains(pool["tags"].([]any), any(backendTLSManagedTag)) {
		t.Fatal("first restrictive update omitted durable TLS protection")
	}
	// Restart with the option off while the original members still exist.
	provider.backendTLSPolicy, spec.Routes[0].Spec.BackendTLS, spec.Routes[0].Spec.BackendTLSObserved = false, nil, false
	reconcile("drain")
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || len(writes) != 0 {
		t.Fatalf("restart reopened the enrolled pool: writes %v, error %v", writes, err)
	}
	provider.backendTLSPolicy, spec.Routes[0].Spec.BackendTLS, spec.Routes[0].Spec.BackendTLSObserved = true, tls, true
	writes = nil
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("the stale request published a CA: %v, %v", writes, err)
	}
	spec.ValidateMutation = nil
	reconcile("claim", "publish", "configure", "enable", "member")
	const oldClient = "https://barbican.example/v1/secrets/old-client"
	const newClient = "https://barbican.example/v1/secrets/new-client"
	spec.Gateway.BackendClientCertificate.Reference = oldClient
	reconcile("disable", "drain", "configure", "enable", "member")
	require.Equal(t, oldClient, pool["tls_container_ref"])
	spec.Gateway.BackendClientCertificate.Reference = newClient
	reconcile("disable", "drain", "configure", "enable", "member")
	require.Equal(t, newClient, pool["tls_container_ref"])
	t.Run("withdraw after policy inventory failure", func(t *testing.T) {
		spec := cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{
			Disposition:              cloud.GraphWithdrawBackendClientCertificate,
			Spec:                     cloud.GatewaySpec{Identity: spec.Identity},
			BackendCAOwned:           true,
			BackendClientCertificate: cloud.BackendClientCertificate{Denied: true},
		}}
		lane := openstackidentity.HTTPBackendKey("default", "weighted", 443)
		oldPoolTags, oldMemberTags, oldMonitorTags := pool["tags"], member["tags"], monitor["tags"]
		pool["tags"] = append(safetyRouteTags(t, id, httpResourceRole(rolePool, lane, "")), backendTLSManagedTag, backendTLSClientTag)
		member["tags"] = safetyRouteTags(t, id, httpResourceRole(roleMember, lane, ""))
		monitor["tags"] = safetyRouteTags(t, id, httpResourceRole(roleMonitor, lane, ""))
		thirdParty := maps.Clone(pool)
		thirdParty["id"] = "third-party-pool"
		thirdParty["tags"] = safetyRouteTags(t, id, rolePool)
		previousLB, previousPools := lb["pools"], reads["/lbaas/pools"]
		lb["pools"] = []map[string]string{{"id": "pool-1"}, {"id": "third-party-pool"}}
		reads["/lbaas/pools"] = map[string]any{"pools": []any{pool, thirdParty}}
		defer func() {
			pool["tags"], member["tags"], monitor["tags"] = oldPoolTags, oldMemberTags, oldMonitorTags
			lb["pools"], reads["/lbaas/pools"] = previousLB, previousPools
		}()
		spec.ValidateMutation = func(context.Context) error { return fence }
		writes = nil
		_, err := provider.ReconcileGatewayGraph(t.Context(), spec)
		require.ErrorIs(t, err, fence)
		require.Empty(t, writes)
		spec.ValidateMutation = func(context.Context) error { return nil }
		for _, expected := range []string{"disable", "drain", "forget-client"} {
			writes = nil
			result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
			require.NoError(t, err)
			require.True(t, result.Outcome.MutationSubmitted)
			require.Equal(t, []string{expected}, writes)
		}
		writes = nil
		result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
		require.NoError(t, err)
		require.Equal(t, cloud.OutcomeReady, result.Outcome.State)
		require.Empty(t, writes)
		require.Equal(t, true, thirdParty["admin_state_up"])
		require.Equal(t, newClient, thirdParty["tls_container_ref"])
	})
	require.Equal(t, false, pool["admin_state_up"])
	require.Nil(t, pool["tls_container_ref"])
	require.Empty(t, memberIDs)
	reconcile("configure", "enable", "member")
	spec.Gateway.BackendClientCertificate = cloud.BackendClientCertificate{}
	tls.Denied = true
	reconcile("disable", "drain", "forget-client")
	require.Nil(t, pool["tls_container_ref"])
	tls.Denied = false
	reconcile("enable", "member")
	tls.Hostname = "rotated.example.test"
	reconcile("disable", "drain", "configure", "enable", "member")
	tls.CAConfigMapUID = "recreated-configmap"
	reconcile("disable", "drain", "publish", "configure", "enable", "member")
	delete(pool, "backend_tls_hostname")
	reconcile("disable", "drain")
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || len(writes) != 0 {
		t.Fatal("incomplete TLS readback caused another pool or forwarding mutation")
	}
	pool["backend_tls_hostname"] = tls.Hostname
	reconcile("enable", "member")
	// Requested fields may be visible even when an Octavia worker failed
	// to install them. Retry a restrictive update before allowing traffic.
	pool["provisioning_status"] = "ERROR"
	reconcile("disable")
	reconcile("disable")
	pool["provisioning_status"] = "ACTIVE"
	reconcile("drain", "enable", "member")

	t.Run("SAN changes require closed pool and verified readback", func(t *testing.T) {
		capabilities = append(capabilities, octavia.GatewayCapabilityBackendTLSSubjectAltNames)
		tls.SubjectAltNames = []cloud.BackendTLSSubjectAltName{
			{Type: cloud.BackendTLSSANHostname, Hostname: "service.example.test"},
			{Type: cloud.BackendTLSSANURI, URI: "spiffe://id.example.test/workload%2fa"},
		}
		reconcile("disable", "drain", "configure")
		require.Contains(t, pool["tags"], backendTLSSANTag)
		require.Equal(t, tls.Hostname, pool["backend_tls_hostname"])

		// Omission, null and an incorrect list are insufficient to reopen.
		actual := pool[sanField]
		for _, invalidReadback := range []any{nil, []any{map[string]string{"type": "Hostname", "hostname": tls.Hostname}}} {
			pool[sanField] = invalidReadback
			reconcile("configure")
			require.Equal(t, false, pool["admin_state_up"])
			require.Empty(t, memberIDs)
		}
		delete(pool, sanField)
		reconcile("configure")
		require.Equal(t, actual, pool[sanField])
		reconcile("enable", "member")

		tls.SubjectAltNames[1].URI = "spiffe://id.example.test/workload%2Fa"
		reconcile("disable", "drain", "configure", "enable", "member")

		// A regional SAN opt-out withdraws traffic without changing trust.
		capabilities = []string{octavia.GatewayCapabilityBackendTLSHostname}
		reconcile("disable", "drain")
		require.Equal(t, false, pool["admin_state_up"])
		require.Contains(t, pool["tags"], backendTLSSANTag)
		require.Contains(t, pool, sanField)
		tls.SubjectAltNames = nil
		reconcile("configure")
		require.Equal(t, []any{}, pool[sanField])
		require.Contains(t, pool["tags"], backendTLSSANTag)

		// After restart, a response omitting the field cannot confirm the clear.
		provider = newTestProvider(t, serviceClients, ProviderConfig{BackendCA: store, BackendTLSPolicy: true})
		delete(pool, sanField)
		reconcile("configure")
		require.Equal(t, false, pool["admin_state_up"])
		require.Empty(t, memberIDs)
		require.Equal(t, []any{}, pool[sanField])
		reconcile("enable", "member")
		require.Contains(t, pool["tags"], backendTLSSANTag)
		require.Equal(t, tls.Hostname, pool["backend_tls_hostname"])
	})

	t.Run("CA revoked while another listener has incomplete filter inventory", func(t *testing.T) {
		// Add a healthy backend on 80 and an unreadable filter policy on 8080
		// to the already programmed TLS route.
		configured := spec
		configured.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "public", Port: 80}, {Name: "filter", Port: 8080}}
		configured.Routes = slices.Clone(spec.Routes)
		configured.Routes[0].Spec.ScopedListenerPorts = []int{80}
		peer := configured.Routes[0]
		peer.Spec.Identity.RouteName, peer.Spec.Identity.RouteUID = "peer", "peer-uid"
		peer.Spec.BackendTLS, peer.Spec.PathValue, peer.Spec.Precedence = nil, "/peer", 1
		peerID, err := provider.identity(peer.Spec.Identity)
		if err != nil {
			t.Fatal(err)
		}
		filter := cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{
			Identity: peer.Spec.Identity, ScopedListenerPorts: []int{8080},
		}}
		filter.Spec.Identity.RouteName, filter.Spec.Identity.RouteUID = "filter", "filter-uid"
		filterID, err := provider.identity(filter.Spec.Identity)
		if err != nil {
			t.Fatal(err)
		}
		configured.Routes = append(configured.Routes, peer, filter)

		refs := func(ids ...string) []map[string]string {
			result := make([]map[string]string, 0, len(ids))
			for _, resourceID := range ids {
				result = append(result, map[string]string{"id": resourceID})
			}
			return result
		}
		peerPool := maps.Clone(pool)
		peerPool["id"], peerPool["tags"], peerPool["healthmonitor_id"], peerPool["members"] = "peer-pool", safetyRouteTags(t, peerID, rolePool), "peer-monitor", refs("peer-member")
		peerPool["tls_enabled"], peerPool["backend_tls_hostname"], peerPool["ca_tls_container_ref"] = false, nil, nil
		delete(peerPool, sanField)
		peerMember, peerMonitor := maps.Clone(member), maps.Clone(monitor)
		peerMember["id"], peerMember["tags"] = "peer-member", safetyRouteTags(t, peerID, roleMember)
		peerMonitor["id"], peerMonitor["pools"], peerMonitor["tags"] = "peer-monitor", refs("peer-pool"), safetyRouteTags(t, peerID, roleMonitor)
		peerPolicy, peerPath, peerHost := maps.Clone(policy), maps.Clone(rules[0]), maps.Clone(rules[1])
		peerPolicy["id"], peerPolicy["redirect_pool_id"], peerPolicy["position"] = "peer-policy", "peer-pool", 2
		peerPolicy["rules"], peerPolicy["tags"] = refs("peer-path", "peer-host"), safetyRouteTags(t, peerID, rolePolicyExact)
		peerPath["id"], peerPath["value"], peerPath["tags"] = "peer-path", "/peer", safetyRouteTags(t, peerID, roleRulePath)
		peerHost["id"], peerHost["tags"] = "peer-host", safetyRouteTags(t, peerID, roleRuleHost)
		filterListener = maps.Clone(listener)
		filterListener["id"], filterListener["protocol_port"], filterListener["l7policies"] = "filter-listener", 8080, refs("filtered")
		filterListener["tags"] = append(safetyGatewayTags(t, id, roleListener), scopedPortTag(cloud.ListenerProtocolHTTP, 8080))
		filterRole := openstackidentity.HTTPWeightedPolicyRole(rolePolicyExact, openstackidentity.HTTPWeightedRuleKey(0),
			openstackidentity.HTTPMatchKey("", cloud.PathMatchExact, "/filtered", ""))
		filterPolicy := map[string]any{"id": "filtered", "project_id": project, "listener_id": "filter-listener", "action": "WEIGHTED_TARGETS",
			"position": 1, "provisioning_status": "ACTIVE", "redirect_pool_id": nil, "targets": []octavia.WeightedHTTPTarget{fixedWeightedHTTPTarget(1)},
			"rules": refs(), "tags": append(safetyRouteTags(t, filterID, filterRole), openstackidentity.BackendRequestFiltersPolicyTag)}

		previousReads, previousLB, previousListener := maps.Clone(reads), maps.Clone(lb), maps.Clone(listener)
		defer func() {
			maps.Copy(lb, previousLB)
			maps.Copy(listener, previousListener)
			reads = previousReads
		}()
		lb["pools"], lb["listeners"], listener["l7policies"] = refs("pool-1", "peer-pool"), refs("listener-1", "filter-listener"), refs("policy-1", "peer-policy")
		reads["/lbaas/listeners"] = map[string]any{"listeners": []any{listener, filterListener}}
		reads["/lbaas/pools"] = map[string]any{"pools": []any{pool, peerPool}}
		reads["/peer-monitor"] = map[string]any{"healthmonitors": []any{peerMonitor}}
		reads["/lbaas/pools/peer-pool/members"] = map[string]any{"members": []any{peerMember}}
		reads["/lbaas/l7policies"] = map[string]any{"l7policies": []any{policy, peerPolicy}}
		reads["/lbaas/l7policies/peer-policy/rules"] = map[string]any{"rules": []any{peerPath, peerHost}}
		reads["/filter-policies"] = map[string]any{"l7policies": []any{filterPolicy}}
		reads["/lbaas/l7policies/filtered/rules"] = map[string]any{"rules": []any{}}

		run := func(expected string, wantErr error) {
			t.Helper()
			writes = nil
			result, err := provider.ReconcileGatewayGraph(t.Context(), configured)
			var wantWrites []string
			if expected != "" {
				wantWrites = []string{expected}
			}
			var incomplete *httpFilterInventoryError
			if (wantErr == nil && err != nil || wantErr == cloud.ErrDependencyNotReady && !errors.As(err, &incomplete) ||
				wantErr != nil && wantErr != cloud.ErrDependencyNotReady && !errors.Is(err, wantErr)) || !slices.Equal(writes, wantWrites) {
				t.Fatalf("expected one reconciliation with write %q and %v: got %v, %v and %+v", expected, wantErr, writes, err, result)
			}
			if expected == "disable" && (!result.Outcome.MutationSubmitted || result.Routes[0].Result.ObservationDeferred) {
				t.Fatalf("the withdrawn route must publish its progress: %+v", result.Routes)
			}
			if listener["admin_state_up"] != true || peerPool["admin_state_up"] != true || peerMember["admin_state_up"] != true {
				t.Fatal("the healthy backend on port 80 was interrupted")
			}
		}
		caReference := pool["ca_tls_container_ref"]
		if pool["admin_state_up"] != true || caReference == nil || len(memberIDs) != 1 {
			t.Fatal("the authenticated backend must initially be serving")
		}
		filterPolicy["tags"] = append(safetyRouteTags(t, filterID, filterRole), openstackidentity.RequestCORSPolicyTag)
		filterPolicy["admin_state_up"] = true
		tls.Denied = true
		for _, missing := range []string{"action", "position"} {
			previous := filterPolicy[missing]
			delete(filterPolicy, missing)
			filterListener["admin_state_up"] = true
			filterPolicy["targets"] = []octavia.WeightedHTTPTarget{{PoolID: "peer-pool", Weight: 1}}
			run("", cloud.ErrOwnershipConflict)
			filterPolicy["targets"] = []octavia.WeightedHTTPTarget{fixedWeightedHTTPTarget(1)}
			run("close-filter-listener", cloud.ErrDependencyNotReady)
			filterPolicy[missing] = previous
		}
		delete(filterPolicy, "position")
		if filterListener["admin_state_up"] != false || pool["admin_state_up"] != true {
			t.Fatal("the affected listener must close before the revoked pool changes")
		}
		configured.ValidateMutation = func(context.Context) error { return fence }
		run("", fence)
		configured.ValidateMutation = nil
		pool["project_id"] = "foreign-project"
		run("", cloud.ErrOwnershipConflict)
		pool["project_id"] = project
		run("disable", cloud.ErrDependencyNotReady)
		if pool["admin_state_up"] != false || pool["ca_tls_container_ref"] != caReference {
			t.Fatal("the revoked backend must be quarantined without changing its trust settings")
		}
		run("drain", cloud.ErrDependencyNotReady)
		run("", cloud.ErrDependencyNotReady)
		tls.Denied = false
		run("", cloud.ErrDependencyNotReady)
		if pool["admin_state_up"] != false || len(memberIDs) != 0 {
			t.Fatal("incomplete policy inventory must prevent reopening and member restoration")
		}
		filterPolicy["position"], filterPolicy["admin_state_up"] = 1, false
		filterPolicy["tags"] = append(safetyRouteTags(t, filterID, filterRole), openstackidentity.BackendRequestFiltersPolicyTag)
		run("enable", nil)
		run("member", nil)
		if pool["admin_state_up"] != true || len(memberIDs) != 1 || pool["ca_tls_container_ref"] != caReference {
			t.Fatal("the backend must resume with the verified CA after complete observation")
		}
	})

	capabilities = []string{}
	reconcile("disable", "drain")
	tls.Denied = true
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || !slices.Equal(writes, []string{"retire"}) ||
		pool["tls_enabled"] != true || pool["admin_state_up"] != false || len(memberIDs) != 0 {
		t.Fatalf("denied policy: writes %v, tls enabled %v, error %v", writes, pool["tls_enabled"], err)
	}
	attached := pool["ca_tls_container_ref"].(string)
	if revision, err := store.Find(t.Context(), spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CADigest, attached); err != nil || revision.Reference() != attached {
		t.Fatal("denial retired the CA still referenced by the closed pool")
	}
	provider.backendTLSPolicy, spec.Routes[0].Spec.BackendTLSObserved = false, false
	spec.Routes[0].Spec.BackendTLS = nil
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || len(writes) != 0 || pool["tls_enabled"] != true {
		t.Fatal("a controller with BackendTLSPolicy turned off removed existing TLS")
	}
	provider.backendTLSPolicy, capabilities, spec.Routes[0].Spec.BackendTLSObserved = true, []string{octavia.GatewayCapabilityBackendTLSHostname}, true
	reconcile("configure", "enable", "mark", "member")

	// Finalization still inventories this Gateway's CAs when enrollment
	// has been disabled. Each unreferenced revision retires independently.
	provider.backendTLSPolicy = false
	cleanup := cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, BackendCAOwned: true}}
	for _, want := range []string{"retire", ""} {
		writes = nil
		initial := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(cloud.GatewayState{})}
		result, err := provider.retireUnusedBackendCA(t.Context(), cleanup, observedGatewayGraph{}, initial)
		if err != nil || want == "" && (len(writes) != 0 || result.Outcome.State != cloud.OutcomeReady) ||
			want != "" && (!slices.Equal(writes, []string{want}) || result.Outcome.State != cloud.OutcomeProgressing) {
			t.Fatalf("retirement after opt-out: writes %v, state %v, error %v", writes, result.Outcome.State, err)
		}
	}
}

func TestGRPCBackendTLSRequiresRegionalEnforcementAndVerifiedPool(t *testing.T) {
	id := safetyTestIdentity(t)
	project := id.Value().OpenStackProjectID
	graph := completeGraphSpec(id)
	spec := &graph.Routes[0].Spec
	spec.GRPC, spec.BackendTLSObserved, spec.BackendTLS = true, true, newGraphBackendTLS(t)
	spec.Gateway.LoadBalancerID = "load-balancer-1"
	desired := desiredGatewayGraph{spec: graph, routes: []desiredRoute{{spec: *spec, identity: id}}}
	desired.spec.Gateway.BackendCAOwned = true
	const clientRef = "https://barbican.example/v1/secrets/grpc-client"
	desired.spec.Gateway.BackendClientCertificate.Reference = clientRef
	route := &desired.routes[0]
	lb := &octavia.LoadBalancer{}
	lb.ID, lb.Provider, lb.ProvisioningStatus = "load-balancer-1", "amphora", "ACTIVE"
	lb.Pools, lb.Listeners = []pools.Pool{{ID: "pool-1"}}, []listeners.Listener{}
	actual := observedGatewayGraph{protocol: cloud.ListenerProtocolHTTP, loadBalancer: lb, routes: []observedRouteGraph{{}}}

	capabilities := []string{octavia.GatewayCapabilityBackendTLSHostname}
	var pool map[string]any
	var sent map[string]any
	members := []any{}
	refuseReopening := false
	handle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/v2.0")
		switch {
		case r.Method == http.MethodGet && path == "/lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": capabilities})
		case r.Method == http.MethodGet && path == "/lbaas/pools":
			safetyWriteJSON(t, w, map[string]any{"pools": []any{pool}})
		case r.Method == http.MethodGet && path == "/lbaas/pools/pool-1/members":
			safetyWriteJSON(t, w, map[string]any{"members": members})
		case r.Method == http.MethodPut && path == "/lbaas/pools/pool-1/members":
			require.Equal(t, false, pool["admin_state_up"])
			var body struct{ Members []json.RawMessage }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.NotNil(t, body.Members)
			require.Empty(t, body.Members)
			members, pool["members"] = []any{}, []any{}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet && path == "/lbaas/healthmonitors":
			safetyWriteJSON(t, w, map[string]any{"healthmonitors": []any{}})
		case r.Method == http.MethodPost && path == "/lbaas/pools", r.Method == http.MethodPut && path == "/lbaas/pools/pool-1":
			var body struct{ Pool map[string]any }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			sent = body.Pool
			if refuseReopening && sent["admin_state_up"] == true {
				http.Error(w, "unable to validate trust", http.StatusBadRequest)
				return
			}
			if r.Method == http.MethodPost {
				pool = map[string]any{"id": "pool-1", "provisioning_status": "ACTIVE", "project_id": project,
					"loadbalancers": []map[string]string{{"id": lb.ID}}, "listeners": []any{}, "members": []any{}}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
			}
			maps.Copy(pool, sent)
			safetyWriteJSON(t, w, map[string]any{"pool": pool})
		default:
			t.Errorf("unexpected Octavia operation %s %s", r.Method, path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	var writes []string
	serviceClients := scalarBackendTLSClients(t, project, handle, &writes)
	store := backendca.New(id.Value().ClusterID, serviceClients)
	revision, err := store.Ensure(t.Context(), spec.Identity.GatewayUID, spec.BackendTLS.CAConfigMapUID, spec.BackendTLS.CAPEM)
	require.NoError(t, err)
	provider := newTestProvider(t, serviceClients, ProviderConfig{BackendCA: store, BackendTLSPolicy: true})
	read := func() ([]observedRouteGraph, error) {
		t.Helper()
		return provider.observeRouteGraphs(t.Context(), []routeGraphOwner{{identity: id, expectsBackendTLS: true}}, lb, nil, cloud.ListenerProtocolHTTP)
	}
	refresh := func() {
		t.Helper()
		var err error
		actual.routes, err = read()
		require.NoError(t, err)
	}
	prepare := func() *graphMutation {
		t.Helper()
		return provider.prepareBackendTLS(t.Context(), &desired, actual)
	}
	mutate := func(step *routeMutation) {
		t.Helper()
		require.NotNil(t, step)
		require.NoError(t, provider.executeRouteMutation(t.Context(), *route, *step))
	}
	poolStep := func() *routeMutation {
		return route.backendTLS.poolMutation(*route, actual.routes[0])
	}

	require.Nil(t, prepare())
	require.Equal(t, backendTLSBlocked, route.backendTLS.phase)
	capabilities = append(capabilities, octavia.GatewayCapabilityBackendTLSH2ALPN)
	require.Nil(t, prepare())
	require.Equal(t, backendTLSConfigure, route.backendTLS.phase)
	mutate(poolStep())
	require.Equal(t, "HTTP", sent["protocol"])
	require.Equal(t, false, sent["admin_state_up"])
	require.Equal(t, true, sent["tls_enabled"])
	require.Equal(t, spec.BackendTLS.Hostname, sent["backend_tls_hostname"])
	require.Equal(t, revision.Reference(), sent["ca_tls_container_ref"])
	require.Equal(t, clientRef, sent["tls_container_ref"])
	require.Equal(t, true, sent["backend_tls_require_h2"])
	require.NotContains(t, sent, "alpn_protocols")
	require.NotContains(t, sent, "backend_h2c")

	pool["loadbalancers"] = []map[string]string{{"id": "another-load-balancer"}}
	_, err = read()
	require.ErrorIs(t, err, cloud.ErrOwnershipConflict)
	pool["loadbalancers"] = []map[string]string{{"id": lb.ID}}

	delete(pool, "tls_container_ref")
	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSBlocked, route.backendTLS.phase)
	require.Nil(t, poolStep())
	pool["tls_container_ref"] = clientRef

	// Merely offering h2 cannot authorize reopening a protected pool.
	pool["alpn_protocols"] = []string{"h2"}
	delete(pool, "backend_tls_require_h2")
	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSBlocked, route.backendTLS.phase)
	require.Nil(t, poolStep())

	pool["backend_tls_require_h2"] = false
	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSConfigure, route.backendTLS.phase)
	mutate(poolStep())
	require.Equal(t, true, sent["backend_tls_require_h2"])
	require.Equal(t, clientRef, sent["tls_container_ref"])
	require.NotContains(t, sent, "admin_state_up")

	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSVerified, route.backendTLS.phase)
	reopen := poolStep()
	require.NotNil(t, reopen)
	refuseReopening = true
	require.Error(t, provider.executeRouteMutation(t.Context(), *route, *reopen))
	require.Equal(t, false, pool["admin_state_up"])
	refuseReopening = false
	mutate(reopen)
	require.Equal(t, true, sent["admin_state_up"])
	require.Equal(t, true, sent["backend_tls_require_h2"])

	// Add an explicit SAN without changing the client identity or h2.
	const sanField = "backend_tls_subject_alt_names"
	require.NotContains(t, sent, sanField)
	require.Equal(t, clientRef, sent["tls_container_ref"])
	capabilities = append(capabilities, octavia.GatewayCapabilityBackendTLSSubjectAltNames)
	spec.BackendTLS.SubjectAltNames = []cloud.BackendTLSSubjectAltName{{Type: cloud.BackendTLSSANURI, URI: "spiffe://id.example.test/grpc"}}
	refresh()
	disable := prepare()
	require.NotNil(t, disable)
	mutate(&disable.route)
	refresh()
	require.Nil(t, prepare())
	mutate(poolStep())
	require.NotEmpty(t, sent[sanField])
	require.Equal(t, true, sent["backend_tls_require_h2"])
	require.Equal(t, clientRef, sent["tls_container_ref"])
	refresh()
	require.Nil(t, prepare())
	mutate(poolStep())
	require.Equal(t, true, sent["admin_state_up"])
	require.NotEmpty(t, sent[sanField])
	require.Equal(t, true, sent["backend_tls_require_h2"])
	require.Equal(t, clientRef, sent["tls_container_ref"])

	// A previously verified pool must be closed before draining its members
	// after regional SAN capability loss.
	members = []any{map[string]any{
		"id": "member-1", "project_id": project, "provisioning_status": "ACTIVE", "admin_state_up": true,
		"address": "10.0.0.10", "protocol_port": 8443, "tags": safetyRouteTags(t, id, roleMember),
	}}
	pool["members"] = []map[string]string{{"id": "member-1"}}
	refresh()
	capabilities = []string{octavia.GatewayCapabilityBackendTLSHostname, octavia.GatewayCapabilityBackendTLSH2ALPN, "backend_tls_subject_alt_names_v2"}
	step := prepare()
	require.NotNil(t, step)
	mutate(&step.route)
	require.Equal(t, map[string]any{"admin_state_up": false}, sent)
	refresh()
	step = prepare()
	require.NotNil(t, step)
	require.Equal(t, routeMutationClearPoolMembers, step.route.kind)
	mutate(&step.route)
	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSBlocked, route.backendTLS.phase)
	require.Nil(t, poolStep())

	// A mixed graph still verifies the ordinary HTTP pool when gRPC SANs are unavailable.
	httpRoute, httpPool := *route, actual.routes[0]
	httpTLS := *httpRoute.spec.BackendTLS
	httpTLS.SubjectAltNames = nil
	httpRoute.spec.GRPC, httpRoute.spec.BackendTLS = false, &httpTLS
	poolCopy := *httpPool.pool
	poolCopy.AdminStateUp = true
	poolCopy.Tags = slices.DeleteFunc(slices.Clone(poolCopy.Tags), func(tag string) bool { return tag == backendTLSSANTag })
	httpPool.pool, httpPool.backendTLS.SubjectAltNames = &poolCopy, nil
	httpPool.backendTLS.RequireH2, httpPool.backendTLS.RequireH2Observed = false, false
	mixedDesired, mixedActual := desired, actual
	mixedDesired.spec.Routes = []cloud.RouteGraphNode{desired.spec.Routes[0], {Disposition: cloud.GraphEnsure}}
	mixedDesired.routes = []desiredRoute{*route, httpRoute}
	mixedActual.routes = []observedRouteGraph{actual.routes[0], httpPool}
	require.Nil(t, provider.prepareBackendTLS(t.Context(), &mixedDesired, mixedActual))
	require.Equal(t, backendTLSBlocked, mixedDesired.routes[0].backendTLS.phase)
	require.Equal(t, backendTLSVerified, mixedDesired.routes[1].backendTLS.phase)

	// Explicit policy removal works even when the SAN capability is unavailable.
	route.spec.BackendTLS = nil
	require.Nil(t, prepare())
	require.Equal(t, backendTLSRelease, route.backendTLS.phase)
	mutate(poolStep())
	require.Equal(t, false, sent["backend_tls_require_h2"])
	require.Equal(t, []any{}, sent[sanField])
	require.Nil(t, sent["backend_tls_hostname"])
	require.Equal(t, false, sent["tls_enabled"])
	require.Contains(t, pool["tags"], backendTLSSANTag)

	// A partial GET after policy deletion still cannot authorize reopening.
	delete(pool, sanField)
	refresh()
	require.Nil(t, prepare())
	require.Equal(t, backendTLSRelease, route.backendTLS.phase)
	mutate(poolStep())
	require.Equal(t, []any{}, sent[sanField])
	require.NotContains(t, sent, "admin_state_up")
	require.Equal(t, false, pool["admin_state_up"])

	refresh()
	require.Nil(t, prepare())
	mutate(poolStep())
	require.Equal(t, map[string]any{"admin_state_up": true}, sent)
	refresh()
	require.Nil(t, prepare())
	mutate(poolStep())
	require.Contains(t, pool["tags"], backendTLSSANTag)
	require.NotContains(t, pool["tags"], backendTLSManagedTag)
}

func newGraphBackendTLS(t *testing.T) *cloud.BackendTLSValidation {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	hash := sha256.Sum256(data)
	return &cloud.BackendTLSValidation{Hostname: "backend.example.test", CAConfigMapUID: "configmap-uid", CADigest: hex.EncodeToString(hash[:]), CAPEM: data}
}

func scalarBackendTLSClients(t *testing.T, project string, octaviaHandler http.Handler, writes *[]string) ServiceClients {
	t.Helper()
	const region = "backend-tls-test-region"
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	mux.HandleFunc("POST /v3/auth/tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Subject-Token", "test")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"token":{"project":{"id":%q},"user":{"id":"test-creator"},"catalog":[
			{"type":"load-balancer","endpoints":[{"interface":"public","region":%q,"url":%q}]},
			{"type":"network","endpoints":[{"interface":"public","region":%q,"url":%q}]},
			{"type":"key-manager","endpoints":[{"interface":"public","region":%q,"url":%q}]}
		]}}`, project, region, server.URL+"/octavia/", region, server.URL+"/neutron/", region, server.URL+"/barbican/")
	})
	for _, service := range []string{"/octavia/", "/neutron/"} {
		mux.HandleFunc("GET "+service+"{$}", func(w http.ResponseWriter, _ *http.Request) {
			safetyWriteJSON(t, w, map[string]any{"versions": []map[string]string{{"id": "v2.0", "status": "CURRENT"}}})
		})
	}
	clouds := filepath.Join(t.TempDir(), "clouds.yaml")
	if err := os.WriteFile(clouds, fmt.Appendf(nil, `clouds:
  test:
    auth_type: v3applicationcredential
    auth:
      auth_url: %q
      application_credential_id: test-id
      application_credential_secret: test-secret
    interface: public
`, server.URL+"/v3"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OS_CACERT", "")
	t.Setenv("OS_CERT", "")
	t.Setenv("OS_KEY", "")
	cfg := clients.ClientConfig{Region: region, Microversion: "2.8", CloudsYAMLPath: clouds, CloudName: "test", APIQPS: 100, APIBurst: 100}
	services, err := clients.NewServiceClients(t.Context(), cfg)
	if err != nil {
		t.Fatalf("could not initialize the local service catalog: %v", err)
	}

	// Keep the regular client and signed project/user scope. Replace only
	// the transport after Keystone authentication to run the APIs in process.
	service := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/barbican/") {
			mux.ServeHTTP(w, r)
			return
		}
		octaviaHandler.ServeHTTP(w, r)
	}))
	services.Network.HTTPClient.Transport = service.HTTPClient.Transport

	var rows []secrets.Secret
	payloads := make(map[string][]byte)
	mux.HandleFunc("GET /barbican/{$}", func(w http.ResponseWriter, _ *http.Request) {
		safetyWriteJSON(t, w, map[string]any{"versions": []any{map[string]any{"id": "v1", "status": "CURRENT",
			"links": []map[string]string{{"rel": "self", "href": server.URL + "/barbican/v1/"}}}}})
	})
	mux.HandleFunc("GET /barbican/v1/secrets", func(w http.ResponseWriter, r *http.Request) {
		items := []secrets.Secret{}
		name := r.URL.Query().Get("name")
		for _, row := range rows {
			if row.Name == name || strings.HasSuffix(name, "%") && strings.HasPrefix(row.Name, strings.TrimSuffix(name, "%")) {
				items = append(items, row)
			}
		}
		safetyWriteJSON(t, w, map[string]any{"secrets": items, "total": len(items)})
	})
	mux.HandleFunc("POST /barbican/v1/secrets", func(w http.ResponseWriter, r *http.Request) {
		var opts secrets.CreateOpts
		if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
			t.Fatal("could not decode local CA request")
		}
		payload, err := base64.StdEncoding.DecodeString(opts.Payload)
		if err != nil {
			t.Fatal("could not read local CA payload")
		}
		id := uuid.NewString()
		ref := server.URL + "/barbican/v1/secrets/" + id
		rows = append(rows, secrets.Secret{Name: opts.Name, SecretRef: ref, CreatorID: "test-creator", Status: "ACTIVE",
			SecretType: string(secrets.CertificateSecret), ContentTypes: map[string]string{"default": "application/octet-stream"}})
		payloads[id] = payload
		*writes = append(*writes, "publish")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		safetyWriteJSON(t, w, map[string]string{"secret_ref": ref})
	})
	mux.HandleFunc("GET /barbican/v1/secrets/{id}", func(w http.ResponseWriter, r *http.Request) {
		for _, row := range rows {
			if strings.HasSuffix(row.SecretRef, "/"+r.PathValue("id")) {
				safetyWriteJSON(t, w, row)
				return
			}
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("GET /barbican/v1/secrets/{id}/payload", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payloads[r.PathValue("id")])
	})
	mux.HandleFunc("DELETE /barbican/v1/secrets/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		rows = slices.DeleteFunc(rows, func(row secrets.Secret) bool { return strings.HasSuffix(row.SecretRef, "/"+id) })
		delete(payloads, id)
		*writes = append(*writes, "retire")
		w.WriteHeader(http.StatusNoContent)
	})
	return services
}

func TestBackendTLSWeightedDenialPreservesHealthyShare(t *testing.T) {
	desired, actual := weightedPlanFixture()
	desired.spec.Gateway.BackendClientCertificate.Denied = true
	actual.loadBalancer.ProvisioningStatus = "ACTIVE"
	remote := &desired.routes[0].httpParts[1]
	remote.spec.BackendTLS = newGraphBackendTLS(t)
	local := actual.routes[0].httpParts[desired.routes[0].httpParts[0].backendKey]
	local.pool.TLSContainerRef = "https://barbican.example/v1/secrets/third-party-client"
	observed := actual.routes[0].httpParts[remote.backendKey]
	observed.pool.Tags = append(observed.pool.Tags, backendTLSManagedTag)
	observed.pool.TLSContainerRef = "https://barbican.example/v1/secrets/revoked-client"
	observed.backendTLS.AdminObserved, observed.backendTLS.ClientRefObserved = true, true
	provider := &Provider{}
	step := provider.prepareBackendTLS(t.Context(), &desired, actual)
	require.NotNil(t, step)
	require.Equal(t, remote.backendKey, step.backendKey)
	require.Equal(t, new(false), step.route.poolAdmin)
	observed.pool.AdminStateUp = false
	step = provider.prepareBackendTLS(t.Context(), &desired, actual)
	require.NotNil(t, step)
	require.Equal(t, remote.backendKey, step.backendKey)
	require.Equal(t, routeMutationClearPoolMembers, step.route.kind)
	observed.members = nil
	step = provider.prepareBackendTLS(t.Context(), &desired, actual)
	require.NotNil(t, step)
	require.Equal(t, remote.backendKey, step.backendKey)
	require.True(t, step.route.clearTLSClientRef)
	observed.pool.TLSContainerRef = ""
	require.Nil(t, provider.prepareBackendTLS(t.Context(), &desired, actual))
	steps := weightedPlan(t, desired, actual)
	if len(steps) != 1 || steps[0].route.kind != routeMutationSetPolicyTarget || !steps[0].withdrawHTTPPolicy || steps[0].route.weightedTargets == nil {
		t.Fatalf("wanted an in-place weighted withdrawal, got %#v", steps)
	}
	targets := *steps[0].route.weightedTargets
	if len(targets) != 2 || !weightedTargetsEqual(targets[0], octavia.WeightedHTTPTarget{Weight: 3, PoolID: "pool-1"}) ||
		!weightedTargetsEqual(targets[1], fixedWeightedHTTPTarget(1)) {
		t.Fatalf("the denied share replaced a healthy weighted target: %#v", targets)
	}
}

func TestBackendTLSRestartHoldsPoolRepresentationWithoutPolicyObservation(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("grouped-replacement-%t", grouped), func(t *testing.T) {
			desired, actual := weightedPlanFixture()
			scalar, pool := convergedRoutePlanFixture()
			pool.members = nil
			pool.pool.ProvisioningStatus, pool.pool.AdminStateUp = "ACTIVE", false
			pool.pool.Tags = append(pool.pool.Tags, backendTLSManagedTag)
			pool.backendTLS = octavia.PoolBackendTLS{AdminObserved: true, EnabledObserved: true, CAObserved: true, HostnameObserved: true, ClientRefObserved: true}
			if grouped {
				actual.routes[0] = pool
			} else {
				desired.routes[0] = scalar
				actual.routes[0] = observedRouteGraph{httpParts: map[string]*observedRouteGraph{"old": &pool}}
			}
			actual.loadBalancer.ProvisioningStatus = "ACTIVE"
			p := &Provider{}
			if step := p.prepareBackendTLS(t.Context(), &desired, actual); step != nil {
				t.Fatalf("a closed pool was changed on restart: %v", step)
			}
			if desired.spec.Routes[0].Disposition != cloud.GraphPreserve {
				t.Fatal("a replacement could lose its old TLS protection")
			}
			for _, step := range weightedPlan(t, desired, actual) {
				if step.kind == graphRouteMutation {
					t.Fatalf("replaced a protected pool without a live policy observation: %v", step.route.kind)
				}
			}
		})
	}
}

func TestBackendTLSFreshWithdrawalPrecedesDisabledPoolRetry(t *testing.T) {
	desired, actual := weightedPlanFixture()
	actual.loadBalancer.ProvisioningStatus = "ACTIVE"
	for i := range 2 {
		part := &desired.routes[0].httpParts[i]
		part.spec.BackendTLS = &cloud.BackendTLSValidation{Denied: true}
		observed := actual.routes[0].httpParts[part.backendKey]
		observed.pool.Tags = append(observed.pool.Tags, backendTLSManagedTag)
		observed.pool.TLSEnabled, observed.backendTLS.AdminObserved = true, true
		observed.pool.AdminStateUp = i == 1
		if i == 0 {
			observed.pool.ProvisioningStatus = "ERROR"
		}
	}
	step := (&Provider{}).prepareBackendTLS(t.Context(), &desired, actual)
	if step == nil || step.backendKey != desired.routes[0].httpParts[1].backendKey || step.route.retry ||
		step.route.poolAdmin == nil || *step.route.poolAdmin {
		t.Fatal("an already-disabled pool prevented a new withdrawal")
	}
}
