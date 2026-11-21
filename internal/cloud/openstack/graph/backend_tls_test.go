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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2/openstack/keymanager/v1/secrets"
)

func TestBackendTLSScalarQuarantinesBeforePublishingAndReopening(t *testing.T) {
	id := safetyTestIdentity(t)
	project := id.Value().OpenStackProjectID
	spec := completeGraphSpec(id)
	spec.Gateway.Spec.ListenerProtocol, spec.Routes[0].Spec.GatewayRequirements.ListenerProtocol = cloud.ListenerProtocolHTTP, cloud.ListenerProtocolHTTP
	tls := newGraphBackendTLS(t)
	spec.Routes[0].Spec.BackendTLS = tls
	const poolURL = "/lbaas/pools/pool-1"
	memberIDs := []map[string]string{{"id": "member-1"}}
	pool := map[string]any{
		"id": "pool-1", "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN", "project_id": project,
		"provisioning_status": "ACTIVE", "admin_state_up": true, "backend_tls_hostname": nil, "ca_tls_container_ref": nil, "tls_enabled": false,
		"healthmonitor_id": "monitor-1", "listeners": []map[string]string{{"id": "listener-1"}},
		"loadbalancers": []map[string]string{{"id": "load-balancer-1"}}, "tags": safetyRouteTags(t, id, rolePool),
	}
	lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, id, roleLoadBalancer), "ACTIVE")
	lb["pools"], lb["listeners"] = []map[string]string{{"id": "pool-1"}}, []map[string]string{{"id": "listener-1"}}
	listener := map[string]any{
		"id": "listener-1", "project_id": project, "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
		"provisioning_status": "ACTIVE", "default_pool_id": nil, "loadbalancers": pool["loadbalancers"],
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
	var writes []string
	handle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/octavia"), "/v2.0")
		if r.Method == http.MethodGet {
			pool["members"] = memberIDs
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
			if _, changed := body.Pool["backend_tls_hostname"]; changed {
				if len(memberIDs) != 0 || body.Pool["tls_enabled"] != (body.Pool["backend_tls_hostname"] != nil) {
					t.Fatal("TLS settings must change together on an empty pool")
				}
				if body.Pool["backend_tls_hostname"] == nil && (pool["admin_state_up"] != false || body.Pool["ca_tls_container_ref"] != nil) {
					t.Fatal("releasing TLS requires an already disabled pool and null CA reference")
				}
				for field, value := range body.Pool {
					pool[field] = value
				}
				writes = append(writes, "configure")
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
	provider := newTestProvider(t, serviceClients, ProviderConfig{BackendCA: store})
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
	reconcile("drain", "mark")
	writes = nil
	spec.ValidateMutation = func(context.Context) error { return fence }
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); !errors.Is(err, fence) || len(writes) != 0 {
		t.Fatalf("the stale request published a CA: %v, %v", writes, err)
	}
	spec.ValidateMutation = nil
	reconcile("publish", "configure", "member")
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
	capabilities = []string{}
	reconcile("disable", "drain")
	tls.Denied = true
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || len(writes) != 0 || pool["tls_enabled"] != true {
		t.Fatal("a denied policy changed the old trust configuration or reopened forwarding")
	}
	provider.backendCA = nil
	spec.Routes[0].Spec.BackendTLS = nil
	writes = nil
	if _, err := provider.ReconcileGatewayGraph(t.Context(), spec); err != nil || len(writes) != 0 || pool["tls_enabled"] != true {
		t.Fatal("a controller with BackendTLSPolicy turned off removed existing TLS")
	}
	provider.backendCA, capabilities = store, []string{octavia.GatewayCapabilityBackendTLSHostname}
	reconcile("configure", "mark", "enable", "member")
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
		for _, row := range rows {
			if row.Name == r.URL.Query().Get("name") {
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
	return services
}

func TestBackendTLSWeightedDenialPreservesHealthyShare(t *testing.T) {
	desired, actual := weightedPlanFixture()
	remote := &desired.routes[0].httpParts[1]
	observed := actual.routes[0].httpParts[remote.backendKey]
	remote.backendTLS = backendTLSPlan{phase: backendTLSDenied}
	observed.members = nil
	observed.pool.Tags = append(observed.pool.Tags, backendTLSManagedTag)
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
