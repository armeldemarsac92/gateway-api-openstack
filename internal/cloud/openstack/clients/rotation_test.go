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

package clients

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestNewServiceClientsReadsRotatedCredentialAndCAAtStartup(t *testing.T) {
	// Make the selected clouds.yaml entry the only source of TLS inputs.
	t.Setenv("OS_CACERT", "")
	t.Setenv("OS_CERT", "")
	t.Setenv("OS_KEY", "")
	t.Setenv("OS_INTERFACE", "")

	type applicationCredential struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	type keystoneConfig struct {
		credential applicationCredential
		tls        *tls.Config
	}
	initialTLS, initialCA := newKeystoneTLS(t, "initial test CA")
	replacementTLS, replacementCA := newKeystoneTLS(t, "replacement test CA")
	initial := applicationCredential{ID: "initial-credential", Secret: rand.Text()}
	replacement := applicationCredential{ID: "replacement-credential", Secret: rand.Text()}
	const project = "test-project"
	const region = "test-region"
	const token = "keystone-test-token"

	var active atomic.Pointer[keystoneConfig]
	active.Store(&keystoneConfig{credential: initial, tls: initialTLS})
	var authRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v3/auth/tokens", func(w http.ResponseWriter, r *http.Request) {
		authRequests.Add(1)
		var body struct {
			Auth struct {
				Identity struct {
					Credential applicationCredential `json:"application_credential"`
				} `json:"identity"`
			} `json:"auth"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if body.Auth.Identity.Credential != active.Load().credential {
			http.Error(w, "credential denied", http.StatusUnauthorized)
			return
		}
		w.Header().Set("X-Subject-Token", token)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if _, err := fmt.Fprintf(w, `{"token":{"project":{"id":%q},"catalog":[
			{"type":"load-balancer","endpoints":[{"interface":"public","region":%q,"url":%q}]},
			{"type":"network","endpoints":[{"interface":"public","region":%q,"url":%q}]}
		]}}`, project, region, "https://"+r.Host+"/octavia/", region, "https://"+r.Host+"/neutron/"); err != nil {
			t.Error("writing the Keystone service catalog failed")
		}
	})
	discoverVersion := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[{"id":"v2.0","status":"CURRENT"}]}`)
	}
	mux.HandleFunc("GET /octavia/{$}", discoverVersion)
	mux.HandleFunc("GET /neutron/{$}", discoverVersion)
	server := httptest.NewUnstartedServer(mux)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = initialTLS.Clone()
	server.TLS.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return active.Load().tls, nil
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	dir := t.TempDir()
	cloudsPath := filepath.Join(dir, "clouds.yaml")
	caPath := filepath.Join(dir, "ca.pem")
	writeInput := func(path string, contents []byte) {
		t.Helper()
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCloud := func(credential applicationCredential) {
		t.Helper()
		writeInput(cloudsPath, fmt.Appendf(nil, `clouds:
  openstack:
    auth_type: v3applicationcredential
    auth:
      auth_url: %q
      application_credential_id: %q
      application_credential_secret: %q
    verify: true
    cacert: %q
`, server.URL+"/v3", credential.ID, credential.Secret, caPath))
	}
	cfg := ClientConfig{
		CloudsYAMLPath: cloudsPath, CloudName: "openstack", Region: region,
		Microversion: "2.5", APIQPS: DefaultAPIQPS, APIBurst: DefaultAPIBurst,
	}
	assertStartup := func(stage string) {
		t.Helper()
		clients, err := NewServiceClients(t.Context(), cfg)
		if err != nil {
			message := strings.NewReplacer(initial.Secret, "[redacted]", replacement.Secret, "[redacted]", token, "[redacted]").Replace(err.Error())
			t.Fatalf("%s: OpenStack client startup failed: %s", stage, message)
		}
		if clients.ProjectID != project || clients.Region != region ||
			clients.LoadBalancer.Endpoint != server.URL+"/octavia/" ||
			clients.Network.Endpoint != server.URL+"/neutron/" {
			t.Fatalf("%s: startup did not discover the expected project and endpoints", stage)
		}
	}

	writeInput(caPath, initialCA)
	writeCloud(initial)
	assertStartup("initial")

	active.Store(&keystoneConfig{credential: replacement, tls: replacementTLS})
	writeCloud(replacement)
	var unknownAuthority x509.UnknownAuthorityError
	if _, err := NewServiceClients(t.Context(), cfg); !errors.As(err, &unknownAuthority) {
		t.Fatalf("startup with retired CA: want certificate trust failure, got %T", err)
	}
	if authRequests.Load() != 1 {
		t.Fatal("startup with retired CA sent a credential before verifying the server")
	}

	writeInput(caPath, replacementCA)
	assertStartup("replacement")
	if authRequests.Load() != 2 {
		t.Fatal("replacement startup should authenticate once with the new credential")
	}

	writeCloud(initial)
	if _, err := NewServiceClients(t.Context(), cfg); !errors.Is(err, cloud.ErrAuthentication) {
		t.Fatalf("startup with retired credential: want authentication failure, got %T", err)
	}
}

func newKeystoneTLS(t *testing.T, caName string) (*tls.Config, []byte) {
	t.Helper()
	caPublicKey, caPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{
		Subject:   pkix.Name{CommonName: caName},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true, IsCA: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublicKey, caPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	serverPublicKey, serverPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := &x509.Certificate{
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, serverPublicKey, caPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverDER}, PrivateKey: serverPrivateKey}},
	}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
