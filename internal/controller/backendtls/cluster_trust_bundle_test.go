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

package backendtls

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestNormalizeClusterTrustBundle(t *testing.T) {
	first, second := testCertificate(t, true), testCertificate(t, true)
	together, err := normalizeTrustBundle([]byte(first + second))
	require.NoError(t, err)
	reordered, err := normalizeTrustBundle([]byte("\n" + second + "\n" + first))
	require.NoError(t, err)
	require.Equal(t, together, reordered)

	for _, want := range []string{first, second} {
		cert, _ := pem.Decode([]byte(want))
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(together))
		root, err := x509.ParseCertificate(cert.Bytes)
		require.NoError(t, err)
		_, err = root.Verify(x509.VerifyOptions{Roots: pool})
		require.NoError(t, err)
	}

	_, err = normalizeTrustBundle([]byte(first + testCertificate(t, false)))
	require.ErrorContains(t, err, "must contain an X.509 CA certificate")
}

func TestClusterTrustBundleWaitsForFreshDiscovery(t *testing.T) {
	var available atomic.Bool
	listStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api", "/apis":
			w.Header().Set("Content-Type", "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
			list := apidiscoveryv2.APIGroupDiscoveryList{TypeMeta: metav1.TypeMeta{APIVersion: "apidiscovery.k8s.io/v2", Kind: "APIGroupDiscoveryList"}}
			if r.URL.Path == "/apis" {
				version := apidiscoveryv2.APIVersionDiscovery{Version: "v1beta1", Freshness: apidiscoveryv2.DiscoveryFreshnessStale}
				if available.Load() {
					version.Freshness = apidiscoveryv2.DiscoveryFreshnessCurrent
					version.Resources = []apidiscoveryv2.APIResourceDiscovery{{
						Resource: "clustertrustbundles", Scope: apidiscoveryv2.ScopeCluster,
						ResponseKind: &metav1.GroupVersionKind{Group: "certificates.k8s.io", Version: "v1beta1", Kind: "ClusterTrustBundle"},
					}}
				}
				list.Items = []apidiscoveryv2.APIGroupDiscovery{{
					ObjectMeta: metav1.ObjectMeta{Name: "certificates.k8s.io"}, Versions: []apidiscoveryv2.APIVersionDiscovery{version},
				}}
			}
			if err := json.NewEncoder(w).Encode(list); err != nil {
				t.Error(err)
			}
		case "/apis/certificates.k8s.io/v1beta1/clustertrustbundles":
			select {
			case listStarted <- struct{}{}:
			default:
			}
			http.Error(w, "forbidden", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	bundles, err := NewClusterTrustBundles(&rest.Config{Host: server.URL}, server.Client())
	require.NoError(t, err)

	_, err = bundles.discoverResource(t.Context())
	require.ErrorContains(t, err, "stale")

	result := make(chan error, 1)
	go func() { result <- bundles.Start(t.Context()) }()
	require.Eventually(t, func() bool {
		observation, err := bundles.Read(t.Context(), "catalog-ca")
		return observation.Invalid == nil && err != nil && strings.Contains(err.Error(), "stale")
	}, time.Second, 10*time.Millisecond)
	available.Store(true)
	select {
	case <-listStarted:
	case <-time.After(8 * time.Second):
		t.Fatal("fresh discovery did not start the optional watch")
	}
	t.Cleanup(func() { require.NoError(t, <-result) })
}
