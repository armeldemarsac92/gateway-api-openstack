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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestRequireGatewayAPIs(t *testing.T) {
	for _, tc := range []struct {
		name, missing  string
		experimental   bool
		tlsPassthrough bool
		tlsTerminate   bool
		backendTLS     bool
	}{
		{name: "standard"},
		{name: "missing referencegrants", missing: "referencegrants"},
		{name: "missing udproutes", missing: "udproutes"},
		{name: "backend TLS policy enabled", backendTLS: true},
		{name: "backend TLS policy CRD missing", backendTLS: true, missing: "backendtlspolicies"},
		{name: "passthrough watches backend TLS policies", tlsPassthrough: true},
		{name: "passthrough needs backend TLS policy inventory", tlsPassthrough: true, missing: "backendtlspolicies"},
		{name: "termination needs backend TLS policy inventory", tlsTerminate: true, missing: "backendtlspolicies"},
		{name: "experimental", experimental: true},
		{name: "missing xbackends", missing: "xbackends", experimental: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resources := metav1.APIResourceList{GroupVersion: gatewayv1.GroupVersion.String()}
			for _, resource := range []string{"gatewayclasses", "gateways", "httproutes", "tcproutes", "udproutes", "referencegrants", "backendtlspolicies", "tlsroutes"} {
				if resource != tc.missing {
					resources.APIResources = append(resources.APIResources, metav1.APIResource{Name: resource})
				}
			}
			experimental := metav1.APIResourceList{GroupVersion: gatewayx.GroupVersion.String()}
			if tc.missing != "xbackends" {
				experimental.APIResources = []metav1.APIResource{{Name: "xbackends"}}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				var response metav1.APIResourceList
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/apis/"+resources.GroupVersion:
					response = resources
				case tc.experimental && request.Method == http.MethodGet && request.URL.Path == "/apis/"+experimental.GroupVersion:
					response = experimental
				default:
					t.Errorf("unexpected discovery request: %s %s", request.Method, request.URL.Path)
					http.NotFound(w, request)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Errorf("encode discovery response: %v", err)
				}
			}))
			t.Cleanup(server.Close)
			configuration := controller.Config{BackendTLSPolicy: tc.backendTLS, TLSSNIPassthrough: tc.tlsPassthrough, TCPTLSTermination: tc.tlsTerminate}
			if tc.experimental {
				configuration.GatewayAPIChannel = "experimental"
			}
			err := requireGatewayAPIs(&rest.Config{Host: server.URL}, configuration)
			if tc.missing == "" && err != nil {
				t.Fatalf("requireGatewayAPIs() rejected available resources: %v", err)
			}
			if tc.missing != "" && (err == nil || !strings.Contains(err.Error(), tc.missing)) {
				t.Fatalf("requireGatewayAPIs() = %v, want missing %s resource", err, tc.missing)
			}
		})
	}
}

func TestConfigureOpenStackRequestMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	first := openstack.ClientConfig{}
	if err := configureOpenStackRequestMetrics(&first, registry); err != nil {
		t.Fatalf("configureOpenStackRequestMetrics() error = %v", err)
	}
	if first.RequestObserver == nil {
		t.Fatal("configureOpenStackRequestMetrics() did not configure an observer")
	}

	second := openstack.ClientConfig{}
	if err := configureOpenStackRequestMetrics(&second, registry); err != nil {
		t.Fatalf("repeated configureOpenStackRequestMetrics() error = %v", err)
	}
	if second.RequestObserver == nil {
		t.Fatal("repeated configureOpenStackRequestMetrics() did not configure an observer")
	}
}

func TestDurationEnvOr(t *testing.T) {
	const name = "GATEWAY_OPENSTACK_TEST_DURATION"

	t.Run("fallback", func(t *testing.T) {
		t.Setenv(name, "")
		got, err := durationEnvOr(name, time.Minute)
		if err != nil || got != time.Minute {
			t.Fatalf("durationEnvOr() = %s, %v, want 1m, nil", got, err)
		}
	})

	t.Run("environment", func(t *testing.T) {
		t.Setenv(name, "90s")
		got, err := durationEnvOr(name, time.Minute)
		if err != nil || got != 90*time.Second {
			t.Fatalf("durationEnvOr() = %s, %v, want 90s, nil", got, err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		t.Setenv(name, "later")
		if _, err := durationEnvOr(name, time.Minute); err == nil {
			t.Fatal("durationEnvOr() unexpectedly accepted an invalid duration")
		}
	})
}

func TestFloat64EnvOr(t *testing.T) {
	const name = "GATEWAY_OPENSTACK_TEST_QPS"

	t.Setenv(name, "")
	if got, err := float64EnvOr(name, 10); err != nil || got != 10 {
		t.Fatalf("float64EnvOr() = %v, %v, want 10, nil", got, err)
	}
	t.Setenv(name, "12.5")
	if got, err := float64EnvOr(name, 10); err != nil || got != 12.5 {
		t.Fatalf("float64EnvOr() = %v, %v, want 12.5, nil", got, err)
	}
	t.Setenv(name, "many")
	if _, err := float64EnvOr(name, 10); err == nil {
		t.Fatal("float64EnvOr() unexpectedly accepted an invalid number")
	}
}

func TestIntEnvOr(t *testing.T) {
	const name = "GATEWAY_OPENSTACK_TEST_BURST"

	t.Setenv(name, "")
	if got, err := intEnvOr(name, 20); err != nil || got != 20 {
		t.Fatalf("intEnvOr() = %d, %v, want 20, nil", got, err)
	}
	t.Setenv(name, "30")
	if got, err := intEnvOr(name, 20); err != nil || got != 30 {
		t.Fatalf("intEnvOr() = %d, %v, want 30, nil", got, err)
	}
	t.Setenv(name, "many")
	if _, err := intEnvOr(name, 20); err == nil {
		t.Fatal("intEnvOr() unexpectedly accepted an invalid integer")
	}
}
