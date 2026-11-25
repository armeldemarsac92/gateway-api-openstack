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

package controller

import (
	"net/netip"
	"testing"
	"time"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"
)

func TestExperimentalExternalHostnameConfigCanDisableAllClasses(t *testing.T) {
	cfg := Config{
		ControllerName: "example.net/gateway-api-openstack", ClusterID: "cluster-a", OpenStackResyncInterval: time.Minute,
		GatewayAPIChannel:               features.FeatureChannelExperimental,
		XBackendExternalHostnameClasses: []string{"edge"},
		XBackendExternalHostnameCIDRs:   []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")},
		XBackendServiceDNSDomains:       []string{"svc.example.test"},
	}
	if err := cfg.Validate(); err != nil || !cfg.RoutedXBackendEnabled("edge") {
		t.Fatalf("valid opt-in must permit edge, got %v", err)
	}
	cfg.XBackendExternalHostnameClasses = nil
	if err := cfg.Validate(); err != nil || cfg.RoutedXBackendEnabled("edge") {
		t.Fatalf("removing the last class must turn the feature off without rejecting the config, got %v", err)
	}
}

func TestConfigFinalizersUseControllerDomain(t *testing.T) {
	cfg := Config{
		ControllerName:          gatewayv1.GatewayController("example.net/gateway-api-openstack"),
		ClusterID:               "cluster-a",
		OpenStackResyncInterval: time.Minute,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := cfg.GatewayFinalizer(); got != "example.net/gateway-finalizer-"+cfg.controllerKey() {
		t.Fatalf("gatewayFinalizer() = %q", got)
	}
	if got := cfg.RouteFinalizer(); got != "example.net/httproute-finalizer-"+cfg.controllerKey() {
		t.Fatalf("routeFinalizer() = %q", got)
	}
	if got := cfg.RouteCleanupFailureAnnotation(); got != "example.net/httproute-cleanup-failure-"+cfg.controllerKey() {
		t.Fatalf("routeCleanupFailureAnnotation() = %q", got)
	}
	other := cfg
	other.ControllerName = "example.net/another-controller"
	if cfg.GatewayFinalizer() == other.GatewayFinalizer() || cfg.RouteGatewayUIDAnnotation() == other.RouteGatewayUIDAnnotation() {
		t.Fatal("controller-scoped metadata keys collide for controllers sharing a DNS domain")
	}
}

func TestConfigValidateRejectsUnsupportedOrAmbiguousValues(t *testing.T) {
	valid := Config{
		ControllerName:          "example.net/gateway-api-openstack",
		ClusterID:               "cluster-a",
		OpenStackResyncInterval: time.Minute,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "Cluster Gateway addresses without Experimental CRDs", mutate: func(cfg *Config) {
			cfg.GatewayInfrastructureMetadata, cfg.ClusterGatewayAddresses = true, true
		}},
		{name: "Cluster Gateway addresses without discovery permissions", mutate: func(cfg *Config) {
			cfg.GatewayAPIChannel = features.FeatureChannelExperimental
			cfg.ClusterGatewayAddresses = true
		}},
		{name: "HTTP query parameters without fixed responses", mutate: func(cfg *Config) { cfg.HTTPQueryParamExact = true }},
		{name: "HTTP method without fixed responses", mutate: func(cfg *Config) { cfg.HTTPMethodMatch = true }},
		{name: "HTTPRoute ExternalAuth without Experimental CRDs", mutate: func(cfg *Config) {
			cfg.RouteExternalAuth, cfg.FixedHTTPResponses = true, true
		}},
		{name: "HTTPRoute ExternalAuth without fixed responses", mutate: func(cfg *Config) {
			cfg.GatewayAPIChannel, cfg.RouteExternalAuth = features.FeatureChannelExperimental, true
		}},
		{name: "XBackend client identity in Standard", mutate: func(cfg *Config) {
			cfg.XBackendClientCertificate, cfg.BackendTLSPolicy = true, true
		}},
		{name: "XBackend client identity without server authentication", mutate: func(cfg *Config) {
			cfg.GatewayAPIChannel, cfg.XBackendClientCertificate = features.FeatureChannelExperimental, true
		}},
		{name: "SNI suffix without passthrough", mutate: func(cfg *Config) { cfg.TLSSNISuffix = true }},
		{name: "invalid controller name", mutate: func(cfg *Config) { cfg.ControllerName = "NOT_A_DOMAIN/controller" }},
		{name: "unknown Gateway API channel", mutate: func(cfg *Config) { cfg.GatewayAPIChannel = "nightly" }},
		{name: "missing cluster identity", mutate: func(cfg *Config) { cfg.ClusterID = "" }},
		{name: "disabled OpenStack resync", mutate: func(cfg *Config) { cfg.OpenStackResyncInterval = 0 }},
		{name: "negative OpenStack resync", mutate: func(cfg *Config) { cfg.OpenStackResyncInterval = -time.Minute }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestConfigAllowsXBackendClientIdentityIndependentlyOfGatewayIdentity(t *testing.T) {
	cfg := Config{
		ControllerName: "example.net/gateway-api-openstack", ClusterID: "cluster-a", OpenStackResyncInterval: time.Minute,
		GatewayAPIChannel: features.FeatureChannelExperimental, BackendTLSPolicy: true, XBackendClientCertificate: true,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
