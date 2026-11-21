//go:build envtest && kindtest

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

package controller_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

var kindUDPCases = []suite.ConformanceTest{
	tests.UDPRouteTest,
	tests.UDPRouteReferenceGrant,
	tests.UDPRouteMultipleRoutesAttachment,
	tests.UDPRouteInvalidCrossNamespaceBackendRef,
	tests.UDPRouteInvalidBackendRefNonexistent,
	tests.UDPRouteParentRefAttachAllListeners,
	tests.UDPRouteParentRefPortAndSectionName,
	tests.UDPRouteWeightedRouting,
	tests.UDPRouteNotAllowedByListeners,
}

func kindUDPCaseSelection(selector string) ([]suite.ConformanceTest, error) {
	switch selector {
	case "":
		return kindUDPCases[:5], nil
	case "all":
		return kindUDPCases, nil
	case "profile":
		return tests.ConformanceTests, nil
	}
	var selected []suite.ConformanceTest
	seen := make(map[string]bool)
	for name := range strings.SplitSeq(selector, ",") {
		name = strings.TrimSpace(name)
		match := func(test suite.ConformanceTest) bool { return test.ShortName == name }
		index := slices.IndexFunc(kindUDPCases, match)
		if index < 0 || seen[name] {
			return nil, fmt.Errorf("HIBISCUS_UDP_CASES contains an unknown or repeated UDP case %q", name)
		}
		selected = append(selected, kindUDPCases[index])
		seen[name] = true
	}
	return selected, nil
}

// TestKindUpstreamUDPTraffic runs upstream cases from a guest that can reach
// the VIPs rendered by the stock Octavia fixture and the kind Pod network.
func TestKindUpstreamUDPTraffic(t *testing.T) {
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if channel != "standard" && channel != "experimental" {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	selection := os.Getenv("HIBISCUS_UDP_CASES")
	cases, err := kindUDPCaseSelection(selection)
	if err != nil {
		t.Fatal(err)
	}
	literalProfile := selection == "profile"
	if literalProfile && os.Getenv("HIBISCUS_STATUS_BASE_DIR") != "" {
		t.Fatal("HIBISCUS_UDP_CASES=profile requires the original upstream base")
	}
	baseManifests, manifestFS := kindConformanceManifests(t)

	const className = "kind-upstream-udp-traffic"
	cfg := testConfig()
	cfg.GatewayAPIChannel = channel
	cfg.OpenStackProjectID = requireKindUDPEnv(t, "HIBISCUS_UDP_PROJECT")
	cfg.ClusterID = "kind-" + requireKindUDPEnv(t, "KIND_CLUSTER_NAME")
	parameters := envtestClassParameters(className)
	parameters.Spec.VIPSubnet.ID = requireKindUDPEnv(t, "HIBISCUS_UDP_VIP_SUBNET")
	parameters.Spec.MemberSubnet.ID = requireKindUDPEnv(t, "HIBISCUS_UDP_MEMBER_SUBNET")
	// The upstream Services are ClusterIP. Reconcile their actual Pod endpoints.
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint

	restConfig := kindUpstreamRESTConfig(t)
	scheme := testScheme(t)
	clientOptions := client.Options{Scheme: scheme}
	apiClient, err := client.New(restConfig, clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateInstalledGatewayAPIVersion(t.Context(), apiClient); err != nil {
		t.Fatal(err)
	}
	manager, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetupIndexes(t.Context(), manager.GetFieldIndexer(), cfg); err != nil {
		t.Fatal(err)
	}
	startKindUDPReconcilers(t, manager, cfg)
	startKindManager(t, manager)

	for _, object := range []client.Object{parameters, kindGatewayClass(cfg, parameters)} {
		if err := apiClient.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.IgnoreNotFound(apiClient.Delete(ctx, object)); err != nil {
				t.Errorf("delete %T %s: %v", object, object.GetName(), err)
			}
		})
	}

	options := suite.ConfigurableOptions{
		GatewayClassName:     className,
		CleanupBaseResources: true,
		CleanupTestResources: true,
		DisableParallelTests: true,
		SupportedFeatures:    []features.FeatureName{features.SupportGateway, features.SupportUDPRoute, features.SupportReferenceGrant},
		TimeoutConfig:        config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second},
		// The upstream case returns without assertions unless TLSRoute is supported.
		SkipTests: []string{tests.UDPRouteNotAllowedByListeners.ShortName},
	}
	if literalProfile {
		options.ConformanceProfiles = []suite.ConformanceProfileName{suite.GatewayUDPConformanceProfileName}
		t.Logf("Upstream GATEWAY-UDP profile attempt on %s with the original upstream base", channel)
	} else if baseManifests == "base/manifests.yaml" {
		t.Logf("Selected upstream UDP cases on %s with the original upstream base", channel)
	} else {
		t.Logf("Focused upstream UDP cases on %s with the HTTPS Gateway omitted from the shared base", channel)
	}
	t.Log("UDPRouteNotAllowedByListeners requires TLSRoute and is excluded when selected")
	s, err := suite.NewConformanceTestSuite(suite.ConformanceOptions{
		ConfigurableOptions: options,
		Client:              apiClient,
		ClientOptions:       clientOptions,
		RestConfig:          restConfig,
		BaseManifests:       baseManifests,
		ManifestFS:          manifestFS,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Setup(t, cases)
	if err := s.Run(t, cases); err != nil {
		t.Fatal(err)
	}
}

func requireKindUDPEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s must identify the disposable fixture resources", name)
	}
	return value
}

func startKindUDPReconcilers(t *testing.T, manager ctrl.Manager, cfg controller.Config) {
	t.Helper()
	endpoint, err := netip.ParseAddrPort(os.Getenv("HIBISCUS_UDP_FIXTURE"))
	if err != nil || !endpoint.Addr().IsLoopback() || endpoint.Port() == 0 {
		t.Fatal("HIBISCUS_UDP_FIXTURE must identify the stock Octavia fixture on loopback")
	}
	network := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/floatingips" {
			t.Errorf("unexpected synthetic Neutron request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"floatingips":[]}`)
	}))
	t.Cleanup(network.Close)
	sdk := &gophercloud.ProviderClient{HTTPClient: http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
	octaviaEndpoint := "http://" + endpoint.String() + "/"
	provider := openstack.NewProvider(openstack.ServiceClients{
		LoadBalancer: &gophercloud.ServiceClient{ProviderClient: sdk, Endpoint: octaviaEndpoint, ResourceBase: octaviaEndpoint + "v2/"},
		Network:      &gophercloud.ServiceClient{ProviderClient: sdk, Endpoint: network.URL + "/"},
		ProjectID:    cfg.OpenStackProjectID,
	}, openstack.ProviderConfig{OperationTimeout: 30 * time.Second, PollInterval: 250 * time.Millisecond})

	// Use fixed synthetic topology and send all graph operations to stock Octavia.
	writer := startGatewayStatusReconcilers(t, manager, cfg, &recordingProvider{})
	writer.Provider = provider
}

func TestKindUDPCaseSelection(t *testing.T) {
	selected, err := kindUDPCaseSelection("")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range selected {
		names = append(names, c.ShortName)
	}
	want := []string{
		"UDPRoute", "UDPRouteReferenceGrant", "UDPRouteMultipleRoutesAttachment",
		"UDPRouteInvalidCrossNamespaceBackendRef", "UDPRouteInvalidBackendRefNonexistent",
	}
	if !slices.Equal(names, want) {
		t.Fatalf("default cases = %v, want %v", names, want)
	}
	selected, err = kindUDPCaseSelection("all")
	if err != nil || len(selected) != 9 {
		t.Fatalf("all selected %d cases with error %v, want nine", len(selected), err)
	}
	selected, err = kindUDPCaseSelection("UDPRouteWeightedRouting, UDPRouteParentRefPortAndSectionName")
	if err != nil || len(selected) != 2 || selected[0].ShortName != "UDPRouteWeightedRouting" ||
		selected[1].ShortName != "UDPRouteParentRefPortAndSectionName" {
		t.Fatalf("explicit selection = %v, error = %v", selected, err)
	}
	for _, input := range []string{"TCPRoute", "UDPRoute,", "UDPRoute,UDPRoute"} {
		if _, err := kindUDPCaseSelection(input); err == nil {
			t.Errorf("selector %q was accepted", input)
		}
	}
}
