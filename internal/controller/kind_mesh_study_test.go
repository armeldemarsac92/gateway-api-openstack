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
	"io/fs"
	"os"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	meshtests "sigs.k8s.io/gateway-api/conformance/tests/mesh"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

// TestKindStockMeshStudy is a negative control using the existing ingress
// reconcilers on disposable kind. Upstream assertions can fail by design.
func TestKindStockMeshStudy(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayAPIChannel = os.Getenv("GATEWAY_API_CHANNEL")
	options := suite.ConfigurableOptions{
		CleanupBaseResources: true,
		CleanupTestResources: true,
		DisableParallelTests: true,
	}
	selection := os.Getenv("HIBISCUS_MESH_STUDY")
	switch selection {
	case "":
		t.Skip("set HIBISCUS_MESH_STUDY to core, all, registered or MeshFrontend on a disposable kind cluster")
	case "core":
		options.ConformanceProfiles = []suite.ConformanceProfileName{
			suite.MeshHTTPConformanceProfileName,
			suite.MeshGRPCConformanceProfileName,
		}
		// v1.7.0 otherwise attempts feature inference before adding profile defaults.
		options.SupportedFeatures = sets.List(suite.MeshHTTPConformanceProfile.CoreFeatures.
			Union(suite.MeshGRPCConformanceProfile.CoreFeatures))
	case "all", "registered":
		options.ConformanceProfiles, options.SupportedFeatures = selectKindProfileFeatures(t, []suite.ConformanceProfile{
			suite.MeshHTTPConformanceProfile,
			suite.MeshGRPCConformanceProfile,
		}, cfg.GatewayAPIChannel)
		if selection == "registered" {
			// Some registered mesh cases require generic HTTPRoute features
			// outside the mesh profile tables in v1.7.0.
			selected := sets.New(options.SupportedFeatures...)
			for _, tc := range tests.ConformanceTests {
				if !slices.Contains(tc.Features, features.SupportMesh) {
					continue
				}
				for _, feature := range tc.Features {
					switch features.GetFeature(feature).Channel {
					case features.FeatureChannelStandard:
						selected.Insert(feature)
					case features.FeatureChannelExperimental:
						if cfg.GatewayAPIChannel == features.FeatureChannelExperimental {
							selected.Insert(feature)
						}
					default:
						t.Fatalf("unknown release channel for mesh feature %s", feature)
					}
				}
			}
			options.SupportedFeatures = sets.List(selected)
		}
		t.Logf("Selected upstream feature union: %v", options.SupportedFeatures)
	case meshtests.MeshFrontend.ShortName:
		options.RunTest = meshtests.MeshFrontend.ShortName
		options.SupportedFeatures = meshtests.MeshFrontend.Features
	default:
		t.Fatal("HIBISCUS_MESH_STUDY must be core, all, registered or MeshFrontend")
	}

	if cfg.GatewayAPIChannel != features.FeatureChannelStandard && cfg.GatewayAPIChannel != features.FeatureChannelExperimental {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	cfg.ClusterID = "kind-" + os.Getenv("KIND_CLUSTER_NAME")
	ctrl.SetLogger(zap.New())
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
	for _, name := range []string{suite.MeshNamespace, suite.MeshConsumerNamespace} {
		err := apiClient.Get(t.Context(), client.ObjectKey{Name: name}, &corev1.Namespace{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("upstream namespace %s must be absent before this study (lookup error: %v)", name, err)
		}
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
	provider := &recordingProvider{}
	t.Cleanup(func() {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		t.Logf("Recorded Gateway graph operations: %d", len(provider.graphSpecs))
	})
	startGatewayStatusReconcilers(t, manager, cfg, provider)
	startKindManager(t, manager)

	s, err := suite.NewConformanceTestSuite(suite.ConformanceOptions{
		ConfigurableOptions: options,
		Client:              apiClient,
		ClientOptions:       clientOptions,
		RestConfig:          restConfig,
		ManifestFS:          []fs.FS{&conformance.Manifests},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stock kind %s mesh study with upstream features %v", cfg.GatewayAPIChannel, s.SupportedFeatures)
	s.Setup(t, tests.ConformanceTests)
	t.Log("Upstream shared setup completed")
	if options.RunTest == "" {
		t.Cleanup(func() {
			report, err := s.Report()
			if err != nil {
				t.Fatal(err)
			}
			data, err := yaml.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Upstream profile results for the negative control:\n%s", data)
		})
	}
	if err := s.Run(t, tests.ConformanceTests); err != nil {
		t.Fatal(err)
	}
}
