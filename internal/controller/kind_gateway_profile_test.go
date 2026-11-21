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
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

// TestKindUpstreamGatewayProfile attempts one upstream profile with the full
// shared setup and a recording cloud adapter.
func TestKindUpstreamGatewayProfile(t *testing.T) {
	var profile suite.ConformanceProfile
	switch suite.ConformanceProfileName(os.Getenv("HIBISCUS_GATEWAY_PROFILE")) {
	case "":
		t.Skip("set HIBISCUS_GATEWAY_PROFILE to GATEWAY-GRPC or GATEWAY-TLS on a disposable kind cluster")
	case suite.GatewayGRPCConformanceProfileName:
		profile = suite.GatewayGRPCConformanceProfile
	case suite.GatewayTLSConformanceProfileName:
		profile = suite.GatewayTLSConformanceProfile
	default:
		t.Fatal("HIBISCUS_GATEWAY_PROFILE must be GATEWAY-GRPC or GATEWAY-TLS")
	}

	cfg := testConfig()
	cfg.GatewayAPIChannel = os.Getenv("GATEWAY_API_CHANNEL")
	if cfg.GatewayAPIChannel != features.FeatureChannelStandard && cfg.GatewayAPIChannel != features.FeatureChannelExperimental {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	cfg.ClusterID = "kind-" + os.Getenv("KIND_CLUSTER_NAME")
	extended := sets.New[features.FeatureName]()
	for feature := range profile.ExtendedFeatures {
		switch features.GetFeature(feature).Channel {
		case features.FeatureChannelStandard:
			extended.Insert(feature)
		case features.FeatureChannelExperimental:
			if cfg.GatewayAPIChannel == features.FeatureChannelExperimental {
				extended.Insert(feature)
			}
		default:
			t.Fatalf("unknown release channel for profile feature %s", feature)
		}
	}
	t.Logf("Attempting %s on %s with the original upstream base and recording cloud adapter", profile.Name, cfg.GatewayAPIChannel)
	t.Logf("Selected upstream Core features: %v", sets.List(profile.CoreFeatures))
	t.Logf("Selected upstream Extended features: %v", sets.List(extended))

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
	if err := checkGatewaySuiteNamespaces(t.Context(), apiClient); err != nil {
		t.Fatal(err)
	}
	const className = "kind-upstream-gateway-profile"
	parameters := envtestClassParameters(className)
	resources := []client.Object{parameters, kindGatewayClass(cfg, parameters)}
	for _, resource := range resources {
		err := apiClient.Get(t.Context(), client.ObjectKeyFromObject(resource), resource.DeepCopyObject().(client.Object))
		if !apierrors.IsNotFound(err) {
			t.Fatalf("%T %s must be absent before this attempt (lookup error: %v)", resource, resource.GetName(), err)
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
	startGatewayStatusReconcilers(t, manager, cfg, &recordingProvider{})
	startKindManager(t, manager)

	s, err := suite.NewConformanceTestSuite(suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName:     className,
			ConformanceProfiles:  []suite.ConformanceProfileName{profile.Name},
			SupportedFeatures:    sets.List(profile.CoreFeatures.Union(extended)),
			Mode:                 "kind-with-recording-cloud",
			CleanupBaseResources: true,
			CleanupTestResources: true,
			DisableParallelTests: true,
		},
		Client:        apiClient,
		ClientOptions: clientOptions,
		RestConfig:    restConfig,
		ManifestFS:    []fs.FS{&conformance.Manifests},
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Applier.MustApplyObjectsWithCleanup(t, apiClient, s.TimeoutConfig, resources, true)
	s.Setup(t, tests.ConformanceTests)
	t.Log("Upstream shared setup completed")
	t.Cleanup(func() {
		report, err := s.Report()
		if err != nil {
			t.Error(err)
			return
		}
		data, err := yaml.Marshal(report)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("Upstream profile results for the recording cloud adapter:\n%s", data)
	})
	if err := s.Run(t, tests.ConformanceTests); err != nil {
		t.Fatal(err)
	}
}
