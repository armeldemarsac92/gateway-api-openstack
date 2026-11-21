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
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestKindAmbientMesh(t *testing.T) {
	waypointPath := os.Getenv("HIBISCUS_AMBIENT_WAYPOINT_MANIFEST")
	if waypointPath == "" {
		t.Skip("set HIBISCUS_AMBIENT_WAYPOINT_MANIFEST to the operator's waypoint.yaml on disposable kind")
	}

	cfg := testConfig()
	cfg.GatewayAPIChannel = os.Getenv("GATEWAY_API_CHANNEL")
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
	var waypointClass gatewayv1.GatewayClass
	if err := apiClient.Get(t.Context(), client.ObjectKey{Name: "istio-waypoint"}, &waypointClass); err != nil {
		t.Fatal(err)
	}
	if waypointClass.Spec.ControllerName != "istio.io/mesh-controller" {
		t.Fatalf("unexpected waypoint controller %q", waypointClass.Spec.ControllerName)
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
		if count := len(provider.graphSpecs); count != 0 {
			t.Errorf("the ingress reconcilers wrote %d cloud graphs for mesh traffic", count)
		}
		t.Logf("Recorded Gateway graph operations: %d", len(provider.graphSpecs))
	})
	startGatewayStatusReconcilers(t, manager, cfg, provider)
	startKindManager(t, manager)

	meshManifest := "ambient-mesh-fixture.yaml"
	fixture, waypointName := ambientMeshFixture(t, waypointPath)
	manifestFS := fstest.MapFS{meshManifest: {Data: fixture}}
	s, err := suite.NewConformanceTestSuite(suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			CleanupBaseResources: true,
			CleanupTestResources: true,
			DisableParallelTests: true,
			NamespaceLabels:      map[string]string{"istio.io/dataplane-mode": "ambient"},
			ConformanceProfiles: []suite.ConformanceProfileName{
				suite.MeshHTTPConformanceProfileName, suite.MeshGRPCConformanceProfileName,
			},
			// v1.7.0 attempts feature inference before it applies profile defaults.
			SupportedFeatures: sets.List(suite.MeshHTTPConformanceProfile.CoreFeatures.
				Union(suite.MeshGRPCConformanceProfile.CoreFeatures)),
		},
		Client:        apiClient,
		ClientOptions: clientOptions,
		RestConfig:    restConfig,
		ManifestFS:    []fs.FS{manifestFS, &conformance.Manifests},
		MeshManifests: meshManifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		report, err := s.Report()
		if err != nil {
			t.Fatal(err)
		}
		data, err := yaml.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Upstream core profile results with Istio ambient:\n%s", data)
	})
	s.Setup(t, tests.ConformanceTests)
	waypointKey := client.ObjectKey{Namespace: suite.MeshNamespace, Name: waypointName}
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		var waypoint gatewayv1.Gateway
		if err := apiClient.Get(ctx, waypointKey, &waypoint); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(waypoint.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		return condition != nil && condition.Status == "True" && condition.ObservedGeneration == waypoint.Generation, nil
	}); err != nil {
		t.Fatalf("fixture waypoint did not become Programmed: %v", err)
	}
	if err := s.Run(t, tests.ConformanceTests); err != nil {
		t.Fatal(err)
	}
}

func ambientMeshFixture(t *testing.T, waypointPath string) ([]byte, string) {
	t.Helper()
	data, err := os.ReadFile(waypointPath)
	if err != nil {
		t.Fatal(err)
	}
	var waypoint gatewayv1.Gateway
	if err := yaml.Unmarshal(data, &waypoint); err != nil {
		t.Fatal(err)
	}
	waypoint.Namespace = suite.MeshNamespace
	data, err = yaml.Marshal(waypoint)
	if err != nil {
		t.Fatal(err)
	}

	fixture, err := conformance.Manifests.Open("mesh/manifests.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	decoder := utilyaml.NewYAMLOrJSONDecoder(fixture, 4096)
	var buf bytes.Buffer
	matchedService := false
	for {
		var obj unstructured.Unstructured
		err := decoder.Decode(&obj)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if obj.Object == nil {
			continue
		}
		if obj.GetKind() == "Service" && obj.GetNamespace() == suite.MeshNamespace && obj.GetName() == "echo" {
			labels := obj.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["istio.io/use-waypoint"] = waypoint.Name
			obj.SetLabels(labels)
			matchedService = true
		}
		doc, err := yaml.Marshal(obj.Object)
		if err != nil {
			t.Fatal(err)
		}
		buf.WriteString("---\n")
		buf.Write(doc)
	}
	if !matchedService {
		t.Fatal("upstream mesh fixture is missing Service echo")
	}
	buf.WriteString("---\n")
	buf.Write(data)
	return buf.Bytes(), waypoint.Name
}
