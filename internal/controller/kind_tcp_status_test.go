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
	"io/fs"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestKindUpstreamTCPStatus(t *testing.T) {
	runKindUpstreamStatus(t, "kind-upstream-status", "standard", features.SupportTCPRoute, []suite.ConformanceTest{
		tests.TCPRouteInvalidNonTCPListener,
		tests.TCPRouteInvalidCrossNamespaceBackendRef,
		tests.TCPRouteInvalidBackendRefNonexistent,
	})
}

func TestKindUpstreamUDPStatus(t *testing.T) {
	s := runKindUpstreamStatus(t, "kind-upstream-udp-status", os.Getenv("GATEWAY_API_CHANNEL"), features.SupportUDPRoute, []suite.ConformanceTest{
		tests.UDPRouteInvalidCrossNamespaceBackendRef,
		tests.UDPRouteInvalidBackendRefNonexistent,
	})

	// The upstream listener-rejection case requires a programmed TLS Gateway.
	// Check rejection on the existing HTTP Gateway with the same status helpers.
	t.Run("UDPRouteRejectedByHTTPListener", func(t *testing.T) {
		gateway := client.ObjectKey{Namespace: suite.InfrastructureNamespace, Name: "same-namespace"}
		route := &gatewayv1.UDPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "udp-route-http-listener"},
			Spec: gatewayv1.UDPRouteSpec{
				CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
					Name: gatewayv1.ObjectName(gateway.Name), SectionName: ptr.To(gatewayv1.SectionName("http")),
				}}},
				Rules: []gatewayv1.UDPRouteRule{{BackendRefs: []gatewayv1.BackendRef{{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Name: "udp-echo", Port: ptr.To(gatewayv1.PortNumber(8080)),
					},
				}}}},
			},
		}
		s.Applier.MustApplyObjectsWithCleanup(t, s.Client, s.TimeoutConfig, []client.Object{route}, true)
		kubernetes.UDPRouteMustHaveCondition(t, s.Client, s.TimeoutConfig, client.ObjectKeyFromObject(route), gateway, metav1.Condition{
			Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionFalse,
			Reason: string(gatewayv1.RouteReasonNotAllowedByListeners),
		})
		kubernetes.GatewayMustHaveZeroRoutes(t, s.Client, s.TimeoutConfig, gateway)
	})
}

func runKindUpstreamStatus(t *testing.T, className, channel string, routeFeature features.FeatureName, cases []suite.ConformanceTest) *suite.ConformanceTestSuite {
	t.Helper()
	kubeconfig := os.Getenv("HIBISCUS_TEST_KUBECONFIG")
	if kubeconfig == "" {
		t.Fatal("HIBISCUS_TEST_KUBECONFIG must point to the disposable cluster kubeconfig")
	}
	name := os.Getenv("KIND_CLUSTER_NAME")
	if name == "" {
		t.Fatal("KIND_CLUSTER_NAME must identify the disposable kind cluster")
	}
	cluster := "kind-" + name
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: cluster},
	).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	scheme := testScheme(t)
	clientOptions := client.Options{Scheme: scheme}
	apiClient, err := client.New(restConfig, clientOptions)
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.GatewayAPIChannel = channel
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
	startGatewayStatusReconcilers(t, manager, cfg, &recordingProvider{})

	managerContext, stopManager := context.WithCancel(t.Context())
	managerResult := make(chan error, 1)
	go func() { managerResult <- manager.Start(managerContext) }()
	t.Cleanup(func() {
		stopManager()
		select {
		case err := <-managerResult:
			if err != nil {
				t.Errorf("manager stopped with error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("manager did not stop")
		}
	})
	cacheContext, cancelCacheSync := context.WithTimeout(managerContext, 30*time.Second)
	defer cancelCacheSync()
	if !manager.GetCache().WaitForCacheSync(cacheContext) {
		t.Fatal("manager cache did not sync")
	}

	parameters := envtestClassParameters(className)
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: className},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: cfg.ControllerName,
			ParametersRef: &gatewayv1.ParametersReference{
				Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
			},
		},
	}
	for _, object := range []client.Object{parameters, class} {
		if err := apiClient.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}

	manifestFS := []fs.FS{&conformance.Manifests}
	baseManifests := "base/manifests.yaml"
	if statusBase := os.Getenv("HIBISCUS_STATUS_BASE_DIR"); statusBase != "" {
		t.Log("Using original base documents with the unrelated HTTPS Gateway omitted")
		manifestFS = append(manifestFS, os.DirFS(statusBase))
		baseManifests = "tcp-status-base.yaml"
	}
	s, err := suite.NewConformanceTestSuite(suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName:     className,
			CleanupTestResources: true,
			DisableParallelTests: true,
			SupportedFeatures: []features.FeatureName{
				features.SupportGateway, routeFeature, features.SupportReferenceGrant,
			},
			TimeoutConfig: config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second},
		},
		Client:        apiClient,
		ClientOptions: clientOptions,
		RestConfig:    restConfig,
		BaseManifests: baseManifests,
		ManifestFS:    manifestFS,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Setup(t, cases)
	if err := s.Run(t, cases); err != nil {
		t.Fatal(err)
	}
	return s
}
