//go:build envtest

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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gatewayclass"
)

func startGatewayStatusReconcilers(t *testing.T, manager ctrl.Manager, cfg controller.Config) {
	t.Helper()
	provider := &recordingProvider{}
	class := &gatewayclass.Reconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: cfg, TopologyResolver: provider}
	if err := class.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	gateway := newEnvtestGatewayReconciler(manager.GetClient(), manager.GetAPIReader(), provider, cfg)
	if err := gateway.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
}

// These upstream cases exercise only the API server and controller watches.
// Upstream profile setup also needs ready workloads and is run separately.
func testUpstreamGatewayStatus(t *testing.T, c client.Client, cfg controller.Config) {
	t.Helper()
	const className = "upstream-status"
	parameters := envtestClassParameters(className)
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: className},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: cfg.ControllerName,
			ParametersRef: &gatewayv1.ParametersReference{Group: configv1alpha1.GroupName,
				Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name},
		},
	}
	for _, obj := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: suite.InfrastructureNamespace}}, parameters, class} {
		if err := c.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	s := &suite.ConformanceTestSuite{
		Client: c, DisableParallelTests: true, CleanupTestResources: true,
		Applier: kubernetes.Applier{GatewayClass: className, ControllerName: string(cfg.ControllerName), ManifestFS: []fs.FS{&conformance.Manifests}},
		TimeoutConfig: config.TimeoutConfig{GWCMustBeAccepted: envtestWaitTimeout, LatestObservedGenerationSet: envtestWaitTimeout,
			GatewayMustHaveCondition: envtestWaitTimeout, GatewayStatusMustHaveListeners: envtestWaitTimeout},
	}
	config.SetupTimeoutConfig(&s.TimeoutConfig)
	kubernetes.GWCMustHaveAcceptedConditionTrue(t, c, s.TimeoutConfig, className)
	for _, tc := range []suite.ConformanceTest{tests.GatewayClassObservedGenerationBump, tests.GatewayInvalidRouteKind, tests.GatewayInvalidParametersRef, tests.GatewayListenerUnsupportedProtocol} {
		t.Run(tc.ShortName, func(t *testing.T) {
			s.RunTest = tc.ShortName
			tc.Run(t, s)
		})
	}
}
