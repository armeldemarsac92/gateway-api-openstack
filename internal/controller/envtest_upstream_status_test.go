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
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubeyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/yaml"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gatewayclass"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/httproute"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/l4route"
)

func startGatewayStatusReconcilers(t *testing.T, manager ctrl.Manager, cfg controller.Config, provider *recordingProvider) *graph.Writer {
	t.Helper()
	class := &gatewayclass.Reconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: cfg, TopologyResolver: provider}
	if err := class.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	gateway := newEnvtestGatewayReconciler(manager.GetClient(), manager.GetAPIReader(), provider, cfg)
	if err := gateway.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	writer := gateway.Writer.(*graph.Writer)
	route := writer.Routes.(*httproute.Reconciler)
	if cfg.BackendTLSPolicy {
		route.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
	}
	tcp := &l4route.Reconciler{Protocol: gatewayv1.TCPProtocolType, Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: cfg, Writer: writer, Coordinator: writer.Coordinator}
	udp := &l4route.Reconciler{Protocol: gatewayv1.UDPProtocolType, Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: cfg, Writer: writer, Coordinator: writer.Coordinator}
	writer.Routes = graph.RouteSources{route, tcp, udp}
	if err := tcp.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := udp.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := route.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	return writer
}

// These upstream cases exercise only the API server and controller watches.
// Upstream profile setup also needs ready workloads and is run separately.
func testUpstreamGatewayStatus(t *testing.T, c client.Client, cfg controller.Config) {
	t.Helper()
	const className = "upstream-status"
	parameters := envtestClassParameters(className)
	// The upstream backend Services use ClusterIP.
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: className},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: cfg.ControllerName,
			ParametersRef: &gatewayv1.ParametersReference{Group: configv1alpha1.GroupName,
				Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name},
		},
	}
	for _, obj := range []client.Object{parameters, class} {
		if err := c.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	createUpstreamStatusBase(t, c, className)
	s := &suite.ConformanceTestSuite{
		ControllerName: string(cfg.ControllerName),
		Client:         c, DisableParallelTests: true, CleanupTestResources: true,
		Applier: kubernetes.Applier{GatewayClass: className, ControllerName: string(cfg.ControllerName), ManifestFS: []fs.FS{&conformance.Manifests}},
		TimeoutConfig: config.TimeoutConfig{GWCMustBeAccepted: envtestWaitTimeout, LatestObservedGenerationSet: envtestWaitTimeout,
			GatewayMustHaveCondition: envtestWaitTimeout, GatewayStatusMustHaveListeners: envtestWaitTimeout,
			GatewayMustHaveAddress: envtestWaitTimeout, HTTPRouteMustHaveCondition: envtestWaitTimeout},
	}
	config.SetupTimeoutConfig(&s.TimeoutConfig)
	kubernetes.GWCMustHaveAcceptedConditionTrue(t, c, s.TimeoutConfig, className)
	// The v1.7.0 suite Applier strips this fixture's address while processing
	// static address placeholders. Apply it directly to preserve the request.
	// Envtest exercises Gateway status without the upstream workload wait.
	t.Run(tests.GatewayOptionalAddressValue.ShortName+" Gateway status", func(t *testing.T) {
		manifest, err := fs.ReadFile(&conformance.Manifests, tests.GatewayOptionalAddressValue.Manifests[0])
		if err != nil {
			t.Fatal(err)
		}
		var gateway gatewayv1.Gateway
		if err := yaml.Unmarshal(manifest, &gateway); err != nil {
			t.Fatal(err)
		}
		gateway.Spec.GatewayClassName = gatewayv1.ObjectName(className)
		if err := c.Create(t.Context(), &gateway); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := c.Delete(t.Context(), &gateway); err != nil {
				t.Error(err)
			}
		}()
		key := client.ObjectKeyFromObject(&gateway)
		var persisted gatewayv1.Gateway
		if err := c.Get(t.Context(), key, &persisted); err != nil {
			t.Fatal(err)
		}
		addresses := persisted.Spec.Addresses
		if len(addresses) != 1 || ptr.Deref(addresses[0].Type, "") != gatewayv1.IPAddressType || addresses[0].Value != "" {
			t.Fatalf("Gateway address request = %#v, want an IPAddress with no value", addresses)
		}
		kubernetes.GatewayMustHaveLatestConditions(t, c, s.TimeoutConfig, key)
		kubernetes.GatewayMustHaveCondition(t, c, s.TimeoutConfig, key, metav1.Condition{
			Type: string(gatewayv1.GatewayConditionAccepted), Status: metav1.ConditionTrue, Reason: string(gatewayv1.GatewayReasonAccepted),
		})
		kubernetes.GatewayMustHaveCondition(t, c, s.TimeoutConfig, key, metav1.Condition{
			Type: string(gatewayv1.GatewayConditionProgrammed), Status: metav1.ConditionTrue, Reason: string(gatewayv1.GatewayReasonProgrammed),
		})
		address, err := kubernetes.WaitForGatewayAddress(t, c, s.TimeoutConfig, kubernetes.NewGatewayRef(key, "http"))
		if err != nil || address != "192.0.2.10:8080" {
			t.Fatalf("Gateway address = %q, error = %v, want 192.0.2.10:8080", address, err)
		}
	})

	t.Run(tests.GatewayStaticAddresses.ShortName, func(t *testing.T) {
		s.RunTest = tests.GatewayStaticAddresses.ShortName
		s.Applier.UsableNetworkAddresses = []gatewayv1.GatewaySpecAddress{
			{Type: ptr.To(gatewayv1.IPAddressType), Value: recordingProviderStaticVIP},
		}
		s.Applier.UnusableNetworkAddresses = []gatewayv1.GatewaySpecAddress{
			{Type: ptr.To(gatewayv1.IPAddressType), Value: "198.51.100.10"},
		}
		tests.GatewayStaticAddresses.Run(t, s)
	})

	for _, tc := range []suite.ConformanceTest{
		tests.GatewayClassObservedGenerationBump,
		tests.GatewayInvalidRouteKind,
		tests.GatewayInvalidParametersRef,
		tests.GatewayListenerUnsupportedProtocol,
		tests.GatewayNameMaximumLength,
		tests.HTTPRouteInvalidCrossNamespaceParentRef,
		tests.HTTPRouteInvalidParentRefNotMatchingListenerPort,
		tests.HTTPRouteInvalidParentRefNotMatchingSectionName,
		tests.HTTPRouteInvalidParentRefSectionNameNotMatchingPort,
		tests.HTTPRoutePreserveForeignStatus,
	} {
		t.Run(tc.ShortName, func(t *testing.T) {
			s.RunTest = tc.ShortName
			tc.Run(t, s)
		})
	}
}

// Use the upstream namespaces, same-namespace Gateway and referenced backend
// Services directly from the published module.
func createUpstreamStatusBase(t *testing.T, c client.Client, className string) {
	t.Helper()
	file, err := conformance.Manifests.Open("base/manifests.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	decoder := kubeyaml.NewYAMLOrJSONDecoder(file, 4096)
	for {
		var obj unstructured.Unstructured
		if err := decoder.Decode(&obj); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		switch {
		case obj.GetKind() == "Namespace" && (obj.GetName() == suite.InfrastructureNamespace || obj.GetName() == suite.WebBackendNamespace):
		case obj.GetKind() == "Service" && obj.GetNamespace() == suite.InfrastructureNamespace && obj.GetName() == "infra-backend-v1":
		case obj.GetKind() == "Service" && obj.GetNamespace() == suite.WebBackendNamespace && obj.GetName() == "web-backend":
		case obj.GetKind() == "Gateway" && obj.GetNamespace() == suite.InfrastructureNamespace && obj.GetName() == "same-namespace":
			if err := unstructured.SetNestedField(obj.Object, className, "spec", "gatewayClassName"); err != nil {
				t.Fatal(err)
			}
		default:
			continue
		}
		if err := c.Create(t.Context(), &obj); err != nil {
			t.Fatalf("create upstream %s %s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
}
