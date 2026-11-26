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
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/certificates"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gatewayclass"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/httproute"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/l4route"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/listenerset"
)

const upstreamStatusGatewayClass = "upstream-status"

func startGatewayStatusReconcilers(t *testing.T, manager ctrl.Manager, cfg controller.Config, provider *recordingProvider) *graph.Writer {
	t.Helper()
	class := &gatewayclass.Reconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: cfg, TopologyResolver: provider}
	if err := class.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	gateway := newEnvtestGatewayReconciler(manager.GetClient(), manager.GetAPIReader(), provider, cfg)
	if cfg.BackendClientCertificate {
		const region = "RegionOne"
		path := filepath.Join(t.TempDir(), "backend-clients.json")
		snapshot := fmt.Appendf(nil, `{"version":1,"purpose":"backend-client","scope":{"cluster":%q,"gatewayClass":%q,"project":%q,"region":%q},"revisions":[]}`,
			cfg.ClusterID, upstreamStatusGatewayClass, cfg.OpenStackProjectID, region)
		if err := os.WriteFile(path, snapshot, 0o600); err != nil {
			t.Fatal(err)
		}
		gateway.BackendClientCertificates = &certificates.Resolver{
			BackendClientPath: path, ClusterID: cfg.ClusterID, ProjectID: cfg.OpenStackProjectID,
			Region: region, Endpoint: "https://barbican.example", Reader: manager.GetAPIReader(),
		}
	}
	if err := gateway.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := listenerset.SetupWithManager(t.Context(), manager, cfg.ControllerName); err != nil {
		t.Fatal(err)
	}
	writer := gateway.Writer.(*graph.Writer)
	route := writer.Routes.(*httproute.Reconciler)
	if cfg.MulticlusterServiceImports {
		route.BackendH2CAvailable = func(context.Context) (bool, error) { return true, nil }
	}
	if cfg.BackendTLSPolicy {
		route.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
		route.XBackendTLSAvailable = func(context.Context, bool, bool) (bool, error) { return true, nil }
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
func testUpstreamGatewayStatus(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider, certificateMap string) {
	t.Helper()
	const className = upstreamStatusGatewayClass
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
			GatewayMustHaveAddress: envtestWaitTimeout, HTTPRouteMustHaveCondition: envtestWaitTimeout,
			NamespacesMustBeReady: envtestWaitTimeout, ListenerSetMustHaveCondition: envtestWaitTimeout},
	}
	config.SetupTimeoutConfig(&s.TimeoutConfig)
	kubernetes.GWCMustHaveAcceptedConditionTrue(t, c, s.TimeoutConfig, className)
	// The original suite creates these certificate fixtures before running cases.
	t.Run(tests.GatewayInvalidFrontendClientCertificateValidation.ShortName, func(t *testing.T) {
		secret := kubernetes.MustCreateSelfSignedCertSecret(t, suite.InfrastructureNamespace, "tls-validity-checks-certificate", []string{"*", "*.org", "*.wildcard.org"})
		local, _, _ := kubernetes.MustCreateCACertConfigMap(t, suite.InfrastructureNamespace, "tls-validity-checks-ca-certificate")
		remote, _, _ := kubernetes.MustCreateCACertConfigMap(t, suite.WebBackendNamespace, "web-backend-cm")
		for _, obj := range []client.Object{secret, local, remote} {
			if err := c.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
		}
		enrollEnvtestServingCertificate(t, cfg, certificateMap, secret)
		tc := tests.GatewayInvalidFrontendClientCertificateValidation
		s.RunTest = tc.ShortName
		tc.Run(t, s)
	})
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
	t.Run("GatewayInfrastructureMetadata", func(t *testing.T) {
		testGatewayInfrastructure(t, c, provider, className)
	})
	t.Run("GatewayAddressRoutability", func(t *testing.T) {
		testGatewayAddressRoutability(t, c, cfg, provider, className)
	})
	if cfg.GatewayAddressRoutabilityClusterEnabled() {
		t.Run("GatewayAddressRoutabilityCluster", func(t *testing.T) {
			testGatewayClusterAddresses(t, c, provider, className)
		})
	}

	t.Run("ListenerSet admission", func(t *testing.T) {
		// Envtest has no scheduler. The original cases require a ready Pod
		// before checking Gateway and ListenerSet status.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: suite.InfrastructureNamespace, Name: "listenerset-status-fixture"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "fixture", Image: "unused"}}},
		}
		if err := c.Create(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
		pod.Status.Phase = corev1.PodSucceeded
		if err := c.Status().Update(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
		t.Run(tests.ListenerSetReferenceGrant.ShortName, func(t *testing.T) {
			testUpstreamListenerSetReferenceGrant(t, s, cfg, provider, certificateMap)
			t.Run("delegated frontend client validation", func(t *testing.T) {
				testDelegatedFrontendClientValidation(t, c, cfg, provider)
			})
		})
		for _, tc := range []suite.ConformanceTest{
			tests.ListenerSetDefaultNotAllowed, tests.ListenerSetAllowedNamespaceNone, tests.ListenerSetAllowedNamespaceSame,
			tests.ListenerSetAllowedNamespaceSelector, tests.ListenerSetHostnameConflict, tests.ListenerSetProtocolConflict,
			tests.ListenerSetAllowedRoutesSupportedKinds, tests.ListenerSetRouteStatusScopedToParentRef,
		} {
			t.Run(tc.ShortName, func(t *testing.T) {
				s.RunTest = tc.ShortName
				if tc.ShortName == tests.ListenerSetAllowedNamespaceSame.ShortName ||
					tc.ShortName == tests.ListenerSetAllowedNamespaceSelector.ShortName {
					// Envtest has no controller manager to finalize Namespaces.
					s.CleanupTestResources = false
					defer func() { s.CleanupTestResources = true }()
				}
				tc.Run(t, s)
			})
		}
	})
	t.Run("ListenerSet parent lifecycle", func(t *testing.T) {
		testListenerSetParentLifecycle(t, s, className, provider)
	})

	for _, tc := range []suite.ConformanceTest{
		tests.GatewayClassObservedGenerationBump,
		tests.GatewayInvalidRouteKind,
		tests.GatewayInvalidParametersRef,
		tests.GatewayInvalidTLSBackendConfiguration,
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

func testListenerSetParentLifecycle(t *testing.T, s *suite.ConformanceTestSuite, className string, provider *recordingProvider) {
	t.Helper()
	parent := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: suite.InfrastructureNamespace, Name: "listenerset-parent-lifecycle"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(className),
			Listeners:        []gatewayv1.Listener{{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}},
		},
	}
	listenerSet := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: suite.WebBackendNamespace, Name: "listenerset-parent-lifecycle"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Namespace: ptr.To(gatewayv1.Namespace(parent.Namespace)),
				Name:      gatewayv1.ObjectName(parent.Name),
			},
			Listeners: []gatewayv1.ListenerEntry{{
				Name: "delegated", Port: 8080, Protocol: gatewayv1.HTTPProtocolType,
				AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
					From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"listener-tenant": "allowed"}},
				}},
			}},
		},
	}
	key := client.ObjectKeyFromObject(listenerSet)
	check := func(status metav1.ConditionStatus, accepted, programmed gatewayv1.ListenerSetConditionReason) {
		t.Helper()
		kubernetes.ListenerSetMustHaveCondition(t, s.Client, s.TimeoutConfig, key, metav1.Condition{
			Type: string(gatewayv1.ListenerSetConditionAccepted), Status: status, Reason: string(accepted),
		})
		kubernetes.ListenerSetMustHaveCondition(t, s.Client, s.TimeoutConfig, key, metav1.Condition{
			Type: string(gatewayv1.ListenerSetConditionProgrammed), Status: status, Reason: string(programmed),
		})
	}
	deny := func() {
		t.Helper()
		check(metav1.ConditionFalse, gatewayv1.ListenerSetReasonNotAllowed, gatewayv1.ListenerSetReasonNotAllowed)
	}
	for _, obj := range []client.Object{listenerSet, parent} {
		if err := s.Client.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	deny()

	setPolicy := func(policy *gatewayv1.AllowedListeners) {
		t.Helper()
		if err := s.Client.Get(t.Context(), client.ObjectKeyFromObject(parent), parent); err != nil {
			t.Fatal(err)
		}
		base := parent.DeepCopy()
		parent.Spec.AllowedListeners = policy
		if err := s.Client.Patch(t.Context(), parent, client.MergeFrom(base)); err != nil {
			t.Fatal(err)
		}
	}
	checkDelegatedPort := func(present bool) {
		t.Helper()
		waitForEnvtest(t, func(context.Context) (bool, error) {
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, graph := range slices.Backward(provider.graphSpecs) {
				if graph.Identity.GatewayName != parent.Name || graph.Gateway.Disposition != cloud.GraphEnsure {
					continue
				}
				hasPort := func(port int) bool {
					return slices.ContainsFunc(graph.Gateway.Spec.ScopedListeners, func(listener cloud.ScopedListener) bool {
						return listener.Port == port
					})
				}
				return hasPort(80) && hasPort(8080) == present, nil
			}
			return false, nil
		})
	}
	setPolicy(&gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}})
	check(metav1.ConditionTrue, gatewayv1.ListenerSetReasonAccepted, gatewayv1.ListenerSetReasonProgrammed)
	checkDelegatedPort(true)
	kubernetes.GatewayMustHaveAttachedListeners(t, s.Client, s.TimeoutConfig, client.ObjectKeyFromObject(parent), 1)
	testListenerSetRouteNamespaceLifecycle(t, s.Client, listenerSet, parent.Namespace)
	setPolicy(&gatewayv1.AllowedListeners{})
	deny()
	checkDelegatedPort(false)
	kubernetes.GatewayMustHaveAttachedListeners(t, s.Client, s.TimeoutConfig, client.ObjectKeyFromObject(parent), 0)

	if err := s.Client.Get(t.Context(), key, listenerSet); err != nil {
		t.Fatal(err)
	}
	base := listenerSet.DeepCopy()
	listenerSet.Spec.Listeners[0].Name = "updated"
	if err := s.Client.Patch(t.Context(), listenerSet, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	deny()

	if err := s.Client.Delete(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	check(metav1.ConditionUnknown, gatewayv1.ListenerSetReasonPending, gatewayv1.ListenerSetReasonPending)

	if err := s.Client.Get(t.Context(), key, listenerSet); err != nil {
		t.Fatal(err)
	}
	base = listenerSet.DeepCopy()
	listenerSet.Spec.ParentRef.Namespace = nil
	if err := s.Client.Patch(t.Context(), listenerSet, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	local := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: listenerSet.Namespace, Name: parent.Name},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: parent.Spec.GatewayClassName,
			Listeners:        parent.Spec.Listeners,
		},
	}
	if err := s.Client.Create(t.Context(), local); err != nil {
		t.Fatal(err)
	}
	deny()
}

func testListenerSetRouteNamespaceLifecycle(t *testing.T, c client.Client, set *gatewayv1.ListenerSet, routeNamespace string) {
	t.Helper()
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: routeNamespace, Name: "listener-tenant"}, Spec: gatewayv1.HTTPRouteSpec{
		CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
			Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: gatewayv1.ObjectName(set.Name), Namespace: ptr.To(gatewayv1.Namespace(set.Namespace)),
		}}},
		Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: "infra-backend-v1", Port: ptr.To(gatewayv1.PortNumber(8080)),
		}}}}}},
	}}
	if err := c.Create(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	for _, allowed := range []bool{false, true, false, true} {
		team := "denied"
		expected, reason, count := metav1.ConditionFalse, gatewayv1.RouteReasonNotAllowedByListeners, int32(0)
		if allowed {
			team = "allowed"
			expected, reason, count = metav1.ConditionTrue, gatewayv1.RouteReasonAccepted, 1
		}
		var namespace corev1.Namespace
		if err := c.Get(t.Context(), client.ObjectKey{Name: routeNamespace}, &namespace); err != nil {
			t.Fatal(err)
		}
		base := namespace.DeepCopy()
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string)
		}
		namespace.Labels["listener-tenant"] = team
		if err := c.Patch(t.Context(), &namespace, client.MergeFrom(base)); err != nil {
			t.Fatal(err)
		}
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var current gatewayv1.HTTPRoute
			if err := c.Get(ctx, client.ObjectKeyFromObject(route), &current); err != nil {
				return false, err
			}
			var delegation gatewayv1.ListenerSet
			if err := c.Get(ctx, client.ObjectKeyFromObject(set), &delegation); err != nil {
				return false, err
			}
			if len(current.Status.Parents) != 1 || len(delegation.Status.Listeners) != 1 {
				return false, nil
			}
			accepted := meta.FindStatusCondition(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
			return accepted != nil && accepted.ObservedGeneration == route.Generation && accepted.Status == expected &&
				accepted.Reason == string(reason) && delegation.Status.Listeners[0].AttachedRoutes == count, nil
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
