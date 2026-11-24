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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackgraph "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/certificates"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

const (
	envtestCertificateRegion     = "RegionOne"
	envtestServingSecretID       = "11111111-1111-4111-8111-111111111111"
	envtestServingCertificateRef = "https://barbican.example/v1/secrets/" + envtestServingSecretID
)

func enableUpstreamListenerSetHTTPS(t *testing.T, writer *graph.Writer, cfg controller.Config, certificateMap string) {
	gateway := writer.Gateway.(*gatewaycontroller.Reconciler)
	gateway.Certificates = &certificates.Resolver{
		Path: certificateMap, ClusterID: cfg.ClusterID, ProjectID: cfg.OpenStackProjectID,
		Region: envtestCertificateRegion, Endpoint: "https://barbican.example", Reader: gateway.APIReader,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/lbaas/providers/amphora/gateway_capabilities" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"gateway_capabilities":[%q]}`, octavia.GatewayCapabilityHTTPSListenerIsolation)
	}))
	t.Cleanup(server.Close)
	provider := openstackgraph.NewProvider(openstackgraph.ServiceClients{
		LoadBalancer: &gophercloud.ServiceClient{ProviderClient: &gophercloud.ProviderClient{}, Endpoint: server.URL + "/"},
	}, openstackgraph.ProviderConfig{HTTPSListenerIsolation: true})
	gateway.HTTPSListenerIsolationAvailable = provider.HTTPSListenerIsolationAvailable
}

func testUpstreamListenerSetReferenceGrant(t *testing.T, s *suite.ConformanceTestSuite, cfg controller.Config, provider *recordingProvider, certificateMap string) {
	t.Helper()
	// The published suite normally creates this Secret before the case runs.
	secret := kubernetes.MustCreateSelfSignedCertSecret(t, suite.WebBackendNamespace, "certificate", []string{"*"})
	if err := s.Client.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	// Envtest simulates Barbican. The actual controller still checks the
	// Kubernetes grant, the Secret metadata, and the enrollment scope/revision.
	snapshot := fmt.Appendf(nil, `{"version":1,"scope":{"cluster":%q,"gatewayClass":%q,"project":%q,"region":%q},"revisions":[{"namespace":%q,"name":%q,"uid":%q,"resourceVersion":%q,"barbicanSecretID":%q,"chainSHA256":[%q],"notBefore":"2020-01-01T00:00:00Z","notAfter":"2050-01-01T00:00:00Z"}]}`,
		cfg.ClusterID, upstreamStatusGatewayClass, cfg.OpenStackProjectID, envtestCertificateRegion,
		secret.Namespace, secret.Name, secret.UID, secret.ResourceVersion, envtestServingSecretID, strings.Repeat("a", 64))
	if err := os.WriteFile(certificateMap, snapshot, 0o444); err != nil {
		t.Fatal(err)
	}

	upstream := tests.ListenerSetReferenceGrant
	s.RunTest = upstream.ShortName
	s.CleanupTestResources = false
	defer func() { s.CleanupTestResources = true }()
	upstream.Run(t, s)

	const gatewayName = "gateway-with-listener-sets-test-reference-grant"
	const delegatedName = "listenerset-with-reference-grant"
	const gatewayHost = "gateway-listener.com"
	const delegatedHost = "listenerset-with-reference-grant-listener.com"
	const delegatedSection = "listenerset-with-reference-grant-listener"
	gwKey := client.ObjectKey{Namespace: suite.InfrastructureNamespace, Name: gatewayName}
	setKey := client.ObjectKey{Namespace: suite.InfrastructureNamespace, Name: delegatedName}
	gatewayRoute := envtestHTTPRoute(suite.InfrastructureNamespace, "certificate-retained", gatewayName, "infra-backend-v1")
	gatewayRoute.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("gateway-listener"))
	delegatedRoute := envtestHTTPRoute(suite.WebBackendNamespace, "certificate-delegated", delegatedName, "web-backend")
	delegatedRoute.Spec.ParentRefs[0].Kind = ptr.To(gatewayv1.Kind("ListenerSet"))
	delegatedRoute.Spec.ParentRefs[0].Namespace = ptr.To(gatewayv1.Namespace(suite.InfrastructureNamespace))
	delegatedRoute.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName(delegatedSection))
	for _, route := range []*gatewayv1.HTTPRoute{gatewayRoute, delegatedRoute} {
		route.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(8080))
		if err := s.Client.Create(t.Context(), route); err != nil {
			t.Fatal(err)
		}
	}

	var initial gatewayv1.Gateway
	if err := s.Client.Get(t.Context(), gwKey, &initial); err != nil {
		t.Fatal(err)
	}
	check := func(gatewayAllowed, delegatedAllowed bool) {
		t.Helper()
		last := ""
		defer func() {
			if t.Failed() && last != "" {
				t.Logf("Gateway grant active=%v, ListenerSet grant active=%v, last observation: %s", gatewayAllowed, delegatedAllowed, last)
			}
		}()
		status := metav1.ConditionFalse
		refReason := string(gatewayv1.ListenerEntryReasonRefNotPermitted)
		setReason := string(gatewayv1.ListenerSetReasonListenersNotValid)
		var hosts []string
		if gatewayAllowed {
			hosts = append(hosts, gatewayHost)
		}
		if delegatedAllowed {
			status, refReason, setReason = metav1.ConditionTrue, string(gatewayv1.ListenerReasonResolvedRefs), string(gatewayv1.ListenerSetReasonProgrammed)
			hosts = append(hosts, delegatedHost)
		}
		gatewayStatus := metav1.ConditionFalse
		gatewayRefReason := string(gatewayv1.ListenerReasonRefNotPermitted)
		if gatewayAllowed {
			gatewayStatus, gatewayRefReason = metav1.ConditionTrue, string(gatewayv1.ListenerReasonResolvedRefs)
		}
		kubernetes.ListenerSetMustHaveCondition(t, s.Client, s.TimeoutConfig, setKey, metav1.Condition{
			Type: string(gatewayv1.ListenerSetConditionProgrammed), Status: status, Reason: setReason,
		})
		kubernetes.ListenerSetListenersMustHaveConditions(t, s.Client, s.TimeoutConfig, setKey, []metav1.Condition{{
			Type: string(gatewayv1.ListenerConditionResolvedRefs), Status: status, Reason: refReason,
		}}, delegatedSection)
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var gateway gatewayv1.Gateway
			if err := s.Client.Get(ctx, gwKey, &gateway); err != nil {
				return false, err
			}
			last = fmt.Sprintf("gateway listener statuses %v", gateway.Status.Listeners)
			if gateway.UID != initial.UID || gateway.Annotations[cfg.GatewayListenerPortAnnotation()] != "443" ||
				gateway.Annotations[cfg.GatewayHTTPSIsolatedPortsAnnotation()] != "443" || len(gateway.Status.Listeners) != 1 ||
				!meta.IsStatusConditionPresentAndEqual(gateway.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed), gatewayStatus) {
				return false, nil
			}
			refs := meta.FindStatusCondition(gateway.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
			if refs == nil || refs.Status != gatewayStatus || refs.Reason != gatewayRefReason {
				return false, nil
			}
			for _, route := range []*gatewayv1.HTTPRoute{gatewayRoute, delegatedRoute} {
				var current gatewayv1.HTTPRoute
				if err := s.Client.Get(ctx, client.ObjectKeyFromObject(route), &current); err != nil {
					return false, err
				}
				expected := status
				if route == gatewayRoute {
					expected = gatewayStatus
				}
				last = fmt.Sprintf("route %s parents %+v", route.Name, current.Status.Parents)
				if len(current.Status.Parents) != 1 ||
					current.Status.Parents[0].ControllerName != cfg.ControllerName ||
					!equality.Semantic.DeepEqual(current.Status.Parents[0].ParentRef, current.Spec.ParentRefs[0]) ||
					!meta.IsStatusConditionPresentAndEqual(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted), expected) {
					return false, nil
				}
				if expected == metav1.ConditionTrue && !meta.IsStatusConditionTrue(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)) {
					return false, nil
				}
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, spec := range slices.Backward(provider.graphSpecs) {
				if spec.Identity.GatewayName != gatewayName || spec.Gateway.Disposition != cloud.GraphEnsure {
					continue
				}
				listeners := spec.Gateway.Spec.ScopedListeners
				last = fmt.Sprintf("planned listeners %+v", listeners)
				if len(listeners) == 1 && listeners[0].TLS != nil {
					last = fmt.Sprintf("planned TLS hosts %v", listeners[0].TLS.VirtualHosts)
				}
				if len(listeners) != 1 || listeners[0].Port != 443 || listeners[0].TLS == nil ||
					listeners[0].TLS.CertificateRef != envtestServingCertificateRef || !slices.Equal(listeners[0].TLS.VirtualHosts, hosts) {
					return false, nil
				}
				routeInPlan := func(name, hostname string) bool {
					return slices.ContainsFunc(spec.Routes, func(node cloud.RouteGraphNode) bool {
						return node.Disposition == cloud.GraphEnsure && node.Spec.Identity.RouteName == name &&
							node.Spec.HTTPSListenerHostname == hostname && slices.Equal(node.Spec.ScopedListenerPorts, []int{443})
					})
				}
				gatewayRoutePresent, delegatedRoutePresent := routeInPlan(gatewayRoute.Name, gatewayHost), routeInPlan(delegatedRoute.Name, delegatedHost)
				last = fmt.Sprintf("gateway route in plan=%v, delegated route in plan=%v; nodes %+v", gatewayRoutePresent, delegatedRoutePresent, spec.Routes)
				return gatewayRoutePresent == gatewayAllowed && delegatedRoutePresent == delegatedAllowed, nil
			}
			return false, nil
		})
		t.Logf("Gateway grant=%v, ListenerSet grant=%v: listener status, scoped route parents and provider hosts %v agree", gatewayAllowed, delegatedAllowed, hosts)
	}
	check(true, true)
	for _, name := range []string{"reference-grant-for-listener-set", "reference-grant-for-gateway"} {
		var grant gatewayv1.ReferenceGrant
		if err := s.Client.Get(t.Context(), client.ObjectKey{Namespace: suite.WebBackendNamespace, Name: name}, &grant); err != nil {
			t.Fatal(err)
		}
		if err := s.Client.Delete(t.Context(), &grant); err != nil {
			t.Fatal(err)
		}
		check(name != "reference-grant-for-gateway", name != "reference-grant-for-listener-set")
		restored := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: grant.Namespace, Name: grant.Name}, Spec: grant.Spec}
		if err := s.Client.Create(t.Context(), restored); err != nil {
			t.Fatal(err)
		}
		check(true, true)
	}
}

func testDelegatedFrontendClientValidation(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	const gatewayName = "delegated-client-validation"
	const listenerName = "secure"
	const host = "delegated-mtls.example.test"
	backendNamespace := suite.WebBackendNamespace
	gatewayNamespace := suite.InfrastructureNamespace
	ca := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: backendNamespace, Name: gatewayName},
		Data:       map[string]string{"ca.crt": backendTLSTestCA(t)},
	}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: backendNamespace, Name: gatewayName},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: gatewayv1.Namespace(gatewayNamespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "ConfigMap", Name: ptr.To(gatewayv1.ObjectName(ca.Name))}},
		},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gatewayNamespace, Name: gatewayName},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: upstreamStatusGatewayClass,
			Listeners:        []gatewayv1.Listener{{Name: "plain", Port: 80, Protocol: gatewayv1.HTTPProtocolType}},
			AllowedListeners: &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}},
			TLS: &gatewayv1.GatewayTLSConfig{Frontend: &gatewayv1.FrontendTLSConfig{
				Default: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
					CACertificateRefs: []gatewayv1.ObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name), Namespace: ptr.To(gatewayv1.Namespace(ca.Namespace))}},
				}},
			}},
		},
	}
	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: backendNamespace, Name: gatewayName},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: gatewayName, Namespace: ptr.To(gatewayv1.Namespace(gatewayNamespace))},
			Listeners: []gatewayv1.ListenerEntry{{
				Name: listenerName, Port: 443, Protocol: gatewayv1.HTTPSProtocolType, Hostname: ptr.To(gatewayv1.Hostname(host)),
				TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "certificate"}}},
			}},
		},
	}
	route := envtestHTTPRoute(backendNamespace, gatewayName, set.Name, "web-backend")
	route.Spec.ParentRefs[0].Kind = ptr.To(gatewayv1.Kind("ListenerSet"))
	route.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName(listenerName))
	route.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(8080))
	for _, object := range []client.Object{ca, grant, gateway, set, route} {
		if err := c.Create(t.Context(), object); err != nil {
			t.Fatalf("create %T: %v", object, err)
		}
	}

	check := func(accepted bool, refReason gatewayv1.ListenerConditionReason) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var current gatewayv1.ListenerSet
			if err := c.Get(ctx, client.ObjectKeyFromObject(set), &current); err != nil {
				return false, err
			}
			if len(current.Status.Listeners) != 1 {
				return false, nil
			}
			listener := current.Status.Listeners[0]
			refs := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
			admitted := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionAccepted))
			if refs == nil || refs.Reason != string(refReason) || admitted == nil ||
				(meta.IsStatusConditionTrue(listener.Conditions, string(gatewayv1.ListenerConditionProgrammed)) != accepted) ||
				(!accepted && (admitted.Reason != string(gatewayv1.ListenerReasonNoValidCACertificate) || refs.Status != metav1.ConditionFalse)) {
				return false, nil
			}
			var currentRoute gatewayv1.HTTPRoute
			if err := c.Get(ctx, client.ObjectKeyFromObject(route), &currentRoute); err != nil {
				return false, err
			}
			if len(currentRoute.Status.Parents) != 1 || currentRoute.Status.Parents[0].ControllerName != cfg.ControllerName ||
				meta.IsStatusConditionTrue(currentRoute.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) != accepted {
				return false, nil
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, spec := range slices.Backward(provider.graphSpecs) {
				if spec.Identity.GatewayName != gatewayName || spec.Gateway.Disposition != cloud.GraphEnsure {
					continue
				}
				hasRoute := slices.ContainsFunc(spec.Routes, func(node cloud.RouteGraphNode) bool {
					return node.Disposition == cloud.GraphEnsure && node.Spec.Identity.RouteName == route.Name &&
						node.Spec.HTTPSListenerHostname == host
				})
				return hasRoute == accepted && slices.ContainsFunc(spec.Gateway.Spec.ScopedListeners, func(listener cloud.ScopedListener) bool {
					if listener.Port != 443 || listener.TLS == nil {
						return false
					}
					if !accepted {
						return listener.TLS.CertificateRef == "" && len(listener.TLS.VirtualHosts) == 0
					}
					validation := listener.TLS.ClientValidation
					return listener.TLS.CertificateRef == envtestServingCertificateRef &&
						slices.Equal(listener.TLS.VirtualHosts, []string{host}) && validation != nil &&
						validation.ConfigMapUID == string(ca.UID) && string(validation.PEM) == ca.Data["ca.crt"]
				}), nil
			}
			return false, nil
		})
		t.Logf("delegated-only 443 with CA status %s: listener, route and provider TLS intent agree", refReason)
	}
	check(true, gatewayv1.ListenerReasonResolvedRefs)
	if err := c.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	check(false, gatewayv1.ListenerReasonRefNotPermitted)
	if err := c.Create(t.Context(), &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: grant.Namespace, Name: grant.Name}, Spec: grant.Spec,
	}); err != nil {
		t.Fatal(err)
	}
	check(true, gatewayv1.ListenerReasonResolvedRefs)
	trustedPEM := ca.Data["ca.crt"]
	ca.Data["ca.crt"] = "invalid CA"
	if err := c.Update(t.Context(), ca); err != nil {
		t.Fatal(err)
	}
	check(false, gatewayv1.ListenerReasonInvalidCACertificateRef)
	ca.Data["ca.crt"] = trustedPEM
	if err := c.Update(t.Context(), ca); err != nil {
		t.Fatal(err)
	}
	check(true, gatewayv1.ListenerReasonResolvedRefs)
}
