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

package gateway

import (
	"context"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

type resolveHTTPSFunc func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason)

func (f resolveHTTPSFunc) Resolve(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	return f(ctx, gateway, listener)
}

func testHTTPSListener(name gatewayv1.SectionName, port gatewayv1.PortNumber) gatewayv1.Listener {
	return gatewayv1.Listener{Name: name, Port: port, Protocol: gatewayv1.HTTPSProtocolType,
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "certificate"}}}}
}

func TestHTTPSGatewayWithdrawsRevokedPortAndPreservesSiblings(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS = true
	gw := programmedTestGateway(cfg)
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 443), testHTTPSListener("second", 8443))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 443, 8443}
	provider := &recordingProvider{gatewayResult: &ready}
	var revoked bool
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			if revoked && listener.Port == 443 {
				return "", gatewayv1.ListenerReasonRefNotPermitted
			}
			return "approved-" + string(listener.Name), ""
		})}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)}
	if result, err := r.Reconcile(t.Context(), request); err != nil || !result.Requeue {
		t.Fatalf("enrollment = %v, %v", result, err)
	}
	if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter <= 0 || result.RequeueAfter > 5*time.Second {
		t.Fatalf("approved HTTPS reconcile = %v, %v", result, err)
	}
	spec := provider.gatewaySpecs[len(provider.gatewaySpecs)-1]
	if spec.ListenerProtocol != "HTTP" || spec.ListenerPort != 80 || len(spec.ScopedListeners) != 3 ||
		spec.ScopedListeners[0].TLS != nil || spec.ScopedListeners[1].TLS == nil ||
		spec.ScopedListeners[1].TLS.CertificateRef != "approved-https" || spec.ScopedListeners[2].TLS == nil {
		t.Fatalf("approved frontend graph = %#v", spec)
	}
	revoked = true
	ready.ScopedListenerReadyPorts = []int{80, 8443}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	spec = provider.gatewaySpecs[len(provider.gatewaySpecs)-1]
	if spec.ScopedListeners[1].TLS == nil || spec.ScopedListeners[1].TLS.CertificateRef != "" ||
		spec.ScopedListeners[2].TLS == nil || spec.ScopedListeners[2].TLS.CertificateRef != "approved-second" {
		t.Fatalf("revoked frontend graph = %#v", spec.ScopedListeners)
	}
	if err := kube.Get(t.Context(), request.NamespacedName, gw); err != nil {
		t.Fatal(err)
	}
	if controller.GatewayListenerScope(cfg, gw) != gatewayv1.HTTPProtocolType {
		t.Fatal("missing durable HTTP scope")
	}
	for _, listener := range gw.Status.Listeners {
		conditions := listener.Conditions
		if listener.Name == "https" {
			if !meta.IsStatusConditionFalse(conditions, string(gatewayv1.ListenerConditionProgrammed)) ||
				!slices.ContainsFunc(conditions, func(c metav1.Condition) bool {
					return c.Type == string(gatewayv1.ListenerConditionResolvedRefs) && c.Status == metav1.ConditionFalse && c.Reason == string(gatewayv1.ListenerReasonRefNotPermitted)
				}) {
				t.Fatalf("revoked listener conditions = %v", conditions)
			}
		} else if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.ListenerConditionProgrammed)) {
			t.Fatalf("sibling %s is not programmed: %v", listener.Name, conditions)
		}
	}
}

type httpsGraphProvider func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error)

func (f httpsGraphProvider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	return f(ctx, spec)
}

func TestHTTPSGatewayWithdrawsAuthorizationChangedDuringProviderCall(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS = true
	gw := programmedTestGateway(cfg)
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 443), testHTTPSListener("second", 8443))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class).Build()
	var revokeDuringWrite, revoked bool
	var operations []cloud.GraphDisposition
	provider := httpsGraphProvider(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		operations = append(operations, spec.Gateway.Disposition)
		if err := spec.ValidateMutation(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if spec.Gateway.Disposition == cloud.GraphWithdrawTLS {
			ports := spec.Gateway.Spec.ScopedListeners
			if len(spec.Routes) != 0 || len(ports) != 1 || ports[0].Port != 443 ||
				ports[0].TLS == nil || ports[0].TLS.CertificateRef != "" {
				t.Fatalf("withdrawal should only affect port 443: %#v", spec)
			}
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
		}
		if revokeDuringWrite {
			revoked = true
		}
		ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
		ready.ScopedListenerReadyPorts = []int{80, 443, 8443}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: ready}, nil
	})
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			if revoked && listener.Port == 443 {
				return "", gatewayv1.ListenerReasonRefNotPermitted
			}
			return "approved-" + string(listener.Name), ""
		})}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	revokeDuringWrite = true
	if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter <= 0 || result.RequeueAfter >= 5*time.Second {
		t.Fatalf("post-write revocation must trigger prompt reconciliation: %v, %v", result, err)
	}
	if !slices.Equal(operations, []cloud.GraphDisposition{cloud.GraphEnsure, cloud.GraphEnsure, cloud.GraphWithdrawTLS}) {
		t.Fatalf("withdrawal did not run under the Gateway writer: %v", operations)
	}
	if err := kube.Get(t.Context(), request.NamespacedName, gw); err != nil {
		t.Fatal(err)
	}
	for _, listener := range gw.Status.Listeners {
		conditions := listener.Conditions
		if listener.Name == "https" {
			if !meta.IsStatusConditionFalse(conditions, string(gatewayv1.ListenerConditionProgrammed)) ||
				!meta.IsStatusConditionFalse(conditions, string(gatewayv1.ListenerConditionResolvedRefs)) {
				t.Fatalf("stale proof remained in status: %v", conditions)
			}
		} else if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.ListenerConditionProgrammed)) {
			t.Fatalf("sibling %s lost readiness: %v", listener.Name, conditions)
		}
	}
}

func TestHTTPSGatewaySharedPortCannotProgramPlainOrTLS(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS = true
	gw := programmedTestGateway(cfg)
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 80), testHTTPSListener("one", 443), testHTTPSListener("two", 443), testHTTPSListener("dedicated", 8443))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{8443}
	provider := &recordingProvider{gatewayResult: &ready}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			if listener.Port != 8443 {
				t.Fatalf("conflicted listener reached certificate resolution: %s", listener.Name)
			}
			return "approved", ""
		})}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	ports := provider.gatewaySpecs[0].ScopedListeners
	if len(ports) != 3 || ports[0].Port != 80 || ports[0].TLS == nil || ports[0].TLS.CertificateRef != "" ||
		ports[1].Port != 443 || ports[1].TLS == nil || ports[1].TLS.CertificateRef != "" ||
		ports[2].Port != 8443 || ports[2].TLS == nil || ports[2].TLS.CertificateRef != "approved" {
		t.Fatalf("unexpected frontends: %#v", ports)
	}
	if err := kube.Get(t.Context(), request.NamespacedName, gw); err != nil {
		t.Fatal(err)
	}
	for _, listener := range gw.Status.Listeners {
		if listener.Name != "dedicated" && (!meta.IsStatusConditionTrue(listener.Conditions, string(gatewayv1.ListenerConditionConflicted)) ||
			!meta.IsStatusConditionFalse(listener.Conditions, string(gatewayv1.ListenerConditionProgrammed))) {
			t.Fatalf("shared section %s must be conflicted and unprogrammed: %#v", listener.Name, listener.Conditions)
		}
	}
}

func TestEnrolledHTTPSGatewayWithdrawsTLSDuringUnsupportedBundle(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS = true
	gw := programmedTestGateway(cfg)
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 443))
	gw.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.HTTPProtocolType)
	completeGatewayTopologyFixture(cfg, gw)
	controller.SetGatewayListenerScope(cfg, gw, gatewayv1.HTTPProtocolType)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	objects := append([]client.Object{gw, class}, unsupportedGatewayAPICRDs()...)
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(objects...).Build()
	var withdrew bool
	provider := httpsGraphProvider(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		if spec.Gateway.Disposition != cloud.GraphWithdrawTLS || spec.Gateway.Spec.ScopedListeners == nil ||
			len(spec.Gateway.Spec.ScopedListeners) != 0 || len(spec.Routes) != 0 {
			t.Fatalf("bundle skew must withdraw only owned TLS frontends: %#v", spec)
		}
		if err := spec.ValidateMutation(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		withdrew = true
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			t.Fatal("cannot resolve certificates against an unsupported bundle")
			return "", ""
		})}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if !withdrew {
		t.Fatal("no verified TLS withdrawal was requested")
	}
}
