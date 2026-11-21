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
	"errors"
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

func (f resolveHTTPSFunc) ReferenceStatus(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	_, reason := f(ctx, gateway, listener)
	return reason
}

type testHTTPSReferenceResolver struct {
	resolveHTTPSFunc
	referenceStatus func(*gatewayv1.Listener) gatewayv1.ListenerConditionReason
}

func (r testHTTPSReferenceResolver) ReferenceStatus(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	return r.referenceStatus(listener)
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
			if len(spec.Routes) != 0 || len(ports) != 2 || ports[0].Port != 443 ||
				ports[0].TLS == nil || ports[0].TLS.CertificateRef != "" || ports[1].Port != 8443 ||
				ports[1].TLS == nil || ports[1].TLS.CertificateRef != "approved-second" || ports[1].Name == "" {
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
	certificateNamespace := gatewayv1.Namespace("certificates")
	granted := testHTTPSListener("granted", 443)
	granted.TLS.CertificateRefs[0].Namespace = &certificateNamespace
	ungranted := testHTTPSListener("ungranted", 443)
	ungranted.TLS.CertificateRefs[0].Namespace = &certificateNamespace
	ungranted.TLS.CertificateRefs[0].Name = "ungranted"
	// A separate unsupported listener request filter must not hide certificate authorization.
	ungranted.Filters = &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{{
		Type:         gatewayv1.ListenerFilterExtensionRef,
		ExtensionRef: &gatewayv1.LocalObjectReference{Group: "example.net", Kind: "Policy", Name: "authorization"},
	}}}
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 80), granted, ungranted, testHTTPSListener("dedicated", 8443))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{8443}
	provider := &recordingProvider{gatewayResult: &ready}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: testHTTPSReferenceResolver{
			resolveHTTPSFunc: func(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
				if listener.Port != 8443 {
					t.Fatalf("conflicted listener reached provider reference resolution: %s", listener.Name)
				}
				return "approved", ""
			},
			referenceStatus: func(listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
				if listener.TLS.CertificateRefs[0].Name == "ungranted" {
					return gatewayv1.ListenerReasonRefNotPermitted
				}
				return ""
			},
		}}, provider)
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
	for _, spec := range gw.Spec.Listeners {
		index := slices.IndexFunc(gw.Status.Listeners, func(s gatewayv1.ListenerStatus) bool { return s.Name == spec.Name })
		if index < 0 {
			t.Fatalf("missing status for listener %s", spec.Name)
		}
		listener := gw.Status.Listeners[index]
		if listener.Name == "dedicated" {
			continue
		}
		conditions := listener.Conditions
		if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.ListenerConditionConflicted)) ||
			!meta.IsStatusConditionFalse(conditions, string(gatewayv1.ListenerConditionAccepted)) ||
			!meta.IsStatusConditionFalse(conditions, string(gatewayv1.ListenerConditionProgrammed)) {
			t.Fatalf("shared section %s must be conflicted, unaccepted and unprogrammed: %v", listener.Name, conditions)
		}
		refs := meta.FindStatusCondition(conditions, string(gatewayv1.ListenerConditionResolvedRefs))
		switch listener.Name {
		case granted.Name:
			if refs == nil || refs.Status != metav1.ConditionTrue || refs.Reason != string(gatewayv1.ListenerReasonResolvedRefs) {
				t.Fatalf("granted section references = %v", refs)
			}
		case ungranted.Name:
			if refs == nil || refs.Status != metav1.ConditionFalse || refs.Reason != string(gatewayv1.ListenerReasonRefNotPermitted) {
				t.Fatalf("ungranted section references = %v", refs)
			}
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

type failingHTTPSRouteSource struct{ err error }

func (s failingHTTPSRouteSource) ObserveRouteGraph(context.Context, graph.Request, *gatewayv1.Gateway) (graph.RouteInputs, error) {
	return graph.RouteInputs{}, s.err
}

func TestHTTPSGatewayWithdrawsUsingLiveAuthorityAfterGraphFailure(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		removeHTTPS               bool
		terminatingClass          bool
		unmanagedClass            bool
		editAddressDuringProvider bool
		revokeSectionDuringWrite  bool
		denyCertificate           bool
		restoreBeforeMutation     bool
		portRefs                  map[int]string
	}{
		{name: "last HTTPS removed while routes are unavailable", removeHTTPS: true},
		{name: "class terminates before snapshot", terminatingClass: true},
		{name: "class controller leaves before snapshot", unmanagedClass: true},
		{name: "unrelated Gateway edit during provider call", editAddressDuringProvider: true, portRefs: map[int]string{443: "approved-https", 8443: "approved-second"}},
		{name: "revised certificate reference during provider failure", revokeSectionDuringWrite: true, portRefs: map[int]string{443: "", 8443: "approved-second"}},
		{name: "stable denial with a provider failure", denyCertificate: true, portRefs: map[int]string{443: "", 8443: "approved-second"}},
		{name: "denied certificate restored before mutation", denyCertificate: true, restoreBeforeMutation: true, portRefs: map[int]string{443: "", 8443: "approved-second"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			cfg.OperatorHTTPS = true
			gw := programmedTestGateway(cfg)
			gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("https", 443), testHTTPSListener("second", 8443))
			gw.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.HTTPProtocolType)
			completeGatewayTopologyFixture(cfg, gw)
			controller.SetGatewayListenerScope(cfg, gw, gatewayv1.HTTPProtocolType)
			if tc.removeHTTPS {
				gw.Spec.Listeners = gw.Spec.Listeners[:1]
				gw.Generation++
			}
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			if tc.terminatingClass {
				now := metav1.Now()
				class.DeletionTimestamp = &now
				class.Finalizers = []string{"example.com/cleanup"}
			}
			if tc.unmanagedClass {
				class.Spec.ControllerName = "example.com/another-controller"
			}
			kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class).Build()
			routeErr := errors.New("list HTTPRoutes failed")
			providerErr := cloud.NewProviderError(cloud.ErrorCategoryRetryableService, errors.New("ordinary provider failure"))
			var withdrew bool
			denied := tc.denyCertificate
			provider := httpsGraphProvider(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				if spec.Gateway.Disposition == cloud.GraphWithdrawTLS {
					if withdrew {
						t.Fatal("duplicate fallback withdrawal")
					}
					withdrew = true
					listeners := spec.Gateway.Spec.ScopedListeners
					if listeners == nil || len(listeners) != len(tc.portRefs) || len(spec.Routes) != 0 {
						t.Fatalf("withdrawal allowlist = %#v", listeners)
					}
					for _, listener := range listeners {
						ref, exists := tc.portRefs[listener.Port]
						if !exists || listener.TLS == nil || listener.TLS.CertificateRef != ref {
							t.Fatalf("withdrawal listener = %#v", listener)
						}
					}
					if tc.restoreBeforeMutation {
						denied = false
					}
					if err := spec.ValidateMutation(ctx); err != nil {
						return cloud.GatewayGraphResult{}, err
					}
					if tc.restoreBeforeMutation {
						t.Fatal("mutation must be fenced when the planned revocation is stale")
					}
					return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
				}
				if err := spec.ValidateMutation(ctx); err != nil {
					return cloud.GatewayGraphResult{}, err
				}
				if tc.editAddressDuringProvider || tc.revokeSectionDuringWrite {
					live := gw.DeepCopy()
					if err := kube.Get(ctx, client.ObjectKeyFromObject(gw), live); err != nil {
						t.Fatal(err)
					}
					live.Generation++
					if tc.revokeSectionDuringWrite {
						live.Spec.Listeners[1].TLS.CertificateRefs[0].Name = "unapproved"
					} else {
						live.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Value: "192.0.2.123"}}
					}
					if err := kube.Update(ctx, live); err != nil {
						t.Fatal(err)
					}
				}
				return cloud.GatewayGraphResult{}, providerErr
			})
			r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
				Certificates: resolveHTTPSFunc(func(_ context.Context, _ *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
					if listener.Port == 443 && (denied || listener.TLS.CertificateRefs[0].Name == "unapproved") {
						return "", gatewayv1.ListenerReasonRefNotPermitted
					}
					return "approved-" + string(listener.Name), ""
				})}, provider)
			wantErr := error(providerErr)
			var err error
			switch {
			case tc.removeHTTPS:
				r.Writer.(*graph.Writer).Routes = failingHTTPSRouteSource{err: routeErr}
				wantErr = routeErr
				_, err = r.ensureGateway(ctx, gw)
			case tc.terminatingClass || tc.unmanagedClass:
				wantErr = errGatewayChanged
				_, err = r.ensureGateway(ctx, gw)
			case tc.denyCertificate:
				_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)})
			default:
				_, err = r.ensureGateway(ctx, gw)
			}
			if !withdrew {
				t.Error("missing fallback withdrawal")
			}
			if !errors.Is(err, wantErr) {
				t.Errorf("graph returned %v, want %v", err, wantErr)
			}
			var changed *graph.TLSAuthorizationChanged
			if tc.revokeSectionDuringWrite {
				if !errors.As(err, &changed) || !slices.Equal(changed.Ports, []int{443}) {
					t.Errorf("changed authorization = %v, want only port 443", changed)
				}
			} else if errors.As(err, &changed) {
				t.Errorf("unchanged authorization was reported as a transition: %v", changed.Ports)
			}
			if tc.restoreBeforeMutation && !errors.Is(err, errGatewayChanged) {
				t.Errorf("restored authorization did not fence withdrawal: %v", err)
			}
			if tc.denyCertificate && !tc.restoreBeforeMutation {
				providerErr = cloud.ErrQuotaExceeded
				policy, _ := controller.ProviderFailurePolicyFor(providerErr)
				want, _ := controller.ProviderFailureResult(policy, providerErr, string(gw.UID))
				withdrew = false
				got, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)})
				if err != nil || got != want || !withdrew {
					t.Errorf("quota failure = %v, %v, withdrawal attempted %t; want %v, nil, true", got, err, withdrew, want)
				}
				if err := kube.Get(ctx, client.ObjectKeyFromObject(gw), gw); err != nil {
					t.Fatal(err)
				}
				programmed := meta.FindStatusCondition(gw.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
				if programmed == nil || programmed.Message != policy.Message {
					t.Errorf("Gateway Programmed = %#v, want quota diagnostic", programmed)
				}
				for _, listener := range gw.Status.Listeners {
					if listener.Name == "https" && !meta.IsStatusConditionFalse(listener.Conditions, string(gatewayv1.ListenerConditionResolvedRefs)) {
						t.Errorf("denied certificate was not reported: %#v", listener.Conditions)
					}
				}
			}
		})
	}
}
