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

package httproute

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func grpcXBackendTestCA(t *testing.T, serial int64) string {
	t.Helper()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, private)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func grpcXBackendFixture(t *testing.T) (*routeModelInputFixture, *gatewayx.XBackend, *corev1.ConfigMap, *gatewayv1.GRPCRoute) {
	f := newExternalHostnameFixture(t)
	f.reconciler.Config.BackendH2C, f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.BackendTLSPolicy = true, true, true
	f.reconciler.XBackendTLSAvailable = func(_ context.Context, requireH2, _ bool) (bool, error) { return requireH2, nil }
	f.route.Spec.Hostnames = []gatewayv1.Hostname{"http.example.test"}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "origin-ca", Namespace: f.route.Namespace, UID: "ca-object"}, Data: map[string]string{"ca.crt": grpcXBackendTestCA(t, 1)}}
	b := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Name: "origin", Namespace: f.route.Namespace, UID: "xb", Generation: 1}, Spec: gatewayx.BackendSpec{
		Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8443},
		EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "rpc"}}},
		TLS:              &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeServerOnly, Validation: &gatewayv1.BackendTLSPolicyValidation{Hostname: "origin.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "origin-ca"}}}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "origin", Namespace: f.route.Namespace, Labels: map[string]string{"app": "rpc"}}, Status: corev1.PodStatus{PodIP: "192.0.2.25", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Name: "native", Namespace: f.route.Namespace, UID: "grpc-native", Generation: 1}}
	grpc.Spec.ParentRefs = f.route.Spec.ParentRefs
	grpc.Spec.Hostnames = []gatewayv1.Hostname{"grpc.example.test"}
	grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{{BackendRefs: []gatewayv1.GRPCBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind("XBackend")), Name: "origin",
	}}}}}}
	projected := model.ProjectGRPCRoute(grpc)
	require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
	grpc.Annotations, grpc.Finalizers = projected.Annotations, projected.Finalizers
	f.create(t, ca, b, pod, grpc)
	return f, b, ca, grpc
}

func TestGRPCXBackendTrustRotationNeedsProviderReadback(t *testing.T) {
	f, _, ca, grpc := grpcXBackendFixture(t)
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(grpc)}
	reconcile := func() bool {
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, grpc))
		require.Len(t, grpc.Status.Parents, 1)
		conditions := grpc.Status.Parents[0].Conditions
		require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionResolvedRefs)), "conditions %v", conditions)
		return meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed")
	}
	digest := func() string {
		for i := len(provider.routeSpecs) - 1; i >= 0; i-- {
			entry := provider.routeSpecs[i]
			if entry.GRPC && len(entry.HTTPBackends) == 1 {
				require.NotNil(t, entry.HTTPBackends[0].BackendTLS)
				require.True(t, entry.HTTPBackends[0].BackendTLS.RequireH2)
				return entry.HTTPBackends[0].BackendTLS.CADigest
			}
		}
		t.Fatal("no native gRPC backend")
		return ""
	}
	require.True(t, reconcile())
	firstDigest := digest()
	require.NotEmpty(t, firstDigest)
	deferred := cloud.RouteProgressingResult("Octavia load balancer is PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	provider.routeResult = &deferred
	ca.Data["ca.crt"] = grpcXBackendTestCA(t, 2)
	require.NoError(t, f.client.Update(t.Context(), ca))
	programmed := reconcile()
	require.NotEqual(t, firstDigest, digest(), "CA really changed between reconciles")
	require.False(t, programmed, "Programmed=True for the previous CA on a deferred observation")
	condition := meta.FindStatusCondition(grpc.Status.Parents[0].Conditions, r.Config.Domain()+"/Programmed")
	require.NotNil(t, condition)
	require.Equal(t, "Pending", condition.Reason)
}

func TestGRPCXBackendExternalHostnameUsesAuthorizedRoutedMembers(t *testing.T) {
	f, backend, _, route := grpcXBackendFixture(t)
	backend.Spec.Type = gatewayx.BackendTypeExternalHostname
	backend.Spec.EndpointSelector = nil
	backend.Spec.ExternalHostname = &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"}
	require.NoError(t, f.client.Update(t.Context(), backend))
	now := time.Now()
	cache := f.reconciler.ExternalHostnames
	cache.now = func() time.Time { return now }
	allowed := netip.MustParseAddr("198.51.100.22")
	outside := netip.MustParseAddr("192.0.2.22")
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	for _, phase := range []struct {
		name    string
		answers []netip.Addr
		members []cloud.Member
	}{
		{"permitted", []netip.Addr{allowed, outside}, []cloud.Member{{Address: allowed.String(), Port: 8443}}},
		{"outside configured networks", []netip.Addr{outside}, nil},
	} {
		t.Run(phase.name, func(t *testing.T) {
			now = now.Add(time.Minute)
			cache.lookup = func(_ context.Context, family, hostname string) ([]netip.Addr, error) {
				if family != "ip4" || hostname != "origin.example.test." {
					t.Errorf("unexpected DNS query %s %s", family, hostname)
				}
				return phase.answers, nil
			}
			before := len(provider.routeSpecs)
			_, err := r.Reconcile(t.Context(), request)
			require.NoError(t, err)
			require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, route))
			require.Len(t, route.Status.Parents, 1)
			require.True(t, meta.IsStatusConditionTrue(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)),
				"route conditions: %v", route.Status.Parents[0].Conditions)
			for _, spec := range provider.routeSpecs[before:] {
				if spec.GRPC && len(spec.HTTPBackends) == 1 {
					pool := spec.HTTPBackends[0]
					require.True(t, pool.RoutedExternal)
					require.Equal(t, phase.members, pool.Members)
					require.Equal(t, len(phase.members) == 0, pool.Unavailable)
					require.NotNil(t, pool.BackendTLS)
					require.True(t, pool.BackendTLS.RequireH2)
					return
				}
			}
			t.Fatal("the provider did not receive the native gRPC pool")
		})
	}
	f.reconciler.Config.XBackendExternalHostnameClasses = nil
	cache.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		t.Error("DNS lookup on a GatewayClass without the external hostname option")
		return nil, nil
	}
	r = newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, route))
	refs := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, refs)
	require.Equal(t, metav1.ConditionFalse, refs.Status)
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), refs.Reason)
}

func TestGRPCExternalHostnameWithdrawsMembersWhenH2CBecomesUnavailable(t *testing.T) {
	f, backend, _, route := grpcXBackendFixture(t)
	backend.Spec.Type = gatewayx.BackendTypeExternalHostname
	backend.Spec.EndpointSelector = nil
	backend.Spec.ExternalHostname = &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"}
	backend.Spec.TLS = nil
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolH2C)
	require.NoError(t, f.client.Update(t.Context(), backend))
	ip := netip.MustParseAddr("198.51.100.22")
	f.reconciler.ExternalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{ip}, nil
	}
	available := true
	f.reconciler.BackendH2CAvailable = func(context.Context) (bool, error) { return available, nil }
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	for _, provided := range []bool{true, false, true} {
		available = provided
		first := len(provider.routeSpecs)
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, route))
		require.Len(t, route.Status.Parents, 1)
		refs := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
		require.NotNil(t, refs)
		require.Equal(t, available, refs.Status == metav1.ConditionTrue)
		if !available {
			require.Equal(t, string(gatewayv1.RouteReasonUnsupportedProtocol), refs.Reason)
		}
		var pools []cloud.HTTPBackend
		for _, spec := range provider.routeSpecs[first:] {
			if spec.GRPC {
				pools = append(pools, spec.HTTPBackends...)
			}
		}
		require.Len(t, pools, 1)
		require.Equal(t, available, pools[0].BackendH2C)
		require.Equal(t, !available, pools[0].Unavailable)
		if available {
			require.Equal(t, []cloud.Member{{Address: ip.String(), Port: 8443}}, pools[0].Members)
		} else {
			require.Empty(t, pools[0].Members)
		}
	}
}

func TestExternalHostnameLookupPreservesSameNamedServiceImport(t *testing.T) {
	f := newExternalHostnameFixture(t)
	backend := addExternalHostnameRule(t, f, f.route, "shared")
	inputs := &routeModelInputs{gateway: f.gateway, input: model.Input{
		XBackendProfile: model.XBackendProfile{ExternalHostname: true},
		XBackends:       []model.XBackendObservation{{Key: client.ObjectKeyFromObject(backend), Spec: backend.Spec}},
	}}
	imported := cloud.HTTPBackend{Kind: cloud.HTTPBackendKindServiceImport, Namespace: backend.Namespace, Name: backend.Name,
		RoutedExternal: true, Port: 9000, Members: []cloud.Member{{Address: "192.0.2.10", Port: 9000}}}
	external := cloud.HTTPBackend{Kind: cloud.HTTPBackendKindXBackend, Namespace: backend.Namespace, Name: backend.Name, RoutedExternal: true, Port: 8089}
	ensures := []routeValidationSnapshot{{route: f.route, gatewayClass: &gatewayv1.GatewayClass{}, desired: cloud.RouteSpec{HTTPBackends: []cloud.HTTPBackend{imported, external}}}}
	f.reconciler.ExternalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("198.51.100.15")}, nil
	}
	_, err := f.reconciler.resolveExternalHostnameMembers(t.Context(), inputs, ensures)
	require.NoError(t, err)
	pools := ensures[0].desired.HTTPBackends
	require.Equal(t, imported, pools[0])
	require.Equal(t, []cloud.Member{{Address: "198.51.100.15", Port: 8089}}, pools[1].Members)
}

type unavailableXBackendPodInventory struct{ client.Reader }

func (r unavailableXBackendPodInventory) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return errors.New("selected Pod list unavailable")
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestGRPCXBackendPublishesWithdrawalWhenPodsCannotBeListed(t *testing.T) {
	f, _, _, grpc := grpcXBackendFixture(t)
	provider := &recordingProvider{}
	f.reconciler.APIReader = unavailableXBackendPodInventory{f.reconciler.APIReader}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(grpc)}
	_, err := r.Reconcile(t.Context(), key)
	require.NoError(t, err)
	denied := false
	for _, spec := range provider.routeSpecs {
		if spec.GRPC && len(spec.HTTPBackends) == 1 && spec.HTTPBackends[0].BackendTLS != nil && spec.HTTPBackends[0].BackendTLS.Denied {
			require.Empty(t, spec.HTTPBackends[0].Members)
			denied = true
		}
	}
	require.True(t, denied, "graph did withdraw the authenticated backend")
	require.NoError(t, f.client.Get(t.Context(), key.NamespacedName, grpc))
	require.Len(t, grpc.Status.Parents, 1, "otherwise a successfully withdrawn backend has no current route status")
	condition := meta.FindStatusCondition(grpc.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionUnknown, condition.Status)
	require.Equal(t, string(gatewayv1.RouteReasonPending), condition.Reason)
}

func TestGRPCXBackendSharedWithHTTPRouteTracksIndependentMTLSTransports(t *testing.T) {
	f, b, _, grpc := grpcXBackendFixture(t)
	f.reconciler.Config.XBackendClientCertificate = true
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(b), b))
	b.Spec.TLS.Mode = gatewayx.BackendTLSModeClientAndServer
	b.Spec.TLS.ClientCertificateRef = &gatewayv1.SecretObjectReference{Name: "client"}
	require.NoError(t, f.client.Update(t.Context(), b))
	f.route.Spec.Rules[0].BackendRefs[0].BackendObjectReference = grpc.Spec.Rules[0].BackendRefs[0].BackendObjectReference
	require.NoError(t, f.client.Update(t.Context(), f.route))
	h2Available := true
	f.reconciler.XBackendTLSAvailable = func(_ context.Context, h2, _ bool) (bool, error) { return !h2 || h2Available, nil }
	identity := "enrolled-A"
	f.reconciler.XBackendClientCertificates = xBackendCertificateResolverFunc(func(context.Context, gatewayv1.ObjectName, *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string) {
		if identity == "" {
			return "", gatewayv1.RouteReasonRefNotPermitted, "enrollment no longer authorized"
		}
		return identity, "", ""
	})
	readSpec := func(input []cloud.RouteGraphNode, native bool) cloud.HTTPBackend {
		t.Helper()
		for _, node := range input {
			if node.Spec.GRPC == native && len(node.Spec.HTTPBackends) == 1 {
				return node.Spec.HTTPBackends[0]
			}
		}
		t.Fatal("expected both routes on different hostnames")
		return cloud.HTTPBackend{}
	}
	first := f.observe(t)
	require.NoError(t, first.Validate(t.Context()))
	require.True(t, readSpec(first.Nodes, true).BackendTLS.RequireH2)
	require.False(t, readSpec(first.Nodes, false).BackendTLS.RequireH2)
	require.Equal(t, identity, readSpec(first.Nodes, true).XBackendClientCertificateReference)
	identity = "enrolled-B"
	require.ErrorIs(t, first.Validate(t.Context()), errHTTPRouteChanged)
	rotated := f.observe(t)
	require.Equal(t, identity, readSpec(rotated.Nodes, true).XBackendClientCertificateReference)
	identity = ""
	require.ErrorIs(t, rotated.Validate(t.Context()), errHTTPRouteChanged)
	withdrawn := readSpec(f.observe(t).Nodes, true)
	require.True(t, withdrawn.BackendTLS.Denied)
	require.Empty(t, withdrawn.Members)
	require.Empty(t, withdrawn.XBackendClientCertificateReference)
	identity = "enrolled-B"
	h2Available = false
	graph := f.observe(t)
	require.True(t, readSpec(graph.Nodes, true).BackendTLS.Denied)
	require.False(t, readSpec(graph.Nodes, false).BackendTLS.Denied)
	ancestors, _, err := (&xBackendStatusReconciler{routes: f.reconciler}).observeAncestors(t.Context(), client.ObjectKeyFromObject(b), b)
	require.NoError(t, err)
	require.Equal(t, metav1.ConditionTrue, ancestors[client.ObjectKeyFromObject(f.gateway)].Status)
}

type unavailableRouteInventory struct {
	client.Reader
	kind gatewayv1.Kind
}

func (r unavailableRouteInventory) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch list.(type) {
	case *gatewayv1.GRPCRouteList:
		if r.kind == model.GRPCRouteKind {
			return errors.New("GRPCRoute list unavailable")
		}
	case *gatewayv1.HTTPRouteList:
		if r.kind == model.HTTPRouteKind {
			return errors.New("HTTPRoute list unavailable")
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestXBackendClientWithdrawalWithIncompleteRouteInventory(t *testing.T) {
	f, backend, _, grpc := grpcXBackendFixture(t)
	r := f.reconciler
	r.Config.XBackendClientCertificate = true
	backend.Spec.TLS.Mode = gatewayx.BackendTLSModeClientAndServer
	backend.Spec.TLS.ClientCertificateRef = &gatewayv1.SecretObjectReference{Name: "client"}
	require.NoError(t, f.client.Update(t.Context(), backend))
	f.route.Spec.Rules[0].BackendRefs[0].BackendObjectReference = grpc.Spec.Rules[0].BackendRefs[0].BackendObjectReference
	require.NoError(t, f.client.Update(t.Context(), f.route))
	controllerutil.AddFinalizer(f.gateway, r.Config.BackendCAFinalizer())
	require.NoError(t, f.client.Update(t.Context(), f.gateway))
	r.XBackendClientCertificates = xBackendCertificateResolverFunc(func(context.Context, gatewayv1.ObjectName, *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string) {
		return "", gatewayv1.RouteReasonRefNotPermitted, "grant revoked"
	})
	id := controller.GatewayIdentity(r.Config, f.gateway)
	before, err := r.ObserveXBackendClientWithdrawals(t.Context(), id, f.gateway)
	require.NoError(t, err)
	require.Len(t, before, 2)

	live := r.APIReader
	for _, failure := range []struct {
		kind      gatewayv1.Kind
		remaining string
	}{
		{kind: model.GRPCRouteKind, remaining: string(f.route.UID)},
		{kind: model.HTTPRouteKind, remaining: string(grpc.UID)},
	} {
		t.Run(string(failure.kind), func(t *testing.T) {
			r.APIReader = unavailableRouteInventory{Reader: live, kind: failure.kind}
			targets, err := r.ObserveXBackendClientWithdrawals(t.Context(), id, f.gateway)
			require.Error(t, err)
			require.Len(t, targets, 1)
			require.Equal(t, failure.remaining, targets[0].Route.RouteUID)
			require.Equal(t, backend.Name, targets[0].BackendName)
			require.Empty(t, targets[0].AuthorizedReference)

			provider := &xBackendWithdrawalTestProvider{}
			active := newTestReconciler(*r, provider)
			_, err = active.Writer.ReconcileGraph(t.Context(), graph.Request{
				Identity: id, Gateway: f.gateway, Action: graph.WithdrawXBackendClientCertificates,
			})
			require.ErrorContains(t, err, string(failure.kind)+" list unavailable")
			require.NoError(t, provider.fenceErr)
			require.Equal(t, targets, provider.withdrawals)
		})
	}
}

func TestGRPCXBackendInDroppedRuleDoesNotAffectSiblingStatus(t *testing.T) {
	f := newExternalHostnameFixture(t)
	cfg := &f.reconciler.Config
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.RouteSessionPersistence = true, true, true
	f.reconciler.BackendH2CAvailable = func(context.Context) (bool, error) { return true, nil }
	f.route.Spec.Hostnames = []gatewayv1.Hostname{"ordinary.example.test"}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	require.NoError(t, f.client.Update(t.Context(), f.service))
	b := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: "only-in-dropped-rule", UID: "dropped", Generation: 1}, Spec: gatewayx.BackendSpec{
		Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8080},
		EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "rpc"}}},
	}}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: "partial", UID: "grpc-partial", Generation: 1}}
	grpc.Spec.ParentRefs, grpc.Spec.Hostnames = f.route.Spec.ParentRefs, []gatewayv1.Hostname{"rpc.example.test"}
	bad := grpcRule("unused", gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Sticky")}})
	bad.BackendRefs[0].BackendObjectReference = gatewayv1.BackendObjectReference{Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind("XBackend")), Name: gatewayv1.ObjectName(b.Name)}
	bad.SessionPersistence = &gatewayv1.SessionPersistence{ //nolint:staticcheck // Still served in the v1.7 Experimental bundle.
		Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "x-session"},
	}
	good := grpcRule(gatewayv1.ObjectName(f.service.Name), gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Ordinary")}})
	good.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{bad, good}
	projected := model.ProjectGRPCRoute(grpc)
	require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
	grpc.Annotations, grpc.Finalizers = projected.Annotations, projected.Finalizers
	f.create(t, grpc, b)
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(grpc)}
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, grpc))
	programmed := false
	for _, spec := range provider.routeSpecs {
		if spec.GRPC && len(spec.HTTPBackends) == 1 && spec.HTTPBackends[0].Name == f.service.Name {
			programmed = true
		}
	}
	require.True(t, programmed, "valid sibling must reach the provider")
	require.Len(t, grpc.Status.Parents, 1)
	require.True(t, meta.IsStatusConditionTrue(grpc.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionPartiallyInvalid)))
	ancestors, _, err := (&xBackendStatusReconciler{routes: f.reconciler}).observeAncestors(t.Context(), client.ObjectKeyFromObject(b), b)
	require.NoError(t, err)
	require.NotContains(t, ancestors, client.ObjectKeyFromObject(f.gateway))
}
