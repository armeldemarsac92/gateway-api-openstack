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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/stretchr/testify/require"
)

type frontendCAReader struct {
	client.Reader
	reads []client.ObjectKey
}

func (r *frontendCAReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok {
		r.reads = append(r.reads, key)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestGatewayFrontendCAStatusAndCrossNamespaceRevocation(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.FrontendClientValidation = true, true
	gw := programmedTestGateway(cfg)
	gw.Spec.Listeners = append(gw.Spec.Listeners, testHTTPSListener("default-https", 443), testHTTPSListener("override", 8443), testHTTPSListener("fallback", 9443))
	gw.Spec.TLS = &gatewayv1.GatewayTLSConfig{Frontend: &gatewayv1.FrontendTLSConfig{
		Default: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
			CACertificateRefs: []gatewayv1.ObjectReference{{Name: "client-ca", Kind: "ConfigMap", Namespace: ptr.To(gatewayv1.Namespace("trust"))}},
		}},
		PerPort: []gatewayv1.TLSPortConfig{
			{Port: 8443, TLS: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
				CACertificateRefs: []gatewayv1.ObjectReference{{Name: "local-ca", Kind: "ConfigMap"}},
			}}},
			{Port: 9443, TLS: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
				Mode: gatewayv1.AllowInsecureFallback, CACertificateRefs: []gatewayv1.ObjectReference{{Name: "local-ca", Kind: "ConfigMap"}},
			}}},
		},
	}}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "client-ca", Namespace: "trust", UID: "remote-ca"}, Data: map[string]string{"ca.crt": frontendTestCA(t)}}
	local := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: gw.Namespace, UID: "local-ca"}, Data: map[string]string{"ca.crt": frontendTestCA(t)}}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "trust-grant", Namespace: "trust"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: gatewayv1.Namespace(gw.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "ConfigMap", Name: ptr.To(gatewayv1.ObjectName("other-ca"))}},
		},
	}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class).WithObjects(gw, class, ca, local, grant).Build()
	reader := &frontendCAReader{Reader: kube}
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 8443}
	ready.FrontendFallbackUnavailablePorts = []int{9443}
	provider := &recordingProvider{gatewayResult: &ready}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: reader, Config: cfg, Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			return "server-certificate", ""
		})}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gw)}
	reconcile := func() []cloud.ScopedListener {
		t.Helper()
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, kube.Get(t.Context(), request.NamespacedName, gw))
		return provider.gatewaySpecs[len(provider.gatewaySpecs)-1].ScopedListeners
	}
	condition := func(name gatewayv1.SectionName, kind gatewayv1.ListenerConditionType) metav1.Condition {
		t.Helper()
		i := slices.IndexFunc(gw.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == name })
		require.GreaterOrEqual(t, i, 0)
		result := meta.FindStatusCondition(gw.Status.Listeners[i].Conditions, string(kind))
		require.NotNil(t, result)
		return *result
	}
	checkDenied := func(reason gatewayv1.ListenerConditionReason) {
		t.Helper()
		refs := condition("default-https", gatewayv1.ListenerConditionResolvedRefs)
		require.Equal(t, metav1.ConditionFalse, refs.Status)
		require.Equal(t, string(reason), refs.Reason)
		require.Equal(t, string(gatewayv1.ListenerReasonNoValidCACertificate), condition("default-https", gatewayv1.ListenerConditionAccepted).Reason)
		require.Equal(t, metav1.ConditionFalse, condition("default-https", gatewayv1.ListenerConditionProgrammed).Status)
		for _, name := range []gatewayv1.SectionName{"http", "override"} {
			require.Equal(t, metav1.ConditionTrue, condition(name, gatewayv1.ListenerConditionProgrammed).Status)
		}
	}

	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	ports := reconcile()
	checkDenied(gatewayv1.ListenerReasonRefNotPermitted)
	require.Equal(t, string(gatewayv1.ListenerReasonPending), condition("default-https", gatewayv1.ListenerConditionProgrammed).Reason)
	require.NotContains(t, reader.reads, client.ObjectKeyFromObject(ca))
	require.Empty(t, r.gatewayRequestsForCertificateGrant(t.Context(), grant))
	require.Nil(t, ports[0].TLS)
	require.Empty(t, ports[1].TLS.CertificateRef)
	require.Equal(t, "local-ca", ports[2].TLS.ClientValidation.ConfigMapUID)
	require.True(t, ports[3].TLS.ClientValidation.InsecureFallback)
	require.Contains(t, condition("fallback", gatewayv1.ListenerConditionAccepted).Message, string(gatewayv1.AllowInsecureFallback))
	require.Equal(t, string(gatewayv1.ListenerReasonUnsupportedValue), condition("fallback", gatewayv1.ListenerConditionAccepted).Reason)

	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(grant), grant))
	grant.Spec.To[0].Name = ptr.To(gatewayv1.ObjectName(ca.Name))
	require.Equal(t, []ctrl.Request{request}, r.gatewayRequestsForCertificateGrant(t.Context(), grant))
	require.NoError(t, kube.Update(t.Context(), grant))
	ready.ScopedListenerReadyPorts = []int{80, 443, 8443}
	ports = reconcile()
	require.Contains(t, reader.reads, client.ObjectKeyFromObject(ca))
	require.Equal(t, "remote-ca", ports[1].TLS.ClientValidation.ConfigMapUID)
	require.Equal(t, metav1.ConditionTrue, condition("default-https", gatewayv1.ListenerConditionProgrammed).Status)

	// Change the referenced CA between cloud discovery and its first write.
	// The ordinary write loses authorization and withdrawal sees the new CA.
	oldDigest := ports[1].TLS.ClientValidation.Digest
	gw.Finalizers = append(gw.Finalizers, cfg.BackendCAFinalizer())
	require.NoError(t, kube.Update(t.Context(), gw))
	backendWithdrawalError := errors.New("backend identity withdrawal failed")
	writer := r.Writer.(*graph.Writer)
	writer.Provider = httpsGraphProvider(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		if spec.Gateway.Disposition == cloud.GraphWithdrawBackendClientCertificate {
			return cloud.GatewayGraphResult{}, backendWithdrawalError
		}
		if spec.Gateway.Disposition == cloud.GraphEnsure {
			require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(ca), ca))
			ca.Data["ca.crt"] = frontendTestCA(t)
			require.NoError(t, kube.Update(ctx, ca))
			return cloud.GatewayGraphResult{}, spec.ValidateMutation(ctx)
		}
		require.Equal(t, cloud.GraphWithdrawTLS, spec.Gateway.Disposition)
		require.Equal(t, []int{443}, spec.Gateway.TLSWithdrawalPorts)
		require.NotEqual(t, oldDigest, spec.Gateway.Spec.ScopedListeners[0].TLS.ClientValidation.Digest)
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	_, err = r.ensureGateway(t.Context(), gw)
	var authorizationChanged *graph.TLSAuthorizationChanged
	require.ErrorAs(t, err, &authorizationChanged)
	require.ErrorIs(t, err, backendWithdrawalError)
	require.Equal(t, []int{443}, authorizationChanged.Ports)
	writer.Provider = provider

	require.NoError(t, kube.Delete(t.Context(), ca))
	ready.ScopedTLSClosedPorts = []int{443, 9443}
	ready.ScopedListenerReadyPorts = []int{80, 8443}
	require.Empty(t, reconcile()[1].TLS.CertificateRef)
	checkDenied(gatewayv1.ListenerReasonInvalidCACertificateRef)
	require.Equal(t, string(gatewayv1.ListenerReasonInvalid), condition("default-https", gatewayv1.ListenerConditionProgrammed).Reason)

	require.NoError(t, kube.Delete(t.Context(), grant))
	reader.reads = nil
	require.Empty(t, reconcile()[1].TLS.CertificateRef)
	checkDenied(gatewayv1.ListenerReasonRefNotPermitted)
	require.NotContains(t, reader.reads, client.ObjectKeyFromObject(ca))

	gw.Spec.TLS.Frontend.Default.Validation.CACertificateRefs[0].Kind = "Secret"
	require.NoError(t, kube.Update(t.Context(), gw))
	require.Empty(t, reconcile()[1].TLS.CertificateRef)
	checkDenied(gatewayv1.ListenerReasonInvalidCACertificateKind)

	fallbackCondition := string(gatewayv1.GatewayConditionInsecureFrontendValidationMode)
	checkFallbackCondition := func() {
		t.Helper()
		mode := meta.FindStatusCondition(gw.Status.Conditions, fallbackCondition)
		require.NotNil(t, mode)
		require.Equal(t, metav1.ConditionTrue, mode.Status)
		require.Equal(t, string(gatewayv1.GatewayReasonConfigurationChanged), mode.Reason)
		require.Equal(t, gw.Generation, mode.ObservedGeneration)
	}
	checkFallbackCondition()

	// Default and per-port modes share the same CA validation. A strict
	// per-port override must not inherit the Gateway's fallback mode.
	ready.FrontendFallbackUnavailablePorts = nil
	ready.ScopedTLSClosedPorts = nil
	ready.ScopedListenerReadyPorts = []int{80, 443, 8443, 9443}
	frontend := gw.Spec.TLS.Frontend
	frontend.Default.Validation.CACertificateRefs = []gatewayv1.ObjectReference{{Name: "local-ca", Kind: "ConfigMap"}}
	frontend.Default.Validation.Mode = gatewayv1.AllowInsecureFallback
	frontend.PerPort[1].TLS.Validation.Mode = gatewayv1.AllowValidOnly
	require.NoError(t, kube.Update(t.Context(), gw))
	ports = reconcile()
	checkFallbackCondition()
	require.True(t, ports[1].TLS.ClientValidation.InsecureFallback)
	require.False(t, ports[2].TLS.ClientValidation.InsecureFallback)
	require.False(t, ports[3].TLS.ClientValidation.InsecureFallback)
	require.Equal(t, metav1.ConditionTrue, condition("default-https", gatewayv1.ListenerConditionProgrammed).Status)

	frontend = gw.Spec.TLS.Frontend
	frontend.Default.Validation.Mode = gatewayv1.AllowValidOnly
	frontend.PerPort[1].TLS.Validation.Mode = gatewayv1.AllowInsecureFallback
	require.NoError(t, kube.Update(t.Context(), gw))
	ports = reconcile()
	checkFallbackCondition()
	require.False(t, ports[1].TLS.ClientValidation.InsecureFallback)
	require.True(t, ports[3].TLS.ClientValidation.InsecureFallback)

	gw.Spec.TLS.Frontend.PerPort[1].TLS.Validation.Mode = gatewayv1.AllowValidOnly
	require.NoError(t, kube.Update(t.Context(), gw))
	require.False(t, reconcile()[3].TLS.ClientValidation.InsecureFallback)
	require.Nil(t, meta.FindStatusCondition(gw.Status.Conditions, fallbackCondition))
}

func frontendTestCA(t *testing.T) string {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
