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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

type listenerSetTLSResolver struct {
	resolveHTTPSFunc
	refStatus gatewayv1.ListenerConditionReason
}

func (r listenerSetTLSResolver) ResolveListenerSet(context.Context, *gatewayv1.Gateway, *gatewayv1.ListenerSet, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	return "delegated-serving-certificate", ""
}

func (r listenerSetTLSResolver) ListenerSetReferenceStatus(context.Context, *gatewayv1.Gateway, *gatewayv1.ListenerSet, *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	return r.refStatus
}

func testTLSListenerSet(gateway *gatewayv1.Gateway, hostname gatewayv1.Hostname) *gatewayv1.ListenerSet {
	gateway.Spec.AllowedListeners = &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}}
	listener := testHTTPSListener("secure", 443)
	listener.Hostname = ptr.To(hostname)
	return &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "delegated", Namespace: "team", Generation: 1},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))},
			Listeners: []gatewayv1.ListenerEntry{gatewayv1.ListenerEntry(listener)},
		},
	}
}

func TestDelegatedHTTPSPortCAOverride(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.HTTPSListenerIsolation, cfg.FrontendClientValidation = true, true, true
	gateway := programmedTestGateway(cfg)
	set := testTLSListenerSet(gateway, "secure.example.test")
	defaultCA := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "default-ca", Namespace: gateway.Namespace, UID: "default-ca"}, Data: map[string]string{"ca.crt": frontendTestCA(t)}}
	overrideCA := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "override-ca", Namespace: "trust", UID: "override-ca"}, Data: map[string]string{"ca.crt": frontendTestCA(t)}}
	gateway.Spec.TLS = &gatewayv1.GatewayTLSConfig{Frontend: &gatewayv1.FrontendTLSConfig{
		Default: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
			CACertificateRefs: []gatewayv1.ObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(defaultCA.Name)}},
		}},
		PerPort: []gatewayv1.TLSPortConfig{{Port: 443, TLS: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
			CACertificateRefs: []gatewayv1.ObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(overrideCA.Name), Namespace: ptr.To(gatewayv1.Namespace(overrideCA.Namespace))}},
		}}}},
	}}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "client-ca", Namespace: overrideCA.Namespace},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: gatewayv1.Namespace(gateway.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "ConfigMap", Name: ptr.To(gatewayv1.ObjectName(overrideCA.Name))}},
		},
	}
	kube := rawIndexedFakeClientBuilder(testScheme(t), cfg).WithIndex(&gatewayv1.Gateway{}, gatewayFrontendCAIndex, gatewayFrontendCAKeys).
		WithObjects(gateway, set, defaultCA, grant).Build()
	r := &Reconciler{Config: cfg, Client: kube, APIReader: kube, Certificates: listenerSetTLSResolver{},
		HTTPSListenerIsolationAvailable: func(context.Context) (bool, error) { return true, nil }}
	original, err := r.ObserveListenerSets(t.Context(), kube, gateway)
	require.NoError(t, err)
	require.Empty(t, original.https[443].CertificateRef)
	require.Empty(t, original.SectionsFor(client.ObjectKeyFromObject(set)))
	status, err := r.listenerSetEntryStatus(t.Context(), set, original.sets[0].entries[0], cloud.GatewayGraphResult{})
	require.NoError(t, err)
	require.Equal(t, string(gatewayv1.ListenerReasonNoValidCACertificate), meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerEntryConditionAccepted)).Reason)
	require.Equal(t, string(gatewayv1.ListenerReasonInvalidCACertificateRef), meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerEntryConditionResolvedRefs)).Reason)

	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	require.Equal(t, []ctrl.Request{request}, r.gatewayRequestsForCertificateGrant(t.Context(), grant))
	require.Equal(t, []ctrl.Request{request}, r.gatewayRequestsForFrontendCA(t.Context(), overrideCA))
	require.NoError(t, kube.Create(t.Context(), overrideCA))
	restored, err := r.ObserveListenerSets(t.Context(), kube, gateway)
	require.NoError(t, err)
	require.Equal(t, "delegated-serving-certificate", restored.https[443].CertificateRef)
	require.Equal(t, "override-ca", restored.https[443].ClientValidation.ConfigMapUID)
	require.Len(t, restored.SectionsFor(client.ObjectKeyFromObject(set)), 1)
}

func TestSharedHTTPSPortKeepsAuthorizedHostAfterCertificateChanges(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.HTTPSListenerIsolation = true, true
	gateway := programmedTestGateway(cfg)
	gateway.Annotations[cfg.GatewayHTTPSIsolatedPortsAnnotation()] = "443"
	set := testTLSListenerSet(gateway, "delegated.example.test")
	parent := testHTTPSListener("gateway-secure", 443)
	parent.Hostname = ptr.To(gatewayv1.Hostname("parent.example.test"))
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, parent)
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(set).Build()
	parentRef := "original-gateway-certificate"
	resolver := listenerSetTLSResolver{resolveHTTPSFunc: func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
		if parentRef == "" {
			return "", gatewayv1.ListenerReasonRefNotPermitted
		}
		return parentRef, ""
	}}
	r := &Reconciler{Config: cfg, APIReader: kube, Certificates: resolver,
		HTTPSListenerIsolationAvailable: func(context.Context) (bool, error) { return true, nil }}
	observe := func() ListenerSetProjection {
		t.Helper()
		projection, err := r.ObserveListenerSets(t.Context(), kube, gateway)
		require.NoError(t, err)
		return projection
	}

	initial := observe()
	require.Equal(t, parentRef, initial.https[443].CertificateRef)
	require.Equal(t, []string{"parent.example.test"}, initial.https[443].VirtualHosts)
	require.Empty(t, initial.SectionsFor(client.ObjectKeyFromObject(set)))

	parentRef = "delegated-serving-certificate"
	rotated := observe()
	require.Equal(t, parentRef, rotated.https[443].CertificateRef)
	require.Equal(t, []string{"delegated.example.test", "parent.example.test"}, rotated.https[443].VirtualHosts)

	parentRef = ""
	revoked := observe()
	require.True(t, revoked.RejectsGatewayHTTPSSection(&parent))
	require.Equal(t, "delegated-serving-certificate", revoked.https[443].CertificateRef)
	require.Equal(t, []string{"delegated.example.test"}, revoked.https[443].VirtualHosts)
	require.Len(t, revoked.SectionsFor(client.ObjectKeyFromObject(set)), 1)
}

func TestConflictedListenerSetStillReportsDeniedCertificateRef(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.HTTPSListenerIsolation = true, true
	gateway := programmedTestGateway(cfg)
	set := testTLSListenerSet(gateway, "shared.example.test")
	parent := testHTTPSListener("gateway-secure", 443)
	parent.Hostname = set.Spec.Listeners[0].Hostname
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, parent)
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(set).Build()
	r := &Reconciler{Config: cfg, APIReader: kube, Certificates: listenerSetTLSResolver{refStatus: gatewayv1.ListenerReasonRefNotPermitted}}
	projection, err := r.ObserveListenerSets(t.Context(), kube, gateway)
	require.NoError(t, err)
	status, err := r.listenerSetEntryStatus(t.Context(), set, projection.sets[0].entries[0], cloud.GatewayGraphResult{})
	require.NoError(t, err)
	conflict := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerEntryConditionConflicted))
	require.Equal(t, metav1.ConditionTrue, conflict.Status)
	require.Equal(t, string(gatewayv1.ListenerReasonHostnameConflict), conflict.Reason)
	refs := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerEntryConditionResolvedRefs))
	require.Equal(t, metav1.ConditionFalse, refs.Status)
	require.Equal(t, string(gatewayv1.ListenerReasonRefNotPermitted), refs.Reason)
}

func TestDelegatedHTTPSSecretRecheckWithBlockedAddress(t *testing.T) {
	cfg := testConfig()
	cfg.OperatorHTTPS, cfg.HTTPSListenerIsolation = true, true
	cfg.OpenStackResyncInterval = 10 * time.Minute
	gateway := programmedTestGateway(cfg)
	set := testTLSListenerSet(gateway, "secure.example.test")
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, class, set).WithObjects(gateway, class, set).Build()
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 443}
	provider := &recordingProvider{gatewayResult: &ready}
	r := newTestReconciler(Reconciler{Config: cfg, Client: kube, APIReader: kube, Coordinator: &graph.Coordinator{}, Certificates: listenerSetTLSResolver{},
		HTTPSListenerIsolationAvailable: func(context.Context) (bool, error) { return true, nil }}, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	for i := 0; i < 3; i++ {
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
	}
	require.NotEmpty(t, provider.gatewaySpecs)
	require.True(t, slices.ContainsFunc(provider.gatewaySpecs[len(provider.gatewaySpecs)-1].ScopedListeners, func(listener cloud.ScopedListener) bool {
		return listener.Port == 443 && listener.TLS != nil && listener.TLS.CertificateRef != ""
	}))

	ready.AddressIssue, ready.AddressBlocked = "Restore the bound VIP", true
	result, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, httpsPollInterval, result.RequeueAfter)
}
