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
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestBackendTLSPolicyProtectsOneWeightedShare(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.BackendTLSPolicy = true
	f.reconciler.Config.WeightedHTTPTargets = true
	f.reconciler.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
	healthy, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	healthy.Name, healthy.UID, healthy.ResourceVersion = "healthy", "healthy-uid", ""
	endpoints.Name, endpoints.UID, endpoints.ResourceVersion = "healthy", "healthy-slice-uid", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = healthy.Name
	f.create(t, healthy, endpoints)

	selected := f.route.Spec.Rules[0].BackendRefs[0].DeepCopy()
	selected.Weight = ptr.To(int32(3))
	other := selected.DeepCopy()
	other.Name, other.Weight = gatewayv1.ObjectName(healthy.Name), ptr.To(int32(1))
	f.route.Spec.Rules[0].BackendRefs = []gatewayv1.HTTPBackendRef{*selected, *other}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	before := f.observe(t)
	require.NoError(t, before.Validate(t.Context()))
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "require-tls", UID: "policy-uid", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "api.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "Secret", Name: "invalid"}},
			},
		},
	}
	f.create(t, policy)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Len(t, provider.routeSpecs, 1)
	spec := provider.routeSpecs[0]
	require.Len(t, spec.WeightedHTTPRules, 1)
	require.Len(t, spec.WeightedHTTPRules[0].Targets, 2)
	require.Len(t, spec.HTTPBackends, 2)
	denied, allowed := spec.WeightedHTTPRules[0].Targets[0], spec.WeightedHTTPRules[0].Targets[1]
	require.Equal(t, 3, denied.Weight)
	require.Equal(t, 1, allowed.Weight)
	require.NotNil(t, denied.BackendIndex)
	require.NotNil(t, allowed.BackendIndex)
	rejected := spec.HTTPBackends[*denied.BackendIndex]
	require.NotNil(t, rejected.BackendTLS)
	require.True(t, rejected.BackendTLS.Denied)
	require.True(t, rejected.Unavailable)
	require.Empty(t, rejected.Members)
	require.False(t, rejected.InvalidReference)
	require.Nil(t, spec.HTTPBackends[*allowed.BackendIndex].BackendTLS)
	require.NotEmpty(t, spec.HTTPBackends[*allowed.BackendIndex].Members)

	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	require.Len(t, f.route.Status.Parents, 1)
	require.True(t, meta.IsStatusConditionTrue(f.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)))
}

func TestGRPCBackendTLSPolicyResolvesAndProtectsBackend(t *testing.T) {
	f, _, grpc := newRouteElectionFixture(t)
	f.reconciler.Config.BackendTLSPolicy = true
	requiredH2, sanCapable := true, false
	f.reconciler.BackendTLSSANAvailable = func(_ context.Context, requireH2 bool) (bool, error) {
		return sanCapable && requireH2, nil
	}
	f.reconciler.GRPCBackendTLSAvailable = func(context.Context) (bool, error) { return requiredH2, nil }
	// Match the upstream fixture: a named TCP Service port 443 with no appProtocol.
	service := f.service.DeepCopy()
	service.Name, service.UID, service.ResourceVersion = "grpc-tls", "grpc-tls-uid", ""
	service.Spec.Ports[0].Name, service.Spec.Ports[0].Port, service.Spec.Ports[0].AppProtocol = "btls", 443, nil
	endpoints := f.endpoints.DeepCopy()
	endpoints.Name, endpoints.UID, endpoints.ResourceVersion = "grpc-tls", "grpc-tls-slice", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = service.Name
	endpoints.Ports[0].Name, endpoints.Ports[0].Port = ptr.To("btls"), ptr.To(int32(8443))
	grpc.Spec.Rules[0].BackendRefs[0].Name = gatewayv1.ObjectName(service.Name)
	grpc.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(443))
	projected := model.ProjectGRPCRoute(grpc)
	require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
	grpc.Annotations, grpc.Finalizers = projected.Annotations, projected.Finalizers

	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	require.NoError(t, err)
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "grpc-ca", UID: "grpc-ca-uid"},
		Data: map[string]string{"ca.crt": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}}
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "grpc-tls", UID: "grpc-policy-uid", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(service.Name)},
				SectionName:                ptr.To(gatewayv1.SectionName("btls")),
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "backend.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name)}},
			},
		},
	}
	f.create(t, service, endpoints, grpc, ca, policy)
	policyKey, routeKey := client.ObjectKeyFromObject(policy), client.ObjectKeyFromObject(grpc)
	status := &backendTLSPolicyStatusReconciler{routes: f.reconciler}
	reconcilePolicy := func() []metav1.Condition {
		t.Helper()
		_, err := status.Reconcile(t.Context(), ctrl.Request{NamespacedName: policyKey})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), policyKey, policy))
		for _, ancestor := range policy.Status.Ancestors {
			if ancestor.ControllerName == f.reconciler.Config.ControllerName {
				require.Equal(t, gatewayv1.ObjectName(f.gateway.Name), ancestor.AncestorRef.Name)
				return ancestor.Conditions
			}
		}
		return nil
	}

	provider := &recordingProvider{}
	route := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	reconcileRoute := func() cloud.HTTPBackend {
		t.Helper()
		provider.routeSpecs = nil
		_, err := route.Reconcile(t.Context(), ctrl.Request{NamespacedName: routeKey})
		require.NoError(t, err)
		for _, spec := range provider.routeSpecs {
			if spec.GRPC {
				require.Len(t, spec.HTTPBackends, 1)
				return spec.HTTPBackends[0]
			}
		}
		t.Fatal("missing GRPCRoute in provider graph")
		return cloud.HTTPBackend{}
	}

	// Pending ancestor admission must neither enroll nor send cleartext.
	before := reconcileRoute()
	require.NotNil(t, before.BackendTLS)
	require.True(t, before.BackendTLS.Denied)
	require.Empty(t, before.Members)
	for i := range backendtls.MaxAncestors {
		policy.Status.Ancestors = append(policy.Status.Ancestors, gatewayv1.PolicyAncestorStatus{
			AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("foreign-%d", i))},
			ControllerName: "foreign.example.com/gateway",
		})
	}
	require.NoError(t, f.client.Status().Update(t.Context(), policy))
	require.Nil(t, reconcilePolicy())
	full := reconcileRoute()
	require.NotNil(t, full.BackendTLS)
	require.True(t, full.BackendTLS.Denied)
	require.NoError(t, f.client.Get(t.Context(), routeKey, grpc))
	require.Len(t, grpc.Status.Parents, 1)
	programmed := meta.FindStatusCondition(grpc.Status.Parents[0].Conditions, f.reconciler.Config.Domain()+"/Programmed")
	require.NotNil(t, programmed)
	require.Equal(t, backendtls.AncestorLimitReason, programmed.Reason)

	foreign := slices.Clone(policy.Status.Ancestors[:backendtls.MaxAncestors-1])
	policy.Status.Ancestors = foreign
	require.NoError(t, f.client.Status().Update(t.Context(), policy))
	conditions := reconcilePolicy()
	require.Len(t, policy.Status.Ancestors, backendtls.MaxAncestors)
	require.Equal(t, foreign, policy.Status.Ancestors[:len(foreign)])
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.PolicyConditionAccepted)))
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.BackendTLSPolicyConditionResolvedRefs)))
	resolved := reconcileRoute()
	require.NotNil(t, resolved.BackendTLS)
	require.False(t, resolved.BackendTLS.Denied)
	require.False(t, resolved.BackendH2C)
	require.Equal(t, "backend.example.test", resolved.BackendTLS.Hostname)
	require.Equal(t, string(ca.UID), resolved.BackendTLS.CAConfigMapUID)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.20", Port: 8443}}, resolved.Members)
	require.NoError(t, f.client.Get(t.Context(), routeKey, grpc))
	require.Len(t, grpc.Status.Parents, 1)
	require.True(t, meta.IsStatusConditionTrue(grpc.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)))

	requiredH2 = false
	conditions = reconcilePolicy()
	accepted := meta.FindStatusCondition(conditions, string(gatewayv1.PolicyConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, metav1.ConditionFalse, accepted.Status)
	require.Equal(t, "UnsupportedProvider", accepted.Reason)
	unavailable := reconcileRoute()
	require.NotNil(t, unavailable.BackendTLS)
	require.True(t, unavailable.BackendTLS.Denied)
	require.Empty(t, unavailable.Members)

	requiredH2 = true
	require.False(t, reconcileRoute().BackendTLS.Denied)
	policy.Spec.Validation.SubjectAltNames = []gatewayv1.SubjectAltName{
		{Type: gatewayv1.URISubjectAltNameType, URI: "spiffe://id.example.test/workload%2fa"},
	}
	policy.Generation++
	require.NoError(t, f.client.Update(t.Context(), policy))
	conditions = reconcilePolicy()
	accepted = meta.FindStatusCondition(conditions, string(gatewayv1.PolicyConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, policy.Generation, accepted.ObservedGeneration)
	require.Equal(t, metav1.ConditionFalse, accepted.Status)
	require.Contains(t, accepted.Message, "subjectAltNames")
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.BackendTLSPolicyConditionResolvedRefs)))
	require.True(t, reconcileRoute().BackendTLS.Denied)

	sanCapable = true
	conditions = reconcilePolicy()
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.PolicyConditionAccepted)))
	resolved = reconcileRoute()
	require.False(t, resolved.BackendTLS.Denied)
	require.Equal(t, []cloud.BackendTLSSubjectAltName{{Type: cloud.BackendTLSSANURI, URI: "spiffe://id.example.test/workload%2fa"}}, resolved.BackendTLS.SubjectAltNames)

	require.NoError(t, f.client.Delete(t.Context(), ca))
	conditions = reconcilePolicy()
	require.True(t, meta.IsStatusConditionFalse(conditions, string(gatewayv1.BackendTLSPolicyConditionResolvedRefs)))
	denied := reconcileRoute()
	require.NotNil(t, denied.BackendTLS)
	require.True(t, denied.BackendTLS.Denied)
	require.False(t, denied.BackendH2C)
	require.Empty(t, denied.Members)

	require.NoError(t, f.client.Delete(t.Context(), policy))
	removed := reconcileRoute()
	require.Nil(t, removed.BackendTLS)
	require.False(t, removed.BackendH2C)
	require.Empty(t, removed.Members)
	require.NoError(t, f.client.Get(t.Context(), routeKey, grpc))
	require.True(t, meta.IsStatusConditionFalse(grpc.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)))
}

func TestGRPCZeroWeightBackendResolvesIncompatibleTLS(t *testing.T) {
	f, _, grpc := newRouteElectionFixture(t)
	f.reconciler.Config.WeightedHTTPTargets = true
	f.reconciler.Config.BackendTLSPolicy = true
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	require.NoError(t, f.client.Update(t.Context(), f.service))
	zero := f.service.DeepCopy()
	zero.Name, zero.UID, zero.ResourceVersion = "zero", "zero-uid", ""
	ref := grpc.Spec.Rules[0].BackendRefs[0].DeepCopy()
	ref.Name, ref.Weight = gatewayv1.ObjectName(zero.Name), ptr.To(int32(0))
	grpc.Spec.Rules[0].BackendRefs = append(grpc.Spec.Rules[0].BackendRefs, *ref)
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: zero.Namespace, Name: "require-tls", UID: "policy-uid"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(zero.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{Hostname: "api.example.test", WellKnownCACertificates: ptr.To(gatewayv1.WellKnownCACertificatesSystem)},
		},
	}
	projected := model.ProjectGRPCRoute(grpc)
	require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
	grpc.Annotations, grpc.Finalizers = projected.Annotations, projected.Finalizers
	f.create(t, zero, grpc, policy)

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	key := client.ObjectKeyFromObject(grpc)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, grpc))
	require.Len(t, grpc.Status.Parents, 1)
	resolved := meta.FindStatusCondition(grpc.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, resolved)
	require.Equal(t, metav1.ConditionFalse, resolved.Status)
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedProtocol), resolved.Reason)
	require.NotEmpty(t, provider.routeSpecs)
	for _, spec := range provider.routeSpecs {
		if spec.GRPC {
			require.Len(t, spec.HTTPBackends, 1)
			require.Len(t, spec.WeightedHTTPRules, 1)
			require.Equal(t, 0, spec.WeightedHTTPRules[0].Targets[1].Weight)
			require.Nil(t, spec.WeightedHTTPRules[0].Targets[1].BackendIndex)
			return
		}
	}
	t.Fatal("missing GRPCRoute in provider graph")
}

func TestBackendTLSAncestorCapacityPreservesRowsAndAllocatesOneReplacement(t *testing.T) {
	f := newRouteModelInputFixture(t)
	controllerName := f.reconciler.Config.ControllerName
	namespace := f.route.Namespace
	holder := types.NamespacedName{Namespace: namespace, Name: "holder"}
	stale := types.NamespacedName{Namespace: namespace, Name: "stale"}
	next := types.NamespacedName{Namespace: namespace, Name: "next"}
	later := types.NamespacedName{Namespace: namespace, Name: "z-later"}
	conditions := backendTLSPolicyConditions{
		accepted:     metav1.Condition{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", Message: "Accepted", ObservedGeneration: 2},
		resolvedRefs: metav1.Condition{Type: "ResolvedRefs", Status: metav1.ConditionTrue, Reason: "ResolvedRefs", Message: "Resolved", ObservedGeneration: 2},
	}
	var foreign []gatewayv1.PolicyAncestorStatus
	for i := range backendtls.MaxAncestors - 2 {
		foreign = append(foreign, gatewayv1.PolicyAncestorStatus{
			AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("foreign-%d", i))},
			ControllerName: "foreign.example.com/other",
			Conditions:     []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionFalse, Reason: "External", Message: "External", ObservedGeneration: 12}},
		})
	}
	policy := &gatewayv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Generation: 2}}
	policy.Status.Ancestors = slices.Concat(foreign, []gatewayv1.PolicyAncestorStatus{
		{AncestorRef: gatewayv1.ParentReference{Name: "holder"}, ControllerName: controllerName},
		{AncestorRef: gatewayAncestorRef(stale), ControllerName: controllerName},
	})
	previous := policy.DeepCopy()
	policy.Status.Ancestors = mergeBackendTLSAncestors(policy, controllerName, map[types.NamespacedName]backendTLSPolicyConditions{
		holder: conditions, next: conditions, later: conditions,
	})
	require.Len(t, policy.Status.Ancestors, backendtls.MaxAncestors)
	require.Equal(t, foreign, policy.Status.Ancestors[:len(foreign)])
	slot := func(key types.NamespacedName) backendtls.AncestorSlot {
		return (backendtls.GatewayAncestor{Key: key, ControllerName: controllerName}).Slot(policy)
	}
	require.Equal(t, backendtls.AncestorSlotHeld, slot(holder))
	require.Equal(t, backendtls.AncestorSlotHeld, slot(next))
	require.Equal(t, backendtls.AncestorSlotFull, slot(stale))
	require.Equal(t, backendtls.AncestorSlotFull, slot(later))
	require.EqualValues(t, 2, meta.FindStatusCondition(policy.Status.Ancestors[len(foreign)+1].Conditions, "Accepted").ObservedGeneration)

	predicate := f.reconciler.backendTLSRoutePredicate()
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: previous, ObjectNew: policy}))
	conditionOnly := policy.DeepCopy()
	conditionOnly.Status.Ancestors[0].Conditions[0].Message = "External condition changed"
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: policy, ObjectNew: conditionOnly}))
	conditionOnly.Status.Ancestors = conditionOnly.Status.Ancestors[1:]
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: policy, ObjectNew: conditionOnly}))
}

func TestBackendTLSPolicyStatusCombinesHTTPAndTLSRouteAncestors(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.BackendTLSPolicy, f.reconciler.Config.TCPTLSTermination = true, true
	f.reconciler.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
	tcpSupported := false
	f.reconciler.TCPBackendTLSAvailable = func(context.Context) (bool, error) { return tcpSupported, nil }
	tlsGateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.gateway.Namespace, Name: "tls", UID: "tls-gateway"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: f.gateway.Spec.GatewayClassName, Listeners: []gatewayv1.Listener{{
			Name: "terminate", Port: 8443, Protocol: gatewayv1.TLSProtocolType,
			Hostname: ptr.To(gatewayv1.Hostname("tls.example.com")),
			TLS:      &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModeTerminate), CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "frontend"}}},
		}}},
	}
	tlsRoute := &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: tlsGateway.Namespace, Name: "tls", UID: "tls-route"},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(tlsGateway.Name)}}},
			Hostnames:       []gatewayv1.Hostname{"tls.example.com"},
			Rules:           []gatewayv1.TLSRouteRule{{BackendRefs: []gatewayv1.BackendRef{f.route.Spec.Rules[0].BackendRefs[0].BackendRef}}},
		},
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	require.NoError(t, err)
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "shared-ca", UID: "shared-ca-uid"},
		Data: map[string]string{"ca.crt": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}}
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "shared-policy", UID: "shared-policy-uid", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "abc.example.com", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name)}},
			},
		},
	}
	f.create(t, tlsGateway, tlsRoute, ca, policy)
	key := client.ObjectKeyFromObject(policy)
	require.NoError(t, f.client.Get(t.Context(), key, policy))
	foreign := gatewayv1.PolicyAncestorStatus{ControllerName: "foreign.example.com/controller", AncestorRef: gatewayv1.ParentReference{Name: "foreign"}}
	policy.Status.Ancestors = []gatewayv1.PolicyAncestorStatus{foreign}
	require.NoError(t, f.client.Status().Update(t.Context(), policy))
	reconcile := func() map[gatewayv1.ObjectName][]metav1.Condition {
		t.Helper()
		status := backendTLSPolicyStatusReconciler{routes: f.reconciler}
		_, err := status.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, policy))
		require.Equal(t, foreign, policy.Status.Ancestors[0])
		conditions := map[gatewayv1.ObjectName][]metav1.Condition{}
		for _, ancestor := range policy.Status.Ancestors[1:] {
			conditions[ancestor.AncestorRef.Name] = ancestor.Conditions
		}
		return conditions
	}
	httpName, tlsName := gatewayv1.ObjectName(f.gateway.Name), gatewayv1.ObjectName(tlsGateway.Name)
	conditions := reconcile()
	require.Len(t, conditions, 2)
	require.True(t, meta.IsStatusConditionTrue(conditions[httpName], string(gatewayv1.PolicyConditionAccepted)))
	require.True(t, meta.IsStatusConditionFalse(conditions[tlsName], string(gatewayv1.PolicyConditionAccepted)))
	require.True(t, meta.IsStatusConditionTrue(conditions[tlsName], string(gatewayv1.BackendTLSPolicyConditionResolvedRefs)))

	tcpSupported = true
	conditions = reconcile()
	require.True(t, meta.IsStatusConditionTrue(conditions[tlsName], string(gatewayv1.PolicyConditionAccepted)))
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(tlsGateway), tlsGateway))
	tlsGateway.Spec.TLS = &gatewayv1.GatewayTLSConfig{Backend: &gatewayv1.GatewayBackendTLS{
		ClientCertificateRef: &gatewayv1.SecretObjectReference{Name: "client"},
	}}
	require.NoError(t, f.client.Update(t.Context(), tlsGateway))
	conditions = reconcile()
	rejected := meta.FindStatusCondition(conditions[tlsName], string(gatewayv1.PolicyConditionAccepted))
	require.NotNil(t, rejected)
	require.Equal(t, metav1.ConditionFalse, rejected.Status)
	require.Equal(t, "BackendClientCertificateUnsupported", rejected.Reason)
	require.True(t, meta.IsStatusConditionTrue(conditions[httpName], string(gatewayv1.PolicyConditionAccepted)))

	tlsGateway.Spec.TLS = nil
	require.NoError(t, f.client.Update(t.Context(), tlsGateway))
	conditions = reconcile()
	require.True(t, meta.IsStatusConditionTrue(conditions[tlsName], string(gatewayv1.PolicyConditionAccepted)))

	require.NoError(t, f.client.Delete(t.Context(), tlsRoute))
	conditions = reconcile()
	require.Len(t, conditions, 1)
	require.True(t, meta.IsStatusConditionTrue(conditions[httpName], string(gatewayv1.PolicyConditionAccepted)))
}
