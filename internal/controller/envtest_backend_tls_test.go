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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testBackendTLSPolicy(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	ctx := t.Context()
	const frontend, backend = "backend-tls-frontend", "backend-tls-service"
	parameters := envtestClassParameters(frontend)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: frontend},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: frontend, Name: "edge"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: gatewayv1.ObjectName(class.Name), Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "api"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "secure", Port: 80, AppProtocol: ptr.To("HTTPS")}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: backend, Name: "api", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("secure"), Port: ptr.To(int32(8443))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.101"}}},
	}
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "aa-tls"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(service.Name)},
				SectionName:                ptr.To(gatewayv1.SectionName("secure")),
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "api.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "trust"}},
			},
		},
	}
	peer := policy.DeepCopy()
	peer.Name, peer.Spec.Validation.Hostname = "zz-tls", "backup.example.test"
	route := envtestHTTPRoute(frontend, "consumer", gateway.Name, service.Name)
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace(backend))
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "allow"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: frontend}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(service.Name))}},
		},
	}
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: frontend}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: backend}},
		parameters, class, gateway, service, endpoints, policy, peer, route,
	} {
		require.NoError(t, c.Create(ctx, object))
	}

	routeKey := client.ObjectKeyFromObject(route)
	var waitingFor string
	defer func() {
		if t.Failed() {
			_ = c.Get(ctx, routeKey, route)
			t.Logf("waiting for %s, latest route status: %+v, policy: %+v, peer: %+v", waitingFor, route.Status, policy.Status, peer.Status)
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, graph := range slices.Backward(provider.graphSpecs) {
				for _, node := range graph.Routes {
					if node.Spec.Identity.RouteUID == string(route.UID) && node.Disposition == cloud.GraphEnsure {
						if tls := node.Spec.BackendTLS; tls != nil {
							t.Logf("latest backend TLS graph, denied=%t hostname=%s members=%d", tls.Denied, tls.Hostname, len(node.Spec.Members))
						}
						return
					}
				}
			}
		}
	}()
	waitResolvedRefs := func(reason gatewayv1.RouteConditionReason) {
		t.Helper()
		waitingFor = "route ResolvedRefs/" + string(reason)
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, routeKey, route); err != nil {
				return false, err
			}
			for _, parent := range route.Status.Parents {
				if parent.ControllerName == cfg.ControllerName {
					condition := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionResolvedRefs))
					return condition != nil && condition.ObservedGeneration == route.Generation && condition.Reason == string(reason), nil
				}
			}
			return false, nil
		})
	}
	waitPolicy := func(current *gatewayv1.BackendTLSPolicy, accepted, resolved gatewayv1.PolicyConditionReason) {
		t.Helper()
		key := client.ObjectKeyFromObject(current)
		waitingFor = current.Name + " Accepted/" + string(accepted) + " ResolvedRefs/" + string(resolved)
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, key, current); err != nil {
				return false, err
			}
			for _, ancestor := range current.Status.Ancestors {
				if ancestor.ControllerName == cfg.ControllerName && ancestor.AncestorRef.Name == gatewayv1.ObjectName(gateway.Name) &&
					ptr.Deref(ancestor.AncestorRef.Namespace, "") == frontend {
					a := meta.FindStatusCondition(ancestor.Conditions, string(gatewayv1.PolicyConditionAccepted))
					r := meta.FindStatusCondition(ancestor.Conditions, string(gatewayv1.BackendTLSPolicyConditionResolvedRefs))
					return a != nil && a.ObservedGeneration == current.Generation && a.Reason == string(accepted) &&
						r != nil && r.ObservedGeneration == current.Generation && r.Reason == string(resolved), nil
				}
			}
			return false, nil
		})
	}
	waitGraph := func(matches func(cloud.RouteSpec) bool) {
		t.Helper()
		waitForEnvtest(t, func(context.Context) (bool, error) {
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, graph := range slices.Backward(provider.graphSpecs) {
				for _, node := range graph.Routes {
					if node.Spec.Identity.RouteUID == string(route.UID) && node.Disposition == cloud.GraphEnsure {
						return matches(node.Spec), nil
					}
				}
			}
			return false, nil
		})
	}
	waitDenied := func() {
		t.Helper()
		waitingFor = "a denied backend graph"
		waitGraph(func(spec cloud.RouteSpec) bool {
			return spec.BackendTLS != nil && spec.BackendTLS.Denied && len(spec.Members) == 0
		})
	}
	waitTLS := func(hostname gatewayv1.PreciseHostname, source *corev1.ConfigMap) {
		t.Helper()
		waitingFor = "a TLS backend graph for " + string(hostname)
		waitGraph(func(spec cloud.RouteSpec) bool {
			tls := spec.BackendTLS
			return tls != nil && !tls.Denied && tls.Hostname == string(hostname) &&
				tls.CAConfigMapUID == string(source.UID) && tls.CADigest != "" && string(tls.CAPEM) == source.Data["ca.crt"] &&
				slices.Equal(spec.Members, []cloud.Member{{Address: "192.0.2.101", Port: 8443}})
		})
	}

	waitResolvedRefs(gatewayv1.RouteReasonRefNotPermitted)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(policy), policy))
	require.Empty(t, policy.Status.Ancestors)
	require.NoError(t, c.Create(ctx, grant))
	waitResolvedRefs(gatewayv1.RouteReasonResolvedRefs)
	waitDenied()
	waitPolicy(policy, gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)
	waitPolicy(peer, gatewayv1.PolicyReasonConflicted, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)

	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "trust"}, Data: map[string]string{"ca.crt": backendTLSTestCA(t)}}
	require.NoError(t, c.Create(ctx, ca))
	waitTLS(policy.Spec.Validation.Hostname, ca)
	waitPolicy(policy, gatewayv1.PolicyReasonAccepted, gatewayv1.BackendTLSPolicyReasonResolvedRefs)

	foreign := gatewayv1.PolicyAncestorStatus{
		ControllerName: "foreign.example.com/gateway",
		AncestorRef:    gatewayv1.ParentReference{Name: "foreign"},
		Conditions:     []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", Message: "External condition", ObservedGeneration: 1, LastTransitionTime: metav1.Now()}},
	}
	policy.Status.Ancestors = append(policy.Status.Ancestors, foreign)
	require.NoError(t, c.Status().Update(ctx, policy))
	policy.Spec.Validation.Hostname = "updated.example.test"
	require.NoError(t, c.Update(ctx, policy))
	waitTLS(policy.Spec.Validation.Hostname, ca)
	waitPolicy(policy, gatewayv1.PolicyReasonAccepted, gatewayv1.BackendTLSPolicyReasonResolvedRefs)
	require.True(t, slices.ContainsFunc(policy.Status.Ancestors, func(entry gatewayv1.PolicyAncestorStatus) bool {
		return entry.ControllerName == foreign.ControllerName && entry.AncestorRef.Name == foreign.AncestorRef.Name &&
			meta.IsStatusConditionTrue(entry.Conditions, "Accepted")
	}))

	ca.Data["ca.crt"] = backendTLSTestCA(t)
	require.NoError(t, c.Update(ctx, ca))
	waitTLS(policy.Spec.Validation.Hostname, ca)
	require.NoError(t, c.Delete(ctx, policy))
	waitTLS(peer.Spec.Validation.Hostname, ca)
	waitPolicy(peer, gatewayv1.PolicyReasonAccepted, gatewayv1.BackendTLSPolicyReasonResolvedRefs)
	require.NoError(t, c.Delete(ctx, ca))
	waitDenied()
	waitPolicy(peer, gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)

	service.Spec.Ports[0].Name, service.Spec.Ports[0].AppProtocol = "other", nil
	require.NoError(t, c.Update(ctx, service))
	waitingFor = "the Service port without a selected policy"
	waitGraph(func(spec cloud.RouteSpec) bool { return spec.BackendTLS == nil })
	waitPolicy(peer, gatewayv1.PolicyReasonTargetNotFound, gatewayv1.PolicyReasonTargetNotFound)
}

func backendTLSTestCA(t *testing.T) string {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(4_100_000_000, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
