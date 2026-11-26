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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testXBackendEndpointSelector(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	ctx := t.Context()
	const namespace = "xbackend-endpoints"
	const backendName = "backend"
	parameters := envtestClassParameters(namespace)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "edge"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: gatewayv1.ObjectName(class.Name), Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
	}
	backendKey := client.ObjectKey{Namespace: namespace, Name: backendName}
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: backendName},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8087},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "backend", "tier": "test"},
			}},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: backendName},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "web", Port: 8087}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: namespace, Name: backendName, Labels: map[string]string{discoveryv1.LabelServiceName: backendName}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("web"), Port: ptr.To(int32(9099))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.1.2.99"}}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "selected", Labels: map[string]string{"app": "backend", "tier": "test"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "example.invalid/http"}}},
	}
	route := envtestHTTPRoute(namespace, "selected", gateway.Name, backendName)
	ref := &route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
	for _, object := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, parameters, class, gateway, service, endpoints, backend, pod, route} {
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	routeKey := client.ObjectKeyFromObject(route)
	defer func() {
		if t.Failed() {
			t.Logf("latest XBackend status: %+v", backend.Status)
			t.Logf("latest HTTPRoute status: %+v", route.Status)
		}
	}()
	wantMembers := []cloud.Member{{Address: "10.1.2.8", Port: 8087}}
	var expectedTLS *cloud.BackendTLSValidation
	wantTCPMonitor := false
	waitForBackend := func(members []cloud.Member, reason gatewayv1.RouteConditionReason) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, routeKey, route); err != nil {
				return false, err
			}
			if len(route.Status.Parents) != 1 || route.Status.Parents[0].ControllerName != cfg.ControllerName {
				return false, nil
			}
			resolved := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
			if resolved == nil || resolved.ObservedGeneration != route.Generation || resolved.Reason != string(reason) {
				return false, nil
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, observation := range slices.Backward(provider.graphSpecs) {
				for _, node := range observation.Routes {
					if node.Spec.Identity.RouteUID == string(route.UID) && node.Disposition == cloud.GraphEnsure && len(node.Spec.HTTPBackends) == 1 {
						target := node.Spec.HTTPBackends[0]
						matchesTLS := target.BackendTLS == nil && expectedTLS == nil ||
							target.BackendTLS != nil && expectedTLS != nil && target.BackendTLS.Denied == expectedTLS.Denied &&
								target.BackendTLS.Hostname == expectedTLS.Hostname && target.BackendTLS.CAConfigMapUID == expectedTLS.CAConfigMapUID
						matchesMonitor := reason != gatewayv1.RouteReasonResolvedRefs || target.XBackendTCPHealthMonitor == wantTCPMonitor
						return target.Kind == cloud.HTTPBackendKindXBackend && slices.Equal(target.Members, members) && matchesTLS && matchesMonitor, nil
					}
				}
			}
			return false, nil
		})
	}
	waitForAccepted := func(status metav1.ConditionStatus, reason string) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, backendKey, backend); err != nil {
				return false, err
			}
			if len(backend.Status.Ancestors) != 1 || backend.Status.Ancestors[0].ControllerName != cfg.ControllerName ||
				backend.Status.Ancestors[0].AncestorRef.Name != gatewayv1.ObjectName(gateway.Name) {
				return false, nil
			}
			accepted := meta.FindStatusCondition(backend.Status.Ancestors[0].Conditions, "Accepted")
			return accepted != nil && accepted.Status == status && accepted.Reason == reason && accepted.ObservedGeneration == backend.Generation, nil
		})
	}
	waitForBackend(nil, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	pod.Status.PodIP = wantMembers[0].Address
	pod.Status.PodIPs = []corev1.PodIP{{IP: pod.Status.PodIP}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	delete(pod.Labels, "tier")
	if err := c.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitForBackend(nil, gatewayv1.RouteReasonResolvedRefs)
	pod.Labels["tier"] = "test"
	if err := c.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)

	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolTCP)
	if err := c.Update(ctx, backend); err != nil {
		t.Fatal(err)
	}
	waitForBackend(nil, gatewayv1.RouteReasonUnsupportedProtocol)
	waitForAccepted(metav1.ConditionFalse, "UnsupportedProtocol")
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolMCP)
	wantTCPMonitor = true
	if err := c.Update(ctx, backend); err != nil {
		t.Fatal(err)
	}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "backend-ca"}, Data: map[string]string{"ca.crt": backendTLSTestCA(t)}}
	require.NoError(t, c.Create(ctx, ca))
	backend.Spec.TLS = &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeServerOnly, Validation: &gatewayv1.BackendTLSPolicyValidation{
		Hostname: "backend.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name)}},
	}}
	require.NoError(t, c.Update(ctx, backend))
	expectedTLS = &cloud.BackendTLSValidation{Hostname: "backend.example.test", CAConfigMapUID: string(ca.UID)}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	backend.Spec.TLS.Validation.Hostname = "rotated.example.test"
	require.NoError(t, c.Update(ctx, backend))
	expectedTLS.Hostname = "rotated.example.test"
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	require.NoError(t, c.Delete(ctx, ca))
	expectedTLS = &cloud.BackendTLSValidation{Denied: true}
	waitForBackend(nil, gatewayv1.RouteConditionReason(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef))
	waitForAccepted(metav1.ConditionFalse, string(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate))

	ca.ObjectMeta = metav1.ObjectMeta{Namespace: namespace, Name: ca.Name}
	require.NoError(t, c.Create(ctx, ca))
	expectedTLS = &cloud.BackendTLSValidation{Hostname: "rotated.example.test", CAConfigMapUID: string(ca.UID)}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	if cfg.ClusterTrustBundles != nil {
		bundle := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "certificates.k8s.io/v1beta1",
			"kind":       "ClusterTrustBundle",
			"metadata":   map[string]any{"name": "xbackend-ca"},
			"spec":       map[string]any{"trustBundle": ca.Data["ca.crt"]},
		}}
		require.NoError(t, c.Create(ctx, bundle))
		backend.Spec.TLS.Validation.CACertificateRefs = []gatewayv1.LocalObjectReference{{
			Group: "certificates.k8s.io", Kind: "ClusterTrustBundle", Name: gatewayv1.ObjectName(bundle.GetName()),
		}}
		require.NoError(t, c.Update(ctx, backend))
		expectedTLS = &cloud.BackendTLSValidation{Hostname: "rotated.example.test", CAConfigMapUID: string(bundle.GetUID())}
		waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
		waitForAccepted(metav1.ConditionTrue, "Accepted")

		require.NoError(t, c.Delete(ctx, bundle))
		expectedTLS = &cloud.BackendTLSValidation{Denied: true}
		waitForBackend(nil, gatewayv1.RouteConditionReason(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef))
		waitForAccepted(metav1.ConditionFalse, string(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate))
	}

	backend.Spec.TLS = &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeNone}
	require.NoError(t, c.Update(ctx, backend))
	expectedTLS = nil
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	waitForAccepted(metav1.ConditionTrue, "Accepted")

	foreign := gatewayx.BackendAncestorStatus{
		ControllerName: "example.net/other-controller", AncestorRef: gatewayv1.ParentReference{Name: "external"},
	}
	backend.Status.Ancestors = append(backend.Status.Ancestors, foreign)
	if err := c.Status().Update(ctx, backend); err != nil {
		t.Fatal(err)
	}
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("default"))
	if err := c.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	waitForBackend(nil, gatewayv1.RouteReasonRefNotPermitted)
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, backendKey, backend); err != nil {
			return false, err
		}
		return len(backend.Status.Ancestors) == 1 && backend.Status.Ancestors[0].ControllerName == foreign.ControllerName &&
			backend.Status.Ancestors[0].AncestorRef.Name == foreign.AncestorRef.Name, nil
	})
	route.Spec.Rules[0].BackendRefs[0].Namespace = nil
	if err := c.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	waitForBackend(wantMembers, gatewayv1.RouteReasonResolvedRefs)
	if err := c.Delete(ctx, backend); err != nil {
		t.Fatal(err)
	}
	waitForBackend(nil, gatewayv1.RouteReasonBackendNotFound)
	if err := c.Get(ctx, backendKey, &gatewayx.XBackend{}); !apierrors.IsNotFound(err) {
		t.Fatalf("XBackend did not disappear: %v", err)
	}
}
