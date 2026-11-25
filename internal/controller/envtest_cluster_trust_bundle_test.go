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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/utils/kubernetes"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

func testClusterTrustBundleLifecycle(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider, certificateMap string) {
	t.Helper()
	ctx := t.Context()
	const name, hostname, group, kind = "cluster-trust-bundle", "api.example.test", "certificates.k8s.io", "ClusterTrustBundle"
	parameters := envtestClassParameters(name)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	secret := kubernetes.MustCreateSelfSignedCertSecret(t, name, "web-cert", []string{hostname})
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "edge"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(name),
			AllowedListeners: &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromSame)}},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80},
				{Name: "https", Protocol: gatewayv1.HTTPSProtocolType, Port: 443, Hostname: ptr.To(gatewayv1.Hostname(hostname)),
					TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: gatewayv1.ObjectName(secret.Name)}}}},
			},
			TLS: &gatewayv1.GatewayTLSConfig{Frontend: &gatewayv1.FrontendTLSConfig{
				Default: gatewayv1.TLSConfig{Validation: &gatewayv1.FrontendTLSValidation{
					CACertificateRefs: []gatewayv1.ObjectReference{{Group: group, Kind: kind, Name: name}},
				}},
			}},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "api"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "secure", Port: 80, AppProtocol: ptr.To("HTTPS")}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: name, Name: "api", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("secure"), Port: ptr.To(int32(8443))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.123"}}},
	}
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "upstream"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(service.Name)},
				SectionName:                ptr.To(gatewayv1.SectionName("secure")),
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: hostname, CACertificateRefs: []gatewayv1.LocalObjectReference{{Group: group, Kind: kind, Name: name}},
			},
		},
	}
	route := envtestHTTPRoute(name, "route", gateway.Name, service.Name)
	route.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("http"))

	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, parameters, class, secret,
	} {
		require.NoError(t, c.Create(ctx, object))
	}
	enrollEnvtestServingCertificateForClass(t, cfg, certificateMap, class.Name, secret)

	scoped := gateway.DeepCopy()
	scoped.Name = "namespaced-reference"
	scoped.Spec.TLS.Frontend.Default.Validation.CACertificateRefs[0].Namespace = ptr.To(gatewayv1.Namespace(name))
	err := c.Create(ctx, scoped, client.DryRunAll)
	if cfg.GatewayAPIChannel == "experimental" {
		require.True(t, apierrors.IsInvalid(err), "Experimental must reject a namespace on ClusterTrustBundle: %v", err)
	} else {
		require.NoError(t, err, "Standard admits the object and leaves namespace checking to the controller")
	}

	for _, object := range []client.Object{gateway, service, endpoints, policy, route} {
		require.NoError(t, c.Create(ctx, object))
	}
	waitState := func(label, uid, pem, invalidReason string) string {
		t.Helper()
		var digest string
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var edge gatewayv1.Gateway
			if err := c.Get(ctx, client.ObjectKeyFromObject(gateway), &edge); err != nil {
				return false, err
			}
			var upstream gatewayv1.BackendTLSPolicy
			if err := c.Get(ctx, client.ObjectKeyFromObject(policy), &upstream); err != nil {
				return false, err
			}
			check := func(conditions []metav1.Condition, generation int64) bool {
				accepted := meta.FindStatusCondition(conditions, "Accepted")
				resolved := meta.FindStatusCondition(conditions, "ResolvedRefs")
				if accepted == nil || resolved == nil || accepted.ObservedGeneration != generation || resolved.ObservedGeneration != generation {
					return false
				}
				if uid != "" {
					return accepted.Status == metav1.ConditionTrue && resolved.Status == metav1.ConditionTrue
				}
				return accepted.Status == metav1.ConditionFalse && accepted.Reason == "NoValidCACertificate" &&
					resolved.Status == metav1.ConditionFalse && resolved.Reason == invalidReason
			}
			if !slices.ContainsFunc(edge.Status.Listeners, func(listener gatewayv1.ListenerStatus) bool {
				return listener.Name == "https" && check(listener.Conditions, edge.Generation) &&
					meta.IsStatusConditionTrue(listener.Conditions, "Programmed") == (uid != "")
			}) || !slices.ContainsFunc(upstream.Status.Ancestors, func(row gatewayv1.PolicyAncestorStatus) bool {
				return row.ControllerName == cfg.ControllerName && row.AncestorRef.Name == gatewayv1.ObjectName(gateway.Name) &&
					check(row.Conditions, upstream.Generation)
			}) {
				return false, nil
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, spec := range slices.Backward(provider.graphSpecs) {
				if spec.Identity.GatewayUID != string(gateway.UID) || spec.Gateway.Disposition != cloud.GraphEnsure {
					continue
				}
				frontend := slices.ContainsFunc(spec.Gateway.Spec.ScopedListeners, func(listener cloud.ScopedListener) bool {
					if listener.Port != 443 || listener.TLS == nil {
						return false
					}
					tls := listener.TLS
					if uid == "" {
						return tls.CertificateRef == "" && tls.ClientValidation == nil
					}
					validation := tls.ClientValidation
					if tls.CertificateRef != envtestServingCertificateRef || validation == nil ||
						validation.ConfigMapUID != uid || string(validation.PEM) != pem || validation.InsecureFallback {
						return false
					}
					digest = validation.Digest
					return digest != ""
				})
				backend := slices.ContainsFunc(spec.Routes, func(node cloud.RouteGraphNode) bool {
					if node.Disposition != cloud.GraphEnsure || node.Spec.Identity.RouteUID != string(route.UID) || node.Spec.BackendTLS == nil {
						return false
					}
					tls := node.Spec.BackendTLS
					if uid == "" {
						return tls.Denied && len(node.Spec.Members) == 0
					}
					return !tls.Denied && tls.CAConfigMapUID == uid && tls.CADigest == digest && tls.Hostname == hostname &&
						string(tls.CAPEM) == pem && slices.Equal(node.Spec.Members, []cloud.Member{{Address: "192.0.2.123", Port: 8443}})
				})
				return frontend && backend, nil
			}
			return false, nil
		})
		t.Logf("%s: Gateway listener, BackendTLSPolicy and provider inputs agree", label)
		return digest
	}
	newBundle := func(pem string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": group + "/v1beta1", "kind": kind,
			"metadata": map[string]any{"name": name},
			"spec":     map[string]any{"trustBundle": pem},
		}}
	}

	waitState("missing", "", "", "InvalidCACertificateRef")
	first, second := backendTLSTestCA(t), backendTLSTestCA(t)
	bundle := newBundle(first)
	require.NoError(t, c.Create(ctx, bundle))
	firstUID := string(bundle.GetUID())
	firstDigest := waitState("creation", firstUID, first, "")
	require.NoError(t, unstructured.SetNestedField(bundle.Object, second, "spec", "trustBundle"))
	require.NoError(t, c.Update(ctx, bundle))
	secondDigest := waitState("rotation", firstUID, second, "")
	require.NotEqual(t, firstDigest, secondDigest)
	require.NoError(t, c.Delete(ctx, bundle))
	waitState("deletion", "", "", "InvalidCACertificateRef")
	bundle = newBundle(second)
	require.NoError(t, c.Create(ctx, bundle))
	recreatedUID := string(bundle.GetUID())
	require.NotEqual(t, firstUID, recreatedUID)
	require.Equal(t, secondDigest, waitState("same-name recreation", recreatedUID, second, ""))

	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "delegated"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: gatewayv1.ObjectName(gateway.Name)},
			Listeners: []gatewayv1.ListenerEntry{{
				Name: "delegated", Port: 8443, Protocol: gatewayv1.HTTPSProtocolType, Hostname: ptr.To(gatewayv1.Hostname(hostname)),
				TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: gatewayv1.ObjectName(secret.Name)}}},
			}},
		},
	}
	require.NoError(t, c.Create(ctx, set))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(set), set); err != nil {
			return false, err
		}
		return len(set.Status.Listeners) == 1 && meta.IsStatusConditionTrue(set.Status.Listeners[0].Conditions, "Programmed"), nil
	})

	var oversized strings.Builder
	for oversized.Len() <= cloud.MaxCAPEMBytes {
		oversized.WriteString(backendTLSTestCA(t))
	}
	require.NoError(t, unstructured.SetNestedField(bundle.Object, oversized.String(), "spec", "trustBundle"))
	require.NoError(t, c.Update(ctx, bundle), "the bundle exceeds only Barbican's limit")
	waitState("Barbican payload limit", "", "", backendtls.CABundleTooLargeReason)
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(set), set); err != nil {
			return false, err
		}
		if len(set.Status.Listeners) != 1 {
			return false, nil
		}
		conditions := set.Status.Listeners[0].Conditions
		accepted, resolved := meta.FindStatusCondition(conditions, "Accepted"), meta.FindStatusCondition(conditions, "ResolvedRefs")
		return accepted != nil && accepted.Status == metav1.ConditionFalse && accepted.Reason == "NoValidCACertificate" &&
			resolved != nil && resolved.Status == metav1.ConditionFalse && resolved.Reason == backendtls.CABundleTooLargeReason, nil
	})
}

func testClusterTrustBundleWatchDenied(t *testing.T, c client.Client, cfg controller.Config, restricted *rest.Config) {
	t.Helper()
	ctx := t.Context()
	metadataClient, err := metadata.NewForConfig(restricted)
	require.NoError(t, err)
	resource := schema.GroupVersionResource{Group: "certificates.k8s.io", Version: "v1beta1", Resource: "clustertrustbundles"}
	_, err = metadataClient.Resource(resource).List(ctx, metav1.ListOptions{})
	require.True(t, apierrors.IsForbidden(err), "optional informer has no list access: %v", err)

	const name = "watch-denied"
	parameters := envtestClassParameters(name)
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "edge"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: name, Listeners: []gatewayv1.Listener{
			{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
		}},
	}
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, parameters, class, gateway,
	} {
		require.NoError(t, c.Create(ctx, object))
	}
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(gateway), gateway); err != nil {
			return false, err
		}
		return meta.IsStatusConditionTrue(gateway.Status.Conditions, "Programmed"), nil
	})
}
