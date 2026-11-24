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
	"io/fs"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func testGatewayInfrastructure(t *testing.T, c client.Client, provider *recordingProvider, className string) {
	t.Helper()
	// The upstream helper waits for actual application Pods in the namespace.
	// Envtest can run the published Gateway manifest and check its objects directly.
	manifest, err := fs.ReadFile(&conformance.Manifests, tests.GatewayInfrastructureMetadata.Manifests[0])
	require.NoError(t, err)
	var gateway gatewayv1.Gateway
	require.NoError(t, yaml.Unmarshal(manifest, &gateway))
	gateway.Spec.GatewayClassName = gatewayv1.ObjectName(className)
	require.NoError(t, c.Create(t.Context(), &gateway))
	gatewayKey := client.ObjectKeyFromObject(&gateway)
	serviceKey := client.ObjectKey{Namespace: gateway.Namespace, Name: "gateway-" + gateway.Name}
	sliceKey := client.ObjectKey{Namespace: gateway.Namespace, Name: serviceKey.Name + "-ipv4"}
	observe := func(ctx context.Context, service *corev1.Service, endpointSlice *discoveryv1.EndpointSlice) (bool, error) {
		for key, object := range map[client.ObjectKey]client.Object{serviceKey: service, sliceKey: endpointSlice} {
			if err := c.Get(ctx, key, object); err != nil {
				return false, client.IgnoreNotFound(err)
			}
		}
		return true, nil
	}

	waitReady := func() {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var current gatewayv1.Gateway
			if err := c.Get(ctx, gatewayKey, &current); err != nil {
				return false, err
			}
			accepted := meta.IsStatusConditionTrue(current.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
			programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
			return accepted && programmed != nil && programmed.Status == metav1.ConditionTrue &&
				programmed.ObservedGeneration == current.Generation && len(current.Status.Addresses) == 1 &&
				current.Status.Addresses[0].Value == recordingProviderVIP, nil
		})
	}

	var service corev1.Service
	var endpointSlice discoveryv1.EndpointSlice
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		return observe(ctx, &service, &endpointSlice)
	})
	waitReady()
	require.Equal(t, corev1.ClusterIPNone, service.Spec.ClusterIP)
	require.Empty(t, service.Spec.Selector)
	require.Len(t, service.Spec.Ports, 1)
	require.Equal(t, int32(8080), service.Spec.Ports[0].Port)
	require.Equal(t, corev1.ProtocolTCP, service.Spec.Ports[0].Protocol)
	require.Equal(t, service.Name, endpointSlice.Labels[discoveryv1.LabelServiceName])
	require.Equal(t, "gateway-api-openstack.github.io", endpointSlice.Labels[discoveryv1.LabelManagedBy])
	require.Equal(t, discoveryv1.AddressTypeIPv4, endpointSlice.AddressType)
	require.Len(t, endpointSlice.Ports, 1)
	require.Equal(t, service.Spec.Ports[0].Port, ptr.Deref(endpointSlice.Ports[0].Port, 0))
	require.Len(t, endpointSlice.Endpoints, 1)
	require.Equal(t, []string{recordingProviderVIP}, endpointSlice.Endpoints[0].Addresses)
	require.Equal(t, ptr.To(true), endpointSlice.Endpoints[0].Conditions.Ready)
	for _, object := range []client.Object{&service, &endpointSlice} {
		require.Equal(t, gateway.UID, metav1.GetControllerOf(object).UID)
		require.Equal(t, gateway.Name, object.GetLabels()[gatewayv1.GatewayNameLabelKey])
		require.Equal(t, "value2", object.GetLabels()["key2"])
		require.Equal(t, "value1", object.GetAnnotations()["key1"])
	}

	// Withdraw only the previous values the controller owns. An independent
	// editor can add another key or take over a previously propagated key.
	baseService := service.DeepCopy()
	service.Labels["other.example.org/keep"] = "outside"
	service.Annotations["key1"] = "outside"
	require.NoError(t, c.Patch(t.Context(), &service, client.MergeFrom(baseService)))
	require.NoError(t, c.Get(t.Context(), gatewayKey, &gateway))
	baseGateway := gateway.DeepCopy()
	gateway.Spec.Infrastructure = &gatewayv1.GatewayInfrastructure{Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
		"team":                        "cloud",
		gatewayv1.GatewayNameLabelKey: "ignored",
		discoveryv1.LabelServiceName:  "ignored",
		discoveryv1.LabelManagedBy:    "ignored",
	}}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "additional", Protocol: gatewayv1.HTTPProtocolType, Port: 8081})
	require.NoError(t, c.Patch(t.Context(), &gateway, client.MergeFrom(baseGateway)))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		ok, err := observe(ctx, &service, &endpointSlice)
		return ok && service.Labels["team"] == "cloud" && endpointSlice.Labels["team"] == "cloud" &&
			len(service.Spec.Ports) == 2 && len(endpointSlice.Ports) == 2, err
	})
	require.Equal(t, baseService.UID, service.UID)
	require.Equal(t, "outside", service.Labels["other.example.org/keep"])
	require.Equal(t, "outside", service.Annotations["key1"])
	require.NotContains(t, service.Labels, "key2")
	require.NotContains(t, endpointSlice.Labels, "key2")
	require.NotContains(t, endpointSlice.Annotations, "key1")
	require.Equal(t, gateway.Name, service.Labels[gatewayv1.GatewayNameLabelKey])
	require.Equal(t, service.Name, endpointSlice.Labels[discoveryv1.LabelServiceName])
	require.Equal(t, "gateway-api-openstack.github.io", endpointSlice.Labels[discoveryv1.LabelManagedBy])

	// A foreign slice must not be advertised under an already published
	// discovery name. Its addition alone queues the Gateway reconciliation.
	foreign := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: sliceKey.Namespace, Name: "foreign-slice", Labels: map[string]string{
			discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "other.example.org",
		}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"198.51.100.20"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
	}
	require.NoError(t, c.Create(t.Context(), foreign))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		err := c.Get(ctx, serviceKey, &corev1.Service{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	var currentForeign discoveryv1.EndpointSlice
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(foreign), &currentForeign))
	require.Equal(t, foreign.ResourceVersion, currentForeign.ResourceVersion)
	require.NoError(t, c.Delete(t.Context(), foreign))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		ok, err := observe(ctx, &service, &endpointSlice)
		return ok && service.UID != baseService.UID && len(endpointSlice.Endpoints) == 1, err
	})
	require.Contains(t, endpointSlice.OwnerReferences, metav1.OwnerReference{
		APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service", Name: service.Name, UID: service.UID,
	})
	recreatedUID := service.UID

	// A finalizer keeps the old slice in the API after deletion. Its DNS
	// address must be withdrawn even before the finalizer completes.
	baseSlice := endpointSlice.DeepCopy()
	endpointSlice.Finalizers = []string{"example.org/test-finalizer"}
	require.NoError(t, c.Patch(t.Context(), &endpointSlice, client.MergeFrom(baseSlice)))
	unavailable := cloud.GatewayProgressingResult("VIP is not assigned", time.Second)
	unavailable.AddressAbsent = true
	provider.mu.Lock()
	provider.gatewayResults = map[string]cloud.GatewayResult{string(gateway.UID): unavailable}
	provider.mu.Unlock()
	trigger := func(phase gatewayv1.LabelValue) {
		t.Helper()
		require.NoError(t, c.Get(t.Context(), gatewayKey, &gateway))
		base := gateway.DeepCopy()
		gateway.Spec.Infrastructure.Labels["phase"] = phase
		require.NoError(t, c.Patch(t.Context(), &gateway, client.MergeFrom(base)))
	}
	trigger("lost-vip")
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, sliceKey, &endpointSlice); err != nil {
			return false, err
		}
		return !endpointSlice.DeletionTimestamp.IsZero() && len(endpointSlice.Endpoints) == 0, nil
	})
	require.NoError(t, c.Get(t.Context(), serviceKey, &service))
	require.Equal(t, recreatedUID, service.UID)
	baseSlice = endpointSlice.DeepCopy()
	endpointSlice.Finalizers = nil
	require.NoError(t, c.Patch(t.Context(), &endpointSlice, client.MergeFrom(baseSlice)))
	provider.mu.Lock()
	delete(provider.gatewayResults, string(gateway.UID))
	provider.mu.Unlock()
	trigger("vip-restored")
	waitReady()
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		ok, err := observe(ctx, &service, &endpointSlice)
		return ok && len(endpointSlice.Endpoints) == 1, err
	})

	require.NoError(t, c.Delete(t.Context(), &gateway))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		serviceErr := c.Get(ctx, serviceKey, &corev1.Service{})
		sliceErr := c.Get(ctx, sliceKey, &discoveryv1.EndpointSlice{})
		if err := client.IgnoreNotFound(serviceErr); err != nil {
			return false, err
		}
		return apierrors.IsNotFound(serviceErr) && apierrors.IsNotFound(sliceErr), client.IgnoreNotFound(sliceErr)
	})
}
