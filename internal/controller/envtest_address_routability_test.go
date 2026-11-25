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
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testGatewayAddressRoutability(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider, className string) {
	t.Helper()
	const sentinel gatewayv1.GatewayAddressRoutabilityType = "testing.x-k8s.io/sentinel"
	defaultScope := ptr.To(gatewayv1.GatewayAddressRoutabilityDefault)
	key := client.ObjectKey{Namespace: suite.InfrastructureNamespace, Name: "gateway-address-routability"}
	class := &gatewayv1.GatewayClass{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: className}, class); err != nil {
		t.Fatal(err)
	}
	var wantFeatures []gatewayv1.SupportedFeature
	if cfg.GatewayAddressRoutabilityEnabled() {
		wantFeatures = []gatewayv1.SupportedFeature{{Name: gatewayv1.FeatureName(features.SupportGatewayAddressRoutability)}}
	}
	if cfg.GatewayAddressRoutabilityClusterEnabled() {
		wantFeatures = append(wantFeatures, gatewayv1.SupportedFeature{Name: gatewayv1.FeatureName(features.SupportGatewayAddressRoutabilityCluster)})
	}
	if cfg.GatewayInfrastructureMetadata {
		wantFeatures = append(wantFeatures, gatewayv1.SupportedFeature{Name: gatewayv1.FeatureName(features.SupportGatewayInfrastructure)})
	}
	if !slices.Equal(class.Status.SupportedFeatures, wantFeatures) {
		t.Fatalf("published GatewayClass features = %+v, want %+v", class.Status.SupportedFeatures, wantFeatures)
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(className),
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
			Addresses:        []gatewayv1.GatewaySpecAddress{{Routability: sentinel}},
		},
	}
	if !cfg.GatewayAddressRoutabilityEnabled() {
		gateway.Spec.Addresses[0].Routability = gatewayv1.GatewayAddressRoutabilityCluster
	}
	if err := c.Create(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	if !cfg.GatewayAddressRoutabilityEnabled() {
		programmed := waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, nil, "", gatewayv1.GatewayReasonProgrammed)
		if programmed.Spec.Addresses[0].Routability != "" {
			t.Fatalf("Standard API did not prune routability: %+v", programmed.Spec.Addresses)
		}
		return
	}

	waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, ptr.To(sentinel), gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)
	patchAddresses := func(value string) {
		t.Helper()
		if err := c.Patch(t.Context(), gateway, client.RawPatch(types.MergePatchType, []byte(value))); err != nil {
			t.Fatal(err)
		}
	}
	// The existing private VIP matches the fixed request. The automatic
	// default and sentinel requests cannot both use that same endpoint.
	patchAddresses(`{"spec":{"addresses":[{"routability":"testing.x-k8s.io/sentinel"},{"routability":""},{"value":"192.0.2.10"},{"routability":"example.com/unknown"}]}}`)
	partial := waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, defaultScope, gatewayv1.GatewayReasonAddressesPartiallyAssigned, gatewayv1.GatewayReasonProgrammed)
	message := meta.FindStatusCondition(partial.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned)).Message
	for _, unmet := range []string{"spec.addresses[0]", "testing.x-k8s.io/sentinel", "spec.addresses[1]", "spec.addresses[3]", "example.com/unknown"} {
		if !strings.Contains(message, unmet) || strings.Contains(message, "spec.addresses[2]") {
			t.Fatalf("partial assignment message %q does not enumerate exactly the unsatisfied requests", message)
		}
	}

	patchAddresses(`{"spec":{"addresses":[{"routability":"example.com/unknown"},{"routability":"testing.x-k8s.io/other"}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, defaultScope, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonProgrammed)
	// Simulate provider unassignment. An unsupported scope must not replace
	// the missing VIP with a new default address.
	provider.mu.Lock()
	delete(provider.gatewayStates, string(gateway.UID))
	provider.mu.Unlock()
	patchAddresses(`{"spec":{"addresses":[{"routability":"example.com/unknown"}]}}`)
	waitGatewayAddressAssignment(t, c, key, "", nil, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonAddressNotAssigned)

	// A new static sentinel can be acquired after the previous address is gone.
	patchAddresses(`{"spec":{"addresses":[{"value":"192.0.2.30","routability":"testing.x-k8s.io/sentinel"}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, ptr.To(sentinel), gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)

	// Explicit empty routability restores the default on the same VIP.
	patchAddresses(`{"spec":{"addresses":[{"routability":""}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, defaultScope, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)

	patchAddresses(`{"spec":{"addresses":null}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, defaultScope, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)
}

func testGatewayClusterAddresses(t *testing.T, c client.Client, provider *recordingProvider, className string) {
	t.Helper()
	ctx := t.Context()
	cluster := gatewayv1.GatewaySpecAddress{Routability: gatewayv1.GatewayAddressRoutabilityCluster}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: suite.InfrastructureNamespace, Name: "gateway-cluster-address"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(className),
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 8080}},
			Addresses:        []gatewayv1.GatewaySpecAddress{cluster, {}},
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"service.kubernetes.io/service-proxy-name": "disabled"},
			},
		},
	}
	require.NoError(t, c.Create(ctx, gateway))
	key := client.ObjectKeyFromObject(gateway)
	waitForAddresses := func(count int, reason gatewayv1.GatewayConditionReason) *gatewayv1.Gateway {
		t.Helper()
		var actual gatewayv1.Gateway
		err := wait.PollUntilContextTimeout(ctx, 25*time.Millisecond, envtestWaitTimeout, true, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, key, &actual); err != nil {
				return false, err
			}
			assignment := meta.FindStatusCondition(actual.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
			programmed := meta.FindStatusCondition(actual.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
			return assignment != nil && assignment.ObservedGeneration == actual.Generation && assignment.Reason == string(reason) &&
				programmed != nil && programmed.ObservedGeneration == actual.Generation && programmed.Status == metav1.ConditionTrue &&
				len(actual.Status.Addresses) == count, nil
		})
		require.NoErrorf(t, err, "Gateway status: %+v", actual.Status)
		return &actual
	}
	checkPublication := func(actual *gatewayv1.Gateway, expectedIP string) corev1.Service {
		t.Helper()
		var clusterIP string
		for _, address := range actual.Status.Addresses {
			require.NotNil(t, address.Routability)
			if *address.Routability == gatewayv1.GatewayAddressRoutabilityCluster {
				if expectedIP == "" || expectedIP == address.Value {
					clusterIP = address.Value
				}
			} else {
				require.Equal(t, gatewayv1.GatewayAddressRoutabilityDefault, *address.Routability)
				require.Equal(t, recordingProviderVIP, address.Value)
			}
		}
		require.NotEmpty(t, clusterIP)
		require.NotEqual(t, recordingProviderVIP, clusterIP)
		var services corev1.ServiceList
		require.NoError(t, c.List(ctx, &services, client.InNamespace(key.Namespace),
			client.MatchingLabels{gatewayv1.GatewayNameLabelKey: key.Name}))
		var published corev1.Service
		for _, svc := range services.Items {
			if svc.Spec.ClusterIP == clusterIP {
				published = svc
			}
		}
		require.NotEmpty(t, published.Name)
		require.Equal(t, corev1.ServiceTypeClusterIP, published.Spec.Type)
		require.Empty(t, published.Spec.Selector)
		require.NotContains(t, published.Labels, "service.kubernetes.io/service-proxy-name")
		require.Equal(t, corev1.ServiceInternalTrafficPolicyCluster, ptr.Deref(published.Spec.InternalTrafficPolicy, ""))
		require.Equal(t, []corev1.ServicePort{{Name: "tcp-8080", Port: 8080, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP}}, published.Spec.Ports)
		var endpoints discoveryv1.EndpointSliceList
		require.NoError(t, c.List(ctx, &endpoints, client.InNamespace(key.Namespace),
			client.MatchingLabels{discoveryv1.LabelServiceName: published.Name}))
		require.Len(t, endpoints.Items, 1)
		require.NotContains(t, endpoints.Items[0].Labels, corev1.IsHeadlessService)
		require.Len(t, endpoints.Items[0].Endpoints, 1)
		require.Equal(t, []string{recordingProviderVIP}, endpoints.Items[0].Endpoints[0].Addresses)
		require.True(t, ptr.Deref(endpoints.Items[0].Endpoints[0].Conditions.Ready, false))
		require.Equal(t, []discoveryv1.EndpointPort{{Name: ptr.To("tcp-8080"), Port: ptr.To(int32(8080)), Protocol: ptr.To(corev1.ProtocolTCP)}}, endpoints.Items[0].Ports)
		return published
	}
	patch := func(addresses ...gatewayv1.GatewaySpecAddress) {
		t.Helper()
		var current gatewayv1.Gateway
		require.NoError(t, c.Get(ctx, key, &current))
		before := current.DeepCopy()
		current.Spec.Addresses = addresses
		require.NoError(t, c.Patch(ctx, &current, client.MergeFrom(before)))
	}
	absent := func(service corev1.Service) {
		t.Helper()
		require.NoError(t, wait.PollUntilContextTimeout(ctx, 25*time.Millisecond, envtestWaitTimeout, true, func(ctx context.Context) (bool, error) {
			var actual corev1.Service
			err := c.Get(ctx, client.ObjectKeyFromObject(&service), &actual)
			return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
		}))
		var endpoints discoveryv1.EndpointSliceList
		require.NoError(t, c.List(ctx, &endpoints, client.InNamespace(service.Namespace),
			client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}))
		require.Empty(t, endpoints.Items)
	}

	allocated := checkPublication(waitForAddresses(2, gatewayv1.GatewayReasonAddressesAssigned), "")
	patch(gatewayv1.GatewaySpecAddress{}, cluster)
	reordered := checkPublication(waitForAddresses(2, gatewayv1.GatewayReasonAddressesAssigned), allocated.Spec.ClusterIP)
	require.Equal(t, allocated.UID, reordered.UID)
	require.Equal(t, allocated.Spec.ClusterIP, reordered.Spec.ClusterIP)
	drifted := reordered.DeepCopy()
	drifted.Labels["service.kubernetes.io/service-proxy-name"] = "disabled"
	drifted.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyLocal)
	require.NoError(t, c.Patch(ctx, drifted, client.MergeFrom(&reordered)))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 25*time.Millisecond, envtestWaitTimeout, true, func(ctx context.Context) (bool, error) {
		var current corev1.Service
		if err := c.Get(ctx, client.ObjectKeyFromObject(drifted), &current); err != nil {
			return false, err
		}
		_, hasProxyName := current.Labels["service.kubernetes.io/service-proxy-name"]
		return current.UID == reordered.UID && !hasProxyName &&
			ptr.Deref(current.Spec.InternalTrafficPolicy, "") == corev1.ServiceInternalTrafficPolicyCluster, nil
	}))
	var headless corev1.Service
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: "gateway-" + key.Name}, &headless))
	require.Equal(t, corev1.ClusterIPNone, headless.Spec.ClusterIP)

	pin := gatewayv1.GatewaySpecAddress{Value: allocated.Spec.ClusterIP, Routability: cluster.Routability}
	patch(cluster, pin)
	pair := waitForAddresses(2, gatewayv1.GatewayReasonAddressesAssigned)
	pinned := checkPublication(pair, pin.Value)
	require.Equal(t, allocated.UID, pinned.UID)
	secondIP := pair.Status.Addresses[0].Value
	require.NotEqual(t, pin.Value, secondIP)
	second := checkPublication(pair, secondIP)
	patch(pin, cluster)
	pair = waitForAddresses(2, gatewayv1.GatewayReasonAddressesAssigned)
	require.Equal(t, pinned.UID, checkPublication(pair, pin.Value).UID)
	require.Equal(t, second.UID, checkPublication(pair, secondIP).UID)

	cluster.Value = "10.0.0.7"
	patch(cluster)
	fixed := checkPublication(waitForAddresses(1, gatewayv1.GatewayReasonAddressesAssigned), cluster.Value)
	require.Equal(t, cluster.Value, fixed.Spec.ClusterIP)
	absent(allocated)
	cluster.Value = "198.51.100.22"
	patch(cluster)
	waitGatewayAddressAssignment(t, c, key, fixed.Spec.ClusterIP, ptr.To(cluster.Routability), gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonProgrammed)
	unmatchedFamily := gatewayv1.GatewaySpecAddress{Value: "2001:db8::22", Routability: cluster.Routability}
	patch(cluster, gatewayv1.GatewaySpecAddress{}, unmatchedFamily)
	partial := waitForAddresses(1, gatewayv1.GatewayReasonAddressesPartiallyAssigned)
	require.Equal(t, recordingProviderVIP, partial.Status.Addresses[0].Value)
	assignment := meta.FindStatusCondition(partial.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	require.Contains(t, assignment.Message, "spec.addresses[0]")
	require.Contains(t, assignment.Message, "spec.addresses[2]")
	absent(fixed)

	foreign := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "unrelated-service"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.0.0.8", Ports: []corev1.ServicePort{{Port: 80}}},
	}
	require.NoError(t, c.Create(ctx, foreign))
	// A new Gateway has no prior active default address to retain.
	conflicting := gateway.DeepCopy()
	conflicting.ObjectMeta = metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name + "-conflict"}
	cluster.Value = foreign.Spec.ClusterIP
	conflicting.Spec.Addresses = []gatewayv1.GatewaySpecAddress{cluster}
	require.NoError(t, c.Create(ctx, conflicting))
	waitGatewayAddressAssignment(t, c, client.ObjectKeyFromObject(conflicting), "", nil, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonAddressNotAssigned)
	provider.mu.Lock()
	_, programmed := provider.gatewayStates[string(conflicting.UID)]
	provider.mu.Unlock()
	require.False(t, programmed)
	var existing corev1.Service
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), &existing))
	require.Equal(t, foreign.UID, existing.UID)
	require.Equal(t, foreign.Spec, existing.Spec)
	require.NoError(t, c.Delete(ctx, foreign))
	recovered := waitGatewayAddressAssignment(t, c, client.ObjectKeyFromObject(conflicting), existing.Spec.ClusterIP,
		ptr.To(cluster.Routability), gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)
	require.Equal(t, conflicting.Generation, recovered.Generation)

	cluster.Value = ""
	patch(cluster)
	assigned := waitForAddresses(1, gatewayv1.GatewayReasonAddressesAssigned)
	active := checkPublication(assigned, "")
	serving := func() bool {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		return provider.gatewayStates[string(gateway.UID)].ListenerID != ""
	}
	require.True(t, serving())
	foreignSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "foreign-cluster-address",
			Labels: map[string]string{discoveryv1.LabelServiceName: active.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.199"}}},
	}
	require.NoError(t, c.Create(ctx, foreignSlice))
	waitGatewayAddressAssignment(t, c, key, "", nil, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonAddressNotAssigned)
	require.Eventually(t, func() bool { return !serving() }, envtestWaitTimeout, 25*time.Millisecond)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(&active), &corev1.Service{})))
	var untouched discoveryv1.EndpointSlice
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreignSlice), &untouched))
	require.Equal(t, foreignSlice.UID, untouched.UID)
	require.Equal(t, foreignSlice.Endpoints, untouched.Endpoints)
	require.NoError(t, c.Delete(ctx, foreignSlice))
	resumed := waitForAddresses(1, gatewayv1.GatewayReasonAddressesAssigned)
	require.Equal(t, assigned.Generation, resumed.Generation)
	active = checkPublication(resumed, "")
	require.True(t, serving())
	require.NoError(t, c.Delete(ctx, gateway))
	absent(active)
}

func waitGatewayAddressAssignment(t *testing.T, c client.Client, key client.ObjectKey, value string, scope *gatewayv1.GatewayAddressRoutabilityType, assignmentReason, programmedReason gatewayv1.GatewayConditionReason) *gatewayv1.Gateway {
	t.Helper()
	gateway := &gatewayv1.Gateway{}
	programmedStatus := metav1.ConditionFalse
	if programmedReason == gatewayv1.GatewayReasonProgrammed {
		programmedStatus = metav1.ConditionTrue
	}
	assignmentStatus := metav1.ConditionFalse
	if assignmentReason == gatewayv1.GatewayReasonAddressesAssigned {
		assignmentStatus = metav1.ConditionTrue
	}
	err := wait.PollUntilContextTimeout(t.Context(), 25*time.Millisecond, envtestWaitTimeout, true, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, key, gateway); err != nil {
			return false, err
		}
		programmed := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		if programmed == nil || programmed.Status != programmedStatus || programmed.Reason != string(programmedReason) || programmed.ObservedGeneration != gateway.Generation {
			return false, nil
		}
		assignment := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
		if assignmentReason == "" {
			if assignment != nil {
				return false, nil
			}
		} else if assignment == nil || assignment.Status != assignmentStatus || assignment.Reason != string(assignmentReason) || assignment.ObservedGeneration != gateway.Generation {
			return false, nil
		}
		addresses := gateway.Status.Addresses
		if value == "" {
			return len(addresses) == 0, nil
		}
		return len(addresses) == 1 && addresses[0].Value == value && ptr.Equal(addresses[0].Routability, scope), nil
	})
	if err != nil {
		t.Fatalf("wait for address %q (routability %v, AddressesAssigned %s, Programmed %s) at generation %d: addresses=%+v conditions=%+v: %v",
			value, ptr.Deref(scope, "<absent>"), assignmentReason, programmedReason, gateway.Generation, gateway.Status.Addresses, gateway.Status.Conditions, err)
	}
	return gateway
}
