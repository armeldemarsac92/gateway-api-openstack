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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestDiscoveryWithdrawsFailedUDPPort(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayInfrastructureMetadata = true
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{
			{Name: "dns", Protocol: gatewayv1.UDPProtocolType, Port: 53},
			{Name: "radius", Protocol: gatewayv1.UDPProtocolType, Port: 1812},
		}},
	}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	_, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{53, 1812}
	r.Writer = fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: ready}}
	_, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	key := discoverySliceKey(discoveryServiceKey(gateway), discoveryv1.AddressTypeIPv4)
	var slice discoveryv1.EndpointSlice
	require.NoError(t, kube.Get(t.Context(), key, &slice))
	require.Len(t, slice.Ports, 2)

	component := cloud.GatewayProgressingResult("Waiting for the Gateway UDP listeners", time.Second)
	component.State = ready.State
	component.ScopedListenerReadyPorts = []int{53}
	r.Writer = fixedGatewayGraph{result: cloud.GatewayGraphResult{Gateway: component}, err: cloud.NewProviderError(cloud.ErrorCategoryResourceFailure, errors.New("Octavia UDP listener provisioning status is ERROR"))}
	_, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	var current gatewayv1.Gateway
	require.NoError(t, kube.Get(t.Context(), req.NamespacedName, &current))
	require.True(t, meta.IsStatusConditionTrue(current.Status.Listeners[0].Conditions, "Programmed"))
	require.True(t, meta.IsStatusConditionFalse(current.Status.Listeners[1].Conditions, "Programmed"))
	require.NoError(t, kube.Get(t.Context(), key, &slice))
	require.Len(t, slice.Ports, 1)
	require.Equal(t, int32(53), *slice.Ports[0].Port)
}

func TestDiscoveryRetainsVerifiedHTTPDuringTLSWithdrawal(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayInfrastructureMetadata = true
	gateway := programmedTestGateway(cfg)
	controller.SetGatewayListenerScope(cfg, gateway, gatewayv1.HTTPProtocolType)
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{Name: "https", Protocol: gatewayv1.HTTPSProtocolType, Port: 443})
	gateway.Status.Listeners = []gatewayv1.ListenerStatus{{Name: "http", Conditions: []metav1.Condition{{
		Type: "Programmed", Status: metav1.ConditionTrue, ObservedGeneration: gateway.Generation,
	}}}}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	initial := cloud.GatewayReadyResult(cloud.GatewayState{VIPAddress: "192.0.2.10"})
	initial.ScopedListenerReadyPorts = []int{80, 443}
	primary := &gateway.Spec.Listeners[0]
	require.NoError(t, r.reconcileGatewayDiscovery(t.Context(), gateway, primary, initial))
	pending := cloud.GatewayProgressingResult("Withdrawing HTTPS", time.Second)
	pending.State = initial.State
	pending.ScopedListenerDeferredPorts = []int{80}
	pending.ScopedListenerServingPorts = []int{443}
	var slice discoveryv1.EndpointSlice
	key := discoverySliceKey(discoveryServiceKey(gateway), discoveryv1.AddressTypeIPv4)
	require.NoError(t, r.reconcileGatewayDiscovery(t.Context(), gateway, primary, pending))
	require.NoError(t, kube.Get(t.Context(), key, &slice))
	require.Len(t, slice.Ports, 2, "the verified shared bind remains discoverable without a Programmed Gateway section on 443")
	require.Equal(t, int32(443), *slice.Ports[1].Port)

	pending.ScopedListenerServingPorts = nil
	require.NoError(t, r.reconcileGatewayDiscovery(t.Context(), gateway, primary, pending))
	require.NoError(t, kube.Get(t.Context(), key, &slice), "unchanged HTTP listener is still enabled and previously programmed at this generation")
	require.Len(t, slice.Ports, 1)
	require.Equal(t, int32(80), *slice.Ports[0].Port)
}

func TestClusterReadbackWithdrawsEndpointWithDifferentPort(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	scope := gatewayv1.GatewayAddressRoutabilityCluster
	setExperimentalStatusAddress(gateway, "10.0.0.8", scope)
	setAddressAssignmentCondition(gateway, -1, "", clusterAddressAssignments{})
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: clusterAddressServicePrefix(gateway) + "auto-0", UID: "service",
			OwnerReferences: []metav1.OwnerReference{discoveryOwner(gateway)},
			Annotations:     map[string]string{discoveryControllerAnnotation: string(cfg.ControllerName), clusterFrontendVIPAnnotation: "192.0.2.10"},
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.0.0.8", ClusterIPs: []string{"10.0.0.8"},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol},
			Ports:      []corev1.ServicePort{{Name: "tcp-80", Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt(80)}},
		},
	}
	family := discoveryv1.AddressTypeIPv4
	key := client.ObjectKeyFromObject(service)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: discoverySliceKey(key, family).Name,
			OwnerReferences: []metav1.OwnerReference{
				discoveryOwner(gateway), {APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID},
			},
			Labels:      map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: discoveryManager},
			Annotations: map[string]string{discoveryControllerAnnotation: string(cfg.ControllerName)},
		},
		AddressType: family,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.10"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
		Ports:       discoveryEndpointPorts(service.Spec.Ports),
	}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(service, slice).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	publication := discoveryPublication{key: key}
	address, err := r.observePublishedClusterAddress(t.Context(), gateway, publication, false)
	require.NoError(t, err)
	require.Equal(t, service.Spec.ClusterIP, address)
	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(slice), slice))
	slice.Ports[0].Port = ptr.To(int32(443))
	require.NoError(t, kube.Update(t.Context(), slice))
	address, err = r.observePublishedClusterAddress(t.Context(), gateway, publication, false)
	require.NoError(t, err)
	require.Empty(t, address)
}

func TestClusterScopeRemovalOmitsAddressAssignmentUntilProviderReadback(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	completeGatewayTopologyFixture(cfg, gateway)
	setExperimentalStatusAddress(gateway, "10.0.0.8", gatewayv1.GatewayAddressRoutabilityCluster)
	setAddressAssignmentCondition(gateway, -1, "", clusterAddressAssignments{})
	gateway.Generation++
	r := &Reconciler{Config: cfg}
	r.observeAddressAssignment(gateway, cloud.GatewayResult{}, clusterAddressAssignments{})
	require.Empty(t, gateway.Status.Addresses)
	require.Nil(t, meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned)))
}
