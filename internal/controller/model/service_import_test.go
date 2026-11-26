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

package model

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
)

func TestServiceImportTransport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   gatewayv1.Kind
		change func(*mcs.ServiceImportSpec)
		want   gatewayv1.RouteConditionReason
	}{
		{name: "gRPC requires advertised h2c", kind: GRPCRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.Ports[0].AppProtocol = nil
		}, want: gatewayv1.RouteReasonUnsupportedProtocol},
		{name: "UDP port", kind: HTTPRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.Ports[0].Protocol = corev1.ProtocolUDP
		}, want: gatewayv1.RouteReasonUnsupportedProtocol},
		{name: "IPv6-only ClusterSetIP", kind: HTTPRouteKind, change: func(spec *mcs.ServiceImportSpec) {
			spec.IPs = []string{"2001:db8::1"}
		}, want: gatewayv1.RouteReasonUnsupportedValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := mcs.ServiceImportSpec{Type: mcs.ClusterSetIP, IPs: []string{"10.2.0.21"},
				Ports: []mcs.ServicePort{{Port: 8080, AppProtocol: ptr.To("kubernetes.io/h2c")}},
			}
			tc.change(&spec)
			port, destinations, diagnostic := (ServiceImportObservation{Spec: &spec}).Destinations(ptr.To(gatewayv1.PortNumber(8080)), tc.kind, true)
			require.NotNil(t, diagnostic)
			require.Equal(t, string(tc.want), diagnostic.Reason)
			require.Nil(t, port)
			require.Empty(t, destinations)
		})
	}
}

func TestHeadlessServiceImportDestinations(t *testing.T) {
	key := types.NamespacedName{Namespace: "backends", Name: "shared"}
	observation := ServiceImportObservation{
		Key: key, UID: "current-import",
		Spec: &mcs.ServiceImportSpec{Type: mcs.Headless, IPs: []string{"10.2.0.21"},
			Ports: []mcs.ServicePort{{Name: "app", Port: 8080, AppProtocol: ptr.To("kubernetes.io/h2c")}},
		},
		SourceClusters: []mcs.ClusterStatus{{Cluster: "east"}, {Cluster: "west"}},
	}
	newSlice := func(cluster, address string, port int32) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Labels: map[string]string{
				mcs.LabelServiceName: key.Name, mcs.LabelSourceCluster: cluster, discoveryv1.LabelManagedBy: "mcs.example.test",
			}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports: []discoveryv1.EndpointPort{
				{Name: ptr.To("other"), Port: ptr.To(int32(8080))},
				{Name: ptr.To("app"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(8090))},
				{Name: ptr.To("app"), Port: ptr.To(port)},
			},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{address}}},
		}
	}
	east := newSlice("east", "10.3.0.21", 8443)
	east.Endpoints = append(east.Endpoints,
		discoveryv1.Endpoint{Addresses: []string{"10.3.0.22"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)}},
		discoveryv1.Endpoint{Addresses: []string{"10.3.0.23"}, Conditions: discoveryv1.EndpointConditions{Terminating: ptr.To(true)}},
		discoveryv1.Endpoint{Addresses: []string{"10.3.0.24"}},
		discoveryv1.Endpoint{Addresses: []string{"127.0.0.1", "169.254.0.10", "2001:db8::1", "invalid"}},
	)
	duplicate := newSlice("west", "10.3.0.21", 8443)
	duplicate.Endpoints = append(duplicate.Endpoints, discoveryv1.Endpoint{Addresses: []string{"10.3.0.24"}, Conditions: discoveryv1.EndpointConditions{Serving: ptr.To(false)}})
	west := newSlice("west", "10.4.0.21", 9443)
	west.OwnerReferences = []metav1.OwnerReference{{APIVersion: mcs.SchemeGroupVersion.String(), Kind: ServiceImportKind, Name: key.Name, UID: observation.UID}}
	wrongCluster := newSlice("south", "10.5.0.21", 8443)
	wrongPort := newSlice("east", "10.5.0.22", 8443)
	wrongPort.Ports = wrongPort.Ports[:2]
	stale := newSlice("east", "10.5.0.23", 8443)
	stale.OwnerReferences = []metav1.OwnerReference{{APIVersion: mcs.SchemeGroupVersion.String(), Kind: ServiceImportKind, Name: key.Name, UID: "old-import"}}
	local := newSlice("east", "10.5.0.24", 8443)
	local.Labels[discoveryv1.LabelManagedBy] = "endpointslice-controller.k8s.io"
	local.Labels[discoveryv1.LabelServiceName] = key.Name
	noSource := newSlice("", "10.5.0.25", 8443)
	observation.EndpointSlices = []discoveryv1.EndpointSlice{*east, *duplicate, *west, *wrongCluster, *wrongPort, *stale, *local, *noSource}

	port, destinations, diagnostic := observation.Destinations(ptr.To(gatewayv1.PortNumber(8080)), GRPCRouteKind, true)
	require.Nil(t, diagnostic)
	require.Equal(t, int32(8080), port.Port)
	require.Equal(t, []netip.AddrPort{netip.MustParseAddrPort("10.3.0.21:8443"), netip.MustParseAddrPort("10.4.0.21:9443")}, destinations)
	// A slice labeled for both APIs cannot become a destination of the
	// homonymous core Service when it has MCS provenance.
	east.Labels[discoveryv1.LabelServiceName] = key.Name
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
	require.Empty(t, ResolveServiceEndpoints(service, port, []discoveryv1.EndpointSlice{*east}))

	observation.Spec.Type = mcs.ClusterSetIP
	_, destinations, diagnostic = observation.Destinations(ptr.To(gatewayv1.PortNumber(8080)), HTTPRouteKind, true)
	require.Nil(t, diagnostic)
	require.Equal(t, []netip.AddrPort{netip.MustParseAddrPort("10.2.0.21:8080")}, destinations)
}

func TestServiceImportOwnedSliceStaysSeparateFromHomonymousService(t *testing.T) {
	local := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "backends", Name: "shared", UID: "local"}}
	derived := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: local.Namespace, Name: "derived", UID: "derived"}}
	remote := discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: local.Namespace, Labels: map[string]string{
			discoveryv1.LabelServiceName: local.Name, mcs.LabelServiceName: local.Name,
			discoveryv1.LabelManagedBy: "mcs.example.test",
		}, OwnerReferences: []metav1.OwnerReference{{APIVersion: mcs.SchemeGroupVersion.String(), Kind: ServiceImportKind, Name: local.Name, UID: "import-uid"}}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: ptr.To(int32(8080))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.3.0.21"}}},
	}
	ordinary := remote.DeepCopy()
	ordinary.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(local, corev1.SchemeGroupVersion.WithKind("Service"))}
	ordinary.Labels = map[string]string{discoveryv1.LabelServiceName: local.Name}
	ordinary.Endpoints[0].Addresses = []string{"10.1.0.11"}
	port := &corev1.ServicePort{Port: 80}
	require.Equal(t, []Endpoint{{Address: "10.1.0.11", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: true}},
		ResolveServiceEndpoints(local, port, []discoveryv1.EndpointSlice{remote, *ordinary}))

	remote.Labels[discoveryv1.LabelServiceName] = derived.Name
	require.Equal(t, []Endpoint{{Address: "10.3.0.21", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: true}},
		ResolveServiceEndpoints(derived, port, []discoveryv1.EndpointSlice{remote}))
}

func TestServiceImportReadinessUsesCurrentGeneration(t *testing.T) {
	imported := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	require.Nil(t, ServiceImportReadiness(imported))
	ready := mcs.NewServiceImportCondition(mcs.ServiceImportConditionReady, metav1.ConditionFalse,
		mcs.ServiceImportReasonIPFamilyNotSupported, "The IP family is unavailable")
	imported.Status.Conditions = []metav1.Condition{ready}
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), ServiceImportReadiness(imported).Reason)
	imported.Status.Conditions[0].ObservedGeneration = 1
	require.Nil(t, ServiceImportReadiness(imported))
	imported.Status.Conditions[0].ObservedGeneration = imported.Generation
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), ServiceImportReadiness(imported).Reason)
}
