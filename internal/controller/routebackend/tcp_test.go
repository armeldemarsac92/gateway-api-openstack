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

package routebackend

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestDeletingEndpointSliceWithdrawsNodePort(t *testing.T) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "database"}}
	port := &corev1.ServicePort{Name: "db", Protocol: corev1.ProtocolTCP}
	deleted := metav1.Now()
	slice := discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deleted}, AddressType: discoveryv1.AddressTypeIPv4,
		Ports:     []discoveryv1.EndpointPort{{Name: ptr.To("db"), Port: ptr.To(int32(5432))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.2.11"}}},
	}
	nodes, diagnostic := ReadyBackendNodes(service, port, []discoveryv1.EndpointSlice{slice})
	if len(nodes) != 0 || diagnostic == nil || diagnostic.Reason != string(gatewayv1.RouteReasonPending) {
		t.Fatalf("a deleting EndpointSlice kept a worker eligible: nodes=%v diagnostic=%v", nodes, diagnostic)
	}
}
