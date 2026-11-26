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

package l4route

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/stretchr/testify/require"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestTCPXBackendRejectsForeignNamespaceAndPortOverride(t *testing.T) {
	for _, test := range []struct {
		name      string
		namespace *gatewayv1.Namespace
		port      *gatewayv1.PortNumber
		reason    gatewayv1.RouteConditionReason
	}{
		{name: "foreign namespace", namespace: ptr.To(gatewayv1.Namespace("foreign")), reason: gatewayv1.RouteReasonRefNotPermitted},
		{name: "port override", port: ptr.To(gatewayv1.PortNumber(9000)), reason: gatewayv1.RouteReasonUnsupportedValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, class, route := routeFixture()
			ref := &route.Spec.Rules[0].BackendRefs[0]
			ref.Group, ref.Kind = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend"))
			ref.Namespace, ref.Port = test.namespace, test.port
			r := testReconcilerForChannel(t, "experimental", gateway, class, route)
			route = reconcileTCPRoute(t, r, route)
			checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, string(test.reason))
		})
	}
}

func TestTCPXBackendFencesPodMembershipChange(t *testing.T) {
	gateway, class, route := routeFixture()
	ref := &route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "backend", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector,
			Port: gatewayx.BackendPort{Number: 9000},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "selected"},
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: "selected", Labels: map[string]string{"app": "selected"}},
		Status: corev1.PodStatus{
			PodIP:      "10.1.2.3",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	r := testReconcilerForChannel(t, "experimental", gateway, class, route, backend, pod)
	r.Coordinator = &graph.Coordinator{}
	id := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	r.Writer = readyRouteWriter(t, r)
	route = reconcileTCPRoute(t, r, route)
	inputs, err := r.ObserveRouteGraph(t.Context(), viewRoute(route).graphRequest(id, graph.Ensure, gateway), gateway)
	require.NoError(t, err)
	require.Len(t, inputs.Nodes, 1)
	require.Equal(t, []cloud.Member{{Address: "10.1.2.3", Port: 9000}}, inputs.Nodes[0].Spec.Members)
	delete(pod.Labels, "app")
	require.NoError(t, r.Update(t.Context(), pod))
	require.ErrorIs(t, inputs.Validate(t.Context()), controller.ErrGraphInputChanged)
}
