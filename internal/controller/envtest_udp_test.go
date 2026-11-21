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

func testUDPNodePort(t *testing.T, kube client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	const edge, backend, className = "controller-envtest", "udp-backends", "udp-nodeport"
	params := envtestClassParameters(className)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: className}, Spec: gatewayv1.GatewayClassSpec{
		ControllerName: cfg.ControllerName,
		ParametersRef: &gatewayv1.ParametersReference{Group: configv1alpha1.GroupName,
			Kind: configv1alpha1.GatewayClassConfigKind, Name: className},
	}}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: edge, Name: className}, Spec: gatewayv1.GatewaySpec{
		GatewayClassName: className, Listeners: []gatewayv1.Listener{{Name: "dns", Port: 5353, Protocol: gatewayv1.UDPProtocolType}},
	}}
	worker := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "dns-worker"}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "dns"}, Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeNodePort, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
		Ports: []corev1.ServicePort{
			{Name: "dns-tcp", Protocol: corev1.ProtocolTCP, Port: 53, NodePort: 32053},
			{Name: "dns-udp", Protocol: corev1.ProtocolUDP, Port: 53, NodePort: 32054},
		},
	}}
	endpoint := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "dns",
		Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("dns-udp"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(1053))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.244.0.7"}, NodeName: ptr.To(worker.Name),
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
	}
	route := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: edge, Name: "dns"}, Spec: gatewayv1.UDPRouteSpec{
		CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: className, SectionName: ptr.To(gatewayv1.SectionName("dns"))}}},
		Rules: []gatewayv1.UDPRouteRule{{BackendRefs: []gatewayv1.BackendRef{{BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: "dns", Namespace: ptr.To(gatewayv1.Namespace(backend)), Port: ptr.To(gatewayv1.PortNumber(53)),
		}}}}},
	}}
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "tcp"}, Spec: gatewayv1.ReferenceGrantSpec{
		From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TCPRoute", Namespace: edge}},
		To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("dns"))}},
	}}
	for _, obj := range []client.Object{params, class, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: backend}}, worker, service, endpoint, grant, gateway, route} {
		if err := kube.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		if err := kube.Delete(t.Context(), worker); client.IgnoreNotFound(err) != nil {
			t.Errorf("remove UDP worker from shared API server: %v", err)
		}
	}()
	worker.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.44"}}
	worker.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if err := kube.Status().Update(t.Context(), worker); err != nil {
		t.Fatal(err)
	}
	check := func(wantResolved, wantServing bool, reason string) {
		t.Helper()
		var observedGateway gatewayv1.Gateway
		var observedRoute gatewayv1.UDPRoute
		defer func() {
			if t.Failed() {
				t.Logf("waiting for %s (serving %t): Gateway %+v, UDPRoute %+v", reason, wantServing, observedGateway.Status, observedRoute.Status)
			}
		}()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			observedGateway, observedRoute = gatewayv1.Gateway{}, gatewayv1.UDPRoute{}
			gw, current := &observedGateway, &observedRoute
			if err := kube.Get(ctx, client.ObjectKeyFromObject(gateway), gw); err != nil {
				return false, err
			}
			if err := kube.Get(ctx, client.ObjectKeyFromObject(route), current); err != nil {
				return false, err
			}
			if len(gw.Status.Listeners) != 1 || gw.Status.Listeners[0].AttachedRoutes != 1 ||
				len(gw.Status.Listeners[0].SupportedKinds) != 1 || gw.Status.Listeners[0].SupportedKinds[0].Kind != "UDPRoute" {
				return false, nil
			}
			if len(current.Status.Parents) != 1 || current.Status.Parents[0].ControllerName != cfg.ControllerName ||
				!controller.ParentRefsEqual(current.Status.Parents[0].ParentRef, edge, route.Spec.ParentRefs[0], edge) ||
				controller.RouteHasControllerBinding(cfg, current) != wantServing {
				return false, nil
			}
			conditions := current.Status.Parents[0].Conditions
			resolved := meta.FindStatusCondition(conditions, "ResolvedRefs")
			programmed := meta.FindStatusCondition(conditions, cfg.Domain()+"/Programmed")
			if !meta.IsStatusConditionTrue(conditions, "Accepted") || resolved == nil || programmed == nil ||
				resolved.ObservedGeneration != current.Generation || programmed.ObservedGeneration != current.Generation ||
				resolved.Status == metav1.ConditionUnknown || (resolved.Status == metav1.ConditionTrue) != wantResolved ||
				programmed.Status == metav1.ConditionUnknown || (programmed.Status == metav1.ConditionTrue) != wantServing || programmed.Reason != reason {
				return false, nil
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for i := len(provider.graphSpecs) - 1; i >= 0; i-- {
				graph := provider.graphSpecs[i]
				if graph.Identity.GatewayUID != string(gateway.UID) {
					continue
				}
				if graph.Gateway.Disposition != cloud.GraphEnsure || graph.Gateway.Spec.ListenerProtocol != "UDP" {
					return false, nil
				}
				var ensured []cloud.RouteSpec
				for _, node := range graph.Routes {
					if node.Disposition == cloud.GraphEnsure {
						ensured = append(ensured, node.Spec)
					}
				}
				if !wantServing {
					return len(ensured) == 0, nil
				}
				return len(ensured) == 1 && ensured[0].Identity.RouteUID == string(route.UID) &&
					ensured[0].GatewayRequirements.ListenerProtocol == "UDP" && ensured[0].MemberMode == cloud.MemberModeNodePort &&
					slices.Equal(ensured[0].Members, []cloud.Member{{Address: "192.0.2.44", Port: 32054}}), nil
			}
			return false, nil
		})
	}
	check(false, false, "RefNotPermitted")
	udpGrant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: backend, Name: "udp"}, Spec: *grant.Spec.DeepCopy()}
	udpGrant.Spec.From[0].Kind = "UDPRoute"
	if err := kube.Create(t.Context(), udpGrant); err != nil {
		t.Fatal(err)
	}
	check(true, true, "Programmed")
	endpoint.Ports[0].Protocol = ptr.To(corev1.ProtocolTCP)
	if err := kube.Update(t.Context(), endpoint); err != nil {
		t.Fatal(err)
	}
	check(true, false, "Pending")
	endpoint.Ports[0].Protocol = ptr.To(corev1.ProtocolUDP)
	if err := kube.Update(t.Context(), endpoint); err != nil {
		t.Fatal(err)
	}
	check(true, true, "Programmed")
	if err := kube.Delete(t.Context(), udpGrant); err != nil {
		t.Fatal(err)
	}
	check(false, false, "RefNotPermitted")
}
