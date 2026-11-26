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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	mcscrd "sigs.k8s.io/mcs-api/config/crd"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
	"sigs.k8s.io/yaml"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testTransportServiceImportBackends(t *testing.T, restConfig *rest.Config, kube client.Client, cfg controller.Config, provider *recordingProvider, probe *serviceImportAPITestProbe) {
	t.Helper()
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(mcscrd.ServiceImportCRD, &crd))
	_, err := envtest.InstallCRDs(restConfig, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{&crd}})
	require.NoError(t, err)
	for _, protocol := range []gatewayv1.ProtocolType{gatewayv1.TCPProtocolType, gatewayv1.UDPProtocolType} {
		t.Run(string(protocol), func(t *testing.T) {
			testTransportImportLifecycle(t, kube, cfg, provider, probe, protocol)
		})
	}
	require.NoError(t, kube.Delete(t.Context(), &crd))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		err := kube.Get(ctx, client.ObjectKeyFromObject(&crd), &crd)
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
}

func testTransportImportLifecycle(t *testing.T, kube client.Client, cfg controller.Config, provider *recordingProvider, probe *serviceImportAPITestProbe, protocol gatewayv1.ProtocolType) {
	t.Helper()
	ctx := t.Context()
	app := strings.ToLower(string(protocol)) + "-import-routes"
	targets := app + "-backends"
	kind := gatewayv1.Kind(string(protocol) + "Route")
	const shared, portName = "shared", "backend"
	const serviceIP, importVIP, remoteIP = "10.3.0.11", "10.4.0.21", "10.4.0.31"
	params := envtestClassParameters(app)
	params.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: app},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: params.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: app, Name: "edge"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: gatewayv1.ObjectName(app),
			Listeners: []gatewayv1.Listener{{Name: "transport", Protocol: protocol, Port: 8060}},
		},
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: portName, Port: 80, Protocol: corev1.Protocol(protocol)}}},
	}
	localSlice := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: targets, Name: shared, Labels: map[string]string{discoveryv1.LabelServiceName: shared}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To(portName), Port: ptr.To(int32(6080)), Protocol: ptr.To(corev1.Protocol(protocol))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{serviceIP}}},
	}
	imported := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: mcs.ServiceImportSpec{
		Type: mcs.ClusterSetIP, IPs: []string{importVIP}, Ports: []mcs.ServicePort{{Name: portName, Protocol: corev1.Protocol(protocol), Port: 80}},
	}}
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-service"}, Spec: gatewayv1.ReferenceGrantSpec{
		From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: kind, Namespace: gatewayv1.Namespace(app)}},
		To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(shared))}},
	}}
	importGrant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-import"}, Spec: *grant.Spec.DeepCopy()}
	importGrant.Spec.From[0].Kind = "TLSRoute"
	importGrant.Spec.To[0].Group, importGrant.Spec.To[0].Kind = mcs.GroupName, "ServiceImport"
	refs := []gatewayv1.BackendRef{
		{BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: shared, Namespace: ptr.To(gatewayv1.Namespace(targets)), Port: ptr.To(gatewayv1.PortNumber(80)),
		}, Weight: ptr.To(int32(1))},
		{BackendObjectReference: gatewayv1.BackendObjectReference{
			Group: ptr.To(gatewayv1.Group(mcs.GroupName)), Kind: ptr.To(gatewayv1.Kind("ServiceImport")),
			Name: shared, Namespace: ptr.To(gatewayv1.Namespace(targets)), Port: ptr.To(gatewayv1.PortNumber(80)),
		}, Weight: ptr.To(int32(3))},
	}
	routeMeta := metav1.ObjectMeta{Namespace: app, Name: "forward"}
	common := gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}}
	var route client.Object
	if protocol == gatewayv1.TCPProtocolType {
		route = &gatewayv1.TCPRoute{ObjectMeta: routeMeta, Spec: gatewayv1.TCPRouteSpec{
			CommonRouteSpec: common, Rules: []gatewayv1.TCPRouteRule{{BackendRefs: refs}},
		}}
	} else {
		route = &gatewayv1.UDPRoute{ObjectMeta: routeMeta, Spec: gatewayv1.UDPRouteSpec{
			CommonRouteSpec: common, Rules: []gatewayv1.UDPRouteRule{{BackendRefs: refs}},
		}}
	}
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: app}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targets}},
		params, class, gateway, service, localSlice, imported, grant, importGrant, route,
	} {
		require.NoError(t, kube.Create(ctx, object), "%T %s", object, object.GetName())
	}

	coreMember := cloud.Member{Address: serviceIP, Port: 6080, Weight: 1}
	firstVIP := cloud.Member{Address: importVIP, Port: 80, Weight: 3, RoutedExternal: true}
	check := func(reason gatewayv1.RouteConditionReason, rejected int, members ...cloud.Member) {
		t.Helper()
		var last cloud.RouteSpec
		defer func() {
			if t.Failed() {
				_, parents, _ := controller.RouteParents(route)
				t.Logf("wanted %s, %d rejected, members %+v; last parents %+v and graph %+v", reason, rejected, members, parents, last)
			}
		}()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := kube.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
				return false, err
			}
			_, parents, _ := controller.RouteParents(route)
			if len(parents) != 1 || parents[0].ControllerName != cfg.ControllerName {
				return false, nil
			}
			conditions := parents[0].Conditions
			resolved := meta.FindStatusCondition(conditions, "ResolvedRefs")
			if resolved == nil || resolved.Reason != string(reason) || resolved.ObservedGeneration != route.GetGeneration() ||
				(resolved.Status == metav1.ConditionTrue) != (reason == gatewayv1.RouteReasonResolvedRefs) ||
				!meta.IsStatusConditionTrue(conditions, "Accepted") || !meta.IsStatusConditionTrue(conditions, cfg.Domain()+"/Programmed") {
				return false, nil
			}
			var found bool
			last, found = transportServiceImportGraph(provider, string(gateway.UID), string(route.GetUID()))
			if !found || !slices.Equal(last.Members, members) {
				return false, nil
			}
			if protocol == gatewayv1.TCPProtocolType {
				return (rejected == 0 && last.TCPConnectionRejection == nil) ||
					(rejected != 0 && last.TCPConnectionRejection != nil && *last.TCPConnectionRejection == (cloud.TCPConnectionRejection{RejectWeight: rejected, TotalWeight: 4})), nil
			}
			denominator := 4
			if rejected == 0 {
				denominator = 1
			}
			return last.UDPPacketDrop != nil && *last.UDPPacketDrop == (cloud.UDPPacketDrop{Numerator: rejected, Denominator: denominator}), nil
		})
	}
	denied := probe.backendGets.Load()
	check(gatewayv1.RouteReasonRefNotPermitted, 3, coreMember)
	require.Equal(t, denied, probe.backendGets.Load(), "a grant for TLSRoute cannot authorize an L4 import read")

	importGrant.Spec.From[0].Kind = kind
	require.NoError(t, kube.Update(ctx, importGrant))
	check(gatewayv1.RouteReasonResolvedRefs, 0, coreMember, firstVIP)

	otherProtocol := corev1.ProtocolUDP
	if protocol == gatewayv1.UDPProtocolType {
		otherProtocol = corev1.ProtocolTCP
	}
	imported.Spec.Ports[0].Protocol = otherProtocol
	require.NoError(t, kube.Update(ctx, imported))
	check(gatewayv1.RouteReasonUnsupportedProtocol, 3, coreMember)
	imported.Spec.Ports[0].Protocol = corev1.Protocol(protocol)

	remote := mcsEndpointSlice(targets, shared, "east", portName, remoteIP, 9080)
	remote.Labels[discoveryv1.LabelServiceName] = shared
	remote.Ports[0].Protocol = ptr.To(corev1.Protocol(protocol))
	require.NoError(t, kube.Create(ctx, remote))
	imported.Spec.Type, imported.Spec.IPs = mcs.Headless, nil
	require.NoError(t, kube.Update(ctx, imported))
	remoteMember := cloud.Member{Address: remoteIP, Port: 9080, Weight: 3, RoutedExternal: true}
	check(gatewayv1.RouteReasonResolvedRefs, 0, coreMember, remoteMember)

	require.NoError(t, kube.Delete(ctx, importGrant))
	check(gatewayv1.RouteReasonRefNotPermitted, 3, coreMember)
	importGrant = &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-import"}, Spec: importGrant.Spec}
	require.NoError(t, kube.Create(ctx, importGrant))
	check(gatewayv1.RouteReasonResolvedRefs, 0, coreMember, remoteMember)

	remote.Endpoints[0].Conditions.Ready = ptr.To(false)
	require.NoError(t, kube.Update(ctx, remote))
	check(gatewayv1.RouteReasonResolvedRefs, 3, coreMember)
	remote.Endpoints[0].Conditions.Ready = ptr.To(true)
	remote.Ports[0].Port = ptr.To(int32(9081))
	require.NoError(t, kube.Update(ctx, remote))
	remoteMember.Port = 9081
	check(gatewayv1.RouteReasonResolvedRefs, 0, coreMember, remoteMember)

	switch route := route.(type) {
	case *gatewayv1.TCPRoute:
		route.Spec.Rules[0].BackendRefs[1].Weight = ptr.To(int32(0))
	case *gatewayv1.UDPRoute:
		route.Spec.Rules[0].BackendRefs[1].Weight = ptr.To(int32(0))
	}
	require.NoError(t, kube.Update(ctx, route))
	require.NoError(t, kube.Delete(ctx, importGrant))
	check(gatewayv1.RouteReasonRefNotPermitted, 0, coreMember)
}

func transportServiceImportGraph(provider *recordingProvider, gatewayUID, routeUID string) (cloud.RouteSpec, bool) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for i := len(provider.graphSpecs) - 1; i >= 0; i-- {
		graph := provider.graphSpecs[i]
		if graph.Identity.GatewayUID != gatewayUID {
			continue
		}
		for _, node := range graph.Routes {
			if node.Spec.Identity.RouteUID == routeUID {
				return node.Spec, node.Disposition == cloud.GraphEnsure
			}
		}
		return cloud.RouteSpec{}, false
	}
	return cloud.RouteSpec{}, false
}
