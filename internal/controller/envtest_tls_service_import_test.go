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
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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

func testTLSRouteServiceImportBackends(t *testing.T, restConfig *rest.Config, c client.Client, cfg controller.Config, provider *recordingProvider, probe *serviceImportAPITestProbe) {
	t.Helper()
	ctx := t.Context()
	const app, targets, shared = "tls-import-routes", "tls-import-backends", "shared"
	const serviceEndpoint, initialVIP, updatedVIP = "10.3.0.11", "10.4.0.21", "10.4.0.22"
	parameters := envtestClassParameters(app)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: app},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: app, Name: "edge"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: app, Listeners: []gatewayv1.Listener{{
			Name: "tls", Protocol: gatewayv1.TLSProtocolType, Port: 443,
			TLS: &gatewayv1.ListenerTLSConfig{Mode: ptr.To(gatewayv1.TLSModePassthrough)},
		}}},
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tls", Port: 443}}},
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: targets, Name: shared, Labels: map[string]string{discoveryv1.LabelServiceName: shared}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("tls"), Port: ptr.To(int32(8443))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{serviceEndpoint}}},
	}
	local := gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Name: shared, Namespace: ptr.To(gatewayv1.Namespace(targets)), Port: ptr.To(gatewayv1.PortNumber(443)),
	}, Weight: ptr.To(int32(2))}
	imported := *local.DeepCopy()
	imported.Group, imported.Kind = ptr.To(gatewayv1.Group(mcs.GroupName)), ptr.To(gatewayv1.Kind("ServiceImport"))
	imported.Weight = ptr.To(int32(3))
	otherPort := *imported.DeepCopy()
	otherPort.Port = ptr.To(gatewayv1.PortNumber(9443))
	newRoute := func(name string, refs ...gatewayv1.BackendRef) *gatewayv1.TLSRoute {
		return &gatewayv1.TLSRoute{ObjectMeta: metav1.ObjectMeta{Namespace: app, Name: name},
			Spec: gatewayv1.TLSRouteSpec{
				CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
				Hostnames:       []gatewayv1.Hostname{gatewayv1.Hostname(name + ".example.test")},
				Rules:           []gatewayv1.TLSRouteRule{{BackendRefs: refs}},
			},
		}
	}
	weighted, independent := newRoute("weighted", local, imported), newRoute("independent", otherPort)
	coreGrant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-service"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TLSRoute", Namespace: app}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(shared))}},
		},
	}
	importGrant := coreGrant.DeepCopy()
	importGrant.Name = "allow-import"
	importGrant.Spec.From[0].Kind = "HTTPRoute"
	importGrant.Spec.To[0].Group, importGrant.Spec.To[0].Kind = mcs.GroupName, "ServiceImport"
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: app}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targets}},
		parameters, class, gateway, service, endpoints, coreGrant, importGrant, weighted, independent,
	} {
		require.NoError(t, c.Create(ctx, object), "%T %s", object, object.GetName())
	}
	serviceAllowed := true
	assertDestination := func(route *gatewayv1.TLSRoute, reason gatewayv1.RouteConditionReason, importedUID, address string, port int, programmed bool) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
				return false, err
			}
			if len(route.Status.Parents) != 1 || route.Status.Parents[0].ControllerName != cfg.ControllerName {
				return false, nil
			}
			conditions := route.Status.Parents[0].Conditions
			refs := meta.FindStatusCondition(conditions, "ResolvedRefs")
			status, ready := metav1.ConditionFalse, metav1.ConditionFalse
			switch reason {
			case gatewayv1.RouteReasonResolvedRefs:
				status = metav1.ConditionTrue
			case gatewayv1.RouteReasonPending:
				status = metav1.ConditionUnknown
			}
			if programmed {
				ready = metav1.ConditionTrue
			}
			proof := meta.FindStatusCondition(conditions, cfg.Domain()+"/Programmed")
			if refs == nil || refs.Status != status || refs.Reason != string(reason) || refs.ObservedGeneration != route.Generation ||
				!meta.IsStatusConditionTrue(conditions, "Accepted") || proof == nil || proof.Status != ready || proof.ObservedGeneration != route.Generation {
				return false, nil
			}
			target, found := serviceImportSNITarget(provider, string(gateway.UID), string(route.UID))
			if route == independent {
				return (address == "" && !found) || found && target.ServiceUID == importedUID && target.ServicePort == 9443 &&
					target.BackendSubnetID == "" && target.BackendTLS == nil &&
					slices.Equal(target.Members, []cloud.Member{{Address: address, Port: port, RoutedExternal: true}}), nil
			}
			if !found || target.BackendSubnetID == "" || target.BackendTLS != nil {
				return false, nil
			}
			if !serviceAllowed {
				return slices.Equal(target.BackendServices, []cloud.TLSBackendService{{UID: importedUID, Port: 443}}) &&
					slices.Equal(target.Members, []cloud.Member{{Address: address, Port: port, Weight: 1, RoutedExternal: true}}) &&
					target.TCPConnectionRejection != nil && *target.TCPConnectionRejection == (cloud.TCPConnectionRejection{RejectWeight: 2, TotalWeight: 5}), nil
			}
			serviceWeight := 2
			if address == "" {
				serviceWeight = 1
			}
			if !slices.Contains(target.BackendServices, cloud.TLSBackendService{UID: string(service.UID), Port: 443}) ||
				!slices.Contains(target.Members, cloud.Member{Address: serviceEndpoint, Port: 8443, Weight: serviceWeight}) {
				return false, nil
			}
			if address == "" {
				return len(target.BackendServices) == 1 && len(target.Members) == 1 &&
					target.TCPConnectionRejection != nil && *target.TCPConnectionRejection == (cloud.TCPConnectionRejection{RejectWeight: 3, TotalWeight: 5}), nil
			}
			return target.TCPConnectionRejection == nil && len(target.BackendServices) == 2 && len(target.Members) == 2 &&
				slices.Contains(target.BackendServices, cloud.TLSBackendService{UID: importedUID, Port: 443}) &&
				slices.Contains(target.Members, cloud.Member{Address: address, Port: port, Weight: 3, RoutedExternal: true}), nil
		})
	}
	assertRoute := func(route *gatewayv1.TLSRoute, reason gatewayv1.RouteConditionReason, importedUID, address string, programmed bool) {
		t.Helper()
		port := 443
		if route == independent {
			port = 9443
		}
		assertDestination(route, reason, importedUID, address, port, programmed)
	}
	checkAvailable := func(object *mcs.ServiceImport, vip string) {
		t.Helper()
		assertRoute(weighted, gatewayv1.RouteReasonResolvedRefs, string(object.UID), vip, true)
		assertRoute(independent, gatewayv1.RouteReasonResolvedRefs, string(object.UID), vip, true)
	}

	assertRoute(weighted, gatewayv1.RouteReasonRefNotPermitted, "", "", true)
	importGrant.Spec.From[0].Kind = "TLSRoute"
	require.NoError(t, c.Update(ctx, importGrant))
	assertRoute(weighted, gatewayv1.RouteReasonBackendNotFound, "", "", true)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(mcscrd.ServiceImportCRD, &crd))
	_, err := envtest.InstallCRDs(restConfig, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{&crd}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.IgnoreNotFound(c.Delete(context.Background(), &crd))) })

	spec := mcs.ServiceImportSpec{Type: mcs.ClusterSetIP, IPs: []string{initialVIP},
		Ports: []mcs.ServicePort{{Name: "tls", Port: 443, AppProtocol: ptr.To("example.net/opaque")}, {Name: "other", Port: 9443}},
	}
	object := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: *spec.DeepCopy()}
	require.NoError(t, c.Create(ctx, object))
	checkAvailable(object, initialVIP)
	t.Run("Headless TLS endpoint lifecycle", func(t *testing.T) {
		const remoteIP = "10.4.0.31"
		remote := mcsEndpointSlice(targets, shared, "east", "tls", remoteIP, 7443)
		remote.Labels[discoveryv1.LabelServiceName] = shared
		remote.Ports = append(remote.Ports, discoveryv1.EndpointPort{Name: ptr.To("other"), Port: ptr.To(int32(10443))})
		require.NoError(t, c.Create(ctx, remote))
		object.Spec.Type = mcs.Headless
		object.Spec.IPs = nil
		require.NoError(t, c.Update(ctx, object))
		uid := string(object.UID)
		assertDestination(weighted, gatewayv1.RouteReasonResolvedRefs, uid, remoteIP, 7443, true)
		assertDestination(independent, gatewayv1.RouteReasonResolvedRefs, uid, remoteIP, 10443, true)

		remote.Endpoints[0].Conditions.Ready = ptr.To(false)
		require.NoError(t, c.Update(ctx, remote))
		assertRoute(weighted, gatewayv1.RouteReasonResolvedRefs, "", "", true)
		assertRoute(independent, gatewayv1.RouteReasonResolvedRefs, "", "", false)
		remote.Endpoints[0].Conditions.Ready = ptr.To(true)
		require.NoError(t, c.Update(ctx, remote))
		assertDestination(weighted, gatewayv1.RouteReasonResolvedRefs, uid, remoteIP, 7443, true)
		assertDestination(independent, gatewayv1.RouteReasonResolvedRefs, uid, remoteIP, 10443, true)

		manager := remote.Labels[discoveryv1.LabelManagedBy]
		delete(remote.Labels, discoveryv1.LabelManagedBy)
		require.NoError(t, c.Update(ctx, remote))
		assertRoute(weighted, gatewayv1.RouteReasonResolvedRefs, "", "", true)
		assertRoute(independent, gatewayv1.RouteReasonResolvedRefs, "", "", false)
		remote.Labels[discoveryv1.LabelManagedBy] = manager
		require.NoError(t, c.Update(ctx, remote))
		assertDestination(weighted, gatewayv1.RouteReasonResolvedRefs, uid, remoteIP, 7443, true)

		object.Spec = *spec.DeepCopy()
		require.NoError(t, c.Update(ctx, object))
		checkAvailable(object, initialVIP)
		require.NoError(t, c.Delete(ctx, remote))
	})

	importGrant.Spec.To[0].Name = ptr.To(gatewayv1.ObjectName("another"))
	require.NoError(t, c.Update(ctx, importGrant))
	assertRoute(weighted, gatewayv1.RouteReasonRefNotPermitted, "", "", true)
	assertRoute(independent, gatewayv1.RouteReasonRefNotPermitted, "", "", false)
	importGrant.Spec.To[0].Name = ptr.To(gatewayv1.ObjectName(shared))
	require.NoError(t, c.Update(ctx, importGrant))
	checkAvailable(object, initialVIP)

	serviceAllowed = false
	require.NoError(t, c.Delete(ctx, coreGrant))
	assertRoute(weighted, gatewayv1.RouteReasonRefNotPermitted, string(object.UID), initialVIP, true)
	coreGrant = &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-service"}, Spec: coreGrant.Spec}
	require.NoError(t, c.Create(ctx, coreGrant))
	serviceAllowed = true
	checkAvailable(object, initialVIP)

	object.Spec.Ports[0].Protocol = corev1.ProtocolUDP
	require.NoError(t, c.Update(ctx, object))
	assertRoute(weighted, gatewayv1.RouteReasonUnsupportedProtocol, "", "", true)
	assertRoute(independent, gatewayv1.RouteReasonResolvedRefs, string(object.UID), initialVIP, true)
	object.Spec.Ports = spec.DeepCopy().Ports
	require.NoError(t, c.Update(ctx, object))

	ready := mcs.NewServiceImportCondition(mcs.ServiceImportConditionReady, metav1.ConditionFalse, mcs.ServiceImportReasonIPFamilyNotSupported, "fixture: no available address family")
	ready.ObservedGeneration = object.Generation
	object.Status.Conditions = []metav1.Condition{ready}
	require.NoError(t, c.Status().Update(ctx, object))
	assertRoute(weighted, gatewayv1.RouteReasonUnsupportedValue, "", "", true)
	ready.Status, ready.Reason = metav1.ConditionTrue, string(mcs.ServiceImportReasonReady)
	object.Status.Conditions = []metav1.Condition{ready}
	require.NoError(t, c.Status().Update(ctx, object))
	checkAvailable(object, initialVIP)

	deferred := cloud.RouteProgressingResult("PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	provider.mu.Lock()
	provider.routeResults = map[string]cloud.RouteResult{string(weighted.UID): deferred, string(independent.UID): deferred}
	provider.mu.Unlock()
	object.Spec.IPs = []string{updatedVIP}
	require.NoError(t, c.Update(ctx, object))
	assertRoute(weighted, gatewayv1.RouteReasonResolvedRefs, string(object.UID), updatedVIP, false)
	assertRoute(independent, gatewayv1.RouteReasonResolvedRefs, string(object.UID), updatedVIP, false)
	provider.mu.Lock()
	provider.routeResults = nil
	provider.mu.Unlock()
	checkAvailable(object, updatedVIP)

	probe.failGet.Store(true)
	object.Spec.IPs = []string{initialVIP}
	require.NoError(t, c.Update(ctx, object))
	assertRoute(weighted, gatewayv1.RouteReasonPending, "", "", false)
	require.GreaterOrEqual(t, probe.failedGets.Load(), int64(2))
	probe.failGet.Store(false)
	checkAvailable(object, initialVIP)

	oldUID := object.UID
	require.NoError(t, c.Delete(ctx, object))
	assertRoute(weighted, gatewayv1.RouteReasonBackendNotFound, "", "", true)
	object = &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: spec}
	require.NoError(t, c.Create(ctx, object))
	require.NotEqual(t, oldUID, object.UID)
	checkAvailable(object, initialVIP)

	require.NoError(t, c.Delete(ctx, &crd))
	assertRoute(weighted, gatewayv1.RouteReasonBackendNotFound, "", "", true)
	assertRoute(independent, gatewayv1.RouteReasonBackendNotFound, "", "", false)
}

func serviceImportSNITarget(provider *recordingProvider, gatewayUID, routeUID string) (cloud.TLSSNIRoute, bool) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, spec := range slices.Backward(provider.graphSpecs) {
		if spec.Identity.GatewayUID != gatewayUID || spec.Gateway.Disposition != cloud.GraphEnsure {
			continue
		}
		for _, target := range spec.Gateway.TLSSNIRoutes {
			if target.Identity.RouteUID == routeUID && !target.Deny {
				return target, true
			}
		}
		break
	}
	return cloud.TLSSNIRoute{}, false
}
