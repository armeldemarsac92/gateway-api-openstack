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
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	httproutecontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/httproute"
)

func testHTTPRouteServiceReferenceGrant(t *testing.T, apiClient, cacheClient client.Client, config controller.Config, gateway *gatewayv1.Gateway) {
	t.Helper()
	target := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "grant-backends-envtest"}}
	port := corev1.ServicePort{Name: "http", Protocol: corev1.ProtocolTCP, Port: 80}
	decoy := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "shared"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{port}}}
	port.NodePort = 31081
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: target.Name, Name: decoy.Name}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{port}}}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: target.Name, Name: "shared-endpoints", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(8080))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.1.1.11"}}},
	}
	route := envtestHTTPRoute(gateway.Namespace, "granted", gateway.Name, service.Name)
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace(target.Name))
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: target.Name, Name: "allow-granted-service", Finalizers: []string{"envtest.example/retain"}},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(service.Name))}},
		},
	}
	for _, object := range []client.Object{target, decoy, service, endpoints, route} {
		if err := apiClient.Create(t.Context(), object); err != nil {
			t.Fatalf("create %T: %v", object, err)
		}
	}
	provider := &recordingProvider{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("last route status: %#v, graph writes: %d", route.Status.Parents, len(provider.graphSpecs))
		}
	})
	gatewayReconciler := newEnvtestGatewayReconciler(cacheClient, apiClient, provider, config)
	routeReconciler := gatewayReconciler.Writer.(*graph.Writer).Routes.(*httproutecontroller.Reconciler)
	routeKey := client.ObjectKeyFromObject(route)
	reconcileUntil := func(conditionType string, status metav1.ConditionStatus, reason string, bound bool, graphCount int) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			current, cached := &gatewayv1.HTTPRoute{}, &gatewayv1.HTTPRoute{}
			if err := apiClient.Get(ctx, routeKey, current); err != nil {
				return false, err
			}
			if err := cacheClient.Get(ctx, routeKey, cached); err != nil || cached.ResourceVersion != current.ResourceVersion {
				return false, client.IgnoreNotFound(err)
			}
			_, err := routeReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: routeKey})
			if err != nil {
				return false, err
			}
			if err := apiClient.Get(ctx, routeKey, route); err != nil {
				return false, err
			}
			if len(route.Status.Parents) != 1 {
				return false, nil
			}
			condition := meta.FindStatusCondition(route.Status.Parents[0].Conditions, conditionType)
			hasBinding := route.Annotations[config.RouteGatewayUIDAnnotation()] == string(gateway.UID)
			return condition != nil && condition.Status == status && condition.Reason == reason &&
				condition.ObservedGeneration == route.Generation && hasBinding == bound && len(provider.graphSpecs) >= graphCount, nil
		})
	}
	reconcileUntil("ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted", false, 0)
	if len(provider.graphSpecs) != 0 {
		t.Fatalf("ungranted route changed the cloud graph: %#v", provider.graphSpecs)
	}
	if err := apiClient.Create(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed", true, 1)
	identity := controller.RouteIdentity(config, gateway, route)
	programmed := provider.graphSpecs[0]
	if len(programmed.Routes) != 1 || programmed.Routes[0].Disposition != cloud.GraphEnsure || programmed.Routes[0].Spec.Identity != identity ||
		len(programmed.Routes[0].Spec.Members) != 1 || programmed.Routes[0].Spec.Members[0].Port != 31081 {
		t.Fatalf("expected only the authorized Service's NodePort on the bound Gateway: %#v", programmed)
	}

	if err := apiClient.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	if err := apiClient.Get(t.Context(), client.ObjectKeyFromObject(grant), grant); err != nil || grant.DeletionTimestamp.IsZero() {
		t.Fatalf("expected the terminating grant to remain visible: %v", err)
	}
	beforeRevoke := len(provider.graphSpecs)
	reconcileUntil("ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted", false, beforeRevoke+1)
	for _, spec := range provider.graphSpecs[beforeRevoke:] {
		if !controller.SameGatewayIdentity(spec.Identity, identity) || spec.Gateway.Disposition != cloud.GraphPreserve ||
			len(spec.Routes) != 1 || spec.Routes[0].Disposition != cloud.GraphDelete || spec.Routes[0].Spec.Identity != identity {
			t.Fatalf("revocation must only remove the affected route from its Gateway: %#v", spec)
		}
	}
}

func testHTTPRouteReferenceIndexUpdates(t *testing.T, apiClient, cacheClient client.Client) {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "reference-index-envtest"}}
	if err := apiClient.Create(t.Context(), namespace); err != nil {
		t.Fatalf("create reference test Namespace: %v", err)
	}
	route := envtestHTTPRoute(namespace.Name, "references", "old-parent", "old-service")
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("old-backends"))
	// References repeated across rules must produce one consumer in each index.
	route.Spec.Rules = append(route.Spec.Rules, *route.Spec.Rules[0].DeepCopy())
	route.Spec.Rules[1].Matches = []gatewayv1.HTTPRouteMatch{{
		Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/second")},
	}}
	if err := apiClient.Create(t.Context(), route); err != nil {
		t.Fatalf("create HTTPRoute with repeated backend references: %v", err)
	}
	assertIndex := func(field, value, wantName string) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var routes gatewayv1.HTTPRouteList
			if err := cacheClient.List(ctx, &routes, client.MatchingFields{field: value}); err != nil {
				return false, err
			}
			if wantName == "" {
				return len(routes.Items) == 0, nil
			}
			return len(routes.Items) == 1 && routes.Items[0].Name == wantName && routes.Items[0].Namespace == namespace.Name, nil
		})
	}
	assertIndex(controller.IndexHTTPRouteByBackendService, "old-backends/old-service", route.Name)
	assertIndex(controller.IndexHTTPRouteByBackendNamespace, "old-backends", route.Name)
	assertIndex(controller.IndexHTTPRouteByParentGateway, namespace.Name+"/old-parent", route.Name)

	base := route.DeepCopy()
	route.Spec.ParentRefs[0].Name = "new-parent"
	for i := range route.Spec.Rules {
		reference := &route.Spec.Rules[i].BackendRefs[0]
		reference.Name = "new-service"
		reference.Namespace = ptr.To(gatewayv1.Namespace("new-backends"))
	}
	if err := apiClient.Patch(t.Context(), route, client.MergeFrom(base)); err != nil {
		t.Fatalf("replace HTTPRoute backend and parent references: %v", err)
	}
	assertIndex(controller.IndexHTTPRouteByBackendService, "old-backends/old-service", "")
	assertIndex(controller.IndexHTTPRouteByBackendNamespace, "old-backends", "")
	assertIndex(controller.IndexHTTPRouteByParentGateway, namespace.Name+"/old-parent", "")
	assertIndex(controller.IndexHTTPRouteByBackendService, "new-backends/new-service", route.Name)
	assertIndex(controller.IndexHTTPRouteByBackendNamespace, "new-backends", route.Name)
	assertIndex(controller.IndexHTTPRouteByParentGateway, namespace.Name+"/new-parent", route.Name)

	base = route.DeepCopy()
	for i := range route.Spec.Rules {
		route.Spec.Rules[i].BackendRefs[0].Namespace = nil
	}
	if err := apiClient.Patch(t.Context(), route, client.MergeFrom(base)); err != nil {
		t.Fatalf("replace HTTPRoute backends with local references: %v", err)
	}
	assertIndex(controller.IndexHTTPRouteByBackendService, "new-backends/new-service", "")
	assertIndex(controller.IndexHTTPRouteByBackendNamespace, "new-backends", "")
	assertIndex(controller.IndexHTTPRouteByBackendNamespace, namespace.Name, "")
	assertIndex(controller.IndexHTTPRouteByBackendService, namespace.Name+"/new-service", route.Name)

	if err := apiClient.Delete(t.Context(), route); err != nil {
		t.Fatalf("delete indexed HTTPRoute: %v", err)
	}
	assertIndex(controller.IndexHTTPRouteByBackendService, namespace.Name+"/new-service", "")
	assertIndex(controller.IndexHTTPRouteByParentGateway, namespace.Name+"/new-parent", "")
}
