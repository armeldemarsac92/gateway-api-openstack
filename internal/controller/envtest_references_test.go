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
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
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

// Exercise the normal controller watches, including a label-only revocation.
func testSharedHTTPGateway(t *testing.T, c client.Client, cfg controller.Config) {
	t.Helper()
	const name = "shared-tenants"
	params := envtestClassParameters(name)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: gatewayv1.GatewayClassSpec{
		ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: params.Name,
		},
	}}
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "controller-envtest"}, Spec: gatewayv1.GatewaySpec{
		GatewayClassName: name, Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80,
			AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
				From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"edge": name}},
			}},
		}},
	}}
	for _, obj := range []client.Object{params, class, gw} {
		if err := c.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	var tenants []*corev1.Namespace
	for i, ns := range []string{"shared-accounts", "shared-catalog"} {
		tenant := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{"edge": name}}}
		service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns}, Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Name: "http", Port: 80, NodePort: int32(31120 + i)}},
		}}
		endpoints := &discoveryv1.EndpointSlice{
			ObjectMeta:  metav1.ObjectMeta{Name: "web", Namespace: ns, Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
			AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(8080))}},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.1.12"}}},
		}
		route := envtestHTTPRoute(ns, "web", gw.Name, service.Name)
		route.Spec.ParentRefs[0].Namespace = ptr.To(gatewayv1.Namespace(gw.Namespace))
		route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Value: ptr.To("/" + ns)}}}
		for _, obj := range []client.Object{tenant, service, endpoints, route} {
			if err := c.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
		}
		tenants = append(tenants, tenant)
	}
	waitForAttachment := func(revoked bool, count int32) {
		t.Helper()
		defer func() {
			if !t.Failed() {
				return
			}
			var current gatewayv1.Gateway
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(gw), &current); err == nil {
				t.Logf("expected %d routes, listener status: %+v", count, current.Status.Listeners)
			}
			for _, tenant := range tenants {
				var route gatewayv1.HTTPRoute
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: tenant.Name, Name: "web"}, &route); err == nil {
					t.Logf("%s/web binding=%q parents=%+v", tenant.Name, route.Annotations[cfg.RouteGatewayUIDAnnotation()], route.Status.Parents)
				}
			}
		}()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			for i, tenant := range tenants {
				var route gatewayv1.HTTPRoute
				if err := c.Get(ctx, client.ObjectKey{Namespace: tenant.Name, Name: "web"}, &route); err != nil {
					return false, err
				}
				if len(route.Status.Parents) != 1 || route.Status.Parents[0].ControllerName != cfg.ControllerName {
					return false, nil
				}
				conditions := route.Status.Parents[0].Conditions
				accepted := meta.FindStatusCondition(conditions, "Accepted")
				bound := route.Annotations[cfg.RouteGatewayUIDAnnotation()] == string(gw.UID)
				if accepted == nil || accepted.ObservedGeneration != route.Generation {
					return false, nil
				}
				if revoked && i == 0 {
					if bound || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
						return false, nil
					}
				} else if !bound || accepted.Status != metav1.ConditionTrue || !meta.IsStatusConditionTrue(conditions, cfg.Domain()+"/Programmed") || len(route.OwnerReferences) != 0 {
					return false, nil
				}
			}
			var current gatewayv1.Gateway
			err := c.Get(ctx, client.ObjectKeyFromObject(gw), &current)
			return err == nil && len(current.Status.Listeners) == 1 && current.Status.Listeners[0].AttachedRoutes == count, err
		})
	}
	waitForAttachment(false, 2)
	before := tenants[0].DeepCopy()
	delete(tenants[0].Labels, "edge")
	if err := c.Patch(t.Context(), tenants[0], client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	waitForAttachment(true, 1)
}

func testDefaultGatewayAttachments(t *testing.T, c client.Client, cfg controller.Config, protocol gatewayv1.ProtocolType) {
	t.Helper()
	namespace := "default-gateways-" + strings.ToLower(string(protocol))
	apps := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace + "-apps", Labels: map[string]string{"default-gateway": namespace}}}
	parameters := envtestClassParameters(namespace)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: namespace}, Spec: gatewayv1.GatewayClassSpec{
		ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name},
	}}
	first := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: namespace}, Spec: gatewayv1.GatewaySpec{
		GatewayClassName: gatewayv1.ObjectName(namespace), DefaultScope: gatewayv1.GatewayDefaultScopeAll,
		Listeners: []gatewayv1.Listener{{Name: "frontend", Protocol: protocol, Port: 80, AllowedRoutes: &gatewayv1.AllowedRoutes{
			Namespaces: &gatewayv1.RouteNamespaces{From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: maps.Clone(apps.Labels)}},
		}}},
	}}
	second := first.DeepCopy()
	second.Name = "second"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), envtestWaitTimeout)
		defer cancel()
		for _, gw := range []*gatewayv1.Gateway{first, second} {
			var remaining gatewayv1.Gateway
			if err := c.Get(ctx, client.ObjectKeyFromObject(gw), &remaining); err != nil {
				require.NoError(t, client.IgnoreNotFound(err))
				continue
			}
			if remaining.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll {
				base := remaining.DeepCopy()
				remaining.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeNone
				require.NoError(t, c.Patch(ctx, &remaining, client.MergeFrom(base)))
			}
		}
	})
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: apps.Name}, Spec: corev1.ServiceSpec{
		Ports: []corev1.ServicePort{{Name: "http", Port: 80}},
	}}
	for _, object := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, apps, parameters, class, first, second, service} {
		require.NoError(t, c.Create(t.Context(), object))
	}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: apps.Name, Labels: map[string]string{discoveryv1.LabelServiceName: service.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(service, corev1.SchemeGroupVersion.WithKind("Service"))}},
		AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.11"}}},
	}
	require.NoError(t, c.Create(t.Context(), endpoints))
	http := envtestHTTPRoute(apps.Name, "web", "", service.Name)
	http.Spec.CommonRouteSpec = gatewayv1.CommonRouteSpec{UseDefaultGateways: gatewayv1.GatewayDefaultScopeAll}
	var route client.Object = http
	if protocol == gatewayv1.TCPProtocolType {
		route = &gatewayv1.TCPRoute{ObjectMeta: http.ObjectMeta, Spec: gatewayv1.TCPRouteSpec{
			CommonRouteSpec: http.Spec.CommonRouteSpec,
			Rules:           []gatewayv1.TCPRouteRule{{BackendRefs: []gatewayv1.BackendRef{http.Spec.Rules[0].BackendRefs[0].BackendRef}}},
		}}
	}
	require.NoError(t, c.Create(t.Context(), route))
	check := func(want map[string]bool) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			live := route.DeepCopyObject().(client.Object)
			if err := c.Get(ctx, client.ObjectKeyFromObject(route), live); err != nil {
				return false, err
			}
			spec, parents := controller.RouteSpecAndParentStatuses(live)
			if len(parents) != len(want) || len(spec.ParentRefs) != 0 || live.GetGeneration() != route.GetGeneration() {
				return false, nil
			}
			for _, status := range parents {
				admitted, known := want[string(status.ParentRef.Name)]
				if !known || status.ControllerName != cfg.ControllerName || status.ParentRef.Namespace == nil || string(*status.ParentRef.Namespace) != namespace {
					return false, nil
				}
				if !admitted {
					accepted := meta.FindStatusCondition(status.Conditions, string(gatewayv1.RouteConditionAccepted))
					if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) || accepted.ObservedGeneration != live.GetGeneration() {
						return false, nil
					}
					continue
				}
				for _, name := range []string{"Accepted", "ResolvedRefs", cfg.Domain() + "/Programmed"} {
					condition := meta.FindStatusCondition(status.Conditions, name)
					if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != live.GetGeneration() {
						return false, nil
					}
				}
			}
			for _, expected := range []*gatewayv1.Gateway{first, second} {
				var gw gatewayv1.Gateway
				if err := c.Get(ctx, client.ObjectKeyFromObject(expected), &gw); err != nil {
					return false, err
				}
				var attached int32
				if want[gw.Name] {
					attached = 1
				}
				if len(gw.Status.Listeners) != 1 || gw.Status.Listeners[0].AttachedRoutes != attached {
					return false, nil
				}
				condition := meta.FindStatusCondition(gw.Status.Conditions, "DefaultGateway")
				if expected.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll {
					if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != gw.Generation {
						return false, nil
					}
				} else if condition != nil {
					return false, nil
				}
			}
			claims, err := controller.StoredRouteGatewayBindings(cfg, live)
			active := 0
			for _, admitted := range want {
				if admitted {
					active++
				}
			}
			if err != nil || len(claims) != active {
				return false, err
			}
			for _, claim := range claims {
				if !want[claim.Identity.GatewayName] || claim.Lifecycle != controller.RouteAttached {
					return false, nil
				}
			}
			return true, nil
		})
	}
	check(map[string]bool{first.Name: true, second.Name: true})
	before := first.DeepCopy()
	first.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeNone
	require.NoError(t, c.Patch(t.Context(), first, client.MergeFrom(before)))
	check(map[string]bool{second.Name: true})
	beforeNS := apps.DeepCopy()
	delete(apps.Labels, "default-gateway")
	require.NoError(t, c.Patch(t.Context(), apps, client.MergeFrom(beforeNS)))
	check(map[string]bool{second.Name: false})
	beforeNS = apps.DeepCopy()
	apps.Labels["default-gateway"] = namespace
	require.NoError(t, c.Patch(t.Context(), apps, client.MergeFrom(beforeNS)))
	check(map[string]bool{second.Name: true})
	beforeRoute := route.DeepCopyObject().(client.Object)
	spec, _ := controller.RouteSpecAndParentStatuses(route)
	spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeNone
	require.NoError(t, c.Patch(t.Context(), route, client.MergeFrom(beforeRoute)))
	check(nil)
	if protocol == gatewayv1.HTTPProtocolType {
		// Other controllers may occupy all 32 status slots. Admission, listener
		// counts, and the active binding still need to reflect the new parent.
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(http), http))
		condition := controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Mesh attachment", http.Generation)
		condition.LastTransitionTime = metav1.Now()
		for i := range controller.RouteParentStatusLimit {
			http.Status.Parents = append(http.Status.Parents, gatewayv1.RouteParentStatus{
				ParentRef: gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("mesh-%d", i))}, ControllerName: "example.test/mesh",
				Conditions: []metav1.Condition{condition},
			})
		}
		require.NoError(t, c.Status().Update(t.Context(), http))
		before := http.DeepCopy()
		http.Spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeAll
		require.NoError(t, c.Patch(t.Context(), http, client.MergeFrom(before)))
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var current gatewayv1.HTTPRoute
			if err := c.Get(ctx, client.ObjectKeyFromObject(http), &current); err != nil {
				return false, err
			}
			if len(current.Status.Parents) != controller.RouteParentStatusLimit || slices.ContainsFunc(current.Status.Parents, func(p gatewayv1.RouteParentStatus) bool {
				return p.ControllerName != "example.test/mesh"
			}) {
				return false, nil
			}
			bindings, err := controller.StoredRouteGatewayBindings(cfg, &current)
			if err != nil || len(bindings) != 1 || bindings[0].Identity.GatewayUID != string(second.UID) || bindings[0].Lifecycle != controller.RouteAttached {
				return false, err
			}
			var gw gatewayv1.Gateway
			if err := c.Get(ctx, client.ObjectKeyFromObject(second), &gw); err != nil {
				return false, err
			}
			return len(gw.Status.Listeners) == 1 && gw.Status.Listeners[0].AttachedRoutes == 1, nil
		})
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
