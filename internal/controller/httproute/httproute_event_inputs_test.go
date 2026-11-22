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

package httproute

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestNodeMapperEnqueuesRouteWithOneNodePortGateway(t *testing.T) {
	r, reader, node := nodeFanoutFixture(t, 1, corev1.ServiceExternalTrafficPolicyLocal)
	ctx := t.Context()
	var endpointGateway gatewayv1.Gateway
	if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: "foreign"}, &endpointGateway); err != nil {
		t.Fatal(err)
	}
	endpointGateway.Spec.GatewayClassName = "managed"
	topology := testBoundTopology()
	topology.Topology.MemberMode = cloud.MemberModeEndpoint
	if err := controller.BindGatewayTopology(r.Config, &endpointGateway, topology); err != nil {
		t.Fatal(err)
	}
	if err := reader.Update(ctx, &endpointGateway); err != nil {
		t.Fatal(err)
	}
	var route gatewayv1.HTTPRoute
	if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: "route-0000"}, &route); err != nil {
		t.Fatal(err)
	}
	route.Spec.ParentRefs = append([]gatewayv1.ParentReference{{Name: "foreign"}}, route.Spec.ParentRefs...)
	if err := reader.Update(ctx, &route); err != nil {
		t.Fatal(err)
	}
	requests := r.enqueueHTTPRoutesForNode(ctx, node)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(&route) {
		t.Fatalf("Node requests = %v, want the route attached to the NodePort Gateway", requests)
	}
}

func TestNodeMapperReobservesDependenciesBetweenEvents(t *testing.T) {
	const count = 3
	r, reader, node := nodeFanoutFixture(t, count, corev1.ServiceExternalTrafficPolicyLocal)
	ctx := context.Background()
	check := func(want int) {
		t.Helper()
		if got := len(r.enqueueHTTPRoutesForNode(ctx, node)); got != want {
			t.Fatalf("enqueued routes = %d, want %d", got, want)
		}
	}
	check(count)
	var endpoint discoveryv1.EndpointSlice
	if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: "backend-1"}, &endpoint); err != nil {
		t.Fatal(err)
	}
	endpoint.Endpoints[0].Conditions.Ready = ptr.To(false)
	if err := reader.Update(ctx, &endpoint); err != nil {
		t.Fatal(err)
	}
	check(0)
	endpoint.Endpoints[0].Conditions.Ready = ptr.To(true)
	endpoint.Endpoints[0].NodeName = ptr.To("other-worker")
	if err := reader.Update(ctx, &endpoint); err != nil {
		t.Fatal(err)
	}
	check(0)
	endpoint.Endpoints[0].NodeName = ptr.To(node.Name)
	if err := reader.Update(ctx, &endpoint); err != nil {
		t.Fatal(err)
	}
	check(count)
	var service corev1.Service
	if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: "backend"}, &service); err != nil {
		t.Fatal(err)
	}
	service.Spec.Type = corev1.ServiceTypeClusterIP
	if err := reader.Update(ctx, &service); err != nil {
		t.Fatal(err)
	}
	check(0)
	service.Spec.Type = corev1.ServiceTypeNodePort
	if err := reader.Update(ctx, &service); err != nil {
		t.Fatal(err)
	}
	check(count)
	var class gatewayv1.GatewayClass
	if err := reader.Get(ctx, client.ObjectKey{Name: "managed"}, &class); err != nil {
		t.Fatal(err)
	}
	class.Spec.ControllerName = "other.example.com/controller"
	if err := reader.Update(ctx, &class); err != nil {
		t.Fatal(err)
	}
	check(0)
}

func TestEndpointDeletionStillEnqueuesLocalBackends(t *testing.T) {
	const count = 3
	r, reader, node := nodeFanoutFixture(t, count, corev1.ServiceExternalTrafficPolicyLocal)
	ctx := context.Background()
	if got := len(r.enqueueHTTPRoutesForNode(ctx, node)); got != count {
		t.Fatalf("initial Node requests = %d, want %d", got, count)
	}
	var endpoint discoveryv1.EndpointSlice
	if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: "backend-1"}, &endpoint); err != nil {
		t.Fatal(err)
	}
	if err := reader.Delete(ctx, &endpoint); err != nil {
		t.Fatal(err)
	}
	if got := len(r.enqueueHTTPRoutesForNode(ctx, node)); got != 0 {
		t.Fatalf("Node event retained deleted local endpoints: %d routes", got)
	}
	requests := r.enqueueHTTPRoutesForEndpointSlice(ctx, &endpoint)
	names := make(map[string]bool, len(requests))
	for _, request := range requests {
		names[request.Name] = true
	}
	for _, name := range []string{"route-0000", "route-0001", "route-0002"} {
		if !names[name] {
			t.Errorf("EndpointSlice deletion did not enqueue %s", name)
		}
	}
}

func TestNodeMapperResolvesListenerSetAndNativeRefsToOneGateway(t *testing.T) {
	r, reader, node := nodeFanoutFixture(t, 2, corev1.ServiceExternalTrafficPolicyCluster)
	ctx := t.Context()
	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "delegation", Name: "tenant"},
		Spec: gatewayv1.ListenerSetSpec{ParentRef: gatewayv1.ParentGatewayReference{
			Name: "edge", Namespace: ptr.To(gatewayv1.Namespace("default")),
		}},
	}
	if err := reader.Create(ctx, set); err != nil {
		t.Fatal(err)
	}
	ref := gatewayv1.ParentReference{Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: "tenant", Namespace: ptr.To(gatewayv1.Namespace("delegation"))}
	var delegated, shared gatewayv1.HTTPRoute
	for _, route := range []struct {
		object *gatewayv1.HTTPRoute
		name   string
		share  bool
	}{{&delegated, "route-0000", false}, {&shared, "route-0001", true}} {
		if err := reader.Get(ctx, client.ObjectKey{Namespace: "default", Name: route.name}, route.object); err != nil {
			t.Fatal(err)
		}
		if !route.share {
			route.object.Spec.ParentRefs = nil
		}
		route.object.Spec.ParentRefs = append(route.object.Spec.ParentRefs, ref)
		if err := reader.Update(ctx, route.object); err != nil {
			t.Fatal(err)
		}
	}
	assertRouteRequests(t, r.enqueueHTTPRoutesForNode(ctx, node), &delegated, &shared)
}

func TestBackendPolicyGatewayPeersIncludeDelegatedHTTPRoutesOnce(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Spec.AllowedListeners = &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}}
	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "delegated"},
		Spec: gatewayv1.ListenerSetSpec{ParentRef: gatewayv1.ParentGatewayReference{
			Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace)),
		}},
	}
	setRef := gatewayv1.ParentReference{Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: "delegated", Namespace: ptr.To(gatewayv1.Namespace(set.Namespace))}
	directRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	delegated := mapperTestHTTPRoute(gateway.Namespace, "only-delegated", setRef, "backend")
	combined := mapperTestHTTPRoute(gateway.Namespace, "both-parents", directRef, "backend")
	combined.Spec.ParentRefs = append(combined.Spec.ParentRefs, setRef)
	kubeClient := rawIndexedFakeClientBuilder(testScheme(t), cfg).WithObjects(gateway, set, delegated, combined).
		WithIndex(&gatewayv1.ListenerSet{}, controller.IndexListenerSetByGateway, func(object client.Object) []string {
			return []string{model.ListenerSetParentKey(object.(*gatewayv1.ListenerSet)).String()}
		}).Build()
	gatewayKey, err := gatewayKeyForRouteParent(t.Context(), kubeClient, delegated, setRef, make(map[client.ObjectKey]client.ObjectKey))
	if err != nil || gatewayKey != client.ObjectKeyFromObject(gateway) {
		t.Fatalf("live delegated Gateway = %s, error %v", gatewayKey, err)
	}
	r := &Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}
	peers, err := r.liveGatewayRoutePeers(t.Context(), gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 || peers[0].Name == peers[1].Name {
		t.Fatalf("policy peer inventory = %#v, want both routes once", peers)
	}
}
