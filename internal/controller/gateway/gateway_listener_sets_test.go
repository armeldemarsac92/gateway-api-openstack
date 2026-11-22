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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestEqualAgeListenerSetsOrderByFullNamespacedName(t *testing.T) {
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "gateway", Name: "edge"}, Spec: gatewayv1.GatewaySpec{
		AllowedListeners: &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}},
		Listeners:        []gatewayv1.Listener{{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}},
	}}
	makeSet := func(namespace string) *gatewayv1.ListenerSet {
		return &gatewayv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "web"}, Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "edge", Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))},
			Listeners: []gatewayv1.ListenerEntry{{Name: "delegated", Port: 8080, Protocol: gatewayv1.HTTPProtocolType}},
		}}
	}
	team, teamA := makeSet("team"), makeSet("team-a")
	kubeClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(team, teamA).Build()
	got, err := ObserveListenerSets(t.Context(), kubeClient, testConfig(), gateway)
	if err != nil {
		t.Fatal(err)
	}
	sections := got.Sections()
	if len(sections[client.ObjectKeyFromObject(teamA)]) != 1 || len(sections[client.ObjectKeyFromObject(team)]) != 0 {
		t.Fatalf("expected team-a/web to win over team/web: %#v", sections)
	}
}

func TestInvalidGatewayClearsAttachedListenerSetCount(t *testing.T) {
	gateway := &gatewayv1.Gateway{Status: gatewayv1.GatewayStatus{AttachedListenerSets: ptr.To(int32(1))}}
	(&Reconciler{Config: testConfig()}).setGatewayFailure(gateway, errors.New("invalid listeners"))
	if gateway.Status.AttachedListenerSets == nil || *gateway.Status.AttachedListenerSets != 0 {
		t.Fatalf("invalid Gateway retained attachment count: %v", gateway.Status.AttachedListenerSets)
	}
}

func TestNamespaceWatchRechecksGatewayForRouteOnForeignListenerSet(t *testing.T) {
	tenant := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}}
	gatewayKey := client.ObjectKey{Namespace: "gateway", Name: "edge"}
	set := &gatewayv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "delegation", Name: "tenant"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "edge", Namespace: ptr.To(gatewayv1.Namespace(gatewayKey.Namespace))},
			Listeners: []gatewayv1.ListenerEntry{{
				Name: "app", Protocol: gatewayv1.HTTPProtocolType, Port: 8080,
				AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
					From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "blue"}},
				}},
			}},
		},
	}
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: tenant.Name, Name: "app"}, Spec: gatewayv1.HTTPRouteSpec{
		CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
			Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Namespace: ptr.To(gatewayv1.Namespace(set.Namespace)), Name: "tenant", SectionName: ptr.To(gatewayv1.SectionName("app")),
		}}},
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(set, route).Build()
	r := &Reconciler{Client: kubeClient}
	requests := r.gatewayRequestsForListenerSetNamespace(t.Context(), tenant)
	if len(requests) != 1 || requests[0].NamespacedName != gatewayKey {
		t.Fatalf("label-dependent remote Gateway requests = %v, want %s", requests, gatewayKey)
	}
}
