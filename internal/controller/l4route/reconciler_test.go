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
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestTCPRouteReportsAttachmentAndDeniedBackendIndependently(t *testing.T) {
	gateway, class, route := routeFixture()
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("other"))
	foreignStatus := gatewayv1.RouteParentStatus{
		ParentRef: route.Spec.ParentRefs[0], ControllerName: "foreign.example/controller",
		Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionFalse, Reason: "Denied"}},
	}
	route.Status.Parents = []gatewayv1.RouteParentStatus{foreignStatus}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "http-only"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: "app"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("backend"))}},
		},
	}
	r := testReconciler(t, gateway, class, route, grant)
	r.APIReader = denyServiceRead{Reader: r.Client}
	current := reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	checkRouteCondition(t, current, foreignStatus.ControllerName, "Accepted", metav1.ConditionFalse, "Denied")
}

func TestTCPRouteKeepsBothAttachmentsWhenTheOlderBackendIsMissing(t *testing.T) {
	gateway, class, oldest := routeFixture()
	newest := oldest.DeepCopy()
	newest.Name, newest.UID = "latest", "route-latest"
	newest.CreationTimestamp = metav1.NewTime(oldest.CreationTimestamp.Add(time.Minute))
	newest.Spec.Rules[0].BackendRefs[0].Name = "available"
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "available"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolTCP}}},
	}
	r := testReconciler(t, gateway, class, oldest, newest, service)
	for _, route := range []*gatewayv1.TCPRoute{oldest, newest} {
		current := reconcileTCPRoute(t, r, route)
		checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
		if len(current.Finalizers) != 0 {
			t.Fatalf("%s gained a binding without forwarding authority", route.Name)
		}
	}
	current := &gatewayv1.TCPRoute{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(newest), current); err != nil {
		t.Fatal(err)
	}
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "NotSelected")
}

func routeFixture() (*gatewayv1.Gateway, *gatewayv1.GatewayClass, *gatewayv1.TCPRoute) {
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "tcp", Protocol: gatewayv1.TCPProtocolType, Port: 8080}},
		},
	}
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack", UID: "class-uid", Generation: 1},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.com/openstack"},
		Status: gatewayv1.GatewayClassStatus{Conditions: []metav1.Condition{
			{Type: "SupportedVersion", Status: metav1.ConditionTrue, ObservedGeneration: 1},
		}},
	}
	route := &gatewayv1.TCPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "oldest", UID: types.UID("route-oldest"),
			Generation: 4, CreationTimestamp: metav1.NewTime(time.Unix(100, 0)),
		},
		Spec: gatewayv1.TCPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge"}}},
			Rules: []gatewayv1.TCPRouteRule{{BackendRefs: []gatewayv1.BackendRef{{BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: "backend", Port: ptr.To(gatewayv1.PortNumber(8080)),
			}}}}},
		},
	}
	return gateway, class, route
}

func testReconciler(t *testing.T, objects ...client.Object) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, gatewayv1.Install, apiextensionsv1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	config := controller.Config{ControllerName: "example.com/openstack", ClusterID: "cluster", OpenStackProjectID: "project"}
	for _, name := range config.GatewayAPICRDNames() {
		objects = append(objects, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{
			Name: name, Annotations: map[string]string{gatewayconsts.ChannelAnnotation: "standard", gatewayconsts.BundleVersionAnnotation: gatewayconsts.BundleVersion},
		}})
	}
	builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&gatewayv1.TCPRoute{}, &gatewayv1.UDPRoute{}).WithObjects(objects...)
	if err := controller.SetupIndexes(t.Context(), fakeRouteIndexes{builder}, config); err != nil {
		t.Fatal(err)
	}
	c := builder.Build()
	return &Reconciler{Client: c, APIReader: c, Config: config}
}

func reconcileTCPRoute(t *testing.T, r *Reconciler, route *gatewayv1.TCPRoute) *gatewayv1.TCPRoute {
	t.Helper()
	key := client.ObjectKeyFromObject(route)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	current := &gatewayv1.TCPRoute{}
	if err := r.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}
	return current
}

func checkRouteCondition(t *testing.T, object client.Object, name gatewayv1.GatewayController, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	route := viewRoute(object)
	for _, parent := range route.status.Parents {
		if parent.ControllerName != name {
			continue
		}
		condition := meta.FindStatusCondition(parent.Conditions, conditionType)
		if condition != nil && condition.Status == status && condition.Reason == reason &&
			(name == "foreign.example/controller" || condition.ObservedGeneration == route.GetGeneration()) {
			return
		}
		t.Fatalf("%s %s condition = %+v, want %s %s (generation %d)", route.GetName(), conditionType, condition, status, reason, route.GetGeneration())
	}
	t.Fatalf("%s has no parent status from %s", route.GetName(), name)
}

type denyServiceRead struct{ client.Reader }

func (r denyServiceRead) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, service := object.(*corev1.Service); service {
		return errors.New("read an unauthorized Service")
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

type fakeRouteIndexes struct{ builder *fake.ClientBuilder }

func (f fakeRouteIndexes) IndexField(_ context.Context, obj client.Object, field string, extract client.IndexerFunc) error {
	f.builder.WithIndex(obj, field, extract)
	return nil
}

func TestNodeEnqueuesOnlyRoutesOfManagedNodePortGateways(t *testing.T) {
	gw, class, route := routeFixture()
	otherGW := gw.DeepCopy()
	otherGW.Name, otherGW.UID = "other", "other-gateway"
	other := route.DeepCopy()
	other.Name, other.UID, other.Spec.ParentRefs[0].Name = "other", "other-route", "other"
	r := testReconciler(t, gw, otherGW, class, route, other)
	bindFixture(t, r, gw, route, cloud.MemberModeNodePort)
	bindFixture(t, r, otherGW, other, cloud.MemberModeEndpoint)
	reqs := r.enqueueNode(t.Context(), &corev1.Node{})
	if len(reqs) != 1 || reqs[0].NamespacedName != client.ObjectKeyFromObject(route) {
		t.Fatalf("Node event enqueued %v, want only %s", reqs, route.Name)
	}
}
