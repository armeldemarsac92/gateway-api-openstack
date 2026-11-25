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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestTCPConnectionRejectionFencesGrantWithdrawalAndRestore(t *testing.T) {
	gateway, class, route := routeFixture()
	localRef, local, localEndpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "local", 2, "10.1.2.3")
	secondLocal := localRef
	secondLocal.Weight = ptr.To(int32(1))
	remoteRef, remote, remoteEndpoints := transportBackendFixture(corev1.ProtocolTCP, "backend", "remote", 1, "10.1.2.4")
	remoteRef.Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{localRef, remoteRef, secondLocal}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow-tcp", UID: "grant"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TCPRoute", Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: &remoteRef.Name}},
		},
	}
	r := testReconciler(t, gateway, class, route, local, localEndpoints, remote, remoteEndpoints, grant)
	r.Coordinator = &graph.Coordinator{}
	r.Config.TCPConnectionRejection = true
	r.TCPConnectionRejectionAvailable = func(context.Context) (bool, error) { return true, nil }
	id := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	r.Writer = readyRouteWriter(t, r)
	route = reconcileTCPRoute(t, r, route)

	request := viewRoute(route).graphRequest(id, graph.Ensure, gateway)
	original, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	wantOriginal := []cloud.Member{{Address: "10.1.2.3", Port: 9090, Weight: 3}, {Address: "10.1.2.4", Port: 9090, Weight: 1}}
	if spec := original.Nodes[0].Spec; spec.TCPConnectionRejection != nil || !slices.Equal(spec.Members, wantOriginal) {
		t.Fatalf("granted target = %+v", spec)
	}
	if err := r.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	r.APIReader = denyServiceRead{Reader: r.Client, namespace: remote.Namespace}
	if err := original.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("withdrawal did not fence a stale graph: %v", err)
	}
	route = reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")
	route = reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
	request = viewRoute(route).graphRequest(id, graph.Ensure, gateway)
	revoked, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	wantPartial := []cloud.Member{{Address: "10.1.2.3", Port: 9090, Weight: 1}}
	if spec := revoked.Nodes[0].Spec; spec.TCPConnectionRejection == nil ||
		*spec.TCPConnectionRejection != (cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}) || !slices.Equal(spec.Members, wantPartial) {
		t.Fatalf("revoked target = %+v", spec)
	}

	restored := grant.DeepCopy()
	restored.ResourceVersion = ""
	if err := r.Create(t.Context(), restored); err != nil {
		t.Fatal(err)
	}
	r.APIReader = r.Client
	if err := revoked.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("restored authorization did not fence the old rejection plan: %v", err)
	}
	route = reconcileTCPRoute(t, r, route)
	route = reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
}

func TestTCPConnectionRejectionDistinguishesUnavailableBackends(t *testing.T) {
	for _, mode := range []cloud.MemberMode{cloud.MemberModeEndpoint, cloud.MemberModeNodePort} {
		t.Run(string(mode), func(t *testing.T) {
			gateway, class, route := routeFixture()
			gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.20"}}
			readyRef, ready, endpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "ready", 3, "10.1.2.3")
			emptyRef, empty, emptyEndpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "empty", 1)
			disabledRef, _, _ := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "disabled", 0)
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{readyRef, emptyRef, disabledRef}
			for i, service := range []*corev1.Service{ready, empty} {
				service.Spec.Type = corev1.ServiceTypeNodePort
				service.Spec.Ports[0].NodePort = int32(32001 + i)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.30"}},
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			}}
			r := testReconciler(t, gateway, class, route, ready, endpoints, empty, emptyEndpoints, node)
			r.Config.TCPConnectionRejection = true
			r.TCPConnectionRejectionAvailable = func(context.Context) (bool, error) { return true, nil }
			id := bindFixture(t, r, gateway, route, mode)
			plan, err := r.planRoute(t.Context(), viewRoute(route), id, true)
			if err != nil || plan.desired == nil || plan.detach || plan.desired.TCPConnectionRejection == nil ||
				*plan.desired.TCPConnectionRejection != (cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}) ||
				len(plan.desired.Members) != 1 || plan.desired.Members[0].Weight != 1 || plan.resolved.Reason != "BackendNotFound" {
				t.Fatalf("one empty backend and one disabled bad ref = %+v, %v", plan, err)
			}

			if err := r.Delete(t.Context(), endpoints); err != nil {
				t.Fatal(err)
			}
			plan, err = r.planRoute(t.Context(), viewRoute(route), id, true)
			if err != nil || plan.desired == nil || plan.desired.TCPConnectionRejection == nil ||
				*plan.desired.TCPConnectionRejection != (cloud.TCPConnectionRejection{RejectWeight: 4, TotalWeight: 4}) || len(plan.desired.Members) != 0 {
				t.Fatalf("all endpoints gone = %+v, %v", plan, err)
			}

			r.TCPConnectionRejectionAvailable = func(context.Context) (bool, error) { return false, nil }
			plan, err = r.planRoute(t.Context(), viewRoute(route), id, true)
			if err != nil || plan.desired != nil || !plan.detach {
				t.Fatalf("provider opt-out did not withdraw = %+v, %v", plan, err)
			}
			*route.Spec.Rules[0].BackendRefs[0].Weight, *route.Spec.Rules[0].BackendRefs[1].Weight = 0, 0
			plan, err = r.planRoute(t.Context(), viewRoute(route), id, true)
			if err != nil || plan.desired != nil || !plan.detach || plan.resolved.Reason != "BackendNotFound" {
				t.Fatalf("zero weights contributed to a traffic target or lost diagnostics = %+v, %v", plan, err)
			}
		})
	}
}

func TestTCPConnectionRejectionDoesNotConvertReadErrorsOrVersionSkew(t *testing.T) {
	gateway, class, route := routeFixture()
	ref, service, endpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "ready", 3, "10.1.2.3")
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
	r := testReconciler(t, gateway, class, route, service, endpoints)
	r.Coordinator = &graph.Coordinator{}
	r.Config.TCPConnectionRejection = true
	r.TCPConnectionRejectionAvailable = func(context.Context) (bool, error) {
		t.Error("uncertain Kubernetes state must not reach capability discovery")
		return true, nil
	}
	bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	r.Writer = readyRouteWriter(t, r)
	route = reconcileTCPRoute(t, r, route)
	r.APIReader = denyServiceRead{Reader: r.Client}
	key := client.ObjectKeyFromObject(route)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err == nil {
		t.Fatal("an active backend read error must retry")
	}
	if err := r.Get(t.Context(), key, route); err != nil {
		t.Fatal(err)
	}
	checkRouteCondition(t, route, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionUnknown, "Pending")
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")

	r.APIReader = r.Client
	setBundleSkew(t, r)
	route.Spec.Rules[0].BackendRefs[0].Name = "missing"
	route.Generation++
	if err := r.Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	route = reconcileTCPRoute(t, r, route)
	if route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("missing backend during version skew was not withdrawn: %+v", route.Annotations)
	}
	checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionUnknown, "UnsupportedVersion")
}
