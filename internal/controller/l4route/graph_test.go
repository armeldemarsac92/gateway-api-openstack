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
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestGraphFencesRevokedTCPReferenceGrant(t *testing.T) {
	gateway, class, route := routeFixture()
	gateway.Spec.Infrastructure = &gatewayv1.GatewayInfrastructure{Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"team": "edge"}}
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("backend"))
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backend", Name: "backend", UID: "service-uid"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolTCP}}},
	}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backend", Name: "tcp", UID: "grant-uid"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TCPRoute", Namespace: "app"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("backend"))}},
		},
	}
	r := testReconciler(t, gateway, class, route, service, grant)
	r.Config.GatewayInfrastructureMetadata = true
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	input, err := r.ObserveRouteGraph(t.Context(), graph.Request{Identity: identity, Action: graph.Ensure, Gateway: gateway, TCPRoute: route}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.GatewayRequirements.ListenerProtocol != "TCP" {
		t.Fatalf("authorized route graph = %+v", input.Nodes)
	}
	if err := r.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	r.APIReader = denyServiceRead{Reader: r.Client}
	if err := input.Validate(t.Context()); !errors.Is(err, controller.ErrGraphInputChanged) {
		t.Fatalf("revocation fence returned %v", err)
	}
}

func TestGraphPreservesFormerWinnerUntilDurableDetach(t *testing.T) {
	gateway, class, oldest := routeFixture()
	newest := oldest.DeepCopy()
	newest.Name, newest.UID = "latest", "route-latest"
	newest.CreationTimestamp = metav1.NewTime(oldest.CreationTimestamp.Add(time.Minute))
	r := testReconciler(t, gateway, class, oldest, newest)
	identity := bindFixture(t, r, gateway, newest, cloud.MemberModeEndpoint)
	input, err := r.ObserveRouteGraph(t.Context(), graph.Request{Identity: controller.GatewayIdentity(r.Config, gateway), Action: graph.Ensure, Gateway: gateway}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphPreserve || input.Nodes[0].Spec.Identity != identity {
		t.Fatalf("a former winner without a durable detach checkpoint produced %+v", input.Nodes)
	}
	base := newest.DeepCopy()
	newest.Annotations[r.Config.RouteLifecycleAnnotation()] = controller.RouteDetaching
	if err := r.Patch(t.Context(), newest, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	// An older, replaced/deleted parent is optional for cleanup.
	input, err = r.ObserveRouteGraph(t.Context(), graph.Request{Identity: identity, Action: graph.DeleteRoute, TCPRoute: newest}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphDelete || input.Nodes[0].Spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolTCP || input.Nodes[0].Spec.Identity != identity {
		t.Fatalf("durably detaching route produced %+v", input.Nodes)
	}
	if err := input.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRevokedTCPGrantDetachesDuringBundleSkew(t *testing.T) {
	gateway, class, route := routeFixture()
	route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("other"))
	r := testReconciler(t, gateway, class, route)
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	setBundleSkew(t, r)
	r.Coordinator = &graph.Coordinator{}
	r.APIReader = denyServiceRead{Reader: r.Client}
	current := reconcileTCPRoute(t, r, route)
	if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("the revoked backend did not checkpoint detach: %v", current.Annotations)
	}
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")

	// Simulate a process restart with the old Gateway absent from the writer.
	restarted := &Reconciler{Client: r.Client, APIReader: r.APIReader, Config: r.Config, Coordinator: &graph.Coordinator{}}
	restarted.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		input, err := restarted.ObserveRouteGraph(ctx, request, nil)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if request.Action != graph.DeleteRoute || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphDelete || input.Nodes[0].Spec.Identity != identity {
			t.Fatalf("detach after restart requested graph %+v", input.Nodes)
		}
		if err := input.Validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Identity: identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}}}}, nil
	})
	current = reconcileTCPRoute(t, restarted, route)
	if controller.RouteHasControllerBinding(r.Config, current) {
		t.Fatalf("verified route deletion retained the binding: %v", current.Finalizers)
	}
}

func TestTCPRouteChangingToTwoParentsCheckpointsDetach(t *testing.T) {
	gateway, class, route := routeFixture()
	r := testReconciler(t, gateway, class, route)
	bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	setBundleSkew(t, r)
	r.Coordinator = &graph.Coordinator{}
	route.Spec.ParentRefs = append(route.Spec.ParentRefs, gatewayv1.ParentReference{Name: "another-gateway"})
	route.Generation++
	if err := r.Update(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	current := reconcileTCPRoute(t, r, route)
	if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatalf("multi-parent edit did not checkpoint detach: %v", current.Annotations)
	}
	if !controller.RouteHasControllerBinding(r.Config, current) {
		t.Fatal("detach checkpoint discarded ownership before cloud cleanup")
	}
}

func TestBundleSkewKeepsOnlyConfirmedTCPForwarding(t *testing.T) {
	for _, change := range []string{"current", "legacy status", "edited backend", "deleted Service"} {
		t.Run(change, func(t *testing.T) {
			gateway, class, route := routeFixture()
			ref := &route.Spec.Rules[0].BackendRefs[0]
			service := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "backend-uid"},
				Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: int32(*ref.Port), Protocol: corev1.ProtocolTCP}}},
			}
			r := testReconciler(t, gateway, class, route, service)
			bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
			if change == "legacy status" {
				route.Status.Parents = []gatewayv1.RouteParentStatus{{ControllerName: r.Config.ControllerName, ParentRef: route.Spec.ParentRefs[0],
					Conditions: []metav1.Condition{{Type: r.Config.Domain() + "/Programmed", Status: metav1.ConditionTrue, Reason: "Programmed", ObservedGeneration: route.Generation}},
				}}
				if err := r.Status().Update(t.Context(), route); err != nil {
					t.Fatal(err)
				}
			} else {
				r.Writer = readyRouteWriter(t, r)
				route = reconcileTCPRoute(t, r, route)
				checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
			}
			r.Writer = applyRouteGraphFunc(func(context.Context, graph.Request) (cloud.GatewayGraphResult, error) {
				t.Fatal("version skew authorized a provider operation without a durable detach")
				return cloud.GatewayGraphResult{}, nil
			})
			setBundleSkew(t, r)
			switch change {
			case "current":
				r.Config.ControllerVersion = "next-build"
			case "edited backend":
				// The new local Service is independently authorized. It cannot
				// authorize continued forwarding to the old Service.
				next := service.DeepCopy()
				next.Name = "replacement"
				next.ResourceVersion = ""
				next.UID = ""
				if err := r.Create(t.Context(), next); err != nil {
					t.Fatal(err)
				}
				route.Spec.Rules[0].BackendRefs[0].Name = "replacement"
				route.Generation++
				if err := r.Update(t.Context(), route); err != nil {
					t.Fatal(err)
				}
			case "deleted Service":
				if err := r.Delete(t.Context(), service); err != nil {
					t.Fatal(err)
				}
			}
			r.Coordinator = &graph.Coordinator{}
			current := reconcileTCPRoute(t, r, route)
			if change == "current" {
				if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteAttached {
					t.Fatal("current authorized forwarding was detached")
				}
				checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
			} else if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
				t.Fatal("unverified forwarding survived the bundle change")
			}
		})
	}
}
func TestTCPRouteNodePortWithdrawsWhenLastEndpointDisappears(t *testing.T) {
	gateway, class, route := routeFixture()
	ref := route.Spec.Rules[0].BackendRefs[0]
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), UID: "backend-uid"}, Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: int32(*ref.Port), Protocol: corev1.ProtocolTCP, NodePort: 31001}},
	}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{
		Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.43"}},
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}}
	endpoints := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "database", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}},
		AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(5432))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.2.11"}, NodeName: &node.Name}},
	}
	r := testReconciler(t, gateway, class, route, service, node, endpoints)
	r.Coordinator = &graph.Coordinator{}
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeNodePort)
	plan, err := r.planRoute(t.Context(), viewRoute(route), identity, true)
	if err != nil || plan.desired == nil || !slices.Equal(plan.desired.Members, []cloud.Member{{Address: "10.0.0.43", Port: 31001}}) {
		t.Fatalf("TCP Service did not resolve to the worker NodePort: plan=%+v error=%v", plan, err)
	}
	if err := r.Delete(t.Context(), endpoints); err != nil {
		t.Fatal(err)
	}
	current := reconcileTCPRoute(t, r, route)
	checkRouteCondition(t, current, r.Config.ControllerName, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	checkRouteCondition(t, current, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionFalse, "Pending")
	if current.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		t.Fatal("a Service without eligible endpoints kept forwarding")
	}
}

func TestMissingTCPBindingCheckpointBlocksMutation(t *testing.T) {
	gateway, class, route := routeFixture()
	r := testReconciler(t, gateway, class, route)
	identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
	base := route.DeepCopy()
	route.Finalizers = []string{r.Config.RouteFinalizer()}
	if err := r.Patch(t.Context(), route, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	_, err := r.ObserveRouteGraph(t.Context(), graph.Request{Identity: identity, Action: graph.DeleteGateway}, gateway)
	if !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("lost checkpoint accepted by writer: %v", err)
	}
	route.SetAnnotations(nil)
	if _, _, err := r.routeBinding(viewRoute(route)); !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("orphan finalizer treated as unbound: %v", err)
	}
}

func setBundleSkew(t *testing.T, r *Reconciler) {
	t.Helper()
	setBundleVersion(t, r, "unsupported")
}

func setBundleVersion(t *testing.T, r *Reconciler, version string) {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := r.Get(t.Context(), client.ObjectKey{Name: "tcproutes.gateway.networking.k8s.io"}, crd); err != nil {
		t.Fatal(err)
	}
	crd.Annotations[gatewayconsts.BundleVersionAnnotation] = version
	if err := r.Update(t.Context(), crd); err != nil {
		t.Fatal(err)
	}
}

type applyRouteGraphFunc func(context.Context, graph.Request) (cloud.GatewayGraphResult, error)

func (f applyRouteGraphFunc) ReconcileGraph(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
	return f(ctx, request)
}

func readyRouteWriter(t *testing.T, r *Reconciler) applyRouteGraphFunc {
	t.Helper()
	return func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		if request.Action != graph.Ensure {
			t.Fatalf("unexpected graph action %s", request.Action)
		}
		input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if err := input.Validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{
			{Identity: request.Identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}},
		}}, nil
	}
}

func bindFixture(t *testing.T, r *Reconciler, gateway *gatewayv1.Gateway, route client.Object, mode cloud.MemberMode) cloud.Identity {
	t.Helper()
	ctx := t.Context()
	for _, object := range []client.Object{gateway, route} {
		if err := r.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
	}
	baseGateway := gateway.DeepCopy()
	gateway.Annotations = map[string]string{
		r.Config.GatewayListenerPortAnnotation(): "8080", r.Config.GatewayListenerProtocolAnnotation(): string(gateway.Spec.Listeners[0].Protocol),
		r.Config.GatewayClusterIDAnnotation(): r.Config.ClusterID, r.Config.GatewayProjectIDAnnotation(): r.Config.OpenStackProjectID,
	}
	gateway.Finalizers = []string{r.Config.GatewayFinalizer()}
	topology := classconfig.Resolved{
		Topology: cloud.Topology{
			MemberMode: mode, VIPSubnetID: "96d7a899-4f5d-4cf5-9842-5cc66ad91575",
			MemberSubnetID: "96d7a899-4f5d-4cf5-9842-5cc66ad91575",
		},
		NodeAddressType: corev1.NodeInternalIP, HealthMonitor: cloud.HealthMonitorConfig{Path: "/", Delay: 5, Timeout: 3, MaxRetries: 3, MaxRetriesDown: 3},
		ParametersName: "openstack", ParametersUID: "parameters-uid", ParametersGeneration: 1,
	}
	if err := controller.BindGatewayTopology(r.Config, gateway, topology); err != nil {
		t.Fatal(err)
	}
	if err := r.Patch(ctx, gateway, controller.OptimisticMergeFrom(baseGateway)); err != nil {
		t.Fatal(err)
	}
	base := viewRoute(route).copyObject()
	identity := controller.RouteIdentity(r.Config, gateway, route)
	controller.ApplyRouteBinding(r.Config, route, identity)
	if err := r.Patch(ctx, route, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	plan, err := r.planRoute(ctx, viewRoute(route), identity, true)
	if err != nil {
		t.Fatal(err)
	}
	if plan.desired != nil {
		plan.targetStatus = routeCondition(viewRoute(route), r.forwardingTargetType(), metav1.ConditionFalse, "Pending", plan.target)
		if err := r.patchStatuses(ctx, viewRoute(route), plan); err != nil {
			t.Fatal(err)
		}
	}
	return identity
}

func TestTCPRouteScopedParentSelectors(t *testing.T) {
	for _, test := range []struct {
		name    string
		section *gatewayv1.SectionName
		port    *gatewayv1.PortNumber
		want    []int
	}{
		{name: "all", want: []int{8080, 9311, 9312, 9313}},
		{name: "port", port: ptr.To(gatewayv1.PortNumber(8080)), want: []int{8080}},
		{name: "section", section: ptr.To(gatewayv1.SectionName("two")), want: []int{9311}},
		{name: "port and section", section: ptr.To(gatewayv1.SectionName("three")), port: ptr.To(gatewayv1.PortNumber(9312)), want: []int{9312}},
		{name: "mismatched port and section", section: ptr.To(gatewayv1.SectionName("four")), port: ptr.To(gatewayv1.PortNumber(9312))},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, class, route := routeFixture()
			route.Spec.ParentRefs[0].SectionName, route.Spec.ParentRefs[0].Port = test.section, test.port
			gateway.Spec.Listeners[0].Name = "one"
			gateway.Spec.Listeners = append(gateway.Spec.Listeners,
				gatewayv1.Listener{Name: "two", Protocol: gatewayv1.TCPProtocolType, Port: 9311},
				gatewayv1.Listener{Name: "three", Protocol: gatewayv1.TCPProtocolType, Port: 9312},
				gatewayv1.Listener{Name: "four", Protocol: gatewayv1.TCPProtocolType, Port: 9313},
			)
			service := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: "backend", UID: "service-uid"},
				Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080, Protocol: corev1.ProtocolTCP}}},
			}
			r := testReconciler(t, gateway, class, route, service)
			r.Coordinator = &graph.Coordinator{}
			if len(test.want) == 0 {
				current := reconcileTCPRoute(t, r, route)
				checkRouteCondition(t, current, r.Config.ControllerName, "Accepted", metav1.ConditionFalse, "NoMatchingParent")
				return
			}
			identity := bindFixture(t, r, gateway, route, cloud.MemberModeEndpoint)
			controller.SetGatewayListenerScope(r.Config, gateway, gatewayv1.TCPProtocolType)
			if err := r.Update(t.Context(), gateway); err != nil {
				t.Fatal(err)
			}
			base := route.DeepCopy()
			controller.SealRouteAnchor(r.Config, route, identity, 8080)
			if err := r.Patch(t.Context(), route, controller.OptimisticMergeFrom(base)); err != nil {
				t.Fatal(err)
			}
			route = reconcileTCPRoute(t, r, route)
			r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
				input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
				if err != nil {
					return cloud.GatewayGraphResult{}, err
				}
				if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure ||
					!slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, test.want) {
					t.Fatalf("TCP parent selected %+v, want ports %v", input.Nodes, test.want)
				}
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{
					{Identity: request.Identity, Result: cloud.RouteReadyResult(cloud.RouteState{})},
				}}, input.Validate(ctx)
			})
			route = reconcileTCPRoute(t, r, route)
			checkRouteCondition(t, route, r.Config.ControllerName, "Accepted", metav1.ConditionTrue, "Accepted")
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
		})
	}
}
