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
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestTransportForwardingWithFullParentStatus(t *testing.T) {
	t.Run("experimental default", func(t *testing.T) { testTransportForwardingWithFullParentStatus(t, "experimental") })
	t.Run("standard explicit", func(t *testing.T) { testTransportForwardingWithFullParentStatus(t, "standard") })
}

func testTransportForwardingWithFullParentStatus(t *testing.T, channel string) {
	t.Helper()
	ctx := t.Context()
	east, class, route := routeFixture()
	east.Status.Conditions = []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionTrue, ObservedGeneration: east.Generation}}
	gateways := []*gatewayv1.Gateway{east}
	if channel == "experimental" {
		east.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeAll
		west := east.DeepCopy()
		west.Name, west.UID = "west", "west-uid"
		gateways = append(gateways, west)
		route.Spec.ParentRefs = nil
		route.Spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeAll
	}
	ref, service, endpoints := transportBackendFixture(corev1.ProtocolTCP, route.Namespace, "backend", 1, "10.1.2.3")
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
	foreign := make([]gatewayv1.RouteParentStatus, controller.RouteParentStatusLimit)
	foreignCondition := controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Admitted by another controller", route.Generation)
	foreignCondition.LastTransitionTime = metav1.NewTime(time.Unix(1234, 0))
	for i := range foreign {
		foreign[i] = gatewayv1.RouteParentStatus{
			ControllerName: "foreign.example/controller",
			ParentRef:      gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("gateway-%d", i))},
			Conditions:     []metav1.Condition{foreignCondition},
		}
	}
	route.Status.Parents = slices.Clone(foreign)
	objects := []client.Object{class, route, service, endpoints}
	for _, gateway := range gateways {
		objects = append(objects, gateway)
	}
	r := testReconcilerForChannel(t, channel, objects...)
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, object client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" && len(viewRoute(object).status.Parents) > controller.RouteParentStatusLimit {
				return fmt.Errorf("CRD rejects oversized status.parents")
			}
			return c.SubResource(sub).Patch(ctx, object, patch, opts...)
		},
	})
	r.Coordinator = &graph.Coordinator{}
	for _, gateway := range gateways {
		bindGatewayFixture(t, r, gateway, cloud.MemberModeEndpoint)
	}
	ensured := map[string]bool{}
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if request.Action != graph.Ensure || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure ||
			len(input.Nodes[0].Spec.Members) != 1 || input.Nodes[0].Spec.Members[0].Address != "10.1.2.3" {
			t.Fatalf("Gateway %s did not receive authorized forwarding: %+v", request.Identity.GatewayName, input.Nodes)
		}
		ensured[request.Identity.GatewayUID] = true
		return cloud.GatewayGraphResult{
			Outcome: cloud.ReadyOutcome(),
			Routes:  []cloud.RouteGraphResult{{Identity: request.Identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}}},
		}, input.Validate(ctx)
	})

	route = reconcileTCPRoute(t, r, route)
	r.Coordinator = &graph.Coordinator{}
	route = reconcileTCPRoute(t, r, route)
	if !reflect.DeepEqual(route.Status.Parents, foreign) || len(ensured) != len(gateways) {
		t.Fatalf("full status prevented forwarding or changed foreign entries: ensured=%v, parents=%+v", ensured, route.Status.Parents)
	}
	bindings, err := r.routeBindings(viewRoute(route))
	if err != nil || len(bindings) != len(gateways) {
		t.Fatalf("want independently sealed Gateway bindings, got %+v, %v", bindings, err)
	}
	for _, binding := range bindings {
		if binding.Forwarding.Value == "" || !binding.Forwarding.Observed || binding.Forwarding.Generation != route.Generation {
			t.Fatalf("missing durable confirmed forwarding for %s: %+v", binding.Identity.GatewayName, binding)
		}
	}
	route.Generation++
	if err := r.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	for _, gateway := range gateways {
		request := graph.Request{Action: graph.Ensure, Gateway: gateway, Identity: controller.GatewayIdentity(r.Config, gateway)}
		input, err := r.ObserveRouteGraph(ctx, request, gateway)
		if err != nil || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphPreserve {
			t.Fatalf("Gateway %s reused a previous generation's target: %+v, %v", gateway.Name, input.Nodes, err)
		}
	}
	if len(gateways) < 2 {
		return
	}
	west := gateways[1]
	route.Status.Parents = append(slices.Clone(foreign[:len(foreign)-1]), gatewayv1.RouteParentStatus{
		ControllerName: r.Config.ControllerName,
		ParentRef:      gatewayv1.ParentReference{Name: gatewayv1.ObjectName(west.Name), Namespace: ptr.To(gatewayv1.Namespace(west.Namespace))},
		Conditions:     []metav1.Condition{foreignCondition},
	})
	if err := r.Status().Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	route = reconcileTCPRoute(t, r, route)
	if len(route.Status.Parents) != controller.RouteParentStatusLimit || string(route.Status.Parents[len(foreign)-1].ParentRef.Name) != west.Name {
		t.Fatalf("a new status displaced the previously represented Gateway: %+v", route.Status.Parents)
	}
}

func TestRecreatedDefaultGatewayDropsOldUIDStatusDuringCleanup(t *testing.T) {
	ctx := t.Context()
	replacement, class, route := routeFixture()
	replacement.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeAll
	old := replacement.DeepCopy()
	old.UID = "old-gateway-uid"
	route.Spec.ParentRefs = nil
	route.Spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeAll
	r := testReconcilerForChannel(t, "experimental", replacement, class, route)
	r.Coordinator = &graph.Coordinator{}
	identity := controller.RouteIdentity(r.Config, old, route)
	key := client.ObjectKeyFromObject(route)
	if err := r.Get(ctx, key, route); err != nil {
		t.Fatal(err)
	}
	base := route.DeepCopy()
	controller.ApplyRouteBinding(r.Config, route, identity)
	if err := r.Patch(ctx, route, controller.OptimisticMergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	ref := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(old.Name), Namespace: ptr.To(gatewayv1.Namespace(old.Namespace))}
	view := viewRoute(route)
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ControllerName: r.Config.ControllerName,
		ParentRef:      ref,
		Conditions: []metav1.Condition{
			routeCondition(view, "Accepted", metav1.ConditionTrue, "Accepted", "Old listener accepted the route"),
			routeCondition(view, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed", "Old Gateway was active"),
			routeCondition(view, r.forwardingTargetType(), metav1.ConditionTrue, "Observed", "old forwarding target"),
		},
	}}
	if err := r.Status().Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
		input, err := r.ObserveRouteGraph(ctx, request, nil)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if request.Action != graph.DeleteRoute || request.Identity != identity || len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphDelete {
			t.Fatalf("replacement received an unexpected graph while old UID was detaching: %+v, %+v", request, input.Nodes)
		}
		outcome := cloud.ProgressingOutcome("still deleting", time.Second)
		return cloud.GatewayGraphResult{Outcome: outcome, Routes: []cloud.RouteGraphResult{{Identity: identity, Result: cloud.RouteResult{Outcome: outcome}}}}, input.Validate(ctx)
	})
	route = reconcileTCPRoute(t, r, route)
	route = reconcileTCPRoute(t, r, route)
	bindings, err := r.routeBindings(viewRoute(route))
	if err != nil || len(bindings) != 1 || bindings[0].Identity != identity || bindings[0].Lifecycle != controller.RouteDetaching {
		t.Fatalf("expected old UID's durable cleanup, got %+v, %v", bindings, err)
	}
	if len(route.Status.Parents) != 0 {
		t.Fatalf("replacement inherited status from old UID: %+v", route.Status.Parents)
	}
}

func TestTransportDefaultGatewaysPreserveLegacyForwardingAndCleanUpIndependently(t *testing.T) {
	for _, protocol := range []gatewayv1.ProtocolType{gatewayv1.TCPProtocolType, gatewayv1.UDPProtocolType} {
		t.Run(string(protocol), func(t *testing.T) {
			ctx := t.Context()
			explicit, class, tcp := routeFixture()
			explicit.Status.Addresses = []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.30"}}
			explicit.Status.Conditions = []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionTrue, ObservedGeneration: explicit.Generation}}
			explicit.Spec.Listeners[0].Protocol = protocol
			ref, service, endpoints := transportBackendFixture(corev1.Protocol(protocol), tcp.Namespace, "backend", 1, "10.1.2.3")
			tcp.Spec.ParentRefs[0].SectionName = ptr.To(explicit.Spec.Listeners[0].Name)
			tcp.Spec.ParentRefs[0].Port = ptr.To(explicit.Spec.Listeners[0].Port)
			tcp.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
			foreign := gatewayv1.RouteParentStatus{
				ControllerName: "foreign.example/controller",
				ParentRef:      gatewayv1.ParentReference{Name: "foreign"},
			}
			tcp.Status.Parents = []gatewayv1.RouteParentStatus{foreign}
			var route client.Object = tcp
			if protocol == gatewayv1.UDPProtocolType {
				route = &gatewayv1.UDPRoute{
					ObjectMeta: tcp.ObjectMeta,
					Spec: gatewayv1.UDPRouteSpec{
						CommonRouteSpec: tcp.Spec.CommonRouteSpec,
						Rules:           []gatewayv1.UDPRouteRule{{BackendRefs: []gatewayv1.BackendRef{ref}}},
					},
					Status: gatewayv1.UDPRouteStatus{RouteStatus: tcp.Status.RouteStatus},
				}
			}
			east, west := explicit.DeepCopy(), explicit.DeepCopy()
			east.Name, east.UID, east.Spec.DefaultScope = "east", "east-uid", gatewayv1.GatewayDefaultScopeAll
			west.Name, west.UID, west.Spec.DefaultScope = "west", "west-uid", gatewayv1.GatewayDefaultScopeAll
			denied := east.DeepCopy()
			denied.Namespace, denied.Name, denied.UID = "other", "denied", "denied-uid"
			explicit.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeAll
			explicit.Spec.Listeners = append(explicit.Spec.Listeners, gatewayv1.Listener{Name: "second", Port: 9311, Protocol: protocol})
			r := testReconcilerForChannel(t, "experimental", explicit, east, west, denied, class, route, service, endpoints)
			r.Protocol, r.Coordinator = protocol, &graph.Coordinator{}
			legacyIdentity := bindFixture(t, r, explicit, route, cloud.MemberModeEndpoint)
			controller.SetGatewayListenerScope(r.Config, explicit, protocol)
			if err := r.Update(ctx, explicit); err != nil {
				t.Fatal(err)
			}
			base := viewRoute(route).copyObject()
			controller.SealRouteAnchor(r.Config, route, legacyIdentity, 8080)
			if err := r.Patch(ctx, route, controller.OptimisticMergeFrom(base)); err != nil {
				t.Fatal(err)
			}
			bindGatewayFixture(t, r, east, cloud.MemberModeEndpoint)
			bindGatewayFixture(t, r, west, cloud.MemberModeEndpoint)
			ensured := map[string]int{}
			deleted := map[string]int{}
			eastBusy := false
			explicitPorts := []int{8080}
			r.Writer = applyRouteGraphFunc(func(ctx context.Context, request graph.Request) (cloud.GatewayGraphResult, error) {
				input, err := r.ObserveRouteGraph(ctx, request, request.Gateway)
				if err != nil {
					return cloud.GatewayGraphResult{}, err
				}
				name := request.Identity.GatewayName
				if len(input.Nodes) != 1 || input.Nodes[0].Spec.Identity != request.Identity {
					t.Fatalf("%s %s produced unexpected nodes %+v", request.Action, name, input.Nodes)
				}
				node := input.Nodes[0]
				outcome := cloud.ReadyOutcome()
				switch request.Action {
				case graph.Ensure:
					if node.Disposition != cloud.GraphEnsure || len(node.Spec.Members) != 1 || node.Spec.Members[0].Address != "10.1.2.3" {
						t.Fatalf("unexpected %s forwarding intent %+v", name, node)
					}
					if name == explicit.Name && !slices.Equal(node.Spec.ScopedListenerPorts, explicitPorts) {
						t.Fatalf("explicit Gateway programmed ports %v, want %v", node.Spec.ScopedListenerPorts, explicitPorts)
					}
					ensured[name]++
				case graph.DeleteRoute:
					if node.Disposition != cloud.GraphDelete {
						t.Fatalf("unexpected %s deletion intent %+v", name, node)
					}
					deleted[name]++
					if name == east.Name && eastBusy {
						outcome = cloud.ProgressingOutcome("busy", time.Second)
					}
				default:
					t.Fatalf("unexpected graph action %s", request.Action)
				}
				return cloud.GatewayGraphResult{
					Outcome: outcome,
					Routes:  []cloud.RouteGraphResult{{Identity: request.Identity, Result: cloud.RouteResult{Outcome: outcome}}},
				}, input.Validate(ctx)
			})
			key := client.ObjectKeyFromObject(route)
			reconcile := func() ctrl.Result {
				t.Helper()
				result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			get := func() client.Object {
				t.Helper()
				current := r.newRouteObject()
				if err := r.Get(ctx, key, current); err != nil {
					t.Fatal(err)
				}
				return current
			}
			reconcile()
			reconcile()
			route = get()
			checkRouteCondition(t, route, r.Config.ControllerName, r.Config.Domain()+"/Programmed", metav1.ConditionTrue, "Programmed")
			if ens := ensured[explicit.Name]; ens != 1 || controller.HasRouteGatewayBindings(r.Config, route) {
				t.Fatalf("expected existing singleton before default opt-in, graph ensures=%v", ensured)
			}
			spec, _ := controller.RouteSpecAndParentStatuses(route)
			spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeAll
			explicitRef := spec.ParentRefs[0]
			route.SetGeneration(route.GetGeneration() + 1)
			expectedSpec := viewRoute(viewRoute(route).copyObject()).spec
			if err := r.Update(ctx, route); err != nil {
				t.Fatal(err)
			}
			requests := r.enqueueGateway(ctx, east)
			if !slices.Contains(requests, ctrl.Request{NamespacedName: key}) {
				t.Fatalf("default scope change did not enqueue the opted-in route: %v", requests)
			}
			explicitPorts = []int{8080, 9311}
			reconcile()
			reconcile()
			check := func(explicitImplicit bool, names ...string) {
				t.Helper()
				current := get()
				view := viewRoute(current)
				if !reflect.DeepEqual(view.spec, expectedSpec) {
					t.Fatalf("route spec changed: got %+v, want %+v", view.spec, expectedSpec)
				}
				parentCount := len(names) + 2
				if explicitImplicit {
					parentCount++
				}
				if len(view.status.Parents) != parentCount || !slices.ContainsFunc(view.status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
					return reflect.DeepEqual(parent, foreign)
				}) {
					t.Fatalf("unexpected parent status or missing foreign status %+v", view.status.Parents)
				}
				deniedRef := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(denied.Name), Namespace: ptr.To(gatewayv1.Namespace(denied.Namespace))}
				rejected := r.currentParentCondition(view, deniedRef, "Accepted")
				if rejected == nil || rejected.Status != metav1.ConditionFalse || rejected.Reason != "NotAllowedByListeners" || ensured[denied.Name] != 0 {
					t.Fatalf("namespace-denied default Gateway was not reported accurately: %+v", rejected)
				}
				bindings, err := r.routeBindings(view)
				if err != nil || len(bindings) != len(names) {
					t.Fatalf("expected bindings %v, got %+v, %v", names, bindings, err)
				}
				for _, name := range names {
					if !slices.ContainsFunc(view.status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
						if string(parent.ParentRef.Name) != name || parent.ControllerName != r.Config.ControllerName {
							return false
						}
						if name == explicit.Name && !controller.ParentRefsEqual(parent.ParentRef, view.GetNamespace(), explicitRef, view.GetNamespace()) {
							return false
						}
						condition := meta.FindStatusCondition(parent.Conditions, r.Config.Domain()+"/Programmed")
						return condition != nil && condition.Status == metav1.ConditionTrue
					}) {
						t.Fatalf("missing programmed %s in %+v", name, view.status.Parents)
					}
				}
				legacy := boundGateway(bindings, legacyIdentity)
				if legacy.Identity != legacyIdentity || legacy.Anchor != 8080 || deleted[explicit.Name] != 0 {
					t.Fatalf("default opt-in replaced the existing explicit binding %+v", bindings)
				}
			}
			check(true, explicit.Name, east.Name, west.Name)
			current := viewRoute(get())
			eastRequest := current.graphRequest(controller.RouteIdentity(r.Config, east, current), graph.Ensure, east)
			oldEast, err := r.ObserveRouteGraph(ctx, eastRequest, east)
			if err != nil {
				t.Fatal(err)
			}
			if err := oldEast.Validate(ctx); err != nil {
				t.Fatal(err)
			}
			east.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeNone
			east.Generation++
			if err := r.Update(ctx, east); err != nil {
				t.Fatal(err)
			}
			explicit.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeNone
			explicit.Generation++
			if err := r.Update(ctx, explicit); err != nil {
				t.Fatal(err)
			}
			explicitPorts = []int{8080}
			if err := oldEast.Validate(ctx); !errors.Is(err, controller.ErrGraphInputChanged) {
				t.Fatalf("the withdrawn default Gateway did not fence its prior graph: %v", err)
			}
			reconcile()
			westBefore := ensured[west.Name]
			eastBusy = true
			reconcile()
			bindings, err := r.routeBindings(viewRoute(get()))
			if err != nil || boundGateway(bindings, eastRequest.Identity).Lifecycle != controller.RouteDetaching || ensured[west.Name] <= westBefore {
				t.Fatalf("pending east cleanup blocked west forwarding or lost its checkpoint: bindings %+v, ensures %v, %v", bindings, ensured, err)
			}
			eastBusy = false
			reconcile()
			check(false, explicit.Name, west.Name)
			if deleted[east.Name] != 2 || deleted[west.Name] != 0 {
				t.Fatalf("scope withdrawal did not independently converge: ensured %v, deleted %v", ensured, deleted)
			}

			if err := r.Delete(ctx, get()); err != nil {
				t.Fatal(err)
			}
			reconcile()
			if current := get(); current.GetDeletionTimestamp().IsZero() || !controller.RouteHasControllerBinding(r.Config, current) {
				t.Fatal("route deletion released the finalizer before provider cleanup")
			}
			reconcile()
			if err := r.Get(ctx, key, r.newRouteObject()); !apierrors.IsNotFound(err) ||
				deleted[explicit.Name] != 1 || deleted[east.Name] != 2 || deleted[west.Name] != 1 {
				t.Fatalf("route deletion did not clean up all bindings: %v, %v", deleted, err)
			}
		})
	}
}
