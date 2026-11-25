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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestScopedTCPForwardingTargetFences(t *testing.T) {
	for _, scenario := range []string{"selector edit retains a listener", "peer election changes before writer"} {
		t.Run(scenario, func(t *testing.T) {
			gw, class, route := routeFixture()
			gw.Spec.Listeners = append(gw.Spec.Listeners, gatewayv1.Listener{Name: "second", Port: 9311, Protocol: gatewayv1.TCPProtocolType})
			peer := route.DeepCopy()
			peer.Name, peer.UID = "peer", "peer"
			peer.CreationTimestamp = metav1.NewTime(time.Unix(50, 0))
			peer.Spec.ParentRefs[0].Port = ptr.To(gatewayv1.PortNumber(9999))
			ref, svc, eps := transportBackendFixture(corev1.ProtocolTCP, "app", "backend", 1, "10.1.2.3")
			route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{ref}
			r := testReconciler(t, gw, class, route, peer, svc, eps)
			r.Coordinator = &graph.Coordinator{}
			id := bindFixture(t, r, gw, route, cloud.MemberModeEndpoint)
			controller.SetGatewayListenerScope(r.Config, gw, gatewayv1.TCPProtocolType)
			if err := r.Update(t.Context(), gw); err != nil {
				t.Fatal(err)
			}
			original := route.DeepCopy()
			controller.SealRouteAnchor(r.Config, route, id, 8080)
			if err := r.Patch(t.Context(), route, controller.OptimisticMergeFrom(original)); err != nil {
				t.Fatal(err)
			}
			route = reconcileTCPRoute(t, r, route)
			r.Writer = readyRouteWriter(t, r)
			route = reconcileTCPRoute(t, r, route)
			if scenario == "selector edit retains a listener" {
				route.Spec.ParentRefs[0].Port = ptr.To(gatewayv1.PortNumber(8080))
				route.Generation++
				if err := r.Update(t.Context(), route); err != nil {
					t.Fatal(err)
				}
				input, err := r.ObserveRouteGraph(t.Context(), graph.Request{Identity: controller.GatewayIdentity(r.Config, gw), Action: graph.Ensure, Gateway: gw}, gw)
				if err != nil {
					t.Fatal(err)
				}
				if err := input.Validate(t.Context()); err != nil {
					t.Fatal(err)
				}
				n := input.Nodes[0]
				if n.Disposition != cloud.GraphPreserve || !slices.Equal(n.RetainListenerPorts, []int{8080}) {
					t.Fatalf("retained listener = %+v", n)
				}
				return
			}
			originalTarget := r.currentParentCondition(viewRoute(route), route.Spec.ParentRefs[0], r.forwardingTargetType()).Message
			// Exercise status advancement from Pending to True, not an unchanged previous condition.
			pending := viewRoute(route)
			plan, err := r.planRoute(t.Context(), pending, id, true)
			if err != nil {
				t.Fatal(err)
			}
			plan.targetStatus = routeCondition(pending, r.forwardingTargetType(), metav1.ConditionFalse, "Pending", originalTarget)
			if err := r.patchStatuses(t.Context(), pending, plan); err != nil {
				t.Fatal(err)
			}
			r.Writer = applyRouteGraphFunc(func(ctx context.Context, req graph.Request) (cloud.GatewayGraphResult, error) {
				if err := r.Get(ctx, client.ObjectKeyFromObject(peer), peer); err != nil {
					t.Fatal(err)
				}
				peer.Spec.ParentRefs[0].Port = ptr.To(gatewayv1.PortNumber(9311))
				peer.Generation++
				if err := r.Update(ctx, peer); err != nil {
					t.Fatal(err)
				}
				input, err := r.ObserveRouteGraph(ctx, req, req.Gateway)
				if err != nil {
					return cloud.GatewayGraphResult{}, err
				}
				if len(input.Nodes) != 1 || len(input.Nodes[0].Spec.Members) != 1 || !slices.Equal(input.Nodes[0].Spec.ScopedListenerPorts, []int{8080}) {
					t.Fatalf("unexpected nodes %+v", input.Nodes)
				}
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Identity: id, Result: cloud.RouteReadyResult(cloud.RouteState{})}}}, input.Validate(ctx)
			})
			key := client.ObjectKeyFromObject(route)
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); !errors.Is(err, controller.ErrGraphInputChanged) {
				t.Fatalf("stale graph reconciliation returned %v", err)
			}
			if err := r.Get(t.Context(), key, route); err != nil {
				t.Fatal(err)
			}
			written := r.currentParentCondition(viewRoute(route), route.Spec.ParentRefs[0], r.forwardingTargetType())
			if written == nil || written.Status != metav1.ConditionFalse || written.Message != originalTarget {
				t.Fatalf("certified stale forwarding target %+v", written)
			}
		})
	}
}
