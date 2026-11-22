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
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestHTTPRouteProfileElectionFencesProviderAndParentStatus(t *testing.T) {
	for _, stage := range []graph.Stage{graph.StageLockWait, graph.StageProvider} {
		t.Run(string(stage), func(t *testing.T) {
			f, _, grpc := newRouteElectionFixture(t)
			f.create(t, grpc)
			provider := &recordingProvider{}
			r := newTestReconciler(*f.reconciler, provider)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
			reconcile := func() error {
				_, err := r.Reconcile(t.Context(), request)
				return err
			}
			readRoute := func() *gatewayv1.HTTPRoute {
				t.Helper()
				var route gatewayv1.HTTPRoute
				if err := f.client.Get(t.Context(), request.NamespacedName, &route); err != nil {
					t.Fatal(err)
				}
				return &route
			}
			if err := reconcile(); err != nil {
				t.Fatal(err)
			}
			current := readRoute()
			assertElectionParentAccepted(t, current, f.route.Spec.ParentRefs[0], metav1.ConditionFalse)
			assertElectionParentAccepted(t, current, f.route.Spec.ParentRefs[1], metav1.ConditionTrue)
			if len(provider.routeSpecs) != 1 || !slices.Equal(electionHostnames(provider.routeSpecs[0]), []string{"delegated.example.test"}) {
				t.Fatalf("the initial winning hostname should be delegated.example.test: %+v", provider.routeSpecs)
			}

			var cleared bool
			r.Writer.(*graph.Writer).Observer = electionStageObserver(func(observed graph.Stage) {
				if observed == stage && !cleared {
					cleared = true
					if err := f.client.Delete(t.Context(), grpc); err != nil {
						t.Fatal(err)
					}
				}
			})
			if err := reconcile(); !errors.Is(err, errHTTPRouteChanged) {
				t.Fatalf("reconcile when the direct parent recovers = %v, want stale input", err)
			}
			wantWrites := 1
			if stage == graph.StageProvider {
				wantWrites++
			}
			if len(provider.routeSpecs) != wantWrites || len(provider.deletedRoutes) != 0 ||
				!reflect.DeepEqual(current.Status, readRoute().Status) {
				t.Fatalf("stale observation changed forwarding or parent status: writes=%d deletes=%d",
					len(provider.routeSpecs), len(provider.deletedRoutes))
			}

			if err := reconcile(); err != nil {
				t.Fatal(err)
			}
			recovered := readRoute()
			assertElectionParentAccepted(t, recovered, f.route.Spec.ParentRefs[0], metav1.ConditionTrue)
			assertElectionParentAccepted(t, recovered, f.route.Spec.ParentRefs[1], metav1.ConditionTrue)
			hostnames := electionHostnames(provider.routeSpecs[len(provider.routeSpecs)-1])
			if want := []string{"delegated.example.test", "direct.example.test"}; !slices.Equal(hostnames, want) {
				t.Fatalf("recovered forwarding hostnames = %v, want %v", hostnames, want)
			}
		})
	}
}

func electionHostnames(spec cloud.RouteSpec) []string {
	var hostnames []string
	for _, backend := range spec.HTTPBackends {
		for _, match := range backend.Matches {
			hostnames = append(hostnames, match.Hostname)
		}
	}
	slices.Sort(hostnames)
	return hostnames
}

func TestHTTPRouteDetachCheckpointRevalidatesElectionAndListenerSet(t *testing.T) {
	for _, cause := range []string{"gRPC election", "ListenerSet admission"} {
		t.Run(cause, func(t *testing.T) {
			f, set, grpc := newRouteElectionFixture(t)
			var blocker client.Object
			var beforeSeal func()
			if cause == "gRPC election" {
				f.route.Spec.ParentRefs = f.route.Spec.ParentRefs[:1]
				blocker = grpc
				f.create(t, blocker)
			} else {
				f.route.Spec.ParentRefs = f.route.Spec.ParentRefs[1:]
				f.reconciler.Config.BackendH2C = false
				older := set.DeepCopy()
				older.UID, older.Name, older.ResourceVersion = "older", "older", ""
				older.CreationTimestamp = metav1.Unix(1, 0)
				blocker = older
				beforeSeal = func() { f.create(t, blocker) }
			}
			if err := f.client.Update(t.Context(), f.route); err != nil {
				t.Fatal(err)
			}

			var gotRoute, gotSets int
			f.reconciler.APIReader = &electionCheckpointReader{
				Reader: f.client,
				beforeGet: func(object client.Object) {
					if _, ok := object.(*gatewayv1.HTTPRoute); ok {
						gotRoute++
						if gotRoute == 2 {
							if err := f.client.Delete(t.Context(), blocker); err != nil {
								t.Fatal(err)
							}
						}
					}
				},
				beforeList: func(list client.ObjectList) {
					if _, ok := list.(*gatewayv1.ListenerSetList); ok && beforeSeal != nil {
						gotSets++
						if gotSets == 2 {
							beforeSeal()
						}
					}
				},
			}
			identity := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
			if err := f.reconciler.checkpointRouteDetaching(t.Context(), f.route, identity); !errors.Is(err, errHTTPRouteChanged) {
				t.Fatalf("checkpoint after recovery = %v, want stale input", err)
			}
			var current gatewayv1.HTTPRoute
			key := client.ObjectKeyFromObject(f.route)
			if err := f.client.Get(t.Context(), key, &current); err != nil {
				t.Fatal(err)
			}
			lifecycle := f.reconciler.Config.RouteLifecycleAnnotation()
			if current.Annotations[lifecycle] != controller.RouteAttached {
				t.Fatalf("the recovered route was marked %s", current.Annotations[lifecycle])
			}
			blocker.SetResourceVersion("")
			f.create(t, blocker)
			f.reconciler.APIReader = f.client
			r := newTestReconciler(*f.reconciler, &recordingProvider{})
			for range 2 {
				if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatalf("reconcile with persistent rejection: %v", err)
				}
			}
			if err := f.client.Get(t.Context(), key, &current); err != nil {
				t.Fatal(err)
			}
			if current.Annotations[lifecycle] != controller.RouteDetaching {
				t.Fatalf("persistent rejection left the route %s", current.Annotations[lifecycle])
			}
		})
	}
}

func newRouteElectionFixture(t *testing.T) (*routeModelInputFixture, *gatewayv1.ListenerSet, *gatewayv1.GRPCRoute) {
	t.Helper()
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.BackendH2C, f.reconciler.Config.FixedHTTPResponses = true, true
	listener := &f.gateway.Spec.Listeners[0]
	listener.Hostname = ptr.To(gatewayv1.Hostname("direct.example.test"))
	f.gateway.Spec.AllowedListeners = &gatewayv1.AllowedListeners{
		Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromSame)},
	}
	cfg := f.reconciler.Config
	f.gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.HTTPProtocolType)
	controller.SetGatewayListenerScope(cfg, f.gateway, gatewayv1.HTTPProtocolType)
	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.gateway.Namespace, Name: "delegated", UID: "delegated-uid", CreationTimestamp: metav1.Unix(2, 0)},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: gatewayv1.ObjectName(f.gateway.Name)},
			Listeners: []gatewayv1.ListenerEntry{{
				Name: listener.Name, Port: listener.Port, Protocol: listener.Protocol,
				Hostname: ptr.To(gatewayv1.Hostname("delegated.example.test")),
			}},
		},
	}
	f.create(t, set)
	f.route.CreationTimestamp = metav1.Unix(3, 0)
	f.route.Spec.Hostnames = []gatewayv1.Hostname{*listener.Hostname, *set.Spec.Listeners[0].Hostname}
	f.route.Spec.ParentRefs[0].SectionName = &listener.Name
	f.route.Spec.ParentRefs = append(f.route.Spec.ParentRefs, gatewayv1.ParentReference{
		Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: gatewayv1.ObjectName(set.Name), SectionName: &listener.Name,
	})
	if err := f.reconciler.applyRouteBinding(f.route, f.gateway); err != nil {
		t.Fatal(err)
	}
	for _, object := range []client.Object{f.gateway, f.route} {
		if err := f.client.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace: f.route.Namespace, Name: "grpc", UID: "grpc-uid", CreationTimestamp: metav1.Unix(1, 0),
	}}
	grpc.Spec.ParentRefs = f.route.Spec.ParentRefs[:1]
	grpc.Spec.Hostnames = []gatewayv1.Hostname{*listener.Hostname}
	grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule(gatewayv1.ObjectName(f.service.Name))}
	return f, set, grpc
}

func assertElectionParentAccepted(t *testing.T, route *gatewayv1.HTTPRoute, ref gatewayv1.ParentReference, want metav1.ConditionStatus) {
	t.Helper()
	for _, parent := range route.Status.Parents {
		if controller.ParentRefsEqual(parent.ParentRef, route.Namespace, ref, route.Namespace) {
			accepted := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionAccepted))
			if accepted == nil || accepted.Status != want || want == metav1.ConditionFalse &&
				accepted.Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
				t.Fatalf("parent %s acceptance = %+v, want %s", ref.Name, accepted, want)
			}
			return
		}
	}
	t.Fatalf("parent %s missing from status", ref.Name)
}

type electionStageObserver func(graph.Stage)

func (observe electionStageObserver) ObserveGraphStage(stage graph.Stage, _ time.Duration, _ bool) {
	observe(stage)
}

type electionCheckpointReader struct {
	client.Reader
	beforeGet  func(client.Object)
	beforeList func(client.ObjectList)
}

func (r *electionCheckpointReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	r.beforeGet(object)
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *electionCheckpointReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	r.beforeList(list)
	return r.Reader.List(ctx, list, options...)
}
