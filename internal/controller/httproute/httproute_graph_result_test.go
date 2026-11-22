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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestHTTPRouteRequiresItsComponentOutcome(t *testing.T) {
	tests := []struct {
		name      string
		result    func(cloud.Identity) cloud.GatewayGraphResult
		wantError bool
		wantState cloud.OutcomeState
	}{
		{name: "missing ready component", wantError: true, result: func(cloud.Identity) cloud.GatewayGraphResult {
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}
		}},
		{name: "missing progressing component", wantState: cloud.OutcomeProgressing, result: func(cloud.Identity) cloud.GatewayGraphResult {
			return cloud.GatewayGraphResult{Outcome: cloud.ProgressingOutcome("Waiting for listener", time.Second)}
		}},
		{name: "invalid outcome", wantError: true, result: func(id cloud.Identity) cloud.GatewayGraphResult {
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Identity: id}}}
		}},
		{name: "foreign identity", wantError: true, result: func(id cloud.Identity) cloud.GatewayGraphResult {
			id.GatewayUID = "other"
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Identity: id, Result: cloud.RouteReadyResult(cloud.RouteState{})}}}
		}},
		{name: "duplicate identity", wantError: true, result: func(id cloud.Identity) cloud.GatewayGraphResult {
			component := cloud.RouteGraphResult{Identity: id, Result: cloud.RouteReadyResult(cloud.RouteState{})}
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{component, component}}
		}},
		{name: "ready component", wantState: cloud.OutcomeReady, result: func(id cloud.Identity) cloud.GatewayGraphResult {
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Routes: []cloud.RouteGraphResult{{Identity: id, Result: cloud.RouteReadyResult(cloud.RouteState{PoolID: "pool-1"})}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			gateway := programmedTestGateway(cfg)
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			route := mapperTestHTTPRoute("default", "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID = "route-uid"
			route.Generation = 1
			seed := &Reconciler{Config: cfg}
			if err := seed.applyRouteBinding(route, gateway); err != nil {
				t.Fatal(err)
			}
			service, endpoints, node := validNodePortBackend("default", "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(class, gateway, route, service, endpoints, node).Build()
			expectedIdentity := controller.RouteIdentity(cfg, gateway, route)
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Coordinator: &graph.Coordinator{}, Writer: fixedRouteGraph{result: test.result(expectedIdentity)}}, nil)
			result, err := reconciler.ensureRouteGraph(context.Background(), route, gateway)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v", err)
			}
			if err == nil && result.Outcome.State != test.wantState {
				t.Fatalf("outcome=%#v", result.Outcome)
			}
		})
	}
}

func TestHTTPRouteCleanupFinishesFromItsOwnResult(t *testing.T) {
	f := newRouteModelInputFixture(t)
	identity := controller.RouteIdentity(testConfig(), f.gateway, f.route)
	peer := identity
	peer.RouteUID, peer.RouteName = "sibling-uid", "sibling"
	f.route.Annotations[testConfig().RouteLifecycleAnnotation()] = controller.RouteDetaching
	failure := errors.New("member cleanup failed")
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ProgressingOutcome("Removing sibling", time.Second),
		Routes: []cloud.RouteGraphResult{
			{Identity: peer, Result: cloud.RouteProgressingResult("Removing sibling", time.Second)},
			{Identity: identity, Result: cloud.RouteReadyResult(cloud.RouteState{})},
		},
	}
	for _, err := range []error{nil, &cloud.GraphError{Identity: peer, Err: failure}} {
		f.reconciler.Writer = fixedRouteGraph{result: result, err: err}
		outcome, gotErr := f.reconciler.deleteRoute(t.Context(), f.route, identity)
		if gotErr != nil || outcome.State != cloud.OutcomeReady {
			t.Fatalf("own cleanup outcome=%v, err=%v (peer error=%v)", outcome, gotErr, err)
		}
	}
	f.reconciler.Writer = fixedRouteGraph{result: result, err: &cloud.GraphError{Identity: identity, Err: failure}}
	if _, err := f.reconciler.deleteRoute(t.Context(), f.route, identity); !errors.Is(err, failure) {
		t.Fatalf("own cleanup failure = %v", err)
	}
}

type fixedRouteGraph struct {
	result cloud.GatewayGraphResult
	err    error
}

func (w fixedRouteGraph) ReconcileGraph(context.Context, graph.Request) (cloud.GatewayGraphResult, error) {
	return w.result, w.err
}

func TestHTTPRouteReportedMatchIncompatibilityKeepsForwardBinding(t *testing.T) {
	f := newRouteModelInputFixture(t)
	component := cloud.RouteReadyResult(cloud.RouteState{PoolID: "forward"})
	component.UnsupportedMatch = "Octavia cannot preserve this redirect's match precedence"
	id := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	f.reconciler.Writer = fixedRouteGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(),
		Routes: []cloud.RouteGraphResult{{Identity: id, Result: component}}}}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	result, err := f.reconciler.Reconcile(t.Context(), request)
	if err != nil || result.RequeueAfter != controller.OpenStackResyncAfter(f.reconciler.Config.OpenStackResyncInterval, f.route.UID) {
		t.Fatalf("want settled incompatibility and periodic resync: %+v %v", result, err)
	}
	var current gatewayv1.HTTPRoute
	if err := f.client.Get(t.Context(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[f.reconciler.Config.RouteLifecycleAnnotation()] != controller.RouteAttached || len(current.Status.Parents) != 1 {
		t.Fatalf("route lost its forwarding binding or parent: %#v", current)
	}
	conditions := current.Status.Parents[0].Conditions
	accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
	programmed := meta.FindStatusCondition(conditions, f.reconciler.Config.Domain()+"/Programmed")
	if accepted == nil || accepted.Status != metav1.ConditionFalse ||
		accepted.Reason != string(gatewayv1.RouteReasonUnsupportedValue) || accepted.Message != component.UnsupportedMatch ||
		programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.Reason != "Invalid" {
		t.Fatalf("want permanent match incompatibility, conditions: %#v", conditions)
	}
}
