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
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestGatewayUDPEnrollmentCheckpointsOneRoutePerReconcile(t *testing.T) {
	for _, change := range []string{"keep both ports", "remove second port", "delete Gateway"} {
		t.Run(change, func(t *testing.T) {
			ctx := t.Context()
			config := testConfig()
			gateway := boundSingletonUDPTestGateway(config)
			gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
				Name: "private", Port: 5353, Protocol: gatewayv1.UDPProtocolType,
			})
			gatewayClass := &gatewayv1.GatewayClass{
				ObjectMeta: metav1.ObjectMeta{Name: "openstack"},
				Spec:       gatewayv1.GatewayClassSpec{ControllerName: config.ControllerName},
			}
			first := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "first", UID: "route-one"}}
			second := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "second", UID: "route-two"}}
			for _, route := range []*gatewayv1.UDPRoute{first, second} {
				controller.ApplyRouteBinding(config, route, controller.RouteIdentity(config, gateway, route))
			}
			kube := indexedFakeClientBuilder(testScheme(t), config).WithObjects(gateway, gatewayClass, first, second).Build()
			key := client.ObjectKeyFromObject(gateway)
			var patches int
			writer := interceptor.NewClient(kube, interceptor.Funcs{
				Patch: func(ctx context.Context, kube client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if err := kube.Patch(ctx, obj, patch, opts...); err != nil {
						return err
					}
					if _, ok := obj.(*gatewayv1.UDPRoute); !ok {
						return nil
					}
					patches++
					if patches != 1 || change == "keep both ports" {
						return nil
					}
					var current gatewayv1.Gateway
					if err := kube.Get(ctx, key, &current); err != nil {
						return err
					}
					if change == "delete Gateway" {
						return kube.Delete(ctx, &current)
					}
					current.Spec.Listeners = current.Spec.Listeners[:1]
					current.Generation++
					return kube.Update(ctx, &current)
				},
			})
			r := &Reconciler{Client: writer, APIReader: kube, Coordinator: &graph.Coordinator{}, Config: config}
			readGateway := func() *gatewayv1.Gateway {
				t.Helper()
				var current gatewayv1.Gateway
				if err := kube.Get(ctx, key, &current); err != nil {
					t.Fatal(err)
				}
				return &current
			}

			err := r.bindGateway(ctx, gateway, "53")
			if patches != 1 || err != nil || controller.GatewayListenerScope(config, readGateway()) == gatewayv1.UDPProtocolType {
				t.Fatalf("first checkpoint patched %d UDPRoutes, returned %v, or prematurely marked the Gateway", patches, err)
			}
			if change == "delete Gateway" {
				if err := r.bindGateway(ctx, gateway, "53"); !errors.Is(err, errGatewayChanged) {
					t.Fatalf("reconcile deleting Gateway: got %v, want changed input", err)
				}
			} else if err := r.bindGateway(ctx, readGateway(), "53"); err != nil {
				t.Fatal(err)
			}
			if change != "keep both ports" {
				if patches != 1 || controller.GatewayListenerScope(config, readGateway()) == gatewayv1.UDPProtocolType {
					t.Fatalf("changed Gateway continued enrollment after %d route checkpoints", patches)
				}
				return
			}
			if patches != 2 || controller.GatewayListenerScope(config, readGateway()) == gatewayv1.UDPProtocolType {
				t.Fatalf("the second checkpoint must seal only the remaining route, got %d route patches", patches)
			}
			if err := r.bindGateway(ctx, readGateway(), "53"); err != nil {
				t.Fatal(err)
			}
			if controller.GatewayListenerScope(config, readGateway()) != gatewayv1.UDPProtocolType {
				t.Fatal("Gateway remained unmarked after both route checkpoints were durable")
			}
		})
	}
}

func TestUDPRouteDeletionUsesOnlyVerifiedScopedAuthority(t *testing.T) {
	for _, test := range []struct {
		name       string
		routePort  int
		edit       func(Config, *gatewayv1.Gateway)
		wantScoped bool
		wantError  bool
	}{
		{
			name: "legacy route can detach with damaged legacy Gateway topology",
			edit: func(config Config, gateway *gatewayv1.Gateway) {
				gateway.Annotations[config.GatewayTopologyAnnotation()] = "damaged"
			},
		},
		{
			name: "missing Gateway mode with surviving scoped finalizer",
			edit: func(config Config, gateway *gatewayv1.Gateway) {
				controller.SetGatewayListenerScope(config, gateway, gatewayv1.UDPProtocolType)
				delete(gateway.Annotations, config.GatewayListenerModeAnnotation(gatewayv1.UDPProtocolType))
			},
			wantError: true,
		},
		{
			name:      "scoped Gateway topology damage",
			routePort: 53,
			edit: func(config Config, gateway *gatewayv1.Gateway) {
				controller.SetGatewayListenerScope(config, gateway, gatewayv1.UDPProtocolType)
				gateway.Annotations[config.GatewayTopologyAnnotation()] = "damaged"
			},
			wantError: true,
		},
		{
			name: "partially enrolled route with matching anchor", routePort: 53, wantScoped: true,
		},
		{
			name: "partially enrolled route with mismatched anchor", routePort: 5353, wantError: true,
		},
		{
			name:      "old route can detach after Gateway teardown",
			routePort: 53,
			edit: func(_ Config, gateway *gatewayv1.Gateway) {
				gateway.Annotations = nil
				gateway.Finalizers = nil
			},
			wantScoped: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			gateway := boundSingletonUDPTestGateway(config)
			if err := controller.BindGatewayTopology(config, gateway, testBoundTopology()); err != nil {
				t.Fatal(err)
			}
			if test.edit != nil {
				test.edit(config, gateway)
			}
			route := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dns", UID: "route"}}
			identity := controller.RouteIdentity(config, gateway, route)
			controller.ApplyRouteBinding(config, route, identity)
			route.Annotations[config.RouteLifecycleAnnotation()] = controller.RouteDetaching
			if test.routePort != 0 {
				controller.SealRouteAnchor(config, route, identity, test.routePort)
			}
			kube := indexedFakeClientBuilder(testScheme(t), config).WithObjects(gateway).Build()
			r := &Reconciler{APIReader: kube, Config: config}
			input, err := r.ObserveGatewayGraph(t.Context(), graph.Request{Identity: identity, Action: graph.DeleteRoute, UDPRoute: route})
			if test.wantError {
				if !errors.Is(err, cloud.ErrOwnershipConflict) {
					t.Fatalf("route deletion got %v, want ownership conflict", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if input.Object == nil || input.Object.UID != gateway.UID || input.Node.Disposition != cloud.GraphPreserve ||
				(input.Node.Spec.ScopedListeners != nil) != test.wantScoped || test.wantScoped && input.Node.Spec.ListenerPort != test.routePort {
				t.Fatalf("unexpected preservation graph: %+v", input)
			}
		})
	}
}

func boundSingletonUDPTestGateway(config Config) *gatewayv1.Gateway {
	annotations := gatewayBindingAnnotations(config, "53")
	annotations[config.GatewayListenerProtocolAnnotation()] = cloud.ListenerProtocolUDP
	return &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway", Generation: 1,
			Annotations: annotations, Finalizers: []string{config.GatewayFinalizer()},
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "public", Port: 53, Protocol: gatewayv1.UDPProtocolType}},
		},
	}
}

func TestGatewayEnrollsDistinctTCPListenersWithoutChangingAnchor(t *testing.T) {
	config := testConfig()
	gateway := boundSingletonUDPTestGateway(config)
	gateway.Annotations[config.GatewayListenerProtocolAnnotation()] = cloud.ListenerProtocolTCP
	gateway.Spec.Listeners = []gatewayv1.Listener{
		{Name: "one", Port: 53, Protocol: gatewayv1.TCPProtocolType},
		{Name: "two", Port: 9301, Protocol: gatewayv1.TCPProtocolType},
		{Name: "three", Port: 9302, Protocol: gatewayv1.TCPProtocolType},
	}
	if err := controller.BindGatewayTopology(config, gateway, testBoundTopology()); err != nil {
		t.Fatal(err)
	}
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "openstack"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: config.ControllerName},
	}
	route := &gatewayv1.TCPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "echo", UID: "route"}}
	identity := controller.RouteIdentity(config, gateway, route)
	controller.ApplyRouteBinding(config, route, identity)
	legacyRouteFinalizer := config.RouteBindingFinalizer(identity.ClusterID, identity.OpenStackProjectID, identity.GatewayNamespace, identity.GatewayName, identity.GatewayUID)
	kube := indexedFakeClientBuilder(testScheme(t), config).WithObjects(gateway, class, route).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Coordinator: &graph.Coordinator{}, Config: config}

	if err := r.bindGateway(t.Context(), gateway, "53"); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
		t.Fatal(err)
	}
	for _, finalizer := range route.Finalizers {
		if finalizer == legacyRouteFinalizer {
			t.Fatal("scoped TCP route retained the legacy cleanup checkpoint")
		}
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(gateway), gateway); err != nil {
		t.Fatal(err)
	}
	if err := r.bindGateway(t.Context(), gateway, "53"); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(gateway), gateway); err != nil {
		t.Fatal(err)
	}
	input, err := r.ObserveGatewayGraph(t.Context(), graph.Request{Identity: controller.GatewayIdentity(config, gateway), Action: graph.Ensure, Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	spec := input.Node.Spec
	if spec.ListenerProtocol != cloud.ListenerProtocolTCP || spec.ListenerPort != 53 ||
		len(spec.ScopedListeners) != 3 || spec.ScopedListeners[0].Port != 53 || spec.ScopedListeners[1].Port != 9301 || spec.ScopedListeners[2].Port != 9302 {
		t.Fatalf("TCP enrollment produced %+v", spec)
	}
}
