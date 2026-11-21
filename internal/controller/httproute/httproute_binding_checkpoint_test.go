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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestLegacyRouteBindingCompletesBeforeRestrictiveGatewayEditIsProgrammed(t *testing.T) {
	t.Run("single port", func(t *testing.T) { testLegacyRouteBindingCompletes(t, false) })
	t.Run("distinct ports", func(t *testing.T) { testLegacyRouteBindingCompletes(t, true) })
}

func testLegacyRouteBindingCompletes(t *testing.T, distinct bool) {
	t.Helper()
	ctx := t.Context()
	cfg := testConfig()
	class, gateway := providerRouteParentObjects(cfg)
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
	route.UID, route.Generation = "route-uid", 1
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
		WithStatusSubresource(gateway, route).
		WithObjects(class, gateway, route, service, endpoints, node).Build()
	provider := &checkpointGraphProvider{}
	coordinator := &graph.Coordinator{}
	gatewayReconciler := newTestGatewayReconciler(gatewaycontroller.Reconciler{
		Client: kubeClient, APIReader: kubeClient, Coordinator: coordinator, Config: cfg,
	}, provider)
	routeReconciler := &Reconciler{
		Client: kubeClient, APIReader: kubeClient, Coordinator: coordinator, Writer: gatewayReconciler.Writer, Config: cfg,
	}
	routeRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	gatewayRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	for range 2 {
		if _, err := routeReconciler.Reconcile(ctx, routeRequest); err != nil {
			t.Fatal(err)
		}
	}
	if len(provider.routeSpecs) != 1 || provider.routeSpecs[0].Hostname != "" {
		t.Fatalf("initial forwarding = %#v, want one hostless route", provider.routeSpecs)
	}
	if err := kubeClient.Get(ctx, routeRequest.NamespacedName, route); err != nil {
		t.Fatal(err)
	}
	delete(route.Annotations, cfg.RouteProjectIDAnnotation())
	if distinct {
		delete(route.Annotations, cfg.RouteClusterIDAnnotation())
	}
	if err := kubeClient.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, gatewayRequest.NamespacedName, gateway); err != nil {
		t.Fatal(err)
	}
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("api.example.test"))
	if distinct {
		gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
			Name: "other", Protocol: gatewayv1.HTTPProtocolType, Port: 8080, Hostname: ptr.To(gatewayv1.Hostname("other.example.test")),
		})
		// As the route had no hostname before the Gateway edit, restrict the
		// other section to a different namespace policy to keep selection unique.
		gateway.Spec.Listeners[1].AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
			From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"other": "true"}},
		}}
	}
	gateway.Generation++
	if err := kubeClient.Update(ctx, gateway); err != nil {
		t.Fatal(err)
	}
	if _, err := gatewayReconciler.Reconcile(ctx, gatewayRequest); err == nil {
		t.Fatal("Gateway writer accepted a route with an unfinished binding checkpoint")
	}
	if result, err := routeReconciler.Reconcile(ctx, routeRequest); err != nil || !result.Requeue {
		t.Fatalf("legacy route binding checkpoint = %#v, %v; want immediate requeue", result, err)
	}
	if err := kubeClient.Get(ctx, routeRequest.NamespacedName, route); err != nil {
		t.Fatal(err)
	}
	if route.Annotations[cfg.RouteProjectIDAnnotation()] != cfg.OpenStackProjectID ||
		route.Annotations[cfg.RouteClusterIDAnnotation()] != cfg.ClusterID ||
		route.Annotations[cfg.RouteLifecycleAnnotation()] != controller.RouteAttached ||
		len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 {
		t.Fatalf("checkpoint metadata = %#v, ensures = %d, deletes = %d", route.Annotations, len(provider.routeSpecs), len(provider.deletedRoutes))
	}
	if distinct {
		for range 2 {
			if _, err := gatewayReconciler.Reconcile(ctx, gatewayRequest); err != nil {
				t.Fatal(err)
			}
		}
		if err := kubeClient.Get(ctx, routeRequest.NamespacedName, route); err != nil {
			t.Fatal(err)
		}
		if port := controller.ScopedRouteAnchor(cfg, route, gatewayv1.HTTPProtocolType); port != 80 {
			t.Fatalf("route enrollment anchor = %d, want 80", port)
		}
	}
	if _, err := gatewayReconciler.Reconcile(ctx, gatewayRequest); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, gatewayRequest.NamespacedName, gateway); err != nil {
		t.Fatal(err)
	}
	if !gatewayProgrammedForGeneration(gateway) || len(provider.routeSpecs) != 2 ||
		provider.routeSpecs[1].Hostname != "api.example.test" || provider.routeSpecs[1].Identity != provider.routeSpecs[0].Identity {
		t.Fatalf("Gateway status = %#v, forwarding = %#v; want current generation with restricted forwarding and unchanged route identity", gateway.Status, provider.routeSpecs)
	}
}

type checkpointGraphProvider struct{ recordingProvider }

func (p *checkpointGraphProvider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	result, err := p.recordingProvider.ReconcileGatewayGraph(ctx, spec)
	for _, listener := range spec.Gateway.Spec.ScopedListeners {
		result.Gateway.ScopedListenerReadyPorts = append(result.Gateway.ScopedListenerReadyPorts, listener.Port)
	}
	return result, err
}

func TestPendingGatewayCannotCompleteRouteBindingWithoutCurrentOwnership(t *testing.T) {
	for _, test := range []struct {
		name           string
		previousUID    bool
		keepFinalizers bool
	}{
		{name: "annotations without finalizers"},
		{name: "replaced Gateway with the same name", previousUID: true, keepFinalizers: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			class, gateway := providerRouteParentObjects(cfg)
			gateway.Generation++
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: "edge"}, "backend")
			route.UID, route.Generation = "route-uid", 1
			r := &Reconciler{Config: cfg}
			boundGateway := gateway.DeepCopy()
			if test.previousUID {
				boundGateway.UID = "previous-gateway-uid"
			}
			r.applyRouteBinding(route, boundGateway)
			delete(route.Annotations, cfg.RouteProjectIDAnnotation())
			if !test.keepFinalizers {
				route.Finalizers = nil
			}
			service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).
				WithObjects(class, gateway, route, service, endpoints, node).Build()
			r.Client, r.APIReader = kubeClient, kubeClient
			if persisted, err := r.bindRoute(t.Context(), route, gateway); persisted || !errors.Is(err, errHTTPRouteChanged) {
				t.Fatalf("route binding checkpoint = %t, %v; want stale-state rejection", persisted, err)
			}
			if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
				t.Fatal(err)
			}
			if route.Annotations[cfg.RouteProjectIDAnnotation()] != "" ||
				route.Annotations[cfg.RouteGatewayUIDAnnotation()] != string(boundGateway.UID) {
				t.Fatalf("unproven route binding changed: %#v", route.Annotations)
			}
		})
	}
}
