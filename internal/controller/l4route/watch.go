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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// SetupWithManager registers the configured route kind and its attachment and backend dependencies.
func (r *Reconciler) SetupWithManager(manager ctrl.Manager) error {
	if r.Coordinator == nil {
		return graph.ErrCoordinatorRequired
	}
	if r.APIReader == nil {
		return controller.ErrAPIReaderRequired
	}
	if r.Writer == nil {
		return errors.New("gateway graph writer is required")
	}
	if r.protocol() != gatewayv1.TCPProtocolType && r.protocol() != gatewayv1.UDPProtocolType {
		return fmt.Errorf("unsupported transport route protocol %q", r.Protocol)
	}
	routeChanges := builder.WithPredicates(r.routeChanges())
	route := r.newRouteObject()
	return ctrl.NewControllerManagedBy(manager).
		For(route, routeChanges).
		Watches(route, handler.EnqueueRequestsFromMapFunc(r.enqueuePeers), routeChanges).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(r.enqueueGateway), builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(r.Config))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.enqueueClass), builder.WithPredicates(controller.GatewayClassForGatewayPredicate())).
		WatchesMetadata(&apiextensionsv1.CustomResourceDefinition{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAll), builder.WithPredicates(controller.GatewayAPICRDReconcilePredicate())).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.enqueueNamespace), builder.WithPredicates(controller.NamespacePredicate())).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.enqueueService), builder.WithPredicates(controller.ServicePredicate())).
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(r.enqueueEndpointSlice), builder.WithPredicates(controller.EndpointSlicePredicate())).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.enqueueNode), builder.WithPredicates(controller.NodePredicate(""))).
		Watches(&gatewayv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(r.enqueueGrant), builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
		Named(strings.ToLower(string(r.kind()))).
		Complete(r)
}

func (r *Reconciler) routeChanges() predicate.Predicate {
	changes := controller.GenerationOrDeletionChangedPredicate()
	return predicate.Funcs{
		UpdateFunc: func(event event.UpdateEvent) bool {
			if changes.Update(event) || !slices.Equal(event.ObjectOld.GetFinalizers(), event.ObjectNew.GetFinalizers()) {
				return true
			}
			old, current := event.ObjectOld.GetAnnotations(), event.ObjectNew.GetAnnotations()
			for _, key := range []string{
				r.Config.RouteLifecycleAnnotation(), r.Config.RouteGatewayUIDAnnotation(),
				r.Config.RouteGatewayNameAnnotation(), r.Config.RouteGatewayNamespaceAnnotation(),
				r.Config.RouteClusterIDAnnotation(), r.Config.RouteProjectIDAnnotation(), r.Config.RouteListenerPortAnnotation(gatewayv1.UDPProtocolType), r.Config.RouteListenerPortAnnotation(gatewayv1.TCPProtocolType),
			} {
				if old[key] != current[key] {
					return true
				}
			}
			return false
		},
	}
}

type routeRequests map[client.ObjectKey]struct{}

func (r *Reconciler) cachedRoutes(ctx context.Context, opts ...client.ListOption) []*transportRoute {
	// Informers return a synthetic continuation even for a complete local
	// listing. These results only enqueue keys; reconciliation rereads the API.
	routes, _, err := r.listRoutes(ctx, r.Client, opts...)
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list routes for dependency change", "kind", r.kind())
		return nil
	}
	return routes
}

func (keys routeRequests) add(routes []*transportRoute) {
	for _, route := range routes {
		keys[client.ObjectKeyFromObject(route)] = struct{}{}
	}
}

func (r *Reconciler) addGatewayRoutes(ctx context.Context, keys routeRequests, gateway client.ObjectKey, indexes ...string) {
	for _, index := range indexes {
		keys.add(r.cachedRoutes(ctx, client.MatchingFields{index: controller.ObjectKeyString(gateway)}))
	}
}

func (r *Reconciler) enqueueGateway(ctx context.Context, object client.Object) []reconcile.Request {
	keys := routeRequests{}
	r.addGatewayRoutes(ctx, keys, client.ObjectKeyFromObject(object), controller.IndexL4RouteByParentGateway, controller.IndexL4RouteByStatusGateway, controller.IndexL4RouteByBoundGateway)
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueClass(ctx context.Context, object client.Object) []reconcile.Request {
	keys := routeRequests{}
	for _, gateway := range r.gatewaysForClass(ctx, object.GetName()) {
		r.addGatewayRoutes(ctx, keys, client.ObjectKeyFromObject(&gateway), controller.IndexL4RouteByParentGateway, controller.IndexL4RouteByStatusGateway, controller.IndexL4RouteByBoundGateway)
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) gatewaysForClass(ctx context.Context, name string) []gatewayv1.Gateway {
	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways, client.MatchingFields{controller.IndexGatewayByClass: name}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for GatewayClass", "gatewayClass", name)
		return nil
	}
	return gateways.Items
}

func (r *Reconciler) enqueuePeers(ctx context.Context, object client.Object) []reconcile.Request {
	route := viewRoute(object)
	if route == nil || route.protocol != r.protocol() {
		return nil
	}
	keys := routeRequests{}
	r.addPeers(ctx, keys, []*transportRoute{route})
	return controller.SortedRequests(keys)
}

func (r *Reconciler) addPeers(ctx context.Context, keys routeRequests, routes []*transportRoute) {
	checked := make(map[client.ObjectKey]bool)
	add := func(key client.ObjectKey) {
		if !checked[key] {
			r.addGatewayRoutes(ctx, keys, key, controller.IndexL4RouteByParentGateway)
			checked[key] = true
		}
	}
	for _, route := range routes {
		for _, ref := range route.parentRefs {
			if controller.IsGatewayParentRef(ref) {
				add(gatewayKey(route, ref))
			}
		}
		if identity, present, err := r.routeBinding(route); present && err == nil {
			add(client.ObjectKey{Namespace: identity.GatewayNamespace, Name: identity.GatewayName})
		}
	}
}

func (r *Reconciler) enqueueNamespace(ctx context.Context, object client.Object) []reconcile.Request {
	routes := r.cachedRoutes(ctx, client.InNamespace(object.GetName()))
	keys := routeRequests{}
	keys.add(routes)
	r.addPeers(ctx, keys, routes)
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueAll(ctx context.Context, _ client.Object) []reconcile.Request {
	keys := routeRequests{}
	keys.add(r.cachedRoutes(ctx))
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueNode(ctx context.Context, _ client.Object) []reconcile.Request {
	var classes gatewayv1.GatewayClassList
	if err := r.List(ctx, &classes, client.MatchingFields{controller.IndexGatewayClassByController: string(r.Config.ControllerName)}); err != nil {
		log.FromContext(ctx).Error(err, "Could not find managed GatewayClasses for Node change")
		return nil
	}
	keys := routeRequests{}
	for _, class := range classes.Items {
		for _, gateway := range r.gatewaysForClass(ctx, class.Name) {
			if !gateway.DeletionTimestamp.IsZero() || gateway.Annotations[r.Config.GatewayListenerProtocolAnnotation()] != string(r.protocol()) {
				continue
			}
			topology, err := controller.StoredGatewayTopology(r.Config, &gateway)
			if err == nil && topology.Topology.MemberMode == cloud.MemberModeNodePort {
				r.addGatewayRoutes(ctx, keys, client.ObjectKeyFromObject(&gateway), controller.IndexL4RouteByParentGateway)
			}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueService(ctx context.Context, object client.Object) []reconcile.Request {
	return r.enqueueServiceKey(ctx, client.ObjectKeyFromObject(object))
}

func (r *Reconciler) enqueueEndpointSlice(ctx context.Context, object client.Object) []reconcile.Request {
	name := object.GetLabels()[discoveryv1.LabelServiceName]
	if name == "" {
		return nil
	}
	return r.enqueueServiceKey(ctx, client.ObjectKey{Namespace: object.GetNamespace(), Name: name})
}

func (r *Reconciler) enqueueServiceKey(ctx context.Context, key client.ObjectKey) []reconcile.Request {
	keys := routeRequests{}
	keys.add(r.cachedRoutes(ctx, client.MatchingFields{controller.IndexL4RouteByBackendService: controller.ObjectKeyString(key)}))
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueGrant(ctx context.Context, object client.Object) []reconcile.Request {
	grant, ok := object.(*gatewayv1.ReferenceGrant)
	if !ok {
		return nil
	}
	keys := routeRequests{}
	for _, from := range grant.Spec.From {
		if from.Group != gatewayv1.GroupName || from.Kind != r.kind() || string(from.Namespace) == grant.Namespace {
			continue
		}
		for _, to := range grant.Spec.To {
			if to.Group != "" || to.Kind != "Service" {
				continue
			}
			field, value := controller.IndexL4RouteByBackendNamespace, grant.Namespace
			if to.Name != nil {
				field = controller.IndexL4RouteByBackendService
				value = controller.ObjectKeyString(client.ObjectKey{Namespace: grant.Namespace, Name: string(*to.Name)})
			}
			keys.add(r.cachedRoutes(ctx, client.InNamespace(string(from.Namespace)), client.MatchingFields{field: value}))
		}
	}
	return controller.SortedRequests(keys)
}
