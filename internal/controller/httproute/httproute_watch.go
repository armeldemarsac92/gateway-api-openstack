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
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

func (r *Reconciler) enqueueHTTPRoutePeers(ctx context.Context, object client.Object) []reconcile.Request {
	spec, _ := controller.RouteSpecAndParentStatuses(object)
	if spec == nil {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	var route gatewayv1.HTTPRoute
	route.Namespace = object.GetNamespace()
	route.Spec.CommonRouteSpec = *spec
	r.addHTTPRoutePeers(ctx, []gatewayv1.HTTPRoute{route}, keys)
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueHTTPRoutesForGateway(ctx context.Context, object client.Object) []reconcile.Request {
	gateway, ok := object.(*gatewayv1.Gateway)
	if !ok {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	r.addHTTPRoutesForGateway(ctx, gateway, keys)
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueueHTTPRoutesForGatewayClass(ctx context.Context, object client.Object) []reconcile.Request {
	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways, client.MatchingFields{controller.IndexGatewayByClass: object.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for GatewayClass", "gatewayClass", object.GetName())
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	for i := range gateways.Items {
		r.addHTTPRoutesForGateway(ctx, &gateways.Items[i], keys)
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) addHTTPRoutesForGateway(ctx context.Context, gateway *gatewayv1.Gateway, keys map[types.NamespacedName]struct{}) {
	gatewayKey := controller.ObjectKeyString(client.ObjectKeyFromObject(gateway))
	for _, indexName := range []string{controller.IndexHTTPRouteByParentGateway, controller.IndexHTTPRouteByStatusGateway, controller.IndexHTTPRouteByBoundGateway} {
		routes, err := r.listRoutes(ctx, client.MatchingFields{indexName: gatewayKey})
		if err != nil {
			log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for Gateway", "gateway", client.ObjectKeyFromObject(gateway))
			continue
		}
		for index := range routes {
			keys[client.ObjectKeyFromObject(&routes[index])] = struct{}{}
		}
	}
	if r.Config.DefaultGatewaysEnabled() && gateway.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll {
		r.addDefaultHTTPRoutes(ctx, keys)
	}
	if r.kind() != model.HTTPRouteKind || model.GatewayDisallowsListenerSets(gateway) && gateway.Status.AttachedListenerSets == nil {
		return
	}
	var sets gatewayv1.ListenerSetList
	if err := r.List(ctx, &sets, client.MatchingFields{controller.IndexListenerSetByGateway: client.ObjectKeyFromObject(gateway).String()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list ListenerSets for Gateway", "gateway", gatewayKey)
		return
	}
	for i := range sets.Items {
		for _, request := range r.enqueueHTTPRoutesForListenerSet(ctx, &sets.Items[i]) {
			keys[request.NamespacedName] = struct{}{}
		}
	}

}

func (r *Reconciler) enqueueHTTPRoutesForServiceKey(ctx context.Context, serviceKey types.NamespacedName) []reconcile.Request {
	direct, err := r.listRoutes(ctx, client.MatchingFields{
		controller.IndexHTTPRouteByBackendService: controller.ObjectKeyString(serviceKey),
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for Service", "service", serviceKey)
		return nil
	}
	return r.enqueueHTTPRouteConsumers(ctx, direct)
}

// A backend change can alter which bound fragments the shared listener can program.
func (r *Reconciler) enqueueHTTPRouteConsumers(ctx context.Context, routes []gatewayv1.HTTPRoute) []reconcile.Request {
	keys := map[types.NamespacedName]struct{}{}
	for index := range routes {
		keys[client.ObjectKeyFromObject(&routes[index])] = struct{}{}
	}
	r.addHTTPRoutePeers(ctx, routes, keys)
	return controller.SortedRequests(keys)
}

func (r *Reconciler) addHTTPRoutePeers(ctx context.Context, routes []gatewayv1.HTTPRoute, keys map[types.NamespacedName]struct{}) {
	gatewayKeys := map[string]struct{}{}
	usesDefaults := false
	for index := range routes {
		for _, gatewayKey := range controller.ParentGatewayKeys(&routes[index]) {
			gatewayKeys[gatewayKey] = struct{}{}
		}
		usesDefaults = usesDefaults || routes[index].Spec.UseDefaultGateways == gatewayv1.GatewayDefaultScopeAll
	}
	defaultEnabled := r.Config.DefaultGatewaysEnabled()
	needDefaultPeers := false
	if defaultEnabled && usesDefaults {
		var gateways gatewayv1.GatewayList
		if err := r.List(ctx, &gateways, client.MatchingFields{controller.IndexGatewayByDefaultScope: string(gatewayv1.GatewayDefaultScopeAll)}); err != nil {
			log.FromContext(ctx).Error(err, "Could not list default Gateways for route peers")
		}
		for index := range gateways.Items {
			gatewayKeys[controller.ObjectKeyString(client.ObjectKeyFromObject(&gateways.Items[index]))] = struct{}{}
		}
		needDefaultPeers = len(gateways.Items) != 0
	}
	for gatewayKey := range gatewayKeys {
		peers, err := r.listRoutes(ctx, client.MatchingFields{controller.IndexHTTPRouteByParentGateway: gatewayKey})
		if err != nil {
			log.FromContext(ctx).Error(err, "Could not list HTTPRoutes sharing a Gateway", "gateway", gatewayKey)
		}
		for index := range peers {
			keys[client.ObjectKeyFromObject(&peers[index])] = struct{}{}
		}
		if defaultEnabled && !needDefaultPeers {
			// An explicit Route can change the election on a default Gateway.
			namespace, name, _ := strings.Cut(gatewayKey, "/")
			var gateway gatewayv1.Gateway
			if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &gateway); err == nil {
				needDefaultPeers = gateway.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll
			} else if !apierrors.IsNotFound(err) {
				log.FromContext(ctx).Error(err, "Could not get parent Gateway for route peers", "gateway", gatewayKey)
			}
		}
	}
	if needDefaultPeers {
		r.addDefaultHTTPRoutes(ctx, keys)
	}
}

func (r *Reconciler) addDefaultHTTPRoutes(ctx context.Context, keys map[types.NamespacedName]struct{}) {
	routes, err := r.listRoutes(ctx, client.MatchingFields{controller.IndexRouteByDefaultGateways: string(gatewayv1.GatewayDefaultScopeAll)})
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list routes for default Gateways", "kind", r.kind())
		return
	}
	for index := range routes {
		keys[client.ObjectKeyFromObject(&routes[index])] = struct{}{}
	}
}

func (r *Reconciler) enqueueHTTPRoutesForService(ctx context.Context, object client.Object) []reconcile.Request {
	return r.enqueueHTTPRoutesForServiceKey(ctx, client.ObjectKeyFromObject(object))
}

func (r *Reconciler) enqueueHTTPRoutesForEndpointSlice(ctx context.Context, object client.Object) []reconcile.Request {
	serviceName := object.GetLabels()[discoveryv1.LabelServiceName]
	if serviceName == "" {
		return nil
	}
	return r.enqueueHTTPRoutesForServiceKey(ctx, types.NamespacedName{Namespace: object.GetNamespace(), Name: serviceName})
}

func (r *Reconciler) enqueueHTTPRoutesForNode(ctx context.Context, object client.Object) []reconcile.Request {
	node, ok := object.(*corev1.Node)
	if !ok {
		return nil
	}
	routes, err := r.listRoutes(ctx, client.MatchingFields{controller.IndexHTTPRouteByNodeBackend: controller.NodeBackendIndexValue})
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for Node", "node", node.Name)
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	inputs := nodeEventInputs{
		parents:   make(map[string]nodeEligibilityObservation),
		services:  make(map[types.NamespacedName]*nodeServiceObservation),
		endpoints: make(map[types.NamespacedName]nodeEligibilityObservation),
	}
	for index := range routes {
		route := &routes[index]
		backends, err := r.validateRouteStructure(route)
		if err != nil {
			continue
		}
		managed, err := inputs.hasNodePortParent(ctx, r, route)
		if err != nil {
			log.FromContext(ctx).Error(err, "Could not resolve HTTPRoute parents for Node", "httpRoute", client.ObjectKeyFromObject(route), "node", node.Name)
			continue
		}
		if !managed {
			continue
		}
		if r.Config.RouteExternalAuthEnabled() || r.Config.RouteExternalAuthGRPCEnabled() {
			for _, rule := range route.Spec.Rules {
				backends = append(backends, model.HTTPRouteExternalAuthReferences(rule)...)
			}
		}
		for _, backend := range backends {
			serviceKey, supported := model.BackendServiceReference(route.Namespace, backend)
			if !supported {
				continue
			}
			if serviceKey.Namespace != route.Namespace {
				// Let reconciliation check the live grant before reading this Service.
				keys[client.ObjectKeyFromObject(route)] = struct{}{}
				break
			}
			service, err := inputs.service(ctx, r.Client, serviceKey)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					log.FromContext(ctx).Error(err, "Could not get backend Service for Node", "service", serviceKey, "node", node.Name)
				}
				continue
			}
			if service.Spec.Type != corev1.ServiceTypeNodePort {
				continue
			}
			if service.Spec.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyLocal {
				local, err := inputs.hasReadyEndpoint(ctx, r, serviceKey, node.Name)
				if err != nil {
					log.FromContext(ctx).Error(err, "Could not list local endpoints for Node", "service", serviceKey, "node", node.Name)
					continue
				}
				if !local {
					continue
				}
			}
			keys[client.ObjectKeyFromObject(route)] = struct{}{}
			break
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) nodeHasReadyEndpoint(ctx context.Context, serviceKey types.NamespacedName, nodeName string) (bool, error) {
	var slices discoveryv1.EndpointSliceList
	if err := r.List(ctx, &slices, client.MatchingFields{
		controller.IndexEndpointSliceByService: controller.ObjectKeyString(serviceKey),
	}); err != nil {
		return false, err
	}
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.NodeName != nil && *endpoint.NodeName == nodeName && routebackend.EndpointReady(endpoint.Conditions) {
				return true, nil
			}
		}
	}
	return false, nil
}

// SetupWithManager registers HTTPRoute reconciliation and dependency watches.
func (r *Reconciler) SetupWithManager(manager ctrl.Manager, imports *controller.ServiceImportWatch) error {
	if r.Coordinator == nil {
		return graph.ErrCoordinatorRequired
	}
	if r.APIReader == nil {
		return controller.ErrAPIReaderRequired
	}
	if r.Writer == nil {
		return errors.New("gateway graph writer is required")
	}
	if err := r.discoverBackendRetryPolicy(manager.GetRESTMapper()); err != nil {
		return err
	}
	if err := r.setupRouteController(manager, &gatewayv1.HTTPRoute{}, "httproute", imports); err != nil {
		return err
	}
	if err := r.forRouteKind(model.GRPCRouteKind).setupRouteController(manager, &gatewayv1.GRPCRoute{}, "grpcroute", imports); err != nil {
		return err
	}
	if r.Config.XBackendEnabled() {
		if err := r.setupXBackendStatusController(manager); err != nil {
			return err
		}
	}
	if r.retryBudgetPolicyServed {
		if err := r.setupBackendRetryBudgetStatus(manager); err != nil {
			return err
		}
	}
	if r.Config.BackendTLSPolicy {
		return r.setupBackendTLSPolicyStatusController(manager)
	}
	return nil
}

func (r *Reconciler) setupRouteController(manager ctrl.Manager, object client.Object, name string, imports *controller.ServiceImportWatch) error {
	setup := ctrl.NewControllerManagedBy(manager).
		For(object, builder.WithPredicates(controller.HTTPRouteReconcilePredicate(r.Config)))
	if imports != nil {
		setup.WatchesRawSource(imports.Subscribe(r.enqueueRoutesForServiceImport))
		if r.kind() == model.HTTPRouteKind {
			imports.WatchCRD(setup)
		}
	}
	// Watch both kinds while observing a shared routing graph, including peers
	// whose earlier policy might have blocked this route from attaching.
	for _, peer := range []client.Object{&gatewayv1.HTTPRoute{}, &gatewayv1.GRPCRoute{}} {
		setup.Watches(peer, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutePeers), builder.WithPredicates(controller.HTTPRoutePeerPredicate()))
	}
	if r.kind() == model.HTTPRouteKind {
		served, err := controller.ListenerSetServed(manager.GetRESTMapper())
		if err != nil {
			return err
		}
		if served {
			setup.Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForListenerSet))
		}
	}
	if r.Config.XBackendEnabled() && r.kind() == model.HTTPRouteKind {
		setup.Watches(&gatewayx.XBackend{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForXBackend), builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
			Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForXBackendPod), builder.WithPredicates(controller.XBackendPodPredicate()))
	}
	if r.retryBudgetPolicyServed && (r.Config.BackendRetryBudgetEnabled() || r.Config.XBackendSessionPersistenceEnabled()) {
		setup.Watches(&gatewayx.XBackendTrafficPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRoutesForRetryBudget), builder.WithPredicates(r.retryBudgetRoutePredicate()))
	}
	if r.Config.BackendTLSPolicy || (r.Config.RouteExternalAuthEnabled() || r.Config.RouteExternalAuthGRPCEnabled()) && r.kind() == model.HTTPRouteKind {
		setup.Watches(&gatewayv1.BackendTLSPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRoutesForBackendTLSPolicy), builder.WithPredicates(r.backendTLSRoutePredicate()))
	}
	if r.Config.BackendTLSPolicy {
		setup.WatchesMetadata(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRoutesForBackendTLSCA))
		if watch := r.Config.ClusterTrustBundles.Watch(r.enqueueRoutesForBackendTLSCA); watch != nil {
			setup.WatchesRawSource(watch)
		}
	}
	return setup.
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForGateway), builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(r.Config))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForGatewayClass), builder.WithPredicates(controller.GatewayClassForGatewayPredicate())).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForService), builder.WithPredicates(controller.ServicePredicate())).
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForEndpointSlice), builder.WithPredicates(controller.EndpointSlicePredicate())).
		Watches(&gatewayv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForReferenceGrant), builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForNamespace), builder.WithPredicates(controller.NamespacePredicate())).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.enqueueHTTPRoutesForNode), builder.WithPredicates(controller.NodePredicate(""))).
		Named(name).
		Complete(r)
}

func (r *Reconciler) enqueueHTTPRoutesForListenerSet(ctx context.Context, object client.Object) []reconcile.Request {
	var routes gatewayv1.HTTPRouteList
	key := client.ObjectKeyFromObject(object)
	if err := r.List(ctx, &routes, client.MatchingFields{controller.IndexHTTPRouteByListenerSet: key.String()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for ListenerSet", "listenerSet", key)
		return nil
	}
	keys := make(map[types.NamespacedName]struct{}, len(routes.Items))
	for i := range routes.Items {
		keys[client.ObjectKeyFromObject(&routes.Items[i])] = struct{}{}
	}
	return controller.SortedRequests(keys)
}
