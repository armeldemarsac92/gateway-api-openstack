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
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) backendTLSPolicies(ctx context.Context, field string, key types.NamespacedName) []gatewayv1.BackendTLSPolicy {
	var policies gatewayv1.BackendTLSPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(key.Namespace), client.MatchingFields{field: controller.ObjectKeyString(key)}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list BackendTLSPolicies for dependency", "dependency", key)
	}
	return policies.Items
}

func (r *Reconciler) enqueueRoutesForBackendTLSConfigMap(ctx context.Context, object client.Object) []reconcile.Request {
	policies := r.backendTLSPolicies(ctx, controller.IndexBackendTLSPolicyByCA, client.ObjectKeyFromObject(object))
	return r.enqueueRoutesForBackendTLSPolicies(ctx, policies)
}

func (r *Reconciler) enqueueRoutesForBackendTLSPolicy(ctx context.Context, object client.Object) []reconcile.Request {
	policy := object.(*gatewayv1.BackendTLSPolicy)
	return r.enqueueRoutesForBackendTLSPolicies(ctx, []gatewayv1.BackendTLSPolicy{*policy})
}

func (r *Reconciler) backendTLSRoutePredicate() predicate.Predicate {
	return predicate.Or(controller.GenerationOrDeletionChangedPredicate(), predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		previous := e.ObjectOld.(*gatewayv1.BackendTLSPolicy)
		current := e.ObjectNew.(*gatewayv1.BackendTLSPolicy)
		if (len(previous.Status.Ancestors) >= backendtls.MaxAncestors) != (len(current.Status.Ancestors) >= backendtls.MaxAncestors) {
			return true
		}
		return !maps.Equal(r.ownedBackendTLSAncestors(previous), r.ownedBackendTLSAncestors(current))
	}})
}

func (r *Reconciler) ownedBackendTLSAncestors(policy *gatewayv1.BackendTLSPolicy) map[types.NamespacedName]struct{} {
	keys := make(map[types.NamespacedName]struct{})
	for _, entry := range policy.Status.Ancestors {
		if entry.ControllerName == r.Config.ControllerName {
			if key := backendtls.GatewayAncestorKey(entry.AncestorRef, policy.Namespace); key.Name != "" {
				keys[key] = struct{}{}
			}
		}
	}
	return keys
}

func (r *Reconciler) enqueueRoutesForBackendTLSPolicies(ctx context.Context, policies []gatewayv1.BackendTLSPolicy) []reconcile.Request {
	var consumers []gatewayv1.HTTPRoute
	seen := map[types.NamespacedName]struct{}{}
	grants := map[string]*gatewayv1.ReferenceGrantList{}
	for i := range policies {
		for _, key := range backendtls.ServiceKeys(&policies[i]) {
			if _, observed := seen[key]; observed {
				continue
			}
			seen[key] = struct{}{}
			routes, err := r.serviceConsumers(ctx, key)
			if err != nil {
				log.FromContext(ctx).Error(err, "Could not list routes for BackendTLSPolicy target", "service", key)
				continue
			}
			for _, route := range routes {
				if route.Namespace != key.Namespace {
					rows, observed := grants[key.Namespace]
					if !observed {
						rows = &gatewayv1.ReferenceGrantList{}
						if err := r.List(ctx, rows, client.InNamespace(key.Namespace)); err != nil {
							log.FromContext(ctx).Error(err, "Could not read ReferenceGrants for BackendTLSPolicy target", "service", key)
							rows = nil
						}
						grants[key.Namespace] = rows
					}
					// The cache narrows notifications. The API reader verifies
					// permission before any policy or CA read during reconcile.
					if rows != nil && !model.BackendReferenceAllowedFor(r.kind(), route.Namespace, key, rows.Items) {
						continue
					}
				}
				consumers = append(consumers, route)
			}
		}
	}
	return r.enqueueHTTPRouteConsumers(ctx, consumers)
}

func (r *Reconciler) serviceConsumers(ctx context.Context, key types.NamespacedName) ([]gatewayv1.HTTPRoute, error) {
	return r.listRoutes(ctx, client.MatchingFields{
		controller.IndexHTTPRouteByBackendService: controller.ObjectKeyString(key),
	})
}

func routeBackendTLSServices(route *gatewayv1.HTTPRoute) map[types.NamespacedName]struct{} {
	keys := map[types.NamespacedName]struct{}{}
	for _, rule := range route.Spec.Rules {
		for _, backend := range model.HTTPServiceBackendReferences(rule, false) {
			if key, supported := model.BackendServiceReference(route.Namespace, backend); supported && validBackendPort(backend.Port) {
				keys[key] = struct{}{}
			}
		}
	}
	return keys
}

func (r *Reconciler) setupBackendTLSPolicyStatusController(manager ctrl.Manager) error {
	status := &backendTLSPolicyStatusReconciler{routes: r}
	policySpec := builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())
	routeState := builder.WithPredicates(controller.HTTPRouteReconcilePredicate(r.Config))
	grpc := r.forRouteKind(model.GRPCRouteKind)
	policiesForRoutes := func(httpRoutes, grpcRoutes handler.MapFunc) handler.MapFunc {
		return func(ctx context.Context, object client.Object) []reconcile.Request {
			requests := r.policyRequestsForRoutes(ctx, httpRoutes(ctx, object), nil)
			if grpcRoutes != nil {
				requests = append(requests, grpc.policyRequestsForRoutes(ctx, grpcRoutes(ctx, object), nil)...)
			}
			return requests
		}
	}
	policiesForPeer := func(ctx context.Context, object client.Object) []reconcile.Request {
		var services map[types.NamespacedName]struct{}
		switch route := object.(type) {
		case *gatewayv1.HTTPRoute:
			services = routeBackendTLSServices(route)
		case *gatewayv1.GRPCRoute:
			services = routeBackendTLSServices(model.ProjectGRPCRoute(route))
		}
		return append(r.policyRequestsForRoutes(ctx, r.enqueueHTTPRoutePeers(ctx, object), services),
			grpc.policyRequestsForRoutes(ctx, grpc.enqueueHTTPRoutePeers(ctx, object), nil)...)
	}
	withTLSRoutes := func(mapEvent handler.MapFunc) handler.MapFunc {
		if !r.Config.TCPTLSTermination {
			return mapEvent
		}
		return func(ctx context.Context, object client.Object) []reconcile.Request {
			return append(mapEvent(ctx, object), r.enqueuePoliciesForTLSRouteDependency(ctx, object)...)
		}
	}
	setup := ctrl.NewControllerManagedBy(manager).
		For(&gatewayv1.BackendTLSPolicy{}).
		Watches(&gatewayv1.BackendTLSPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePoliciesForSharedService), policySpec).
		WatchesMetadata(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePoliciesForConfigMap)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePoliciesForService), builder.WithPredicates(controller.ServicePredicate())).
		Watches(&gatewayv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(withTLSRoutes(policiesForRoutes(r.enqueueHTTPRoutesForReferenceGrant, grpc.enqueueHTTPRoutesForReferenceGrant))), policySpec).
		Watches(&gatewayv1.HTTPRoute{}, handler.EnqueueRequestsFromMapFunc(policiesForPeer), routeState).
		Watches(&gatewayv1.GRPCRoute{}, handler.EnqueueRequestsFromMapFunc(policiesForPeer), routeState).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(withTLSRoutes(policiesForRoutes(r.enqueueHTTPRoutesForGateway, grpc.enqueueHTTPRoutesForGateway))), builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(r.Config))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(withTLSRoutes(policiesForRoutes(r.enqueueHTTPRoutesForGatewayClass, grpc.enqueueHTTPRoutesForGatewayClass))), builder.WithPredicates(controller.GatewayClassForGatewayPredicate())).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(withTLSRoutes(policiesForRoutes(r.enqueueHTTPRoutesForNamespace, grpc.enqueueHTTPRoutesForNamespace))), builder.WithPredicates(controller.NamespacePredicate()))
	if r.Config.TCPTLSTermination {
		setup.Watches(&gatewayv1.TLSRoute{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePoliciesForTLSRouteDependency), policySpec)
	}

	served, err := controller.ListenerSetServed(manager.GetRESTMapper())
	if err != nil {
		return err
	}
	if served {
		setup.Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(policiesForRoutes(r.enqueueHTTPRoutesForListenerSet, nil)))
	}
	return setup.Named("backendtlspolicy").
		Complete(status)
}

func (r *Reconciler) enqueuePoliciesForSharedService(ctx context.Context, object client.Object) []reconcile.Request {
	keys := map[types.NamespacedName]struct{}{}
	for _, key := range backendtls.ServiceKeys(object.(*gatewayv1.BackendTLSPolicy)) {
		r.addPolicyRequests(ctx, keys, controller.IndexBackendTLSPolicyByService, key)
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueuePoliciesForConfigMap(ctx context.Context, object client.Object) []reconcile.Request {
	keys := map[types.NamespacedName]struct{}{}
	r.addPolicyRequests(ctx, keys, controller.IndexBackendTLSPolicyByCA, client.ObjectKeyFromObject(object))
	return controller.SortedRequests(keys)
}

func (r *Reconciler) enqueuePoliciesForService(ctx context.Context, object client.Object) []reconcile.Request {
	keys := map[types.NamespacedName]struct{}{}
	r.addPolicyRequests(ctx, keys, controller.IndexBackendTLSPolicyByService, client.ObjectKeyFromObject(object))
	return controller.SortedRequests(keys)
}

func (r *Reconciler) addPolicyRequests(ctx context.Context, requests map[types.NamespacedName]struct{}, field string, key types.NamespacedName) {
	policies := r.backendTLSPolicies(ctx, field, key)
	for i := range policies {
		requests[client.ObjectKeyFromObject(&policies[i])] = struct{}{}
	}
}

func (r *Reconciler) policyRequestsForRoutes(ctx context.Context, requests []reconcile.Request, services map[types.NamespacedName]struct{}) []reconcile.Request {
	if services == nil {
		services = map[types.NamespacedName]struct{}{}
	}
	for _, request := range requests {
		var route gatewayv1.HTTPRoute
		if err := r.readRoute(ctx, r.Client, request.NamespacedName, &route); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				log.FromContext(ctx).Error(err, "Could not get route for BackendTLSPolicy", "kind", r.kind(), "route", request.NamespacedName)
			}
			continue
		}
		for key := range routeBackendTLSServices(&route) {
			services[key] = struct{}{}
		}
	}
	keys := map[types.NamespacedName]struct{}{}
	for service := range services {
		r.addPolicyRequests(ctx, keys, controller.IndexBackendTLSPolicyByService, service)
	}
	return controller.SortedRequests(keys)
}

// Candidate discovery can use cached routes. The status reconciler repeats
// election and authorization against the live API before claiming ancestors.
func (r *Reconciler) enqueuePoliciesForTLSRouteDependency(ctx context.Context, dependency client.Object) []reconcile.Request {
	var routes gatewayv1.TLSRouteList
	if err := r.List(ctx, &routes); err != nil {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	services := map[types.NamespacedName]struct{}{}
	addRoute := func(route *gatewayv1.TLSRoute) {
		if key, ok := gatewaycontroller.TLSRouteBackendService(route); ok {
			services[key] = struct{}{}
		}
	}
	parentKeys := func(route *gatewayv1.TLSRoute) []types.NamespacedName {
		var keys []types.NamespacedName
		for _, ref := range route.Spec.ParentRefs {
			if controller.IsGatewayParentRef(ref) {
				keys = append(keys, types.NamespacedName{Namespace: string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(route.Namespace))), Name: string(ref.Name)})
			}
		}
		return keys
	}
	for i := range routes.Items {
		route := &routes.Items[i]
		switch affected := dependency.(type) {
		case *gatewayv1.TLSRoute:
			if slices.ContainsFunc(parentKeys(route), func(key types.NamespacedName) bool { return slices.Contains(parentKeys(affected), key) }) {
				addRoute(route)
			}
		case *gatewayv1.Gateway:
			if slices.Contains(parentKeys(route), client.ObjectKeyFromObject(affected)) {
				addRoute(route)
			}
		case *gatewayv1.ReferenceGrant:
			if key, ok := gatewaycontroller.TLSRouteBackendService(route); ok && key.Namespace == affected.Namespace && route.Namespace != key.Namespace {
				services[key] = struct{}{}
			}
		case *corev1.Namespace:
			if route.Namespace == affected.Name {
				addRoute(route)
			}
		case *gatewayv1.GatewayClass:
			addRoute(route)
		}
	}
	if route, ok := dependency.(*gatewayv1.TLSRoute); ok {
		addRoute(route)
	}
	for service := range services {
		r.addPolicyRequests(ctx, keys, controller.IndexBackendTLSPolicyByService, service)
	}
	return controller.SortedRequests(keys)
}
