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
	"slices"

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

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) gatewayRequestsForRoute(ctx context.Context, route client.Object) []reconcile.Request {
	parents, statuses, ok := controller.RouteParents(route)
	if !ok {
		return nil
	}
	keys := make(map[types.NamespacedName]struct{}, len(parents)+len(statuses)+1)
	addParent := func(parentRef gatewayv1.ParentReference) {
		if !controller.IsGatewayParentRef(parentRef) || parentRef.Name == "" {
			if key := model.ListenerSetRefKey(parentRef, route.GetNamespace()); key.Name != "" {
				var set gatewayv1.ListenerSet
				if err := r.Get(ctx, key, &set); err == nil {
					if gateway := model.ListenerSetParentKey(&set); gateway.Name != "" {
						keys[gateway] = struct{}{}
					}
				}
			}
			return
		}
		gatewayNamespace := route.GetNamespace()
		if parentRef.Namespace != nil {
			gatewayNamespace = string(*parentRef.Namespace)
		}
		keys[types.NamespacedName{Namespace: gatewayNamespace, Name: string(parentRef.Name)}] = struct{}{}
	}
	for _, parentRef := range parents {
		addParent(parentRef)
	}
	for _, parentStatus := range statuses {
		if parentStatus.ControllerName != r.Config.ControllerName {
			continue
		}
		addParent(parentStatus.ParentRef)
	}
	annotations := route.GetAnnotations()
	boundNamespace := annotations[r.Config.RouteGatewayNamespaceAnnotation()]
	boundName := annotations[r.Config.RouteGatewayNameAnnotation()]
	boundUID := annotations[r.Config.RouteGatewayUIDAnnotation()]
	if boundNamespace != "" && boundName != "" && boundUID != "" {
		keys[types.NamespacedName{Namespace: boundNamespace, Name: boundName}] = struct{}{}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) gatewayRequestsForGatewayClass(ctx context.Context, object client.Object) []reconcile.Request {
	gatewayClass, ok := object.(*gatewayv1.GatewayClass)
	if !ok {
		return nil
	}

	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways, client.MatchingFields{
		controller.IndexGatewayByClass: gatewayClass.Name,
	}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for GatewayClass", "gatewayClass", gatewayClass.Name)
		return nil
	}
	keys := make(map[types.NamespacedName]struct{}, len(gateways.Items))
	for _, gateway := range gateways.Items {
		keys[client.ObjectKeyFromObject(&gateway)] = struct{}{}
	}
	return controller.SortedRequests(keys)
}

// SetupWithManager registers Gateway reconciliation and dependency watches.
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
	gatewayController := ctrl.NewControllerManagedBy(manager).
		For(&gatewayv1.Gateway{}, builder.WithPredicates(controller.GatewayReconcilePredicate(r.Config)))
	for _, route := range []client.Object{&gatewayv1.HTTPRoute{}, &gatewayv1.GRPCRoute{}, &gatewayv1.TCPRoute{}, &gatewayv1.UDPRoute{}} {
		gatewayController.Watches(route, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForRoute),
			builder.WithPredicates(controller.RouteForGatewayPredicate(r.Config)))
	}
	if r.Config.TLSRoutesEnabled() {
		gatewayController.Watches(&gatewayv1.TLSRoute{}, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForRoute),
			builder.WithPredicates(controller.RouteForGatewayPredicate(r.Config)))
		for _, dependency := range []client.Object{&corev1.Service{}, &discoveryv1.EndpointSlice{}, &corev1.Namespace{}, &gatewayv1.ReferenceGrant{}} {
			gatewayController.Watches(dependency, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForSNIDependency))
		}
	}
	served, err := controller.ListenerSetServed(manager.GetRESTMapper())
	if err != nil {
		return err
	}
	if served {
		gatewayController.Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(gatewayRequestsForListenerSet),
			builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
			Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForListenerSetNamespace),
				builder.WithPredicates(controller.NamespacePredicate()))
	}
	if r.Config.OperatorHTTPS {
		gatewayController.Watches(&gatewayv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForCertificateGrant))
	}
	return gatewayController.
		Watches(
			&gatewayv1.GatewayClass{},
			handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForGatewayClass),
			builder.WithPredicates(controller.GatewayClassForGatewayPredicate()),
		).
		Watches(&configv1alpha1.OpenStackGatewayClassConfig{}, handler.EnqueueRequestsFromMapFunc(r.gatewayRequestsForParameters), builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
		Named("gateway").
		Complete(r)
}

// Parameter updates can leave GatewayClass conditions unchanged. Wake unbound
// Gateways directly so they cannot remain rejected after the inputs recover.
func (r *Reconciler) gatewayRequestsForParameters(ctx context.Context, object client.Object) []reconcile.Request {
	if object == nil {
		return nil
	}
	var classes gatewayv1.GatewayClassList
	if err := r.List(ctx, &classes, client.MatchingFields{controller.IndexGatewayClassByParameters: object.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list GatewayClasses for parameters")
		return nil
	}
	keys := make(map[types.NamespacedName]struct{})
	for _, class := range classes.Items {
		if class.Spec.ControllerName != r.Config.ControllerName {
			continue
		}
		var gateways gatewayv1.GatewayList
		if err := r.List(ctx, &gateways, client.MatchingFields{controller.IndexGatewayByClass: class.Name}); err != nil {
			log.FromContext(ctx).Error(err, "Could not list Gateways for class parameters", "gatewayClass", class.Name)
			continue
		}
		for _, gateway := range gateways.Items {
			if !controller.HasGatewayTopologyBinding(r.Config, &gateway) && gateway.DeletionTimestamp.IsZero() {
				keys[client.ObjectKeyFromObject(&gateway)] = struct{}{}
			}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) gatewayRequestsForCertificateGrant(ctx context.Context, object client.Object) []reconcile.Request {
	grant, ok := object.(*gatewayv1.ReferenceGrant)
	if !ok || !slices.ContainsFunc(grant.Spec.To, func(target gatewayv1.ReferenceGrantTo) bool {
		return target.Group == "" && target.Kind == "Secret"
	}) {
		return nil
	}
	keys := make(map[types.NamespacedName]struct{})
	namespaces := make(map[gatewayv1.Namespace]bool)
	for _, source := range grant.Spec.From {
		if source.Group != gatewayv1.GroupName || source.Kind != "Gateway" || namespaces[source.Namespace] {
			continue
		}
		namespaces[source.Namespace] = true
		var gateways gatewayv1.GatewayList
		if err := r.List(ctx, &gateways, client.InNamespace(string(source.Namespace))); err != nil {
			log.FromContext(ctx).Error(err, "Could not list Gateways for certificate ReferenceGrant")
			continue
		}
		for i := range gateways.Items {
			gateway := &gateways.Items[i]
			if slices.ContainsFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
				return (listener.Protocol == gatewayv1.HTTPSProtocolType || terminatesTLS(&listener)) && listener.TLS != nil &&
					slices.ContainsFunc(listener.TLS.CertificateRefs, func(ref gatewayv1.SecretObjectReference) bool {
						return ref.Namespace != nil && string(*ref.Namespace) == grant.Namespace
					})
			}) {
				keys[client.ObjectKeyFromObject(gateway)] = struct{}{}
			}
		}
	}
	return controller.SortedRequests(keys)
}

func gatewayRequestsForListenerSet(_ context.Context, object client.Object) []reconcile.Request {
	key := model.ListenerSetParentKey(object.(*gatewayv1.ListenerSet))
	if key.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}

func (r *Reconciler) gatewayRequestsForListenerSetNamespace(ctx context.Context, namespace client.Object) []reconcile.Request {
	var sets gatewayv1.ListenerSetList
	if err := r.List(ctx, &sets, client.InNamespace(namespace.GetName())); err != nil {
		log.FromContext(ctx).Error(err, "Could not list ListenerSets in Namespace", "namespace", namespace.GetName())
		return nil
	}
	keys := make(map[types.NamespacedName]struct{}, len(sets.Items))
	for i := range sets.Items {
		if key := model.ListenerSetParentKey(&sets.Items[i]); key.Name != "" {
			keys[key] = struct{}{}
		}
	}
	// ListenerSet-owned Route selectors can observe a namespace different
	// from both the ListenerSet and its Gateway.
	var routes gatewayv1.HTTPRouteList
	if err := r.List(ctx, &routes, client.InNamespace(namespace.GetName())); err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes in Namespace", "namespace", namespace.GetName())
		return controller.SortedRequests(keys)
	}
	observed := make(map[types.NamespacedName]*gatewayv1.ListenerSet)
	for _, route := range routes.Items {
		for _, ref := range route.Spec.ParentRefs {
			key := model.ListenerSetRefKey(ref, route.Namespace)
			if key.Name == "" {
				continue
			}
			set, found := observed[key]
			if !found {
				var candidate gatewayv1.ListenerSet
				if err := r.Get(ctx, key, &candidate); err == nil {
					set = &candidate
				} else if !apierrors.IsNotFound(err) {
					log.FromContext(ctx).Error(err, "Could not get ListenerSet for Route namespace", "listenerSet", key)
				}
				observed[key] = set
			}
			if set != nil && model.ListenerSetRefObservesRouteNamespace(set, ref) {
				if gateway := model.ListenerSetParentKey(set); gateway.Name != "" {
					keys[gateway] = struct{}{}
				}
			}
		}
	}
	return controller.SortedRequests(keys)
}
