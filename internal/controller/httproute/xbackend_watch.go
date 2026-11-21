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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) listXBackendConsumers(ctx context.Context, key types.NamespacedName) ([]gatewayv1.HTTPRoute, error) {
	return r.listRoutes(ctx, client.InNamespace(key.Namespace), client.MatchingFields{
		controller.IndexHTTPRouteByXBackend: controller.ObjectKeyString(key),
	})
}

func (r *Reconciler) enqueueHTTPRoutesForXBackend(ctx context.Context, object client.Object) []reconcile.Request {
	key := client.ObjectKeyFromObject(object)
	consumers, err := r.listXBackendConsumers(ctx, key)
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for XBackend", "xBackend", key)
		return nil
	}
	return r.enqueueHTTPRouteConsumers(ctx, consumers)
}

func (r *Reconciler) enqueueHTTPRoutesForXBackendPod(ctx context.Context, object client.Object) []reconcile.Request {
	keys := map[types.NamespacedName]struct{}{}
	var consumers []gatewayv1.HTTPRoute
	for _, request := range r.enqueueXBackendsForPod(ctx, object) {
		routes, err := r.listXBackendConsumers(ctx, request.NamespacedName)
		if err != nil {
			log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for XBackend", "xBackend", request.NamespacedName)
			continue
		}
		for i := range routes {
			key := client.ObjectKeyFromObject(&routes[i])
			if _, found := keys[key]; !found {
				keys[key] = struct{}{}
				consumers = append(consumers, routes[i])
			}
		}
	}
	return r.enqueueHTTPRouteConsumers(ctx, consumers)
}

func (r *Reconciler) enqueueXBackendsForPod(ctx context.Context, object client.Object) []reconcile.Request {
	pod, ok := object.(*corev1.Pod)
	if !ok || !model.XBackendPodReady(pod) {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	for key, value := range pod.Labels {
		var backends gatewayx.XBackendList
		if err := r.List(ctx, &backends, client.InNamespace(pod.Namespace), client.MatchingFields{
			controller.IndexXBackendByPodLabel: controller.PodLabelIndexKey(key, value),
		}); err != nil {
			log.FromContext(ctx).Error(err, "Could not list XBackends matching Pod labels", "pod", client.ObjectKeyFromObject(pod))
			continue
		}
		for i := range backends.Items {
			backend := &backends.Items[i]
			if backend.DeletionTimestamp.IsZero() && model.XBackendSelectsLabels(backend.Spec, pod.Labels) {
				keys[client.ObjectKeyFromObject(backend)] = struct{}{}
			}
		}
	}
	return controller.SortedRequests(keys)
}

func xBackendPodPredicate() predicate.Predicate {
	ready := func(object client.Object) bool {
		pod, ok := object.(*corev1.Pod)
		return ok && model.XBackendPodReady(pod)
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return ready(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool { return ready(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK {
				return false
			}
			oldReady, newReady := model.XBackendPodReady(oldPod), model.XBackendPodReady(newPod)
			return oldReady != newReady || newReady &&
				(!maps.Equal(oldPod.Labels, newPod.Labels) || oldPod.Status.PodIP != newPod.Status.PodIP ||
					!slices.Equal(oldPod.Status.PodIPs, newPod.Status.PodIPs))
		},
	}
}

func (r *Reconciler) setupXBackendStatusController(manager ctrl.Manager) error {
	status := &xBackendStatusReconciler{routes: r}
	// Mapped update events run for both objects, including the old set of
	// references and matching labels on a detach or label deletion.
	mapRoute := func(ctx context.Context, object client.Object) []reconcile.Request {
		var direct []types.NamespacedName
		if route, ok := object.(*gatewayv1.HTTPRoute); ok {
			direct = model.LocalXBackendReferences(route)
		}
		return r.xBackendsForRouteRequests(ctx, r.enqueueHTTPRoutePeers(ctx, object), direct)
	}
	mapRoutes := func(mapper handler.MapFunc) handler.MapFunc {
		return func(ctx context.Context, object client.Object) []reconcile.Request {
			return r.xBackendsForRouteRequests(ctx, mapper(ctx, object), nil)
		}
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&gatewayx.XBackend{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.enqueueXBackendsForPod), builder.WithPredicates(xBackendPodPredicate())).
		Watches(&gatewayv1.HTTPRoute{}, handler.EnqueueRequestsFromMapFunc(mapRoute), builder.WithPredicates(controller.HTTPRouteReconcilePredicate(r.Config))).
		Watches(&gatewayv1.GRPCRoute{}, handler.EnqueueRequestsFromMapFunc(mapRoute), builder.WithPredicates(controller.HTTPRouteReconcilePredicate(r.Config))).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueHTTPRoutesForGateway)), builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(r.Config))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueHTTPRoutesForGatewayClass)), builder.WithPredicates(controller.GatewayClassForGatewayPredicate())).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueHTTPRoutesForNamespace)), builder.WithPredicates(controller.NamespacePredicate())).
		Named("xbackend").
		Complete(status)
}

func (r *Reconciler) xBackendsForRouteRequests(ctx context.Context, requests []reconcile.Request, direct []types.NamespacedName) []reconcile.Request {
	keys := make(map[types.NamespacedName]struct{}, len(direct))
	for _, key := range direct {
		keys[key] = struct{}{}
	}
	for _, request := range requests {
		var route gatewayv1.HTTPRoute
		if err := r.Get(ctx, request.NamespacedName, &route); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				log.FromContext(ctx).Error(err, "Could not get HTTPRoute for XBackend", "route", request.NamespacedName)
			}
			continue
		}
		for _, key := range model.LocalXBackendReferences(&route) {
			keys[key] = struct{}{}
		}
	}
	return controller.SortedRequests(keys)
}
