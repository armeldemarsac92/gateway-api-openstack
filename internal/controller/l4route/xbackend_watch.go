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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) enqueueTCPRoutesForXBackend(ctx context.Context, object client.Object) []reconcile.Request {
	return controller.SortedRequests(r.xBackendConsumers(ctx, client.ObjectKeyFromObject(object)))
}

func (r *Reconciler) enqueueTCPRoutesForXBackendPod(ctx context.Context, object client.Object) []reconcile.Request {
	keys := routeRequests{}
	for _, request := range controller.XBackendsForPod(ctx, r.Client, object) {
		for key := range r.xBackendConsumers(ctx, request.NamespacedName) {
			keys[key] = struct{}{}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) xBackendConsumers(ctx context.Context, key client.ObjectKey) routeRequests {
	keys := routeRequests{}
	keys.add(r.cachedRoutes(ctx, client.InNamespace(key.Namespace), client.MatchingFields{
		controller.IndexTCPRouteByXBackend: controller.ObjectKeyString(key),
	}))
	return keys
}

// WatchXBackendAncestors queues backend status when TCPRoute attachment can change.
func (r *Reconciler) WatchXBackendAncestors(setup *builder.Builder) {
	mapRoutes := func(mapper handler.MapFunc) handler.MapFunc {
		return func(ctx context.Context, object client.Object) []reconcile.Request {
			keys := routeRequests{}
			for _, request := range mapper(ctx, object) {
				var route gatewayv1.TCPRoute
				if err := r.Get(ctx, request.NamespacedName, &route); err != nil {
					if err := client.IgnoreNotFound(err); err != nil {
						log.FromContext(ctx).Error(err, "Could not get TCPRoute for XBackend status", "route", request.NamespacedName)
					}
					continue
				}
				for _, key := range model.LocalTCPXBackendReferences(&route) {
					keys[key] = struct{}{}
				}
			}
			return controller.SortedRequests(keys)
		}
	}
	setup.
		Watches(&gatewayv1.TCPRoute{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			keys := routeRequests{}
			for _, key := range model.LocalTCPXBackendReferences(object.(*gatewayv1.TCPRoute)) {
				keys[key] = struct{}{}
			}
			return controller.SortedRequests(keys)
		}), builder.WithPredicates(r.routeChanges())).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueGateway)), builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(r.Config))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueClass)), builder.WithPredicates(controller.GatewayClassForGatewayPredicate())).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(mapRoutes(r.enqueueNamespace)), builder.WithPredicates(controller.NamespacePredicate()))
}

// ObserveXBackendAncestors evaluates the XBackend configuration for each live,
// managed Gateway to which a TCPRoute consumer is attached.
func (r *Reconciler) ObserveXBackendAncestors(ctx context.Context, backend *gatewayx.XBackend) (map[client.ObjectKey]metav1.Condition, error) {
	key := client.ObjectKeyFromObject(backend)
	var candidates gatewayv1.TCPRouteList
	if err := r.List(ctx, &candidates, client.InNamespace(key.Namespace), client.MatchingFields{
		controller.IndexTCPRouteByXBackend: controller.ObjectKeyString(key),
	}); err != nil {
		return nil, err
	}
	ancestors := map[client.ObjectKey]metav1.Condition{}
	for i := range candidates.Items {
		var tcp gatewayv1.TCPRoute
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(&candidates.Items[i]), &tcp); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		if !tcp.DeletionTimestamp.IsZero() || !slices.Contains(model.LocalTCPXBackendReferences(&tcp), key) {
			continue
		}
		route := viewRoute(&tcp)
		gateways, err := r.routeGatewayKeys(ctx, route, nil)
		if err != nil {
			return nil, err
		}
		for _, gateway := range gateways {
			if _, found := ancestors[gateway]; found {
				continue
			}
			parents, _, err := r.managedParents(ctx, route, gateway)
			if err != nil {
				return nil, err
			}
			if !slices.ContainsFunc(parents, func(parent routeParent) bool {
				return len(parent.listeners) != 0 && controller.GatewayClassSupportsInstalledVersion(parent.class)
			}) {
				continue
			}
			var mode cloud.MemberMode
			if controller.HasGatewayTopologyBinding(r.Config, parents[0].gateway) {
				topology, err := controller.StoredGatewayTopology(r.Config, parents[0].gateway)
				if errors.Is(err, cloud.ErrOwnershipConflict) {
					ancestors[gateway] = controller.Condition(string(gatewayx.BackendConditionAccepted), metav1.ConditionUnknown,
						controller.ProviderReasonOwnershipConflict, "Gateway member topology binding is invalid", backend.Generation)
					continue
				}
				if err != nil {
					return nil, err
				}
				mode = topology.Topology.MemberMode
			}
			status, reason, message := metav1.ConditionTrue, string(gatewayx.BackendReasonAccepted), "XBackend is accepted by the Gateway"
			if diagnostic := model.ValidateTCPXBackend(backend.Spec, mode); diagnostic != nil {
				status, reason, message = metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message
			} else if mode == "" {
				status, reason, message = metav1.ConditionUnknown, "Pending", "Gateway member mode is pending"
			}
			ancestors[gateway] = controller.Condition(string(gatewayx.BackendConditionAccepted), status, reason, message, backend.Generation)
		}
	}
	return ancestors, nil
}
