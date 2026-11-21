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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) enqueueHTTPRoutesForReferenceGrant(ctx context.Context, object client.Object) []reconcile.Request {
	grant, ok := object.(*gatewayv1.ReferenceGrant)
	if !ok {
		return nil
	}
	// Index the declared references, including denied and unresolved references,
	// so adding the first grant or revoking an existing grant wakes its consumers.
	var consumers []gatewayv1.HTTPRoute
	observed := make(map[types.NamespacedName]struct{})
	for _, from := range grant.Spec.From {
		if from.Group != gatewayv1.GroupName || from.Kind != r.kind() || string(from.Namespace) == grant.Namespace {
			continue
		}
		for _, to := range grant.Spec.To {
			if to.Group != "" || to.Kind != "Service" {
				continue
			}
			query := types.NamespacedName{Namespace: string(from.Namespace)}
			field, value := controller.IndexHTTPRouteByBackendNamespace, grant.Namespace
			if to.Name != nil {
				query.Name = string(*to.Name)
				field = controller.IndexHTTPRouteByBackendService
				value = controller.ObjectKeyString(types.NamespacedName{Namespace: grant.Namespace, Name: query.Name})
			}
			if _, found := observed[query]; found {
				continue
			}
			observed[query] = struct{}{}
			routes, err := r.listRoutes(ctx, r.Client, client.InNamespace(query.Namespace), client.MatchingFields{field: value})
			if err != nil {
				log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for ReferenceGrant", "referenceGrant", client.ObjectKeyFromObject(grant))
				continue
			}
			consumers = append(consumers, routes...)
		}
	}
	return r.enqueueHTTPRouteConsumers(ctx, consumers)
}

func (r *Reconciler) enqueueHTTPRoutesForNamespace(ctx context.Context, object client.Object) []reconcile.Request {
	namespace, ok := object.(*corev1.Namespace)
	if !ok {
		return nil
	}
	// The cache's namespace index bounds this read to possible consumers of the
	// changed namespace. Parent lookups then avoid scanning all Gateways.
	routes, err := r.listRoutes(ctx, r.Client, client.InNamespace(namespace.Name))
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list HTTPRoutes for Namespace", "namespace", namespace.Name)
		return nil
	}
	gateways := make(map[types.NamespacedName]*gatewayv1.Gateway)
	var consumers []gatewayv1.HTTPRoute
	for _, route := range routes {
		for _, parent := range route.Spec.ParentRefs {
			if !controller.IsGatewayParentRef(parent) {
				continue
			}
			key := types.NamespacedName{Namespace: route.Namespace, Name: string(parent.Name)}
			if parent.Namespace != nil {
				key.Namespace = string(*parent.Namespace)
			}
			gateway, observed := gateways[key]
			if !observed {
				gateway = &gatewayv1.Gateway{}
				if err := r.Get(ctx, key, gateway); err != nil {
					if !apierrors.IsNotFound(err) {
						log.FromContext(ctx).Error(err, "Could not get Gateway for Namespace", "gateway", key, "namespace", namespace.Name)
					}
					gateway = nil
				}
				gateways[key] = gateway
			}
			if gateway != nil && parentObservesRouteNamespace(gateway, parent) {
				consumers = append(consumers, route)
				break
			}
		}
	}
	return r.enqueueHTTPRouteConsumers(ctx, consumers)
}

func parentObservesRouteNamespace(gateway *gatewayv1.Gateway, parent gatewayv1.ParentReference) bool {
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if parent.SectionName != nil && *parent.SectionName != listener.Name || parent.Port != nil && *parent.Port != listener.Port {
			continue
		}
		if model.ObservesRouteNamespace(listener) {
			return true
		}
	}
	return false
}
