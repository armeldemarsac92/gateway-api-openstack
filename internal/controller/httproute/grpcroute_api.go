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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) forRouteKind(kind gatewayv1.Kind) *Reconciler {
	if kind == r.kind() {
		return r
	}
	view := *r
	view.routeKind = kind
	return &view
}

func (r *Reconciler) kind() gatewayv1.Kind {
	if r.routeKind == model.GRPCRouteKind {
		return model.GRPCRouteKind
	}
	return model.HTTPRouteKind
}

func (r *Reconciler) readRoute(ctx context.Context, reader client.Reader, key client.ObjectKey, route *gatewayv1.HTTPRoute) error {
	if r.kind() != model.GRPCRouteKind {
		return reader.Get(ctx, key, route)
	}
	var grpc gatewayv1.GRPCRoute
	if err := reader.Get(ctx, key, &grpc); err != nil {
		return err
	}
	*route = *model.ProjectGRPCRoute(&grpc)
	return nil
}

func (r *Reconciler) listRoutes(ctx context.Context, reader client.Reader, opts ...client.ListOption) ([]gatewayv1.HTTPRoute, error) {
	if r.kind() != model.GRPCRouteKind {
		var list gatewayv1.HTTPRouteList
		if err := reader.List(ctx, &list, opts...); err != nil {
			return nil, err
		}
		if list.Continue != "" {
			return nil, fmt.Errorf("incomplete HTTPRoute list")
		}
		return list.Items, nil
	}
	var list gatewayv1.GRPCRouteList
	if err := reader.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	if list.Continue != "" {
		return nil, fmt.Errorf("incomplete GRPCRoute list")
	}
	routes := make([]gatewayv1.HTTPRoute, 0, len(list.Items))
	for i := range list.Items {
		routes = append(routes, *model.ProjectGRPCRoute(&list.Items[i]))
	}
	return routes, nil
}

func routeAPIObject(route *gatewayv1.HTTPRoute) client.Object {
	if model.HTTPProfileRouteKind(route) != model.GRPCRouteKind {
		return route
	}
	return &gatewayv1.GRPCRoute{
		TypeMeta:   route.TypeMeta,
		ObjectMeta: *route.ObjectMeta.DeepCopy(),
		Status:     gatewayv1.GRPCRouteStatus{RouteStatus: *route.Status.RouteStatus.DeepCopy()},
	}
}

// patchRoute updates only metadata or status using the original resource kind.
// The projected HTTP rules are never written to Kubernetes.
func (r *Reconciler) patchRoute(ctx context.Context, route, base *gatewayv1.HTTPRoute, status bool) error {
	object := routeAPIObject(route)
	patch := controller.OptimisticMergeFrom(routeAPIObject(base))
	var err error
	if status {
		err = r.Status().Patch(ctx, object, patch)
	} else {
		err = r.Patch(ctx, object, patch)
	}
	if object, ok := object.(*gatewayv1.GRPCRoute); ok && err == nil {
		*route = *model.ProjectGRPCRoute(object)
	}
	return err
}
