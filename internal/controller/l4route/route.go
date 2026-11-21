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
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// transportRoute is a view of the actual Kubernetes object. Keep the full
// typed spec for revision checks and pass the concrete object to the client.
type transportRoute struct {
	client.Object
	parentRefs  []gatewayv1.ParentReference
	backendRefs []gatewayv1.BackendRef // Populated only for a single rule.
	status      *gatewayv1.RouteStatus
	spec        any
	protocol    gatewayv1.ProtocolType
}

func viewRoute(object client.Object) *transportRoute {
	route := &transportRoute{Object: object}
	switch object := object.(type) {
	case *gatewayv1.TCPRoute:
		route.protocol = gatewayv1.TCPProtocolType
		route.parentRefs = object.Spec.ParentRefs
		if len(object.Spec.Rules) == 1 {
			route.backendRefs = object.Spec.Rules[0].BackendRefs
		}
		route.status = &object.Status.RouteStatus
		route.spec = object.Spec
	case *gatewayv1.UDPRoute:
		route.protocol = gatewayv1.UDPProtocolType
		route.parentRefs = object.Spec.ParentRefs
		if len(object.Spec.Rules) == 1 {
			route.backendRefs = object.Spec.Rules[0].BackendRefs
		}
		route.status = &object.Status.RouteStatus
		route.spec = object.Spec
	default:
		return nil
	}
	return route
}

func (r *Reconciler) protocol() gatewayv1.ProtocolType {
	if r.Protocol == "" {
		return gatewayv1.TCPProtocolType
	}
	return r.Protocol
}

func (r *Reconciler) kind() gatewayv1.Kind {
	if r.protocol() == gatewayv1.UDPProtocolType {
		return "UDPRoute"
	}
	return "TCPRoute"
}

func (r *Reconciler) newRouteObject() client.Object {
	if r.protocol() == gatewayv1.UDPProtocolType {
		return &gatewayv1.UDPRoute{}
	}
	return &gatewayv1.TCPRoute{}
}

func (r *Reconciler) readRoutes(ctx context.Context) ([]*transportRoute, error) {
	routes, continuation, err := r.listRoutes(ctx, r.APIReader)
	if err == nil && continuation != "" {
		return nil, fmt.Errorf("incomplete %s attachment list", r.kind())
	}
	return routes, err
}

func (r *Reconciler) listRoutes(ctx context.Context, reader client.Reader, opts ...client.ListOption) ([]*transportRoute, string, error) {
	var list client.ObjectList = &gatewayv1.TCPRouteList{}
	if r.protocol() == gatewayv1.UDPProtocolType {
		list = &gatewayv1.UDPRouteList{}
	}
	if err := reader.List(ctx, list, opts...); err != nil {
		return nil, "", fmt.Errorf("list %s attachments: %w", r.kind(), err)
	}
	var routes []*transportRoute
	err := meta.EachListItem(list, func(item runtime.Object) error {
		routes = append(routes, viewRoute(item.(client.Object)))
		return nil
	})
	return routes, list.GetContinue(), err
}

func (route *transportRoute) copyObject() client.Object {
	return route.DeepCopyObject().(client.Object)
}

func (route *transportRoute) graphRequest(identity cloud.Identity, action graph.Action, gateway *gatewayv1.Gateway) graph.Request {
	request := graph.Request{Identity: identity, Action: action, Gateway: gateway}
	switch object := route.Object.(type) {
	case *gatewayv1.TCPRoute:
		request.TCPRoute = object
	case *gatewayv1.UDPRoute:
		request.UDPRoute = object
	}
	return request
}

func (r *Reconciler) requestRoute(request graph.Request) *transportRoute {
	if r.protocol() == gatewayv1.UDPProtocolType {
		if request.UDPRoute != nil {
			return viewRoute(request.UDPRoute)
		}
	} else if request.TCPRoute != nil {
		return viewRoute(request.TCPRoute)
	}
	return nil
}

func sameRouteRevision(current, expected *transportRoute) bool {
	return client.ObjectKeyFromObject(current) == client.ObjectKeyFromObject(expected) &&
		current.GetUID() == expected.GetUID() && current.GetGeneration() == expected.GetGeneration() &&
		current.GetDeletionTimestamp().Equal(expected.GetDeletionTimestamp()) && reflect.DeepEqual(current.spec, expected.spec)
}
