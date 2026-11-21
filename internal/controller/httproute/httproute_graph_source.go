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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// ObserveRouteGraph rebuilds every bound fragment from a complete cluster read.
// Cached indexes cannot establish completeness for a destructive cloud plan.
func (r *Reconciler) ObserveRouteGraph(ctx context.Context, request graph.Request, gateway *gatewayv1.Gateway) (graph.RouteInputs, error) {
	if r.APIReader == nil {
		return graph.RouteInputs{}, controller.ErrAPIReaderRequired
	}
	routes, err := r.liveGraphRoutes(ctx, request.Identity)
	if err != nil {
		return graph.RouteInputs{}, err
	}
	var backends *routeModelInputs
	if request.Action == graph.Ensure {
		if gateway == nil {
			return graph.RouteInputs{}, errors.New("gateway is required to ensure routes")
		}
		listener, err := gatewaycontroller.Validate(gateway)
		if err != nil {
			return graph.RouteInputs{}, err
		}
		if listener.Protocol == gatewayv1.HTTPProtocolType {
			backends, err = r.collectGraphRouteModelInputs(ctx, gateway, routes, request.Route)
			if err != nil {
				return graph.RouteInputs{}, err
			}
		} else if request.Route != nil {
			return graph.RouteInputs{}, newRouteBuildError(routeErrorNoMatchingParent, "no HTTP listener matches this route")
		}
	}
	input := graph.RouteInputs{Nodes: make([]cloud.RouteGraphNode, 0, len(routes))}
	var bindings []routeGraphBinding
	if backends == nil {
		bindings = r.routeGraphBindings(routes, request.Identity)
	}
	fences := make([]func(context.Context) error, 0, len(routes))
	foundRequested := request.Route == nil
	for i := range routes {
		route := &routes[i]
		if !controller.RouteClaimsGateway(r.Config, route, request.Identity) {
			continue
		}
		identity, present, err := r.storedRouteIdentity(route)
		if err != nil {
			return graph.RouteInputs{}, err
		}
		if !present || !controller.SameGatewayIdentity(identity, request.Identity) {
			return graph.RouteInputs{}, fmt.Errorf("%w: HTTPRoute binding does not match the Gateway graph", cloud.ErrOwnershipConflict)
		}
		requested := request.Route != nil && route.UID == request.Route.UID
		if requested {
			foundRequested = true
			if !sameHTTPRouteRevision(route, request.Route.UID, request.Route.Generation, !request.Route.DeletionTimestamp.IsZero()) {
				return graph.RouteInputs{}, errHTTPRouteChanged
			}
		}
		node := cloud.RouteGraphNode{Spec: cloud.RouteSpec{Identity: identity}, Disposition: cloud.GraphPreserve}
		lifecycle := route.Annotations[r.Config.RouteLifecycleAnnotation()]
		switch lifecycle {
		case controller.RouteAttached, controller.RouteDetaching:
		default:
			return graph.RouteInputs{}, fmt.Errorf("%w: HTTPRoute fragment lifecycle is missing or invalid", cloud.ErrOwnershipConflict)
		}
		if request.Action == graph.DeleteGateway {
			input.Nodes = append(input.Nodes, node)
			continue
		}
		if lifecycle == controller.RouteDetaching {
			node.Disposition = cloud.GraphDelete
			snapshot := routeCleanupSnapshot{route: route, identity: identity, disposition: routeCleanupRequired}
			fences = append(fences, func(ctx context.Context) error { return r.sealRouteCleanupSnapshot(ctx, snapshot) })
		} else if backends != nil {
			if requested {
				if !route.DeletionTimestamp.IsZero() {
					return graph.RouteInputs{}, errHTTPRouteChanged
				}
			} else if _, eligible := backends.admission.eligible[client.ObjectKeyFromObject(route)]; !eligible {
				// The peer reconciles revocation through its durable detach checkpoint.
				node.WaitForHTTPPolicyRemoval = true
				input.Nodes = append(input.Nodes, node)
				continue
			}
			snapshot, err := r.buildRouteValidationSnapshotFromInputs(ctx, route, gateway, routeSnapshotForEnsure, backends)
			if err != nil {
				var rejection *routeBuildError
				if !requested && (errors.As(err, &rejection) || apierrors.IsNotFound(err)) {
					node.WaitForHTTPPolicyRemoval = true
					input.Nodes = append(input.Nodes, node)
					continue
				}
				return graph.RouteInputs{}, &cloud.GraphError{Identity: identity, Err: err}
			}
			if snapshot.disposition != routeValidationReady {
				return graph.RouteInputs{}, &cloud.GraphError{Identity: identity, Err: errHTTPRouteChanged}
			}
			node.Disposition, node.Spec = cloud.GraphEnsure, snapshot.desired
			fences = append(fences, func(ctx context.Context) error {
				return r.sealRouteOwnershipSnapshot(ctx, snapshot, routeBindingRequired)
			})
		}
		if request.Action == graph.DeleteRoute && requested && node.Disposition != cloud.GraphDelete {
			return graph.RouteInputs{}, errHTTPRouteChanged
		}
		input.Nodes = append(input.Nodes, node)
	}
	if !foundRequested {
		return graph.RouteInputs{}, errHTTPRouteChanged
	}
	input.Validate = func(ctx context.Context) error {
		current, err := r.liveGraphRoutes(ctx, request.Identity)
		if err != nil {
			return err
		}
		if backends == nil && !slices.Equal(bindings, r.routeGraphBindings(current, request.Identity)) {
			return errHTTPRouteChanged
		}
		for _, fence := range fences {
			if err := fence(ctx); err != nil {
				return err
			}
		}
		if backends != nil {
			return r.validateGraphRouteModel(ctx, backends, current, request.Route)
		}
		return nil
	}
	return input, nil
}

func (r *Reconciler) liveGraphRoutes(ctx context.Context, identity cloud.Identity) ([]gatewayv1.HTTPRoute, error) {
	var list gatewayv1.HTTPRouteList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list Gateway route fragments: %w", err)
	}
	if list.Continue != "" {
		return nil, errors.New("incomplete Gateway route fragment list")
	}
	result := make([]gatewayv1.HTTPRoute, 0)
	for _, route := range list.Items {
		if controller.RouteClaimsGateway(r.Config, &route, identity) {
			result = append(result, route)
			continue
		}
		// An unbound candidate can change route selection while a graph is built.
		for _, parent := range route.Spec.ParentRefs {
			if routeTargetsGateway(&route, parent, identity.GatewayNamespace, identity.GatewayName) {
				result = append(result, route)
				break
			}
		}
	}
	slices.SortFunc(result, func(a, b gatewayv1.HTTPRoute) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID))
	})
	return result, nil
}

type routeGraphBinding struct {
	route, boundNamespace, boundName, boundUID, lifecycle, project, cluster, finalizers string
}

func (r *Reconciler) routeGraphBindings(routes []gatewayv1.HTTPRoute, identity cloud.Identity) []routeGraphBinding {
	bindings := make([]routeGraphBinding, 0)
	for _, route := range routes {
		if !controller.RouteClaimsGateway(r.Config, &route, identity) {
			continue
		}
		binding := routeGraphBinding{
			route:          route.Namespace + "/" + route.Name + "/" + string(route.UID),
			boundNamespace: route.Annotations[r.Config.RouteGatewayNamespaceAnnotation()],
			boundName:      route.Annotations[r.Config.RouteGatewayNameAnnotation()],
			boundUID:       route.Annotations[r.Config.RouteGatewayUIDAnnotation()],
			lifecycle:      route.Annotations[r.Config.RouteLifecycleAnnotation()],
			project:        route.Annotations[r.Config.RouteProjectIDAnnotation()],
			cluster:        route.Annotations[r.Config.RouteClusterIDAnnotation()],
		}
		var finalizers []string
		for _, finalizer := range route.Finalizers {
			if finalizer == r.Config.RouteFinalizer() || strings.HasPrefix(finalizer, r.Config.RouteBindingFinalizerPrefix()) {
				finalizers = append(finalizers, finalizer)
			}
		}
		slices.Sort(finalizers)
		binding.finalizers = strings.Join(finalizers, "\x00")
		bindings = append(bindings, binding)
	}
	return bindings
}
