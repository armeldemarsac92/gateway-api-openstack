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

package tcproute

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

var _ graph.RouteSource = (*Reconciler)(nil)

type routeGraphSnapshot struct {
	nodes     []cloud.RouteGraphNode
	revisions []string
	winner    types.UID
	backend   types.UID
	gateway   *gatewayv1.Gateway
	class     types.UID
}

// ObserveRouteGraph constructs and fences the TCPRoute fragment of the graph
// while the shared writer holds the Gateway UID lock.
func (r *Reconciler) ObserveRouteGraph(ctx context.Context, request graph.Request, gateway *gatewayv1.Gateway) (graph.RouteInputs, error) {
	if r.APIReader == nil {
		return graph.RouteInputs{}, controller.ErrAPIReaderRequired
	}
	snapshot, err := r.observeRoutes(ctx, request, gateway)
	if err != nil {
		return graph.RouteInputs{}, err
	}
	return graph.RouteInputs{
		Nodes: snapshot.nodes,
		Validate: func(ctx context.Context) error {
			latest, err := r.observeRoutes(ctx, request, gateway)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(snapshot, latest) {
				return controller.ErrGraphInputChanged
			}
			return nil
		},
	}, nil
}

func (r *Reconciler) observeRoutes(ctx context.Context, request graph.Request, expectedGateway *gatewayv1.Gateway) (routeGraphSnapshot, error) {
	var snapshot routeGraphSnapshot
	routes, err := r.listRoutes(ctx)
	if err != nil {
		return snapshot, err
	}
	slices.SortFunc(routes, func(left, right gatewayv1.TCPRoute) int {
		return cmp.Or(cmp.Compare(left.Namespace, right.Namespace), cmp.Compare(left.Name, right.Name))
	})

	var listener *gatewayv1.Listener
	if request.Action == graph.Ensure {
		gateway, err := r.liveGateway(ctx, request.Identity, expectedGateway)
		if err != nil {
			return snapshot, err
		}
		// Store only safety-sensitive inputs. A status update from an unrelated
		// controller must not invalidate this route observation.
		snapshot.gateway = gateway.DeepCopy()
		snapshot.gateway.ResourceVersion = ""
		snapshot.gateway.Status = gatewayv1.GatewayStatus{}
		listener, err = gatewaycontroller.Validate(gateway)
		if err == nil && listener.Protocol == gatewayv1.TCPProtocolType && gateway.DeletionTimestamp.IsZero() {
			winner, err := r.selectedRoute(ctx, gateway, listener, routes)
			if err != nil {
				return snapshot, err
			}
			if winner != nil {
				snapshot.winner = winner.UID
			}
		}
	}

	requestedIndex, winnerIndex := -1, -1
	var winner *gatewayv1.TCPRoute
	peerAttached := false
	for i := range routes {
		route := &routes[i]
		if !controller.RouteClaimsGateway(r.Config, route, request.Identity) {
			continue
		}
		identity, present, err := r.routeBinding(route)
		if err != nil {
			return snapshot, err
		}
		if !present || !controller.SameGatewayIdentity(identity, request.Identity) {
			return snapshot, fmt.Errorf("%w: TCPRoute binding does not match the Gateway graph", cloud.ErrOwnershipConflict)
		}
		requested := request.TCPRoute != nil && route.UID == request.TCPRoute.UID
		if requested {
			if request.Identity != identity || !sameRouteRevision(route, request.TCPRoute) {
				return snapshot, controller.ErrGraphInputChanged
			}
			requestedIndex = len(snapshot.nodes)
		}
		node := cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{
			Identity: identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolTCP},
		}}
		lifecycle := route.Annotations[r.Config.RouteLifecycleAnnotation()]
		switch {
		case lifecycle == controller.RouteDetaching:
			if request.Action == graph.Ensure || request.Action == graph.DeleteRoute && requested {
				node.Disposition = cloud.GraphDelete
			}
		case route.UID == snapshot.winner && route.DeletionTimestamp.IsZero():
			winnerIndex, winner = len(snapshot.nodes), route
		default:
			peerAttached = true
		}
		snapshot.nodes = append(snapshot.nodes, node)
		snapshot.revisions = append(snapshot.revisions, route.ResourceVersion)
	}
	if winner != nil && !peerAttached {
		spec, backend, class, err := r.ensureRouteNode(ctx, winner, snapshot.gateway, listener)
		if err != nil {
			return snapshot, err
		}
		if spec != nil {
			snapshot.nodes[winnerIndex].Disposition = cloud.GraphEnsure
			snapshot.nodes[winnerIndex].Spec = *spec
			snapshot.backend, snapshot.class = backend, class
		}
	}
	if request.TCPRoute != nil {
		if requestedIndex < 0 ||
			request.Action == graph.Ensure && snapshot.nodes[requestedIndex].Disposition != cloud.GraphEnsure ||
			request.Action == graph.DeleteRoute && snapshot.nodes[requestedIndex].Disposition != cloud.GraphDelete {
			return snapshot, controller.ErrGraphInputChanged
		}
	}
	return snapshot, nil
}

func (r *Reconciler) liveGateway(ctx context.Context, identity cloud.Identity, expected *gatewayv1.Gateway) (*gatewayv1.Gateway, error) {
	if expected == nil || identity.GatewayUID != string(expected.UID) ||
		identity.GatewayNamespace != expected.Namespace || identity.GatewayName != expected.Name {
		return nil, controller.ErrGraphInputChanged
	}
	gateway := &gatewayv1.Gateway{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), gateway); err != nil {
		return nil, errors.Join(controller.ErrGraphInputChanged, client.IgnoreNotFound(err))
	}
	if !sameGatewayRevision(r.Config, expected, gateway) {
		return nil, controller.ErrGraphInputChanged
	}
	return gateway, nil
}

func (r *Reconciler) ensureRouteNode(ctx context.Context, route *gatewayv1.TCPRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (*cloud.RouteSpec, types.UID, types.UID, error) {
	class, err := r.gatewayClass(ctx, gateway)
	if err != nil || class == nil || !controller.GatewayClassSupportsInstalledVersion(class) {
		return nil, "", "", err
	}
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			err = nil
		}
		return nil, "", "", err
	}
	topology, valid, err := r.gatewayTopology(gateway, listener)
	if err != nil || !valid {
		return nil, "", "", err
	}
	ref, diagnostic, err := r.authorizedBackend(ctx, route)
	if err != nil || diagnostic != nil || ref.disabled {
		return nil, "", "", err
	}
	service, diagnostic, err := r.backendService(ctx, ref)
	if err != nil || diagnostic != nil {
		return nil, "", "", err
	}
	spec, diagnostic, err := r.routeSpec(ctx, route, gateway, listener, ref, service, topology)
	if err != nil || diagnostic != nil {
		return nil, "", "", err
	}
	return &spec, service.UID, class.UID, nil
}
