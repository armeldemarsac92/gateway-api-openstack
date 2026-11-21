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
	"fmt"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// ObserveGatewayGraph validates the live Gateway component under the writer lock.
func (r *Reconciler) ObserveGatewayGraph(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	if r.APIReader == nil {
		return graph.GatewayInput{}, controller.ErrAPIReaderRequired
	}
	if request.Action == graph.DeleteRoute {
		return r.observeGatewayForRouteDeletion(ctx, request)
	}
	if request.Gateway == nil {
		return graph.GatewayInput{}, errors.New("expected Gateway revision is required")
	}
	if request.Action == graph.DeleteGateway {
		snapshot, err := r.observeGatewayCleanupSnapshot(ctx, request.Gateway)
		if err != nil {
			return graph.GatewayInput{}, err
		}
		if snapshot.disposition == gatewayCleanupHold || snapshot.identity != request.Identity {
			return graph.GatewayInput{}, errGatewayChanged
		}
		port, _ := strconv.Atoi(snapshot.boundListenerPort)
		spec := cloud.GatewaySpec{ListenerProtocol: string(snapshot.boundListenerProtocol), ListenerPort: port}
		if snapshot.udpListenerMode {
			spec.UDPListeners = []cloud.UDPListener{}
		}
		return graph.GatewayInput{
			Object: request.Gateway.DeepCopy(),
			Node: cloud.GatewayGraphNode{
				Disposition: cloud.GraphDelete,
				Spec:        spec,
			},
			Validate: func(ctx context.Context) error {
				latest, err := r.observeGatewayCleanupSnapshot(ctx, request.Gateway)
				if err != nil {
					return err
				}
				if *latest != *snapshot {
					return errGatewayChanged
				}
				return nil
			},
		}, nil
	}
	if request.Action != graph.Ensure {
		return graph.GatewayInput{}, errors.New("invalid Gateway graph action")
	}
	snapshot, err := r.observeGatewayMutationSnapshot(ctx, request.Gateway, gatewayBindingRequired, "")
	if err != nil {
		return graph.GatewayInput{}, err
	}
	desired := r.gatewaySpec(snapshot.gateway, snapshot.listeners, snapshot.topology)
	return graph.GatewayInput{
		Object: snapshot.gateway, Node: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: desired},
		Validate: func(ctx context.Context) error { return r.sealGatewayMutationSnapshot(ctx, snapshot) },
	}, nil
}

func (r *Reconciler) observeGatewayForRouteDeletion(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	input := graph.GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}}
	var routeAnchor int
	if route := request.UDPRoute; route != nil {
		identity, present, err := controller.StoredRouteIdentity(r.Config, route)
		if err != nil {
			return graph.GatewayInput{}, err
		}
		if !present || identity != request.Identity ||
			!controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) ||
			route.Annotations[r.Config.RouteClusterIDAnnotation()] != identity.ClusterID ||
			route.Annotations[r.Config.RouteProjectIDAnnotation()] != identity.OpenStackProjectID ||
			route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
			return graph.GatewayInput{}, errGatewayChanged
		}
		routeAnchor = controller.UDPRouteAnchor(r.Config, route)
		if routeAnchor != 0 {
			input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolUDP, ListenerPort: routeAnchor, UDPListeners: []cloud.UDPListener{}}
		}
	}

	var current gatewayv1.Gateway
	key := client.ObjectKey{Namespace: request.Identity.GatewayNamespace, Name: request.Identity.GatewayName}
	if err := r.APIReader.Get(ctx, key, &current); err != nil {
		return input, client.IgnoreNotFound(err)
	}
	if string(current.UID) != request.Identity.GatewayUID {
		return input, nil
	}
	input.Object = &current
	boundProtocol := r.boundListenerProtocol(&current)
	scopedBinding := controller.HasGatewayUDPListenerModeBinding(r.Config, &current)
	if !scopedBinding && boundProtocol != gatewayv1.UDPProtocolType && routeAnchor == 0 {
		return input, nil
	}
	input.Validate = func(ctx context.Context) error {
		var latest gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, key, &latest); err != nil {
			return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if latest.UID != current.UID || !controller.GatewayBindingMetadataEqual(r.Config, &current, &latest) {
			return errGatewayChanged
		}
		return nil
	}
	if !controller.GatewayHasControllerBinding(r.Config, &current) {
		// A fully cleared Gateway binding can outlive its old routes after
		// teardown. Their sealed anchors still authorize route-owned cleanup.
		return input, nil
	}
	if !scopedBinding && routeAnchor == 0 && request.UDPRoute != nil && !hasMultipleUDPListeners(&current) {
		// The legacy singleton cleanup is authorized by the route's durable
		// ownership alone. It does not provision from the Gateway topology.
		return input, nil
	}
	if err := controller.ValidateGatewayBinding(r.Config, &current); err != nil {
		// Scoped ownership comes only from a verified Gateway or route
		// checkpoint. Never reinterpret a damaged current binding as legacy.
		return graph.GatewayInput{}, err
	}
	owner := request.Identity
	owner.RouteNamespace, owner.RouteName, owner.RouteUID = "", "", ""
	port, err := strconv.Atoi(current.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if err != nil || port < 1 || port > 65535 || !controllerutil.ContainsFinalizer(&current, r.Config.GatewayFinalizer()) ||
		controller.StoredGatewayIdentity(r.Config, &current) != owner {
		if scopedBinding || routeAnchor != 0 {
			return graph.GatewayInput{}, fmt.Errorf("%w: Gateway cleanup binding is incomplete or belongs to a different owner", cloud.ErrOwnershipConflict)
		}
		return input, nil
	}
	switch {
	case controller.GatewayUsesMultipleUDPListeners(r.Config, &current):
		if request.UDPRoute != nil && routeAnchor != port {
			return graph.GatewayInput{}, fmt.Errorf("%w: UDPRoute cleanup lacks the enrolled Gateway's frozen anchor", cloud.ErrOwnershipConflict)
		}
		input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolUDP, ListenerPort: port, UDPListeners: []cloud.UDPListener{}}
	case boundProtocol != gatewayv1.UDPProtocolType:
		// Changing a bound protocol requires verified teardown of the old
		// graph. Use the current legacy preservation path for its replacement.
		input.Node.Spec = cloud.GatewaySpec{}
	case routeAnchor != 0 && routeAnchor != port:
		return graph.GatewayInput{}, fmt.Errorf("%w: UDPRoute cleanup disagrees with the Gateway's frozen anchor", cloud.ErrOwnershipConflict)
	case routeAnchor != port:
		input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolUDP, ListenerPort: port}
	}
	return input, nil
}
