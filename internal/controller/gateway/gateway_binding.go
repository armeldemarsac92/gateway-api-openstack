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
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) bindGateway(
	ctx context.Context,
	expected *gatewayv1.Gateway,
	desiredListenerPort string,
) error {
	if r.gatewayNeedsUDPListenerMode(expected) {
		release, err := graph.Acquire(ctx, r.Coordinator, string(expected.UID))
		if err != nil {
			return err
		}
		defer release()
	}
	snapshot, err := r.observeGatewayMutationSnapshot(
		ctx,
		expected,
		gatewayBindingOptional,
		desiredListenerPort,
	)
	if err != nil {
		return err
	}
	gateway := snapshot.gateway.DeepCopy()

	metadataBase := gateway.DeepCopy()
	if err := controller.BindGatewayTopology(r.Config, gateway, snapshot.topology); err != nil {
		return err
	}
	if gateway.Annotations == nil {
		gateway.Annotations = map[string]string{}
	}
	gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] = desiredListenerPort
	gateway.Annotations[r.Config.GatewayListenerProtocolAnnotation()] = string(snapshot.listener.Protocol)
	gateway.Annotations[r.Config.GatewayClusterIDAnnotation()] = r.Config.ClusterID
	gateway.Annotations[r.Config.GatewayProjectIDAnnotation()] = r.Config.OpenStackProjectID
	controllerutil.AddFinalizer(gateway, r.Config.GatewayFinalizer())
	if r.gatewayNeedsUDPListenerMode(gateway) {
		anchor, err := strconv.Atoi(desiredListenerPort)
		if err != nil || anchor < 1 || anchor > 65535 {
			return fmt.Errorf("invalid frozen UDP Gateway port")
		}
		if err := r.sealGatewayMutationSnapshot(ctx, snapshot); err != nil {
			return err
		}
		if err := r.sealUDPRoutes(ctx, gateway, anchor); err != nil {
			return err
		}
		controller.SetGatewayUDPListenerMode(r.Config, gateway, true)
	}
	if err := controller.ValidateGatewayBinding(r.Config, gateway); err != nil {
		return err
	}
	if equality.Semantic.DeepEqual(metadataBase.ObjectMeta, gateway.ObjectMeta) {
		return nil
	}
	if err := r.sealGatewayMutationSnapshot(ctx, snapshot); err != nil {
		return err
	}
	if err := r.Patch(ctx, gateway, controller.OptimisticMergeFrom(metadataBase)); err != nil {
		return fmt.Errorf("patch Gateway resource binding: %w", err)
	}
	return nil
}

// sealUDPRoutes migrates the existing route checkpoints before the Gateway
// marker can authorize a scoped cloud write. Each migration is independently
// durable, so a conflict or restart can safely repeat the enrollment.
func (r *Reconciler) sealUDPRoutes(ctx context.Context, gateway *gatewayv1.Gateway, anchor int) error {
	gatewayID := controller.GatewayIdentity(r.Config, gateway)
	for _, verify := range []bool{false, true} {
		var list gatewayv1.UDPRouteList
		if err := r.APIReader.List(ctx, &list); err != nil {
			return err
		}
		if list.Continue != "" {
			return fmt.Errorf("incomplete UDPRoute enrollment list")
		}
		for i := range list.Items {
			route := &list.Items[i]
			if !controller.RouteClaimsGateway(r.Config, route, gatewayID) {
				continue
			}
			id, present, err := controller.StoredRouteIdentity(r.Config, route)
			if err != nil {
				return err
			}
			lifecycle := route.Annotations[r.Config.RouteLifecycleAnnotation()]
			if !present || !controller.SameGatewayIdentity(id, gatewayID) ||
				!controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) ||
				route.Annotations[r.Config.RouteClusterIDAnnotation()] != id.ClusterID || route.Annotations[r.Config.RouteProjectIDAnnotation()] != id.OpenStackProjectID ||
				(lifecycle != controller.RouteAttached && lifecycle != controller.RouteDetaching) {
				return fmt.Errorf("incomplete UDPRoute enrollment binding for %s/%s", route.Namespace, route.Name)
			}
			port := controller.UDPRouteAnchor(r.Config, route)
			if port == anchor {
				continue
			}
			// Kubernetes forbids adding a finalizer once deletion begins. Keep
			// the legacy cleanup path available until the route is gone.
			if verify || port != 0 || !route.DeletionTimestamp.IsZero() {
				return errGatewayChanged
			}
			base := route.DeepCopy()
			controller.SealUDPRouteAnchor(r.Config, route, id, anchor)
			if err := r.Patch(ctx, route, controller.OptimisticMergeFrom(base)); err != nil {
				return err
			}
		}
	}
	return nil
}
