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
	"context"
	"errors"
	"fmt"
	"reflect"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) routeBinding(route *gatewayv1.TCPRoute) (cloud.Identity, bool, error) {
	identity, present, err := controller.StoredRouteIdentity(r.Config, route)
	if err != nil {
		return cloud.Identity{}, false, errors.Join(cloud.ErrOwnershipConflict, err)
	}
	if !present {
		if controller.RouteHasControllerBinding(r.Config, route) || route.Annotations[r.Config.RouteLifecycleAnnotation()] != "" {
			return cloud.Identity{}, false, fmt.Errorf("%w: TCPRoute %s has an incomplete lifecycle checkpoint", cloud.ErrOwnershipConflict, client.ObjectKeyFromObject(route))
		}
		return cloud.Identity{}, false, nil
	}
	lifecycle := route.Annotations[r.Config.RouteLifecycleAnnotation()]
	if !controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) ||
		!controllerutil.ContainsFinalizer(route, r.bindingFinalizer(identity)) ||
		route.Annotations[r.Config.RouteClusterIDAnnotation()] != identity.ClusterID ||
		route.Annotations[r.Config.RouteProjectIDAnnotation()] != identity.OpenStackProjectID ||
		(lifecycle != controller.RouteAttached && lifecycle != controller.RouteDetaching) {
		return cloud.Identity{}, false, fmt.Errorf("%w: TCPRoute %s lacks a complete lifecycle checkpoint", cloud.ErrOwnershipConflict, client.ObjectKeyFromObject(route))
	}
	return identity, true, nil
}

func (r *Reconciler) bindingFinalizer(identity cloud.Identity) string {
	return r.Config.RouteBindingFinalizer(identity.ClusterID, identity.OpenStackProjectID, identity.GatewayNamespace, identity.GatewayName, identity.GatewayUID)
}

func (r *Reconciler) bind(ctx context.Context, expected *gatewayv1.TCPRoute, identity cloud.Identity) (ctrl.Result, error) {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return ctrl.Result{}, err
	}
	defer release()
	current, err := r.liveRoute(ctx, expected)
	if err != nil {
		return ctrl.Result{}, err
	}
	if controller.RouteHasControllerBinding(r.Config, current) {
		return ctrl.Result{}, controller.ErrGraphInputChanged
	}
	first, err := r.planRoute(ctx, current, cloud.Identity{}, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	second, err := r.planRoute(ctx, current, cloud.Identity{}, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	if first.desired == nil || first.desired.Identity != identity || !reflect.DeepEqual(first.desired, second.desired) ||
		!sameGatewayRevision(r.Config, first.parents[0].gateway, second.parents[0].gateway) {
		return ctrl.Result{}, controller.ErrGraphInputChanged
	}
	base := current.DeepCopy()
	controller.ApplyRouteBinding(r.Config, current, identity)
	if err := r.Patch(ctx, current, controller.OptimisticMergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *Reconciler) liveRoute(ctx context.Context, expected *gatewayv1.TCPRoute) (*gatewayv1.TCPRoute, error) {
	current := &gatewayv1.TCPRoute{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), current); err != nil {
		return nil, errors.Join(controller.ErrGraphInputChanged, client.IgnoreNotFound(err))
	}
	if !sameRouteRevision(current, expected) || current.ResourceVersion != expected.ResourceVersion {
		return nil, controller.ErrGraphInputChanged
	}
	return current, nil
}

func (r *Reconciler) detach(ctx context.Context, route *gatewayv1.TCPRoute, identity cloud.Identity) (ctrl.Result, error) {
	if route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		return ctrl.Result{Requeue: true}, r.checkpointDetach(ctx, route, identity)
	}
	if r.Writer == nil {
		return ctrl.Result{}, errors.New("gateway graph writer is required")
	}
	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{Identity: identity, Action: graph.DeleteRoute, TCPRoute: route})
	if err != nil && !siblingFailure(err, identity) {
		if errors.Is(err, controller.ErrGraphInputChanged) {
			return ctrl.Result{}, err
		}
		policy, classified := controller.ProviderFailurePolicyFor(err)
		if !classified {
			policy = controller.ProviderFailurePolicy{Message: "OpenStack could not clean up the TCPRoute", ReturnError: true}
		} else {
			policy = controller.ProviderCleanupFailurePolicy(policy, "TCPRoute")
		}
		return controller.ProviderFinalizationFailureResult(policy, err, string(route.UID))
	}
	component, err := routeResult(result, identity)
	if err != nil {
		return ctrl.Result{}, err
	}
	if component.Outcome.State == cloud.OutcomeProgressing {
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(component.Outcome, route.UID)}, nil
	}
	return ctrl.Result{Requeue: route.DeletionTimestamp.IsZero()}, r.clearBinding(ctx, route, identity)
}

func (r *Reconciler) checkpointDetach(ctx context.Context, expected *gatewayv1.TCPRoute, identity cloud.Identity) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	route, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	binding, present, err := r.routeBinding(route)
	if err != nil {
		return err
	}
	if !present || binding != identity {
		return controller.ErrGraphInputChanged
	}
	var plan routePlan
	if route.DeletionTimestamp.IsZero() {
		plan, err = r.planRoute(ctx, route, identity, true)
		if err != nil {
			return err
		}
		if !plan.detach {
			return controller.ErrGraphInputChanged
		}
	}
	base := route.DeepCopy()
	route.Annotations[r.Config.RouteLifecycleAnnotation()] = controller.RouteDetaching
	if err := r.Patch(ctx, route, controller.OptimisticMergeFrom(base)); err != nil {
		return err
	}
	if route.DeletionTimestamp.IsZero() {
		return r.patchStatuses(ctx, route, plan)
	}
	return nil
}

func (r *Reconciler) clearBinding(ctx context.Context, expected *gatewayv1.TCPRoute, identity cloud.Identity) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	route, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	binding, present, err := r.routeBinding(route)
	if err != nil {
		return err
	}
	if !present || binding != identity || route.Annotations[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
		return controller.ErrGraphInputChanged
	}
	base := route.DeepCopy()
	controller.ClearRouteBinding(r.Config, route)
	return r.Patch(ctx, route, controller.OptimisticMergeFrom(base))
}

func sameGatewayRevision(config controller.Config, left, right *gatewayv1.Gateway) bool {
	return left.UID == right.UID && left.Generation == right.Generation &&
		left.DeletionTimestamp.Equal(right.DeletionTimestamp) && reflect.DeepEqual(left.Spec, right.Spec) &&
		controller.GatewayBindingMetadataEqual(config, left, right)
}
