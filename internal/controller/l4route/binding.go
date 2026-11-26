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
	"fmt"
	"reflect"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) routeBindings(route *transportRoute) ([]controller.RouteGatewayBinding, error) {
	bindings, err := controller.StoredRouteGatewayBindings(r.Config, route.Object)
	if err != nil {
		return nil, errors.Join(cloud.ErrOwnershipConflict, err)
	}
	if len(bindings) == 0 {
		if controller.RouteHasControllerBinding(r.Config, route) || route.GetAnnotations()[r.Config.RouteLifecycleAnnotation()] != "" {
			return nil, fmt.Errorf("%w: %s %s has an incomplete lifecycle checkpoint", cloud.ErrOwnershipConflict, r.kind(), client.ObjectKeyFromObject(route))
		}
		return nil, nil
	}
	for _, binding := range bindings {
		if !controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) || !binding.ExplicitScope ||
			(binding.Lifecycle != controller.RouteAttached && binding.Lifecycle != controller.RouteDetaching) {
			return nil, fmt.Errorf("%w: %s %s lacks a complete lifecycle checkpoint", cloud.ErrOwnershipConflict, r.kind(), client.ObjectKeyFromObject(route))
		}
	}
	return bindings, nil
}

func boundGateway(bindings []controller.RouteGatewayBinding, identity cloud.Identity) controller.RouteGatewayBinding {
	for _, binding := range bindings {
		if controller.SameGatewayIdentity(binding.Identity, identity) {
			return binding
		}
	}
	return controller.RouteGatewayBinding{}
}

func (r *Reconciler) bind(ctx context.Context, expected *transportRoute, identity cloud.Identity) (ctrl.Result, error) {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return ctrl.Result{}, err
	}
	defer release()
	current, err := r.liveRoute(ctx, expected)
	if err != nil {
		return ctrl.Result{}, err
	}
	bindings, err := r.routeBindings(current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if boundGateway(bindings, identity).Identity == identity {
		return ctrl.Result{}, controller.ErrGraphInputChanged
	}
	first, err := r.planRoute(ctx, current, identity, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	second, err := r.planRoute(ctx, current, identity, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	if first.desired == nil || second.desired == nil || first.desired.Identity != identity || second.desired.Identity != identity ||
		first.target != second.target || !slices.Equal(first.listenerPorts, second.listenerPorts) ||
		!reflect.DeepEqual(first.serviceImports, second.serviceImports) ||
		revisionOf(first.parent.class) != revisionOf(second.parent.class) ||
		!sameGatewayRevision(r.Config, first.parent.gateway, second.parent.gateway) {
		return ctrl.Result{}, controller.ErrGraphInputChanged
	}
	// Set the admitted target before binding. A route that binds again
	// cannot use a prior binding's Programmed or ForwardingTarget status.
	first.targetStatus = routeCondition(current, r.forwardingTargetType(), metav1.ConditionFalse, "Pending", first.target)
	if err := r.patchParentStatuses(ctx, current, first); err != nil {
		return ctrl.Result{}, err
	}
	base := current.copyObject()
	binding := controller.RouteGatewayBinding{Identity: identity, Lifecycle: controller.RouteAttached}
	if len(first.desired.ScopedListenerPorts) != 0 {
		binding.Anchor = first.desired.GatewayRequirements.ListenerPort
	}
	if r.needsDurableTarget(current) {
		binding.Forwarding = controller.RouteGatewayTarget{Value: first.target, Generation: current.GetGeneration()}
	}
	if err := controller.ApplyRouteGatewayBinding(r.Config, current.Object, bindings, binding); err != nil {
		return ctrl.Result{}, err
	}
	if err := first.serviceImports.validate(ctx, r); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Patch(ctx, current.Object, controller.OptimisticMergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	*expected = *current
	return ctrl.Result{Requeue: true}, nil
}

func (r *Reconciler) liveRoute(ctx context.Context, expected *transportRoute) (*transportRoute, error) {
	object := r.newRouteObject()
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), object); err != nil {
		return nil, errors.Join(controller.ErrGraphInputChanged, client.IgnoreNotFound(err))
	}
	current := viewRoute(object)
	if !sameRouteRevision(current, expected) || current.GetResourceVersion() != expected.GetResourceVersion() {
		return nil, controller.ErrGraphInputChanged
	}
	return current, nil
}

func (r *Reconciler) detach(ctx context.Context, route *transportRoute, binding controller.RouteGatewayBinding) (ctrl.Result, error) {
	identity := binding.Identity
	if binding.Lifecycle != controller.RouteDetaching {
		return ctrl.Result{Requeue: true}, r.checkpointDetach(ctx, route, identity)
	}
	if r.Writer == nil {
		return ctrl.Result{}, errors.New("gateway graph writer is required")
	}
	result, err := r.Writer.ReconcileGraph(ctx, route.graphRequest(identity, graph.DeleteRoute, nil))
	if err != nil && !siblingFailure(err, identity) {
		if errors.Is(err, controller.ErrGraphInputChanged) {
			return ctrl.Result{}, err
		}
		policy, classified := controller.ProviderFailurePolicyFor(err)
		if !classified {
			policy = controller.ProviderFailurePolicy{Message: fmt.Sprintf("OpenStack could not clean up the %s", r.kind()), ReturnError: true}
		} else {
			policy = controller.ProviderCleanupFailurePolicy(policy, string(r.kind()))
		}
		return controller.ProviderFinalizationFailureResult(policy, err, string(route.GetUID()))
	}
	component, err := r.routeResult(result, identity, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	if component.Outcome.State == cloud.OutcomeProgressing {
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(component.Outcome, route.GetUID())}, nil
	}
	return ctrl.Result{Requeue: route.GetDeletionTimestamp().IsZero()}, r.clearBinding(ctx, route, identity)
}

func (r *Reconciler) checkpointDetach(ctx context.Context, expected *transportRoute, identity cloud.Identity) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	route, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	bindings, err := r.routeBindings(route)
	if err != nil {
		return err
	}
	binding := boundGateway(bindings, identity)
	if binding.Identity != identity || binding.Lifecycle != controller.RouteAttached {
		return controller.ErrGraphInputChanged
	}
	if route.GetDeletionTimestamp().IsZero() {
		plan, err := r.planRoute(ctx, route, identity, true)
		if err != nil {
			return err
		}
		if !plan.detach {
			return controller.ErrGraphInputChanged
		}
	}
	base := route.copyObject()
	binding.Lifecycle = controller.RouteDetaching
	binding.Forwarding = controller.RouteGatewayTarget{}
	if err := controller.ApplyRouteGatewayBinding(r.Config, route.Object, bindings, binding); err != nil {
		return err
	}
	if err := r.Patch(ctx, route.Object, controller.OptimisticMergeFrom(base)); err != nil {
		return err
	}
	*expected = *route
	return nil
}

func (r *Reconciler) clearBinding(ctx context.Context, expected *transportRoute, identity cloud.Identity) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	route, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	bindings, err := r.routeBindings(route)
	if err != nil {
		return err
	}
	binding := boundGateway(bindings, identity)
	if binding.Identity != identity || binding.Lifecycle != controller.RouteDetaching {
		return controller.ErrGraphInputChanged
	}
	base := route.copyObject()
	if err := controller.RemoveRouteGatewayBinding(r.Config, route.Object, bindings, identity); err != nil {
		return err
	}
	if err := r.Patch(ctx, route.Object, controller.OptimisticMergeFrom(base)); err != nil {
		return err
	}
	*expected = *route
	return nil
}

func sameGatewayRevision(config controller.Config, left, right *gatewayv1.Gateway) bool {
	return left.UID == right.UID && left.Generation == right.Generation &&
		left.DeletionTimestamp.Equal(right.DeletionTimestamp) && reflect.DeepEqual(left.Spec, right.Spec) &&
		controller.GatewayBindingMetadataEqual(config, left, right)
}
