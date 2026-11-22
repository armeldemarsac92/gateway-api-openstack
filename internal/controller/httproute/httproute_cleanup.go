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
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) detachPreviousGateway(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
) (ctrl.Result, error) {
	routeUID, generation, isDeleting := route.UID, route.Generation, !route.DeletionTimestamp.IsZero()
	current := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.Client, client.ObjectKeyFromObject(route), current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !sameHTTPRouteRevision(current, routeUID, generation, isDeleting) {
		return ctrl.Result{}, errHTTPRouteChanged
	}
	*route = *current

	stored, present, err := r.storedRouteIdentity(current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !present {
		if controllerutil.ContainsFinalizer(current, r.Config.RouteFinalizer()) {
			return ctrl.Result{}, fmt.Errorf("HTTPRoute %s/%s has the controller finalizer but no complete stored Gateway identity", current.Namespace, current.Name)
		}
		return ctrl.Result{}, nil
	}
	if stored.GatewayNamespace == gateway.Namespace && stored.GatewayName == gateway.Name && stored.GatewayUID == string(gateway.UID) {
		return ctrl.Result{}, nil
	}
	return r.detachRoute(ctx, route)
}

func (r *Reconciler) detachRoute(ctx context.Context, route *gatewayv1.HTTPRoute) (ctrl.Result, error) {
	routeKey := client.ObjectKeyFromObject(route)
	routeUID, generation, isDeleting := route.UID, route.Generation, !route.DeletionTimestamp.IsZero()
	current, stored, present, err := r.getRouteBinding(ctx, routeKey, routeUID, generation, isDeleting)
	if err != nil {
		return ctrl.Result{}, err
	}
	if current == nil {
		return ctrl.Result{}, nil
	}
	if !present && controllerutil.ContainsFinalizer(current, r.Config.RouteFinalizer()) {
		return ctrl.Result{}, fmt.Errorf("HTTPRoute %s has the controller finalizer but no complete stored Gateway identity", routeKey)
	}
	if !present {
		return ctrl.Result{}, r.clearGatewayBinding(ctx, route, cloud.Identity{})
	}
	return r.detachGatewayBinding(ctx, route, current, stored)
}

func (r *Reconciler) detachGatewayBinding(ctx context.Context, route, current *gatewayv1.HTTPRoute, stored cloud.Identity) (ctrl.Result, error) {
	outcome, err := r.deleteRoute(ctx, current, stored)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete detached HTTPRoute resources: %w", err)
	}
	if err := outcome.Validate(); err != nil {
		return ctrl.Result{}, fmt.Errorf("validate detached HTTPRoute deletion outcome: %w", err)
	}
	if outcome.State == cloud.OutcomeProgressing {
		if _, err := r.patchRouteCleanupFailure(ctx, current, stored, ""); err != nil {
			return ctrl.Result{}, fmt.Errorf("clear HTTPRoute cleanup failure checkpoint: %w", err)
		}
		*route = *current
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(outcome, current.UID)}, nil
	}
	log.FromContext(ctx).V(1).Info("Deleted detached HTTPRoute resources", "gateway", types.NamespacedName{Namespace: stored.GatewayNamespace, Name: stored.GatewayName})
	if err := r.clearGatewayBinding(ctx, route, stored); err != nil {
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).V(1).Info("Detached HTTPRoute from Gateway")
	return ctrl.Result{}, nil
}

func (r *Reconciler) finalizeRoute(ctx context.Context, scope *httpRouteScope) (ctrl.Result, error) {
	route := scope.route
	current := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.routePatchReader(), client.ObjectKeyFromObject(route), current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !sameHTTPRouteRevision(current, route.UID, route.Generation, !route.DeletionTimestamp.IsZero()) {
		return ctrl.Result{}, errHTTPRouteChanged
	}
	bindings, err := controller.StoredRouteGatewayBindings(r.Config, current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(bindings) == 0 && controllerutil.ContainsFinalizer(current, r.Config.RouteFinalizer()) {
		return ctrl.Result{}, fmt.Errorf("HTTPRoute %s has the controller finalizer but no complete stored Gateway identity", client.ObjectKeyFromObject(route))
	}
	var result ctrl.Result
	var cleanupErrors error
	for _, binding := range bindings {
		progress, err := r.detachGatewayBinding(ctx, route, current, binding.Identity)
		if err != nil {
			*route = *current
			progress, err = r.handleRouteFinalizationFailure(ctx, scope, binding.Identity, err)
			// Publish the affected parent's failure before processing another binding.
			if len(bindings) > 1 {
				err = errors.Join(err, r.patchHTTPRoute(ctx, scope))
				scope.statusPlan, scope.warning, scope.patchCause = nil, nil, nil
			}
		}
		result = mergeRouteResult(result, progress)
		cleanupErrors = errors.Join(cleanupErrors, err)
		*current = *route
	}
	return result, cleanupErrors
}

func (r *Reconciler) patchRouteCleanupFailure(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	identity cloud.Identity,
	reason string,
) (bool, error) {
	routeKey := client.ObjectKeyFromObject(route)
	routeUID, generation, isDeleting := route.UID, route.Generation, !route.DeletionTimestamp.IsZero()
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		current := &gatewayv1.HTTPRoute{}
		if err := r.readRoute(ctx, r.routePatchReader(), routeKey, current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !sameHTTPRouteRevision(current, routeUID, generation, isDeleting) {
			return errHTTPRouteChanged
		}
		bindings, err := controller.StoredRouteGatewayBindings(r.Config, current)
		if err != nil {
			return err
		}
		target := identity
		if target.RouteUID == "" && len(bindings) == 1 {
			target = bindings[0].Identity
		}
		if !slices.ContainsFunc(bindings, func(binding controller.RouteGatewayBinding) bool {
			return binding.Identity == target
		}) {
			return errHTTPRouteChanged
		}
		key := r.Config.RouteCleanupFailureAnnotation()
		if controller.HasRouteGatewayBindings(r.Config, current) {
			key = r.Config.RouteGatewayCleanupFailureAnnotation(target.GatewayUID)
		}
		if current.Annotations[key] == reason {
			*route = *current
			return nil
		}

		base := current.DeepCopy()
		if reason == "" {
			delete(current.Annotations, key)
		} else {
			if current.Annotations == nil {
				current.Annotations = map[string]string{}
			}
			current.Annotations[key] = reason
		}
		if err := r.patchRoute(ctx, current, base, false); err != nil {
			return client.IgnoreNotFound(err)
		}
		changed = true
		*route = *current
		return nil
	})
	return changed, err
}

func (r *Reconciler) deleteRoute(
	ctx context.Context,
	expected *gatewayv1.HTTPRoute,
	identity cloud.Identity,
) (cloud.Outcome, error) {
	if r.APIReader == nil {
		return cloud.Outcome{}, controller.ErrAPIReaderRequired
	}
	if r.Writer == nil {
		return cloud.Outcome{}, fmt.Errorf("gateway graph writer is required")
	}
	binding, err := controller.StoredRouteGatewayBinding(r.Config, expected, identity)
	if err != nil {
		return cloud.Outcome{}, err
	}
	if binding.Identity != identity {
		return cloud.Outcome{}, errHTTPRouteChanged
	}
	if binding.Lifecycle != controller.RouteDetaching {
		if err := r.checkpointRouteDetaching(ctx, expected, identity); err != nil {
			return cloud.Outcome{}, err
		}
		return cloud.ProgressingOutcome("HTTPRoute cleanup checkpoint was stored", time.Second), nil
	}
	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{Identity: identity, Action: graph.DeleteRoute, Route: expected})
	if err != nil {
		var fragment *cloud.GraphError
		if !errors.As(err, &fragment) || fragment.Identity.RouteUID == "" || fragment.Identity.RouteUID == identity.RouteUID {
			return cloud.Outcome{}, err
		}
		// A failure acting on a sibling does not invalidate our own completed
		// observation. Keep the finalizer on failures outside this Gateway.
		parent := fragment.Identity
		parent.RouteNamespace, parent.RouteName, parent.RouteUID = identity.RouteNamespace, identity.RouteName, identity.RouteUID
		if parent != identity {
			return cloud.Outcome{}, err
		}
	}
	component, err := routeComponentResult(result, identity)
	return component.Outcome, err
}

func (r *Reconciler) checkpointRouteDetaching(ctx context.Context, expected *gatewayv1.HTTPRoute, identity cloud.Identity) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return fmt.Errorf("acquire Gateway graph: %w", err)
	}
	defer release()
	snapshot, err := r.buildRouteCleanupSnapshot(ctx, expected, identity)
	if err != nil {
		return err
	}
	if snapshot.disposition != routeCleanupRequired {
		return errHTTPRouteChanged
	}
	if err := r.sealRouteCleanupSnapshot(ctx, snapshot); err != nil {
		return err
	}
	base := snapshot.route.DeepCopy()
	desired := snapshot.route.DeepCopy()
	if err := controller.MarkRouteGatewayDetaching(r.Config, desired, identity); err != nil {
		return err
	}
	if err := r.patchRoute(ctx, desired, base, false); err != nil {
		return err
	}
	*expected = *desired
	return nil
}
