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
	"reflect"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func (r *Reconciler) bindRoute(ctx context.Context, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) (bool, error) {
	if r.APIReader == nil {
		return false, controller.ErrAPIReaderRequired
	}
	snapshot, err := r.buildRouteValidationSnapshot(ctx, route, gateway, routeSnapshotForBinding)
	if err != nil {
		return false, err
	}
	return r.bindRouteFromSnapshot(ctx, route, gateway, snapshot)
}

// bindRouteFromSnapshot uses the model already built while holding the
// Gateway graph lock. A conflict retry rebuilds the snapshot because the
// original observation is no longer current.
func (r *Reconciler) bindRouteFromSnapshot(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	gateway *gatewayv1.Gateway,
	initial routeValidationSnapshot,
) (bool, error) {
	bindingStored := false
	snapshot := initial
	firstAttempt := true
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if firstAttempt {
			firstAttempt = false
		} else {
			refreshed, err := r.buildRouteValidationSnapshot(ctx, route, gateway, routeSnapshotForBinding)
			if err != nil {
				return err
			}
			snapshot = refreshed
		}
		if err := r.validateRouteBindingTransition(snapshot); err != nil {
			return err
		}
		if err := r.sealRouteMutationSnapshot(ctx, snapshot, routeBindingOptional); err != nil {
			return err
		}

		base := snapshot.route.DeepCopy()
		desired := snapshot.route.DeepCopy()
		if err := r.applyRouteBinding(desired, snapshot.gateway); err != nil {
			return err
		}
		if routeBindingMetadataEqual(base, desired) {
			*route = *desired
			return nil
		}
		if err := r.patchRoute(ctx, desired, base, false); err != nil {
			return err
		}
		bindingStored = true
		*route = *desired
		log.FromContext(ctx).V(1).Info("Bound HTTPRoute to Gateway", "gateway", client.ObjectKeyFromObject(snapshot.gateway))
		return nil
	})
	return bindingStored, err
}

func (r *Reconciler) getRouteBinding(
	ctx context.Context,
	routeKey types.NamespacedName,
	routeUID types.UID,
	generation int64,
	isDeleting bool,
) (*gatewayv1.HTTPRoute, cloud.Identity, bool, error) {
	current := &gatewayv1.HTTPRoute{}
	if err := r.readRoute(ctx, r.Client, routeKey, current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, cloud.Identity{}, false, nil
		}
		return nil, cloud.Identity{}, false, err
	}
	if !sameHTTPRouteRevision(current, routeUID, generation, isDeleting) {
		return current, cloud.Identity{}, false, errHTTPRouteChanged
	}
	stored, present, err := r.storedRouteIdentity(current)
	return current, stored, present, err
}

// clearGatewayBinding removes only the completed fragment from a fresh route.
func (r *Reconciler) clearGatewayBinding(ctx context.Context, route *gatewayv1.HTTPRoute, identity cloud.Identity) error {
	if r.APIReader == nil {
		return controller.ErrAPIReaderRequired
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &gatewayv1.HTTPRoute{}
		if err := r.readRoute(ctx, r.APIReader, client.ObjectKeyFromObject(route), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !sameHTTPRouteRevision(current, route.UID, route.Generation, !route.DeletionTimestamp.IsZero()) {
			return errHTTPRouteChanged
		}
		bindings, err := controller.StoredRouteGatewayBindings(r.Config, current)
		if err != nil {
			return err
		}
		if identity.RouteUID == "" {
			if len(bindings) != 0 || controllerutil.ContainsFinalizer(current, r.Config.RouteFinalizer()) {
				return errHTTPRouteChanged
			}
		} else {
			found := false
			for _, binding := range bindings {
				if binding.Identity == identity {
					found = true
					if binding.Lifecycle != controller.RouteDetaching {
						return errHTTPRouteChanged
					}
					break
				}
			}
			if !found {
				*route = *current
				return nil
			}
		}
		base := current.DeepCopy()
		if err := controller.RemoveRouteGatewayBinding(r.Config, current, bindings, identity); err != nil {
			return err
		}
		if !routeBindingMetadataEqual(base, current) {
			if err := r.patchRoute(ctx, current, base, false); err != nil {
				return client.IgnoreNotFound(err)
			}
		}
		*route = *current
		return nil
	})
}

func (r *Reconciler) applyRouteBinding(route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) error {
	identity := controller.RouteIdentity(r.Config, gateway, route)
	bindings, err := controller.StoredRouteGatewayBindings(r.Config, route)
	if err != nil {
		return err
	}
	port, _ := strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
	binding := controller.RouteGatewayBinding{Identity: identity, Lifecycle: controller.RouteAttached, ExplicitScope: true}
	for _, existing := range bindings {
		if existing.Identity == identity && existing.Anchor == port {
			binding.Anchor = existing.Anchor
		}
	}
	if controller.GatewayListenerScope(r.Config, gateway) == gatewayv1.HTTPProtocolType {
		binding.Anchor = port
	}
	return controller.ApplyRouteGatewayBinding(r.Config, route, bindings, binding)
}

func (r *Reconciler) clearRouteBinding(route *gatewayv1.HTTPRoute) {
	controller.ClearRouteBinding(r.Config, route)
}

func routeBindingMetadataEqual(left, right *gatewayv1.HTTPRoute) bool {
	return reflect.DeepEqual(left.Annotations, right.Annotations) && reflect.DeepEqual(left.Finalizers, right.Finalizers)
}

func sameHTTPRouteRevision(route *gatewayv1.HTTPRoute, uid types.UID, generation int64, isDeleting bool) bool {
	currentIsDeleting := !route.DeletionTimestamp.IsZero()
	return route.UID == uid && route.Generation == generation && currentIsDeleting == isDeleting
}

func (r *Reconciler) storedRouteIdentity(route *gatewayv1.HTTPRoute) (cloud.Identity, bool, error) {
	return controller.StoredRouteIdentity(r.Config, route)
}
