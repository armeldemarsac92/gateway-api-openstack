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
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type managedRouteParent struct {
	ref          gatewayv1.ParentReference
	gateway      *gatewayv1.Gateway
	gatewayClass *gatewayv1.GatewayClass
	listener     *gatewayv1.Listener
}

type routeParentObservation struct {
	managedRouteParent
	managed bool
}

func (r *Reconciler) managedParents(ctx context.Context, route *gatewayv1.HTTPRoute) ([]managedRouteParent, error) {
	return r.managedParentsWithReader(ctx, r.Client, route)
}

func (r *Reconciler) managedParentsWithReader(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
) ([]managedRouteParent, error) {
	observations, err := r.observeRouteParents(ctx, reader, route)
	if err != nil {
		return nil, err
	}
	return managedParentsFromObservations(observations), nil
}

func (r *Reconciler) observeRouteParents(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute) ([]routeParentObservation, error) {
	parents := make([]routeParentObservation, 0, len(route.Spec.ParentRefs))
	for _, parentRef := range route.Spec.ParentRefs {
		if !controller.IsGatewayParentRef(parentRef) {
			continue
		}
		parent, err := r.observeRouteParent(ctx, reader, route.Namespace, parentRef)
		if err != nil {
			return nil, err
		}
		parents = append(parents, parent)
	}
	return parents, nil
}

func (r *Reconciler) observeRouteParent(ctx context.Context, reader client.Reader, namespace string, ref gatewayv1.ParentReference) (routeParentObservation, error) {
	parent := routeParentObservation{managedRouteParent: managedRouteParent{ref: ref}}
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	var gateway gatewayv1.Gateway
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}, &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return parent, nil
		}
		return routeParentObservation{}, fmt.Errorf("get parent Gateway: %w", err)
	}
	parent.gateway = &gateway
	var class gatewayv1.GatewayClass
	if err := reader.Get(ctx, types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return parent, nil
		}
		return routeParentObservation{}, fmt.Errorf("get parent GatewayClass: %w", err)
	}
	parent.gatewayClass = &class
	parent.managed = class.Spec.ControllerName == r.Config.ControllerName
	if parent.managed {
		parent.listener, _ = gatewaycontroller.HTTPListener(&gateway)
	}
	return parent, nil
}

func managedParentsFromObservations(observations []routeParentObservation) []managedRouteParent {
	parents := make([]managedRouteParent, 0, len(observations))
	for _, observation := range observations {
		if observation.managed {
			parents = append(parents, observation.managedRouteParent)
		}
	}
	return parents
}

// soleParentForManagedGateway checks the other references against live ownership
// while reusing an already validated Gateway from the current graph observation.
func (r *Reconciler) soleParentForManagedGateway(ctx context.Context, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) (*gatewayv1.ParentReference, error) {
	var selected *gatewayv1.ParentReference
	for i := range route.Spec.ParentRefs {
		ref := &route.Spec.ParentRefs[i]
		if !controller.IsGatewayParentRef(*ref) {
			continue
		}
		if routeTargetsGateway(route, *ref, gateway.Namespace, gateway.Name) {
			if selected != nil {
				return nil, nil
			}
			selected = ref
			continue
		}
		parent, err := r.observeRouteParent(ctx, r.APIReader, route.Namespace, *ref)
		if err != nil {
			return nil, err
		}
		if parent.managed {
			return nil, nil
		}
	}
	return selected, nil
}

func validateRouteParentListener(route *gatewayv1.HTTPRoute, parent managedRouteParent) error {
	if parent.gateway.DeletionTimestamp != nil {
		return newRouteBuildError(routeErrorPending, "parent Gateway is being deleted")
	}
	if !routeTargetsGateway(route, parent.ref, parent.gateway.Namespace, parent.gateway.Name) {
		return newRouteBuildError(routeErrorNoMatchingParent, "parentRef does not target this Gateway")
	}
	listener := parent.listener
	selectsListener := func(candidate gatewayv1.Listener) bool {
		return (parent.ref.SectionName == nil || *parent.ref.SectionName == candidate.Name) &&
			(parent.ref.Port == nil || *parent.ref.Port == candidate.Port)
	}
	if listener == nil || !selectsListener(*listener) {
		if !slices.ContainsFunc(parent.gateway.Spec.Listeners, selectsListener) {
			return newRouteBuildError(routeErrorNoMatchingParent, "parentRef does not select a Gateway listener")
		}
		if listener == nil {
			return newRouteBuildError(routeErrorUnsupported, "parent Gateway must have one HTTP listener on its own port")
		}
		return newRouteBuildError(routeErrorNotAllowed, "selected listener does not support HTTPRoute")
	}
	if listener.Protocol != gatewayv1.HTTPProtocolType || listener.TLS != nil {
		return newRouteBuildError(routeErrorUnsupported, "parent listener is not a supported plain HTTP listener")
	}
	if listener.AllowedRoutes != nil {
		allowsHTTPRoute := len(listener.AllowedRoutes.Kinds) == 0
		for _, kind := range listener.AllowedRoutes.Kinds {
			group := gatewayv1.Group(gatewayv1.GroupName)
			if kind.Group != nil {
				group = *kind.Group
			}
			if group == gatewayv1.Group(gatewayv1.GroupName) && kind.Kind == gatewayv1.Kind("HTTPRoute") {
				allowsHTTPRoute = true
			}
		}
		if !allowsHTTPRoute {
			return newRouteBuildError(routeErrorNotAllowed, "listener allowedRoutes does not include HTTPRoute")
		}
	}
	return nil
}

func routeTargetsGateway(route *gatewayv1.HTTPRoute, parent gatewayv1.ParentReference, namespace, name string) bool {
	parentNamespace := route.Namespace
	if parent.Namespace != nil {
		parentNamespace = string(*parent.Namespace)
	}
	return controller.IsGatewayParentRef(parent) && parentNamespace == namespace && string(parent.Name) == name
}

func validateRouteAttachment(route *gatewayv1.HTTPRoute, parent managedRouteParent, namespaces []corev1.Namespace) error {
	if !model.AllowsRouteNamespace(parent.listener, parent.gateway.Namespace, route.Namespace, namespaces) {
		return newRouteBuildError(routeErrorNotAllowed, "listener allowedRoutes does not allow this HTTPRoute namespace")
	}
	return validateRouteHostnames(parent.listener.Hostname, route.Spec.Hostnames)
}

func readRouteNamespaces(ctx context.Context, reader client.Reader, listener *gatewayv1.Listener, routeNamespace string) ([]corev1.Namespace, error) {
	if !model.ObservesRouteNamespace(listener) {
		return nil, nil
	}
	if reader == nil {
		return nil, controller.ErrAPIReaderRequired
	}
	namespace, err := graph.ReadRouteNamespace(ctx, reader, routeNamespace)
	if err != nil || namespace == nil {
		return nil, err
	}
	return []corev1.Namespace{*namespace}, nil
}

func boundRouteAttachmentRejection(route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, namespaces []corev1.Namespace) error {
	rejection := newRouteBuildError(routeErrorNoMatchingParent, "HTTPRoute no longer targets the bound Gateway")
	for _, ref := range route.Spec.ParentRefs {
		if !routeTargetsGateway(route, ref, gateway.Namespace, gateway.Name) {
			continue
		}
		parent := managedRouteParent{ref: ref, gateway: gateway, listener: listener}
		rejection = validateRouteParentListener(route, parent)
		if rejection == nil {
			rejection = validateRouteAttachment(route, parent, namespaces)
		}
		if rejection == nil {
			return nil
		}
	}
	return rejection
}

// liveBoundRouteAttachmentRejection observes the authorization for established
// forwarding independently of GatewayClass and bundle readiness.
func (r *Reconciler) liveBoundRouteAttachmentRejection(ctx context.Context, route *gatewayv1.HTTPRoute, identity cloud.Identity) (rejection, readErr error) {
	if !slices.ContainsFunc(route.Spec.ParentRefs, func(ref gatewayv1.ParentReference) bool {
		return routeTargetsGateway(route, ref, identity.GatewayNamespace, identity.GatewayName)
	}) {
		return newRouteBuildError(routeErrorNoMatchingParent, "HTTPRoute no longer targets the bound Gateway"), nil
	}
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: identity.GatewayNamespace, Name: identity.GatewayName}, &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return newRouteBuildError(routeErrorNoMatchingParent, "the bound Gateway no longer exists"), nil
		}
		return nil, fmt.Errorf("get bound Gateway attachment policy: %w", err)
	}
	if string(gateway.UID) != identity.GatewayUID {
		return newRouteBuildError(routeErrorNoMatchingParent, "the bound Gateway was replaced"), nil
	}
	listener, _ := gatewaycontroller.HTTPListener(&gateway)
	namespaces, err := readRouteNamespaces(ctx, r.APIReader, listener, route.Namespace)
	if err != nil {
		return nil, err
	}
	return boundRouteAttachmentRejection(route, &gateway, listener, namespaces), nil
}

func validateRouteHostnames(listener *gatewayv1.Hostname, hostnames []gatewayv1.Hostname) error {
	if listener != nil {
		if err := amphora.ValidateHostname(string(*listener)); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid listener hostname: %s", err)
		}
	}
	for _, hostname := range hostnames {
		if err := amphora.ValidateHostname(string(hostname)); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid HTTPRoute hostname: %s", err)
		}
	}

	effective := model.IntersectHostnames(listener, hostnames)
	if len(effective) == 0 {
		return newRouteBuildError(routeErrorNoMatchingHostname, "HTTPRoute and listener hostnames do not intersect")
	}
	if len(effective) > 1 {
		return newRouteBuildError(routeErrorUnsupported, "at most one effective hostname is supported per HTTPRoute")
	}
	if effective[0] != "" {
		if _, err := amphora.HostnameRuleValue(effective[0]); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "%s", err)
		}
	}
	return nil
}
