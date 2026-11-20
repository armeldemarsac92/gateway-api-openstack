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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type managedRouteParent struct {
	ref          gatewayv1.ParentReference
	gateway      *gatewayv1.Gateway
	gatewayClass *gatewayv1.GatewayClass
	listener     *gatewayv1.Listener
}

func (r *Reconciler) managedParents(ctx context.Context, route *gatewayv1.HTTPRoute) ([]managedRouteParent, error) {
	return r.managedParentsWithReader(ctx, r.Client, route)
}

func (r *Reconciler) managedParentsWithReader(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
) ([]managedRouteParent, error) {
	parents := make([]managedRouteParent, 0, len(route.Spec.ParentRefs))
	for _, parentRef := range route.Spec.ParentRefs {
		if !controller.IsGatewayParentRef(parentRef) {
			continue
		}
		namespace := route.Namespace
		if parentRef.Namespace != nil {
			namespace = string(*parentRef.Namespace)
		}
		var gateway gatewayv1.Gateway
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: string(parentRef.Name)}, &gateway); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get parent Gateway: %w", err)
		}
		var class gatewayv1.GatewayClass
		if err := reader.Get(ctx, types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get parent GatewayClass: %w", err)
		}
		if class.Spec.ControllerName != r.Config.ControllerName {
			continue
		}
		listener, _ := gatewaycontroller.HTTPListener(&gateway)
		parents = append(parents, managedRouteParent{ref: parentRef, gateway: &gateway, gatewayClass: &class, listener: listener})
	}
	return parents, nil
}

func validateRouteParent(route *gatewayv1.HTTPRoute, parent managedRouteParent) error {
	if parent.gateway.DeletionTimestamp != nil {
		return newRouteBuildError(routeErrorPending, "parent Gateway is being deleted")
	}
	if parent.gateway.Namespace != route.Namespace {
		return newRouteBuildError(routeErrorNotAllowed, "cross-namespace parentRefs are not supported")
	}
	if parent.listener == nil {
		return newRouteBuildError(routeErrorUnsupported, "parent Gateway must have one HTTP listener on its own port")
	}
	listener := parent.listener
	selectsListener := func(candidate gatewayv1.Listener) bool {
		return (parent.ref.SectionName == nil || *parent.ref.SectionName == candidate.Name) &&
			(parent.ref.Port == nil || *parent.ref.Port == candidate.Port)
	}
	if !selectsListener(*listener) {
		if slices.ContainsFunc(parent.gateway.Spec.Listeners, selectsListener) {
			return newRouteBuildError(routeErrorNotAllowed, "selected listener does not support HTTPRoute")
		}
		return newRouteBuildError(routeErrorNoMatchingParent, "parentRef does not select a Gateway listener")
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
		if listener.AllowedRoutes.Namespaces != nil && listener.AllowedRoutes.Namespaces.From != nil &&
			*listener.AllowedRoutes.Namespaces.From != gatewayv1.NamespacesFromSame {
			return newRouteBuildError(routeErrorNotAllowed, "listeners only allow same-namespace HTTPRoutes")
		}
	}
	return validateRouteHostnames(listener.Hostname, route.Spec.Hostnames)
}

func validateRouteHostnames(listener *gatewayv1.Hostname, hostnames []gatewayv1.Hostname) error {
	if len(hostnames) > 1 {
		return newRouteBuildError(routeErrorUnsupported, "at most one hostname is supported")
	}
	if listener != nil {
		if err := amphora.ValidateHostname(string(*listener)); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid listener hostname: %s", err)
		}
	}
	if len(hostnames) == 1 {
		if err := amphora.ValidateHostname(string(hostnames[0])); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid HTTPRoute hostname: %s", err)
		}
	}

	effective := model.IntersectHostnames(listener, hostnames)
	if len(effective) == 0 {
		return newRouteBuildError(routeErrorNoMatchingHostname, "HTTPRoute and listener hostnames do not intersect")
	}
	if effective[0] != "" {
		if _, err := amphora.HostnameRuleValue(effective[0]); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "%s", err)
		}
	}
	return nil
}
