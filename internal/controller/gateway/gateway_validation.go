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
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
)

type gatewayValidationError struct {
	message       string
	gatewayReason gatewayv1.GatewayConditionReason
}

func hasListenerRequestFilters(gateway *gatewayv1.Gateway) bool {
	return slices.ContainsFunc(gateway.Spec.Listeners, func(l gatewayv1.Listener) bool {
		return l.Protocol == gatewayv1.HTTPProtocolType && l.Filters != nil && len(l.Filters.Requests) != 0
	})
}

func (e *gatewayValidationError) Error() string { return e.message }

func (r *Reconciler) getGatewayClass(ctx context.Context, gateway *gatewayv1.Gateway) (*gatewayv1.GatewayClass, bool, error) {
	var gatewayClass gatewayv1.GatewayClass
	key := types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}
	if err := r.Get(ctx, key, &gatewayClass); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get GatewayClass %q: %w", key.Name, err)
	}
	return &gatewayClass, gatewayClass.Spec.ControllerName == r.Config.ControllerName, nil
}

// Validate checks whether a Gateway can be represented by the current HTTP
// profile and returns its single supported listener.
func Validate(gateway *gatewayv1.Gateway) (*gatewayv1.Listener, error) {
	if len(gateway.Spec.Addresses) != 0 {
		return nil, &gatewayValidationError{
			message:       "spec.addresses is not supported",
			gatewayReason: gatewayv1.GatewayReasonUnsupportedAddress,
		}
	}
	if gateway.Spec.Infrastructure != nil {
		return nil, &gatewayValidationError{
			message:       "spec.infrastructure is not supported",
			gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
		}
	}
	if gateway.Spec.AllowedListeners != nil {
		return nil, &gatewayValidationError{
			message:       "spec.allowedListeners and ListenerSet attachment are not supported",
			gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
		}
	}
	if gateway.Spec.TLS != nil {
		return nil, &gatewayValidationError{
			message:       "gateway-wide spec.tls is not supported",
			gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
		}
	}
	listener, err := HTTPListener(gateway)
	if err != nil {
		return nil, err
	}
	if listener.TLS != nil {
		return nil, fmt.Errorf("only a plain HTTP listener is supported")
	}
	if hasListenerRequestFilters(gateway) {
		return nil, fmt.Errorf("listener request filters cannot be enforced by the stock Amphora HTTP profile")
	}
	if listener.Hostname != nil {
		if err := amphora.ValidateHostname(string(*listener.Hostname)); err != nil {
			return nil, fmt.Errorf("invalid listener hostname: %w", err)
		}
	}
	if listener.AllowedRoutes != nil && listener.AllowedRoutes.Namespaces != nil &&
		listener.AllowedRoutes.Namespaces.From != nil &&
		*listener.AllowedRoutes.Namespaces.From != gatewayv1.NamespacesFromSame {
		return nil, fmt.Errorf("only same-namespace HTTPRoutes are supported")
	}
	if listener.AllowedRoutes != nil && len(listener.AllowedRoutes.Kinds) != 0 {
		for _, kind := range listener.AllowedRoutes.Kinds {
			if !isHTTPRouteGroupKind(kind) {
				return nil, fmt.Errorf("only HTTPRoute is supported in listener allowedRoutes.kinds")
			}
		}
	}
	return listener, nil
}

// HTTPListener selects the sole HTTP listener when each listener has its own
// port. Validate checks whether the selected listener can be programmed.
func HTTPListener(gateway *gatewayv1.Gateway) (*gatewayv1.Listener, error) {
	var http *gatewayv1.Listener
	ports := make(map[gatewayv1.PortNumber]struct{}, len(gateway.Spec.Listeners))
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if _, exists := ports[listener.Port]; exists {
			return nil, fmt.Errorf("listeners sharing a port are not supported")
		}
		ports[listener.Port] = struct{}{}
		if listener.Protocol == gatewayv1.HTTPProtocolType {
			if http != nil {
				return nil, fmt.Errorf("only one HTTP listener is supported")
			}
			http = listener
		}
	}
	if http == nil {
		return nil, fmt.Errorf("a plain HTTP listener is required")
	}
	return http, nil
}

func listenerAllowsHTTPRoute(listener gatewayv1.Listener) bool {
	if listener.AllowedRoutes == nil || len(listener.AllowedRoutes.Kinds) == 0 {
		return true
	}
	for _, kind := range listener.AllowedRoutes.Kinds {
		if isHTTPRouteGroupKind(kind) {
			return true
		}
	}
	return false
}

func isHTTPRouteGroupKind(routeKind gatewayv1.RouteGroupKind) bool {
	group := gatewayv1.Group(gatewayv1.GroupName)
	if routeKind.Group != nil {
		group = *routeKind.Group
	}
	return group == gatewayv1.Group(gatewayv1.GroupName) && routeKind.Kind == gatewayv1.Kind("HTTPRoute")
}
