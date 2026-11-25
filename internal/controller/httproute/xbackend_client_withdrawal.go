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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// ObserveXBackendClientWithdrawals reads only bound HTTPRoutes and the current
// authorization of their client identities. A Service dependency may be unreadable.
func (r *Reconciler) ObserveXBackendClientWithdrawals(ctx context.Context, identity cloud.Identity, gateway *gatewayv1.Gateway) ([]cloud.XBackendClientWithdrawal, error) {
	if !r.Config.XBackendEnabled() || !controllerutil.ContainsFinalizer(gateway, r.Config.BackendCAFinalizer()) {
		return nil, nil
	}
	routes, err := r.forRouteKind(model.HTTPRouteKind).listLiveRoutes(ctx)
	if err != nil {
		return nil, err
	}
	type observation struct {
		reference string
		known     bool
	}
	observed := make(map[types.NamespacedName]observation)
	var targets []cloud.XBackendClientWithdrawal
	var readErrors error
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() || !controller.RouteClaimsGateway(r.Config, route, identity) ||
			!controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) {
			continue
		}
		binding, err := controller.StoredRouteGatewayBinding(r.Config, route, identity)
		if err != nil || binding.Lifecycle != controller.RouteAttached || binding.Identity != controller.RouteIdentity(r.Config, gateway, route) {
			continue
		}
		keys := model.LocalXBackendReferences(route)
		slices.SortFunc(keys, func(a, b types.NamespacedName) int { return cmp.Compare(a.Name, b.Name) })
		for _, key := range slices.Compact(keys) {
			certificate, checked := observed[key]
			if !checked {
				reference, readErr := r.xBackendClientReference(ctx, gateway.Spec.GatewayClassName, key)
				if readErr != nil {
					readErrors = errors.Join(readErrors, fmt.Errorf("observe XBackend %s client identity: %w", key, readErr))
				}
				certificate = observation{reference: reference, known: readErr == nil}
				observed[key] = certificate
			}
			if certificate.known {
				targets = append(targets, cloud.XBackendClientWithdrawal{
					Route: binding.Identity, BackendName: key.Name, AuthorizedReference: certificate.reference,
				})
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(targets, func(a, b cloud.XBackendClientWithdrawal) int {
		return cmp.Or(cmp.Compare(a.Route.RouteNamespace, b.Route.RouteNamespace), cmp.Compare(a.Route.RouteName, b.Route.RouteName),
			cmp.Compare(a.Route.RouteUID, b.Route.RouteUID), cmp.Compare(a.BackendName, b.BackendName))
	})
	return targets, readErrors
}

func (r *Reconciler) xBackendClientReference(ctx context.Context, class gatewayv1.ObjectName, key types.NamespacedName) (string, error) {
	var backend gatewayx.XBackend
	if err := r.APIReader.Get(ctx, key, &backend); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if !backend.DeletionTimestamp.IsZero() || backend.Spec.TLS == nil || backend.Spec.TLS.Mode != gatewayx.BackendTLSModeClientAndServer ||
		!r.Config.XBackendClientCertificate || r.XBackendClientCertificates == nil {
		return "", nil
	}
	reference, reason, _ := r.XBackendClientCertificates.ResolveXBackendClientCertificate(ctx, class, &backend)
	if reason != "" {
		return "", ctx.Err()
	}
	return reference, ctx.Err()
}
