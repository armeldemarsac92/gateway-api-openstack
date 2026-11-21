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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// checkRouteBackendProfile rejects terminal Service profiles using the Gateway's
// frozen member mode. Missing or terminating dependencies are handled during lowering.
func (r *Reconciler) checkRouteBackendProfile(ctx context.Context, reader client.Reader, namespace string, backend gatewayv1.BackendObjectReference, gateway *gatewayv1.Gateway) error {
	if !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return err
	}
	key, _ := model.BackendServiceReference(namespace, backend)
	if key.Namespace != namespace {
		rejection, err := r.liveBackendReferenceRejection(ctx, namespace, key)
		if err != nil {
			return err
		}
		if rejection != nil {
			return rejection
		}
		reader = r.APIReader
	}
	var service corev1.Service
	if err := reader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get backend Service: %w", err)
	}
	_, err = servicePortForBackend(&service, *backend.Port, topology.Topology.MemberMode, r.Config.BackendH2C)
	var buildError *routeBuildError
	if errors.As(err, &buildError) && buildError.kind == routeErrorPending {
		return nil
	}
	return err
}

func (r *Reconciler) liveBackendReferenceRejection(ctx context.Context, namespace string, key types.NamespacedName) (*routeBuildError, error) {
	if key.Namespace == namespace {
		return nil, nil
	}
	if r.APIReader == nil {
		return nil, controller.ErrAPIReaderRequired
	}
	grants, err := graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
	if err != nil {
		return nil, err
	}
	return backendReferenceRejection(r.kind(), namespace, key, grants), nil
}

// liveRouteBackendReferenceRejection checks grants independently of the
// bundle version and Service readiness.
func (r *Reconciler) liveRouteBackendReferenceRejection(ctx context.Context, route *gatewayv1.HTTPRoute) (*routeBuildError, error) {
	observed := make(map[string][]gatewayv1.ReferenceGrant)
	var rejection *routeBuildError
	for _, rule := range route.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			if target, xbackend := model.XBackendReference(route.Namespace, ref.BackendObjectReference); xbackend && r.Config.XBackendEnabled() && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind && target.Namespace != route.Namespace {
				return &routeBuildError{kind: routeErrorRefNotPermitted, message: "XBackend must be in the same namespace as its HTTPRoute"}, nil
			}
			key, supported := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
			if !supported || key.Namespace == route.Namespace {
				continue
			}
			if r.APIReader == nil {
				return nil, controller.ErrAPIReaderRequired
			}
			grants, found := observed[key.Namespace]
			if !found {
				var err error
				grants, err = graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
				if err != nil {
					return nil, err
				}
				observed[key.Namespace] = grants
			}
			if denied := backendReferenceRejection(model.HTTPProfileRouteKind(route), route.Namespace, key, grants); denied != nil && rejection == nil {
				rejection = denied
			}
		}
	}
	return rejection, nil
}
