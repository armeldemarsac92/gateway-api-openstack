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
func (r *Reconciler) checkRouteBackendProfile(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) error {
	if !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return err
	}
	backend := route.Spec.Rules[0].BackendRefs[0]
	key, _ := model.BackendServiceReference(route.Namespace, backend.BackendObjectReference)
	if key.Namespace != route.Namespace {
		rejection, err := r.liveBackendReferenceRejection(ctx, route.Namespace, key)
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
	_, err = servicePortForBackend(&service, *backend.Port, topology.Topology.MemberMode)
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
	return backendReferenceRejection(namespace, key, grants), nil
}

// liveRouteBackendReferenceRejection checks the grant independently of the
// bundle version and Service readiness. Cleanup needs only the single backend
// reference understood by the forwarding profile.
func (r *Reconciler) liveRouteBackendReferenceRejection(ctx context.Context, route *gatewayv1.HTTPRoute) (*routeBuildError, error) {
	if len(route.Spec.Rules) != 1 || len(route.Spec.Rules[0].BackendRefs) != 1 {
		return nil, nil
	}
	ref := route.Spec.Rules[0].BackendRefs[0].BackendObjectReference
	key, supported := model.BackendServiceReference(route.Namespace, ref)
	if !supported {
		return nil, nil
	}
	return r.liveBackendReferenceRejection(ctx, route.Namespace, key)
}
