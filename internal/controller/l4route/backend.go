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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type serviceReference struct {
	key      client.ObjectKey
	port     *gatewayv1.PortNumber
	disabled bool
}

func backendRejection(reason gatewayv1.RouteConditionReason, message string) *model.Diagnostic {
	return &model.Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs), Reason: string(reason), Message: message}
}

func (r *Reconciler) authorizedBackend(ctx context.Context, route *transportRoute) (serviceReference, *model.Diagnostic, error) {
	if len(route.backendRefs) != 1 {
		return serviceReference{}, backendRejection(gatewayv1.RouteReasonUnsupportedValue, fmt.Sprintf("%s forwarding requires one rule with exactly one Service backendRef", r.protocol())), nil
	}
	backend := route.backendRefs[0]
	key, supported := model.BackendServiceReference(route.GetNamespace(), backend.BackendObjectReference)
	if !supported {
		return serviceReference{}, backendRejection(gatewayv1.RouteReasonInvalidKind, fmt.Sprintf("the %s backendRef must refer to a core Service", r.kind())), nil
	}
	if key.Namespace != route.GetNamespace() {
		var grants gatewayv1.ReferenceGrantList
		if err := r.APIReader.List(ctx, &grants, client.InNamespace(key.Namespace)); err != nil {
			return serviceReference{}, nil, err
		}
		if grants.Continue != "" {
			return serviceReference{}, nil, fmt.Errorf("incomplete %s ReferenceGrant list", r.kind())
		}
		if !model.BackendReferenceAllowedFor(r.kind(), route.GetNamespace(), key, grants.Items) {
			return serviceReference{}, backendRejection(gatewayv1.RouteReasonRefNotPermitted, fmt.Sprintf("no ReferenceGrant permits this %s to use its backend Service", r.kind())), nil
		}
	}
	return serviceReference{key: key, port: backend.Port, disabled: backend.Weight != nil && *backend.Weight == 0}, nil, nil
}

func (r *Reconciler) backendService(ctx context.Context, ref serviceReference) (*corev1.Service, *model.Diagnostic, error) {
	var service corev1.Service
	err := r.APIReader.Get(ctx, ref.key, &service)
	if (err == nil && !service.DeletionTimestamp.IsZero()) || apierrors.IsNotFound(err) {
		return nil, backendRejection(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend Service %s was not found", ref.key)), nil
	}
	if err != nil {
		return nil, nil, err
	}
	if _, diagnostic := model.ResolveTransportServicePort(&service, ref.port, corev1.Protocol(r.protocol())); diagnostic != nil {
		return nil, diagnostic, nil
	}
	return &service, nil, nil
}
