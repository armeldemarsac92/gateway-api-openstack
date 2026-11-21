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

// Package routebackend selects OpenStack members from Kubernetes Service backends.
package routebackend

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func pending(format string, args ...any) *model.Diagnostic {
	return &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf(format, args...)}
}

func unsupported(format string, args ...any) *model.Diagnostic {
	return &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: fmt.Sprintf(format, args...)}
}

// TCPMembers resolves a TCP Service through the Gateway's frozen member mode.
// The caller authorizes the Service reference before reading it.
func TCPMembers(ctx context.Context, reader client.Reader, service *corev1.Service, port *gatewayv1.PortNumber, topology classconfig.Resolved) ([]cloud.Member, *model.Diagnostic, error) {
	if !service.DeletionTimestamp.IsZero() {
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: "The backend Service is being deleted"}, nil
	}
	selected, diagnostic := model.ResolveTCPServicePort(service, port)
	if diagnostic != nil {
		return nil, diagnostic, nil
	}
	mode := topology.Topology.MemberMode
	switch mode {
	case cloud.MemberModeNodePort:
		if service.Spec.Type != corev1.ServiceTypeNodePort || selected.NodePort == 0 {
			return nil, unsupported("backend Service %s requires an allocated NodePort", service.Name), nil
		}
	case cloud.MemberModeEndpoint:
		if service.Spec.Type != "" && service.Spec.Type != corev1.ServiceTypeClusterIP && service.Spec.Type != corev1.ServiceTypeNodePort {
			return nil, unsupported("backend Service %s must be ClusterIP or NodePort", service.Name), nil
		}
		if len(service.Spec.IPFamilies) != 0 && !slices.Contains(service.Spec.IPFamilies, corev1.IPv4Protocol) {
			return nil, unsupported("endpoint members require an IPv4 Service"), nil
		}
	default:
		return nil, unsupported("backend member mode is unsupported"), nil
	}
	var endpoints discoveryv1.EndpointSliceList
	if err := reader.List(ctx, &endpoints, client.InNamespace(service.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}); err != nil {
		return nil, nil, fmt.Errorf("list backend EndpointSlices: %w", err)
	}
	if endpoints.Continue != "" {
		return nil, nil, fmt.Errorf("incomplete backend EndpointSlice list")
	}
	if mode == cloud.MemberModeNodePort {
		return NodePortMembers(ctx, reader, service, selected, topology.NodeAddressType, endpoints.Items)
	}
	members, diagnostic := EndpointMembers(service, model.ResolveServiceEndpoints(service, selected, endpoints.Items))
	return members, diagnostic, nil
}
