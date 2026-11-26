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
	"net/netip"
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

// ValidateEndpointService checks the Service constraints for direct endpoint forwarding.
func ValidateEndpointService(service *corev1.Service) *model.Diagnostic {
	if service.Spec.Type != "" && service.Spec.Type != corev1.ServiceTypeClusterIP && service.Spec.Type != corev1.ServiceTypeNodePort {
		return unsupported("backend Service %s must be ClusterIP or NodePort", service.Name)
	}
	if len(service.Spec.IPFamilies) != 0 && !slices.Contains(service.Spec.IPFamilies, corev1.IPv4Protocol) {
		return unsupported("endpoint members require an IPv4 Service")
	}
	return nil
}

// ValidateNodePortService checks that Kubernetes allocated the selected NodePort.
func ValidateNodePortService(service *corev1.Service, port *corev1.ServicePort) *model.Diagnostic {
	if service.Spec.Type != corev1.ServiceTypeNodePort || port.NodePort == 0 {
		return unsupported("backend Service %s requires an allocated NodePort", service.Name)
	}
	return nil
}

// TransportMemberOptions constrain which NodePort destinations can serve a transport listener.
type TransportMemberOptions struct {
	MatchVIPFamily     bool
	RequireUnicastIPv4 bool
}

// TransportMembers resolves a TCP or UDP Service through the Gateway's frozen member mode.
// The caller authorizes the Service reference before reading it.
func TransportMembers(ctx context.Context, reader client.Reader, service *corev1.Service, port *gatewayv1.PortNumber, protocol corev1.Protocol, topology classconfig.Resolved, gatewayAddresses []gatewayv1.GatewayStatusAddress, options TransportMemberOptions) ([]cloud.Member, *model.Diagnostic, error) {
	if !service.DeletionTimestamp.IsZero() {
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: "The backend Service is being deleted"}, nil
	}
	selected, diagnostic := model.ResolveTransportServicePort(service, port, protocol)
	if diagnostic != nil {
		return nil, diagnostic, nil
	}
	mode := topology.Topology.MemberMode
	switch mode {
	case cloud.MemberModeNodePort:
		if diagnostic := ValidateNodePortService(service, selected); diagnostic != nil {
			return nil, diagnostic, nil
		}
	case cloud.MemberModeEndpoint:
		if diagnostic := ValidateEndpointService(service); diagnostic != nil {
			return nil, diagnostic, nil
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
		if options.MatchVIPFamily {
			// Count only workers of the VIP's address family before assigning
			// weighted shares. IPVS also omits real servers of other families.
			// A published floating IP and its private VIP are both IPv4.
			if len(gatewayAddresses) != 1 || gatewayAddresses[0].Type != nil && *gatewayAddresses[0].Type != gatewayv1.IPAddressType {
				return nil, pending("waiting for the %s Gateway IP address", protocol), nil
			}
			vip, err := netip.ParseAddr(gatewayAddresses[0].Value)
			if err != nil || vip.Is4In6() || vip.Zone() != "" {
				return nil, pending("waiting for a valid %s Gateway IP address", protocol), nil
			}
			family := discoveryv1.AddressTypeIPv6
			if vip.Is4() {
				family = discoveryv1.AddressTypeIPv4
			}
			endpoints.Items = slices.DeleteFunc(endpoints.Items, func(endpoint discoveryv1.EndpointSlice) bool { return endpoint.AddressType != family })
		}
		return nodePortMembers(ctx, reader, service, selected, topology.NodeAddressType, endpoints.Items, options.RequireUnicastIPv4)
	}
	members, diagnostic := EndpointMembers(service, model.ResolveServiceEndpoints(service, selected, endpoints.Items))
	return members, diagnostic, nil
}
