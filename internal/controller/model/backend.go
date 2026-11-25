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

package model

import (
	"cmp"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ResolveBackends resolves each declared backend without redistributing its weight.
// Input objects are immutable observations. This function performs no I/O and
// neither selects physical members nor authorizes a new forwarding mode.
func ResolveBackends(route *gatewayv1.HTTPRoute, rule gatewayv1.HTTPRouteRule, input Input) []BackendGroup {
	groups := make([]BackendGroup, 0, len(rule.BackendRefs))
	for _, reference := range rule.BackendRefs {
		groups = append(groups, resolveBackend(route, reference, input))
	}
	return groups
}

func resolveBackend(route *gatewayv1.HTTPRoute, reference gatewayv1.HTTPBackendRef, input Input) BackendGroup {
	group := BackendGroup{
		Reference: *reference.DeepCopy(), Weight: ptr.Deref(reference.Weight, 1),
		Authorization: BackendAuthorizationNotChecked, State: BackendInvalid,
		Endpoints: []Endpoint{},
	}
	if key, xbackend := XBackendReference(route.Namespace, reference.BackendObjectReference); xbackend && input.EnableXBackend && HTTPProfileRouteKind(route) == HTTPRouteKind {
		return resolveXBackend(route, group, key, input)
	}
	serviceKey, supported := BackendServiceReference(route.Namespace, reference.BackendObjectReference)
	if !supported {
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonInvalidKind,
			fmt.Sprintf("backendRef group %q and kind %q are unsupported", ptr.Deref(reference.Group, ""), ptr.Deref(reference.Kind, "Service")))
		return group
	}
	group.Service = serviceKey
	if !BackendReferenceAllowedFor(HTTPProfileRouteKind(route), route.Namespace, group.Service, input.ReferenceGrants) {
		group.Authorization = BackendAuthorizationDenied
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonRefNotPermitted,
			fmt.Sprintf("backend Service %s is not permitted by a ReferenceGrant", group.Service))
		return group
	}
	group.Authorization = BackendAuthorizationAllowed
	service := findBackendService(group.Service, input.Services)
	if service == nil {
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend Service %s was not found", group.Service))
		return group
	}
	group.ServiceUID = service.UID
	if service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeExternalName {
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend Service must use ClusterIP or NodePort type")
		return group
	}
	selection := input.BackendTLSPolicies[ServicePortKey{Service: group.Service, Port: int32(ptr.Deref(reference.Port, 0))}]
	requiresTLS := selection.Winner != nil
	port, diagnostic := ResolveServicePortForBackendTLS(service, reference.Port, input.BackendH2C, requiresTLS)
	// Retain a selected policy even when gRPC cannot use the Service's protocol.
	grpc := HTTPProfileRouteKind(route) == GRPCRouteKind
	if grpc && diagnostic == nil {
		diagnostic = CheckGRPCBackendProtocol(port, requiresTLS)
	}
	if diagnostic == nil || grpc {
		group.BackendTLS = selection.Clone()
	}
	if diagnostic != nil {
		group.Diagnostic = diagnostic
		return group
	}
	group.Port = port.DeepCopy()
	group.Port.Protocol = corev1.ProtocolTCP
	group.State = BackendResolved
	group.Endpoints = ResolveServiceEndpoints(service, port, input.EndpointSlices)
	return group
}

// BackendServiceReference normalizes a core Service reference using its source namespace.
// Other groups and kinds remain unsupported rather than being interpreted as Services.
func BackendServiceReference(namespace string, reference gatewayv1.BackendObjectReference) (types.NamespacedName, bool) {
	if ptr.Deref(reference.Group, "") != "" || ptr.Deref(reference.Kind, "Service") != "Service" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{
		Namespace: string(ptr.Deref(reference.Namespace, gatewayv1.Namespace(namespace))),
		Name:      string(reference.Name),
	}, true
}

// BackendReferenceAllowed checks Service reference authorization against observed grants.
// It performs no reads and does not confer ownership of the referenced Service.
func BackendReferenceAllowed(namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) bool {
	return BackendReferenceAllowedFor("HTTPRoute", namespace, target, grants)
}

// BackendReferenceAllowedFor checks the exact source kind, namespace and Service target.
func BackendReferenceAllowedFor(kind gatewayv1.Kind, namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) bool {
	return BackendReferenceAllowedForGroup(gatewayv1.GroupName, kind, namespace, target, grants)
}

// BackendReferenceAllowedForGroup also accepts sources in the Experimental API group.
func BackendReferenceAllowedForGroup(group gatewayv1.Group, kind gatewayv1.Kind, namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) bool {
	if namespace == target.Namespace {
		return true
	}
	for _, grant := range grants {
		if grant.Namespace != target.Namespace || !grant.DeletionTimestamp.IsZero() {
			continue
		}
		fromAllowed := slices.ContainsFunc(grant.Spec.From, func(from gatewayv1.ReferenceGrantFrom) bool {
			return from.Group == group && from.Kind == kind && string(from.Namespace) == namespace
		})
		if !fromAllowed {
			continue
		}
		if slices.ContainsFunc(grant.Spec.To, func(to gatewayv1.ReferenceGrantTo) bool {
			return to.Group == "" && to.Kind == "Service" && (to.Name == nil || string(*to.Name) == target.Name)
		}) {
			return true
		}
	}
	return false
}

func findBackendService(key types.NamespacedName, services []corev1.Service) *corev1.Service {
	for index := range services {
		service := &services[index]
		if service.Namespace == key.Namespace && service.Name == key.Name && service.DeletionTimestamp.IsZero() {
			return service
		}
	}
	return nil
}

// ResolveServicePort resolves the referenced TCP port and HTTP appProtocol.
func ResolveServicePort(service *corev1.Service, selected *gatewayv1.PortNumber, backendH2C bool) (*corev1.ServicePort, *Diagnostic) {
	return ResolveServicePortForBackendTLS(service, selected, backendH2C, false)
}

// ResolveServicePortForBackendTLS also allows a port that explicitly requires
// HTTPS when the caller establishes that plaintext forwarding is prohibited.
func ResolveServicePortForBackendTLS(service *corev1.Service, selected *gatewayv1.PortNumber, backendH2C, backendTLS bool) (*corev1.ServicePort, *Diagnostic) {
	port, diagnostic := ResolveTCPServicePort(service, selected)
	if diagnostic == nil && port.AppProtocol != nil &&
		(!backendH2C || !ServicePortUsesH2C(port)) &&
		(!backendTLS || !strings.EqualFold(*port.AppProtocol, "https")) &&
		!slices.Contains([]string{"http", "kubernetes.io/http", "kubernetes.io/ws"}, *port.AppProtocol) {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "backend Service appProtocol is not supported for HTTP forwarding")
	}
	return port, diagnostic
}

// CheckGRPCBackendProtocol checks the transport against the Service port and
// the selected policy, even when the policy's CA is temporarily unavailable.
func CheckGRPCBackendProtocol(port *corev1.ServicePort, requiresTLS bool) *Diagnostic {
	if requiresTLS {
		if port.AppProtocol == nil || strings.EqualFold(*port.AppProtocol, "https") {
			return nil
		}
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "GRPCRoute cannot meet the backend Service's BackendTLSPolicy with its declared appProtocol")
	}
	if ServicePortUsesH2C(port) {
		return nil
	}
	return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "GRPCRoute requires kubernetes.io/h2c or a BackendTLSPolicy on the backend Service port")
}

// ServicePortUsesH2C reports whether the Service port explicitly selects cleartext HTTP/2.
func ServicePortUsesH2C(port *corev1.ServicePort) bool {
	return port != nil && ptr.Deref(port.AppProtocol, "") == "kubernetes.io/h2c"
}

// ResolveTCPServicePort selects a TCP port regardless of the application protocol it transports.
func ResolveTCPServicePort(service *corev1.Service, selected *gatewayv1.PortNumber) (*corev1.ServicePort, *Diagnostic) {
	return ResolveTransportServicePort(service, selected, corev1.ProtocolTCP)
}

// ResolveTransportServicePort selects the requested Service port and IP protocol.
// Kubernetes defaults a missing Service port protocol to TCP.
func ResolveTransportServicePort(service *corev1.Service, selected *gatewayv1.PortNumber, protocol corev1.Protocol) (*corev1.ServicePort, *Diagnostic) {
	var unsupportedProtocol bool
	for index := range service.Spec.Ports {
		port := &service.Spec.Ports[index]
		if selected == nil || port.Port != int32(*selected) {
			continue
		}
		if servicePortProtocol(port) != protocol {
			unsupportedProtocol = true
			continue
		}
		return port, nil
	}
	if unsupportedProtocol {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("backend Service port must use %s", protocol))
	}
	return nil, backendDiagnostic(gatewayv1.RouteReasonBackendNotFound,
		fmt.Sprintf("backend Service %s/%s does not expose the referenced port", service.Namespace, service.Name))
}

// ResolveServiceEndpoints identifies distinct endpoint observations for the selected Service port.
func ResolveServiceEndpoints(service *corev1.Service, servicePort *corev1.ServicePort, endpointSlices []discoveryv1.EndpointSlice) []Endpoint {
	observations := map[Endpoint]struct{}{}
	for _, endpointSlice := range endpointSlices {
		if !endpointSlice.DeletionTimestamp.IsZero() || endpointSlice.Namespace != service.Namespace || endpointSlice.Labels[discoveryv1.LabelServiceName] != service.Name ||
			!EndpointSliceOwnerMatchesService(&endpointSlice, service) {
			continue
		}
		if endpointSlice.AddressType != discoveryv1.AddressTypeIPv4 && endpointSlice.AddressType != discoveryv1.AddressTypeIPv6 {
			continue
		}
		if len(service.Spec.IPFamilies) != 0 && !slices.Contains(service.Spec.IPFamilies, corev1.IPFamily(endpointSlice.AddressType)) {
			continue
		}
		port := EndpointSlicePort(servicePort, endpointSlice.Ports)
		if port != 0 {
			collectEndpointObservations(endpointSlice, port, observations)
		}
	}
	endpoints := make([]Endpoint, 0, len(observations))
	for endpoint := range observations {
		endpoints = append(endpoints, endpoint)
	}
	slices.SortFunc(endpoints, compareEndpoints)
	return endpoints
}

// EndpointSliceOwnerMatchesService checks the identity of a controlling core
// Service when the backend Service has a UID. Other slices rely on the Service label.
func EndpointSliceOwnerMatchesService(slice *discoveryv1.EndpointSlice, service *corev1.Service) bool {
	owner := endpointSliceServiceOwner(slice)
	return service.UID == "" || owner == nil || (owner.UID == service.UID && owner.Name == service.Name)
}

// EndpointSliceServiceOwnerChanged compares the controller identity used to select members.
func EndpointSliceServiceOwnerChanged(oldSlice, newSlice *discoveryv1.EndpointSlice) bool {
	oldOwner, newOwner := endpointSliceServiceOwner(oldSlice), endpointSliceServiceOwner(newSlice)
	if oldOwner == nil || newOwner == nil {
		return oldOwner != newOwner
	}
	return oldOwner.UID != newOwner.UID || oldOwner.Name != newOwner.Name
}

func endpointSliceServiceOwner(slice *discoveryv1.EndpointSlice) *metav1.OwnerReference {
	owner := metav1.GetControllerOfNoCopy(slice)
	if owner == nil || owner.APIVersion != corev1.SchemeGroupVersion.String() || owner.Kind != "Service" {
		return nil
	}
	return owner
}

func servicePortProtocol(port *corev1.ServicePort) corev1.Protocol {
	if port.Protocol == "" {
		return corev1.ProtocolTCP
	}
	return port.Protocol
}

// EndpointSlicePort matches a ServicePort's name and transport protocol.
// A named targetPort may resolve to a different number in each EndpointSlice.
func EndpointSlicePort(servicePort *corev1.ServicePort, ports []discoveryv1.EndpointPort) int32 {
	protocol := servicePortProtocol(servicePort)
	for _, port := range ports {
		if ptr.Deref(port.Name, "") == servicePort.Name && ptr.Deref(port.Protocol, corev1.ProtocolTCP) == protocol && ptr.Deref(port.Port, 0) > 0 {
			return *port.Port
		}
	}
	return 0
}

func collectEndpointObservations(endpointSlice discoveryv1.EndpointSlice, port int32, observations map[Endpoint]struct{}) {
	for _, endpoint := range endpointSlice.Endpoints {
		for _, address := range endpoint.Addresses {
			ip, err := netip.ParseAddr(address)
			if err != nil {
				continue
			}
			ip = ip.Unmap()
			if ip.Is4() != (endpointSlice.AddressType == discoveryv1.AddressTypeIPv4) {
				continue
			}
			observations[Endpoint{
				Address: ip.String(), Port: port, AddressType: endpointSlice.AddressType,
				Ready: ptr.Deref(endpoint.Conditions.Ready, true), Serving: ptr.Deref(endpoint.Conditions.Serving, true),
				Terminating: ptr.Deref(endpoint.Conditions.Terminating, false),
			}] = struct{}{}
		}
	}
}

func compareEndpoints(left, right Endpoint) int {
	return cmp.Or(
		cmp.Compare(left.Address, right.Address), cmp.Compare(left.Port, right.Port), cmp.Compare(left.AddressType, right.AddressType),
		compareEndpointCondition(left.Ready, right.Ready), compareEndpointCondition(left.Serving, right.Serving),
		compareEndpointCondition(left.Terminating, right.Terminating),
	)
}

func compareEndpointCondition(left, right bool) int {
	if left == right {
		return 0
	}
	if left {
		return 1
	}
	return -1
}

func backendDiagnostic(reason gatewayv1.RouteConditionReason, message string) *Diagnostic {
	return &Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs), Reason: string(reason), Message: message}
}
