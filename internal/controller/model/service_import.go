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
	"fmt"
	"net/netip"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
)

const (
	ServiceImportGroup = mcs.GroupName
	ServiceImportKind  = "ServiceImport"
)

// ServiceImportObservation records a live read for an authorized import.
type ServiceImportObservation struct {
	Key            types.NamespacedName
	UID            types.UID
	Generation     int64
	Spec           *mcs.ServiceImportSpec
	SourceClusters []mcs.ClusterStatus
	EndpointSlices []discoveryv1.EndpointSlice
	ReadyIssue     *Diagnostic
	ReadIssue      *Diagnostic
	RetryRead      bool
}

// ServiceImportReadiness reports an explicit adverse condition for the current
// spec. MCS providers may omit Ready or leave observedGeneration unset.
func ServiceImportReadiness(backend *mcs.ServiceImport) *Diagnostic {
	ready := meta.FindStatusCondition(backend.Status.Conditions, string(mcs.ServiceImportConditionReady))
	if ready == nil || ready.Status == metav1.ConditionTrue ||
		ready.ObservedGeneration != 0 && ready.ObservedGeneration != backend.Generation {
		return nil
	}
	if ready.Reason == string(mcs.ServiceImportReasonIPFamilyNotSupported) {
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "The MCS provider cannot import the Service with the requested IP families")
	}
	return backendDiagnostic(gatewayv1.RouteReasonPending, "The MCS provider reports that the ServiceImport is not ready")
}

func ServiceImportReference(namespace string, reference gatewayv1.BackendObjectReference) (types.NamespacedName, bool) {
	if ptr.Deref(reference.Group, "") != ServiceImportGroup || ptr.Deref(reference.Kind, "Service") != ServiceImportKind {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{
		Namespace: string(ptr.Deref(reference.Namespace, gatewayv1.Namespace(namespace))),
		Name:      string(reference.Name),
	}, true
}

func ServiceImportReferenceAllowed(kind gatewayv1.Kind, namespace string, target types.NamespacedName, grants []gatewayv1.ReferenceGrant) bool {
	return backendTargetAllowed(gatewayv1.GroupName, kind, namespace, ServiceImportGroup, ServiceImportKind, target, grants)
}

// ServiceImportEndpointSliceKey uses the association label even on incomplete
// slices so that consumers are notified when a slice gains or loses provenance.
func ServiceImportEndpointSliceKey(slice *discoveryv1.EndpointSlice) types.NamespacedName {
	return types.NamespacedName{Namespace: slice.Namespace, Name: slice.Labels[mcs.LabelServiceName]}
}

func isMCSEndpointSlice(slice *discoveryv1.EndpointSlice) bool {
	return slice.Labels[mcs.LabelServiceName] != "" && slice.Labels[mcs.LabelSourceCluster] != "" &&
		hasServiceImportProviderManager(slice)
}

func hasServiceImportProviderManager(slice *discoveryv1.EndpointSlice) bool {
	switch slice.Labels[discoveryv1.LabelManagedBy] {
	case "", "endpointslice-controller.k8s.io", "endpointslicemirroring-controller.k8s.io":
		return false
	default:
		return true
	}
}

func hasServiceImportOwner(slice *discoveryv1.EndpointSlice, name string) bool {
	return name != "" && slices.ContainsFunc(slice.OwnerReferences, func(owner metav1.OwnerReference) bool {
		return owner.Name == name && isServiceImportOwner(owner)
	})
}

func isServiceImportOwner(owner metav1.OwnerReference) bool {
	return owner.Kind == ServiceImportKind && strings.HasPrefix(owner.APIVersion, ServiceImportGroup+"/")
}

func resolveServiceImport(route *gatewayv1.HTTPRoute, group BackendGroup, key types.NamespacedName, input Input) BackendGroup {
	group.ServiceImport = key
	kind := HTTPProfileRouteKind(route)
	if !ServiceImportReferenceAllowed(kind, route.Namespace, key, input.ReferenceGrants) {
		group.Authorization = BackendAuthorizationDenied
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonRefNotPermitted,
			fmt.Sprintf("backend ServiceImport %s is not permitted by a ReferenceGrant", key))
		return group
	}
	group.Authorization = BackendAuthorizationAllowed
	for _, observation := range input.ServiceImports {
		if observation.Key != key {
			continue
		}
		group.Port, group.ServiceImportDestinations, group.Diagnostic = observation.Destinations(group.Reference.Port, kind, input.BackendH2C)
		if group.Diagnostic == nil {
			group.State = BackendResolved
		}
		return group
	}
	group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend ServiceImport %s was not found", key))
	return group
}

// Destinations resolves the backend reference independently of member availability.
func (observation ServiceImportObservation) Destinations(selected *gatewayv1.PortNumber, kind gatewayv1.Kind, backendH2C bool) (*corev1.ServicePort, []netip.AddrPort, *Diagnostic) {
	if observation.ReadIssue != nil {
		return nil, nil, observation.ReadIssue
	}
	if observation.ReadyIssue != nil {
		return nil, nil, observation.ReadyIssue
	}
	spec := observation.Spec
	if spec == nil {
		return nil, nil, backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend ServiceImport %s was not found", observation.Key))
	}
	if spec.Type != mcs.ClusterSetIP && spec.Type != mcs.Headless {
		return nil, nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport must use ClusterSetIP or Headless")
	}
	protocol := corev1.ProtocolTCP
	if kind == "UDPRoute" {
		protocol = corev1.ProtocolUDP
	}
	port, diagnostic := serviceImportPort(spec, selected, protocol)
	if diagnostic != nil {
		return nil, nil, diagnostic
	}
	if kind == GRPCRouteKind && (!backendH2C || !ServicePortUsesH2C(port)) {
		return nil, nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "GRPCRoute requires kubernetes.io/h2c and enabled h2c transport for a ServiceImport")
	}
	if (kind == HTTPRouteKind || kind == GRPCRouteKind) && !supportsHTTPAppProtocol(port, backendH2C, false) {
		return nil, nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "backend ServiceImport appProtocol is not supported for HTTP forwarding")
	}
	if spec.Type == mcs.Headless {
		return port, observation.headlessDestinations(port), nil
	}
	ipv4, diagnostic := serviceImportVIP(spec.IPs)
	if diagnostic != nil {
		return nil, nil, diagnostic
	}
	return port, []netip.AddrPort{netip.AddrPortFrom(ipv4, uint16(port.Port))}, nil
}

func serviceImportVIP(ips []string) (netip.Addr, *Diagnostic) {
	var ipv4 netip.Addr
	for _, ip := range ips {
		address, err := netip.ParseAddr(ip)
		if err != nil {
			return netip.Addr{}, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport publishes an invalid ClusterSet IP")
		}
		if !address.Is4() {
			continue
		}
		if !serviceImportIPv4(address) {
			return netip.Addr{}, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires a unicast IPv4 ClusterSet IP")
		}
		if ipv4.IsValid() && ipv4 != address {
			return netip.Addr{}, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport publishes multiple IPv4 ClusterSet IPs")
		}
		ipv4 = address
	}
	if !ipv4.IsValid() {
		return netip.Addr{}, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires a unicast IPv4 ClusterSet IP")
	}
	return ipv4, nil
}

func (observation ServiceImportObservation) headlessDestinations(port *corev1.ServicePort) []netip.AddrPort {
	if families := observation.Spec.IPFamilies; len(families) != 0 && !slices.Contains(families, corev1.IPv4Protocol) {
		return nil
	}
	available := make(map[netip.AddrPort]bool)
	for i := range observation.EndpointSlices {
		slice := &observation.EndpointSlices[i]
		if slice.AddressType != discoveryv1.AddressTypeIPv4 || !slice.DeletionTimestamp.IsZero() || !observation.matchesEndpointSlice(slice) {
			continue
		}
		destinationPort := EndpointSlicePort(port, slice.Ports)
		if destinationPort < 1 || destinationPort > 65535 {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			ready := ptr.Deref(endpoint.Conditions.Ready, true) && ptr.Deref(endpoint.Conditions.Serving, true) && !ptr.Deref(endpoint.Conditions.Terminating, false)
			for _, value := range endpoint.Addresses {
				address, err := netip.ParseAddr(value)
				if err != nil || !serviceImportIPv4(address) {
					continue
				}
				key := netip.AddrPortFrom(address, uint16(destinationPort))
				previous, seen := available[key]
				available[key] = ready && (!seen || previous)
			}
		}
	}
	var destinations []netip.AddrPort
	for address, ready := range available {
		if ready {
			destinations = append(destinations, address)
		}
	}
	slices.SortFunc(destinations, netip.AddrPort.Compare)
	return destinations
}

func (observation ServiceImportObservation) matchesEndpointSlice(slice *discoveryv1.EndpointSlice) bool {
	if ServiceImportEndpointSliceKey(slice) != observation.Key || !isMCSEndpointSlice(slice) {
		return false
	}
	cluster := mcs.ClusterStatus{Cluster: slice.Labels[mcs.LabelSourceCluster]}
	if len(observation.SourceClusters) != 0 && !slices.Contains(observation.SourceClusters, cluster) {
		return false
	}
	for _, owner := range slice.OwnerReferences {
		if isServiceImportOwner(owner) && (owner.Name != observation.Key.Name || owner.UID != observation.UID) {
			return false
		}
	}
	return true
}

func serviceImportIPv4(address netip.Addr) bool {
	return address.Is4() && address.IsGlobalUnicast() && !address.IsLoopback() && !address.IsLinkLocalUnicast()
}

func serviceImportPort(spec *mcs.ServiceImportSpec, selected *gatewayv1.PortNumber, protocol corev1.Protocol) (*corev1.ServicePort, *Diagnostic) {
	number := int32(ptr.Deref(selected, 0))
	if number < 1 || number > 65535 {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires a port between 1 and 65535")
	}
	var port *corev1.ServicePort
	var unsupportedProtocol bool
	for _, candidate := range spec.Ports {
		if candidate.Port != number {
			continue
		}
		candidateProtocol := candidate.Protocol
		if candidateProtocol == "" {
			candidateProtocol = corev1.ProtocolTCP
		}
		if candidateProtocol != protocol {
			unsupportedProtocol = true
			continue
		}
		if port != nil {
			return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, fmt.Sprintf("backend ServiceImport publishes more than one %s port with the requested number", protocol))
		}
		port = &corev1.ServicePort{Port: number, Name: candidate.Name, Protocol: protocol, AppProtocol: candidate.AppProtocol}
	}
	if port != nil {
		return port, nil
	}
	if unsupportedProtocol {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("backend ServiceImport port must use %s", protocol))
	}
	return nil, backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, "backend ServiceImport does not expose the referenced port")
}
