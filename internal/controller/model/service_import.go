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

	corev1 "k8s.io/api/core/v1"
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
	Key        types.NamespacedName
	UID        types.UID
	Generation int64
	Spec       *mcs.ServiceImportSpec
	ReadyIssue *Diagnostic
	ReadIssue  *Diagnostic
	RetryRead  bool
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
		group.Port, group.ServiceImportVIP, group.Diagnostic = observation.Destination(group.Reference.Port, kind, input.BackendH2C)
		if group.Diagnostic == nil {
			group.State = BackendResolved
		}
		return group
	}
	group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend ServiceImport %s was not found", key))
	return group
}

// Destination selects the advertised IPv4 address and TCP port for a route's transport.
func (observation ServiceImportObservation) Destination(selected *gatewayv1.PortNumber, kind gatewayv1.Kind, backendH2C bool) (*corev1.ServicePort, string, *Diagnostic) {
	if observation.ReadIssue != nil {
		return nil, "", observation.ReadIssue
	}
	if observation.ReadyIssue != nil {
		return nil, "", observation.ReadyIssue
	}
	spec := observation.Spec
	if spec == nil {
		return nil, "", backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend ServiceImport %s was not found", observation.Key))
	}
	if spec.Type != mcs.ClusterSetIP {
		return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport must use ClusterSetIP")
	}
	port, diagnostic := serviceImportPort(spec, selected)
	if diagnostic != nil {
		return nil, "", diagnostic
	}
	if kind == GRPCRouteKind && (!backendH2C || !ServicePortUsesH2C(port)) {
		return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "GRPCRoute requires kubernetes.io/h2c and enabled h2c transport for a ServiceImport")
	}
	if (kind == HTTPRouteKind || kind == GRPCRouteKind) && !supportsHTTPAppProtocol(port, backendH2C, false) {
		return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "backend ServiceImport appProtocol is not supported for HTTP forwarding")
	}
	var ipv4 netip.Addr
	for _, ip := range spec.IPs {
		address, err := netip.ParseAddr(ip)
		if err != nil {
			return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport publishes an invalid ClusterSet IP")
		}
		if !address.Is4() {
			continue
		}
		if !address.IsGlobalUnicast() || address.IsLoopback() || address.IsLinkLocalUnicast() {
			return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires a unicast IPv4 ClusterSet IP")
		}
		if ipv4.IsValid() && ipv4 != address {
			return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport publishes multiple IPv4 ClusterSet IPs")
		}
		ipv4 = address
	}
	if !ipv4.IsValid() {
		return nil, "", backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires a unicast IPv4 ClusterSet IP")
	}
	return port, ipv4.String(), nil
}

func serviceImportPort(spec *mcs.ServiceImportSpec, selected *gatewayv1.PortNumber) (*corev1.ServicePort, *Diagnostic) {
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
		if candidate.Protocol != "" && candidate.Protocol != corev1.ProtocolTCP {
			unsupportedProtocol = true
			continue
		}
		if port != nil {
			return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport publishes more than one TCP port with the requested number")
		}
		port = &corev1.ServicePort{Port: number, Name: candidate.Name, Protocol: corev1.ProtocolTCP, AppProtocol: candidate.AppProtocol}
	}
	if port != nil {
		return port, nil
	}
	if unsupportedProtocol {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "backend ServiceImport port must use TCP")
	}
	return nil, backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, "backend ServiceImport does not expose the referenced port")
}
