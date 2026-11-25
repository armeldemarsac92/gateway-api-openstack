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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

const XBackendKind = "XBackend"

// XBackendProfile describes the transport and topology permitted for this Gateway.
type XBackendProfile struct {
	MemberMode        cloud.MemberMode
	H2C               bool
	ServerTLS         bool
	ExternalHostname  bool
	ServiceDNSDomains []string
}

// XBackendObservation contains the backend configuration and eligible endpoints
// used for cloud lowering. Status and unrelated Pod updates do not change it.
type XBackendObservation struct {
	Key        types.NamespacedName
	UID        types.UID
	Generation int64
	Spec       gatewayx.BackendSpec
	Endpoints  []Endpoint
	TLS        backendtls.ValidationResult
	ReadIssue  *Diagnostic
}

// XBackendTLSCAKey identifies the trust input of an XBackend using ServerOnly.
func XBackendTLSCAKey(backend *gatewayx.XBackend, bundlesEnabled bool) (types.NamespacedName, bool) {
	tls := backend.Spec.TLS
	if tls == nil || tls.Mode != gatewayx.BackendTLSModeServerOnly || tls.Validation == nil {
		return types.NamespacedName{}, false
	}
	return backendtls.ValidationCAKey(backend.Namespace, *tls.Validation, bundlesEnabled)
}

func XBackendRequiresTLS(spec gatewayx.BackendSpec) bool {
	return spec.TLS != nil && spec.TLS.Mode != gatewayx.BackendTLSModeNone
}

func XBackendReference(namespace string, reference gatewayv1.BackendObjectReference) (types.NamespacedName, bool) {
	if ptr.Deref(reference.Group, "") != gatewayx.GroupName || ptr.Deref(reference.Kind, "") != XBackendKind {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{
		Namespace: string(ptr.Deref(reference.Namespace, gatewayv1.Namespace(namespace))),
		Name:      string(reference.Name),
	}, true
}

// LocalXBackendReferences lists the XBackends whose state may affect this HTTPRoute.
func LocalXBackendReferences(route *gatewayv1.HTTPRoute) []types.NamespacedName {
	var keys []types.NamespacedName
	for _, rule := range route.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			if key, supported := XBackendReference(route.Namespace, ref.BackendObjectReference); supported && key.Namespace == route.Namespace && ref.Port == nil {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func resolveXBackend(route *gatewayv1.HTTPRoute, group BackendGroup, key types.NamespacedName, input Input) BackendGroup {
	if group.Reference.Port != nil {
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "backendRef.port must be omitted for XBackend")
		return group
	}
	group.XBackend = key
	if key.Namespace != route.Namespace {
		group.Authorization = BackendAuthorizationDenied
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonRefNotPermitted, "XBackend must be in the same namespace as its HTTPRoute")
		return group
	}
	group.Authorization = BackendAuthorizationAllowed
	var backend *XBackendObservation
	for i := range input.XBackends {
		if input.XBackends[i].Key == key {
			backend = &input.XBackends[i]
			break
		}
	}
	if backend != nil {
		group.XBackendTLSRequired = XBackendRequiresTLS(backend.Spec) || backend.ReadIssue != nil
		if backend.Spec.Port.Number >= 1 && backend.Spec.Port.Number <= 65535 {
			group.XBackendPort = int(backend.Spec.Port.Number)
		}
	}
	if diagnostic := CheckXBackendObservation(key, backend, input.XBackendProfile); diagnostic != nil {
		group.Diagnostic = diagnostic
		return group
	}
	spec := &backend.Spec
	group.XBackendRoutedExternal = spec.Type == gatewayx.BackendTypeExternalHostname
	group.XBackendTLS = backend.TLS.Validation
	group.State = BackendResolved
	group.XBackendUsesH2C = ptr.Deref(spec.Protocol, "") == gatewayx.BackendProtocolH2C
	group.Endpoints = backend.Endpoints
	return group
}

func xBackendTLSReferenceDiagnostic(tls backendtls.ValidationResult) *Diagnostic {
	switch {
	case tls.Pending:
		return backendDiagnostic(gatewayv1.RouteReasonPending, tls.Message)
	case tls.RefReason != "":
		return backendDiagnostic(gatewayv1.RouteConditionReason(tls.RefReason), tls.Message)
	case tls.Message != "":
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, tls.Message)
	default:
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "XBackend TLS validation could not be resolved")
	}
}

// CheckXBackendObservation resolves an already authorized XBackend reference
// using its live configuration and trust dependencies.
func CheckXBackendObservation(key types.NamespacedName, backend *XBackendObservation, profile XBackendProfile) *Diagnostic {
	if backend == nil {
		return backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("XBackend %s was not found", key))
	}
	if backend.ReadIssue != nil {
		return backend.ReadIssue
	}
	if diagnostic := ValidateXBackend(backend.Spec, profile); diagnostic != nil {
		return diagnostic
	}
	if XBackendRequiresTLS(backend.Spec) && backend.TLS.Validation == nil {
		return xBackendTLSReferenceDiagnostic(backend.TLS)
	}
	return nil
}

// ValidateXBackend defines the XBackend configuration supported by the HTTPRoute profile.
func ValidateXBackend(spec gatewayx.BackendSpec, profile XBackendProfile) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	switch {
	case profile.MemberMode != cloud.MemberModeEndpoint:
		return backendDiagnostic(unsupported, "XBackend requires Endpoint member mode with Unmanaged backend security")
	case spec.Port.Number < 1 || spec.Port.Number > 65535:
		return backendDiagnostic(unsupported, "XBackend must specify a port between 1 and 65535")
	case spec.SessionPersistence != nil:
		return backendDiagnostic(unsupported, "XBackend sessionPersistence is unsupported")
	}
	switch spec.Type {
	case gatewayx.BackendTypeEndpointSelector:
		if spec.ExternalHostname != nil || spec.EndpointSelector == nil || len(spec.EndpointSelector.MatchLabels) == 0 {
			return backendDiagnostic(unsupported, "XBackend EndpointSelector requires a nonempty endpointSelector.matchLabels")
		}
		for key, value := range spec.EndpointSelector.MatchLabels {
			if len(validation.IsQualifiedName(string(key))) != 0 || len(validation.IsValidLabelValue(string(value))) != 0 {
				return backendDiagnostic(unsupported, "XBackend endpointSelector contains an invalid label")
			}
		}
	case gatewayx.BackendTypeExternalHostname:
		if !profile.ExternalHostname {
			return backendDiagnostic(unsupported, "XBackend ExternalHostname requires operator opt-in for this GatewayClass")
		}
		if spec.EndpointSelector != nil || spec.ExternalHostname == nil {
			return backendDiagnostic(unsupported, "XBackend ExternalHostname requires externalHostname")
		}
		if !validExternalHostname(string(spec.ExternalHostname.Hostname), profile.ServiceDNSDomains) {
			return backendDiagnostic(unsupported, "XBackend ExternalHostname must be a DNS hostname outside the Kubernetes Service domains")
		}
	default:
		return backendDiagnostic(unsupported, "XBackend type is unsupported")
	}
	protocol := ptr.Deref(spec.Protocol, "")
	if tls := spec.TLS; tls != nil {
		switch tls.Mode {
		case gatewayx.BackendTLSModeNone:
			if tls.Validation != nil || tls.ClientCertificateRef != nil {
				return backendDiagnostic(unsupported, "XBackend TLS mode None does not accept validation or a client certificate")
			}
		case gatewayx.BackendTLSModeServerOnly:
			if !profile.ServerTLS {
				return backendDiagnostic(unsupported, "XBackend TLS mode ServerOnly requires the controller's backend TLS opt-in")
			}
			if tls.Validation == nil || !backendtls.IsPreciseHostname(string(tls.Validation.Hostname)) || tls.ClientCertificateRef != nil {
				return backendDiagnostic(unsupported, "XBackend TLS mode ServerOnly requires validation.hostname to be a precise DNS hostname and no client certificate reference")
			}
			if protocol != "" && protocol != gatewayx.BackendProtocolHTTP && protocol != gatewayx.BackendProtocolHTTP11 {
				return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "XBackend TLS mode ServerOnly supports HTTP and HTTP11")
			}
		default:
			return backendDiagnostic(unsupported, fmt.Sprintf("XBackend TLS mode %q is unsupported", tls.Mode))
		}
	}
	switch protocol {
	case "", gatewayx.BackendProtocolHTTP, gatewayx.BackendProtocolHTTP11:
		return nil
	case gatewayx.BackendProtocolH2C:
		if spec.Type == gatewayx.BackendTypeEndpointSelector && profile.H2C {
			return nil
		}
	}
	return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("XBackend protocol %q is unsupported by the configured HTTP backend profile", protocol))
}

func validExternalHostname(hostname string, serviceDomains []string) bool {
	if strings.HasSuffix(hostname, ".") ||
		len(validation.IsFullyQualifiedDomainName(field.NewPath("hostname"), hostname)) != 0 {
		return false
	}
	// Reject numeric address forms, including abbreviated or zero-padded IPv4.
	if lastLabel := hostname[strings.LastIndexByte(hostname, '.')+1:]; strings.Trim(lastLabel, "0123456789") == "" {
		return false
	}
	return !slices.ContainsFunc(serviceDomains, func(domain string) bool {
		return hostnameInDNSDomain(hostname, domain)
	}) && !hostnameInDNSDomain(hostname, "cluster.local")
}

func hostnameInDNSDomain(hostname, domain string) bool {
	return hostname == domain || strings.HasSuffix(hostname, "."+domain)
}

// XBackendSelectsLabels applies the nonempty AND of matchLabels.
func XBackendSelectsLabels(spec gatewayx.BackendSpec, labels map[string]string) bool {
	if spec.Type != gatewayx.BackendTypeEndpointSelector || spec.EndpointSelector == nil || len(spec.EndpointSelector.MatchLabels) == 0 {
		return false
	}
	for key, want := range spec.EndpointSelector.MatchLabels {
		if value, present := labels[string(key)]; !present || value != string(want) {
			return false
		}
	}
	return true
}

func XBackendPodReady(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp.IsZero() && !pod.Spec.HostNetwork &&
		pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded &&
		slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
			return condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
		})
}

func ResolveXBackendEndpoints(backend *gatewayx.XBackend, pods []corev1.Pod) []Endpoint {
	selected := map[netip.Addr]struct{}{}
	add := func(address string) {
		ip, err := netip.ParseAddr(address)
		if err == nil && ip.Is4() && ip.IsGlobalUnicast() && !ip.IsLoopback() {
			selected[ip] = struct{}{}
		}
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Namespace != backend.Namespace || !XBackendPodReady(pod) || !XBackendSelectsLabels(backend.Spec, pod.Labels) {
			continue
		}
		if len(pod.Status.PodIPs) == 0 {
			add(pod.Status.PodIP)
		}
		for _, address := range pod.Status.PodIPs {
			add(address.IP)
		}
	}
	endpoints := make([]Endpoint, 0, len(selected))
	for address := range selected {
		endpoints = append(endpoints, Endpoint{
			Address: address.String(), Port: int32(backend.Spec.Port.Number), AddressType: discoveryv1.AddressTypeIPv4,
			Ready: true, Serving: true,
		})
	}
	slices.SortFunc(endpoints, compareEndpoints)
	return endpoints
}
