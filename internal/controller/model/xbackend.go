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

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const XBackendKind = "XBackend"

// XBackendObservation contains the backend configuration and eligible endpoints
// used for cloud lowering. Status and unrelated Pod updates do not change it.
type XBackendObservation struct {
	Key        types.NamespacedName
	UID        types.UID
	Generation int64
	Spec       gatewayx.BackendSpec
	Endpoints  []Endpoint
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
	var spec *gatewayx.BackendSpec
	if backend != nil {
		spec = &backend.Spec
	}
	if diagnostic := CheckXBackendTarget(key, spec, input.MemberMode, input.BackendH2C); diagnostic != nil {
		group.Diagnostic = diagnostic
		return group
	}
	group.State = BackendResolved
	group.XBackendPort = int(backend.Spec.Port.Number)
	group.XBackendUsesH2C = ptr.Deref(backend.Spec.Protocol, "") == gatewayx.BackendProtocolH2C
	group.Endpoints = backend.Endpoints
	return group
}

// CheckXBackendTarget resolves an already authorized XBackend object reference.
func CheckXBackendTarget(key types.NamespacedName, spec *gatewayx.BackendSpec, mode cloud.MemberMode, backendH2C bool) *Diagnostic {
	if spec == nil {
		return backendDiagnostic(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("XBackend %s was not found", key))
	}
	return ValidateXBackend(*spec, mode, backendH2C)
}

// ValidateXBackend defines the XBackend configuration supported by the HTTPRoute profile.
func ValidateXBackend(spec gatewayx.BackendSpec, mode cloud.MemberMode, backendH2C bool) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	switch {
	case mode != cloud.MemberModeEndpoint:
		return backendDiagnostic(unsupported, "XBackend requires the Endpoint member mode")
	case spec.Type != gatewayx.BackendTypeEndpointSelector || spec.ExternalHostname != nil:
		return backendDiagnostic(unsupported, "only XBackend EndpointSelector is supported")
	case spec.EndpointSelector == nil || len(spec.EndpointSelector.MatchLabels) == 0:
		return backendDiagnostic(unsupported, "XBackend requires a nonempty endpointSelector.matchLabels")
	case spec.Port.Number < 1 || spec.Port.Number > 65535:
		return backendDiagnostic(unsupported, "XBackend must specify a port between 1 and 65535")
	case spec.SessionPersistence != nil:
		return backendDiagnostic(unsupported, "XBackend sessionPersistence is unsupported")
	case spec.TLS != nil && (spec.TLS.Mode != gatewayx.BackendTLSModeNone || spec.TLS.ClientCertificateRef != nil || spec.TLS.Validation != nil):
		return backendDiagnostic(unsupported, "XBackend TLS is supported only with mode None")
	}
	for key, value := range spec.EndpointSelector.MatchLabels {
		if len(validation.IsQualifiedName(string(key))) != 0 || len(validation.IsValidLabelValue(string(value))) != 0 {
			return backendDiagnostic(unsupported, "XBackend endpointSelector contains an invalid label")
		}
	}
	protocol := ptr.Deref(spec.Protocol, "")
	switch protocol {
	case "", gatewayx.BackendProtocolHTTP, gatewayx.BackendProtocolHTTP11:
		return nil
	case gatewayx.BackendProtocolH2C:
		if backendH2C {
			return nil
		}
	}
	return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("XBackend protocol %q is unsupported by the configured HTTP backend profile", protocol))
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
