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

package gateway

import (
	"cmp"
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type gatewayValidationError struct {
	message          string
	gatewayReason    gatewayv1.GatewayConditionReason
	programmedReason gatewayv1.GatewayConditionReason
}

func hasListenerRequestFilters(config controller.Config, gateway *gatewayv1.Gateway) bool {
	sections, _ := listenerSectionsWithoutTCPConflicts(config, gateway)
	multipleTransport := hasMultipleTransportListeners(sections)
	return slices.ContainsFunc(sections, func(l *gatewayv1.Listener) bool {
		supportedProtocol := l.Protocol == gatewayv1.HTTPProtocolType || (isL4Listener(l.Protocol) && len(sections) == 1) || multipleTransport
		return supportedProtocol && l.Filters != nil && len(l.Filters.Requests) != 0
	})
}

func (e *gatewayValidationError) Error() string { return e.message }

func listenerProtocolSharesTCP(config controller.Config, listener gatewayv1.Listener) bool {
	switch listener.Protocol {
	case gatewayv1.HTTPProtocolType:
		return true
	case gatewayv1.HTTPSProtocolType:
		return config.OperatorHTTPS
	case gatewayv1.TLSProtocolType:
		return tlsListenerModeEnabled(config, &listener)
	default:
		return false
	}
}

func tcpListenerProtocolConflict(config controller.Config, listeners []gatewayv1.Listener, listener gatewayv1.Listener) bool {
	if listener.Protocol == gatewayv1.TCPProtocolType {
		return slices.ContainsFunc(listeners, func(peer gatewayv1.Listener) bool {
			return peer.Port == listener.Port && listenerProtocolSharesTCP(config, peer)
		})
	}
	return listenerProtocolSharesTCP(config, listener) && slices.ContainsFunc(listeners, func(peer gatewayv1.Listener) bool {
		return peer.Port == listener.Port && peer.Protocol == gatewayv1.TCPProtocolType
	})
}

// Keep pointers to the live Gateway's sections so route selectors and policy
// observations use the same listeners as the graph.
func listenerSectionsWithoutTCPConflicts(config controller.Config, gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, []string) {
	listeners := make([]*gatewayv1.Listener, 0, len(gateway.Spec.Listeners))
	var conflicts []string
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if tcpListenerProtocolConflict(config, gateway.Spec.Listeners, *listener) {
			conflicts = append(conflicts, string(listener.Name))
		} else {
			listeners = append(listeners, listener)
		}
	}
	return listeners, conflicts
}

func gatewayTCPProtocolConflictMessage(conflicts []string) string {
	return "TCP listeners share ports with HTTP, HTTPS or TLS listeners. Conflicted sections are " + strings.Join(conflicts, ", ")
}

func (r *Reconciler) getGatewayClass(ctx context.Context, gateway *gatewayv1.Gateway) (*gatewayv1.GatewayClass, bool, error) {
	var gatewayClass gatewayv1.GatewayClass
	key := types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}
	if err := r.Get(ctx, key, &gatewayClass); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get GatewayClass %q: %w", key.Name, err)
	}
	return &gatewayClass, gatewayClass.Spec.ControllerName == r.Config.ControllerName, nil
}

// Validate returns the primary listener supported by the stock Amphora profile.
// Use SupportedListeners to obtain all admitted listeners.
func Validate(gateway *gatewayv1.Gateway) (*gatewayv1.Listener, error) {
	listeners, err := SupportedListeners(gateway)
	if err != nil {
		return nil, err
	}
	return listeners[0], nil
}

// SupportedListeners returns the admitted listeners in ascending port order.
// Address assignment is evaluated separately from listener and route admission.
func SupportedListeners(gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, error) {
	return SupportedListenersForConfig(controller.Config{}, gateway)
}

// SupportedListenersForConfig also returns disabled HTTPS sections for withdrawal.
func SupportedListenersForConfig(config controller.Config, gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, error) {
	if infrastructure := gateway.Spec.Infrastructure; infrastructure != nil {
		if !config.GatewayInfrastructureMetadata {
			return nil, &gatewayValidationError{
				message:       "spec.infrastructure requires Gateway infrastructure metadata opt-in",
				gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
			}
		}
		if infrastructure.ParametersRef != nil {
			return nil, &gatewayValidationError{
				message:       "spec.infrastructure.parametersRef is not supported",
				gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
			}
		}
	}
	if model.UnsupportedGatewayWideTLS(gateway.Spec.TLS) {
		return nil, &gatewayValidationError{
			message:       "Gateway-wide spec.tls requires frontend or backend configuration",
			gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
		}
	}
	// A bound Gateway with an enrollment checkpoint may still observe its
	// listeners for withdrawal. Route compilation cannot run without policies.
	if BackendClientCertificateRef(gateway) != nil && !config.BackendTLSPolicy &&
		(!controllerutil.ContainsFinalizer(gateway, config.GatewayFinalizer()) || !controllerutil.ContainsFinalizer(gateway, config.BackendCAFinalizer())) {
		return nil, &gatewayValidationError{
			message:       "Gateway backend client certificates require BackendTLSPolicy observation",
			gatewayReason: gatewayv1.GatewayReasonInvalidParameters,
		}
	}
	sections, conflicts := listenerSectionsWithoutTCPConflicts(config, gateway)
	if len(sections) == 0 && len(conflicts) != 0 {
		return nil, fmt.Errorf("%s", gatewayTCPProtocolConflictMessage(conflicts))
	}
	var listeners []*gatewayv1.Listener
	switch {
	case config.TLSRoutesEnabled() && slices.ContainsFunc(sections, func(section *gatewayv1.Listener) bool {
		return section.Protocol == gatewayv1.TLSProtocolType
	}):
		var err error
		listeners, err = supportedTLSListeners(config, gateway, sections)
		if err != nil {
			return nil, err
		}
	case len(sections) == 1 && sections[0].Protocol == gatewayv1.TLSProtocolType:
		return nil, fmt.Errorf("TLSRoute requires explicit controller opt-in")
	case usesHTTPSProfile(config, gateway):
		listeners := httpsProfileListeners(config, gateway)
		if len(listeners) == 0 {
			return nil, fmt.Errorf("a listener with a valid HTTP or HTTPS port is required")
		}
		if len(conflicts) != 0 && !slices.ContainsFunc(sections, func(listener *gatewayv1.Listener) bool {
			return listenerFamily(listener) == gatewayv1.HTTPProtocolType && httpsProfileIssue(config, gateway, listener) == nil
		}) {
			return nil, fmt.Errorf("%s", gatewayTCPProtocolConflictMessage(conflicts))
		}
		return listeners, nil
	case hasMultipleTransportListeners(sections):
		listeners = sections
		slices.SortFunc(listeners, func(left, right *gatewayv1.Listener) int {
			return cmp.Compare(left.Port, right.Port)
		})
	case len(sections) == 1 && isL4Listener(sections[0].Protocol):
		listeners = sections
	default:
		http, err := httpListeners(config, gateway, sections)
		if err != nil {
			return nil, err
		}
		listeners = http
		for _, listener := range listeners {
			if listener.TLS != nil {
				return nil, fmt.Errorf("only plain HTTP listeners are supported")
			}
			if listener.Hostname != nil {
				if err := amphora.ValidateHostname(string(*listener.Hostname)); err != nil {
					return nil, fmt.Errorf("invalid listener hostname: %w", err)
				}
			}
		}
	}
	for _, listener := range listeners {
		if isL4Listener(listener.Protocol) {
			if err := validateL4Listener(listener); err != nil {
				return nil, err
			}
		}
		if listenerHasRequestFilters(listener) && (!config.GatewayListenerFiltersEnabled() || listener.Protocol != gatewayv1.HTTPProtocolType) {
			return nil, fmt.Errorf("listener request filters cannot be enforced by the stock Amphora %s profile", listener.Protocol)
		}
		kind := listenerRouteKind(listener.Protocol)
		if listener.AllowedRoutes != nil && slices.ContainsFunc(listener.AllowedRoutes.Kinds, func(ref gatewayv1.RouteGroupKind) bool {
			return !listenerSupportsRouteKind(config, *listener, ref)
		}) {
			if listener.Protocol == gatewayv1.HTTPProtocolType && config.GRPCExactMethodEnabled() {
				return nil, fmt.Errorf("only HTTPRoute and GRPCRoute are supported in listener allowedRoutes.kinds")
			}
			return nil, fmt.Errorf("only %s is supported in listener allowedRoutes.kinds", kind)
		}
	}
	return listeners, nil
}

// supportedTLSListeners keeps the native TLS SNI graph dedicated to one port.
func supportedTLSListeners(config controller.Config, gateway *gatewayv1.Gateway, sections []*gatewayv1.Listener) ([]*gatewayv1.Listener, error) {
	first := sections[0]
	mixed := mixedTLSBinding(config, gateway, first.Port)
	if len(sections) != 1 && !mixed {
		return nil, fmt.Errorf("TLSRoute requires a dedicated Gateway with one listener or an enabled same-port mixed TLS listener")
	}
	var listeners []*gatewayv1.Listener
	hosts := make(map[string]bool)
	for _, listener := range sections {
		if listener.Protocol != gatewayv1.TLSProtocolType || listener.Port < 1 || listener.Port > 65535 ||
			listener.Port != first.Port || !tlsListenerModeEnabled(config, listener) || len(listener.TLS.Options) != 0 {
			return nil, fmt.Errorf("TLSRoute requires enabled TLS modes on one valid port and no TLS options")
		}
		if !terminatesTLS(listener) && len(listener.TLS.CertificateRefs) != 0 {
			return nil, fmt.Errorf("TLS passthrough mode does not support certificate references")
		}
		if listener.Hostname != nil {
			hostname := string(*listener.Hostname)
			if err := amphora.ValidateHostname(hostname); err != nil {
				return nil, fmt.Errorf("invalid listener hostname: %w", err)
			}
			if mixed && hosts[hostname] {
				return nil, fmt.Errorf("same-port TLS listeners require distinct hostnames")
			}
			hosts[hostname] = true
		} else if mixed {
			return nil, fmt.Errorf("same-port TLS listeners require explicitly named hostnames")
		}
		if err := validateRouteNamespacePolicy(listener); err != nil {
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	// The existing TLS certificate and lifecycle checkpoints use the primary
	// terminating listener on a mixed port.
	slices.SortFunc(listeners, func(a, b *gatewayv1.Listener) int {
		if terminatesTLS(a) == terminatesTLS(b) {
			return cmp.Compare(a.Name, b.Name)
		}
		if terminatesTLS(a) {
			return -1
		}
		return 1
	})
	return listeners, nil
}

// hasMultipleTransportListeners describes the Gateway shape eligible for enrollment.
func hasMultipleTransportListeners(listeners []*gatewayv1.Listener) bool {
	if len(listeners) < 2 {
		return false
	}
	protocol := listeners[0].Protocol
	if !isL4Listener(protocol) {
		return false
	}
	ports := make(map[gatewayv1.PortNumber]struct{}, len(listeners))
	for _, listener := range listeners {
		if listener.Protocol != protocol || listener.Port < 1 || listener.Port > 65535 {
			return false
		}
		if _, exists := ports[listener.Port]; exists {
			return false
		}
		ports[listener.Port] = struct{}{}
	}
	return true
}

func isL4Listener(protocol gatewayv1.ProtocolType) bool {
	return protocol == gatewayv1.TCPProtocolType || protocol == gatewayv1.UDPProtocolType
}

func validateL4Listener(listener *gatewayv1.Listener) error {
	if listener.Hostname != nil || listener.TLS != nil {
		return fmt.Errorf("%s listeners do not support hostname or TLS", listener.Protocol)
	}
	return validateRouteNamespacePolicy(listener)
}

func validateRouteNamespacePolicy(listener *gatewayv1.Listener) error {
	if listener.AllowedRoutes == nil || listener.AllowedRoutes.Namespaces == nil {
		return nil
	}
	namespaces := listener.AllowedRoutes.Namespaces
	if namespaces.From == nil {
		return nil
	}
	switch *namespaces.From {
	case gatewayv1.NamespacesFromSame, gatewayv1.NamespacesFromAll:
		return nil
	case gatewayv1.NamespacesFromSelector:
		if namespaces.Selector == nil {
			return fmt.Errorf("listener allowedRoutes namespace policy requires a selector")
		}
		if _, err := metav1.LabelSelectorAsSelector(namespaces.Selector); err != nil {
			return fmt.Errorf("invalid listener allowedRoutes namespace selector: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("listener allowedRoutes namespace policy is unsupported")
	}
}

func validateAddresses(addresses []gatewayv1.GatewaySpecAddress) *gatewayValidationError {
	programmedReason := gatewayv1.GatewayReasonAddressNotAssigned
	for _, request := range addresses {
		if request.Value != "" {
			programmedReason = gatewayv1.GatewayReasonAddressNotUsable
			break
		}
	}
	for _, request := range addresses {
		if request.Type != nil && *request.Type != gatewayv1.IPAddressType {
			return &gatewayValidationError{
				message:          "only the IPAddress Gateway address type is supported",
				gatewayReason:    gatewayv1.GatewayReasonUnsupportedAddress,
				programmedReason: programmedReason,
			}
		}
	}

	for _, request := range addresses {
		if request.Routability != gatewayv1.GatewayAddressRoutabilityDefault {
			return &gatewayValidationError{
				message:          "only default Gateway address routability is supported",
				gatewayReason:    gatewayv1.GatewayReasonInvalid,
				programmedReason: programmedReason,
			}
		}
		if request.Value == "" {
			continue
		}
		address, err := netip.ParseAddr(request.Value)
		if err != nil || address.Zone() != "" {
			return &gatewayValidationError{
				message:          "the requested Gateway IP address is invalid",
				gatewayReason:    gatewayv1.GatewayReasonInvalid,
				programmedReason: programmedReason,
			}
		}
		if !address.Is4() {
			return &gatewayValidationError{
				message:          "a fixed Gateway address must be IPv4",
				gatewayReason:    gatewayv1.GatewayReasonAccepted,
				programmedReason: programmedReason,
			}
		}
	}
	if len(addresses) > 1 {
		reason := gatewayv1.GatewayReasonInvalid
		if programmedReason == gatewayv1.GatewayReasonAddressNotUsable {
			reason = gatewayv1.GatewayReasonAccepted
		}
		return &gatewayValidationError{
			message:          "only one Gateway address may be requested",
			gatewayReason:    reason,
			programmedReason: programmedReason,
		}
	}
	return nil
}

// retainsLongHTTPListenerHostnames keeps an already bound HTTP family's
// topology intact during opt-out. Its graph fences unrepresentable ports.
func retainsLongHTTPListenerHostnames(config controller.Config, gateway *gatewayv1.Gateway) bool {
	if config.LongHTTPHostnames || !controllerutil.ContainsFinalizer(gateway, config.GatewayFinalizer()) ||
		controller.ValidateGatewayBinding(config, gateway) != nil {
		return false
	}
	bound := gateway.Annotations[config.GatewayListenerProtocolAnnotation()]
	return bound == "" || bound == string(gatewayv1.HTTPProtocolType)
}

// HTTPListeners selects the HTTP sections of the stock profile. Multiple
// sections on each port must have pairwise distinct, encodable hostnames.
func HTTPListeners(config controller.Config, gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, error) {
	listeners, _ := listenerSectionsWithoutTCPConflicts(config, gateway)
	return httpListeners(config, gateway, listeners)
}

func httpListeners(config controller.Config, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener) ([]*gatewayv1.Listener, error) {
	var http []*gatewayv1.Listener
	ports := make(map[gatewayv1.PortNumber]int, len(listeners))
	duplicatePort := false
	for _, listener := range listeners {
		duplicatePort = duplicatePort || ports[listener.Port] != 0
		ports[listener.Port]++
		if listener.Protocol == gatewayv1.HTTPProtocolType {
			http = append(http, listener)
		}
	}
	switch len(http) {
	case 0:
		return nil, fmt.Errorf("a plain HTTP listener or same-protocol TCP or UDP listeners on distinct ports are required")
	case 1:
		if duplicatePort {
			return nil, fmt.Errorf("listeners sharing a port are not supported")
		}
		return http, nil
	}
	if len(http) != len(listeners) {
		return nil, fmt.Errorf("multiple HTTP sections require a Gateway with only plain HTTP listeners")
	}
	allowLongHostnames := config.LongHTTPHostnames || retainsLongHTTPListenerHostnames(config, gateway)
	hostnames := make(map[gatewayv1.PortNumber]map[string]bool)
	names := make(map[gatewayv1.SectionName]bool, len(http))
	for _, listener := range http {
		if listener.Port < 1 || listener.Port > 65535 {
			return nil, fmt.Errorf("listener port must be between 1 and 65535")
		}
		if listener.TLS != nil || names[listener.Name] {
			return nil, fmt.Errorf("HTTP sections require plain HTTP with distinct names")
		}
		names[listener.Name] = true
		if ports[listener.Port] == 1 {
			continue
		}
		if hostnames[listener.Port] == nil {
			hostnames[listener.Port] = make(map[string]bool)
		}
		host := listenerHostname(listener)
		if hostnames[listener.Port][host] {
			return nil, fmt.Errorf("same-port HTTP sections require distinct hostnames")
		}
		if host != "" {
			if _, err := amphora.HostnameRuleValue(host, allowLongHostnames); err != nil {
				return nil, fmt.Errorf("unsupported shared HTTP listener hostname: %w", err)
			}
		}
		hostnames[listener.Port][host] = true
	}
	return http, nil
}

// HasMultipleHTTPListenerPorts identifies Gateways that need per-port bindings.
func HasMultipleHTTPListenerPorts(config controller.Config, gateway *gatewayv1.Gateway) bool {
	http, err := HTTPListeners(config, gateway)
	return err == nil && slices.ContainsFunc(http, func(listener *gatewayv1.Listener) bool { return listener.Port != http[0].Port })
}

func listenerRouteKind(protocol gatewayv1.ProtocolType) gatewayv1.Kind {
	switch protocol {
	case gatewayv1.HTTPProtocolType, gatewayv1.HTTPSProtocolType:
		return "HTTPRoute"
	case gatewayv1.TCPProtocolType:
		return "TCPRoute"
	case gatewayv1.UDPProtocolType:
		return "UDPRoute"
	case gatewayv1.TLSProtocolType:
		return "TLSRoute"
	default:
		return ""
	}
}

func listenerSupportsRouteKind(config controller.Config, listener gatewayv1.Listener, ref gatewayv1.RouteGroupKind) bool {
	return (listener.Protocol != gatewayv1.TLSProtocolType || tlsListenerModeEnabled(config, &listener)) && isRouteGroupKind(ref, listenerRouteKind(listener.Protocol)) ||
		listener.Protocol == gatewayv1.HTTPProtocolType && config.GRPCExactMethodEnabled() && isRouteGroupKind(ref, model.GRPCRouteKind)
}

func listenerAllowsRouteKind(listener gatewayv1.Listener, routeKind gatewayv1.Kind) bool {
	if listener.AllowedRoutes == nil || len(listener.AllowedRoutes.Kinds) == 0 {
		return true
	}
	for _, kind := range listener.AllowedRoutes.Kinds {
		if isRouteGroupKind(kind, routeKind) {
			return true
		}
	}
	return false
}

func isRouteGroupKind(routeKind gatewayv1.RouteGroupKind, supportedKind gatewayv1.Kind) bool {
	group := gatewayv1.Group(gatewayv1.GroupName)
	if routeKind.Group != nil {
		group = *routeKind.Group
	}
	return supportedKind != "" && group == gatewayv1.Group(gatewayv1.GroupName) && routeKind.Kind == supportedKind
}
