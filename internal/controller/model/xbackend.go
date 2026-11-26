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
	"crypto/sha256"
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
	RouteKind          gatewayv1.Kind
	MemberMode         cloud.MemberMode
	H2C                bool
	ServerTLS          bool
	ClientTLS          bool
	ExternalHostname   bool
	SessionPersistence bool
	ServiceDNSDomains  []string
}

// XBackendObservation contains the backend configuration and eligible endpoints
// used for cloud lowering. Status and unrelated Pod updates do not change it.
type XBackendObservation struct {
	Key               types.NamespacedName
	UID               types.UID
	Generation        int64
	Spec              gatewayx.BackendSpec
	Endpoints         []Endpoint
	TLS               backendtls.ValidationResult
	ReadIssue         *Diagnostic
	EndpointReadIssue *Diagnostic

	ClientCertificateReference string `json:"-"`
	ClientCertificateIssue     *Diagnostic
}

// XBackendTLSCAKey identifies the trust input of an authenticated XBackend.
func XBackendTLSCAKey(backend *gatewayx.XBackend, bundlesEnabled bool) (types.NamespacedName, bool) {
	tls := backend.Spec.TLS
	if tls == nil || tls.Validation == nil || tls.Mode != gatewayx.BackendTLSModeServerOnly && tls.Mode != gatewayx.BackendTLSModeClientAndServer {
		return types.NamespacedName{}, false
	}
	return backendtls.ValidationCAKey(backend.Namespace, *tls.Validation, bundlesEnabled)
}

func XBackendRequiresTLS(spec gatewayx.BackendSpec) bool {
	return spec.TLS != nil && spec.TLS.Mode != gatewayx.BackendTLSModeNone
}

// gRPC uses HTTP/2 on both transports. TLS mode determines whether the pool
// speaks h2c or requires h2 during the authenticated TLS handshake.
func XBackendUsesH2C(spec gatewayx.BackendSpec) bool {
	protocol := ptr.Deref(spec.Protocol, "")
	return protocol == gatewayx.BackendProtocolH2C || protocol == gatewayx.BackendProtocolGRPC && !XBackendRequiresTLS(spec)
}

func XBackendRequiresH2(spec gatewayx.BackendSpec) bool {
	protocol := ptr.Deref(spec.Protocol, "")
	return protocol == gatewayx.BackendProtocolHTTP2 || protocol == gatewayx.BackendProtocolGRPC && XBackendRequiresTLS(spec)
}

// XBackendSpecForRoute selects the inherited transport while leaving the observed spec intact.
func XBackendSpecForRoute(spec gatewayx.BackendSpec, kind gatewayv1.Kind) gatewayx.BackendSpec {
	if kind == GRPCRouteKind && spec.Protocol == nil {
		spec.Protocol = ptr.To(gatewayx.BackendProtocolGRPC)
	}
	return spec
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

// LocalXBackendReferences lists the well-formed local primary references of an HTTPRoute or projected GRPCRoute.
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

// LocalTCPXBackendReferences lists local XBackends with a well-formed TCPRoute reference.
func LocalTCPXBackendReferences(route *gatewayv1.TCPRoute) []types.NamespacedName {
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
		group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonRefNotPermitted, fmt.Sprintf("XBackend must be in the same namespace as its %s", HTTPProfileRouteKind(route)))
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
	profile := input.XBackendProfile
	profile.RouteKind = HTTPProfileRouteKind(route)
	if diagnostic := CheckXBackendObservation(key, backend, profile); diagnostic != nil {
		group.Diagnostic = diagnostic
		return group
	}
	spec := XBackendSpecForRoute(backend.Spec, profile.RouteKind)
	group.XBackendRoutedExternal = spec.Type == gatewayx.BackendTypeExternalHostname
	group.XBackendTLS = backend.TLS.Validation
	group.XBackendClientCertificateReference = backend.ClientCertificateReference
	if persistence := spec.SessionPersistence; persistence != nil && IsHeaderSessionPersistence(*persistence) {
		timeout, _ := durationMilliseconds(persistence.AbsoluteTimeout)
		group.XBackendSessionHeader = &cloud.HTTPSessionHeaderPersistence{
			Name: strings.ToLower(string(persistence.Header.Name)), TimeoutMS: timeout,
		}
	} else if persistence != nil {
		var resolved bool
		group.XBackendCookieName, resolved = xBackendSessionCookieName(*spec.SessionPersistence, backend.UID)
		if !resolved {
			group.Diagnostic = backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "XBackend session cookie needs an observed XBackend UID")
			return group
		}
		group.XBackendCookiePath = xBackendCookiePath(*spec.SessionPersistence)
		if timeout, _ := durationMilliseconds(spec.SessionPersistence.AbsoluteTimeout); timeout != nil {
			group.XBackendCookieDeadline = &cloud.HTTPSessionCookieDeadline{
				TimeoutMS: *timeout,
				Scope:     fmt.Sprintf("%x", sha256.Sum256([]byte(backend.UID))),
				Permanent: XBackendCookieRequirements(*spec.SessionPersistence)&cloud.HTTPSessionCookiePermanent != 0,
			}
		}
	}
	group.State = BackendResolved
	group.XBackendUsesH2C = XBackendUsesH2C(spec)
	group.XBackendRequiresH2 = XBackendRequiresH2(spec)
	group.XBackendTCPHealthMonitor = profile.RouteKind == GRPCRouteKind
	// These application protocols need not serve an ordinary HTTP GET.
	switch ptr.Deref(spec.Protocol, "") {
	case gatewayx.BackendProtocolGRPC, gatewayx.BackendProtocolWSS, gatewayx.BackendProtocolMCP:
		group.XBackendTCPHealthMonitor = true
	}
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
	if backend.EndpointReadIssue != nil {
		return backend.EndpointReadIssue
	}
	if diagnostic := ValidateXBackend(backend.Spec, profile); diagnostic != nil {
		return diagnostic
	}
	if persistence := backend.Spec.SessionPersistence; persistence != nil && !IsHeaderSessionPersistence(*persistence) && persistence.AbsoluteTimeout != nil && backend.UID == "" {
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "XBackend session cookie absoluteTimeout needs an observed XBackend UID")
	}
	if XBackendRequiresTLS(backend.Spec) && backend.TLS.Validation == nil {
		return xBackendTLSReferenceDiagnostic(backend.TLS)
	}
	if backend.Spec.TLS != nil && backend.Spec.TLS.Mode == gatewayx.BackendTLSModeClientAndServer {
		if backend.ClientCertificateIssue != nil {
			return backend.ClientCertificateIssue
		}
		if backend.ClientCertificateReference == "" {
			return backendDiagnostic(gatewayv1.RouteReasonPending, "Waiting to resolve the XBackend client certificate")
		}
	}
	return nil
}

// ValidateXBackend checks the XBackend in its consuming route's profile.
func ValidateXBackend(spec gatewayx.BackendSpec, profile XBackendProfile) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	spec = XBackendSpecForRoute(spec, profile.RouteKind)
	if profile.RouteKind == GRPCRouteKind {
		switch {
		case spec.Type != gatewayx.BackendTypeEndpointSelector:
			return backendDiagnostic(unsupported, "GRPCRoute XBackend requires type EndpointSelector")
		case spec.SessionPersistence != nil:
			return backendDiagnostic(unsupported, "XBackend sessionPersistence is unsupported on GRPCRoute")
		case *spec.Protocol != gatewayx.BackendProtocolGRPC && *spec.Protocol != gatewayx.BackendProtocolHTTP2 && *spec.Protocol != gatewayx.BackendProtocolH2C:
			return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "GRPCRoute XBackend requires GRPC, HTTP2 with authenticated TLS, or H2C without TLS")
		}
	}
	switch {
	case profile.MemberMode != cloud.MemberModeEndpoint:
		return backendDiagnostic(unsupported, "XBackend requires Endpoint member mode with Unmanaged backend security")
	case spec.Port.Number < 1 || spec.Port.Number > 65535:
		return backendDiagnostic(unsupported, "XBackend must specify a port between 1 and 65535")
	}
	switch spec.Type {
	case gatewayx.BackendTypeEndpointSelector:
		if diagnostic := validateXBackendEndpointSelector(spec); diagnostic != nil {
			return diagnostic
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
	if persistence := spec.SessionPersistence; persistence != nil {
		if diagnostic := validateXBackendSessionPersistence(spec.Type, protocol, *persistence, profile); diagnostic != nil {
			return diagnostic
		}
	}
	if tls := spec.TLS; tls != nil {
		switch tls.Mode {
		case gatewayx.BackendTLSModeNone:
			if tls.Validation != nil || tls.ClientCertificateRef != nil {
				return backendDiagnostic(unsupported, "XBackend TLS mode None does not accept validation or a client certificate")
			}
		case gatewayx.BackendTLSModeServerOnly, gatewayx.BackendTLSModeClientAndServer:
			if !profile.ServerTLS {
				return backendDiagnostic(unsupported, "Authenticated XBackends require the controller's backend TLS opt-in")
			}
			if tls.Mode == gatewayx.BackendTLSModeClientAndServer && !profile.ClientTLS {
				return backendDiagnostic(unsupported, "XBackend TLS mode ClientAndServer requires the operator's XBackend client certificate map")
			}
			if (tls.Mode == gatewayx.BackendTLSModeClientAndServer) != (tls.ClientCertificateRef != nil) {
				return backendDiagnostic(unsupported, "XBackend TLS requires a client certificate reference only in ClientAndServer mode")
			}
			if tls.Validation == nil || !backendtls.IsPreciseHostname(string(tls.Validation.Hostname)) {
				return backendDiagnostic(unsupported, "Authenticated XBackends require validation.hostname to be a precise DNS hostname")
			}
			// The authenticated HTTP/1.1 pool forwards RFC 6455 upgrades and
			// MCP Streamable HTTP. gRPC instead requires the h2 ALPN path.
			switch protocol {
			case "", gatewayx.BackendProtocolHTTP, gatewayx.BackendProtocolHTTP11, gatewayx.BackendProtocolHTTP2,
				gatewayx.BackendProtocolGRPC, gatewayx.BackendProtocolWSS, gatewayx.BackendProtocolMCP:
				return nil
			case gatewayx.BackendProtocolH2C:
				return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "XBackend H2C requires TLS mode None")
			default:
				return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("XBackend protocol %q is unsupported with authenticated TLS", protocol))
			}
		default:
			return backendDiagnostic(unsupported, fmt.Sprintf("XBackend TLS mode %q is unsupported", tls.Mode))
		}
	}
	switch protocol {
	case "", gatewayx.BackendProtocolHTTP, gatewayx.BackendProtocolHTTP11, gatewayx.BackendProtocolMCP:
		return nil
	case gatewayx.BackendProtocolH2C, gatewayx.BackendProtocolGRPC:
		if profile.H2C {
			return nil
		}
	case gatewayx.BackendProtocolHTTP2:
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "XBackend HTTP2 requires authenticated TLS. Use H2C for cleartext HTTP/2")
	case gatewayx.BackendProtocolWSS:
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "XBackend WSS requires ServerOnly or ClientAndServer TLS")
	}
	return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, fmt.Sprintf("XBackend protocol %q is unsupported by the configured HTTP backend profile", protocol))
}

// ValidateTCPXBackend checks the backend in a raw Gateway TCPRoute context.
func ValidateTCPXBackend(spec gatewayx.BackendSpec, mode cloud.MemberMode) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	switch {
	case mode != "" && mode != cloud.MemberModeEndpoint:
		return backendDiagnostic(unsupported, "TCP XBackend requires Endpoint member mode with Unmanaged backend security")
	case spec.Type != gatewayx.BackendTypeEndpointSelector:
		return backendDiagnostic(unsupported, "TCP XBackend requires type EndpointSelector")
	case spec.Port.Number < 1 || spec.Port.Number > 65535:
		return backendDiagnostic(unsupported, "XBackend must specify a port between 1 and 65535")
	case ptr.Deref(spec.Protocol, gatewayx.BackendProtocolTCP) != gatewayx.BackendProtocolTCP:
		return backendDiagnostic(gatewayv1.RouteReasonUnsupportedProtocol, "TCPRoute XBackend requires protocol TCP or an omitted protocol")
	case spec.TLS != nil && (spec.TLS.Mode != gatewayx.BackendTLSModeNone || spec.TLS.Validation != nil || spec.TLS.ClientCertificateRef != nil):
		return backendDiagnostic(unsupported, "TCP XBackend requires TLS mode None without validation or a client certificate")
	case spec.SessionPersistence != nil:
		return backendDiagnostic(unsupported, "HTTP Cookie and Header session persistence is unsupported on TCP XBackend")
	}
	return validateXBackendEndpointSelector(spec)
}

func validateXBackendEndpointSelector(spec gatewayx.BackendSpec) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	if spec.ExternalHostname != nil || spec.EndpointSelector == nil || len(spec.EndpointSelector.MatchLabels) == 0 {
		return backendDiagnostic(unsupported, "XBackend EndpointSelector requires a nonempty endpointSelector.matchLabels")
	}
	for key, value := range spec.EndpointSelector.MatchLabels {
		if len(validation.IsQualifiedName(string(key))) != 0 || len(validation.IsValidLabelValue(string(value))) != 0 {
			return backendDiagnostic(unsupported, "XBackend endpointSelector contains an invalid label")
		}
	}
	return nil
}

func validateXBackendSessionPersistence(kind gatewayx.BackendType, protocol gatewayx.BackendProtocol, persistence gatewayv1.SessionPersistence, profile XBackendProfile) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	switch {
	case !profile.SessionPersistence:
		return backendDiagnostic(unsupported, "XBackend sessionPersistence requires the controller's Experimental Octavia session persistence opt-in")
	case kind != gatewayx.BackendTypeEndpointSelector:
		return backendDiagnostic(unsupported, "XBackend sessionPersistence requires EndpointSelector")
	}
	if diagnostic := validateSessionPersistence(persistence, "XBackend"); diagnostic != nil || !IsHeaderSessionPersistence(persistence) {
		return diagnostic
	}
	name := string(persistence.Header.Name)
	switch protocol {
	case gatewayx.BackendProtocolMCP:
		if strings.EqualFold(name, "MCP-Session-Id") {
			return backendDiagnostic(unsupported, "XBackend MCP requires an affinity header distinct from MCP-Session-Id")
		}
	case gatewayx.BackendProtocolGRPC:
		header := cloud.HTTPSessionHeaderPersistence{Name: name}
		if !header.ValidForGRPC() {
			return backendDiagnostic(unsupported, "XBackend GRPC Header sessionPersistence requires a custom ASCII metadata key without the -bin suffix")
		}
	}
	return nil
}

func validateSessionPersistence(persistence gatewayv1.SessionPersistence, subject string) *Diagnostic {
	const unsupported = gatewayv1.RouteReasonUnsupportedValue
	timeout, valid := durationMilliseconds(persistence.AbsoluteTimeout)
	if !valid {
		return backendDiagnostic(unsupported, subject+" sessionPersistence absoluteTimeout must be a Gateway API duration")
	}
	if IsHeaderSessionPersistence(persistence) {
		if persistence.Header == nil || persistence.Cookie != nil {
			return backendDiagnostic(unsupported, subject+" Header sessionPersistence requires header settings and cannot include cookie settings")
		}
		header := cloud.HTTPSessionHeaderPersistence{Name: string(persistence.Header.Name), TimeoutMS: timeout}
		if !header.Valid() {
			return backendDiagnostic(unsupported, subject+" Header sessionPersistence requires a valid, unprotected HTTP header name and a supported absoluteTimeout")
		}
		return nil
	}
	if ptr.Deref(persistence.Type, gatewayv1.CookieBasedSessionPersistence) != gatewayv1.CookieBasedSessionPersistence || persistence.Header != nil {
		return backendDiagnostic(unsupported, subject+" sessionPersistence requires Cookie or Header with matching settings")
	}
	if cookie := persistence.Cookie; cookie != nil {
		switch {
		case cookie.Path != nil && !cloud.ValidHTTPSessionCookiePath(*cookie.Path):
			return backendDiagnostic(unsupported, subject+" sessionPersistence cookie path must be at most 1024 printable ASCII bytes without semicolons")
		case cookie.Name != nil && !cloud.ValidHTTPSessionCookieName(string(*cookie.Name)):
			return backendDiagnostic(unsupported, subject+" sessionPersistence cookie name must be a safe Octavia token without a reserved cookie prefix")
		}
		switch lifetime := ptr.Deref(cookie.LifetimeType, gatewayv1.SessionCookieLifetimeType); lifetime {
		case gatewayv1.SessionCookieLifetimeType:
		case gatewayv1.PermanentCookieLifetimeType:
			if persistence.AbsoluteTimeout == nil {
				return backendDiagnostic(unsupported, subject+" sessionPersistence requires absoluteTimeout for cookie lifetimeType Permanent")
			}
		default:
			return backendDiagnostic(unsupported, fmt.Sprintf("%s sessionPersistence cookie lifetimeType %q is unsupported", subject, lifetime))
		}
	}
	return nil
}

// ServiceSessionPersistence validates and lowers the direct Service policy.
func ServiceSessionPersistence(persistence gatewayv1.SessionPersistence, uid types.UID) (*cloud.ServiceSessionPersistence, *Diagnostic) {
	if diagnostic := validateSessionPersistence(persistence, "Service"); diagnostic != nil {
		return nil, diagnostic
	}
	if !IsHeaderSessionPersistence(persistence) && uid == "" {
		return nil, backendDiagnostic(gatewayv1.RouteReasonUnsupportedValue, "Service session cookie needs an observed Service UID")
	}
	scope := fmt.Sprintf("%x", sha256.Sum256([]byte("service/"+string(uid))))
	return poolSessionPersistence(persistence, "service", scope), nil
}

func poolSessionPersistence(persistence gatewayv1.SessionPersistence, prefix, scope string) *cloud.ServiceSessionPersistence {
	timeout, _ := durationMilliseconds(persistence.AbsoluteTimeout)
	if IsHeaderSessionPersistence(persistence) {
		return &cloud.ServiceSessionPersistence{Header: &cloud.HTTPSessionHeaderPersistence{
			Name: strings.ToLower(string(persistence.Header.Name)), TimeoutMS: timeout,
		}}
	}
	name := prefix + "-" + scope
	if persistence.Cookie != nil && persistence.Cookie.Name != nil {
		name = string(*persistence.Cookie.Name)
	}
	settings := &cloud.ServiceSessionPersistence{CookieName: name, CookiePath: xBackendCookiePath(persistence)}
	if timeout != nil {
		settings.CookieDeadline = &cloud.HTTPSessionCookieDeadline{
			TimeoutMS: *timeout, Scope: scope, Permanent: XBackendCookieRequirements(persistence)&cloud.HTTPSessionCookiePermanent != 0,
		}
	}
	return settings
}

func IsHeaderSessionPersistence(persistence gatewayv1.SessionPersistence) bool {
	return ptr.Deref(persistence.Type, gatewayv1.CookieBasedSessionPersistence) == gatewayv1.HeaderBasedSessionPersistence
}

// XBackendCookieRequirements selects regional capabilities for a validated cookie configuration.
func XBackendCookieRequirements(persistence gatewayv1.SessionPersistence) cloud.HTTPSessionCookieRequirements {
	path := xBackendCookiePath(persistence)
	if persistence.Cookie != nil && ptr.Deref(persistence.Cookie.LifetimeType, gatewayv1.SessionCookieLifetimeType) == gatewayv1.PermanentCookieLifetimeType {
		return cloud.HTTPSessionCookiePermanent.WithPath(path)
	}
	if persistence.AbsoluteTimeout != nil {
		return cloud.HTTPSessionCookieTimed.WithPath(path)
	}
	return cloud.HTTPSessionCookieNamed.WithPath(path)
}

func xBackendCookiePath(persistence gatewayv1.SessionPersistence) *string {
	if cookie := persistence.Cookie; cookie != nil && cookie.Path != nil && *cookie.Path != "/" {
		return cookie.Path
	}
	return nil
}

func xBackendSessionCookieName(persistence gatewayv1.SessionPersistence, uid types.UID) (string, bool) {
	if cookie := persistence.Cookie; cookie != nil && cookie.Name != nil {
		return string(*cookie.Name), true
	}
	if uid == "" {
		return "", false
	}
	return fmt.Sprintf("xbackend-%x", sha256.Sum256([]byte(uid))), true
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
