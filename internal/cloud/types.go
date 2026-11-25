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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package cloud

import (
	"fmt"
	"iter"
	"slices"
)

// Identity is the immutable ownership anchor for a Gateway and its resources.
// Route fields are set only for resources whose lifecycle is owned by one
// route.
type Identity struct {
	OpenStackProjectID string `json:"openStackProjectID,omitempty"`
	ClusterID          string `json:"clusterID"`
	Controller         string `json:"controller"`
	ControllerVersion  string `json:"controllerVersion,omitempty"`
	GatewayNamespace   string `json:"gatewayNamespace"`
	GatewayName        string `json:"gatewayName"`
	GatewayUID         string `json:"gatewayUID"`
	RouteNamespace     string `json:"routeNamespace,omitempty"`
	RouteName          string `json:"routeName,omitempty"`
	RouteUID           string `json:"routeUID,omitempty"`
}

// GatewaySpec describes the OpenStack resources owned by a Gateway.
type GatewaySpec struct {
	Identity          Identity
	Provider          string
	VIPSubnetID       string
	ExternalNetworkID string
	FlavorID          string
	AvailabilityZone  string
	ListenerName      string
	ListenerPort      int
	ListenerProtocol  string
	// ScopedListeners lists every desired physical port after the Gateway has entered
	// multi-listener mode. ListenerPort retains its original binding port so
	// an existing unscoped listener can be recognized after a restart.
	// Cleanup carries an empty, non-nil set and the same frozen port.
	ScopedListeners []ScopedListener
	// HTTPListenerHostnames lists all sections on plaintext ports that need hostname isolation.
	// "*" denotes the section with no hostname.
	HTTPListenerHostnames map[int][]string `json:",omitempty"`
	// RequestFilterSections describes ordered pre-routing filters by physical port.
	RequestFilterSections map[int][]ListenerRequestFilterSection `json:",omitempty"`
	VIPSecurityGroupIDs   []string

	// RequestedVIPAddress is an optional fixed private IPv4 address on VIPSubnetID.
	RequestedVIPAddress string
	// RequestedVIPAddresses lists alternatives for the single VIP in preference
	// order. An empty string allows automatic allocation. A nil list uses the
	// legacy single-address contract. A non-nil empty list rejects assignment.
	RequestedVIPAddresses []string
	// AddressRejection records an invalid address request on a bound Gateway.
	// Existing listeners and routes remain eligible for reconciliation.
	AddressRejection string
	// ClusterAddressLoss is set when Cluster requests cannot reserve an
	// address. Only a valid provider address or a retained active address
	// can then justify keeping the frontend enabled.
	ClusterAddressLoss *ClusterAddressLoss
}

type ListenerRequestFilterSection struct {
	Hostname string
	Reject   bool
	Requests []ListenerExternalAuth
}

const ExternalAuthProtocolGRPC = "GRPC"

// ListenerExternalAuth describes an authorization Service resolved from
// the referring Gateway or HTTPRoute.
type ListenerExternalAuth struct {
	// Protocol is empty for HTTP. Only HTTPRoute rules can select GRPC.
	Protocol               string
	BackendTLS             *BackendTLSValidation
	ServiceUID             string
	ServicePort            int
	MemberSubnetID         string
	BackendSecurityGroups  []string
	Members                []Member
	PathPrefix             string
	AllowedRequestHeaders  []string
	AllowedResponseHeaders []string
}

type ClusterAddressLoss struct {
	RetainedClusterVIPs       []string
	RetainedProviderAddresses []string
}

// ScopedListener is one named Octavia listener on a shared Gateway load balancer.
type ScopedListener struct {
	Name string
	Port int
	TLS  *ListenerTLS
}

// ListenerTLS requests a terminated HTTPS frontend. An empty CertificateRef
// withdraws the frontend until an authorized reference becomes available.
type ListenerTLS struct {
	CertificateRef   string
	ClientValidation *FrontendClientValidation
	// VirtualHosts is the complete set of isolated HTTPS listener hostnames on
	// this port. "*" denotes the hostname-unset section. Nil selects dedicated TLS.
	VirtualHosts []string `json:",omitempty"`
}

// MaxCAPEMBytes bounds the normalized CA payload stored in Barbican.
const MaxCAPEMBytes = 128 * 1024

// FrontendClientValidation identifies the trust anchor and validation mode on
// an HTTPS port. The PEM is excluded from graph serialization.
type FrontendClientValidation struct {
	ConfigMapUID     string // UID of the ConfigMap or ClusterTrustBundle
	Digest           string
	PEM              []byte `json:"-"`
	InsecureFallback bool   `json:",omitempty"`
}

// Equal compares the immutable identity and digest of the trust anchor.
func (v *FrontendClientValidation) Equal(other *FrontendClientValidation) bool {
	if v == nil || other == nil {
		return v == other
	}
	return v.ConfigMapUID == other.ConfigMapUID && v.Digest == other.Digest &&
		v.InsecureFallback == other.InsecureFallback
}

// Equal compares frontend authorization across independent observations.
func (t *ListenerTLS) Equal(other *ListenerTLS) bool {
	if t == nil || other == nil {
		return t == other
	}
	return t.CertificateRef == other.CertificateRef && t.ClientValidation.Equal(other.ClientValidation) &&
		slices.Equal(t.VirtualHosts, other.VirtualHosts)
}

// GatewayState is the provider-neutral result published to Gateway status and
// passed to route reconciliation.
type GatewayState struct {
	LoadBalancerID    string
	VIPPortID         string
	VIPAddress        string
	FloatingIPID      string
	FloatingIPAddress string
	ListenerID        string
}

// GatewayRequirements contains the immutable Gateway fields and the settings
// that route reconciliation must verify before it can restore traffic.
type GatewayRequirements struct {
	Provider            string
	VIPSubnetID         string
	ExternalNetworkID   string
	FlavorID            string
	AvailabilityZone    string
	ListenerPort        int
	ListenerProtocol    string
	VIPSecurityGroupIDs []string
}

// Equal compares the frozen requirements, including canonical security group IDs.
func (r GatewayRequirements) Equal(other GatewayRequirements) bool {
	return r.Provider == other.Provider && r.VIPSubnetID == other.VIPSubnetID &&
		r.ExternalNetworkID == other.ExternalNetworkID && r.FlavorID == other.FlavorID &&
		r.AvailabilityZone == other.AvailabilityZone && r.ListenerPort == other.ListenerPort &&
		NormalizeListenerProtocol(r.ListenerProtocol) == NormalizeListenerProtocol(other.ListenerProtocol) &&
		slices.Equal(r.VIPSecurityGroupIDs, other.VIPSecurityGroupIDs)
}

const (
	ListenerProtocolHTTP            = "HTTP"
	ListenerProtocolTerminatedHTTPS = "TERMINATED_HTTPS"
	ListenerProtocolTCP             = "TCP"
	ListenerProtocolUDP             = "UDP"
	// ListenerProtocolTLSSNI describes a native TCP listener with no default.
	ListenerProtocolTLSSNI = "TLS"
	// ListenerProtocolTLSTerminate identifies the immutable TCP termination profile.
	ListenerProtocolTLSTerminate = "TLS_TERMINATE"
	// ListenerProtocolTLSMixed binds a shared TCP frontend with a mode per SNI policy.
	ListenerProtocolTLSMixed = "TLS_MIXED"
)

// IsTCPTLSListenerProtocol identifies TCP frontend profiles with authorized certificates.
func IsTCPTLSListenerProtocol(protocol string) bool {
	return protocol == ListenerProtocolTLSTerminate || protocol == ListenerProtocolTLSMixed
}

// NormalizeListenerProtocol interprets bindings written before listener
// protocols were recorded as HTTP.
func NormalizeListenerProtocol(protocol string) string {
	if protocol == "" {
		return ListenerProtocolHTTP
	}
	return protocol
}

// Address returns the client-facing address, preferring a Floating IP when
// one is configured.
func (s GatewayState) Address() string {
	if s.FloatingIPAddress != "" {
		return s.FloatingIPAddress
	}
	return s.VIPAddress
}

// PathMatchType is the supported HTTP path matching subset.
type PathMatchType string

const (
	// PathMatchExact maps to an Octavia EQUAL_TO path rule.
	PathMatchExact PathMatchType = "Exact"
	// PathMatchPrefix maps to an exact policy plus a segment-prefix policy so
	// /foo does not accidentally match /foobar.
	PathMatchPrefix PathMatchType = "PathPrefix"
)

// Member is one Octavia backend member.
type Member struct {
	Address string
	Port    int
	// Draining keeps an endpoint registered at weight zero while it is terminating.
	Draining bool
	// ProviderID identifies the Node's server for read-only worker-port validation.
	// It is not part of the Octavia member identity, which remains address and port.
	ProviderID string
	// Weight is an explicit traffic weight for TCP or UDP members.
	// Zero uses the default for ready members. Draining endpoints always use zero.
	Weight int `json:",omitempty"`
}

// HTTPPathMatch describes one path and its optional conjunctive predicates at its listener rank.
type HTTPPathMatch struct {
	// HTTPListenerHostname identifies the section of a shared plaintext port.
	HTTPListenerHostname string `json:",omitempty"`
	// HTTPSListenerHostname restricts this alternative to one logical HTTPS listener.
	HTTPSListenerHostname    string `json:",omitempty"`
	Hostname                 string
	PathType                 PathMatchType
	PathValue                string
	Precedence               int
	Method                   string                `json:",omitempty"`
	Headers                  []HTTPHeaderMatch     `json:",omitempty"`
	QueryParams              []HTTPQueryParamMatch `json:",omitempty"`
	RequestHeaderOperations  []HTTPHeaderOperation `json:",omitempty"`
	URLRewrite               *HTTPURLRewrite       `json:",omitempty"`
	RequestPolicy            *HTTPRequestPolicy    `json:",omitempty"`
	RequestMirrors           []HTTPMirrorTarget    `json:",omitempty"`
	ExternalAuth             *ListenerExternalAuth `json:",omitempty"`
	CORS                     *HTTPCORS             `json:",omitempty"`
	ResponseHeaderOperations []HTTPHeaderOperation `json:",omitempty"`
}

// HTTPURLRewrite changes the request sent to the selected backend. A prefix
// replacement uses the original PathPrefix of the enclosing HTTPPathMatch.
type HTTPURLRewrite struct {
	Hostname           string
	ReplaceFullPath    *string
	ReplacePrefixMatch *string
}

type HTTPBackendKind string

const HTTPBackendKindXBackend HTTPBackendKind = "gateway.networking.x-k8s.io/XBackend"

// BackendTLSValidation requires authenticated TLS to a backend. The
// hostname is used as SNI and authenticates the certificate if no SANs are
// configured. Denied keeps
// the backend's HTTP matches and weight while withholding all forwarding.
type BackendTLSValidation struct {
	Trust           BackendTLSTrust `json:",omitempty"`
	Hostname        string
	SubjectAltNames []BackendTLSSubjectAltName `json:",omitempty"`
	CAConfigMapUID  string                     // UID of the ConfigMap or ClusterTrustBundle
	CADigest        string
	CAPEM           []byte `json:"-"`
	Denied          bool
	// RequireH2 requires negotiated HTTP/2 on an authenticated HTTP XBackend.
	RequireH2 bool `json:",omitempty"`
}

// BackendTLSTrust distinguishes a referenced CA from the Amphora system store.
type BackendTLSTrust string

const (
	BackendTLSTrustReference BackendTLSTrust = ""
	BackendTLSTrustSystem    BackendTLSTrust = "System"
)

// BackendTLSSubjectAltName is one typed certificate identity in Octavia's API.
type BackendTLSSubjectAltName struct {
	Type     string `json:"type"`
	Hostname string `json:"hostname,omitempty"`
	URI      string `json:"uri,omitempty"`
}

const (
	BackendTLSSANHostname = "Hostname"
	BackendTLSSANURI      = "URI"
)

// ServiceSessionPersistence configures affinity to endpoints of a core Service.
type ServiceSessionPersistence struct {
	CookieName     string
	CookiePath     *string
	CookieDeadline *HTTPSessionCookieDeadline
	Header         *HTTPSessionHeaderPersistence
}

// HTTPBackend groups the paths served by one backend within one HTTPRoute.
// The zero kind denotes a core Service port.
// Unavailable marks an unusable target. Its matches retain their position and
// its pool must contain no members.
type HTTPBackend struct {
	BackendRetryBudget        *BackendRetryBudget `json:",omitempty"`
	BackendRetryBudgetPending bool                `json:",omitempty"`
	// XBackendClientCertificateReference is an authorized Barbican identity for this XBackend alone.
	XBackendClientCertificateReference string `json:"-"`
	// SessionCookieName enables an HTTP session cookie on an XBackend's pool.
	SessionCookieName                string                     `json:",omitempty"`
	ServiceSessionPersistence        *ServiceSessionPersistence `json:",omitempty"`
	ServiceSessionPersistencePending bool                       `json:",omitempty"`
	// SessionPersistenceRule identifies an inline rule within the owning Route.
	SessionPersistenceRule string `json:",omitempty"`
	// SessionPersistenceServiceUID binds an inline pool to the observed Service.
	SessionPersistenceServiceUID string `json:",omitempty"`
	// SessionCookiePath holds a custom Path value, including an explicitly empty path.
	SessionCookiePath     *string                       `json:",omitempty"`
	SessionCookieDeadline *HTTPSessionCookieDeadline    `json:",omitempty"`
	SessionHeader         *HTTPSessionHeaderPersistence `json:",omitempty"`
	// InvalidReference distinguishes an unresolved reference from a valid backend without endpoints.
	InvalidReference bool `json:",omitempty"`
	BackendH2C       bool `json:",omitempty"`
	// BackendH2CPending limits this XBackend to already verified h2c members.
	BackendH2CPending bool                  `json:",omitempty"`
	BackendTLS        *BackendTLSValidation `json:",omitempty"`
	Kind              HTTPBackendKind       `json:",omitempty"`
	Namespace         string
	Name              string
	Port              int
	Members           []Member
	Unavailable       bool
	Matches           []HTTPPathMatch
	// RoutedExternal uses IPv4 destinations reached by Amphora's existing routes.
	// Octavia receives these members without a Neutron subnet binding.
	RoutedExternal bool `json:",omitempty"`
}

// HTTPRedirect groups the alternatives of one redirect rule.
type HTTPRedirect struct {
	RuleIndex  int
	Prefix     string
	StatusCode int
	Matches    []HTTPPathMatch
	Request    *HTTPRedirectRequest `json:",omitempty"`
}

// HTTPRedirectRequest holds the listener-specific destination and any replacement.
// A prefix replacement uses the prefix of each individual HTTPPathMatch.
type HTTPRedirectRequest struct {
	Scheme             string
	Port               int
	Hostname           string
	ReplaceFullPath    *string
	ReplacePrefixMatch *string
}

// HTTPFixedRule groups the matches that require a local HTTP 500 response.
type HTTPFixedRule struct {
	RuleIndex int
	Matches   []HTTPPathMatch
}

// HTTPWeightedTarget selects a backend pool by its index in RouteSpec.HTTPBackends.
// A nil index reserves this share for HTTP 500 when the reference is unresolved.
type HTTPWeightedTarget struct {
	BackendIndex             *int
	Weight                   int
	RequestHeaderOperations  []HTTPHeaderOperation `json:",omitempty"`
	ResponseHeaderOperations []HTTPHeaderOperation `json:",omitempty"`
	URLRewrite               *HTTPURLRewrite       `json:",omitempty"`
	RequestMirrors           []HTTPMirrorTarget    `json:",omitempty"`
}

// HTTPWeightedRule preserves backendRef order and owns the policies for its matches.
type HTTPWeightedRule struct {
	RuleIndex int
	Matches   []HTTPPathMatch
	Targets   []HTTPWeightedTarget
}

// TCPConnectionRejection reserves a share of new connections for active rejection.
type TCPConnectionRejection struct {
	RejectWeight int `json:"reject_weight"`
	TotalWeight  int `json:"total_weight"`
}

func (r TCPConnectionRejection) Valid() bool {
	return r.TotalWeight > 0 && r.TotalWeight <= 16_000_000 && r.RejectWeight >= 0 && r.RejectWeight <= r.TotalWeight
}

// UDPPacketDrop reserves a fraction of UDP traffic for discard.
type UDPPacketDrop struct {
	Numerator   int `json:"numerator"`
	Denominator int `json:"denominator"`
}

func (r UDPPacketDrop) Valid() bool {
	return r.Denominator > 0 && r.Denominator <= 16_000_000 && r.Numerator >= 0 && r.Numerator <= r.Denominator
}

func (r UDPPacketDrop) DropsAll() bool { return r.Numerator == r.Denominator }

// RouteSpec describes the traffic configuration owned by one route.
type RouteSpec struct {
	TCPConnectionRejection *TCPConnectionRejection `json:",omitempty"`
	UDPPacketDrop          *UDPPacketDrop          `json:",omitempty"`
	BackendRetryBudget     *BackendRetryBudget     `json:",omitempty"`
	// BackendRetryBudgetPending waits for a reservable Gateway ancestor.
	BackendRetryBudgetPending        bool                       `json:",omitempty"`
	ServiceSessionPersistence        *ServiceSessionPersistence `json:",omitempty"`
	ServiceSessionPersistencePending bool                       `json:",omitempty"`
	Identity                         Identity
	Gateway                          GatewayState
	GatewayRequirements              GatewayRequirements
	// BackendTLSObserved records a current policy inventory for this route.
	// Only such an observation can release a previously protected backend.
	BackendTLSObserved bool `json:"-"`
	// HTTPListenerHostname scopes a scalar route on a shared plaintext port.
	HTTPListenerHostname string `json:",omitempty"`
	// HTTPSListenerHostname scopes scalar routes and supplies the default section
	// for grouped matches that do not specify their own.
	HTTPSListenerHostname string `json:",omitempty"`
	// ScopedListenerPorts identifies a physical HTTP port or the elected ports
	// of a transport Route. Unscoped bindings use GatewayRequirements.ListenerPort.
	ScopedListenerPorts []int `json:",omitempty"`
	// ScopeHTTPResources distinguishes this port's resources from the legacy
	// roles on the Gateway binding's anchor port.
	ScopeHTTPResources bool `json:",omitempty"`
	// AdditionalHTTPPorts contains the same route's desired traffic on other
	// physical ports. All ports share one ownership binding and result.
	AdditionalHTTPPorts     []RouteSpec `json:",omitempty"`
	MemberMode              MemberMode
	MemberSubnetID          string
	BackendSecurityGroupIDs []string
	HealthMonitor           HealthMonitorConfig
	// GRPC restricts policies to native gRPC media types and uses TCP probes.
	GRPC bool `json:",omitempty"`

	Hostname  string
	PathType  PathMatchType
	PathValue string
	// Precedence is the zero-based rank of this route on the Gateway listener.
	// Smaller ranks are evaluated first. Ranks must be distinct among ensured routes.
	Precedence int
	Members    []Member
	// BackendH2C selects h2c for a scalar HTTP backend pool. Grouped routes
	// carry transport on each HTTPBackend.
	BackendH2C bool                  `json:",omitempty"`
	BackendTLS *BackendTLSValidation `json:",omitempty"`
	// HTTPBackends carries the complete set of paths and backend groups when
	// the HTTPRoute has several rules or alternative matches.
	HTTPBackends      []HTTPBackend      `json:",omitempty"`
	FixedHTTPRules    []HTTPFixedRule    `json:",omitempty"`
	HTTPRedirects     []HTTPRedirect     `json:",omitempty"`
	WeightedHTTPRules []HTTPWeightedRule `json:",omitempty"`
}

// HTTPPortSpecs visits each physical port's traffic specification.
func (s RouteSpec) HTTPPortSpecs() iter.Seq[RouteSpec] {
	return func(yield func(RouteSpec) bool) {
		if !yield(s) {
			return
		}
		for _, port := range s.AdditionalHTTPPorts {
			if !yield(port) {
				return
			}
		}
	}
}

// RouteState records the OpenStack objects produced for one route.
type RouteState struct {
	PoolID      string
	MemberIDs   []string
	MonitorID   string
	L7PolicyIDs []string
	L7RuleIDs   []string
	// Routes with several backend groups have one pool and optional monitor per group.
	PoolIDs    []string `json:",omitempty"`
	MonitorIDs []string `json:",omitempty"`
}

// ValidateGatewayIdentity verifies the common Gateway ownership tuple.
func ValidateGatewayIdentity(id Identity) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "cluster ID", value: id.ClusterID},
		{name: "controller", value: id.Controller},
		{name: "Gateway namespace", value: id.GatewayNamespace},
		{name: "Gateway name", value: id.GatewayName},
		{name: "Gateway UID", value: id.GatewayUID},
	} {
		if field.value == "" {
			return fmt.Errorf("%s must not be empty", field.name)
		}
	}
	return nil
}

// ValidateRouteIdentity verifies both the Gateway and route ownership
// tuples.
func ValidateRouteIdentity(id Identity) error {
	if err := ValidateGatewayIdentity(id); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "HTTPRoute namespace", value: id.RouteNamespace},
		{name: "HTTPRoute name", value: id.RouteName},
		{name: "HTTPRoute UID", value: id.RouteUID},
	} {
		if field.value == "" {
			return fmt.Errorf("%s must not be empty", field.name)
		}
	}
	return nil
}
