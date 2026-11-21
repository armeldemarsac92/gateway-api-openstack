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
	ScopedListeners     []ScopedListener
	VIPSecurityGroupIDs []string

	// RequestedVIPAddress is an optional fixed private IPv4 address on VIPSubnetID.
	RequestedVIPAddress string
	// AddressRejection records an invalid address request on a bound Gateway.
	// Existing listeners and routes remain eligible for reconciliation.
	AddressRejection string
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
	CertificateRef string
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
	ListenerProtocolHTTP = "HTTP"
	ListenerProtocolTCP  = "TCP"
	ListenerProtocolUDP  = "UDP"
)

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
	// Weight is an explicit traffic weight for UDP members or TCP endpoints.
	// Zero uses the default for ready members. Draining endpoints always use zero.
	Weight int `json:",omitempty"`
}

// HTTPPathMatch describes an alternative at its rank on the physical listener.
type HTTPPathMatch struct {
	Hostname                string
	PathType                PathMatchType
	PathValue               string
	Precedence              int
	RequestHeaderOperations []HTTPHeaderOperation `json:",omitempty"`
}

// HTTPBackend groups the paths served by one Service port within one HTTPRoute.
// Unavailable marks an unusable target. Its matches retain their position and
// its pool must contain no members.
type HTTPBackend struct {
	// InvalidReference distinguishes an unresolved reference from a valid Service without endpoints.
	InvalidReference bool `json:",omitempty"`
	BackendH2C       bool `json:",omitempty"`
	Namespace        string
	Name             string
	Port             int
	Members          []Member
	Unavailable      bool
	Matches          []HTTPPathMatch
}

// HTTPRedirect groups the alternatives of one fixed-destination redirect rule.
type HTTPRedirect struct {
	RuleIndex  int
	Prefix     string
	StatusCode int
	Matches    []HTTPPathMatch
}

// HTTPFixedRule groups the matches that require a local HTTP 500 response.
type HTTPFixedRule struct {
	RuleIndex int
	Matches   []HTTPPathMatch
}

// HTTPWeightedTarget selects a Service pool by its index in RouteSpec.HTTPBackends.
// A nil index reserves this share for HTTP 500 when the reference is unresolved.
type HTTPWeightedTarget struct {
	BackendIndex *int
	Weight       int
}

// HTTPWeightedRule preserves backendRef order and owns the policies for its matches.
type HTTPWeightedRule struct {
	RuleIndex int
	Matches   []HTTPPathMatch
	Targets   []HTTPWeightedTarget
}

// RouteSpec describes the traffic configuration owned by one route.
type RouteSpec struct {
	Identity            Identity
	Gateway             GatewayState
	GatewayRequirements GatewayRequirements
	// ScopedListenerPorts identifies the authorized listener ports. An HTTPRoute
	// selects exactly one scoped port, while transport routes may win several.
	// Single-listener bindings use GatewayRequirements.ListenerPort.
	ScopedListenerPorts     []int `json:",omitempty"`
	MemberMode              MemberMode
	MemberSubnetID          string
	BackendSecurityGroupIDs []string
	HealthMonitor           HealthMonitorConfig
	Hostname                string
	PathType                PathMatchType
	PathValue               string
	// Precedence is the zero-based rank of this route on the Gateway listener.
	// Smaller ranks are evaluated first. Ranks must be distinct among ensured routes.
	Precedence int
	Members    []Member
	// BackendH2C selects h2c for a scalar HTTP backend pool. Grouped routes
	// carry transport on each HTTPBackend.
	BackendH2C bool `json:",omitempty"`
	// HTTPBackends carries the complete set of paths and backend groups when
	// the HTTPRoute has several rules or alternative matches.
	HTTPBackends      []HTTPBackend      `json:",omitempty"`
	FixedHTTPRules    []HTTPFixedRule    `json:",omitempty"`
	HTTPRedirects     []HTTPRedirect     `json:",omitempty"`
	WeightedHTTPRules []HTTPWeightedRule `json:",omitempty"`
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
