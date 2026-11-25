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

package cloud

import "context"

// GraphDisposition states the authority granted for one graph component.
type GraphDisposition string

const (
	// GraphEnsure reconciles a complete desired component.
	GraphEnsure GraphDisposition = "Ensure"
	// GraphPreserve validates a component without changing it.
	GraphPreserve GraphDisposition = "Preserve"
	// GraphDelete removes a component after a durable Kubernetes checkpoint.
	GraphDelete GraphDisposition = "Delete"
	// GraphWithdrawTLS closes owned frontends inconsistent with the current
	// authorized HTTPS ports or TCP TLS certificate. This operation cannot
	// create, enable or reconfigure listeners.
	GraphWithdrawTLS GraphDisposition = "WithdrawTLS"
	// GraphWithdrawBackendClientCertificate restricts owned HTTP pools carrying
	// stale client identities without requiring a complete route graph.
	GraphWithdrawBackendClientCertificate GraphDisposition = "WithdrawBackendClientCertificate"
	// GraphWithdrawListenerFilters applies confirmed section rejections on
	// existing HTTP-family frontends without changing any backend resources.
	GraphWithdrawListenerFilters GraphDisposition = "WithdrawListenerFilters"
	// GraphReconcileTelemetry configures or withdraws tracing without changing application forwarding.
	GraphReconcileTelemetry GraphDisposition = "ReconcileTelemetry"
)

// GatewayGraphSpec contains the complete authority and desired graph for one
// immutable Gateway identity. Route identities remain separate ownership anchors.
type GatewayGraphSpec struct {
	Identity Identity
	Gateway  GatewayGraphNode
	Routes   []RouteGraphNode
	// ValidateMutation rechecks the request-local authority after cloud reads
	// and planning, immediately before the provider sends a mutation.
	ValidateMutation func(context.Context) error `json:"-"`
	// HTTPSPolicyAuthority supplies complete current route bindings for
	// a scoped HTTP cleanup or TLS withdrawal.
	HTTPSPolicyAuthority *GatewayGraphSpec `json:"-"`
}

// GatewayGraphNode describes the Gateway component. Spec is required for Ensure.
type GatewayGraphNode struct {
	Disposition  GraphDisposition
	Spec         GatewaySpec
	TLSSNIRoutes []TLSSNIRoute
	// SNIPoliciesObserved permits replacement of a protected pool after an
	// authorized inventory confirms that its hostname no longer needs TLS.
	SNIPoliciesObserved bool `json:",omitempty"`
	// TCPTLSCertificateRef is the currently authorized certificate for the raw
	// TCP termination profile. An empty reference requires the listener closed.
	TCPTLSCertificateRef string `json:",omitempty"`
	// RetainSNIAddress keeps the load balancer and its addresses while the
	// TLS listener is removed for a same-port change between SNI profiles.
	RetainSNIAddress bool `json:",omitempty"`
	// TLSWithdrawalPorts restricts a known authorization change to these
	// physical HTTPS ports. A nil list checks all TLS frontends.
	TLSWithdrawalPorts []int `json:",omitempty"`
	// TLSWithdrawalPreviousHosts is present only when a fresh snapshot confirms
	// that a targeted port's authorization changed without changing its TLS material.
	TLSWithdrawalPreviousHosts map[int][]string `json:",omitempty"`
	// BackendClientCertificate applies only to HTTP-family pools selected by BackendTLSPolicy.
	BackendClientCertificate BackendClientCertificate
	// XBackendClientWithdrawals limits certificate checks to these bound XBackend references.
	XBackendClientWithdrawals []XBackendClientWithdrawal `json:",omitempty"`
	// BackendCAOwned checkpoints ownership of both frontend and backend trust anchors.
	BackendCAOwned bool `json:",omitempty"`
	// ClaimBackendCA checkpoints ownership before creating a Gateway CA revision.
	ClaimBackendCA         func(context.Context) error `json:"-"`
	TelemetryPolicyPresent bool                        `json:",omitempty"`
	Telemetry              *GatewayTelemetry           `json:",omitempty"`
}

// GatewayTelemetry describes the collector and eligible physical listeners for a Gateway.
// A nil value in a GraphReconcileTelemetry request withdraws listener tracing.
type GatewayTelemetry struct {
	Listeners               []TelemetryListener
	ServiceUID              string
	ServicePort             int
	ServiceName             string
	SpanName                string
	Sampling                *TelemetrySampling       `json:",omitempty"`
	Attributes              []TelemetryAttribute     `json:",omitempty"`
	Headers                 TelemetryExporterHeaders `json:"-"`
	Members                 []Member
	MemberSubnetID          string
	BackendSecurityGroupIDs []string
	BackendTLS              *BackendTLSValidation
}

type TelemetryListener struct {
	Port     int
	Protocol string
}

// AddressWithdrawalOnly indicates that there is no remaining address to
// evaluate. The provider only needs to observe and disable the owned frontend.
func (node GatewayGraphNode) AddressWithdrawalOnly() bool {
	loss := node.Spec.ClusterAddressLoss
	return node.Disposition == GraphEnsure && loss != nil && len(node.Spec.RequestedVIPAddresses) == 0 &&
		len(loss.RetainedClusterVIPs) == 0 && len(loss.RetainedProviderAddresses) == 0
}

// BackendClientCertificate is the current Gateway client identity, or a denial
// when the Gateway requests an identity that cannot be authorized.
type BackendClientCertificate struct {
	Reference string
	Denied    bool
}

// XBackendClientWithdrawal identifies one HTTPRoute's local XBackend reference.
type XBackendClientWithdrawal struct {
	Route       Identity
	BackendName string
	// AuthorizedReference is the only client certificate the pool may retain.
	// An empty value withdraws any client identity for this target.
	AuthorizedReference string
}

// TLSSNIRoute is one effective hostname and its authorized TCP destinations.
type TLSSNIRoute struct {
	// Deny selects an empty pool so the hostname cannot fall through to a broader suffix.
	Deny        bool `json:",omitempty"`
	Passthrough bool `json:",omitempty"`
	Identity    Identity
	Hostname    string
	ServiceUID  string
	// ServicePort identifies the Service section independently of the selected
	// SNI hostname, endpoints and their target port.
	ServicePort             int `json:",omitempty"`
	BackendSubnetID         string
	BackendSecurityGroupIDs []string
	Members                 []Member
	BackendTLS              *BackendTLSValidation `json:",omitempty"`
	// BackendServices identifies the contributing Services and ports of a
	// weighted rule. Single-backend targets keep their original identity.
	BackendServices []TLSBackendService `json:",omitempty"`
	// BackendServiceMembers retains the per-reference weights, endpoints and
	// empty shares before flattening. It does not change the flat pool identity.
	BackendServiceMembers []TLSSNIBackendPool `json:"-"`
	// BackendPools keeps each Service's transport and exact connection share
	// when a single pool cannot preserve the route's forwarding requirements.
	BackendPools []TLSSNIBackendPool `json:",omitempty"`
	// TCPConnectionRejection reserves the unavailable share of new connections.
	TCPConnectionRejection *TCPConnectionRejection `json:",omitempty"`
}

// UseBackendPools replaces flattened member weights with independent pools.
func (route *TLSSNIRoute) UseBackendPools(pools []TLSSNIBackendPool) {
	route.BackendServices, route.BackendServiceMembers, route.Members = nil, nil, nil
	route.TCPConnectionRejection = nil
	route.BackendPools = pools
}

// TLSBackendService identifies a Service port that contributes to an SNI pool.
type TLSBackendService struct {
	UID  string
	Port int
}

// TLSSNIBackendPool represents one positive-weight backend reference.
// A reference with only a weight reserves its share for connection rejection.
type TLSSNIBackendPool struct {
	Weight      int
	ServiceUID  string
	ServicePort int
	Members     []Member
	BackendTLS  *BackendTLSValidation `json:",omitempty"`
}

// MaxTCPRoundRobinMembers is the active server limit of an HAProxy round-robin pool.
const MaxTCPRoundRobinMembers = 4095

// RouteGraphNode describes one exact route component. Spec.Identity is always
// required; the remaining desired fields are required only for Ensure.
type RouteGraphNode struct {
	Disposition GraphDisposition
	Spec        RouteSpec
	// RetainListenerPorts names the existing defaults a preserved TCP or UDP
	// pool can keep after readback. An empty, non-nil list withdraws all defaults
	// in a scoped election.
	RetainListenerPorts []int
	// WaitForHTTPPolicyRemoval guards Gateway readiness while an attached peer
	// with no current desired model still owns a policy on the HTTP frontend.
	// Only the route's durable detach checkpoint authorizes its deletion.
	WaitForHTTPPolicyRemoval bool
}

// GatewayGraphResult separates whole-graph scheduling from component status.
type GatewayGraphResult struct {
	Outcome                        Outcome
	Gateway                        GatewayResult
	Routes                         []RouteGraphResult
	TelemetryUnsupported           string
	TelemetryCapabilityUnavailable bool
}

// RouteGraphResult identifies a route result without relying on input ordering.
type RouteGraphResult struct {
	Identity Identity
	Result   RouteResult
}

// GraphError identifies the component whose validation or mutation failed.
// An identity without route fields identifies the Gateway component.
type GraphError struct {
	Identity Identity
	Err      error
}

func (e *GraphError) Error() string { return e.Err.Error() }

// Unwrap preserves classified provider errors and ownership conflicts.
func (e *GraphError) Unwrap() error { return e.Err }

// GraphProvider reconciles one complete graph with at most one cloud mutation.
type GraphProvider interface {
	ReconcileGatewayGraph(context.Context, GatewayGraphSpec) (GatewayGraphResult, error)
}
