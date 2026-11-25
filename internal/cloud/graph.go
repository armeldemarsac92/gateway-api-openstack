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
	// TLSWithdrawalPorts restricts a known authorization change to these
	// physical HTTPS ports. A nil list checks all TLS frontends.
	TLSWithdrawalPorts []int `json:",omitempty"`
	// TLSWithdrawalPreviousHosts is present only when a fresh snapshot confirms
	// that a targeted port's authorization changed without changing its TLS material.
	TLSWithdrawalPreviousHosts map[int][]string `json:",omitempty"`
	// BackendClientCertificate applies only to HTTP-family pools selected by BackendTLSPolicy.
	BackendClientCertificate BackendClientCertificate
	// BackendCAOwned checkpoints ownership of both frontend and backend trust anchors.
	BackendCAOwned bool `json:",omitempty"`
	// ClaimBackendCA checkpoints ownership before creating a Gateway CA revision.
	ClaimBackendCA func(context.Context) error `json:"-"`
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

// TLSSNIRoute is one effective hostname and its authorized TCP destinations.
type TLSSNIRoute struct {
	// Deny selects an empty pool so the hostname cannot fall through to a broader suffix.
	Deny       bool `json:",omitempty"`
	Identity   Identity
	Hostname   string
	ServiceUID string
	// ServicePort identifies the Service section independently of the selected
	// SNI hostname, endpoints and their target port.
	ServicePort             int `json:",omitempty"`
	BackendSubnetID         string
	BackendSecurityGroupIDs []string
	Members                 []Member
	BackendTLS              *BackendTLSValidation `json:",omitempty"`
}

// RouteGraphNode describes one exact route component. Spec.Identity is always
// required; the remaining desired fields are required only for Ensure.
type RouteGraphNode struct {
	Disposition GraphDisposition
	Spec        RouteSpec
	// RetainScopedListenerPorts limits which existing defaults may remain on a
	// preserved pool in a full TCP or UDP Gateway election. A nil set grants no
	// authority to change those associations.
	RetainScopedListenerPorts []int
	// WaitForHTTPPolicyRemoval guards Gateway readiness while an attached peer
	// with no current desired model still owns a policy on the HTTP frontend.
	// Only the route's durable detach checkpoint authorizes its deletion.
	WaitForHTTPPolicyRemoval bool
}

// GatewayGraphResult separates whole-graph scheduling from component status.
type GatewayGraphResult struct {
	Outcome Outcome
	Gateway GatewayResult
	Routes  []RouteGraphResult
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
