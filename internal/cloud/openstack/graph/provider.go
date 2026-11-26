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

// Package graph observes, validates, and reconciles the complete OpenStack
// resource graph owned by Gateway API objects.
package graph

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/apierrors"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/clients"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/neutron"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/topology"
	"github.com/gophercloud/gophercloud/v2"
)

const (
	openStackResourceNamePrefix = "gateway-api-openstack"

	roleLoadBalancer = openstackidentity.RoleLoadBalancer
	roleListener     = openstackidentity.RoleListener
	roleFloatingIP   = openstackidentity.RoleFloatingIP
	rolePool         = openstackidentity.RolePool
	roleMember       = openstackidentity.RoleMember
	roleMonitor      = openstackidentity.RoleMonitor
	rolePolicyExact  = openstackidentity.RolePolicyExact
	rolePolicyPrefix = openstackidentity.RolePolicyPrefix
	roleRulePath     = openstackidentity.RoleRulePath
	roleRuleHost     = openstackidentity.RoleRuleHost
)

// ServiceClients is an internal alias used by the graph tests and
// constructor. Authentication remains owned by the clients package.
type ServiceClients = clients.ServiceClients

// Identity is the immutable OpenStack metadata representation used while
// observing and validating the graph.
type Identity = openstackidentity.Identity

// NewIdentity validates and wraps a provider-neutral immutable identity.
func NewIdentity(value cloud.Identity) (Identity, error) {
	return openstackidentity.NewIdentity(value)
}

// ProviderConfig bounds one provider call and controls how soon the controller
// observes an asynchronous Octavia operation again.
type ProviderConfig struct {
	// BackendCA can retire previously published revisions after opt-out.
	BackendCA              *backendca.Store
	BackendTLSPolicy       bool
	BackendRetryBudget     bool
	TCPConnectionRejection bool
	TCPDefaultPoolTLS      bool
	UDPPacketDrop          bool
	FixedHTTPResponses     bool
	LongHTTPHostnames      bool
	LiteralHTTPPaths       bool
	HTTPHeaderExact        bool
	HTTPHeaderName256      bool
	HTTPQueryParamExact    bool
	HTTPRequestRedirect    bool
	HTTPRequestURLRewrite  bool
	HTTPRequestPolicy      bool
	HTTPRequestMirror      bool
	RouteExternalAuth      bool
	RouteExternalAuthGRPC  bool
	HTTPCORS               bool
	BackendRequestFilters  bool
	HTTPMethodMatch        bool
	WeightedHTTPTargets    bool
	RequestHeaderModifier  bool
	ResponseHeaderModifier bool
	BackendH2C             bool
	GRPCContentType        bool
	TLSSNIPassthrough      bool
	TLSSNISuffix           bool
	TCPTLSTermination      bool
	TCPTLSMixedMode        bool
	OperationTimeout       time.Duration
	PollInterval           time.Duration

	TCPWeightedDefaultPools bool

	XBackendSessionPersistence bool
	RouteSessionPersistence    bool

	HTTPSListenerIsolation     bool
	InitialL7Rules             bool
	L7PolicyBatchActivation    bool
	L7PolicyConditionalRemoval bool
}

// Provider owns the OpenStack resource graph for managed Gateway API objects.
type Provider struct {
	backendCA              *backendca.Store
	backendTLSPolicy       bool
	poolALPN               bool
	backendRetryBudget     bool
	tcpConnectionRejection bool
	tcpDefaultPoolTLS      bool
	udpPacketDrop          bool
	transportReplayMu      sync.Mutex
	transportReplayAfter   map[string]time.Time
	fixedHTTPResponses     bool
	longHTTPHostnames      bool
	literalHTTPPaths       bool
	httpHeaderExact        bool
	httpHeaderName256      bool
	httpQueryParamExact    bool
	httpRequestRedirect    bool
	httpRequestURLRewrite  bool
	httpRequestPolicy      bool
	httpRequestMirror      bool
	routeExternalAuth      bool
	routeExternalAuthGRPC  bool
	httpCORS               bool
	backendRequestFilters  bool
	httpMethodMatch        bool
	weightedHTTPTargets    bool
	requestHeaderModifier  bool
	responseHeaderModifier bool
	backendH2C             bool
	grpcContentType        bool
	tlsSNIPassthrough      bool
	tlsSNISuffix           bool
	tcpTLSTermination      bool
	tcpTLSMixedMode        bool
	projectID              string
	region                 string
	neutron                *neutron.Service
	octavia                *octavia.Service
	topology               *topology.Resolver
	operationTimeout       time.Duration
	pollInterval           time.Duration

	tcpWeightedDefaultPools bool

	xBackendSessionPersistence bool
	routeSessionPersistence    bool

	httpsListenerIsolation     bool
	initialL7Rules             bool
	l7PolicyBatchActivation    bool
	l7PolicyConditionalRemoval bool
}

// NewProvider creates an OpenStack provider. The clients are expected to have
// an Octavia microversion that supports tags (2.5 or later).
func NewProvider(serviceClients ServiceClients, cfg ProviderConfig) *Provider {
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = 10 * time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	return &Provider{
		backendCA:              cfg.BackendCA,
		backendTLSPolicy:       cfg.BackendTLSPolicy,
		poolALPN:               serviceClients.LoadBalancer != nil && clients.SupportsPoolALPN(serviceClients.LoadBalancer.Microversion),
		backendRetryBudget:     cfg.BackendRetryBudget,
		tcpConnectionRejection: cfg.TCPConnectionRejection,
		tcpDefaultPoolTLS:      cfg.TCPDefaultPoolTLS,
		udpPacketDrop:          cfg.UDPPacketDrop,
		fixedHTTPResponses:     cfg.FixedHTTPResponses || cfg.WeightedHTTPTargets,
		longHTTPHostnames:      cfg.LongHTTPHostnames,
		literalHTTPPaths:       cfg.LiteralHTTPPaths,
		httpHeaderExact:        cfg.HTTPHeaderExact,
		httpHeaderName256:      cfg.HTTPHeaderName256,
		httpQueryParamExact:    cfg.HTTPQueryParamExact,
		httpRequestURLRewrite:  cfg.HTTPRequestURLRewrite,
		httpRequestPolicy:      cfg.HTTPRequestPolicy,
		httpRequestMirror:      cfg.HTTPRequestMirror,
		routeExternalAuth:      cfg.RouteExternalAuth,
		routeExternalAuthGRPC:  cfg.RouteExternalAuthGRPC,
		httpCORS:               cfg.HTTPCORS,
		backendRequestFilters:  cfg.BackendRequestFilters,
		httpRequestRedirect:    cfg.HTTPRequestRedirect,
		httpMethodMatch:        cfg.HTTPMethodMatch,
		weightedHTTPTargets:    cfg.WeightedHTTPTargets,
		requestHeaderModifier:  cfg.RequestHeaderModifier,
		responseHeaderModifier: cfg.ResponseHeaderModifier,
		backendH2C:             cfg.BackendH2C,
		grpcContentType:        cfg.GRPCContentType,
		tlsSNIPassthrough:      cfg.TLSSNIPassthrough,
		tlsSNISuffix:           cfg.TLSSNISuffix,
		tcpTLSTermination:      cfg.TCPTLSTermination,
		tcpTLSMixedMode:        cfg.TCPTLSMixedMode,
		projectID:              serviceClients.ProjectID,
		region:                 serviceClients.Region,
		neutron:                neutron.NewService(serviceClients.Network),
		octavia:                octavia.NewService(serviceClients.LoadBalancer),
		topology:               topology.NewResolver(serviceClients, cfg.OperationTimeout),
		operationTimeout:       cfg.OperationTimeout,
		pollInterval:           cfg.PollInterval,

		tcpWeightedDefaultPools: cfg.TCPWeightedDefaultPools,

		xBackendSessionPersistence: cfg.XBackendSessionPersistence,
		routeSessionPersistence:    cfg.RouteSessionPersistence,

		httpsListenerIsolation:     cfg.HTTPSListenerIsolation,
		initialL7Rules:             cfg.InitialL7Rules,
		l7PolicyBatchActivation:    cfg.L7PolicyBatchActivation,
		l7PolicyConditionalRemoval: cfg.L7PolicyConditionalRemoval,
	}
}

var _ cloud.GraphProvider = (*Provider)(nil)

// BackendTLSAvailable discovers hostname validation on the regional Amphora endpoint.
func (p *Provider) BackendTLSAvailable(ctx context.Context) (bool, error) {
	return p.backendTLSAvailable(ctx, false, false)
}

// GRPCBackendTLSAvailable also requires enforcement of negotiated h2.
func (p *Provider) GRPCBackendTLSAvailable(ctx context.Context) (bool, error) {
	return p.backendTLSAvailable(ctx, true, false)
}

// BackendTLSSANAvailable discovers explicit SAN authentication for the
// selected transport on the regional Amphora endpoint.
func (p *Provider) BackendTLSSANAvailable(ctx context.Context, requireH2 bool) (bool, error) {
	return p.backendTLSAvailable(ctx, requireH2, true)
}

// XBackendTLSAvailable also requires the configured pool ALPN API.
func (p *Provider) XBackendTLSAvailable(ctx context.Context, requireH2, requireSAN bool) (bool, error) {
	if !p.poolALPN {
		return false, nil
	}
	return p.backendTLSAvailable(ctx, requireH2, requireSAN)
}

// BackendH2CAvailable reports whether Amphora can use cleartext HTTP/2 for a backend.
func (p *Provider) BackendH2CAvailable(ctx context.Context) (bool, error) {
	if !p.backendH2C {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityBackendH2C)
}

// RouteExternalAuthAvailable reports whether the regional policy API can
// authorize a request on its selected HTTP route before forwarding it.
func (p *Provider) RouteExternalAuthAvailable(ctx context.Context, auth cloud.ListenerExternalAuth, weighted bool) (bool, error) {
	if !p.routeAuthorizerTransportEnabled(auth) || !p.fixedHTTPResponses {
		return false, nil
	}
	return amphoraDiscoveryResult(p.octavia.VerifyGatewayCapabilitiesForBody(ctx, "amphora", auth.ForwardBodyMaxSize, routeAuthorizerCapabilities(auth, weighted)...))
}

func (p *Provider) routeAuthorizerTransportEnabled(auth cloud.ListenerExternalAuth) bool {
	tls := auth.BackendTLS
	return p.routeAuthorizerEnabled(auth) && (tls == nil || p.backendTLSPolicy &&
		(!tls.RequireH2 || p.poolALPN) && (tls.Trust != cloud.BackendTLSTrustReference || p.backendCA != nil))
}

// ListenerExternalAuthTLSAvailable checks the native listener authorizer's verified transport.
func (p *Provider) ListenerExternalAuthTLSAvailable(ctx context.Context, auth cloud.ListenerExternalAuth) (bool, error) {
	if auth.BackendTLS == nil || !p.listenerAuthorizerTransportEnabled(auth) {
		return false, nil
	}
	capabilities := append([]string{octavia.GatewayCapabilityListenerRequestFilters}, listenerAuthorizerCapabilities(auth)...)
	return p.amphoraCapabilityAvailable(ctx, capabilities...)
}

func (p *Provider) backendTLSAvailable(ctx context.Context, requireH2, requireSAN bool) (bool, error) {
	if !p.backendTLSPolicy || p.backendCA == nil {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, backendTLSCapabilities(requireH2, requireSAN)...)
}

// BackendTLSSystemCAAvailable checks the selected authenticated HTTP profile with system trust.
func (p *Provider) BackendTLSSystemCAAvailable(ctx context.Context, requireH2, requireSAN, xBackend bool) (bool, error) {
	if !p.backendTLSPolicy || xBackend && !p.poolALPN {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, append(backendTLSCapabilities(requireH2, requireSAN), octavia.GatewayCapabilityBackendTLSSystemCA)...)
}

// TCPBackendTLSSystemCAAvailable checks the selected terminating TLSRoute profile with system trust.
func (p *Provider) TCPBackendTLSSystemCAAvailable(ctx context.Context, weighted bool) (bool, error) {
	if !p.backendTLSPolicy || !p.tcpTLSTermination {
		return false, nil
	}
	required := append(tcpBackendTLSCapabilities(), octavia.GatewayCapabilityBackendTLSSystemCA)
	if weighted {
		required = append(required, octavia.GatewayCapabilityTLSSNIWeightedPools)
	}
	return p.amphoraCapabilityAvailable(ctx, required...)
}

// TCPBackendTLSAvailable checks authenticated backend TLS on terminating TCP listeners.
func (p *Provider) TCPBackendTLSAvailable(ctx context.Context) (bool, error) {
	if !p.backendTLSPolicy || !p.tcpTLSTermination || p.backendCA == nil {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, tcpBackendTLSCapabilities()...)
}

// TCPWeightedBackendTLSAvailable checks independent authenticated pools on terminating TCP listeners.
func (p *Provider) TCPWeightedBackendTLSAvailable(ctx context.Context) (bool, error) {
	if !p.backendTLSPolicy || !p.tcpTLSTermination || p.backendCA == nil {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, append(tcpBackendTLSCapabilities(), octavia.GatewayCapabilityTLSSNIWeightedPools)...)
}

func tcpBackendTLSCapabilities() []string {
	return []string{octavia.GatewayCapabilityTCPTLSTermination, octavia.GatewayCapabilityBackendTLSHostname, octavia.GatewayCapabilityTCPBackendTLSAuth}
}

// HTTPSListenerIsolationAvailable checks the configured region before the
// Kubernetes controller accepts virtual HTTPS listeners sharing a port.
func (p *Provider) HTTPSListenerIsolationAvailable(ctx context.Context) (bool, error) {
	if !p.httpsListenerIsolation {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityHTTPSListenerIsolation)
}

// BackendRetryBudgetAvailable reports whether Amphora accepts new Service budgets.
func (p *Provider) BackendRetryBudgetAvailable(ctx context.Context) (bool, error) {
	if !p.backendRetryBudget {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	available, err := p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityBackendRetryBudget)
	if isNotFound(err) {
		return false, nil
	}
	return available, err
}

// TCPConnectionRejectionAvailable checks explicit opt-in and regional Amphora support.
func (p *Provider) TCPConnectionRejectionAvailable(ctx context.Context) (bool, error) {
	if !p.tcpConnectionRejection {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	available, err := p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityTCPConnectionRejection)
	if isNotFound(err) {
		return false, nil
	}
	return available, err
}

// UDPPacketDropAvailable checks the opt-in and regional Amphora support.
func (p *Provider) UDPPacketDropAvailable(ctx context.Context) (bool, error) {
	return p.udpPacketDropAvailable(ctx, octavia.GatewayCapabilityUDPPacketDrop)
}

// UDPExtendedMemberWeightsAvailable requires both regional UDP batch capabilities.
func (p *Provider) UDPExtendedMemberWeightsAvailable(ctx context.Context) (bool, error) {
	return p.udpPacketDropAvailable(ctx, octavia.GatewayCapabilityUDPPacketDrop, octavia.GatewayCapabilityUDPExtendedMemberWeights)
}

func (p *Provider) udpPacketDropAvailable(ctx context.Context, capabilities ...string) (bool, error) {
	if !p.udpPacketDrop {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	available, err := p.amphoraCapabilityAvailable(ctx, capabilities...)
	if isNotFound(err) {
		return false, nil
	}
	return available, err
}

func (p *Provider) amphoraCapabilityAvailable(ctx context.Context, capabilities ...string) (bool, error) {
	return amphoraDiscoveryResult(p.octavia.VerifyGatewayCapabilities(ctx, "amphora", capabilities...))
}

func amphoraDiscoveryResult(err error) (bool, error) {
	var missing *octavia.MissingGatewayCapability
	if errors.As(err, &missing) {
		return false, nil
	}
	return err == nil, err
}

func (p *Provider) verifyGatewayHTTPCapabilities(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, required []string) error {
	return p.verifyGatewayHTTPRequirements(ctx, desired, actual, required, 0)
}

func (p *Provider) verifyGatewayHTTPRequirements(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, required []string, bodySize int32) error {
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	if err := p.octavia.VerifyGatewayCapabilitiesForBody(ctx, provider, bodySize, required...); err != nil {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("verify Octavia Gateway HTTP capabilities in region %q: %w", p.region, err))
	}
	return nil
}

func (p *Provider) identity(resourceIdentity cloud.Identity) (Identity, error) {
	if strings.TrimSpace(p.projectID) == "" {
		return Identity{}, fmt.Errorf("authenticated OpenStack project ID is missing from service clients")
	}
	if strings.TrimSpace(resourceIdentity.OpenStackProjectID) == "" {
		return Identity{}, fmt.Errorf("controller resource identity must include an OpenStack project ID")
	}
	if resourceIdentity.OpenStackProjectID != p.projectID {
		return Identity{}, fmt.Errorf("%w: identity project %s does not match authenticated project %s", cloud.ErrOwnershipConflict, resourceIdentity.OpenStackProjectID, p.projectID)
	}
	return NewIdentity(resourceIdentity)
}

func (p *Provider) validateLoadBalancerProject(id, projectID string) error {
	if projectID != p.projectID {
		return fmt.Errorf("%w: load balancer %s belongs to project %s, not authenticated project %s", cloud.ErrOwnershipConflict, id, projectID, p.projectID)
	}
	return nil
}

func (p *Provider) validateFloatingIPProject(id, projectID, tenantID string) error {
	if projectID == "" {
		projectID = tenantID
	}
	if projectID != p.projectID {
		return fmt.Errorf("%w: Floating IP %s belongs to project %s, not authenticated project %s", cloud.ErrOwnershipConflict, id, projectID, p.projectID)
	}
	return nil
}

func (p *Provider) validateOptionalProject(resource, id, projectID string) error {
	if projectID != "" && projectID != p.projectID {
		return fmt.Errorf("%w: %s %s belongs to project %s, not authenticated project %s", cloud.ErrOwnershipConflict, resource, id, projectID, p.projectID)
	}
	return nil
}

func (p *Provider) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.operationTimeout)
}

func boolPointer(value bool) *bool { return &value }

func classifyOpenStackError(err error) error {
	return apierrors.Classify(err)
}

func classifyOctaviaMutationError(err error) error {
	return apierrors.ClassifyOctaviaMutation(err)
}

func isNotFound(err error) bool {
	return gophercloud.ResponseCodeIs(err, http.StatusNotFound)
}
