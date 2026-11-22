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
	FixedHTTPResponses     bool
	HTTPHeaderExact        bool
	HTTPQueryParamExact    bool
	HTTPRequestRedirect    bool
	HTTPMethodMatch        bool
	WeightedHTTPTargets    bool
	RequestHeaderModifier  bool
	ResponseHeaderModifier bool
	BackendH2C             bool
	TLSSNIPassthrough      bool
	TLSSNISuffix           bool
	TCPTLSTermination      bool
	OperationTimeout       time.Duration
	PollInterval           time.Duration

	HTTPSListenerIsolation bool
}

// Provider owns the OpenStack resource graph for managed Gateway API objects.
type Provider struct {
	backendCA              *backendca.Store
	backendTLSPolicy       bool
	fixedHTTPResponses     bool
	httpHeaderExact        bool
	httpQueryParamExact    bool
	httpRequestRedirect    bool
	httpMethodMatch        bool
	weightedHTTPTargets    bool
	requestHeaderModifier  bool
	responseHeaderModifier bool
	backendH2C             bool
	tlsSNIPassthrough      bool
	tlsSNISuffix           bool
	tcpTLSTermination      bool
	projectID              string
	region                 string
	neutron                *neutron.Service
	octavia                *octavia.Service
	topology               *topology.Resolver
	operationTimeout       time.Duration
	pollInterval           time.Duration

	httpsListenerIsolation bool
}

// NewProvider creates an OpenStack provider. The clients are expected to have
// an Octavia microversion that supports tags (2.5 or later).
func NewProvider(clients ServiceClients, cfg ProviderConfig) *Provider {
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = 10 * time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	return &Provider{
		backendCA:              cfg.BackendCA,
		backendTLSPolicy:       cfg.BackendTLSPolicy,
		fixedHTTPResponses:     cfg.FixedHTTPResponses || cfg.WeightedHTTPTargets,
		httpHeaderExact:        cfg.HTTPHeaderExact,
		httpQueryParamExact:    cfg.HTTPQueryParamExact,
		httpRequestRedirect:    cfg.HTTPRequestRedirect,
		httpMethodMatch:        cfg.HTTPMethodMatch,
		weightedHTTPTargets:    cfg.WeightedHTTPTargets,
		requestHeaderModifier:  cfg.RequestHeaderModifier,
		responseHeaderModifier: cfg.ResponseHeaderModifier,
		backendH2C:             cfg.BackendH2C,
		tlsSNIPassthrough:      cfg.TLSSNIPassthrough,
		tlsSNISuffix:           cfg.TLSSNISuffix,
		tcpTLSTermination:      cfg.TCPTLSTermination,
		projectID:              clients.ProjectID,
		region:                 clients.Region,
		neutron:                neutron.NewService(clients.Network),
		octavia:                octavia.NewService(clients.LoadBalancer),
		topology:               topology.NewResolver(clients, cfg.OperationTimeout),
		operationTimeout:       cfg.OperationTimeout,
		pollInterval:           cfg.PollInterval,

		httpsListenerIsolation: cfg.HTTPSListenerIsolation,
	}
}

var _ cloud.GraphProvider = (*Provider)(nil)

// BackendTLSAvailable discovers support on the authenticated regional Amphora endpoint.
func (p *Provider) BackendTLSAvailable(ctx context.Context) (bool, error) {
	if !p.backendTLSPolicy || p.backendCA == nil {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityBackendTLSHostname)
}

// HTTPSListenerIsolationAvailable checks the configured region before the
// Kubernetes controller accepts virtual HTTPS listeners sharing a port.
func (p *Provider) HTTPSListenerIsolationAvailable(ctx context.Context) (bool, error) {
	if !p.httpsListenerIsolation {
		return false, nil
	}
	return p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityHTTPSListenerIsolation)
}

func (p *Provider) amphoraCapabilityAvailable(ctx context.Context, capability string) (bool, error) {
	err := p.octavia.VerifyGatewayCapabilities(ctx, "amphora", capability)
	var missing *octavia.MissingGatewayCapability
	if errors.As(err, &missing) {
		return false, nil
	}
	return err == nil, err
}

func (p *Provider) verifyGatewayHTTPCapabilities(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, required []string) error {
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	if err := p.octavia.VerifyGatewayCapabilities(ctx, provider, required...); err != nil {
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
