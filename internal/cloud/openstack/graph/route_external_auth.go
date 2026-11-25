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

package graph

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const routeExternalAuthTag = "gw-api-route-http-external-auth-v1"
const grpcAuthPoolTag = "gw-api-grpc-auth-pool-v1"
const grpcAuthPoolNamePrefix = "gateway-api-grpc-auth-"

var routeExternalAuthPolicy = routeAuthorizerPredicate(octavia.GatewayCapabilityPolicyExternalAuth, "")
var routeExternalAuthGRPCPolicy = routeAuthorizerPredicate(octavia.GatewayCapabilityPolicyExternalAuthGRPC, cloud.ExternalAuthProtocolGRPC)

func isRouteAuthorizerCapability(capability string) bool {
	return capability == octavia.GatewayCapabilityPolicyExternalAuth || capability == octavia.GatewayCapabilityPolicyExternalAuthGRPC
}

func routeAuthorizerPredicate(capability, protocol string) protectedHTTPPredicate {
	return protectedHTTPPredicate{
		capability: capability,
		label:      "HTTPRoute external authorization",
		required: func(wanted desiredPolicy) bool {
			return wanted.externalAuth != nil && wanted.externalAuth.Protocol == protocol
		},
		observed: func(observed observedRoutePolicy) bool {
			return observed.hasExternalAuth() && (observed.externalAuth == nil || observed.externalAuth.Protocol == protocol)
		},
		settled: desiredPolicy.externalAuthMatches,
		additionalCapabilities: func(policy desiredPolicy) []string {
			return routeAuthorizerCapabilities(*policy.externalAuth, policy.weighted)[1:]
		},
	}
}

func (p *Provider) routeAuthorizerEnabled(auth cloud.ListenerExternalAuth) bool {
	switch auth.Protocol {
	case "":
		return p.routeExternalAuth
	case cloud.ExternalAuthProtocolGRPC:
		return p.routeExternalAuthGRPC
	default:
		return false
	}
}

func routeAuthorizerCapabilities(auth cloud.ListenerExternalAuth, weighted bool) []string {
	required := []string{octavia.GatewayCapabilityPolicyExternalAuth, octavia.GatewayCapabilityFixedHTTPResponses}
	if auth.Protocol == cloud.ExternalAuthProtocolGRPC {
		required = grpcAuthorizerCapabilities(auth.BackendTLS)
	}
	if weighted {
		required = append(required, octavia.GatewayCapabilityWeightedHTTPTargets)
	}
	return required
}

func grpcAuthorizerCapabilities(tls *cloud.BackendTLSValidation) []string {
	required := []string{octavia.GatewayCapabilityPolicyExternalAuthGRPC, octavia.GatewayCapabilityFixedHTTPResponses}
	if tls == nil {
		return append(required, octavia.GatewayCapabilityBackendH2C)
	}
	required = append(required, backendTLSCapabilities(true, len(tls.SubjectAltNames) != 0)...)
	if tls.Trust == cloud.BackendTLSTrustSystem {
		required = append(required, octavia.GatewayCapabilityBackendTLSSystemCA)
	}
	return required
}

func routeAuthorizerPoolName(port int, auth cloud.ListenerExternalAuth, certificate string) string {
	if auth.Protocol != cloud.ExternalAuthProtocolGRPC {
		return listenerAuthPoolName(port, auth)
	}
	key := fmt.Appendf(nil, "%d:%s:%d", port, auth.ServiceUID, auth.ServicePort)
	if tls := auth.BackendTLS; tls != nil {
		key = fmt.Appendf(key, "|%s|%s|%v", tls.Hostname, backendTLSRevisionTag(tls), tls.SubjectAltNames)
		if certificate != "" {
			key = fmt.Appendf(key, "|%s", certificate)
		}
	}
	digest := sha256.Sum256(key)
	return fmt.Sprintf("%s%x", grpcAuthPoolNamePrefix, digest[:16])
}

func routeAuthorizerPoolTag(protocol string) string {
	if protocol == cloud.ExternalAuthProtocolGRPC {
		return grpcAuthPoolTag
	}
	return listenerAuthPoolTag
}

func validateRouteAuthorizer(auth cloud.ListenerExternalAuth) error {
	if auth.Protocol == cloud.ExternalAuthProtocolGRPC && (auth.PathPrefix != "" || len(auth.AllowedResponseHeaders) != 0) ||
		auth.BackendTLS != nil && (auth.Protocol != cloud.ExternalAuthProtocolGRPC || !auth.BackendTLS.RequireH2 || auth.BackendTLS.Denied || !validBackendTLS(auth.BackendTLS)) {
		return errors.New("invalid HTTPRoute authorization protocol or transport")
	}
	if auth.ServiceUID == "" || auth.MemberSubnetID == "" || auth.ServicePort < 1 || auth.ServicePort > 65535 ||
		len(auth.Members) == 0 || !cloud.ValidExternalAuthPathPrefix(auth.PathPrefix) {
		return errors.New("HTTPRoute authorization requires a resolved Service, port, endpoints and valid path prefix")
	}
	if !cloud.ValidExternalAuthHeaders(auth.AllowedRequestHeaders, auth.Protocol == cloud.ExternalAuthProtocolGRPC) ||
		!cloud.ValidExternalAuthHeaders(auth.AllowedResponseHeaders, false) {
		return errors.New("invalid HTTPRoute authorization header allowlist")
	}
	for _, member := range auth.Members {
		ip, err := netip.ParseAddr(member.Address)
		if err != nil || ip.Zone() != "" || member.Draining || member.Port < 1 || member.Port > 65535 {
			return errors.New("invalid HTTPRoute authorization Service endpoint")
		}
	}
	return nil
}

func policyAuthorizer(poolID string, auth cloud.ListenerExternalAuth) *octavia.PolicyExternalAuth {
	return &octavia.PolicyExternalAuth{Protocol: auth.Protocol, PoolID: poolID, PathPrefix: auth.PathPrefix,
		AllowedRequestHeaders: auth.AllowedRequestHeaders, AllowedResponseHeaders: auth.AllowedResponseHeaders}
}

func (observed observedRoutePolicy) hasExternalAuth() bool {
	return observed.externalAuthPresent || observed.externalAuthTagged
}

func (observed observedRoutePolicy) externalAuthReadable() bool {
	return !observed.hasExternalAuth() || observed.externalAuthReported && (!observed.externalAuthPresent || observed.externalAuth != nil)
}

func (wanted desiredPolicy) externalAuthMatches(observed observedRoutePolicy) bool {
	if wanted.externalAuth == nil {
		return !observed.hasExternalAuth()
	}
	a, b := wanted.resolvedExternalAuth, observed.externalAuth
	return a != nil && b != nil && observed.externalAuthTagged && observed.externalAuthReported && observed.externalAuthPresent &&
		a.Protocol == b.Protocol && a.PoolID == b.PoolID && a.PathPrefix == b.PathPrefix && slices.Equal(a.AllowedRequestHeaders, b.AllowedRequestHeaders) &&
		slices.Equal(a.AllowedResponseHeaders, b.AllowedResponseHeaders)
}

func (d desiredGatewayGraph) routeAuthorizers() map[string]*cloud.ListenerExternalAuth {
	var pools map[string]*cloud.ListenerExternalAuth
	if d.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	for i, route := range d.routes {
		if d.spec.Routes[i].Disposition != cloud.GraphEnsure {
			continue
		}
		for _, part := range route.httpParts {
			port := httpURLPolicyRoutePort(d, part)
			for _, policy := range part.policies {
				if policy.externalAuth != nil {
					if pools == nil {
						pools = make(map[string]*cloud.ListenerExternalAuth)
					}
					pools[routeAuthorizerPoolName(port, *policy.externalAuth, d.spec.Gateway.BackendClientCertificate.Reference)] = policy.externalAuth
				}
			}
		}
	}
	return pools
}

func (p *Provider) planRouteAuthPools(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, wanted map[string]*cloud.ListenerExternalAuth, owned map[string]observedAuthPool, referenced map[string]bool, allObserved bool) {
	if len(wanted) == 0 {
		return
	}
	actual.routeAuthPools = make(map[string]string, len(wanted))
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		pool := owned[name]
		canRemove := allObserved && !referenced[pool.pool.ID] && len(pool.pool.Listeners) == 0
		auth := wanted[name]
		var ready bool
		var step *graphMutation
		if auth.Protocol == cloud.ExternalAuthProtocolGRPC {
			ready, step = p.planGRPCAuthorizerPool(ctx, desired, *actual, auth, name, pool, canRemove)
		} else {
			ready, step = listenerAuthPoolPlan(*auth, name, pool, canRemove)
		}
		if ready {
			actual.routeAuthPools[name] = pool.pool.ID
		} else if step != nil && actual.routeAuthPoolMutation == nil {
			actual.routeAuthPoolMutation = step
		}
	}
}

func (p *Provider) planGRPCAuthorizerPool(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, auth *cloud.ListenerExternalAuth,
	name string, pool observedAuthPool, canRemove bool) (bool, *graphMutation) {
	tls := auth.BackendTLS
	client := desired.spec.Gateway.BackendClientCertificate
	if !p.routeExternalAuthGRPC || tls != nil && (!p.backendTLSPolicy || !p.poolALPN || client.Denied) ||
		p.verifyGatewayHTTPURLCapabilities(ctx, desired, actual, grpcAuthorizerCapabilities(tls)) != nil {
		return false, nil
	}

	change := listenerFilterMutation{grpcAuth: auth}
	if tls != nil && (tls.Trust == cloud.BackendTLSTrustReference || client.Reference != "") && !desired.spec.Gateway.BackendCAOwned {
		if desired.spec.Gateway.ClaimBackendCA == nil {
			return false, nil
		}
		change.claimCA = true
		return false, &graphMutation{kind: graphListenerFiltersMutation, listenerFilters: change,
			message: "Reserving ownership of the gRPC authorizer trust or client identity"}
	}
	if tls != nil && tls.Trust == cloud.BackendTLSTrustReference {
		if p.backendCA == nil {
			return false, nil
		}
		revision, err := p.backendCA.Find(ctx, desired.spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CADigest, pool.pool.CATLSContainerRef)
		if err != nil {
			return false, nil
		}
		if revision.Reference() == "" {
			change.publishCA = true
			return false, &graphMutation{kind: graphListenerFiltersMutation, listenerFilters: change,
				message: "Publishing the gRPC authorizer CA to Barbican"}
		}
		if revision.Digest() != tls.CADigest {
			return false, nil
		}
		change.caReference = revision.Reference()
	}

	if pool.pool.ID != "" {
		if pool.pool.ProvisioningStatus == "ACTIVE" && !pool.admin || strings.HasPrefix(pool.pool.ProvisioningStatus, "PENDING_") {
			return false, nil
		}
		if pool.pool.ProvisioningStatus == "ERROR" || !grpcAuthorizerTransportMatches(pool, tls, change.caReference, client.Reference) {
			if canRemove {
				return false, listenerAuthPoolRemoval(pool)
			}
			return false, nil
		}
		if tls != nil && tls.Trust == cloud.BackendTLSTrustSystem && !pool.tls.SystemCAApplied() {
			return false, nil
		}
	}
	ready, step := listenerAuthPoolPlan(*auth, name, pool, canRemove)
	if step != nil && step.kind == graphListenerFiltersMutation {
		step.listenerFilters.grpcAuth = auth
		step.listenerFilters.caReference = change.caReference
	}
	return ready, step
}

func grpcAuthorizerTransportMatches(observed observedAuthPool, tls *cloud.BackendTLSValidation, caRef, clientRef string) bool {
	pool, current := observed.pool, observed.tls
	if pool.Protocol != cloud.ListenerProtocolHTTP || !current.EnabledObserved {
		return false
	}
	if tls == nil {
		return current.H2CObserved && observed.h2c && !hasBackendTLSSettings(pool, current) && len(pool.ALPNProtocols) == 0
	}
	trustMatches := caRef != "" && pool.CATLSContainerRef == caRef && !hasUnclearedSystemCA(pool, current)
	if tls.Trust == cloud.BackendTLSTrustSystem {
		trustMatches = configuredSystemCA(pool, current) && slices.Contains(pool.Tags, backendTLSSystemCATag)
	}
	return trustMatches && current.Complete() && pool.TLSEnabled && !observed.h2c && current.Hostname == tls.Hostname &&
		current.RequireH2Observed && current.RequireH2 && current.ALPNObserved && slices.Equal(pool.ALPNProtocols, []string{xBackendHTTP2ALPN}) &&
		pool.TLSContainerRef == clientRef && slices.Contains(pool.Tags, backendTLSClientTag) == (clientRef != "") &&
		pool.CRLContainerRef == "" && slices.Contains(pool.Tags, backendTLSRevisionTag(tls)) &&
		!differentBackendTLSRevision(pool.Tags, backendTLSRevisionTag(tls)) &&
		backendTLSSubjectAltNamesMatch(tls.SubjectAltNames, observedRouteGraph{pool: &pool, backendTLS: current})
}

func grpcAuthorizerPoolCreate(opts pools.CreateOpts, tls *cloud.BackendTLSValidation, caRef, clientRef string) octavia.CreatePoolOpts {
	if tls == nil {
		return octavia.CreatePoolOpts{CreateOpts: opts, BackendH2C: true}
	}
	opts.TLSEnabled = true
	opts.CATLSContainerRef = caRef
	opts.TLSContainerRef = clientRef
	opts.ALPNProtocols = []string{xBackendHTTP2ALPN}
	systemCA := tls.Trust == cloud.BackendTLSTrustSystem
	opts.Tags = backendTLSTags(opts.Tags, true, backendTLSRevisionTag(tls), clientRef != "", len(tls.SubjectAltNames) != 0)
	if systemCA {
		opts.Tags = append(opts.Tags, backendTLSSystemCATag)
	}
	return octavia.CreatePoolOpts{CreateOpts: opts, BackendTLSHostname: tls.Hostname, BackendTLSSubjectAltNames: tls.SubjectAltNames,
		BackendTLSRequireH2: true, BackendTLSSystemCA: systemCA}
}

func (actual *observedGatewayGraph) authListeners() []listeners.Listener {
	if !actual.scoped && actual.listener != nil {
		return []listeners.Listener{*actual.listener}
	}
	return actual.scopedListeners
}

// Pool.listeners includes authorization references even for a disabled policy.
// Keep them until the complete policy collection proves that the last reference
// was removed, including during a listener or route opt-out.
func (p *Provider) observeRouteAuthReferences(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) (map[string]map[string]string, error) {
	refs := make(map[string]map[string]string)
	for _, listener := range actual.authListeners() {
		if len(listener.L7Policies) == 0 && listener.L7Policies != nil {
			continue
		}
		rows, cached := actual.httpsIsolationPolicies[listener.ID]
		if !cached {
			rows = p.readHTTPSPolicyInventory(ctx, listener)
			if actual.httpsIsolationPolicies == nil {
				actual.httpsIsolationPolicies = make(map[string]httpsPolicyInventory)
			}
			actual.httpsIsolationPolicies[listener.ID] = rows
		}
		if !rows.complete || rows.err != nil {
			return nil, incompleteRouteAuthInventory(listener.ID, "", rows.err)
		}
		for _, row := range rows.items {
			if row.Policy.ProvisioningStatus == "PENDING_DELETE" || row.Policy.ProvisioningStatus == "DELETED" {
				continue
			}
			tagged := slices.Contains(row.Policy.Tags, routeExternalAuthTag)
			if !row.ExternalAuthPresent && !tagged {
				continue
			}
			role, valid := readRouteResourceRole(row.Policy.Tags, rolePolicyExact, rolePolicyPrefix)
			if _, owned := desired.identity.RouteOwner(row.Policy.Tags, role.raw); !valid || !owned ||
				row.Policy.ProjectID != p.projectID || row.Policy.ListenerID != listener.ID {
				return nil, fmt.Errorf("%w: unproven HTTPRoute authorization policy", cloud.ErrOwnershipConflict)
			}
			if !row.ExternalAuthReported || row.ExternalAuthPresent && row.ExternalAuth == nil {
				return nil, incompleteRouteAuthInventory(listener.ID, row.Policy.ID, nil)
			}
			if row.ExternalAuth != nil {
				id := row.ExternalAuth.PoolID
				if refs[id] == nil {
					refs[id] = make(map[string]string)
				}
				tag := routeAuthorizerPoolTag(row.ExternalAuth.Protocol)
				if previous := refs[id][listener.ID]; previous != "" && previous != tag {
					return nil, fmt.Errorf("%w: HTTPRoute authorization protocols share a pool", cloud.ErrOwnershipConflict)
				}
				refs[id][listener.ID] = tag
			}
		}
	}
	return refs, nil
}

func incompleteRouteAuthInventory(listenerID, policyID string, cause error) error {
	return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, &httpFilterInventoryError{
		listenerID: listenerID, policyID: policyID, filter: "HTTPRoute external authorization", cause: cause})
}

func (p *Provider) protectRouteAuthTraffic(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, plan []graphMutation, result *cloud.GatewayGraphResult) []graphMutation {
	for _, guard := range []struct {
		predicate protectedHTTPPredicate
		enabled   bool
	}{
		{routeExternalAuthPolicy, p.routeExternalAuth},
		{routeExternalAuthGRPCPolicy, p.routeExternalAuthGRPC},
	} {
		plan = p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, guard.predicate, guard.enabled,
			func(_ desiredRoute, step routeMutation) bool {
				return guard.predicate.required(step.policy) && (step.kind == routeMutationCreatePolicy || step.kind == routeMutationEnablePolicy)
			})
		if isHTTPListenerWithdrawal(plan) {
			break
		}
	}
	return plan
}

// A fixed response can replace an obsolete authorization policy while its
// frontend is closed if it cannot send the original request or a mirror.
func (route desiredRoute) rejectsWithoutForwarding() bool {
	return route.fixedResponse && !route.spec.GRPC && !slices.ContainsFunc(route.policies, func(policy desiredPolicy) bool {
		return policy.externalAuth != nil || len(policy.requestMirrors) != 0 || len(policy.resolvedRequestMirrors) != 0
	})
}
