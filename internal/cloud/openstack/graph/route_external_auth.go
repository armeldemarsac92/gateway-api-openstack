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
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"golang.org/x/net/http/httpguts"
)

const routeExternalAuthTag = "gw-api-route-http-external-auth-v1"

var routeExternalAuthPolicy = protectedHTTPPredicate{
	capability: octavia.GatewayCapabilityPolicyExternalAuth,
	label:      "HTTPRoute external authorization",
	required:   func(wanted desiredPolicy) bool { return wanted.externalAuth != nil },
	observed:   observedRoutePolicy.hasExternalAuth,
	settled:    desiredPolicy.externalAuthMatches,
	additionalCapabilities: func(policy desiredPolicy) []string {
		if policy.weighted {
			return []string{octavia.GatewayCapabilityWeightedHTTPTargets, octavia.GatewayCapabilityFixedHTTPResponses}
		}
		return nil
	},
}

func validateRouteAuthorizer(auth cloud.ListenerExternalAuth) error {
	if auth.ServiceUID == "" || auth.MemberSubnetID == "" || auth.ServicePort < 1 || auth.ServicePort > 65535 ||
		len(auth.Members) == 0 || !cloud.ValidExternalAuthPathPrefix(auth.PathPrefix) {
		return errors.New("HTTPRoute authorization requires a resolved Service, port, endpoints and valid path prefix")
	}
	for _, headers := range [][]string{auth.AllowedRequestHeaders, auth.AllowedResponseHeaders} {
		if len(headers) > 64 || slices.ContainsFunc(headers, func(name string) bool { return len(name) > 256 || !httpguts.ValidHeaderFieldName(name) }) {
			return errors.New("invalid HTTPRoute authorization header allowlist")
		}
	}
	for _, member := range auth.Members {
		ip, err := netip.ParseAddr(member.Address)
		if err != nil || ip.Zone() != "" || member.Draining || member.Port < 1 || member.Port > 65535 {
			return errors.New("invalid HTTPRoute authorization Service endpoint")
		}
	}
	return nil
}

func policyAuthorizer(poolID string, auth cloud.ListenerExternalAuth) *octavia.ListenerHTTPExternalAuth {
	return &octavia.ListenerHTTPExternalAuth{PoolID: poolID, PathPrefix: auth.PathPrefix,
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
		a.PoolID == b.PoolID && a.PathPrefix == b.PathPrefix && slices.Equal(a.AllowedRequestHeaders, b.AllowedRequestHeaders) &&
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
					pools[listenerAuthPoolName(port, *policy.externalAuth)] = policy.externalAuth
				}
			}
		}
	}
	return pools
}

func (actual *observedGatewayGraph) planRouteAuthPools(wanted map[string]*cloud.ListenerExternalAuth, owned map[string]observedAuthPool, referenced map[string]bool, allObserved bool) {
	if len(wanted) == 0 {
		return
	}
	actual.routeAuthPools = make(map[string]string, len(wanted))
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		pool := owned[name]
		canRemove := allObserved && !referenced[pool.pool.ID] && len(pool.pool.Listeners) == 0
		ready, step := listenerAuthPoolPlan(*wanted[name], name, pool, canRemove)
		if ready {
			actual.routeAuthPools[name] = pool.pool.ID
		} else if step != nil && actual.routeAuthPoolMutation == nil {
			actual.routeAuthPoolMutation = step
		}
	}
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
func (p *Provider) observeRouteAuthReferences(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) (map[string]map[string]bool, error) {
	refs := make(map[string]map[string]bool)
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
					refs[id] = make(map[string]bool)
				}
				refs[id][listener.ID] = true
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
	return p.protectHTTPURLPolicyTraffic(ctx, desired, actual, plan, result, routeExternalAuthPolicy, p.routeExternalAuth,
		func(_ desiredRoute, step routeMutation) bool {
			return step.policy.externalAuth != nil && (step.kind == routeMutationCreatePolicy || step.kind == routeMutationEnablePolicy)
		})
}

// A fixed response can replace an obsolete authorization policy while its
// frontend is closed if it cannot send the original request or a mirror.
func (route desiredRoute) rejectsWithoutForwarding() bool {
	return route.fixedResponse && !route.spec.GRPC && !slices.ContainsFunc(route.policies, func(policy desiredPolicy) bool {
		return policy.externalAuth != nil || len(policy.requestMirrors) != 0 || len(policy.resolvedRequestMirrors) != 0
	})
}
