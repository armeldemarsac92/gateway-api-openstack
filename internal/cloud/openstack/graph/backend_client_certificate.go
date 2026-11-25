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
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

// This path only disables owned pools, drains their owned members and clears
// stale identities. It operates even when the Kubernetes policy list failed.
func (p *Provider) withdrawBackendClientCertificate(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	if len(spec.Routes) != 0 || spec.ValidateMutation == nil || !spec.Gateway.BackendCAOwned ||
		spec.Identity.RouteUID != "" || spec.Identity.RouteName != "" || spec.Identity.RouteNamespace != "" ||
		cloud.NormalizeListenerProtocol(spec.Gateway.Spec.ListenerProtocol) != cloud.ListenerProtocolHTTP ||
		spec.Gateway.Spec.Identity != spec.Identity {
		return cloud.GatewayGraphResult{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("backend client certificate withdrawal requires an owned HTTP Gateway binding"))
	}
	result := func(outcome cloud.Outcome) cloud.GatewayGraphResult {
		return cloud.GatewayGraphResult{Outcome: outcome, Gateway: cloud.GatewayResult{Outcome: outcome}}
	}
	balancer, err := p.findGatewayLoadBalancer(ctx, identity)
	if balancer == nil || err != nil {
		return result(cloud.ReadyOutcome()), err
	}
	observed, err := p.observeLoadBalancerOnce(ctx, balancer.ID)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	if observed.phase == loadBalancerPhaseAbsent {
		return result(cloud.ReadyOutcome()), nil
	}
	lb := observed.loadBalancer
	if !identity.MatchesGateway(balancer.Tags, roleLoadBalancer) || lb.ID != balancer.ID || lb.ProjectID != p.projectID || lb.Provider != "amphora" ||
		!identity.MatchesGateway(lb.Tags, roleLoadBalancer) || slices.Contains(lb.Tags, sniProfileTag) || slices.Contains(lb.Tags, tcpTLSProfileTag) {
		return cloud.GatewayGraphResult{}, fmt.Errorf("%w: backend client certificate withdrawal could not prove the HTTP load balancer", cloud.ErrOwnershipConflict)
	}
	if observed.phase != loadBalancerPhaseActive {
		return result(observed.outcome), nil
	}
	inventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	slices.SortFunc(inventory.Items, func(a, b pools.Pool) int { return strings.Compare(a.ID, b.ID) })
	plan := backendTLSPlan{phase: backendTLSDenied, clearClientCertificate: true}
	apply := func(step *routeMutation) (cloud.GatewayGraphResult, error) {
		if err := spec.ValidateMutation(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		outcome := p.progressingOutcome(step.message())
		err := p.executeRouteMutation(ctx, desiredRoute{}, *step)
		outcome.MutationSubmitted = err == nil
		return result(outcome), err
	}
	type candidate struct {
		graph observedRouteGraph
		proof openstackidentity.RouteOwnerProof
		role  routeResourceRole
	}
	var closed []candidate
	var blocked error
	var pending bool
	for i, pool := range inventory.Items {
		tls := inventory.BackendTLS[pool.ID]
		if !slices.Contains(pool.Tags, backendTLSManagedTag) || !staleBackendClientCertificate(pool, tls, spec.Gateway.BackendClientCertificate) {
			continue
		}
		role, validRole := readRouteResourceRole(pool.Tags, rolePool)
		proof, owned := identity.RouteOwner(pool.Tags, role.raw)
		if !validRole || !owned || pool.ProjectID != p.projectID || validateBoundPoolParent(pool, lb.ID, cloud.ListenerProtocolHTTP) != nil ||
			i > 0 && inventory.Items[i-1].ID == pool.ID || i+1 < len(inventory.Items) && inventory.Items[i+1].ID == pool.ID {
			blocked = errors.Join(blocked, fmt.Errorf("%w: TLS pool %s could not be proven for backend client certificate withdrawal", cloud.ErrOwnershipConflict, pool.ID))
			continue
		}
		actual := observedRouteGraph{pool: &pool, backendTLS: tls}
		if step := plan.withdrawal(true, false, actual); step != nil && step.poolAdmin != nil {
			if !step.retry {
				return apply(step)
			}
			// Retry ERROR pools after fresh, independently available withdrawals.
			pending = true
		}
		closed = append(closed, candidate{graph: actual, proof: proof, role: role})
	}
	for _, pool := range closed {
		if pool.graph.pool.ProvisioningStatus != "ACTIVE" || !pool.graph.backendTLS.AdminObserved || pool.graph.pool.AdminStateUp {
			pending = true
			continue
		}
		owner := routeGraphOwner{identity: identity, recovery: &pool.proof}
		if err := p.observeRoutePoolChildrenWithRole(ctx, owner, &pool.graph, pool.role.backend); err != nil {
			blocked = errors.Join(blocked, err)
			continue
		}
		if err := pool.graph.validateScopedHTTPPoolChildren(); err != nil {
			blocked = errors.Join(blocked, err)
			continue
		}
		if step := plan.withdrawal(true, false, pool.graph); step != nil {
			return apply(step)
		}
		pending = pending || !pool.graph.backendTLS.ClientRefObserved
	}
	if pending {
		for _, pool := range closed {
			if step := plan.withdrawal(true, false, pool.graph); step != nil && step.retry {
				return apply(step)
			}
		}
	}
	if blocked != nil {
		return cloud.GatewayGraphResult{}, blocked
	}
	if pending {
		return result(p.progressingOutcome("Waiting to verify backend client certificate withdrawal")), nil
	}
	return result(cloud.ReadyOutcome()), nil
}

func staleBackendClientCertificate(pool pools.Pool, tls octavia.PoolBackendTLS, authorized cloud.BackendClientCertificate) bool {
	if slices.Contains(pool.Tags, xBackendTLSClientTag) {
		return false
	}
	if !tls.ClientRefObserved {
		return slices.Contains(pool.Tags, backendTLSClientTag)
	}
	return pool.TLSContainerRef != "" && (authorized.Denied || pool.TLSContainerRef != authorized.Reference)
}
