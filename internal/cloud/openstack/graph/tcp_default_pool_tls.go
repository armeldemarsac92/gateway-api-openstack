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
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const tcpDefaultPoolTLSTag = "gateway-api-openstack.tcp-default-pool-tls=v1"

var errTCPDefaultPoolTLS = errors.New("TCP default pool TLS")

type tcpDefaultPoolTLSPlan struct {
	step       *routeMutation
	issue      string
	verifiedCA string
}

func (p *Provider) TCPDefaultPoolTLSAvailable(ctx context.Context) (bool, error) {
	if !p.tcpDefaultPoolTLS || !p.backendTLSPolicy || p.backendCA == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	available, err := p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityTCPDefaultPoolTLS, octavia.GatewayCapabilityBackendTLSHostname)
	if isNotFound(err) {
		return false, nil
	}
	return available, err
}

func hasTCPDefaultPoolTLSTarget(desired desiredGatewayGraph) bool {
	return desired.protocol == cloud.ListenerProtocolTCP &&
		slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return route.spec.BackendTLS != nil })
}

func (route observedRouteGraph) managedTCPDefaultPoolTLS() bool {
	return route.pool != nil && route.pool.Protocol == cloud.ListenerProtocolTCP && slices.Contains(route.pool.Tags, tcpDefaultPoolTLSTag)
}

func (actual observedGatewayGraph) listenerHasManagedTCPDefaultPoolTLS(listenerID string) bool {
	for _, route := range actual.routes {
		if route.managedTCPDefaultPoolTLS() {
			for _, listener := range servingPoolListeners(actual, route.pool.ID) {
				if listener.ID == listenerID {
					return true
				}
			}
		}
	}
	return false
}

func (actual observedGatewayGraph) tcpDefaultPoolTLSMatches(listenerID string, route desiredRoute) bool {
	return actual.tcpDefaultPoolTLS[listenerID] == (route.spec.BackendTLS != nil)
}

func (actual observedGatewayGraph) canAttachTCPDefaultPoolTLS(listenerID string, route desiredRoute) bool {
	if route.spec.BackendTLS == nil {
		return true
	}
	_, known := actual.tcpDefaultPoolTLS[listenerID]
	return actual.defaultPoolsObserved && known
}

// Snapshot the planned transport before any member batch or ordinary graph
// mutation. Closed or obsolete defaults are withdrawn even if another
// dependency prevents the Gateway from converging.
func (p *Provider) prepareTCPDefaultPoolTLS(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if desired.protocol != cloud.ListenerProtocolTCP || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	if hasTCPDefaultPoolTLSTarget(*desired) && (actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora") {
		desired.tcpDefaultPoolTLSAvailable, _ = p.TCPDefaultPoolTLSAvailable(ctx)
	}
	var withdrawal *graphMutation
	for index := range desired.routes {
		route := &desired.routes[index]
		node := desired.spec.Routes[index]
		var observed observedRouteGraph
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		if node.Disposition == cloud.GraphEnsure || len(node.RetainListenerPorts) > 0 {
			route.tcpDefaultPoolTLS = p.observeTCPDefaultPoolTLS(ctx, *route, observed, desired.tcpDefaultPoolTLSAvailable, desired.spec.Gateway.BackendCAOwned)
		}
		if withdrawal != nil || observed.pool == nil || actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
			continue
		}
		for _, listener := range servingPoolListeners(actual, observed.pool.ID) {
			if !observed.managedTCPDefaultPoolTLS() && route.spec.BackendTLS == nil && !actual.tcpDefaultPoolTLS[listener.ID] {
				continue
			}
			keep := node.Disposition == cloud.GraphEnsure ||
				node.Disposition == cloud.GraphPreserve && slices.Contains(node.RetainListenerPorts, listener.ProtocolPort)
			if keep && route.tcpDefaultPoolTLS.issue == "" && route.tcpDefaultPoolTLS.step == nil &&
				observed.pool.AdminStateUp && actual.tcpDefaultPoolTLSMatches(listener.ID, *route) {
				continue
			}
			if listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" {
				continue
			}
			step := clearListenerDefaultPool(actual.protocol)
			step.listenerID, step.routeIndex = listener.ID, index
			withdrawal = &step
			break
		}
		if withdrawal == nil && observed.managedTCPDefaultPoolTLS() {
			plan := route.tcpDefaultPoolTLS
			if plan.step != nil && plan.step.kind == routeMutationSetBackendTLS {
				withdrawal = &graphMutation{kind: graphRouteMutation, routeIndex: index, route: *plan.step, message: plan.step.message()}
			}
		}
	}
	return withdrawal
}

// A failed parent still permits removal of a proven TLS default. Listener
// and pool ownership are sufficient for this restrictive write. Do not depend
// on member, monitor or network inventory while retiring the association.
func (p *Provider) observeFailedTCPDefaultPoolTLSWithdrawal(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) (*graphMutation, error) {
	lb := actual.loadBalancer
	deletingTCPRoute := (desired.protocol == "" || desired.protocol == cloud.ListenerProtocolTCP) &&
		desired.spec.Gateway.Disposition == cloud.GraphPreserve && slices.ContainsFunc(desired.spec.Routes, func(node cloud.RouteGraphNode) bool {
		return node.Disposition == cloud.GraphDelete && node.Spec.GatewayRequirements.ListenerProtocol == cloud.ListenerProtocolTCP
	})
	ensuringTCP := desired.protocol == cloud.ListenerProtocolTCP && desired.spec.Gateway.Disposition == cloud.GraphEnsure
	if lb == nil || lb.ProvisioningStatus != "ERROR" || lb.Provider != "amphora" ||
		!ensuringTCP && !deletingTCPRoute || len(desired.routes) == 0 || hasSNIProfile(lb.Tags) {
		return nil, nil
	}

	inventory, err := p.observeGatewayListenerInventory(ctx, desired, actual)
	if err != nil {
		return nil, err
	}
	if err := validateRawTLSDefault(inventory); err != nil {
		return nil, err
	}
	if err := requireListenerDefaultPoolInventory(lb.ID, inventory); err != nil {
		return nil, err
	}
	anchor := desired.spec.Gateway.Spec.ListenerPort
	var owned []listeners.Listener
	if actual.scoped {
		owned, err = p.managedScopedListeners(desired.identity, lb, inventory.Items, cloud.ListenerProtocolTCP, anchor)
		if err != nil {
			return nil, err
		}
		actual.scopedListeners = owned
		actual.state.ListenerID = scopedRepresentativeListenerID(owned, anchor)
	} else {
		listener, err := p.managedGatewayListener(desired.identity, lb.ID, inventory.Items)
		if err != nil {
			return nil, err
		}
		if listener == nil || listener.Protocol != cloud.ListenerProtocolTCP || listener.ProjectID != p.projectID ||
			anchor != 0 && listener.ProtocolPort != anchor {
			return nil, nil
		}
		if err := validateDefaultPoolListener(listener, lb.ID); err != nil {
			return nil, err
		}
		actual.listener, owned = listener, []listeners.Listener{*listener}
		actual.state.ListenerID = listener.ID
	}
	if !slices.ContainsFunc(owned, failedTCPDefaultPoolMayBeWithdrawn) {
		return nil, nil
	}

	poolInventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return nil, err
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, poolInventory.Items, func(pool pools.Pool) string { return pool.ID }); err != nil {
		return nil, err
	}
	available := false
	if hasTCPDefaultPoolTLSTarget(desired) {
		available, _ = p.TCPDefaultPoolTLSAvailable(ctx)
	}
	for _, listener := range owned {
		if !failedTCPDefaultPoolMayBeWithdrawn(listener) {
			continue
		}
		index := slices.IndexFunc(poolInventory.Items, func(pool pools.Pool) bool { return pool.ID == listener.DefaultPoolID })
		if index == -1 {
			return nil, unprovenDefaultPool(&listener)
		}
		pool := &poolInventory.Items[index]
		observed := observedRouteGraph{pool: pool, backendTLS: poolInventory.BackendTLS[pool.ID], backendH2C: poolInventory.BackendH2C[pool.ID]}
		if !observed.managedTCPDefaultPoolTLS() {
			continue
		}
		if err := p.validateScopedDefaultPool(*pool, lb.ID, owned, cloud.ListenerProtocolTCP); err != nil {
			return nil, err
		}
		owner := slices.IndexFunc(desired.routes, func(route desiredRoute) bool {
			return route.identity.MatchesRoute(pool.Tags, rolePool)
		})
		if owner == -1 {
			return nil, unprovenDefaultPool(&listener)
		}
		node := desired.spec.Routes[owner]
		if deletingTCPRoute && node.Disposition != cloud.GraphDelete {
			continue
		}
		keep := node.Disposition == cloud.GraphEnsure && (!actual.scoped || slices.Contains(node.Spec.ScopedListenerPorts, listener.ProtocolPort)) ||
			node.Disposition == cloud.GraphPreserve && slices.Contains(node.RetainListenerPorts, listener.ProtocolPort)
		if keep && node.Spec.BackendTLS != nil {
			plan := p.observeTCPDefaultPoolTLS(ctx, desired.routes[owner], observed, available, desired.spec.Gateway.BackendCAOwned)
			if plan.verifiedCA != "" && actual.tcpDefaultPoolTLS[listener.ID] {
				continue
			}
		}
		step := clearListenerDefaultPool(cloud.ListenerProtocolTCP)
		step.listenerID, step.routeIndex, step.tcpDefaultPoolTLS = listener.ID, owner, true
		return &step, nil
	}
	return nil, nil
}

func failedTCPDefaultPoolMayBeWithdrawn(listener listeners.Listener) bool {
	return listener.DefaultPoolID != "" && (listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR")
}

func (p *Provider) observeTCPDefaultPoolTLS(ctx context.Context, route desiredRoute, observed observedRouteGraph, available, ownsCA bool) tcpDefaultPoolTLSPlan {
	tls := route.spec.BackendTLS
	pool := observed.pool
	switch {
	case tls == nil:
		if observed.managedTCPDefaultPoolTLS() {
			return retireTCPDefaultPoolTLS(observed)
		}
		return tcpDefaultPoolTLSPlan{}
	case !available:
		return holdTCPDefaultPoolTLS(observed, "Waiting for regional Octavia TCP default pool TLS support")
	case pool != nil && !observed.managedTCPDefaultPoolTLS():
		return retireTCPDefaultPoolTLS(observed)
	case !ownsCA:
		return tcpDefaultPoolTLSPlan{step: &routeMutation{kind: routeMutationClaimBackendCA}}
	}

	client := route.spec.TCPXBackendClientCertificateReference
	var preferred string
	if pool != nil {
		read := observed.backendTLS
		if !read.Complete() {
			return holdTCPDefaultPoolTLS(observed, "Waiting for complete Octavia TCP backend TLS readback")
		}
		if !observed.matchesTCPDefaultPoolTLS(route.spec) {
			return retireTCPDefaultPoolTLS(observed)
		}
		preferred = pool.CATLSContainerRef
	}
	revision, err := p.backendCA.Find(ctx, route.spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CADigest, preferred)
	if err != nil {
		return holdTCPDefaultPoolTLS(observed, "Waiting to verify the backend CA in Barbican")
	}
	if revision.Reference() == "" {
		if pool != nil {
			return retireTCPDefaultPoolTLS(observed)
		}
		return tcpDefaultPoolTLSPlan{step: &routeMutation{kind: routeMutationPublishBackendCA}}
	}
	if revision.Digest() != tls.CADigest {
		return holdTCPDefaultPoolTLS(observed, "Waiting for the authorized backend CA revision")
	}
	if pool == nil {
		settings := octavia.BackendTLSSettings{Hostname: tls.Hostname, CAReference: revision.Reference(), ClientCertificateReference: client}
		tags := backendTLSTags([]string{tcpDefaultPoolTLSTag}, true, backendTLSRevisionTag(tls), false, false)
		// TCPRoute observes and withdraws its own client certificate. The
		// HTTP-specific withdrawal does not own the TCP pool.
		return tcpDefaultPoolTLSPlan{step: &routeMutation{
			kind: routeMutationCreatePool, backendTLS: &settings, poolTags: &tags, poolAdmin: boolPointer(false),
		}}
	}
	if preferred != revision.Reference() {
		return retireTCPDefaultPoolTLS(observed)
	}
	return tcpDefaultPoolTLSPlan{verifiedCA: preferred}
}

func (observed observedRouteGraph) matchesTCPDefaultPoolTLS(spec cloud.RouteSpec) bool {
	if !observed.managedTCPDefaultPoolTLS() {
		return false
	}
	pool, read := observed.pool, observed.backendTLS
	tls := spec.BackendTLS
	revision := backendTLSRevisionTag(tls)
	// Octavia can persist regional ALPN defaults. The opaque TCP default
	// association omits them from the server and health-check configuration.
	return read.Complete() && pool.TLSEnabled && read.Hostname == tls.Hostname &&
		pool.TLSContainerRef == spec.TCPXBackendClientCertificateReference && pool.CRLContainerRef == "" &&
		!observed.backendH2C && !read.RequireH2 && !hasUnclearedSystemCA(*pool, read) &&
		!hasUnclearedBackendTLSSubjectAltNames(*pool, read) && slices.Contains(pool.Tags, revision) &&
		!differentBackendTLSRevision(pool.Tags, revision) && pool.ProvisioningStatus != "ERROR"
}

func holdTCPDefaultPoolTLS(observed observedRouteGraph, issue string) tcpDefaultPoolTLSPlan {
	if observed.managedTCPDefaultPoolTLS() {
		if step := disableDetachedTCPPool(observed); step != nil {
			return tcpDefaultPoolTLSPlan{step: step}
		}
	}
	return tcpDefaultPoolTLSPlan{issue: issue}
}

func disableDetachedTCPPool(observed observedRouteGraph) *routeMutation {
	pool := observed.pool
	if pool.Listeners == nil || len(pool.Listeners) != 0 || !observed.backendTLS.AdminObserved || !pool.AdminStateUp ||
		pool.ProvisioningStatus != "ACTIVE" && pool.ProvisioningStatus != "ERROR" {
		return nil
	}
	return &routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID, poolAdmin: boolPointer(false)}
}

func retireTCPDefaultPoolTLS(observed observedRouteGraph) tcpDefaultPoolTLSPlan {
	if observed.pool.Listeners == nil || len(observed.pool.Listeners) != 0 {
		return tcpDefaultPoolTLSPlan{issue: "Waiting for the superseded Octavia TCP pool to detach"}
	}
	if issue := observed.backendProvisioningIssue(true, true); issue != "" {
		return tcpDefaultPoolTLSPlan{issue: issue}
	}
	if !observed.backendTLS.AdminObserved {
		return tcpDefaultPoolTLSPlan{issue: "Waiting for Octavia TCP pool admin state readback"}
	}
	if step := disableDetachedTCPPool(observed); step != nil {
		return tcpDefaultPoolTLSPlan{step: step}
	}
	steps := buildRouteDeletionPlan(observed)
	return tcpDefaultPoolTLSPlan{step: &steps[0]}
}

func (route desiredRoute) planTCPDefaultPoolTLS(actual observedRouteGraph) (*routeMutation, error) {
	if route.protocol() != cloud.ListenerProtocolTCP || route.spec.BackendTLS == nil && !actual.managedTCPDefaultPoolTLS() {
		return nil, nil
	}
	plan := route.tcpDefaultPoolTLS
	if plan.step != nil {
		return plan.step, nil
	}
	if plan.issue != "" {
		return nil, fmt.Errorf("%w: %s", errTCPDefaultPoolTLS, plan.issue)
	}
	if route.spec.BackendTLS == nil || plan.verifiedCA == "" || !actual.matchesTCPDefaultPoolTLS(route.spec) ||
		actual.pool.CATLSContainerRef != plan.verifiedCA {
		return nil, fmt.Errorf("%w: waiting for verified Octavia TCP backend TLS configuration", errTCPDefaultPoolTLS)
	}
	return nil, nil
}
