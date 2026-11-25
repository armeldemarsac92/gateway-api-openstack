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
	"net/http"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const listenerAuthPoolTag = "gw-api-listener-auth-pool-v1"

func listenerAuthPoolName(port int, auth cloud.ListenerExternalAuth) string {
	digest := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%d", port, auth.ServiceUID, auth.ServicePort))
	return fmt.Sprintf("gateway-api-listener-auth-%x", digest[:16])
}

type observedAuthPool struct {
	pool    pools.Pool
	members []pools.Member
	admin   bool
	tls     octavia.PoolBackendTLS
	h2c     bool
}

type listenerFilterMutation struct {
	sections      []octavia.ListenerRequestFilterSection
	createPool    string
	createMember  *cloud.Member
	memberSubnet  string
	poolID        string
	enablePool    bool
	protectedAuth *cloud.ListenerExternalAuth
	claimCA       bool
	publishCA     bool
	caReference   string
}

func (p *Provider) observeListenerAuthPools(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, inventory octavia.PoolInventory) (map[string]observedAuthPool, error) {
	owned := make(map[string]observedAuthPool)
	poolsByID := make(map[string]pools.Pool)
	var routeRefs map[string]map[string]string
	if len(desired.routeAuthorizers()) != 0 || slices.ContainsFunc(inventory.Items, func(pool pools.Pool) bool {
		return isProtectedAuthPool(pool) || isListenerAuthPool(pool) && (p.routeExternalAuth || slices.ContainsFunc(pool.Listeners, func(parent pools.ListenerID) bool {
			return !listenerAuthPoolIDs(actual.requestFilters[parent.ID].Sections)[pool.ID]
		}))
	}) {
		var err error
		routeRefs, err = p.observeRouteAuthReferences(ctx, desired, actual)
		if err != nil {
			return nil, err
		}
	}
	for _, pool := range inventory.Items {
		if !isAuthorizationPool(pool) {
			continue
		}
		if err := p.validatePoolParent(pool, actual.loadBalancer.ID, rolePool, desired.identity); err != nil {
			return nil, err
		}
		tag, prefix := authorizationPoolRole(pool)
		readback, h2c := inventory.BackendTLS[pool.ID], inventory.BackendH2C[pool.ID]
		if tag == "" {
			return nil, fmt.Errorf("%w: authorization pool has conflicting roles", cloud.ErrOwnershipConflict)
		}
		if tag == listenerAuthPoolTag && (hasBackendTLSSettings(pool, readback) || hasObservedBackendTLS(pool, readback) || h2c) {
			return nil, fmt.Errorf("%w: cleartext HTTP authorization pool uses an unexpected transport", cloud.ErrOwnershipConflict)
		}
		if err := validateBoundPoolParent(pool, actual.loadBalancer.ID, cloud.ListenerProtocolHTTP); err != nil {
			return nil, err
		}
		if pool.ProjectID != p.projectID || poolsByID[pool.ID].ID != "" || !strings.HasPrefix(pool.Name, prefix) ||
			owned[pool.Name].pool.ID != "" || pool.MonitorID != "" {
			return nil, fmt.Errorf("%w: invalid authorization pool inventory", cloud.ErrOwnershipConflict)
		}
		for _, parent := range pool.Listeners {
			listenerReference := listenerAuthPoolIDs(actual.requestFilters[parent.ID].Sections)[pool.ID]
			protocol, routeReference := routeRefs[pool.ID][parent.ID]
			if listenerReference && tag != listenerAuthPoolTag || !listenerReference && (!routeReference || !authorizationPoolProtocolMatches(pool, protocol)) {
				return nil, fmt.Errorf("%w: authorization pool has an unproven listener reference", cloud.ErrOwnershipConflict)
			}
		}
		members, err := p.observeAuxiliaryHTTPPoolMembers(ctx, desired.identity, pool, tag)
		if err != nil {
			return nil, err
		}
		owned[pool.Name] = observedAuthPool{pool: pool, members: members, admin: readback.AdminObserved, tls: readback, h2c: h2c}
		poolsByID[pool.ID] = pool
	}
	for id, parents := range routeRefs {
		pool, found := poolsByID[id]
		if !found {
			return nil, fmt.Errorf("%w: HTTPRoute authorization references an unproven pool", cloud.ErrOwnershipConflict)
		}
		for listenerID, protocol := range parents {
			if !authorizationPoolProtocolMatches(pool, protocol) || !slices.Contains(pool.Listeners, pools.ListenerID{ID: listenerID}) {
				return nil, fmt.Errorf("%w: HTTPRoute authorization pool omits its listener", cloud.ErrOwnershipConflict)
			}
		}
	}
	return owned, nil
}

func (p *Provider) observeAuxiliaryHTTPPoolMembers(ctx context.Context, identity Identity, pool pools.Pool, tag string) ([]pools.Member, error) {
	members, missingAdmin, err := p.octavia.ListMembersWithAdminState(ctx, pool.ID, pools.ListMembersOpts{ProjectID: p.projectID})
	if err != nil {
		return nil, err
	}
	if len(missingAdmin) != 0 {
		return nil, fmt.Errorf("auxiliary pool member admin state is incomplete")
	}
	if err := crossCheckCompleteChildInventory("Gateway auxiliary pool", pool.ID, "member", pool.Members, members, func(member pools.Member) string { return member.ID }); err != nil {
		return nil, err
	}
	for _, member := range members {
		if member.ProjectID != p.projectID || member.PoolID != "" && member.PoolID != pool.ID ||
			!identity.MatchesGateway(member.Tags, roleMember) || !slices.Contains(member.Tags, tag) {
			return nil, fmt.Errorf("%w: Gateway auxiliary pool contains an unproven member", cloud.ErrOwnershipConflict)
		}
	}
	return members, nil
}

func listenerAuthPoolIDs(sections []octavia.ListenerRequestFilterSection) map[string]bool {
	ids := make(map[string]bool)
	for _, section := range sections {
		for _, request := range section.Requests {
			ids[request.HTTP.PoolID] = true
		}
	}
	return ids
}

func listenerAuthMemberPlan(auth cloud.ListenerExternalAuth, pool observedAuthPool) (waiting bool, change *graphMutation) {
	addresses := make(map[string]cloud.Member, len(auth.Members))
	memberKey := func(address string, port int) string { return fmt.Sprintf("%s:%d", address, port) }
	for _, member := range auth.Members {
		addresses[memberKey(member.Address, member.Port)] = member
	}
	for _, member := range pool.members {
		key := memberKey(member.Address, member.ProtocolPort)
		if strings.HasPrefix(member.ProvisioningStatus, "PENDING_") {
			return true, nil
		}
		if _, wanted := addresses[key]; !wanted || !member.AdminStateUp || member.ProvisioningStatus != "ACTIVE" || member.SubnetID != auth.MemberSubnetID {
			return false, &graphMutation{kind: graphDeleteChild, deletion: gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: member.ID, parentID: pool.pool.ID},
				message: "Removing an obsolete Octavia listener authorization member"}
		}
		delete(addresses, key)
	}
	for _, member := range auth.Members {
		if _, missing := addresses[memberKey(member.Address, member.Port)]; missing {
			return false, &graphMutation{kind: graphListenerFiltersMutation, message: "Creating an Octavia listener authorization member",
				listenerFilters: listenerFilterMutation{poolID: pool.pool.ID, createMember: &member, memberSubnet: auth.MemberSubnetID}}
		}
	}
	return false, nil
}

func listenerAuthPoolRemoval(pool observedAuthPool) *graphMutation {
	if strings.HasPrefix(pool.pool.ProvisioningStatus, "PENDING_") {
		return nil
	}
	step := gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: pool.pool.ID}
	if len(pool.members) != 0 {
		member := pool.members[0]
		if strings.HasPrefix(member.ProvisioningStatus, "PENDING_") {
			return nil
		}
		step = gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: member.ID, parentID: pool.pool.ID}
	}
	return &graphMutation{kind: graphDeleteChild, deletion: step, message: "Removing an Octavia listener authorization backend"}
}

func listenerAuthPoolPlan(auth cloud.ListenerExternalAuth, name string, pool observedAuthPool, canRemove bool) (bool, *graphMutation) {
	switch {
	case pool.pool.ID == "":
		return false, &graphMutation{kind: graphListenerFiltersMutation, message: "Creating an Octavia listener authorization pool",
			listenerFilters: listenerFilterMutation{createPool: name}}
	case pool.pool.ProvisioningStatus == "ERROR" && canRemove:
		return false, listenerAuthPoolRemoval(pool)
	case pool.pool.ProvisioningStatus != "ACTIVE" || !pool.admin:
		return false, nil
	case !pool.pool.AdminStateUp:
		return false, &graphMutation{kind: graphListenerFiltersMutation, message: "Re-enabling an Octavia listener authorization pool",
			listenerFilters: listenerFilterMutation{poolID: pool.pool.ID, enablePool: true}}
	}
	waiting, step := listenerAuthMemberPlan(auth, pool)
	return !waiting && step == nil, step
}

func desiredListenerFilterSections(port int, sections []cloud.ListenerRequestFilterSection, owned map[string]observedAuthPool, referenced map[string]bool, allReferencesObserved bool) ([]octavia.ListenerRequestFilterSection, []string, *graphMutation) {
	result := make([]octavia.ListenerRequestFilterSection, 0, len(sections))
	var recovering []string
	var next *graphMutation
	for _, section := range sections {
		configured := octavia.ListenerRequestFilterSection{Hostname: section.Hostname, Reject: section.Reject}
		for _, auth := range section.Requests {
			name := listenerAuthPoolName(port, auth)
			pool := owned[name]
			canRemove := allReferencesObserved && !referenced[pool.pool.ID] && len(pool.pool.Listeners) == 0
			ready, step := listenerAuthPoolPlan(auth, name, pool, canRemove)
			if !ready {
				configured.Reject, configured.Requests = true, nil
				recovering = append(recovering, section.Hostname)
				if next == nil {
					next = step
				}
				break
			}
			configured.Requests = append(configured.Requests, octavia.ListenerRequestFilter{
				Type: "ExternalAuth",
				HTTP: octavia.ListenerHTTPExternalAuth{PoolID: pool.pool.ID, PathPrefix: auth.PathPrefix,
					ForwardBodyMaxSize:    auth.ForwardBodyMaxSize,
					AllowedRequestHeaders: auth.AllowedRequestHeaders, AllowedResponseHeaders: auth.AllowedResponseHeaders},
			})
		}
		result = append(result, configured)
	}
	return result, recovering, next
}

func listenerFilterSectionsMatch(desired []octavia.ListenerRequestFilterSection, actual octavia.ListenerRequestFilterObservation, present bool) bool {
	if !present {
		return len(desired) == 0
	}
	if !actual.Readable || len(desired) != len(actual.Sections) {
		return false
	}
	return slices.EqualFunc(desired, actual.Sections, func(a, b octavia.ListenerRequestFilterSection) bool {
		return a.Hostname == b.Hostname && a.Reject == b.Reject && slices.EqualFunc(a.Requests, b.Requests, func(x, y octavia.ListenerRequestFilter) bool {
			return x.Type == y.Type && x.HTTP.PoolID == y.HTTP.PoolID && x.HTTP.PathPrefix == y.HTTP.PathPrefix &&
				x.HTTP.ForwardBodyMaxSize == y.HTTP.ForwardBodyMaxSize &&
				slices.Equal(x.HTTP.AllowedRequestHeaders, y.HTTP.AllowedRequestHeaders) &&
				slices.Equal(x.HTTP.AllowedResponseHeaders, y.HTTP.AllowedResponseHeaders)
		})
	})
}

func (actual observedGatewayGraph) hasListenerFilterConfig(desired desiredGatewayGraph) bool {
	return len(desired.spec.Gateway.Spec.RequestFilterSections) != 0 || slices.ContainsFunc(actual.scopedListeners, func(listener listeners.Listener) bool {
		value, present := actual.requestFilters[listener.ID]
		return present && (!value.Readable || len(value.Sections) != 0)
	})
}

type listenerFilterPoolInventory struct {
	loadBalancer *octavia.LoadBalancer
	routes       octavia.PoolInventory
	auth         map[string]observedAuthPool
}

func (p *Provider) observeListenerFilterPools(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) error {
	if actual.httpPools != nil {
		return nil
	}
	inventory, err := p.listGatewayPoolInventory(ctx, actual.loadBalancer)
	if err != nil {
		return err
	}
	owned, err := p.observeListenerAuthPools(ctx, desired, actual, inventory)
	if err != nil {
		return err
	}
	routeLoadBalancer := actual.loadBalancer
	if len(owned) != 0 {
		// The route observer verifies its own pool inventory. Keep the full
		// load balancer readback for other operations, including L7 activation.
		inventory.Items = slices.DeleteFunc(inventory.Items, isAuthorizationPool)
		lb := *actual.loadBalancer
		authIDs := make(map[string]bool, len(owned))
		for _, pool := range owned {
			authIDs[pool.pool.ID] = true
		}
		lb.Pools = slices.DeleteFunc(slices.Clone(lb.Pools), func(pool pools.Pool) bool { return authIDs[pool.ID] })
		routeLoadBalancer = &lb
	}
	actual.httpPools = &listenerFilterPoolInventory{loadBalancer: routeLoadBalancer, routes: inventory, auth: owned}
	return nil
}

func (p *Provider) observeListenerFilterGraph(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) error {
	err := p.observeListenerFilterPools(ctx, desired, actual)
	if err != nil {
		p.planListenerFilters(ctx, desired, actual, nil)
		return err
	}
	if actual.unverifiedListenerFilters == nil {
		p.planListenerFilters(ctx, desired, actual, actual.httpPools.auth)
	}
	return nil
}

func authorizationPoolRole(pool pools.Pool) (tag, prefix string) {
	if slices.Contains(pool.Tags, telemetryPoolTag) {
		return "", ""
	}
	for _, role := range []struct{ tag, prefix string }{
		{listenerAuthPoolTag, "gateway-api-listener-auth-"},
		{httpAuthTLSPoolTag, httpAuthTLSPoolNamePrefix},
		{grpcAuthPoolTag, grpcAuthPoolNamePrefix},
	} {
		if slices.Contains(pool.Tags, role.tag) {
			if tag != "" {
				return "", ""
			}
			tag, prefix = role.tag, role.prefix
		}
	}
	return tag, prefix
}

func authorizationPoolProtocolMatches(pool pools.Pool, protocol string) bool {
	tag, _ := authorizationPoolRole(pool)
	switch protocol {
	case "":
		return tag == listenerAuthPoolTag || tag == httpAuthTLSPoolTag
	case cloud.ExternalAuthProtocolGRPC:
		return tag == grpcAuthPoolTag
	default:
		return false
	}
}

func isAuthorizationPool(pool pools.Pool) bool {
	return isListenerAuthPool(pool) || isProtectedAuthPool(pool)
}

func isProtectedAuthPool(pool pools.Pool) bool {
	return isGRPCAuthPool(pool) || slices.Contains(pool.Tags, httpAuthTLSPoolTag)
}

func isGRPCAuthPool(pool pools.Pool) bool {
	return slices.Contains(pool.Tags, grpcAuthPoolTag)
}

func isListenerAuthPool(pool pools.Pool) bool {
	return slices.Contains(pool.Tags, listenerAuthPoolTag)
}

func validateListenerFilterWithdrawal(spec cloud.GatewayGraphSpec) error {
	gateway := spec.Gateway.Spec
	if spec.ValidateMutation == nil || len(spec.Routes) != 0 || spec.HTTPSPolicyAuthority != nil ||
		spec.Identity != gateway.Identity || gateway.ListenerProtocol != cloud.ListenerProtocolHTTP ||
		gateway.ScopedListeners == nil || gateway.ListenerPort < 1 || gateway.ListenerPort > 65535 ||
		len(gateway.RequestFilterSections) == 0 {
		return errors.New("listener filter withdrawal requires a scoped HTTP Gateway binding and confirmed rejections")
	}
	for port, sections := range gateway.RequestFilterSections {
		if port < 1 || port > 65535 || len(sections) == 0 {
			return errors.New("listener filter withdrawal requires a valid port for each rejection")
		}
		hosts := make(map[string]bool, len(sections))
		for _, section := range sections {
			if !section.Reject || len(section.Requests) != 0 || hosts[section.Hostname] ||
				section.Hostname != "*" && amphora.ValidateHostname(section.Hostname) != nil {
				return errors.New("listener filter withdrawal requires distinct rejected hostnames")
			}
			hosts[section.Hostname] = true
		}
	}
	return nil
}

func (p *Provider) withdrawListenerFilters(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, error) {
	result := cloud.GatewayGraphResult{Outcome: actual.outcome, Gateway: cloud.GatewayResult{Outcome: actual.outcome}}
	if actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return result, nil
	}
	step, pending := planListenerFilterWithdrawal(desired.spec.Gateway.Spec.RequestFilterSections, actual)
	if pending {
		result.Outcome = p.progressingOutcome("Waiting to confirm Octavia listener filter withdrawal")
		result.Gateway.Outcome = result.Outcome
	}
	if step == nil {
		return result, nil
	}
	if step.kind == graphListenerFiltersMutation {
		// A provider without filter support must keep the frontend closed.
		err := p.octavia.VerifyGatewayCapabilities(ctx, actual.loadBalancer.Provider, octavia.GatewayCapabilityListenerRequestFilters)
		if err != nil {
			return result, err
		}
	}
	if err := desired.spec.ValidateMutation(ctx); err != nil {
		return result, err
	}
	result.Outcome = p.progressingOutcome(step.message)
	err := p.executeGraphMutation(ctx, desired, actual, *step)
	result.Outcome.MutationSubmitted = err == nil
	result.Gateway.Outcome = result.Outcome
	return result, err
}

func planListenerFilterWithdrawal(rejections map[int][]cloud.ListenerRequestFilterSection, actual observedGatewayGraph) (*graphMutation, bool) {
	var update *graphMutation
	var pending bool
	for _, listener := range actual.scopedListeners {
		sections := rejections[listener.ProtocolPort]
		if len(sections) == 0 {
			continue
		}
		current, present := actual.requestFilters[listener.ID]
		readable := !present || current.Readable
		var wanted []octavia.ListenerRequestFilterSection
		if readable {
			wanted = slices.Clone(current.Sections)
			for _, denied := range sections {
				rejection := octavia.ListenerRequestFilterSection{Hostname: denied.Hostname, Reject: true}
				index := slices.IndexFunc(wanted, func(section octavia.ListenerRequestFilterSection) bool {
					return section.Hostname == denied.Hostname
				})
				if index < 0 {
					wanted = append(wanted, rejection)
				} else {
					wanted[index] = rejection
				}
			}
			if listener.ProvisioningStatus == "ACTIVE" && listenerFilterSectionsMatch(wanted, current, present) {
				continue
			}
		}
		pending = true
		if listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" {
			continue
		}
		if listener.AdminStateUp || !actual.adminObserved[listener.ID] || listener.ProvisioningStatus == "ERROR" {
			step := closeGuardedHTTPListener(listener, "Disabling Octavia listener before withdrawing pre-routing authorization")
			return &step, true
		}
		if readable && update == nil {
			update = &graphMutation{kind: graphListenerFiltersMutation, listenerID: listener.ID,
				listenerFilters: listenerFilterMutation{sections: wanted}, scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort},
				message: "Withdrawing Octavia pre-routing authorization"}
		}
	}
	return update, pending
}

func (p *Provider) planListenerFilters(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, owned map[string]observedAuthPool) {
	disposition := desired.spec.Gateway.Disposition
	if disposition != cloud.GraphEnsure && disposition != cloud.GraphDelete {
		return
	}
	configured := desired.spec.Gateway.Spec.RequestFilterSections
	if disposition == cloud.GraphDelete {
		configured = nil
	}
	routeAuth := desired.routeAuthorizers()
	if len(owned) == 0 && len(routeAuth) == 0 && !actual.hasListenerFilterConfig(desired) {
		return
	}
	actual.unverifiedListenerFilters = make(map[int]bool)
	actual.recoveringListenerFilterSections = make(map[int][]string)
	actual.unavailableListenerFilters = make(map[int]bool)
	actual.unavailableListenerBodySections = make(map[int][]string)
	actual.pendingListenerFilterCapability = make(map[int]bool)
	ports := scopedHTTPListeners(desired)
	capable := true
	if len(configured) != 0 {
		limits, err := p.octavia.ListenerRequestFilterBodyLimits(ctx, actual.loadBalancer.Provider)
		capable = err == nil
		var missing *octavia.MissingGatewayCapability
		for port := range configured {
			if errors.As(err, &missing) || gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
				actual.unavailableListenerFilters[port] = true
			} else if err != nil {
				actual.pendingListenerFilterCapability[port] = true
			}
		}
		if capable {
			configured = actual.guardUnavailableListenerBodies(configured, ports, limits)
		}
	}
	desiredPools, referencedPools := make(map[string]bool), make(map[string]bool)
	for name := range routeAuth {
		desiredPools[name] = true
	}
	for port, sections := range configured {
		for _, section := range sections {
			for _, auth := range section.Requests {
				desiredPools[listenerAuthPoolName(port, auth)] = true
			}
		}
	}
	allReferencesObserved := true
	for _, frontend := range actual.scopedListeners {
		current, present := actual.requestFilters[frontend.ID]
		// Octavia omits the field on unconfigured listeners when the
		// capability is disabled. A malformed field supplies no proof.
		if present && !current.Readable {
			allReferencesObserved = false
		}
		for id := range listenerAuthPoolIDs(current.Sections) {
			referencedPools[id] = true
		}
	}

	p.planRouteAuthPools(ctx, desired, actual, routeAuth, owned, referencedPools, allReferencesObserved)
	var next, repair *graphMutation
	for _, frontend := range actual.scopedListeners {
		port := frontend.ProtocolPort
		current, present := actual.requestFilters[frontend.ID]
		sections := configured[port]
		if listener, found := ports[port]; found && frontend.Protocol != scopedHTTPProtocol(listener) {
			// Withdraw bindings from the retiring frontend before replacing
			// it with the listener that can accept the new protocol's filters.
			sections = nil
		}
		if len(sections) == 0 && (!present || current.Readable && len(current.Sections) == 0) {
			continue
		}
		canConfigure := capable || len(sections) == 0
		var wanted []octavia.ListenerRequestFilterSection
		var recovering []string
		var step *graphMutation
		if canConfigure {
			wanted, recovering, step = desiredListenerFilterSections(port, sections, owned, referencedPools, allReferencesObserved)
			if len(recovering) != 0 {
				actual.recoveringListenerFilterSections[port] = recovering
			}
		}
		if canConfigure && listenerFilterSectionsMatch(wanted, current, present) && frontend.ProvisioningStatus == "ACTIVE" {
			// Restore a frontend with a proven section-local rejection
			// before repairing its backend, so its other sections can serve.
			if frontend.AdminStateUp && step != nil && repair == nil {
				repair = step
			}
			continue
		}
		actual.unverifiedListenerFilters[port] = true
		if actual.loadBalancer.ProvisioningStatus != "ACTIVE" ||
			frontend.ProvisioningStatus != "ACTIVE" && frontend.ProvisioningStatus != "ERROR" {
			continue
		}
		if !actual.adminObserved[frontend.ID] || frontend.AdminStateUp || frontend.ProvisioningStatus == "ERROR" {
			if actual.listenerFilterMutation == nil {
				closed := closeGuardedHTTPListener(frontend, "Disabling Octavia listener before changing pre-routing filters")
				actual.listenerFilterMutation = &closed
			}
			continue
		}
		if canConfigure && next == nil {
			next = &graphMutation{kind: graphListenerFiltersMutation, listenerID: frontend.ID,
				listenerFilters: listenerFilterMutation{sections: wanted}, scopedListener: cloud.ScopedListener{Port: port},
				message: "Updating Octavia pre-routing listener filters"}
		}
	}
	if actual.loadBalancer.ProvisioningStatus != "ACTIVE" || actual.listenerFilterMutation != nil {
		return
	}
	if next == nil {
		next = repair
	}
	actual.listenerFilterMutation = next
	if next != nil || !allReferencesObserved {
		return
	}
	names := make([]string, 0, len(owned))
	for name := range owned {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		pool := owned[name]
		if desiredPools[name] || referencedPools[pool.pool.ID] || len(pool.pool.Listeners) != 0 {
			continue
		}
		if step := listenerAuthPoolRemoval(pool); step != nil {
			actual.listenerFilterMutation = step
			return
		}
	}
}

func (actual *observedGatewayGraph) guardUnavailableListenerBodies(configured map[int][]cloud.ListenerRequestFilterSection, ports map[int]cloud.ScopedListener, limits octavia.ListenerRequestFilterBodyLimits) map[int][]cloud.ListenerRequestFilterSection {
	guarded := make(map[int][]cloud.ListenerRequestFilterSection, len(configured))
	for port, sections := range configured {
		limit := limits.HTTP
		if ports[port].TLS != nil {
			limit = limits.HTTPS
		}
		guarded[port] = slices.Clone(sections)
		for index, section := range sections {
			if slices.ContainsFunc(section.Requests, func(auth cloud.ListenerExternalAuth) bool {
				return auth.ForwardBodyMaxSize > limit
			}) {
				guarded[port][index] = cloud.ListenerRequestFilterSection{Hostname: section.Hostname, Reject: true}
				actual.unavailableListenerBodySections[port] = append(actual.unavailableListenerBodySections[port], section.Hostname)
			}
		}
	}
	return guarded
}

func (actual observedGatewayGraph) listenerFilterCanUseAuthorizers(port int, section cloud.ListenerRequestFilterSection) bool {
	// A section rejected during capability discovery has no authorizer to
	// reach. Authorizers undergoing pool recovery still need validation.
	return !section.Reject && !slices.Contains(actual.unavailableListenerBodySections[port], section.Hostname)
}

func (p *Provider) executeListenerFilterMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) error {
	change := step.listenerFilters
	tag := listenerAuthPoolTag
	if change.protectedAuth != nil {
		tag = routeAuthorizerPoolTag(*change.protectedAuth)
	}
	switch {
	case change.claimCA:
		return desired.spec.Gateway.ClaimBackendCA(ctx)
	case change.publishCA:
		return p.publishBackendCA(ctx, cloud.RouteSpec{Identity: desired.spec.Identity, BackendTLS: change.protectedAuth.BackendTLS})
	case change.createPool != "":
		tags, err := desired.identity.GatewayTags(rolePool)
		if err != nil {
			return err
		}
		opts := pools.CreateOpts{LoadbalancerID: actual.loadBalancer.ID, Name: change.createPool, Protocol: pools.ProtocolHTTP,
			LBMethod: pools.LBMethodRoundRobin, AdminStateUp: boolPointer(true), Tags: append(tags, tag)}
		var request pools.CreateOptsBuilder = opts
		if auth := change.protectedAuth; auth != nil {
			request = protectedAuthorizerPoolCreate(opts, auth.BackendTLS, change.caReference, desired.spec.Gateway.BackendClientCertificate.Reference)
		}
		_, err = p.octavia.CreatePool(ctx, request)
		return err
	case change.enablePool:
		_, err := p.octavia.UpdatePool(ctx, change.poolID, pools.UpdateOpts{AdminStateUp: boolPointer(true)})
		return err
	case change.createMember != nil:
		tags, err := desired.identity.GatewayTags(roleMember)
		if err != nil {
			return err
		}
		_, err = p.octavia.CreateMember(ctx, change.poolID, pools.CreateMemberOpts{Address: change.createMember.Address,
			ProtocolPort: change.createMember.Port, SubnetID: change.memberSubnet, AdminStateUp: boolPointer(true),
			Weight: new(1), Tags: append(tags, tag)})
		return err
	default:
		return p.octavia.SetListenerRequestFilters(ctx, step.listenerID, change.sections)
	}
}
