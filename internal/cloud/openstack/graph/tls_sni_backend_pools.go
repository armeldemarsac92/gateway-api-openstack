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
	"cmp"
	"context"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

type sniBackendPoolIdentity struct {
	Weight     int
	ServiceUID string
	Port       int
	Members    []cloud.Member
	TLS        *sniBackendTLSIdentity
}

func sniBackendPoolIdentities(backends []cloud.TLSSNIBackendPool) []sniBackendPoolIdentity {
	if len(backends) == 0 {
		return nil
	}
	result := make([]sniBackendPoolIdentity, 0, len(backends))
	for _, backend := range backends {
		identity := sniBackendPoolIdentity{Weight: backend.Weight, ServiceUID: backend.ServiceUID, Port: backend.ServicePort, Members: backend.Members}
		if tls := backend.BackendTLS; tls != nil {
			identity.TLS = &sniBackendTLSIdentity{Hostname: tls.Hostname, CARevision: backendTLSRevisionTag(tls), ServicePort: backend.ServicePort}
		}
		result = append(result, identity)
	}
	return result
}

func sameSNIBackendPools(a, b []cloud.TLSSNIBackendPool) bool {
	return slices.EqualFunc(a, b, func(left, right cloud.TLSSNIBackendPool) bool {
		return left.Weight == right.Weight && left.ServiceUID == right.ServiceUID && left.ServicePort == right.ServicePort &&
			slices.Equal(left.Members, right.Members) && sameSNIBackendTLS(left.BackendTLS, right.BackendTLS)
	})
}

func normalizeSNIBackendPools(route *cloud.TLSSNIRoute) error {
	if route.Passthrough || route.ServiceUID != "" || route.ServicePort != 0 || len(route.Members) != 0 ||
		route.BackendTLS != nil || len(route.BackendServices) != 0 || route.TCPConnectionRejection != nil ||
		len(route.BackendPools) > openstackidentity.MaxSNIBackendPools {
		return invalidSNI("weighted TLS SNI pools require an exclusive terminating backend representation")
	}
	route.BackendPools = slices.Clone(route.BackendPools)
	for i, backend := range route.BackendPools {
		if backend.Weight < 1 || backend.Weight > 1_000_000 {
			return invalidSNI("weighted TLS SNI pools require positive reference weights")
		}
		if backend.ServiceUID == "" && backend.ServicePort == 0 && len(backend.Members) == 0 && backend.BackendTLS == nil {
			continue
		}
		if backend.ServiceUID == "" || backend.ServicePort < 1 || backend.ServicePort > 65535 || len(backend.Members) == 0 {
			return invalidSNI("weighted TLS SNI pools require identified Services with ready members")
		}
		if tls := backend.BackendTLS; tls != nil && (tls.Denied || len(tls.SubjectAltNames) != 0 || !validBackendTLS(tls)) {
			return invalidSNI("weighted TLS SNI backend TLS requires an authorized CA and a DNS hostname")
		}
		members, err := normalizeSNIMembers(*route, backend.Members)
		if err != nil {
			return err
		}
		route.BackendPools[i].Members = members
	}
	return nil
}

func normalizeSNIMembers(route cloud.TLSSNIRoute, members []cloud.Member) ([]cloud.Member, error) {
	members = slices.Clone(members)
	slices.SortFunc(members, func(a, b cloud.Member) int {
		return cmp.Or(cmp.Compare(a.Address, b.Address), cmp.Compare(a.Port, b.Port))
	})
	for i, member := range members {
		address, err := netip.ParseAddr(member.Address)
		if err != nil || !address.Is4() || !address.IsGlobalUnicast() || address.IsLoopback() ||
			member.Port < 1 || member.Port > 65535 || member.Draining || !validSNIMemberWeight(route, member.Weight) ||
			i > 0 && members[i-1].Address == member.Address && members[i-1].Port == member.Port {
			return nil, invalidSNI("invalid or duplicate IPv4 TLSRoute member")
		}
	}
	return members, nil
}

func sniPoolSlot(tags []string) (int, error) {
	if slot, ok := openstackidentity.SNIPoolSlot(tags); ok {
		return slot, nil
	}
	if !openstackidentity.HasSNIPoolSlot(tags) && !slices.Contains(tags, openstackidentity.SNIWeightedPoolsTag) {
		return -1, nil
	}
	return -1, fmt.Errorf("%w: invalid weighted TLS SNI pool slot", cloud.ErrOwnershipConflict)
}

func (target desiredSNIFragment) usesBackendPools() bool {
	return len(target.route.BackendPools) != 0
}

func (target desiredSNIFragment) backendPool(slot int) desiredSNIFragment {
	backend := target.route.BackendPools[slot]
	target.route.ServiceUID, target.route.ServicePort, target.route.Members = backend.ServiceUID, backend.ServicePort, backend.Members
	target.route.BackendTLS = backend.BackendTLS
	if len(target.backendPoolPlans) != 0 {
		target.backendTLS = target.backendPoolPlans[slot]
	}
	// Keep the outer pool profile on the member and pool while constructing
	// their scalar wire representation.
	return target
}

func hasHTTPOnlySNIOptions(row octavia.L7PolicyObservation) bool {
	tags := row.Policy.Tags
	return row.RedirectRequestPresent || row.RequestHeaderOperationsErr != nil || len(row.RequestHeaderOperations) != 0 ||
		row.ResponseHeaderOperationsErr != nil || len(row.ResponseHeaderOperations) != 0 ||
		row.RequestURLRewritePresent || slices.Contains(tags, openstackidentity.RequestURLRewritePolicyTag) ||
		row.HTTPRequestPolicyPresent || slices.Contains(tags, openstackidentity.HTTPRequestPolicyTag) ||
		row.RequestCORSPresent || slices.Contains(tags, openstackidentity.RequestCORSPolicyTag)
}

func (fragment *observedSNIFragment) observeBackendTargets(row octavia.L7PolicyObservation) {
	fragment.targetsUnverified = row.Policy.Action != string(octavia.ActionWeightedTargets) ||
		len(row.WeightedTCPPolicyTargets) == 0 || row.Target == nil || row.Target.FixedResponse != nil ||
		string(row.PoolReference) != "null" || row.HTTPSListenerHostnamePresent || row.RedirectOptionsPresent || hasHTTPOnlySNIOptions(row) ||
		row.TargetRequestFiltersPresent || row.TargetMirrorsPresent || row.RequestMirrorsErr != nil || len(row.RequestMirrors) != 0 ||
		row.TCPTLSPassthrough != nil && *row.TCPTLSPassthrough
	for slot, target := range row.WeightedTCPPolicyTargets {
		leaf := fragment.backendPools[slot]
		if leaf == nil || target.PoolID != leaf.pool.ID {
			fragment.targetsUnverified = true
		}
	}
	fragment.backendTargets = row.WeightedTCPPolicyTargets
}

func (fragment *observedSNIFragment) observeBackendRule(rule octavia.RuleObservation) {
	// Amphora permits only an absent match key on TLS SNI rules.
	fragment.backendRuleUnverified = rule.AdminStateUp && (rule.HasKey() || rule.Invert ||
		rule.RuleType != ruleTLSSNI || rule.CompareType != sniCompareType(rule.Value) || amphora.ValidateSNIHostname(rule.Value) != nil)
	fragment.targetsUnverified = fragment.targetsUnverified || fragment.backendRuleUnverified
}

func (fragment *observedSNIFragment) verifyBackendPoolAttachments() {
	attached := fragment.policy != nil && fragment.policy.AdminStateUp && fragment.rule != nil
	ids := make(map[string]bool)
	for _, target := range fragment.backendTargets {
		ids[target.PoolID] = true
	}
	for _, leaf := range fragment.backendPools {
		if leaf.pool.Listeners == nil || (len(leaf.pool.Listeners) == 1) != (attached && ids[leaf.pool.ID]) {
			fragment.targetsUnverified = true
		}
	}
}

func (fragment *observedSNIFragment) pending() bool {
	if fragment.policy != nil && strings.HasPrefix(fragment.policy.ProvisioningStatus, "PENDING_") ||
		fragment.rule != nil && strings.HasPrefix(fragment.rule.ProvisioningStatus, "PENDING_") {
		return true
	}
	if fragment.backendPools != nil {
		for _, pool := range fragment.backendPools {
			if pool.pending() {
				return true
			}
		}
		return false
	}
	return strings.HasPrefix(fragment.pool.ProvisioningStatus, "PENDING_") ||
		slices.ContainsFunc(fragment.members, func(member pools.Member) bool { return strings.HasPrefix(member.ProvisioningStatus, "PENDING_") })
}

func (fragment *observedSNIFragment) hasUnverifiedMembers() bool {
	if fragment.backendPools != nil {
		for _, pool := range fragment.backendPools {
			if pool.membersUnverified {
				return true
			}
		}
	}
	return fragment.membersUnverified
}

func (fragment *observedSNIFragment) unprovenDetachedBackendPools() bool {
	return fragment.backendPools != nil && fragment.policy == nil && fragment.targetsUnverified
}

func (fragment *observedSNIFragment) firstOpenPool() *observedSNIFragment {
	if fragment.backendPools == nil {
		if !fragment.closedBackendPool() {
			return fragment
		}
		return nil
	}
	for _, slot := range slices.Sorted(maps.Keys(fragment.backendPools)) {
		leaf := fragment.backendPools[slot]
		if !leaf.closedBackendPool() {
			return leaf
		}
	}
	return nil
}

func (fragment *observedSNIFragment) emptyActivePool() bool {
	return fragment.pool != nil && fragment.pool.ProvisioningStatus == "ACTIVE" &&
		fragment.pool.AdminStateUp && !fragment.membersUnverified && len(fragment.members) == 0
}

func (fragment *observedSNIFragment) weightedBackendTargetShape(target desiredSNIFragment) bool {
	if fragment.backendPools == nil || fragment.targetsUnverified || len(fragment.backendPools) > len(target.route.BackendPools) {
		return false
	}
	if fragment.policy != nil {
		if len(fragment.backendTargets) != len(target.route.BackendPools) || len(fragment.backendPools) != len(fragment.backendTargets) {
			return false
		}
		for slot, backend := range target.route.BackendPools {
			if fragment.backendTargets[slot].Weight != backend.Weight {
				return false
			}
		}
	}
	return true
}

func (leaf *observedSNIFragment) backendPoolSettingsValid(target desiredSNIFragment) bool {
	if leaf.pool.ProvisioningStatus != "ACTIVE" || !leaf.backendTLS.AdminObserved || !leaf.backendTLS.EnabledObserved ||
		leaf.pool.LBMethod != string(pools.LBMethodRoundRobin) || leaf.pool.Persistence.Type != "" ||
		leaf.tcpRejection.Invalid || leaf.tcpRejection.Value != nil ||
		leaf.backendTLS.RequireH2 {
		return false
	}
	if tls := target.route.BackendTLS; tls != nil {
		return slices.Contains(leaf.pool.Tags, backendTLSManagedTag) &&
			slices.Contains(leaf.pool.Tags, backendTLSRevisionTag(tls)) &&
			slices.Contains(leaf.pool.Tags, sniBackendServiceTag(target.route)) &&
			!differentBackendTLSRevision(leaf.pool.Tags, backendTLSRevisionTag(tls))
	}
	return !leaf.protectedBackendTLS() &&
		(target.route.ServiceUID == "" || slices.Contains(leaf.pool.Tags, sniBackendServiceTag(target.route)))
}

func (fragment *observedSNIFragment) matchesBackendPools(target desiredSNIFragment) bool {
	if !target.usesBackendPools() || !fragment.weightedBackendTargetShape(target) || !fragment.matchesSNISelector(target) {
		return false
	}
	for slot, leaf := range fragment.backendPools {
		if slot >= len(target.route.BackendPools) {
			return false
		}
		want := target.backendPool(slot)
		if !leaf.backendPoolSettingsValid(want) || len(leaf.members) > len(want.route.Members) ||
			!leaf.matchesSNIMembers(want.route) {
			return false
		}
	}
	return true
}

func (leaf *observedSNIFragment) matchesNativeBackendTransport(target desiredSNIFragment) bool {
	if !leaf.backendPoolSettingsValid(target) || target.backendTLS.denies() {
		return false
	}
	if tls := target.route.BackendTLS; tls != nil {
		return target.backendTLS.settings.CAReference != "" && leaf.backendTLS.Complete() &&
			leaf.pool.TLSEnabled && leaf.backendTLS.Hostname == tls.Hostname &&
			leaf.pool.CATLSContainerRef == target.backendTLS.settings.CAReference &&
			leaf.pool.TLSContainerRef == "" && leaf.pool.CRLContainerRef == "" &&
			!hasUnclearedBackendTLSSubjectAltNames(*leaf.pool, leaf.backendTLS)
	}
	return true
}

func (leaf *observedSNIFragment) backendPoolServing(target desiredSNIFragment) bool {
	return leaf.pool.AdminStateUp && !leaf.membersUnverified && len(leaf.members) == len(target.route.Members) &&
		leaf.matchesNativeBackendTransport(target) &&
		(target.route.BackendTLS == nil || target.backendTLS.phase == backendTLSVerified)
}

func (fragment *observedSNIFragment) backendPoolsConverged(target desiredSNIFragment) bool {
	if len(fragment.backendPools) != len(target.route.BackendPools) {
		return false
	}
	for slot, leaf := range fragment.backendPools {
		want := target.backendPool(slot)
		if want.route.ServiceUID == "" || want.backendTLS.denies() ||
			want.route.BackendTLS != nil && want.backendTLS.phase != backendTLSVerified || leaf.membersUnverified {
			if !leaf.closedBackendPool() {
				return false
			}
			continue
		}
		if !leaf.backendPoolServing(want) {
			return false
		}
	}
	return true
}

func (fragment *observedSNIFragment) backendPoolsReady(target desiredSNIFragment) bool {
	return !target.weightedBlocked && fragment.backendPoolsConverged(target) &&
		!slices.ContainsFunc(target.backendPoolPlans, func(plan backendTLSPlan) bool {
			return plan.phase != backendTLSUnmanaged && plan.phase != backendTLSVerified
		}) && !fragment.hasUnverifiedMembers()
}

func (p *Provider) prepareSNIBackendPools(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph, tlsCapable bool) error {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	usesPools := slices.ContainsFunc(desired.sni, desiredSNIFragment.usesBackendPools)
	for _, fragment := range actual.sni {
		usesPools = usesPools || fragment.backendPools != nil
	}
	if !usesPools {
		return nil
	}
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	desired.sniBackendPoolsCapable = p.octavia.VerifyGatewayCapabilities(ctx, provider, octavia.GatewayCapabilityTLSSNIWeightedPools) == nil
	var guards []desiredSNIFragment
	for i := range desired.sni {
		target := &desired.sni[i]
		if !target.usesBackendPools() {
			continue
		}
		target.weightedBlocked = !desired.sniBackendPoolsCapable
		target.backendPoolPlans = make([]backendTLSPlan, len(target.route.BackendPools))
		fragment := actual.sni[target.key]
		for slot, backend := range target.route.BackendPools {
			leafTarget := target.backendPool(slot)
			if backend.BackendTLS == nil {
				if backend.ServiceUID != "" && (!tlsCapable || !desired.spec.Gateway.SNIPoliciesObserved) {
					for key, observed := range actual.sni {
						if observed.protectsService(key, leafTarget) {
							target.backendPoolPlans[slot] = backendTLSPlan{phase: backendTLSBlocked}
							break
						}
					}
				}
				continue
			}
			var observed observedRouteGraph
			if fragment != nil {
				if leaf := fragment.backendPools[slot]; leaf != nil {
					observed.pool, observed.backendTLS = leaf.pool, leaf.backendTLS
				}
			}
			clientIdentity := desired.spec.Gateway.BackendClientCertificate
			clientIdentity.Denied = clientIdentity.Denied || clientIdentity.Reference != ""
			route := desiredRoute{spec: cloud.RouteSpec{Identity: target.route.Identity, BackendTLS: backend.BackendTLS}}
			target.backendPoolPlans[slot] = p.observeBackendTLS(ctx, route, observed, func(bool, bool) bool { return tlsCapable },
				desired.spec.Gateway.BackendCAOwned, clientIdentity)
		}
		verified := fragment != nil && fragment.hasActiveSelector(target.route.Hostname) && fragment.matches(*target) && fragment.backendPoolsConverged(*target)
		predecessor := false
		for key, observed := range actual.sni {
			if observed.canStageBehindSNIBackendPools(key, *target, *desired) {
				predecessor = true
				break
			}
		}
		if (!verified || target.weightedBlocked) && !predecessor && hasSNIFallback(target.route.Hostname, desired.sni, actual.sni) {
			guard, err := buildSNITargets(desired.spec.Identity, []cloud.TLSSNIRoute{{
				Identity: target.route.Identity, Hostname: target.route.Hostname, Deny: true,
			}})
			if err != nil {
				return err
			}
			guard[0].weightedGuard = true
			guards = append(guards, guard[0])
		}
	}
	desired.sni = append(desired.sni, guards...)
	return ctx.Err()
}

func withdrawSNIBackendPools(desired desiredGatewayGraph, actual observedGatewayGraph, keys []sniFragmentKey, wanted map[sniFragmentKey]desiredSNIFragment) (graphMutation, bool) {
	for _, key := range keys {
		fragment := actual.sni[key]
		if fragment.backendPools == nil {
			continue
		}
		target, keep := wanted[key]
		if replacement, preserve := sniBackendPoolSuccessor(desired, key, fragment); !keep && preserve {
			for _, slot := range slices.Sorted(maps.Keys(fragment.backendPools)) {
				leaf := fragment.backendPools[slot]
				if !leaf.closedBackendPool() && !replacement.permitsBackendPool(leaf) {
					return sniDisablePoolStep(leaf), true
				}
			}
			continue
		}
		if fragment.policy != nil && fragment.policy.AdminStateUp &&
			(fragment.targetsUnverified || desired.spec.Gateway.Disposition == cloud.GraphEnsure && !desired.sniBackendPoolsCapable) {
			// An unreadable action can point at pools outside this fragment.
			// Remove the owned policy only after any covering suffix is safe.
			var claims []string
			if host := fragment.forwardingHostname(); host != "" {
				claims = append(claims, host)
			} else {
				// A malformed rule can shut down Amphora's whole TCP
				// frontend. Keep covering policies closed when removing
				// it, including for the currently desired owner names.
				for _, candidate := range desired.sni {
					if candidate.key.sniRouteKey == key.sniRouteKey {
						claims = append(claims, candidate.route.Hostname)
					}
				}
			}
			for _, other := range keys {
				fallback := actual.sni[other]
				if fallback.policy == nil || !fallback.policy.AdminStateUp {
					continue
				}
				for _, host := range claims {
					if !amphora.SNISuffixCovers(fallback.selectorHostname(), host) {
						continue
					}
					shielded := slices.ContainsFunc(desired.sni, func(guard desiredSNIFragment) bool {
						proof := actual.sni[guard.key]
						return guard.route.Deny && guard.route.Hostname == host && proof != nil &&
							proof.hasActiveSelector(host) && proof.matches(guard) && proof.policy.Position < fragment.policy.Position
					})
					if !shielded {
						return suspendWeightedSNI(fallback), true
					}
				}
			}
			return suspendWeightedSNI(fragment), true
		}
		matches := keep && fragment.matchesBackendPools(target)
		if fragment.policy == nil && !matches {
			// A detached pool cannot be serving. Rebuild it without trying
			// to change admin state on a failed create.
			continue
		}
		for _, slot := range slices.Sorted(maps.Keys(fragment.backendPools)) {
			leaf := fragment.backendPools[slot]
			if leaf.closedBackendPool() {
				continue
			}
			if !matches || target.weightedBlocked {
				return sniDisablePoolStep(leaf), true
			}
			want := target.backendPool(slot)
			if want.backendTLS.denies() || want.route.ServiceUID == "" || leaf.membersUnverified ||
				want.route.BackendTLS != nil && (want.backendTLS.phase != backendTLSVerified || !leaf.matchesNativeBackendTransport(want)) ||
				fragment.policy != nil && fragment.policy.AdminStateUp && len(leaf.members) != len(want.route.Members) {
				return sniDisablePoolStep(leaf), true
			}
		}
	}
	return graphMutation{}, false
}

func (fragment *observedSNIFragment) canStageBehindSNIBackendPools(key sniFragmentKey, target desiredSNIFragment, desired desiredGatewayGraph) bool {
	return desired.sniBackendPoolsCapable && fragment.backendPools != nil && key != target.key &&
		key.sniRouteKey == target.key.sniRouteKey && !target.route.Deny && !target.route.Passthrough &&
		!target.weightedBlocked && !desired.sniPaused[key] && !desired.sniPaused[target.key] &&
		!fragment.targetsUnverified &&
		len(fragment.backendTargets) != 0 && fragment.hasActiveSelector(target.route.Hostname)
}

func sniBackendPoolSuccessor(desired desiredGatewayGraph, key sniFragmentKey, fragment *observedSNIFragment) (desiredSNIFragment, bool) {
	for _, target := range desired.sni {
		if fragment.canStageBehindSNIBackendPools(key, target, desired) {
			return target, true
		}
	}
	return desiredSNIFragment{}, false
}

func (target desiredSNIFragment) permitsBackendPool(leaf *observedSNIFragment) bool {
	if leaf.membersUnverified {
		return false
	}
	allowed := func(want desiredSNIFragment) bool {
		return want.route.ServiceUID != "" && leaf.matchesNativeBackendTransport(want) && leaf.matchesSNIMembers(want.route)
	}
	if target.usesBackendPools() {
		for slot := range target.route.BackendPools {
			// The predecessor and successor can use different positions.
			// Transport checks also prove this Service UID and port.
			if allowed(target.backendPool(slot)) {
				return true
			}
		}
		return false
	}
	if len(target.route.BackendServiceMembers) != 0 {
		// Provenance scopes the Service but the current forwarding graph
		// must still include every endpoint kept in the predecessor.
		for _, member := range leaf.members {
			if !slices.ContainsFunc(target.route.Members, func(current cloud.Member) bool {
				return current.Address == member.Address && current.Port == member.ProtocolPort
			}) {
				return false
			}
		}
		for _, backend := range target.route.BackendServiceMembers {
			if backend.BackendTLS != nil || !slices.Contains(target.route.BackendServices,
				cloud.TLSBackendService{UID: backend.ServiceUID, Port: backend.ServicePort}) {
				continue
			}
			destination := target
			destination.route.ServiceUID, destination.route.ServicePort = backend.ServiceUID, backend.ServicePort
			destination.route.Members = backend.Members
			if allowed(destination) {
				return true
			}
		}
		return false
	}
	switch len(target.route.BackendServices) {
	case 0:
		return allowed(target)
	case 1:
		// A flattened pool with one Service still proves which
		// endpoints belong to that Service.
		service := target.route.BackendServices[0]
		target.route.ServiceUID, target.route.ServicePort = service.UID, service.Port
		target.route.Members = slices.Clone(target.route.Members)
		for i := range target.route.Members {
			target.route.Members[i].Weight = 0
		}
		return allowed(target)
	default:
		// A flattened multi-Service endpoint inventory cannot prove
		// which Service owns an endpoint in the predecessor.
		return false
	}
}

// Stage the replacement behind a proven selector whose unauthorized
// destinations are already closed. A failed create cannot interrupt the
// predecessor's remaining destinations or let a covering wildcard inherit it.
func stageSNIBackendPoolSuccessor(desired desiredGatewayGraph, actual observedGatewayGraph, keys []sniFragmentKey) (graphMutation, bool, bool) {
	waiting := false
	for _, key := range keys {
		predecessor := actual.sni[key]
		target, found := sniBackendPoolSuccessor(desired, key, predecessor)
		if !found {
			continue
		}
		waiting = true
		replacement := actual.sni[target.key]
		if replacement != nil {
			if replacement.hasUnverifiedMembers() || replacement.unprovenDetachedBackendPools() {
				continue
			}
			if !replacement.matches(target) {
				return sniDeletionStep(replacement), true, false
			}
			if replacement.hasActiveSelector(target.route.Hostname) &&
				(target.usesBackendPools() && replacement.backendPoolsConverged(target) ||
					!target.usesBackendPools() && replacement.ready(target)) {
				return suspendWeightedSNI(predecessor), true, false
			}
		}
		if step, ok := nextSNIFragmentMutation(target, replacement); ok {
			step.sni.preferPolicy = true
			return step, true, false
		}
	}
	return graphMutation{}, false, waiting
}

func nextSNIBackendPoolsMutation(target desiredSNIFragment, fragment *observedSNIFragment) (graphMutation, bool) {
	if target.weightedBlocked || fragment != nil && !fragment.matchesBackendPools(target) {
		return graphMutation{}, false
	}
	step := func(mutation sniMutation) (graphMutation, bool) {
		return graphMutation{kind: graphSNIMutation, sni: mutation, redactTLSError: mutation.target.route.BackendTLS != nil,
			message: "Converging the weighted TLS SNI backend pools"}, true
	}
	targets := make([]octavia.WeightedTCPPolicyTarget, 0, len(target.route.BackendPools))
	var caMutation *sniMutation
	for slot, backend := range target.route.BackendPools {
		want := target.backendPool(slot)
		mutation := sniMutation{target: want, poolSlot: new(slot)}
		var leaf *observedSNIFragment
		if fragment != nil {
			leaf = fragment.backendPools[slot]
		}
		if leaf == nil {
			mutation.kind = sniCreatePool
			mutation.closedPool = backend.ServiceUID == "" || want.backendTLS.denies() ||
				backend.BackendTLS != nil && want.backendTLS.phase != backendTLSConfigure
			return step(mutation)
		}
		targets = append(targets, octavia.WeightedTCPPolicyTarget{PoolID: leaf.pool.ID, Weight: backend.Weight})
		mutation.parentID = leaf.pool.ID
		if leaf.membersUnverified || want.backendTLS.denies() || backend.ServiceUID == "" {
			continue
		}
		switch want.backendTLS.phase {
		case backendTLSClaim:
			mutation.kind = sniClaimBackendCA
			caMutation = &mutation
			continue
		case backendTLSPublish:
			mutation.kind = sniPublishBackendCA
			caMutation = &mutation
			continue
		case backendTLSConfigure:
			mutation.kind = sniConfigureBackendTLSPool
			return step(mutation)
		}
		for _, member := range backend.Members {
			if !slices.ContainsFunc(leaf.members, func(observed pools.Member) bool {
				return observed.Address == member.Address && observed.ProtocolPort == member.Port
			}) {
				mutation.kind, mutation.member = sniCreateMember, member
				return step(mutation)
			}
		}
		if !leaf.pool.AdminStateUp {
			mutation.kind = sniEnableBackendTLSPool
			return step(mutation)
		}
	}
	mutation := sniMutation{target: target}
	switch {
	case fragment.policy == nil:
		mutation.kind, mutation.poolTargets = sniCreatePolicy, targets
	case fragment.rule == nil:
		mutation.kind, mutation.parentID = sniCreateRule, fragment.policy.ID
	case !fragment.policy.AdminStateUp:
		mutation.kind, mutation.parentID = sniEnablePolicy, fragment.policy.ID
	case caMutation != nil:
		mutation = *caMutation
	default:
		return graphMutation{}, false
	}
	return step(mutation)
}
