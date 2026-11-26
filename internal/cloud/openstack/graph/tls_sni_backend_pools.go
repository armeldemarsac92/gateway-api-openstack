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
	if route.ServiceUID != "" || route.ServicePort != 0 || len(route.Members) != 0 ||
		route.BackendTLS != nil || len(route.BackendServices) != 0 || route.TCPConnectionRejection != nil ||
		len(route.BackendPools) > openstackidentity.MaxSNIBackendPools {
		return invalidSNI("weighted TLS SNI pools require an exclusive backend representation")
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
		if tls := backend.BackendTLS; tls != nil && (route.Passthrough || tls.Denied || len(tls.SubjectAltNames) != 0 || !validBackendTLS(tls)) {
			return invalidSNI("weighted TLS SNI backend TLS requires an authorized CA and a DNS hostname")
		}
		leaf := *route
		leaf.BackendTLS = backend.BackendTLS
		members, err := normalizeSNIMembers(leaf, backend.Members)
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
			member.RoutedExternal && (route.BackendTLS != nil || address.IsLinkLocalUnicast()) ||
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
		row.ExternalAuthPresent || slices.Contains(tags, routeExternalAuthTag) ||
		row.ResponseHeaderOperationsErr != nil || len(row.ResponseHeaderOperations) != 0 ||
		row.RequestURLRewritePresent || slices.Contains(tags, openstackidentity.RequestURLRewritePolicyTag) ||
		row.HTTPRequestPolicyPresent || slices.Contains(tags, openstackidentity.HTTPRequestPolicyTag) ||
		row.RequestCORSPresent || slices.Contains(tags, openstackidentity.RequestCORSPolicyTag)
}

func (fragment *observedSNIFragment) observeBackendTargets(row octavia.L7PolicyObservation) {
	fragment.targetsUnverified = row.Policy.Action != string(octavia.ActionWeightedTargets) ||
		len(row.WeightedTCPPolicyTargets) == 0 || row.Target == nil || row.Target.FixedResponse != nil ||
		string(row.PoolReference) != "null" || row.HTTPSListenerHostnamePresent || row.RedirectOptionsPresent || hasHTTPOnlySNIOptions(row) ||
		row.TargetRequestFiltersPresent || row.TargetResponseHeadersPresent || row.TargetMirrorsPresent || row.RequestMirrorsErr != nil || len(row.RequestMirrors) != 0
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

func sniBackendPoolAlgorithm(memberCount int) pools.LBMethod {
	if memberCount > cloud.MaxTCPRoundRobinMembers {
		// Connection shares are selected by the policy. A large Service
		// can use least connections with uniform weights within its pool.
		return pools.LBMethodLeastConnections
	}
	return pools.LBMethodRoundRobin
}

func (leaf *observedSNIFragment) backendPoolSettingsValid(target desiredSNIFragment, algorithm pools.LBMethod) bool {
	if leaf.pool.ProvisioningStatus != "ACTIVE" || !leaf.backendTLS.AdminObserved || !leaf.backendTLS.EnabledObserved ||
		leaf.pool.LBMethod != string(algorithm) || leaf.pool.Persistence.Type != "" ||
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
		if !leaf.backendPoolSettingsValid(want, sniBackendPoolAlgorithm(len(want.route.Members))) || len(leaf.members) > len(want.route.Members) ||
			!leaf.matchesSNIMembers(want.route) {
			return false
		}
	}
	return true
}

func (leaf *observedSNIFragment) matchesNativeBackendTransport(target desiredSNIFragment, algorithm pools.LBMethod) bool {
	if !leaf.backendPoolSettingsValid(target, algorithm) || target.backendTLS.denies() {
		return false
	}
	if tls := target.route.BackendTLS; tls != nil {
		trustMatches := target.backendTLS.settings.CAReference != "" && !hasUnclearedSystemCA(*leaf.pool, leaf.backendTLS)
		if tls.Trust == cloud.BackendTLSTrustSystem {
			trustMatches = leaf.backendTLS.SystemCAApplied() && configuredSystemCA(*leaf.pool, leaf.backendTLS)
		}
		return trustMatches && leaf.backendTLS.Complete() &&
			leaf.pool.TLSEnabled && leaf.backendTLS.Hostname == tls.Hostname &&
			leaf.pool.CATLSContainerRef == target.backendTLS.settings.CAReference &&
			leaf.pool.TLSContainerRef == "" && leaf.pool.CRLContainerRef == "" &&
			!hasUnclearedBackendTLSSubjectAltNames(*leaf.pool, leaf.backendTLS)
	}
	return true
}

func (leaf *observedSNIFragment) backendPoolServing(target desiredSNIFragment) bool {
	return leaf.pool.AdminStateUp && !leaf.membersUnverified && len(leaf.members) == len(target.route.Members) &&
		leaf.matchesNativeBackendTransport(target, sniBackendPoolAlgorithm(len(target.route.Members))) &&
		(target.route.BackendTLS == nil || target.backendTLS.phase == backendTLSVerified)
}

func (fragment *observedSNIFragment) backendPoolsConverged(target desiredSNIFragment) bool {
	if len(fragment.backendPools) != len(target.route.BackendPools) {
		return false
	}
	for slot, leaf := range fragment.backendPools {
		want := target.backendPool(slot)
		if want.backendTLS.stagesSystemCA() {
			if !leaf.emptyActivePool() {
				return false
			}
			continue
		}
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

func (p *Provider) prepareSNIBackendPools(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph, tlsCapable bool, trustCapability func(*cloud.BackendTLSValidation) (supported, pending bool)) error {
	var guards []desiredSNIFragment
	for i := range desired.sni {
		target := &desired.sni[i]
		if !target.usesBackendPools() {
			continue
		}
		target.weightedBlocked = !desired.sniBackendPoolCapability(target.route.Passthrough)
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
					observed.pool, observed.backendTLS, observed.members = leaf.pool, leaf.backendTLS, leaf.members
				}
			}
			trustCapable, trustPending := trustCapability(backend.BackendTLS)
			clientIdentity := desired.spec.Gateway.BackendClientCertificate
			clientIdentity.Denied = clientIdentity.Denied || clientIdentity.Reference != ""
			route := desiredRoute{spec: cloud.RouteSpec{Identity: target.route.Identity, BackendTLS: backend.BackendTLS}}
			observe := func(state observedRouteGraph, supported bool) backendTLSPlan {
				return p.observeBackendTLS(ctx, route, state, func(bool, bool) bool { return supported },
					desired.spec.Gateway.BackendCAOwned, clientIdentity)
			}
			plan := observe(observed, trustCapable)
			if trustPending {
				// A current peer readback can preserve an authorized destination
				// during a discovery outage. It cannot enroll another endpoint.
				for key, current := range actual.sni {
					if current.targetsUnverified || key != target.key && !current.canReplaceBackendPools(key, *target) || !current.hasActiveSelector(target.route.Hostname) {
						continue
					}
					for _, leaf := range current.backendPools {
						if !leaf.pool.AdminStateUp || leaf.membersUnverified || len(leaf.members) == 0 || !leaf.matchesSNIMembers(leafTarget.route) {
							continue
						}
						leafTarget.backendTLS = observe(observedRouteGraph{pool: leaf.pool, backendTLS: leaf.backendTLS, members: leaf.members}, true)
						if leafTarget.backendTLS.phase == backendTLSVerified && leaf.matchesNativeBackendTransport(leafTarget, sniBackendPoolAlgorithm(len(leaf.members))) {
							plan = leafTarget.backendTLS
							plan.retainServingOnly = true
							break
						}
					}
					if plan.retainServingOnly {
						break
					}
				}
			}
			target.backendPoolPlans[slot] = plan
		}
		verified := fragment != nil && fragment.hasActiveSelector(target.route.Hostname) && fragment.matches(*target) && fragment.backendPoolsConverged(*target)
		predecessor := false
		for key, observed := range actual.sni {
			if observed.canStageBehindSNIBackendPools(key, *target, *desired) ||
				!target.weightedBlocked && observed.canKeepFlatSNI(key, *target) ||
				observed.keepsSNISelectorDuringDiscovery(key, *target, *desired) {
				predecessor = true
				break
			}
		}
		if (!verified || target.weightedBlocked) && !predecessor && hasSNIFallback(target.route.Hostname, desired.sni, actual.sni) {
			guard, err := buildSNITargets(desired.spec.Identity, []cloud.TLSSNIRoute{{
				Identity: target.route.Identity, Hostname: target.route.Hostname, Deny: true, Passthrough: target.route.Passthrough,
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
		if replacement, found := fragment.replacementBackendPools(key, desired); !keep && found {
			for _, slot := range slices.Sorted(maps.Keys(fragment.backendPools)) {
				leaf := fragment.backendPools[slot]
				if !leaf.closedBackendPool() && !replacement.permitsBackendPool(leaf) {
					return sniDisablePoolStep(leaf), true
				}
			}
			if fragment.canStageBehindSNIBackendPools(key, replacement, desired) ||
				desired.sniBackendPoolDiscoveryPending && !desired.sniPaused[key] && !desired.sniPaused[replacement.key] {
				continue
			}
		}
		if fragment.policy != nil && fragment.policy.AdminStateUp &&
			(fragment.targetsUnverified || desired.spec.Gateway.Disposition == cloud.GraphEnsure && desired.sniBackendPoolCapabilityLost(fragment.isPassthrough())) {
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
			if !matches || target.weightedBlocked && !desired.sniBackendPoolDiscoveryPending {
				return sniDisablePoolStep(leaf), true
			}
			want := target.backendPool(slot)
			if want.backendTLS.stagesSystemCA() && len(leaf.members) == 0 && !leaf.membersUnverified {
				continue
			}
			// A System pool must stay enabled while its first authorized members
			// are added. Disabling it would invalidate the digest we just read.
			allowPartial := want.backendTLS.retainServingOnly ||
				want.backendTLS.phase == backendTLSVerified && want.route.BackendTLS != nil && want.route.BackendTLS.Trust == cloud.BackendTLSTrustSystem
			if want.backendTLS.denies() || want.route.ServiceUID == "" || leaf.membersUnverified ||
				want.route.BackendTLS != nil && (want.backendTLS.phase != backendTLSVerified || !leaf.matchesNativeBackendTransport(want, sniBackendPoolAlgorithm(len(want.route.Members)))) ||
				!allowPartial && fragment.policy != nil && fragment.policy.AdminStateUp && len(leaf.members) != len(want.route.Members) {
				return sniDisablePoolStep(leaf), true
			}
		}
	}
	return graphMutation{}, false
}

func (fragment *observedSNIFragment) canStageBehindSNIBackendPools(key sniFragmentKey, target desiredSNIFragment, desired desiredGatewayGraph) bool {
	return fragment.canReplaceBackendPools(key, target) &&
		(!target.usesBackendPools() || desired.sniBackendPoolCapability(target.route.Passthrough)) &&
		!target.weightedBlocked && !desired.sniPaused[key] && !desired.sniPaused[target.key]
}

func (fragment *observedSNIFragment) keepsSNISelectorDuringDiscovery(key sniFragmentKey, target desiredSNIFragment, desired desiredGatewayGraph) bool {
	return desired.sniBackendPoolDiscoveryPending && !desired.sniPaused[key] && !desired.sniPaused[target.key] &&
		(fragment.canReplaceBackendPools(key, target) || fragment.canKeepFlatSNI(key, target))
}

func (fragment *observedSNIFragment) canReplaceBackendPools(key sniFragmentKey, target desiredSNIFragment) bool {
	return fragment.backendPools != nil && !fragment.targetsUnverified && len(fragment.backendTargets) != 0 &&
		key != target.key && key.sniRouteKey == target.key.sniRouteKey && !target.route.Deny &&
		fragment.isPassthrough() == target.route.Passthrough && fragment.hasActiveSelector(target.route.Hostname)
}

func (fragment *observedSNIFragment) replacementBackendPools(key sniFragmentKey, desired desiredGatewayGraph) (desiredSNIFragment, bool) {
	for _, target := range desired.sni {
		if fragment.canReplaceBackendPools(key, target) {
			return target, true
		}
	}
	return desiredSNIFragment{}, false
}

func (fragment *observedSNIFragment) isPassthrough() bool {
	return fragment.passthrough != nil && *fragment.passthrough
}

func (desired desiredGatewayGraph) sniBackendPoolCapability(passthrough bool) bool {
	if desired.protocol == cloud.ListenerProtocolTLSSNI || passthrough {
		return desired.sniPassthroughBackendPoolsCapable
	}
	return desired.sniBackendPoolsCapable
}

func (desired desiredGatewayGraph) sniBackendPoolCapabilityLost(passthrough bool) bool {
	return !desired.sniBackendPoolDiscoveryPending && !desired.sniBackendPoolCapability(passthrough)
}

// canKeepFlatSNI proves that the old graph serves only currently authorized
// destinations. Its weights may differ while the replacement is staging.
func (fragment *observedSNIFragment) canKeepFlatSNI(key sniFragmentKey, target desiredSNIFragment) bool {
	if !target.usesBackendPools() || fragment.backendPools != nil || !fragment.weighted() ||
		key.sniRouteKey != target.key.sniRouteKey || fragment.isPassthrough() != target.route.Passthrough ||
		!fragment.hasActiveSelector(target.route.Hostname) || !fragment.pool.AdminStateUp ||
		fragment.pool.ProvisioningStatus != "ACTIVE" || !fragment.backendTLS.AdminObserved ||
		!fragment.backendTLS.EnabledObserved || fragment.protectedBackendTLS() ||
		fragment.pool.LBMethod != string(pools.LBMethodRoundRobin) || fragment.pool.Persistence.Type != "" ||
		fragment.tcpRejection.Invalid || fragment.membersUnverified || len(fragment.members) == 0 {
		return false
	}
	allowed := make(map[netip.AddrPort]cloud.Member)
	for slot, pool := range target.route.BackendPools {
		// A missing reference can no longer prove which of the old,
		// flattened destinations belonged to the withdrawn Service.
		if pool.ServiceUID == "" || pool.BackendTLS != nil || target.backendPool(slot).backendTLS.denies() {
			return false
		}
		for _, member := range pool.Members {
			address, err := netip.ParseAddr(member.Address)
			if err != nil {
				return false
			}
			key := netip.AddrPortFrom(address, uint16(member.Port))
			if previous, exists := allowed[key]; exists && previous.RoutedExternal != member.RoutedExternal {
				return false
			}
			allowed[key] = member
		}
	}
	proof := cloud.TLSSNIRoute{BackendSubnetID: target.route.BackendSubnetID}
	for _, member := range fragment.members {
		address, err := netip.ParseAddr(member.Address)
		destination, found := allowed[netip.AddrPortFrom(address, uint16(member.ProtocolPort))]
		if err != nil || !found || member.Weight < 1 || member.Weight > 256 {
			return false
		}
		destination.Weight = member.Weight
		proof.Members = append(proof.Members, destination)
	}
	return fragment.matchesSNIMembers(proof)
}

func sniBackendPoolSuccessor(desired desiredGatewayGraph, key sniFragmentKey, fragment *observedSNIFragment) (desiredSNIFragment, bool) {
	target, found := fragment.replacementBackendPools(key, desired)
	if found && fragment.canStageBehindSNIBackendPools(key, target, desired) {
		return target, true
	}
	for _, target := range desired.sni {
		if !target.weightedBlocked && !desired.sniPaused[key] && !desired.sniPaused[target.key] && fragment.canKeepFlatSNI(key, target) {
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
		algorithm := sniBackendPoolAlgorithm(len(leaf.members))
		return want.route.ServiceUID != "" && leaf.matchesNativeBackendTransport(want, algorithm) && leaf.matchesSNIMembers(want.route)
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
	if (fragment == nil || !fragment.hasActiveSelector(target.route.Hostname)) &&
		slices.ContainsFunc(target.backendPoolPlans, func(plan backendTLSPlan) bool { return plan.retainServingOnly }) {
		return graphMutation{}, false
	}
	step := func(mutation sniMutation) (graphMutation, bool) {
		return graphMutation{kind: graphSNIMutation, sni: mutation, sniWithdrawal: mutation.kind == sniClearMembers,
			redactTLSError: mutation.target.route.BackendTLS != nil,
			message:        "Converging the weighted TLS SNI backend pools"}, true
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
		if leaf.membersUnverified || want.backendTLS.denies() || want.backendTLS.retainServingOnly || backend.ServiceUID == "" {
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
		case backendTLSAwaitSystemCA:
			switch {
			case len(leaf.members) != 0:
				mutation.kind = sniClearMembers
			case !leaf.pool.AdminStateUp:
				mutation.kind = sniEnableBackendTLSPool
			default:
				continue
			}
			return step(mutation)
		}
		type memberAddress struct {
			address string
			port    int
		}
		observedMembers := make(map[memberAddress]bool, len(leaf.members))
		for _, observed := range leaf.members {
			observedMembers[memberAddress{observed.Address, observed.ProtocolPort}] = true
		}
		for _, member := range backend.Members {
			if !observedMembers[memberAddress{member.Address, member.Port}] {
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
