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
	"net/http"
	"net/netip"
	"slices"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func validSNIDestinations(route cloud.TLSSNIRoute) bool {
	if len(route.BackendServices) == 0 {
		return route.ServiceUID != "" && route.TCPConnectionRejection == nil
	}
	if route.ServiceUID != "" || route.ServicePort != 0 || route.BackendTLS != nil {
		return false
	}
	services := route.BackendServices
	for i, service := range services {
		if service.UID == "" || service.Port < 1 || service.Port > 65535 {
			return false
		}
		if i > 0 && cmp.Or(cmp.Compare(services[i-1].UID, service.UID), cmp.Compare(services[i-1].Port, service.Port)) >= 0 {
			return false
		}
	}
	fraction := route.TCPConnectionRejection
	return fraction == nil || fraction.Valid() && fraction.RejectWeight > 0 && fraction.RejectWeight < fraction.TotalWeight
}

func validSNIMemberWeight(route cloud.TLSSNIRoute, weight int) bool {
	if len(route.BackendServices) != 0 {
		return weight >= 1 && weight <= 256
	}
	return weight == 0
}

func sameSNIRejection(current, wanted *cloud.TCPConnectionRejection) bool {
	if current == nil || wanted == nil {
		return current == wanted
	}
	return *current == *wanted
}

func (target desiredSNIFragment) weighted() bool {
	return len(target.route.BackendServices) != 0
}

func (fragment *observedSNIFragment) weighted() bool {
	return slices.Contains(fragment.pool.Tags, sniWeightedPoolTag) || fragment.tcpRejection.Invalid || fragment.tcpRejection.Value != nil
}

func (fragment *observedSNIFragment) hasUnverifiedSNIWeights(target desiredSNIFragment) bool {
	return fragment.membersUnverified || target.weighted() && (!fragment.backendTLS.AdminObserved || fragment.tcpRejection.Invalid ||
		target.route.TCPConnectionRejection != nil && !fragment.tcpRejection.Present)
}

func (fragment *observedSNIFragment) matchesSNIRejection(target desiredSNIFragment) bool {
	if !target.weighted() {
		return true
	}
	readback := fragment.tcpRejection
	wanted := target.route.TCPConnectionRejection
	if wanted == nil {
		return readback.Value == nil
	}
	if readback.Value == nil {
		// A new detached pool is eligible for its first batch.
		return fragment.policy == nil && len(fragment.members) == 0
	}
	return sameSNIRejection(readback.Value, wanted) && len(fragment.members) == len(target.route.Members)
}

func (p *Provider) prepareWeightedSNI(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) error {
	if !isSNIProtocol(desired.protocol) || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	available := false
	if (actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora") && hasSNIRejectionTarget(*desired) {
		available, _ = p.TCPConnectionRejectionAvailable(ctx)
	}
	var guards []desiredSNIFragment
	for i := range desired.sni {
		target := &desired.sni[i]
		fragment := actual.sni[target.key]
		if !target.weighted() && (fragment == nil || !fragment.membersUnverified) {
			continue
		}
		target.weightedBlocked = target.route.TCPConnectionRejection != nil && !available ||
			fragment != nil && fragment.hasUnverifiedSNIWeights(*target)
		verified := fragment != nil && fragment.hasActiveSelector(target.route.Hostname) && fragment.matches(*target)
		needsGuard := target.weightedBlocked || target.route.TCPConnectionRejection != nil && !verified || fragment != nil && !fragment.matches(*target)
		if !needsGuard || !hasSNIFallback(target.route.Hostname, desired.sni, actual.sni) {
			continue
		}
		// A guard lets the overlapping wildcard resume while this selector
		// is unavailable or while a damaged pool with this identity is rebuilt.
		denial, err := buildSNITargets(desired.spec.Identity, []cloud.TLSSNIRoute{{
			Identity: target.route.Identity, Hostname: target.route.Hostname, Deny: true, Passthrough: target.route.Passthrough,
		}})
		if err != nil {
			return err
		}
		denial[0].weightedGuard = true
		guards = append(guards, denial[0])
	}
	desired.sni = append(desired.sni, guards...)
	return ctx.Err()
}

// Passthrough Amphora omits policies attached to admin-down pools. Suspend
// covering policies before disabling a stale selector, then stage its verified
// replacement. This also works when member batch capabilities are unavailable.
func withdrawWeightedSNI(desired desiredGatewayGraph, actual observedGatewayGraph, keys []sniFragmentKey, wanted map[sniFragmentKey]desiredSNIFragment, byHostname map[string]desiredSNIFragment) (graphMutation, string, bool) {
	for _, key := range keys {
		fragment := actual.sni[key]
		if desired.sniHeldClosed[key] || desired.sniPaused[key] || fragment.policy == nil ||
			!fragment.policy.AdminStateUp && fragment.policy.ProvisioningStatus == "ACTIVE" {
			continue
		}
		target, keep := wanted[key]
		hostname := fragment.forwardingHostname()
		if !fragment.weighted() && !fragment.membersUnverified && (!byHostname[hostname].weighted() || !fragment.hasForwardingMembers()) {
			continue
		}
		if keep && fragment.matches(target) && len(fragment.members) == len(target.route.Members) {
			continue
		}
		for _, other := range keys {
			fallback := actual.sni[other]
			if !fallback.hasForwardingMembers() && !fallback.membersUnverified {
				continue
			}
			cover := fallback.forwardingHostname()
			if amphora.SNISuffixCovers(cover, hostname) || keep && amphora.SNISuffixCovers(cover, target.route.Hostname) ||
				hostname == "" && slices.ContainsFunc(desired.sni, func(candidate desiredSNIFragment) bool {
					return candidate.key.sniRouteKey == key.sniRouteKey && amphora.SNISuffixCovers(cover, candidate.route.Hostname)
				}) {
				return suspendWeightedSNI(fallback), other.uid, true
			}
		}
		return suspendWeightedSNI(fragment), key.uid, true
	}
	return graphMutation{}, "", false
}

func hasWeightedSNI(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	if slices.ContainsFunc(desired.sni, desiredSNIFragment.weighted) {
		return true
	}
	for _, fragment := range actual.sni {
		if fragment.weighted() {
			return true
		}
	}
	return false
}

func hasSNIRejectionTarget(desired desiredGatewayGraph) bool {
	return slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool { return target.route.TCPConnectionRejection != nil })
}

func suspendWeightedSNI(fragment *observedSNIFragment) graphMutation {
	return graphMutation{kind: graphDeleteChild, sniWithdrawal: true,
		deletion: gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: fragment.policy.ID},
		message:  "Suspending an unverified TLS SNI forwarding policy"}
}

// A partial target can be retried while an exact denial shields its claim.
// Restore the already-staged wildcard first so a failed batch cannot keep
// unrelated names on that wildcard closed.
func restoreGuardedSNIFallback(desired desiredGatewayGraph, actual observedGatewayGraph) (graphMutation, bool) {
	verified := func(target desiredSNIFragment) bool {
		fragment := actual.sni[target.key]
		return fragment != nil && len(fragment.members) == len(target.route.Members) && fragment.matches(target) &&
			fragment.hasActiveSelector(target.route.Hostname)
	}
	guards := make(map[string]bool)
	for _, target := range desired.sni {
		if target.weightedGuard && verified(target) {
			guards[target.route.Hostname] = true
		}
	}
	if len(guards) == 0 {
		return graphMutation{}, false
	}
	for _, fallback := range desired.sni {
		activeGuard := false
		for host := range guards {
			activeGuard = activeGuard || amphora.SNISuffixCovers(fallback.route.Hostname, host)
		}
		if !activeGuard || fallback.route.Deny || actual.sni[fallback.key] == nil || !actual.sni[fallback.key].matches(fallback) ||
			slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool {
				host := target.route.Hostname
				return amphora.SNISuffixCovers(fallback.route.Hostname, host) && !guards[host] && !verified(target)
			}) {
			continue
		}
		if step, needed := nextSNIFragmentMutation(fallback, actual.sni[fallback.key]); needed && step.sni.kind == sniEnablePolicy {
			return step, true
		}
	}
	return graphMutation{}, false
}

func (fragment *observedSNIFragment) detached() bool {
	return fragment.policy == nil && fragment.pool.Listeners != nil && len(fragment.pool.Listeners) == 0
}

func sniBatchMember(address string, port int, subnet string, weight int, tags []string) pools.BatchUpdateMemberOpts {
	var subnetID *string
	if subnet != "" {
		subnetID = &subnet
	}
	return pools.BatchUpdateMemberOpts{
		Address: address, ProtocolPort: port, SubnetID: subnetID,
		AdminStateUp: new(true), Backup: new(false), Weight: new(weight), Tags: tags,
	}
}

func (p *Provider) replaceSNITCPMembers(ctx context.Context, step sniMutation, tags []string) error {
	route := step.target.route
	tags = slices.Clone(tags)
	slices.Sort(tags)
	members := make([]pools.BatchUpdateMemberOpts, 0, len(route.Members))
	for _, member := range route.Members {
		members = append(members, sniBatchMember(member.Address, member.Port, route.BackendSubnetID, member.Weight, tags))
	}
	return p.octavia.ReplaceTCPMembersAndRejection(ctx, step.parentID, members, route.TCPConnectionRejection)
}

func (fragment *observedSNIFragment) stagedSNIRejectionMembers() []pools.BatchUpdateMemberOpts {
	if !slices.Contains(fragment.pool.Tags, sniWeightedPoolTag) || !fragment.detached() || fragment.membersUnverified ||
		fragment.pool.ProvisioningStatus != "PENDING_UPDATE" || fragment.protectedBackendTLS() ||
		!fragment.tcpRejection.Present || fragment.tcpRejection.Invalid || fragment.tcpRejection.Value == nil {
		return nil
	}
	fraction := fragment.tcpRejection.Value
	if fraction.RejectWeight <= 0 || fraction.RejectWeight >= fraction.TotalWeight {
		return nil
	}
	seen := make(map[netip.AddrPort]bool)
	batch := make([]pools.BatchUpdateMemberOpts, 0, len(fragment.members))
	for _, member := range fragment.members {
		address, err := netip.ParseAddr(member.Address)
		if err != nil || !address.Is4() || !address.IsGlobalUnicast() || address.IsLoopback() ||
			member.ProtocolPort < 1 || member.ProtocolPort > 65535 || member.Weight < 1 || member.Weight > 256 ||
			!member.AdminStateUp || member.Backup || member.ProvisioningStatus != "PENDING_CREATE" {
			return nil
		}
		key := netip.AddrPortFrom(address, uint16(member.ProtocolPort))
		if seen[key] {
			return nil
		}
		seen[key] = true
		tags := slices.Clone(member.Tags)
		slices.Sort(tags)
		batch = append(batch, sniBatchMember(member.Address, member.ProtocolPort, member.SubnetID, member.Weight, tags))
	}
	return batch
}

// Replay only the exact staged payload that Octavia already accepted. Its
// operation token remains private, and the pool is still detached.
func (p *Provider) replayPendingSNIRejection(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, bool, error) {
	if !isSNIProtocol(desired.protocol) || actual.loadBalancer == nil || actual.loadBalancer.Provider != "amphora" ||
		actual.loadBalancer.ProvisioningStatus != "PENDING_UPDATE" {
		return cloud.GatewayGraphResult{}, false, nil
	}
	for _, fragment := range actual.sni {
		batch := fragment.stagedSNIRejectionMembers()
		if len(batch) == 0 {
			continue
		}
		delay := p.transportReplayDelay(fragment.pool.ID, fragment.tcpRejection.UpdatedAt, time.Now())
		actual.outcome = cloud.ProgressingOutcome("Waiting for Octavia TLS SNI batch activation", max(delay, transportReplayInterval, p.pollInterval))
		result := p.pendingGraphResult(ctx, desired, actual)
		if validate := desired.spec.ValidateMutation; validate != nil {
			if err := validate(ctx); err != nil {
				return cloud.GatewayGraphResult{}, true, err
			}
		}
		if delay != 0 {
			return result, true, nil
		}
		err := p.octavia.ReplaceTCPMembersAndRejection(ctx, fragment.pool.ID, batch, fragment.tcpRejection.Value)
		if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
			return result, true, nil
		}
		result.Outcome.MutationSubmitted = err == nil
		return result, true, err
	}
	return cloud.GatewayGraphResult{}, false, nil
}
