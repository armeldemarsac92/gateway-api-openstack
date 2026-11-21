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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const (
	roleSNIPool   = openstackidentity.RoleSNIPool
	roleSNIMember = openstackidentity.RoleSNIMember
	roleSNIPolicy = openstackidentity.RoleSNIPolicy
	roleSNIRule   = openstackidentity.RoleSNIRule
	ruleTLSSNI    = "TLS_SNI"
	sniEqualTo    = "EQUAL_TO"
	sniEndsWith   = "ENDS_WITH"
)

var sniProfileTag = openstackidentity.Tag("profile", "tls-sni-v1")

type sniRouteKey struct {
	namespace string
	name      string
	uid       string
}

type sniFragmentKey struct {
	sniRouteKey
	target string
}

type desiredSNIFragment struct {
	route     cloud.TLSSNIRoute
	identity  Identity
	key       sniFragmentKey
	targetTag string
}

type observedSNIFragment struct {
	pool    *pools.Pool
	members []pools.Member
	policy  *l7policies.L7Policy
	rule    *l7policies.Rule
}

func sniCompareType(hostname string) string {
	if strings.HasPrefix(hostname, "*.") {
		return sniEndsWith
	}
	return sniEqualTo
}

func (step graphMutation) isRestrictiveSNI() bool {
	return step.sniWithdrawal || step.kind == graphSNIMutation && step.sni.target.route.Deny
}

func buildSNITargets(gateway cloud.Identity, routes []cloud.TLSSNIRoute) ([]desiredSNIFragment, error) {
	result := make([]desiredSNIFragment, 0, len(routes))
	names := make(map[string]bool)
	owners := make(map[string]cloud.TLSSNIRoute)
	for _, route := range routes {
		id, err := NewIdentity(route.Identity)
		if err != nil {
			return nil, err
		}
		if !sameGatewayIdentity(gateway, route.Identity) || cloud.ValidateRouteIdentity(route.Identity) != nil {
			return nil, fmt.Errorf("%w: TLSRoute belongs to a different Gateway or has incomplete identity", cloud.ErrOwnershipConflict)
		}
		if amphora.ValidateSNIHostname(route.Hostname) != nil || names[route.Hostname] {
			return nil, invalidSNI("TLSRoutes require distinct DNS selectors")
		}
		if route.Deny {
			if route.ServiceUID != "" || len(route.Members) != 0 || route.BackendSubnetID != "" || len(route.BackendSecurityGroupIDs) != 0 {
				return nil, invalidSNI("TLS SNI denial requires an empty target without a backend identity")
			}
		} else if route.ServiceUID == "" || len(route.Members) == 0 {
			return nil, invalidSNI("TLSRoute forwarding requires one identified Service with ready members")
		}
		names[route.Hostname] = true
		route.Members = slices.Clone(route.Members)
		slices.SortFunc(route.Members, func(a, b cloud.Member) int {
			return cmp.Or(cmp.Compare(a.Address, b.Address), cmp.Compare(a.Port, b.Port))
		})
		for i, member := range route.Members {
			address, err := netip.ParseAddr(member.Address)
			if err != nil || !address.Is4() || !address.IsGlobalUnicast() || address.IsLoopback() ||
				member.Port < 1 || member.Port > 65535 || member.Draining || member.Weight != 0 ||
				i > 0 && route.Members[i-1].Address == member.Address && route.Members[i-1].Port == member.Port {
				return nil, invalidSNI("invalid or duplicate IPv4 TLSRoute member")
			}
		}
		route.BackendSecurityGroupIDs = slices.Clone(route.BackendSecurityGroupIDs)
		slices.Sort(route.BackendSecurityGroupIDs)
		if owner, exists := owners[route.Identity.RouteUID]; exists {
			if owner.Identity != route.Identity {
				return nil, fmt.Errorf("%w: inconsistent identity for the same TLSRoute UID", cloud.ErrOwnershipConflict)
			}
			if owner.ServiceUID != route.ServiceUID || owner.BackendSubnetID != route.BackendSubnetID ||
				!slices.Equal(owner.BackendSecurityGroupIDs, route.BackendSecurityGroupIDs) || !slices.Equal(owner.Members, route.Members) {
				return nil, invalidSNI("hostnames on one TLSRoute require the same backend Service, topology and members")
			}
		} else {
			owners[route.Identity.RouteUID] = route
		}
		target, err := json.Marshal(struct {
			Hostname   string
			ServiceUID string
			SubnetID   string
			Members    []cloud.Member
		}{route.Hostname, route.ServiceUID, route.BackendSubnetID, route.Members})
		if err != nil {
			return nil, err
		}
		targetTag := openstackidentity.Tag("sni-target", fmt.Sprintf("%x", sha256.Sum256(target)))
		_, encodedTarget, _ := openstackidentity.ParseTag(targetTag)
		key := sniFragmentKey{
			sniRouteKey: sniRouteKey{
				namespace: encodedSNITag("route-namespace", route.Identity.RouteNamespace),
				name:      encodedSNITag("route-name", route.Identity.RouteName),
				uid:       encodedSNITag("route-uid", route.Identity.RouteUID),
			},
			target: encodedTarget,
		}
		fragment := desiredSNIFragment{route: route, identity: id, key: key, targetTag: targetTag}
		for _, role := range []string{roleSNIPool, roleSNIMember, roleSNIPolicy, roleSNIRule} {
			if _, err := fragment.tags(role); err != nil {
				return nil, err
			}
		}
		result = append(result, fragment)
	}
	slices.SortFunc(result, func(a, b desiredSNIFragment) int {
		return compareSNISelectors(a.route.Hostname, b.route.Hostname)
	})
	return result, nil
}

func compareSNISelectors(a, b string) int {
	aSuffix, bSuffix := sniCompareType(a) == sniEndsWith, sniCompareType(b) == sniEndsWith
	switch {
	case aSuffix && bSuffix:
		return cmp.Or(cmp.Compare(len(b), len(a)), cmp.Compare(a, b))
	case aSuffix:
		return 1
	case bSuffix:
		return -1
	default:
		return cmp.Compare(a, b)
	}
}

func sniRouteOwners(fragments []desiredSNIFragment) []desiredSNIFragment {
	var owners []desiredSNIFragment
	seen := make(map[string]bool)
	for _, fragment := range fragments {
		uid := fragment.route.Identity.RouteUID
		if !fragment.route.Deny && !seen[uid] {
			owners = append(owners, fragment)
			seen[uid] = true
		}
	}
	return owners
}

func sniRouteResults(fragments []desiredSNIFragment, observation cloud.RouteResult) []cloud.RouteGraphResult {
	var results []cloud.RouteGraphResult
	for _, owner := range sniRouteOwners(fragments) {
		results = append(results, cloud.RouteGraphResult{Identity: owner.route.Identity, Result: observation})
	}
	return results
}

func invalidSNI(message string) error {
	return cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, fmt.Errorf("%s", message))
}

func encodedSNITag(key, value string) string {
	_, encoded, _ := openstackidentity.ParseTag(openstackidentity.Tag(key, value))
	return encoded
}

func (fragment desiredSNIFragment) tags(role string) ([]string, error) {
	tags, err := fragment.identity.RouteTags(role)
	if err != nil {
		return nil, err
	}
	// The input was encoded once while building the key. Use the exact same
	// representation to recover an interrupted create from a later inventory.
	return append(tags, fragment.targetTag), nil
}

func (p *Provider) sniOwnership(identity Identity, tags []string, project, id, role string) (sniFragmentKey, error) {
	if err := p.validateOptionalProject(role, id, project); err != nil {
		return sniFragmentKey{}, err
	}
	key := sniFragmentKey{}
	fields := map[string]*string{"route-namespace": &key.namespace, "route-name": &key.name, "route-uid": &key.uid, "sni-target": &key.target}
	for _, tag := range tags {
		name, encoded, ok := openstackidentity.ParseTag(tag)
		if field, known := fields[name]; ok && known {
			if *field != "" || encoded == "" {
				return key, fmt.Errorf("%w: ambiguous TLSRoute identity for %s %s", cloud.ErrOwnershipConflict, role, id)
			}
			*field = encoded
		}
	}
	if id == "" || !identity.MatchesGateway(tags, role) || key.namespace == "" || key.name == "" || key.uid == "" || key.target == "" {
		return key, fmt.Errorf("%w: incomplete TLSRoute identity for %s %s", cloud.ErrOwnershipConflict, role, id)
	}
	if !openstackidentity.ValidSNITarget(key.target) {
		return key, fmt.Errorf("%w: invalid TLSRoute target identity for %s %s", cloud.ErrOwnershipConflict, role, id)
	}
	return key, nil
}

func (p *Provider) observeSNIGraph(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, inventory octavia.ListenerInventory) (observedGatewayGraph, error) {
	lb := actual.loadBalancer
	if !slices.Contains(lb.Tags, sniProfileTag) {
		return actual, fmt.Errorf("%w: load balancer lacks the TLS SNI profile identity", cloud.ErrOwnershipConflict)
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return actual, err
	}
	listener, err := p.managedGatewayListener(desired.identity, lb.ID, inventory.Items)
	if err != nil {
		return actual, err
	}
	actual.listener = listener
	pendingUpdate := lb.ProvisioningStatus == "PENDING_UPDATE" && desired.spec.Gateway.Disposition == cloud.GraphEnsure
	if listener != nil {
		actual.state.ListenerID = listener.ID
		if listener.Protocol != cloud.ListenerProtocolTCP || listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort ||
			!slices.Contains(listener.Tags, sniProfileTag) || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID ||
			!actual.adminObserved[listener.ID] || listener.DefaultTlsContainerRef != "" || len(listener.SniContainerRefs) != 0 {
			return actual, fmt.Errorf("%w: listener differs from the TLS SNI profile binding", cloud.ErrOwnershipConflict)
		}
		if err := requireListenerDefaultPoolInventory(lb.ID, inventory); err != nil {
			return actual, err
		}
		if listener.DefaultPoolID != "" {
			// Prove ownership of the frontend before closing it. An unexpected
			// default may belong to somebody else, so leave its edge untouched.
			return actual, nil
		}
		if listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" &&
			(!pendingUpdate || listener.ProvisioningStatus != "PENDING_UPDATE") {
			actual.outcome = p.progressingOutcome("Waiting for TLS SNI listener provisioning")
			return actual, nil
		}
	}
	// A policy update can mark the parent resources PENDING_UPDATE while
	// disjoint SNI routes retain complete ACTIVE forwarding fragments.
	if actual.outcome.State != cloud.OutcomeReady && (!pendingUpdate || !sniFrontendObserved(desired, actual)) {
		return actual, nil
	}
	owners := make(map[string]sniRouteKey)
	for _, target := range desired.sni {
		owners[target.key.uid] = target.key.sniRouteKey
	}
	byKey := make(map[sniFragmentKey]*observedSNIFragment)
	fragmentFor := func(key sniFragmentKey) *observedSNIFragment {
		if byKey[key] == nil {
			byKey[key] = &observedSNIFragment{}
		}
		return byKey[key]
	}
	poolInventory, err := p.listGatewayPoolInventory(ctx, lb)
	if err != nil {
		return actual, err
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, poolInventory.Items, func(pool pools.Pool) string { return pool.ID }); err != nil {
		return actual, err
	}
	for i := range poolInventory.Items {
		pool := &poolInventory.Items[i]
		key, err := p.sniOwnership(desired.identity, pool.Tags, pool.ProjectID, pool.ID, roleSNIPool)
		if err != nil {
			return actual, err
		}
		if owner, exists := owners[key.uid]; exists && owner != key.sniRouteKey {
			return actual, fmt.Errorf("%w: inconsistent TLSRoute owner for pool %s", cloud.ErrOwnershipConflict, pool.ID)
		}
		owners[key.uid] = key.sniRouteKey
		if fragmentFor(key).pool != nil || pool.Protocol != cloud.ListenerProtocolTCP || len(pool.Loadbalancers) != 1 ||
			pool.Loadbalancers[0].ID != lb.ID || pool.MonitorID != "" || poolInventory.BackendH2C[pool.ID] ||
			len(pool.Listeners) > 1 || (len(pool.Listeners) == 1 && (listener == nil || pool.Listeners[0].ID != listener.ID)) {
			return actual, fmt.Errorf("%w: invalid TLS SNI pool relationship", cloud.ErrOwnershipConflict)
		}
		members, unreported, err := p.octavia.ListMembersWithAdminState(ctx, pool.ID, pools.ListMembersOpts{ProjectID: p.projectID})
		if err != nil {
			return actual, err
		}
		if err := crossCheckCompleteChildInventory("pool", pool.ID, "member", pool.Members, members, func(m pools.Member) string { return m.ID }); err != nil {
			return actual, err
		}
		for _, member := range members {
			owner, err := p.sniOwnership(desired.identity, member.Tags, member.ProjectID, member.ID, roleSNIMember)
			if err != nil {
				return actual, err
			}
			if _, omitted := unreported[member.ID]; omitted || owner != key || member.PoolID != "" && member.PoolID != pool.ID {
				return actual, fmt.Errorf("%w: invalid TLS SNI member relationship", cloud.ErrOwnershipConflict)
			}
		}
		fragmentFor(key).pool, fragmentFor(key).members = pool, members
	}
	if listener != nil {
		rows, complete, err := p.octavia.ListPolicyObservations(ctx, l7policies.ListOpts{ListenerID: listener.ID, ProjectID: p.projectID})
		if err != nil {
			return actual, err
		}
		if !complete {
			return actual, invalidSNI("Octavia omitted the TLS SNI policy inventory")
		}
		policies := make([]l7policies.L7Policy, 0, len(rows))
		for _, row := range rows {
			policies = append(policies, row.Policy)
			policy := row.Policy
			key, err := p.sniOwnership(desired.identity, policy.Tags, policy.ProjectID, policy.ID, roleSNIPolicy)
			if err != nil {
				return actual, err
			}
			fragment := fragmentFor(key)
			if fragment.policy != nil || fragment.pool == nil || policy.ListenerID != listener.ID || policy.RedirectPoolID != fragment.pool.ID ||
				policy.Action != string(l7policies.ActionRedirectToPool) || row.AdminState == nil || row.ExtendedTargetFields ||
				row.RequestHeaderOperationsErr != nil || len(row.RequestHeaderOperations) != 0 || policy.RedirectURL != "" || policy.RedirectPrefix != "" {
				return actual, fmt.Errorf("%w: invalid TLS SNI policy relationship", cloud.ErrOwnershipConflict)
			}
			rules, err := p.octavia.ObserveRules(ctx, policy.ID, l7policies.ListRulesOpts{ProjectID: p.projectID})
			if err != nil {
				return actual, err
			}
			listed := make([]l7policies.Rule, 0, len(rules))
			for _, rule := range rules {
				listed = append(listed, rule.Rule)
			}
			if err := crossCheckCompleteChildInventory("policy", policy.ID, "rule", policy.Rules, listed, func(rule l7policies.Rule) string { return rule.ID }); err != nil {
				return actual, err
			}
			if len(rules) > 1 {
				return actual, fmt.Errorf("%w: unexpected rules for one TLS SNI policy", cloud.ErrOwnershipConflict)
			}
			if len(rules) == 1 {
				rule := rules[0]
				owner, err := p.sniOwnership(desired.identity, rule.Tags, rule.ProjectID, rule.ID, roleSNIRule)
				if err != nil {
					return actual, err
				}
				if owner != key || rule.AdminState == nil || rule.Inverted == nil {
					return actual, fmt.Errorf("%w: invalid TLS SNI rule relationship", cloud.ErrOwnershipConflict)
				}
				fragment.rule = &rule.Rule
			}
			fragment.policy = &policy
		}
		if err := crossCheckCompleteChildInventory("listener", listener.ID, "policy", listener.L7Policies, policies, func(policy l7policies.L7Policy) string { return policy.ID }); err != nil {
			return actual, err
		}
	}
	for _, fragment := range byKey {
		attached := fragment.policy != nil && fragment.policy.AdminStateUp && fragment.rule != nil
		if fragment.pool.Listeners == nil || (len(fragment.pool.Listeners) == 1) != attached {
			return actual, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("incomplete TLS SNI pool attachment readback"))
		}
	}
	actual.sni = byKey
	return actual, nil
}

func (fragment *observedSNIFragment) matches(wanted desiredSNIFragment) bool {
	if fragment.pool == nil || fragment.pool.ProvisioningStatus != "ACTIVE" || !fragment.pool.AdminStateUp ||
		fragment.pool.LBMethod != string(pools.LBMethodRoundRobin) || fragment.pool.TLSEnabled || fragment.pool.Persistence.Type != "" ||
		len(fragment.members) > len(wanted.route.Members) {
		return false
	}
	seen := make(map[netip.AddrPort]bool)
	for _, member := range fragment.members {
		address, err := netip.ParseAddr(member.Address)
		if err != nil || member.ProtocolPort < 1 || member.ProtocolPort > 65535 {
			return false
		}
		key := netip.AddrPortFrom(address, uint16(member.ProtocolPort))
		if seen[key] || member.ProvisioningStatus != "ACTIVE" || !member.AdminStateUp || member.Backup || member.Weight != 1 ||
			wanted.route.BackendSubnetID != "" && member.SubnetID != wanted.route.BackendSubnetID ||
			!slices.ContainsFunc(wanted.route.Members, func(m cloud.Member) bool { return m.Address == member.Address && m.Port == member.ProtocolPort }) {
			return false
		}
		seen[key] = true
	}
	if fragment.policy != nil {
		if fragment.policy.ProvisioningStatus != "ACTIVE" || fragment.policy.AdminStateUp && fragment.rule == nil {
			return false
		}
	}
	return fragment.rule == nil || fragment.rule.ProvisioningStatus == "ACTIVE" && fragment.rule.AdminStateUp && !fragment.rule.Invert &&
		fragment.rule.RuleType == string(ruleTLSSNI) && fragment.rule.CompareType == sniCompareType(wanted.route.Hostname) &&
		fragment.rule.Key == "" && fragment.rule.Value == wanted.route.Hostname
}

func sniFrontendObserved(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	lb, listener := actual.loadBalancer, actual.listener
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || lb == nil || !lb.AdminStateUp ||
		listener == nil || !listener.AdminStateUp || listener.DefaultPoolID != "" ||
		desired.spec.Gateway.Spec.ExternalNetworkID != "" && actual.state.FloatingIPAddress == "" {
		return false
	}
	return lb.ProvisioningStatus == "ACTIVE" && listener.ProvisioningStatus == "ACTIVE" ||
		lb.ProvisioningStatus == "PENDING_UPDATE" && (listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "PENDING_UPDATE")
}

func (fragment *observedSNIFragment) forwardingHostname() string {
	policy, rule := fragment.policy, fragment.rule
	// Pending changes may have already updated admin_state_up in the API
	// while the previous policy is still forwarding in Amphora.
	if policy == nil || rule == nil || !policy.AdminStateUp && policy.ProvisioningStatus == "ACTIVE" ||
		!rule.AdminStateUp && rule.ProvisioningStatus == "ACTIVE" ||
		rule.RuleType != string(ruleTLSSNI) || rule.CompareType != sniCompareType(rule.Value) || rule.Invert || rule.Key != "" {
		return ""
	}
	return strings.ToLower(rule.Value)
}

func readySNIRoutes(desired desiredGatewayGraph, actual observedGatewayGraph) map[string]bool {
	if !sniFrontendObserved(desired, actual) {
		return nil
	}
	wanted := make(map[sniFragmentKey]bool, len(desired.sni))
	wantedHostnames := make(map[string]bool, len(desired.sni))
	ready := make(map[string]bool)
	for _, target := range desired.sni {
		wanted[target.key] = true
		wantedHostnames[target.route.Hostname] = true
		if !target.route.Deny {
			ready[target.key.uid] = true
		}
	}
	selectors := make(map[string]int)
	for key, fragment := range actual.sni {
		if !wanted[key] {
			// Retired fragments still belong to their route until removed.
			delete(ready, key.uid)
		}
		if hostname := fragment.forwardingHostname(); hostname != "" {
			selectors[hostname]++
			if !wantedHostnames[hostname] {
				// The first covering desired suffix inherits these names once
				// Octavia confirms that the old selector is no longer active.
				for _, target := range desired.sni {
					if amphora.SNISuffixCovers(target.route.Hostname, hostname) {
						delete(ready, target.key.uid)
						break
					}
				}
			}
		}
	}
	for _, target := range desired.sni {
		fragment := actual.sni[target.key]
		if ready[target.key.uid] && (selectors[target.route.Hostname] != 1 || fragment == nil ||
			fragment.policy == nil || !fragment.policy.AdminStateUp || fragment.rule == nil ||
			len(fragment.members) != len(target.route.Members) || !fragment.matches(target)) {
			delete(ready, target.key.uid)
		}
	}
	return ready
}

func sniDeletionStep(fragment *observedSNIFragment) graphMutation {
	var step gatewayDeletionStep
	switch {
	case fragment.policy != nil && fragment.policy.AdminStateUp:
		step = gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: fragment.policy.ID}
	case fragment.rule != nil:
		step = gatewayDeletionStep{resource: gatewayDeletionL7Rule, resourceID: fragment.rule.ID, parentID: fragment.policy.ID}
	case fragment.policy != nil:
		step = gatewayDeletionStep{resource: gatewayDeletionL7Policy, resourceID: fragment.policy.ID}
	case len(fragment.members) != 0:
		step = gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: fragment.members[0].ID, parentID: fragment.pool.ID}
	default:
		step = gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: fragment.pool.ID}
	}
	return graphMutation{kind: graphDeleteChild, deletion: step, sniWithdrawal: true, message: "Withdrawing an obsolete TLS SNI resource"}
}

func (p *Provider) buildSNIGraphPlan(desired desiredGatewayGraph, actual observedGatewayGraph) ([]graphMutation, cloud.GatewayGraphResult, error) {
	pending := p.progressingOutcome("Waiting for the TLS SNI graph")
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(actual.state),
		Routes: sniRouteResults(desired.sni, cloud.RouteResult{Outcome: pending}),
	}
	plan := func(step graphMutation) ([]graphMutation, cloud.GatewayGraphResult, error) {
		result.Outcome = p.progressingOutcome(step.message)
		result.Gateway.Outcome = result.Outcome
		return []graphMutation{step}, result, nil
	}
	if actual.listener != nil && actual.listener.DefaultPoolID != "" {
		if actual.listener.AdminStateUp {
			return plan(graphMutation{kind: graphDisableListener, sniWithdrawal: true, message: "Closing TLS SNI listener with an unexpected default pool"})
		}
		result.Outcome, result.Gateway.Outcome = pending, pending
		return nil, result, fmt.Errorf("%w: TLS SNI listener has an unexpected default pool", cloud.ErrOwnershipConflict)
	}
	wanted := make(map[sniFragmentKey]desiredSNIFragment)
	byHostname := make(map[string]desiredSNIFragment)
	for _, frag := range desired.sni {
		wanted[frag.key], byHostname[frag.route.Hostname] = frag, frag
	}
	keys := slices.SortedFunc(maps.Keys(actual.sni), func(a, b sniFragmentKey) int {
		hostA, hostB := actual.sni[a].forwardingHostname(), actual.sni[b].forwardingHostname()
		// Turn off broad selectors first when their narrower selectors are
		// also retiring, so withdrawal itself cannot expose a fallback.
		if sniCompareType(hostA) == sniEndsWith || sniCompareType(hostB) == sniEndsWith {
			if order := compareSNISelectors(hostB, hostA); order != 0 {
				return order
			}
		}
		return cmp.Or(cmp.Compare(a.namespace, b.namespace), cmp.Compare(a.name, b.name), cmp.Compare(a.uid, b.uid), cmp.Compare(a.target, b.target))
	})
	var retire []sniFragmentKey
	waitForOctavia := false
	for _, key := range keys {
		fragment := actual.sni[key]
		target, keep := wanted[key]
		if slices.ContainsFunc(fragment.members, func(m pools.Member) bool { return strings.HasPrefix(m.ProvisioningStatus, "PENDING_") }) ||
			strings.HasPrefix(fragment.pool.ProvisioningStatus, "PENDING_") ||
			fragment.policy != nil && strings.HasPrefix(fragment.policy.ProvisioningStatus, "PENDING_") ||
			fragment.rule != nil && strings.HasPrefix(fragment.rule.ProvisioningStatus, "PENDING_") {
			waitForOctavia = true
			continue
		}
		if !keep || !fragment.matches(target) {
			retire = append(retire, key)
		}
	}
	ready := readySNIRoutes(desired, actual)
	for i, route := range result.Routes {
		if ready[encodedSNITag("route-uid", route.Identity.RouteUID)] {
			result.Routes[i].Result.Outcome = cloud.ReadyOutcome()
		}
	}
	if waitForOctavia {
		result.Outcome, result.Gateway.Outcome = pending, pending
		return nil, result, nil
	}
	if actual.listener != nil && actual.listener.AdminStateUp {
		step, coveredUID, needed := protectSNIDenials(desired.sni, actual.sni, keys)
		if needed {
			for i, route := range result.Routes {
				if encodedSNITag("route-uid", route.Identity.RouteUID) == coveredUID {
					result.Routes[i].Result.Outcome = pending
				}
			}
			return plan(step)
		}
		// Stage new negative claims first. Enabling their empty target can
		// only restrict the existing frontend.
		for _, target := range desired.sni {
			if !target.route.Deny {
				continue
			}
			fragment := actual.sni[target.key]
			if fragment == nil || fragment.matches(target) {
				if step, ok := nextSNIFragmentMutation(target, fragment); ok {
					return plan(step)
				}
			}
		}
	}
	for _, key := range retire {
		fragment := actual.sni[key]
		step := sniDeletionStep(fragment)
		if step.deletion.resource != gatewayDisableL7Policy {
			continue
		}
		if actual.listener != nil && actual.listener.AdminStateUp {
			hostname := fragment.forwardingHostname()
			if target, remains := byHostname[hostname]; remains && hasSNIFallback(hostname, desired.sni, actual.sni) {
				replacement := actual.sni[target.key]
				switch {
				case replacement == nil || replacement.matches(target):
					// Keep the old rule effective until its replacement is
					// active. This also preserves a denial on grant restore.
					if replacementStep, ok := nextSNIFragmentMutation(target, replacement); ok {
						return plan(replacementStep)
					}
				case target.key != key:
					// Rebuild a damaged replacement while the preceding
					// selector still shields the less specific route.
					return plan(sniDeletionStep(replacement))
				default:
					return plan(graphMutation{kind: graphDisableListener, sniWithdrawal: true, message: "Closing TLS SNI listener while repairing a protective selector"})
				}
			}
		}
		return plan(step)
	}
	if len(retire) != 0 {
		return plan(sniDeletionStep(actual.sni[retire[0]]))
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		if actual.listener != nil {
			return plan(graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(actual.listener.ID), sniWithdrawal: true, message: "Deleting TLS SNI listener"})
		}
		if steps := gatewayGraphDeletionPlan(actual); len(steps) != 0 {
			return plan(steps[0])
		}
		return nil, result, nil
	}
	if actual.listener != nil && actual.listener.ProvisioningStatus != "ACTIVE" {
		result.Outcome, result.Gateway.Outcome = pending, pending
		return nil, result, cloud.NewProviderError(cloud.ErrorCategoryResourceFailure, fmt.Errorf("TLS SNI listener provisioning failed"))
	}
	if steps, err := buildGatewayPrerequisitePlan(desired, actual); err != nil {
		return nil, result, err
	} else if len(steps) != 0 {
		return plan(steps[0])
	}
	for _, target := range desired.sni {
		if step, ok := nextSNIFragmentMutation(target, actual.sni[target.key]); ok {
			return plan(step)
		}
	}
	if steps, err := buildGatewayTrafficRestorationPlan(desired, actual); err != nil {
		return nil, result, err
	} else if len(steps) != 0 {
		return plan(steps[0])
	}
	return nil, result, nil
}

// protectSNIDenials only changes existing resources. A full pool, policy or
// rule quota must not keep an unauthorized match forwarding.
func protectSNIDenials(desired []desiredSNIFragment, actual map[sniFragmentKey]*observedSNIFragment, keys []sniFragmentKey) (graphMutation, string, bool) {
	for _, target := range desired {
		if !target.route.Deny {
			continue
		}
		hostname := target.route.Hostname
		var guard *observedSNIFragment
		for _, key := range keys {
			fragment := actual[key]
			if fragment.forwardingHostname() == hostname && fragment.matches(target) &&
				(guard == nil || fragment.policy.Position < guard.policy.Position) {
				guard = fragment
			}
		}
		for _, key := range keys {
			fragment := actual[key]
			if fragment.forwardingHostname() != hostname || len(fragment.members) == 0 {
				continue
			}
			if guard != nil && guard.policy.Position > 0 && guard.policy.Position < fragment.policy.Position {
				continue
			}
			// Preserve the rule and the pool, removing all destinations in
			// one write. Its name cannot fall through while we stage a guard.
			return graphMutation{kind: graphSNIMutation, sniWithdrawal: true,
				sni:     sniMutation{kind: sniClearMembers, parentID: fragment.pool.ID},
				message: "Removing unauthorized TLS SNI pool members"}, "", true
		}
		if guard == nil {
			// The keys are ordered broadest first, so disabling one covering
			// suffix cannot expose a wider candidate for the denied names.
			for _, key := range keys {
				fragment := actual[key]
				if len(fragment.members) != 0 && amphora.SNISuffixCovers(fragment.forwardingHostname(), hostname) {
					return graphMutation{kind: graphDeleteChild, sniWithdrawal: true,
						deletion: gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: fragment.policy.ID},
						message:  "Suspending a broader TLS SNI policy until the denial is installed"}, key.uid, true
				}
			}
		}
	}
	return graphMutation{}, "", false
}

func hasSNIFallback(hostname string, desired []desiredSNIFragment, actual map[sniFragmentKey]*observedSNIFragment) bool {
	for _, target := range desired {
		if !target.route.Deny && amphora.SNISuffixCovers(target.route.Hostname, hostname) {
			return true
		}
	}
	for _, fragment := range actual {
		if len(fragment.members) != 0 && amphora.SNISuffixCovers(fragment.forwardingHostname(), hostname) {
			return true
		}
	}
	return false
}

func nextSNIFragmentMutation(target desiredSNIFragment, fragment *observedSNIFragment) (graphMutation, bool) {
	mutation := sniMutation{target: target}
	switch {
	case fragment == nil:
		mutation.kind = sniCreatePool
	case len(fragment.members) != len(target.route.Members):
		mutation.kind, mutation.parentID = sniCreateMember, fragment.pool.ID
		for _, member := range target.route.Members {
			if !slices.ContainsFunc(fragment.members, func(m pools.Member) bool { return m.Address == member.Address && m.ProtocolPort == member.Port }) {
				mutation.member = member
				break
			}
		}
	case fragment.policy == nil:
		mutation.kind, mutation.parentID = sniCreatePolicy, fragment.pool.ID
	case fragment.rule == nil:
		mutation.kind, mutation.parentID = sniCreateRule, fragment.policy.ID
	case !fragment.policy.AdminStateUp:
		mutation.kind, mutation.parentID = sniEnablePolicy, fragment.policy.ID
	default:
		return graphMutation{}, false
	}
	return graphMutation{kind: graphSNIMutation, sni: mutation, message: "Converging the TLS SNI route"}, true
}

type sniMutationKind uint8

const (
	sniCreatePool sniMutationKind = iota
	sniCreateMember
	sniCreatePolicy
	sniCreateRule
	sniEnablePolicy
	sniClearMembers
)

type sniMutation struct {
	kind     sniMutationKind
	target   desiredSNIFragment
	parentID string
	member   cloud.Member
}

func (p *Provider) executeSNIMutation(ctx context.Context, actual observedGatewayGraph, step sniMutation) error {
	tagsFor := func(role string) []string {
		tags, _ := step.target.tags(role) // Validated before the graph was observed.
		return tags
	}
	switch step.kind {
	case sniCreatePool:
		_, err := p.octavia.CreatePool(ctx, pools.CreateOpts{
			Protocol: pools.ProtocolTCP, LBMethod: pools.LBMethodRoundRobin, LoadbalancerID: actual.state.LoadBalancerID,
			Name: resourceName(step.target.route.Identity, roleSNIPool), AdminStateUp: boolPointer(true), Tags: tagsFor(roleSNIPool),
		})
		return err
	case sniCreateMember:
		_, err := p.octavia.CreateMember(ctx, step.parentID, pools.CreateMemberOpts{
			Address: step.member.Address, ProtocolPort: step.member.Port, SubnetID: step.target.route.BackendSubnetID,
			AdminStateUp: boolPointer(true), Weight: new(1), Tags: tagsFor(roleSNIMember),
		})
		return err
	case sniCreatePolicy:
		var position int32
		if step.target.route.Deny {
			// The provider uses policy position to break ties for identical
			// selectors. Prefer the denial as soon as it is enabled.
			position = 1
		}
		_, err := p.octavia.CreatePolicy(ctx, l7policies.CreateOpts{
			Name: resourceName(step.target.route.Identity, roleSNIPolicy), ListenerID: actual.state.ListenerID, Action: l7policies.ActionRedirectToPool,
			RedirectPoolID: step.parentID, AdminStateUp: boolPointer(false), Tags: tagsFor(roleSNIPolicy), Position: position,
		}, nil)
		return err
	case sniCreateRule:
		_, err := p.octavia.CreateRule(ctx, step.parentID, l7policies.CreateRuleOpts{
			RuleType: ruleTLSSNI, CompareType: l7policies.CompareType(sniCompareType(step.target.route.Hostname)), Value: step.target.route.Hostname,
			AdminStateUp: boolPointer(true), Tags: tagsFor(roleSNIRule),
		})
		return err
	case sniClearMembers:
		return p.octavia.ClearMembers(ctx, step.parentID)
	case sniEnablePolicy:
		_, err := p.octavia.UpdatePolicy(ctx, step.parentID, l7policies.UpdateOpts{AdminStateUp: boolPointer(true)})
		return err
	default:
		return fmt.Errorf("unknown TLS SNI mutation")
	}
}

func (p *Provider) verifySNICapability(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) error {
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	required := []string{octavia.GatewayCapabilityTLSSNIPassthrough}
	if graphUsesSNISuffix(desired, actual) {
		required = append(required, octavia.GatewayCapabilityTLSSNISuffix)
	}
	if err := p.octavia.VerifyGatewayCapabilities(ctx, provider, required...); err != nil {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("verify Octavia TLS SNI capabilities in region %q: %w", p.region, err))
	}
	return nil
}

func graphUsesSNISuffix(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	if slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool {
		return sniCompareType(target.route.Hostname) == sniEndsWith
	}) {
		return true
	}
	for _, fragment := range actual.sni {
		if sniCompareType(fragment.forwardingHostname()) == sniEndsWith {
			return true
		}
	}
	return false
}

func (p *Provider) withdrawUndiscoverableSNI(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, cause error) (cloud.GatewayGraphResult, error) {
	outcome := p.progressingOutcome("Octavia TLS SNI passthrough capability is unavailable")
	result := cloud.GatewayGraphResult{
		Outcome: outcome, Gateway: cloud.GatewayResult{State: actual.state, Outcome: outcome},
		Routes: sniRouteResults(desired.sni, cloud.RouteResult{Outcome: outcome}),
	}
	if actual.listener == nil || !actual.listener.AdminStateUp {
		return result, cause
	}
	if fence := desired.spec.ValidateMutation; fence != nil {
		if err := fence(ctx); err != nil {
			return result, errors.Join(cause, err)
		}
	}
	step := graphMutation{kind: graphDisableListener, sniWithdrawal: true, message: "Closing TLS SNI listener until Octavia advertises its capability"}
	return result, errors.Join(cause, p.executeGraphMutation(ctx, desired, actual, step))
}

// Neutron failures must not hold up restrictive updates on a proven Octavia
// graph. Keep all address handling and traffic expansion behind full inventory.
func (p *Provider) reconcileSNIWithdrawalWithoutAddress(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) (cloud.GatewayGraphResult, error) {
	plan, result, err := p.buildSNIGraphPlan(desired, actual)
	if err != nil {
		return result, err
	}
	result.Gateway.Outcome = p.progressingOutcome("Waiting to observe the Gateway Floating IP inventory")
	// A previously proven sibling can stay Programmed during this read
	// failure. Do not create a new serving proof without address inventory.
	deferReadySNIReadback(&result, result.Gateway.Outcome)
	if len(plan) != 1 || !plan[0].isRestrictiveSNI() ||
		plan[0].kind == graphDeleteChild && plan[0].deletion.resource != gatewayDisableL7Policy {
		return result, actual.addressReadError
	}
	step, err := p.prepareGraphMutation(ctx, desired, actual, plan[0])
	if err != nil {
		return p.withdrawUndiscoverableSNI(ctx, desired, actual, err)
	}
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
	}
	if err := p.executeGraphMutation(ctx, desired, actual, step); err != nil {
		return result, err
	}
	p.recordGraphMutationOutcome(&result, step)
	return result, nil
}

func deferReadySNIReadback(result *cloud.GatewayGraphResult, outcome cloud.Outcome) {
	for i := range result.Routes {
		if result.Routes[i].Result.Outcome.State == cloud.OutcomeReady {
			result.Routes[i].Result = cloud.RouteResult{Outcome: outcome, ObservationDeferred: true}
		}
	}
}
