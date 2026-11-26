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

var sniWeightedPoolTag = openstackidentity.Tag("sni-weighted", "v1")

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
	route            cloud.TLSSNIRoute
	identity         Identity
	key              sniFragmentKey
	targetTag        string
	backendTLS       backendTLSPlan
	weightedBlocked  bool
	weightedGuard    bool
	backendPoolPlans []backendTLSPlan
}

type observedSNIFragment struct {
	pool                  *pools.Pool
	members               []pools.Member
	policy                *l7policies.L7Policy
	rule                  *l7policies.Rule
	backendTLS            octavia.PoolBackendTLS
	passthrough           *bool
	tcpRejection          octavia.PoolTCPConnectionRejection
	membersUnverified     bool
	backendPools          map[int]*observedSNIFragment
	backendTargets        []octavia.WeightedTCPPolicyTarget
	targetsUnverified     bool
	backendRuleUnverified bool
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
	type routeMode struct {
		uid         string
		passthrough bool
	}
	owners := make(map[routeMode]cloud.TLSSNIRoute)
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
			if route.ServiceUID != "" || route.ServicePort != 0 || len(route.Members) != 0 || route.BackendSubnetID != "" || len(route.BackendSecurityGroupIDs) != 0 || route.BackendTLS != nil || len(route.BackendServices) != 0 || route.TCPConnectionRejection != nil || len(route.BackendPools) != 0 {
				return nil, invalidSNI("TLS SNI denial requires an empty target without a backend identity")
			}
		} else if len(route.BackendPools) != 0 {
			if err := normalizeSNIBackendPools(&route); err != nil {
				return nil, err
			}
		} else if len(route.Members) == 0 || !validSNIDestinations(route) {
			return nil, invalidSNI("TLSRoute forwarding requires identified Services with ready members")
		}
		if tls := route.BackendTLS; tls != nil && (tls.Denied || route.ServicePort < 1 || route.ServicePort > 65535 || len(tls.SubjectAltNames) != 0 || !validBackendTLS(tls)) {
			return nil, invalidSNI("TLSRoute backend TLS requires an authorized CA and a DNS hostname")
		}
		names[route.Hostname] = true
		route.BackendServices = slices.Clone(route.BackendServices)
		if route.TCPConnectionRejection != nil {
			route.TCPConnectionRejection = new(*route.TCPConnectionRejection)
		}
		route.Members, err = normalizeSNIMembers(route, route.Members)
		if err != nil {
			return nil, err
		}
		route.BackendSecurityGroupIDs = slices.Clone(route.BackendSecurityGroupIDs)
		slices.Sort(route.BackendSecurityGroupIDs)
		mode := routeMode{uid: route.Identity.RouteUID, passthrough: route.Passthrough}
		otherMode := routeMode{uid: route.Identity.RouteUID, passthrough: !route.Passthrough}
		for _, candidateMode := range []routeMode{mode, otherMode} {
			owner, exists := owners[candidateMode]
			if !exists {
				continue
			}
			if owner.Identity != route.Identity {
				return nil, fmt.Errorf("%w: inconsistent identity for the same TLSRoute UID", cloud.ErrOwnershipConflict)
			}
			// A mixed listener can expose different hostnames and backend
			// transports from one TLSRoute in each mode.
			independentModes := owner.Passthrough != route.Passthrough && (len(owner.BackendPools) != 0 || len(route.BackendPools) != 0)
			if !owner.Deny && !route.Deny && !independentModes && (owner.ServiceUID != route.ServiceUID || owner.ServicePort != route.ServicePort || owner.BackendSubnetID != route.BackendSubnetID ||
				!slices.Equal(owner.BackendServices, route.BackendServices) || !sameSNIBackendPools(owner.BackendPools, route.BackendPools) ||
				!sameSNIRejection(owner.TCPConnectionRejection, route.TCPConnectionRejection) ||
				!slices.Equal(owner.BackendSecurityGroupIDs, route.BackendSecurityGroupIDs) || !slices.Equal(owner.Members, route.Members) ||
				owner.Passthrough == route.Passthrough && !sameSNIBackendTLS(owner.BackendTLS, route.BackendTLS)) {
				return nil, invalidSNI("hostnames on one TLSRoute require the same backend Services, topology and members")
			}
		}
		if owner, ok := owners[mode]; !ok || owner.Deny {
			owners[mode] = route
		}
		var identity *sniBackendTLSIdentity
		if tls := route.BackendTLS; tls != nil {
			identity = &sniBackendTLSIdentity{Hostname: tls.Hostname, CARevision: backendTLSRevisionTag(tls), ServicePort: route.ServicePort}
		}
		target, err := json.Marshal(struct {
			Hostname               string
			ServiceUID             string
			SubnetID               string
			Members                []cloud.Member
			BackendTLS             *sniBackendTLSIdentity        `json:",omitempty"`
			Passthrough            bool                          `json:",omitempty"`
			BackendServices        []cloud.TLSBackendService     `json:",omitempty"`
			TCPConnectionRejection *cloud.TCPConnectionRejection `json:",omitempty"`
			BackendPools           []sniBackendPoolIdentity      `json:",omitempty"`
		}{route.Hostname, route.ServiceUID, route.BackendSubnetID, route.Members, identity, route.Passthrough, route.BackendServices, route.TCPConnectionRejection, sniBackendPoolIdentities(route.BackendPools)})
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
	tags = append(tags, fragment.targetTag)
	if fragment.usesBackendPools() {
		tags = append(tags, openstackidentity.SNIWeightedPoolsTag)
	}
	return tags, nil
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
	if taggedSNIProfile(lb.Tags) == "" {
		return actual, fmt.Errorf("%w: load balancer lacks the TLS SNI profile identity", cloud.ErrOwnershipConflict)
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return actual, err
	}
	listener, err := p.managedGatewayListener(desired.identity, lb.ID, inventory.Items)
	if err != nil {
		return actual, err
	}
	actual.listener = nil
	pendingUpdate := lb.ProvisioningStatus == "PENDING_UPDATE"
	if listener != nil {
		if listener.ID == "" || listener.Protocol != cloud.ListenerProtocolTCP || listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort ||
			!matchesSNIProfile(listener.Tags, desired.protocol) ||
			len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID || !actual.adminObserved[listener.ID] ||
			desired.protocol == cloud.ListenerProtocolTLSSNI && !isPassthroughSNIListener(*listener, inventory) {
			return actual, fmt.Errorf("%w: listener differs from the TLS SNI profile binding", cloud.ErrOwnershipConflict)
		}
		if err := requireListenerDefaultPoolInventory(lb.ID, inventory); err != nil {
			return actual, err
		}
		actual.listener, actual.state.ListenerID = listener, listener.ID
		actual.sniListenerAttached = true
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
	// disjoint SNI routes retain complete ACTIVE forwarding fragments. A
	// detached member batch can also be observed on a still-closed listener.
	if actual.outcome.State != cloud.OutcomeReady && !pendingUpdate {
		return actual, nil
	}
	owners := make(map[string]sniRouteKey)
	wantedModes := make(map[sniFragmentKey]bool)
	weighted := make(map[sniFragmentKey]bool)
	for _, target := range desired.sni {
		owners[target.key.uid] = target.key.sniRouteKey
		wantedModes[target.key] = target.route.Passthrough
		weighted[target.key] = target.weighted() || target.usesBackendPools()
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
	actual.sniBackendPoolsObserved = slices.ContainsFunc(poolInventory.Items, func(pool pools.Pool) bool {
		return slices.Contains(pool.Tags, openstackidentity.SNIWeightedPoolsTag)
	})
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
		fragment := fragmentFor(key)
		slot, err := sniPoolSlot(pool.Tags)
		if err != nil {
			return actual, err
		}
		if slot >= 0 {
			if fragment.backendPools == nil {
				fragment.backendPools = make(map[int]*observedSNIFragment)
			}
			if fragment.pool != nil || fragment.backendPools[slot] != nil {
				return actual, fmt.Errorf("%w: duplicate TLS SNI backend pool", cloud.ErrOwnershipConflict)
			}
			fragment.backendPools[slot] = &observedSNIFragment{}
			fragment = fragment.backendPools[slot]
		} else if fragment.backendPools != nil {
			return actual, fmt.Errorf("%w: mixed TLS SNI pool representations", cloud.ErrOwnershipConflict)
		}
		if fragment.pool != nil || pool.Protocol != cloud.ListenerProtocolTCP || len(pool.Loadbalancers) != 1 ||
			pool.Loadbalancers[0].ID != lb.ID || pool.MonitorID != "" || poolInventory.BackendH2C[pool.ID] ||
			len(pool.Listeners) > 1 || (len(pool.Listeners) == 1 && (listener == nil || pool.Listeners[0].ID != listener.ID)) {
			return actual, fmt.Errorf("%w: invalid TLS SNI pool relationship", cloud.ErrOwnershipConflict)
		}
		if slot < 0 && cloud.IsTCPTLSListenerProtocol(desired.protocol) && desired.spec.Gateway.Disposition == cloud.GraphEnsure &&
			(!pendingUpdate || actual.rawTLSVerified) &&
			!poolInventory.BackendTLS[pool.ID].EnabledObserved &&
			!hasObservedBackendTLS(*pool, poolInventory.BackendTLS[pool.ID]) {
			message := "Waiting to verify that TCP TLS pool transport is plaintext"
			actual.outcome = p.progressingOutcome(message)
			actual.rawTLSVerified = false
			if listener != nil && listener.AdminStateUp && lb.ProvisioningStatus == "ACTIVE" {
				actual.withdrawTLS = &graphMutation{kind: graphDisableListener, redactTLSError: true, sniWithdrawal: true, message: message}
			}
			return actual, nil
		}
		fragment.pool, fragment.backendTLS, fragment.tcpRejection = pool, poolInventory.BackendTLS[pool.ID], poolInventory.TCPConnectionRejection[pool.ID]
		var members []pools.Member
		var unreported, unverified map[string]struct{}
		strict := slot >= 0 || fragment.weighted() || weighted[key]
		opts := pools.ListMembersOpts{ProjectID: p.projectID}
		if strict {
			members, unreported, unverified, err = p.octavia.ListMembersForPolicyActivation(ctx, pool.ID, opts, true)
		} else {
			members, unreported, err = p.octavia.ListMembersWithAdminState(ctx, pool.ID, opts)
		}
		if err == nil {
			err = crossCheckCompleteChildInventory("pool", pool.ID, "member", pool.Members, members, func(m pools.Member) string { return m.ID })
		}
		if err != nil && (!strict || ctx.Err() != nil) {
			return actual, err
		}
		// A known policy can be disabled even when its member readback is
		// incomplete. Member writes still require complete inventory.
		fragment.membersUnverified = err != nil || len(unverified) != 0
		for _, member := range members {
			owner, err := p.sniOwnership(desired.identity, member.Tags, member.ProjectID, member.ID, roleSNIMember)
			memberSlot, slotErr := sniPoolSlot(member.Tags)
			if _, omitted := unreported[member.ID]; err != nil || omitted || owner != key || slotErr != nil || memberSlot != slot || member.PoolID != "" && member.PoolID != pool.ID {
				fragment.membersUnverified = true
			}
		}
		fragment.members = members
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
			weightedPolicy := slices.Contains(policy.Tags, openstackidentity.SNIWeightedPoolsTag)
			actual.sniBackendPoolsObserved = actual.sniBackendPoolsObserved || weightedPolicy
			if openstackidentity.HasSNIPoolSlot(policy.Tags) {
				return actual, fmt.Errorf("%w: TLS SNI policy has a pool slot identity", cloud.ErrOwnershipConflict)
			}
			if fragment.policy != nil || policy.ListenerID != listener.ID || row.AdminState == nil ||
				!weightedPolicy && (fragment.pool == nil || policy.RedirectPoolID != fragment.pool.ID ||
					policy.Action != string(l7policies.ActionRedirectToPool) || row.ExtendedTargetFields ||
					hasHTTPOnlySNIOptions(row) || policy.RedirectURL != "" || policy.RedirectPrefix != "") ||
				weightedPolicy && fragment.pool != nil {
				return actual, fmt.Errorf("%w: invalid TLS SNI policy relationship", cloud.ErrOwnershipConflict)
			}
			fragment.passthrough = row.TCPTLSPassthrough
			if weightedPolicy {
				if fragment.backendPools == nil {
					fragment.backendPools = make(map[int]*observedSNIFragment)
				}
				fragment.observeBackendTargets(row)
				wanted, known := wantedModes[key]
				if (desired.protocol == cloud.ListenerProtocolTLSMixed && (fragment.passthrough == nil || known && wanted != fragment.isPassthrough())) ||
					(desired.protocol != cloud.ListenerProtocolTLSMixed && fragment.isPassthrough()) {
					fragment.targetsUnverified = true
				}
			}
			if !weightedPolicy && desired.protocol == cloud.ListenerProtocolTLSMixed && desired.spec.Gateway.Disposition == cloud.GraphEnsure {
				wanted, known := wantedModes[key]
				if (!pendingUpdate || actual.rawTLSVerified) && (fragment.passthrough == nil || known && wanted != *fragment.passthrough && listener.AdminStateUp) {
					p.closeUnverifiedMixedSNI(&actual)
					return actual, nil
				}
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
				hasSlot := openstackidentity.HasSNIPoolSlot(rule.Tags)
				if owner != key || rule.AdminState == nil || rule.Inverted == nil || hasSlot ||
					weightedPolicy != slices.Contains(rule.Tags, openstackidentity.SNIWeightedPoolsTag) {
					return actual, fmt.Errorf("%w: invalid TLS SNI rule relationship", cloud.ErrOwnershipConflict)
				}
				fragment.rule = &rule.Rule
				if weightedPolicy {
					fragment.observeBackendRule(rule)
				}
			}
			fragment.policy = &policy
		}
		if err := crossCheckCompleteChildInventory("listener", listener.ID, "policy", listener.L7Policies, policies, func(policy l7policies.L7Policy) string { return policy.ID }); err != nil {
			return actual, err
		}
	}
	for _, fragment := range byKey {
		if fragment.backendPools != nil {
			fragment.verifyBackendPoolAttachments()
			continue
		}
		attached := fragment.policy != nil && fragment.policy.AdminStateUp && fragment.rule != nil
		if fragment.pool.Listeners == nil || (len(fragment.pool.Listeners) == 1) != attached {
			return actual, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("incomplete TLS SNI pool attachment readback"))
		}
	}
	actual.sni = byKey
	return actual, nil
}

func (p *Provider) closeUnverifiedMixedSNI(actual *observedGatewayGraph) {
	p.closeUnverifiedSNIFrontend(actual, "Waiting for verified TCP TLS policy modes")
}

func (p *Provider) closeUnverifiedSNIFrontend(actual *observedGatewayGraph, message string) {
	actual.rawTLSVerified = false
	actual.outcome = p.progressingOutcome(message)
	if actual.listener.AdminStateUp && actual.loadBalancer.ProvisioningStatus == "ACTIVE" {
		actual.withdrawTLS = &graphMutation{kind: graphDisableListener, redactTLSError: true, sniWithdrawal: true, message: message}
	}
}

func (fragment *observedSNIFragment) matches(wanted desiredSNIFragment) bool {
	if wanted.usesBackendPools() || fragment.backendPools != nil {
		return fragment.matchesBackendPools(wanted)
	}
	if fragment.pool == nil || fragment.pool.ProvisioningStatus != "ACTIVE" || !fragment.matchesBackendTransport(wanted) ||
		fragment.pool.LBMethod != string(pools.LBMethodRoundRobin) || fragment.pool.Persistence.Type != "" ||
		wanted.weightedBlocked || fragment.hasUnverifiedSNIWeights(wanted) || !fragment.matchesSNIRejection(wanted) ||
		len(fragment.members) > len(wanted.route.Members) ||
		wanted.weighted() && fragment.policy != nil && len(fragment.members) != len(wanted.route.Members) {
		return false
	}
	if !fragment.matchesSNIMembers(wanted.route) {
		return false
	}
	return fragment.matchesSNISelector(wanted)
}

func (fragment *observedSNIFragment) matchesSNISelector(wanted desiredSNIFragment) bool {
	if fragment.policy != nil {
		if fragment.policy.ProvisioningStatus != "ACTIVE" || fragment.policy.AdminStateUp && fragment.rule == nil ||
			fragment.passthrough != nil && *fragment.passthrough != wanted.route.Passthrough {
			return false
		}
	}
	return fragment.rule == nil || fragment.rule.ProvisioningStatus == "ACTIVE" && fragment.rule.AdminStateUp && !fragment.rule.Invert &&
		fragment.rule.RuleType == string(ruleTLSSNI) && fragment.rule.CompareType == sniCompareType(wanted.route.Hostname) &&
		fragment.rule.Key == "" && fragment.rule.Value == wanted.route.Hostname
}

func sniMemberSubnet(route cloud.TLSSNIRoute, member cloud.Member) string {
	if member.RoutedExternal {
		return ""
	}
	return route.BackendSubnetID
}

func (fragment *observedSNIFragment) matchesSNIMembers(route cloud.TLSSNIRoute) bool {
	type memberAddress struct {
		address string
		port    int
	}
	wanted := make(map[memberAddress]cloud.Member, len(route.Members))
	for _, member := range route.Members {
		wanted[memberAddress{member.Address, member.Port}] = member
	}
	seen := make(map[netip.AddrPort]bool)
	for _, member := range fragment.members {
		address, err := netip.ParseAddr(member.Address)
		if err != nil || member.ProtocolPort < 1 || member.ProtocolPort > 65535 {
			return false
		}
		key := netip.AddrPortFrom(address, uint16(member.ProtocolPort))
		destination, found := wanted[memberAddress{member.Address, member.ProtocolPort}]
		subnet := sniMemberSubnet(route, destination)
		if !found || seen[key] || member.ProvisioningStatus != "ACTIVE" || !member.AdminStateUp || member.Backup ||
			(destination.RoutedExternal || subnet != "") && member.SubnetID != subnet ||
			member.Weight != max(1, destination.Weight) {
			return false
		}
		seen[key] = true
	}
	return true
}

func sniFrontendObserved(desired desiredGatewayGraph, actual observedGatewayGraph) bool {
	lb, listener := actual.loadBalancer, actual.listener
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure ||
		cloud.IsTCPTLSListenerProtocol(desired.protocol) && (!actual.rawTLSCapabilityVerified || !actual.rawTLSVerified) || lb == nil || !lb.AdminStateUp ||
		listener == nil || !listener.AdminStateUp || listener.DefaultPoolID != "" ||
		desired.spec.Gateway.Spec.ExternalNetworkID != "" && actual.state.FloatingIPAddress == "" {
		return false
	}
	for _, fragment := range actual.sni {
		if fragment.backendRuleUnverified && fragment.policy != nil && fragment.policy.AdminStateUp {
			// Amphora closes the TCP frontend if it cannot scope an
			// active weighted selector to a valid hostname.
			return false
		}
	}
	return lb.ProvisioningStatus == "ACTIVE" && listener.ProvisioningStatus == "ACTIVE" ||
		lb.ProvisioningStatus == "PENDING_UPDATE" && (listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "PENDING_UPDATE")
}

func (fragment *observedSNIFragment) forwardingHostname() string {
	policy, rule := fragment.policy, fragment.rule
	// Pending changes may have already updated admin_state_up in the API
	// while the previous policy is still forwarding in Amphora.
	if policy == nil || rule == nil || !policy.AdminStateUp && policy.ProvisioningStatus == "ACTIVE" ||
		!rule.AdminStateUp && rule.ProvisioningStatus == "ACTIVE" {
		return ""
	}
	return fragment.selectorHostname()
}

func (fragment *observedSNIFragment) hasActiveSelector(hostname string) bool {
	policy, rule := fragment.policy, fragment.rule
	return policy != nil && policy.ProvisioningStatus == "ACTIVE" && policy.AdminStateUp &&
		rule != nil && rule.ProvisioningStatus == "ACTIVE" && rule.AdminStateUp &&
		fragment.selectorHostname() == hostname
}

func (fragment *observedSNIFragment) selectorHostname() string {
	rule := fragment.rule
	if rule == nil || rule.RuleType != string(ruleTLSSNI) || rule.CompareType != sniCompareType(rule.Value) || rule.Invert || rule.Key != "" {
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
		if !target.route.Deny && !desired.sniPaused[target.key] && !desired.sniBlockedRoutes[target.key.uid] {
			ready[target.key.uid] = true
		}
	}
	selectors := make(map[string]int)
	for key, fragment := range actual.sni {
		if desired.sniHeldClosed[key] {
			wantedHostnames[fragment.forwardingHostname()] = true
		}
	}
	for key, fragment := range actual.sni {
		if !wanted[key] && !desired.sniHeldClosed[key] && !desired.sniPaused[key] {
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
		if ready[target.key.uid] && (desired.sniPaused[target.key] || selectors[target.route.Hostname] != 1 || fragment == nil || !fragment.ready(target)) {
			delete(ready, target.key.uid)
		}
	}
	return ready
}

func (fragment *observedSNIFragment) ready(target desiredSNIFragment) bool {
	return fragment.policy != nil && fragment.policy.AdminStateUp && fragment.rule != nil && fragment.matches(target) &&
		(target.usesBackendPools() && fragment.backendPoolsReady(target) ||
			!target.usesBackendPools() && !target.backendTLS.stagesSystemCA() && fragment.pool != nil && fragment.pool.AdminStateUp && len(fragment.members) == len(target.route.Members))
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
	case fragment.backendPools != nil:
		for _, slot := range slices.Sorted(maps.Keys(fragment.backendPools)) {
			step = gatewayDeletionStep{resource: gatewayDeletionPool, resourceID: fragment.backendPools[slot].pool.ID}
			break
		}
	case len(fragment.members) != 0 && !fragment.weighted():
		step = gatewayDeletionStep{resource: gatewayDeletionMember, resourceID: fragment.members[0].ID, parentID: fragment.pool.ID}
	default:
		// After its policies are gone, Octavia can delete the weighted pool
		// and its owned members, including rows locked by a failed batch.
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
	for _, target := range desired.sni {
		if target.usesBackendPools() && target.weightedBlocked {
			for i := range result.Routes {
				if result.Routes[i].Identity.RouteUID == target.route.Identity.RouteUID {
					result.Routes[i].Result.Outcome = p.progressingOutcome("Exact TLSRoute backend shares require verified Octavia weighted TLS SNI pools in this region")
				}
			}
		}
	}
	plan := func(step graphMutation) ([]graphMutation, cloud.GatewayGraphResult, error) {
		result.Outcome = p.progressingOutcome(step.message)
		result.Gateway.Outcome = result.Outcome
		return []graphMutation{step}, result, nil
	}
	if desired.spec.Gateway.RetainSNIAddress && actual.listener != nil && actual.listener.AdminStateUp {
		return plan(graphMutation{kind: graphDisableListener, sniWithdrawal: true, message: "Closing TLS SNI listener before replacement"})
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
		wanted[frag.key] = frag
		if _, exists := byHostname[frag.route.Hostname]; !exists {
			byHostname[frag.route.Hostname] = frag
		}
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
	// A retired selector can otherwise send its former clients to the
	// surviving, broader listener, potentially changing TLS modes.
	retiredGuards := make(map[sniFragmentKey]bool)
	if desired.protocol == cloud.ListenerProtocolTLSMixed && desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		if desired.sniHeldClosed == nil {
			desired.sniHeldClosed = make(map[sniFragmentKey]bool)
		}
		for _, key := range keys {
			fragment := actual.sni[key]
			hostname := fragment.selectorHostname()
			pausedFallback := slices.ContainsFunc(keys, func(other sniFragmentKey) bool {
				return desired.sniPaused[other] && amphora.SNISuffixCovers(actual.sni[other].selectorHostname(), hostname)
			})
			unavailableWeightedGuard := fragment.backendPools != nil &&
				(desired.sniBackendPoolCapabilityLost(fragment.isPassthrough()) || fragment.targetsUnverified)
			if _, stillClaimed := byHostname[hostname]; !stillClaimed && hostname != "" &&
				(fragment.hasActiveSelector(hostname) || fragment.closedBackendPool() || unavailableWeightedGuard) &&
				(pausedFallback || hasSNIFallback(hostname, desired.sni, actual.sni)) {
				desired.sniHeldClosed[key] = true
				retiredGuards[key] = true
				if unavailableWeightedGuard {
					// The retired selector cannot remain as a rejecting
					// guard. Keep its broader neighbors closed until it
					// can reserve the name again.
					if desired.sniPaused == nil {
						desired.sniPaused = make(map[sniFragmentKey]bool)
					}
					for _, broader := range desired.sni {
						if amphora.SNISuffixCovers(broader.route.Hostname, hostname) {
							desired.sniPaused[broader.key] = true
						}
					}
				}
			}
		}
	}
	var retire []sniFragmentKey
	waitForOctavia := false
	for _, key := range keys {
		fragment := actual.sni[key]
		target, keep := wanted[key]
		retainedDuringDiscovery := fragment.keepsSNISelectorDuringDiscovery(key, byHostname[fragment.forwardingHostname()], desired)
		if fragment.weighted() && fragment.detached() && fragment.pool.ProvisioningStatus == "ERROR" {
			// A failed batch can leave child rows pending even though its
			// parent load balancer is mutable. Only the pool can remove them.
			if !fragment.membersUnverified {
				retire = append(retire, key)
			}
			continue
		}
		if fragment.pending() {
			waitForOctavia = true
			continue
		}
		if !desired.sniPaused[key] && !desired.sniHeldClosed[key] && !retainedDuringDiscovery && !fragment.hasUnverifiedMembers() && !fragment.unprovenDetachedBackendPools() &&
			(!keep || !target.weightedBlocked && !fragment.matches(target)) {
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
	if step, needed := withdrawSNIBackendPools(desired, actual, keys, wanted); needed {
		return plan(step)
	}
	if step, affected, needed := withdrawWeightedSNI(desired, actual, keys, wanted, byHostname); needed {
		for i, route := range result.Routes {
			if encodedSNITag("route-uid", route.Identity.RouteUID) == affected {
				result.Routes[i].Result.Outcome = pending
			}
		}
		return plan(step)
	}
	for _, key := range keys {
		fragment := actual.sni[key]
		if desired.sniPaused[key] && fragment.policy != nil && fragment.policy.AdminStateUp {
			return plan(graphMutation{kind: graphDeleteChild, sniWithdrawal: true,
				deletion: gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: fragment.policy.ID},
				message:  "Suspending TLS SNI suffix policy without its regional capability"})
		}
	}
	if actual.listener != nil && actual.listener.AdminStateUp {
		for _, key := range retire {
			fragment := actual.sni[key]
			hostname := fragment.forwardingHostname()
			target, claimed := byHostname[hostname]
			if !claimed || target.route.Deny || !fragment.hasForwardingMembers() || !hasSNIFallback(hostname, desired.sni, actual.sni) {
				continue
			}
			changedOwner := key.sniRouteKey != target.key.sniRouteKey
			changedImport := key != target.key && fragment.backendPools == nil && !fragment.weighted() &&
				!target.weighted() && onlyRoutedSNIMembers(target.route)
			if !changedOwner && !changedImport {
				continue
			}
			// Keep the selector ahead of the fallback while the successor
			// is staged, even if allocation of its new pool cannot proceed.
			if leaf := fragment.firstOpenPool(); leaf != nil {
				return plan(sniRestrictMembers(leaf))
			}
		}
	}
	if step, needed, waiting := stageSNIBackendPoolSuccessor(desired, actual, keys); needed {
		return plan(step)
	} else if waiting {
		result.Outcome = pending
		return nil, result, nil
	}

	for _, key := range keys {
		fragment := actual.sni[key]
		target := byHostname[fragment.forwardingHostname()]
		retiring := slices.Contains(retire, key)
		current := wanted[key]
		closing := cloud.IsTCPTLSListenerProtocol(desired.protocol) &&
			(current.backendTLS.stagesSystemCA() && (len(fragment.members) != 0 || fragment.membersUnverified) || desired.sniHeldClosed[key] || retiring && (fragment.protectedBackendTLS() || target.route.BackendTLS != nil))
		if closing && !fragment.closedBackendPool() {
			if leaf := fragment.firstOpenPool(); leaf != nil {
				return plan(sniDisablePoolStep(leaf))
			}
		}
		if retiredGuards[key] && !desired.sniPaused[key] &&
			(fragment.backendPools == nil || desired.sniBackendPoolCapability(fragment.isPassthrough()) && !fragment.targetsUnverified) && fragment.policy != nil &&
			fragment.rule != nil && !fragment.policy.AdminStateUp {
			// Restore a closed selector before its broader neighbor can
			// resume after the regional suffix capability returns.
			return plan(graphMutation{kind: graphSNIMutation, sniWithdrawal: true,
				sni:     sniMutation{kind: sniEnablePolicy, parentID: fragment.policy.ID},
				message: "Restoring the closed TLS SNI selector before its broader neighbor"})
		}
	}
	if actual.listener != nil && actual.listener.AdminStateUp {
		step, coveredUID, needed := protectSNIClaims(desired, actual.sni, keys)
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
				if target.weightedBlocked {
					continue
				}
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
		witnesses := sniBackendTLSWitnesses(wanted, actual.sni, retire)
		for _, key := range retire {
			if step, ok := sniDeleteObsoleteResource(actual.sni[key], witnesses); ok {
				return plan(step)
			}
		}
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		if slices.ContainsFunc(keys, func(key sniFragmentKey) bool {
			return actual.sni[key].hasUnverifiedMembers() || actual.sni[key].unprovenDetachedBackendPools()
		}) {
			result.Outcome, result.Gateway.Outcome = pending, pending
			return nil, result, fmt.Errorf("%w: cannot delete a TLS SNI graph with unverified pool relationships or members", cloud.ErrOwnershipConflict)
		}
		if actual.listener != nil {
			return plan(graphMutation{kind: graphDeleteChild, deletion: listenerDeletionStep(actual.listener.ID), sniWithdrawal: true, message: "Deleting TLS SNI listener"})
		}
		if !desired.spec.Gateway.RetainSNIAddress {
			if steps := gatewayGraphDeletionPlan(actual); len(steps) != 0 {
				return plan(steps[0])
			}
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
	if step, needed := restoreGuardedSNIFallback(desired, actual); needed {
		return plan(step)
	}
	for _, target := range desired.sni {
		if desired.sniPaused[target.key] {
			continue
		}
		if step, ok := nextSNIFragmentMutation(target, actual.sni[target.key]); ok {
			return plan(step)
		}
	}
	if steps, err := buildGatewayTrafficRestorationPlan(desired, actual); err != nil {
		return nil, result, err
	} else if len(steps) != 0 {
		return plan(steps[0])
	}
	if slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool { return target.weightedBlocked }) {
		result.Outcome = p.progressingOutcome("Waiting for verified Octavia weighted TLS SNI backends")
	} else if slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool {
		return target.backendTLS.stagesSystemCA() || slices.ContainsFunc(target.backendPoolPlans, backendTLSPlan.stagesSystemCA)
	}) {
		result.Outcome = p.progressingOutcome("Waiting for Octavia to verify the applied Amphora system CA bundle")
	}
	return nil, result, nil
}

// protectSNIClaims prevents fallback while a denied or protected selector is
// being staged. It needs no allocation or CA publication to withdraw traffic.
func protectSNIClaims(desired desiredGatewayGraph, actual map[sniFragmentKey]*observedSNIFragment, keys []sniFragmentKey) (graphMutation, string, bool) {
	rawTLS := cloud.IsTCPTLSListenerProtocol(desired.protocol)
	for _, target := range desired.sni {
		if !target.route.Deny && !target.weighted() && !target.weightedBlocked && target.route.BackendTLS == nil && !target.usesBackendPools() &&
			(desired.protocol != cloud.ListenerProtocolTLSMixed || !hasCrossModeSNIFallback(target, actual)) {
			continue
		}
		hostname := target.route.Hostname
		var guard *observedSNIFragment
		for _, key := range keys {
			fragment := actual[key]
			rejects := (rawTLS || fragment.backendPools != nil) && fragment.closedBackendPool() || fragment.emptyActivePool()
			if fragment.hasActiveSelector(hostname) && (fragment.matches(target) || rejects || fragment.keepsSNISelectorDuringDiscovery(key, target, desired)) &&
				(guard == nil || fragment.policy.Position < guard.policy.Position) {
				guard = fragment
			}
		}
		for _, key := range keys {
			fragment := actual[key]
			if fragment == guard || fragment.forwardingHostname() != hostname || !fragment.hasForwardingMembers() {
				continue
			}
			if guard != nil && guard.policy.Position > 0 && guard.policy.Position < fragment.policy.Position {
				continue
			}
			// Preserve the rule and the pool, removing all destinations in
			// one write. Its name cannot fall through while we stage a guard.
			if leaf := fragment.firstOpenPool(); leaf != nil {
				return sniRestrictMembers(leaf), "", true
			}
		}
		if guard == nil {
			// The keys are ordered broadest first, so disabling one covering
			// suffix cannot expose a wider candidate for the denied names.
			for _, key := range keys {
				fragment := actual[key]
				if (fragment.hasForwardingMembers() || fragment.hasUnverifiedMembers()) && amphora.SNISuffixCovers(fragment.forwardingHostname(), hostname) {
					return graphMutation{kind: graphDeleteChild, sniWithdrawal: true,
						deletion: gatewayDeletionStep{resource: gatewayDisableL7Policy, resourceID: fragment.policy.ID},
						message:  "Suspending a broader TLS SNI policy until the specific selector is verified"}, key.uid, true
				}
			}
		}
	}
	return graphMutation{}, "", false
}

func hasCrossModeSNIFallback(target desiredSNIFragment, actual map[sniFragmentKey]*observedSNIFragment) bool {
	for _, fragment := range actual {
		if fragment.passthrough == nil || *fragment.passthrough == target.route.Passthrough || !fragment.hasForwardingMembers() {
			continue
		}
		host := fragment.forwardingHostname()
		if host == target.route.Hostname || amphora.SNISuffixCovers(host, target.route.Hostname) {
			return true
		}
	}
	return false
}

func sniClearMembersStep(fragment *observedSNIFragment) graphMutation {
	return graphMutation{kind: graphSNIMutation, sniWithdrawal: true,
		sni:     sniMutation{kind: sniClearMembers, parentID: fragment.pool.ID},
		message: "Removing unauthorized TLS SNI pool members"}
}

func hasSNIFallback(hostname string, desired []desiredSNIFragment, actual map[sniFragmentKey]*observedSNIFragment) bool {
	for _, target := range desired {
		if !target.route.Deny && amphora.SNISuffixCovers(target.route.Hostname, hostname) {
			return true
		}
	}
	for _, fragment := range actual {
		if fragment.hasForwardingMembers() && amphora.SNISuffixCovers(fragment.forwardingHostname(), hostname) {
			return true
		}
	}
	return false
}

func nextSNIFragmentMutation(target desiredSNIFragment, fragment *observedSNIFragment) (graphMutation, bool) {
	if target.usesBackendPools() {
		return nextSNIBackendPoolsMutation(target, fragment)
	}
	mutation := sniMutation{target: target}
	switch {
	case target.weightedBlocked || target.weighted() && fragment != nil && !fragment.matches(target):
		return graphMutation{}, false
	case target.backendTLS.phase == backendTLSClaim:
		mutation.kind = sniClaimBackendCA
	case target.backendTLS.phase == backendTLSPublish:
		mutation.kind = sniPublishBackendCA
	case fragment == nil:
		mutation.kind = sniCreatePool
	case !target.backendTLS.stagesSystemCA() && target.route.TCPConnectionRejection != nil && !sameSNIRejection(fragment.tcpRejection.Value, target.route.TCPConnectionRejection):
		mutation.kind, mutation.parentID = sniReplaceTCPMembers, fragment.pool.ID
	case target.backendTLS.stagesSystemCA() && fragment.membersUnverified:
		return graphMutation{}, false
	case target.backendTLS.stagesSystemCA() && len(fragment.members) != 0:
		mutation.kind, mutation.parentID = sniClearMembers, fragment.pool.ID
	case !target.backendTLS.stagesSystemCA() && len(fragment.members) != len(target.route.Members):
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
	case target.route.BackendTLS != nil && !fragment.pool.AdminStateUp:
		mutation.kind, mutation.parentID = sniEnableBackendTLSPool, fragment.pool.ID
	case !fragment.policy.AdminStateUp:
		mutation.kind, mutation.parentID = sniEnablePolicy, fragment.policy.ID
	default:
		return graphMutation{}, false
	}
	return graphMutation{kind: graphSNIMutation, sni: mutation, sniWithdrawal: mutation.kind == sniClearMembers,
		redactTLSError: target.route.BackendTLS != nil, message: "Converging the TLS SNI route"}, true
}

type sniMutationKind uint8

const (
	sniCreatePool sniMutationKind = iota
	sniCreateMember
	sniCreatePolicy
	sniCreateRule
	sniEnablePolicy
	sniClearMembers
	sniDisableBackendTLSPool
	sniEnableBackendTLSPool
	sniClaimBackendCA
	sniPublishBackendCA
	sniReplaceTCPMembers
	sniConfigureBackendTLSPool
)

type sniMutation struct {
	kind         sniMutationKind
	target       desiredSNIFragment
	parentID     string
	member       cloud.Member
	poolSlot     *int
	closedPool   bool
	preferPolicy bool
	poolTargets  []octavia.WeightedTCPPolicyTarget
}

func (p *Provider) executeSNIMutation(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step sniMutation) error {
	tagsFor := func(role string) []string {
		tags, _ := step.target.tags(role) // Validated before the graph was observed.
		if step.poolSlot != nil {
			tags = append(tags, openstackidentity.SNIPoolSlotTag(*step.poolSlot))
		}
		return tags
	}
	switch step.kind {
	case sniClaimBackendCA:
		if desired.spec.Gateway.ClaimBackendCA == nil {
			return errors.New("backend CA ownership checkpoint is unavailable")
		}
		return desired.spec.Gateway.ClaimBackendCA(ctx)
	case sniPublishBackendCA:
		return p.publishBackendCA(ctx, cloud.RouteSpec{Identity: step.target.route.Identity, BackendTLS: step.target.route.BackendTLS})
	case sniDisableBackendTLSPool, sniEnableBackendTLSPool:
		_, err := p.octavia.UpdatePool(ctx, step.parentID, pools.UpdateOpts{AdminStateUp: boolPointer(step.kind == sniEnableBackendTLSPool)})
		return err
	case sniConfigureBackendTLSPool:
		_, err := p.octavia.UpdatePool(ctx, step.parentID, octavia.UpdatePoolOpts{BackendTLS: &step.target.backendTLS.settings})
		return err
	case sniCreatePool:
		opts := pools.CreateOpts{
			Protocol: pools.ProtocolTCP, LBMethod: pools.LBMethodRoundRobin, LoadbalancerID: actual.state.LoadBalancerID,
			Name: resourceName(step.target.route.Identity, roleSNIPool), AdminStateUp: boolPointer(true), Tags: tagsFor(roleSNIPool),
		}
		if step.poolSlot != nil {
			opts.LBMethod = sniBackendPoolAlgorithm(len(step.target.route.Members))
		}
		if step.target.weighted() {
			opts.Tags = append(opts.Tags, sniWeightedPoolTag)
		}
		if tls := step.target.route.BackendTLS; tls != nil {
			opts.Tags = backendTLSTags(opts.Tags, true, backendTLSRevisionTag(tls), false, false)
			if tls.Trust == cloud.BackendTLSTrustSystem {
				opts.Tags = append(opts.Tags, backendTLSSystemCATag)
			}
		}
		if step.target.route.BackendTLS != nil || step.poolSlot != nil && step.target.route.ServiceUID != "" {
			opts.Tags = append(opts.Tags, sniBackendServiceTag(step.target.route))
		}
		var builder pools.CreateOptsBuilder = opts
		if step.closedPool {
			opts.AdminStateUp = boolPointer(false)
			builder = octavia.PlaintextTCPPoolOpts{CreateOpts: opts}
		} else if tls := step.target.route.BackendTLS; tls != nil {
			opts.AdminStateUp = boolPointer(false)
			opts.TLSEnabled = true
			opts.CATLSContainerRef = step.target.backendTLS.settings.CAReference
			builder = octavia.CreatePoolOpts{CreateOpts: opts, BackendTLSHostname: tls.Hostname, BackendTLSSystemCA: tls.Trust == cloud.BackendTLSTrustSystem}
		} else if cloud.IsTCPTLSListenerProtocol(actual.protocol) {
			builder = octavia.PlaintextTCPPoolOpts{CreateOpts: opts}
		}
		_, err := p.octavia.CreatePool(ctx, builder)
		return err
	case sniCreateMember:
		_, err := p.octavia.CreateMember(ctx, step.parentID, pools.CreateMemberOpts{
			Address: step.member.Address, ProtocolPort: step.member.Port, SubnetID: sniMemberSubnet(step.target.route, step.member),
			AdminStateUp: boolPointer(true), Weight: new(max(1, step.member.Weight)), Tags: tagsFor(roleSNIMember),
		})
		return err
	case sniReplaceTCPMembers:
		return p.replaceSNITCPMembers(ctx, step, tagsFor(roleSNIMember))
	case sniCreatePolicy:
		var position int32
		if step.target.route.Deny || step.preferPolicy {
			// The provider uses policy position to break ties for identical
			// selectors. Prefer this replacement when it is enabled.
			position = 1
		}
		var passthrough *bool
		if actual.protocol == cloud.ListenerProtocolTLSMixed {
			passthrough = &step.target.route.Passthrough
		}
		_, err := p.octavia.CreatePolicy(ctx, octavia.PolicyCreateOpts{TCPTLSPassthrough: passthrough, WeightedTCPPolicyTargets: step.poolTargets, CreateOpts: l7policies.CreateOpts{
			Name: resourceName(step.target.route.Identity, roleSNIPolicy), ListenerID: actual.state.ListenerID, Action: l7policies.ActionRedirectToPool,
			RedirectPoolID: step.parentID, AdminStateUp: boolPointer(false), Tags: tagsFor(roleSNIPolicy), Position: position,
		}})
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

// Pause unsupported passthrough suffixes and any broader suffix which
// could inherit their traffic. Exact policies remain eligible for readback.
func (p *Provider) isolateUnavailableSNISuffix(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure ||
		(desired.protocol != cloud.ListenerProtocolTLSMixed && desired.protocol != cloud.ListenerProtocolTLSSNI) {
		return
	}
	pure := desired.protocol == cloud.ListenerProtocolTLSSNI
	var affected []string
	for _, target := range desired.sni {
		if (pure || target.route.Passthrough) && sniCompareType(target.route.Hostname) == sniEndsWith {
			affected = append(affected, target.route.Hostname)
		}
	}
	for _, fragment := range actual.sni {
		if hostname := fragment.selectorHostname(); (pure || fragment.passthrough != nil && *fragment.passthrough) && sniCompareType(hostname) == sniEndsWith {
			affected = append(affected, hostname)
		}
	}
	if len(affected) == 0 {
		return
	}
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	if p.tlsSNISuffix {
		err := p.octavia.VerifyGatewayCapabilities(ctx, provider, octavia.GatewayCapabilityTLSSNISuffix)
		if err == nil || sniCapabilityDiscoveryPending(err) {
			return
		}
	}
	isolate := func(hostname string) bool {
		return slices.ContainsFunc(affected, func(unsupported string) bool {
			return hostname == unsupported || amphora.SNISuffixCovers(hostname, unsupported)
		})
	}
	desired.sniPaused = make(map[sniFragmentKey]bool)
	for key, fragment := range actual.sni {
		if isolate(fragment.selectorHostname()) {
			desired.sniPaused[key] = true
		}
	}
	if desired.sniBlockedRoutes == nil {
		desired.sniBlockedRoutes = make(map[string]bool)
	}
	desired.sni = slices.DeleteFunc(desired.sni, func(target desiredSNIFragment) bool {
		if isolate(target.route.Hostname) {
			desired.sniBlockedRoutes[target.key.uid] = true
			return true
		}
		return false
	})
}

func (p *Provider) verifySNICapability(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) error {
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	required := []string{octavia.GatewayCapabilityTLSSNIPassthrough}
	if cloud.IsTCPTLSListenerProtocol(desired.protocol) {
		if actual.rawTLSCapabilityVerified {
			return nil
		}
		required = []string{octavia.GatewayCapabilityTCPTLSTermination}
		if desired.protocol == cloud.ListenerProtocolTLSMixed {
			required = []string{octavia.GatewayCapabilityTCPTLSMixedMode}
		}
	} else if graphUsesSNISuffix(desired, actual) {
		required = append(required, octavia.GatewayCapabilityTLSSNISuffix)
	}
	if err := p.octavia.VerifyGatewayCapabilities(ctx, provider, required...); err != nil {
		if desired.protocol == cloud.ListenerProtocolTLSMixed && isNotFound(err) {
			err = errors.Join(err, &octavia.MissingGatewayCapability{Provider: provider, Capability: octavia.GatewayCapabilityTCPTLSMixedMode})
		}
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("verify Octavia TLS SNI capabilities in region %q: %w", p.region, err))
	}
	return nil
}

func sniCapabilityDiscoveryPending(err error) bool {
	var missing *octavia.MissingGatewayCapability
	return err != nil && !isNotFound(err) && !errors.As(err, &missing)
}

func (p *Provider) handleSNICapabilityError(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, result cloud.GatewayGraphResult, cause error) (cloud.GatewayGraphResult, error) {
	if !sniCapabilityDiscoveryPending(cause) {
		return p.withdrawUndiscoverableSNI(ctx, desired, actual, cause)
	}
	outcome := p.progressingOutcome("Waiting for Octavia TLS SNI capability discovery")
	result.Outcome, result.Gateway.Outcome = outcome, outcome
	deferReadySNIReadback(&result, outcome)
	return result, ctx.Err()
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
	outcome := p.progressingOutcome("Octavia TLS SNI capability is unavailable")
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
