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
)

var sniProfileTag = openstackidentity.Tag("profile", "tls-sni-v1")

type sniFragmentKey struct {
	namespace string
	name      string
	uid       string
	target    string
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

func buildSNITargets(gateway cloud.Identity, routes []cloud.TLSSNIRoute) ([]desiredSNIFragment, error) {
	result := make([]desiredSNIFragment, 0, len(routes))
	names := make(map[string]bool)
	uids := make(map[string]bool)
	for _, route := range routes {
		id, err := NewIdentity(route.Identity)
		if err != nil {
			return nil, err
		}
		if !sameGatewayIdentity(gateway, route.Identity) || cloud.ValidateRouteIdentity(route.Identity) != nil {
			return nil, fmt.Errorf("%w: TLSRoute belongs to a different Gateway or has incomplete identity", cloud.ErrOwnershipConflict)
		}
		if amphora.ValidateExactHostname(route.Hostname) != nil ||
			names[route.Hostname] || uids[route.Identity.RouteUID] || route.ServiceUID == "" || len(route.Members) == 0 {
			return nil, invalidSNI("TLSRoutes require distinct exact DNS names and one identified Service with ready members")
		}
		names[route.Hostname], uids[route.Identity.RouteUID] = true, true
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
			namespace: encodedSNITag("route-namespace", route.Identity.RouteNamespace),
			name:      encodedSNITag("route-name", route.Identity.RouteName),
			uid:       encodedSNITag("route-uid", route.Identity.RouteUID),
			target:    encodedTarget,
		}
		fragment := desiredSNIFragment{route: route, identity: id, key: key, targetTag: targetTag}
		for _, role := range []string{roleSNIPool, roleSNIMember, roleSNIPolicy, roleSNIRule} {
			if _, err := fragment.tags(role); err != nil {
				return nil, err
			}
		}
		result = append(result, fragment)
	}
	slices.SortFunc(result, func(a, b desiredSNIFragment) int { return cmp.Compare(a.route.Hostname, b.route.Hostname) })
	return result, nil
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
		if listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" {
			actual.outcome = p.progressingOutcome("Waiting for TLS SNI listener provisioning")
			return actual, nil
		}
	}
	if actual.outcome.State != cloud.OutcomeReady {
		return actual, nil
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
		if fragment.policy.ProvisioningStatus != "ACTIVE" || len(fragment.members) != len(wanted.route.Members) ||
			fragment.policy.AdminStateUp && fragment.rule == nil {
			return false
		}
	}
	return fragment.rule == nil || fragment.rule.ProvisioningStatus == "ACTIVE" && fragment.rule.AdminStateUp && !fragment.rule.Invert &&
		fragment.rule.RuleType == string(ruleTLSSNI) && fragment.rule.CompareType == "EQUAL_TO" &&
		fragment.rule.Key == "" && fragment.rule.Value == wanted.route.Hostname
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
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(actual.state)}
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
	for _, frag := range desired.sni {
		wanted[frag.key] = frag
	}
	keys := slices.SortedFunc(maps.Keys(actual.sni), func(a, b sniFragmentKey) int {
		return cmp.Or(cmp.Compare(a.namespace, b.namespace), cmp.Compare(a.name, b.name), cmp.Compare(a.uid, b.uid), cmp.Compare(a.target, b.target))
	})
	var retire []graphMutation
	for _, key := range keys {
		fragment := actual.sni[key]
		if slices.ContainsFunc(fragment.members, func(m pools.Member) bool { return strings.HasPrefix(m.ProvisioningStatus, "PENDING_") }) ||
			strings.HasPrefix(fragment.pool.ProvisioningStatus, "PENDING_") ||
			fragment.policy != nil && strings.HasPrefix(fragment.policy.ProvisioningStatus, "PENDING_") ||
			fragment.rule != nil && strings.HasPrefix(fragment.rule.ProvisioningStatus, "PENDING_") {
			result.Outcome, result.Gateway.Outcome = pending, pending
			return nil, result, nil
		}
		if target, keep := wanted[key]; !keep || !fragment.matches(target) {
			retire = append(retire, sniDeletionStep(fragment))
		} else if fragment.policy != nil && fragment.policy.AdminStateUp && fragment.rule != nil &&
			actual.loadBalancer.AdminStateUp && actual.listener != nil && actual.listener.AdminStateUp &&
			actual.listener.ProvisioningStatus == "ACTIVE" && desired.spec.Gateway.Disposition == cloud.GraphEnsure &&
			(desired.spec.Gateway.Spec.ExternalNetworkID == "" || actual.state.FloatingIPAddress != "") {
			result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: target.route.Identity, Result: cloud.RouteResult{Outcome: cloud.ReadyOutcome()}})
		}
	}
	// Close every stale policy before staging a new route, including when a
	// hostname moves to a different Service UID or is claimed by two routes.
	if len(retire) != 0 {
		for _, step := range retire {
			if step.deletion.resource == gatewayDisableL7Policy {
				return plan(step)
			}
		}
		return plan(retire[0])
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
		fragment := actual.sni[target.key]
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
			continue
		}
		return plan(graphMutation{kind: graphSNIMutation, sni: mutation, message: "Converging the TLS SNI route"})
	}
	if steps, err := buildGatewayTrafficRestorationPlan(desired, actual); err != nil {
		return nil, result, err
	} else if len(steps) != 0 {
		return plan(steps[0])
	}
	return nil, result, nil
}

type sniMutationKind uint8

const (
	sniCreatePool sniMutationKind = iota
	sniCreateMember
	sniCreatePolicy
	sniCreateRule
	sniEnablePolicy
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
		_, err := p.octavia.CreatePolicy(ctx, l7policies.CreateOpts{
			Name: resourceName(step.target.route.Identity, roleSNIPolicy), ListenerID: actual.state.ListenerID, Action: l7policies.ActionRedirectToPool,
			RedirectPoolID: step.parentID, AdminStateUp: boolPointer(false), Tags: tagsFor(roleSNIPolicy),
		}, nil)
		return err
	case sniCreateRule:
		_, err := p.octavia.CreateRule(ctx, step.parentID, l7policies.CreateRuleOpts{
			RuleType: ruleTLSSNI, CompareType: "EQUAL_TO", Value: step.target.route.Hostname,
			AdminStateUp: boolPointer(true), Tags: tagsFor(roleSNIRule),
		})
		return err
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
	if err := p.octavia.VerifyGatewayCapabilities(ctx, provider, octavia.GatewayCapabilityTLSSNIPassthrough); err != nil {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("verify Octavia TLS SNI passthrough: %w", err))
	}
	return nil
}

func (p *Provider) withdrawUndiscoverableSNI(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, cause error) (cloud.GatewayGraphResult, error) {
	outcome := p.progressingOutcome("Octavia TLS SNI passthrough capability is unavailable")
	result := cloud.GatewayGraphResult{Outcome: outcome, Gateway: cloud.GatewayResult{State: actual.state, Outcome: outcome}}
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
	for i := range result.Routes {
		// A previously proven sibling can stay Programmed during this read
		// failure. Do not create a new serving proof without address inventory.
		result.Routes[i].Result = cloud.RouteResult{Outcome: result.Gateway.Outcome, ObservationDeferred: true}
	}
	if len(plan) != 1 || plan[0].kind != graphDeleteChild || plan[0].deletion.resource != gatewayDisableL7Policy || !plan[0].sniWithdrawal {
		return result, actual.addressReadError
	}
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
	}
	if err := p.executeGraphMutation(ctx, desired, actual, plan[0]); err != nil {
		return result, err
	}
	p.recordGraphMutationOutcome(&result, plan[0])
	return result, nil
}
