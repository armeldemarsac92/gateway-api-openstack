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
	"maps"
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

type requestMirrorAvailability uint8

const (
	requestMirrorAvailable requestMirrorAvailability = iota
	requestMirrorMissing
	requestMirrorUnverified
)

var errRequestMirrorsUnverified = errors.New("waiting for active Octavia HTTP request mirror readback")

type desiredHTTPMirror struct {
	backendKey  string
	numerator   int32
	denominator int32
	phase       cloud.HTTPMirrorPhase
}

func collectHTTPMirrorPools(spec cloud.RouteSpec, used map[int]bool) error {
	matches := func(matches []cloud.HTTPPathMatch) {
		for _, match := range matches {
			for _, mirror := range match.RequestMirrors {
				used[mirror.BackendIndex] = true
			}
		}
	}
	for _, backend := range spec.HTTPBackends {
		matches(backend.Matches)
	}
	for _, rule := range spec.FixedHTTPRules {
		matches(rule.Matches)
	}
	for _, rule := range spec.WeightedHTTPRules {
		matches(rule.Matches)
		for _, target := range rule.Targets {
			for _, mirror := range target.RequestMirrors {
				used[mirror.BackendIndex] = true
			}
		}
	}
	for index := range used {
		if index < 0 || index >= len(spec.HTTPBackends) {
			return errors.New("HTTP request mirror must reference a backend pool")
		}
	}
	return nil
}

func (p *Provider) compileHTTPMirrors(spec cloud.RouteSpec, mirrors []cloud.HTTPMirrorTarget) ([]desiredHTTPMirror, error) {
	if len(mirrors) == 0 {
		return nil, nil
	}
	if !p.httpRequestMirror {
		return nil, errors.New("mirroring requires --octavia-http-request-mirror")
	}
	result := make([]desiredHTTPMirror, 0, len(mirrors))
	for _, mirror := range mirrors {
		if mirror.BackendIndex < 0 || mirror.BackendIndex >= len(spec.HTTPBackends) ||
			!cloud.ValidHTTPMirrorRatio(mirror.Numerator, mirror.Denominator) || !mirror.Phase.Valid() {
			return nil, errors.New("invalid HTTP request mirror destination, ratio or phase")
		}
		backend := spec.HTTPBackends[mirror.BackendIndex]
		if backend.InvalidReference || backend.Kind != "" && backend.Kind != cloud.HTTPBackendKindServiceImport ||
			backend.BackendTLS != nil && backend.BackendTLS.Denied {
			return nil, errors.New("HTTP request mirror must reference an authorized Service or ServiceImport backend")
		}
		if backend.Kind == cloud.HTTPBackendKindServiceImport && (!backend.RoutedExternal || backend.Unavailable || len(backend.Members) == 0) {
			return nil, errors.New("ServiceImport request mirror requires a routed pool with eligible members")
		}
		if spec.GRPC && !backend.BackendH2C && backend.BackendTLS == nil {
			return nil, errors.New("gRPC request mirror requires a backend using h2c or authenticated HTTP/2")
		}
		result = append(result, desiredHTTPMirror{backendKey: scopeHTTPKey(spec, openstackidentity.HTTPBackendPoolKey(backend)),
			numerator: mirror.Numerator, denominator: mirror.Denominator, phase: mirror.Phase})
	}
	return result, nil
}

// A new mirror needs a converged pool. An existing mirror may keep a pool
// while new members converge, provided every live member remains authorized.
func resolveHTTPMirrors(route desiredRoute, observed observedRouteGraph, mirrors []desiredHTTPMirror, current []octavia.HTTPRequestMirror) []octavia.HTTPRequestMirror {
	if route.mirrorAvailability == requestMirrorMissing {
		return nil
	}
	retained := retainHTTPMirrors(route, observed, mirrors, current)
	if route.mirrorAvailability == requestMirrorUnverified {
		return retained
	}
	var result []octavia.HTTPRequestMirror
	for _, mirror := range mirrors {
		backend := observed.httpParts[mirror.backendKey]
		if backend == nil || backend.pool == nil {
			continue
		}
		value := mirror.forPool(backend.pool.ID)
		if i := slices.Index(retained, value); i >= 0 {
			retained = slices.Delete(retained, i, i+1)
		} else if !weightedBackendReady(route, mirror.backendKey, *backend) {
			continue
		}
		result = append(result, value)
	}
	return result
}

func observedMirrorPolicy(part *observedRouteGraph, role string) observedRoutePolicy {
	if part != nil {
		if index := slices.IndexFunc(part.policies, func(policy observedRoutePolicy) bool { return policy.role == role }); index >= 0 {
			return part.policies[index]
		}
	}
	return observedRoutePolicy{}
}

func (mirror desiredHTTPMirror) forPool(poolID string) octavia.HTTPRequestMirror {
	return octavia.HTTPRequestMirror{PoolID: poolID, Numerator: mirror.numerator, Denominator: mirror.denominator, Phase: mirror.phase}
}

func (policy observedRoutePolicy) hasRequestMirrors() bool {
	return policy.requestMirrorsTagged || len(policy.requestMirrors) != 0 || weightedTargetHasMirrors(policy.weightedTargets)
}

func (policy observedRoutePolicy) allRequestMirrors() []octavia.HTTPRequestMirror {
	mirrors := slices.Clone(policy.requestMirrors)
	for _, target := range policy.weightedTargets {
		mirrors = append(mirrors, target.RequestMirrors...)
	}
	return mirrors
}

func weightedTargetHasMirrors(targets []octavia.WeightedHTTPTarget) bool {
	return slices.ContainsFunc(targets, func(target octavia.WeightedHTTPTarget) bool {
		return len(target.RequestMirrors) != 0
	})
}

func (policy desiredPolicy) requestMirrorsMatch(observed observedRoutePolicy) bool {
	wanted := policy.resolvedRequestMirrors
	targets := policy.weightedPlan != nil && weightedTargetHasMirrors(policy.weightedPlan.targets)
	hasMirrors := len(wanted) != 0 || targets
	return (!hasMirrors || observed.adminObserved && observed.requestMirrorsReported && observed.requestMirrorsTagged) &&
		!observed.requestMirrorsUnknown && slices.Equal(wanted, observed.requestMirrors) &&
		(observed.requestMirrorsTagged == hasMirrors)
}

func mirrorPolicyTags(tags []string, hasMirrors bool) []string {
	tags = slices.DeleteFunc(slices.Clone(tags), func(tag string) bool {
		return tag == openstackidentity.HTTPRequestMirrorTag
	})
	if hasMirrors {
		tags = append(tags, openstackidentity.HTTPRequestMirrorTag)
	}
	return tags
}

func setRequestMirrorUpdate(step *routeMutation, wanted desiredPolicy, observed observedRoutePolicy) {
	tags := step.policyTags
	if tags == nil {
		tags = observed.policy.Tags
		if step.requestURLRewriteUpdate != nil {
			tags = step.requestURLRewriteUpdate.Tags
		}
	}
	hasMirrors := len(wanted.resolvedRequestMirrors) != 0 || wanted.weightedPlan != nil && weightedTargetHasMirrors(wanted.weightedPlan.targets)
	step.requestMirrorsUpdate = &octavia.RequestMirrorsUpdate{Value: wanted.resolvedRequestMirrors}
	step.policyTags = mirrorPolicyTags(tags, hasMirrors)
}

func hasDesiredRequestMirrors(route desiredRoute) bool {
	return slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
		return slices.ContainsFunc(part.policies, func(policy desiredPolicy) bool { return len(policy.requestMirrors) != 0 }) ||
			slices.ContainsFunc(part.weightedTargets, func(target desiredHTTPWeightedTarget) bool { return len(target.requestMirrors) != 0 })
	})
}

func requestMirrorPoolKeys(route desiredRoute) map[string]bool {
	keys := make(map[string]bool)
	for _, part := range route.httpParts {
		for _, policy := range part.policies {
			for _, mirror := range policy.requestMirrors {
				keys[mirror.backendKey] = true
			}
		}
		for _, target := range part.weightedTargets {
			for _, mirror := range target.requestMirrors {
				keys[mirror.backendKey] = true
			}
		}
	}
	return keys
}

func isPrimaryHTTPPool(route desiredRoute, key string, observed observedRouteGraph, pool *pools.Pool) bool {
	wanted := slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
		return slices.ContainsFunc(part.weightedTargets, func(target desiredHTTPWeightedTarget) bool { return target.backendKey == key })
	})
	if wanted || pool == nil {
		return wanted
	}
	for _, part := range observed.parts() {
		for _, policy := range part.policies {
			if policy.policy.RedirectPoolID == pool.ID || slices.ContainsFunc(policy.weightedTargets, func(target octavia.WeightedHTTPTarget) bool {
				return target.PoolID == pool.ID && target.Weight > 0
			}) {
				return true
			}
		}
	}
	return false
}

func requestMirrorCapabilities(grpc bool) []string {
	if grpc {
		return []string{octavia.GatewayCapabilityHTTPRequestMirror, octavia.GatewayCapabilityGRPCContentType, octavia.GatewayCapabilityGRPCRequestMirror}
	}
	return []string{octavia.GatewayCapabilityHTTPRequestMirror}
}

func (p *Provider) gateRequestMirrors(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || !p.httpRequestMirror {
		return
	}
	for _, grpc := range []bool{false, true} {
		usesCapability := func(route desiredRoute) bool { return route.spec.GRPC == grpc && hasDesiredRequestMirrors(route) }
		if !slices.ContainsFunc(desired.routes, usesCapability) {
			continue
		}
		required := requestMirrorCapabilities(grpc)
		err := p.verifyGatewayHTTPURLCapabilities(ctx, *desired, actual, required)
		if err == nil {
			continue
		}
		availability, capability := requestMirrorUnverified, required[len(required)-1]
		var missing *octavia.MissingGatewayCapability
		if errors.As(err, &missing) {
			availability, capability = requestMirrorMissing, missing.Capability
		}
		for index, route := range desired.routes {
			if usesCapability(route) {
				desired.routes[index].mirrorAvailability = availability
				desired.routes[index].mirrorUnavailableCapability = capability
			}
		}
	}
}

func (p *Provider) reportRequestMirrorAvailability(desired desiredGatewayGraph, result *cloud.GatewayGraphResult) {
	for index, route := range desired.routes {
		if route.mirrorAvailability == requestMirrorAvailable || index >= len(result.Routes) {
			continue
		}
		outcome := &result.Routes[index].Result
		message := "Waiting to verify Octavia " + route.mirrorUnavailableCapability
		if route.mirrorAvailability == requestMirrorMissing {
			message = "Octavia does not advertise " + route.mirrorUnavailableCapability
			if !routeHasIndependentMirrorlessPolicies(route) {
				outcome.UnsupportedMatch = message
			}
		}
		outcome.Outcome = p.progressingOutcome(message)
	}
}

func routeHasIndependentMirrorlessPolicies(route desiredRoute) bool {
	return slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
		return !slices.ContainsFunc(part.weightedTargets, func(target desiredHTTPWeightedTarget) bool { return len(target.requestMirrors) > 0 }) &&
			slices.ContainsFunc(part.policies, func(policy desiredPolicy) bool { return len(policy.requestMirrors) == 0 })
	})
}

func (p *Provider) reconcileRequestMirrorWithdrawal(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (cloud.GatewayGraphResult, error) {
	step, isolated := p.protectMirrorWithdrawalTargetResponses(ctx, desired, actual, step)
	validate := desired.spec.ValidateMutation
	if validate != nil {
		if err := validate(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
	}
	actual.outcome = p.progressingOutcome(step.message)
	if step.kind == graphDisableListener {
		actual.listenerFilterMutation = &step
	}
	result := p.pendingGraphResult(ctx, desired, actual)
	result.Gateway.HTTPPolicyTransition = isolated
	err := p.executeGraphMutation(ctx, desired, actual, step)
	if err != nil && step.kind == graphRouteMutation && step.route.kind == routeMutationSetRequestMirrors && !gophercloud.ResponseCodeIs(err, http.StatusConflict) && ctx.Err() == nil {
		// When Octavia rejects an in-place withdrawal, close only that owned
		// policy. Its previous generation may still be serving the copy.
		if validate != nil {
			if invalid := validate(ctx); invalid != nil {
				return cloud.GatewayGraphResult{}, invalid
			}
		}
		step.route = routeMutation{kind: routeMutationDisablePolicy, id: step.route.id, retry: true, withdrawHTTPPolicy: true}
		err = p.executeGraphMutation(ctx, desired, actual, step)
	}
	if err != nil {
		return result, &cloud.GraphError{Identity: desired.routes[step.routeIndex].spec.Identity, Err: err}
	}
	result.Outcome.MutationSubmitted = true
	result.Routes[step.routeIndex].Result.ObservationDeferred = false
	result.Routes[step.routeIndex].Result.Outcome.MutationSubmitted = true
	return result, nil
}

// Octavia replaces every field in a target slot on PUT. The urgent mirror
// withdrawal may replay the vector only if its response filters are verified.
func (p *Provider) protectMirrorWithdrawalTargetResponses(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (graphMutation, bool) {
	if step.route.weightedTargets == nil {
		return step, false
	}
	inventory := actual.routes[step.routeIndex]
	current := inventory.httpParts[step.backendKey]
	if current == nil {
		return step, false
	}
	i := slices.IndexFunc(current.policies, func(policy observedRoutePolicy) bool { return policy.policy.ID == step.route.id })
	if i < 0 {
		return step, false
	}
	observed := current.policies[i]
	route := desired.routes[step.routeIndex]
	part := graphRoutePart(desired, step)
	wantedIndex := slices.IndexFunc(part.policies, func(policy desiredPolicy) bool { return policy.role == observed.role })
	if !observed.hasBackendResponseHeaders() &&
		(wantedIndex < 0 || !backendResponseHeadersPolicy.required(part.policies[wantedIndex])) {
		return step, false
	}
	if wantedIndex >= 0 && part.weightedTargets != nil && p.responseHeaderModifier && p.weightedHTTPTargets {
		part.grpcFailurePoolID = observedGRPCFailurePoolID(route, inventory)
		part = resolveWeightedHTTPPart(part, route, inventory, servingWeightedListenerID(actual, part))
		wanted := part.policies[wantedIndex]
		if mirrorWithdrawalResponseHeadersMatch(wanted, observed) {
			required := append([]string{octavia.GatewayCapabilityTargetResponseHeaders}, backendResponseHeadersPolicy.additionalCapabilities(wanted)...)
			if p.verifyGatewayHTTPURLCapabilities(ctx, desired, actual, required) == nil {
				return step, false
			}
		}
	}

	reason := "Waiting for verified Octavia backend response headers while withdrawing request mirrors"
	if listener := p.ownedHTTPFilterListener(actual, step.listenerID); listener != nil &&
		(listener.ProvisioningStatus == "ERROR" || listener.ProvisioningStatus == "ACTIVE" && (listener.AdminStateUp || !actual.adminObserved[listener.ID])) {
		closeListener := closeGuardedHTTPListener(*listener, reason)
		closeListener.routeIndex = step.routeIndex
		return closeListener, true
	}
	// Remove the disabled policy through the same ownership path used for
	// unverified mirrors. Normal planning reconstructs and verifies its slots
	// before opening the listener again.
	kind := routeMutationDisablePolicy
	if observedPolicyDisabled(observed) {
		kind = routeMutationDeletePolicy
	}
	step.route = routeMutation{kind: kind, id: observed.policy.ID, retry: true, withdrawHTTPPolicy: true}
	step.message = reason
	return step, true
}

func mirrorWithdrawalResponseHeadersMatch(wanted desiredPolicy, observed observedRoutePolicy) bool {
	return backendResponseHeadersPolicy.required(wanted) && observed.backendResponseHeadersTagged &&
		wanted.weightedPlan != nil && !wanted.weightedPlan.pending &&
		slices.Equal(wanted.responseHeaderOperations, observed.responseHeaderOperations) &&
		(wanted.grpcContentTypeRule != grpcContentTypeRuleType || observed.hasNativeGRPCContentType()) &&
		slices.EqualFunc(wanted.weightedPlan.targets, observed.weightedTargets, func(expected, actual octavia.WeightedHTTPTarget) bool {
			expected.RequestMirrors, actual.RequestMirrors = nil, nil
			return weightedTargetsEqual(expected, actual)
		})
}

// Withdraws revoked occurrences before changing the primary target, general
// filters, or any pool. Filtering by occurrence preserves order and duplicates
// and never replaces an authorized sample with an expanded sample.
func nextRequestMirrorWithdrawal(desired desiredGatewayGraph, actual observedGatewayGraph) (graphMutation, bool) {
	for index, observedRoute := range actual.routes {
		if index >= len(desired.routes) {
			break
		}
		route := desired.routes[index]
		if desired.spec.Routes[index].Disposition == cloud.GraphPreserve {
			continue
		}
		for _, key := range append([]string{""}, slices.Sorted(maps.Keys(observedRoute.httpParts))...) {
			part := observedRoute
			if key != "" {
				part = *observedRoute.httpParts[key]
			}
			wantedIndex := slices.IndexFunc(route.httpParts, func(candidate desiredRoute) bool { return candidate.backendKey == key })
			var wantedPart desiredRoute
			if wantedIndex >= 0 && desired.spec.Routes[index].Disposition == cloud.GraphEnsure && route.mirrorAvailability != requestMirrorMissing {
				wantedPart = route.httpParts[wantedIndex]
			}
			for _, observed := range part.policies {
				if !observed.hasRequestMirrors() || observed.pending() {
					continue
				}
				var wanted desiredPolicy
				if i := slices.IndexFunc(wantedPart.policies, func(policy desiredPolicy) bool { return policy.role == observed.role }); i >= 0 {
					wanted = wantedPart.policies[i]
				}
				step, needed := requestMirrorWithdrawal(route, observedRoute, wantedPart, wanted, observed)
				if needed {
					return graphMutation{kind: graphRouteMutation, routeIndex: index, backendKey: key,
						listenerID: observed.policy.ListenerID, route: step, withdrawHTTPPolicy: true,
						message: "Withdrawing obsolete Octavia HTTP request mirrors"}, true
				}
			}
		}
	}
	return graphMutation{}, false
}

func requestMirrorWithdrawal(route desiredRoute, inventory observedRouteGraph, part desiredRoute, wanted desiredPolicy, observed observedRoutePolicy) (routeMutation, bool) {
	step := routeMutation{kind: routeMutationSetRequestMirrors, id: observed.policy.ID, withdrawHTTPPolicy: true}
	ruleMirrors := retainHTTPMirrors(route, inventory, wanted.requestMirrors, observed.requestMirrors)
	if !slices.Equal(ruleMirrors, observed.requestMirrors) {
		step.requestMirrorsUpdate = &octavia.RequestMirrorsUpdate{Value: ruleMirrors}
	}
	targets := slices.Clone(observed.weightedTargets)
	targetUpdate := false
	for i := range targets {
		var wantedMirrors []desiredHTTPMirror
		if i < len(part.weightedTargets) {
			target := part.weightedTargets[i]
			backend := inventory.httpParts[target.backendKey]
			if backend != nil && backend.pool != nil && target.weight > 0 && targets[i].PoolID == backend.pool.ID {
				wantedMirrors = target.requestMirrors
			}
		}
		remaining := retainHTTPMirrors(route, inventory, wantedMirrors, targets[i].RequestMirrors)
		if !slices.Equal(remaining, targets[i].RequestMirrors) {
			targets[i].RequestMirrors = remaining
			targetUpdate = true
		}
	}
	if targetUpdate {
		step.weightedTargets = &targets
	}
	hasMirrors := len(ruleMirrors) != 0 || weightedTargetHasMirrors(targets)
	if observed.requestMirrorsUnknown || observed.policy.ProvisioningStatus == "ERROR" {
		if !observedPolicyDisabled(observed) {
			return routeMutation{kind: routeMutationDisablePolicy, id: observed.policy.ID, retry: true, withdrawHTTPPolicy: true}, true
		}
		if observed.requestMirrorsUnknown {
			// A downlevel list cannot prove the stored copies are gone.
			// Remove the closed policy before reconstructing its primary.
			return routeMutation{kind: routeMutationDeletePolicy, id: observed.policy.ID, withdrawHTTPPolicy: true}, true
		}
	}
	if step.requestMirrorsUpdate == nil && !targetUpdate && (observed.requestMirrorsTagged == hasMirrors) {
		return routeMutation{}, false
	}
	if !hasMirrors && len(observed.requestMirrors) != 0 && (observed.fixedResponse || allWeightedTargetsFixed(targets)) {
		// These request transformations existed only for the rule's copies.
		// Drop them with the last copy and keep the primary error response.
		step.requestHeaderOperations = policyHeaderPatch(nil, observed.requestHeaderOperations, false)
		step.requestURLRewriteUpdate = policyURLRewritePatch(nil, observed)
		if step.requestURLRewriteUpdate != nil {
			step.requestURLRewriteUpdate.KeepPolicyState = true
		}
	}
	if observed.policy.ProvisioningStatus == "ERROR" {
		step.retry = true
	}
	// Keep the ownership marker until an ACTIVE readback confirms the
	// withdrawal. A worker error can leave the previous Amphora config live.
	if step.requestMirrorsUpdate == nil && !targetUpdate {
		step.policyTags = mirrorPolicyTags(observed.policy.Tags, hasMirrors)
	} else if !observed.requestMirrorsTagged {
		tags := observed.policy.Tags
		if step.requestURLRewriteUpdate != nil {
			tags = step.requestURLRewriteUpdate.Tags
		}
		step.policyTags = mirrorPolicyTags(tags, true)
	}
	return step, true
}

func retainHTTPMirrors(route desiredRoute, inventory observedRouteGraph, wanted []desiredHTTPMirror, existing []octavia.HTTPRequestMirror) []octavia.HTTPRequestMirror {
	var authorized []octavia.HTTPRequestMirror
	for _, mirror := range wanted {
		backend := inventory.httpParts[mirror.backendKey]
		if backend == nil || backend.pool == nil || !requestMirrorMembersAuthorized(route, mirror.backendKey, *backend) {
			continue
		}
		authorized = append(authorized, mirror.forPool(backend.pool.ID))
	}
	var result []octavia.HTTPRequestMirror
	for _, mirror := range existing {
		if index := slices.Index(authorized, mirror); index >= 0 {
			result = append(result, mirror)
			authorized = slices.Delete(authorized, index, index+1)
		}
	}
	return result
}

func requestMirrorMembersAuthorized(route desiredRoute, key string, observed observedRouteGraph) bool {
	i := slices.IndexFunc(route.httpParts, func(part desiredRoute) bool { return part.backendKey == key })
	if i < 0 {
		return false
	}
	backend := route.httpParts[i]
	if backend.backendTransportNeedsWithdrawal(observed) {
		return false
	}
	if tls := backend.spec.BackendTLS; tls != nil {
		if tls.Denied || !observed.pool.TLSEnabled || !backendTLSReadbackComplete(backend, observed) ||
			!backendTLSProtocolMatches(backend, observed) ||
			tls.Hostname != observed.backendTLS.Hostname || !backendTLSSubjectAltNamesMatch(tls.SubjectAltNames, observed) ||
			!slices.Contains(observed.pool.Tags, backendTLSRevisionTag(tls)) {
			return false
		}
	} else if hasObservedBackendTLS(*observed.pool, observed.backendTLS) {
		return false
	}
	for _, member := range observed.members {
		_, adminUnknown := observed.memberAdminUnreported[member.ID]
		if !member.AdminStateUp && !adminUnknown {
			continue
		}
		if !slices.ContainsFunc(backend.members, func(wanted cloud.Member) bool {
			return member.Address == wanted.Address && member.ProtocolPort == wanted.Port &&
				member.SubnetID == backend.spec.MemberSubnetID && (!wanted.Draining || member.Weight == 0)
		}) {
			return false
		}
	}
	return true
}

func requestMirrorWriteRequiresCapability(step routeMutation) bool {
	if step.withdrawHTTPPolicy {
		return false
	}
	if step.requestMirrorsUpdate != nil && len(step.requestMirrorsUpdate.Value) != 0 ||
		step.weightedTargets != nil && weightedTargetHasMirrors(*step.weightedTargets) {
		return true
	}
	return (step.kind == routeMutationCreatePolicy || step.kind == routeMutationEnablePolicy) &&
		(len(step.policy.resolvedRequestMirrors) != 0 ||
			step.policy.weightedPlan != nil && weightedTargetHasMirrors(step.policy.weightedPlan.targets))
}
