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
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const tcpWeightedPartPrefix = "tcp/"

type tcpWeightedRoutePlan struct {
	issue string
	step  *graphMutation
}

var errTCPWeightedReadback = cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
	errors.New("waiting for complete Octavia TCP default target readback"))

func (p *Provider) TCPWeightedDefaultPoolsAvailable(ctx context.Context) (bool, error) {
	if !p.tcpWeightedDefaultPools || !p.tcpDefaultPoolTLS || !p.backendTLSPolicy || p.backendCA == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	available, err := p.amphoraCapabilityAvailable(ctx, octavia.GatewayCapabilityTCPWeightedDefaultPools, octavia.GatewayCapabilityBackendTLSHostname)
	if isNotFound(err) {
		return false, nil
	}
	return available, err
}

func tcpWeightedResourceRole(base, key string) string {
	return "tcp-weighted-v1/" + base + "/" + strings.TrimPrefix(key, tcpWeightedPartPrefix)
}

func parseTCPWeightedResourceRole(raw string) (routeResourceRole, bool) {
	tail, ok := strings.CutPrefix(raw, "tcp-weighted-v1/")
	base, key, found := strings.Cut(tail, "/")
	if !ok || !found || !validTCPWeightedKey(key) || base != rolePool && base != roleMember && base != roleMonitor {
		return routeResourceRole{}, false
	}
	return routeResourceRole{raw: raw, base: base, backend: tcpWeightedPartPrefix + key}, true
}

func validTCPWeightedKey(key string) bool {
	if len(key) != 24 || key != strings.ToLower(key) {
		return false
	}
	_, err := hex.DecodeString(key)
	return err == nil
}

func (p *Provider) buildDesiredTCPWeightedRoute(spec cloud.RouteSpec) (desiredRoute, error) {
	invalid := func() (desiredRoute, error) {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("invalid weighted TCP backend targets"))
	}
	if spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolTCP || len(spec.TCPWeightedBackends) < 2 ||
		len(spec.TCPWeightedBackends) > octavia.MaxTCPDefaultTargets || spec.TCPConnectionRejection != nil || spec.UDPPacketDrop != nil ||
		len(spec.Members) != 0 || spec.BackendTLS != nil || spec.TCPXBackendClientCertificateReference != "" ||
		len(spec.HTTPBackends) != 0 || len(spec.HTTPRedirects) != 0 || len(spec.FixedHTTPRules) != 0 || len(spec.WeightedHTTPRules) != 0 {
		return invalid()
	}
	partSpec := spec
	partSpec.TCPWeightedBackends = nil
	route, err := p.buildDesiredRoutePart(partSpec, "", true, false)
	if err != nil {
		return desiredRoute{}, err
	}
	route.spec, route.unavailable = spec, false
	seen := make(map[string]bool, len(spec.TCPWeightedBackends))
	for _, target := range spec.TCPWeightedBackends {
		if target.Weight < 1 || target.Weight > octavia.MaxTCPDefaultTargetWeight {
			return invalid()
		}
		if target.Key == "" {
			if len(target.Members) != 0 || target.BackendTLS != nil || target.ClientCertificateReference != "" {
				return invalid()
			}
			continue
		}
		if !validTCPWeightedKey(target.Key) || seen[target.Key] ||
			!slices.ContainsFunc(target.Members, func(member cloud.Member) bool { return !member.Draining }) {
			return invalid()
		}
		seen[target.Key] = true
		partSpec.Members, partSpec.BackendTLS = target.Members, target.BackendTLS
		partSpec.TCPXBackendClientCertificateReference = target.ClientCertificateReference
		part, err := p.buildDesiredRoutePart(partSpec, tcpWeightedPartPrefix+target.Key, false, false)
		if err != nil {
			return desiredRoute{}, err
		}
		route.tcpParts = append(route.tcpParts, part)
	}
	return route, nil
}

func weightedRoutePoolIDs(observed observedRouteGraph) []string {
	ids := make([]string, 0, len(observed.tcpParts))
	for _, part := range sortedTCPWeightedPools(observed) {
		ids = append(ids, part.pool.ID)
	}
	return ids
}

func weightedTCPMutation(listenerID string, routeIndex int, targets []octavia.TCPDefaultTarget, clearScalar bool) *graphMutation {
	return &graphMutation{kind: graphSetTCPDefaultTargets, listenerID: listenerID, routeIndex: routeIndex,
		tcpDefaultTargets: targets, clearScalarTCPDefault: clearScalar, message: "Updating ordered Octavia TCP default targets"}
}

func tcpWeightedNodeUsesPort(desired desiredGatewayGraph, actual observedGatewayGraph, index, port int) bool {
	node := desired.spec.Routes[index]
	switch {
	case desired.spec.Gateway.Disposition != cloud.GraphEnsure:
		return false
	case node.Disposition == cloud.GraphPreserve:
		return slices.Contains(node.RetainListenerPorts, port)
	case node.Disposition != cloud.GraphEnsure:
		return false
	case actual.scoped:
		return slices.Contains(node.Spec.ScopedListenerPorts, port)
	default:
		return port == desired.spec.Gateway.Spec.ListenerPort
	}
}

type tcpWeightedPoolPlan struct {
	id    string
	issue string
	step  *routeMutation
}

func (p *Provider) observeTCPWeightedPart(ctx context.Context, desired desiredGatewayGraph, part desiredRoute, observed observedRouteGraph, available bool) tcpWeightedPoolPlan {
	if part.spec.BackendTLS == nil && observed.pool != nil && !observed.managedTCPDefaultPoolTLS() &&
		(!observed.backendTLS.Complete() || hasBackendTLSSettings(*observed.pool, observed.backendTLS) || observed.backendH2C) {
		return tcpWeightedPoolPlan{issue: "Waiting for clear TCP pool transport readback"}
	}
	if observed.tcpRejection.Invalid || observed.tcpRejection.Value != nil {
		return tcpWeightedPoolPlan{issue: "The weighted TCP pool has an incompatible connection rejection policy"}
	}
	if len(observed.memberBatchUnreported) != 0 {
		return tcpWeightedPoolPlan{issue: "Waiting for complete TCP pool member readback"}
	}
	part.tcpDefaultPoolTLS = p.observeTCPDefaultPoolTLS(ctx, part, observed, available, desired.spec.Gateway.BackendCAOwned)
	steps, _, err := buildRouteEnsurePlan(part, observed)
	if err != nil {
		return tcpWeightedPoolPlan{issue: err.Error()}
	}
	if issue := observed.backendProvisioningIssue(len(steps) != 0 && observed.managedTCPDefaultPoolTLS(), true); issue != "" {
		return tcpWeightedPoolPlan{issue: issue}
	}
	if len(steps) != 0 {
		if observed.pool != nil && len(observed.pool.Listeners) != 0 {
			return tcpWeightedPoolPlan{issue: "Waiting for the weighted TCP pool to detach before updating it"}
		}
		return tcpWeightedPoolPlan{step: &steps[0], issue: steps[0].message()}
	}
	if observed.pool == nil || !observed.pool.AdminStateUp {
		return tcpWeightedPoolPlan{issue: "Waiting for the weighted TCP pool to be active"}
	}
	return tcpWeightedPoolPlan{id: observed.pool.ID}
}

func weightedTCPRouteStep(index int, key string, step routeMutation) *graphMutation {
	return &graphMutation{kind: graphRouteMutation, routeIndex: index, backendKey: key,
		route: step, message: step.message(), redactTLSError: true}
}

// prepareTCPWeightedDefaultPools withdraws stale traffic before any ordinary
// graph mutation. Opening, pool staging and cleanup use the settled planner.
func (p *Provider) prepareTCPWeightedDefaultPools(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if actual.protocol != cloud.ListenerProtocolTCP || actual.loadBalancer == nil ||
		actual.loadBalancer.Provider != "amphora" || actual.loadBalancer.ProvisioningStatus != "ACTIVE" ||
		len(actual.routes) != len(desired.routes) || !hasTCPWeightedTarget(*desired) && !hasTCPWeightedReadback(actual) {
		return nil
	}
	available := false
	if hasTCPWeightedTarget(*desired) {
		available, _ = p.TCPWeightedDefaultPoolsAvailable(ctx)
	}
	frontends := actual.tcpListeners()
	plans := make([]map[string]tcpWeightedPoolPlan, len(desired.routes))
	for index, route := range desired.routes {
		plans[index] = make(map[string]tcpWeightedPoolPlan, len(route.tcpParts))
		for _, part := range route.tcpParts {
			var observed observedRouteGraph
			if found := actual.routes[index].tcpParts[part.backendKey]; found != nil {
				observed = *found
			}
			plans[index][part.backendKey] = p.observeTCPWeightedPart(ctx, *desired, part, observed, available)
		}
	}
	for index := range desired.routes {
		desired.routes[index].tcpWeightedPlan = p.planTCPWeightedRoute(*desired, actual, index, plans[index], available)
	}
	for _, listener := range frontends {
		if !p.eligibleTCPWeightedListener(listener) || listener.ProvisioningStatus != "ACTIVE" && listener.ProvisioningStatus != "ERROR" {
			continue
		}
		readback := actual.tcpDefaultTargets[listener.ID]
		if readback.Invalid || len(readback.Targets) != 0 && (!actual.defaultPoolsObserved || actual.tcpDefaultPoolTLS[listener.ID]) {
			if weightedTCPNeedsAdminWithdrawal(actual, listener) {
				return &graphMutation{kind: graphDisableListener, listenerID: listener.ID, routeIndex: selectedTCPWeightedRoute(*desired, actual, listener.ProtocolPort),
					scopedListener: cloud.ScopedListener{Port: listener.ProtocolPort}, message: "Closing the listener until its TCP default targets can be verified"}
			}
			continue
		}
		if step := p.withdrawTCPWeightedTargets(*desired, actual, listener, plans, available); step != nil {
			return step
		}
	}
	return nil
}

func weightedTCPPoolOwner(actual observedGatewayGraph, id string) (index int, key string) {
	for i, route := range actual.routes {
		for key, part := range route.tcpParts {
			if part.pool.ID == id {
				return i, key
			}
		}
	}
	return -1, ""
}

func selectedTCPWeightedRoute(desired desiredGatewayGraph, actual observedGatewayGraph, port int) int {
	for index, route := range desired.routes {
		if len(route.spec.TCPWeightedBackends) != 0 && tcpWeightedNodeUsesPort(desired, actual, index, port) {
			return index
		}
	}
	return -1
}

func (p *Provider) withdrawTCPWeightedTargets(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener,
	plans []map[string]tcpWeightedPoolPlan, available bool,
) *graphMutation {
	readback := actual.tcpDefaultTargets[listener.ID]
	selected := selectedTCPWeightedRoute(desired, actual, listener.ProtocolPort)
	if readback.PublicationStatus == "PENDING_UPDATE" {
		return nil
	}
	failed := readback.PublicationStatus == "ERROR" ||
		readback.PublicationStatus == "" && listener.ProvisioningStatus == "ERROR"
	replayOwner := selected
	if desired.spec.Gateway.Disposition == cloud.GraphPreserve {
		replayOwner = deletedTCPWeightedRouteForListener(desired, actual, listener.ID)
	}
	if len(readback.Targets) == 0 {
		if failed && readback.Present && listener.DefaultPoolID == "" &&
			(desired.spec.Gateway.Disposition != cloud.GraphPreserve || replayOwner != -1) {
			return weightedTCPMutation(listener.ID, replayOwner, nil, false)
		}
		if selected == -1 || listener.DefaultPoolID == "" || !actual.defaultPoolsObserved ||
			defaultPoolOwner(&listener, actual.routes) == -1 || !available || listener.ProvisioningStatus != "ACTIVE" {
			return nil
		}
		// Replace the old scalar default atomically. Until each new pool is
		// staged its declared interval explicitly rejects connections.
		targets := rejectTCPWeightedTargets(desired.routes[selected].spec.TCPWeightedBackends)
		return weightedTCPMutation(listener.ID, selected, targets, true)
	}
	owner := -1
	next := slices.Clone(readback.Targets)
	withdraw := false
	for i, target := range readback.Targets {
		if target.Reject {
			continue
		}
		index, key := weightedTCPPoolOwner(actual, target.PoolID)
		if index < 0 {
			return nil // ownership validation owns the diagnostic
		}
		owner = index
		node := desired.spec.Routes[index]
		if desired.spec.Gateway.Disposition == cloud.GraphPreserve && node.Disposition != cloud.GraphDelete {
			return nil
		}
		if index != selected || plans[index][key].id != target.PoolID {
			next[i] = octavia.TCPDefaultTarget{Reject: true, Weight: target.Weight}
			withdraw = true
		}
	}
	if owner == -1 {
		owner = replayOwner
	}
	if selected == -1 {
		if desired.spec.Gateway.Disposition == cloud.GraphPreserve && owner == -1 {
			return nil
		}
		return weightedTCPMutation(listener.ID, owner, nil, false)
	}
	retryable := readback.Retryable != nil && *readback.Retryable
	if readback.PublicationStatus == "ERROR" && !retryable {
		// After a failed admission the previous publication can still
		// differ from the desired list. Close all intervals before opening
		// that listener again.
		return weightedTCPMutation(listener.ID, owner, nil, false)
	}
	if withdraw || failed && (retryable || readback.PublicationStatus == "" &&
		slices.ContainsFunc(next, func(target octavia.TCPDefaultTarget) bool { return target.Reject })) {
		// An identical retry preserves the recorded targets and weights.
		// Octavia also checks the last published generation.
		return weightedTCPMutation(listener.ID, owner, next, false)
	}
	return nil
}

func deletedTCPWeightedRouteForListener(desired desiredGatewayGraph, actual observedGatewayGraph, listenerID string) int {
	for index, route := range actual.routes {
		if desired.spec.Routes[index].Disposition != cloud.GraphDelete {
			continue
		}
		for _, part := range route.tcpParts {
			if slices.ContainsFunc(part.pool.Listeners, func(ref pools.ListenerID) bool { return ref.ID == listenerID }) {
				return index
			}
		}
	}
	return -1
}

func rejectTCPWeightedTargets(backends []cloud.TCPWeightedBackend) []octavia.TCPDefaultTarget {
	targets := make([]octavia.TCPDefaultTarget, len(backends))
	for index, backend := range backends {
		targets[index] = octavia.TCPDefaultTarget{Reject: true, Weight: backend.Weight}
	}
	return targets
}

func (p *Provider) planTCPWeightedRoute(desired desiredGatewayGraph, actual observedGatewayGraph, index int,
	plans map[string]tcpWeightedPoolPlan, available bool,
) tcpWeightedRoutePlan {
	route, node, observed := desired.routes[index], desired.spec.Routes[index], actual.routes[index]
	wanted := len(route.spec.TCPWeightedBackends) != 0
	if !wanted && len(observed.tcpParts) == 0 || desired.spec.Gateway.Disposition == cloud.GraphPreserve && node.Disposition != cloud.GraphDelete {
		return tcpWeightedRoutePlan{}
	}
	var selected []listeners.Listener
	for _, listener := range actual.tcpListeners() {
		readback := actual.tcpDefaultTargets[listener.ID]
		usesPort := wanted && tcpWeightedNodeUsesPort(desired, actual, index, listener.ProtocolPort)
		if (usesPort || len(observed.tcpParts) != 0) && !tcpWeightedDetachmentReadback(actual, listener) {
			return tcpWeightedRoutePlan{issue: errTCPWeightedReadback.Error()}
		}
		if usesPort {
			if listener.ProvisioningStatus != "ACTIVE" || !p.eligibleTCPWeightedListener(listener) || !actual.adminObserved[listener.ID] ||
				len(readback.Targets) != 0 && actual.tcpDefaultPoolTLS[listener.ID] {
				return tcpWeightedRoutePlan{issue: errTCPWeightedReadback.Error()}
			}
			selected = append(selected, listener)
		}
	}

	if node.Disposition != cloud.GraphPreserve {
		for _, pool := range sortedTCPWeightedPools(observed) {
			_, key := weightedTCPPoolOwner(actual, pool.pool.ID)
			_, wanted := plans[key]
			if wanted && node.Disposition == cloud.GraphEnsure {
				continue
			}
			if len(pool.pool.Listeners) != 0 || slices.ContainsFunc(actual.tcpListeners(), func(listener listeners.Listener) bool {
				return listener.ProvisioningStatus != "ACTIVE"
			}) {
				return tcpWeightedRoutePlan{issue: "Waiting to confirm retired TCP targets have detached"}
			}
			if issue := pool.backendProvisioningIssue(true, true); issue != "" {
				return tcpWeightedRoutePlan{issue: issue}
			}
			steps := buildRouteDeletionPlan(*pool)
			return tcpWeightedRoutePlan{issue: steps[0].message(), step: weightedTCPRouteStep(index, key, steps[0])}
		}
	}
	if !wanted || node.Disposition != cloud.GraphEnsure {
		return tcpWeightedRoutePlan{}
	}
	if observed.pool != nil {
		if observed.pool.ProjectID != p.projectID || observed.pool.Listeners == nil || len(observed.pool.Listeners) != 0 ||
			slices.ContainsFunc(actual.tcpListeners(), func(listener listeners.Listener) bool { return listener.ProvisioningStatus != "ACTIVE" }) {
			return tcpWeightedRoutePlan{issue: "Waiting to confirm the previous scalar TCP default has detached"}
		}
		if issue := observed.backendProvisioningIssue(true, true); issue != "" {
			return tcpWeightedRoutePlan{issue: issue}
		}
		step := buildRouteDeletionPlan(observed)[0]
		return tcpWeightedRoutePlan{issue: step.message(), step: weightedTCPRouteStep(index, "", step)}
	}
	if !available {
		return tcpWeightedRoutePlan{issue: "Waiting for regional Octavia TCP weighted default pool support"}
	}
	lb := actual.loadBalancer
	if err := validateScopedRouteGatewayIDs(route.spec.Gateway, actual.state, actual.tcpListeners()); err != nil {
		return tcpWeightedRoutePlan{issue: err.Error()}
	}
	if lb.Pools == nil || lb.Listeners == nil {
		return tcpWeightedRoutePlan{issue: errTCPWeightedReadback.Error()}
	}
	if len(selected) == 0 || actual.scoped && len(selected) != len(route.spec.ScopedListenerPorts) {
		return tcpWeightedRoutePlan{issue: "Waiting for the selected TCP listeners"}
	}

	targets := rejectTCPWeightedTargets(route.spec.TCPWeightedBackends)
	var pending tcpWeightedRoutePlan
	for i, backend := range route.spec.TCPWeightedBackends {
		if backend.Key == "" {
			continue
		}
		key := tcpWeightedPartPrefix + backend.Key
		plan := plans[key]
		if plan.id != "" {
			targets[i] = octavia.TCPDefaultTarget{PoolID: plan.id, Weight: backend.Weight}
		} else if pending.step == nil && plan.step != nil {
			pending = tcpWeightedRoutePlan{issue: plan.issue, step: weightedTCPRouteStep(index, key, *plan.step)}
		} else if pending.issue == "" {
			pending.issue = plan.issue
		}
	}
	for _, listener := range selected {
		if !slices.Equal(targets, actual.tcpDefaultTargets[listener.ID].Targets) {
			// Only attach verified pools. An unavailable part continues to
			// consume its explicit rejected interval while staging progresses.
			step := weightedTCPMutation(listener.ID, index, targets, listener.DefaultPoolID != "")
			return tcpWeightedRoutePlan{issue: step.message, step: step}
		}
	}
	return pending
}

func (p *Provider) weightedTCPRouteResult(route desiredRoute, observed observedRouteGraph, gateway cloud.Outcome) cloud.RouteResult {
	state := routeStateFromObservation(observed)
	state.PoolIDs = weightedRoutePoolIDs(observed)
	for _, part := range observed.tcpParts {
		child := routeStateFromObservation(*part)
		state.MemberIDs = append(state.MemberIDs, child.MemberIDs...)
		if child.MonitorID != "" {
			state.MonitorIDs = append(state.MonitorIDs, child.MonitorID)
		}
	}
	slices.Sort(state.MemberIDs)
	slices.Sort(state.MonitorIDs)
	result := cloud.RouteReadyResult(state)
	if issue := route.tcpWeightedPlan.issue; issue != "" {
		result.Outcome = p.progressingOutcome(issue)
	} else if gateway.State != cloud.OutcomeReady {
		result.Outcome = gateway
	}
	return result
}

func (p *Provider) applyTCPWeightedWithdrawal(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, step graphMutation) (cloud.GatewayGraphResult, error) {
	actual.outcome = p.progressingOutcome(step.message)
	result := p.pendingTransportBatchResult(ctx, desired, actual, step.routeIndex)
	for index, route := range desired.routes {
		if index == step.routeIndex || !actual.scoped || len(route.spec.TCPWeightedBackends) == 0 ||
			desired.spec.Routes[index].Disposition != cloud.GraphEnsure || route.tcpWeightedPlan.issue != "" {
			continue
		}
		if actual.loadBalancer.ProvisioningStatus == "ACTIVE" && len(actual.routes) == len(desired.routes) &&
			!slices.Contains(route.spec.ScopedListenerPorts, step.scopedListener.Port) &&
			allTCPWeightedPortsReady(route, result.Gateway.ScopedListenerReadyPorts) {
			result.Routes[index].Result.ObservationDeferred = true
		}
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
	result.Outcome.MutationSubmitted = true
	if step.routeIndex >= 0 && step.routeIndex < len(result.Routes) {
		result.Routes[step.routeIndex].Result.Outcome.MutationSubmitted = true
	}
	return result, nil
}

func allTCPWeightedPortsReady(route desiredRoute, ports []int) bool {
	return len(route.spec.ScopedListenerPorts) != 0 && !slices.ContainsFunc(route.spec.ScopedListenerPorts, func(port int) bool {
		return !slices.Contains(ports, port)
	})
}

func tcpWeightedListenerCanRestore(desired desiredGatewayGraph, actual observedGatewayGraph, listener listeners.Listener) bool {
	readback := actual.tcpDefaultTargets[listener.ID]
	if readback.Invalid || readback.PublicationStatus != "" && readback.PublicationStatus != "ACTIVE" {
		return false
	}
	index := selectedTCPWeightedRoute(desired, actual, listener.ProtocolPort)
	if index == -1 {
		return len(readback.Targets) == 0
	}
	return readback.Present && desired.spec.Routes[index].Disposition == cloud.GraphEnsure &&
		desired.routes[index].tcpWeightedPlan.issue == ""
}

func appendWeightedTCPDeletionSteps(plan *gatewayDeletionPlan, route observedRouteGraph) {
	for _, part := range sortedTCPWeightedPools(route) {
		if issue := part.backendProvisioningIssue(true, true); issue != "" {
			plan.defaultPoolPending = issue
		}
		plan.pools = appendDefaultPoolDeletionSteps(plan.pools, *part)
	}
}

func sortedTCPWeightedPools(route observedRouteGraph) []*observedRouteGraph {
	parts := make([]*observedRouteGraph, 0, len(route.tcpParts))
	for _, part := range route.tcpParts {
		parts = append(parts, part)
	}
	slices.SortFunc(parts, func(a, b *observedRouteGraph) int { return strings.Compare(a.pool.ID, b.pool.ID) })
	return parts
}
