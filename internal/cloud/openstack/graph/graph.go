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
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/apierrors"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
)

type desiredGatewayGraph struct {
	spec     cloud.GatewayGraphSpec
	identity Identity
	routes   []desiredRoute
	protocol string
}

type observedGatewayGraph struct {
	loadBalancer    *octavia.LoadBalancer
	listener        *listeners.Listener
	scopedListeners []listeners.Listener
	scoped          bool
	floatingIP      *floatingips.FloatingIP
	routes          []observedRouteGraph
	deletion        gatewayDeletionPlan
	state           cloud.GatewayState
	addressAbsent   bool
	outcome         cloud.Outcome
	protocol        string
}

type graphMutationKind uint8

const (
	graphCreateLoadBalancer graphMutationKind = iota
	graphCreateFloatingIP
	graphAssociateFloatingIP
	graphDeleteFloatingIP
	graphCreateListener
	graphDisableListener
	graphClearListenerDefaultPool
	graphSetListenerDefaultPool
	graphRouteMutation
	graphEnableListener
	graphEnableLoadBalancer
	graphDisableLoadBalancer
	graphDeleteChild
	graphDeleteLoadBalancer
)

type graphMutation struct {
	kind               graphMutationKind
	routeIndex         int
	route              routeMutation
	deletion           gatewayDeletionStep
	poolID             string
	listenerID         string
	withdrawHTTPPolicy bool
	scopedListener     cloud.ScopedListener
	message            string
}

// ReconcileGatewayGraph validates and observes every component before building
// one ordered plan. It executes at most its first mutation and never caches the
// plan across calls, including after a timeout or a lost create response.
func (p *Provider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (result cloud.GatewayGraphResult, retErr error) {
	var addressIssue string
	var addressBlocked bool
	var actual observedGatewayGraph
	defer func() {
		result.Gateway.AddressAbsent = actual.addressAbsent
		if retErr == nil {
			result.Gateway.AddressIssue = addressIssue
			result.Gateway.AddressBlocked = addressBlocked
			return
		}
		var component *cloud.GraphError
		if errors.As(retErr, &component) {
			retErr = &cloud.GraphError{Identity: component.Identity, Err: classifyOpenStackError(component.Err)}
		} else {
			retErr = &cloud.GraphError{Identity: spec.Identity, Err: classifyOpenStackError(retErr)}
		}
	}()
	ctx, cancel := p.operationContext(ctx)
	defer cancel()

	desired, err := p.buildDesiredGatewayGraph(spec)
	if err != nil {
		return result, err
	}
	actual, err = p.observeGatewayGraph(ctx, desired)
	if err != nil {
		result.Gateway = observedGatewayResult(desired, actual, p.progressingOutcome("Waiting for the Gateway infrastructure"))
		return result, err
	}
	requested := spec.Gateway.Spec.RequestedVIPAddress
	replacing := spec.Gateway.Disposition == cloud.GraphDelete && actual.loadBalancer != nil
	if hasAddressRequest(spec.Gateway.Spec) && (spec.Gateway.Disposition == cloud.GraphEnsure || replacing) {
		addressIssue, err = p.requestedVIPIssue(ctx, spec.Gateway.Spec, actual)
		if err != nil {
			return result, err
		}
		floatingIPUnassigned := spec.Gateway.Spec.ExternalNetworkID != "" && actual.addressAbsent
		if actual.outcome.State == cloud.OutcomeReady && addressIssue != "" && (actual.loadBalancer == nil || replacing || floatingIPUnassigned) {
			addressBlocked = true
			actual.outcome = p.progressingOutcome(addressIssue)
		}
	}
	if actual.outcome.State == cloud.OutcomeProgressing {
		return p.pendingGraphResult(ctx, desired, actual), nil
	}
	if actual.protocol == cloud.ListenerProtocolUDP && actual.loadBalancer != nil {
		for index, route := range desired.routes {
			if desired.spec.Routes[index].Disposition == cloud.GraphEnsure {
				route.members = udpMembersInVIPFamily(route.members, actual.state.VIPAddress)
				route.spec.Members = route.members
				desired.routes[index] = route
			}
		}
	}
	var plan []graphMutation
	defaultPool := usesDefaultPool(actual.protocol)
	planBeforeSecurity := defaultPool || actual.scoped && actual.protocol == cloud.ListenerProtocolHTTP
	if planBeforeSecurity {
		plan, result, err = p.buildGatewayGraphPlan(desired, actual)
		if err != nil {
			return result, err
		}
	}
	if spec.Gateway.Disposition == cloud.GraphEnsure && (len(plan) == 0 || plan[0].kind != graphClearListenerDefaultPool && !plan[0].withdrawHTTPPolicy) {
		routes := make([]cloud.RouteSpec, 0, len(desired.routes))
		for index, route := range desired.routes {
			if desired.spec.Routes[index].Disposition == cloud.GraphEnsure {
				routes = append(routes, route.spec)
			}
		}
		if err := p.topology.ValidateGatewaySecurity(ctx, desired.spec.Gateway.Spec, routes); err != nil {
			result.Routes = observedRouteDeleteCompletions(desired, actual)
			return result, err
		}
	}
	if !planBeforeSecurity {
		plan, result, err = p.buildGatewayGraphPlan(desired, actual)
		if err != nil {
			return result, err
		}
	}
	if len(plan) == 0 {
		return result, nil
	}
	step := plan[0]
	if spec.ValidateMutation != nil {
		if err := spec.ValidateMutation(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
	}
	if err := p.executeGraphMutation(ctx, desired, actual, step); err != nil {
		if step.kind == graphCreateLoadBalancer && requested != "" && apierrors.IsOctaviaVIPAddressConflict(err, requested) {
			addressIssue = fmt.Sprintf("Octavia reports that VIP %s is already in use. Request an available IPv4 address on the GatewayClass VIP subnet", requested)
			addressBlocked = true
			actual.outcome = p.progressingOutcome(addressIssue)
			return p.pendingGraphResult(ctx, desired, actual), nil
		}
		if step.kind == graphRouteMutation {
			return result, &cloud.GraphError{Identity: desired.routes[step.routeIndex].spec.Identity, Err: err}
		}
		return result, err
	}
	result.Outcome = p.progressingOutcome(step.message)
	return result, nil
}

func hasAddressRequest(spec cloud.GatewaySpec) bool {
	return spec.RequestedVIPAddress != "" || spec.AddressRejection != ""
}

func (p *Provider) requestedVIPIssue(ctx context.Context, spec cloud.GatewaySpec, actual observedGatewayGraph) (string, error) {
	if spec.AddressRejection != "" {
		return spec.AddressRejection, nil
	}
	if spec.ExternalNetworkID != "" {
		return "A fixed Gateway address requires a GatewayClass without a Floating IP", nil
	}
	requested, err := netip.ParseAddr(spec.RequestedVIPAddress)
	if err != nil || !requested.Is4() {
		return "A fixed Gateway address must be IPv4", nil
	}
	if actual.loadBalancer != nil {
		if actual.state.VIPAddress != "" && actual.state.VIPAddress != requested.String() {
			return fmt.Sprintf("Octavia VIP %s cannot change to %s. Restore %s in spec.addresses or use a separate Gateway",
				actual.state.VIPAddress, requested, actual.state.VIPAddress), nil
		}
		return "", nil
	}
	supported, err := p.topology.VIPSubnetSupports(ctx, spec.VIPSubnetID, requested)
	if err != nil {
		return "", err
	}
	if !supported {
		return fmt.Sprintf("Requested VIP %s is outside the GatewayClass VIP subnet or reserved by it. Request a usable IPv4 address on that subnet", requested), nil
	}
	return "", nil
}

func (p *Provider) buildDesiredGatewayGraph(spec cloud.GatewayGraphSpec) (desiredGatewayGraph, error) {
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return desiredGatewayGraph{}, err
	}
	invalid := func(message string) (desiredGatewayGraph, error) {
		return desiredGatewayGraph{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New(message))
	}
	if spec.Identity.RouteUID != "" || spec.Identity.RouteName != "" || spec.Identity.RouteNamespace != "" {
		return invalid("Gateway graph identity must not contain route fields")
	}
	if !validGraphDisposition(spec.Gateway.Disposition) {
		return invalid("Gateway graph disposition is invalid")
	}
	var protocol string
	if spec.Gateway.Disposition != cloud.GraphPreserve || spec.Gateway.Spec.ScopedListeners != nil {
		protocol = spec.Gateway.Spec.ListenerProtocol
	}
	if spec.Gateway.Disposition == cloud.GraphEnsure {
		if !sameGatewayIdentity(spec.Identity, spec.Gateway.Spec.Identity) {
			return desiredGatewayGraph{}, fmt.Errorf("%w: Gateway model and graph identities differ", cloud.ErrOwnershipConflict)
		}
		if spec.Gateway.Spec.Provider != "amphora" || strings.TrimSpace(spec.Gateway.Spec.VIPSubnetID) == "" ||
			spec.Gateway.Spec.ListenerPort < 1 || spec.Gateway.Spec.ListenerPort > 65535 {
			return invalid("Gateway requires Amphora, a VIP subnet, and a listener port within 1-65535")
		}
		protocol = cloud.NormalizeListenerProtocol(protocol)
		for _, role := range []string{roleLoadBalancer, roleListener} {
			if _, err := identity.GatewayTags(role); err != nil {
				return desiredGatewayGraph{}, err
			}
		}
	}
	if protocol != "" && !supportedListenerProtocol(protocol) {
		return invalid("Gateway listener protocol must be HTTP, TCP or UDP")
	}
	if spec.Gateway.Spec.ScopedListeners != nil {
		if !supportedListenerProtocol(protocol) || spec.Gateway.Spec.ListenerPort < 1 || spec.Gateway.Spec.ListenerPort > 65535 {
			return invalid("scoped listeners require a frozen port within 1-65535")
		}
		if spec.Gateway.Disposition == cloud.GraphEnsure && len(spec.Gateway.Spec.ScopedListeners) == 0 {
			return invalid("an ensured multi-listener Gateway requires its desired ports")
		}
	}
	ports := make(map[int]bool, len(spec.Gateway.Spec.ScopedListeners))
	if len(spec.Gateway.Spec.ScopedListeners) != 0 {
		if spec.Gateway.Disposition != cloud.GraphEnsure {
			return invalid("a listener set requires an ensured Gateway")
		}
		names := make(map[string]bool, len(spec.Gateway.Spec.ScopedListeners))
		for _, listener := range spec.Gateway.Spec.ScopedListeners {
			if listener.Port < 1 || listener.Port > 65535 || strings.TrimSpace(listener.Name) == "" || ports[listener.Port] || names[listener.Name] {
				return invalid("scoped listeners require unique names and distinct ports within 1-65535")
			}
			ports[listener.Port] = true
			names[listener.Name] = true
		}
		spec.Gateway.Spec.ScopedListeners = slices.Clone(spec.Gateway.Spec.ScopedListeners)
		sort.Slice(spec.Gateway.Spec.ScopedListeners, func(i, j int) bool {
			return spec.Gateway.Spec.ScopedListeners[i].Port < spec.Gateway.Spec.ScopedListeners[j].Port
		})
	}
	// Copy before sorting: callers can reuse their immutable reconciliation input.
	spec.Routes = append([]cloud.RouteGraphNode(nil), spec.Routes...)
	sort.Slice(spec.Routes, func(i, j int) bool {
		first, second := spec.Routes[i], spec.Routes[j]
		firstEnsure, secondEnsure := first.Disposition == cloud.GraphEnsure, second.Disposition == cloud.GraphEnsure
		if firstEnsure != secondEnsure {
			return firstEnsure
		}
		if firstEnsure && first.Spec.Precedence != second.Spec.Precedence {
			return first.Spec.Precedence < second.Spec.Precedence
		}
		a, b := first.Spec.Identity, second.Spec.Identity
		if a.RouteNamespace != b.RouteNamespace {
			return a.RouteNamespace < b.RouteNamespace
		}
		if a.RouteName != b.RouteName {
			return a.RouteName < b.RouteName
		}
		return a.RouteUID < b.RouteUID
	})
	desired := desiredGatewayGraph{spec: spec, identity: identity, protocol: protocol}
	identities := make(map[string]struct{}, len(spec.Routes))
	lastRank := -1
	portWinners := make(map[int]bool, len(ports))
	rankByPort := make(map[int]int)
	positionByPort := make(map[int]int32)
	for _, node := range spec.Routes {
		id := node.Spec.Identity
		if !validGraphDisposition(node.Disposition) {
			return invalid("route graph disposition is invalid")
		}
		if err := cloud.ValidateRouteIdentity(id); err != nil {
			return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: err}
		}
		if !sameGatewayIdentity(spec.Identity, id) {
			return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: fmt.Errorf("%w: route belongs to a different Gateway graph", cloud.ErrOwnershipConflict)}
		}
		if _, duplicate := identities[id.RouteUID]; duplicate {
			return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: fmt.Errorf("%w: duplicate route fragment", cloud.ErrOwnershipConflict)}
		}
		identities[id.RouteUID] = struct{}{}
		if node.WaitForHTTPPolicyRemoval && (node.Disposition != cloud.GraphPreserve ||
			spec.Gateway.Disposition != cloud.GraphEnsure || protocol != cloud.ListenerProtocolHTTP) {
			return invalid("waiting for HTTP policy removal requires a preserved HTTPRoute on an ensured HTTP Gateway")
		}
		assigned := node.RetainScopedListenerPorts
		if assigned != nil && (node.Disposition != cloud.GraphPreserve || spec.Gateway.Disposition != cloud.GraphEnsure || len(ports) == 0) {
			return invalid("limiting a preserved pool's defaults requires a complete Gateway listener set")
		}
		if node.Disposition == cloud.GraphEnsure {
			assigned = node.Spec.ScopedListenerPorts
		}
		var httpPort int
		if protocol == cloud.ListenerProtocolHTTP && len(ports) != 0 && node.Disposition == cloud.GraphEnsure {
			if len(assigned) != 1 || !ports[assigned[0]] {
				return invalid("a scoped HTTPRoute requires exactly one port from the Gateway listener set")
			}
			httpPort = assigned[0]
		} else {
			for index, port := range assigned {
				if !ports[port] || portWinners[port] || index > 0 && assigned[index-1] >= port {
					return invalid("scoped route elections require sorted, distinct ports from the Gateway listener set")
				}
				portWinners[port] = true
			}
		}
		routeIdentity, err := p.identity(id)
		if err != nil {
			return desiredGatewayGraph{}, err
		}
		route := desiredRoute{identity: routeIdentity, spec: node.Spec}
		if transport := node.Spec.GatewayRequirements.ListenerProtocol; node.Disposition == cloud.GraphDelete && transport != "" && !supportedListenerProtocol(transport) {
			return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: fmt.Errorf("%w: unsupported detaching route protocol %q", cloud.ErrOwnershipConflict, transport)}
		}
		if len(node.RetainScopedListenerPorts) != 0 {
			if protocol != cloud.ListenerProtocolTCP || !node.Spec.GatewayRequirements.Equal(gatewayRequirements(spec.Gateway.Spec)) ||
				!slices.Equal(node.RetainScopedListenerPorts, node.Spec.ScopedListenerPorts) {
				return invalid("retaining TCP defaults requires the matching Gateway and backend specification")
			}
			route, err = p.buildDesiredRoute(node.Spec)
			if err != nil {
				return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: err}
			}
		}
		if node.Disposition == cloud.GraphEnsure {
			if spec.Gateway.Disposition != cloud.GraphEnsure {
				return invalid("an ensured route requires an ensured Gateway")
			}
			if usesDefaultPool(protocol) && len(ports) == 0 && lastRank != -1 {
				return invalid(fmt.Sprintf("a %s listener allows at most one ensured route", protocol))
			}
			previousRank, hasRank := rankByPort[httpPort]
			if node.Spec.Precedence < 0 || (len(ports) == 0 || httpPort != 0) && hasRank && node.Spec.Precedence == previousRank {
				return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation,
					fmt.Errorf("route precedence %d must be nonnegative and distinct among ensured routes", node.Spec.Precedence))}
			}
			lastRank = node.Spec.Precedence
			rankByPort[httpPort] = node.Spec.Precedence
			if len(ports) == 0 && len(node.Spec.ScopedListenerPorts) != 0 || len(ports) != 0 && len(node.Spec.ScopedListenerPorts) == 0 {
				return invalid("an ensured route of a multi-listener Gateway requires its selected ports")
			}
			if !node.Spec.GatewayRequirements.Equal(gatewayRequirements(spec.Gateway.Spec)) {
				return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: fmt.Errorf("%w: route and Gateway infrastructure requirements differ", cloud.ErrOwnershipConflict)}
			}
			route, err = p.buildDesiredRoute(node.Spec)
			if err != nil {
				return desiredGatewayGraph{}, &cloud.GraphError{Identity: id, Err: err}
			}
			for i := range route.policies {
				positionByPort[httpPort]++
				route.policies[i].position = positionByPort[httpPort]
			}
		}
		desired.routes = append(desired.routes, route)
	}
	return desired, nil
}

func observedGatewayResult(desired desiredGatewayGraph, actual observedGatewayGraph, pending cloud.Outcome) cloud.GatewayResult {
	result := cloud.GatewayResult{State: actual.state, Outcome: pending}
	if actual.scoped {
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure {
			if actual.protocol == cloud.ListenerProtocolHTTP {
				return scopedHTTPGatewayResult(desired, actual, pending)
			}
			return scopedGatewayResult(desired.spec.Gateway.Spec, actual, pending)
		}
		return result
	}
	if actual.loadBalancer == nil || actual.listener == nil || !actual.loadBalancer.AdminStateUp || !actual.listener.AdminStateUp {
		return result
	}
	if desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		if message := httpPolicyTransition(desired, actual); message != "" {
			result.Outcome.Message = message
			result.HTTPPolicyTransition = true
			return result
		}
		if actual.state.VIPAddress == "" {
			return result
		}
		protocol := cloud.NormalizeListenerProtocol(desired.spec.Gateway.Spec.ListenerProtocol)
		if actual.listener.DefaultPoolID != "" || actual.listener.ProvisioningStatus == "ERROR" ||
			usesDefaultPool(protocol) && actual.listener.ProvisioningStatus != "ACTIVE" ||
			actual.listener.Protocol != protocol || actual.listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort {
			return result
		}
		networkID := desired.spec.Gateway.Spec.ExternalNetworkID
		if networkID == "" && actual.floatingIP != nil || networkID != "" && actual.state.FloatingIPAddress == "" {
			return result
		}
		result.Outcome = cloud.ReadyOutcome()
	}
	return result
}

func validGraphDisposition(disposition cloud.GraphDisposition) bool {
	return disposition == cloud.GraphEnsure || disposition == cloud.GraphPreserve || disposition == cloud.GraphDelete
}

func supportedListenerProtocol(protocol string) bool {
	return protocol == cloud.ListenerProtocolHTTP || usesDefaultPool(protocol)
}

func sameGatewayIdentity(a, b cloud.Identity) bool {
	return a.OpenStackProjectID == b.OpenStackProjectID && a.ClusterID == b.ClusterID && a.Controller == b.Controller &&
		a.GatewayNamespace == b.GatewayNamespace && a.GatewayName == b.GatewayName && a.GatewayUID == b.GatewayUID
}

func gatewayRequirements(spec cloud.GatewaySpec) cloud.GatewayRequirements {
	return cloud.GatewayRequirements{Provider: spec.Provider, VIPSubnetID: spec.VIPSubnetID, ExternalNetworkID: spec.ExternalNetworkID,
		FlavorID: spec.FlavorID, AvailabilityZone: spec.AvailabilityZone, ListenerPort: spec.ListenerPort,
		ListenerProtocol: spec.ListenerProtocol, VIPSecurityGroupIDs: spec.VIPSecurityGroupIDs}
}

func (d desiredGatewayGraph) routeOwners() []routeGraphOwner {
	owners := make([]routeGraphOwner, len(d.routes))
	for index, route := range d.routes {
		owners[index].identity = route.identity
		if d.spec.Routes[index].Disposition == cloud.GraphDelete {
			owners[index].cleanupProtocol = route.spec.GatewayRequirements.ListenerProtocol
		}
	}
	return owners
}

func (p *Provider) observeGatewayGraph(ctx context.Context, desired desiredGatewayGraph) (observedGatewayGraph, error) {
	actual := observedGatewayGraph{outcome: cloud.ReadyOutcome(), protocol: desired.protocol, scoped: desired.spec.Gateway.Spec.ScopedListeners != nil}
	lb, err := p.findGatewayLoadBalancer(ctx, desired.identity)
	if err != nil {
		return actual, err
	}
	if lb == nil {
		return p.observeAbsentGateway(ctx, desired)
	}
	if !desired.identity.MatchesGateway(lb.Tags, roleLoadBalancer) {
		return actual, fmt.Errorf("%w: load balancer %s has an incomplete or stale identity", cloud.ErrOwnershipConflict, lb.ID)
	}
	observation, err := p.observeLoadBalancerOnce(ctx, lb.ID)
	if err != nil {
		return actual, err
	}
	if observation.loadBalancer != nil {
		observed := observation.loadBalancer
		if observed.ID != lb.ID || !desired.identity.MatchesGateway(observed.Tags, roleLoadBalancer) {
			return actual, fmt.Errorf("%w: load balancer identity changed during observation", cloud.ErrOwnershipConflict)
		}
		if err := p.validateLoadBalancerProject(observed.ID, observed.ProjectID); err != nil {
			return actual, err
		}
	}
	if observation.phase == loadBalancerPhaseAbsent {
		return p.observeAbsentGateway(ctx, desired)
	}
	actual.outcome = observation.outcome
	actual.loadBalancer = observation.loadBalancer
	lb = actual.loadBalancer
	actual.state = cloud.GatewayState{LoadBalancerID: lb.ID, VIPPortID: lb.VipPortID, VIPAddress: lb.VipAddress}
	if observation.phase == loadBalancerPhasePending && desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return actual, nil
	}
	if desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		if err := validateGraphLoadBalancer(lb, gatewayRequirements(desired.spec.Gateway.Spec)); err != nil {
			return actual, err
		}
	}
	addressSpec := desired.spec.Gateway.Spec
	observeAddress := desired.spec.Gateway.Disposition == cloud.GraphEnsure ||
		desired.spec.Gateway.Disposition == cloud.GraphDelete && hasAddressRequest(addressSpec)
	if observeAddress && lb.VipAddress == "" {
		actual.outcome = p.progressingOutcome("Waiting to observe the Octavia VIP address")
		return actual, nil
	}
	actual.floatingIP, err = p.findGatewayFloatingIP(ctx, desired.identity, lb.VipPortID, lb.VipAddress)
	if err != nil {
		return actual, err
	}
	if actual.floatingIP != nil {
		actual.state.FloatingIPID = actual.floatingIP.ID
		if actual.floatingIP.PortID != "" && (!observeAddress || actual.floatingIP.FloatingNetworkID == addressSpec.ExternalNetworkID) {
			actual.state.FloatingIPAddress = actual.floatingIP.FloatingIP
		}
	}
	if observeAddress && addressSpec.ExternalNetworkID != "" {
		actual.addressAbsent = lb.VipPortID != "" && (actual.floatingIP == nil || actual.floatingIP.PortID == "")
		if actual.floatingIP != nil && actual.floatingIP.FloatingNetworkID == addressSpec.ExternalNetworkID &&
			actual.floatingIP.PortID != "" && actual.floatingIP.FloatingIP == "" {
			actual.outcome = p.progressingOutcome("Waiting to observe the Gateway Floating IP address")
			return actual, nil
		}
	}
	owners := desired.routeOwners()
	listenerList, markedProtocol, err := p.listGatewayListeners(ctx, lb, actual.scoped)
	if err != nil {
		return actual, err
	}
	if markedProtocol != "" {
		if actual.protocol != "" && actual.protocol != markedProtocol {
			return actual, fmt.Errorf("%w: scoped listeners differ from the Gateway protocol binding", cloud.ErrOwnershipConflict)
		}
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure && !actual.scoped {
			return actual, fmt.Errorf("%w: scoped listeners require the Gateway listener set", cloud.ErrOwnershipConflict)
		}
		actual.protocol, actual.scoped = markedProtocol, true
	}
	if actual.scoped {
		return p.observeScopedListenerGraph(ctx, desired, actual, owners, listenerList, observation.phase == loadBalancerPhasePending)
	}
	if desired.spec.Gateway.Disposition == cloud.GraphDelete {
		if observation.phase == loadBalancerPhasePending {
			return actual, nil
		}
		actual.deletion, err = p.buildGatewayDeletionPlanFromListeners(ctx, desired.identity, lb, owners, desired.protocol, listenerList)
		if actual.deletion.defaultPoolListener != nil {
			actual.listener = actual.deletion.defaultPoolListener
			actual.state.ListenerID = actual.listener.ID
		}
		if actual.deletion.defaultPoolPending != "" {
			actual.outcome = p.progressingOutcome(actual.deletion.defaultPoolPending)
		}
		return actual, err
	}
	actual.listener, err = p.managedGatewayListener(desired.identity, lb.ID, listenerList)
	if err != nil {
		return actual, err
	}
	if actual.listener != nil {
		protocol := actual.listener.Protocol
		if !supportedListenerProtocol(protocol) || desired.protocol != "" && protocol != desired.protocol {
			return actual, fmt.Errorf("%w: Gateway listener protocol %q differs from its binding", cloud.ErrOwnershipConflict, protocol)
		}
		actual.protocol = protocol
		actual.state.ListenerID = actual.listener.ID
		if desired.spec.Gateway.Disposition == cloud.GraphEnsure && actual.listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort {
			return actual, fmt.Errorf("%w: Gateway listener port differs from its binding", cloud.ErrOwnershipConflict)
		}
		if usesDefaultPool(protocol) {
			if err := validateDefaultPoolListener(actual.listener, lb.ID); err != nil {
				return actual, err
			}
		} else if desired.spec.Gateway.Disposition == cloud.GraphEnsure && actual.listener.ProvisioningStatus == "ERROR" {
			return actual, cloud.NewProviderError(cloud.ErrorCategoryResourceFailure, fmt.Errorf("octavia listener %s provisioning status is ERROR", actual.listener.ID))
		}
		if len(lb.VIPSecurityGroupIDs) > 0 && len(actual.listener.AllowedCIDRs) > 0 {
			return actual, fmt.Errorf("%w: custom VIP security groups cannot be combined with listener allowed CIDRs", cloud.ErrOwnershipConflict)
		}
	}
	// Octavia's parent provisioning status also covers route mutations. Observe
	// the immutable Gateway component without changing it so member progress does
	// not erase a still-programmed Gateway from component status.
	if observation.phase == loadBalancerPhasePending {
		return actual, nil
	}
	if actual.listener == nil && actual.protocol == "" {
		actual.protocol, err = detachingProtocolWithoutListener(owners)
		if err != nil {
			return actual, err
		}
	}
	defaultPool := usesDefaultPool(actual.protocol)
	if defaultPool && actual.listener != nil {
		actual.outcome, err = p.defaultPoolListenerOutcome(actual.listener, true)
		if err != nil {
			return actual, err
		}
		if actual.outcome.State == cloud.OutcomeProgressing {
			return actual, nil
		}
	}
	actual.routes, err = p.observeRouteGraphs(ctx, owners, lb, actual.listener, actual.protocol)
	if err != nil {
		return actual, err
	}
	if !defaultPool && actual.listener != nil && strings.HasPrefix(actual.listener.ProvisioningStatus, "PENDING_") {
		actual.outcome = p.progressingOutcome("Waiting for Octavia listener provisioning")
		return actual, nil
	}
	if err := validateObservedPolicyLayout(actual.routes); err != nil {
		return actual, err
	}
	if (defaultPool || desired.spec.Gateway.Disposition == cloud.GraphEnsure || hasOtherCleanupProtocol(owners, actual.protocol)) &&
		actual.listener != nil && actual.listener.DefaultPoolID != "" && defaultPoolOwner(actual.listener, actual.routes) == -1 {
		return actual, unprovenDefaultPool(actual.listener)
	}
	return actual, nil
}

func validateObservedPolicyLayout(routes []observedRouteGraph) error {
	positions := make(map[int32]bool)
	policyCount := 0
	for _, route := range routes {
		for _, policy := range route.policies {
			positions[policy.policy.Position] = true
			policyCount++
		}
	}
	// Octavia moves policies by popping their current one-based position from
	// the listener's policy list. Wait for a complete, consistent readback.
	for position := int32(1); position <= int32(policyCount); position++ {
		if !positions[position] {
			return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("missing Octavia L7 policy at position %d", position))
		}
	}
	return nil
}

// observeAbsentGateway checks independently allocated Neutron resources before
// declaring a deleted graph absent or creating a replacement load balancer.
func (p *Provider) observeAbsentGateway(ctx context.Context, desired desiredGatewayGraph) (observedGatewayGraph, error) {
	actual := observedGatewayGraph{outcome: cloud.ReadyOutcome(), protocol: desired.protocol, scoped: desired.spec.Gateway.Spec.ScopedListeners != nil}
	floatingIP, err := p.findGatewayFloatingIP(ctx, desired.identity, "", "")
	if err != nil {
		return actual, err
	}
	actual.floatingIP = floatingIP
	if floatingIP == nil {
		actual.addressAbsent = true
		return actual, nil
	}
	actual.state.FloatingIPID = floatingIP.ID
	if desired.spec.Gateway.Disposition == cloud.GraphEnsure {
		return actual, fmt.Errorf("%w: Gateway Floating IP remains after its load balancer disappeared", cloud.ErrOwnershipConflict)
	}
	return actual, nil
}

func validateGraphLoadBalancer(lb *octavia.LoadBalancer, requirements cloud.GatewayRequirements) error {
	if lb.Provider != requirements.Provider || lb.VipSubnetID != requirements.VIPSubnetID ||
		requirements.FlavorID != "" && lb.FlavorID != requirements.FlavorID ||
		requirements.AvailabilityZone != "" && lb.AvailabilityZone != requirements.AvailabilityZone {
		return fmt.Errorf("%w: load balancer %s differs from the bound topology", cloud.ErrOwnershipConflict, lb.ID)
	}
	groups := slices.Clone(lb.VIPSecurityGroupIDs)
	slices.Sort(groups)
	if !slices.Equal(groups, requirements.VIPSecurityGroupIDs) {
		return fmt.Errorf("%w: load balancer VIP security groups differ from the bound topology", cloud.ErrOwnershipConflict)
	}
	return nil
}

func (p *Provider) pendingGraphResult(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph) cloud.GatewayGraphResult {
	result := cloud.GatewayGraphResult{Outcome: actual.outcome, Gateway: observedGatewayResult(desired, actual, actual.outcome)}
	var deferred map[int]bool
	if actual.scoped {
		if actual.protocol == cloud.ListenerProtocolHTTP {
			deferred = deferredScopedHTTPRoutes(desired, actual, result.Gateway.ScopedListenerReadyPorts)
		} else {
			deferred = p.deferredScopedRouteObservations(ctx, desired, actual, result.Gateway.ScopedListenerReadyPorts)
		}
	}
	for index, route := range desired.routes {
		result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: route.spec.Identity, Result: cloud.RouteResult{
			Outcome: actual.outcome, ObservationDeferred: !actual.scoped || deferred[index],
		}})
	}
	return result
}
