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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const (
	telemetryListenerTag    = "gw-api-listener-otlp-v1"
	telemetryPoolTag        = "gw-api-otlp-collector-pool-v1"
	telemetryPoolNamePrefix = "gateway-api-otlp-collector-"
)

type telemetryPool struct {
	observedAuthPool
	tls octavia.PoolBackendTLS
	h2c bool
}

func telemetryPoolName(wanted *cloud.GatewayTelemetry, clientCertificate string) string {
	key := fmt.Appendf(nil, "%s:%d:%s", wanted.ServiceUID, wanted.ServicePort, wanted.MemberSubnetID)
	if tls := wanted.BackendTLS; tls != nil {
		key = fmt.Appendf(key, "|%s|%s|%v|%s", tls.Hostname, backendTLSRevisionTag(tls), tls.SubjectAltNames, clientCertificate)
	}
	for _, member := range wanted.Members {
		key = fmt.Appendf(key, "|%s:%d", member.Address, member.Port)
	}
	digest := sha256.Sum256(key)
	return fmt.Sprintf("%s%x", telemetryPoolNamePrefix, digest[:16])
}

func isTelemetryPool(pool pools.Pool) bool {
	return slices.Contains(pool.Tags, telemetryPoolTag)
}

func hasRouteOwnership(tags []string) bool {
	return slices.ContainsFunc(tags, func(tag string) bool {
		key, _, _ := openstackidentity.ParseTag(tag)
		return strings.HasPrefix(key, "route-")
	})
}

// Collector pools use Gateway ownership, so an HTTPRoute must never adopt or
// retire one. Check the shared identity tags before removing it from the
// application inventory and keep the original parent snapshot intact.
func omitTelemetryPoolsFromRoutes(lb *octavia.LoadBalancer, inventory []pools.Pool) (*octavia.LoadBalancer, []pools.Pool, error) {
	if !slices.ContainsFunc(inventory, isTelemetryPool) {
		return lb, inventory, nil
	}
	ids := map[string]bool{}
	var expected []string
	for _, tag := range lb.Tags {
		key, _, _ := openstackidentity.ParseTag(tag)
		switch key {
		case "managed", "cluster", "controller", "namespace", "name", "uid", "project":
			expected = append(expected, tag)
		}
	}
	expected = append(expected, openstackidentity.Tag("role", rolePool))
	for _, pool := range inventory {
		if !isTelemetryPool(pool) {
			continue
		}
		if len(expected) != 8 || !openstackidentity.HasAllTags(pool.Tags, expected) || pool.ProjectID != lb.ProjectID || hasRouteOwnership(pool.Tags) ||
			validateBoundPoolParent(pool, lb.ID, cloud.ListenerProtocolHTTP) != nil || ids[pool.ID] ||
			!strings.HasPrefix(pool.Name, telemetryPoolNamePrefix) {
			return nil, nil, fmt.Errorf("%w: invalid Octavia collector pool ownership", cloud.ErrOwnershipConflict)
		}
		ids[pool.ID] = true
	}
	remaining := slices.DeleteFunc(slices.Clone(inventory), isTelemetryPool)
	parent := *lb
	parent.Pools = slices.DeleteFunc(slices.Clone(lb.Pools), func(pool pools.Pool) bool { return ids[pool.ID] })
	return &parent, remaining, nil
}

func eligibleTelemetryListener(protocol string) bool {
	return protocol == string(listeners.ProtocolHTTP) || protocol == string(listeners.ProtocolTerminatedHTTPS)
}

func telemetryListenerWanted(listener listeners.Listener, wanted *cloud.GatewayTelemetry) bool {
	return wanted != nil && slices.Contains(wanted.Listeners, cloud.TelemetryListener{Port: listener.ProtocolPort, Protocol: listener.Protocol})
}

type telemetryGraph struct {
	identity     Identity
	spec         cloud.GatewayGraphSpec
	loadBalancer *octavia.LoadBalancer
	listeners    []listeners.Listener
	tracing      map[string]octavia.ListenerOTLPTracingObservation
	refs         map[string]bool
	unverified   bool
}

func (p *Provider) reconcileGatewayTelemetry(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	if spec.ValidateMutation == nil || len(spec.Routes) != 0 || spec.Identity.RouteUID != "" ||
		spec.Identity.RouteName != "" || spec.Identity.RouteNamespace != "" {
		return cloud.GatewayGraphResult{}, errors.New("listener tracing requires fenced Gateway authority")
	}
	wanted := spec.Gateway.Telemetry
	ready := telemetryResult(cloud.ReadyOutcome())
	discovered, err := p.findGatewayLoadBalancer(ctx, identity)
	if err != nil {
		return ready, err
	}
	if discovered == nil {
		if wanted != nil {
			return p.telemetryPending("Waiting for the Gateway load balancer"), nil
		}
		return ready, nil
	}
	balance, err := p.observeLoadBalancerOnce(ctx, discovered.ID)
	if err != nil {
		return ready, err
	}
	if balance.phase == loadBalancerPhaseAbsent {
		return p.telemetryPending("Waiting to observe the Gateway load balancer"), nil
	}
	lb := balance.loadBalancer
	if lb.ID != discovered.ID || !identity.MatchesGateway(discovered.Tags, roleLoadBalancer) ||
		!identity.MatchesGateway(lb.Tags, roleLoadBalancer) || p.validateLoadBalancerProject(lb.ID, lb.ProjectID) != nil {
		return ready, fmt.Errorf("%w: listener tracing could not prove the Gateway load balancer", cloud.ErrOwnershipConflict)
	}
	if balance.phase != loadBalancerPhaseActive {
		return telemetryResult(balance.outcome), nil
	}
	inventory, _, err := p.listGatewayListeners(ctx, lb)
	if err != nil {
		return ready, err
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return ready, err
	}
	graph := telemetryGraph{identity: identity, spec: spec, loadBalancer: lb, tracing: inventory.OTLPTracing, refs: map[string]bool{}}
	for _, listener := range inventory.Items {
		if !eligibleTelemetryListener(listener.Protocol) {
			continue
		}
		if listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 ||
			listener.Loadbalancers[0].ID != lb.ID || !identity.MatchesGateway(listener.Tags, roleListener) {
			return ready, fmt.Errorf("%w: listener tracing could not prove listener %s", cloud.ErrOwnershipConflict, listener.ID)
		}
		graph.listeners = append(graph.listeners, listener)
		if current := graph.tracing[listener.ID]; current.Value != nil {
			graph.refs[current.Value.CollectorPoolID] = true
		}
		graph.unverified = graph.unverified || unknownTelemetryReadback(listener, graph.tracing)
	}
	slices.SortFunc(graph.listeners, func(a, b listeners.Listener) int { return cmp.Compare(a.ID, b.ID) })
	return p.planGatewayTelemetry(ctx, graph, wanted)
}

func unknownTelemetryReadback(listener listeners.Listener, observations map[string]octavia.ListenerOTLPTracingObservation) bool {
	current, present := observations[listener.ID]
	return present && !current.Readable ||
		(current.Value != nil || slices.Contains(listener.Tags, telemetryListenerTag)) && listener.ProvisioningStatus != "ACTIVE"
}

func telemetryResult(outcome cloud.Outcome) cloud.GatewayGraphResult {
	return cloud.GatewayGraphResult{Outcome: outcome, Gateway: cloud.GatewayResult{Outcome: outcome}}
}

func (p *Provider) telemetryPending(message string) cloud.GatewayGraphResult {
	return telemetryResult(p.progressingOutcome(message))
}

func (p *Provider) applyTelemetryMutation(ctx context.Context, graph telemetryGraph, message string, run func() error) (cloud.GatewayGraphResult, error) {
	if err := graph.spec.ValidateMutation(ctx); err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	result := p.telemetryPending(message)
	err := run()
	result.Outcome.MutationSubmitted = err == nil
	return result, classifyOctaviaMutationError(err)
}

func (p *Provider) updateListenerTelemetry(ctx context.Context, graph telemetryGraph, frontend listeners.Listener, value *octavia.ListenerOTLPTracing) (cloud.GatewayGraphResult, error) {
	return p.applyTelemetryMutation(ctx, graph, "Waiting for Octavia listener tracing readback", func() error {
		tags := slices.Clone(frontend.Tags)
		if !slices.Contains(tags, telemetryListenerTag) {
			tags = append(tags, telemetryListenerTag)
		}
		return p.octavia.SetListenerOTLPTracing(ctx, frontend.ID, value, tags)
	})
}

func (p *Provider) telemetryWithdrawal(ctx context.Context, graph telemetryGraph, retain func(listeners.Listener, *octavia.ListenerOTLPTracing) bool) (cloud.GatewayGraphResult, bool, error) {
	waiting := false
	for _, frontend := range graph.listeners {
		current := graph.tracing[frontend.ID]
		if current.Value == nil && !unknownTelemetryReadback(frontend, graph.tracing) || retain != nil && retain(frontend, current.Value) {
			continue
		}
		if frontend.ProvisioningStatus != "ACTIVE" && frontend.ProvisioningStatus != "ERROR" {
			waiting = true
			continue
		}
		result, err := p.updateListenerTelemetry(ctx, graph, frontend, nil)
		return result, true, err
	}
	if waiting {
		return p.telemetryPending("Waiting to withdraw Octavia listener tracing"), true, nil
	}
	return cloud.GatewayGraphResult{}, false, nil
}

func (p *Provider) planGatewayTelemetry(ctx context.Context, graph telemetryGraph, wanted *cloud.GatewayTelemetry) (cloud.GatewayGraphResult, error) {
	var unsupported string
	var dependencyErr error
	if wanted != nil {
		var required = []string{octavia.GatewayCapabilityListenerOTLPTracing}
		if tls := wanted.BackendTLS; tls != nil {
			if !p.backendTLSPolicy || p.backendCA == nil || !validBackendTLS(tls) || graph.spec.Gateway.BackendClientCertificate.Denied || !graph.spec.Gateway.BackendCAOwned {
				unsupported = "The collector requires authenticated TLS and an authorized Gateway CA store"
			}
			required = append(required, backendTLSCapabilities(false, len(tls.SubjectAltNames) != 0)...)
		}
		if unsupported == "" {
			err := p.octavia.VerifyGatewayCapabilities(ctx, graph.loadBalancer.Provider, required...)
			var missing *octavia.MissingGatewayCapability
			switch {
			case errors.As(err, &missing):
				unsupported = "Regional Octavia does not advertise " + missing.Capability
			case gophercloud.ResponseCodeIs(err, http.StatusNotFound):
				unsupported = "Regional Octavia does not advertise listener tracing"
			case err != nil:
				dependencyErr = err
			}
		}
	}
	if unsupported != "" || dependencyErr != nil {
		wanted = nil
	}
	if wanted == nil {
		if result, changed, err := p.telemetryWithdrawal(ctx, graph, nil); changed {
			return result, err
		}
	}

	owned, err := p.observeTelemetryPools(ctx, graph)
	if err != nil {
		if result, changed, withdrawalErr := p.telemetryWithdrawal(ctx, graph, nil); changed {
			return result, withdrawalErr
		}
		return cloud.GatewayGraphResult{}, err
	}
	ready := telemetryResult(cloud.ReadyOutcome())
	ready.TelemetryUnsupported = unsupported
	if wanted == nil {
		result, changed, err := p.retireTelemetryPools(ctx, graph, owned, "")
		if changed {
			result.TelemetryUnsupported = unsupported
			return result, err
		}
		return ready, dependencyErr
	}
	return p.convergeGatewayTelemetry(ctx, graph, owned, wanted)
}

func (p *Provider) convergeGatewayTelemetry(ctx context.Context, graph telemetryGraph, owned map[string]telemetryPool, wanted *cloud.GatewayTelemetry) (cloud.GatewayGraphResult, error) {
	name := telemetryPoolName(wanted, graph.spec.Gateway.BackendClientCertificate.Reference)
	pool := owned[name]
	backend := cloud.RouteSpec{Identity: graph.spec.Identity, Members: wanted.Members, MemberSubnetID: wanted.MemberSubnetID,
		BackendSecurityGroupIDs: wanted.BackendSecurityGroupIDs, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolHTTP}}
	securityErr := p.topology.ValidateGatewaySecurity(ctx, cloud.GatewaySpec{}, []cloud.RouteSpec{backend})
	var verifiedRef string
	var trustErr error
	if tls := wanted.BackendTLS; tls != nil && pool.pool.ID != "" {
		revision, err := p.backendCA.Find(ctx, graph.spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CADigest, pool.pool.CATLSContainerRef)
		if err != nil {
			trustErr = err
		} else if revision.Digest() == tls.CADigest {
			verifiedRef = revision.Reference()
		}
	}
	active := pool.pool.ID != "" && pool.admin && pool.pool.AdminStateUp && pool.pool.ProvisioningStatus == "ACTIVE" && !pool.h2c
	tlsMatches := telemetryPoolTLSMatches(pool, wanted.BackendTLS, verifiedRef, graph.spec.Gateway.BackendClientCertificate.Reference)
	active = active && tlsMatches
	waiting, nextMember := listenerAuthMemberPlan(cloud.ListenerExternalAuth{Members: wanted.Members, MemberSubnetID: wanted.MemberSubnetID}, pool.observedAuthPool)
	active = active && !waiting && nextMember == nil
	retain := func(listener listeners.Listener, current *octavia.ListenerOTLPTracing) bool {
		return active && securityErr == nil && telemetryListenerWanted(listener, wanted) && current != nil &&
			current.CollectorPoolID == pool.pool.ID && !unknownTelemetryReadback(listener, graph.tracing)
	}
	if result, changed, err := p.telemetryWithdrawal(ctx, graph, retain); changed {
		return result, err
	}
	if trustErr != nil {
		return cloud.GatewayGraphResult{}, trustErr
	}
	if securityErr != nil {
		return cloud.GatewayGraphResult{}, securityErr
	}
	if !slices.ContainsFunc(graph.listeners, func(listener listeners.Listener) bool { return telemetryListenerWanted(listener, wanted) }) {
		return p.telemetryPending("Waiting for an Octavia HTTP listener"), nil
	}

	if !active {
		if result, changed, err := p.retireTelemetryPools(ctx, graph, owned, name); changed {
			return result, err
		}
		return p.convergeTelemetryPool(ctx, graph, wanted, name, pool, tlsMatches, nextMember)
	}
	wire := octavia.ListenerOTLPTracing{CollectorPoolID: pool.pool.ID, ServiceName: wanted.ServiceName, SpanName: wanted.SpanName}
	observed := 0
	for _, frontend := range graph.listeners {
		if !telemetryListenerWanted(frontend, wanted) || frontend.ProvisioningStatus != "ACTIVE" {
			continue
		}
		current := graph.tracing[frontend.ID]
		if current.Value == nil || *current.Value != wire {
			return p.updateListenerTelemetry(ctx, graph, frontend, &wire)
		}
		observed++
	}
	if result, changed, err := p.retireTelemetryPools(ctx, graph, owned, name); changed {
		return result, err
	}
	if observed != len(wanted.Listeners) {
		return p.telemetryPending("Waiting for all HTTP listeners to confirm tracing"), nil
	}
	return telemetryResult(cloud.ReadyOutcome()), nil
}

func (p *Provider) convergeTelemetryPool(ctx context.Context, graph telemetryGraph, wanted *cloud.GatewayTelemetry, name string, pool telemetryPool, tlsMatches bool, nextMember *graphMutation) (cloud.GatewayGraphResult, error) {
	if pool.pool.ID == "" {
		return p.createTelemetryPool(ctx, graph, name, wanted)
	}
	if pool.pool.ProvisioningStatus == "ERROR" || pool.pool.ProvisioningStatus == "ACTIVE" &&
		(!pool.admin || !pool.pool.AdminStateUp || pool.h2c || !tlsMatches) {
		if result, changed, err := p.retireTelemetryPools(ctx, graph, map[string]telemetryPool{name: pool}, ""); changed {
			return result, err
		}
	} else if pool.pool.ProvisioningStatus == "ACTIVE" && nextMember != nil && !graph.unverified && !graph.refs[pool.pool.ID] {
		return p.changeTelemetryMember(ctx, graph, nextMember)
	}
	return p.telemetryPending("Waiting for the Octavia collector pool"), nil
}

func telemetryPoolTLSMatches(pool telemetryPool, wanted *cloud.BackendTLSValidation, verifiedRef, certificate string) bool {
	if wanted == nil {
		return !hasBackendTLSSettings(pool.pool, pool.tls) && !hasObservedBackendTLS(pool.pool, pool.tls)
	}
	return pool.tls.Complete() && pool.pool.TLSEnabled && pool.tls.Hostname == wanted.Hostname &&
		verifiedRef != "" && pool.pool.CATLSContainerRef == verifiedRef && pool.pool.TLSContainerRef == certificate && pool.pool.CRLContainerRef == "" &&
		!pool.tls.RequireH2 && slices.Contains(pool.pool.Tags, backendTLSRevisionTag(wanted)) &&
		!differentBackendTLSRevision(pool.pool.Tags, backendTLSRevisionTag(wanted)) &&
		backendTLSSubjectAltNamesMatch(wanted.SubjectAltNames, observedRouteGraph{pool: &pool.pool, backendTLS: pool.tls})
}

func (p *Provider) createTelemetryPool(ctx context.Context, graph telemetryGraph, name string, wanted *cloud.GatewayTelemetry) (cloud.GatewayGraphResult, error) {
	tags, err := graph.identity.GatewayTags(rolePool)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	request := octavia.CreatePoolOpts{CreateOpts: pools.CreateOpts{
		LoadbalancerID: graph.loadBalancer.ID, Name: name, Protocol: pools.ProtocolHTTP,
		LBMethod: pools.LBMethodRoundRobin, AdminStateUp: boolPointer(true), Tags: append(tags, telemetryPoolTag),
	}}
	if tls := wanted.BackendTLS; tls != nil {
		uid := graph.spec.Identity.GatewayUID
		revision, err := p.backendCA.Find(ctx, uid, tls.CAConfigMapUID, tls.CADigest, "")
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if revision.Reference() == "" {
			return p.applyTelemetryMutation(ctx, graph, "Publishing the collector trust anchor to Barbican", func() error {
				_, err := p.backendCA.Ensure(ctx, uid, tls.CAConfigMapUID, tls.CAPEM)
				return err
			})
		}
		request.TLSEnabled = true
		request.CATLSContainerRef = revision.Reference()
		request.TLSContainerRef = graph.spec.Gateway.BackendClientCertificate.Reference
		request.BackendTLSHostname = tls.Hostname
		request.BackendTLSSubjectAltNames = tls.SubjectAltNames
		request.Tags = backendTLSTags(request.Tags, true, backendTLSRevisionTag(tls), request.TLSContainerRef != "", len(tls.SubjectAltNames) != 0)
	}
	return p.applyTelemetryMutation(ctx, graph, "Creating the Octavia collector pool", func() error {
		_, err := p.octavia.CreatePool(ctx, request)
		return err
	})
}

func (p *Provider) changeTelemetryMember(ctx context.Context, graph telemetryGraph, step *graphMutation) (cloud.GatewayGraphResult, error) {
	if step.listenerFilters.createMember == nil {
		return p.applyTelemetryMutation(ctx, graph, "Removing an obsolete Octavia collector member", func() error {
			return p.executeGatewayDeletionStep(ctx, step.deletion)
		})
	}
	tags, err := graph.identity.GatewayTags(roleMember)
	if err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	change := step.listenerFilters
	return p.applyTelemetryMutation(ctx, graph, "Creating an Octavia collector member", func() error {
		_, err := p.octavia.CreateMember(ctx, change.poolID, pools.CreateMemberOpts{Address: change.createMember.Address,
			ProtocolPort: change.createMember.Port, SubnetID: change.memberSubnet, AdminStateUp: boolPointer(true),
			Weight: new(1), Tags: append(tags, telemetryPoolTag)})
		return err
	})
}

func (p *Provider) observeTelemetryPools(ctx context.Context, graph telemetryGraph) (map[string]telemetryPool, error) {
	inventory, err := p.listGatewayPoolInventory(ctx, graph.loadBalancer)
	if err != nil {
		return nil, err
	}
	result := map[string]telemetryPool{}
	for _, pool := range inventory.Items {
		if !isTelemetryPool(pool) {
			continue
		}
		if err := p.validatePoolParent(pool, graph.loadBalancer.ID, rolePool, graph.identity); err != nil {
			return nil, err
		}
		if pool.ProjectID != p.projectID || !strings.HasPrefix(pool.Name, telemetryPoolNamePrefix) || hasRouteOwnership(pool.Tags) ||
			result[pool.Name].pool.ID != "" || pool.MonitorID != "" || validateBoundPoolParent(pool, graph.loadBalancer.ID, cloud.ListenerProtocolHTTP) != nil {
			return nil, fmt.Errorf("%w: invalid Octavia collector pool inventory", cloud.ErrOwnershipConflict)
		}
		if slices.ContainsFunc(graph.listeners, func(frontend listeners.Listener) bool { return frontend.DefaultPoolID == pool.ID }) {
			return nil, fmt.Errorf("%w: the Octavia collector pool is a listener default", cloud.ErrOwnershipConflict)
		}
		for _, ref := range pool.Listeners {
			observation := graph.tracing[ref.ID]
			if observation.Value == nil || observation.Value.CollectorPoolID != pool.ID {
				return nil, fmt.Errorf("%w: the Octavia collector pool has an unproven listener reference", cloud.ErrOwnershipConflict)
			}
		}
		members, err := p.observeAuxiliaryHTTPPoolMembers(ctx, graph.identity, pool, telemetryPoolTag)
		if err != nil {
			return nil, err
		}
		tls := inventory.BackendTLS[pool.ID]
		result[pool.Name] = telemetryPool{observedAuthPool: observedAuthPool{pool: pool, members: members, admin: tls.AdminObserved},
			tls: tls, h2c: inventory.BackendH2C[pool.ID]}
	}
	return result, nil
}

func (p *Provider) retireTelemetryPools(ctx context.Context, graph telemetryGraph, owned map[string]telemetryPool, keep string) (cloud.GatewayGraphResult, bool, error) {
	names := make([]string, 0, len(owned))
	for name := range owned {
		if name != keep {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		pool := owned[name]
		if graph.unverified || graph.refs[pool.pool.ID] || len(pool.pool.Listeners) != 0 {
			continue
		}
		if step := listenerAuthPoolRemoval(pool.observedAuthPool); step != nil {
			result, err := p.applyTelemetryMutation(ctx, graph, "Retiring an unused Octavia collector pool", func() error {
				return p.executeGatewayDeletionStep(ctx, step.deletion)
			})
			return result, true, err
		}
	}
	if len(names) != 0 {
		return p.telemetryPending("Waiting to confirm that an Octavia collector pool is unused"), true, nil
	}
	return cloud.GatewayGraphResult{}, false, nil
}
