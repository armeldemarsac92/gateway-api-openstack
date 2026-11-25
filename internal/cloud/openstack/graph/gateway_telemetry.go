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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const (
	telemetryListenerTag    = "gw-api-listener-otlp-v1"
	telemetryAttributesTag  = "gw-api-listener-otlp-attributes-v1"
	telemetryPoolTag        = "gw-api-otlp-collector-pool-v1"
	telemetryPoolNamePrefix = "gateway-api-otlp-collector-"
	telemetryExportPoolTag  = "gw-api-listener-otlp-pool-"
	telemetryRetryTag       = "gw-api-listener-otlp-retry-"
	telemetryRetryInterval  = 15 * time.Second
	telemetryALPN           = "http/1.1"
)

type telemetryPool struct {
	observedAuthPool
	tls octavia.PoolBackendTLS
	h2c bool
}

// TelemetryBackendTLSAvailable checks the public contract used by the
// collector when reporting its BackendTLSPolicy ancestor.
func (p *Provider) TelemetryBackendTLSAvailable(ctx context.Context, requireSAN, systemCA bool) (bool, error) {
	if !p.backendTLSPolicy || !systemCA && p.backendCA == nil {
		return false, nil
	}
	required := []string{octavia.GatewayCapabilityListenerOTLPTracing}
	return p.amphoraCapabilityAvailable(ctx, append(required, telemetryBackendTLSCapabilities(requireSAN, systemCA)...)...)
}

func telemetryBackendTLSCapabilities(requireSAN, systemCA bool) []string {
	required := backendTLSCapabilities(false, requireSAN)
	if systemCA {
		required = append(required, octavia.GatewayCapabilityBackendTLSSystemCA)
	}
	return required
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
			validateBoundPoolParent(pool, lb.ID, cloud.ListenerProtocolHTTP) != nil || ids[pool.ID] {
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
	poolProof    map[string]bool
	listedPools  map[string]bool
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
		if spec.Gateway.TelemetryPolicyPresent {
			var err error
			ready.TelemetryUnsupported, ready.TelemetryCapabilityUnavailable, err = p.verifyTelemetryCapabilities(ctx, "amphora", spec.Gateway, wanted)
			if err != nil || ready.TelemetryUnsupported != "" {
				return ready, err
			}
		}
		if wanted != nil {
			return p.telemetryPending("Waiting for the Gateway load balancer"), nil
		}
		return ready, nil
	}
	balance, balanceErr := p.observeLoadBalancerOnce(ctx, discovered.ID)
	if balanceErr != nil && balance.phase != loadBalancerPhaseError {
		return ready, balanceErr
	}
	if balance.phase == loadBalancerPhaseAbsent {
		return p.telemetryPending("Waiting to observe the Gateway load balancer"), nil
	}
	lb := balance.loadBalancer
	if lb.ID != discovered.ID || !identity.MatchesGateway(discovered.Tags, roleLoadBalancer) ||
		!identity.MatchesGateway(lb.Tags, roleLoadBalancer) || p.validateLoadBalancerProject(lb.ID, lb.ProjectID) != nil {
		return ready, fmt.Errorf("%w: listener tracing could not prove the Gateway load balancer", cloud.ErrOwnershipConflict)
	}
	if balance.phase == loadBalancerPhasePending {
		return telemetryResult(balance.outcome), nil
	}
	inventory, err := p.octavia.ListTelemetryListenerInventory(ctx, listeners.ListOpts{LoadbalancerID: lb.ID, ProjectID: p.projectID})
	if err != nil && len(inventory.Items) == 0 {
		return ready, err
	}
	graph := telemetryGraph{identity: identity, spec: spec, loadBalancer: lb, tracing: inventory.OTLPTracing,
		refs: map[string]bool{}, poolProof: map[string]bool{}, listedPools: map[string]bool{}}
	graph.unverified = err != nil || !inventory.DefaultPoolsObserved ||
		crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }) != nil
	counts := make(map[string]int, len(inventory.Items))
	for _, listener := range inventory.Items {
		counts[listener.ID]++
	}
	for _, listener := range inventory.Items {
		current, present := graph.tracing[listener.ID]
		if current.Value != nil {
			graph.refs[current.Value.CollectorPoolID] = true
		} else if current.Status == "ERROR" || current.Status == "" && listener.ProvisioningStatus == "ERROR" && slices.Contains(listener.Tags, telemetryListenerTag) {
			// The running proxy can still use the previous collector. Even if
			// we cannot retry this listener, keep that collector intact.
			if id, valid := recordedTelemetryPool(listener.Tags); valid && id != "" {
				graph.refs[id] = true
			} else {
				graph.unverified = true
			}
		}
		unknownStatus := current.Status != "" && current.Status != "ACTIVE" && current.Status != "ERROR"
		graph.unverified = graph.unverified || present && (!current.Readable || current.StatusMalformed || unknownStatus)
		if !eligibleTelemetryListener(listener.Protocol) {
			graph.unverified = graph.unverified || listener.Protocol == "" || current.Value != nil
			continue
		}
		if listener.ID == "" || counts[listener.ID] != 1 || listener.ProjectID != p.projectID || len(listener.Loadbalancers) != 1 ||
			listener.Loadbalancers[0].ID != lb.ID || !identity.MatchesGateway(listener.Tags, roleListener) || hasRouteOwnership(listener.Tags) {
			graph.unverified = true
			continue
		}
		graph.listeners = append(graph.listeners, listener)
		if listener.DefaultPoolID != "" {
			graph.refs[listener.DefaultPoolID] = true
		}
	}
	slices.SortFunc(graph.listeners, func(a, b listeners.Listener) int { return cmp.Compare(a.ID, b.ID) })
	owned, poolErr := p.observeTelemetryPools(ctx, &graph)
	return p.planGatewayTelemetry(ctx, graph, wanted, owned, poolErr, balanceErr)
}

func recordedTelemetryPool(tags []string) (string, bool) {
	var recorded string
	for _, tag := range tags {
		if poolID, found := strings.CutPrefix(tag, telemetryExportPoolTag); found {
			if recorded != "" || poolID == "" {
				return "", false
			}
			recorded = poolID
		}
	}
	return recorded, true
}

func (graph telemetryGraph) ownsExport(frontend listeners.Listener, config *octavia.ListenerOTLPTracing) bool {
	recorded, valid := recordedTelemetryPool(frontend.Tags)
	if !valid || !slices.Contains(frontend.Tags, telemetryListenerTag) {
		return false
	}
	// Previous controllers supplied the general marker and an owned pool.
	// New writes also name the pool in the listener tags, allowing withdrawal
	// when the pool inventory is unavailable. A proven foreign pool wins.
	if recorded == "" {
		return config == nil || graph.poolProof[config.CollectorPoolID]
	}
	return (config == nil || recorded == config.CollectorPoolID) &&
		(graph.poolProof[recorded] || !graph.listedPools[recorded])
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

func (p *Provider) updateListenerTelemetry(ctx context.Context, graph telemetryGraph, frontend listeners.Listener, value *octavia.ListenerOTLPTracing, retry bool) (cloud.GatewayGraphResult, error) {
	return p.applyTelemetryMutation(ctx, graph, "Waiting for Octavia listener tracing readback", func() error {
		tags := slices.DeleteFunc(slices.Clone(frontend.Tags), func(tag string) bool {
			return strings.HasPrefix(tag, telemetryExportPoolTag) || strings.HasPrefix(tag, telemetryRetryTag)
		})
		if !slices.Contains(tags, telemetryListenerTag) {
			tags = append(tags, telemetryListenerTag)
		}
		if value != nil && len(value.Attributes) > 0 && !slices.Contains(tags, telemetryAttributesTag) {
			// Keep this until exporter withdrawal is acknowledged. After a failed
			// reload, the public profile can be narrower than the running one.
			tags = append(tags, telemetryAttributesTag)
		}
		recorded := value
		if recorded == nil {
			recorded = graph.tracing[frontend.ID].Value
		}
		if recorded != nil {
			tags = append(tags, telemetryExportPoolTag+recorded.CollectorPoolID)
		} else if poolID, valid := recordedTelemetryPool(frontend.Tags); valid && poolID != "" {
			tags = append(tags, telemetryExportPoolTag+poolID)
		}
		if retry {
			tags = append(tags, telemetryRetryTag+strconv.FormatInt(time.Now().Add(telemetryRetryInterval).Unix(), 10))
		}
		return p.octavia.SetListenerOTLPTracing(ctx, frontend.ID, value, tags)
	})
}

func telemetryRetryRemaining(tags []string, now time.Time) time.Duration {
	for _, tag := range tags {
		if raw, found := strings.CutPrefix(tag, telemetryRetryTag); found {
			until, err := strconv.ParseInt(raw, 10, 64)
			wait := time.Unix(until, 0).Sub(now)
			if err == nil && wait > 0 && wait <= telemetryRetryInterval {
				return wait
			}
		}
	}
	return 0
}

func (p *Provider) telemetryWithdrawal(ctx context.Context, graph telemetryGraph, retain func(listeners.Listener, *octavia.ListenerOTLPTracing) bool) (cloud.GatewayGraphResult, bool, error) {
	waiting := false
	var acknowledged *listeners.Listener
	for i := range graph.listeners {
		frontend := &graph.listeners[i]
		current, present := graph.tracing[frontend.ID]
		if graph.loadBalancer.ProvisioningStatus == "ERROR" && current.Value == nil && current.Status != "ERROR" {
			continue
		}
		cleared := current.Value == nil
		if !graph.ownsExport(*frontend, current.Value) {
			continue
		}
		var reduced *octavia.ListenerOTLPTracing
		if !cleared && retain != nil && retain(*frontend, current.Value) {
			if graph.loadBalancer.ProvisioningStatus != "ERROR" {
				continue
			}
			if current.Status == "PENDING_UPDATE" {
				waiting = true
				continue
			}
			wanted := graph.spec.Gateway.Telemetry.Attributes
			if slices.ContainsFunc(current.Value.Attributes, func(a cloud.TelemetryAttribute) bool { return !slices.Contains(wanted, a) }) {
				// Octavia accepts the current profile minus all attributes while the
				// balancer is in ERROR. Additions wait for ordinary reconciliation.
				profile := *current.Value
				profile.Attributes = nil
				reduced = &profile
			} else if current.Status != "ERROR" || len(current.Value.Attributes) != 0 || !slices.Contains(frontend.Tags, telemetryAttributesTag) {
				continue
			}
		}
		writable := frontend.ProvisioningStatus == "ACTIVE" || frontend.ProvisioningStatus == "ERROR"
		if !writable || cleared && present && (!current.Readable || current.StatusMalformed) {
			waiting = true
			continue
		}
		if cleared {
			status := current.Status
			if status == "" {
				// Earlier providers report only the listener status after a clear.
				status = frontend.ProvisioningStatus
			}
			switch status {
			case "ACTIVE":
				acknowledged = frontend
				continue
			case "ERROR":
				if telemetryRetryRemaining(frontend.Tags, time.Now()) > 0 {
					waiting = true
					continue
				}
			default:
				waiting = true
				continue
			}
		}
		if graph.loadBalancer.ProvisioningStatus == "ERROR" {
			// A withdrawal on an ERROR balancer must omit tags. The worker
			// retains the ownership tags and the collector reservation.
			result, err := p.applyTelemetryMutation(ctx, graph, "Retrying withdrawal on a failed Octavia load balancer", func() error {
				return p.octavia.SetListenerOTLPTracing(ctx, frontend.ID, reduced, nil)
			})
			result.Outcome.RequeueAfter = telemetryRetryInterval
			result.Gateway.Outcome.RequeueAfter = telemetryRetryInterval
			return result, true, err
		}
		result, err := p.updateListenerTelemetry(ctx, graph, *frontend, nil, cleared)
		return result, true, err
	}
	if acknowledged != nil {
		result, err := p.applyTelemetryMutation(ctx, graph, "Releasing the acknowledged listener exporter", func() error {
			tags := slices.DeleteFunc(slices.Clone(acknowledged.Tags), func(tag string) bool {
				return tag == telemetryListenerTag || tag == telemetryAttributesTag || strings.HasPrefix(tag, telemetryExportPoolTag) || strings.HasPrefix(tag, telemetryRetryTag)
			})
			_, err := p.octavia.UpdateListener(ctx, acknowledged.ID, listeners.UpdateOpts{Tags: &tags})
			return err
		})
		return result, true, err
	}
	if waiting {
		return p.telemetryPending("Waiting to withdraw Octavia listener tracing"), true, nil
	}
	return cloud.GatewayGraphResult{}, false, nil
}

func (p *Provider) planGatewayTelemetry(ctx context.Context, graph telemetryGraph, wanted *cloud.GatewayTelemetry, owned map[string]telemetryPool, poolErr, balanceErr error) (result cloud.GatewayGraphResult, retErr error) {
	var unsupported string
	var dependencyErr error
	unavailable := false
	defer func() {
		result.TelemetryUnsupported = unsupported
		result.TelemetryCapabilityUnavailable = unavailable
	}()
	if wanted != nil || graph.spec.Gateway.TelemetryPolicyPresent {
		unsupported, unavailable, dependencyErr = p.verifyTelemetryCapabilities(ctx, graph.loadBalancer.Provider, graph.spec.Gateway, wanted)
	}
	eligible := wanted != nil && unsupported == "" && dependencyErr == nil && poolErr == nil
	var retain func(listeners.Listener, *octavia.ListenerOTLPTracing) bool
	if eligible {
		certificate := graph.spec.Gateway.BackendClientCertificate.Reference
		possible := owned[telemetryPoolName(wanted, certificate)]
		retain = func(listener listeners.Listener, current *octavia.ListenerOTLPTracing) bool {
			return graph.tracing[listener.ID].Readable && telemetryListenerWanted(listener, wanted) && current.CollectorPoolID == possible.pool.ID &&
				(!telemetryUsesSystemCA(wanted) || telemetryPoolTLSMatches(possible, wanted.BackendTLS, "", certificate))
		}
	}
	if result, changed, err := p.telemetryWithdrawal(ctx, graph, retain); changed {
		return result, err
	}
	if balanceErr != nil {
		return cloud.GatewayGraphResult{}, balanceErr
	}
	if !eligible {
		if poolErr != nil {
			return cloud.GatewayGraphResult{}, poolErr
		}
		owned, err := p.observeTelemetryPoolChildren(ctx, graph, owned)
		if err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		if result, changed, err := p.retireTelemetryPools(ctx, graph, owned, ""); changed {
			return result, err
		}
		return telemetryResult(cloud.ReadyOutcome()), dependencyErr
	}
	owned, err := p.observeTelemetryPoolChildren(ctx, graph, owned)
	if err != nil {
		if result, changed, withdrawalErr := p.telemetryWithdrawal(ctx, graph, nil); changed {
			return result, withdrawalErr
		}
		return cloud.GatewayGraphResult{}, err
	}
	return p.convergeGatewayTelemetry(ctx, graph, owned, wanted)
}

func (p *Provider) verifyTelemetryCapabilities(ctx context.Context, provider string, gateway cloud.GatewayGraphNode, wanted *cloud.GatewayTelemetry) (string, bool, error) {
	required := []string{octavia.GatewayCapabilityListenerOTLPTracing}
	if wanted != nil && wanted.Sampling != nil {
		required = append(required, octavia.GatewayCapabilityListenerOTLPSampling)
	}
	if wanted != nil && len(wanted.Attributes) > 0 {
		required = append(required, octavia.GatewayCapabilityListenerOTLPSpanAttributes)
	}
	var missingTrust string
	if wanted != nil && wanted.BackendTLS != nil {
		tls := wanted.BackendTLS
		systemCA := telemetryUsesSystemCA(wanted)
		switch {
		case !p.backendTLSPolicy || tls.Denied || !validBackendTLS(tls) || gateway.BackendClientCertificate.Denied:
			missingTrust = "The collector requires an authorized backend TLS profile"
		case !systemCA && (p.backendCA == nil || !gateway.BackendCAOwned && gateway.ClaimBackendCA == nil):
			missingTrust = "The collector requires an authorized Gateway CA store"
		default:
			required = append(required, telemetryBackendTLSCapabilities(len(tls.SubjectAltNames) != 0, systemCA)...)
		}
	}
	err := p.octavia.VerifyGatewayCapabilities(ctx, provider, required...)
	var missing *octavia.MissingGatewayCapability
	switch {
	case errors.As(err, &missing):
		return "Regional Octavia does not advertise " + missing.Capability, missing.Capability == octavia.GatewayCapabilityListenerOTLPTracing, nil
	case gophercloud.ResponseCodeIs(err, http.StatusNotFound):
		return "Regional Octavia does not advertise listener tracing", true, nil
	case err != nil:
		return "", false, err
	case missingTrust != "":
		return missingTrust, false, nil
	default:
		return "", false, nil
	}
}

func telemetryUsesSystemCA(wanted *cloud.GatewayTelemetry) bool {
	return wanted.BackendTLS != nil && wanted.BackendTLS.Trust == cloud.BackendTLSTrustSystem
}

func (p *Provider) convergeGatewayTelemetry(ctx context.Context, graph telemetryGraph, owned map[string]telemetryPool, wanted *cloud.GatewayTelemetry) (cloud.GatewayGraphResult, error) {
	name := telemetryPoolName(wanted, graph.spec.Gateway.BackendClientCertificate.Reference)
	pool := owned[name]
	backend := cloud.RouteSpec{Identity: graph.spec.Identity, Members: wanted.Members, MemberSubnetID: wanted.MemberSubnetID,
		BackendSecurityGroupIDs: wanted.BackendSecurityGroupIDs, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: cloud.ListenerProtocolHTTP}}
	securityErr := p.topology.ValidateGatewaySecurity(ctx, cloud.GatewaySpec{}, []cloud.RouteSpec{backend})
	var verifiedRef string
	var trustErr error
	if tls := wanted.BackendTLS; tls != nil && tls.Trust == cloud.BackendTLSTrustReference && pool.pool.ID != "" && graph.spec.Gateway.BackendCAOwned {
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
			current.CollectorPoolID == pool.pool.ID
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

	if tls := wanted.BackendTLS; tls != nil && tls.Trust == cloud.BackendTLSTrustReference && !graph.spec.Gateway.BackendCAOwned {
		return p.applyTelemetryMutation(ctx, graph, "Reserving ownership of the collector trust anchor", func() error {
			return graph.spec.Gateway.ClaimBackendCA(ctx)
		})
	}
	if !active {
		if result, changed, err := p.retireTelemetryPools(ctx, graph, owned, name); changed {
			return result, err
		}
		return p.convergeTelemetryPool(ctx, graph, wanted, name, pool, tlsMatches, nextMember)
	}
	// A pending neighbor can leave the inventory incomplete even when
	// this pool and its existing exporters are independently verified.
	if telemetryUsesSystemCA(wanted) && graph.unverified {
		return p.telemetryPending("Waiting to verify the listener inventory before programming tracing"), nil
	}
	wire := octavia.ListenerOTLPTracing{CollectorPoolID: pool.pool.ID, ServiceName: wanted.ServiceName, SpanName: wanted.SpanName,
		Sampling: wanted.Sampling, Attributes: wanted.Attributes}
	observed := 0
	var retryAfter time.Duration
	for _, frontend := range graph.listeners {
		if !telemetryListenerWanted(frontend, wanted) {
			continue
		}
		current, present := graph.tracing[frontend.ID]
		if present && (!current.Readable || current.StatusMalformed) || current.Status == "PENDING_UPDATE" ||
			current.Value != nil && !graph.ownsExport(frontend, current.Value) {
			continue
		}
		if current.Status != "ACTIVE" && current.Status != "ERROR" && (current.Value != nil || current.Status != "") {
			continue
		}
		canWrite := frontend.ProvisioningStatus == "ACTIVE" || frontend.ProvisioningStatus == "ERROR"
		if !reflect.DeepEqual(current.Value, &wire) {
			if !canWrite {
				continue
			}
			return p.updateListenerTelemetry(ctx, graph, frontend, &wire, false)
		}
		switch current.Status {
		case "ACTIVE":
			observed++
		case "ERROR":
			wait := telemetryRetryRemaining(frontend.Tags, time.Now())
			if wait == 0 && canWrite {
				return p.updateListenerTelemetry(ctx, graph, frontend, &wire, true)
			}
			if wait > 0 && (retryAfter == 0 || wait < retryAfter) {
				retryAfter = wait
			}
		}
	}
	if retryAfter > 0 {
		return telemetryResult(cloud.ProgressingOutcome("Waiting to retry the Octavia listener exporter", retryAfter)), nil
	}

	if result, changed, err := p.retireTelemetryPools(ctx, graph, owned, name); changed {
		return result, err
	}
	if graph.unverified || observed != len(wanted.Listeners) {
		return p.telemetryPending("Waiting for all HTTP listeners to confirm tracing"), nil
	}
	return telemetryResult(cloud.ReadyOutcome()), nil
}

func (p *Provider) convergeTelemetryPool(ctx context.Context, graph telemetryGraph, wanted *cloud.GatewayTelemetry, name string, pool telemetryPool, tlsMatches bool, nextMember *graphMutation) (cloud.GatewayGraphResult, error) {
	if pool.pool.ID == "" {
		return p.createTelemetryPool(ctx, graph, name, wanted)
	}
	status := pool.pool.ProvisioningStatus
	waitingForAttestation := status == "ERROR" || status == "ACTIVE" && !tlsMatches
	if telemetryUsesSystemCA(wanted) && waitingForAttestation && pool.pool.AdminStateUp && len(pool.members) == 0 && telemetryPoolTLSConfigured(pool, wanted.BackendTLS, "", graph.spec.Gateway.BackendClientCertificate.Reference) {
		// The worker can mark this empty pool ERROR when a peer's bundle
		// diverges. Retry the same profile without reallocating the pool.
		if wait := telemetryRetryRemaining(pool.pool.Tags, time.Now()); wait > 0 {
			return telemetryResult(cloud.ProgressingOutcome("Waiting for Octavia to attest the collector system CA", wait)), nil
		}
		if graph.unverified || graph.refs[pool.pool.ID] || pool.pool.Listeners == nil {
			return p.telemetryPending("Waiting for Octavia to attest the collector system CA"), nil
		}
		return p.applyTelemetryMutation(ctx, graph, "Retrying the Octavia collector system CA attestation", func() error {
			tags := withTelemetryRetryTag(pool.pool.Tags)
			_, err := p.octavia.UpdatePool(ctx, pool.pool.ID, pools.UpdateOpts{Tags: &tags})
			return err
		})
	}
	if status == "ERROR" || status == "ACTIVE" && (!pool.admin || !pool.pool.AdminStateUp || pool.h2c || !tlsMatches) {
		if result, changed, err := p.retireTelemetryPools(ctx, graph, map[string]telemetryPool{name: pool}, ""); changed {
			return result, err
		}
	} else if status == "ACTIVE" && nextMember != nil && !graph.unverified && !graph.refs[pool.pool.ID] {
		return p.changeTelemetryMember(ctx, graph, nextMember)
	}
	return p.telemetryPending("Waiting for the Octavia collector pool"), nil
}

func telemetryPoolTLSMatches(pool telemetryPool, wanted *cloud.BackendTLSValidation, verifiedRef, certificate string) bool {
	if wanted == nil {
		return !hasBackendTLSSettings(pool.pool, pool.tls) && !hasObservedBackendTLS(pool.pool, pool.tls)
	}
	return telemetryPoolTLSConfigured(pool, wanted, verifiedRef, certificate) &&
		(wanted.Trust != cloud.BackendTLSTrustSystem || pool.tls.SystemCAApplied())
}

func telemetryPoolTLSConfigured(pool telemetryPool, wanted *cloud.BackendTLSValidation, verifiedRef, certificate string) bool {
	trustMatches := verifiedRef != "" && pool.pool.CATLSContainerRef == verifiedRef && !hasUnclearedSystemCA(pool.pool, pool.tls)
	if wanted.Trust == cloud.BackendTLSTrustSystem {
		trustMatches = configuredSystemCA(pool.pool, pool.tls) && slices.Contains(pool.pool.Tags, backendTLSSystemCATag) &&
			pool.tls.H2CObserved && pool.tls.RequireH2Observed && pool.tls.ALPNObserved
	}
	return trustMatches && pool.tls.Complete() && pool.pool.TLSEnabled && !pool.h2c && pool.tls.Hostname == wanted.Hostname &&
		slices.Equal(pool.pool.ALPNProtocols, []string{telemetryALPN}) &&
		pool.pool.TLSContainerRef == certificate && pool.pool.CRLContainerRef == "" &&
		!pool.tls.RequireH2 && slices.Contains(pool.pool.Tags, backendTLSRevisionTag(wanted)) &&
		!differentBackendTLSRevision(pool.pool.Tags, backendTLSRevisionTag(wanted)) &&
		backendTLSSubjectAltNamesMatch(wanted.SubjectAltNames, observedRouteGraph{pool: &pool.pool, backendTLS: pool.tls})
}

func withTelemetryRetryTag(tags []string) []string {
	tags = slices.DeleteFunc(slices.Clone(tags), func(tag string) bool { return strings.HasPrefix(tag, telemetryRetryTag) })
	return append(tags, telemetryRetryTag+strconv.FormatInt(time.Now().Add(telemetryRetryInterval).Unix(), 10))
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
		if tls.Trust == cloud.BackendTLSTrustReference {
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
			request.CATLSContainerRef = revision.Reference()
		} else {
			request.BackendTLSSystemCA = true
			request.Tags = withTelemetryRetryTag(append(request.Tags, backendTLSSystemCATag))
		}
		request.TLSEnabled = true
		request.ALPNProtocols = []string{telemetryALPN}
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

func (p *Provider) observeTelemetryPools(ctx context.Context, graph *telemetryGraph) (map[string]telemetryPool, error) {
	lb := graph.loadBalancer
	inventory, err := p.octavia.ListPoolInventory(ctx, pools.ListOpts{LoadbalancerID: lb.ID, ProjectID: p.projectID})
	if err != nil {
		return nil, err
	}
	inventoryErr := crossCheckCompleteChildInventory("load balancer", lb.ID, "pool", lb.Pools, inventory.Items, func(pool pools.Pool) string { return pool.ID })
	counts := make(map[string]int, len(inventory.Items))
	for _, pool := range inventory.Items {
		graph.listedPools[pool.ID] = true
		counts[pool.ID]++
	}
	result := map[string]telemetryPool{}
	for _, pool := range inventory.Items {
		if !isTelemetryPool(pool) {
			continue
		}
		if counts[pool.ID] != 1 || !graph.identity.MatchesGateway(pool.Tags, rolePool) || pool.ProjectID != p.projectID ||
			hasRouteOwnership(pool.Tags) || validateBoundPoolParent(pool, lb.ID, cloud.ListenerProtocolHTTP) != nil {
			inventoryErr = errors.Join(inventoryErr, fmt.Errorf("%w: invalid Octavia collector pool ownership", cloud.ErrOwnershipConflict))
			continue
		}
		graph.poolProof[pool.ID] = true
		if result[pool.Name].pool.ID != "" || pool.MonitorID != "" ||
			slices.ContainsFunc(graph.listeners, func(frontend listeners.Listener) bool { return frontend.DefaultPoolID == pool.ID }) {
			inventoryErr = errors.Join(inventoryErr, fmt.Errorf("%w: invalid Octavia collector pool attachments", cloud.ErrOwnershipConflict))
			continue
		}
		for _, ref := range pool.Listeners {
			graph.refs[pool.ID] = true
			observation := graph.tracing[ref.ID]
			if observation.Value == nil || observation.Value.CollectorPoolID != pool.ID {
				graph.unverified = true
			}
		}
		tls := inventory.BackendTLS[pool.ID]
		result[pool.Name] = telemetryPool{observedAuthPool: observedAuthPool{pool: pool, admin: tls.AdminObserved},
			tls: tls, h2c: inventory.BackendH2C[pool.ID]}
	}
	return result, inventoryErr
}

func (p *Provider) observeTelemetryPoolChildren(ctx context.Context, graph telemetryGraph, owned map[string]telemetryPool) (map[string]telemetryPool, error) {
	for name, observation := range owned {
		members, err := p.observeAuxiliaryHTTPPoolMembers(ctx, graph.identity, observation.pool, telemetryPoolTag)
		if err != nil {
			return nil, err
		}
		observation.members = members
		owned[name] = observation
	}
	return owned, nil
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
		if graph.unverified || graph.refs[pool.pool.ID] || pool.pool.Listeners == nil || len(pool.pool.Listeners) != 0 {
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
