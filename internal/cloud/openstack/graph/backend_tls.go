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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const backendTLSManagedTag = "gateway-api-openstack.backend-tls=v1"
const backendTLSSystemCATag = "gateway-api-openstack.backend-tls-system-ca=v1"
const backendTLSRevisionTagPrefix = "gateway-api-openstack.backend-tls-ca="
const backendTLSClientTag = "gateway-api-openstack.backend-tls-client=v1"
const xBackendTLSClientTag = "gateway-api-openstack.xbackend-tls-client=v1"
const (
	xBackendServerALPN = "http/1.1"
	xBackendHTTP2ALPN  = "h2"
)

// backendTLSSANTag also identifies pools that previously used explicit SANs.
const backendTLSSANTag = "gateway-api-openstack.backend-tls-san=v1"

type backendTLSPhase uint8

const (
	backendTLSUnmanaged backendTLSPhase = iota
	backendTLSDenied
	backendTLSBlocked
	backendTLSClaim
	backendTLSPublish
	backendTLSConfigure
	backendTLSAwaitSystemCA
	backendTLSVerified
	backendTLSRelease
)

type backendTLSPlan struct {
	phase                  backendTLSPhase
	settings               octavia.BackendTLSSettings
	issue                  string
	clearClientCertificate bool
	retainServingOnly      bool
}

type backendTLSPart struct {
	index             int
	key               string
	desired           *desiredRoute
	observed          observedRouteGraph
	clientCertificate cloud.BackendClientCertificate
	retiring          bool
}

func (plan backendTLSPlan) denies() bool {
	return plan.phase == backendTLSDenied || plan.phase == backendTLSBlocked
}

func (plan backendTLSPlan) allowsMembers() bool {
	return plan.phase == backendTLSUnmanaged || plan.phase == backendTLSVerified
}

func (plan backendTLSPlan) stagesSystemCA() bool {
	return plan.phase == backendTLSAwaitSystemCA
}

func hasObservedBackendTLS(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return slices.Contains(pool.Tags, backendTLSManagedTag) || tls.Hostname != "" || tls.RequireH2 || hasUnclearedSystemCA(pool, tls) || hasUnclearedBackendTLSSubjectAltNames(pool, tls)
}

func (r desiredRoute) requiresBackendTLSH2() bool {
	return r.spec.GRPC || r.xBackend && r.spec.BackendTLS != nil && r.spec.BackendTLS.RequireH2
}

func (r desiredRoute) xBackendALPN() string {
	if r.requiresBackendTLSH2() {
		return xBackendHTTP2ALPN
	}
	return xBackendServerALPN
}

func backendTLSReadbackComplete(desired desiredRoute, observed observedRouteGraph) bool {
	// An existing h2 XBackend also needs the flag readback when changing
	// protocols, so an omitted field cannot conceal an uncleared requirement.
	needsH2Readback := desired.requiresBackendTLSH2() ||
		desired.xBackend && slices.Contains(observed.pool.ALPNProtocols, xBackendHTTP2ALPN)
	return observed.backendTLS.Complete() && (!needsH2Readback || observed.backendTLS.RequireH2Observed) &&
		(!desired.xBackend || !observed.pool.TLSEnabled || observed.backendTLS.ALPNObserved)
}

func backendTLSProtocolMatches(desired desiredRoute, observed observedRouteGraph) bool {
	if desired.xBackend {
		return observed.backendTLS.RequireH2 == desired.requiresBackendTLSH2() &&
			slices.Equal(observed.pool.ALPNProtocols, []string{desired.xBackendALPN()})
	}
	return !desired.requiresBackendTLSH2() || observed.backendTLS.RequireH2
}

func backendTLSCapabilities(requireH2, requireSAN bool) []string {
	required := []string{octavia.GatewayCapabilityBackendTLSHostname}
	if requireH2 {
		required = append(required, octavia.GatewayCapabilityBackendTLSH2ALPN)
	}
	if requireSAN {
		required = append(required, octavia.GatewayCapabilityBackendTLSSubjectAltNames)
	}
	return required
}

func hasUnclearedBackendTLSSubjectAltNames(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.SubjectAltNamesInvalid || len(tls.SubjectAltNames) != 0 ||
		!tls.SubjectAltNamesObserved && slices.Contains(pool.Tags, backendTLSSANTag)
}

func backendTLSSubjectAltNamesMatch(names []cloud.BackendTLSSubjectAltName, observed observedRouteGraph) bool {
	if len(names) == 0 {
		// Historical pools can omit the field. Once enrolled, an explicit
		// empty list is the only proof that Octavia cleared the identities.
		return !hasUnclearedBackendTLSSubjectAltNames(*observed.pool, observed.backendTLS)
	}
	return !observed.backendTLS.SubjectAltNamesInvalid && slices.Equal(names, observed.backendTLS.SubjectAltNames)
}

func hasUnclearedSystemCA(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.SystemCA || tls.SystemCAInvalid || tls.SystemCASHA256 != "" ||
		slices.Contains(pool.Tags, backendTLSSystemCATag) && (!tls.SystemCAObserved || !tls.SystemCADigestObserved)
}

func configuredSystemCA(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.SystemCAObserved && tls.SystemCA && pool.CATLSContainerRef == "" && pool.CRLContainerRef == ""
}

func hasBackendTLSSettings(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.Hostname != "" || tls.RequireH2 || pool.TLSEnabled || pool.CATLSContainerRef != "" ||
		pool.TLSContainerRef != "" || pool.CRLContainerRef != "" || hasUnclearedSystemCA(pool, tls) || hasUnclearedBackendTLSSubjectAltNames(pool, tls)
}

func xBackendPoolWithoutTLS(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.AdminObserved && tls.EnabledObserved && tls.CAObserved && tls.ClientRefObserved &&
		!hasBackendTLSSettings(pool, tls) && len(pool.ALPNProtocols) == 0 &&
		!slices.ContainsFunc(pool.Tags, func(tag string) bool {
			return tag == backendTLSClientTag || tag == xBackendTLSClientTag || tag == backendTLSSANTag || strings.HasPrefix(tag, backendTLSRevisionTagPrefix)
		})
}

func xBackendTLSCleared(observed observedRouteGraph) bool {
	pool, tls := observed.pool, observed.backendTLS
	return pool != nil && pool.ProvisioningStatus == "ACTIVE" && len(observed.members) == 0 &&
		tls.Complete() && tls.ALPNObserved && len(pool.ALPNProtocols) == 0 && !hasBackendTLSSettings(*pool, tls)
}

func routeHasBackendTLSPolicy(route desiredRoute) bool {
	return route.spec.BackendTLS != nil || slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
		return part.spec.BackendTLS != nil
	})
}

// prepareBackendTLS runs before ordinary writes. Quarantine takes priority
// over dependencies such as Floating IPs and security-group validation.
func (p *Provider) prepareBackendTLS(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return nil
	}
	return p.planBackendTLS(ctx, desired, actual)
}

// planBackendTLS classifies owned pools using readback alone, so it can also
// verify a serving subset while another Octavia operation is pending.
func (p *Provider) planBackendTLS(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if actual.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure ||
		len(actual.routes) != len(desired.routes) {
		return nil
	}
	parts := backendTLSParts(desired, actual.routes)
	early := firstBackendTLSWithdrawal(parts, func(part backendTLSPart) backendTLSPlan {
		return part.preflight()
	})
	if early != nil && !early.route.retry {
		return early
	}
	type requirements struct{ requireH2, requireSAN, systemCA bool }
	capabilities := make(map[requirements]bool)
	capable := func(requireH2, requireSAN, systemCA bool) bool {
		key := requirements{requireH2: requireH2, requireSAN: requireSAN, systemCA: systemCA}
		supported, checked := capabilities[key]
		if !checked {
			required := backendTLSCapabilities(requireH2, requireSAN)
			if systemCA {
				required = append(required, octavia.GatewayCapabilityBackendTLSSystemCA)
			}
			supported = p.backendTLSPolicy &&
				p.verifyGatewayHTTPCapabilities(ctx, *desired, actual, required) == nil
			capabilities[key] = supported
		}
		return supported
	}
	frontends := scopedHTTPListeners(*desired)
	step := firstBackendTLSWithdrawal(parts, func(part backendTLSPart) backendTLSPlan {
		if part.retiring {
			return part.preflight()
		}
		if part.desired == nil {
			// Keep the old pool and its durable marker until a live policy
			// inventory authorizes the replacement representation.
			if pool := part.observed.pool; pool != nil && hasObservedBackendTLS(*pool, part.observed.backendTLS) &&
				(!desired.routes[part.index].spec.BackendTLSObserved || !capable(false, false, false) ||
					pool.CATLSContainerRef != "" && p.backendCA == nil) {
				desired.spec.Routes[part.index].Disposition = cloud.GraphPreserve
				desired.routes[part.index].backendTLS = backendTLSPlan{phase: backendTLSBlocked, issue: "Waiting for a current backend TLS policy inventory before replacing the route pools"}
			}
			return backendTLSPlan{phase: backendTLSDenied}
		}
		if listener, configured := frontends[scopedHTTPRoutePort(*part.desired)]; configured && !hasAuthorizedListener(listener) {
			part.desired.backendTLS = backendTLSPlan{phase: backendTLSBlocked, issue: "Waiting for the authorized HTTP listener"}
			return part.desired.backendTLS
		}
		systemCA := part.desired.spec.BackendTLS != nil && part.desired.spec.BackendTLS.Trust == cloud.BackendTLSTrustSystem
		part.desired.backendTLS = p.observeBackendTLS(ctx, *part.desired, part.observed, func(requireH2, requireSAN bool) bool {
			if part.desired.spec.BackendTLS == nil && part.observed.pool != nil &&
				part.observed.pool.CATLSContainerRef != "" && p.backendCA == nil {
				return false
			}
			return capable(requireH2, requireSAN, systemCA)
		}, desired.spec.Gateway.BackendCAOwned, part.clientCertificate)
		return part.desired.backendTLS
	})
	if step != nil && (!step.route.retry || early == nil) {
		return step
	}
	return early
}

func backendTLSParts(desired *desiredGatewayGraph, routes []observedRouteGraph) []backendTLSPart {
	var parts []backendTLSPart
	for index := range desired.routes {
		retiring := desired.spec.Routes[index].Disposition != cloud.GraphEnsure
		route := &desired.routes[index]
		observed := routes[index]
		hasPolicy := routeHasBackendTLSPolicy(*route)
		add := func(part *desiredRoute, key string, actualPart observedRouteGraph) {
			if part == nil && hasPolicy && actualPart.pool != nil || part != nil && part.spec.BackendTLS != nil ||
				actualPart.pool != nil && hasObservedBackendTLS(*actualPart.pool, actualPart.backendTLS) {
				clientCertificate := desired.spec.Gateway.BackendClientCertificate
				if part != nil && part.xBackend {
					clientCertificate = cloud.BackendClientCertificate{Reference: part.xBackendClientCertificateReference}
				} else if actualPart.pool != nil && slices.Contains(actualPart.pool.Tags, xBackendTLSClientTag) {
					// Retiring XBackend pools cannot keep an identity through
					// a Gateway-wide enrollment for another backend.
					clientCertificate = cloud.BackendClientCertificate{}
				}
				parts = append(parts, backendTLSPart{index: index, key: key, desired: part, observed: actualPart,
					retiring: retiring, clientCertificate: clientCertificate})
			}
		}
		if !retiring && (len(route.httpParts) == 0 || len(route.policies) != 0) {
			add(route, "", observed)
		} else {
			add(nil, "", observed)
		}
		for i := range route.httpParts {
			part := &route.httpParts[i]
			var actualPart observedRouteGraph
			if previous := observed.httpParts[part.backendKey]; previous != nil {
				actualPart = *previous
			}
			add(part, part.backendKey, actualPart)
		}
		// Moving between a scalar and grouped route can leave the retiring
		// representation serving an old predicate until policy handover.
		keys := slices.Sorted(maps.Keys(observed.httpParts))
		for _, key := range keys {
			if !slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool { return part.backendKey == key }) {
				add(nil, key, *observed.httpParts[key])
			}
		}
	}
	return parts
}

func (part backendTLSPart) preflight() backendTLSPlan {
	pool := part.observed.pool
	if pool == nil {
		return backendTLSPlan{}
	}
	managed := slices.Contains(pool.Tags, backendTLSManagedTag)
	staleClient := part.clientCertificate.Denied || pool.TLSContainerRef != part.clientCertificate.Reference ||
		!part.observed.backendTLS.ClientRefObserved && slices.Contains(pool.Tags, xBackendTLSClientTag)
	clearClient := managed && staleClient
	if part.retiring {
		if managed && (staleClient || !part.observed.backendTLS.ClientRefObserved && slices.Contains(pool.Tags, backendTLSClientTag)) {
			return backendTLSPlan{phase: backendTLSDenied, clearClientCertificate: true}
		}
		return backendTLSPlan{}
	}
	if part.clientCertificate.Denied && part.desired != nil && part.desired.spec.BackendTLS != nil {
		return backendTLSPlan{phase: backendTLSBlocked, clearClientCertificate: clearClient}
	}
	if part.desired == nil || part.desired.spec.BackendTLS == nil || part.desired.spec.BackendTLS.Denied {
		return backendTLSPlan{phase: backendTLSDenied, clearClientCertificate: clearClient}
	}
	tls := part.desired.spec.BackendTLS
	observed := part.observed.backendTLS
	if pool.ProvisioningStatus != "ACTIVE" || !pool.AdminStateUp && len(part.observed.members) != 0 ||
		!validBackendTLS(tls) || !backendTLSReadbackComplete(*part.desired, part.observed) || observed.Hostname != tls.Hostname ||
		!backendTLSSubjectAltNamesMatch(tls.SubjectAltNames, part.observed) || !backendTLSProtocolMatches(*part.desired, part.observed) ||
		!pool.TLSEnabled || part.observed.backendH2C || part.desired.spec.BackendH2C ||
		pool.TLSContainerRef != part.clientCertificate.Reference || pool.CRLContainerRef != "" || differentBackendTLSRevision(pool.Tags, backendTLSRevisionTag(tls)) {
		return backendTLSPlan{phase: backendTLSConfigure}
	}
	if tls.Trust == cloud.BackendTLSTrustSystem {
		if !configuredSystemCA(*pool, observed) || !observed.SystemCAApplied() && len(part.observed.members) != 0 {
			return backendTLSPlan{phase: backendTLSConfigure}
		}
	} else if pool.CATLSContainerRef == "" || hasUnclearedSystemCA(*pool, observed) {
		return backendTLSPlan{phase: backendTLSConfigure}
	}
	return backendTLSPlan{}
}

func firstBackendTLSWithdrawal(parts []backendTLSPart, plan func(backendTLSPart) backendTLSPlan) *graphMutation {
	var first *graphMutation
	for _, part := range parts {
		if pool := part.observed.pool; pool != nil && pool.TLSContainerRef != "" && !slices.Contains(pool.Tags, backendTLSManagedTag) &&
			(part.desired == nil || part.desired.spec.BackendTLS == nil) {
			continue
		}
		protect := part.desired == nil || part.desired.spec.BackendTLS != nil
		requiresClosedPool := part.desired != nil && protect && (part.desired.spec.GRPC ||
			part.desired.spec.BackendTLS != nil && (len(part.desired.spec.BackendTLS.SubjectAltNames) != 0 ||
				part.desired.spec.BackendTLS.Trust == cloud.BackendTLSTrustSystem))
		tlsPlan := plan(part)
		var step *routeMutation
		if part.desired != nil && part.desired.xBackend && !part.retiring {
			if part.desired.xBackendPlaintext && xBackendTLSCleared(part.observed) {
				// Release may reopen an observed empty plaintext pool before
				// removing its durable protection marks. Do not close it again.
				continue
			}
			step = tlsPlan.xBackendWithdrawal(part.observed, part.clientCertificate.Reference != "")
		} else {
			step = tlsPlan.withdrawal(protect, requiresClosedPool, part.observed)
		}
		// An unsupported protected pool can block member drains anywhere on
		// the load balancer. Disable unsafe protected pools first.
		if step != nil && (first == nil || first.route.retry && !step.retry ||
			first.route.retry == step.retry && first.route.poolAdmin == nil && step.poolAdmin != nil) {
			first = &graphMutation{kind: graphRouteMutation, routeIndex: part.index, backendKey: part.key,
				route: *step, withdrawHTTPPolicy: true, message: step.message()}
			if step.poolAdmin != nil && !step.retry {
				return first
			}
		}
	}
	return first
}

func (plan backendTLSPlan) xBackendWithdrawal(observed observedRouteGraph, wantsClientCertificate bool) *routeMutation {
	pool := observed.pool
	if pool == nil || plan.allowsMembers() {
		return nil
	}
	if plan.stagesSystemCA() && len(observed.members) == 0 && pool.ProvisioningStatus == "ACTIVE" {
		return nil
	}
	if hasUnclearedSystemCA(*pool, observed.backendTLS) || wantsClientCertificate && len(observed.members) != 0 ||
		pool.TLSContainerRef != "" || slices.Contains(pool.Tags, xBackendTLSClientTag) ||
		observed.backendTLS.RequireH2 || slices.Contains(pool.Tags, backendTLSManagedTag) && slices.Contains(pool.ALPNProtocols, xBackendHTTP2ALPN) {
		// Close before draining a client identity or reloading an enrolled
		// HTTP/2 pool. The authenticated renderer keeps the match reserved.
		return plan.withdrawal(true, true, observed)
	}
	switch pool.ProvisioningStatus {
	case "ACTIVE":
		// Stock Octavia drops policies targeting disabled pools. Drain
		// while the match still selects this pool, before any TLS staging.
		// An empty plaintext fallback stays enabled until TLS can be staged.
		if len(observed.members) != 0 {
			return &routeMutation{kind: routeMutationClearPoolMembers, id: pool.ID}
		}
		if plan.clearClientCertificate && !pool.AdminStateUp {
			return plan.withdrawal(true, false, observed)
		}
	case "ERROR":
		// Member batches require an ACTIVE pool. Recover an enrolled pool
		// with an admin-only PUT. Mark a previously plain pool only once
		// regional TLS support is confirmed, so the renderer retains its
		// match while closed.
		if hasObservedBackendTLS(*pool, observed.backendTLS) || plan.phase == backendTLSClaim ||
			plan.phase == backendTLSPublish || plan.settings.Hostname != "" {
			return plan.withdrawal(true, true, observed)
		}
	}
	return nil
}

func (p *Provider) observeBackendTLS(ctx context.Context, desired desiredRoute, observed observedRouteGraph, capable func(requireH2, requireSAN bool) bool, ownsCA bool, clientCertificate cloud.BackendClientCertificate) backendTLSPlan {
	tls, pool := desired.spec.BackendTLS, observed.pool
	blocked := func(message string) backendTLSPlan {
		return backendTLSPlan{phase: backendTLSBlocked, issue: message}
	}
	if tls == nil {
		if pool == nil || !hasObservedBackendTLS(*pool, observed.backendTLS) {
			return backendTLSPlan{}
		}
		if desired.xBackendPlaintext && slices.Contains(pool.Tags, backendTLSManagedTag) && xBackendPoolWithoutTLS(*pool, observed.backendTLS) {
			// A live mode None can release a temporary quarantine even on
			// stock Octavia. Actual TLS enrollment still needs the TLS gate.
			return backendTLSPlan{phase: backendTLSRelease}
		}
		if desired.xBackend && !p.poolALPN {
			return blocked("XBackend TLS changes require Octavia API microversion 2.24 or newer")
		}
		// Only a current, enabled snapshot may release a protection installed
		// by this controller. An unmarked TLS pool may be externally managed.
		if !desired.spec.BackendTLSObserved || !slices.Contains(pool.Tags, backendTLSManagedTag) || !capable(false, false) {
			return blocked("Waiting for an authorized BackendTLSPolicy snapshot")
		}
		if !backendTLSReadbackComplete(desired, observed) || !systemCAModeReadbackComplete(*pool, observed.backendTLS) ||
			desired.xBackend && slices.Contains(pool.Tags, xBackendTLSClientTag) && !observed.backendTLS.ALPNObserved {
			return blocked("Waiting for complete Octavia backend TLS readback")
		}
		return backendTLSPlan{phase: backendTLSRelease}
	}
	if tls.Denied {
		if desired.xBackend && pool != nil && slices.Contains(pool.Tags, xBackendTLSClientTag) &&
			(!observed.backendTLS.ClientRefObserved || !observed.backendTLS.AdminObserved) {
			return blocked("Waiting to verify XBackend client certificate withdrawal")
		}
		return backendTLSPlan{phase: backendTLSDenied}
	}
	if clientCertificate.Denied {
		return backendTLSPlan{phase: backendTLSBlocked, issue: "Waiting for an authorized Gateway backend client certificate", clearClientCertificate: true}
	}
	if !validBackendTLS(tls) || desired.spec.BackendH2C {
		return blocked("Backend TLS requires valid trust and a DNS hostname")
	}
	systemCA := tls.Trust == cloud.BackendTLSTrustSystem
	if desired.xBackend && !p.poolALPN {
		return blocked("Authenticated XBackends require Octavia API microversion 2.24 or newer")
	}
	if !capable(desired.requiresBackendTLSH2(), len(tls.SubjectAltNames) != 0) {
		if systemCA {
			return blocked("Waiting for Octavia to advertise system CA trust for the selected backend protocol")
		}
		if len(tls.SubjectAltNames) != 0 {
			return blocked("Waiting for Octavia to advertise explicit backend TLS subjectAltNames for the selected protocol")
		}
		if desired.requiresBackendTLSH2() {
			return blocked("Waiting for Octavia to advertise authenticated backend TLS with required HTTP/2 ALPN")
		}
		return blocked("Waiting for Octavia to advertise authenticated backend TLS")
	}
	if pool != nil && (!backendTLSReadbackComplete(desired, observed) ||
		!systemCAModeReadbackComplete(*pool, observed.backendTLS)) {
		return blocked("Waiting for complete Octavia backend TLS readback")
	}
	settings := octavia.BackendTLSSettings{Hostname: tls.Hostname, ClientCertificateReference: clientCertificate.Reference}
	var preferred string
	if pool != nil {
		preferred = pool.CATLSContainerRef
	}
	if systemCA {
		settings.SystemCA = boolPointer(true)
	} else {
		if p.backendCA == nil {
			return blocked("Waiting for a regional Barbican CA store")
		}
		if !ownsCA {
			return backendTLSPlan{phase: backendTLSClaim}
		}
		revision, err := p.backendCA.Find(ctx, desired.spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CADigest, preferred)
		switch {
		case err != nil:
			return blocked("Waiting to verify the backend CA in Barbican")
		case revision.Reference() == "":
			return backendTLSPlan{phase: backendTLSPublish}
		case revision.Digest() != tls.CADigest:
			return blocked("Waiting for the authorized backend CA revision")
		}
		settings.CAReference = revision.Reference()
		if pool != nil && hasUnclearedSystemCA(*pool, observed.backendTLS) {
			settings.SystemCA = boolPointer(false)
		}
	}
	switch {
	case len(tls.SubjectAltNames) != 0:
		settings.SubjectAltNames = tls.SubjectAltNames
	case pool != nil && hasUnclearedBackendTLSSubjectAltNames(*pool, observed.backendTLS):
		settings.SubjectAltNames = []cloud.BackendTLSSubjectAltName{}
	}
	if desired.requiresBackendTLSH2() {
		settings.RequireH2 = boolPointer(true)
	} else if desired.xBackend && observed.backendTLS.RequireH2 {
		settings.RequireH2 = boolPointer(false)
	}
	if pool == nil || pool.ProvisioningStatus != "ACTIVE" || !pool.TLSEnabled || observed.backendTLS.Hostname != settings.Hostname ||
		!backendTLSSubjectAltNamesMatch(tls.SubjectAltNames, observed) || preferred != settings.CAReference || observed.backendH2C || !backendTLSProtocolMatches(desired, observed) ||
		pool.TLSContainerRef != settings.ClientCertificateReference || pool.CRLContainerRef != "" ||
		systemCA && !configuredSystemCA(*pool, observed.backendTLS) ||
		!systemCA && hasUnclearedSystemCA(*pool, observed.backendTLS) ||
		differentBackendTLSRevision(pool.Tags, backendTLSRevisionTag(tls)) {
		return backendTLSPlan{phase: backendTLSConfigure, settings: settings}
	}
	if systemCA && (!pool.AdminStateUp || !observed.backendTLS.SystemCAApplied()) {
		return backendTLSPlan{phase: backendTLSAwaitSystemCA, settings: settings,
			issue: "Waiting for Octavia to verify the applied Amphora system CA bundle"}
	}
	return backendTLSPlan{phase: backendTLSVerified, settings: settings}
}

func systemCAModeReadbackComplete(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return !slices.Contains(pool.Tags, backendTLSSystemCATag) || tls.SystemCAObserved && tls.SystemCADigestObserved
}

func validBackendTLS(tls *cloud.BackendTLSValidation) bool {
	names, err := amphora.NormalizeBackendTLSSubjectAltNames(tls.SubjectAltNames)
	if err != nil || !slices.Equal(tls.SubjectAltNames, names) || amphora.ValidateExactHostname(tls.Hostname) != nil {
		return false
	}
	switch tls.Trust {
	case cloud.BackendTLSTrustSystem:
		return tls.CAConfigMapUID == "" && tls.CADigest == "" && len(tls.CAPEM) == 0
	case cloud.BackendTLSTrustReference:
		digest := sha256.Sum256(tls.CAPEM)
		return tls.CAConfigMapUID != "" && len(tls.CAPEM) != 0 && hex.EncodeToString(digest[:]) == tls.CADigest
	default:
		return false
	}
}

func backendTLSRevisionTag(tls *cloud.BackendTLSValidation) string {
	if tls.Trust == cloud.BackendTLSTrustSystem {
		return backendTLSRevisionTagPrefix + "system"
	}
	digest := sha256.Sum256([]byte(tls.CAConfigMapUID + "\x00" + tls.CADigest))
	return backendTLSRevisionTagPrefix + hex.EncodeToString(digest[:])
}

func differentBackendTLSRevision(tags []string, expected string) bool {
	return slices.ContainsFunc(tags, func(tag string) bool {
		return strings.HasPrefix(tag, backendTLSRevisionTagPrefix) && tag != expected
	})
}

func backendTLSTags(tags []string, managed bool, revision string, clientCertificate, trackSAN bool) []string {
	tags = slices.DeleteFunc(slices.Clone(tags), func(tag string) bool {
		return tag == backendTLSManagedTag || tag == backendTLSClientTag || tag == xBackendTLSClientTag ||
			!managed && tag == backendTLSSystemCATag || strings.HasPrefix(tag, backendTLSRevisionTagPrefix)
	})
	if managed {
		tags = append(tags, backendTLSManagedTag)
	}
	if revision != "" {
		tags = append(tags, revision)
	}
	if clientCertificate {
		tags = append(tags, backendTLSClientTag)
	}
	if trackSAN && !slices.Contains(tags, backendTLSSANTag) {
		tags = append(tags, backendTLSSANTag)
	}
	return tags
}

func (plan backendTLSPlan) poolTags(desired desiredRoute, tags []string) []string {
	clientCertificate := plan.settings.ClientCertificateReference != ""
	tags = backendTLSTags(tags, true, backendTLSRevisionTag(desired.spec.BackendTLS), clientCertificate && !desired.xBackend, plan.settings.SubjectAltNames != nil)
	if desired.spec.BackendTLS.Trust == cloud.BackendTLSTrustSystem && !slices.Contains(tags, backendTLSSystemCATag) {
		tags = append(tags, backendTLSSystemCATag)
	}
	if clientCertificate && desired.xBackend {
		tags = append(tags, xBackendTLSClientTag)
	}
	return tags
}

func (plan backendTLSPlan) withdrawal(protect, requiresClosedPool bool, observed observedRouteGraph) *routeMutation {
	pool := observed.pool
	if pool == nil || plan.allowsMembers() || pool.ProvisioningStatus != "ACTIVE" && pool.ProvisioningStatus != "ERROR" {
		return nil
	}
	if plan.stagesSystemCA() && len(observed.members) == 0 && pool.ProvisioningStatus == "ACTIVE" {
		return nil
	}
	managed := slices.Contains(pool.Tags, backendTLSManagedTag)
	enrolling := protect && !managed
	if enrolling || (pool.AdminStateUp || !observed.backendTLS.AdminObserved || pool.ProvisioningStatus == "ERROR") &&
		(requiresClosedPool || plan.settings.ClientCertificateReference != "" ||
			hasBackendTLSSettings(*pool, observed.backendTLS) || managed && (!observed.backendTLS.Complete() || len(observed.members) != 0 || pool.ProvisioningStatus == "ERROR")) {
		// A regional or runtime capability loss can reject a reload of a
		// protected pool even with no members. Administrative disable is
		// supported without reenrolling TLS or reading revoked CA material.
		step := routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID, poolAdmin: boolPointer(false),
			retry: pool.ProvisioningStatus == "ERROR" && observed.backendTLS.AdminObserved && !pool.AdminStateUp}
		if enrolling {
			step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSManagedTag))
		}
		return &step
	}
	if len(observed.members) != 0 && pool.ProvisioningStatus == "ACTIVE" {
		// The public batch API removes both server traffic and health probes
		// in one reload, while an enabled empty pool still selects this match.
		return &routeMutation{kind: routeMutationClearPoolMembers, id: pool.ID}
	}
	if plan.clearClientCertificate && pool.ProvisioningStatus == "ACTIVE" && observed.backendTLS.AdminObserved &&
		!pool.AdminStateUp && observed.backendTLS.ClientRefObserved && pool.TLSContainerRef != "" {
		return &routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID, clearTLSClientRef: true}
	}
	return nil
}

func (plan backendTLSPlan) poolMutation(desired desiredRoute, observed observedRouteGraph) *routeMutation {
	if plan.phase == backendTLSUnmanaged || plan.stagesSystemCA() && desired.zeroWeightOnly {
		return nil
	}
	if plan.phase == backendTLSClaim {
		return &routeMutation{kind: routeMutationClaimBackendCA}
	}
	pool := observed.pool
	if desired.xBackend && plan.denies() {
		switch {
		case desired.fixedResponse:
			return nil
		case pool == nil:
			// An enabled empty pool preserves the policy on stock Amphora.
			return &routeMutation{kind: routeMutationCreatePool, persistenceFallback: desired.authenticatedH2Cookie()}
		case pool.ProvisioningStatus == "ACTIVE" && observed.backendTLS.AdminObserved &&
			!pool.AdminStateUp && len(observed.members) == 0 &&
			xBackendPoolWithoutTLS(*pool, observed.backendTLS):
			// Regional Amphora renders disabled authenticated pools as closed
			// targets. A stock API needs the empty target enabled.
			return &routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID, poolAdmin: boolPointer(true)}
		default:
			return nil
		}
	}
	managedTags := []string{backendTLSManagedTag}
	if plan.phase == backendTLSPublish {
		if pool != nil && pool.ProvisioningStatus == "ACTIVE" && !slices.Contains(pool.Tags, backendTLSManagedTag) {
			return &routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID,
				poolTags: new(append(slices.Clone(pool.Tags), backendTLSManagedTag))}
		}
		return &routeMutation{kind: routeMutationPublishBackendCA}
	}
	if pool == nil {
		step := routeMutation{kind: routeMutationCreatePool, poolTags: &managedTags}
		if plan.phase == backendTLSConfigure {
			step.backendTLS = &plan.settings
			step.poolTags = new(plan.poolTags(desired, nil))
		}
		return &step
	}
	if pool.ProvisioningStatus != "ACTIVE" {
		return nil
	}
	step := routeMutation{kind: routeMutationSetBackendTLS, id: pool.ID}
	if plan.phase == backendTLSRelease {
		if len(observed.members) != 0 {
			return nil
		}
		switch {
		case hasBackendTLSSettings(*pool, observed.backendTLS) || desired.xBackend && len(pool.ALPNProtocols) > 0:
			// The regional API requires an independent disabled read before
			// clearing hostname authentication and prohibits doing both in a PUT.
			if pool.AdminStateUp {
				step.poolAdmin = boolPointer(false)
			} else {
				step.backendTLS = &octavia.BackendTLSSettings{}
				if desired.xBackend && len(pool.ALPNProtocols) > 0 {
					step.poolALPNProtocols = new([]string{})
				}
				if hasUnclearedBackendTLSSubjectAltNames(*pool, observed.backendTLS) {
					step.backendTLS.SubjectAltNames = []cloud.BackendTLSSubjectAltName{}
					if !slices.Contains(pool.Tags, backendTLSSANTag) {
						step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSSANTag))
					}
				}
				if hasUnclearedSystemCA(*pool, observed.backendTLS) {
					step.backendTLS.SystemCA = boolPointer(false)
				}
				if observed.backendTLS.RequireH2 {
					step.backendTLS.RequireH2 = boolPointer(false)
				}
			}
		case !pool.AdminStateUp:
			// The renderer keeps the disabled marked pool selected even
			// after its hostname is cleared. Reopen it empty before unmarking.
			step.poolAdmin = boolPointer(true)
		default:
			step.poolTags = new(backendTLSTags(pool.Tags, false, "", false, false))
		}
		return &step
	}
	if desired.xBackend && plan.phase == backendTLSConfigure && pool.AdminStateUp {
		if len(observed.members) != 0 {
			return nil
		}
		// Capabilities and the CA were verified before closing the drained
		// pool. Enroll the marker in the same write so the regional renderer
		// keeps its predicate active until TLS is read back.
		step.poolAdmin = boolPointer(false)
		if !slices.Contains(pool.Tags, backendTLSManagedTag) {
			step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSManagedTag))
		}
		return &step
	}
	if desired.spec.BackendTLS != nil && !slices.Contains(pool.Tags, backendTLSManagedTag) {
		step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSManagedTag))
	}
	if plan.phase == backendTLSConfigure || plan.stagesSystemCA() || plan.phase == backendTLSVerified {
		revision := backendTLSRevisionTag(desired.spec.BackendTLS)
		clientCertificate := plan.settings.ClientCertificateReference != ""
		trackSAN := plan.settings.SubjectAltNames != nil
		clearedSystem := plan.phase == backendTLSVerified && desired.spec.BackendTLS.Trust != cloud.BackendTLSTrustSystem &&
			slices.Contains(pool.Tags, backendTLSSystemCATag) && !hasUnclearedSystemCA(*pool, observed.backendTLS)
		if step.poolTags != nil || !slices.Contains(pool.Tags, revision) || differentBackendTLSRevision(pool.Tags, revision) ||
			slices.Contains(pool.Tags, backendTLSClientTag) != (clientCertificate && !desired.xBackend) ||
			slices.Contains(pool.Tags, xBackendTLSClientTag) != (clientCertificate && desired.xBackend) ||
			desired.spec.BackendTLS.Trust == cloud.BackendTLSTrustSystem && !slices.Contains(pool.Tags, backendTLSSystemCATag) ||
			clearedSystem || trackSAN && !slices.Contains(pool.Tags, backendTLSSANTag) {
			tags := plan.poolTags(desired, pool.Tags)
			if clearedSystem {
				tags = slices.DeleteFunc(tags, func(tag string) bool { return tag == backendTLSSystemCATag })
			}
			step.poolTags = &tags
		}
	}
	switch {
	case plan.phase == backendTLSConfigure:
		if len(observed.members) != 0 {
			return nil
		}
		if pool.AdminStateUp && plan.settings.SystemCA != nil {
			step.poolTags = nil
			step.poolAdmin = boolPointer(false)
			return &step
		}
		if pool.AdminStateUp && (plan.settings.ClientCertificateReference != "" || plan.settings.SubjectAltNames != nil) {
			return nil
		}
		step.backendTLS = &plan.settings
		if desired.xBackend {
			step.poolALPNProtocols = new([]string{desired.xBackendALPN()})
		}
		if observed.backendH2C {
			step.backendH2C = boolPointer(false)
		}
	case plan.stagesSystemCA() && !pool.AdminStateUp && len(observed.members) == 0:
		step.poolAdmin = boolPointer(true)
	case plan.phase == backendTLSVerified && !pool.AdminStateUp:
		step.poolAdmin = boolPointer(true)
		if desired.requiresBackendTLSH2() || len(plan.settings.SubjectAltNames) != 0 {
			// Reassert the verified settings when reopening an extended TLS pool.
			step.backendTLS = &plan.settings
		}
	case step.poolTags == nil:
		return nil
	}
	return &step
}

func (p *Provider) publishBackendCA(ctx context.Context, spec cloud.RouteSpec) error {
	tls := spec.BackendTLS
	revision, err := p.backendCA.Ensure(ctx, spec.Identity.GatewayUID, tls.CAConfigMapUID, tls.CAPEM)
	if err != nil {
		return err
	}
	if revision.Reference() == "" || revision.Digest() != tls.CADigest {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("backend CA publication did not return the authorized revision"))
	}
	return nil
}

func redactBackendTLSMutationError(step routeMutation, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	category, ok := cloud.ErrorCategoryOf(classifyOpenStackError(classifyOctaviaMutationError(err)))
	if !ok {
		category = cloud.ErrorCategoryResourceFailure
	}
	return cloud.NewProviderError(category, errors.New(step.message()))
}
