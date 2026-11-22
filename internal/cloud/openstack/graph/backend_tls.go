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
	"sync"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const backendTLSManagedTag = "gateway-api-openstack.backend-tls=v1"
const backendTLSRevisionTagPrefix = "gateway-api-openstack.backend-tls-ca="

type backendTLSPhase uint8

const (
	backendTLSUnmanaged backendTLSPhase = iota
	backendTLSDenied
	backendTLSBlocked
	backendTLSClaim
	backendTLSPublish
	backendTLSConfigure
	backendTLSVerified
	backendTLSRelease
)

type backendTLSPlan struct {
	phase    backendTLSPhase
	settings octavia.BackendTLSSettings
	issue    string
}

type backendTLSPart struct {
	index    int
	key      string
	desired  *desiredRoute
	observed observedRouteGraph
}

func (plan backendTLSPlan) denies() bool {
	return plan.phase == backendTLSDenied || plan.phase == backendTLSBlocked
}

func (plan backendTLSPlan) allowsMembers() bool {
	return plan.phase == backendTLSUnmanaged || plan.phase == backendTLSVerified
}

func hasObservedBackendTLS(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return slices.Contains(pool.Tags, backendTLSManagedTag) || tls.Hostname != ""
}

func hasBackendTLSSettings(pool pools.Pool, tls octavia.PoolBackendTLS) bool {
	return tls.Hostname != "" || pool.TLSEnabled || pool.CATLSContainerRef != "" ||
		pool.TLSContainerRef != "" || pool.CRLContainerRef != ""
}

func routeHasBackendTLSPolicy(route desiredRoute) bool {
	return route.spec.BackendTLS != nil || slices.ContainsFunc(route.httpParts, func(part desiredRoute) bool {
		return part.spec.BackendTLS != nil
	})
}

// prepareBackendTLS classifies each owned pool before any ordinary ensure can
// affect traffic. A quarantine step takes priority over dependencies such as
// Floating IPs and security-group validation.
func (p *Provider) prepareBackendTLS(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if actual.protocol != cloud.ListenerProtocolHTTP || desired.spec.Gateway.Disposition != cloud.GraphEnsure ||
		actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" || len(actual.routes) != len(desired.routes) {
		return nil
	}
	parts := backendTLSParts(desired, actual.routes)
	early := firstBackendTLSWithdrawal(parts, func(part backendTLSPart) backendTLSPlan {
		return part.preflight()
	})
	if early != nil && !early.route.retry {
		return early
	}
	capable := func() bool {
		return p.backendTLSPolicy && p.backendCA != nil && p.verifyGatewayHTTPCapabilities(ctx, *desired, actual,
			[]string{octavia.GatewayCapabilityBackendTLSHostname}) == nil
	}
	capability := sync.OnceValue(capable)
	frontends := scopedHTTPListeners(*desired)
	step := firstBackendTLSWithdrawal(parts, func(part backendTLSPart) backendTLSPlan {
		if part.desired == nil {
			// Keep the old pool and its durable marker until a live policy
			// inventory authorizes the replacement representation.
			if pool := part.observed.pool; pool != nil && hasObservedBackendTLS(*pool, part.observed.backendTLS) &&
				(!desired.routes[part.index].spec.BackendTLSObserved || !capability()) {
				desired.spec.Routes[part.index].Disposition = cloud.GraphPreserve
				desired.routes[part.index].backendTLS = backendTLSPlan{phase: backendTLSBlocked, issue: "Waiting for a current backend TLS policy inventory before replacing the route pools"}
			}
			return backendTLSPlan{phase: backendTLSDenied}
		}
		if listener, configured := frontends[scopedHTTPRoutePort(*part.desired)]; configured && !hasAuthorizedListener(listener) {
			part.desired.backendTLS = backendTLSPlan{phase: backendTLSBlocked, issue: "Waiting for the authorized HTTP listener"}
			return part.desired.backendTLS
		}
		part.desired.backendTLS = p.observeBackendTLS(ctx, *part.desired, part.observed, capability, desired.spec.Gateway.BackendCAOwned)
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
		if desired.spec.Routes[index].Disposition != cloud.GraphEnsure {
			continue
		}
		route := &desired.routes[index]
		observed := routes[index]
		hasPolicy := routeHasBackendTLSPolicy(*route)
		add := func(part *desiredRoute, key string, actualPart observedRouteGraph) {
			if part == nil && hasPolicy && actualPart.pool != nil || part != nil && part.spec.BackendTLS != nil ||
				actualPart.pool != nil && hasObservedBackendTLS(*actualPart.pool, actualPart.backendTLS) {
				parts = append(parts, backendTLSPart{index: index, key: key, desired: part, observed: actualPart})
			}
		}
		if len(route.httpParts) == 0 || len(route.policies) != 0 {
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
	if part.desired == nil || part.desired.spec.BackendTLS == nil || part.desired.spec.BackendTLS.Denied {
		return backendTLSPlan{phase: backendTLSDenied}
	}
	tls := part.desired.spec.BackendTLS
	observed := part.observed.backendTLS
	if pool.ProvisioningStatus != "ACTIVE" || !pool.AdminStateUp && len(part.observed.members) != 0 ||
		!validBackendTLS(tls) || !observed.Complete() || observed.Hostname != tls.Hostname ||
		!pool.TLSEnabled || pool.CATLSContainerRef == "" || part.observed.backendH2C || part.desired.spec.BackendH2C ||
		pool.TLSContainerRef != "" || pool.CRLContainerRef != "" || differentBackendTLSRevision(pool.Tags, backendTLSRevisionTag(tls)) {
		return backendTLSPlan{phase: backendTLSConfigure}
	}
	return backendTLSPlan{}
}

func firstBackendTLSWithdrawal(parts []backendTLSPart, plan func(backendTLSPart) backendTLSPlan) *graphMutation {
	var first *graphMutation
	for _, part := range parts {
		protect := part.desired == nil || part.desired.spec.BackendTLS != nil
		step := plan(part).withdrawal(protect, part.observed)
		// An unsupported protected pool can block member drains anywhere on
		// the load balancer. Disable unsafe protected pools first.
		if step != nil && (first == nil || first.route.retry && !step.retry ||
			first.route.retry == step.retry && first.route.kind == routeMutationClearPoolMembers && step.kind == routeMutationSetBackendTLS) {
			first = &graphMutation{kind: graphRouteMutation, routeIndex: part.index, backendKey: part.key,
				route: *step, withdrawHTTPPolicy: true, message: step.message()}
			if step.kind == routeMutationSetBackendTLS && !step.retry {
				return first
			}
		}
	}
	return first
}

func (p *Provider) observeBackendTLS(ctx context.Context, desired desiredRoute, observed observedRouteGraph, capable func() bool, ownsCA bool) backendTLSPlan {
	tls, pool := desired.spec.BackendTLS, observed.pool
	blocked := func(message string) backendTLSPlan {
		return backendTLSPlan{phase: backendTLSBlocked, issue: message}
	}
	if tls == nil {
		if pool == nil || !hasObservedBackendTLS(*pool, observed.backendTLS) {
			return backendTLSPlan{}
		}
		// Only a current, enabled snapshot may release a protection installed
		// by this controller. An unmarked TLS pool may be externally managed.
		if !desired.spec.BackendTLSObserved || !slices.Contains(pool.Tags, backendTLSManagedTag) || !capable() {
			return blocked("Waiting for an authorized BackendTLSPolicy snapshot")
		}
		if !observed.backendTLS.Complete() {
			return blocked("Waiting for complete Octavia backend TLS readback")
		}
		return backendTLSPlan{phase: backendTLSRelease}
	}
	if tls.Denied {
		return backendTLSPlan{phase: backendTLSDenied}
	}
	if !validBackendTLS(tls) || desired.spec.BackendH2C {
		return blocked("Backend TLS requires a valid CA and DNS hostname on an HTTP backend")
	}
	if !capable() {
		return blocked("Waiting for Octavia to advertise authenticated backend TLS")
	}
	if pool != nil && !observed.backendTLS.Complete() {
		return blocked("Waiting for complete Octavia backend TLS readback")
	}
	if !ownsCA {
		return backendTLSPlan{phase: backendTLSClaim}
	}
	var preferred string
	if pool != nil {
		preferred = pool.CATLSContainerRef
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
	settings := octavia.BackendTLSSettings{Hostname: tls.Hostname, CAReference: revision.Reference()}
	if pool == nil || pool.ProvisioningStatus != "ACTIVE" || !pool.TLSEnabled || observed.backendTLS.Hostname != settings.Hostname ||
		preferred != settings.CAReference || observed.backendH2C || pool.TLSContainerRef != "" || pool.CRLContainerRef != "" {
		return backendTLSPlan{phase: backendTLSConfigure, settings: settings}
	}
	return backendTLSPlan{phase: backendTLSVerified}
}

func validBackendTLS(tls *cloud.BackendTLSValidation) bool {
	digest := sha256.Sum256(tls.CAPEM)
	return tls.CAConfigMapUID != "" && len(tls.CAPEM) != 0 && hex.EncodeToString(digest[:]) == tls.CADigest &&
		amphora.ValidateExactHostname(tls.Hostname) == nil
}

func backendTLSRevisionTag(tls *cloud.BackendTLSValidation) string {
	digest := sha256.Sum256([]byte(tls.CAConfigMapUID + "\x00" + tls.CADigest))
	return backendTLSRevisionTagPrefix + hex.EncodeToString(digest[:])
}

func differentBackendTLSRevision(tags []string, expected string) bool {
	return slices.ContainsFunc(tags, func(tag string) bool {
		return strings.HasPrefix(tag, backendTLSRevisionTagPrefix) && tag != expected
	})
}

func backendTLSTags(tags []string, managed bool, revision string) []string {
	tags = slices.DeleteFunc(slices.Clone(tags), func(tag string) bool {
		return tag == backendTLSManagedTag || strings.HasPrefix(tag, backendTLSRevisionTagPrefix)
	})
	if managed {
		tags = append(tags, backendTLSManagedTag)
	}
	if revision != "" {
		tags = append(tags, revision)
	}
	return tags
}

func (plan backendTLSPlan) withdrawal(protect bool, observed observedRouteGraph) *routeMutation {
	pool := observed.pool
	if pool == nil || plan.allowsMembers() || pool.ProvisioningStatus != "ACTIVE" && pool.ProvisioningStatus != "ERROR" {
		return nil
	}
	managed := slices.Contains(pool.Tags, backendTLSManagedTag)
	enrolling := protect && !managed
	if enrolling || (pool.AdminStateUp || !observed.backendTLS.AdminObserved || pool.ProvisioningStatus == "ERROR") &&
		(hasBackendTLSSettings(*pool, observed.backendTLS) || managed && (!observed.backendTLS.Complete() || len(observed.members) != 0 || pool.ProvisioningStatus == "ERROR")) {
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
	return nil
}

func (plan backendTLSPlan) poolMutation(desired desiredRoute, observed observedRouteGraph) *routeMutation {
	if plan.phase == backendTLSUnmanaged {
		return nil
	}
	if plan.phase == backendTLSClaim {
		return &routeMutation{kind: routeMutationClaimBackendCA}
	}
	pool := observed.pool
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
			step.poolTags = new(backendTLSTags(nil, true, backendTLSRevisionTag(desired.spec.BackendTLS)))
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
		case hasBackendTLSSettings(*pool, observed.backendTLS):
			// The regional API requires an independent disabled read before
			// clearing hostname authentication and prohibits doing both in a PUT.
			if pool.AdminStateUp {
				step.poolAdmin = boolPointer(false)
			} else {
				step.backendTLS = &octavia.BackendTLSSettings{}
			}
		case !pool.AdminStateUp:
			// The renderer keeps the disabled marked pool selected even
			// after its hostname is cleared. Reopen it empty before unmarking.
			step.poolAdmin = boolPointer(true)
		default:
			step.poolTags = new(backendTLSTags(pool.Tags, false, ""))
		}
		return &step
	}
	if desired.spec.BackendTLS != nil && !slices.Contains(pool.Tags, backendTLSManagedTag) {
		step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSManagedTag))
	}
	if plan.phase == backendTLSConfigure || plan.phase == backendTLSVerified {
		revision := backendTLSRevisionTag(desired.spec.BackendTLS)
		if step.poolTags != nil || !slices.Contains(pool.Tags, revision) || differentBackendTLSRevision(pool.Tags, revision) {
			tags := backendTLSTags(pool.Tags, true, revision)
			step.poolTags = &tags
		}
	}
	switch {
	case plan.phase == backendTLSConfigure && len(observed.members) == 0:
		step.backendTLS = &plan.settings
		if observed.backendH2C {
			step.backendH2C = boolPointer(false)
		}
	case plan.phase == backendTLSVerified && !pool.AdminStateUp:
		step.poolAdmin = boolPointer(true)
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
