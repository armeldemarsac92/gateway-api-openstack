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
	"slices"
	"sync"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const (
	httpCookiePersistence     = "HTTP_COOKIE"
	xBackendCookieTag         = "gateway-api-openstack.xbackend-session-cookie=v1"
	xBackendCookieDeadlineTag = "gateway-api-openstack.xbackend-session-cookie-absolute-timeout=v1"
)

type xBackendCookieCapability uint8

const (
	xBackendCookieNotObserved xBackendCookieCapability = iota
	xBackendCookieUnverified
	xBackendCookieUnavailable
	xBackendCookieAvailable
)

var (
	errXBackendCookieUnavailable         = errors.New("regional Octavia provider does not advertise http_cookie_name_v1")
	errXBackendCookieDeadlineUnavailable = errors.New("regional Octavia provider does not advertise both http_cookie_name_v1 and http_cookie_absolute_timeout_v1")
	errXBackendCookieUnverified          = errors.New("waiting to verify Octavia XBackend session persistence")
)

// XBackendCookieAvailable checks regional support for the Amphora provider
// selected for HTTPRoutes. Mutations also check the effective provider.
func (p *Provider) XBackendCookieAvailable(ctx context.Context, withDeadline bool) (bool, error) {
	return p.discoverXBackendCookie(ctx, "amphora", withDeadline)
}

func (p *Provider) discoverXBackendCookie(ctx context.Context, provider string, withDeadline bool) (bool, error) {
	if !p.xBackendSessionPersistence {
		return false, nil
	}
	discovery, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	required := []string{octavia.GatewayCapabilityHTTPCookieName}
	if withDeadline {
		required = append(required, octavia.GatewayCapabilityHTTPCookieAbsoluteTimeout)
	}
	err := p.octavia.VerifyGatewayCapabilities(discovery, provider, required...)
	var missing *octavia.MissingGatewayCapability
	if errors.As(err, &missing) || isNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// Prepare positive cookie pools against the effective provider. Keep the
// predicate installed while emptying a pool with unverified persistence.
func (p *Provider) prepareXBackendCookies(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) *graphMutation {
	if actual.loadBalancer == nil || actual.protocol != cloud.ListenerProtocolHTTP ||
		desired.spec.Gateway.Disposition != cloud.GraphEnsure || len(desired.routes) != len(actual.routes) {
		return nil
	}
	capability := func(withDeadline bool) func() xBackendCookieCapability {
		return sync.OnceValue(func() xBackendCookieCapability {
			available, err := p.discoverXBackendCookie(ctx, actual.loadBalancer.Provider, withDeadline)
			switch {
			case err != nil:
				return xBackendCookieUnverified
			case available:
				return xBackendCookieAvailable
			default:
				return xBackendCookieUnavailable
			}
		})
	}
	named, timed := capability(false), capability(true)
	var withdrawal *graphMutation
	for i := range desired.routes {
		if desired.spec.Routes[i].Disposition != cloud.GraphEnsure {
			continue
		}
		targets := make(map[string]bool)
		for j := range desired.routes[i].httpParts {
			part := &desired.routes[i].httpParts[j]
			targets[part.backendKey] = true
			current := actual.routes[i].httpParts[part.backendKey]
			switch {
			case part.sessionCookieDeadline != nil:
				part.sessionCookieCapability = timed()
				if current != nil && current.pool != nil {
					part.cookieReadbackPending = current.pool.ProvisioningStatus != "ACTIVE" || !part.cookieDeadlineMatches(*current) ||
						current.pool.Persistence != (pools.SessionPersistence{Type: httpCookiePersistence, CookieName: part.sessionCookieName})
				}
			case part.sessionCookieName != "":
				part.sessionCookieCapability = named()
			}
			if current == nil || withdrawal != nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
				continue
			}
			if step, _ := planXBackendCookie(*part, *current); step != nil &&
				(step.kind == routeMutationClearPoolMembers || step.poolPersistence != nil && *step.poolPersistence == (pools.SessionPersistence{})) {
				withdrawal = &graphMutation{kind: graphRouteMutation, routeIndex: i, backendKey: part.backendKey,
					route: *step, withdrawHTTPPolicy: true, message: step.message()}
			}
		}
		if withdrawal == nil && actual.loadBalancer.ProvisioningStatus == "ACTIVE" {
			for _, key := range slices.Sorted(maps.Keys(actual.routes[i].httpParts)) {
				old := actual.routes[i].httpParts[key]
				if targets[key] || old.pool == nil || !slices.Contains(old.pool.Tags, xBackendCookieTag) &&
					!slices.Contains(old.pool.Tags, xBackendCookieDeadlineTag) {
					continue
				}
				// A rejected weighted target is absent from the desired graph.
				// Withdraw its old persistence before ordinary policy cleanup.
				if step, _ := planXBackendCookie(desiredRoute{xBackend: true}, *old); step != nil {
					withdrawal = &graphMutation{kind: graphRouteMutation, routeIndex: i, backendKey: key,
						route: *step, withdrawHTTPPolicy: true, message: step.message()}
					break
				}
			}
		}
	}
	return withdrawal
}

func planXBackendCookie(desired desiredRoute, observed observedRouteGraph) (*routeMutation, error) {
	if !desired.xBackend {
		return nil, nil
	}
	wanted := pools.SessionPersistence{}
	if desired.sessionCookieName != "" {
		wanted = pools.SessionPersistence{Type: httpCookiePersistence, CookieName: desired.sessionCookieName}
	}
	pool := observed.pool
	managed := pool != nil && slices.Contains(pool.Tags, xBackendCookieTag)
	timed := pool != nil && slices.Contains(pool.Tags, xBackendCookieDeadlineTag)
	needsCookie := desired.sessionCookieName != ""
	needsDeadline := desired.sessionCookieDeadline != nil
	issue := xBackendCookieIssue(desired)
	switch {
	case pool == nil:
		return nil, issue
	case !needsCookie && !managed && !timed:
		return nil, nil
	case pool.Protocol != cloud.ListenerProtocolHTTP:
		return nil, errXBackendCookieUnverified
	case pool.ProvisioningStatus == "ERROR":
		if observed.backendTLS.AdminObserved {
			// Octavia locks member writes on ERROR pools but permits a
			// pool PUT. Keep the current admin state and predicate. Verify
			// the pool and any pending cookie change after it is ACTIVE.
			return &routeMutation{kind: routeMutationSetXBackendCookie, id: pool.ID,
				poolAdmin: boolPointer(pool.AdminStateUp), retry: true}, nil
		}
		return nil, errXBackendCookieUnverified
	case pool.ProvisioningStatus != "ACTIVE":
		return nil, errXBackendCookieUnverified
	case issue == nil && pool.Persistence == wanted && managed == needsCookie && timed == needsDeadline &&
		desired.cookieDeadlineMatches(observed) && observed.backendTLS.AdminObserved:
		return nil, nil
	}
	if len(observed.members) != 0 {
		// A disabled stock HTTP pool can drop the direct L7 match in
		// Amphora. An empty pool keeps ownership of its exact predicate.
		return &routeMutation{kind: routeMutationClearPoolMembers, id: pool.ID, xBackendCookieDrain: true}, nil
	}
	if !observed.backendTLS.AdminObserved {
		return nil, errXBackendCookieUnverified
	}
	step := routeMutation{kind: routeMutationSetXBackendCookie, id: pool.ID}
	switch {
	case issue == nil:
		step.poolPersistence = &wanted
		step.poolCookieDeadline = desired.sessionCookieDeadline
		step.clearPoolCookieDeadline = needsCookie && !needsDeadline && (timed || !desired.cookieDeadlineMatches(observed))
		if managed != needsCookie || timed != needsDeadline {
			tags := slices.DeleteFunc(slices.Clone(pool.Tags), func(tag string) bool {
				return tag == xBackendCookieTag || tag == xBackendCookieDeadlineTag
			})
			if needsCookie {
				tags = append(tags, xBackendCookieTag)
			}
			if needsDeadline {
				tags = append(tags, xBackendCookieDeadlineTag)
			}
			step.poolTags = &tags
		}
	case desired.sessionCookieCapability == xBackendCookieUnavailable &&
		(pool.Persistence != (pools.SessionPersistence{}) || observed.cookieDeadline.Value != nil || observed.cookieDeadline.Invalid):
		// Octavia accepts removal of the optional stock field without the
		// regional extension. Keep the pool closed until support returns.
		step.poolPersistence = &pools.SessionPersistence{}
	default:
		return nil, issue
	}
	return &step, nil
}

func xBackendCookieIssue(route desiredRoute) error {
	switch route.cookieCapabilityIssue() {
	case cloud.SessionPersistenceUnsupported:
		return errXBackendCookieUnavailable
	case cloud.SessionPersistenceDeadlineUnsupported:
		return errXBackendCookieDeadlineUnavailable
	case cloud.SessionPersistenceNotObserved, cloud.SessionPersistenceUnverified, cloud.SessionPersistenceDeadlineUnverified:
		return errXBackendCookieUnverified
	}
	return nil
}

func (route desiredRoute) cookieDeadlineMatches(observed observedRouteGraph) bool {
	want, have := route.sessionCookieDeadline, observed.cookieDeadline.Value
	if observed.cookieDeadline.Invalid {
		return false
	}
	if want == nil || have == nil {
		return want == have
	}
	return *want == *have
}

func (route desiredRoute) cookieCapabilityIssue() cloud.SessionPersistenceIssue {
	if route.sessionCookieName != "" {
		switch route.sessionCookieCapability {
		case xBackendCookieUnavailable:
			if route.sessionCookieDeadline != nil {
				return cloud.SessionPersistenceDeadlineUnsupported
			}
			return cloud.SessionPersistenceUnsupported
		case xBackendCookieUnverified:
			if route.sessionCookieDeadline != nil {
				return cloud.SessionPersistenceDeadlineUnverified
			}
			return cloud.SessionPersistenceUnverified
		case xBackendCookieNotObserved:
			return cloud.SessionPersistenceNotObserved
		}
	}
	return cloud.SessionPersistenceAvailable
}

func (route desiredRoute) cookieSupportIssue() cloud.SessionPersistenceIssue {
	issue := route.cookieCapabilityIssue()
	if route.cookieReadbackPending && issue == cloud.SessionPersistenceAvailable {
		issue = cloud.SessionPersistenceDeadlineReadbackUnverified
	}
	for _, part := range route.httpParts {
		issue = max(issue, part.cookieSupportIssue())
	}
	return issue
}

// cookieFallback supports ordinary direct L7 policy reconciliation with a
// stock enabled empty pool while discovery cannot authorize persistence.
// Weighted rules already have their own same-weight failure response.
func (route desiredRoute) cookieFallback(observed observedRouteGraph) bool {
	if len(route.policies) == 0 || route.fixedResponse ||
		route.sessionCookieCapability != xBackendCookieUnavailable && route.sessionCookieCapability != xBackendCookieUnverified {
		return false
	}
	return observed.pool == nil || observed.pool.ProvisioningStatus == "ACTIVE" &&
		observed.backendTLS.AdminObserved && len(observed.members) == 0
}
