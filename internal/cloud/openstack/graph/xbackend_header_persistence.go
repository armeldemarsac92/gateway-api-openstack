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
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"k8s.io/utils/ptr"
)

const xBackendHeaderTag = "gateway-api-openstack.xbackend-session-header=v1"

var errXBackendHeaderPersistence = errors.New("waiting for Octavia XBackend HTTP header persistence to converge")

// XBackendHeaderAvailable discovers support on the regional Amphora endpoint.
func (p *Provider) XBackendHeaderAvailable(ctx context.Context) (bool, error) {
	return p.discoverXBackendHeader(ctx, "amphora")
}

func (p *Provider) discoverXBackendHeader(ctx context.Context, provider string) (bool, error) {
	return p.discoverXBackendPersistence(ctx, provider, octavia.GatewayCapabilityHTTPHeaderPersistence)
}

func (route desiredRoute) sessionPersistenceIssue() cloud.SessionPersistenceIssue {
	issue := max(route.cookieSupportIssue(), route.headerPersistenceIssue())
	for _, part := range route.httpParts {
		issue = max(issue, part.sessionPersistenceIssue())
	}
	return issue
}

func (route desiredRoute) headerPersistenceIssue() cloud.SessionPersistenceIssue {
	if route.sessionHeader != nil {
		switch route.sessionHeaderCapability {
		case xBackendCookieNotObserved:
			return cloud.SessionPersistenceNotObserved
		case xBackendCookieUnverified:
			return cloud.SessionPersistenceHeaderUnverified
		case xBackendCookieUnavailable:
			return cloud.SessionPersistenceHeaderUnsupported
		}
	}
	if route.headerReadbackPending {
		return cloud.SessionPersistenceHeaderReadbackUnverified
	}
	return cloud.SessionPersistenceAvailable
}

func (route desiredRoute) activeHeaderMatches(observed observedRouteGraph) bool {
	pool, want, have := observed.pool, route.sessionHeader, observed.headerPersistence.Value
	return pool != nil && pool.Protocol == cloud.ListenerProtocolHTTP && pool.ProvisioningStatus == "ACTIVE" &&
		observed.backendTLS.AdminObserved && slices.Contains(pool.Tags, xBackendHeaderTag) &&
		pool.Persistence == (pools.SessionPersistence{Type: cloud.HTTPHeaderPersistenceType}) &&
		observed.headerPersistence.Observed && !observed.headerPersistence.Invalid && !observed.cookieDeadline.Invalid && observed.cookieDeadline.Value == nil &&
		!observed.cookiePath.Invalid && observed.cookiePath.Value == nil &&
		want != nil && have != nil && strings.EqualFold(want.Name, have.Name) && ptr.Equal(want.TimeoutMS, have.TimeoutMS)
}

func enrolledHeader(observed observedRouteGraph) bool {
	return observed.pool != nil && (slices.Contains(observed.pool.Tags, xBackendHeaderTag) ||
		observed.pool.Persistence.Type == cloud.HTTPHeaderPersistenceType || observed.headerPersistence.Invalid)
}

func planXBackendPersistence(desired desiredRoute, observed observedRouteGraph) (*routeMutation, error) {
	if !desired.xBackend {
		return nil, nil
	}
	if desired.sessionHeader != nil && observed.pool != nil && slices.ContainsFunc(observed.pool.Tags, isXBackendCookieTag) {
		// Withdraw the cookie and any of its extension fields before enrolling Header.
		return planXBackendCookie(desired, observed)
	}
	if step, err := planXBackendHeaderPersistence(desired, observed); step != nil || err != nil {
		return step, err
	}
	return planXBackendCookie(desired, observed)
}

func planXBackendHeaderPersistence(desired desiredRoute, observed observedRouteGraph) (*routeMutation, error) {
	pool := observed.pool
	want := desired.sessionHeader
	enrolled := enrolledHeader(observed)
	supported := desired.sessionHeaderCapability == xBackendCookieAvailable
	if want == nil && !enrolled || desired.activeHeaderMatches(observed) && supported {
		return nil, nil
	}
	if pool == nil {
		if supported {
			// Create the managed pool through the ordinary transport plan. A
			// later PUT and an independent readback gate member creation.
			return nil, nil
		}
		return nil, errXBackendHeaderPersistence
	}
	if pool.Protocol != cloud.ListenerProtocolHTTP || !observed.backendTLS.AdminObserved {
		return nil, errXBackendHeaderPersistence
	}
	step := &routeMutation{kind: routeMutationSetXBackendPersistence, id: pool.ID}
	if pool.ProvisioningStatus == "ERROR" && enrolled {
		// A member batch needs an ACTIVE pool. Recover it through a stock
		// PUT that clears persistence. Keep a closed pool's match installed
		// until its previous members can be removed.
		step.retry = true
		step.poolPersistence = &pools.SessionPersistence{}
		if len(observed.members) != 0 {
			step.poolAdmin = ptr.To(false)
			if !slices.Contains(pool.Tags, backendTLSManagedTag) {
				step.poolTags = new(append(slices.Clone(pool.Tags), backendTLSManagedTag))
			}
		}
		return step, nil
	}
	if pool.ProvisioningStatus != "ACTIVE" {
		return nil, errXBackendHeaderPersistence
	}
	if len(observed.members) != 0 {
		return &routeMutation{kind: routeMutationClearPoolMembers, id: pool.ID, persistenceDrain: true}, nil
	}
	managed := slices.Contains(pool.Tags, xBackendHeaderTag)
	cleared := observed.headerPersistence.Observed && !observed.headerPersistence.Invalid &&
		pool.Persistence.Type != cloud.HTTPHeaderPersistenceType
	switch {
	case want == nil && cleared:
		step.poolTags = new(slices.DeleteFunc(slices.Clone(pool.Tags), func(tag string) bool { return tag == xBackendHeaderTag }))
	case want == nil || desired.sessionHeaderCapability == xBackendCookieUnavailable && enrolled && !cleared:
		// Withdraw with json null even when the regional capability has
		// been revoked. Keep the ownership tag through the next readback.
		step.poolPersistence = &pools.SessionPersistence{}
	case supported:
		step.poolHeaderPersistence = want
		if !managed {
			step.poolTags = new(append(slices.Clone(pool.Tags), xBackendHeaderTag))
		}
	default:
		return nil, errXBackendHeaderPersistence
	}
	return step, nil
}
