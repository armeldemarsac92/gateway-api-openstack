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

// Package graph observes, validates, and reconciles the complete OpenStack
// resource graph owned by Gateway API objects.
package graph

import (
	"context"
	"errors"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

var (
	errBackendRetryBudgetAncestor   = errors.New("waiting for the Gateway ancestor of the Service retry budget")
	errBackendRetryBudgetCapability = errors.New("waiting for Octavia Service retry budget capability")
	errBackendRetryBudgetReadback   = errors.New("waiting for a valid Octavia Service retry budget readback")
)

// Discovery failure preserves the observed budget and holds only a pool
// whose requested budget cannot be verified. Withdrawals need no discovery.
func (p *Provider) observeBackendRetryBudgetCapability(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if !p.backendRetryBudget || actual.loadBalancer == nil || desired.protocol != cloud.ListenerProtocolHTTP {
		return
	}
	var targets []*desiredRoute
	for i := range desired.routes {
		route := &desired.routes[i]
		if route.spec.BackendRetryBudget != nil {
			targets = append(targets, route)
		}
		for j := range route.httpParts {
			part := &route.httpParts[j]
			if part.spec.BackendRetryBudget != nil {
				targets = append(targets, part)
			}
		}
	}
	if len(targets) == 0 {
		return
	}
	err := p.verifyGatewayHTTPURLCapabilities(ctx, *desired, actual, []string{octavia.GatewayCapabilityBackendRetryBudget})
	if err == nil {
		for _, target := range targets {
			target.backendRetryBudgetAvailable = true
		}
	}
}

func planBackendRetryBudget(desired desiredRoute, observed observedRouteGraph) (*routeMutation, error) {
	if desired.spec.BackendRetryBudgetPending {
		return nil, errBackendRetryBudgetAncestor
	}
	wanted := desired.spec.BackendRetryBudget
	if observed.pool == nil {
		if wanted != nil && !desired.backendRetryBudgetAvailable {
			return nil, errBackendRetryBudgetCapability
		}
		return nil, nil
	}
	if observed.pool.Protocol != cloud.ListenerProtocolHTTP {
		return nil, nil
	}
	current := observed.backendRetryBudget
	if wanted != nil {
		switch {
		case current.Value != nil && *current.Value == *wanted:
			// Readback proves that Octavia already has this Service budget.
			return nil, nil
		case current.Present && current.Value == nil:
			return nil, errBackendRetryBudgetReadback
		case !desired.backendRetryBudgetAvailable:
			return nil, errBackendRetryBudgetCapability
		}
	} else if !current.Present {
		return nil, nil
	}
	return &routeMutation{
		kind: routeMutationSetBackendRetryBudget, id: observed.pool.ID,
		backendRetryBudget: &octavia.BackendRetryBudgetUpdate{Value: wanted},
	}, nil
}
