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

// Discovery failure preserves configured budgets and allows ordinary HTTP
// reconciliation to continue. An explicit withdrawal never requires the
// advertised capability for new writes.
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
	if observed.pool == nil || observed.pool.Protocol != cloud.ListenerProtocolHTTP {
		return nil, nil
	}
	wanted := desired.spec.BackendRetryBudget
	current := observed.backendRetryBudget
	if wanted != nil {
		if !desired.backendRetryBudgetAvailable || current.Value != nil && *current.Value == *wanted {
			return nil, nil
		}
		if current.Present && current.Value == nil {
			return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("invalid Octavia backend retry budget readback"))
		}
	} else if !current.Present {
		return nil, nil
	}
	return &routeMutation{
		kind: routeMutationSetBackendRetryBudget, id: observed.pool.ID,
		backendRetryBudget: &octavia.BackendRetryBudgetUpdate{Value: wanted},
	}, nil
}
