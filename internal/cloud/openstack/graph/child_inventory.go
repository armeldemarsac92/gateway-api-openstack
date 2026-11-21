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
	"fmt"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func (p *Provider) listGatewayListeners(ctx context.Context, lb *octavia.LoadBalancer, scoped bool) ([]listeners.Listener, string, error) {
	items, defaultPoolsObserved, err := p.octavia.ListListenersWithDefaultPools(ctx, listeners.ListOpts{LoadbalancerID: lb.ID, ProjectID: p.projectID})
	if err != nil {
		return nil, "", fmt.Errorf("list Gateway listeners: %w", err)
	}
	markedProtocol, err := scopedListenerProtocol(items)
	if err != nil {
		return nil, "", err
	}
	if !defaultPoolsObserved && (scoped || markedProtocol != "") {
		return nil, "", cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("load balancer %s has an incomplete scoped listener default pool inventory", lb.ID))
	}
	return items, markedProtocol, crossCheckChildInventory("load balancer", lb.ID, "listener", lb.Listeners, items, func(item listeners.Listener) string { return item.ID })
}

func (p *Provider) listGatewayPools(ctx context.Context, lb *octavia.LoadBalancer) ([]pools.Pool, error) {
	items, err := p.octavia.ListCompletePools(ctx, pools.ListOpts{LoadbalancerID: lb.ID, ProjectID: p.projectID})
	if err != nil {
		return nil, fmt.Errorf("list pools beneath Gateway: %w", err)
	}
	return items, crossCheckChildInventory("load balancer", lb.ID, "pool", lb.Pools, items, func(item pools.Pool) string { return item.ID })
}

// Octavia's parent response supplies arrays of ID references. A missing or
// null field supplies no additional observation. When the field is present,
// refuse to plan from two snapshots that disagree about their child IDs.
func crossCheckChildInventory[T any](parentKind, parentID, kind string, referenced, listed []T, idOf func(T) string) error {
	if referenced == nil {
		return nil
	}
	remaining := make(map[string]bool, len(referenced))
	for _, child := range referenced {
		remaining[idOf(child)] = true
	}
	for _, child := range listed {
		id := idOf(child)
		if id == "" || !remaining[id] {
			return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
				fmt.Errorf("%s %s and its %s list disagree about ID %q", parentKind, parentID, kind, id))
		}
		delete(remaining, id)
	}
	if len(remaining) != 0 {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("%s %s references %d unlisted %s resources", parentKind, parentID, len(remaining), kind))
	}
	return nil
}
