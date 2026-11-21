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

package octavia

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

// ListListeners lists and extracts listeners matching opts.
func (s *Service) ListListeners(ctx context.Context, opts listeners.ListOpts) ([]listeners.Listener, error) {
	inventory, err := s.ListListenerInventory(ctx, opts)
	return inventory.Items, err
}

// ListenerInventory retains field presence needed to prove a listener has
// been disabled and detached. Both zero values otherwise accept missing JSON.
type ListenerInventory struct {
	Items                []listeners.Listener
	DefaultPoolsObserved bool
	AdminStatesObserved  map[string]bool
}

func (s *Service) ListListenerInventory(ctx context.Context, opts listeners.ListOpts) (ListenerInventory, error) {
	pages, err := listeners.List(s.client, opts).AllPages(ctx)
	if err != nil {
		return ListenerInventory{}, fmt.Errorf("list listeners: %w", err)
	}
	var response struct {
		Listeners []struct {
			listeners.Listener
			DefaultPool  json.RawMessage `json:"default_pool_id"`
			AdminStateUp *bool           `json:"admin_state_up"`
		} `json:"listeners"`
	}
	if err := pages.(listeners.ListenerPage).ExtractInto(&response); err != nil {
		return ListenerInventory{}, fmt.Errorf("extract listeners: %w", err)
	}
	inventory := ListenerInventory{DefaultPoolsObserved: true}
	if len(response.Listeners) > 0 {
		inventory.Items = make([]listeners.Listener, len(response.Listeners))
		inventory.AdminStatesObserved = make(map[string]bool, len(response.Listeners))
	}
	for i, observed := range response.Listeners {
		item := &inventory.Items[i]
		*item = observed.Listener
		if observed.AdminStateUp != nil {
			item.AdminStateUp = *observed.AdminStateUp
			inventory.AdminStatesObserved[item.ID] = true
		}
		switch string(observed.DefaultPool) {
		case "null":
			// The public API explicitly reports a detached listener.
		case "", `""`:
			inventory.DefaultPoolsObserved = false
		default:
			if err := json.Unmarshal(observed.DefaultPool, &item.DefaultPoolID); err != nil {
				return ListenerInventory{}, fmt.Errorf("extract listener default pool: %w", err)
			}
		}
	}
	return inventory, nil
}

// CreateListener creates one listener.
func (s *Service) CreateListener(ctx context.Context, opts listeners.CreateOpts) (*listeners.Listener, error) {
	return listeners.Create(ctx, s.client, opts).Extract()
}

// UpdateListener updates one listener.
func (s *Service) UpdateListener(ctx context.Context, id string, opts listeners.UpdateOpts) (*listeners.Listener, error) {
	return listeners.Update(ctx, s.client, id, opts).Extract()
}

// DeleteListener deletes one listener.
func (s *Service) DeleteListener(ctx context.Context, id string) error {
	return listeners.Delete(ctx, s.client, id).ExtractErr()
}
