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
	"net/http"

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
	DefaultHTTPResponseCodes map[string]int
	Items                    []listeners.Listener
	DefaultPoolsObserved     bool
	AdminStatesObserved      map[string]bool
}

func (s *Service) ListListenerInventory(ctx context.Context, opts listeners.ListOpts) (ListenerInventory, error) {
	pages, err := listeners.List(s.client, opts).AllPages(ctx)
	if err != nil {
		return ListenerInventory{}, fmt.Errorf("list listeners: %w", err)
	}
	var response struct {
		Listeners []struct {
			listeners.Listener
			DefaultHTTPResponse *struct {
				StatusCode int `json:"status_code"`
			} `json:"default_http_response"`
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
		if observed.DefaultHTTPResponse != nil {
			if inventory.DefaultHTTPResponseCodes == nil {
				inventory.DefaultHTTPResponseCodes = make(map[string]int)
			}
			inventory.DefaultHTTPResponseCodes[item.ID] = observed.DefaultHTTPResponse.StatusCode
		}
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

type notFoundListenerOptions struct{ listeners.CreateOpts }

func (opts notFoundListenerOptions) ToListenerCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToListenerCreateMap()
	if err != nil {
		return nil, err
	}
	body["listener"].(map[string]any)["default_http_response"] = map[string]int{"status_code": http.StatusNotFound}
	return body, nil
}

func (notFoundListenerOptions) ToListenerUpdateMap() (map[string]any, error) {
	return map[string]any{"listener": map[string]any{"default_http_response": map[string]int{"status_code": http.StatusNotFound}}}, nil
}

// CreateListenerWithNotFound creates an HTTP-family listener on the opt-in provider.
func (s *Service) CreateListenerWithNotFound(ctx context.Context, opts listeners.CreateOpts) (*listeners.Listener, error) {
	return listeners.Create(ctx, s.client, notFoundListenerOptions{opts}).Extract()
}

// SetListenerNotFound restores the unmatched response on an existing owned listener.
func (s *Service) SetListenerNotFound(ctx context.Context, id string) error {
	_, err := listeners.Update(ctx, s.client, id, notFoundListenerOptions{}).Extract()
	return err
}
