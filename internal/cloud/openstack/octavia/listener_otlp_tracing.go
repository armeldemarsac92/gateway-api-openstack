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
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

const GatewayCapabilityListenerOTLPTracing = "listener_otlp_tracing_v1"

type ListenerOTLPTracing struct {
	CollectorPoolID string `json:"collector_pool_id"`
	ServiceName     string `json:"service_name"`
	SpanName        string `json:"span_name"`
}

type ListenerOTLPTracingObservation struct {
	Value           *ListenerOTLPTracing
	Readable        bool
	Status          string
	StatusMalformed bool
}

func decodeListenerOTLPTracing(raw, status json.RawMessage) ListenerOTLPTracingObservation {
	var observed ListenerOTLPTracingObservation
	// A malformed or omitted status never proves application on Amphora.
	if len(status) != 0 {
		observed.StatusMalformed = json.Unmarshal(status, &observed.Status) != nil
	}
	if len(raw) == 0 {
		return observed
	}
	var value *ListenerOTLPTracing
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return observed
	}
	if value != nil {
		id, err := uuid.Parse(value.CollectorPoolID)
		if err != nil || id == uuid.Nil || value.ServiceName == "" || value.SpanName == "" {
			return observed
		}
	}
	observed.Value, observed.Readable = value, true
	return observed
}

type listenerOTLPTracingUpdate struct {
	value *ListenerOTLPTracing
	tags  []string
}

func (update listenerOTLPTracingUpdate) ToListenerUpdateMap() (map[string]any, error) {
	return map[string]any{"listener": map[string]any{"otlp_tracing": update.value, "tags": update.tags}}, nil
}

func (s *Service) SetListenerOTLPTracing(ctx context.Context, listenerID string, value *ListenerOTLPTracing, tags []string) error {
	_, err := listeners.Update(ctx, s.client, listenerID, listenerOTLPTracingUpdate{value, tags}).Extract()
	return err
}

// ListTelemetryListenerInventory keeps listener identities independently of
// unrelated unreadable rows. Pool retirement still needs a complete view.
func (s *Service) ListTelemetryListenerInventory(ctx context.Context, opts listeners.ListOpts) (ListenerInventory, error) {
	inventory := ListenerInventory{OTLPTracing: map[string]ListenerOTLPTracingObservation{}, DefaultPoolsObserved: true}
	query, err := opts.ToListenerListQuery()
	if err != nil {
		return inventory, err
	}
	for pageURL := s.client.ServiceURL("lbaas", "listeners") + query; pageURL != ""; {
		var response struct {
			Listeners []json.RawMessage  `json:"listeners"`
			Links     []gophercloud.Link `json:"listeners_links"`
		}
		// The SDK pager unmarshals every stock listener before exposing its
		// raw fields. Decode rows separately so a neighbor cannot veto a
		// provable listener withdrawal.
		if _, err := s.client.Get(ctx, pageURL, &response, &gophercloud.RequestOpts{OkCodes: []int{200, 204, 300}}); err != nil {
			return inventory, fmt.Errorf("list telemetry listeners: %w", err)
		}
		if response.Listeners == nil {
			return inventory, missingCollection("listeners")
		}
		for _, row := range response.Listeners {
			var listener struct {
				listeners.Listener
				DefaultPool json.RawMessage `json:"default_pool_id"`
				Tracing     json.RawMessage `json:"otlp_tracing"`
				Status      json.RawMessage `json:"otlp_tracing_status"`
			}
			if err := json.Unmarshal(row, &listener); err != nil {
				inventory.DefaultPoolsObserved = false
				continue
			}
			switch string(listener.DefaultPool) {
			case "null":
			case "", `""`:
				inventory.DefaultPoolsObserved = false
			default:
				if err := json.Unmarshal(listener.DefaultPool, &listener.DefaultPoolID); err != nil {
					inventory.DefaultPoolsObserved = false
				}
			}
			if listener.Tracing != nil || listener.Status != nil {
				inventory.OTLPTracing[listener.ID] = decodeListenerOTLPTracing(listener.Tracing, listener.Status)
			}
			inventory.Items = append(inventory.Items, listener.Listener)
		}
		pageURL, err = gophercloud.ExtractNextURL(response.Links)
		if err != nil {
			return inventory, fmt.Errorf("list telemetry listeners: %w", err)
		}
	}
	return inventory, nil
}
