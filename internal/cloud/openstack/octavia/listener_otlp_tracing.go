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
	"crypto/sha256"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/apierrors"
	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

const (
	GatewayCapabilityListenerOTLPTracing        = "listener_otlp_tracing_v1"
	GatewayCapabilityListenerOTLPSampling       = "listener_otlp_sampling_v1"
	GatewayCapabilityListenerOTLPSpanAttributes = "listener_otlp_span_attributes_v1"
	GatewayCapabilityListenerOTLPExportHeaders  = "listener_otlp_export_headers_v1"
)

type ListenerOTLPTracing struct {
	CollectorPoolID string                         `json:"collector_pool_id"`
	ServiceName     string                         `json:"service_name"`
	SpanName        string                         `json:"span_name"`
	Sampling        *cloud.TelemetrySampling       `json:"sampling,omitempty"`
	Attributes      []cloud.TelemetryAttribute     `json:"attributes,omitempty"`
	Headers         cloud.TelemetryExporterHeaders `json:"headers,omitempty"`
}

type ListenerOTLPTracingObservation struct {
	Value            *ListenerOTLPTracing
	Readable         bool
	Status           string
	StatusMalformed  bool
	HeadersSHA256    string
	HeadersMalformed bool
}

func decodeListenerOTLPTracing(raw, status json.RawMessage) ListenerOTLPTracingObservation {
	var observed ListenerOTLPTracingObservation
	// A malformed or omitted status never proves application on Amphora.
	if len(status) != 0 {
		observed.StatusMalformed = json.Unmarshal(status, &observed.Status) != nil || observed.Status == ""
	}
	if len(raw) == 0 {
		return observed
	}
	// A malformed extension must leave the collector identity available for withdrawal.
	var wire *struct {
		ListenerOTLPTracing
		RawSampling     json.RawMessage `json:"sampling"`
		RawAttributes   json.RawMessage `json:"attributes"`
		RawHeaders      json.RawMessage `json:"headers"`
		RawHeaderDigest json.RawMessage `json:"headers_sha256"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return observed
	}
	if wire != nil {
		value := &wire.ListenerOTLPTracing
		id, err := uuid.Parse(value.CollectorPoolID)
		if err != nil || id == uuid.Nil || value.ServiceName == "" || value.SpanName == "" {
			return observed
		}
		observed.Value = value
		// A projected write value must never come back from the public API.
		if wire.RawHeaders != nil {
			observed.HeadersMalformed = true
			return observed
		}
		if wire.RawHeaderDigest != nil {
			if err := json.Unmarshal(wire.RawHeaderDigest, &observed.HeadersSHA256); err != nil ||
				len(observed.HeadersSHA256) != sha256.Size*2 || strings.Trim(observed.HeadersSHA256, "0123456789abcdef") != "" {
				observed.HeadersMalformed = true
				return observed
			}
		}
		if len(wire.RawAttributes) != 0 {
			var valid bool
			value.Attributes, valid = decodeTelemetryAttributes(wire.RawAttributes)
			if !valid {
				return observed
			}
		}
		if len(wire.RawSampling) != 0 {
			var sampling struct {
				Base   cloud.TelemetryRate `json:"base"`
				Parent json.RawMessage     `json:"parent"`
			}
			decoder := json.NewDecoder(bytes.NewReader(wire.RawSampling))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&sampling); err != nil || !sampling.Base.Valid() {
				return observed
			}
			decoded := &cloud.TelemetrySampling{Base: sampling.Base}
			if len(sampling.Parent) != 0 {
				var parent cloud.TelemetryRate
				if err := json.Unmarshal(sampling.Parent, &parent); err != nil {
					return observed
				}
				decoded.Parent = &parent
			}
			value.Sampling = decoded
		}
	}
	observed.Readable = true
	return observed
}

func decodeTelemetryAttributes(raw json.RawMessage) ([]cloud.TelemetryAttribute, bool) {
	var rows []map[string]string
	if err := jsonv2.Unmarshal(raw, &rows); err != nil || rows == nil {
		return nil, false
	}
	attrs := make([]cloud.TelemetryAttribute, 0, len(rows))
	for _, row := range rows {
		// Each entry must contain the name, the discriminator and just its chosen value.
		if len(row) != 3 {
			return nil, false
		}
		attrs = append(attrs, cloud.TelemetryAttribute{Name: row["name"], SourceType: row["source_type"],
			LiteralValue: row["literal_value"], HeaderName: row["header_name"], AttributeKey: row["attribute_key"]})
	}
	return cloud.CanonicalTelemetryAttributes(attrs)
}

type listenerOTLPTracingUpdate struct {
	value *ListenerOTLPTracing
	tags  []string
}

func (update listenerOTLPTracingUpdate) ToListenerUpdateMap() (map[string]any, error) {
	fields := map[string]any{"otlp_tracing": update.value}
	if update.tags != nil {
		fields["tags"] = update.tags
	}
	return map[string]any{"listener": fields}, nil
}

func (s *Service) SetListenerOTLPTracing(ctx context.Context, listenerID string, value *ListenerOTLPTracing, tags []string) error {
	_, err := listeners.Update(ctx, s.client, listenerID, listenerOTLPTracingUpdate{value, tags}).Extract()
	if value != nil && len(value.Headers) != 0 {
		return apierrors.RedactOctaviaMutation(err, "update Octavia collector request headers")
	}
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
