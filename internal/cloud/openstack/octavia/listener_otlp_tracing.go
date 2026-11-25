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

	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

const GatewayCapabilityListenerOTLPTracing = "listener_otlp_tracing_v1"

type ListenerOTLPTracing struct {
	CollectorPoolID string `json:"collector_pool_id"`
	ServiceName     string `json:"service_name"`
	SpanName        string `json:"span_name"`
}

type ListenerOTLPTracingObservation struct {
	Value    *ListenerOTLPTracing
	Readable bool
}

func decodeListenerOTLPTracing(raw json.RawMessage) ListenerOTLPTracingObservation {
	var value *ListenerOTLPTracing
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return ListenerOTLPTracingObservation{}
	}
	if value != nil {
		id, err := uuid.Parse(value.CollectorPoolID)
		if err != nil || id == uuid.Nil || value.ServiceName == "" || value.SpanName == "" {
			return ListenerOTLPTracingObservation{}
		}
	}
	return ListenerOTLPTracingObservation{Value: value, Readable: true}
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
