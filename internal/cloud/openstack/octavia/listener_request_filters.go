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

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

const GatewayCapabilityListenerRequestFilters = "listener_request_filters_v1"

type ListenerRequestFilterSection struct {
	Hostname string                  `json:"hostname"`
	Reject   bool                    `json:"reject,omitempty"`
	Requests []ListenerRequestFilter `json:"requests,omitempty"`
}

type ListenerRequestFilter struct {
	Type string                   `json:"type"`
	HTTP ListenerHTTPExternalAuth `json:"http"`
}

type ListenerHTTPExternalAuth struct {
	PoolID                 string   `json:"pool_id"`
	PathPrefix             string   `json:"path_prefix,omitempty"`
	AllowedRequestHeaders  []string `json:"allowed_request_headers,omitempty"`
	AllowedResponseHeaders []string `json:"allowed_response_headers,omitempty"`
}

type ListenerRequestFilterObservation struct {
	Sections []ListenerRequestFilterSection
	Readable bool
}

func decodeListenerRequestFilters(raw json.RawMessage) ListenerRequestFilterObservation {
	var sections []ListenerRequestFilterSection
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&sections)
	return ListenerRequestFilterObservation{Sections: sections, Readable: err == nil && sections != nil}
}

type listenerRequestFilterUpdate []ListenerRequestFilterSection

func (sections listenerRequestFilterUpdate) ToListenerUpdateMap() (map[string]any, error) {
	if sections == nil {
		sections = listenerRequestFilterUpdate{}
	}
	return map[string]any{"listener": map[string]any{"request_filter_sections": sections}}, nil
}

func (s *Service) SetListenerRequestFilters(ctx context.Context, listenerID string, sections []ListenerRequestFilterSection) error {
	_, err := listeners.Update(ctx, s.client, listenerID, listenerRequestFilterUpdate(sections)).Extract()
	return err
}
