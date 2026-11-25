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
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const tcpConnectionRejectionField = "tcp_connection_rejection"

// PoolTCPConnectionRejection distinguishes stock omission, explicit null and invalid readback.
type PoolTCPConnectionRejection struct {
	Present   bool
	Invalid   bool
	Value     *cloud.TCPConnectionRejection
	UpdatedAt time.Time
}

func decodeTCPConnectionRejection(raw json.RawMessage) PoolTCPConnectionRejection {
	if len(raw) == 0 {
		return PoolTCPConnectionRejection{}
	}
	result := PoolTCPConnectionRejection{Present: true}
	if string(raw) == "null" {
		return result
	}
	result.Invalid = true
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if start, err := decoder.Token(); err != nil || start != json.Delim('{') {
		return result
	}
	weights := make(map[string]int, 2)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil || key != "reject_weight" && key != "total_weight" {
			return result
		}
		var weight *int
		field := key.(string)
		if _, duplicate := weights[field]; duplicate || decoder.Decode(&weight) != nil || weight == nil {
			return result
		}
		weights[field] = *weight
	}
	value := cloud.TCPConnectionRejection{RejectWeight: weights["reject_weight"], TotalWeight: weights["total_weight"]}
	if len(weights) == 2 && value.Valid() {
		result.Value, result.Invalid = &value, false
	}
	return result
}

// ReplaceTCPMembersAndRejection updates a complete set of real members and one
// rejection fraction. A nil fraction explicitly clears the stored value.
func (s *Service) ReplaceTCPMembersAndRejection(ctx context.Context, poolID string, members []pools.BatchUpdateMemberOpts, rejection *cloud.TCPConnectionRejection) error {
	items := make([]map[string]any, 0, len(members))
	for _, member := range members {
		item, err := member.ToBatchMemberUpdateMap()
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	body := map[string]any{"members": items, tcpConnectionRejectionField: rejection}
	_, err := s.client.Put(ctx, s.client.ServiceURL("lbaas", "pools", poolID, "members"), body, nil, &gophercloud.RequestOpts{OkCodes: []int{202}})
	return err
}
