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
	weights := decodeBatchFraction(raw, "reject_weight", "total_weight")
	value := cloud.TCPConnectionRejection{RejectWeight: weights["reject_weight"], TotalWeight: weights["total_weight"]}
	result.Invalid = len(weights) != 2 || !value.Valid()
	if !result.Invalid {
		result.Value = &value
	}
	return result
}

func decodeBatchFraction(raw json.RawMessage, numerator, denominator string) map[string]int {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if start, err := decoder.Token(); err != nil || start != json.Delim('{') {
		return nil
	}
	weights := make(map[string]int, 2)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil || key != numerator && key != denominator {
			return nil
		}
		var weight *int
		field := key.(string)
		if _, duplicate := weights[field]; duplicate || decoder.Decode(&weight) != nil || weight == nil {
			return nil
		}
		weights[field] = *weight
	}
	return weights
}

// ReplaceTCPMembersAndRejection updates a complete set of real members and one
// rejection fraction. A nil fraction explicitly clears the stored value.
func (s *Service) ReplaceTCPMembersAndRejection(ctx context.Context, poolID string, members []pools.BatchUpdateMemberOpts, rejection *cloud.TCPConnectionRejection) error {
	return s.replaceMemberBatch(ctx, poolID, members, tcpConnectionRejectionField, rejection)
}

func (s *Service) replaceMemberBatch(ctx context.Context, poolID string, members []pools.BatchUpdateMemberOpts, extension string, fraction any) error {
	items := make([]map[string]any, 0, len(members))
	for _, member := range members {
		item, err := member.ToBatchMemberUpdateMap()
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	body := map[string]any{"members": items, extension: fraction}
	_, err := s.client.Put(ctx, s.client.ServiceURL("lbaas", "pools", poolID, "members"), body, nil, &gophercloud.RequestOpts{OkCodes: []int{202}})
	return err
}
