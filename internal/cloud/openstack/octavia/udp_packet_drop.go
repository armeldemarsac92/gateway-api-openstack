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
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

const udpPacketDropField = "udp_packet_drop"

// PoolUDPPacketDrop carries the explicit fraction and its staging time.
type PoolUDPPacketDrop struct {
	Present   bool
	Invalid   bool
	Value     cloud.UDPPacketDrop
	UpdatedAt time.Time
}

func decodeUDPPacketDrop(raw json.RawMessage) PoolUDPPacketDrop {
	if len(raw) == 0 {
		return PoolUDPPacketDrop{}
	}
	weights := decodeBatchFraction(raw, "numerator", "denominator")
	value := cloud.UDPPacketDrop{Numerator: weights["numerator"], Denominator: weights["denominator"]}
	return PoolUDPPacketDrop{Present: true, Invalid: len(weights) != 2 || !value.Valid(), Value: value}
}

// ReplaceUDPMembersAndDrop atomically replaces every real member and the drop share.
func (s *Service) ReplaceUDPMembersAndDrop(ctx context.Context, poolID string, members []pools.BatchUpdateMemberOpts, drop cloud.UDPPacketDrop) error {
	return s.replaceMemberBatch(ctx, poolID, members, udpPacketDropField, drop)
}
