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
	"errors"

	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

const (
	MaxTCPDefaultTargets      = 16
	MaxTCPDefaultTargetWeight = 1_000_000
)

type TCPDefaultTarget struct {
	PoolID string `json:"pool_id,omitempty"`
	Reject bool   `json:"reject,omitempty"`
	Weight int    `json:"weight"`
}

type TCPDefaultTargetObservation struct {
	Present           bool
	Invalid           bool
	Targets           []TCPDefaultTarget
	PublicationStatus string
	Retryable         *bool
}

func validTCPDefaultTargets(targets []TCPDefaultTarget) bool {
	if len(targets) > MaxTCPDefaultTargets {
		return false
	}
	for _, target := range targets {
		if target.Weight < 1 || target.Weight > MaxTCPDefaultTargetWeight || target.Reject == (target.PoolID != "") {
			return false
		}
		if !target.Reject {
			id, err := uuid.Parse(target.PoolID)
			if err != nil || id == uuid.Nil || id.String() != target.PoolID {
				return false
			}
		}
	}
	return true
}

func decodeTCPDefaultTargets(raw, publication, retryable json.RawMessage) TCPDefaultTargetObservation {
	targets := decodeTCPDefaultTargetList(raw)
	if publication != nil {
		if err := json.Unmarshal(publication, &targets.PublicationStatus); err != nil || !targets.Present ||
			targets.PublicationStatus != "ACTIVE" && targets.PublicationStatus != "ERROR" && targets.PublicationStatus != "PENDING_UPDATE" {
			targets.Invalid = true
		}
	}
	if retryable != nil && (json.Unmarshal(retryable, &targets.Retryable) != nil || targets.Retryable == nil || publication == nil) {
		targets.Invalid = true
	}
	return targets
}

func decodeTCPDefaultTargetList(raw json.RawMessage) TCPDefaultTargetObservation {
	if raw == nil {
		return TCPDefaultTargetObservation{}
	}
	invalid := TCPDefaultTargetObservation{Present: true, Invalid: true}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil || rows == nil || len(rows) > MaxTCPDefaultTargets {
		return invalid
	}
	targets := make([]TCPDefaultTarget, len(rows))
	for i, row := range rows {
		target := &targets[i]
		if len(row) != 2 || json.Unmarshal(row["weight"], &target.Weight) != nil {
			return invalid
		}
		if raw, reject := row["reject"]; reject {
			if json.Unmarshal(raw, &target.Reject) != nil || !target.Reject {
				return invalid
			}
		} else if json.Unmarshal(row["pool_id"], &target.PoolID) != nil || target.PoolID == "" {
			return invalid
		}
	}
	if !validTCPDefaultTargets(targets) {
		return invalid
	}
	return TCPDefaultTargetObservation{Present: true, Targets: targets}
}

type tcpDefaultTargetsUpdate struct {
	targets     []TCPDefaultTarget
	clearScalar bool
}

func (opts tcpDefaultTargetsUpdate) ToListenerUpdateMap() (map[string]any, error) {
	if !validTCPDefaultTargets(opts.targets) {
		return nil, errors.New("invalid TCP default targets")
	}
	if opts.targets == nil {
		opts.targets = []TCPDefaultTarget{}
	}
	body := map[string]any{"tcp_default_targets": opts.targets}
	if opts.clearScalar {
		body["default_pool_id"] = nil
		body["tcp_default_pool_tls"] = false
	}
	return map[string]any{"listener": body}, nil
}

// SetTCPDefaultTargets replaces all ordered targets. An empty list withdraws
// the weighted association. clearScalar transitions from a scalar default in
// the same provider operation.
func (s *Service) SetTCPDefaultTargets(ctx context.Context, id string, targets []TCPDefaultTarget, clearScalar bool) error {
	_, err := listeners.Update(ctx, s.client, id, tcpDefaultTargetsUpdate{targets: targets, clearScalar: clearScalar}).Extract()
	return err
}
