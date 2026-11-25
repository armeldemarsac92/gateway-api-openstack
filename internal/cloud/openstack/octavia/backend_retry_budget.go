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
	"encoding/json"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const backendRetryBudgetField = "backend_retry_budget"

// BackendRetryBudgetUpdate distinguishes omission from an explicit null.
type BackendRetryBudgetUpdate struct {
	Value *cloud.BackendRetryBudget
}

// PoolBackendRetryBudget records present but undecodable readback separately
// from absent state. Octavia omits the field when no budget is stored.
type PoolBackendRetryBudget struct {
	Present bool
	Value   *cloud.BackendRetryBudget
}

func decodeBackendRetryBudget(raw json.RawMessage) PoolBackendRetryBudget {
	if len(raw) == 0 || string(raw) == "null" {
		return PoolBackendRetryBudget{}
	}
	result := PoolBackendRetryBudget{Present: true}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 5 {
		return result
	}
	for _, field := range []string{"service_uid", "budget_percent", "budget_interval_ms", "min_retry_count", "min_retry_interval_ms"} {
		if value := fields[field]; len(value) == 0 || string(value) == "null" {
			return result
		}
	}
	var budget cloud.BackendRetryBudget
	if json.Unmarshal(raw, &budget) == nil && budget.Valid() {
		result.Value = &budget
	}
	return result
}
