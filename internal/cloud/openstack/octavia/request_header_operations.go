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

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

type policyWithRequestHeaders struct {
	update     l7policies.UpdateOptsBuilder
	operations []cloud.HTTPHeaderOperation
}

func (opts policyWithRequestHeaders) ToL7PolicyUpdateMap() (map[string]any, error) {
	body, err := opts.update.ToL7PolicyUpdateMap()
	if err == nil {
		writePolicyRequestHeaderOperations(body, opts.operations)
	}
	return body, err
}

// UpdatePolicyRequestHeaders can change the target and header operations in
// one update. A nil slice omits the field and an empty slice clears it.
func (s *Service) UpdatePolicyRequestHeaders(ctx context.Context, id string, opts l7policies.UpdateOptsBuilder, operations []cloud.HTTPHeaderOperation) (*l7policies.L7Policy, error) {
	return l7policies.Update(ctx, s.client, id, policyWithRequestHeaders{update: opts, operations: operations}).Extract()
}

func writePolicyRequestHeaderOperations(body map[string]any, operations []cloud.HTTPHeaderOperation) {
	if operations == nil {
		return
	}
	entries := make([]map[string]any, 0, len(operations))
	for _, operation := range operations {
		entry := map[string]any{"operation": operation.Operation, "name": operation.Name}
		if operation.Operation != cloud.HTTPHeaderRemove {
			entry["value"] = operation.Value
		}
		entries = append(entries, entry)
	}
	body["l7policy"].(map[string]any)["request_header_operations"] = entries
}

func readPolicyRequestHeaderOperations(raw json.RawMessage) ([]cloud.HTTPHeaderOperation, error) {
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil || entries == nil {
		return nil, errors.New("request_header_operations must be an array")
	}
	operations := make([]cloud.HTTPHeaderOperation, 0, len(entries))
	for _, entry := range entries {
		var operation *cloud.HTTPHeaderAction
		var name *string
		if json.Unmarshal(entry["operation"], &operation) != nil || operation == nil ||
			json.Unmarshal(entry["name"], &name) != nil || name == nil {
			return nil, errors.New("request header operation requires an action and a name")
		}
		decoded := cloud.HTTPHeaderOperation{Operation: *operation, Name: *name}
		if *operation == cloud.HTTPHeaderRemove {
			if len(entry) != 2 {
				return nil, errors.New("REMOVE takes a name without a value")
			}
		} else {
			var value *string
			if len(entry) != 3 || json.Unmarshal(entry["value"], &value) != nil || value == nil {
				return nil, errors.New("SET and ADD require a literal value")
			}
			decoded.Value = *value
		}
		operations = append(operations, decoded)
	}
	if len(operations) == 0 {
		return operations, nil
	}
	return cloud.NormalizeHTTPHeaderOperations(operations)
}
