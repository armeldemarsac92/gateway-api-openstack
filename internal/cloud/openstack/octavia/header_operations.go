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
	"fmt"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const (
	requestHeaderOperationsField  = "request_header_operations"
	responseHeaderOperationsField = "response_header_operations"
)

// PolicyFilters preserves omission and explicit removal independently.
type PolicyFilters struct {
	Request, Response []cloud.HTTPHeaderOperation
	URLRewrite        *RequestURLRewriteUpdate
	CORS              *RequestCORSUpdate
	RequestPolicy     *HTTPRequestPolicyUpdate
	RequestMirrors    *RequestMirrorsUpdate
	Tags              []string
}

func (filters PolicyFilters) write(body map[string]any) error {
	writePolicyHeaderOperations(body, requestHeaderOperationsField, filters.Request)
	writePolicyHeaderOperations(body, responseHeaderOperationsField, filters.Response)
	if filters.URLRewrite != nil {
		if err := filters.URLRewrite.write(body); err != nil {
			return err
		}
	}
	if filters.CORS != nil {
		if err := filters.CORS.write(body); err != nil {
			return err
		}
	}
	if filters.RequestPolicy != nil {
		if err := filters.RequestPolicy.write(body); err != nil {
			return err
		}
	}
	if filters.RequestMirrors != nil {
		if err := filters.RequestMirrors.write(body); err != nil {
			return err
		}
	}
	if filters.Tags != nil {
		body["l7policy"].(map[string]any)["tags"] = filters.Tags
	}
	return nil
}

type policyWithFilters struct {
	update  l7policies.UpdateOptsBuilder
	filters PolicyFilters
}

func (opts policyWithFilters) ToL7PolicyUpdateMap() (map[string]any, error) {
	body, err := opts.update.ToL7PolicyUpdateMap()
	if err == nil {
		err = opts.filters.write(body)
	}
	return body, err
}

// UpdatePolicyFilters can change the target and forwarding filters in one update.
func (s *Service) UpdatePolicyFilters(ctx context.Context, id string, opts l7policies.UpdateOptsBuilder, filters PolicyFilters) (*l7policies.L7Policy, error) {
	return l7policies.Update(ctx, s.client, id, policyWithFilters{update: opts, filters: filters}).Extract()
}

func writePolicyHeaderOperations(body map[string]any, field string, operations []cloud.HTTPHeaderOperation) {
	if operations == nil {
		return
	}
	body["l7policy"].(map[string]any)[field] = headerOperationEntries(operations)
}

func headerOperationEntries(operations []cloud.HTTPHeaderOperation) []map[string]any {
	entries := make([]map[string]any, 0, len(operations))
	for _, operation := range operations {
		entry := map[string]any{"operation": operation.Operation, "name": operation.Name}
		if operation.Operation != cloud.HTTPHeaderRemove {
			entry["value"] = operation.Value
		}
		entries = append(entries, entry)
	}
	return entries
}

func readPolicyHeaderOperations(raw json.RawMessage, field string) ([]cloud.HTTPHeaderOperation, error) {
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil || entries == nil {
		return nil, fmt.Errorf("%s must be an array", field)
	}
	operations := make([]cloud.HTTPHeaderOperation, 0, len(entries))
	for _, entry := range entries {
		var operation *cloud.HTTPHeaderAction
		var name *string
		if json.Unmarshal(entry["operation"], &operation) != nil || operation == nil ||
			json.Unmarshal(entry["name"], &name) != nil || name == nil {
			return nil, errors.New("header operation requires an action and a name")
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
	if field == responseHeaderOperationsField {
		return cloud.NormalizeHTTPResponseHeaderOperations(operations)
	}
	return cloud.NormalizeHTTPHeaderOperations(operations)
}
