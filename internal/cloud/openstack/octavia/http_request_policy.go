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
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

const httpRequestPolicyField = "http_request_policy"

func readHTTPRequestPolicy(raw json.RawMessage) *cloud.HTTPRequestPolicy {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil
	}
	for key, value := range fields {
		switch key {
		case "request_timeout_ms", "backend_request_timeout_ms":
			if string(value) == "null" {
				return nil
			}
		case "retry":
			var retry map[string]json.RawMessage
			if json.Unmarshal(value, &retry) != nil || retry == nil {
				return nil
			}
			for key, value := range retry {
				if key != "attempts" && key != "codes" && key != "backoff_ms" || string(value) == "null" {
					return nil
				}
			}
		default:
			return nil
		}
	}
	var policy cloud.HTTPRequestPolicy
	if json.Unmarshal(raw, &policy) != nil || policy.Validate() != nil {
		return nil
	}
	if policy.Retry != nil {
		slices.Sort(policy.Retry.Codes)
	}
	return &policy
}

func (o L7PolicyObservation) HasValidHTTPRequestPolicy() bool {
	if o.HTTPRequestPolicy == nil || o.AdminState == nil || o.Target == nil || o.RedirectRequestPresent ||
		o.Policy.RedirectURL != "" || o.Policy.RedirectPrefix != "" || o.Policy.RedirectHttpCode != 0 {
		return false
	}
	switch o.Policy.Action {
	case string(l7policies.ActionRedirectToPool):
		return o.Policy.RedirectPoolID != "" && !o.ExtendedTargetFields
	case string(ActionWeightedTargets):
		return string(o.PoolReference) == "null" && o.Target.FixedResponse == nil &&
			len(o.Target.WeightedTargets) != 0
	default:
		return false
	}
}

// HTTPRequestPolicyUpdate distinguishes omission from explicit null.
type HTTPRequestPolicyUpdate struct {
	Value *cloud.HTTPRequestPolicy
}

func (update HTTPRequestPolicyUpdate) write(body map[string]any) error {
	if update.Value != nil {
		if err := update.Value.Validate(); err != nil {
			return fmt.Errorf("invalid %s: %w", httpRequestPolicyField, err)
		}
	}
	body["l7policy"].(map[string]any)[httpRequestPolicyField] = update.Value
	return nil
}
