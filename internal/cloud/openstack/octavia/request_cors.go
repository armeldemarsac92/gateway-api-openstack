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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

const requestCORSField = "request_cors"

func readRequestCORS(raw json.RawMessage) *cloud.HTTPCORS {
	// An empty object is meaningful, whereas a null property within the
	// object is invalid under the provider's field contract.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil
	}
	for key, value := range fields {
		switch key {
		case "allow_origins", "allow_methods", "allow_headers", "expose_headers", "allow_credentials", "max_age":
			if string(value) == "null" {
				return nil
			}
		default:
			return nil
		}
	}
	var cors cloud.HTTPCORS
	if json.Unmarshal(raw, &cors) != nil || cors.Validate() != nil || fields["max_age"] != nil && cors.MaxAge == 0 {
		return nil
	}
	return &cors
}

func (o L7PolicyObservation) HasValidRequestCORS() bool {
	if o.RequestCORS == nil || o.AdminState == nil || o.Target == nil || o.RedirectRequestPresent ||
		o.Policy.RedirectURL != "" || o.Policy.RedirectPrefix != "" {
		return false
	}
	switch o.Policy.Action {
	case string(l7policies.ActionRedirectToPool):
		return o.Policy.RedirectPoolID != "" && !o.ExtendedTargetFields
	case string(ActionWeightedTargets):
		return o.Policy.RedirectPoolID == "" && string(o.PoolReference) == "null" &&
			len(o.Target.WeightedTargets) != 0 && o.Target.FixedResponse == nil
	default:
		return false
	}
}

// RequestCORSUpdate distinguishes omission from an explicit null. Updates
// keep the policy closed until the graph has verified its readback.
type RequestCORSUpdate struct {
	Value *cloud.HTTPCORS
	Tags  []string
}

func (update RequestCORSUpdate) write(body map[string]any) error {
	policy := body["l7policy"].(map[string]any)
	if update.Value != nil {
		if err := update.Value.Validate(); err != nil {
			return err
		}
		if action, explicit := policy["action"]; explicit && !forwardsToPools(fmt.Sprint(action)) {
			return fmt.Errorf("CORS requires a forwarding policy")
		}
	}
	policy[requestCORSField] = update.Value
	policy["admin_state_up"] = false
	policy["tags"] = update.Tags
	return nil
}
