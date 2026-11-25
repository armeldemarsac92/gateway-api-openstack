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

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"k8s.io/utils/ptr"
)

const requestURLRewriteField = "request_url_rewrite"

// RequestURLRewrite changes the request sent to the selected pool. The
// provider retains the original query and applies the rewrite after matching.
type RequestURLRewrite struct {
	Hostname        string                 `json:"hostname,omitempty"`
	ReplaceFullPath *string                `json:"replace_full_path,omitempty"`
	ReplacePrefix   *PathPrefixReplacement `json:"replace_prefix,omitempty"`
}

func (r RequestURLRewrite) Valid() bool {
	return (r.Hostname != "" || r.ReplaceFullPath != nil || r.ReplacePrefix != nil) &&
		validURLModifier(r.Hostname, r.ReplaceFullPath, r.ReplacePrefix)
}

func (r *RequestURLRewrite) Equal(other *RequestURLRewrite) bool {
	if r == nil || other == nil {
		return r == other
	}
	return r.Hostname == other.Hostname && ptr.Equal(r.ReplaceFullPath, other.ReplaceFullPath) && ptr.Equal(r.ReplacePrefix, other.ReplacePrefix)
}

func readRequestURLRewrite(raw json.RawMessage) *RequestURLRewrite {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	var rewrite RequestURLRewrite
	for key, value := range fields {
		switch key {
		case "hostname":
			if json.Unmarshal(value, &rewrite.Hostname) != nil || rewrite.Hostname == "" {
				return nil
			}
		case "replace_full_path":
			if json.Unmarshal(value, &rewrite.ReplaceFullPath) != nil || rewrite.ReplaceFullPath == nil {
				return nil
			}
		case "replace_prefix":
			rewrite.ReplacePrefix = readPathPrefixReplacement(value)
			if rewrite.ReplacePrefix == nil {
				return nil
			}
		default:
			return nil
		}
	}
	if !rewrite.Valid() {
		return nil
	}
	return &rewrite
}

func (o L7PolicyObservation) HasValidRequestURLRewrite() bool {
	if o.RequestURLRewrite == nil || o.AdminState == nil || o.Target == nil ||
		o.Policy.RedirectURL != "" || o.Policy.RedirectPrefix != "" || o.RedirectRequestPresent {
		return false
	}
	switch o.Policy.Action {
	case string(l7policies.ActionRedirectToPool):
		return o.Policy.RedirectPoolID != "" && o.Target.FixedResponse == nil && o.Target.WeightedTargets == nil && !o.ExtendedTargetFields
	case string(ActionWeightedTargets):
		return o.Policy.RedirectPoolID == "" && o.Target.FixedResponse == nil && string(o.PoolReference) == "null" && len(o.Target.WeightedTargets) != 0
	case string(ActionFixedResponse):
		return o.RequestMirrorsErr == nil && len(o.RequestMirrors) != 0 && o.Target.IsFixedServerError() &&
			o.Policy.RedirectPoolID == "" && len(o.Target.WeightedTargets) == 0
	default:
		return false
	}
}

func forwardsToPools(action string) bool {
	return action == string(l7policies.ActionRedirectToPool) || action == string(ActionWeightedTargets)
}

// RequestURLRewriteUpdate distinguishes a concrete rewrite from an explicit
// null. The tags record that field's use across restarts and stock downgrades.
type RequestURLRewriteUpdate struct {
	Value           *RequestURLRewrite
	Tags            []string
	KeepPolicyState bool
}

func (update RequestURLRewriteUpdate) write(body map[string]any) error {
	if update.Value != nil && !update.Value.Valid() {
		return fmt.Errorf("invalid HTTP request URL rewrite")
	}
	policy := body["l7policy"].(map[string]any)
	policy[requestURLRewriteField] = update.Value
	if !update.KeepPolicyState {
		policy["admin_state_up"] = false
	}
	if update.Tags != nil {
		policy["tags"] = update.Tags
	}
	return nil
}
