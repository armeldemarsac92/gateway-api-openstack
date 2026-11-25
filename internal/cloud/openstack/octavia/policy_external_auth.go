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
	"encoding/json"
	"errors"
	"slices"

	"github.com/google/uuid"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const GatewayCapabilityPolicyExternalAuth = "l7policy_http_external_auth_v1"
const GatewayCapabilityPolicyExternalAuthGRPC = "l7policy_grpc_external_auth_v1"
const GatewayCapabilityPolicyExternalAuthForwardBody = "l7policy_external_auth_forward_body_v1"
const policyExternalAuthField = "external_auth"

// PolicyExternalAuth is the authorization filter attached to an L7 policy.
type PolicyExternalAuth struct {
	Protocol               string   `json:"protocol,omitempty"`
	PoolID                 string   `json:"pool_id"`
	PathPrefix             string   `json:"path_prefix,omitempty"`
	ForwardBodyMaxSize     int32    `json:"forward_body_max_size,omitempty"`
	AllowedRequestHeaders  []string `json:"allowed_request_headers,omitempty"`
	AllowedResponseHeaders []string `json:"allowed_response_headers,omitempty"`
}

// PolicyExternalAuthUpdate distinguishes an explicit removal from an unrelated policy edit.
type PolicyExternalAuthUpdate struct {
	Value *PolicyExternalAuth
}

func (opts PolicyCreateOpts) supportsExternalAuth() bool {
	return forwardsToPools(string(opts.Action)) && opts.TCPTLSPassthrough == nil && len(opts.WeightedTCPPolicyTargets) == 0 &&
		len(opts.RequestHeaderOperations) == 0 && len(opts.ResponseHeaderOperations) == 0 && opts.RequestCORS == nil &&
		opts.RequestURLRewrite == nil && opts.HTTPRequestPolicy == nil && len(opts.RequestMirrors) == 0 &&
		!slices.ContainsFunc(opts.WeightedTargets, func(target WeightedHTTPTarget) bool {
			return target.HasRequestFilters() || len(target.ResponseHeaderOperations) != 0 || len(target.RequestMirrors) != 0
		})
}

func (update PolicyExternalAuthUpdate) write(body map[string]any) error {
	if auth := update.Value; auth != nil && !auth.validPolicyAuth() {
		return errors.New("invalid external_auth protocol, pool or options")
	}
	body["l7policy"].(map[string]any)[policyExternalAuthField] = update.Value
	return nil
}

func (auth PolicyExternalAuth) validPolicyAuth() bool {
	id, err := uuid.Parse(auth.PoolID)
	if err != nil || id.String() != auth.PoolID || auth.ForwardBodyMaxSize < 0 || !cloud.ValidExternalAuthPathPrefix(auth.PathPrefix) {
		return false
	}
	switch auth.Protocol {
	case "":
	case cloud.ExternalAuthProtocolGRPC:
		if auth.ForwardBodyMaxSize > cloud.MaxGRPCExternalAuthBodySize || auth.PathPrefix != "" || len(auth.AllowedResponseHeaders) != 0 {
			return false
		}
	default:
		return false
	}
	return cloud.ValidExternalAuthHeaders(auth.AllowedRequestHeaders, auth.Protocol == cloud.ExternalAuthProtocolGRPC) &&
		cloud.ValidExternalAuthHeaders(auth.AllowedResponseHeaders, false)
}

func readPolicyExternalAuth(raw json.RawMessage) *PolicyExternalAuth {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	for _, v := range fields {
		if !hasJSONValue(v) {
			return nil
		}
	}
	if string(fields["protocol"]) == `"GRPC"` {
		if fields["path_prefix"] != nil || fields["allowed_response_headers"] != nil {
			return nil
		}
	}
	var auth PolicyExternalAuth
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&auth) != nil || !auth.validPolicyAuth() {
		return nil
	}
	return &auth
}
