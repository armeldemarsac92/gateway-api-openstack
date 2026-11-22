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
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"k8s.io/utils/ptr"
)

// RequestRedirect supplies an effective scheme and port while preserving the
// incoming hostname and path by default. The provider retains the query string.
type RequestRedirect struct {
	Scheme          string                 `json:"scheme"`
	Port            int                    `json:"port"`
	Hostname        string                 `json:"hostname,omitempty"`
	ReplaceFullPath *string                `json:"replace_full_path,omitempty"`
	ReplacePrefix   *PathPrefixReplacement `json:"replace_prefix,omitempty"`
}

type PathPrefixReplacement struct {
	Match       string `json:"match"`
	Replacement string `json:"replacement"`
}

func (r RequestRedirect) Valid() bool {
	if r.Scheme != "http" && r.Scheme != "https" || r.Port < 1 || r.Port > 65535 {
		return false
	}
	return validURLModifier(r.Hostname, r.ReplaceFullPath, r.ReplacePrefix)
}

func validURLModifier(hostname string, full *string, prefix *PathPrefixReplacement) bool {
	if hostname != "" {
		for i := range len(hostname) {
			if hostname[i] >= utf8.RuneSelf {
				return false
			}
		}
		if amphora.ValidateExactHostname(strings.ToLower(hostname)) != nil {
			return false
		}
	}
	if full != nil {
		return prefix == nil && cloud.ValidHTTPPathReplacement(*full)
	}
	return prefix == nil || len(prefix.Match) <= 1024 && cloud.ValidHTTPPath(prefix.Match) && cloud.ValidHTTPPathReplacement(prefix.Replacement)
}

func readRequestRedirect(raw json.RawMessage) *RequestRedirect {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	for key := range fields {
		switch key {
		case "scheme", "port", "hostname", "replace_full_path", "replace_prefix":
		default:
			return nil
		}
	}
	var r RequestRedirect
	if json.Unmarshal(fields["scheme"], &r.Scheme) != nil || json.Unmarshal(fields["port"], &r.Port) != nil {
		return nil
	}
	if hostname, present := fields["hostname"]; present {
		if json.Unmarshal(hostname, &r.Hostname) != nil || r.Hostname == "" {
			return nil
		}
	}
	if full, present := fields["replace_full_path"]; present {
		if json.Unmarshal(full, &r.ReplaceFullPath) != nil || r.ReplaceFullPath == nil {
			return nil
		}
	}
	if rawPrefix, present := fields["replace_prefix"]; present {
		r.ReplacePrefix = readPathPrefixReplacement(rawPrefix)
		if r.ReplacePrefix == nil {
			return nil
		}
	}
	if !r.Valid() {
		return nil
	}
	return &r
}

func readPathPrefixReplacement(raw json.RawMessage) *PathPrefixReplacement {
	var prefix map[string]json.RawMessage
	var match string
	var replacement *string
	// An empty string removes the matching prefix. Its field must be present.
	if json.Unmarshal(raw, &prefix) != nil || len(prefix) != 2 ||
		json.Unmarshal(prefix["match"], &match) != nil || json.Unmarshal(prefix["replacement"], &replacement) != nil || replacement == nil {
		return nil
	}
	return &PathPrefixReplacement{Match: match, Replacement: *replacement}
}

func (r *RequestRedirect) Equal(other *RequestRedirect) bool {
	if r == nil || other == nil {
		return r == other
	}
	return r.Scheme == other.Scheme && r.Port == other.Port && r.Hostname == other.Hostname &&
		ptr.Equal(r.ReplaceFullPath, other.ReplaceFullPath) && ptr.Equal(r.ReplacePrefix, other.ReplacePrefix)
}

func SupportedRedirectCode(code int32) bool {
	switch code {
	case 301, 302, 303, 307, 308:
		return true
	default:
		return false
	}
}

func (o L7PolicyObservation) HasValidRequestRedirect() bool {
	policy := o.Policy
	return policy.Action == string(ActionRedirectRequest) && o.RedirectRequest != nil &&
		string(o.PoolReference) == "null" && policy.RedirectPoolID == "" && policy.RedirectURL == "" && policy.RedirectPrefix == "" &&
		SupportedRedirectCode(policy.RedirectHttpCode) && !o.ExtendedTargetFields && o.AdminState != nil
}

type requestRedirectPolicyUpdate struct {
	redirect RequestRedirect
	code     int32
}

func (opts requestRedirectPolicyUpdate) ToL7PolicyUpdateMap() (map[string]any, error) {
	if !opts.redirect.Valid() || !SupportedRedirectCode(opts.code) {
		return nil, fmt.Errorf("invalid request redirect destination or status code")
	}
	return map[string]any{"l7policy": map[string]any{
		"action": ActionRedirectRequest, "redirect_request": opts.redirect, "redirect_http_code": opts.code, "admin_state_up": false,
	}}, nil
}

// SetPolicyRequestRedirect changes the complete redirect destination while disabled.
func (s *Service) SetPolicyRequestRedirect(ctx context.Context, id string, redirect RequestRedirect, code int32) (*l7policies.L7Policy, error) {
	return l7policies.Update(ctx, s.client, id, requestRedirectPolicyUpdate{redirect: redirect, code: code}).Extract()
}
