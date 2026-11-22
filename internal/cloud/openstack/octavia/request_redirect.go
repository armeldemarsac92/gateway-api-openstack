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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

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
	ReplacePrefix   *RedirectPrefixReplace `json:"replace_prefix,omitempty"`
}

type RedirectPrefixReplace struct {
	Match       string `json:"match"`
	Replacement string `json:"replacement"`
}

func (r RequestRedirect) Valid() bool {
	if r.Scheme != "http" && r.Scheme != "https" || r.Port < 1 || r.Port > 65535 {
		return false
	}
	if r.Hostname != "" && amphora.ValidateExactHostname(r.Hostname) != nil {
		return false
	}
	if r.ReplaceFullPath != nil {
		return r.ReplacePrefix == nil && utf8.RuneCountInString(*r.ReplaceFullPath) <= 1024
	}
	return r.ReplacePrefix == nil || strings.HasPrefix(r.ReplacePrefix.Match, "/") && utf8.RuneCountInString(r.ReplacePrefix.Replacement) <= 1024
}

func readRequestRedirect(raw json.RawMessage) *RequestRedirect {
	// A missing replacement differs from an explicitly empty replacement.
	var data struct {
		Scheme          string  `json:"scheme"`
		Port            int     `json:"port"`
		Hostname        string  `json:"hostname"`
		ReplaceFullPath *string `json:"replace_full_path"`
		ReplacePrefix   *struct {
			Match       string  `json:"match"`
			Replacement *string `json:"replacement"`
		} `json:"replace_prefix"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&data) != nil {
		return nil
	}
	r := RequestRedirect{Scheme: data.Scheme, Port: data.Port, Hostname: data.Hostname, ReplaceFullPath: data.ReplaceFullPath}
	if prefix := data.ReplacePrefix; prefix != nil {
		if prefix.Replacement == nil {
			return nil
		}
		r.ReplacePrefix = &RedirectPrefixReplace{Match: prefix.Match, Replacement: *prefix.Replacement}
	}
	if !r.Valid() {
		return nil
	}
	return &r
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
