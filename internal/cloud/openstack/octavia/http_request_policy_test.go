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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package octavia

import (
	"encoding/json"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestHTTPRequestPolicyWire(t *testing.T) {
	policy := &cloud.HTTPRequestPolicy{RequestTimeoutMS: ptr.To(int64(0)), Retry: &cloud.HTTPRetry{Attempts: ptr.To(0)}}
	opts := PolicyCreateOpts{CreateOpts: l7policies.CreateOpts{ListenerID: "listener", Action: l7policies.ActionRedirectToPool, RedirectPoolID: "pool"},
		HTTPRequestPolicy: policy}
	requireWire := func(fields map[string]any, want string) {
		t.Helper()
		value, ok := fields["l7policy"].(map[string]any)[httpRequestPolicyField]
		require.Equal(t, want != "", ok)
		if ok {
			encoded, err := json.Marshal(value)
			require.NoError(t, err)
			require.JSONEq(t, want, string(encoded))
		}
	}
	body, err := opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	requireWire(body, `{"request_timeout_ms":0,"retry":{"attempts":0}}`)
	opts.WeightedTargets = []WeightedHTTPTarget{{Weight: 1, PoolID: "pool"}}
	body, err = opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	requireWire(body, `{"request_timeout_ms":0,"retry":{"attempts":0}}`)
	opts.WeightedTargets, opts.FixedServerError = nil, true
	_, err = opts.ToL7PolicyCreateMap()
	require.Error(t, err)

	for _, test := range []struct {
		update *HTTPRequestPolicyUpdate
		want   string
	}{
		{nil, ""},
		{&HTTPRequestPolicyUpdate{}, "null"},
		{&HTTPRequestPolicyUpdate{Value: policy}, `{"request_timeout_ms":0,"retry":{"attempts":0}}`},
	} {
		update := policyWithFilters{update: l7policies.UpdateOpts{}, filters: PolicyFilters{RequestPolicy: test.update}}
		body, err = update.ToL7PolicyUpdateMap()
		require.NoError(t, err)
		requireWire(body, test.want)
	}
}

func TestHTTPRequestPolicyReadback(t *testing.T) {
	const row = `{"id":"policy","action":"REDIRECT_TO_POOL","redirect_pool_id":"pool","admin_state_up":true`
	for _, test := range []struct {
		wire  string
		valid bool
	}{
		{"", false},
		{"null", false},
		{`{"request_timeout_ms":0,"retry":{}}`, true},
		{`{"retry":{"attempts":0,"codes":[501]}}`, false},
		{`{"retry":{"backoff":"1s"}}`, false},
	} {
		data := row
		if test.wire != "" {
			data += `,"http_request_policy":` + test.wire
		}
		got, err := readPolicyObservation([]byte(data + "}"))
		require.NoError(t, err)
		require.Equal(t, test.wire != "", got.HTTPRequestPolicyReported)
		require.Equal(t, test.wire != "" && test.wire != "null", got.HTTPRequestPolicyPresent)
		require.Equal(t, test.valid, got.HasValidHTTPRequestPolicy())
		if test.valid {
			require.True(t, got.HTTPRequestPolicy.Equal(&cloud.HTTPRequestPolicy{
				RequestTimeoutMS: ptr.To(int64(0)), Retry: &cloud.HTTPRetry{Attempts: ptr.To(1)},
			}))
			require.Contains(t, got.ActivationOptions, httpRequestPolicyField)
		}
	}
}

func TestHTTPRequestPolicyActivationPreservesInt64(t *testing.T) {
	const row = `{"id":"policy","action":"REDIRECT_TO_POOL","redirect_pool_id":"pool","admin_state_up":true,
		"http_request_policy":{"request_timeout_ms":9223372036854775807}}`
	got, err := readPolicyObservation([]byte(row))
	require.NoError(t, err)
	require.True(t, got.HasValidHTTPRequestPolicy())
	require.Contains(t, got.ActivationOptions, "9223372036854775807")
}
