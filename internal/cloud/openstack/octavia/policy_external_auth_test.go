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
	"testing"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/stretchr/testify/require"
)

func TestPolicyExternalAuthRoundTrip(t *testing.T) {
	auth := &PolicyExternalAuth{PoolID: "12345678-1234-4000-8000-123456789abc", PathPrefix: "/check", AllowedResponseHeaders: []string{"x-role"}}
	opts := PolicyCreateOpts{CreateOpts: l7policies.CreateOpts{ListenerID: "listener", Action: l7policies.ActionRedirectToPool, RedirectPoolID: "backend"}, ExternalAuth: auth}
	body, err := opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	fields := body["l7policy"].(map[string]any)
	encoded, err := json.Marshal(fields["external_auth"])
	require.NoError(t, err)
	require.JSONEq(t, `{"pool_id":"12345678-1234-4000-8000-123456789abc","path_prefix":"/check","allowed_response_headers":["x-role"]}`, string(encoded))
	fields["id"] = "policy"
	raw, err := json.Marshal(fields)
	require.NoError(t, err)
	observed, err := readPolicyObservation(raw)
	require.NoError(t, err)
	require.True(t, observed.ExternalAuthReported)
	require.True(t, observed.ExternalAuthPresent)
	require.Equal(t, auth, observed.ExternalAuth)
	require.Contains(t, observed.ActivationOptions, "external_auth")

	patch := policyWithFilters{update: l7policies.UpdateOpts{}}
	body, err = patch.ToL7PolicyUpdateMap()
	require.NoError(t, err)
	require.NotContains(t, body["l7policy"], "external_auth")
	patch.filters.ExternalAuth = &PolicyExternalAuthUpdate{}
	body, err = patch.ToL7PolicyUpdateMap()
	require.NoError(t, err)
	require.Contains(t, body["l7policy"], "external_auth")
	require.Nil(t, body["l7policy"].(map[string]any)["external_auth"])

	unreadable, err := readPolicyObservation([]byte(`{"id":"policy","external_auth":{"future_option":"value"}}`))
	require.NoError(t, err)
	require.True(t, unreadable.ExternalAuthPresent)
	require.Nil(t, unreadable.ExternalAuth)
}

func TestGRPCPolicyExternalAuthWireAndReadback(t *testing.T) {
	auth := &PolicyExternalAuth{Protocol: "GRPC", PoolID: "12345678-1234-4000-8000-123456789abc"}
	opts := PolicyCreateOpts{CreateOpts: l7policies.CreateOpts{ListenerID: "listener", Action: l7policies.ActionRedirectToPool, RedirectPoolID: "origin"}, ExternalAuth: auth}
	body, err := opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	wire, err := json.Marshal(body["l7policy"].(map[string]any)["external_auth"])
	require.NoError(t, err)
	require.JSONEq(t, `{"protocol":"GRPC","pool_id":"12345678-1234-4000-8000-123456789abc"}`, string(wire))
	require.Equal(t, auth, readPolicyExternalAuth(wire))

	auth.AllowedRequestHeaders = []string{":authority"}
	body, err = opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	wire, err = json.Marshal(body["l7policy"].(map[string]any)["external_auth"])
	require.NoError(t, err)
	require.JSONEq(t, `{"protocol":"GRPC","pool_id":"12345678-1234-4000-8000-123456789abc","allowed_request_headers":[":authority"]}`, string(wire))
	require.Equal(t, auth, readPolicyExternalAuth(wire))
	auth.PathPrefix = "/http-only"
	_, err = opts.ToL7PolicyCreateMap()
	require.Error(t, err)
}
