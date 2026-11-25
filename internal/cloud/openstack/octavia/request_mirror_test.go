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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestRequestMirrorReadbackAndClear(t *testing.T) {
	o, err := readPolicyObservation(json.RawMessage(`{
		"id":"policy", "action":"WEIGHTED_TARGETS", "admin_state_up":true,
		"request_mirrors":[
			{"pool_id":"e1cab506-0bcb-4705-8f0e-22f16e5b9cf7","numerator":0,"denominator":100},
			{"pool_id":"e1cab506-0bcb-4705-8f0e-22f16e5b9cf7","numerator":3,"denominator":13,"phase":"before_transforms"}
		],
		"targets":[{"weight":7,"pool_id":"primary","request_mirrors":[
			{"pool_id":"e1cab506-0bcb-4705-8f0e-22f16e5b9cf7","numerator":31,"denominator":89,"phase":"after_headers"}
		]}]
	}`))
	require.NoError(t, err)
	require.NoError(t, o.RequestMirrorsErr)
	require.True(t, o.RequestMirrorsReported)
	require.Len(t, o.RequestMirrors, 2)
	require.Equal(t, cloud.HTTPMirrorAfterRewrite, o.RequestMirrors[0].Phase)
	require.EqualValues(t, 0, o.RequestMirrors[0].Numerator)
	require.Equal(t, cloud.HTTPMirrorBeforeTransforms, o.RequestMirrors[1].Phase)
	require.NotNil(t, o.Target)
	require.True(t, o.TargetMirrorsPresent)
	require.EqualValues(t, 89, o.Target.WeightedTargets[0].RequestMirrors[0].Denominator)

	body := map[string]any{"l7policy": map[string]any{}}
	require.NoError(t, (RequestMirrorsUpdate{}).write(body))
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"l7policy":{"request_mirrors":null}}`, string(encoded))
}

func TestBackendRequestMirrorCreateShowsEmptyRuleList(t *testing.T) {
	opts := PolicyCreateOpts{
		CreateOpts: l7policies.CreateOpts{ListenerID: "listener", Action: ActionWeightedTargets},
		WeightedTargets: []WeightedHTTPTarget{{PoolID: "primary", Weight: 7, RequestMirrors: []HTTPRequestMirror{
			{PoolID: "e1cab506-0bcb-4705-8f0e-22f16e5b9cf7", Numerator: 1, Denominator: 5, Phase: cloud.HTTPMirrorAfterRewrite},
		}}},
		RequestMirrors: []HTTPRequestMirror{},
	}
	body, err := opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	payload := body["l7policy"].(map[string]any)
	payload["id"] = "policy"
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	observation, err := readPolicyObservation(raw)
	require.NoError(t, err)
	require.NoError(t, observation.RequestMirrorsErr)
	require.True(t, observation.RequestMirrorsReported)
	require.Empty(t, observation.RequestMirrors)
	require.True(t, observation.TargetMirrorsPresent)
}

func TestFixedResponseRequestMirrorAcceptsURLRewriteAndHeaders(t *testing.T) {
	path := "/mirrored"
	opts := PolicyCreateOpts{
		CreateOpts:              l7policies.CreateOpts{ListenerID: "listener"},
		FixedServerError:        true,
		RequestMirrors:          []HTTPRequestMirror{{PoolID: "e1cab506-0bcb-4705-8f0e-22f16e5b9cf7", Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite}},
		RequestURLRewrite:       &RequestURLRewrite{ReplaceFullPath: &path},
		RequestHeaderOperations: []cloud.HTTPHeaderOperation{{Name: "x-private", Operation: cloud.HTTPHeaderRemove}},
	}
	body, err := opts.ToL7PolicyCreateMap()
	require.NoError(t, err)
	policy := body["l7policy"].(map[string]any)
	policy["id"], policy["admin_state_up"], policy["redirect_pool_id"] = "fixed", true, nil
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	observation, err := readPolicyObservation(raw)
	require.NoError(t, err)
	require.True(t, observation.HasValidRequestURLRewrite())
	require.True(t, observation.Target.IsFixedServerError())
	require.Equal(t, opts.RequestHeaderOperations, observation.RequestHeaderOperations)
	opts.RequestMirrors = nil
	_, err = opts.ToL7PolicyCreateMap()
	require.Error(t, err)
}
