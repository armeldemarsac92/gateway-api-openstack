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
	"net/http"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/stretchr/testify/require"
)

func TestConditionalL7PolicyRemovalUsesTheObservedRevision(t *testing.T) {
	revision := strings.Repeat("ab", 32)
	policy := map[string]any{
		"id": "policy-1", "listener_id": "listener-1", "project_id": "project-1", "action": "REDIRECT_TO_POOL",
		"admin_state_up": false, "rules": []map[string]any{{"id": "rule-1", "tags": []string{"route-owner"}}},
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/lbaas/l7policies/policy-1/conditional_removal", req.URL.Path)
		switch req.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"l7policy_removal": map[string]any{
				"revision": revision, "loadbalancer_id": "lb-1", "policy": policy,
			}}))
		case http.MethodPost:
			var body map[string]map[string]string
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Equal(t, map[string]map[string]string{"l7policy_removal": {"expected_revision": revision}}, body)
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	})
	service := NewService(&gophercloud.ServiceClient{ProviderClient: &gophercloud.ProviderClient{
		HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}},
	}, Endpoint: "http://octavia.test/"})
	snapshot, err := service.ObservePolicyRemoval(t.Context(), "policy-1")
	require.NoError(t, err)
	require.Equal(t, "lb-1", snapshot.LoadBalancerID)
	require.Equal(t, "policy-1", snapshot.Policy.Policy.ID)
	require.Len(t, snapshot.Rules, 1)
	require.Equal(t, []string{"route-owner"}, snapshot.Rules[0].Tags)
	require.NoError(t, service.RemovePolicyRevision(t.Context(), PolicyRemoval{PolicyID: "policy-1", ExpectedRevision: snapshot.Revision}))

	delete(policy, "rules")
	_, err = service.ObservePolicyRemoval(t.Context(), "policy-1")
	category, _ := cloud.ErrorCategoryOf(err)
	require.Equal(t, cloud.ErrorCategoryDependencyNotReady, category)
}
