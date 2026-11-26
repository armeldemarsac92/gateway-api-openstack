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
	"errors"
	"fmt"
	"net/http"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
)

// PolicyRemovalSnapshot binds a policy and its complete rules to their load balancer.
type PolicyRemovalSnapshot struct {
	LoadBalancerID           string
	ListenerClosureConfirmed bool
	PolicySnapshot
}

func (s *Service) ObservePolicyRemoval(ctx context.Context, policyID string) (PolicyRemovalSnapshot, error) {
	var response struct {
		Removal struct {
			LoadBalancerID           string          `json:"loadbalancer_id"`
			ListenerClosureConfirmed bool            `json:"listener_closure_confirmed"`
			Revision                 string          `json:"revision"`
			Policy                   json.RawMessage `json:"policy"`
		} `json:"l7policy_removal"`
	}
	url := s.client.ServiceURL("lbaas", "l7policies", policyID, "conditional_removal")
	if _, err := s.client.Get(ctx, url, &response, nil); err != nil {
		return PolicyRemovalSnapshot{}, fmt.Errorf("read Octavia L7 policy removal snapshot: %w", err)
	}
	wire := response.Removal
	snapshot, err := readPolicySnapshot(wire.Revision, wire.Policy)
	if err != nil {
		return PolicyRemovalSnapshot{}, err
	}
	if wire.LoadBalancerID == "" || snapshot.Policy.Policy.ID != policyID {
		return PolicyRemovalSnapshot{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("incomplete Octavia L7 policy removal identity"))
	}
	return PolicyRemovalSnapshot{
		LoadBalancerID: wire.LoadBalancerID, ListenerClosureConfirmed: wire.ListenerClosureConfirmed, PolicySnapshot: snapshot,
	}, nil
}

// PolicyRemoval identifies the revision to remove and the required listener state.
type PolicyRemoval struct {
	PolicyID                string `json:"-"`
	ExpectedRevision        string `json:"expected_revision"`
	RequireListenerDisabled bool   `json:"require_listener_disabled,omitempty"`
}

// RemovePolicyRevision requests deletion only while the observed revision is current.
func (s *Service) RemovePolicyRevision(ctx context.Context, removal PolicyRemoval) error {
	if removal.PolicyID == "" || !validPolicyRevision(removal.ExpectedRevision) {
		return errors.New("invalid Octavia L7 policy removal identity or revision")
	}
	body := map[string]PolicyRemoval{"l7policy_removal": removal}
	url := s.client.ServiceURL("lbaas", "l7policies", removal.PolicyID, "conditional_removal")
	_, err := s.client.Post(ctx, url, body, nil, &gophercloud.RequestOpts{OkCodes: []int{http.StatusAccepted}})
	if err != nil {
		return fmt.Errorf("request conditional Octavia L7 policy removal: %w", err)
	}
	return nil
}
