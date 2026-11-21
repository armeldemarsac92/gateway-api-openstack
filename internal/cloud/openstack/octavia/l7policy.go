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
	"net/http"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// ListCompletePolicies requires an explicit collection on every page.
func (s *Service) ListCompletePolicies(ctx context.Context, opts l7policies.ListOpts) ([]l7policies.L7Policy, error) {
	return listCompleteCollection(ctx, l7policies.List(s.client, opts), completePolicyPage, "l7policies", l7policies.ExtractL7Policies)
}

func completePolicyPage(result pagination.PageResult) pagination.Page {
	if result.StatusCode == http.StatusNoContent {
		result.Body = map[string]any{"l7policies": []any{}}
	}
	return l7policies.L7PolicyPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
}

// PolicyObservation preserves readback fields whose absence matters before
// removing a redirect's rules or changing its action.
type PolicyObservation struct {
	l7policies.L7Policy
	AdminState    *bool           `json:"admin_state_up"`
	PoolReference json.RawMessage `json:"redirect_pool_id"`
}

func (s *Service) ObservePolicies(ctx context.Context, opts l7policies.ListOpts) ([]PolicyObservation, error) {
	return listCompleteCollection(ctx, l7policies.List(s.client, opts), completePolicyPage, "l7policies", func(page pagination.Page) ([]PolicyObservation, error) {
		var response struct {
			Policies []PolicyObservation `json:"l7policies"`
		}
		err := page.(l7policies.L7PolicyPage).ExtractInto(&response)
		for i := range response.Policies {
			item := &response.Policies[i]
			if item.AdminState != nil {
				item.AdminStateUp = *item.AdminState
			}
			if len(item.PoolReference) != 0 {
				if decodeErr := json.Unmarshal(item.PoolReference, &item.RedirectPoolID); decodeErr != nil {
					return nil, decodeErr
				}
			}
		}
		return response.Policies, err
	})
}

// CreatePolicy creates one L7 policy.
func (s *Service) CreatePolicy(ctx context.Context, opts l7policies.CreateOpts) (*l7policies.L7Policy, error) {
	return l7policies.Create(ctx, s.client, opts).Extract()
}

// UpdatePolicy updates one L7 policy.
func (s *Service) UpdatePolicy(ctx context.Context, id string, opts l7policies.UpdateOpts) (*l7policies.L7Policy, error) {
	return l7policies.Update(ctx, s.client, id, opts).Extract()
}

// DeletePolicy deletes one L7 policy.
func (s *Service) DeletePolicy(ctx context.Context, id string) error {
	return l7policies.Delete(ctx, s.client, id).ExtractErr()
}

// ListRules lists and extracts L7 rules beneath one policy.
func (s *Service) ListRules(ctx context.Context, policyID string, opts l7policies.ListRulesOpts) ([]l7policies.Rule, error) {
	pages, err := l7policies.ListRules(s.client, policyID, opts).AllPages(ctx)
	if err != nil {
		return nil, fmt.Errorf("list L7 rules: %w", err)
	}
	items, err := l7policies.ExtractRules(pages)
	if err != nil {
		return nil, fmt.Errorf("extract L7 rules: %w", err)
	}
	return items, nil
}

// ListCompleteRules requires an explicit collection on every page.
func (s *Service) ListCompleteRules(ctx context.Context, policyID string, opts l7policies.ListRulesOpts) ([]l7policies.Rule, error) {
	return listCompleteCollection(ctx, l7policies.ListRules(s.client, policyID, opts), func(result pagination.PageResult) pagination.Page {
		return l7policies.RulePage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, "rules", l7policies.ExtractRules)
}

// RuleObservation preserves the flags needed to confirm each redirect rule.
type RuleObservation struct {
	l7policies.Rule
	AdminState *bool `json:"admin_state_up"`
	Inverted   *bool `json:"invert"`
}

func (s *Service) ObserveRules(ctx context.Context, policyID string, opts l7policies.ListRulesOpts) ([]RuleObservation, error) {
	return listCompleteCollection(ctx, l7policies.ListRules(s.client, policyID, opts), func(result pagination.PageResult) pagination.Page {
		return l7policies.RulePage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, "rules", func(page pagination.Page) ([]RuleObservation, error) {
		var response struct {
			Rules []RuleObservation `json:"rules"`
		}
		err := page.(l7policies.RulePage).ExtractInto(&response)
		for i := range response.Rules {
			item := &response.Rules[i]
			if item.AdminState != nil {
				item.AdminStateUp = *item.AdminState
			}
			if item.Inverted != nil {
				item.Invert = *item.Inverted
			}
		}
		return response.Rules, err
	})
}

// CreateRule creates one L7 rule beneath a policy.
func (s *Service) CreateRule(ctx context.Context, policyID string, opts l7policies.CreateRuleOpts) (*l7policies.Rule, error) {
	return l7policies.CreateRule(ctx, s.client, policyID, opts).Extract()
}

// UpdateRule updates one L7 rule beneath a policy.
func (s *Service) UpdateRule(ctx context.Context, policyID, ruleID string, opts l7policies.UpdateRuleOpts) (*l7policies.Rule, error) {
	return l7policies.UpdateRule(ctx, s.client, policyID, ruleID, opts).Extract()
}

// DeleteRule deletes one L7 rule beneath a policy.
func (s *Service) DeleteRule(ctx context.Context, policyID, ruleID string) error {
	return l7policies.DeleteRule(ctx, s.client, policyID, ruleID).ExtractErr()
}
