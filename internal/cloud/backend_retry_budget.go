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

package cloud

import "regexp"

// BackendRetryBudget is the per-Service retry constraint accepted by Octavia.
type BackendRetryBudget struct {
	ServiceUID         string `json:"service_uid"`
	BudgetPercent      int    `json:"budget_percent"`
	BudgetIntervalMS   int64  `json:"budget_interval_ms"`
	MinRetryCount      int    `json:"min_retry_count"`
	MinRetryIntervalMS int64  `json:"min_retry_interval_ms"`
}

var retryBudgetServiceUID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func (budget BackendRetryBudget) Valid() bool {
	return retryBudgetServiceUID.MatchString(budget.ServiceUID) &&
		budget.BudgetPercent >= 0 && budget.BudgetPercent <= 100 &&
		budget.BudgetIntervalMS >= 1000 && budget.BudgetIntervalMS <= 3600000 &&
		budget.MinRetryCount >= 1 && budget.MinRetryCount <= 1000000 &&
		budget.MinRetryIntervalMS >= 1 && budget.MinRetryIntervalMS <= 3600000
}
