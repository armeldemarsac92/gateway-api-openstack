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

package httproute

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func retryBudgetTargetsService(policy *gatewayx.XBackendTrafficPolicy, service string) bool {
	return slices.ContainsFunc(policy.Spec.TargetRefs, func(ref gatewayx.LocalPolicyTargetReference) bool {
		return ref.Group == "" && ref.Kind == "Service" && string(ref.Name) == service
	})
}

// Direct attachment makes the oldest policy authoritative for the whole
// Service, including when the winning policy has no retry budget.
func retryBudgetPolicyForService(policies []gatewayx.XBackendTrafficPolicy, service string) *gatewayx.XBackendTrafficPolicy {
	var winner *gatewayx.XBackendTrafficPolicy
	for i := range policies {
		policy := &policies[i]
		if !policy.DeletionTimestamp.IsZero() || !retryBudgetTargetsService(policy, service) {
			continue
		}
		if winner == nil || cmp.Or(policy.CreationTimestamp.Compare(winner.CreationTimestamp.Time), cmp.Compare(policy.Name, winner.Name)) < 0 {
			winner = policy
		}
	}
	return winner
}

func serviceRetryBudget(constraint *gatewayx.RetryConstraint, uid types.UID) (*cloud.BackendRetryBudget, bool) {
	if constraint == nil {
		return nil, true
	}
	budget := cloud.BackendRetryBudget{
		ServiceUID: string(uid), BudgetPercent: 20, BudgetIntervalMS: 10000,
		MinRetryCount: 10, MinRetryIntervalMS: 1000,
	}
	if details := constraint.Budget; details != nil {
		budget.BudgetPercent = ptr.Deref(details.Percent, budget.BudgetPercent)
		budget.BudgetIntervalMS = retryBudgetMilliseconds(details.Interval, budget.BudgetIntervalMS)
	}
	if rate := constraint.MinRetryRate; rate != nil {
		budget.MinRetryCount = ptr.Deref(rate.Count, budget.MinRetryCount)
		budget.MinRetryIntervalMS = retryBudgetMilliseconds(rate.Interval, budget.MinRetryIntervalMS)
	}
	return &budget, budget.Valid()
}

func retryBudgetMilliseconds(raw *gatewayx.Duration, fallback int64) int64 {
	if raw == nil {
		return fallback
	}
	duration, err := time.ParseDuration(string(*raw))
	if err != nil || duration%time.Millisecond != 0 {
		return 0
	}
	return duration.Milliseconds()
}

type routeRetryBudget struct {
	budget cloud.BackendRetryBudget
	slot   backendtls.AncestorSlot
}

func collectBackendRetryBudgets(ctx context.Context, reader client.Reader, services []corev1.Service, ancestor backendtls.GatewayAncestor) (map[types.NamespacedName]routeRetryBudget, error) {
	policies := make(map[string][]gatewayx.XBackendTrafficPolicy)
	budgets := make(map[types.NamespacedName]routeRetryBudget)
	for i := range services {
		service := &services[i]
		if !service.DeletionTimestamp.IsZero() {
			continue
		}
		items, listed := policies[service.Namespace]
		if !listed {
			var list gatewayx.XBackendTrafficPolicyList
			if err := reader.List(ctx, &list, client.InNamespace(service.Namespace)); err != nil {
				return nil, fmt.Errorf("read Service retry policies: %w", err)
			}
			if list.Continue != "" {
				return nil, fmt.Errorf("incomplete Service retry policy list")
			}
			items = list.Items
			policies[service.Namespace] = items
		}
		if winner := retryBudgetPolicyForService(items, service.Name); winner != nil {
			if budget, valid := serviceRetryBudget(winner.Spec.RetryConstraint, service.UID); valid && budget != nil {
				budgets[client.ObjectKeyFromObject(service)] = routeRetryBudget{
					budget: *budget, slot: ancestor.SlotFor(winner.Namespace, winner.Status.Ancestors),
				}
			}
		}
	}
	return budgets, nil
}

func (inputs *routeModelInputs) backendRetryBudget(backend model.BackendGroup) *cloud.BackendRetryBudget {
	if entry, found := inputs.retryBudgetForBackend(backend); found && entry.slot == backendtls.AncestorSlotHeld {
		return &entry.budget
	}
	return nil
}

func (inputs *routeModelInputs) retryBudgetForBackend(backend model.BackendGroup) (routeRetryBudget, bool) {
	entry, found := inputs.retryBudgets[backend.Service]
	return entry, found && backend.Diagnostic == nil && entry.budget.ServiceUID == string(backend.ServiceUID)
}

func (inputs *routeModelInputs) retryBudgetAncestorFull(rules []model.Rule) bool {
	full := func(backend model.BackendGroup) bool {
		entry, found := inputs.retryBudgetForBackend(backend)
		return found && entry.slot == backendtls.AncestorSlotFull
	}
	for _, rule := range rules {
		if slices.ContainsFunc(rule.Backends, func(backend model.BackendGroup) bool { return backend.Weight > 0 && full(backend) }) ||
			slices.ContainsFunc(rule.RequestMirrors, func(mirror model.RequestMirror) bool { return mirror.Numerator > 0 && full(mirror.Backend) }) {
			return true
		}
	}
	return false
}
