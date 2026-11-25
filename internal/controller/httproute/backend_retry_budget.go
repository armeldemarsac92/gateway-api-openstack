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
	"iter"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
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

type routeServicePersistence struct {
	serviceUID types.UID
	settings   *cloud.ServiceSessionPersistence
	slot       backendtls.AncestorSlot
}

func backendPolicyPersistence(policy *gatewayx.XBackendTrafficPolicy) *gatewayv1.SessionPersistence {
	return policy.Spec.SessionPersistence //nolint:staticcheck // The deprecated field is served in the v1.7 Experimental API.
}

func collectBackendTrafficPolicies(ctx context.Context, reader client.Reader, services []corev1.Service, ancestor backendtls.GatewayAncestor, retry, affinity bool) (map[types.NamespacedName]routeRetryBudget, map[types.NamespacedName]routeServicePersistence, error) {
	policies := make(map[string][]gatewayx.XBackendTrafficPolicy)
	budgets := make(map[types.NamespacedName]routeRetryBudget)
	persistence := make(map[types.NamespacedName]routeServicePersistence)
	for i := range services {
		service := &services[i]
		if !service.DeletionTimestamp.IsZero() {
			continue
		}
		items, listed := policies[service.Namespace]
		if !listed {
			var list gatewayx.XBackendTrafficPolicyList
			if err := reader.List(ctx, &list, client.InNamespace(service.Namespace)); err != nil {
				return nil, nil, fmt.Errorf("read Service backend traffic policies: %w", err)
			}
			if list.Continue != "" {
				return nil, nil, fmt.Errorf("incomplete Service backend traffic policy list")
			}
			items = list.Items
			policies[service.Namespace] = items
		}
		winner := retryBudgetPolicyForService(items, service.Name)
		if winner == nil {
			continue
		}
		key := client.ObjectKeyFromObject(service)
		slot := ancestor.SlotFor(winner.Namespace, winner.Status.Ancestors)
		if retry {
			if budget, valid := serviceRetryBudget(winner.Spec.RetryConstraint, service.UID); valid && budget != nil {
				budgets[key] = routeRetryBudget{budget: *budget, slot: slot}
			}
		}
		if spec := backendPolicyPersistence(winner); spec != nil && affinity {
			if settings, diagnostic := model.ServiceSessionPersistence(*spec, service.UID); diagnostic == nil {
				persistence[key] = routeServicePersistence{serviceUID: service.UID, settings: settings, slot: slot}
			}
		}
	}
	return budgets, persistence, nil
}

func (inputs *routeModelInputs) serviceSessionPersistence(backend model.BackendGroup, grpc bool) (*cloud.ServiceSessionPersistence, backendtls.AncestorSlot) {
	entry, found := inputs.servicePersistence[backend.Service]
	if !found || backend.Diagnostic != nil || entry.serviceUID != backend.ServiceUID ||
		grpc && entry.settings.Header != nil && !entry.settings.Header.ValidForGRPC() {
		return nil, backendtls.AncestorSlotHeld
	}
	if entry.slot == backendtls.AncestorSlotHeld {
		return entry.settings, entry.slot
	}
	return nil, entry.slot
}

func (inputs *routeModelInputs) backendRetryBudget(backend model.BackendGroup) (*cloud.BackendRetryBudget, bool) {
	entry, found := inputs.retryBudgetForBackend(backend)
	if !found {
		return nil, false
	}
	if entry.slot == backendtls.AncestorSlotHeld {
		return &entry.budget, false
	}
	return nil, entry.slot == backendtls.AncestorSlotPending
}

func (inputs *routeModelInputs) retryBudgetForBackend(backend model.BackendGroup) (routeRetryBudget, bool) {
	entry, found := inputs.retryBudgets[backend.Service]
	return entry, found && backend.Diagnostic == nil && entry.budget.ServiceUID == string(backend.ServiceUID)
}

func (inputs *routeModelInputs) retryBudgetAncestorFull(matches []routeLoweredMatch, rules []model.Rule, requireH2 bool) bool {
	if len(inputs.retryBudgets) == 0 {
		return false
	}
	for backend := range activeRetryBudgetBackends(matches, rules, inputs.retryBudgetTransportUsable(requireH2)) {
		if entry, found := inputs.retryBudgetForBackend(backend); found && entry.slot == backendtls.AncestorSlotFull {
			return true
		}
	}
	return false
}

func (inputs *routeModelInputs) retryBudgetTransportUsable(requireH2 bool) func(model.BackendGroup) bool {
	return func(backend model.BackendGroup) bool {
		tls := inputs.backendTLSValidation(backend, requireH2)
		return tls == nil || !tls.Denied
	}
}

func retryBudgetTransportSelectable(backend model.BackendGroup) bool {
	// Reserve retry-policy ancestry without waiting for TLS-policy ancestry
	// or provider capability, which have their own reconciliation.
	winner := backend.BackendTLS.Winner
	return winner == nil || winner.Validation != nil && supportsBackendTLS(backend)
}

// activeRetryBudgetBackends visits Service references that can receive traffic
// from the admitted matches, including both levels of sampled mirrors.
func activeRetryBudgetBackends(matches []routeLoweredMatch, rules []model.Rule, transportUsable func(model.BackendGroup) bool) iter.Seq[model.BackendGroup] {
	return activeServiceBackends(matches, rules, transportUsable, true)
}

func activeServicePersistenceBackends(matches []routeLoweredMatch, rules []model.Rule, transportUsable func(model.BackendGroup) bool) iter.Seq[model.BackendGroup] {
	return func(yield func(model.BackendGroup) bool) {
		for backend := range activeServiceBackends(matches, rules, transportUsable, false) {
			if backend.RouteSessionPersistence == nil && !yield(backend) {
				return
			}
		}
	}
}

func activeServiceBackends(matches []routeLoweredMatch, rules []model.Rule, transportUsable func(model.BackendGroup) bool, includeMirrors bool) iter.Seq[model.BackendGroup] {
	return func(yield func(model.BackendGroup) bool) {
		emit := func(backend model.BackendGroup) bool {
			return backend.Service.Name == "" || yield(backend)
		}
		mirrors := func(refs []model.RequestMirror) bool {
			for _, mirror := range refs {
				if backend := mirror.Backend; mirror.Numerator > 0 && mirror.Denominator > 0 &&
					backend.Diagnostic == nil && transportUsable(backend) && !emit(backend) {
					return false
				}
			}
			return true
		}
		seen := make([]bool, len(rules))
		for _, match := range matches {
			index := match.RuleIndex
			if seen[index] {
				continue
			}
			seen[index] = true
			rule := rules[index]
			if rule.Redirect != nil || len(rule.Backends) == 0 {
				continue
			}
			if includeMirrors && !mirrors(rule.RequestMirrors) {
				return
			}
			for _, backend := range rule.Backends {
				if backend.Weight <= 0 || backend.Diagnostic != nil || backend.FilterDiagnostic != nil || !transportUsable(backend) {
					continue
				}
				if !emit(backend) || includeMirrors && !mirrors(backend.RequestMirrors) {
					return
				}
			}
		}
	}
}
