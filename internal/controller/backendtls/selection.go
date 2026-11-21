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

// Package backendtls evaluates BackendTLSPolicy for a resolved Service port.
package backendtls

import (
	"cmp"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Selection contains the effective policy and per-policy evaluations relevant to
// the Service port. A nil Winner means no policy applies. A Winner with nil
// Validation requires the caller to fail traffic closed.
type Selection struct {
	Winner   *PolicyResult
	Policies []PolicyResult
}

// PolicyResult provides the inputs for an ancestor status. The caller establishes
// Gateway ancestry and verifies that the data path can enforce the policy before
// publishing an Accepted condition.
type PolicyResult struct {
	Key          types.NamespacedName
	UID          types.UID
	Generation   int64
	Accepted     metav1.Condition
	ResolvedRefs metav1.Condition
	Validation   *Validation
}

type targetMatch int

const (
	unrelated targetMatch = iota
	missingPort
	wholeService
	namedPort
)

type candidate struct {
	policy *gatewayv1.BackendTLSPolicy
	match  targetMatch
}

// Evaluate selects the policy for a Service and the port already resolved from
// that Service. It uses snapshots of policies and ConfigMaps, and does not
// perform Kubernetes reads or update status. Named-port and whole-Service
// winners can both be accepted, with the named-port winner effective for that port.
func Evaluate(service *corev1.Service, port *corev1.ServicePort, policies []gatewayv1.BackendTLSPolicy, configMaps map[types.NamespacedName]*corev1.ConfigMap) Selection {
	if service == nil || port == nil {
		return Selection{}
	}
	var candidates []candidate
	for i := range policies {
		policy := &policies[i]
		if policy.Namespace != service.Namespace || !policy.DeletionTimestamp.IsZero() {
			continue
		}
		match := unrelated
		for _, target := range policy.Spec.TargetRefs {
			match = max(match, matchTarget(service, port, target))
		}
		if match != unrelated {
			candidates = append(candidates, candidate{policy: policy, match: match})
		}
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		return cmp.Or(
			a.policy.CreationTimestamp.Time.Compare(b.policy.CreationTimestamp.Time),
			cmp.Compare(a.policy.Namespace, b.policy.Namespace),
			cmp.Compare(a.policy.Name, b.policy.Name),
		)
	})

	selection := Selection{}
	winners := map[targetMatch]int{}
	unsupported := -1
	for _, candidate := range candidates {
		policy, match := candidate.policy, candidate.match
		result := newPolicyResult(policy)
		index := len(selection.Policies)
		switch {
		case len(policy.Spec.TargetRefs) > 1:
			message := "BackendTLSPolicy requires exactly one target reference"
			result.reject(gatewayv1.PolicyReasonInvalid, message)
			result.rejectRef("UnsupportedTargetRefs", message)
			if match >= wholeService && unsupported < 0 {
				unsupported = index
			}
		case match == missingPort:
			message := fmt.Sprintf("Service %s/%s has no port named %q", service.Namespace, service.Name, *policy.Spec.TargetRefs[0].SectionName)
			result.reject(gatewayv1.PolicyReasonTargetNotFound, message)
			result.rejectRef(gatewayv1.PolicyReasonTargetNotFound, message)
		default:
			validate(&result, policy.Spec, configMaps)
			if first, exists := winners[match]; exists {
				message := fmt.Sprintf("BackendTLSPolicy %s has precedence for the same target and section", selection.Policies[first].Key)
				result.reject(gatewayv1.PolicyReasonConflicted, message)
			} else {
				winners[match] = index
			}
		}
		selection.Policies = append(selection.Policies, result)
	}
	if index, exists := winners[namedPort]; exists {
		selection.Winner = &selection.Policies[index]
	} else if index, exists := winners[wholeService]; exists {
		selection.Winner = &selection.Policies[index]
	} else if unsupported >= 0 {
		// An unsupported multi-target policy cannot displace a single-target
		// policy or configure TLS. Block plaintext where it explicitly targets
		// this port and no supported target matches.
		selection.Winner = &selection.Policies[unsupported]
	}
	return selection
}

func matchTarget(service *corev1.Service, port *corev1.ServicePort, target gatewayv1.LocalPolicyTargetReferenceWithSectionName) targetMatch {
	if target.Group != "" || target.Kind != "Service" || string(target.Name) != service.Name {
		return unrelated
	}
	if target.SectionName == nil {
		return wholeService
	}
	name := string(*target.SectionName)
	if name != "" && name == port.Name {
		return namedPort
	}
	if name != "" && slices.ContainsFunc(service.Spec.Ports, func(candidate corev1.ServicePort) bool {
		return candidate.Name == name
	}) {
		return unrelated
	}
	return missingPort
}

func newPolicyResult(policy *gatewayv1.BackendTLSPolicy) PolicyResult {
	return PolicyResult{
		Key:        types.NamespacedName{Namespace: policy.Namespace, Name: policy.Name},
		UID:        policy.UID,
		Generation: policy.Generation,
		Accepted: metav1.Condition{
			Type:               string(gatewayv1.PolicyConditionAccepted),
			Status:             metav1.ConditionTrue,
			Reason:             string(gatewayv1.PolicyReasonAccepted),
			Message:            "Backend TLS validation is supported",
			ObservedGeneration: policy.Generation,
		},
		ResolvedRefs: metav1.Condition{
			Type:               string(gatewayv1.BackendTLSPolicyConditionResolvedRefs),
			Status:             metav1.ConditionTrue,
			Reason:             string(gatewayv1.BackendTLSPolicyReasonResolvedRefs),
			Message:            "All BackendTLSPolicy references are resolved",
			ObservedGeneration: policy.Generation,
		},
	}
}

func (r *PolicyResult) reject(reason gatewayv1.PolicyConditionReason, message string) {
	r.Accepted.Status = metav1.ConditionFalse
	r.Accepted.Reason = string(reason)
	r.Accepted.Message = message
	r.Validation = nil
}

func (r *PolicyResult) rejectRef(reason gatewayv1.PolicyConditionReason, message string) {
	r.ResolvedRefs.Status = metav1.ConditionFalse
	r.ResolvedRefs.Reason = string(reason)
	r.ResolvedRefs.Message = message
}
