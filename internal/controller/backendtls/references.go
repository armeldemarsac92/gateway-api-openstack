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

package backendtls

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ServiceKeys returns local Service targets irrespective of their port section.
func ServiceKeys(policy *gatewayv1.BackendTLSPolicy) []types.NamespacedName {
	var keys []types.NamespacedName
	for _, target := range policy.Spec.TargetRefs {
		if target.Group == "" && target.Kind == "Service" && target.Name != "" {
			key := types.NamespacedName{Namespace: policy.Namespace, Name: string(target.Name)}
			if !slices.Contains(keys, key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// AppliesToServicePort reports whether validating the policy requires reading
// its CA for this Service port.
func AppliesToServicePort(policy *gatewayv1.BackendTLSPolicy, service *corev1.Service, port *corev1.ServicePort) bool {
	return policy.Namespace == service.Namespace && slices.ContainsFunc(policy.Spec.TargetRefs, func(target gatewayv1.LocalPolicyTargetReferenceWithSectionName) bool {
		return matchTarget(service, port, target) >= wholeService
	})
}

// CAKey identifies one supported CA reference and its scope.
func CAKey(policy *gatewayv1.BackendTLSPolicy, bundlesEnabled bool) (types.NamespacedName, bool) {
	validation := policy.Spec.Validation
	if len(policy.Spec.TargetRefs) != 1 || len(validation.CACertificateRefs) != 1 || validation.WellKnownCACertificates != nil {
		return types.NamespacedName{}, false
	}
	ref := validation.CACertificateRefs[0]
	switch {
	case ref.Group == "" && ref.Kind == "ConfigMap" && ref.Name != "":
		return types.NamespacedName{Namespace: policy.Namespace, Name: string(ref.Name)}, true
	case bundlesEnabled && IsClusterTrustBundle(ref.Group, ref.Kind) && ValidClusterTrustBundleName(string(ref.Name)):
		return types.NamespacedName{Name: string(ref.Name)}, true
	default:
		return types.NamespacedName{}, false
	}
}
