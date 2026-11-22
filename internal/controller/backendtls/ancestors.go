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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const MaxAncestors = 16

const (
	AncestorLimitReason  = "BackendTLSPolicyAncestorLimit"
	AncestorLimitMessage = "A BackendTLSPolicy used by this route has no room for this Gateway in its ancestor status"
)

// AncestorSlot is the capacity observation used by the Gateway forwarding graph.
type AncestorSlot uint8

const (
	AncestorSlotHeld AncestorSlot = iota
	AncestorSlotPending
	AncestorSlotFull
)

type GatewayAncestor struct {
	Key            types.NamespacedName
	ControllerName gatewayv1.GatewayController
}

func (a GatewayAncestor) Slot(policy *gatewayv1.BackendTLSPolicy) AncestorSlot {
	for _, entry := range policy.Status.Ancestors {
		if entry.ControllerName == a.ControllerName && GatewayAncestorKey(entry.AncestorRef, policy.Namespace) == a.Key {
			return AncestorSlotHeld
		}
	}
	if len(policy.Status.Ancestors) >= MaxAncestors {
		return AncestorSlotFull
	}
	return AncestorSlotPending
}

// GatewayAncestorKey recognizes references to a Gateway as a whole.
func GatewayAncestorKey(ref gatewayv1.ParentReference, namespace string) types.NamespacedName {
	if ptr.Deref(ref.Group, gatewayv1.GroupName) != gatewayv1.GroupName ||
		ptr.Deref(ref.Kind, "Gateway") != "Gateway" || ref.SectionName != nil || ref.Port != nil {
		return types.NamespacedName{}
	}
	return types.NamespacedName{
		Namespace: string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(namespace))),
		Name:      string(ref.Name),
	}
}
