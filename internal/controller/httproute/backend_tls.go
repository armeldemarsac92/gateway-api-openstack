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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type backendTLSAvailability int

const (
	backendTLSUnknown backendTLSAvailability = iota
	backendTLSUnsupported
	backendTLSAvailable
)

type backendTLSRequirements struct {
	requireH2  bool
	requireSAN bool
	xBackend   bool
}

func (r *Reconciler) backendTLSAvailability(ctx context.Context, requirements backendTLSRequirements) backendTLSAvailability {
	discover := r.BackendTLSAvailable
	switch {
	case requirements.xBackend:
		if r.XBackendTLSAvailable == nil {
			return backendTLSUnknown
		}
		discover = func(ctx context.Context) (bool, error) {
			return r.XBackendTLSAvailable(ctx, requirements.requireSAN)
		}
	case requirements.requireSAN:
		if r.BackendTLSSANAvailable == nil {
			return backendTLSUnknown
		}
		discover = func(ctx context.Context) (bool, error) {
			return r.BackendTLSSANAvailable(ctx, requirements.requireH2)
		}
	case requirements.requireH2:
		discover = r.GRPCBackendTLSAvailable
	}
	if discover == nil {
		return backendTLSUnknown
	}
	supported, err := discover(ctx)
	switch {
	case err != nil:
		return backendTLSUnknown
	case !supported:
		return backendTLSUnsupported
	default:
		return backendTLSAvailable
	}
}

// discoverBackendTLSProfiles probes ordinary and explicit SAN policies
// separately, including when HTTP and gRPC share a graph.
func (r *Reconciler) discoverBackendTLSProfiles(ctx context.Context, routes []gatewayv1.HTTPRoute, input model.Input) map[backendTLSRequirements]backendTLSAvailability {
	modes := make(map[bool]struct{}, 2)
	for _, selection := range input.BackendTLSPolicies {
		winner := selection.Winner
		if winner != nil && winner.Validation != nil && winner.AncestorSlot == backendtls.AncestorSlotHeld {
			modes[len(winner.Validation.SubjectAltNames) != 0] = struct{}{}
		}
	}
	availability := make(map[backendTLSRequirements]backendTLSAvailability, 4)
	check := func(requirements backendTLSRequirements) {
		if _, checked := availability[requirements]; !checked {
			availability[requirements] = r.backendTLSAvailability(ctx, requirements)
		}
	}
	for i := range routes {
		requireH2 := model.HTTPProfileRouteKind(&routes[i]) == model.GRPCRouteKind
		for requireSAN := range modes {
			check(backendTLSRequirements{requireH2: requireH2, requireSAN: requireSAN})
		}
	}
	for _, backend := range input.XBackends {
		if validation := backend.TLS.Validation; validation != nil && backend.ReadIssue == nil {
			check(xBackendTLSRequirements(validation))
		}
	}
	return availability
}

func supportsBackendTLS(backend model.BackendGroup) bool {
	return backend.State == model.BackendResolved && !backend.UsesH2C()
}

func (inputs *routeModelInputs) backendTLSValidation(backend model.BackendGroup, requireH2 bool) *cloud.BackendTLSValidation {
	if backend.XBackendTLSRequired {
		if validation := backend.XBackendTLS; validation != nil && supportsBackendTLS(backend) && !requireH2 &&
			inputs.backendTLSAvailability[xBackendTLSRequirements(validation)] == backendTLSAvailable {
			return validation.CloudValidation()
		}
		return &cloud.BackendTLSValidation{Denied: true}
	}
	winner := backend.BackendTLS.Winner
	if winner == nil {
		return nil
	}
	if winner.AncestorSlot != backendtls.AncestorSlotHeld || winner.Validation == nil ||
		!supportsBackendTLS(backend) ||
		inputs.backendTLSAvailability[backendTLSRequirements{requireH2: requireH2, requireSAN: len(winner.Validation.SubjectAltNames) != 0}] != backendTLSAvailable {
		return &cloud.BackendTLSValidation{Denied: true}
	}
	return winner.Validation.CloudValidation()
}

func xBackendTLSRequirements(validation *backendtls.Validation) backendTLSRequirements {
	return backendTLSRequirements{xBackend: true, requireSAN: len(validation.SubjectAltNames) != 0}
}

func (inputs *routeModelInputs) xBackendTLSDiagnostic(backend model.BackendGroup) model.Diagnostic {
	if backend.Diagnostic != nil {
		return *backend.Diagnostic
	}
	requirements := xBackendTLSRequirements(backend.XBackendTLS)
	return xBackendTLSProviderDiagnostic(inputs.backendTLSAvailability[requirements], requirements)
}

func xBackendTLSProviderDiagnostic(availability backendTLSAvailability, requirements backendTLSRequirements) model.Diagnostic {
	condition := availability.applyXBackend(metav1.Condition{Status: metav1.ConditionTrue}, requirements.requireSAN)
	return model.Diagnostic{Reason: condition.Reason, Message: condition.Message}
}

func (a backendTLSAvailability) applyXBackend(condition metav1.Condition, requireSAN bool) metav1.Condition {
	condition = a.apply(condition, true, requireSAN)
	if condition.Reason == "UnsupportedProvider" {
		condition.Message = "XBackend ServerOnly requires Octavia API microversion 2.24 or newer and regional backend TLS support"
		if requireSAN {
			condition.Message += " for explicit subjectAltNames"
		}
	}
	return condition
}

func routeBackendTLSAncestorSlot(route *gatewayv1.HTTPRoute, inputs *routeModelInputs) backendtls.AncestorSlot {
	var slot backendtls.AncestorSlot
	if inputs.backendTLSAncestor == nil {
		return slot
	}
	for _, compiled := range inputs.logical.Routes {
		if compiled.Source.UID != route.UID || compiled.Source.Namespace != route.Namespace || compiled.Source.Name != route.Name {
			continue
		}
		for _, rule := range compiled.Rules {
			if rule.Redirect != nil {
				continue
			}
			for _, backend := range rule.Backends {
				if winner := backend.BackendTLS.Winner; backend.Weight > 0 && winner != nil {
					slot = max(slot, winner.AncestorSlot)
				}
			}
		}
	}
	return slot
}

func (a backendTLSAvailability) apply(condition metav1.Condition, supportedProtocol, requireSAN bool) metav1.Condition {
	if condition.Status != metav1.ConditionTrue {
		return condition
	}
	profile := "authenticated backend TLS"
	if requireSAN {
		profile += " with explicit subjectAltNames"
	}
	switch {
	case !supportedProtocol:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionFalse, "UnsupportedProtocol", "The backend Service protocol is incompatible with BackendTLSPolicy"
	case a == backendTLSUnsupported:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionFalse, "UnsupportedProvider", "The regional provider does not support "+profile
	case a == backendTLSUnknown:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionUnknown, "Pending", "Regional provider support for "+profile+" could not be verified"
	default:
		condition.Message = "Backend TLS validation is accepted by the Gateway"
	}
	return condition
}
