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
	requireH2      bool
	requireSAN     bool
	xBackend       bool
	systemCA       bool
	grpcAuthorizer bool
}

func (r *Reconciler) backendTLSAvailability(ctx context.Context, requirements backendTLSRequirements) backendTLSAvailability {
	discover := r.BackendTLSAvailable
	switch {
	case requirements.systemCA:
		if r.BackendTLSSystemCAAvailable == nil {
			return backendTLSUnknown
		}
		discover = func(ctx context.Context) (bool, error) {
			return r.BackendTLSSystemCAAvailable(ctx, requirements.requireH2, requirements.requireSAN, requirements.xBackend)
		}
	case requirements.xBackend:
		if r.XBackendTLSAvailable == nil {
			return backendTLSUnknown
		}
		discover = func(ctx context.Context) (bool, error) {
			return r.XBackendTLSAvailable(ctx, requirements.requireH2, requirements.requireSAN)
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
	return discoverBackendTLSAvailability(ctx, discover)
}

func discoverBackendTLSAvailability(ctx context.Context, discover func(context.Context) (bool, error)) backendTLSAvailability {
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
	modes := make(map[backendTLSRequirements]struct{})
	for _, selection := range input.BackendTLSPolicies {
		winner := selection.Winner
		if winner != nil && winner.Validation != nil && winner.AncestorSlot == backendtls.AncestorSlotHeld {
			modes[policyBackendTLSRequirements(winner.Validation, false)] = struct{}{}
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
		for requirements := range modes {
			requirements.requireH2 = requireH2
			check(requirements)
		}
	}
	for _, backend := range input.XBackends {
		if validation := backend.TLS.Validation; validation != nil && backend.ReadIssue == nil {
			check(xBackendTLSRequirements(validation, model.XBackendRequiresH2(backend.Spec)))
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
			inputs.backendTLSAvailability[xBackendTLSRequirements(validation, backend.XBackendRequiresH2)] == backendTLSAvailable {
			tls := validation.CloudValidation()
			tls.RequireH2 = backend.XBackendRequiresH2
			return tls
		}
		return &cloud.BackendTLSValidation{Denied: true, RequireH2: backend.XBackendRequiresH2}
	}
	winner := backend.BackendTLS.Winner
	if winner == nil {
		return nil
	}
	if winner.AncestorSlot != backendtls.AncestorSlotHeld || winner.Validation == nil ||
		!supportsBackendTLS(backend) ||
		inputs.backendTLSAvailability[policyBackendTLSRequirements(winner.Validation, requireH2)] != backendTLSAvailable {
		return &cloud.BackendTLSValidation{Denied: true}
	}
	return winner.Validation.CloudValidation()
}

func policyBackendTLSRequirements(validation *backendtls.Validation, requireH2 bool) backendTLSRequirements {
	return backendTLSRequirements{requireH2: requireH2, requireSAN: len(validation.SubjectAltNames) != 0,
		systemCA: validation.Trust == cloud.BackendTLSTrustSystem}
}

func xBackendTLSRequirements(validation *backendtls.Validation, requireH2 bool) backendTLSRequirements {
	requirements := policyBackendTLSRequirements(validation, requireH2)
	requirements.xBackend = true
	return requirements
}

func (inputs *routeModelInputs) xBackendTLSDiagnostic(backend model.BackendGroup) model.Diagnostic {
	if backend.Diagnostic != nil {
		return *backend.Diagnostic
	}
	requirements := xBackendTLSRequirements(backend.XBackendTLS, backend.XBackendRequiresH2)
	return xBackendTLSProviderDiagnostic(inputs.backendTLSAvailability[requirements], requirements)
}

func xBackendTLSProviderDiagnostic(availability backendTLSAvailability, requirements backendTLSRequirements) model.Diagnostic {
	condition := availability.applyXBackend(metav1.Condition{Status: metav1.ConditionTrue}, requirements)
	return model.Diagnostic{Reason: condition.Reason, Message: condition.Message}
}

func (a backendTLSAvailability) applyXBackend(condition metav1.Condition, requirements backendTLSRequirements) metav1.Condition {
	condition = a.apply(condition, true, requirements.requireSAN, requirements.systemCA)
	if condition.Reason == "UnsupportedProvider" {
		condition.Message = "Authenticated XBackends require Octavia API microversion 2.24 or newer and regional backend TLS support"
		if requirements.requireSAN {
			condition.Message += " for explicit subjectAltNames"
		}
	}
	if requirements.requireH2 && condition.Status != metav1.ConditionTrue {
		condition.Message += " and mandatory h2 ALPN"
	}
	return condition
}

func routeBackendTLSAncestorSlot(rules []model.Rule) backendtls.AncestorSlot {
	var slot backendtls.AncestorSlot
	for _, rule := range rules {
		if rule.Redirect != nil {
			continue
		}
		for _, backend := range rule.Backends {
			if winner := backend.BackendTLS.Winner; backend.Weight > 0 && winner != nil {
				slot = max(slot, winner.AncestorSlot)
			}
		}
	}
	return slot
}

func (a backendTLSAvailability) apply(condition metav1.Condition, supportedProtocol, requireSAN, systemCA bool) metav1.Condition {
	if condition.Status != metav1.ConditionTrue {
		return condition
	}
	profile := "authenticated backend TLS"
	if systemCA {
		profile += " using system CA certificates"
	}
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
	if systemCA && condition.Status == metav1.ConditionFalse {
		condition.Reason = string(gatewayv1.PolicyReasonInvalid)
	}
	return condition
}
