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
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type backendTLSAvailability int

const (
	backendTLSUnknown backendTLSAvailability = iota
	backendTLSUnsupported
	backendTLSAvailable
)

func (r *Reconciler) backendTLSAvailability(ctx context.Context) backendTLSAvailability {
	if r.BackendTLSAvailable == nil {
		return backendTLSUnknown
	}
	supported, err := r.BackendTLSAvailable(ctx)
	switch {
	case err != nil:
		return backendTLSUnknown
	case !supported:
		return backendTLSUnsupported
	default:
		return backendTLSAvailable
	}
}

func supportsBackendTLS(backend model.BackendGroup, ordinaryHTTP bool) bool {
	return ordinaryHTTP && !backend.UsesH2C()
}

func (a backendTLSAvailability) validation(backend model.BackendGroup, ordinaryHTTP bool) *cloud.BackendTLSValidation {
	winner := backend.BackendTLS.Winner
	if winner == nil {
		return nil
	}
	if winner.Validation == nil || a != backendTLSAvailable || !supportsBackendTLS(backend, ordinaryHTTP) {
		return &cloud.BackendTLSValidation{Denied: true}
	}
	return &cloud.BackendTLSValidation{
		Hostname: winner.Validation.Hostname, CAConfigMapUID: string(winner.Validation.CA.UID),
		CADigest: winner.Validation.CA.Digest, CAPEM: slices.Clone(winner.Validation.CA.PEM),
	}
}

func (a backendTLSAvailability) apply(condition metav1.Condition, supportedProtocol bool) metav1.Condition {
	if condition.Status != metav1.ConditionTrue {
		return condition
	}
	switch {
	case !supportedProtocol:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionFalse, "UnsupportedProtocol", "Backend TLS requires an HTTPRoute with an HTTP/1 backend"
	case a == backendTLSUnsupported:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionFalse, "UnsupportedProvider", "The regional provider does not support authenticated backend TLS"
	case a == backendTLSUnknown:
		condition.Status, condition.Reason, condition.Message =
			metav1.ConditionUnknown, "Pending", "Regional provider support for authenticated backend TLS could not be verified"
	default:
		condition.Message = "Backend TLS validation is accepted by the Gateway"
	}
	return condition
}
