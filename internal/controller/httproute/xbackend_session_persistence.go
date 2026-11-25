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
	"errors"
	"sync"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type xBackendCookieCapability func() (bool, error)

func (r *Reconciler) xBackendCookieCapability(ctx context.Context) xBackendCookieCapability {
	return sync.OnceValues(func() (bool, error) {
		if r.XBackendCookieAvailable == nil {
			return false, errXBackendCookieDiscovery
		}
		return r.XBackendCookieAvailable(ctx)
	})
}

var errXBackendCookieDiscovery = errors.New("regional Octavia HTTP cookie capability discovery is unavailable")

func (observe xBackendCookieCapability) apply(status metav1.ConditionStatus, reason, message string) (metav1.ConditionStatus, string, string) {
	available, err := observe()
	switch {
	case err != nil:
		return xBackendCookieCondition(status, reason, message, cloud.SessionPersistenceUnverified)
	case !available:
		return xBackendCookieCondition(status, reason, message, cloud.SessionPersistenceUnsupported)
	default:
		return status, reason, message
	}
}

func xBackendCookieCondition(status metav1.ConditionStatus, reason, message string, issue cloud.SessionPersistenceIssue) (metav1.ConditionStatus, string, string) {
	switch issue {
	case cloud.SessionPersistenceUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for XBackend HTTP session cookies could not be verified"
	case cloud.SessionPersistenceUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "The regional Octavia provider does not advertise http_cookie_name_v1"
	default:
		return status, reason, message
	}
}

func (s routeReconcileStatus) withXBackendCookieIssue(issue cloud.SessionPersistenceIssue) routeReconcileStatus {
	if s.resolved != metav1.ConditionFalse {
		s.resolved, s.resolvedReason, s.resolvedMessage = xBackendCookieCondition(s.resolved, s.resolvedReason, s.resolvedMessage, issue)
	}
	return s
}
