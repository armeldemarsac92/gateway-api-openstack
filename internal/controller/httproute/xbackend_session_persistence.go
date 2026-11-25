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

type xBackendCookieAvailability func(bool) (bool, error)

func (r *Reconciler) xBackendCookieCapability(ctx context.Context) xBackendCookieAvailability {
	discover := func(withDeadline bool) func() (bool, error) {
		return sync.OnceValues(func() (bool, error) {
			if r.XBackendCookieAvailable == nil {
				return false, errXBackendCookieDiscovery
			}
			return r.XBackendCookieAvailable(ctx, withDeadline)
		})
	}
	named, timed := discover(false), discover(true)
	return func(withDeadline bool) (bool, error) {
		if withDeadline {
			return timed()
		}
		return named()
	}
}

var errXBackendCookieDiscovery = errors.New("regional Octavia HTTP cookie capability discovery is unavailable")

func (observe xBackendCookieAvailability) apply(status metav1.ConditionStatus, reason, message string, withDeadline bool) (metav1.ConditionStatus, string, string) {
	available, err := observe(withDeadline)
	switch {
	case err != nil && withDeadline:
		return xBackendCookieCondition(status, reason, message, cloud.SessionPersistenceDeadlineUnverified)
	case err != nil:
		return xBackendCookieCondition(status, reason, message, cloud.SessionPersistenceUnverified)
	case !available && withDeadline:
		return xBackendCookieCondition(status, reason, message, cloud.SessionPersistenceDeadlineUnsupported)
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
	case cloud.SessionPersistenceDeadlineUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for XBackend HTTP session cookie absoluteTimeout could not be verified"
	case cloud.SessionPersistenceDeadlineReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed an ACTIVE pool with the requested XBackend HTTP session cookie absoluteTimeout and scope"
	case cloud.SessionPersistenceUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "The regional Octavia provider does not advertise http_cookie_name_v1"
	case cloud.SessionPersistenceDeadlineUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "XBackend session cookie absoluteTimeout requires both http_cookie_name_v1 and http_cookie_absolute_timeout_v1 from the regional Octavia provider"
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
