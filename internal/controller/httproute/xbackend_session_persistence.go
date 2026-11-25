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
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
)

type xBackendCookieAvailability func(cloud.HTTPSessionCookieRequirements) (bool, error)

func (r *Reconciler) xBackendCookieCapability(ctx context.Context) xBackendCookieAvailability {
	type observation struct {
		available bool
		err       error
	}
	observations := make(map[cloud.HTTPSessionCookieRequirements]observation)
	return func(requirements cloud.HTTPSessionCookieRequirements) (bool, error) {
		if r.XBackendCookieAvailable == nil {
			return false, errXBackendCookieDiscovery
		}
		if seen, ok := observations[requirements]; ok {
			return seen.available, seen.err
		}
		available, err := r.XBackendCookieAvailable(ctx, requirements)
		observations[requirements] = observation{available, err}
		return available, err
	}
}

var errXBackendCookieDiscovery = errors.New("regional Octavia HTTP cookie capability discovery is unavailable")

func (observe xBackendCookieAvailability) apply(status metav1.ConditionStatus, reason, message string, requirements cloud.HTTPSessionCookieRequirements) (metav1.ConditionStatus, string, string) {
	available, err := observe(requirements)
	return xBackendCookieCondition(status, reason, message, requirements.DiscoveryIssue(available, err))
}

func xBackendCookieCondition(status metav1.ConditionStatus, reason, message string, issue cloud.SessionPersistenceIssue) (metav1.ConditionStatus, string, string) {
	switch issue {
	case cloud.SessionPersistenceUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for XBackend HTTP session cookies could not be verified"
	case cloud.SessionPersistenceDeadlineUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for XBackend HTTP session cookie absoluteTimeout could not be verified"
	case cloud.SessionPersistenceDeadlineReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed an ACTIVE pool with the requested XBackend HTTP session cookie absoluteTimeout and scope"
	case cloud.SessionPersistencePermanentUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for XBackend Permanent cookies could not be verified"
	case cloud.SessionPersistencePermanentReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the requested Permanent cookie settings on all pools for this XBackend"
	case cloud.SessionPersistencePathUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for the XBackend HTTP session cookie path could not be verified"
	case cloud.SessionPersistencePathReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the requested XBackend HTTP session cookie path"
	case cloud.SessionPersistencePathUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "XBackend custom cookie paths require http_cookie_path_v1 in addition to the regional Octavia capabilities for the requested cookie lifetime"
	case cloud.SessionPersistenceUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "The regional Octavia provider does not advertise http_cookie_name_v1"
	case cloud.SessionPersistenceDeadlineUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "XBackend session cookie absoluteTimeout requires both http_cookie_name_v1 and http_cookie_absolute_timeout_v1 from the regional Octavia provider"
	case cloud.SessionPersistencePermanentUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "XBackend Permanent cookies require http_cookie_name_v1, http_cookie_absolute_timeout_v1 and http_cookie_permanent_v1 from the regional Octavia provider"
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

const xBackendCookieRevisionCondition = "/XBackendCookieConfiguration"

// A Route's generation does not include edits to the XBackends it references.
// Bind deferred Octavia readback to the backend revisions confirmed on that parent.
func xBackendCookieRevision(route *gatewayv1.HTTPRoute, observations []model.XBackendObservation) string {
	digest := sha256.New()
	seen := make(map[types.NamespacedName]bool)
	for _, key := range model.LocalXBackendReferences(route) {
		if seen[key] {
			continue
		}
		index := slices.IndexFunc(observations, func(observation model.XBackendObservation) bool { return observation.Key == key })
		if index < 0 {
			continue
		}
		backend := observations[index]
		if backend.Spec.SessionPersistence == nil {
			continue
		}
		seen[key] = true
		path := "/"
		if cookie := backend.Spec.SessionPersistence.Cookie; cookie != nil && cookie.Path != nil {
			path = *cookie.Path
		}
		_, _ = fmt.Fprintf(digest, "%q %q %q %d %q\n", key.Namespace, key.Name, backend.UID, backend.Generation, path)
	}
	if len(seen) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

const xBackendTransportRevisionCondition = "/XBackendTransportConfiguration"

func xBackendTransportRevision(route *gatewayv1.HTTPRoute, observations []model.XBackendObservation) string {
	digest := sha256.New()
	seen := make(map[types.NamespacedName]bool)
	for _, key := range model.LocalXBackendReferences(route) {
		if seen[key] {
			continue
		}
		index := slices.IndexFunc(observations, func(observation model.XBackendObservation) bool { return observation.Key == key })
		if index < 0 || observations[index].ReadIssue != nil {
			continue
		}
		seen[key] = true
		backend := observations[index]
		var protocol gatewayx.BackendProtocol
		if backend.Spec.Protocol != nil {
			protocol = *backend.Spec.Protocol
		}
		var tlsMode gatewayx.BackendTLSMode
		if backend.Spec.TLS != nil {
			tlsMode = backend.Spec.TLS.Mode
		}
		_, _ = fmt.Fprintf(digest, "%q %q %q %d %q %q %q\n", key.Namespace, key.Name, backend.UID, backend.Generation, backend.Spec.Type, protocol, tlsMode)
	}
	if len(seen) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func (r *Reconciler) currentXBackendRevision(route *gatewayv1.HTTPRoute, parentRef gatewayv1.ParentReference, conditionType, revision string) bool {
	for _, parent := range route.Status.Parents {
		if parent.ControllerName != r.Config.ControllerName ||
			!controller.ParentRefsEqual(parent.ParentRef, route.Namespace, parentRef, route.Namespace) {
			continue
		}
		proof := meta.FindStatusCondition(parent.Conditions, r.Config.Domain()+conditionType)
		if proof != nil {
			return proof.Status == metav1.ConditionTrue && proof.ObservedGeneration == route.Generation && proof.Message == revision
		}
		break
	}
	return revision == ""
}

func (r *Reconciler) liveXBackendRevisions(ctx context.Context, route *gatewayv1.HTTPRoute) ([]model.XBackendObservation, error) {
	var observations []model.XBackendObservation
	seen := make(map[types.NamespacedName]bool)
	for _, key := range model.LocalXBackendReferences(route) {
		if seen[key] {
			continue
		}
		seen[key] = true
		var backend gatewayx.XBackend
		if err := r.routePatchReader().Get(ctx, key, &backend); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("read XBackend %s for parent status: %w", key, err)
		}
		if backend.DeletionTimestamp.IsZero() {
			observations = append(observations, model.XBackendObservation{
				Key: key, UID: backend.UID, Generation: backend.Generation, Spec: backend.Spec,
			})
		}
	}
	return observations, nil
}
