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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

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

var errXBackendPersistenceDiscovery = errors.New("regional Octavia session persistence capability discovery is unavailable")

func (r *Reconciler) xBackendPersistenceCapability(ctx context.Context) func(gatewayv1.SessionPersistence) cloud.SessionPersistenceIssue {
	header := sync.OnceValue(func() cloud.SessionPersistenceIssue {
		if r.XBackendHeaderAvailable == nil {
			return cloud.SessionPersistenceHeaderUnverified
		}
		return cloud.HeaderPersistenceDiscoveryIssue(r.XBackendHeaderAvailable(ctx))
	})
	cookies := make(map[cloud.HTTPSessionCookieRequirements]cloud.SessionPersistenceIssue)
	return func(persistence gatewayv1.SessionPersistence) cloud.SessionPersistenceIssue {
		if model.IsHeaderSessionPersistence(persistence) {
			return header()
		}
		requirements := model.XBackendCookieRequirements(persistence)
		if r.XBackendCookieAvailable == nil {
			return requirements.DiscoveryIssue(false, errXBackendPersistenceDiscovery)
		}
		if issue, ok := cookies[requirements]; ok {
			return issue
		}
		issue := requirements.DiscoveryIssue(r.XBackendCookieAvailable(ctx, requirements))
		cookies[requirements] = issue
		return issue
	}
}

func xBackendPersistenceCondition(status metav1.ConditionStatus, reason, message string, issue cloud.SessionPersistenceIssue) (metav1.ConditionStatus, string, string) {
	return sessionPersistenceCondition("XBackend", status, reason, message, issue)
}

func sessionPersistenceCondition(subject string, status metav1.ConditionStatus, reason, message string, issue cloud.SessionPersistenceIssue) (metav1.ConditionStatus, string, string) {
	switch issue {
	case cloud.SessionPersistenceHeaderUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for " + subject + " HTTP header persistence could not be verified"
	case cloud.SessionPersistenceHeaderReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the requested " + subject + " HTTP header persistence"
	case cloud.SessionPersistenceRevisionUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the current " + subject + " session persistence configuration"
	case cloud.SessionPersistenceHeaderUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "The regional Octavia provider does not advertise http_header_persistence_v1"
	case cloud.SessionPersistenceUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for " + subject + " HTTP session cookies could not be verified"
	case cloud.SessionPersistenceDeadlineUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for " + subject + " HTTP session cookie absoluteTimeout could not be verified"
	case cloud.SessionPersistenceDeadlineReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed an ACTIVE pool with the requested " + subject + " HTTP session cookie absoluteTimeout and scope"
	case cloud.SessionPersistencePermanentUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for " + subject + " Permanent cookies could not be verified"
	case cloud.SessionPersistencePermanentReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the requested Permanent cookie settings on all pools for this " + subject
	case cloud.SessionPersistencePathUnverified:
		return metav1.ConditionUnknown, "Pending", "Regional Octavia support for the " + subject + " HTTP session cookie path could not be verified"
	case cloud.SessionPersistencePathReadbackUnverified:
		return metav1.ConditionUnknown, "Pending", "Octavia has not confirmed the requested " + subject + " HTTP session cookie path"
	case cloud.SessionPersistencePathUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), subject + " custom cookie paths require http_cookie_path_v1 in addition to the regional Octavia capabilities for the requested cookie lifetime"
	case cloud.SessionPersistenceUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "The regional Octavia provider does not advertise http_cookie_name_v1"
	case cloud.SessionPersistenceDeadlineUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), subject + " session cookie absoluteTimeout requires both http_cookie_name_v1 and http_cookie_absolute_timeout_v1 from the regional Octavia provider"
	case cloud.SessionPersistencePermanentUnsupported:
		return metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), subject + " Permanent cookies require http_cookie_name_v1, http_cookie_absolute_timeout_v1 and http_cookie_permanent_v1 from the regional Octavia provider"
	default:
		return status, reason, message
	}
}

func (s routeReconcileStatus) withBackendPersistenceIssue(issue cloud.SessionPersistenceIssue, service bool) routeReconcileStatus {
	subject := "XBackend"
	if service {
		subject = "Service backend"
	}
	if s.resolved != metav1.ConditionFalse {
		s.resolved, s.resolvedReason, s.resolvedMessage = sessionPersistenceCondition(subject, s.resolved, s.resolvedReason, s.resolvedMessage, issue)
	}
	return s
}

const servicePersistenceRevisionCondition = "/ServiceSessionConfiguration"

// A policy can change while the owning Route keeps the same generation. This
// digest ties optional reuse of a previous provider observation to the same
// projected Service pool affinity configuration, including its removal.
func servicePersistenceRevision(spec cloud.RouteSpec) string {
	digest := sha256.New()
	observed := false
	record := func(port, backendPort int, namespace, name, rule, uid string, settings *cloud.ServiceSessionPersistence, pending bool) {
		if settings == nil && !pending {
			return
		}
		observed = true
		config, _ := json.Marshal(settings)
		_, _ = fmt.Fprintf(digest, "%d %q %q %d %t %s\n", port, namespace, name, backendPort, pending, config)
		if rule != "" {
			_, _ = fmt.Fprintf(digest, "rule %q %q\n", rule, uid)
		}
	}
	for _, part := range append([]cloud.RouteSpec{spec}, spec.AdditionalHTTPPorts...) {
		port := part.GatewayRequirements.ListenerPort
		if len(part.ScopedListenerPorts) != 0 {
			port = part.ScopedListenerPorts[0]
		}
		record(port, 0, "", "", "", "", part.ServiceSessionPersistence, part.ServiceSessionPersistencePending)
		for _, backend := range part.HTTPBackends {
			record(port, backend.Port, backend.Namespace, backend.Name, backend.SessionPersistenceRule, backend.SessionPersistenceServiceUID, backend.ServiceSessionPersistence, backend.ServiceSessionPersistencePending)
		}
	}
	if !observed {
		return ""
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

// Preserve the condition key used to store cookie revisions on existing routes.
const xBackendPersistenceRevisionCondition = "/XBackendCookieConfiguration"

// A Route's generation does not include edits to the XBackends it references.
// Bind deferred Octavia readback to the backend revisions confirmed on that parent.
func xBackendPersistenceRevision(route *gatewayv1.HTTPRoute, observations []model.XBackendObservation) string {
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
		if model.IsHeaderSessionPersistence(*backend.Spec.SessionPersistence) {
			// The generation tracks changes in the validated spec, including the
			// header name, timeout, transport and persistence removal.
			_, _ = fmt.Fprintf(digest, "header %q %q %q %d\n", key.Namespace, key.Name, backend.UID, backend.Generation)
			continue
		}
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
