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
	"cmp"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const maxXBackendAncestors = 32
const xBackendClientCertificateResyncInterval = 5 * time.Second

// xBackendStatusReconciler only publishes status. HTTPRoute reconciles the
// backend's cloud resources through the existing Gateway graph writer.
type xBackendStatusReconciler struct {
	routes *Reconciler
}

func (r *xBackendStatusReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	backend := &gatewayx.XBackend{}
	if err := r.routes.APIReader.Get(ctx, request.NamespacedName, backend); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A finalizer may keep a terminating XBackend visible after its routes
	// have withdrawn it. Clear our status while the object still exists.
	var ancestors map[types.NamespacedName]metav1.Condition
	if backend.DeletionTimestamp.IsZero() {
		var err error
		ancestors, err = r.observeAncestors(ctx, request.NamespacedName, backend)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	base := backend.DeepCopy()
	result := ctrl.Result{}
	if len(ancestors) > 0 && (r.routes.Config.BackendTLSPolicy && model.XBackendRequiresTLS(backend.Spec) ||
		r.routes.Config.BackendH2C && model.XBackendUsesH2C(backend.Spec) ||
		r.routes.Config.XBackendSessionPersistenceEnabled() && backend.Spec.SessionPersistence != nil) {
		result.RequeueAfter = controller.OpenStackResyncAfter(r.routes.Config.OpenStackResyncInterval, backend.UID)
		if r.routes.Config.XBackendClientCertificate && backend.Spec.TLS != nil && backend.Spec.TLS.Mode == gatewayx.BackendTLSModeClientAndServer {
			result.RequeueAfter = min(result.RequeueAfter, xBackendClientCertificateResyncInterval)
		}
	}
	backend.Status.Ancestors = mergeXBackendAncestors(backend, r.routes.Config.ControllerName, ancestors)
	if equality.Semantic.DeepEqual(base.Status, backend.Status) {
		return result, nil
	}
	return result, client.IgnoreNotFound(r.routes.Status().Patch(ctx, backend, controller.OptimisticMergeFrom(base)))
}

func (r *xBackendStatusReconciler) observeAncestors(ctx context.Context, key types.NamespacedName, backend *gatewayx.XBackend) (map[types.NamespacedName]metav1.Condition, error) {
	candidates, err := r.routes.listXBackendConsumers(ctx, key)
	if err != nil {
		return nil, err
	}
	byGateway := map[types.NamespacedName]*gatewayv1.Gateway{}
	for i := range candidates {
		var route gatewayv1.HTTPRoute
		err := r.routes.APIReader.Get(ctx, client.ObjectKeyFromObject(&candidates[i]), &route)
		if err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		if !route.DeletionTimestamp.IsZero() || !slices.Contains(model.LocalXBackendReferences(&route), key) {
			continue
		}
		parents, err := r.routes.managedParentsWithReader(ctx, r.routes.APIReader, &route)
		if err != nil {
			return nil, err
		}
		// A class held by its referencing Gateways still owns them.
		for _, group := range groupManagedRouteParents(parents) {
			if managedParentsSupportInstalledVersion(group.parents) {
				byGateway[client.ObjectKeyFromObject(group.gateway)] = group.gateway
			}
		}
	}
	ancestors := make(map[types.NamespacedName]metav1.Condition, len(byGateway))
	persistenceCapability := r.routes.xBackendPersistenceCapability(ctx)
	h2cDiagnostic := sync.OnceValue(func() model.Diagnostic { return r.routes.xBackendH2CDiagnostic(ctx) })
	tlsObserver := graph.NewBackendTLSObserver(r.routes.APIReader, r.routes.Config.ClusterTrustBundles)
	for gatewayKey, gateway := range byGateway {
		attached, err := r.hasAttachedConsumer(ctx, gateway, key)
		if err != nil {
			return nil, err
		}
		if !attached {
			continue
		}
		profile, err := r.routes.xBackendProfile(gateway)
		if err != nil {
			return nil, err
		}
		condition := xBackendAcceptedCondition(backend, profile)
		if condition.Status != metav1.ConditionFalse && model.XBackendRequiresTLS(backend.Spec) {
			var observation model.XBackendObservation
			if err := tlsObserver.ObserveXBackendTLS(ctx, &observation, backend, gateway.Spec.GatewayClassName, r.routes.XBackendClientCertificates); err != nil {
				return nil, err
			}
			validation := observation.TLS
			switch {
			case validation.Pending:
				condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, string(validation.Reason), validation.Message
			case validation.Validation == nil:
				condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, string(validation.Reason), validation.Message
			case observation.ClientCertificateIssue != nil:
				issue := observation.ClientCertificateIssue
				condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, issue.Reason, issue.Message
				if issue.Reason == string(gatewayv1.RouteReasonPending) {
					condition.Status = metav1.ConditionUnknown
				}
			default:
				requirements := xBackendTLSRequirements(validation.Validation, model.XBackendRequiresH2(backend.Spec))
				availability := r.routes.backendTLSAvailability(ctx, requirements)
				if availability != backendTLSAvailable || condition.Status == metav1.ConditionTrue {
					condition.Status = metav1.ConditionTrue
					condition = availability.applyXBackend(condition, requirements)
				}
			}
		}
		if condition.Status == metav1.ConditionTrue && model.XBackendUsesH2C(backend.Spec) {
			if diagnostic := h2cDiagnostic(); diagnostic.Reason != "" {
				condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message
				if diagnostic.Reason == string(gatewayv1.RouteReasonPending) {
					condition.Status = metav1.ConditionUnknown
				}
			}
		}
		if condition.Status == metav1.ConditionTrue && backend.Spec.SessionPersistence != nil {
			condition.Status, condition.Reason, condition.Message = xBackendPersistenceCondition(condition.Status, condition.Reason, condition.Message,
				persistenceCapability(*backend.Spec.SessionPersistence))
		}
		ancestors[gatewayKey] = condition
	}
	return ancestors, nil
}

func (r *xBackendStatusReconciler) hasAttachedConsumer(ctx context.Context, gateway *gatewayv1.Gateway, backend types.NamespacedName) (bool, error) {
	candidates, err := r.routes.liveGatewayRoutePeers(ctx, gateway)
	if err != nil {
		return false, err
	}
	admission, err := r.routes.observeRouteGraphAdmission(ctx, gateway, candidates, nil)
	if err != nil {
		return false, err
	}
	for i := range candidates {
		route := &candidates[i]
		if _, eligible := admission.eligible[routeKey(route)]; eligible && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind &&
			slices.Contains(model.LocalXBackendReferences(route), backend) {
			return true, nil
		}
	}
	return false, nil
}

func (r *Reconciler) xBackendH2CDiagnostic(ctx context.Context) model.Diagnostic {
	return r.backendH2CDiagnostic(ctx, "XBackends")
}

func (r *Reconciler) backendH2CDiagnostic(ctx context.Context, kind string) model.Diagnostic {
	if r.BackendH2CAvailable != nil {
		available, err := r.BackendH2CAvailable(ctx)
		if err == nil {
			if available {
				return model.Diagnostic{}
			}
			return model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: "The regional provider does not support cleartext HTTP/2 for " + kind,
			}
		}
	}
	return model.Diagnostic{
		Reason: string(gatewayv1.RouteReasonPending), Message: "Regional provider support for cleartext HTTP/2 could not be verified",
	}
}

func xBackendAcceptedCondition(backend *gatewayx.XBackend, profile model.XBackendProfile) metav1.Condition {
	condition := metav1.Condition{
		Type: string(gatewayx.BackendConditionAccepted), Status: metav1.ConditionTrue, Reason: string(gatewayx.BackendReasonAccepted),
		Message: "XBackend is accepted by the Gateway", ObservedGeneration: backend.Generation,
	}
	pending := profile.MemberMode == ""
	if pending {
		profile.MemberMode = cloud.MemberModeEndpoint
	}
	if diagnostic := model.ValidateXBackend(backend.Spec, profile); diagnostic != nil {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message
	} else if persistence := backend.Spec.SessionPersistence; persistence != nil && !model.IsHeaderSessionPersistence(*persistence) && persistence.AbsoluteTimeout != nil && backend.UID == "" {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), "XBackend session cookie absoluteTimeout needs an observed XBackend UID"
	} else if pending {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "Pending", "Gateway member mode is pending"
	}
	return condition
}

func mergeXBackendAncestors(backend *gatewayx.XBackend, controllerName gatewayv1.GatewayController, desired map[types.NamespacedName]metav1.Condition) []gatewayx.BackendAncestorStatus {
	var merged []gatewayx.BackendAncestorStatus
	remaining := maps.Clone(desired)
	for _, existing := range backend.Status.Ancestors {
		if existing.ControllerName != controllerName {
			merged = append(merged, existing)
			continue
		}
		if !controller.IsGatewayParentRef(existing.AncestorRef) {
			continue
		}
		key := types.NamespacedName{
			Namespace: string(ptr.Deref(existing.AncestorRef.Namespace, gatewayv1.Namespace(backend.Namespace))),
			Name:      string(existing.AncestorRef.Name),
		}
		if condition, keep := remaining[key]; keep {
			existing.Conditions = slices.Clone(existing.Conditions)
			meta.SetStatusCondition(&existing.Conditions, condition)
			existing.AncestorRef = gatewayAncestorRef(key)
			merged = append(merged, existing)
			delete(remaining, key)
		}
	}
	for _, key := range slices.SortedFunc(maps.Keys(remaining), func(a, b types.NamespacedName) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}) {
		if len(merged) >= maxXBackendAncestors {
			break
		}
		entry := gatewayx.BackendAncestorStatus{ControllerName: controllerName, AncestorRef: gatewayAncestorRef(key)}
		meta.SetStatusCondition(&entry.Conditions, remaining[key])
		merged = append(merged, entry)
	}
	return merged
}

func gatewayAncestorRef(key types.NamespacedName) gatewayv1.ParentReference {
	return gatewayv1.ParentReference{
		Group: ptr.To(gatewayv1.Group(gatewayv1.GroupName)), Kind: ptr.To(gatewayv1.Kind("Gateway")),
		Namespace: ptr.To(gatewayv1.Namespace(key.Namespace)), Name: gatewayv1.ObjectName(key.Name),
	}
}
