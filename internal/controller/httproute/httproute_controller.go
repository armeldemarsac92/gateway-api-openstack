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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

var errHTTPRouteChanged = controller.ErrGraphInputChanged

type cachedRouteDisposition int

const (
	cachedRouteUnknown cachedRouteDisposition = iota
	cachedRouteUnsupportedVersion
	cachedRouteDetach
	cachedRouteReady
)

// cachedRouteDecision records the Kubernetes cache view used for status and
// lifecycle routing. Provider mutations build and seal a separate live
// routeValidationSnapshot while holding the Gateway graph lock.
type cachedRouteDecision struct {
	parent         managedRouteParent
	storedIdentity cloud.Identity
	bindingPresent bool
	statusUpdates  []parentStatusUpdate
	disposition    cachedRouteDisposition
}

// Reconciler owns the route-scoped Octavia resources for the current
// HTTP forwarding profile.
type Reconciler struct {
	routeKind gatewayv1.Kind
	// Client reads and patches cached Kubernetes objects.
	client.Client
	// Writer rebuilds and reconciles the complete Gateway graph.
	Writer graph.Reconciler
	// Coordinator serializes graph mutation by Gateway UID.
	Coordinator *graph.Coordinator
	// APIReader performs safety-sensitive live reads.
	APIReader client.Reader
	// Recorder publishes Kubernetes Events.
	Recorder events.EventRecorder
	// Config contains immutable controller configuration.
	Config controller.Config
}

// Reconcile evaluates one HTTPRoute and converges the route resources in its
// bound Gateway graph.
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes;grpcroutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes/status;grpcroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes/finalizers;grpcroutes/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways;gatewayclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=referencegrants,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=services;nodes;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (result ctrl.Result, retErr error) {
	logger := log.FromContext(ctx).WithValues("route", req.NamespacedName, "kind", r.kind())
	ctx = log.IntoContext(ctx, logger)

	scope, responsible, err := r.newHTTPRouteScope(ctx, req)
	if err != nil || !responsible {
		return ctrl.Result{}, err
	}
	defer func() {
		if err := r.patchHTTPRoute(ctx, scope); err != nil {
			result = ctrl.Result{}
			retErr = errors.Join(retErr, scope.patchCause, err)
		}
	}()

	logger.V(4).Info("Reconciling HTTPRoute", "generation", scope.route.Generation)
	if !scope.route.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, scope)
	}
	return r.reconcileNormal(ctx, scope)
}

func (r *Reconciler) reconcileDelete(ctx context.Context, scope *httpRouteScope) (ctrl.Result, error) {
	log.FromContext(ctx).V(1).Info("Finalizing HTTPRoute")
	result, err := r.finalizeRoute(ctx, scope)
	if err == nil && result.RequeueAfter == 0 && !controllerutil.ContainsFinalizer(scope.route, r.Config.RouteFinalizer()) {
		scope.skipPatch = true
	}
	return result, err
}

func (r *Reconciler) reconcileNormal(ctx context.Context, scope *httpRouteScope) (ctrl.Result, error) {
	if scope.route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
		return r.finalizeRoute(ctx, scope)
	}

	decision, err := r.buildCachedRouteDecision(ctx, scope.route)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch decision.disposition {
	case cachedRouteUnsupportedVersion:
		return r.setRouteUnsupportedVersion(ctx, scope)
	case cachedRouteDetach:
		return r.setRouteStatusesAndDetach(ctx, scope, decision.statusUpdates)
	case cachedRouteReady:
		return r.reconcileSelectedRoute(ctx, scope, decision)
	default:
		return ctrl.Result{}, fmt.Errorf("invalid cached HTTPRoute decision disposition %d", decision.disposition)
	}
}

func (r *Reconciler) buildCachedRouteDecision(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
) (cachedRouteDecision, error) {
	parents, err := r.managedParents(ctx, route)
	if err != nil {
		if _, _, bindingErr := r.storedRouteIdentity(route); bindingErr != nil {
			return cachedRouteDecision{}, bindingErr
		}
		return cachedRouteDecision{}, err
	}
	storedIdentity, bindingPresent, err := r.storedRouteIdentity(route)
	if err != nil {
		return cachedRouteDecision{}, err
	}
	base := cachedRouteDecision{
		storedIdentity: storedIdentity,
		bindingPresent: bindingPresent,
	}
	if !managedParentsSupportInstalledVersion(parents) {
		base.disposition = cachedRouteUnsupportedVersion
		return base, nil
	}
	if len(parents) == 0 {
		base.disposition = cachedRouteDetach
		return base, nil
	}
	if len(parents) != 1 {
		base.disposition = cachedRouteDetach
		base.statusUpdates = unsupportedParentCountUpdates(parents)
		return base, nil
	}
	return r.validateCachedRouteDecision(ctx, route, parents[0], base)
}

func (r *Reconciler) validateCachedRouteDecision(
	ctx context.Context,
	route *gatewayv1.HTTPRoute,
	parent managedRouteParent,
	decision cachedRouteDecision,
) (cachedRouteDecision, error) {
	decision.parent = parent
	if model.HTTPProfileRouteKind(route) == model.GRPCRouteKind {
		if _, err := r.validateRouteStructure(route); err != nil {
			decision.disposition = cachedRouteDetach
			decision.statusUpdates = []parentStatusUpdate{{parent: parent.ref, status: statusForRouteBuildError(err)}}
			return decision, nil
		}
	}
	http, _ := gatewaycontroller.HTTPRouteListeners(r.Config, parent.gateway)
	candidates, parentErr := routeParentCandidates(route, parent, http)
	validationErr := parentErr
	if parentErr == nil {
		namespaces, err := readRouteNamespaces(ctx, r.APIReader, candidates, route.Namespace)
		if err != nil {
			return cachedRouteDecision{}, err
		}
		_, validationErr = selectRouteAttachment(route, parent.gateway, candidates, namespaces)
	}
	if validationErr != nil {
		status := statusForRouteBuildError(validationErr)
		resolveBackend := status.acceptedReason == string(gatewayv1.RouteReasonNoMatchingParent) ||
			(parentErr == nil && status.acceptedReason == string(gatewayv1.RouteReasonNotAllowedByListeners))
		// An established route must detach even if a backend read fails.
		// Clearing its binding enqueues it again for reference evaluation.
		if !decision.bindingPresent && resolveBackend {
			if err := r.resolveRouteReferences(ctx, route, &status); err != nil {
				return cachedRouteDecision{}, err
			}
		}
		decision.disposition = cachedRouteDetach
		decision.statusUpdates = []parentStatusUpdate{{
			parent: parent.ref,
			status: status,
		}}
		return decision, nil
	}
	// Grouped routes resolve each backend profile independently from the live inputs.
	if backends, err := r.validateRouteStructure(route); err == nil && len(backends) == 1 && !r.usesHTTPBackendGroups(route) {
		if err := r.checkRouteBackendProfile(ctx, r.Client, route.Namespace, backends[0], parent.gateway); err != nil {
			var rejection *routeBuildError
			if !errors.As(err, &rejection) {
				return cachedRouteDecision{}, err
			}
			decision.disposition = cachedRouteDetach
			decision.statusUpdates = []parentStatusUpdate{{
				parent: parent.ref,
				status: backendProfileRejectionStatus(rejection),
			}}
			return decision, nil
		}
	}
	decision.disposition = cachedRouteReady
	return decision, nil
}

func managedParentsSupportInstalledVersion(parents []managedRouteParent) bool {
	for _, parent := range parents {
		if !controller.GatewayClassSupportsInstalledVersion(parent.gatewayClass) {
			return false
		}
	}
	return true
}

func unsupportedParentCountUpdates(parents []managedRouteParent) []parentStatusUpdate {
	status := rejectedRouteStatus(
		string(gatewayv1.RouteReasonUnsupportedValue),
		"exactly one managed Gateway parentRef is supported",
	)
	updates := make([]parentStatusUpdate, 0, len(parents))
	for _, parent := range parents {
		updates = append(updates, parentStatusUpdate{parent: parent.ref, status: status})
	}
	return updates
}

func (r *Reconciler) reconcileSelectedRoute(
	ctx context.Context,
	scope *httpRouteScope,
	decision cachedRouteDecision,
) (ctrl.Result, error) {
	httpRoute := scope.route
	parent := decision.parent
	if decision.bindingPresent && !controller.SameGatewayIdentity(
		decision.storedIdentity,
		controller.RouteIdentity(r.Config, parent.gateway, httpRoute),
	) {
		updates := []parentStatusUpdate{{
			parent: parent.ref,
			status: pendingRouteStatus("OpenStack resources for the previous Gateway are being removed"),
		}}
		checkpointNeeded, err := r.routeCleanupStatusCheckpointNeeded(ctx, scope, updates)
		if err != nil {
			return ctrl.Result{}, err
		}
		if checkpointNeeded {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	detachResult, err := r.detachPreviousGateway(ctx, httpRoute, parent.gateway)
	if err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return r.setRouteUnsupportedVersion(ctx, scope)
		}
		return r.handleRouteProviderFailure(scope, parent.ref, false, model.Diagnostic{}, err, "DeleteHTTPRoute")
	}
	if detachResult.RequeueAfter > 0 {
		status := pendingRouteStatus("OpenStack resources for the previous Gateway are being removed")
		scope.setStatuses([]parentStatusUpdate{{parent: parent.ref, status: status}})
		return detachResult, nil
	}
	return r.reconcileSelectedRouteGraph(ctx, scope, parent)
}

func (r *Reconciler) reconcileSelectedRouteGraph(
	ctx context.Context,
	scope *httpRouteScope,
	parent managedRouteParent,
) (ctrl.Result, error) {
	httpRoute := scope.route
	graphResult, err := r.ensureRouteGraph(ctx, httpRoute, parent.gateway)
	if err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return r.setRouteUnsupportedVersion(ctx, scope)
		}
		var bindingError *routeBindingCheckpointError
		if errors.As(err, &bindingError) {
			return ctrl.Result{}, fmt.Errorf("bind HTTPRoute to Gateway: %w", bindingError)
		}
		var fragmentError *cloud.GraphError
		if errors.As(err, &fragmentError) && fragmentError.Identity.RouteUID != "" && fragmentError.Identity.RouteUID != string(httpRoute.UID) {
			return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "Another HTTPRoute in the Gateway graph could not be reconciled"}, err)
		}
		var semanticError *routeBuildError
		if errors.As(err, &semanticError) {
			status := statusForRouteBuildError(err)
			return r.setRouteStatusesAndDetach(ctx, scope, []parentStatusUpdate{{parent: parent.ref, status: status}})
		}
		if errors.Is(err, errHTTPRouteChanged) {
			return ctrl.Result{}, err
		}
		return r.handleRouteProviderFailure(scope, parent.ref, true, graphResult.backendDiagnostic, err, "EnsureHTTPRoute")
	}
	switch graphResult.bindingState {
	case routeBindingPersisted:
		return ctrl.Result{Requeue: true}, nil
	case routeBindingGatewayPending:
		status := pendingRouteStatus("Controller-owned Octavia load balancer or listener is not ready").withBackendDiagnostic(graphResult.backendDiagnostic)
		if r.usesHTTPBackendGroups(httpRoute) && !graphResult.backendObserved {
			if err := r.resolveRouteReferences(ctx, httpRoute, &status); err != nil {
				return ctrl.Result{}, err
			}
		}
		scope.setStatuses([]parentStatusUpdate{{parent: parent.ref, status: status}})
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	case routeBindingReady:
		// The graph writer returned a validated cloud outcome.
	default:
		return ctrl.Result{}, fmt.Errorf("invalid HTTPRoute binding state %d", graphResult.bindingState)
	}
	if graphResult.UnsupportedMatch != "" {
		status := programmedRouteStatus()
		status.accepted, status.acceptedReason, status.acceptedMessage = metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue), graphResult.UnsupportedMatch
		status.programmed, status.programmedReason, status.programmedMessage = metav1.ConditionFalse, "Invalid", graphResult.UnsupportedMatch
		scope.setStatuses([]parentStatusUpdate{{parent: parent.ref, status: status.withBackendDiagnostic(graphResult.backendDiagnostic)}})
		if graphResult.Outcome.State == cloud.OutcomeProgressing {
			return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(graphResult.Outcome, httpRoute.UID)}, nil
		}
		return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, httpRoute.UID)}, nil
	}
	if graphResult.Outcome.State == cloud.OutcomeProgressing {
		if graphResult.backendDiagnostic.Reason == "" && graphResult.UnsupportedBackendProtocol != "" {
			graphResult.backendDiagnostic = model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: graphResult.UnsupportedBackendProtocol,
			}
		}
		if !graphResult.ObservationDeferred || r.currentRouteProgrammedCondition(httpRoute, parent.ref) == nil ||
			!r.currentBackendResolution(httpRoute, parent.ref, graphResult.backendDiagnostic) {
			message := controller.ProviderProgressMessage(graphResult.Outcome, "Octavia resources for the HTTPRoute are still progressing")
			scope.setStatuses([]parentStatusUpdate{{parent: parent.ref, status: pendingRouteStatus(message).withBackendDiagnostic(graphResult.backendDiagnostic)}})
		}
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(graphResult.Outcome, httpRoute.UID)}, nil
	}
	scope.setStatuses([]parentStatusUpdate{{parent: parent.ref, status: programmedRouteStatus().withBackendDiagnostic(graphResult.backendDiagnostic)}})
	log.FromContext(ctx).V(1).Info("Programmed HTTPRoute", "gateway", client.ObjectKeyFromObject(parent.gateway))
	return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, httpRoute.UID)}, nil
}
