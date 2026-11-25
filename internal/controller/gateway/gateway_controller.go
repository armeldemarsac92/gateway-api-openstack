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

package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// Reconciler owns the Gateway-scoped Octavia resources.
type Reconciler struct {
	// Certificates resolves operator-approved certificate references from live metadata.
	Certificates              CertificateResolver
	BackendClientCertificates BackendClientCertificateResolver
	// AdmitsHTTPRouteListener checks a parent omitted from a full Route status.
	AdmitsHTTPRouteListener func(*gatewayv1.HTTPRoute, *gatewayv1.Gateway, gatewayv1.ParentReference, *gatewayv1.Listener, []corev1.Namespace) bool
	// HTTPSListenerIsolationAvailable checks authenticated regional support.
	HTTPSListenerIsolationAvailable func(context.Context) (bool, error)
	// Client reads and patches cached Kubernetes objects.
	client.Client
	// Writer rebuilds and reconciles the complete Gateway graph.
	Writer graph.Reconciler
	// GraphRefresh enqueues delayed observations derived from the complete graph.
	GraphRefresh *graph.GatewayRefreshQueue
	// Coordinator serializes graph mutation by Gateway UID.
	Coordinator *graph.Coordinator
	// APIReader performs safety-sensitive live reads.
	APIReader client.Reader
	// Recorder publishes Kubernetes Events.
	Recorder events.EventRecorder
	// TopologyResolver resolves class infrastructure before the initial binding.
	TopologyResolver cloud.TopologyResolver
	// Config contains immutable controller configuration.
	Config controller.Config
}

var (
	errGatewayChanged = controller.ErrGraphInputChanged
)

// Reconcile evaluates one Gateway and converges the graph owned by it.
// +kubebuilder:rbac:groups=gateway-api-openstack.github.io,resources=openstackgatewayclassconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gatewayclasses;httproutes;tlsroutes,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tlsroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
func (r *Reconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (result ctrl.Result, retErr error) {
	logger := log.FromContext(ctx).WithValues("gateway", req.NamespacedName)
	ctx = log.IntoContext(ctx, logger)
	logger.V(4).Info("Reconciling Gateway")

	scope, responsible, err := r.newGatewayScope(ctx, req)
	if r.Config.TLSRoutesEnabled() || scope != nil && cloud.IsTCPTLSListenerProtocol(string(r.boundListenerProtocol(scope.gateway))) {
		// Route rejection and status cleanup do not gate Gateway graph mutations.
		defer func() {
			var versionBlocked *gatewayv1.Gateway
			if scope != nil && scope.rawTLSVersionUnsupported {
				versionBlocked = scope.gateway
			}
			if err := r.reconcileSNIParentStatuses(ctx, req.NamespacedName, versionBlocked); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}()
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !responsible {
		return ctrl.Result{}, r.reportTelemetryWithoutBinding(ctx, req.NamespacedName)
	}
	defer func() {
		if !scope.skipPatch && scope.managed && scope.gateway.DeletionTimestamp.IsZero() {
			r.setBackendClientCertificateStatus(ctx, scope.gateway)
			setFrontendFallbackStatus(scope.gateway)
		}
		if err := r.patchGateway(ctx, scope); err != nil {
			result = ctrl.Result{}
			retErr = errors.Join(retErr, scope.patchCause, err)
		}
		if retErr == nil && (scope.patchCause == nil || scope.hasDelegatedHTTPS && result.RequeueAfter > 0) && !result.Requeue && scope.gateway.DeletionTimestamp.IsZero() &&
			(scope.hasDelegatedHTTPS || usesHTTPSProfile(r.Config, scope.gateway) || r.Config.TLSRoutesEnabled() && hasTLSListener(scope.gateway) ||
				r.Config.BackendClientCertificate && BackendClientCertificateRef(scope.gateway) != nil) &&
			(result.RequeueAfter == 0 || result.RequeueAfter > httpsPollInterval) {
			result.RequeueAfter = httpsPollInterval
		}
	}()

	deleting := !scope.gateway.DeletionTimestamp.IsZero()
	withdrawTelemetry := deleting || r.decideGatewayPhase(scope).phase != gatewayPhaseEnsure
	telemetryInterval, telemetryErr := r.reconcileGatewayTelemetry(ctx, scope, withdrawTelemetry)
	if withdrawTelemetry && (telemetryErr != nil || telemetryInterval > 0) {
		return ctrl.Result{RequeueAfter: telemetryInterval}, telemetryErr
	}
	defer func() {
		boundTelemetryResult(&result, telemetryInterval)
		retErr = errors.Join(retErr, telemetryErr)
	}()

	if deleting {
		// Owner references also allow Kubernetes to collect discovery objects
		// when the Gateway is removed. A discovery error must not hold up Octavia cleanup.
		defer func() {
			retErr = errors.Join(retErr, r.withdrawGatewayPublications(ctx, scope.gateway, true))
		}()
		return r.reconcileDelete(ctx, scope)
	}
	return r.reconcileNormal(ctx, scope)
}

func (r *Reconciler) reconcileDelete(ctx context.Context, scope *gatewayScope) (ctrl.Result, error) {
	if result, withdrawn, err := r.withdrawBoundTLS(ctx, scope.gateway); err != nil || !withdrawn {
		return result, err
	}
	result, err := r.cleanupGateway(ctx, scope)
	if err != nil || result.RequeueAfter > 0 {
		return result, err
	}
	log.FromContext(ctx).V(1).Info("Cleaned up OpenStack resources for Gateway")
	return ctrl.Result{}, nil
}

func (r *Reconciler) reconcileNormal(ctx context.Context, scope *gatewayScope) (result ctrl.Result, retErr error) {
	decision := r.decideGatewayPhase(scope)
	if decision.phase == gatewayPhaseCleanupUnmanaged {
		// Clean up the discovery name before releasing our last responsibility
		// for a Gateway that has been handed off to another controller.
		if err := r.withdrawGatewayPublications(ctx, scope.gateway, true); err != nil {
			return ctrl.Result{}, err
		}
	}
	switch decision.phase {
	case gatewayPhaseUnsupportedVersion, gatewayPhaseInvalidBinding, gatewayPhaseInvalidSpec:
		defer func() { retErr = errors.Join(retErr, r.withdrawGatewayPublications(ctx, scope.gateway, true)) }()
	case gatewayPhaseReplaceGraph:
		defer func() { retErr = errors.Join(retErr, r.withdrawGatewayPublications(ctx, scope.gateway, false)) }()
	}
	if scope.managed && !r.Config.GatewayAddressRoutabilityEnabled() {
		meta.RemoveStatusCondition(&scope.gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	}
	switch decision.phase {
	case gatewayPhaseIgnore:
		return ctrl.Result{}, nil
	case gatewayPhaseCleanupUnmanaged:
		return r.reconcileUnmanagedGateway(ctx, scope)
	case gatewayPhaseUnsupportedVersion:
		return r.setGatewayUnsupportedVersion(ctx, scope)
	case gatewayPhaseInvalidBinding:
		r.setGatewayFailureReason(scope.gateway, gatewayv1.GatewayReasonInvalidParameters, decision.validation.Error())
		return ctrl.Result{}, nil
	case gatewayPhaseInvalidSpec:
		return r.reconcileRejectedGateway(ctx, scope, decision)
	case gatewayPhaseReplaceGraph:
		return r.reconcileGatewayReplacement(ctx, scope, decision)
	case gatewayPhaseBind:
		return r.reconcileGatewayBinding(ctx, scope, decision)
	case gatewayPhaseEnsure:
		if BackendClientCertificateRef(scope.gateway) != nil && !r.Config.BackendTLSPolicy {
			result, err := r.withdrawGatewayCertificates(ctx, scope.gateway)
			if usesHTTPSProfile(r.Config, scope.gateway) {
				issues, readErr := r.listenerTLSIssues(ctx, scope.gateway, ListenerSetProjection{}, nil)
				err = errors.Join(err, readErr)
				if readErr == nil {
					r.applyHTTPSListenerStatus(scope.gateway, scope.original.Status, issues, nil, false)
				}
			}
			return result, errors.Join(err, r.withdrawGatewayPublications(ctx, scope.gateway, false))
		}
		return r.reconcileGatewayGraph(ctx, scope, decision)
	default:
		return ctrl.Result{}, fmt.Errorf("unsupported Gateway reconciliation phase %d", decision.phase)
	}
}

func (r *Reconciler) reconcileUnmanagedGateway(ctx context.Context, scope *gatewayScope) (ctrl.Result, error) {
	if result, withdrawn, err := r.withdrawBoundTLS(ctx, scope.gateway); err != nil || !withdrawn {
		return result, err
	}
	result, err := r.cleanupGateway(ctx, scope)
	if err == nil && result.RequeueAfter == 0 {
		log.FromContext(ctx).V(1).Info("Cleaned up OpenStack resources for unmanaged Gateway")
	}
	return result, err
}

func (r *Reconciler) reconcileRejectedGateway(
	ctx context.Context,
	scope *gatewayScope,
	decision gatewayPhaseDecision,
) (ctrl.Result, error) {
	r.setGatewayFailure(scope.gateway, decision.validation)
	if BackendClientCertificateRef(scope.gateway) != nil && !r.Config.BackendTLSPolicy {
		return r.withdrawGatewayCertificates(ctx, scope.gateway)
	}
	if result, withdrawn, err := r.withdrawBoundTLS(ctx, scope.gateway); err != nil || !withdrawn {
		return result, err
	}
	var validation *gatewayValidationError
	if errors.As(decision.validation, &validation) && validation.programmedReason != "" {
		// Address validation here precedes the initial binding.
		return ctrl.Result{}, nil
	}
	return r.cleanupGatewayAfterStatusCheckpoint(ctx, scope)
}

func (r *Reconciler) reconcileGatewayReplacement(
	ctx context.Context,
	scope *gatewayScope,
	decision gatewayPhaseDecision,
) (ctrl.Result, error) {
	if result, withdrawn, err := r.withdrawBoundTLS(ctx, scope.gateway); err != nil || !withdrawn {
		return result, err
	}
	// A rejected replacement already checkpointed this generation. Keep
	// observing the old graph without overwriting the address diagnosis.
	programmed := meta.FindStatusCondition(scope.gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	assignment := meta.FindStatusCondition(scope.gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	assignmentRejected := r.Config.GatewayAddressRoutabilityEnabled() && assignment != nil &&
		assignment.ObservedGeneration == scope.gateway.Generation && assignment.Status == metav1.ConditionFalse &&
		assignment.Reason == string(gatewayv1.GatewayReasonAddressesNotAssigned)
	if programmed != nil && programmed.ObservedGeneration == scope.gateway.Generation &&
		programmed.Status == metav1.ConditionFalse && (assignmentRejected ||
		programmed.Reason == string(gatewayv1.GatewayReasonAddressNotUsable) ||
		programmed.Reason == string(gatewayv1.GatewayReasonAddressNotAssigned)) {
		return r.cleanupGateway(ctx, scope)
	}
	message := "Listener port or protocol changed. Checking whether the OpenStack resource graph can be replaced"
	if err := r.setGatewayProgressing(ctx, scope.gateway, decision.listener, message); err != nil {
		return ctrl.Result{}, err
	}
	return r.cleanupGatewayAfterStatusCheckpoint(ctx, scope)
}

func (r *Reconciler) cleanupGatewayAfterStatusCheckpoint(
	ctx context.Context,
	scope *gatewayScope,
) (ctrl.Result, error) {
	if scope.statusChanged() {
		return ctrl.Result{Requeue: true}, nil
	}
	return r.cleanupGateway(ctx, scope)
}

func (r *Reconciler) reconcileGatewayBinding(
	ctx context.Context,
	scope *gatewayScope,
	decision gatewayPhaseDecision,
) (ctrl.Result, error) {
	if err := r.bindGateway(ctx, scope.gateway, decision.listenerPort); err != nil {
		var invalid *classconfig.InvalidParametersError
		if errors.As(err, &invalid) {
			r.setGatewayFailureReason(scope.gateway, gatewayv1.GatewayReasonInvalidParameters, invalid.Error())
			return ctrl.Result{}, nil
		}

		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return r.setGatewayUnsupportedVersion(ctx, scope)
		}
		if _, classified := controller.ProviderFailurePolicyFor(err); classified {
			return r.handleGatewayProviderFailure(ctx, scope, decision.listener, err, cloud.GatewayResult{})
		}
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).V(1).Info("Reconciled Gateway resource binding checkpoint")
	// Publish status from the next read of the checkpointed Gateway.
	scope.skipPatch = true
	return ctrl.Result{Requeue: true}, nil
}

func (r *Reconciler) reconcileGatewayGraph(
	ctx context.Context,
	scope *gatewayScope,
	decision gatewayPhaseDecision,
) (result ctrl.Result, retErr error) {
	sets, err := r.ObserveListenerSets(ctx, r.APIReader, scope.gateway)
	if err != nil {
		return ctrl.Result{}, err
	}
	scope.hasDelegatedHTTPS = len(sets.https) != 0
	if len(sets.isolationPorts) != 0 {
		if err := r.checkpointListenerSetHTTPS(ctx, scope.gateway, sets); err != nil {
			return ctrl.Result{}, err
		}
		scope.skipPatch = true
		return ctrl.Result{Requeue: true}, nil
	}
	graphResult, err := r.ensureGateway(ctx, scope.gateway)
	trustUnavailable := errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable)
	var changedTLS *graph.TLSAuthorizationChanged
	var tlsIssues map[gatewayv1.SectionName]httpsListenerStatusIssue
	defer func() {
		r.reportFrontendCAWithdrawal(scope.gateway, tlsIssues, graphResult.Gateway.ScopedTLSClosedPorts)
	}()
	if r.Config.GatewayListenerFiltersEnabled() && hasHTTPFamilyListenerFilters(scope.gateway) &&
		!errors.Is(err, errGatewayChanged) && !errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
		topology, topologyErr := controller.StoredGatewayTopology(r.Config, scope.gateway)
		listeners, listenerErr := SupportedListenersForConfig(r.Config, scope.gateway)
		if topologyErr == nil && listenerErr == nil {
			filters, filterErr := r.observeListenerFilters(ctx, scope.gateway, listeners, sets, topology)
			if filterErr != nil && !trustUnavailable {
				return ctrl.Result{}, filterErr
			}
			if filterErr != nil {
				err = errors.Join(err, filterErr)
			}
			if trustUnavailable {
				filters = filters.confirmedRejections(listeners)
			}
			defer func() {
				r.applyListenerFilterStatus(scope.gateway, scope.original.Status, filters, graphResult.Gateway, changedTLS)
			}()
		}
	}
	if trustUnavailable {
		if errors.As(err, &changedTLS) {
			current, readErr := r.ObserveListenerSets(ctx, r.APIReader, scope.gateway)
			if readErr != nil {
				return ctrl.Result{}, errors.Join(err, readErr)
			}
			if len(current.sets) != 0 {
				if reportErr := r.reportListenerSets(ctx, scope.gateway, current, graphResult, changedTLS.Ports...); reportErr != nil {
					return ctrl.Result{}, errors.Join(err, reportErr)
				}
			}
			tlsIssues, readErr = r.listenerTLSIssues(ctx, scope.gateway, current, nil)
			if readErr != nil {
				return ctrl.Result{}, errors.Join(err, readErr)
			}
			r.applyHTTPSListenerStatus(scope.gateway, scope.original.Status, tlsIssues, changedTLS, true)
		}
		return ctrl.Result{}, err
	}
	if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
		result, versionErr := r.setGatewayUnsupportedVersion(ctx, scope)
		return result, errors.Join(versionErr, r.withdrawGatewayPublications(ctx, scope.gateway, true))
	}
	var componentError *cloud.GraphError
	routeFailure := errors.As(err, &componentError) && componentError.Identity.RouteUID != ""
	policy, classified := controller.ProviderFailurePolicyFor(err)
	var cluster clusterAddressAssignments
	if r.Config.GatewayInfrastructureMetadata && !errors.Is(err, errGatewayChanged) {
		observation := graphResult.Gateway
		// Explicit scoped readback still describes the serving listeners if
		// another listener failed. A nil observation leaves discovery intact.
		observed := err == nil || routeFailure || observation.AddressAbsent || len(observation.ScopedListenerDeferredPorts) != 0 ||
			len(observation.ScopedListenerServingPorts) != 0 ||
			scopedGatewayFailurePorts(r.Config, scope.gateway, policy.Category, observation) != nil
		var clusterErr error
		cluster, clusterErr = r.reconcileClusterAddresses(ctx, scope.gateway, decision.listener, observation, observed)
		if errors.Is(clusterErr, errGatewayChanged) {
			return ctrl.Result{}, clusterErr
		}
		defer func() { retErr = errors.Join(retErr, clusterErr) }()
		if observed {
			defer func() {
				if clusterAddressRejection(scope.gateway, cluster) {
					retErr = errors.Join(retErr, r.withdrawGatewayDiscovery(ctx, scope.gateway, false))
					return
				}
				retErr = errors.Join(retErr, r.reconcileGatewayDiscovery(ctx, scope.gateway, decision.listener, observation))
			}()
		} else {
			defer func() {
				retErr = errors.Join(retErr, r.checkPublishedDiscoveryService(ctx, scope.gateway))
			}()
		}
	}
	if r.Config.GatewayAddressRoutabilityEnabled() && !errors.Is(err, errGatewayChanged) &&
		(graphResult.Gateway.AddressIssue == "" && (err == nil || routeFailure || graphResult.Gateway.AddressAbsent) ||
			err != nil && (cluster.requested || hasClusterStatusAddress(scope.gateway))) {
		// Address readback is independent of listener and route readiness.
		r.observeAddressAssignment(scope.gateway, graphResult.Gateway, cluster)
	}
	if !model.GatewayDisallowsListenerSets(scope.gateway) && !errors.Is(err, errGatewayChanged) {
		statusResult := graphResult
		var authorizationChange *graph.TLSAuthorizationChanged
		if errors.As(err, &authorizationChange) {
			statusResult.Gateway.ScopedListenerReadyPorts = slices.DeleteFunc(slices.Clone(graphResult.Gateway.ScopedListenerReadyPorts), func(port int) bool {
				return slices.Contains(authorizationChange.Ports, port)
			})
		}
		if err := r.reportListenerSets(ctx, scope.gateway, sets, statusResult); err != nil {
			return ctrl.Result{}, err
		}
	}
	var keepGatewayProgrammed bool
	if usesHTTPSProfile(r.Config, scope.gateway) {
		var readErr error
		tlsIssues, readErr = r.listenerTLSIssues(ctx, scope.gateway, sets, graphResult.Gateway.FrontendFallbackUnavailablePorts)
		if readErr != nil {
			return ctrl.Result{}, errors.Join(err, readErr)
		}
		defer func() {
			r.applyHTTPSListenerStatus(scope.gateway, scope.original.Status, tlsIssues, changedTLS, keepGatewayProgrammed)
		}()
	}
	if terminatesTLS(decision.listener) {
		defer func() {
			listeners, err := SupportedListenersForConfig(r.Config, scope.gateway)
			if err == nil {
				_, issues := r.rawTLSCertificate(ctx, scope.gateway, listeners)
				for _, listener := range listeners {
					if terminatesTLS(listener) {
						r.applyRawTLSListenerStatus(scope.gateway, listener, issues[listener.Name], changedTLS != nil)
					}
				}
			}
		}()
	}
	if ports := graphResult.Gateway.ScopedListenerDeferredPorts; len(ports) != 0 {
		defer func() { preserveProgrammedListeners(scope.gateway, scope.original.Status, ports) }()
	}
	if err != nil {
		if errors.As(err, &changedTLS) && err == changedTLS {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		if errors.Is(err, errGatewayChanged) && !classified {
			return ctrl.Result{}, err
		}
		if clusterAddressRejection(scope.gateway, cluster) {
			defer controller.SetCondition(&scope.gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed),
				metav1.ConditionFalse, string(gatewayv1.GatewayReasonAddressNotAssigned), "No Gateway address request could be assigned", scope.gateway.Generation))
		}
		if routeFailure {
			if graphResult.Gateway.HTTPPolicyTransition {
				message := controller.ProviderProgressMessage(graphResult.Gateway.Outcome, "Waiting for HTTP policy changes")
				if statusErr := r.setGatewayListenerStatuses(ctx, scope.gateway, decision.listener,
					metav1.ConditionFalse, gatewayv1.GatewayReasonNoResources, gatewayv1.ListenerReasonPending,
					message, message, true, graphResult.Gateway.ScopedListenerReadyPorts); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
			} else if isL4Listener(decision.listener.Protocol) {
				if statusErr := r.refreshTransportListenerStatuses(ctx, scope.gateway); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
			}
			if classified {
				if !policy.ReturnError {
					scope.patchCause = controller.SafeProviderReconcileError(policy, err)
				}
				return controller.ProviderFailureResult(policy, err, string(scope.gateway.UID))
			}
			return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "The route graph could not be reconciled"}, err)
		}
		keepGatewayProgrammed = classified
		return r.handleGatewayProviderFailure(ctx, scope, decision.listener, err, graphResult.Gateway)
	}
	gatewayResult := graphResult.Gateway
	if err := gatewayResult.Outcome.Validate(); err != nil {
		return ctrl.Result{}, fmt.Errorf("validate Gateway component outcome: %w", err)
	}
	// Listener section changes take effect through the HTTP policies. This
	// includes removing the last hostname restriction from a previously
	// shared listener. Wait for the graph before certifying a new generation.
	progress := gatewayResult.Outcome
	if listenerFamily(decision.listener) == gatewayv1.HTTPProtocolType && graphResult.Outcome.State == cloud.OutcomeProgressing && progress.State == cloud.OutcomeReady {
		programmed := meta.FindStatusCondition(scope.gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		if programmed == nil || programmed.Status != metav1.ConditionTrue || programmed.ObservedGeneration != scope.gateway.Generation {
			progress = graphResult.Outcome
		}
	}
	if controller.GatewayListenerScope(r.Config, scope.gateway) != "" {
		// A provider result must carry current proof for every desired port
		// before the aggregate Gateway can be reported as programmed.
		for _, listener := range scope.gateway.Spec.Listeners {
			_, hasIssue := tlsIssues[listener.Name]
			if !hasIssue && listenerFamily(&listener) == listenerFamily(decision.listener) && progress.State == cloud.OutcomeReady && !slices.Contains(gatewayResult.ScopedListenerReadyPorts, int(listener.Port)) {
				progress = cloud.ProgressingOutcome("Waiting for listener readiness", time.Second)
				break
			}
		}
	}
	if gatewayResult.AddressIssue != "" {
		if err := r.setGatewayAddressUnusable(ctx, scope.gateway, decision.listener, gatewayResult, progress, cluster); err != nil {
			return ctrl.Result{}, err
		}
		if !gatewayResult.AddressBlocked && gatewayResult.State.LoadBalancerID != "" && graphResult.Outcome.State == cloud.OutcomeProgressing {
			return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(graphResult.Outcome, scope.gateway.UID)}, nil
		}
		return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, scope.gateway.UID)}, nil
	}
	address := r.observedGatewayAddress(scope.gateway, gatewayResult.State)
	if progress.State == cloud.OutcomeReady && address == "" && len(cluster.addresses) == 0 {
		progress = cloud.ProgressingOutcome("Waiting to observe the Gateway address", time.Second)
	}
	if listenerFamily(decision.listener) == gatewayv1.TLSProtocolType && graphResult.Outcome.State == cloud.OutcomeProgressing && progress.State == cloud.OutcomeReady {
		progress = graphResult.Outcome
	}
	if clusterAddressRejection(scope.gateway, cluster) && (progress.State == cloud.OutcomeReady || len(cluster.issues) != 0) {
		const message = "No Gateway address request could be assigned"
		if err := r.setGatewayProvisioningStatus(ctx, scope.gateway, decision.listener, metav1.ConditionFalse,
			gatewayv1.GatewayReasonAddressNotAssigned, gatewayv1.ListenerReasonPending, message, true); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, scope.gateway.UID)}, nil
	}
	if progress.State == cloud.OutcomeProgressing {
		message := controller.ProviderProgressMessage(progress, "Octavia resources for the Gateway are still progressing")
		if err := r.setGatewayListenerStatuses(ctx, scope.gateway, decision.listener,
			metav1.ConditionFalse, gatewayv1.GatewayReasonNoResources, gatewayv1.ListenerReasonPending, message, message, true, gatewayResult.ScopedListenerReadyPorts); err != nil {
			return ctrl.Result{}, err
		}
		if !r.Config.GatewayAddressRoutabilityEnabled() {
			if gatewayResult.AddressAbsent {
				scope.gateway.Status.Addresses = nil
			} else if address != "" && len(gatewayResult.ScopedListenerReadyPorts) != 0 {
				setGatewayAddress(scope.gateway, address)
			}
		}
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(progress, scope.gateway.UID)}, nil
	}
	gatewayState := gatewayResult.State
	log.FromContext(ctx).V(1).Info("Ensured OpenStack resources for Gateway", "loadBalancerID", gatewayState.LoadBalancerID, "listenerID", gatewayState.ListenerID)
	if err := r.setGatewayProgrammed(ctx, scope.gateway, decision.listener, address); err != nil {
		return ctrl.Result{}, err
	}
	if graphResult.Outcome.State == cloud.OutcomeProgressing {
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(graphResult.Outcome, scope.gateway.UID)}, nil
	}
	return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, scope.gateway.UID)}, nil
}
