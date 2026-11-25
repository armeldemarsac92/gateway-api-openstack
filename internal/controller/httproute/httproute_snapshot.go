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
	"slices"
	"strconv"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeSnapshotPurpose int

const (
	routeSnapshotForEnsure routeSnapshotPurpose = iota
	routeSnapshotForBinding
)

type routeValidationDisposition int

const (
	routeValidationUnknown routeValidationDisposition = iota
	routeValidationGatewayPending
	routeValidationBindingRequired
	routeValidationReady
)

type routeBindingRequirement int

const (
	routeBindingOptional routeBindingRequirement = iota
	routeBindingRequired
)

type routeBindingSnapshot struct {
	identity       cloud.Identity
	present        bool
	hasFinalizer   bool
	matchesDesired bool
	lifecycle      string
	explicitScope  bool
	otherBindings  bool
	legacySibling  cloud.Identity
}

// routeValidationSnapshot contains one live view of every Kubernetes input
// that authorizes a route graph mutation. It is local to one mutation attempt
// and must not be retained after releasing the Gateway graph lock.
type routeValidationSnapshot struct {
	route                   *gatewayv1.HTTPRoute
	gateway                 *gatewayv1.Gateway
	gatewayClass            *gatewayv1.GatewayClass
	parents                 []managedRouteParent
	selection               routeSelection
	binding                 routeBindingSnapshot
	desired                 cloud.RouteSpec
	backends                *routeModelInputs
	rejectedSections        []model.ListenerSection
	backendDiagnostic       model.Diagnostic
	corsFallback            bool
	droppedInlineRules      string
	backendTLSAncestorSlot  backendtls.AncestorSlot
	retryBudgetAncestorFull bool
	gatewayReadyForBinding  bool
	disposition             routeValidationDisposition
}

type routeCleanupDisposition int

const (
	routeCleanupUnknown routeCleanupDisposition = iota
	routeCleanupRequired
	routeCleanupStillDesired
)

type routeCleanupSnapshot struct {
	route             *gatewayv1.HTTPRoute
	identity          cloud.Identity
	disposition       routeCleanupDisposition
	referenceRevoked  bool
	attachmentRevoked bool
	bundleUnsupported bool
	parents           []routeParentObservation
	profileRejected   bool
}

func (r *Reconciler) buildRouteValidationSnapshot(
	ctx context.Context,
	expectedRoute *gatewayv1.HTTPRoute,
	expectedGateway *gatewayv1.Gateway,
	purpose routeSnapshotPurpose,
) (routeValidationSnapshot, error) {
	return r.buildRouteValidationSnapshotFromInputs(ctx, expectedRoute, expectedGateway, purpose, nil)
}

func (r *Reconciler) buildRouteValidationSnapshotFromInputs(
	ctx context.Context,
	expectedRoute *gatewayv1.HTTPRoute,
	expectedGateway *gatewayv1.Gateway,
	purpose routeSnapshotPurpose,
	inputs *routeModelInputs,
) (routeValidationSnapshot, error) {
	snapshot, err := r.observeRouteValidationInputs(ctx, expectedRoute, expectedGateway)
	if err != nil {
		return routeValidationSnapshot{}, err
	}

	if err := r.observeRouteBinding(ctx, &snapshot); err != nil {
		return routeValidationSnapshot{}, err
	}
	if _, validationErr := gatewaycontroller.ValidatedHTTPRouteListeners(r.Config, snapshot.gateway); validationErr != nil {
		if purpose == routeSnapshotForBinding {
			return snapshot, errHTTPRouteChanged
		}
		snapshot.disposition = routeValidationGatewayPending
		return snapshot, nil
	}
	if _, err := r.validateRouteStructure(snapshot.route); err != nil {
		return routeValidationSnapshot{}, routeSnapshotRejection(purpose, err)
	}
	if inputs == nil {
		routes, err := r.liveGraphRoutes(ctx, controller.GatewayIdentity(r.Config, snapshot.gateway))
		if err != nil {
			return routeValidationSnapshot{}, err
		}
		inputs, err = r.collectGraphRouteModelInputs(ctx, snapshot.gateway, routes, snapshot.route)
		if err != nil {
			return routeValidationSnapshot{}, err
		}
	}
	key := routeKey(snapshot.route)
	selection, _, err := r.selectRouteParents(ctx, snapshot.route, snapshot.parents, inputs.admission.listenerSets)
	if err != nil {
		return routeValidationSnapshot{}, err
	}
	snapshot.selection = selection
	snapshot.rejectedSections = inputs.admission.rejectedSections[key]
	if inputs.admission.profileRejectedParent(key) {
		return snapshot, routeSnapshotRejection(purpose, routeProfileConflict())
	}
	selection = selection.withoutProfileConflicts(snapshot.rejectedSections)
	listeners := selection.listeners()
	if len(listeners) == 0 {
		return snapshot, routeSnapshotRejection(purpose, selection[0].rejection)
	}
	if controller.GatewayListenerScope(r.Config, snapshot.gateway) != gatewayv1.HTTPProtocolType {
		if gatewaycontroller.RequiresHTTPListenerScope(r.Config, snapshot.gateway) {
			// An established route can repair its binding before enrollment.
			if !r.establishedRouteBinding(snapshot) || snapshot.binding.explicitScope {
				snapshot.disposition = routeValidationGatewayPending
				return snapshot, nil
			}
		} else if snapshot.gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(int(listeners[0].Port)) {
			return routeValidationSnapshot{}, errHTTPRouteChanged
		}
		snapshot.gatewayReadyForBinding = gatewayProgrammedForGeneration(snapshot.gateway)
	} else {
		multiplePorts := slices.ContainsFunc(listeners[1:], func(listener *gatewayv1.Listener) bool {
			return listener.Port != listeners[0].Port
		})
		snapshot.gatewayReadyForBinding = routeSelectionReadyForBinding(snapshot.gateway, selection, multiplePorts)
	}
	if purpose == routeSnapshotForBinding {
		if err := r.validateRouteBindingTransition(snapshot); err != nil {
			return routeValidationSnapshot{}, err
		}
	}
	desired, diagnostics, err := r.buildRouteModelFromInputs(ctx, r.APIReader, snapshot.route, snapshot.gateway, selection, inputs)
	if err != nil {
		return snapshot, routeSnapshotReadError(purpose, err)
	}
	snapshot.desired = desired
	snapshot.backends = inputs
	snapshot.backendDiagnostic = diagnostics.backend
	snapshot.corsFallback = diagnostics.corsFallback
	snapshot.droppedInlineRules = diagnostics.droppedInlineRules
	snapshot.retryBudgetAncestorFull = diagnostics.retryBudgetAncestorFull
	snapshot.backendTLSAncestorSlot = diagnostics.backendTLSAncestorSlot

	if purpose == routeSnapshotForEnsure {
		if err := r.setRouteBindingDisposition(&snapshot); err != nil {
			return routeValidationSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (r *Reconciler) observeRouteValidationInputs(
	ctx context.Context,
	expectedRoute *gatewayv1.HTTPRoute,
	expectedGateway *gatewayv1.Gateway,
) (routeValidationSnapshot, error) {
	if r.APIReader == nil {
		return routeValidationSnapshot{}, controller.ErrAPIReaderRequired
	}

	var route gatewayv1.HTTPRoute
	if err := r.readRoute(ctx, r.APIReader, client.ObjectKeyFromObject(expectedRoute), &route); err != nil {
		return routeValidationSnapshot{}, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if !sameHTTPRouteRevision(
		&route,
		expectedRoute.UID,
		expectedRoute.Generation,
		!expectedRoute.DeletionTimestamp.IsZero(),
	) {
		return routeValidationSnapshot{}, errHTTPRouteChanged
	}

	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expectedGateway), &gateway); err != nil {
		return routeValidationSnapshot{}, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gateway.UID != expectedGateway.UID || gateway.Generation != expectedGateway.Generation ||
		!gateway.DeletionTimestamp.IsZero() {
		return routeValidationSnapshot{}, errHTTPRouteChanged
	}

	var gatewayClass gatewayv1.GatewayClass
	if err := r.APIReader.Get(
		ctx,
		types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)},
		&gatewayClass,
	); err != nil {
		return routeValidationSnapshot{}, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gatewayClass.Spec.ControllerName != r.Config.ControllerName {
		return routeValidationSnapshot{}, errHTTPRouteChanged
	}
	parents, err := r.parentsForGateway(ctx, r.APIReader, &route, &gateway)
	if err != nil {
		return routeValidationSnapshot{}, err
	}
	if len(parents) == 0 {
		return routeValidationSnapshot{}, errHTTPRouteChanged
	}
	if !controller.GatewayClassSupportsInstalledVersion(&gatewayClass) {
		return routeValidationSnapshot{}, controller.ErrUnsupportedGatewayAPIVersion
	}
	if !controllerutil.ContainsFinalizer(&gateway, r.Config.GatewayFinalizer()) ||
		controller.ValidateGatewayBinding(r.Config, &gateway) != nil {
		return routeValidationSnapshot{}, errHTTPRouteChanged
	}
	return routeValidationSnapshot{
		route:        &route,
		gateway:      &gateway,
		gatewayClass: &gatewayClass,
		parents:      parents,
		disposition:  routeValidationReady,
	}, nil
}

func routeSelectionReadyForBinding(gateway *gatewayv1.Gateway, selection routeSelection, multiplePorts bool) bool {
	// A scoped Route can start on its ready ports while the provider still
	// verifies all selected ports before reporting the Route as programmed.
	var selected, ready int
	for _, parent := range selection {
		if parent.rejection != nil {
			continue
		}
		for _, listener := range parent.listeners {
			selected++
			if parent.parent.listenerSet == nil {
				if listenerProgrammedForGeneration(gateway, listener.Name) {
					ready++
				}
				continue
			}
			set := parent.parent.listenerSet
			if slices.ContainsFunc(set.Status.Listeners, func(status gatewayv1.ListenerEntryStatus) bool {
				if status.Name != listener.Name {
					return false
				}
				programmed := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerEntryConditionProgrammed))
				return programmed != nil && programmed.Status == metav1.ConditionTrue && programmed.ObservedGeneration == set.Generation
			}) {
				ready++
			}
		}
	}
	return ready != 0 && (multiplePorts || ready == selected)
}

func (r *Reconciler) observeRouteBinding(ctx context.Context, snapshot *routeValidationSnapshot) error {
	desiredIdentity := controller.RouteIdentity(r.Config, snapshot.gateway, snapshot.route)
	binding, err := controller.StoredRouteGatewayBinding(r.Config, snapshot.route, desiredIdentity)
	if err != nil {
		return err
	}
	if binding.Lifecycle == controller.RouteDetaching {
		return errHTTPRouteChanged
	}
	snapshot.binding = routeBindingSnapshot{
		identity:       binding.Identity,
		present:        binding.Identity.RouteUID != "",
		hasFinalizer:   controllerutil.ContainsFinalizer(snapshot.route, r.Config.RouteFinalizer()),
		matchesDesired: binding.Identity == desiredIdentity,
		lifecycle:      binding.Lifecycle,
		explicitScope:  binding.ExplicitScope,
	}
	if !snapshot.binding.present {
		bindings, err := controller.StoredRouteGatewayBindings(r.Config, snapshot.route)
		if err != nil {
			return err
		}
		snapshot.binding.otherBindings = len(bindings) != 0
		for _, existing := range bindings {
			old := existing.Identity
			// Preserve the singleton handoff and fence Gateway recreation by UID.
			if old.GatewayNamespace == desiredIdentity.GatewayNamespace && old.GatewayName == desiredIdentity.GatewayName {
				return errHTTPRouteChanged
			}
			if !controller.HasRouteGatewayBindings(r.Config, snapshot.route) {
				if err := r.validateLegacySiblingBinding(ctx, snapshot.route, old); err != nil {
					return err
				}
				snapshot.binding.legacySibling = old
			}
		}
	}
	return nil
}

// A legacy binding can be carried into the multi-Gateway checkpoint only
// while a current parent still resolves to its original Gateway UID.
func (r *Reconciler) validateLegacySiblingBinding(ctx context.Context, route *gatewayv1.HTTPRoute, identity cloud.Identity) error {
	parents, err := r.managedParentsWithReader(ctx, r.APIReader, route)
	if err != nil {
		return err
	}
	if len(managedParentsForGateway(parents, identity)) == 0 {
		return errHTTPRouteChanged
	}
	return nil
}

func (r *Reconciler) setRouteBindingDisposition(snapshot *routeValidationSnapshot) error {
	boundToCurrentGateway := snapshot.binding.matchesDesired && snapshot.binding.hasFinalizer
	if !snapshot.gatewayReadyForBinding && !boundToCurrentGateway {
		snapshot.disposition = routeValidationGatewayPending
		return nil
	}
	desiredBinding := snapshot.route.DeepCopy()
	if err := r.applyRouteBinding(desiredBinding, snapshot.gateway); err != nil {
		return err
	}
	if !routeBindingMetadataEqual(snapshot.route, desiredBinding) {
		snapshot.disposition = routeValidationBindingRequired
	}
	return nil
}

// validateRouteBindingTransition prevents a binding checkpoint from adopting
// incomplete metadata or overwriting cleanup authority for another Gateway.
// It is pure so an ensure snapshot can be reused without rebuilding the
// backend model.
func (r *Reconciler) validateRouteBindingTransition(snapshot routeValidationSnapshot) error {
	// Both finalizers and the stored identity prove this attached route already
	// belongs to the Gateway. Its metadata must be complete before the graph
	// writer can program a newer Gateway generation.
	if !snapshot.gatewayReadyForBinding && !r.establishedRouteBinding(snapshot) {
		return errHTTPRouteChanged
	}
	if !snapshot.binding.present && snapshot.binding.hasFinalizer && !snapshot.binding.otherBindings {
		return fmt.Errorf(
			"HTTPRoute %s has the controller finalizer but no complete stored Gateway identity",
			client.ObjectKeyFromObject(snapshot.route),
		)
	}
	desiredIdentity := controller.RouteIdentity(r.Config, snapshot.gateway, snapshot.route)
	if snapshot.binding.present && !controller.SameGatewayIdentity(snapshot.binding.identity, desiredIdentity) {
		return errHTTPRouteChanged
	}
	return nil
}

func (r *Reconciler) establishedRouteBinding(snapshot routeValidationSnapshot) bool {
	return snapshot.binding.matchesDesired && snapshot.binding.hasFinalizer &&
		snapshot.binding.lifecycle == controller.RouteAttached
}

func gatewayProgrammedForGeneration(gateway *gatewayv1.Gateway) bool {
	programmed := meta.FindStatusCondition(
		gateway.Status.Conditions,
		string(gatewayv1.GatewayConditionProgrammed),
	)
	return programmed != nil && programmed.Status == metav1.ConditionTrue &&
		programmed.ObservedGeneration == gateway.Generation
}

func listenerProgrammedForGeneration(gateway *gatewayv1.Gateway, section gatewayv1.SectionName) bool {
	for _, listener := range gateway.Status.Listeners {
		if listener.Name == section {
			programmed := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionProgrammed))
			return programmed != nil && programmed.Status == metav1.ConditionTrue && programmed.ObservedGeneration == gateway.Generation
		}
	}
	return false
}

func routeSnapshotReadError(purpose routeSnapshotPurpose, err error) error {
	if purpose == routeSnapshotForBinding {
		return errors.Join(errHTTPRouteChanged, err)
	}
	return err
}

func routeSnapshotRejection(purpose routeSnapshotPurpose, rejection error) error {
	if purpose == routeSnapshotForBinding {
		return errHTTPRouteChanged
	}
	return rejection
}

// sealRouteMutationSnapshot verifies that the ownership inputs used to build
// a desired graph have not changed before the first Kubernetes or OpenStack
// mutation. The repeated live reads are a mutation fence, not a second model
// build.
func (r *Reconciler) sealRouteMutationSnapshot(
	ctx context.Context,
	snapshot routeValidationSnapshot,
	bindingRequirement routeBindingRequirement,
) error {
	if err := r.sealRouteOwnershipSnapshot(ctx, snapshot, bindingRequirement); err != nil {
		return err
	}
	routes, err := r.liveGraphRoutes(ctx, controller.GatewayIdentity(r.Config, snapshot.gateway))
	if err != nil {
		return err
	}
	return r.validateGraphRouteModel(ctx, snapshot.backends, routes, snapshot.route)
}

// sealRouteOwnershipSnapshot checks durable route and Gateway authority when
// reconciling an individual route.
func (r *Reconciler) sealRouteOwnershipSnapshot(ctx context.Context, snapshot routeValidationSnapshot, bindingRequirement routeBindingRequirement) error {
	gateway, err := r.readRouteGatewayAuthority(ctx, snapshot.gateway)
	if err != nil {
		return err
	}
	return r.sealRouteOwnershipWithGateway(ctx, snapshot, bindingRequirement, gateway)
}

// readRouteGatewayAuthority observes the common authority for a single fence.
// Each fragment must still match its own expected Gateway and route revision.
func (r *Reconciler) readRouteGatewayAuthority(ctx context.Context, expected *gatewayv1.Gateway) (*gatewayv1.Gateway, error) {
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		return nil, err
	}
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &gateway); err != nil {
		return nil, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gateway.UID != expected.UID || gateway.Generation != expected.Generation || !gateway.DeletionTimestamp.IsZero() ||
		!controllerutil.ContainsFinalizer(&gateway, r.Config.GatewayFinalizer()) ||
		!controller.GatewayBindingMetadataEqual(r.Config, &gateway, expected) ||
		controller.ValidateGatewayBinding(r.Config, &gateway) != nil {
		return nil, errHTTPRouteChanged
	}
	var gatewayClass gatewayv1.GatewayClass
	if err := r.APIReader.Get(
		ctx,
		types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)},
		&gatewayClass,
	); err != nil {
		return nil, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gatewayClass.Spec.ControllerName != r.Config.ControllerName {
		return nil, errHTTPRouteChanged
	}
	if !controller.GatewayClassSupportsInstalledVersion(&gatewayClass) {
		return nil, controller.ErrUnsupportedGatewayAPIVersion
	}
	return &gateway, nil
}

func (r *Reconciler) sealRouteOwnershipWithGateway(ctx context.Context, snapshot routeValidationSnapshot, bindingRequirement routeBindingRequirement, gateway *gatewayv1.Gateway) error {
	var route gatewayv1.HTTPRoute
	if err := r.readRoute(ctx, r.APIReader, client.ObjectKeyFromObject(snapshot.route), &route); err != nil {
		return errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if !sameHTTPRouteRevision(
		&route,
		snapshot.route.UID,
		snapshot.route.Generation,
		!snapshot.route.DeletionTimestamp.IsZero(),
	) {
		return errHTTPRouteChanged
	}
	if gateway.UID != snapshot.gateway.UID || gateway.Generation != snapshot.gateway.Generation ||
		!controller.GatewayBindingMetadataEqual(r.Config, gateway, snapshot.gateway) {
		return errHTTPRouteChanged
	}
	desired := controller.RouteIdentity(r.Config, gateway, &route)
	binding, err := controller.StoredRouteGatewayBinding(r.Config, &route, desired)
	if err != nil {
		return err
	}
	if legacy := snapshot.binding.legacySibling; legacy.RouteUID != "" {
		sibling, err := controller.StoredRouteGatewayBinding(r.Config, &route, legacy)
		if err != nil {
			return err
		}
		if sibling.Identity != legacy {
			return errHTTPRouteChanged
		}
		if err := r.validateLegacySiblingBinding(ctx, &route, legacy); err != nil {
			return err
		}
	}
	if binding.Lifecycle != snapshot.binding.lifecycle {
		return errHTTPRouteChanged
	}
	if bindingRequirement == routeBindingRequired &&
		(binding.Identity != desired || !controllerutil.ContainsFinalizer(&route, r.Config.RouteFinalizer())) {
		return errHTTPRouteChanged
	}
	parents, err := r.parentsForGateway(ctx, r.APIReader, &route, gateway)
	if err != nil {
		return err
	}
	if !slices.EqualFunc(parents, snapshot.parents, func(current, previous managedRouteParent) bool {
		if !controller.ParentRefsEqual(current.ref, route.Namespace, previous.ref, snapshot.route.Namespace) {
			return false
		}
		return current.listenerSet == nil && previous.listenerSet == nil || current.listenerSet != nil && previous.listenerSet != nil &&
			current.listenerSet.UID == previous.listenerSet.UID && current.listenerSet.Generation == previous.listenerSet.Generation
	}) {
		return errHTTPRouteChanged
	}

	return nil
}

func (r *Reconciler) buildRouteCleanupSnapshot(
	ctx context.Context,
	expected *gatewayv1.HTTPRoute,
	expectedIdentity cloud.Identity,
) (routeCleanupSnapshot, error) {
	if r.APIReader == nil {
		return routeCleanupSnapshot{}, controller.ErrAPIReaderRequired
	}
	var route gatewayv1.HTTPRoute
	if err := r.readRoute(ctx, r.APIReader, client.ObjectKeyFromObject(expected), &route); err != nil {
		return routeCleanupSnapshot{}, errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if !sameHTTPRouteRevision(
		&route,
		expected.UID,
		expected.Generation,
		!expected.DeletionTimestamp.IsZero(),
	) || route.ResourceVersion != expected.ResourceVersion {
		return routeCleanupSnapshot{}, errHTTPRouteChanged
	}
	storedBinding, err := controller.StoredRouteGatewayBinding(r.Config, &route, expectedIdentity)
	stored := storedBinding.Identity
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	if stored != expectedIdentity {
		return routeCleanupSnapshot{}, errHTTPRouteChanged
	}
	snapshot := routeCleanupSnapshot{
		route:       &route,
		identity:    stored,
		disposition: routeCleanupRequired,
	}
	if !route.DeletionTimestamp.IsZero() || storedBinding.Lifecycle == controller.RouteDetaching {
		return snapshot, nil
	}
	attachment, attachmentRejection, err := r.liveBoundRouteAttachment(ctx, &route, stored)
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	if attachmentRejection != nil {
		snapshot.attachmentRevoked = true
		return snapshot, nil
	}

	parentObservations, err := r.observeRouteParents(ctx, r.APIReader, &route)
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	if routeCleanupAllowedDuringVersionMismatch(parentObservations, stored) {
		snapshot.parents = parentObservations
		return snapshot, nil
	}
	rejection, err := r.liveRouteBackendReferenceRejection(ctx, &route)
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	if rejection != nil && !r.usesHTTPBackendGroups(&route) {
		snapshot.referenceRevoked = true
		return snapshot, nil
	}
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) && rejection != nil {
			snapshot.referenceRevoked = true
			snapshot.bundleUnsupported = true
			return snapshot, nil
		}
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) &&
			!r.routeForwardingEstablished(&route, managedParentsForGateway(managedParentsFromObservations(parentObservations), stored), stored, attachment) {
			snapshot.bundleUnsupported = true
			snapshot.parents = parentObservations
			return snapshot, nil
		}
		return routeCleanupSnapshot{}, err
	}
	parentObservations, err = r.observeRouteParents(ctx, r.APIReader, &route)
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	snapshot.parents = parentObservations
	managedParents := managedParentsForGateway(managedParentsFromObservations(parentObservations), stored)
	if len(managedParents) == 0 {
		return snapshot, nil
	}
	parent := managedParents[0]
	if !controller.GatewayClassSupportsInstalledVersion(parent.gatewayClass) {
		if rejection != nil {
			snapshot.referenceRevoked = true
			return snapshot, nil
		}
		if !r.routeForwardingEstablished(&route, managedParents, stored, attachment) {
			return snapshot, nil
		}
		return routeCleanupSnapshot{}, controller.ErrUnsupportedGatewayAPIVersion
	}
	if !r.structurallySupportedRoute(&route) {
		return snapshot, nil
	}
	if r.Config.GRPCExactMethodEnabled() {
		admission, err := r.liveRouteGraphAdmission(ctx, parent.gateway, &route)
		if err != nil {
			return routeCleanupSnapshot{}, err
		}
		if admission.profileRejectedParent(routeKey(&route)) {
			snapshot.profileRejected = true
			return snapshot, nil
		}
	}
	desired, err := r.buildRouteModelWithReader(ctx, r.APIReader, &route, parent.gateway)
	if err != nil {
		var buildError *routeBuildError
		if errors.As(err, &buildError) {
			// Attachment authorization can change after the earlier read.
			snapshot.attachmentRevoked = errors.Is(err, errRouteAttachmentRejected)
			return snapshot, nil
		}
		return routeCleanupSnapshot{}, err
	}
	if desired.Identity == stored {
		snapshot.disposition = routeCleanupStillDesired
	}
	return snapshot, nil
}

func routeCleanupAllowedDuringVersionMismatch(
	parents []routeParentObservation,
	stored cloud.Identity,
) bool {
	for _, parent := range parents {
		if parent.gateway == nil {
			continue
		}
		if parent.gatewayClass == nil {
			return false
		}
		if !parent.managed {
			continue
		}
		boundGatewayIsDeleting := parent.gateway.Namespace == stored.GatewayNamespace &&
			parent.gateway.Name == stored.GatewayName && string(parent.gateway.UID) == stored.GatewayUID &&
			!parent.gateway.DeletionTimestamp.IsZero()
		if !boundGatewayIsDeleting {
			return false
		}
	}
	return true
}

func (r *Reconciler) sealRouteCleanupSnapshot(
	ctx context.Context,
	snapshot routeCleanupSnapshot,
) error {
	var route gatewayv1.HTTPRoute
	if err := r.readRoute(ctx, r.APIReader, client.ObjectKeyFromObject(snapshot.route), &route); err != nil {
		return errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if !sameHTTPRouteRevision(
		&route,
		snapshot.route.UID,
		snapshot.route.Generation,
		!snapshot.route.DeletionTimestamp.IsZero(),
	) || route.ResourceVersion != snapshot.route.ResourceVersion {
		return errHTTPRouteChanged
	}
	binding, err := controller.StoredRouteGatewayBinding(r.Config, &route, snapshot.identity)
	if err != nil {
		return err
	}
	if binding.Identity != snapshot.identity {
		return errHTTPRouteChanged
	}
	if snapshot.referenceRevoked {
		rejection, err := r.liveRouteBackendReferenceRejection(ctx, &route)
		if err != nil {
			return err
		}
		if rejection == nil {
			return errHTTPRouteChanged
		}
	}
	if snapshot.parents != nil {
		parents, err := r.observeRouteParents(ctx, r.APIReader, &route)
		if err != nil {
			return err
		}
		if !slices.EqualFunc(snapshot.parents, parents, sameCleanupParentObservation) {
			return errHTTPRouteChanged
		}
		if snapshot.profileRejected {
			managedParents := managedParentsForGateway(managedParentsFromObservations(parents), snapshot.identity)
			if len(managedParents) == 0 {
				return errHTTPRouteChanged
			}
			admission, err := r.liveRouteGraphAdmission(ctx, managedParents[0].gateway, &route)
			if err != nil {
				return err
			}
			if !admission.profileRejectedParent(routeKey(&route)) {
				return errHTTPRouteChanged
			}
		}
	}
	if snapshot.bundleUnsupported {
		err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader)
		if err == nil {
			return errHTTPRouteChanged
		}
		if !errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return err
		}
	}
	if snapshot.attachmentRevoked {
		_, rejection, err := r.liveBoundRouteAttachment(ctx, &route, binding.Identity)
		if err != nil {
			return err
		}
		if rejection == nil {
			return errHTTPRouteChanged
		}
	}
	return nil
}

func sameCleanupParentObservation(a, b routeParentObservation) bool {
	if a.managed != b.managed || (a.gateway == nil) != (b.gateway == nil) || (a.gatewayClass == nil) != (b.gatewayClass == nil) ||
		(a.listenerSet == nil) != (b.listenerSet == nil) {
		return false
	}
	if a.listenerSet != nil && (a.listenerSet.UID != b.listenerSet.UID || a.listenerSet.Generation != b.listenerSet.Generation ||
		!a.listenerSet.DeletionTimestamp.Equal(b.listenerSet.DeletionTimestamp)) {
		return false
	}
	if a.gateway != nil && (a.gateway.UID != b.gateway.UID || a.gateway.Generation != b.gateway.Generation) {
		return false
	}
	return a.gatewayClass == nil || (a.gatewayClass.UID == b.gatewayClass.UID &&
		controller.GatewayClassSupportsInstalledVersion(a.gatewayClass) == controller.GatewayClassSupportsInstalledVersion(b.gatewayClass))
}
