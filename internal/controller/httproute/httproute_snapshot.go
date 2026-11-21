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
}

// routeValidationSnapshot contains one live view of every Kubernetes input
// that authorizes a route graph mutation. It is local to one mutation attempt
// and must not be retained after releasing the Gateway graph lock.
type routeValidationSnapshot struct {
	route             *gatewayv1.HTTPRoute
	gateway           *gatewayv1.Gateway
	parentRef         gatewayv1.ParentReference
	binding           routeBindingSnapshot
	desired           cloud.RouteSpec
	backends          *routeModelInputs
	backendDiagnostic model.Diagnostic
	gatewayProgrammed bool
	disposition       routeValidationDisposition
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

	if err := r.observeRouteBinding(&snapshot); err != nil {
		return routeValidationSnapshot{}, err
	}
	candidates, err := r.validateRouteSnapshotParent(&snapshot, purpose)
	if err != nil || snapshot.disposition == routeValidationGatewayPending {
		return snapshot, err
	}
	if _, err := validateRouteStructure(snapshot.route); err != nil {
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
	listener, err := selectRouteAttachment(snapshot.route, snapshot.gateway, candidates, inputs.input.Namespaces)
	if err != nil {
		return routeValidationSnapshot{}, routeSnapshotRejection(purpose, err)
	}
	if controller.GatewayListenerScope(r.Config, snapshot.gateway) == gatewayv1.HTTPProtocolType {
		snapshot.gatewayProgrammed = listenerProgrammedForGeneration(snapshot.gateway, listener.Name)
	}
	if purpose == routeSnapshotForBinding {
		if err := r.validateRouteBindingTransition(snapshot); err != nil {
			return routeValidationSnapshot{}, err
		}
	}
	desired, diagnostic, err := r.buildRouteModelFromInputs(ctx, r.APIReader, snapshot.route, snapshot.gateway, *listener, inputs)
	if err != nil {
		return routeValidationSnapshot{}, routeSnapshotReadError(purpose, err)
	}
	snapshot.desired = desired
	snapshot.backends = inputs
	snapshot.backendDiagnostic = diagnostic

	if purpose == routeSnapshotForEnsure {
		r.setRouteBindingDisposition(&snapshot)
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
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expectedRoute), &route); err != nil {
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
	parentRef, err := r.soleParentForManagedGateway(ctx, &route, &gateway)
	if err != nil {
		return routeValidationSnapshot{}, err
	}
	if parentRef == nil {
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
		route:       &route,
		gateway:     &gateway,
		parentRef:   *parentRef,
		disposition: routeValidationReady,
	}, nil
}

func (r *Reconciler) validateRouteSnapshotParent(
	snapshot *routeValidationSnapshot,
	purpose routeSnapshotPurpose,
) ([]*gatewayv1.Listener, error) {
	listeners, validationErr := gatewaycontroller.ValidatedHTTPRouteListeners(r.Config, snapshot.gateway)
	if validationErr != nil {
		if purpose == routeSnapshotForBinding {
			return nil, errHTTPRouteChanged
		}
		snapshot.disposition = routeValidationGatewayPending
		return nil, nil
	}
	parent := managedRouteParent{
		ref:     snapshot.parentRef,
		gateway: snapshot.gateway,
	}
	candidates, err := routeParentCandidates(snapshot.route, parent, listeners)
	if err != nil {
		return nil, routeSnapshotRejection(purpose, err)
	}
	if controller.GatewayListenerScope(r.Config, snapshot.gateway) != gatewayv1.HTTPProtocolType {
		if gatewaycontroller.RequiresHTTPListenerScope(r.Config, snapshot.gateway) {
			// Enrollment requires complete route checkpoints, so an established
			// route must be able to repair its binding before enrollment finishes.
			annotations := snapshot.route.Annotations
			if !r.establishedRouteBinding(*snapshot) ||
				(annotations[r.Config.RouteClusterIDAnnotation()] != "" && annotations[r.Config.RouteProjectIDAnnotation()] != "") {
				snapshot.disposition = routeValidationGatewayPending
				return nil, nil
			}
		} else if snapshot.gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(int(candidates[0].Port)) {
			return nil, errHTTPRouteChanged
		}
	}
	snapshot.gatewayProgrammed = gatewayProgrammedForGeneration(snapshot.gateway)
	return candidates, nil
}

func (r *Reconciler) observeRouteBinding(snapshot *routeValidationSnapshot) error {
	if snapshot.route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
		return errHTTPRouteChanged
	}
	storedIdentity, bindingPresent, err := r.storedRouteIdentity(snapshot.route)
	if err != nil {
		return err
	}
	desiredIdentity := controller.RouteIdentity(r.Config, snapshot.gateway, snapshot.route)
	snapshot.binding = routeBindingSnapshot{
		identity:       storedIdentity,
		present:        bindingPresent,
		hasFinalizer:   controllerutil.ContainsFinalizer(snapshot.route, r.Config.RouteFinalizer()),
		matchesDesired: bindingPresent && storedIdentity == desiredIdentity,
	}
	return nil
}

func (r *Reconciler) setRouteBindingDisposition(snapshot *routeValidationSnapshot) {
	boundToCurrentGateway := snapshot.binding.matchesDesired && snapshot.binding.hasFinalizer
	if !snapshot.gatewayProgrammed && !boundToCurrentGateway {
		snapshot.disposition = routeValidationGatewayPending
		return
	}
	desiredBinding := snapshot.route.DeepCopy()
	r.applyRouteBinding(desiredBinding, snapshot.gateway)
	if !routeBindingMetadataEqual(snapshot.route, desiredBinding) {
		snapshot.disposition = routeValidationBindingRequired
	}
}

// validateRouteBindingTransition prevents a binding checkpoint from adopting
// incomplete metadata or overwriting cleanup authority for another Gateway.
// It is pure so an ensure snapshot can be reused without rebuilding the
// backend model.
func (r *Reconciler) validateRouteBindingTransition(snapshot routeValidationSnapshot) error {
	// Both finalizers and the stored identity prove this attached route already
	// belongs to the Gateway. Its metadata must be complete before the graph
	// writer can program a newer Gateway generation.
	if !snapshot.gatewayProgrammed && !r.establishedRouteBinding(snapshot) {
		return errHTTPRouteChanged
	}
	if !snapshot.binding.present && snapshot.binding.hasFinalizer {
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
		snapshot.route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteAttached
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

// sealRouteOwnershipSnapshot checks durable route and Gateway authority. The
// graph writer validates shared backend inputs once after all route fences.
func (r *Reconciler) sealRouteOwnershipSnapshot(ctx context.Context, snapshot routeValidationSnapshot, bindingRequirement routeBindingRequirement) error {
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		return err
	}
	var route gatewayv1.HTTPRoute
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(snapshot.route), &route); err != nil {
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
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(snapshot.gateway), &gateway); err != nil {
		return errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gateway.UID != snapshot.gateway.UID || gateway.Generation != snapshot.gateway.Generation ||
		!gateway.DeletionTimestamp.IsZero() {
		return errHTTPRouteChanged
	}
	if !controllerutil.ContainsFinalizer(&gateway, r.Config.GatewayFinalizer()) ||
		!controller.GatewayBindingMetadataEqual(r.Config, &gateway, snapshot.gateway) ||
		controller.ValidateGatewayBinding(r.Config, &gateway) != nil {
		return errHTTPRouteChanged
	}
	if route.Annotations[r.Config.RouteLifecycleAnnotation()] != snapshot.route.Annotations[r.Config.RouteLifecycleAnnotation()] {
		return errHTTPRouteChanged
	}
	if bindingRequirement == routeBindingRequired {
		stored, present, err := r.storedRouteIdentity(&route)
		if err != nil {
			return err
		}
		if !present || stored != controller.RouteIdentity(r.Config, &gateway, &route) ||
			!controllerutil.ContainsFinalizer(&route, r.Config.RouteFinalizer()) {
			return errHTTPRouteChanged
		}
	}
	var gatewayClass gatewayv1.GatewayClass
	if err := r.APIReader.Get(
		ctx,
		types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)},
		&gatewayClass,
	); err != nil {
		return errors.Join(errHTTPRouteChanged, client.IgnoreNotFound(err))
	}
	if gatewayClass.Spec.ControllerName != r.Config.ControllerName {
		return errHTTPRouteChanged
	}
	if !controller.GatewayClassSupportsInstalledVersion(&gatewayClass) {
		return controller.ErrUnsupportedGatewayAPIVersion
	}
	parentRef, err := r.soleParentForManagedGateway(ctx, &route, &gateway)
	if err != nil {
		return err
	}
	if parentRef == nil || !controller.ParentRefsEqual(*parentRef, route.Namespace, snapshot.parentRef, snapshot.route.Namespace) {
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
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &route); err != nil {
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
	stored, present, err := r.storedRouteIdentity(&route)
	if err != nil {
		return routeCleanupSnapshot{}, err
	}
	if !present || stored != expectedIdentity {
		return routeCleanupSnapshot{}, errHTTPRouteChanged
	}
	snapshot := routeCleanupSnapshot{
		route:       &route,
		identity:    stored,
		disposition: routeCleanupRequired,
	}
	if !route.DeletionTimestamp.IsZero() || route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
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
	if rejection != nil && !hasMultipleRoutePaths(&route) {
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
			!r.routeForwardingEstablished(&route, managedParentsFromObservations(parentObservations), stored, attachment) {
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
	managedParents := managedParentsFromObservations(parentObservations)
	if len(managedParents) != 1 {
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
	if !structurallySupportedRoute(&route) {
		return snapshot, nil
	}
	desired, err := r.buildRouteModelWithReader(ctx, r.APIReader, &route, parent.gateway)
	if err != nil {
		var buildError *routeBuildError
		if errors.As(err, &buildError) {
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
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(snapshot.route), &route); err != nil {
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
	stored, present, err := r.storedRouteIdentity(&route)
	if err != nil {
		return err
	}
	if !present || stored != snapshot.identity {
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
	if snapshot.attachmentRevoked {
		_, rejection, err := r.liveBoundRouteAttachment(ctx, &route, stored)
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
	return nil
}

func sameCleanupParentObservation(a, b routeParentObservation) bool {
	if a.managed != b.managed || (a.gateway == nil) != (b.gateway == nil) || (a.gatewayClass == nil) != (b.gatewayClass == nil) {
		return false
	}
	if a.gateway != nil && (a.gateway.UID != b.gateway.UID || a.gateway.Generation != b.gateway.Generation) {
		return false
	}
	return a.gatewayClass == nil || (a.gatewayClass.UID == b.gatewayClass.UID &&
		controller.GatewayClassSupportsInstalledVersion(a.gatewayClass) == controller.GatewayClassSupportsInstalledVersion(b.gatewayClass))
}
