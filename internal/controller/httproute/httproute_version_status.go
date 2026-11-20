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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func (r *Reconciler) setRouteUnsupportedVersion(
	ctx context.Context,
	scope *httpRouteScope,
) (ctrl.Result, error) {
	route := scope.route
	scope.statusPlan = nil
	if r.APIReader == nil {
		return ctrl.Result{}, controller.ErrAPIReaderRequired
	}
	var current gatewayv1.HTTPRoute
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(route), &current); err != nil {
		if apierrors.IsNotFound(err) {
			scope.skipPatch = true
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !sameHTTPRouteRevision(&current, route.UID, route.Generation, false) {
		scope.skipPatch = true
		return ctrl.Result{}, nil
	}
	parents, err := r.managedParentsWithReader(ctx, r.APIReader, &current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(parents) == 0 {
		scope.skipPatch = true
		return ctrl.Result{}, nil
	}
	liveClassUnsupported := false
	for _, parent := range parents {
		if !controller.GatewayClassSupportsInstalledVersion(parent.gatewayClass) {
			liveClassUnsupported = true
			break
		}
	}
	recheckVersion := false
	if !liveClassUnsupported {
		versionErr := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader)
		if versionErr == nil {
			return ctrl.Result{}, errHTTPRouteChanged
		}
		if !errors.Is(versionErr, controller.ErrUnsupportedGatewayAPIVersion) {
			return ctrl.Result{}, versionErr
		}
		recheckVersion = true
	}
	rejection, err := r.liveRouteBackendReferenceRejection(ctx, &current)
	if err != nil {
		return ctrl.Result{}, err
	}
	identity, bound, err := r.storedRouteIdentity(&current)
	if err != nil {
		return ctrl.Result{}, err
	}
	detach := bound && (rejection != nil || !r.routeForwardingEstablished(&current, parents, identity))
	updates := make([]parentStatusUpdate, 0, len(parents)+1)
	for _, parent := range parents {
		status := unsupportedGatewayAPIVersionRouteStatus()
		if rejection != nil {
			unresolved := statusForRouteBuildError(rejection)
			unresolved.accepted, unresolved.acceptedReason, unresolved.acceptedMessage =
				status.accepted, status.acceptedReason, status.acceptedMessage
			status = unresolved
		} else if !detach {
			if programmed := r.currentRouteProgrammedCondition(&current, parent.ref); programmed != nil {
				status.programmed = programmed.Status
				status.programmedReason = programmed.Reason
				status.programmedMessage = programmed.Message
			}
		}
		updates = append(updates, parentStatusUpdate{
			parent: parent.ref,
			status: status,
		})
	}
	*route = current
	if detach {
		scope.versionSkew = true
		return r.setRouteStatusesAndDetach(ctx, scope, updates)
	}
	scope.setStatuses(updates)
	result := ctrl.Result{}
	if recheckVersion {
		result.RequeueAfter = controller.GatewayAPIVersionRequeueAfter(route.UID)
	}
	return result, nil
}

// routeForwardingEstablished proves that the declared backend belongs to the
// last programmed route revision. A binding alone does not record that revision.
func (r *Reconciler) routeForwardingEstablished(route *gatewayv1.HTTPRoute, parents []managedRouteParent, identity cloud.Identity) bool {
	if len(route.Spec.ParentRefs) != 1 || len(parents) != 1 ||
		!controller.SameGatewayIdentity(identity, controller.RouteIdentity(r.Config, parents[0].gateway, route)) {
		return false
	}
	return r.currentRouteProgrammedCondition(route, parents[0].ref) != nil
}

func (r *Reconciler) currentRouteProgrammedCondition(
	route *gatewayv1.HTTPRoute,
	parentRef gatewayv1.ParentReference,
) *metav1.Condition {
	for _, parent := range route.Status.Parents {
		if parent.ControllerName != r.Config.ControllerName ||
			!controller.ParentRefsEqual(parent.ParentRef, route.Namespace, parentRef, route.Namespace) {
			continue
		}
		programmed := meta.FindStatusCondition(parent.Conditions, r.Config.Domain()+"/Programmed")
		if programmed != nil && programmed.Status == metav1.ConditionTrue &&
			programmed.ObservedGeneration == route.Generation {
			return programmed
		}
		break
	}
	return nil
}
