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
	"slices"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
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
	identity, bound, err := r.storedRouteIdentity(&current)
	if err != nil {
		return ctrl.Result{}, err
	}
	var attachment boundHTTPAttachment
	var attachmentRejection error
	if bound {
		attachment, attachmentRejection, err = r.liveBoundRouteAttachment(ctx, &current, identity)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(parents) == 0 {
		if attachmentRejection != nil {
			*route = current
			scope.versionSkew = true
			return r.setRouteStatusesAndDetach(ctx, scope, nil)
		}
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
	var rejection *routeBuildError
	if attachmentRejection == nil {
		rejection, err = r.liveRouteBackendReferenceRejection(ctx, &current)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	detach := bound && (attachmentRejection != nil || rejection != nil || !r.routeForwardingEstablished(&current, parents, identity, attachment))
	updates := make([]parentStatusUpdate, 0, len(parents)+1)
	for _, parent := range parents {
		status := unsupportedGatewayAPIVersionRouteStatus()
		if attachmentRejection != nil && controller.SameGatewayIdentity(identity, controller.RouteIdentity(r.Config, parent.gateway, &current)) {
			status = statusForRouteBuildError(attachmentRejection)
		} else if rejection != nil {
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

// routeForwardingEstablished requires the current route revision and proof
// that the established forwarding fits within the current listener hostname.
func (r *Reconciler) routeForwardingEstablished(route *gatewayv1.HTTPRoute, parents []managedRouteParent, identity cloud.Identity, attachment boundHTTPAttachment) bool {
	listener := attachment.listener
	if listener == nil || len(parents) != 1 || parents[0].gateway.Generation != attachment.gatewayGeneration ||
		!controller.SameGatewayIdentity(identity, controller.RouteIdentity(r.Config, parents[0].gateway, route)) {
		return false
	}
	parent := parents[0]
	listeners, err := gatewaycontroller.SupportedListeners(parent.gateway)
	if err != nil {
		return false
	}
	gatewayProgrammed := gatewayProgrammedForGeneration(parent.gateway)
	if controller.GatewayListenerScope(r.Config, parent.gateway) == gatewayv1.HTTPProtocolType {
		if controller.ValidateGatewayBinding(r.Config, parent.gateway) != nil {
			return false
		}
		// An explicit port stays fixed for this Route revision. A section name
		// needs current provider proof because the section may change ports.
		if parent.ref.SectionName != nil {
			gatewayProgrammed = listenerProgrammedForGeneration(parent.gateway, listener.Name)
			if parent.ref.Port == nil && !gatewayProgrammed {
				return false
			}
		} else if parent.ref.Port == nil && (!gatewayProgrammed || gatewaycontroller.HasMultipleHTTPListenerPorts(parent.gateway)) {
			return false
		}
	} else if parent.gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(int(listener.Port)) {
		return false
	}
	if r.currentRouteProgrammedCondition(route, parent.ref) == nil {
		return false
	}
	if listener.Hostname == nil {
		return true
	}
	// Namespace labels can move an unsectioned route between shared sections
	// without a new Gateway or Route generation. In that case a current status
	// alone cannot prove which exact Host predicate was installed.
	if (len(listeners) == 1 || parent.ref.SectionName != nil) && gatewayProgrammed {
		return true
	}
	// The previous listener could select any declared hostname. Keep
	// forwarding only if all declarations already fit the current restriction.
	effective := model.IntersectHostnames(listener.Hostname, route.Spec.Hostnames)
	return slices.Equal(effective, model.IntersectHostnames(nil, route.Spec.Hostnames))
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
