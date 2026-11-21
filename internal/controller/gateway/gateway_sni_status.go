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

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

const sniParentNoMatchingListenerMessage = "The parentRef does not select a listener"

// Gateway API limits RouteStatus.Parents to 32 entries across all controllers.
const maxSNIRouteParentStatuses = 32

func (r *Reconciler) reconcileSNIParentStatuses(ctx context.Context, key client.ObjectKey) error {
	var routes gatewayv1.TLSRouteList
	if err := r.List(ctx, &routes, client.MatchingFields{controller.IndexTLSRouteByGateway: controller.ObjectKeyString(key)}); err != nil {
		return fmt.Errorf("list TLSRoutes for Gateway: %w", err)
	}
	if len(routes.Items) == 0 {
		return nil
	}
	if r.APIReader == nil {
		return controller.ErrAPIReaderRequired
	}
	gateway, managed, err := r.readSNIParentGateway(ctx, key)
	if err != nil {
		return err
	}
	var reportErr error
	for i := range routes.Items {
		var route gatewayv1.TLSRoute
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(&routes.Items[i]), &route); err != nil {
			reportErr = errors.Join(reportErr, client.IgnoreNotFound(err))
			continue
		}
		if !route.DeletionTimestamp.IsZero() {
			continue
		}
		target := route.DeepCopy()
		if managed {
			r.updateSNIRouteParents(target, gateway, func(status *gatewayv1.RouteParentStatus) bool {
				reason, message := r.sniParentListenerRejection(gateway.Spec.Listeners, status.ParentRef)
				if reason == "" {
					return false
				}
				r.setRejectedSNIParent(status, reason, message, route.Generation)
				return true
			})
		} else {
			r.removeOwnedSNIParents(target, gateway)
		}
		if equality.Semantic.DeepEqual(route.Status, target.Status) {
			continue
		}
		latest, stillManaged, err := r.readSNIParentGateway(ctx, key)
		if err != nil {
			return errors.Join(reportErr, err)
		}
		if managed != stillManaged || gateway.UID != latest.UID || gateway.Generation != latest.Generation {
			return errors.Join(reportErr, errGatewayChanged)
		}
		reportErr = errors.Join(reportErr, r.patchSNIRouteStatus(ctx, &route, target))
	}
	return reportErr
}

func (r *Reconciler) readSNIParentGateway(ctx context.Context, key client.ObjectKey) (*gatewayv1.Gateway, bool, error) {
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
	if err := r.APIReader.Get(ctx, key, gateway); err != nil {
		return gateway, false, client.IgnoreNotFound(err)
	}
	if !gateway.DeletionTimestamp.IsZero() {
		return gateway, false, nil
	}
	var class gatewayv1.GatewayClass
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
		return gateway, false, client.IgnoreNotFound(err)
	}
	// A GatewayClass finalizer can keep a deleting class in use by this Gateway.
	return gateway, class.Spec.ControllerName == r.Config.ControllerName, nil
}

func sniParentSelectsListener(ref gatewayv1.ParentReference, listener gatewayv1.Listener) bool {
	return (ref.SectionName == nil || *ref.SectionName == listener.Name) && (ref.Port == nil || *ref.Port == listener.Port)
}

func (r *Reconciler) sniParentListenerRejection(listeners []gatewayv1.Listener, ref gatewayv1.ParentReference) (gatewayv1.RouteConditionReason, string) {
	selected := false
	for _, listener := range listeners {
		if !sniParentSelectsListener(ref, listener) {
			continue
		}
		selected = true
		if listener.Protocol == gatewayv1.TLSProtocolType && len(listenerSupportedRouteKinds(r.Config, listener)) > 0 {
			return "", ""
		}
	}
	if selected {
		return gatewayv1.RouteReasonNotAllowedByListeners, "No selected listener accepts TLSRoutes"
	}
	return gatewayv1.RouteReasonNoMatchingParent, sniParentNoMatchingListenerMessage
}

func (r *Reconciler) setRejectedSNIParent(status *gatewayv1.RouteParentStatus, reason gatewayv1.RouteConditionReason, message string, generation int64) {
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(reason), message, generation))
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.RouteConditionResolvedRefs), metav1.ConditionUnknown, string(gatewayv1.RouteReasonPending), "The TLSRoute is not attached", generation))
	controller.SetCondition(&status.Conditions, controller.Condition(r.Config.Domain()+"/Programmed", metav1.ConditionFalse, string(reason), message, generation))
	meta.RemoveStatusCondition(&status.Conditions, r.Config.Domain()+"/ForwardingTarget")
}

func (r *Reconciler) ownsSNIParent(route *gatewayv1.TLSRoute, gateway *gatewayv1.Gateway, status gatewayv1.RouteParentStatus) bool {
	return status.ControllerName == r.Config.ControllerName && sniParentTargetsGateway(route.Namespace, status.ParentRef, gateway)
}

func (r *Reconciler) removeOwnedSNIParents(route *gatewayv1.TLSRoute, gateway *gatewayv1.Gateway) {
	route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
		return r.ownsSNIParent(route, gateway, status)
	})
}

// updateSNIRouteParents preserves the order of existing status entries so
// independently reconciling Gateways do not move their parents back and forth.
func (r *Reconciler) updateSNIRouteParents(route *gatewayv1.TLSRoute, gateway *gatewayv1.Gateway, update func(*gatewayv1.RouteParentStatus) bool) {
	matches := func(ref gatewayv1.ParentReference, status gatewayv1.RouteParentStatus) bool {
		return controller.ParentRefsEqual(ref, route.Namespace, status.ParentRef, route.Namespace)
	}
	route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
		return r.ownsSNIParent(route, gateway, status) && !slices.ContainsFunc(route.Spec.ParentRefs, func(ref gatewayv1.ParentReference) bool {
			return matches(ref, status)
		})
	})
	for _, ref := range route.Spec.ParentRefs {
		if !sniParentTargetsGateway(route.Namespace, ref, gateway) {
			continue
		}
		index := slices.IndexFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
			return r.ownsSNIParent(route, gateway, status) && matches(ref, status)
		})
		if index < 0 && len(route.Status.Parents) >= maxSNIRouteParentStatuses {
			continue
		}
		status := gatewayv1.RouteParentStatus{ParentRef: ref, ControllerName: r.Config.ControllerName}
		if index >= 0 {
			status.Conditions = route.Status.Parents[index].Conditions
		}
		if !update(&status) {
			continue
		}
		if index >= 0 {
			route.Status.Parents[index] = status
		} else {
			route.Status.Parents = append(route.Status.Parents, status)
		}
	}
}
