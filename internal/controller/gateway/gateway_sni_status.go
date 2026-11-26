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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const sniParentNoMatchingListenerMessage = "The parentRef does not select a listener"

func (r *Reconciler) reconcileSNIParentStatuses(ctx context.Context, key client.ObjectKey, versionBlocked *gatewayv1.Gateway) error {
	if r.APIReader == nil {
		return controller.ErrAPIReaderRequired
	}
	var routes gatewayv1.TLSRouteList
	if r.Config.TLSRoutesEnabled() {
		if err := r.List(ctx, &routes, client.MatchingFields{controller.IndexTLSRouteByGateway: controller.ObjectKeyString(key)}); err != nil {
			return fmt.Errorf("list TLSRoutes for Gateway: %w", err)
		}
	} else {
		// The index is disabled after opt-out, but existing statuses can
		// still belong to a bound TCP TLS listener.
		if err := r.APIReader.List(ctx, &routes); err != nil {
			if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if routes.Continue != "" {
			return fmt.Errorf("incomplete TLSRoute inventory")
		}
		target := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
		routes.Items = slices.DeleteFunc(routes.Items, func(route gatewayv1.TLSRoute) bool {
			return !slices.ContainsFunc(route.Spec.ParentRefs, func(ref gatewayv1.ParentReference) bool {
				return parentRefTargetsGateway(route.Namespace, ref, target)
			}) && !slices.ContainsFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
				return r.ownsSNIParent(&route, target, status)
			})
		})
	}
	discoverDefaults := r.Config.TLSRoutesEnabled() && r.Config.DefaultGatewaysEnabled()
	if len(routes.Items) == 0 && !discoverDefaults {
		return nil
	}
	gateway, managed, err := r.readSNIParentGateway(ctx, key)
	if err != nil {
		return err
	}
	if managed && discoverDefaults && gateway.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll {
		var defaults gatewayv1.TLSRouteList
		if err := r.List(ctx, &defaults, client.MatchingFields{controller.IndexRouteByDefaultGateways: string(gatewayv1.GatewayDefaultScopeAll)}); err != nil {
			return fmt.Errorf("list TLSRoutes using default Gateways: %w", err)
		}
		seen := make(map[client.ObjectKey]bool, len(routes.Items))
		for i := range routes.Items {
			seen[client.ObjectKeyFromObject(&routes.Items[i])] = true
		}
		for i := range defaults.Items {
			if !seen[client.ObjectKeyFromObject(&defaults.Items[i])] {
				routes.Items = append(routes.Items, defaults.Items[i])
			}
		}
	}
	rawTLSVersionUnsupported := versionBlocked != nil && gateway.UID == versionBlocked.UID &&
		gateway.Generation == versionBlocked.Generation && controller.GatewayBindingMetadataEqual(r.Config, gateway, versionBlocked)
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
				if rawTLSVersionUnsupported {
					return r.invalidateSNIForwarding(status, route.Generation)
				}
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
		if managed != stillManaged || gateway.UID != latest.UID || gateway.Generation != latest.Generation ||
			rawTLSVersionUnsupported && !controller.GatewayBindingMetadataEqual(r.Config, gateway, latest) {
			return errors.Join(reportErr, errGatewayChanged)
		}
		reportErr = errors.Join(reportErr, r.patchSNIRouteStatus(ctx, &route, target))
	}
	return reportErr
}

func (r *Reconciler) invalidateSNIForwarding(status *gatewayv1.RouteParentStatus, generation int64) bool {
	programmedType, targetType := r.Config.Domain()+"/Programmed", r.Config.Domain()+"/ForwardingTarget"
	if !meta.IsStatusConditionTrue(status.Conditions, programmedType) && meta.FindStatusCondition(status.Conditions, targetType) == nil {
		return false
	}
	controller.SetCondition(&status.Conditions, controller.Condition(programmedType, metav1.ConditionFalse,
		string(gatewayv1.RouteReasonPending), controller.UnsupportedGatewayAPIVersionMessage, generation))
	meta.RemoveStatusCondition(&status.Conditions, targetType)
	return true
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
		if listener.Protocol == gatewayv1.TLSProtocolType && len(listenerSupportedRouteKinds(r.Config, listener)) > 0 &&
			!tcpListenerProtocolConflict(r.Config, listeners, listener) && (r.Config.TCPTLSMixedMode || !mixedTLSModes(listeners, listener)) {
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
	return status.ControllerName == r.Config.ControllerName && parentRefTargetsGateway(route.Namespace, status.ParentRef, gateway)
}

func (r *Reconciler) removeOwnedSNIParents(route *gatewayv1.TLSRoute, gateway *gatewayv1.Gateway) {
	route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
		return r.ownsSNIParent(route, gateway, status)
	})
}

// updateSNIRouteParents preserves the order of existing status entries so
// independently reconciling Gateways do not move their parents back and forth.
func (r *Reconciler) updateSNIRouteParents(route *gatewayv1.TLSRoute, gateway *gatewayv1.Gateway, update func(*gatewayv1.RouteParentStatus) bool) {
	refs := model.EffectiveParentRefs(route.Spec.CommonRouteSpec, route.Namespace, gateway, r.Config.DefaultGatewaysEnabled())
	matches := func(ref gatewayv1.ParentReference, status gatewayv1.RouteParentStatus) bool {
		return controller.ParentRefsEqual(ref, route.Namespace, status.ParentRef, route.Namespace)
	}
	route.Status.Parents = slices.DeleteFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
		return r.ownsSNIParent(route, gateway, status) && !slices.ContainsFunc(refs, func(ref gatewayv1.ParentReference) bool {
			return matches(ref, status)
		})
	})
	for _, ref := range refs {
		if !parentRefTargetsGateway(route.Namespace, ref, gateway) {
			continue
		}
		index := slices.IndexFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
			return r.ownsSNIParent(route, gateway, status) && matches(ref, status)
		})
		if index < 0 && len(route.Status.Parents) >= controller.RouteParentStatusLimit {
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
