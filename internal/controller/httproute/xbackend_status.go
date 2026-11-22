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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const maxXBackendAncestors = 32

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
	backend.Status.Ancestors = mergeXBackendAncestors(backend, r.routes.Config.ControllerName, ancestors)
	if equality.Semantic.DeepEqual(base.Status, backend.Status) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, client.IgnoreNotFound(r.routes.Status().Patch(ctx, backend, controller.OptimisticMergeFrom(base)))
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
		if parentsShareGateway(parents) && managedParentsSupportInstalledVersion(parents) {
			gateway := parents[0].gateway
			byGateway[client.ObjectKeyFromObject(gateway)] = gateway
		}
	}
	ancestors := make(map[types.NamespacedName]metav1.Condition, len(byGateway))
	for gatewayKey, gateway := range byGateway {
		attached, err := r.hasAttachedConsumer(ctx, gateway, key)
		if err != nil {
			return nil, err
		}
		if !attached {
			continue
		}
		mode, err := r.routes.xBackendMemberMode(gateway)
		if err != nil {
			return nil, err
		}
		ancestors[gatewayKey] = xBackendAcceptedCondition(backend, mode, r.routes.Config.BackendH2C)
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

func xBackendAcceptedCondition(backend *gatewayx.XBackend, mode cloud.MemberMode, backendH2C bool) metav1.Condition {
	condition := metav1.Condition{
		Type: string(gatewayx.BackendConditionAccepted), Status: metav1.ConditionTrue, Reason: string(gatewayx.BackendReasonAccepted),
		Message: "XBackend is accepted by the Gateway", ObservedGeneration: backend.Generation,
	}
	pending := mode == ""
	if pending {
		mode = cloud.MemberModeEndpoint
	}
	if diagnostic := model.ValidateXBackend(backend.Spec, mode, backendH2C); diagnostic != nil {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message
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
