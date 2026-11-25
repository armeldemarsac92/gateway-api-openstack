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
	"errors"
	"maps"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type backendRetryBudgetStatusReconciler struct {
	routes *Reconciler
}

// retryPolicyAncestor lets the controller report a rejected target even when
// there is no usable Gateway on which to publish a condition.
type retryPolicyAncestor struct {
	group, kind, namespace, name string
}

func (a retryPolicyAncestor) reference() gatewayv1.ParentReference {
	return gatewayv1.ParentReference{
		Group: ptr.To(gatewayv1.Group(a.group)), Kind: ptr.To(gatewayv1.Kind(a.kind)),
		Namespace: ptr.To(gatewayv1.Namespace(a.namespace)), Name: gatewayv1.ObjectName(a.name),
	}
}

func (r *backendRetryBudgetStatusReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	var policy gatewayx.XBackendTrafficPolicy
	if err := r.routes.APIReader.Get(ctx, request.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !policy.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	status, err := r.observeRetryPolicy(ctx, &policy)
	if err != nil {
		return ctrl.Result{}, err
	}
	result := ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.routes.Config.OpenStackResyncInterval, policy.UID)}
	base := policy.DeepCopy()
	policy.Status.Ancestors = mergeRetryPolicyAncestors(&policy, r.routes.Config.ControllerName, status)
	if equality.Semantic.DeepEqual(base.Status, policy.Status) {
		return result, nil
	}
	return result, client.IgnoreNotFound(r.routes.Status().Patch(ctx, &policy, controller.OptimisticMergeFrom(base)))
}

func (r *backendRetryBudgetStatusReconciler) observeRetryPolicy(ctx context.Context, policy *gatewayx.XBackendTrafficPolicy) (map[retryPolicyAncestor]metav1.Condition, error) {
	var policies gatewayx.XBackendTrafficPolicyList
	if err := r.routes.APIReader.List(ctx, &policies, client.InNamespace(policy.Namespace)); err != nil {
		return nil, err
	}
	if policies.Continue != "" {
		return nil, errors.New("incomplete Service retry policy list")
	}
	availability := sync.OnceValues(func() (bool, error) {
		if !r.routes.Config.BackendRetryBudgetEnabled() || r.routes.BackendRetryBudgetAvailable == nil {
			return false, nil
		}
		return r.routes.BackendRetryBudgetAvailable(ctx)
	})
	targets := make(map[types.NamespacedName]metav1.Condition)
	status := make(map[retryPolicyAncestor]metav1.Condition)
	for _, ref := range policy.Spec.TargetRefs {
		ancestor := retryPolicyAncestor{group: string(ref.Group), kind: string(ref.Kind), namespace: policy.Namespace, name: string(ref.Name)}
		if ref.Group != "" || ref.Kind != "Service" {
			status[ancestor] = retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "This controller supports retry constraints for core Services")
			continue
		}
		key := types.NamespacedName{Namespace: policy.Namespace, Name: string(ref.Name)}
		condition, err := r.observeRetryPolicyTarget(ctx, policy, policies.Items, key, availability)
		if err != nil {
			return nil, err
		}
		targets[key] = condition
	}
	if len(targets) == 0 {
		return status, nil
	}
	gateways, err := r.retryPolicyGateways(ctx, targets)
	if err != nil {
		return nil, err
	}
	used := make(map[types.NamespacedName]bool)
	for _, gateway := range gateways {
		consumers, err := r.retryPolicyConsumers(ctx, gateway, targets)
		if err != nil {
			return nil, err
		}
		if len(consumers) == 0 {
			continue
		}
		conditions := make([]metav1.Condition, 0, len(consumers))
		for key := range consumers {
			used[key] = true
			conditions = append(conditions, targets[key])
		}
		key := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: gateway.Namespace, name: gateway.Name}
		status[key] = retryPolicyGatewayCondition(policy, conditions)
	}
	for key, condition := range targets {
		// Missing targets have no resolved consumer. Keep their direct status
		// even when other targets participate in a Gateway.
		if !used[key] && condition.Status != metav1.ConditionTrue {
			status[retryPolicyAncestor{kind: "Service", namespace: key.Namespace, name: key.Name}] = condition
		}
	}
	return status, nil
}

func (r *backendRetryBudgetStatusReconciler) observeRetryPolicyTarget(ctx context.Context, policy *gatewayx.XBackendTrafficPolicy, peers []gatewayx.XBackendTrafficPolicy, key types.NamespacedName, availability func() (bool, error)) (metav1.Condition, error) {
	var service corev1.Service
	err := r.routes.APIReader.Get(ctx, key, &service)
	switch {
	case apierrors.IsNotFound(err) || err == nil && !service.DeletionTimestamp.IsZero():
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonTargetNotFound), "The target Service does not exist"), nil
	case err != nil:
		return metav1.Condition{}, err
	}
	winner := retryBudgetPolicyForService(peers, key.Name)
	if winner == nil || winner.UID != policy.UID || winner.Generation != policy.Generation {
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonConflicted), "The target Service has an older or lexically prior XBackendTrafficPolicy"), nil
	}
	if _, valid := serviceRetryBudget(policy.Spec.RetryConstraint, service.UID); !valid {
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "The Service retry constraint is outside Octavia's supported range"), nil
	}
	// Report the deprecated field explicitly while it remains in the v1.7 API.
	legacyPersistence := policy.Spec.SessionPersistence != nil //nolint:staticcheck // The deprecated field is still accepted by the API.
	if policy.Spec.RetryConstraint == nil && legacyPersistence {
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "This controller does not support sessionPersistence on XBackendTrafficPolicy"), nil
	}
	if policy.Spec.RetryConstraint != nil {
		available, err := availability()
		if err != nil {
			return retryPolicyCondition(policy, metav1.ConditionUnknown, "Pending", "Waiting for Octavia retry budget capability discovery"), nil
		}
		if !available {
			return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "Service retry budgets require controller opt-in and OpenStack Amphora support"), nil
		}
	}
	message := "The Service policy is accepted for HTTP-family backends"
	if legacyPersistence {
		message += ". Session persistence is unsupported"
	}
	return retryPolicyCondition(policy, metav1.ConditionTrue, string(gatewayv1.PolicyReasonAccepted), message), nil
}

func retryPolicyCondition(policy *gatewayx.XBackendTrafficPolicy, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: string(gatewayv1.PolicyConditionAccepted), Status: status, Reason: reason, Message: message, ObservedGeneration: policy.Generation}
}

func retryPolicyGatewayCondition(policy *gatewayx.XBackendTrafficPolicy, conditions []metav1.Condition) metav1.Condition {
	slices.SortFunc(conditions, func(a, b metav1.Condition) int {
		return cmp.Or(cmp.Compare(a.Status, b.Status), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Message, b.Message))
	})
	first, last := conditions[0], conditions[len(conditions)-1]
	hasAccepted := slices.ContainsFunc(conditions, func(condition metav1.Condition) bool { return condition.Status == metav1.ConditionTrue })
	if hasAccepted && (first.Status != metav1.ConditionTrue || last.Status != metav1.ConditionTrue) {
		return retryPolicyCondition(policy, metav1.ConditionTrue, string(gatewayv1.PolicyReasonAccepted), "Some Service targets are accepted for HTTP-family backends")
	}
	return last
}

func (r *backendRetryBudgetStatusReconciler) retryPolicyGateways(ctx context.Context, targets map[types.NamespacedName]metav1.Condition) (map[types.NamespacedName]*gatewayv1.Gateway, error) {
	result := make(map[types.NamespacedName]*gatewayv1.Gateway)
	for _, kind := range []gatewayv1.Kind{model.HTTPRouteKind, model.GRPCRouteKind} {
		routes := r.routes.forRouteKind(kind)
		items, err := routes.listLiveRoutes(ctx)
		if err != nil {
			return nil, err
		}
		for i := range items {
			route := &items[i]
			if !route.DeletionTimestamp.IsZero() || !routeUsesRetryPolicyTarget(route, targets) {
				continue
			}
			parents, err := routes.managedParentsWithReader(ctx, routes.APIReader, route)
			if err != nil {
				return nil, err
			}
			for _, group := range groupManagedRouteParents(parents) {
				if managedParentsSupportInstalledVersion(group.parents) {
					result[client.ObjectKeyFromObject(group.gateway)] = group.gateway
				}
			}
		}
	}
	return result, nil
}

func routeUsesRetryPolicyTarget(route *gatewayv1.HTTPRoute, targets map[types.NamespacedName]metav1.Condition) bool {
	for key := range routeBackendTLSServices(route) {
		if _, ok := targets[key]; ok {
			return true
		}
	}
	return false
}

func (r *backendRetryBudgetStatusReconciler) retryPolicyConsumers(ctx context.Context, gateway *gatewayv1.Gateway, targets map[types.NamespacedName]metav1.Condition) (map[types.NamespacedName]bool, error) {
	routes, err := r.routes.liveGraphRoutes(ctx, controller.GatewayIdentity(r.routes.Config, gateway))
	if err != nil {
		return nil, err
	}
	admission, err := r.routes.observeRouteGraphAdmission(ctx, gateway, routes, nil)
	if err != nil {
		return nil, err
	}
	var candidates []gatewayv1.HTTPRoute
	for _, route := range routes {
		if _, allowed := admission.eligible[routeKey(&route)]; allowed && routeUsesRetryPolicyTarget(&route, targets) {
			candidates = append(candidates, route)
		}
	}
	input, err := graph.CollectBackendInputsWithNamespaces(ctx, r.routes.APIReader, gateway, candidates, admission.namespaces)
	if err != nil {
		return nil, err
	}
	input.BackendH2C = r.routes.Config.BackendH2C
	consumers := make(map[types.NamespacedName]bool)
	for i := range candidates {
		route := &candidates[i]
		for _, rule := range route.Spec.Rules {
			for _, backend := range model.ResolveBackendsAndMirrors(route, rule, input) {
				if _, targeted := targets[backend.Service]; targeted && backend.Diagnostic == nil && backend.Weight > 0 {
					consumers[backend.Service] = true
				}
			}
		}
	}
	return consumers, nil
}

func mergeRetryPolicyAncestors(policy *gatewayx.XBackendTrafficPolicy, name gatewayv1.GatewayController, desired map[retryPolicyAncestor]metav1.Condition) []gatewayv1.PolicyAncestorStatus {
	var result []gatewayv1.PolicyAncestorStatus
	remaining := maps.Clone(desired)
	for _, entry := range policy.Status.Ancestors {
		if entry.ControllerName != name {
			result = append(result, entry)
			continue
		}
		ref := entry.AncestorRef
		key := retryPolicyAncestor{
			group: string(ptr.Deref(ref.Group, gatewayv1.Group(gatewayv1.GroupName))), kind: string(ptr.Deref(ref.Kind, "Gateway")),
			namespace: string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(policy.Namespace))), name: string(ref.Name),
		}
		if condition, keep := remaining[key]; keep {
			entry.AncestorRef = key.reference()
			entry.Conditions = slices.Clone(entry.Conditions)
			meta.SetStatusCondition(&entry.Conditions, condition)
			result = append(result, entry)
			delete(remaining, key)
		}
	}
	for _, key := range slices.SortedFunc(maps.Keys(remaining), func(a, b retryPolicyAncestor) int {
		return cmp.Or(cmp.Compare(a.kind, b.kind), cmp.Compare(a.namespace, b.namespace), cmp.Compare(a.name, b.name), cmp.Compare(a.group, b.group))
	}) {
		if len(result) >= backendtls.MaxAncestors {
			break
		}
		entry := gatewayv1.PolicyAncestorStatus{ControllerName: name, AncestorRef: key.reference()}
		meta.SetStatusCondition(&entry.Conditions, remaining[key])
		result = append(result, entry)
	}
	return result
}
