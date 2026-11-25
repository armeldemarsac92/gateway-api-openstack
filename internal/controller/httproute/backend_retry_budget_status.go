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

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
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
	persistence := backendPolicyPersistence(policy)
	persistenceCapability := r.routes.xBackendPersistenceCapability(ctx)
	availability := sync.OnceValues(func() (bool, error) {
		if !r.routes.Config.BackendRetryBudgetEnabled() || r.routes.BackendRetryBudgetAvailable == nil {
			return false, nil
		}
		return r.routes.BackendRetryBudgetAvailable(ctx)
	})
	targets := make(map[types.NamespacedName]retryPolicyTarget)
	status := make(map[retryPolicyAncestor]metav1.Condition)
	for _, ref := range policy.Spec.TargetRefs {
		ancestor := retryPolicyAncestor{group: string(ref.Group), kind: string(ref.Kind), namespace: policy.Namespace, name: string(ref.Name)}
		if ref.Group != "" || ref.Kind != "Service" {
			status[ancestor] = retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "This controller supports backend traffic policies for core Services")
			continue
		}
		key := types.NamespacedName{Namespace: policy.Namespace, Name: string(ref.Name)}
		condition, err := r.observeRetryPolicyTarget(ctx, policy, policies.Items, key, availability, persistenceCapability)
		if err != nil {
			return nil, err
		}
		targets[key] = condition
	}
	if len(targets) == 0 {
		return status, nil
	}
	routes, err := r.routes.listLiveHTTPFamilyRoutes(ctx)
	if err != nil {
		return nil, err
	}
	gateways, err := r.retryPolicyGateways(ctx, routes, targets)
	if err != nil {
		return nil, err
	}
	used := make(map[types.NamespacedName]bool)
	setParents := make(map[types.NamespacedName]types.NamespacedName)
	for _, gateway := range gateways {
		peers, err := r.routes.filterGatewayGraphRoutes(ctx, routes, controller.GatewayIdentity(r.routes.Config, gateway), setParents)
		if err != nil {
			return nil, err
		}
		consumers, err := r.retryPolicyConsumers(ctx, gateway, peers, targets)
		if err != nil {
			return nil, err
		}
		if len(consumers) == 0 {
			continue
		}
		conditions := make([]metav1.Condition, 0, len(consumers))
		for key, consumer := range consumers {
			used[key] = true
			target := targets[key]
			if target.persistence.Status == metav1.ConditionTrue {
				target.persistence = r.servicePersistenceConsumerCondition(policy, target.persistence, gateway, consumer, persistence)
			}
			conditions = append(conditions, target.condition(policy))
		}
		key := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: gateway.Namespace, name: gateway.Name}
		status[key] = retryPolicyGatewayCondition(policy, conditions)
	}
	for key, target := range targets {
		condition := target.condition(policy)
		// Missing targets have no resolved consumer. Keep their direct status
		// even when other targets participate in a Gateway.
		if !used[key] && condition.Status != metav1.ConditionTrue {
			status[retryPolicyAncestor{kind: "Service", namespace: key.Namespace, name: key.Name}] = condition
		}
	}
	return status, nil
}

type retryPolicyTarget struct {
	rejected    metav1.Condition
	retry       metav1.Condition
	persistence metav1.Condition
}

func (target retryPolicyTarget) condition(policy *gatewayx.XBackendTrafficPolicy) metav1.Condition {
	if target.rejected.Status != "" {
		return target.rejected
	}
	retry, persistence := target.retry, target.persistence
	switch {
	case retry.Status == "" && persistence.Status == "":
		return retryPolicyCondition(policy, metav1.ConditionTrue, string(gatewayv1.PolicyReasonAccepted), "The Service policy is accepted for HTTP-family backends")
	case retry.Status == "":
		return persistence
	case persistence.Status == "":
		return retry
	case retry.Status == metav1.ConditionTrue || persistence.Status == metav1.ConditionTrue:
		return retryPolicyCondition(policy, metav1.ConditionTrue, string(gatewayv1.PolicyReasonAccepted), retry.Message+". "+persistence.Message)
	case retry.Status == metav1.ConditionUnknown || persistence.Status == metav1.ConditionUnknown:
		return retryPolicyCondition(policy, metav1.ConditionUnknown, "Pending", retry.Message+". "+persistence.Message)
	default:
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), retry.Message+". "+persistence.Message)
	}
}

func (r *backendRetryBudgetStatusReconciler) observeRetryPolicyTarget(ctx context.Context, policy *gatewayx.XBackendTrafficPolicy, peers []gatewayx.XBackendTrafficPolicy, key types.NamespacedName, availability func() (bool, error), persistenceCapability func(gatewayv1.SessionPersistence) cloud.SessionPersistenceIssue) (retryPolicyTarget, error) {
	var service corev1.Service
	var target retryPolicyTarget
	err := r.routes.APIReader.Get(ctx, key, &service)
	switch {
	case apierrors.IsNotFound(err) || err == nil && !service.DeletionTimestamp.IsZero():
		target.rejected = retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonTargetNotFound), "The target Service does not exist")
		return target, nil
	case err != nil:
		return target, err
	}
	winner := retryBudgetPolicyForService(peers, key.Name)
	if winner == nil || winner.UID != policy.UID || winner.Generation != policy.Generation {
		target.rejected = retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonConflicted), "The target Service has an older or lexically prior XBackendTrafficPolicy")
		return target, nil
	}
	if policy.Spec.RetryConstraint != nil {
		target.retry = serviceRetryBudgetCondition(policy, service.UID, availability)
	}
	if persistence := backendPolicyPersistence(policy); persistence != nil {
		target.persistence = r.servicePersistenceCondition(policy, service.UID, *persistence, persistenceCapability)
	}
	return target, nil
}

func serviceRetryBudgetCondition(policy *gatewayx.XBackendTrafficPolicy, uid types.UID, availability func() (bool, error)) metav1.Condition {
	if _, valid := serviceRetryBudget(policy.Spec.RetryConstraint, uid); !valid {
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "The Service retry constraint is outside Octavia's supported range")
	}
	available, err := availability()
	switch {
	case err != nil:
		return retryPolicyCondition(policy, metav1.ConditionUnknown, "Pending", "Waiting for Octavia retry budget capability discovery")
	case !available:
		return retryPolicyCondition(policy, metav1.ConditionFalse, string(gatewayv1.PolicyReasonInvalid), "Service retry budgets require controller opt-in and OpenStack Amphora support")
	default:
		return retryPolicyCondition(policy, metav1.ConditionTrue, string(gatewayv1.PolicyReasonAccepted), "The Service retry constraint is accepted")
	}
}

func (r *backendRetryBudgetStatusReconciler) servicePersistenceCondition(policy *gatewayx.XBackendTrafficPolicy, uid types.UID, spec gatewayv1.SessionPersistence, capability func(gatewayv1.SessionPersistence) cloud.SessionPersistenceIssue) metav1.Condition {
	const invalid = string(gatewayv1.PolicyReasonInvalid)
	if !r.routes.Config.XBackendSessionPersistenceEnabled() {
		return retryPolicyCondition(policy, metav1.ConditionFalse, invalid, "Service session persistence requires --octavia-xbackend-session-persistence")
	}
	if _, diagnostic := model.ServiceSessionPersistence(spec, uid); diagnostic != nil {
		return retryPolicyCondition(policy, metav1.ConditionFalse, invalid, diagnostic.Message)
	}
	status, _, explanation := sessionPersistenceCondition("Service", metav1.ConditionTrue, "", "", capability(spec))
	switch status {
	case metav1.ConditionFalse:
		return retryPolicyCondition(policy, status, invalid, explanation)
	case metav1.ConditionUnknown:
		return retryPolicyCondition(policy, status, "Pending", explanation)
	default:
		return retryPolicyCondition(policy, status, string(gatewayv1.PolicyReasonAccepted), "Service session persistence is accepted")
	}
}

type retryPolicyConsumer struct {
	httpPrimary bool
	grpcPrimary bool
}

func (r *backendRetryBudgetStatusReconciler) servicePersistenceConsumerCondition(policy *gatewayx.XBackendTrafficPolicy, accepted metav1.Condition, gateway *gatewayv1.Gateway, consumer retryPolicyConsumer, persistence *gatewayv1.SessionPersistence) metav1.Condition {
	const invalid = string(gatewayv1.PolicyReasonInvalid)
	if !consumer.httpPrimary && !consumer.grpcPrimary {
		return retryPolicyCondition(policy, metav1.ConditionFalse, invalid, "Service session persistence requires a primary forwarding backend")
	}
	if !controller.HasGatewayTopologyBinding(r.routes.Config, gateway) {
		return retryPolicyCondition(policy, metav1.ConditionUnknown, "Pending", "Waiting for the Gateway member topology")
	}
	topology, err := controller.StoredGatewayTopology(r.routes.Config, gateway)
	if err != nil {
		return retryPolicyCondition(policy, metav1.ConditionUnknown, "Pending", "Waiting for the Gateway member topology")
	}
	if topology.Topology.MemberMode != cloud.MemberModeEndpoint {
		return retryPolicyCondition(policy, metav1.ConditionFalse, invalid, "Service session persistence requires Endpoint member mode")
	}
	if consumer.grpcPrimary && model.IsHeaderSessionPersistence(*persistence) {
		header := cloud.HTTPSessionHeaderPersistence{Name: string(persistence.Header.Name)}
		if !header.ValidForGRPC() {
			const detail = "GRPCRoute session persistence requires a custom ASCII metadata key without the -bin suffix"
			if !consumer.httpPrimary {
				return retryPolicyCondition(policy, metav1.ConditionFalse, invalid, detail)
			}
			accepted.Message = "Service session persistence is accepted for HTTPRoute consumers. " + detail
		}
	}
	return accepted
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

func (r *backendRetryBudgetStatusReconciler) retryPolicyGateways(ctx context.Context, candidates []gatewayv1.HTTPRoute, targets map[types.NamespacedName]retryPolicyTarget) (map[types.NamespacedName]*gatewayv1.Gateway, error) {
	result := make(map[types.NamespacedName]*gatewayv1.Gateway)
	for i := range candidates {
		route := &candidates[i]
		if !route.DeletionTimestamp.IsZero() || !routeUsesRetryPolicyTarget(route, targets) {
			continue
		}
		routes := r.routes.forRouteKind(model.HTTPProfileRouteKind(route))
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
	return result, nil
}

func routeUsesRetryPolicyTarget(route *gatewayv1.HTTPRoute, targets map[types.NamespacedName]retryPolicyTarget) bool {
	for key := range routeBackendTLSServices(route) {
		if _, ok := targets[key]; ok {
			return true
		}
	}
	return false
}

func (r *backendRetryBudgetStatusReconciler) retryPolicyConsumers(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, targets map[types.NamespacedName]retryPolicyTarget) (map[types.NamespacedName]retryPolicyConsumer, error) {
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
	if len(candidates) == 0 {
		return nil, nil
	}
	inputs, err := r.routes.collectRouteForwardingInputs(ctx, r.routes.APIReader, gateway, candidates, admission.namespaces, admission.listenerSets)
	if err != nil {
		return nil, err
	}
	inputs.admission = admission
	consumers := make(map[types.NamespacedName]retryPolicyConsumer)
	for i := range candidates {
		route := &candidates[i]
		matches, rules, err := r.routes.lowerableRouteRules(route, inputs, admission.eligible[routeKey(route)].listeners)
		if err != nil {
			var rejected *routeBuildError
			if errors.As(err, &rejected) {
				continue
			}
			return nil, err
		}
		for backend := range activeRetryBudgetBackends(matches, rules, retryBudgetTransportSelectable) {
			if _, targeted := targets[backend.Service]; targeted {
				if _, present := consumers[backend.Service]; !present {
					consumers[backend.Service] = retryPolicyConsumer{}
				}
			}
		}
		for backend := range activeServiceBackends(matches, rules, retryBudgetTransportSelectable, false) {
			if _, targeted := targets[backend.Service]; targeted {
				consumer := consumers[backend.Service]
				if model.HTTPProfileRouteKind(route) == model.GRPCRouteKind {
					consumer.grpcPrimary = true
				} else {
					consumer.httpPrimary = true
				}
				consumers[backend.Service] = consumer
			}
		}
	}
	return consumers, nil
}

func mergeRetryPolicyAncestors(policy *gatewayx.XBackendTrafficPolicy, name gatewayv1.GatewayController, desired map[retryPolicyAncestor]metav1.Condition) []gatewayv1.PolicyAncestorStatus {
	result := make([]gatewayv1.PolicyAncestorStatus, 0, len(policy.Status.Ancestors))
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
			if current := meta.FindStatusCondition(entry.Conditions, condition.Type); current == nil || current.ObservedGeneration <= condition.ObservedGeneration {
				meta.SetStatusCondition(&entry.Conditions, condition)
			}
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
