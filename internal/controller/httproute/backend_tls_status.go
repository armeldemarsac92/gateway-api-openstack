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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const maxBackendTLSAncestors = 16

type backendTLSPolicyStatusReconciler struct {
	routes *Reconciler
}

type backendTLSPolicyConditions struct {
	accepted, resolvedRefs metav1.Condition
}

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=backendtlspolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=backendtlspolicies/status,verbs=get;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
func (r *backendTLSPolicyStatusReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	policy := &gatewayv1.BackendTLSPolicy{}
	if err := r.routes.APIReader.Get(ctx, request.NamespacedName, policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var ancestors map[types.NamespacedName]backendTLSPolicyConditions
	if policy.DeletionTimestamp.IsZero() {
		var err error
		ancestors, err = r.observeAncestors(ctx, policy)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	result := ctrl.Result{}
	if len(ancestors) > 0 {
		result.RequeueAfter = controller.OpenStackResyncAfter(r.routes.Config.OpenStackResyncInterval, policy.UID)
	}
	base := policy.DeepCopy()
	policy.Status.Ancestors = mergeBackendTLSAncestors(policy, r.routes.Config.ControllerName, ancestors)
	if equality.Semantic.DeepEqual(base.Status, policy.Status) {
		return result, nil
	}
	return result, client.IgnoreNotFound(r.routes.Status().Patch(ctx, policy, controller.OptimisticMergeFrom(base)))
}

func (r *backendTLSPolicyStatusReconciler) observeAncestors(ctx context.Context, policy *gatewayv1.BackendTLSPolicy) (map[types.NamespacedName]backendTLSPolicyConditions, error) {
	targets := backendtls.ServiceKeys(policy)
	candidates := map[types.NamespacedName]struct{}{}
	for _, target := range targets {
		consumers, err := r.routes.serviceConsumers(ctx, target)
		if err != nil {
			return nil, err
		}
		for i := range consumers {
			candidates[client.ObjectKeyFromObject(&consumers[i])] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	gateways := map[types.NamespacedName]*gatewayv1.Gateway{}
	for key := range candidates {
		var route gatewayv1.HTTPRoute
		if err := r.routes.APIReader.Get(ctx, key, &route); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		services := routeBackendTLSServices(&route)
		if !route.DeletionTimestamp.IsZero() || !slices.ContainsFunc(targets, func(key types.NamespacedName) bool {
			_, used := services[key]
			return used
		}) {
			continue
		}
		parents, err := r.routes.managedParentsWithReader(ctx, r.routes.APIReader, &route)
		if err != nil {
			return nil, err
		}
		if len(parents) == 1 && managedParentsSupportInstalledVersion(parents) {
			gateway := parents[0].gateway
			gateways[client.ObjectKeyFromObject(gateway)] = gateway
		}
	}
	ancestors := make(map[types.NamespacedName]backendTLSPolicyConditions, len(gateways))
	availability := backendTLSUnknown
	checked := false
	for key, gateway := range gateways {
		selection, supportedProtocol, err := r.observeGatewayConsumer(ctx, policy, gateway, targets)
		if err != nil {
			return nil, err
		}
		if selection == nil {
			continue
		}
		if selection.Accepted.Status == metav1.ConditionTrue && supportedProtocol && !checked {
			availability, checked = r.routes.backendTLSAvailability(ctx), true
		}
		ancestors[key] = backendTLSPolicyConditions{
			accepted: availability.apply(selection.Accepted, supportedProtocol), resolvedRefs: selection.ResolvedRefs,
		}
	}
	return ancestors, nil
}

func (r *backendTLSPolicyStatusReconciler) observeGatewayConsumer(ctx context.Context, policy *gatewayv1.BackendTLSPolicy, gateway *gatewayv1.Gateway, targets []types.NamespacedName) (*backendtls.PolicyResult, bool, error) {
	peers, err := r.routes.liveGatewayRoutePeers(ctx, gateway)
	if err != nil {
		return nil, false, err
	}
	admission, err := r.routes.observeRouteGraphAdmission(ctx, gateway, peers, nil)
	if err != nil {
		return nil, false, err
	}
	input := model.Input{BackendH2C: r.routes.Config.BackendH2C}
	consumers := map[types.NamespacedName]map[string]struct{}{}
	grantsRequired := false
	for i := range peers {
		route := &peers[i]
		if _, eligible := admission.eligible[routeKey(route)]; !eligible || model.HTTPProfileRouteKind(route) != model.HTTPRouteKind {
			continue
		}
		services := routeBackendTLSServices(route)
		usesTarget := false
		for _, key := range targets {
			if _, used := services[key]; !used {
				continue
			}
			if consumers[key] == nil {
				consumers[key] = map[string]struct{}{}
			}
			consumers[key][route.Namespace] = struct{}{}
			usesTarget = true
			grantsRequired = grantsRequired || route.Namespace != key.Namespace
		}
		if usesTarget {
			input.Routes = append(input.Routes, *route)
		}
	}
	if grantsRequired {
		input.ReferenceGrants, err = graph.ListBackendReferenceGrants(ctx, r.routes.APIReader, policy.Namespace)
		if err != nil {
			return nil, false, err
		}
	}
	for key, namespaces := range consumers {
		authorized := false
		for namespace := range namespaces {
			if model.BackendReferenceAllowed(namespace, key, input.ReferenceGrants) {
				authorized = true
				break
			}
		}
		if !authorized {
			continue
		}
		var service corev1.Service
		if err := r.routes.APIReader.Get(ctx, key, &service); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, false, err
			}
			continue
		}
		input.Services = append(input.Services, service)
	}
	if err := graph.CollectBackendTLSInputs(ctx, r.routes.APIReader, &input); err != nil {
		return nil, false, err
	}
	var selected *backendtls.PolicyResult
	supportedProtocol := true
	for i := range input.Routes {
		route := &input.Routes[i]
		for _, rule := range route.Spec.Rules {
			for _, backend := range model.ResolveBackends(route, rule, input) {
				if backend.Weight == 0 {
					continue
				}
				for _, result := range backend.BackendTLS.Policies {
					if result.UID != policy.UID || result.Generation != policy.Generation {
						continue
					}
					selected = &result
					supportedProtocol = supportedProtocol && supportsBackendTLS(backend, true)
				}
			}
		}
	}
	return selected, supportedProtocol, nil
}

// liveGatewayRoutePeers refreshes candidate routes before evaluating attachment.
// The indexed discovery only nominates candidates for policy status reporting.
func (r *Reconciler) liveGatewayRoutePeers(ctx context.Context, gateway *gatewayv1.Gateway) ([]gatewayv1.HTTPRoute, error) {
	gatewayKey := controller.ObjectKeyString(client.ObjectKeyFromObject(gateway))
	filter := client.MatchingFields{controller.IndexHTTPRouteByParentGateway: gatewayKey}
	var candidates []gatewayv1.HTTPRoute
	for _, kind := range []gatewayv1.Kind{model.HTTPRouteKind, model.GRPCRouteKind} {
		routes := r.forRouteKind(kind)
		peers, err := routes.listRoutes(ctx, filter)
		if err != nil {
			return nil, err
		}
		for i := range peers {
			var live gatewayv1.HTTPRoute
			if err := routes.readRoute(ctx, routes.APIReader, client.ObjectKeyFromObject(&peers[i]), &live); err != nil {
				if err := client.IgnoreNotFound(err); err != nil {
					return nil, err
				}
				continue
			}
			candidates = append(candidates, live)
		}
	}
	return candidates, nil
}

func mergeBackendTLSAncestors(policy *gatewayv1.BackendTLSPolicy, name gatewayv1.GatewayController, desired map[types.NamespacedName]backendTLSPolicyConditions) []gatewayv1.PolicyAncestorStatus {
	var merged []gatewayv1.PolicyAncestorStatus
	remaining := maps.Clone(desired)
	for _, entry := range policy.Status.Ancestors {
		if entry.ControllerName != name {
			merged = append(merged, entry)
			continue
		}
		if !controller.IsGatewayParentRef(entry.AncestorRef) {
			continue
		}
		key := types.NamespacedName{
			Namespace: string(ptr.Deref(entry.AncestorRef.Namespace, gatewayv1.Namespace(policy.Namespace))),
			Name:      string(entry.AncestorRef.Name),
		}
		if conditions, keep := remaining[key]; keep {
			entry.Conditions = slices.Clone(entry.Conditions)
			setBackendTLSConditions(&entry, conditions)
			entry.AncestorRef = gatewayAncestorRef(key)
			merged = append(merged, entry)
			delete(remaining, key)
		}
	}
	for _, key := range slices.SortedFunc(maps.Keys(remaining), func(a, b types.NamespacedName) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}) {
		if len(merged) == maxBackendTLSAncestors {
			break
		}
		entry := gatewayv1.PolicyAncestorStatus{ControllerName: name, AncestorRef: gatewayAncestorRef(key)}
		setBackendTLSConditions(&entry, remaining[key])
		merged = append(merged, entry)
	}
	return merged
}

func setBackendTLSConditions(entry *gatewayv1.PolicyAncestorStatus, conditions backendTLSPolicyConditions) {
	for _, condition := range []metav1.Condition{conditions.accepted, conditions.resolvedRefs} {
		current := meta.FindStatusCondition(entry.Conditions, condition.Type)
		if current == nil || current.ObservedGeneration <= condition.ObservedGeneration {
			meta.SetStatusCondition(&entry.Conditions, condition)
		}
	}
}
