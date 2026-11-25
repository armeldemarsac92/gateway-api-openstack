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
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) discoverBackendRetryPolicy(mapper meta.RESTMapper) error {
	if !r.Config.XBackendEnabled() {
		return nil
	}
	kind := schema.GroupKind{Group: gatewayx.GroupVersion.Group, Kind: "XBackendTrafficPolicy"}
	_, err := mapper.RESTMapping(kind, gatewayx.GroupVersion.Version)
	if meta.IsNoMatchError(err) && !r.Config.BackendRetryBudgetEnabled() {
		return nil
	}
	if err != nil {
		return fmt.Errorf("discover XBackendTrafficPolicy: %w", err)
	}
	r.retryBudgetPolicyServed = true
	return nil
}

func (r *Reconciler) enqueueRoutesForRetryBudget(ctx context.Context, object client.Object) []reconcile.Request {
	policy := object.(*gatewayx.XBackendTrafficPolicy)
	keys := make(map[types.NamespacedName]struct{})
	for _, ref := range policy.Spec.TargetRefs {
		if ref.Group != "" || ref.Kind != "Service" {
			continue
		}
		for _, request := range r.enqueueHTTPRoutesForServiceKey(ctx, types.NamespacedName{Namespace: policy.Namespace, Name: string(ref.Name)}) {
			keys[request.NamespacedName] = struct{}{}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) retryBudgetGatewayAncestors(policy *gatewayx.XBackendTrafficPolicy) map[types.NamespacedName]bool {
	keys := make(map[types.NamespacedName]bool)
	for _, entry := range policy.Status.Ancestors {
		if key := backendtls.GatewayAncestorKey(entry.AncestorRef, policy.Namespace); key.Name != "" && entry.ControllerName == r.Config.ControllerName {
			keys[key] = true
		}
	}
	return keys
}

func (r *Reconciler) retryBudgetRoutePredicate() predicate.Predicate {
	return predicate.Or(controller.GenerationOrDeletionChangedPredicate(), predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before := e.ObjectOld.(*gatewayx.XBackendTrafficPolicy)
		after := e.ObjectNew.(*gatewayx.XBackendTrafficPolicy)
		return (len(before.Status.Ancestors) >= backendtls.MaxAncestors) != (len(after.Status.Ancestors) >= backendtls.MaxAncestors) ||
			!maps.Equal(r.retryBudgetGatewayAncestors(before), r.retryBudgetGatewayAncestors(after))
	}})
}

func (r *Reconciler) retryBudgetStatusPredicate() predicate.Predicate {
	return predicate.Or(controller.GenerationOrDeletionChangedPredicate(), predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before := e.ObjectOld.(*gatewayx.XBackendTrafficPolicy)
		after := e.ObjectNew.(*gatewayx.XBackendTrafficPolicy)
		if len(before.Status.Ancestors) >= backendtls.MaxAncestors && len(after.Status.Ancestors) < backendtls.MaxAncestors {
			return true
		}
		current := r.retryBudgetGatewayAncestors(after)
		for key := range r.retryBudgetGatewayAncestors(before) {
			if !current[key] {
				return true
			}
		}
		return false
	}})
}

func (r *Reconciler) setupBackendRetryBudgetStatus(manager ctrl.Manager) error {
	spec := builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())
	route := builder.WithPredicates(controller.HTTPRouteReconcilePredicate(r.Config))
	setup := ctrl.NewControllerManagedBy(manager).
		For(&gatewayx.XBackendTrafficPolicy{}, builder.WithPredicates(r.retryBudgetStatusPredicate())).
		Watches(&gatewayx.XBackendTrafficPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), spec).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), builder.WithPredicates(controller.ServicePredicate())).
		Watches(&gatewayv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), spec).
		Watches(&gatewayv1.HTTPRoute{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), route).
		Watches(&gatewayv1.GRPCRoute{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), route)
	if r.Config.BackendTLSPolicy {
		setup.Watches(&gatewayv1.BackendTLSPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueueRetryPoliciesForDependency), spec)
	}
	return setup.Named("xbackendtrafficpolicy-retry-status").
		Complete(&backendRetryBudgetStatusReconciler{routes: r})
}

func (r *Reconciler) enqueueRetryPoliciesForDependency(ctx context.Context, object client.Object) []reconcile.Request {
	services := map[string]map[string]bool{}
	add := func(namespace, name string) {
		if services[namespace] == nil {
			services[namespace] = make(map[string]bool)
		}
		services[namespace][name] = true
	}
	switch item := object.(type) {
	case *gatewayx.XBackendTrafficPolicy:
		for _, ref := range item.Spec.TargetRefs {
			if ref.Group == "" && ref.Kind == "Service" {
				add(item.Namespace, string(ref.Name))
			}
		}
	case *gatewayv1.BackendTLSPolicy:
		for _, key := range backendtls.ServiceKeys(item) {
			add(key.Namespace, key.Name)
		}
	case *corev1.Service:
		add(item.Namespace, item.Name)
	case *gatewayv1.ReferenceGrant:
		for _, ref := range item.Spec.To {
			if ref.Group == "" && ref.Kind == "Service" {
				if ref.Name == nil {
					add(item.Namespace, "")
				} else {
					add(item.Namespace, string(*ref.Name))
				}
			}
		}
	case *gatewayv1.HTTPRoute:
		for key := range routeBackendTLSServices(item) {
			add(key.Namespace, key.Name)
		}
	case *gatewayv1.GRPCRoute:
		for key := range routeBackendTLSServices(model.ProjectGRPCRoute(item)) {
			add(key.Namespace, key.Name)
		}
	}

	keys := make(map[types.NamespacedName]struct{})
	for namespace, names := range services {
		var list gatewayx.XBackendTrafficPolicyList
		if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
			log.FromContext(ctx).Error(err, "Could not list Service retry policies for dependency")
			continue
		}
		for i := range list.Items {
			policy := &list.Items[i]
			for name := range names {
				if name == "" || retryBudgetTargetsService(policy, name) {
					keys[client.ObjectKeyFromObject(policy)] = struct{}{}
					break
				}
			}
		}
	}
	return controller.SortedRequests(keys)
}
