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

package listenerset

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type reconciler struct {
	client.Client
	apiReader      client.Reader
	controllerName gatewayv1.GatewayController
}

// SetupWithManager registers ListenerSet v1 reconciliation when the API is served.
func SetupWithManager(ctx context.Context, manager ctrl.Manager, controllerName gatewayv1.GatewayController) error {
	served, err := controller.ListenerSetServed(manager.GetRESTMapper())
	if err != nil || !served {
		return err
	}
	if err := manager.GetFieldIndexer().IndexField(ctx, &gatewayv1.ListenerSet{}, controller.IndexListenerSetByGateway, func(object client.Object) []string {
		parent := model.ListenerSetParentKey(object.(*gatewayv1.ListenerSet))
		if parent.Name == "" {
			return nil
		}
		return []string{parent.String()}
	}); err != nil {
		return fmt.Errorf("index ListenerSets by parent Gateway: %w", err)
	}
	r := &reconciler{Client: manager.GetClient(), apiReader: manager.GetAPIReader(), controllerName: controllerName}
	return ctrl.NewControllerManagedBy(manager).
		For(&gatewayv1.ListenerSet{}).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(r.requestsForGateway),
			builder.WithPredicates(controller.GatewayForHTTPRoutePredicate(controller.Config{ControllerName: controllerName}))).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.requestsForClass),
			builder.WithPredicates(controller.GenerationOrDeletionChangedPredicate())).
		Complete(r)
}

func (r *reconciler) requestsForGateway(ctx context.Context, gateway client.Object) []reconcile.Request {
	key := client.ObjectKeyFromObject(gateway)
	var sets gatewayv1.ListenerSetList
	if err := r.List(ctx, &sets, client.MatchingFields{controller.IndexListenerSetByGateway: key.String()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list ListenerSets for Gateway", "gateway", key)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sets.Items))
	for i := range sets.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&sets.Items[i])})
	}
	return requests
}

func (r *reconciler) requestsForClass(ctx context.Context, class client.Object) []reconcile.Request {
	var gateways gatewayv1.GatewayList
	if err := r.List(ctx, &gateways, client.MatchingFields{controller.IndexGatewayByClass: class.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "Could not list Gateways for ListenerSet reconciliation", "gatewayClass", class.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range gateways.Items {
		requests = append(requests, r.requestsForGateway(ctx, &gateways.Items[i])...)
	}
	return requests
}

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=listenersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=listenersets/status,verbs=patch
func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var listenerSet gatewayv1.ListenerSet
	if err := r.apiReader.Get(ctx, req.NamespacedName, &listenerSet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !listenerSet.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	parent := model.ListenerSetParentKey(&listenerSet)
	if parent.Name == "" {
		return ctrl.Result{}, r.clearOwnedStatus(ctx, &listenerSet)
	}
	var gateway gatewayv1.Gateway
	if err := r.apiReader.Get(ctx, parent, &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.clearOwnedStatus(ctx, &listenerSet)
		}
		return ctrl.Result{}, err
	}
	var class gatewayv1.GatewayClass
	if err := r.apiReader.Get(ctx, client.ObjectKey{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.clearOwnedStatus(ctx, &listenerSet)
		}
		return ctrl.Result{}, err
	}
	if class.Spec.ControllerName != r.controllerName {
		return ctrl.Result{}, nil
	}

	base := listenerSet.DeepCopy()
	acceptedReason, programmedReason := gatewayv1.ListenerSetReasonListenersNotValid, gatewayv1.ListenerSetReasonInvalid
	message := "The controller cannot configure delegated listeners"
	switch {
	case !gateway.DeletionTimestamp.IsZero() || !class.DeletionTimestamp.IsZero():
		acceptedReason = gatewayv1.ListenerSetReasonParentNotAccepted
		message = "The parent Gateway or its GatewayClass is being deleted"
	case model.GatewayDisallowsListenerSets(&gateway):
		acceptedReason, programmedReason = gatewayv1.ListenerSetReasonNotAllowed, gatewayv1.ListenerSetReasonNotAllowed
		message = "The parent Gateway does not allow ListenerSets"
	case gatewayRejected(&gateway):
		acceptedReason = gatewayv1.ListenerSetReasonParentNotAccepted
		message = "The parent Gateway is not accepted"
	default:
		// The parent Gateway's graph reconciler reports admitted and denied
		// namespaces with the winning listener and provider readiness.
		return ctrl.Result{}, nil
	}
	message += r.messageSuffix()
	controller.SetCondition(&listenerSet.Status.Conditions, controller.Condition(string(gatewayv1.ListenerSetConditionAccepted), metav1.ConditionFalse, string(acceptedReason), message, listenerSet.Generation))
	controller.SetCondition(&listenerSet.Status.Conditions, controller.Condition(string(gatewayv1.ListenerSetConditionProgrammed), metav1.ConditionFalse, string(programmedReason), message, listenerSet.Generation))
	listenerSet.Status.Listeners = nil
	return ctrl.Result{}, r.patchStatus(ctx, base, &listenerSet)
}

func gatewayRejected(gateway *gatewayv1.Gateway) bool {
	accepted := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	return accepted != nil && accepted.ObservedGeneration == gateway.Generation && accepted.Status == metav1.ConditionFalse
}

func (r *reconciler) messageSuffix() string {
	return " (controller " + string(r.controllerName) + ")"
}

// ListenerSet v1 has one shared status without controller or parent entries.
// Without a valid parent, withdraw the status published by this controller.
func (r *reconciler) clearOwnedStatus(ctx context.Context, listenerSet *gatewayv1.ListenerSet) error {
	base := listenerSet.DeepCopy()
	for _, kind := range []gatewayv1.ListenerSetConditionType{gatewayv1.ListenerSetConditionAccepted, gatewayv1.ListenerSetConditionProgrammed} {
		condition := meta.FindStatusCondition(listenerSet.Status.Conditions, string(kind))
		if condition == nil || !strings.HasSuffix(condition.Message, r.messageSuffix()) {
			continue
		}
		controller.SetCondition(&listenerSet.Status.Conditions, controller.Condition(string(kind), metav1.ConditionUnknown, string(gatewayv1.ListenerSetReasonPending), "Waiting for a managed parent Gateway", listenerSet.Generation))
	}
	if !equality.Semantic.DeepEqual(base.Status.Conditions, listenerSet.Status.Conditions) {
		listenerSet.Status.Listeners = nil
	}
	return r.patchStatus(ctx, base, listenerSet)
}

func (r *reconciler) patchStatus(ctx context.Context, base, desired *gatewayv1.ListenerSet) error {
	if equality.Semantic.DeepEqual(base.Status, desired.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Patch(ctx, desired, controller.OptimisticMergeFrom(base)))
}
