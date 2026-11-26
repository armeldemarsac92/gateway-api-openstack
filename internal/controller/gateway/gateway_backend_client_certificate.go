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
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

const tcpBackendClientCertificateReason = "BackendClientCertificateUnsupported"
const tcpBackendClientCertificateMessage = "Authenticated TCP backends do not support a Gateway backend client certificate"

// BackendClientCertificateResolver authorizes one live operator-enrolled Secret revision.
type BackendClientCertificateResolver interface {
	ResolveBackendClientCertificate(context.Context, *gatewayv1.Gateway) (string, gatewayv1.GatewayConditionReason, string)
}

// BackendClientCertificateRef returns the optional Gateway-wide client identity reference.
func BackendClientCertificateRef(gateway *gatewayv1.Gateway) *gatewayv1.SecretObjectReference {
	if gateway == nil || gateway.Spec.TLS == nil || gateway.Spec.TLS.Backend == nil {
		return nil
	}
	return gateway.Spec.TLS.Backend.ClientCertificateRef
}

func (r *Reconciler) hasBoundBackendTLS(gateway *gatewayv1.Gateway) bool {
	return r.gatewayBindingComplete(gateway) && r.boundListenerProtocol(gateway) == gatewayv1.HTTPProtocolType &&
		controllerutil.ContainsFinalizer(gateway, r.Config.BackendCAFinalizer())
}

func (r *Reconciler) withdrawGatewayCertificates(ctx context.Context, gateway *gatewayv1.Gateway) (ctrl.Result, error) {
	frontend, _, frontendErr := r.withdrawBoundTLS(ctx, gateway)
	backend, backendErr := r.withdrawBoundBackendClientCertificate(ctx, gateway)
	delay := frontend.RequeueAfter
	if delay == 0 || backend.RequeueAfter != 0 && backend.RequeueAfter < delay {
		delay = backend.RequeueAfter
	}
	var withdrawalErr error
	if err := errors.Join(frontendErr, backendErr); err != nil {
		withdrawalErr = controller.SafeProviderReconcileError(
			controller.ProviderFailurePolicy{Message: "Gateway TLS certificate withdrawal failed"}, err)
	}
	if delay > 0 {
		if withdrawalErr != nil {
			// Returning an error would make controller-runtime ignore the delay
			// needed by the other withdrawal's in-progress operation.
			log.FromContext(ctx).Error(withdrawalErr, "Retrying both TLS certificate withdrawals")
		}
		return ctrl.Result{RequeueAfter: delay}, nil
	}
	return ctrl.Result{}, withdrawalErr
}

func (r *Reconciler) withdrawBoundBackendClientCertificate(ctx context.Context, gateway *gatewayv1.Gateway) (ctrl.Result, error) {
	if !r.hasBoundBackendTLS(gateway) {
		return ctrl.Result{}, nil
	}
	if r.Writer == nil {
		return ctrl.Result{}, errors.New("gateway graph writer is required")
	}
	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{
		Action: graph.WithdrawBackendClientCertificate, Gateway: gateway, Identity: controller.StoredGatewayIdentity(r.Config, gateway),
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := result.Outcome.Validate(); err != nil {
		return ctrl.Result{}, err
	}
	if result.Outcome.State == cloud.OutcomeProgressing {
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(result.Outcome, gateway.UID)}, nil
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) observeBackendClientCertificateWithdrawal(ctx context.Context, expected *gatewayv1.Gateway, identity cloud.Identity, xBackendOnly bool) (graph.GatewayInput, error) {
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	observe := func(ctx context.Context) (cloud.BackendClientCertificate, error) {
		var live gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
			return cloud.BackendClientCertificate{}, errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if live.UID != expected.UID || live.Generation != expected.Generation || !reflect.DeepEqual(live.Spec, expected.Spec) ||
			!live.DeletionTimestamp.IsZero() || !r.hasBoundBackendTLS(&live) ||
			!controller.GatewayBindingMetadataEqual(r.Config, &live, expected) ||
			identity != controller.StoredGatewayIdentity(r.Config, &live) {
			return cloud.BackendClientCertificate{}, errGatewayChanged
		}
		if err := controller.ValidateGatewayBinding(r.Config, &live); err != nil {
			return cloud.BackendClientCertificate{}, err
		}
		key := client.ObjectKey{Name: string(live.Spec.GatewayClassName)}
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, key, &class); err != nil {
			return cloud.BackendClientCertificate{}, err
		}
		if class.Spec.ControllerName != r.Config.ControllerName {
			return cloud.BackendClientCertificate{}, errGatewayChanged
		}
		if xBackendOnly {
			return cloud.BackendClientCertificate{}, nil
		}
		certificate, _, _ := r.observeBackendClientCertificate(ctx, &live)
		return certificate, nil
	}
	certificate, err := observe(ctx)
	if err != nil {
		return graph.GatewayInput{}, err
	}
	spec := cloud.GatewaySpec{Identity: identity, ListenerProtocol: cloud.ListenerProtocolHTTP}
	if xBackendOnly && controller.GatewayListenerScope(r.Config, expected) == gatewayv1.HTTPProtocolType {
		spec.ScopedListeners = []cloud.ScopedListener{}
	}
	return graph.GatewayInput{
		Node: cloud.GatewayGraphNode{
			Disposition:              cloud.GraphWithdrawBackendClientCertificate,
			Spec:                     spec,
			BackendCAOwned:           true,
			BackendClientCertificate: certificate,
		},
		Validate: func(ctx context.Context) error {
			latest, err := observe(ctx)
			if err == nil && latest != certificate {
				return errGatewayChanged
			}
			return err
		},
	}, nil
}

// BackendClientCertificateForGateway observes the same authorized client identity used by the Gateway writer.
func (r *Reconciler) BackendClientCertificateForGateway(ctx context.Context, gateway *gatewayv1.Gateway) cloud.BackendClientCertificate {
	certificate, _, _ := r.observeBackendClientCertificate(ctx, gateway)
	return certificate
}

func (r *Reconciler) observeBackendClientCertificate(ctx context.Context, gateway *gatewayv1.Gateway) (cloud.BackendClientCertificate, gatewayv1.GatewayConditionReason, string) {
	if BackendClientCertificateRef(gateway) == nil {
		return cloud.BackendClientCertificate{}, "", ""
	}
	sections, _ := listenerSectionsWithoutTCPConflicts(r.Config, gateway)
	if len(sections) == 1 && terminatesTLS(sections[0]) {
		return cloud.BackendClientCertificate{Denied: true}, tcpBackendClientCertificateReason, tcpBackendClientCertificateMessage
	}
	if !r.Config.BackendTLSPolicy || !r.Config.BackendClientCertificate {
		return cloud.BackendClientCertificate{Denied: true}, "BackendClientCertificateDisabled", "The operator has disabled Gateway backend client certificates"
	}
	if r.BackendClientCertificates == nil {
		return cloud.BackendClientCertificate{Denied: true}, "BackendClientCertificateUnavailable", "The operator backend client certificate resolver is unavailable"
	}
	reference, reason, message := r.BackendClientCertificates.ResolveBackendClientCertificate(ctx, gateway)
	return cloud.BackendClientCertificate{Reference: reference, Denied: reason != ""}, reason, message
}

func (r *Reconciler) setBackendClientCertificateStatus(ctx context.Context, gateway *gatewayv1.Gateway) {
	conditionType := string(gatewayv1.GatewayConditionResolvedRefs)
	_, reason, message := r.observeBackendClientCertificate(ctx, gateway)
	if reason == "" {
		for _, listener := range gateway.Status.Listeners {
			condition := meta.FindStatusCondition(listener.Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
			if condition != nil && condition.Status == metav1.ConditionFalse && condition.ObservedGeneration == gateway.Generation {
				reason, message = gatewayv1.GatewayReasonListenersNotResolved, "One or more listener references could not be resolved"
				break
			}
		}
	}
	status := metav1.ConditionFalse
	if reason == "" {
		if BackendClientCertificateRef(gateway) == nil {
			meta.RemoveStatusCondition(&gateway.Status.Conditions, conditionType)
			return
		}
		status = metav1.ConditionTrue
		reason, message = gatewayv1.GatewayReasonResolvedRefs, "The Gateway backend client certificate reference is resolved"
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(conditionType, status, string(reason), message, gateway.Generation))
}
