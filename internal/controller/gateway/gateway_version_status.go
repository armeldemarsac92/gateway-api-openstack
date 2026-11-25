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
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) setGatewayUnsupportedVersion(
	ctx context.Context,
	scope *gatewayScope,
) (ctrl.Result, error) {
	gateway, gatewayClass, managed, err := r.gatewayStillManaged(ctx, scope.gateway)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !managed {
		scope.skipPatch = true
		return ctrl.Result{}, nil
	}
	if err := scope.refresh(gateway); err != nil {
		return ctrl.Result{}, err
	}
	gateway = scope.gateway
	recheckVersion := false
	if controller.GatewayClassSupportsInstalledVersion(gatewayClass) {
		versionErr := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader)
		if versionErr == nil {
			return ctrl.Result{}, errGatewayChanged
		}
		if !errors.Is(versionErr, controller.ErrUnsupportedGatewayAPIVersion) {
			return ctrl.Result{}, versionErr
		}
		recheckVersion = true
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
		string(gatewayv1.GatewayConditionAccepted),
		metav1.ConditionUnknown,
		string(gatewayv1.GatewayReasonPending),
		controller.UnsupportedGatewayAPIVersionMessage,
		gateway.Generation,
	))
	scope.rawTLSVersionUnsupported = cloud.IsTCPTLSListenerProtocol(string(r.boundListenerProtocol(gateway)))
	if scope.rawTLSVersionUnsupported {
		r.markRawTLSVersionUnsupported(gateway)
	}
	result, withdrawn, err := r.withdrawBoundTLS(ctx, gateway)
	if usesHTTPSProfile(r.Config, gateway) {
		r.applyHTTPSListenerStatus(gateway, scope.original.Status, nil, &graph.TLSAuthorizationChanged{Ports: httpsPorts(gateway)}, false)
	}
	if err != nil || !withdrawn {
		return result, err
	}
	programmed := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.ObservedGeneration != gateway.Generation {
		controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
			string(gatewayv1.GatewayConditionProgrammed),
			metav1.ConditionUnknown,
			string(gatewayv1.GatewayReasonPending),
			controller.UnsupportedGatewayAPIVersionMessage,
			gateway.Generation,
		))
	}
	result = ctrl.Result{}
	if recheckVersion {
		result.RequeueAfter = controller.GatewayAPIVersionRequeueAfter(gateway.UID)
	}
	return result, nil
}

func (r *Reconciler) markRawTLSVersionUnsupported(gateway *gatewayv1.Gateway) {
	message := controller.UnsupportedGatewayAPIVersionMessage
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed),
		metav1.ConditionFalse, string(gatewayv1.GatewayReasonPending), message, gateway.Generation))
	// Only update the logical sections on the bound physical TLS port.
	mixed := r.boundListenerProtocol(gateway) == cloud.ListenerProtocolTLSMixed
	port, _ := strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
	for i := range gateway.Status.Listeners {
		status := &gateway.Status.Listeners[i]
		if len(gateway.Status.Listeners) == 1 || slices.ContainsFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
			return listener.Name == status.Name && int(listener.Port) == port &&
				(terminatesTLS(&listener) || mixed && listener.Protocol == gatewayv1.TLSProtocolType)
		}) {
			controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse, string(gatewayv1.ListenerReasonPending), message, gateway.Generation))
		}
	}
}

func (r *Reconciler) gatewayStillManaged(
	ctx context.Context,
	expected *gatewayv1.Gateway,
) (*gatewayv1.Gateway, *gatewayv1.GatewayClass, bool, error) {
	if r.APIReader == nil {
		return nil, nil, false, controller.ErrAPIReaderRequired
	}
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("get Gateway before status update: %w", err)
	}
	if gateway.UID != expected.UID || gateway.Generation != expected.Generation || !gateway.DeletionTimestamp.IsZero() {
		return nil, nil, false, nil
	}
	var gatewayClass gatewayv1.GatewayClass
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}, &gatewayClass); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("get GatewayClass before Gateway status update: %w", err)
	}
	if gatewayClass.Spec.ControllerName != r.Config.ControllerName {
		return nil, nil, false, nil
	}
	return &gateway, &gatewayClass, true, nil
}
