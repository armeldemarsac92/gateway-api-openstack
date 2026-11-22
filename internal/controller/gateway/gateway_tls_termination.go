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
	"slices"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// Read only the binding and certificate inputs. Withdrawal remains possible
// when route inventory is incomplete or the Gateway spec can no longer serve.
func (r *Reconciler) rawTLSWithdrawalInput(ctx context.Context, expected *gatewayv1.Gateway, identity cloud.Identity) (graph.GatewayInput, error) {
	port, err := strconv.Atoi(expected.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if err != nil || port < 1 || port > 65535 {
		return graph.GatewayInput{}, errGatewayChanged
	}
	observe := func(ctx context.Context) (string, error) {
		var live gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
			return "", errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if live.UID != expected.UID || !controllerutil.ContainsFinalizer(&live, r.Config.GatewayFinalizer()) ||
			controller.ValidateGatewayBinding(r.Config, &live) != nil ||
			r.boundListenerProtocol(&live) != cloud.ListenerProtocolTLSTerminate ||
			!controller.GatewayBindingMetadataEqual(r.Config, expected, &live) ||
			controller.StoredGatewayIdentity(r.Config, &live) != identity {
			return "", errGatewayChanged
		}
		if !live.DeletionTimestamp.IsZero() || !r.Config.TCPTLSTermination {
			return "", nil
		}
		listeners, err := SupportedListenersForConfig(r.Config, &live)
		if err != nil || len(listeners) != 1 || !terminatesTLS(listeners[0]) || int(listeners[0].Port) != port ||
			r.validateBoundSNITopology(&live, listeners[0]) != nil {
			return "", nil
		}
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(live.Spec.GatewayClassName)}, &class); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil
			}
			return "", err
		}
		if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() ||
			!controller.GatewayClassSupportsInstalledVersion(&class) {
			return "", nil
		}
		if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
			if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
				return "", nil
			}
			return "", err
		}
		return r.resolveCertificate(ctx, &live, listeners[0]).tls.CertificateRef, nil
	}
	ref, err := observe(ctx)
	if err != nil {
		return graph.GatewayInput{}, err
	}
	return graph.GatewayInput{
		Node: cloud.GatewayGraphNode{
			Disposition:          cloud.GraphWithdrawTLS,
			Spec:                 cloud.GatewaySpec{Identity: identity, ListenerProtocol: cloud.ListenerProtocolTLSTerminate, ListenerPort: port},
			TCPTLSCertificateRef: ref,
		},
		Validate: func(ctx context.Context) error {
			latest, err := observe(ctx)
			if err == nil && latest != ref {
				return errGatewayChanged
			}
			return err
		},
	}, nil
}

func (r *Reconciler) applyRawTLSListenerStatus(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, changed bool) {
	issue := r.resolveCertificate(ctx, gateway, listener).issue
	if issue == nil && !changed {
		return
	}
	index := slices.IndexFunc(gateway.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == listener.Name })
	if index == -1 {
		return
	}
	conditions := &gateway.Status.Listeners[index].Conditions
	reason, message := gatewayv1.ListenerReasonPending, "Listener certificate authorization changed during reconciliation"
	if issue != nil {
		reason, message = issue.reason, issue.message
		controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), metav1.ConditionFalse, string(reason), message, gateway.Generation))
	}
	controller.SetCondition(conditions, controller.Condition(string(gatewayv1.ListenerConditionProgrammed), metav1.ConditionFalse, string(reason), message, gateway.Generation))
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(string(gatewayv1.GatewayConditionProgrammed),
		metav1.ConditionFalse, string(gatewayv1.GatewayReasonListenersNotValid), message, gateway.Generation))
}
