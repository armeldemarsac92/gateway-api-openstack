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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// ObserveTelemetryBackendTLSPolicy supplies collector consumers to the shared
// BackendTLSPolicy status reconciler. It never updates the policy status itself.
func (r *Reconciler) ObserveTelemetryBackendTLSPolicy(ctx context.Context, policy *gatewayv1.BackendTLSPolicy) (map[types.NamespacedName]backendtls.PolicyResult, error) {
	if !r.Config.BackendTLSPolicy || !r.Config.TelemetryPolicyEnabled() {
		return nil, nil
	}
	targets := backendtls.ServiceKeys(policy)
	if len(targets) == 0 {
		return nil, nil
	}
	policies, err := r.listTelemetryPolicies(ctx, r.APIReader, "")
	if err != nil {
		return nil, err
	}
	candidates := map[client.ObjectKey]struct{}{}
	for i := range policies {
		tracing := &policies[i]
		ref, valid := model.BackendServiceReference(tracing.Namespace, tracing.Spec.Tracing.Provider.BackendRef)
		if tracing.Spec.Tracing.Mode != gatewayx.TracingModeEnabled || !valid || !slices.Contains(targets, ref) {
			continue
		}
		for _, request := range gatewayRequestsForTelemetryPolicy(ctx, tracing) {
			candidates[request.NamespacedName] = struct{}{}
		}
	}
	consumers := map[types.NamespacedName]backendtls.PolicyResult{}
	for key := range candidates {
		var gateway gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, key, &gateway); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		managed, err := r.telemetryOwnsAncestor(ctx, &gateway)
		if err != nil {
			return nil, err
		}
		if !managed || !r.gatewayBindingComplete(&gateway) {
			continue
		}
		snapshot, err := r.observeGatewayMutationSnapshot(ctx, &gateway, gatewayBindingRequired, "")
		if errors.Is(err, errGatewayChanged) || errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(r.telemetryListeners(snapshot, ListenerSetProjection{})) == 0 {
			continue
		}
		observed, err := r.observeGatewayTelemetry(ctx, &gateway, true, snapshot)
		if err != nil {
			return nil, err
		}
		for _, result := range observed.backendTLS.Policies {
			if result.Key == client.ObjectKeyFromObject(policy) && result.UID == policy.UID && result.Generation == policy.Generation {
				consumers[key] = result
				break
			}
		}
	}
	return consumers, nil
}
