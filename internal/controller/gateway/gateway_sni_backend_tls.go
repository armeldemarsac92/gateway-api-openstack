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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// TLSRouteBackendService identifies a TLSRoute's single active reference for BackendTLSPolicy.
func TLSRouteBackendService(route *gatewayv1.TLSRoute) (types.NamespacedName, bool) {
	if len(route.Spec.Rules) != 1 || len(route.Spec.Rules[0].BackendRefs) != 1 {
		return types.NamespacedName{}, false
	}
	ref := route.Spec.Rules[0].BackendRefs[0]
	if ptr.Deref(ref.Weight, 1) <= 0 {
		return types.NamespacedName{}, false
	}
	return model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
}

// ObserveTLSRouteBackendTLSPolicy evaluates the TLSRoute consumers of this policy.
// The shared BackendTLSPolicy status controller merges these ancestors with the
// HTTP and gRPC consumers. No route or Gateway status is used as authorization.
func (r *Reconciler) ObserveTLSRouteBackendTLSPolicy(ctx context.Context, policy *gatewayv1.BackendTLSPolicy) (map[types.NamespacedName]backendtls.PolicyResult, error) {
	targets := backendtls.ServiceKeys(policy)
	if !r.Config.TCPTLSTermination || !r.Config.BackendTLSPolicy || len(targets) == 0 {
		return nil, nil
	}
	routes, err := r.readLiveSNIRoutes(ctx)
	if err != nil {
		return nil, err
	}
	peers := map[types.NamespacedName][]gatewayv1.TLSRoute{}
	candidates := map[types.NamespacedName]bool{}
	for _, route := range routes {
		if len(route.Spec.ParentRefs) != 1 || !route.DeletionTimestamp.IsZero() {
			continue
		}
		ref := route.Spec.ParentRefs[0]
		if !controller.IsGatewayParentRef(ref) {
			continue
		}
		parent := types.NamespacedName{Namespace: string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(route.Namespace))), Name: string(ref.Name)}
		peers[parent] = append(peers[parent], route)
		if key, ok := TLSRouteBackendService(&route); ok && slices.Contains(targets, key) {
			candidates[parent] = true
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return nil, nil
		}
		return nil, err
	}
	observer := graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	ancestors := make(map[types.NamespacedName]backendtls.PolicyResult, len(candidates))
	for key := range candidates {
		parent := &gatewayv1.Gateway{}
		if err := r.APIReader.Get(ctx, key, parent); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		if !parent.DeletionTimestamp.IsZero() {
			continue
		}
		listeners, err := SupportedListenersForConfig(r.Config, parent)
		if err != nil || !terminatesTLS(listeners[0]) {
			continue
		}
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(parent.Spec.GatewayClassName)}, &class); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return nil, err
			}
			continue
		}
		if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() ||
			!controller.GatewayClassSupportsInstalledVersion(&class) {
			continue
		}
		if err := r.validateBoundSNITopology(parent, listeners[0]); err != nil {
			continue
		}
		snapshot := &gatewayMutationSnapshot{gateway: parent, listener: listeners[0], listeners: listeners}
		observation, err := r.admitSNIRoutes(ctx, snapshot, peers[key])
		if err != nil {
			return nil, err
		}
		owners := observation.hostnameOwners()
		for i := range observation {
			entry := &observation[i]
			if !terminatesTLS(entry.listener) || !slices.ContainsFunc(entry.hostnames, func(host string) bool { return owners[host].index == i }) {
				continue
			}
			backend, ok := TLSRouteBackendService(entry.route)
			if !ok || !slices.Contains(targets, backend) {
				continue
			}
			service, port, diagnostic, err := r.observeSNIService(ctx, entry.route, entry.route.Spec.Rules[0].BackendRefs[0])
			if err != nil {
				return nil, err
			}
			if diagnostic != nil {
				continue
			}
			selection, err := observer.Observe(ctx, service, port, nil)
			if err != nil {
				return nil, err
			}
			for _, result := range selection.Policies {
				if result.UID == policy.UID && result.Generation == policy.Generation {
					if result.Accepted.Status == metav1.ConditionTrue && BackendClientCertificateRef(parent) != nil {
						result.Accepted.Status, result.Accepted.Reason, result.Accepted.Message =
							metav1.ConditionFalse, tcpBackendClientCertificateReason, tcpBackendClientCertificateMessage
					}
					ancestors[key] = result
				}
			}
		}
	}
	return ancestors, nil
}
