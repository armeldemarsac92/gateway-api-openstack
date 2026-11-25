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
	"iter"
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

// TLSRouteBackendServiceRefs enumerates positive Service refs for policy
// discovery. The live observer still checks listener admission and authorization.
func TLSRouteBackendServiceRefs(route *gatewayv1.TLSRoute) iter.Seq2[types.NamespacedName, gatewayv1.BackendRef] {
	return func(yield func(types.NamespacedName, gatewayv1.BackendRef) bool) {
		if len(route.Spec.Rules) != 1 {
			return
		}
		for _, ref := range route.Spec.Rules[0].BackendRefs {
			if ptr.Deref(ref.Weight, 1) <= 0 {
				continue
			}
			if key, supported := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference); supported && !yield(key, ref) {
				return
			}
		}
	}
}

// TLSRouteBackendTLSPolicyConsumer captures the conditions and transport needed
// by the selected TLSRoutes for one Gateway.
type TLSRouteBackendTLSPolicyConsumer struct {
	backendtls.PolicyResult
	Weighted bool
}

// ObserveTLSRouteBackendTLSPolicy evaluates the TLSRoute consumers of this policy.
// The shared BackendTLSPolicy status controller merges these ancestors with the
// HTTP and gRPC consumers. No route or Gateway status is used as authorization.
func (r *Reconciler) ObserveTLSRouteBackendTLSPolicy(ctx context.Context, policy *gatewayv1.BackendTLSPolicy) (map[types.NamespacedName]TLSRouteBackendTLSPolicyConsumer, error) {
	targets := backendtls.ServiceKeys(policy)
	if !r.Config.TCPTLSTermination || !r.Config.BackendTLSPolicy || len(targets) == 0 {
		return nil, nil
	}
	routes, err := r.readLiveSNIRoutes(ctx)
	if err != nil {
		return nil, err
	}
	candidates := map[types.NamespacedName]bool{}
	includeDefaults := false
	for _, route := range routes {
		if !route.DeletionTimestamp.IsZero() {
			continue
		}
		for key := range TLSRouteBackendServiceRefs(&route) {
			if !slices.Contains(targets, key) {
				continue
			}
			for _, ref := range route.Spec.ParentRefs {
				if controller.IsGatewayParentRef(ref) {
					parent := types.NamespacedName{Namespace: string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(route.Namespace))), Name: string(ref.Name)}
					candidates[parent] = true
				}
			}
			includeDefaults = includeDefaults || route.Spec.UseDefaultGateways == gatewayv1.GatewayDefaultScopeAll
			break
		}
	}
	if r.Config.DefaultGatewaysEnabled() && includeDefaults {
		parents, err := controller.DefaultGatewayParents(ctx, r.APIReader, "", gatewayv1.CommonRouteSpec{UseDefaultGateways: gatewayv1.GatewayDefaultScopeAll})
		if err != nil {
			return nil, err
		}
		for _, ref := range parents {
			candidates[types.NamespacedName{Namespace: string(*ref.Namespace), Name: string(ref.Name)}] = true
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
	ancestors := make(map[types.NamespacedName]TLSRouteBackendTLSPolicyConsumer, len(candidates))
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
		// Binding establishes Endpoint eligibility and triggers the policy watch.
		topology, err := controller.StoredGatewayTopology(r.Config, parent)
		if err != nil || validateSNITopology(listeners[0], topology) != nil {
			continue
		}
		snapshot := &gatewayMutationSnapshot{gateway: parent, listener: listeners[0], listeners: listeners}
		observation, err := r.admitSNIRoutes(ctx, snapshot, routes)
		if err != nil {
			return nil, err
		}
		owners := observation.hostnameOwners()
		for i := range observation {
			entry := &observation[i]
			if !terminatesTLS(entry.listener) || !slices.ContainsFunc(entry.hostnames, func(host string) bool { return owners[host].index == i }) {
				continue
			}
			result, err := r.observeSNIBackendTLSPolicy(ctx, parent, entry.route, policy, targets, observer)
			if err != nil {
				return nil, err
			}
			if result.UID != "" {
				consumer := ancestors[key]
				consumer.PolicyResult = mergeSNIBackendTLSConsumer(consumer.PolicyResult, result)
				consumer.Weighted = consumer.Weighted || sniHasMultipleActiveBackends(entry.route.Spec.Rules[0].BackendRefs)
				ancestors[key] = consumer
			}
		}
	}
	return ancestors, nil
}

type unreportedSNIAttachment struct {
	route   client.ObjectKey
	section gatewayv1.SectionName
}

// A saturated TLSRoute status can omit an accepted parent. Use the same
// hostname election and backend resolution as the SNI graph for that section.
func (r *Reconciler) observeUnreportedSNIAncestorLimit(ctx context.Context, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener, routes []gatewayv1.TLSRoute, unreported map[unreportedSNIAttachment]struct{}) (bool, error) {
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil || validateSNITopology(listeners[0], topology) != nil {
		return false, nil
	}
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: listeners[0], listeners: listeners, topology: topology}
	observation, err := r.admitSNIRoutes(ctx, snapshot, routes)
	if err != nil {
		return false, err
	}
	owners := observation.hostnameOwners()
	tls := graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	mixed := mixedTLSBinding(r.Config, gateway, snapshot.listener.Port)
	for i := range observation {
		entry := &observation[i]
		key := unreportedSNIAttachment{route: client.ObjectKeyFromObject(entry.route), section: entry.listener.Name}
		if _, needed := unreported[key]; !needed || entry.acceptedReason != gatewayv1.RouteReasonAccepted {
			continue
		}
		entry.retainOwnedHostnames(owners, i)
		if len(entry.hostnames) == 0 {
			continue
		}
		if err := r.resolveSNIBackend(ctx, snapshot, entry, tls, mixed); err != nil {
			return false, err
		}
		if entry.resolvedReason == backendtls.AncestorLimitReason {
			return true, nil
		}
	}
	return false, nil
}

func sniHasMultipleActiveBackends(refs []gatewayv1.BackendRef) bool {
	found := false
	for _, ref := range refs {
		if ptr.Deref(ref.Weight, 1) > 0 {
			if found {
				return true
			}
			found = true
		}
	}
	return false
}

func (r *Reconciler) observeSNIBackendTLSPolicy(ctx context.Context, parent *gatewayv1.Gateway, route *gatewayv1.TLSRoute, policy *gatewayv1.BackendTLSPolicy, targets []types.NamespacedName, observer *graph.BackendTLSObserver) (backendtls.PolicyResult, error) {
	var observed backendtls.PolicyResult
	observe := observer.Observe
	if len(route.Spec.Rules[0].BackendRefs) > 1 {
		observe = observer.ObserveWeightedService
	}
	for key, ref := range TLSRouteBackendServiceRefs(route) {
		if !slices.Contains(targets, key) {
			continue
		}
		service, port, diagnostic, err := r.observeSNIService(ctx, route, ref)
		if err != nil {
			return observed, err
		}
		if diagnostic != nil {
			continue
		}
		selection, err := observe(ctx, service, port, nil)
		if err != nil {
			return observed, err
		}
		for _, result := range selection.Policies {
			if result.UID != policy.UID || result.Generation != policy.Generation {
				continue
			}
			if result.Accepted.Status == metav1.ConditionTrue && BackendClientCertificateRef(parent) != nil {
				result.Accepted.Status, result.Accepted.Reason, result.Accepted.Message =
					metav1.ConditionFalse, tcpBackendClientCertificateReason, tcpBackendClientCertificateMessage
			}
			observed = mergeSNIBackendTLSConsumer(observed, result)
		}
	}
	return observed, nil
}

func mergeSNIBackendTLSConsumer(prior, current backendtls.PolicyResult) backendtls.PolicyResult {
	if prior.UID != "" {
		current.Accepted = backendtls.LeastSatisfiedCondition(prior.Accepted, current.Accepted)
		current.ResolvedRefs = backendtls.LeastSatisfiedCondition(prior.ResolvedRefs, current.ResolvedRefs)
	}
	return current
}
