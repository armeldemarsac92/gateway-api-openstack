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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

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
	if len(targets) == 0 {
		return nil, nil
	}
	// A missing cache index entry must not reclaim an occupied ancestor slot.
	routes, err := r.routes.listLiveRoutes(ctx)
	if err != nil {
		return nil, err
	}
	if r.routes.Config.GRPCExactMethodEnabled() {
		grpc, err := r.routes.forRouteKind(model.GRPCRouteKind).listLiveRoutes(ctx)
		if err != nil {
			return nil, err
		}
		routes = append(routes, grpc...)
	}
	gateways := map[types.NamespacedName]*gatewayv1.Gateway{}
	for i := range routes {
		route := &routes[i]
		services := r.routes.backendTLSPolicyServices(route)
		if !route.DeletionTimestamp.IsZero() || !slices.ContainsFunc(targets, func(key types.NamespacedName) bool {
			_, used := services[key]
			return used
		}) {
			continue
		}
		parents, err := r.routes.managedParentsWithReader(ctx, r.routes.APIReader, route)
		if err != nil {
			return nil, err
		}
		for _, group := range groupManagedRouteParents(parents) {
			if managedParentsSupportInstalledVersion(group.parents) {
				gateways[client.ObjectKeyFromObject(group.gateway)] = group.gateway
			}
		}
	}
	peers := make(map[types.NamespacedName][]gatewayv1.HTTPRoute, len(gateways))
	setParents := make(map[types.NamespacedName]types.NamespacedName)
	for _, route := range routes {
		seen := map[types.NamespacedName]struct{}{}
		for _, ref := range route.Spec.ParentRefs {
			key, err := gatewayKeyForRouteParent(ctx, r.routes.APIReader, &route, ref, setParents)
			if err != nil {
				return nil, err
			}
			if _, included := seen[key]; !included && gateways[key] != nil {
				peers[key] = append(peers[key], route)
				seen[key] = struct{}{}
			}
		}
		if r.routes.Config.DefaultGatewaysEnabled() && route.Spec.UseDefaultGateways == gatewayv1.GatewayDefaultScopeAll {
			for key, gateway := range gateways {
				if _, included := seen[key]; included {
					continue
				}
				if _, ok := model.DefaultGatewayParent(route.Spec.CommonRouteSpec, route.Namespace, gateway); ok {
					peers[key] = append(peers[key], route)
				}
			}
		}
	}
	ancestors := make(map[types.NamespacedName]backendTLSPolicyConditions, len(gateways))
	availability := make(map[backendTLSRequirements]backendTLSAvailability, 2)
	requireSAN := len(policy.Spec.Validation.SubjectAltNames) != 0
	systemCA := policy.Spec.Validation.WellKnownCACertificates != nil &&
		*policy.Spec.Validation.WellKnownCACertificates == gatewayv1.WellKnownCACertificatesSystem
	for key, gateway := range gateways {
		consumer, err := r.observeGatewayConsumer(ctx, policy, gateway, targets, peers[key])
		if err != nil {
			return nil, err
		}
		if consumer.selection == nil {
			continue
		}
		requirements := backendTLSRequirements{requireH2: consumer.requiresHTTP2, requireSAN: requireSAN, systemCA: systemCA, grpcAuthorizer: consumer.grpcAuthorizer}
		if _, checked := availability[requirements]; !checked &&
			consumer.selection.Accepted.Status == metav1.ConditionTrue && consumer.supportedProtocol {
			if requirements.grpcAuthorizer {
				availability[requirements] = r.grpcAuthorizerTLSAvailability(ctx, consumer.selection.Validation)
			} else {
				availability[requirements] = r.routes.backendTLSAvailability(ctx, requirements)
			}
		}
		ancestors[key] = backendTLSPolicyConditions{
			accepted:     availability[requirements].apply(consumer.selection.Accepted, consumer.supportedProtocol, requireSAN, systemCA),
			resolvedRefs: consumer.selection.ResolvedRefs,
		}
	}
	if r.routes.Config.TCPTLSTermination {
		gatewayObserver := gatewaycontroller.Reconciler{APIReader: r.routes.APIReader, Config: r.routes.Config}
		consumers, err := gatewayObserver.ObserveTLSRouteBackendTLSPolicy(ctx, policy)
		if err != nil {
			return nil, err
		}
		capabilities := map[bool]backendTLSAvailability{}
		for key, consumer := range consumers {
			capability, checked := capabilities[consumer.Weighted]
			if !checked && !requireSAN && consumer.Accepted.Status == metav1.ConditionTrue {
				capability = r.tcpBackendTLSAvailability(ctx, consumer.Weighted, systemCA)
				capabilities[consumer.Weighted] = capability
			}
			conditions := backendTLSPolicyConditions{
				accepted: capability.apply(consumer.Accepted, !requireSAN, requireSAN, systemCA), resolvedRefs: consumer.ResolvedRefs,
			}
			if !systemCA && consumer.Weighted && consumer.Accepted.Status == metav1.ConditionTrue {
				switch conditions.accepted.Reason {
				case "UnsupportedProvider":
					conditions.accepted.Message = "The regional provider does not support weighted authenticated TLSRoute upstreams"
				case "Pending":
					conditions.accepted.Message = "Regional provider support for weighted authenticated TLSRoute upstreams could not be verified"
				}
			}
			// A policy must be enforceable by every profile consuming it
			// before the single ancestor can report Accepted.
			mergeBackendTLSConsumer(ancestors, key, conditions)
		}
	}
	if r.routes.Config.TelemetryPolicyEnabled() {
		observer := gatewaycontroller.Reconciler{APIReader: r.routes.APIReader, Config: r.routes.Config}
		consumers, err := observer.ObserveTelemetryBackendTLSPolicy(ctx, policy)
		if err != nil {
			return nil, err
		}
		capability := backendTLSUnknown
		if len(consumers) != 0 && r.routes.TelemetryBackendTLSAvailable != nil {
			supported, err := r.routes.TelemetryBackendTLSAvailable(ctx, requireSAN, systemCA)
			if err == nil {
				capability = backendTLSUnsupported
				if supported {
					capability = backendTLSAvailable
				}
			}
		}
		for key, consumer := range consumers {
			mergeBackendTLSConsumer(ancestors, key, backendTLSPolicyConditions{
				accepted:     capability.apply(consumer.Accepted, true, requireSAN, systemCA),
				resolvedRefs: consumer.ResolvedRefs,
			})
		}
	}
	return ancestors, nil
}

func (r *backendTLSPolicyStatusReconciler) tcpBackendTLSAvailability(ctx context.Context, weighted, systemCA bool) backendTLSAvailability {
	discover := r.routes.TCPBackendTLSAvailable
	if weighted {
		discover = r.routes.TCPWeightedBackendTLSAvailable
	}
	if systemCA {
		if r.routes.TCPBackendTLSSystemCAAvailable == nil {
			return backendTLSUnknown
		}
		discover = func(ctx context.Context) (bool, error) {
			return r.routes.TCPBackendTLSSystemCAAvailable(ctx, weighted)
		}
	}
	if discover == nil {
		return backendTLSUnknown
	}
	supported, err := discover(ctx)
	switch {
	case err != nil:
		return backendTLSUnknown
	case !supported:
		return backendTLSUnsupported
	default:
		return backendTLSAvailable
	}
}

func (r *backendTLSPolicyStatusReconciler) grpcAuthorizerTLSAvailability(ctx context.Context, validation *backendtls.Validation) backendTLSAvailability {
	probe := r.routes.RouteExternalAuthAvailable
	if probe == nil || validation == nil {
		return backendTLSUnknown
	}
	tls := validation.CloudValidation()
	tls.RequireH2 = true
	auth := cloud.ListenerExternalAuth{Protocol: cloud.ExternalAuthProtocolGRPC, BackendTLS: tls}
	return discoverBackendTLSAvailability(ctx, func(ctx context.Context) (bool, error) { return probe(ctx, auth, false) })
}

func mergeBackendTLSConsumer(ancestors map[types.NamespacedName]backendTLSPolicyConditions, key types.NamespacedName, conditions backendTLSPolicyConditions) {
	if prior, exists := ancestors[key]; exists {
		conditions.accepted = backendtls.LeastSatisfiedCondition(prior.accepted, conditions.accepted)
		conditions.resolvedRefs = backendtls.LeastSatisfiedCondition(prior.resolvedRefs, conditions.resolvedRefs)
	}
	ancestors[key] = conditions
}

type backendTLSConsumer struct {
	selection         *backendtls.PolicyResult
	supportedProtocol bool
	requiresHTTP2     bool
	grpcAuthorizer    bool
}

func (r *backendTLSPolicyStatusReconciler) observeGatewayConsumer(ctx context.Context, policy *gatewayv1.BackendTLSPolicy, gateway *gatewayv1.Gateway, targets []types.NamespacedName, peers []gatewayv1.HTTPRoute) (backendTLSConsumer, error) {
	admission, err := r.routes.observeRouteGraphAdmission(ctx, gateway, peers, nil)
	if err != nil {
		return backendTLSConsumer{}, err
	}
	input := model.Input{BackendH2C: r.routes.Config.BackendH2C}
	type source struct {
		kind      gatewayv1.Kind
		namespace string
	}
	consumers := map[types.NamespacedName]map[source]struct{}{}
	grantsRequired := false
	for i := range peers {
		route := &peers[i]
		if _, eligible := admission.eligible[routeKey(route)]; !eligible {
			continue
		}
		services := r.routes.backendTLSPolicyServices(route)
		usesTarget := false
		for _, key := range targets {
			if _, used := services[key]; !used {
				continue
			}
			if consumers[key] == nil {
				consumers[key] = map[source]struct{}{}
			}
			consumers[key][source{kind: model.HTTPProfileRouteKind(route), namespace: route.Namespace}] = struct{}{}
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
			return backendTLSConsumer{}, err
		}
	}
	services := make(map[types.NamespacedName]*corev1.Service, len(consumers))
	for key, sources := range consumers {
		authorized := false
		for consumer := range sources {
			if model.BackendReferenceAllowedFor(consumer.kind, consumer.namespace, key, input.ReferenceGrants) {
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
				return backendTLSConsumer{}, err
			}
			continue
		}
		input.Services = append(input.Services, service)
		services[key] = &service
	}
	tlsObserver := graph.NewBackendTLSObserver(r.routes.APIReader, r.routes.Config.ClusterTrustBundles)
	if err := tlsObserver.CollectServiceInputs(ctx, &input, nil); err != nil {
		return backendTLSConsumer{}, err
	}
	result := backendTLSConsumer{supportedProtocol: true}
	for i := range input.Routes {
		route := &input.Routes[i]
		for _, rule := range route.Spec.Rules {
			if auth := r.routes.grpcAuthorizerForRule(route, rule); auth != nil {
				key, _ := model.BackendServiceReference(route.Namespace, auth.BackendRef)
				service := services[key]
				if service != nil && service.DeletionTimestamp.IsZero() &&
					model.BackendReferenceAllowedFor(model.HTTPRouteKind, route.Namespace, key, input.ReferenceGrants) {
					port, diagnostic := model.ResolveTCPServicePort(service, auth.BackendRef.Port)
					if diagnostic == nil {
						selection, err := tlsObserver.Observe(ctx, service, port, nil)
						if err != nil {
							return backendTLSConsumer{}, err
						}
						for _, selection := range selection.Policies {
							if selection.UID == policy.UID && selection.Generation == policy.Generation {
								result.selection = &selection
								result.requiresHTTP2, result.grpcAuthorizer = true, true
							}
						}
					}
				}
			}
			for _, backend := range model.ResolveBackendsAndMirrors(route, rule, input) {
				if backend.Weight == 0 {
					continue
				}
				for _, selection := range backend.BackendTLS.Policies {
					if selection.UID != policy.UID || selection.Generation != policy.Generation {
						continue
					}
					result.selection = &selection
					result.supportedProtocol = result.supportedProtocol && supportsBackendTLS(backend)
					result.requiresHTTP2 = result.requiresHTTP2 || model.HTTPProfileRouteKind(route) == model.GRPCRouteKind
				}
			}
		}
	}
	return result, nil
}

// liveGatewayRoutePeers refreshes candidate routes before evaluating attachment.
// The indexed discovery only nominates candidates for policy status reporting.
func (r *Reconciler) liveGatewayRoutePeers(ctx context.Context, gateway *gatewayv1.Gateway) ([]gatewayv1.HTTPRoute, error) {
	if r.Config.DefaultGatewaysEnabled() && gateway.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll {
		// A default candidate that is absent from the cache may win profile election.
		return r.liveGraphRoutes(ctx, controller.GatewayIdentity(r.Config, gateway), gateway)
	}
	gatewayKey := controller.ObjectKeyString(client.ObjectKeyFromObject(gateway))
	filter := client.MatchingFields{controller.IndexHTTPRouteByParentGateway: gatewayKey}
	var candidates []gatewayv1.HTTPRoute
	for _, kind := range []gatewayv1.Kind{model.HTTPRouteKind, model.GRPCRouteKind} {
		routes := r.forRouteKind(kind)
		peers, err := routes.listRoutes(ctx, filter)
		if err != nil {
			return nil, err
		}
		if kind == model.HTTPRouteKind && !model.GatewayDisallowsListenerSets(gateway) {
			var sets gatewayv1.ListenerSetList
			if err := r.List(ctx, &sets, client.MatchingFields{controller.IndexListenerSetByGateway: client.ObjectKeyFromObject(gateway).String()}); err != nil {
				if !meta.IsNoMatchError(err) {
					return nil, err
				}
			}
			for _, set := range sets.Items {
				delegated, err := routes.listRoutes(ctx, client.MatchingFields{controller.IndexHTTPRouteByListenerSet: client.ObjectKeyFromObject(&set).String()})
				if err != nil {
					return nil, err
				}
				peers = append(peers, delegated...)
			}
		}
		seen := make(map[types.NamespacedName]bool, len(peers))
		for i := range peers {
			key := client.ObjectKeyFromObject(&peers[i])
			if seen[key] {
				continue
			}
			seen[key] = true
			var live gatewayv1.HTTPRoute
			if err := routes.readRoute(ctx, routes.APIReader, key, &live); err != nil {
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
	merged := make([]gatewayv1.PolicyAncestorStatus, 0, len(policy.Status.Ancestors))
	remaining := maps.Clone(desired)
	for _, entry := range policy.Status.Ancestors {
		if entry.ControllerName != name {
			merged = append(merged, entry)
			continue
		}
		key := backendtls.GatewayAncestorKey(entry.AncestorRef, policy.Namespace)
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
		if len(merged) >= backendtls.MaxAncestors {
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
