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
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// routeModelInputs keeps selection and lowering on the same request-local
// observation. The graph source supplies the complete current route inventory.
type routeModelInputs struct {
	retryBudgetAncestor       *backendtls.GatewayAncestor
	retryBudgets              map[types.NamespacedName]routeRetryBudget
	servicePersistence        map[types.NamespacedName]routeServicePersistence
	retryBudgetEnabled        bool
	servicePersistenceEnabled bool
	gateway                   *gatewayv1.Gateway
	input                     model.Input
	logical                   model.Gateway
	externalAuth              map[*model.HTTPExternalAuth]routeExternalAuthObservation
	backendClientCertificate  func(context.Context, *gatewayv1.Gateway) cloud.BackendClientCertificate
	backendTLSAvailability    map[backendTLSRequirements]backendTLSAvailability
	backendTLSAncestor        *backendtls.GatewayAncestor
	inlineEndpointRules       bool
	clusterTrustBundles       *backendtls.ClusterTrustBundles
	admission                 routeGraphAdmission

	xBackendClientCertificates graph.XBackendClientCertificateResolver
	xBackendH2CIssue           model.Diagnostic
	serviceImportH2CIssue      model.Diagnostic
}

func (inputs *routeModelInputs) routeUsesXBackendClientTLS(route *gatewayv1.HTTPRoute) bool {
	if model.HTTPProfileRouteKind(route) != model.HTTPRouteKind {
		return false
	}
	refs := model.LocalXBackendReferences(route)
	return slices.ContainsFunc(inputs.input.XBackends, func(backend model.XBackendObservation) bool {
		return backend.Spec.TLS != nil && backend.Spec.TLS.Mode == gatewayx.BackendTLSModeClientAndServer && slices.Contains(refs, backend.Key)
	})
}

type routeGraphAdmission struct {
	listenerSets     gatewaycontroller.ListenerSetProjection
	bindings         []routeGraphBinding
	eligible         map[routeObjectKey]routeGraphCandidate
	rejectedSections map[routeObjectKey][]model.ListenerSection
	active           []gatewayv1.HTTPRoute
	namespaces       []corev1.Namespace
}

func (a routeGraphAdmission) profileRejectedParent(key routeObjectKey) bool {
	_, eligible := a.eligible[key]
	return !eligible && len(a.rejectedSections[key]) != 0
}

type routeGraphCandidate struct {
	listeners              map[model.ListenerSection]*gatewayv1.Listener
	routeUID, namespaceUID types.UID
	sections               []sectionIdentity
}

type routeProfileCandidate struct {
	route     *gatewayv1.HTTPRoute
	selection routeSelection
	namespace types.UID
}

type routeObjectKey struct {
	kind gatewayv1.Kind
	key  types.NamespacedName
}

func routeKey(route *gatewayv1.HTTPRoute) routeObjectKey {
	return routeObjectKey{kind: model.HTTPProfileRouteKind(route), key: client.ObjectKeyFromObject(route)}
}

func (r *Reconciler) liveRouteGraphAdmission(ctx context.Context, gateway *gatewayv1.Gateway, route *gatewayv1.HTTPRoute) (routeGraphAdmission, error) {
	routes, err := r.liveGraphRoutes(ctx, controller.GatewayIdentity(r.Config, gateway), gateway)
	if err != nil {
		return routeGraphAdmission{}, err
	}
	return r.observeRouteGraphAdmission(ctx, gateway, routes, route)
}

func (r *Reconciler) observeRouteGraphAdmission(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (routeGraphAdmission, error) {
	identity := controller.GatewayIdentity(r.Config, gateway)
	sets, err := r.observeListenerSets(ctx, r.APIReader, gateway)
	if err != nil {
		return routeGraphAdmission{}, err
	}
	admission := routeGraphAdmission{
		listenerSets: sets,
		bindings:     r.routeGraphBindings(routes, identity),
		eligible:     make(map[routeObjectKey]routeGraphCandidate),
	}
	namespacesByName := make(map[string][]corev1.Namespace)
	var candidatesForElection []routeProfileCandidate
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() || !r.structurallySupportedRoute(route) {
			continue
		}
		parents, err := r.parentsForGateway(ctx, r.APIReader, route, gateway)
		if err != nil {
			return routeGraphAdmission{}, err
		}
		if len(parents) == 0 {
			continue
		}
		if controller.RouteClaimsGateway(r.Config, route, identity) {
			binding, err := controller.StoredRouteGatewayBinding(r.Config, route, identity)
			if err != nil {
				return routeGraphAdmission{}, err
			}
			if binding.Lifecycle == controller.RouteDetaching {
				continue
			}
		}
		selection, namespaces, err := r.selectRouteParentsWithNamespaces(ctx, r.APIReader, route, parents, admission.listenerSets, namespacesByName)
		if err != nil {
			return routeGraphAdmission{}, err
		}
		selected := selection.listeners()
		if len(selected) == 0 {
			continue
		}
		candidate := routeProfileCandidate{route: route, selection: selection}
		if len(namespaces) != 0 && slices.ContainsFunc(selected, model.ObservesRouteNamespace) {
			candidate.namespace = namespaces[0].UID
		}
		candidatesForElection = append(candidatesForElection, candidate)
	}
	for _, namespace := range namespacesByName {
		admission.namespaces = append(admission.namespaces, namespace...)
	}
	slices.SortFunc(admission.namespaces, func(a, b corev1.Namespace) int { return cmp.Compare(a.Name, b.Name) })
	admission.rejectedSections = electRouteProfiles(candidatesForElection)
	for _, candidate := range candidatesForElection {
		route := candidate.route
		key := routeKey(route)
		var sections []sectionIdentity
		listeners := candidate.selection.withoutProfileConflicts(admission.rejectedSections[key]).sections()
		for section, listener := range listeners {
			sections = append(sections, listenerSectionIdentity(section, *listener))
		}
		if len(sections) == 0 {
			continue
		}
		slices.SortFunc(sections, func(a, b sectionIdentity) int {
			return cmp.Or(cmp.Compare(a.section.ListenerSet.Namespace, b.section.ListenerSet.Namespace),
				cmp.Compare(a.section.ListenerSet.Name, b.section.ListenerSet.Name), cmp.Compare(a.section.Name, b.section.Name))
		})
		admission.eligible[key] = routeGraphCandidate{listeners: listeners, routeUID: route.UID, sections: sections, namespaceUID: candidate.namespace}
		if controller.RouteClaimsGateway(r.Config, route, identity) || requested != nil && route.UID == requested.UID && routeKey(route) == routeKey(requested) {
			admission.active = append(admission.active, *route)
		}
	}
	return admission, nil
}

// One listener section cannot direct the same effective hostname to both route kinds
// without allowing HTTPRoute fallbacks to consume unmatched RPCs. Elect the
// oldest eligible Route, then namespace/name, as allowed by the GRPCRoute API.
func electRouteProfiles(candidates []routeProfileCandidate) map[routeObjectKey][]model.ListenerSection {
	if !slices.ContainsFunc(candidates, func(candidate routeProfileCandidate) bool {
		return model.HTTPProfileRouteKind(candidate.route) == model.GRPCRouteKind
	}) {
		return nil
	}
	slices.SortStableFunc(candidates, func(a, b routeProfileCandidate) int {
		return cmp.Or(a.route.CreationTimestamp.Compare(b.route.CreationTimestamp.Time),
			cmp.Compare(a.route.Namespace, b.route.Namespace), cmp.Compare(a.route.Name, b.route.Name))
	})
	type hostProfile struct {
		port    gatewayv1.PortNumber
		section model.ListenerSection
		kind    gatewayv1.Kind
	}
	accepted := make(map[hostProfile][]gatewayv1.Hostname)
	conflicts := make(map[routeObjectKey][]model.ListenerSection)
	for _, candidate := range candidates {
		kind := model.HTTPProfileRouteKind(candidate.route)
		peer := model.GRPCRouteKind
		if kind == model.GRPCRouteKind {
			peer = model.HTTPRouteKind
		}
		key := routeKey(candidate.route)
		for _, parent := range candidate.selection {
			if parent.rejection != nil {
				continue
			}
			for _, listener := range parent.listeners {
				section := parent.section(listener.Name)
				profile := hostProfile{port: listener.Port, section: section, kind: kind}
				var hostnames []gatewayv1.Hostname
				for _, hostname := range model.IntersectHostnames(listener.Hostname, candidate.route.Spec.Hostnames) {
					hostnames = append(hostnames, gatewayv1.Hostname(hostname))
				}
				peerHosts := accepted[hostProfile{port: listener.Port, section: section, kind: peer}]
				if slices.ContainsFunc(peerHosts, func(other gatewayv1.Hostname) bool {
					return other == "" || slices.Contains(hostnames, "") || len(model.IntersectHostnames(&other, hostnames)) != 0
				}) {
					conflicts[key] = append(conflicts[key], section)
					continue
				}
				accepted[profile] = append(accepted[profile], hostnames...)
			}
		}
	}
	return conflicts
}

func (r *Reconciler) collectGraphRouteModelInputs(ctx context.Context, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) (*routeModelInputs, error) {
	admission, err := r.observeRouteGraphAdmission(ctx, gateway, routes, requested)
	if err != nil {
		return nil, err
	}
	inputs, err := r.collectRouteModelInputsForSets(ctx, r.APIReader, gateway, admission.active, admission.namespaces, admission.listenerSets)
	if err != nil {
		return nil, err
	}
	inputs.admission = admission
	return inputs, nil
}

func (r *Reconciler) validateGraphRouteModel(ctx context.Context, inputs *routeModelInputs, routes []gatewayv1.HTTPRoute, requested *gatewayv1.HTTPRoute) error {
	current, err := r.observeRouteGraphAdmission(ctx, inputs.gateway, routes, requested)
	if err != nil {
		return err
	}
	if !equality.Semantic.DeepEqual(inputs.admission.listenerSets.Sections(), current.listenerSets.Sections()) || !maps.EqualFunc(inputs.admission.eligible, current.eligible, func(a, b routeGraphCandidate) bool {
		return a.routeUID == b.routeUID && a.namespaceUID == b.namespaceUID && slices.Equal(a.sections, b.sections)
	}) ||
		!maps.EqualFunc(inputs.admission.rejectedSections, current.rejectedSections, slices.Equal[[]model.ListenerSection]) ||
		!slices.Equal(inputs.admission.bindings, current.bindings) ||
		!slices.EqualFunc(inputs.admission.active, current.active, func(a, b gatewayv1.HTTPRoute) bool {
			return model.HTTPProfileRouteKind(&a) == model.HTTPProfileRouteKind(&b) && a.UID == b.UID && a.Generation == b.Generation && a.CreationTimestamp.Equal(&b.CreationTimestamp) &&
				equality.Semantic.DeepEqual(a.Spec, b.Spec)
		}) {
		return errHTTPRouteChanged
	}
	// Namespace authorization and admitted route specs were checked above.
	// Compare only the backend dependencies used to build this model here.
	if err := inputs.validateBackends(ctx, r.APIReader); err != nil {
		return err
	}
	return inputs.validateExternalAuth(ctx, r.APIReader, r.Config)
}

func (r *Reconciler) collectRouteModelInputs(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace) (*routeModelInputs, error) {
	return r.collectRouteModelInputsForSets(ctx, reader, gateway, routes, namespaces, gatewaycontroller.ListenerSetProjection{})
}

func (r *Reconciler) collectRouteModelInputsForSets(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace, sets gatewaycontroller.ListenerSetProjection) (*routeModelInputs, error) {
	inputs, err := r.collectRouteForwardingInputs(ctx, reader, gateway, routes, namespaces, sets)
	if err != nil {
		return nil, err
	}
	if inputs.backendTLSAncestor != nil {
		inputs.backendTLSAvailability = r.discoverBackendTLSProfiles(ctx, routes, inputs.input)
	}
	for _, backend := range inputs.input.XBackends {
		if model.XBackendUsesH2C(backend.Spec) && model.CheckXBackendObservation(backend.Key, &backend, inputs.input.XBackendProfile) == nil {
			inputs.xBackendH2CIssue = r.xBackendH2CDiagnostic(ctx)
			break
		}
	}
	if len(inputs.input.ServiceImports) != 0 {
		// Reuse one capability check for all imported ports in this graph.
		for _, route := range inputs.logical.Routes {
			usesH2C := slices.ContainsFunc(route.Rules, func(rule model.Rule) bool {
				return slices.ContainsFunc(rule.Backends, func(backend model.BackendGroup) bool {
					return backend.ServiceImport.Name != "" && backend.UsesH2C()
				})
			})
			if usesH2C {
				inputs.serviceImportH2CIssue = r.backendH2CDiagnostic(ctx, "ServiceImports")
				break
			}
		}
	}
	inputs.retryBudgetEnabled = r.Config.BackendRetryBudgetEnabled()
	inputs.servicePersistenceEnabled = r.Config.XBackendSessionPersistenceEnabled() && inputs.input.XBackendProfile.MemberMode == cloud.MemberModeEndpoint
	if r.retryBudgetPolicyServed && (inputs.retryBudgetEnabled || inputs.servicePersistenceEnabled) {
		inputs.retryBudgetAncestor = &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
		inputs.retryBudgets, inputs.servicePersistence, err = collectBackendTrafficPolicies(ctx, reader, inputs.input.Services, *inputs.retryBudgetAncestor, inputs.retryBudgetEnabled, inputs.servicePersistenceEnabled)
		if err != nil {
			return nil, err
		}
	}
	inputs.externalAuth, err = inputs.observeExternalAuth(ctx, reader, r.Config)
	if err != nil {
		return nil, err
	}
	r.checkExternalAuthCapabilities(ctx, inputs.externalAuth)
	return inputs, ctx.Err()
}

// collectRouteForwardingInputs compiles the same admitted routes, transport
// references and selected Service ports for planning and policy status.
func (r *Reconciler) collectRouteForwardingInputs(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace, sets gatewaycontroller.ListenerSetProjection) (*routeModelInputs, error) {
	if !r.Config.BackendTLSPolicy && gatewaycontroller.BackendClientCertificateRef(gateway) != nil {
		return nil, errors.New("backend client certificates on a Gateway require BackendTLSPolicy observation")
	}
	routes = slices.Clone(routes)
	var originalRulePositions map[types.UID][]int
	for i := range routes {
		if !r.hasProjectableRules(&routes[i]) {
			continue
		}
		if rules, positions, dropped, _ := r.admittedHTTPRouteRules(&routes[i]); dropped != "" {
			routes[i].Spec.Rules = rules
			if originalRulePositions == nil {
				originalRulePositions = make(map[types.UID][]int)
			}
			originalRulePositions[routes[i].UID] = positions
		}
	}
	input, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, gateway, routes, namespaces, r.Config.MulticlusterServiceImports)
	if err != nil {
		return nil, err
	}
	input.ListenerSetSections = sets.Sections()
	input.OperatorHTTPS = r.Config.OperatorHTTPS
	input.RequestHeaderModifier = r.Config.RequestHeaderModifier
	input.ResponseHeaderModifier = r.Config.ResponseHeaderModifier
	input.HTTPRequestRedirect = r.Config.HTTPRequestRedirect
	input.HTTPRequestURLRewrite = r.Config.HTTPRequestURLRewrite
	input.HTTPRequestPolicy = r.Config.HTTPRequestPolicy
	input.HTTPRequestMirror = r.Config.HTTPRequestMirror
	input.CheckRouteExternalAuth = r.routeExternalAuthRulesEnabled()
	input.RouteExternalAuth = r.Config.RouteExternalAuthEnabled()
	input.RouteExternalAuthGRPC = r.Config.RouteExternalAuthGRPCEnabled()
	input.WeightedHTTPTargets = r.Config.WeightedHTTPTargets
	input.HTTPCORS = r.Config.HTTPCORS
	input.BackendRequestFilters = r.Config.BackendRequestFilters && r.Config.WeightedHTTPTargets
	input.BackendResponseHeaders = r.Config.ResponseHeaderModifier && r.Config.WeightedHTTPTargets
	input.BackendH2C = r.Config.BackendH2C
	input.RouteSessionPersistence = r.Config.RouteSessionPersistenceEnabled()
	input.DefaultGateways = r.Config.DefaultGatewaysEnabled()
	input.OriginalRulePositions = originalRulePositions
	tlsObserver := graph.NewBackendTLSObserver(reader, r.Config.ClusterTrustBundles)
	input.EnableXBackend = r.Config.XBackendEnabled()
	if input.EnableXBackend {
		profile, err := r.xBackendProfile(gateway)
		if err != nil {
			return nil, err
		}
		input.XBackendProfile = profile
		if err := graph.CollectXBackendInputs(ctx, reader, &input, tlsObserver, r.XBackendClientCertificates); err != nil {
			return nil, err
		}
	}
	if r.Config.OperatorHTTPS && slices.ContainsFunc(gateway.Spec.Listeners, func(l gatewayv1.Listener) bool { return l.Protocol == gatewayv1.HTTPSProtocolType }) {
		listeners, err := gatewaycontroller.HTTPRouteListeners(r.Config, gateway)
		if err != nil {
			return nil, err
		}
		input.Gateway = gateway.DeepCopy()
		input.Gateway.Spec.Listeners = make([]gatewayv1.Listener, 0, len(listeners))
		for _, listener := range listeners {
			if !sets.RejectsGatewayHTTPSSection(listener) {
				input.Gateway.Spec.Listeners = append(input.Gateway.Spec.Listeners, *listener)
			}
		}
	}
	if r.Config.GatewayListenerFiltersEnabled() {
		input.GatewayListenerRequestFilters = make(map[gatewayv1.SectionName]bool)
		for i := range input.Gateway.Spec.Listeners {
			listener := &input.Gateway.Spec.Listeners[i]
			if gatewaycontroller.GatewayOwnsListenerRequestFilters(r.Config, listener) {
				input.GatewayListenerRequestFilters[listener.Name] = true
			}
		}
	}
	var ancestor *backendtls.GatewayAncestor
	var inlineEndpoints bool
	if r.Config.BackendTLSPolicy {
		ancestor = &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
		inlineEndpoints = r.supportsInlineEndpointRules(gateway)
		if err := tlsObserver.CollectServiceInputs(ctx, &input, ancestor, inlineEndpoints); err != nil {
			return nil, err
		}
	}
	logical, err := model.Compile(input)
	if err != nil {
		return nil, fmt.Errorf("compile Gateway backend model: %w", err)
	}
	return &routeModelInputs{
		gateway: gateway, input: input, logical: logical,
		backendTLSAncestor: ancestor, inlineEndpointRules: inlineEndpoints, clusterTrustBundles: r.Config.ClusterTrustBundles,
		xBackendClientCertificates: r.XBackendClientCertificates,
		backendClientCertificate:   r.BackendClientCertificateForGateway,
	}, nil
}

func (r *Reconciler) supportsInlineEndpointRules(gateway *gatewayv1.Gateway) bool {
	if !r.Config.RouteSessionPersistenceEnabled() || !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return false
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	return err == nil && topology.Topology.MemberMode == cloud.MemberModeEndpoint
}

func (r *Reconciler) xBackendProfile(gateway *gatewayv1.Gateway) (model.XBackendProfile, error) {
	profile := model.XBackendProfile{
		H2C:                r.Config.BackendH2C,
		ServerTLS:          r.Config.BackendTLSPolicy,
		ClientTLS:          r.Config.XBackendClientCertificate,
		ExternalHostname:   r.Config.RoutedXBackendEnabled(gateway.Spec.GatewayClassName),
		SessionPersistence: r.Config.XBackendSessionPersistenceEnabled(),
		ServiceDNSDomains:  r.Config.XBackendServiceDNSDomains,
	}
	if !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return profile, nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	profile.MemberMode = topology.Topology.MemberMode
	return profile, err
}

func (inputs *routeModelInputs) validateBackends(ctx context.Context, reader client.Reader) error {
	current, err := graph.CollectBackendInputsWithNamespaces(ctx, reader, inputs.input.Gateway, inputs.input.Routes, inputs.input.Namespaces, inputs.input.EnableServiceImport)
	if err != nil {
		return err
	}
	current.EnableXBackend = inputs.input.EnableXBackend
	current.XBackendProfile = inputs.input.XBackendProfile
	tlsObserver := graph.NewBackendTLSObserver(reader, inputs.clusterTrustBundles)
	if err := graph.CollectXBackendInputs(ctx, reader, &current, tlsObserver, inputs.xBackendClientCertificates); err != nil {
		return err
	}
	for _, route := range inputs.input.Routes {
		kind := model.HTTPProfileRouteKind(&route)
		for _, rule := range route.Spec.Rules {
			if inputs.input.EnableServiceImport {
				for _, ref := range rule.BackendRefs {
					key, imported := model.ServiceImportReference(route.Namespace, ref.BackendObjectReference)
					if imported && model.ServiceImportReferenceAllowed(kind, route.Namespace, key, inputs.input.ReferenceGrants) !=
						model.ServiceImportReferenceAllowed(kind, route.Namespace, key, current.ReferenceGrants) {
						return errHTTPRouteChanged
					}
				}
			}
			for _, ref := range model.HTTPServiceBackendReferences(rule, true) {
				key, supported := model.BackendServiceReference(route.Namespace, ref)
				if supported && model.BackendReferenceAllowedFor(kind, route.Namespace, key, inputs.input.ReferenceGrants) !=
					model.BackendReferenceAllowedFor(kind, route.Namespace, key, current.ReferenceGrants) {
					return errHTTPRouteChanged
				}
			}
		}
	}
	if inputs.input.BackendTLSPolicies != nil {
		if err := tlsObserver.CollectServiceInputs(ctx, &current, inputs.backendTLSAncestor, inputs.inlineEndpointRules); err != nil {
			return err
		}
	}
	if inputs.retryBudgetAncestor != nil {
		budgets, persistence, err := collectBackendTrafficPolicies(ctx, reader, current.Services, *inputs.retryBudgetAncestor, inputs.retryBudgetEnabled, inputs.servicePersistenceEnabled)
		if err != nil {
			return err
		}
		if !maps.Equal(inputs.retryBudgets, budgets) || !maps.EqualFunc(inputs.servicePersistence, persistence, func(a, b routeServicePersistence) bool {
			return a.serviceUID == b.serviceUID && a.slot == b.slot && equality.Semantic.DeepEqual(a.settings, b.settings)
		}) {
			return errHTTPRouteChanged
		}
	}
	if inputs.input.BackendTrustWithdrawalOnly != current.BackendTrustWithdrawalOnly ||
		!equality.Semantic.DeepEqual(inputs.input.BackendTLSPolicies, current.BackendTLSPolicies) ||
		!equality.Semantic.DeepEqual(inputs.input.Services, current.Services) ||
		!equality.Semantic.DeepEqual(inputs.input.ServiceImports, current.ServiceImports) ||
		!equality.Semantic.DeepEqual(inputs.input.EndpointSlices, current.EndpointSlices) ||
		!equality.Semantic.DeepEqual(inputs.input.XBackends, current.XBackends) {
		return errHTTPRouteChanged
	}
	return nil
}

func (inputs *routeModelInputs) service(key types.NamespacedName) *corev1.Service {
	for i := range inputs.input.Services {
		service := &inputs.input.Services[i]
		if client.ObjectKeyFromObject(service) == key {
			return service
		}
	}
	return nil
}

func (inputs *routeModelInputs) endpointSlices(key types.NamespacedName) []discoveryv1.EndpointSlice {
	var result []discoveryv1.EndpointSlice
	for _, slice := range inputs.input.EndpointSlices {
		if slice.Namespace == key.Namespace && slice.Labels[discoveryv1.LabelServiceName] == key.Name {
			result = append(result, slice)
		}
	}
	return result
}

// routeFragment preserves all rules and backends of one route and the precedence
// assigned to its matches across the entire listener.
func (inputs *routeModelInputs) routeFragment(route *gatewayv1.HTTPRoute) model.Gateway {
	logical := inputs.logical
	logical.Routes = nil
	rejected := inputs.admission.rejectedSections[routeKey(route)]
	for _, candidate := range inputs.logical.Routes {
		if candidate.Source.UID == route.UID && candidate.Source.Namespace == route.Namespace && candidate.Source.Name == route.Name {
			if len(rejected) != 0 {
				candidate.Parents = slices.Clone(candidate.Parents)
				for i := range candidate.Parents {
					parent := &candidate.Parents[i]
					parent.Listeners = slices.DeleteFunc(slices.Clone(parent.Listeners), func(listener model.ListenerAttachment) bool {
						return slices.Contains(rejected, model.ListenerSection{ListenerSet: model.ListenerSetRefKey(parent.Reference, route.Namespace), Name: listener.Name})
					})
				}
			}
			logical.Routes = []model.Route{candidate}
			break
		}
	}
	logical.Listeners = slices.Clone(logical.Listeners)
	// The compiler orders each logical section independently. The physical
	// listener needs one distinct rank while preserving each section's order.
	sectionOffset := 0
	for i := range logical.Listeners {
		listener := &logical.Listeners[i]
		listener.Rules = nil
		for _, rule := range inputs.logical.Listeners[i].Rules {
			if rule.Route.UID == route.UID && rule.Route.Namespace == route.Namespace && rule.Route.Name == route.Name &&
				!slices.Contains(rejected, model.ListenerSection{ListenerSet: listener.ListenerSet, Name: listener.Name}) {
				rule.Precedence += sectionOffset
				listener.Rules = append(listener.Rules, rule)
			}
		}
		sectionOffset += len(inputs.logical.Listeners[i].Rules)
	}
	return logical
}
