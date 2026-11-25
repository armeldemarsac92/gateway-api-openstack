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
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestRequestMirrorGrantRevocationAndRestoration(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.HTTPRequestMirror = true
	f.reconciler.Config.FixedHTTPResponses = true
	f.reconciler.Config.WeightedHTTPTargets = true
	remote, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	remote.Namespace, remote.Name, remote.UID, remote.ResourceVersion = "shared", "shadow", "shadow-uid", ""
	endpoints.Namespace, endpoints.UID, endpoints.ResourceVersion = remote.Namespace, "shadow-slice-uid", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = remote.Name
	grant := serviceReferenceGrant(remote.Namespace, f.route.Namespace, remote.Name)
	f.create(t, remote, endpoints, grant)
	other, otherEndpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	other.Name, other.UID, other.ResourceVersion = "secondary", "secondary-uid", ""
	otherEndpoints.Name, otherEndpoints.UID, otherEndpoints.ResourceVersion = "secondary", "secondary-slice-uid", ""
	otherEndpoints.Labels[discoveryv1.LabelServiceName] = other.Name
	f.create(t, other, otherEndpoints)

	filter := gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterRequestMirror, RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{
		BackendRef: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(remote.Name), Namespace: ptr.To(gatewayv1.Namespace(remote.Namespace)), Port: f.route.Spec.Rules[0].BackendRefs[0].Port},
		Percent:    ptr.To(int32(17)),
	}}
	localMirror := *filter.DeepCopy()
	localMirror.RequestMirror.BackendRef.Name = gatewayv1.ObjectName(other.Name)
	localMirror.RequestMirror.BackendRef.Namespace = nil
	rule := &f.route.Spec.Rules[0]
	rule.Filters = []gatewayv1.HTTPRouteFilter{filter, localMirror}
	rule.BackendRefs[0].Weight = ptr.To(int32(3))
	secondary := *rule.BackendRefs[0].DeepCopy()
	secondary.Name, secondary.Weight = gatewayv1.ObjectName(other.Name), ptr.To(int32(1))
	secondary.Filters = []gatewayv1.HTTPRouteFilter{filter}
	rule.BackendRefs = append(rule.BackendRefs, secondary)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	reconcile := func(resolved metav1.ConditionStatus, reason gatewayv1.RouteConditionReason) cloud.RouteSpec {
		t.Helper()
		provider.routeSpecs = nil
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, f.route))
		require.Len(t, provider.routeSpecs, 1)
		require.Empty(t, provider.deletedRoutes)
		require.Len(t, f.route.Status.Parents, 1)
		conditions := f.route.Status.Parents[0].Conditions
		require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
		for _, conditionType := range []gatewayv1.RouteConditionType{gatewayv1.RouteConditionAccepted, gatewayv1.RouteConditionResolvedRefs} {
			condition := meta.FindStatusCondition(conditions, string(conditionType))
			require.NotNil(t, condition)
			require.Equal(t, f.route.Generation, condition.ObservedGeneration)
		}
		condition := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
		require.Equal(t, resolved, condition.Status)
		require.Equal(t, string(reason), condition.Reason)
		return provider.routeSpecs[0]
	}
	allowed := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	weighted := allowed.WeightedHTTPRules[0]
	require.Len(t, allowed.HTTPBackends, 3)
	require.Len(t, weighted.Matches[0].RequestMirrors, 2)
	require.Empty(t, weighted.Targets[0].RequestMirrors)
	require.Len(t, weighted.Targets[1].RequestMirrors, 1)
	require.Equal(t, 3, weighted.Targets[0].Weight)
	require.Equal(t, 1, weighted.Targets[1].Weight)
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForService(t.Context(), remote))
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForEndpointSlice(t.Context(), endpoints))

	previous := f.observe(t)
	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, previous.Validate(t.Context()), errHTTPRouteChanged)
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForReferenceGrant(t.Context(), grant))
	denied := reconcile(metav1.ConditionFalse, gatewayv1.RouteReasonRefNotPermitted)
	require.Len(t, denied.HTTPBackends, 2)
	require.Len(t, denied.WeightedHTTPRules[0].Matches[0].RequestMirrors, 1)
	require.Empty(t, denied.WeightedHTTPRules[0].Targets[1].RequestMirrors)
	require.Equal(t, 3, denied.WeightedHTTPRules[0].Targets[0].Weight)
	require.Equal(t, 1, denied.WeightedHTTPRules[0].Targets[1].Weight)

	revoked := f.observe(t)
	f.create(t, serviceReferenceGrant(remote.Namespace, f.route.Namespace, remote.Name))
	require.ErrorIs(t, revoked.Validate(t.Context()), errHTTPRouteChanged)
	restored := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	require.Equal(t, allowed, restored)
}

func TestRequestMirrorDropsOnlyInvalidNodePortWithBackendTLS(t *testing.T) {
	f := newRouteModelInputFixture(t)
	r := f.reconciler
	r.Config.WeightedHTTPTargets = true
	invalid := *f.service.DeepCopy()
	invalid.Name, invalid.Spec.Type = "shadow-without-nodeport", corev1.ServiceTypeClusterIP
	healthy := *f.service.DeepCopy()
	healthy.Name, healthy.Spec.Type = "healthy-shadow", corev1.ServiceTypeNodePort
	healthy.Spec.Ports[0].NodePort = 30080
	input := model.Input{
		Services: []corev1.Service{invalid, healthy},
		BackendTLSPolicies: map[model.ServicePortKey]backendtls.Selection{{
			Service: types.NamespacedName{Namespace: invalid.Namespace, Name: invalid.Name}, Port: invalid.Spec.Ports[0].Port,
		}: {Winner: &backendtls.PolicyResult{AncestorSlot: backendtls.AncestorSlotHeld, Validation: &backendtls.Validation{Hostname: "shadow.example"}}}},
	}
	filters := make([]gatewayv1.HTTPRouteFilter, 0, len(input.Services))
	for _, service := range input.Services {
		filters = append(filters, gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterRequestMirror,
			RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{BackendRef: gatewayv1.BackendObjectReference{
				Name: gatewayv1.ObjectName(service.Name), Port: ptr.To(gatewayv1.PortNumber(service.Spec.Ports[0].Port)),
			}}})
	}
	mirrors, diagnostic := model.NormalizeRequestMirrors(filters, true, true)
	require.Nil(t, diagnostic)
	model.ResolveRequestMirrorBackends(f.route, mirrors, input)
	require.NotNil(t, mirrors[0].Backend.BackendTLS.Winner)
	inputs := &routeModelInputs{input: input, backendTLSAvailability: map[backendTLSRequirements]backendTLSAvailability{
		{}: backendTLSAvailable,
	}}
	spec := cloud.RouteSpec{MemberMode: cloud.MemberModeNodePort,
		HTTPBackends: []cloud.HTTPBackend{{Name: "primary"}}}
	targets, issue, err := r.lowerRequestMirrors(t.Context(), f.reader, &spec, inputs, mirrors, corev1.NodeInternalIP, make(map[backendKey]httpBackendResolution))
	require.NoError(t, err)
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), issue.Reason)
	require.Contains(t, issue.Message, "must be type NodePort")
	require.Len(t, spec.HTTPBackends, 2)
	require.Equal(t, "primary", spec.HTTPBackends[0].Name)
	require.Equal(t, healthy.Name, spec.HTTPBackends[1].Name)
	require.Equal(t, []cloud.HTTPMirrorTarget{{BackendIndex: 1, Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite}}, targets)
}

func TestGRPCRequestMirrorReconcilesBothScopesWithGRPCRouteGrant(t *testing.T) {
	f, _, route := newRouteElectionFixture(t)
	f.reconciler.Config.HTTPRequestMirror, f.reconciler.Config.WeightedHTTPTargets = true, true
	f.reconciler.Config.RequestHeaderModifier, f.reconciler.Config.BackendRequestFilters = true, true
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	require.NoError(t, f.client.Update(t.Context(), f.service))
	shadow, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	shadow.Namespace, shadow.Name, shadow.UID, shadow.ResourceVersion = "shared", "shadow", "grpc-shadow-uid", ""
	endpoints.Namespace, endpoints.UID, endpoints.ResourceVersion = shadow.Namespace, "grpc-shadow-slice", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = shadow.Name
	grant := serviceReferenceGrant(shadow.Namespace, route.Namespace, shadow.Name)
	grant.Spec.From[0].Kind = model.GRPCRouteKind
	f.create(t, shadow, endpoints, grant)

	rule := &route.Spec.Rules[0]
	rule.Matches = []gatewayv1.GRPCRouteMatch{{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Echo")}}}
	copyRequest := gatewayv1.GRPCRouteFilter{Type: gatewayv1.GRPCRouteFilterRequestMirror, RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{
		BackendRef: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(shadow.Name), Namespace: ptr.To(gatewayv1.Namespace(shadow.Namespace)), Port: rule.BackendRefs[0].Port},
		Fraction:   &gatewayv1.Fraction{Numerator: 3, Denominator: ptr.To(int32(7))},
	}}
	metadata := func(name string) gatewayv1.GRPCRouteFilter {
		return gatewayv1.GRPCRouteFilter{Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{Set: []gatewayv1.HTTPHeader{{Name: gatewayv1.HTTPHeaderName(name), Value: "enabled"}}}}
	}
	rule.Filters = []gatewayv1.GRPCRouteFilter{copyRequest, metadata("x-rule")}
	targetCopy := *copyRequest.DeepCopy()
	targetCopy.RequestMirror.Fraction, targetCopy.RequestMirror.Percent = nil, ptr.To(int32(25))
	// Both occurrences share one pool. Only selection of the second can send its copy.
	rule.BackendRefs[0].Weight = ptr.To(int32(3))
	second := *rule.BackendRefs[0].DeepCopy()
	second.Weight = ptr.To(int32(1))
	second.Filters = []gatewayv1.GRPCRouteFilter{metadata("x-selected"), targetCopy}
	rule.BackendRefs = append(rule.BackendRefs, second)
	projected := model.ProjectGRPCRoute(route)
	require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
	route.Annotations, route.Finalizers = projected.Annotations, projected.Finalizers
	f.create(t, route)

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
	reconcile := func(resolved metav1.ConditionStatus, reason gatewayv1.RouteConditionReason) cloud.RouteSpec {
		t.Helper()
		provider.routeSpecs = nil
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, route))
		require.Len(t, route.Status.Parents, 1)
		conditions := route.Status.Parents[0].Conditions
		require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
		condition := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
		require.NotNil(t, condition)
		require.Equal(t, route.Generation, condition.ObservedGeneration)
		require.Equal(t, resolved, condition.Status)
		require.Equal(t, string(reason), condition.Reason)
		require.Empty(t, provider.deletedRoutes)
		for _, spec := range provider.routeSpecs {
			if spec.GRPC {
				return spec
			}
		}
		t.Fatal("missing GRPCRoute in provider graph")
		return cloud.RouteSpec{}
	}
	allowed := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	require.Len(t, allowed.HTTPBackends, 2)
	require.Equal(t, shadow.Name, allowed.HTTPBackends[0].Name)
	require.True(t, allowed.HTTPBackends[0].BackendH2C)
	rulePlan := allowed.WeightedHTTPRules[0]
	require.Equal(t, "/example.Echo/Echo", rulePlan.Matches[0].PathValue)
	require.Equal(t, []cloud.HTTPMirrorTarget{{BackendIndex: 0, Numerator: 3, Denominator: 7, Phase: cloud.HTTPMirrorBeforeTransforms}}, rulePlan.Matches[0].RequestMirrors)
	require.Equal(t, []cloud.HTTPWeightedTarget{
		{BackendIndex: ptr.To(1), Weight: 3},
		{BackendIndex: ptr.To(1), Weight: 1, RequestHeaderOperations: []cloud.HTTPHeaderOperation{{Operation: cloud.HTTPHeaderSet, Name: "x-selected", Value: "enabled"}},
			RequestMirrors: []cloud.HTTPMirrorTarget{{BackendIndex: 0, Numerator: 25, Denominator: 100, Phase: cloud.HTTPMirrorAfterRewrite}}},
	}, rulePlan.Targets)
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForService(t.Context(), shadow))
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForEndpointSlice(t.Context(), endpoints))

	previous, err := r.ObserveRouteGraph(t.Context(), graph.Request{
		Identity: controller.RouteIdentity(r.Config, f.gateway, projected), Action: graph.Ensure, Route: projected,
	}, f.gateway)
	require.NoError(t, err)
	grant.Spec.From[0].Kind = model.HTTPRouteKind
	require.NoError(t, f.client.Update(t.Context(), grant))
	require.ErrorIs(t, previous.Validate(t.Context()), errHTTPRouteChanged)
	denied := reconcile(metav1.ConditionFalse, gatewayv1.RouteReasonRefNotPermitted)
	require.Len(t, denied.HTTPBackends, 1)
	require.Equal(t, f.service.Name, denied.HTTPBackends[0].Name)
	require.Empty(t, denied.WeightedHTTPRules[0].Matches[0].RequestMirrors)
	for i, target := range denied.WeightedHTTPRules[0].Targets {
		require.Equal(t, ptr.To(0), target.BackendIndex)
		require.Equal(t, rulePlan.Targets[i].Weight, target.Weight)
		require.Equal(t, rulePlan.Targets[i].RequestHeaderOperations, target.RequestHeaderOperations)
		require.Empty(t, target.RequestMirrors)
	}
	checkPendingReferences := func(expected gatewayv1.RouteConditionReason) {
		t.Helper()
		status := pendingRouteStatus("waiting for the listener")
		require.NoError(t, r.resolveRouteReferences(t.Context(), model.ProjectGRPCRoute(route), f.gateway, &status))
		require.Equal(t, string(expected), status.resolvedReason)
	}
	checkPendingReferences(gatewayv1.RouteReasonRefNotPermitted)
	grant.Spec.From[0].Kind = model.GRPCRouteKind
	require.NoError(t, f.client.Update(t.Context(), grant))
	require.Equal(t, []ctrl.Request{request}, r.enqueueHTTPRoutesForReferenceGrant(t.Context(), grant))
	restored := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	require.Equal(t, allowed, restored)

	shadow.Spec.Ports[0].AppProtocol = ptr.To("http")
	require.NoError(t, f.client.Update(t.Context(), shadow))
	require.Equal(t, denied, reconcile(metav1.ConditionFalse, gatewayv1.RouteReasonUnsupportedProtocol))
	checkPendingReferences(gatewayv1.RouteReasonUnsupportedProtocol)
	require.NoError(t, f.client.Delete(t.Context(), shadow))
	require.Equal(t, denied, reconcile(metav1.ConditionFalse, gatewayv1.RouteReasonBackendNotFound))
}

func TestGRPCRequestMirrorBackendTLSNeedsH2Discovery(t *testing.T) {
	f, _, route := newRouteElectionFixture(t)
	projected := model.ProjectGRPCRoute(route)
	f.service.Spec.Ports[0].AppProtocol = ptr.To("https")
	port := f.service.Spec.Ports[0].Port
	input := model.Input{
		Services:       []corev1.Service{*f.service},
		EndpointSlices: []discoveryv1.EndpointSlice{*f.endpoints},
		BackendTLSPolicies: map[model.ServicePortKey]backendtls.Selection{{
			Service: client.ObjectKeyFromObject(f.service), Port: port,
		}: {Winner: &backendtls.PolicyResult{
			AncestorSlot: backendtls.AncestorSlotHeld,
			Validation:   &backendtls.Validation{Hostname: "shadow.example", Trust: cloud.BackendTLSTrustSystem},
		}}},
	}
	mirrors := []model.RequestMirror{{
		Backend:   model.BackendGroup{Reference: projected.Spec.Rules[0].BackendRefs[0]},
		Numerator: 1, Denominator: 1, Phase: cloud.HTTPMirrorAfterRewrite,
	}}
	model.ResolveRequestMirrorBackends(projected, mirrors, input)
	require.Nil(t, mirrors[0].Backend.Diagnostic)

	profile := backendTLSRequirements{systemCA: true, requireH2: true}
	inputs := &routeModelInputs{input: input, backendTLSAvailability: map[backendTLSRequirements]backendTLSAvailability{profile: backendTLSAvailable}}
	lower := func() (cloud.RouteSpec, []cloud.HTTPMirrorTarget, model.Diagnostic) {
		t.Helper()
		spec := cloud.RouteSpec{GRPC: true, MemberMode: cloud.MemberModeEndpoint}
		targets, issue, err := f.reconciler.lowerRequestMirrors(t.Context(), f.reader, &spec, inputs, mirrors,
			corev1.NodeInternalIP, make(map[backendKey]httpBackendResolution))
		require.NoError(t, err)
		return spec, targets, issue
	}
	spec, targets, issue := lower()
	require.Empty(t, issue.Reason)
	require.Len(t, targets, 1)
	require.Len(t, spec.HTTPBackends, 1)
	shadow := spec.HTTPBackends[targets[0].BackendIndex]
	require.Equal(t, "shadow.example", shadow.BackendTLS.Hostname)
	require.False(t, shadow.BackendH2C)
	require.NotEmpty(t, shadow.Members)

	inputs.backendTLSAvailability = map[backendTLSRequirements]backendTLSAvailability{{systemCA: true}: backendTLSAvailable}
	spec, targets, issue = lower()
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), issue.Reason)
	require.Empty(t, targets)
	require.Empty(t, spec.HTTPBackends)
}

func TestGRPCRequestMirrorReferencesAgreeWhileListenerPending(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		reason gatewayv1.RouteConditionReason
	}{
		{"missing CA", gatewayv1.RouteReasonUnsupportedValue},
		{"permission denial takes priority over a later missing Service", gatewayv1.RouteReasonRefNotPermitted},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f, _, route := newRouteElectionFixture(t)
			f.reconciler.Config.HTTPRequestMirror, f.reconciler.Config.BackendTLSPolicy = true, true
			f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
			require.NoError(t, f.client.Update(t.Context(), f.service))
			shadow := f.service.DeepCopy()
			shadow.Name, shadow.UID, shadow.ResourceVersion = "shadow", "shadow-uid", ""
			shadow.Spec.Ports[0].AppProtocol = ptr.To("https")
			mirror := func(name string, namespace *gatewayv1.Namespace) gatewayv1.GRPCRouteFilter {
				return gatewayv1.GRPCRouteFilter{Type: gatewayv1.GRPCRouteFilterRequestMirror,
					RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{BackendRef: gatewayv1.BackendObjectReference{
						Name: gatewayv1.ObjectName(name), Namespace: namespace, Port: route.Spec.Rules[0].BackendRefs[0].Port,
					}},
				}
			}
			if scenario.reason == gatewayv1.RouteReasonRefNotPermitted {
				shadow.Namespace = "other"
				route.Spec.Rules[0].Filters = []gatewayv1.GRPCRouteFilter{
					mirror(shadow.Name, ptr.To(gatewayv1.Namespace(shadow.Namespace))), mirror("missing", nil),
				}
			} else {
				route.Spec.Rules[0].Filters = []gatewayv1.GRPCRouteFilter{mirror(shadow.Name, nil)}
				f.create(t, &gatewayv1.BackendTLSPolicy{
					ObjectMeta: metav1.ObjectMeta{Namespace: shadow.Namespace, Name: "shadow-tls", UID: "shadow-policy", Generation: 1},
					Spec: gatewayv1.BackendTLSPolicySpec{
						TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
							LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(shadow.Name)},
						}},
						Validation: gatewayv1.BackendTLSPolicyValidation{
							Hostname: "shadow.example", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "missing-ca"}},
						},
					},
				})
			}
			projected := model.ProjectGRPCRoute(route)
			require.NoError(t, f.reconciler.applyRouteBinding(projected, f.gateway))
			route.Annotations, route.Finalizers = projected.Annotations, projected.Finalizers
			f.create(t, shadow, route)

			r := newTestReconciler(*f.reconciler, &recordingProvider{}).forRouteKind(model.GRPCRouteKind)
			key := client.ObjectKeyFromObject(route)
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			require.NoError(t, f.client.Get(t.Context(), key, route))
			require.Len(t, route.Status.Parents, 1)
			active := meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
			require.NotNil(t, active)
			require.Equal(t, metav1.ConditionFalse, active.Status)
			require.Equal(t, string(scenario.reason), active.Reason)

			pending := pendingRouteStatus("waiting for the listener")
			require.NoError(t, r.resolveRouteReferences(t.Context(), projected, f.gateway, &pending))
			require.Equal(t, active.Status, pending.resolved)
			require.Equal(t, active.Reason, pending.resolvedReason)
			require.Equal(t, active.Message, pending.resolvedMessage)
		})
	}
}
