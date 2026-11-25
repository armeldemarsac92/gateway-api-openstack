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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
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
