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
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestHTTPRouteExternalAuthGrantAndEndpointChangesKeepPublicFallback(t *testing.T) {
	f, grant, endpoints := externalAuthFixture(t)
	grant.Spec.From[0].Kind = "Gateway"
	require.NoError(t, f.client.Update(t.Context(), grant))
	assertRejected := func() {
		t.Helper()
		input := f.observe(t)
		require.Len(t, input.Nodes, 1)
		spec := input.Nodes[0].Spec
		require.Len(t, spec.FixedHTTPRules, 1)
		require.Len(t, spec.FixedHTTPRules[0].Matches, 1)
		require.Equal(t, "/private", spec.FixedHTTPRules[0].Matches[0].PathValue)
		require.Len(t, spec.HTTPBackends, 1)
		require.Len(t, spec.HTTPBackends[0].Matches, 1)
		public := spec.HTTPBackends[0].Matches[0]
		require.Equal(t, "/", public.PathValue)
		require.Greater(t, public.Precedence, spec.FixedHTTPRules[0].Matches[0].Precedence)
		require.Nil(t, public.ExternalAuth)
	}
	assertRejected()
	provider := &recordingProvider{}
	reconciler := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions := f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	require.Nil(t, meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid)))
	resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, resolved)
	require.Equal(t, metav1.ConditionFalse, resolved.Status)
	require.Equal(t, string(gatewayv1.RouteReasonRefNotPermitted), resolved.Reason)

	grant.Spec.From[0].Kind = "HTTPRoute"
	require.NoError(t, f.client.Update(t.Context(), grant))
	assertRouteRequests(t, f.reconciler.enqueueHTTPRoutesForReferenceGrant(t.Context(), grant), f.route)
	_, err = reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions = f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionResolvedRefs)))
	authorized := f.observe(t)
	require.Len(t, authorized.Nodes, 1)
	spec := authorized.Nodes[0].Spec
	require.Empty(t, spec.FixedHTTPRules)
	require.Len(t, spec.HTTPBackends, 1)
	require.Len(t, spec.HTTPBackends[0].Matches, 2)
	for _, path := range spec.HTTPBackends[0].Matches {
		if path.PathValue == "/" {
			require.Nil(t, path.ExternalAuth)
			continue
		}
		require.Equal(t, "/private", path.PathValue)
		require.NotNil(t, path.ExternalAuth)
		require.Equal(t, "authorize-uid", path.ExternalAuth.ServiceUID)
		require.Equal(t, "/check", path.ExternalAuth.PathPrefix)
		require.Equal(t, []string{"authorization", "host"}, path.ExternalAuth.AllowedRequestHeaders)
		require.Equal(t, []string{"x-user"}, path.ExternalAuth.AllowedResponseHeaders)
		require.Len(t, path.ExternalAuth.Members, 1)
		require.Equal(t, "192.0.2.77", path.ExternalAuth.Members[0].Address)
	}
	require.NoError(t, authorized.Validate(t.Context()))
	endpoints.Endpoints[0].Conditions.Ready = ptr.To(false)
	require.NoError(t, f.client.Update(t.Context(), endpoints))
	assertRouteRequests(t, f.reconciler.enqueueHTTPRoutesForEndpointSlice(t.Context(), endpoints), f.route)
	require.ErrorIs(t, authorized.Validate(t.Context()), errHTTPRouteChanged)
	assertRejected()
	_, err = reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions = f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	require.Nil(t, meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid)))
	resolved = meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, resolved)
	require.Equal(t, metav1.ConditionUnknown, resolved.Status)
	require.Equal(t, string(gatewayv1.RouteReasonPending), resolved.Reason)

	endpoints.Endpoints[0].Conditions.Ready = ptr.To(true)
	require.NoError(t, f.client.Update(t.Context(), endpoints))
	authorized = f.observe(t)
	require.Empty(t, authorized.Nodes[0].Spec.FixedHTTPRules)
	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, authorized.Validate(t.Context()), errHTTPRouteChanged)
	assertRejected()
}

func TestHTTPRouteExternalAuthRejectsUnsupportedCompositionAndRegion(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		change  func(*testing.T, *routeModelInputFixture)
		message string
	}{
		{"backend filter", func(_ *testing.T, f *routeModelInputFixture) {
			f.route.Spec.Rules[1].BackendRefs[0].Filters = []gatewayv1.HTTPRouteFilter{{
				Type: gatewayv1.HTTPRouteFilterRequestHeaderModifier, RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{},
			}}
		}, "only filter"},
		{"TLS authorization Service", func(t *testing.T, f *routeModelInputFixture) {
			policy := &gatewayv1.BackendTLSPolicy{
				ObjectMeta: metav1.ObjectMeta{Namespace: "auth", Name: "authorize"},
				Spec: gatewayv1.BackendTLSPolicySpec{
					TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
						LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: "authorize"},
					}},
				},
			}
			f.create(t, policy)
			assertRouteRequests(t, f.reconciler.enqueueRoutesForBackendTLSPolicy(t.Context(), policy), f.route)
		}, "backend TLS is unsupported"},
		{"unenrolled region", func(_ *testing.T, f *routeModelInputFixture) {
			f.reconciler.RouteExternalAuthAvailable = func(context.Context, cloud.ListenerExternalAuth, bool) (bool, error) { return false, nil }
		}, "does not advertise"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f, _, _ := externalAuthFixture(t)
			scenario.change(t, f)
			require.NoError(t, f.client.Update(t.Context(), f.route))
			provider := &recordingProvider{}
			reconciler := newTestReconciler(*f.reconciler, provider)
			key := client.ObjectKeyFromObject(f.route)
			_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			require.Len(t, provider.routeSpecs, 1)
			spec := provider.routeSpecs[0]
			require.Len(t, spec.FixedHTTPRules, 1)
			require.Equal(t, "/private", spec.FixedHTTPRules[0].Matches[0].PathValue)
			require.Len(t, spec.HTTPBackends, 1)
			require.Len(t, spec.HTTPBackends[0].Matches, 1)
			require.Equal(t, "/", spec.HTTPBackends[0].Matches[0].PathValue)
			require.NoError(t, f.client.Get(t.Context(), key, f.route))
			conditions := f.route.Status.Parents[0].Conditions
			require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
			condition := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid))
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionTrue, condition.Status)
			require.True(t, strings.HasPrefix(condition.Message, "Dropped Rule "), condition.Message)
			require.Contains(t, condition.Message, scenario.message)
		})
	}
}

func TestHTTPRouteExternalAuthRejectsRouteWithNoSupportedRuleWhileServingLocalErrors(t *testing.T) {
	f, _, _ := externalAuthFixture(t)
	protected := f.route.Spec.Rules[1]
	protected.BackendRefs[0].Filters = []gatewayv1.HTTPRouteFilter{{
		Type: gatewayv1.HTTPRouteFilterRequestHeaderModifier, RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{},
	}}
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{protected}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	provider := &recordingProvider{}
	reconciler := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Len(t, provider.routeSpecs, 1)
	require.Empty(t, provider.deletedRoutes)
	spec := provider.routeSpecs[0]
	require.Empty(t, spec.HTTPBackends)
	require.Len(t, spec.FixedHTTPRules, 1)
	require.Len(t, spec.FixedHTTPRules[0].Matches, 1)
	require.Equal(t, "/private", spec.FixedHTTPRules[0].Matches[0].PathValue)

	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions := f.route.Status.Parents[0].Conditions
	accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, metav1.ConditionFalse, accepted.Status)
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), accepted.Reason)
	require.Contains(t, accepted.Message, "only filter")
	require.Nil(t, meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid)))
	programmed := meta.FindStatusCondition(conditions, reconciler.Config.Domain()+"/Programmed")
	require.NotNil(t, programmed)
	require.Equal(t, metav1.ConditionTrue, programmed.Status)
	require.Contains(t, programmed.Message, "serving local rejections")
}

func TestHTTPRouteExternalAuthPreservesWeightedOrigins(t *testing.T) {
	f, _, _ := externalAuthFixture(t)
	f.reconciler.Config.WeightedHTTPTargets = true
	service, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	service.Name, service.UID, service.ResourceVersion = "second-origin", "second-origin-uid", ""
	endpoints.Name, endpoints.UID, endpoints.ResourceVersion = service.Name, "second-origin-slice", ""
	endpoints.OwnerReferences = nil
	endpoints.Labels[discoveryv1.LabelServiceName] = service.Name
	endpoints.Endpoints[0].Addresses = []string{"192.0.2.78"}
	f.create(t, service, endpoints)
	protected := &f.route.Spec.Rules[1]
	second := *protected.BackendRefs[0].DeepCopy()
	protected.BackendRefs[0].Weight = ptr.To(int32(3))
	second.Name, second.Weight = gatewayv1.ObjectName(service.Name), ptr.To(int32(1))
	protected.BackendRefs = append(protected.BackendRefs, second)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	spec := f.observe(t).Nodes[0].Spec
	require.Empty(t, spec.FixedHTTPRules)
	require.Len(t, spec.WeightedHTTPRules, 1)
	rule := spec.WeightedHTTPRules[0]
	require.Len(t, rule.Targets, 2)
	require.Equal(t, 3, rule.Targets[0].Weight)
	require.Equal(t, 1, rule.Targets[1].Weight)
	require.Len(t, rule.Matches, 1)
	require.Equal(t, "/private", rule.Matches[0].PathValue)
	require.NotNil(t, rule.Matches[0].ExternalAuth)
	require.Equal(t, "authorize-uid", rule.Matches[0].ExternalAuth.ServiceUID)
}

func TestHTTPRouteChecksBodyForwardingCapabilityPerRule(t *testing.T) {
	f, _, _ := externalAuthFixture(t)
	withBody := f.route.Spec.Rules[1].DeepCopy()
	withBody.Matches[0].Path.Value = ptr.To("/upload")
	withBody.Filters[0].ExternalAuth.ForwardBody = &gatewayv1.ForwardBodyConfig{MaxSize: 65536}
	smallBody := withBody.DeepCopy()
	smallBody.Matches[0].Path.Value = ptr.To("/small-upload")
	smallBody.Filters[0].ExternalAuth.ForwardBody.MaxSize = 1024
	f.route.Spec.Rules = append(f.route.Spec.Rules, *withBody, *smallBody)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	regionalLimit := int32(1024)
	f.reconciler.RouteExternalAuthAvailable = func(_ context.Context, auth cloud.ListenerExternalAuth, _ bool) (bool, error) {
		return auth.ForwardBodyMaxSize <= regionalLimit, nil
	}
	observed := f.observe(t).Nodes[0].Spec
	require.Len(t, observed.FixedHTTPRules, 1)
	require.Equal(t, "/upload", observed.FixedHTTPRules[0].Matches[0].PathValue)
	require.True(t, slices.ContainsFunc(observed.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
		return match.PathValue == "/private" && match.ExternalAuth != nil && match.ExternalAuth.ForwardBodyMaxSize == 0
	}))

	require.True(t, slices.ContainsFunc(observed.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
		return match.PathValue == "/small-upload" && match.ExternalAuth != nil && match.ExternalAuth.ForwardBodyMaxSize == 1024
	}))

	regionalLimit = 65536
	observed = f.observe(t).Nodes[0].Spec
	require.Empty(t, observed.FixedHTTPRules)
	require.True(t, slices.ContainsFunc(observed.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
		return match.PathValue == "/upload" && match.ExternalAuth != nil && match.ExternalAuth.ForwardBodyMaxSize == 65536
	}))
}

func TestHTTPRouteGRPCExternalAuthIsOptInAndKeepsOtherRulesServing(t *testing.T) {
	f, grant, _ := externalAuthFixture(t)
	grpc := f.route.Spec.Rules[1].DeepCopy()
	grpc.Matches[0].Path.Value = ptr.To("/grpc-auth")
	auth := grpc.Filters[0].ExternalAuth
	auth.ExternalAuthProtocol = gatewayv1.HTTPRouteExternalAuthGRPCProtocol
	auth.HTTPAuthConfig = nil
	auth.GRPCAuthConfig = &gatewayv1.GRPCAuthConfig{AllowedRequestHeaders: []string{"X-User", "authorization"}}
	auth.ForwardBody = &gatewayv1.ForwardBodyConfig{MaxSize: 0}
	f.route.Spec.Rules = append(f.route.Spec.Rules, *grpc)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	assertLocalRejection := func(paths ...string) {
		t.Helper()
		spec := f.observe(t).Nodes[0].Spec
		var rejected []string
		for _, rule := range spec.FixedHTTPRules {
			for _, match := range rule.Matches {
				rejected = append(rejected, match.PathValue)
			}
		}
		require.ElementsMatch(t, paths, rejected)
		require.True(t, slices.ContainsFunc(spec.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
			return match.PathValue == "/" && match.ExternalAuth == nil
		}))
	}
	assertLocalRejection("/grpc-auth")

	f.reconciler.Config.RouteExternalAuthGRPC = true
	f.reconciler.RouteExternalAuthAvailable = func(_ context.Context, config cloud.ListenerExternalAuth, _ bool) (bool, error) {
		require.Nil(t, config.BackendTLS)
		return config.Protocol != cloud.ExternalAuthProtocolGRPC, nil
	}
	assertLocalRejection("/grpc-auth")
	key := client.ObjectKeyFromObject(f.route)
	_, err := newTestReconciler(*f.reconciler, &recordingProvider{}).Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions := f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionPartiallyInvalid)))

	f.reconciler.RouteExternalAuthAvailable = func(_ context.Context, config cloud.ListenerExternalAuth, _ bool) (bool, error) {
		return config.ForwardBodyMaxSize == 0, nil
	}
	f.route.Spec.Rules[2].Filters[0].ExternalAuth.ForwardBody.MaxSize = 1024
	require.NoError(t, f.client.Update(t.Context(), f.route))
	assertLocalRejection("/grpc-auth")
	f.reconciler.RouteExternalAuthAvailable = func(context.Context, cloud.ListenerExternalAuth, bool) (bool, error) { return true, nil }
	f.route.Spec.Rules[2].Filters[0].ExternalAuth.ForwardBody.MaxSize = cloud.MaxGRPCExternalAuthBodySize + 1
	require.NoError(t, f.client.Update(t.Context(), f.route))
	assertLocalRejection("/grpc-auth")
	f.route.Spec.Rules[2].Filters[0].ExternalAuth.ForwardBody.MaxSize = 1024
	require.NoError(t, f.client.Update(t.Context(), f.route))
	snapshot := f.observe(t)
	spec := snapshot.Nodes[0].Spec
	require.Empty(t, spec.FixedHTTPRules)
	matches := spec.HTTPBackends[0].Matches
	i := slices.IndexFunc(matches, func(match cloud.HTTPPathMatch) bool { return match.PathValue == "/grpc-auth" })
	require.GreaterOrEqual(t, i, 0)
	protected := matches[i]
	require.Equal(t, cloud.ExternalAuthProtocolGRPC, protected.ExternalAuth.Protocol)
	require.Equal(t, int32(1024), protected.ExternalAuth.ForwardBodyMaxSize)
	require.Equal(t, []string{"authorization", "x-user"}, protected.ExternalAuth.AllowedRequestHeaders)
	public := slices.IndexFunc(matches, func(match cloud.HTTPPathMatch) bool { return match.PathValue == "/" })
	require.GreaterOrEqual(t, public, 0)
	require.Less(t, protected.Precedence, matches[public].Precedence)

	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, snapshot.Validate(t.Context()), errHTTPRouteChanged)
	assertLocalRejection("/private", "/grpc-auth")
}

func TestHTTPRouteGRPCExternalAuthWeightedLossPreservesSiblingRules(t *testing.T) {
	f, _, _ := externalAuthFixture(t)
	f.reconciler.Config.RouteExternalAuthGRPC, f.reconciler.Config.WeightedHTTPTargets = true, true
	f.reconciler.RouteExternalAuthAvailable = func(_ context.Context, _ cloud.ListenerExternalAuth, weighted bool) (bool, error) {
		return !weighted, nil
	}
	single := &f.route.Spec.Rules[1]
	filter := single.Filters[0].ExternalAuth
	filter.ExternalAuthProtocol, filter.HTTPAuthConfig = gatewayv1.HTTPRouteExternalAuthGRPCProtocol, nil
	filter.GRPCAuthConfig = &gatewayv1.GRPCAuthConfig{AllowedRequestHeaders: []string{":authority"}}
	weighted := single.DeepCopy()
	weighted.Matches[0].Path.Value = ptr.To("/weighted")
	weighted.BackendRefs = append(weighted.BackendRefs, gatewayv1.HTTPBackendRef{
		BackendRef: gatewayv1.BackendRef{BackendObjectReference: filter.BackendRef},
	})
	f.route.Spec.Rules = append(f.route.Spec.Rules, *weighted)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	spec := f.observe(t).Nodes[0].Spec
	require.Len(t, spec.FixedHTTPRules, 1)
	require.Equal(t, "/weighted", spec.FixedHTTPRules[0].Matches[0].PathValue)
	require.Empty(t, spec.WeightedHTTPRules)
	require.Len(t, spec.HTTPBackends, 1)
	require.True(t, slices.ContainsFunc(spec.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
		return match.PathValue == "/" && match.ExternalAuth == nil
	}))
	require.True(t, slices.ContainsFunc(spec.HTTPBackends[0].Matches, func(match cloud.HTTPPathMatch) bool {
		return match.PathValue == "/private" && match.ExternalAuth != nil &&
			slices.Equal(match.ExternalAuth.AllowedRequestHeaders, []string{":authority"})
	}))
}

func TestHTTPRouteGRPCExternalAuthUsesBackendTLSPolicyAndFencesChanges(t *testing.T) {
	f, _, _ := externalAuthFixture(t)
	f.reconciler.Config.RouteExternalAuthGRPC, f.reconciler.Config.BackendTLSPolicy = true, true
	f.reconciler.RouteExternalAuthAvailable = func(_ context.Context, config cloud.ListenerExternalAuth, _ bool) (bool, error) {
		tls := config.BackendTLS
		require.NotNil(t, tls)
		require.Equal(t, cloud.BackendTLSTrustSystem, tls.Trust)
		require.Equal(t, "authorize.example.test", tls.Hostname)
		require.True(t, tls.RequireH2)
		return true, nil
	}
	filter := f.route.Spec.Rules[1].Filters[0].ExternalAuth
	filter.ExternalAuthProtocol, filter.HTTPAuthConfig = gatewayv1.HTTPRouteExternalAuthGRPCProtocol, nil
	filter.GRPCAuthConfig = &gatewayv1.GRPCAuthConfig{}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "auth", Name: "authorize-tls", UID: "auth-policy-uid"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: "authorize"}}},
			Validation: gatewayv1.BackendTLSPolicyValidation{Hostname: "authorize.example.test", WellKnownCACertificates: ptr.To(gatewayv1.WellKnownCACertificatesSystem)},
		},
	}
	f.create(t, policy)
	policyStatus := &backendTLSPolicyStatusReconciler{routes: f.reconciler}
	_, err := policyStatus.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(policy), policy))
	require.Len(t, policy.Status.Ancestors, 1)
	require.True(t, meta.IsStatusConditionTrue(policy.Status.Ancestors[0].Conditions, string(gatewayv1.PolicyConditionAccepted)))

	snapshot := f.observe(t)
	spec := snapshot.Nodes[0].Spec
	require.Empty(t, spec.FixedHTTPRules)
	require.Len(t, spec.HTTPBackends[0].Matches, 2)
	for _, match := range spec.HTTPBackends[0].Matches {
		if match.PathValue == "/private" {
			require.NotNil(t, match.ExternalAuth.BackendTLS)
			require.Empty(t, match.ExternalAuth.AllowedRequestHeaders)
		}
	}
	policy.Spec.Validation.Hostname = "*"
	require.NoError(t, f.client.Update(t.Context(), policy))
	require.ErrorIs(t, snapshot.Validate(t.Context()), errHTTPRouteChanged)
	require.Len(t, f.observe(t).Nodes[0].Spec.FixedHTTPRules, 1)

	policy.Spec.Validation.Hostname = "authorize.example.test"
	require.NoError(t, f.client.Update(t.Context(), policy))
	f.gateway.Spec.TLS = &gatewayv1.GatewayTLSConfig{Backend: &gatewayv1.GatewayBackendTLS{
		ClientCertificateRef: &gatewayv1.SecretObjectReference{Name: "authorizer-client"},
	}}
	require.NoError(t, f.client.Update(t.Context(), f.gateway))
	certificate := cloud.BackendClientCertificate{Reference: "https://barbican.example/v1/secrets/current-client"}
	f.reconciler.BackendClientCertificateForGateway = func(context.Context, *gatewayv1.Gateway) cloud.BackendClientCertificate {
		return certificate
	}
	snapshot = f.observe(t)
	require.Empty(t, snapshot.Nodes[0].Spec.FixedHTTPRules)
	certificate = cloud.BackendClientCertificate{Denied: true}
	require.ErrorIs(t, snapshot.Validate(t.Context()), errHTTPRouteChanged)
	spec = f.observe(t).Nodes[0].Spec
	require.Len(t, spec.FixedHTTPRules, 1)
	require.Equal(t, "/private", spec.FixedHTTPRules[0].Matches[0].PathValue)
	require.Len(t, spec.HTTPBackends, 1)
	require.Equal(t, "/", spec.HTTPBackends[0].Matches[0].PathValue)
}

func externalAuthFixture(t *testing.T) (*routeModelInputFixture, *gatewayv1.ReferenceGrant, *discoveryv1.EndpointSlice) {
	t.Helper()
	config := testConfig()
	config.GatewayAPIChannel = features.FeatureChannelExperimental
	config.RouteExternalAuth, config.FixedHTTPResponses = true, true
	f := newRouteModelInputFixtureWithConfig(t, config)
	f.reconciler.RouteExternalAuthAvailable = func(context.Context, cloud.ListenerExternalAuth, bool) (bool, error) { return true, nil }
	auth := f.service.DeepCopy()
	auth.Namespace, auth.Name, auth.UID, auth.ResourceVersion = "auth", "authorize", "authorize-uid", ""
	endpoints := f.endpoints.DeepCopy()
	endpoints.Namespace, endpoints.Name, endpoints.UID, endpoints.ResourceVersion = auth.Namespace, auth.Name, "authorize-slice", ""
	endpoints.OwnerReferences = nil
	endpoints.Labels[discoveryv1.LabelServiceName] = auth.Name
	endpoints.Endpoints[0].Addresses = []string{"192.0.2.77"}
	grant := serviceReferenceGrant(auth.Namespace, f.route.Namespace, auth.Name)
	f.create(t, auth, endpoints, grant)
	public := *f.route.Spec.Rules[0].DeepCopy()
	public.Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Value: ptr.To("/")}}}
	protected := *public.DeepCopy()
	protected.Matches[0].Path.Value = ptr.To("/private")
	protected.Filters = []gatewayv1.HTTPRouteFilter{{
		Type: gatewayv1.HTTPRouteFilterExternalAuth,
		ExternalAuth: &gatewayv1.HTTPExternalAuthFilter{
			ExternalAuthProtocol: gatewayv1.HTTPRouteExternalAuthHTTPProtocol,
			BackendRef: gatewayv1.BackendObjectReference{Namespace: ptr.To(gatewayv1.Namespace(auth.Namespace)), Name: gatewayv1.ObjectName(auth.Name),
				Port: ptr.To(gatewayv1.PortNumber(auth.Spec.Ports[0].Port))},
			HTTPAuthConfig: &gatewayv1.HTTPAuthConfig{
				Path: "/check", AllowedRequestHeaders: []string{"Authorization", ":authority"}, AllowedResponseHeaders: []string{"X-User"},
			},
		},
	}}
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{public, protected}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	return f, grant, endpoints
}
