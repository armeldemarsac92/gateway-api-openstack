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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestRouteSessionPersistenceIsolationAndServicePolicy(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	cfg := &f.reconciler.Config
	cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets, cfg.RouteSessionPersistence = true, true, true
	cfg.BackendRetryBudget = false
	cookie, header, plain := f.route.Spec.Rules[0].DeepCopy(), f.route.Spec.Rules[0].DeepCopy(), f.route.Spec.Rules[0].DeepCopy()
	cookie.Name = ptr.To(gatewayv1.SectionName("checkout"))
	cookie.SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	header.SessionPersistence = &gatewayv1.SessionPersistence{  //nolint:staticcheck // The v1.7 Experimental field.
		Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "X-Rule-Affinity"},
	}
	for i, path := range []string{"/checkout", "/reports", "/other"} {
		[]*gatewayv1.HTTPRouteRule{cookie, header, plain}[i].Matches = []gatewayv1.HTTPRouteMatch{{
			Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To(path)},
		}}
	}
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{*cookie, *header, *plain}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	desired := f.observe(t).Nodes[0].Spec
	require.Len(t, desired.HTTPBackends, 3)
	cookiePool, headerPool, ordinaryPool := desired.HTTPBackends[0], desired.HTTPBackends[1], desired.HTTPBackends[2]
	require.Equal(t, "name/checkout", cookiePool.SessionPersistenceRule)
	require.Equal(t, "index/1", headerPool.SessionPersistenceRule)
	require.Regexp(t, "^route-[a-f0-9]{64}$", cookiePool.ServiceSessionPersistence.CookieName)
	require.Equal(t, "x-rule-affinity", headerPool.ServiceSessionPersistence.Header.Name)
	require.Nil(t, ordinaryPool.ServiceSessionPersistence)

	cfg.XBackendSessionPersistence = true
	policy := newServiceHeaderPolicy(f)
	retryPolicyStatusForGateway(f, policy)
	f.create(t, policy)
	desired = f.observe(t).Nodes[0].Spec
	require.Equal(t, cookiePool.ServiceSessionPersistence, desired.HTTPBackends[0].ServiceSessionPersistence)
	require.Equal(t, headerPool.ServiceSessionPersistence, desired.HTTPBackends[1].ServiceSessionPersistence)
	require.Equal(t, string(backendPolicyPersistence(policy).Header.Name), desired.HTTPBackends[2].ServiceSessionPersistence.Header.Name)

	f.route.Spec.Rules[1].SessionPersistence = nil //nolint:staticcheck // Remove the v1.7 Experimental field.
	require.NoError(t, f.client.Update(t.Context(), f.route))
	desired = f.observe(t).Nodes[0].Spec
	require.Len(t, desired.HTTPBackends, 2)
	require.Equal(t, cookiePool.ServiceSessionPersistence, desired.HTTPBackends[0].ServiceSessionPersistence)
	require.Empty(t, desired.HTTPBackends[1].SessionPersistenceRule)
	require.Equal(t, string(backendPolicyPersistence(policy).Header.Name), desired.HTTPBackends[1].ServiceSessionPersistence.Header.Name)
	require.Len(t, desired.HTTPBackends[1].Matches, 2)

	// Backend resolution continues to provide the usual failure action.
	require.NoError(t, f.client.Delete(t.Context(), f.service))
	desired = f.observe(t).Nodes[0].Spec
	require.Len(t, desired.HTTPBackends, 2)
	for _, backend := range desired.HTTPBackends {
		require.True(t, backend.InvalidReference)
		require.Empty(t, backend.Members)
		require.Nil(t, backend.ServiceSessionPersistence)
	}
}

func TestGRPCRouteSessionPersistenceAdmission(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	cfg := &f.reconciler.Config
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets = true, true, true
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	require.NoError(t, f.client.Update(t.Context(), f.service))
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: f.route.Name, UID: "grpc"}}
	route.Spec.ParentRefs = f.route.Spec.ParentRefs
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{
		grpcRule(gatewayv1.ObjectName(f.service.Name), gatewayv1.GRPCRouteMatch{}),
	}
	rule := &route.Spec.Rules[0]
	rule.Name = ptr.To(gatewayv1.SectionName("rpc"))
	rule.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	rule.SessionPersistence = &gatewayv1.SessionPersistence{ //nolint:staticcheck // The v1.7 Experimental field.
		Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "X-Rpc-Affinity"},
	}
	build := func() (cloud.RouteSpec, error) {
		return f.reconciler.forRouteKind(model.GRPCRouteKind).buildRouteModelWithReader(t.Context(), f.reader, model.ProjectGRPCRoute(route), f.gateway)
	}
	_, err := build()
	require.ErrorContains(t, err, "--experimental-route-session-persistence")
	cfg.RouteSessionPersistence = true
	desired, err := build()
	require.NoError(t, err)
	require.Len(t, desired.HTTPBackends, 1)
	backend := desired.HTTPBackends[0]
	require.True(t, backend.BackendH2C)
	require.Equal(t, "name/rpc", backend.SessionPersistenceRule)
	require.Equal(t, "x-rpc-affinity", backend.ServiceSessionPersistence.Header.Name)

	rule.BackendRefs = append(rule.BackendRefs, *rule.BackendRefs[0].DeepCopy())
	rule.BackendRefs[0].Weight, rule.BackendRefs[1].Weight = ptr.To(int32(3)), ptr.To(int32(1))
	_, err = build()
	require.ErrorContains(t, err, "exactly one backendRef")

	rule.BackendRefs = rule.BackendRefs[:1]
	rule.BackendRefs[0].Weight = ptr.To(int32(0))
	desired, err = build()
	require.NoError(t, err)
	require.Empty(t, desired.HTTPBackends)
	require.Len(t, desired.WeightedHTTPRules, 1)
	require.Nil(t, desired.WeightedHTTPRules[0].Targets[0].BackendIndex)

	// The same refusal is partial when another method can be served.
	rule.BackendRefs[0].Weight = ptr.To(int32(3))
	rule.BackendRefs = append(rule.BackendRefs, *rule.BackendRefs[0].DeepCopy())
	rule.BackendRefs[1].Weight = ptr.To(int32(1))
	sibling := grpcRule(gatewayv1.ObjectName(f.service.Name), gatewayv1.GRPCRouteMatch{
		Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Echo")},
	})
	sibling.BackendRefs[0].Port = rule.BackendRefs[0].Port
	route.Spec.Rules = append(route.Spec.Rules, sibling)
	desired, err = build()
	require.NoError(t, err)
	require.Len(t, desired.HTTPBackends, 1)
	require.Empty(t, desired.HTTPBackends[0].SessionPersistenceRule)
	require.Equal(t, "/example.Echo/Echo", desired.HTTPBackends[0].Matches[0].PathValue)
	require.Empty(t, desired.WeightedHTTPRules)
}

func TestHTTPRouteSessionPersistenceRefusesOnlyInvalidRule(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.RouteSessionPersistence = true
	first, second := f.route.Spec.Rules[0].DeepCopy(), f.route.Spec.Rules[0].DeepCopy()
	first.Name = ptr.To(gatewayv1.SectionName("invalid"))
	first.SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	first.BackendRefs = append(first.BackendRefs, *first.BackendRefs[0].DeepCopy())
	first.BackendRefs[1].Name = "another-service"
	first.BackendRefs[0].Weight, first.BackendRefs[1].Weight = ptr.To(int32(3)), ptr.To(int32(1))
	second.SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{*first, *second}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Empty(t, provider.deletedRoutes)
	require.Len(t, provider.routeSpecs, 1)
	require.Len(t, provider.routeSpecs[0].HTTPBackends, 1)
	require.Equal(t, "index/1", provider.routeSpecs[0].HTTPBackends[0].SessionPersistenceRule)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	require.Len(t, f.route.Status.Parents, 1)
	conditions := f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	partial := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid))
	require.NotNil(t, partial)
	require.Equal(t, metav1.ConditionTrue, partial.Status)
	require.Contains(t, partial.Message, "invalid (rule sessionPersistence requires a forwarding rule with exactly one backendRef)")
}

func TestRouteSessionPersistencePreservesSiblingWhenBackendRequiresTLS(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.RouteSessionPersistence = true
	f.reconciler.Config.BackendTLSPolicy = true
	f.reconciler.BackendTLSSystemCAAvailable = func(context.Context, bool, bool, bool) (bool, error) { return true, nil }
	sibling, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	sibling.Name, sibling.UID, sibling.ResourceVersion = "ordinary", "sibling-service", ""
	endpoints.Name, endpoints.UID, endpoints.ResourceVersion = "ordinary-endpoints", "sibling-endpoints", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = sibling.Name
	f.create(t, sibling, endpoints)
	inline := pathBackendRule("/inline", gatewayv1.PathMatchExact, gatewayv1.ObjectName(f.service.Name))
	inline.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	inline.Name = ptr.To(gatewayv1.SectionName("inline"))
	inline.SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	ordinary := pathBackendRule("/ordinary", gatewayv1.PathMatchExact, gatewayv1.ObjectName(sibling.Name))
	ordinary.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(sibling.Spec.Ports[0].Port))
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{inline, ordinary}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Len(t, provider.routeSpecs, 1)
	require.Len(t, provider.routeSpecs[0].HTTPBackends, 2)
	require.NotEmpty(t, provider.routeSpecs[0].HTTPBackends[0].Members)
	siblingPool := provider.routeSpecs[0].HTTPBackends[1]
	require.NotEmpty(t, siblingPool.Members)

	f.create(t, &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "require-tls", UID: "tls-policy", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "backend.example.test", WellKnownCACertificates: ptr.To(gatewayv1.WellKnownCACertificatesSystem),
			},
		},
	})
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Empty(t, provider.deletedRoutes)
	require.Len(t, provider.routeSpecs, 2)
	backends := provider.routeSpecs[1].HTTPBackends
	require.Len(t, backends, 1)
	require.Equal(t, siblingPool.Members, backends[0].Members)
	require.Equal(t, openstackidentity.HTTPBackendPoolKey(siblingPool), openstackidentity.HTTPBackendPoolKey(backends[0]))
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	require.Len(t, f.route.Status.Parents, 1)
	conditions := f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)))
	partial := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid))
	require.NotNil(t, partial)
	require.Equal(t, metav1.ConditionTrue, partial.Status)
	require.Contains(t, partial.Message, "inline (rule sessionPersistence requires")
	require.True(t, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed"))
}

func TestRouteSessionPersistenceHeaderScopeFollowsServiceUID(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.RouteSessionPersistence = true
	f.route.Spec.Rules[0].SessionPersistence = &gatewayv1.SessionPersistence{ //nolint:staticcheck // The v1.7 Experimental field.
		Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "X-Rule-Affinity"},
	}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	before := f.observe(t)
	spec := before.Nodes[0].Spec
	old := spec.HTTPBackends[0]
	require.Equal(t, string(f.service.UID), old.SessionPersistenceServiceUID)
	replaceRouteModelInputObject(t, f.client, f.service)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)
	next := f.observe(t).Nodes[0].Spec
	replacement := next.HTTPBackends[0]
	require.Equal(t, old.Members, replacement.Members)
	require.Equal(t, old.ServiceSessionPersistence, replacement.ServiceSessionPersistence)
	require.NotEqual(t, openstackidentity.HTTPBackendPoolKey(old), openstackidentity.HTTPBackendPoolKey(replacement))
	require.NotEqual(t, servicePersistenceRevision(spec), servicePersistenceRevision(next))
}

func TestRouteSessionPersistenceWithdrawsUnauthorizedService(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.RouteSessionPersistence = true, true
	remote, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	remote.Namespace, remote.UID, remote.ResourceVersion = "shared", "remote-service", ""
	endpoints.Namespace, endpoints.UID, endpoints.ResourceVersion = remote.Namespace, "remote-endpoints", ""
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: gatewayv1.Namespace(f.route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(remote.Name))}},
		},
	}
	f.create(t, remote, endpoints, grant)
	inline := pathBackendRule("/shared", gatewayv1.PathMatchExact, gatewayv1.ObjectName(remote.Name))
	inline.BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	inline.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(remote.Spec.Ports[0].Port))
	inline.SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	f.route.Spec.Rules = append(f.route.Spec.Rules, inline)
	require.NoError(t, f.client.Update(t.Context(), f.route))
	before := f.observe(t)
	require.Len(t, before.Nodes[0].Spec.HTTPBackends, 2)
	remotePool := before.Nodes[0].Spec.HTTPBackends[1]
	require.NotEmpty(t, remotePool.Members)
	require.NotNil(t, remotePool.ServiceSessionPersistence)
	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Empty(t, provider.deletedRoutes)
	require.Len(t, provider.routeSpecs, 1)
	backends := provider.routeSpecs[0].HTTPBackends
	require.Len(t, backends, 2)
	require.Equal(t, before.Nodes[0].Spec.HTTPBackends[0], backends[0])
	rejected := backends[1]
	require.True(t, rejected.InvalidReference)
	require.Empty(t, rejected.Members)
	require.Nil(t, rejected.ServiceSessionPersistence)
	require.NotEqual(t, openstackidentity.HTTPBackendPoolKey(remotePool), openstackidentity.HTTPBackendPoolKey(rejected))
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	require.Len(t, f.route.Status.Parents, 1)
	resolved := meta.FindStatusCondition(f.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NotNil(t, resolved)
	require.Equal(t, metav1.ConditionFalse, resolved.Status)
	require.Equal(t, string(gatewayv1.RouteReasonRefNotPermitted), resolved.Reason)
}

func TestRouteSessionPersistenceOverrideDoesNotOccupyServicePolicyAncestor(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.RouteSessionPersistence, f.reconciler.Config.XBackendSessionPersistence = true, true
	f.reconciler.XBackendHeaderAvailable = func(context.Context) (bool, error) { return true, nil }
	f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }
	f.route.Spec.Rules[0].SessionPersistence = &gatewayv1.SessionPersistence{} //nolint:staticcheck // The v1.7 Experimental field.
	require.NoError(t, f.client.Update(t.Context(), f.route))
	policy := newServiceHeaderPolicy(f)
	retryPolicyStatusForGateway(f, policy)
	f.create(t, policy)
	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	ancestors, err := status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	require.Empty(t, ancestors)
	require.Empty(t, mergeRetryPolicyAncestors(policy, f.reconciler.Config.ControllerName, ancestors))

	policy.Spec.RetryConstraint = &gatewayx.RetryConstraint{}
	require.NoError(t, f.client.Update(t.Context(), policy))
	ancestors, err = status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	parent := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
	require.Len(t, ancestors, 1)
	require.Equal(t, metav1.ConditionTrue, ancestors[parent].Status)
	require.Equal(t, "The Service retry constraint is accepted", ancestors[parent].Message)
}

func TestServiceSessionPersistencePolicyLifecycle(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.XBackendSessionPersistence = true
	f.reconciler.XBackendCookieAvailable = func(_ context.Context, requirements cloud.HTTPSessionCookieRequirements) (bool, error) {
		return requirements == cloud.HTTPSessionCookiePermanent|cloud.HTTPSessionCookiePath, nil
	}
	f.reconciler.XBackendHeaderAvailable = func(context.Context) (bool, error) { return true, nil }
	f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }
	policy := &gatewayx.XBackendTrafficPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "affinity", UID: "affinity-policy", Generation: 1},
		Spec: gatewayx.BackendTrafficPolicySpec{
			TargetRefs:      []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)}},
			RetryConstraint: &gatewayx.RetryConstraint{},
			SessionPersistence: &gatewayv1.SessionPersistence{ //nolint:staticcheck // Exercise the v1.7 Experimental field.
				AbsoluteTimeout: ptr.To(gatewayv1.Duration("30s")),
				Cookie:          &gatewayv1.CookieConfig{Path: ptr.To("/reports"), LifetimeType: ptr.To(gatewayv1.PermanentCookieLifetimeType)},
			},
		},
	}
	f.create(t, policy)

	pending := f.observe(t)
	require.True(t, pending.Nodes[0].Spec.ServiceSessionPersistencePending)
	require.Nil(t, pending.Nodes[0].Spec.ServiceSessionPersistence)
	for range backendtls.MaxAncestors {
		policy.Status.Ancestors = append(policy.Status.Ancestors, gatewayv1.PolicyAncestorStatus{ControllerName: "test.example/foreign"})
	}
	require.NoError(t, f.client.Update(t.Context(), policy))
	require.ErrorIs(t, pending.Validate(t.Context()), errHTTPRouteChanged)
	full := f.observe(t)
	require.False(t, full.Nodes[0].Spec.ServiceSessionPersistencePending)
	require.Nil(t, full.Nodes[0].Spec.ServiceSessionPersistence)
	snapshot, err := f.reconciler.buildRouteValidationSnapshot(t.Context(), f.route, f.gateway, routeSnapshotForEnsure)
	require.NoError(t, err)
	require.True(t, snapshot.retryBudgetAncestorFull)

	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	ancestors, err := status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	parent := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
	require.Equal(t, metav1.ConditionTrue, ancestors[parent].Status)
	require.Equal(t, policy.Status.Ancestors, mergeRetryPolicyAncestors(policy, f.reconciler.Config.ControllerName, ancestors))
	retryPolicyStatusForGateway(f, policy)
	require.NoError(t, f.client.Update(t.Context(), policy))

	cookie := f.observe(t)
	require.NoError(t, cookie.Validate(t.Context()))
	spec := cookie.Nodes[0].Spec
	require.NotNil(t, spec.BackendRetryBudget)
	require.False(t, spec.ServiceSessionPersistencePending)
	settings := spec.ServiceSessionPersistence
	require.NotNil(t, settings)
	require.Regexp(t, "^service-[a-f0-9]{64}$", settings.CookieName)
	require.Equal(t, "/reports", *settings.CookiePath)
	require.True(t, settings.CookieDeadline.Permanent)
	require.EqualValues(t, 30000, settings.CookieDeadline.TimeoutMS)
	cookieRevision := servicePersistenceRevision(spec)
	require.NotEmpty(t, cookieRevision)

	policy.Generation++
	*backendPolicyPersistence(policy) = gatewayv1.SessionPersistence{
		Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "X-Reports-Affinity"},
	}
	policy.Spec.RetryConstraint = nil
	require.NoError(t, f.client.Update(t.Context(), policy))
	require.ErrorIs(t, cookie.Validate(t.Context()), errHTTPRouteChanged)

	f.reconciler.Config.BackendRetryBudget = false
	header := f.observe(t)
	spec = header.Nodes[0].Spec
	require.Nil(t, spec.BackendRetryBudget)
	require.Equal(t, &cloud.ServiceSessionPersistence{Header: &cloud.HTTPSessionHeaderPersistence{Name: "x-reports-affinity"}}, spec.ServiceSessionPersistence)
	require.NotEqual(t, cookieRevision, servicePersistenceRevision(spec))

	require.NoError(t, f.client.Delete(t.Context(), policy))
	require.ErrorIs(t, header.Validate(t.Context()), errHTTPRouteChanged)
	spec = f.observe(t).Nodes[0].Spec
	require.Nil(t, spec.ServiceSessionPersistence)
	require.Empty(t, servicePersistenceRevision(spec))
}

func TestGRPCServiceSessionPersistenceUsesH2CBackend(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.BackendRetryBudget = false
	f.reconciler.Config.BackendH2C, f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.XBackendSessionPersistence = true, true, true
	f.reconciler.XBackendHeaderAvailable = func(context.Context) (bool, error) { return true, nil }
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	require.NoError(t, f.client.Update(t.Context(), f.service))
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: "grpc-affinity", UID: "grpc-affinity"}}
	route.Spec.ParentRefs = f.route.Spec.ParentRefs
	route.Spec.Hostnames = []gatewayv1.Hostname{"rpc.example.test"}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule(gatewayv1.ObjectName(f.service.Name), gatewayv1.GRPCRouteMatch{
		Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Echo")},
	})}
	route.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	projection := model.ProjectGRPCRoute(route)
	projection.Name = f.route.Name
	policy := &gatewayx.XBackendTrafficPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "grpc-affinity", UID: "grpc-affinity-policy"},
		Spec: gatewayx.BackendTrafficPolicySpec{
			TargetRefs: []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)}},
			SessionPersistence: &gatewayv1.SessionPersistence{ //nolint:staticcheck // Exercise the v1.7 Experimental field.
				Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "x-grpc-affinity"},
			},
		},
	}
	retryPolicyStatusForGateway(f, policy)
	f.create(t, policy)
	r := f.reconciler.forRouteKind(model.GRPCRouteKind)
	spec, err := r.buildRouteModelWithReader(t.Context(), f.reader, projection, f.gateway)
	require.NoError(t, err)
	require.True(t, spec.GRPC)
	require.Len(t, spec.HTTPBackends, 1)
	backend := spec.HTTPBackends[0]
	require.True(t, backend.BackendH2C)
	require.NotNil(t, backend.ServiceSessionPersistence)
	require.Equal(t, "x-grpc-affinity", backend.ServiceSessionPersistence.Header.Name)
	require.Nil(t, backend.BackendRetryBudget)

	require.NoError(t, f.client.Delete(t.Context(), f.route))
	f.create(t, route)
	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	ancestors, err := status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	parent := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
	require.Equal(t, metav1.ConditionTrue, ancestors[parent].Status)

	backendPolicyPersistence(policy).Header.Name = "x-grpc-affinity-bin"
	require.NoError(t, f.client.Update(t.Context(), policy))
	spec, err = r.buildRouteModelWithReader(t.Context(), f.reader, projection, f.gateway)
	require.NoError(t, err)
	require.Len(t, spec.HTTPBackends, 1)
	require.Nil(t, spec.HTTPBackends[0].ServiceSessionPersistence)
	require.True(t, spec.HTTPBackends[0].BackendH2C)
	ancestors, err = status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	require.Equal(t, metav1.ConditionFalse, ancestors[parent].Status)
	require.Contains(t, ancestors[parent].Message, "-bin")

	neighbor := f.candidate("http-neighbor")
	neighbor.Spec.Hostnames = []gatewayv1.Hostname{"http.example.test"}
	f.create(t, neighbor)
	httpSpec, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, neighbor, f.gateway)
	require.NoError(t, err)
	require.Len(t, httpSpec.HTTPBackends, 1)
	require.Equal(t, "x-grpc-affinity-bin", httpSpec.HTTPBackends[0].ServiceSessionPersistence.Header.Name)
	ancestors, err = status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	require.Equal(t, metav1.ConditionTrue, ancestors[parent].Status)
	require.Contains(t, ancestors[parent].Message, "HTTPRoute consumers")
	require.Contains(t, ancestors[parent].Message, "GRPCRoute")
}

func TestServiceSessionPersistencePolicyAcceptsAvailableSettingsIndependently(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.XBackendHeaderAvailable = func(context.Context) (bool, error) { return true, nil }
	f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }
	policy := newServiceHeaderPolicy(f)
	policy.Spec.RetryConstraint = &gatewayx.RetryConstraint{}
	retryPolicyStatusForGateway(f, policy)
	f.create(t, policy)
	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	parent := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
	check := func(retry, affinity bool, message, unavailable string) {
		t.Helper()
		f.reconciler.Config.BackendRetryBudget, f.reconciler.Config.XBackendSessionPersistence = retry, affinity
		ancestors, err := status.observeRetryPolicy(t.Context(), policy)
		require.NoError(t, err)
		require.Equal(t, metav1.ConditionTrue, ancestors[parent].Status)
		require.Contains(t, ancestors[parent].Message, message)
		if unavailable != "" {
			require.Contains(t, ancestors[parent].Message, unavailable)
		}
		spec := f.observe(t).Nodes[0].Spec
		require.Equal(t, retry && policy.Spec.RetryConstraint != nil, spec.BackendRetryBudget != nil)
		require.Equal(t, affinity && backendPolicyPersistence(policy) != nil, spec.ServiceSessionPersistence != nil)
	}
	check(false, true, "Service session persistence is accepted", "Service retry budgets require")
	check(true, false, "The Service retry constraint is accepted", "--octavia-xbackend-session-persistence")

	backendPolicyPersistence(policy).Type = ptr.To(gatewayv1.CookieBasedSessionPersistence)
	backendPolicyPersistence(policy).Header = nil
	f.reconciler.XBackendCookieAvailable = func(context.Context, cloud.HTTPSessionCookieRequirements) (bool, error) { return true, nil }
	policy.Spec.RetryConstraint = nil
	require.NoError(t, f.client.Update(t.Context(), policy))
	check(false, true, "Service session persistence is accepted", "")
	require.NoError(t, f.client.Delete(t.Context(), policy))
	require.Nil(t, f.observe(t).Nodes[0].Spec.ServiceSessionPersistence)
}

func TestServiceSessionPersistenceChangesDuringDeferredGraph(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.Config.XBackendSessionPersistence = true
	policy := newServiceHeaderPolicy(f)
	retryPolicyStatusForGateway(f, policy)
	f.create(t, policy)
	provider := &xBackendCookieStatusTestProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key, generation := client.ObjectKeyFromObject(f.route), f.route.Generation
	check := func(programmed, checkpoint bool) {
		t.Helper()
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, f.route))
		require.Equal(t, generation, f.route.Generation)
		require.Len(t, f.route.Status.Parents, 1)
		conditions := f.route.Status.Parents[0].Conditions
		require.Equal(t, programmed, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed"))
		require.Equal(t, checkpoint, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+servicePersistenceRevisionCondition))
	}
	check(true, true)
	deferred := cloud.RouteProgressingResult("Octavia has a busy neighbor", time.Second)
	deferred.ObservationDeferred = true
	deferred.BackendSessionPersistenceIssue = cloud.SessionPersistenceNotObserved
	provider.routeResult = &deferred
	check(true, true)
	provider.beforeGraph = func() { require.NoError(t, f.client.Delete(t.Context(), policy)) }
	check(false, false)
	provider.routeResult = nil
	check(true, false)
}

func TestServiceSessionPersistenceGatewayWatchFindsListenerSetRoutes(t *testing.T) {
	f := newRouteModelInputFixture(t)
	require.NoError(t, gatewayx.Install(f.client.Scheme()))
	gateway := f.gateway
	gateway.Spec.AllowedListeners = &gatewayv1.AllowedListeners{Namespaces: &gatewayv1.ListenerNamespaces{From: ptr.To(gatewayv1.NamespacesFromAll)}}
	set := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "delegated"},
		Spec: gatewayv1.ListenerSetSpec{ParentRef: gatewayv1.ParentGatewayReference{
			Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace)),
		}},
	}
	ref := gatewayv1.ParentReference{Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: "delegated", Namespace: ptr.To(gatewayv1.Namespace(set.Namespace))}
	delegated := mapperTestHTTPRoute(gateway.Namespace, "only-delegated", ref, f.service.Name)
	policy := newServiceHeaderPolicy(f)
	kubeClient := rawIndexedFakeClientBuilder(f.client.Scheme(), f.reconciler.Config).WithObjects(gateway, set, delegated, policy).
		WithIndex(&gatewayv1.ListenerSet{}, controller.IndexListenerSetByGateway, func(object client.Object) []string {
			return []string{model.ListenerSetParentKey(object.(*gatewayv1.ListenerSet)).String()}
		}).Build()
	r := &Reconciler{Client: kubeClient, Config: f.reconciler.Config}
	requests := r.enqueueRetryPoliciesForDependency(t.Context(), gateway)
	require.Equal(t, []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(policy)}}, requests)
}

func newServiceHeaderPolicy(f *routeModelInputFixture) *gatewayx.XBackendTrafficPolicy {
	return &gatewayx.XBackendTrafficPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "header-affinity", UID: "header-affinity", Generation: 1},
		Spec: gatewayx.BackendTrafficPolicySpec{
			TargetRefs: []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)}},
			SessionPersistence: &gatewayv1.SessionPersistence{ //nolint:staticcheck // Exercise the v1.7 Experimental field.
				Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "x-service-affinity"},
			},
		},
	}
}
