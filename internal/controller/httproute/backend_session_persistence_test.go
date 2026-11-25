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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

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
