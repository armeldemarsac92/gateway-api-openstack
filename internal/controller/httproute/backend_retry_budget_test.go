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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestServiceRetryBudgetUsesLivePolicyWinner(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)
	f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }

	otherPort := f.service.Spec.Ports[0].DeepCopy()
	otherPort.Name, otherPort.Port = "other", otherPort.Port+1
	f.service.Spec.Ports = append(f.service.Spec.Ports, *otherPort)
	require.NoError(t, f.client.Update(t.Context(), f.service))
	secondRule := pathBackendRule("/another", gatewayv1.PathMatchExact, gatewayv1.ObjectName(f.service.Name))
	secondRule.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(otherPort.Port))
	f.route.Spec.Rules = append(f.route.Spec.Rules, secondRule)
	require.NoError(t, f.client.Update(t.Context(), f.route))

	before := f.observe(t)
	require.NoError(t, before.Validate(t.Context()))
	created := metav1.NewTime(time.Unix(100, 0))
	older := &gatewayx.XBackendTrafficPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "first", UID: "policy-first", CreationTimestamp: created, Generation: 2},
		Spec: gatewayx.BackendTrafficPolicySpec{
			TargetRefs:      []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)}},
			RetryConstraint: &gatewayx.RetryConstraint{},
		},
	}
	newer := older.DeepCopy()
	newer.Name, newer.UID = "second", "policy-second"
	newer.Spec.RetryConstraint.Budget = &gatewayx.BudgetDetails{Percent: ptr.To(0), Interval: ptr.To(gatewayx.Duration("1s500ms"))}
	newer.Spec.RetryConstraint.MinRetryRate = &gatewayx.RequestRate{Count: ptr.To(3), Interval: ptr.To(gatewayx.Duration("500ms"))}
	retryPolicyStatusForGateway(f, older)
	retryPolicyStatusForGateway(f, newer)
	f.create(t, newer, older)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)

	observed := f.observe(t)
	require.Len(t, observed.Nodes, 1)
	require.Len(t, observed.Nodes[0].Spec.HTTPBackends, 2)
	want := &cloud.BackendRetryBudget{ServiceUID: string(f.service.UID), BudgetPercent: 20, BudgetIntervalMS: 10000, MinRetryCount: 10, MinRetryIntervalMS: 1000}
	for _, backend := range observed.Nodes[0].Spec.HTTPBackends {
		require.Equal(t, want, backend.BackendRetryBudget)
	}
	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	ancestors, err := status.observeRetryPolicy(t.Context(), newer)
	require.NoError(t, err)
	gateway := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
	require.Equal(t, string(gatewayv1.PolicyReasonConflicted), ancestors[gateway].Reason)
	require.Equal(t, newer.Generation, ancestors[gateway].ObservedGeneration)

	require.NoError(t, f.client.Delete(t.Context(), older))
	require.ErrorIs(t, observed.Validate(t.Context()), errHTTPRouteChanged)
	current := f.observe(t)
	want = &cloud.BackendRetryBudget{ServiceUID: string(f.service.UID), BudgetPercent: 0, BudgetIntervalMS: 1500, MinRetryCount: 3, MinRetryIntervalMS: 500}
	for _, backend := range current.Nodes[0].Spec.HTTPBackends {
		require.Equal(t, want, backend.BackendRetryBudget)
	}
	f.reconciler.Config.BackendRetryBudget = false
	for _, backend := range f.observe(t).Nodes[0].Spec.HTTPBackends {
		require.Nil(t, backend.BackendRetryBudget)
	}
}

func TestServiceRetryBudgetRequiresAuthorizedBackend(t *testing.T) {
	f := newRouteModelInputFixture(t)
	enableRetryBudgetTest(t, f)

	remote, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	remote.Namespace, remote.UID, remote.ResourceVersion = "shared", types.UID("remote-uid"), ""
	endpoints.Namespace, endpoints.ResourceVersion = remote.Namespace, ""
	ref := pathBackendRule("/shared", gatewayv1.PathMatchExact, gatewayv1.ObjectName(remote.Name))
	ref.BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	ref.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(remote.Spec.Ports[0].Port))
	f.route.Spec.Rules = append(f.route.Spec.Rules, ref)
	require.NoError(t, f.client.Update(t.Context(), f.route))
	policy := &gatewayx.XBackendTrafficPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "budget", UID: "policy-shared"},
		Spec: gatewayx.BackendTrafficPolicySpec{
			TargetRefs:      []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(remote.Name)}},
			RetryConstraint: &gatewayx.RetryConstraint{},
		},
	}
	retryPolicyStatusForGateway(f, policy)
	f.create(t, remote, endpoints, policy)
	before := f.observe(t)
	for _, backend := range before.Nodes[0].Spec.HTTPBackends {
		require.Nil(t, backend.BackendRetryBudget)
	}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "allow"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: gatewayv1.Namespace(f.route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(remote.Name))}},
		},
	}
	f.create(t, grant)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)
	after := f.observe(t)
	require.Len(t, after.Nodes[0].Spec.HTTPBackends, 2)
	for _, backend := range after.Nodes[0].Spec.HTTPBackends {
		if backend.Namespace == remote.Namespace {
			require.NotNil(t, backend.BackendRetryBudget)
			require.Equal(t, string(remote.UID), backend.BackendRetryBudget.ServiceUID)
		} else {
			require.Nil(t, backend.BackendRetryBudget)
		}
	}
	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, after.Validate(t.Context()), errHTTPRouteChanged)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.route), f.route))
	for _, backend := range f.observe(t).Nodes[0].Spec.HTTPBackends {
		require.Nil(t, backend.BackendRetryBudget)
	}
}

func enableRetryBudgetTest(t *testing.T, f *routeModelInputFixture) {
	t.Helper()
	require.NoError(t, gatewayx.Install(f.client.Scheme()))
	cfg := &f.reconciler.Config
	cfg.GatewayAPIChannel = features.FeatureChannelExperimental
	cfg.BackendRetryBudget = true
	f.reconciler.retryBudgetPolicyServed = true
	for _, name := range cfg.GatewayAPICRDNames() {
		definition := gatewayAPICRD(name, gatewayconsts.BundleVersion)
		err := f.client.Get(t.Context(), client.ObjectKeyFromObject(definition), definition)
		definition.Annotations[gatewayconsts.ChannelAnnotation] = features.FeatureChannelExperimental
		if err == nil {
			require.NoError(t, f.client.Update(t.Context(), definition))
		} else {
			require.NoError(t, f.client.Create(t.Context(), definition))
		}
	}
}

func retryPolicyStatusForGateway(f *routeModelInputFixture, policy *gatewayx.XBackendTrafficPolicy) {
	policy.Status.Ancestors = []gatewayv1.PolicyAncestorStatus{{
		AncestorRef: gatewayAncestorRef(client.ObjectKeyFromObject(f.gateway)), ControllerName: f.reconciler.Config.ControllerName,
	}}
}
