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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
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
	f.create(t, newer, older)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)
	pending := f.observe(t)
	for _, backend := range pending.Nodes[0].Spec.HTTPBackends {
		require.True(t, backend.BackendRetryBudgetPending)
		require.Nil(t, backend.BackendRetryBudget)
	}
	for _, policy := range []*gatewayx.XBackendTrafficPolicy{older, newer} {
		retryPolicyStatusForGateway(f, policy)
		require.NoError(t, f.client.Update(t.Context(), policy))
	}
	require.ErrorIs(t, pending.Validate(t.Context()), errHTTPRouteChanged)

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
	f.reconciler.Config.XBackendSessionPersistence = true
	f.reconciler.XBackendCookieAvailable = func(context.Context, cloud.HTTPSessionCookieRequirements) (bool, error) { return true, nil }
	f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }

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
			TargetRefs:         []gatewayx.LocalPolicyTargetReference{{Kind: "Service", Name: gatewayv1.ObjectName(remote.Name)}},
			RetryConstraint:    &gatewayx.RetryConstraint{},
			SessionPersistence: &gatewayv1.SessionPersistence{}, //nolint:staticcheck // Exercise the v1.7 Experimental field.
		},
	}
	retryPolicyStatusForGateway(f, policy)
	f.create(t, remote, endpoints, policy)
	before := f.observe(t)
	for _, backend := range before.Nodes[0].Spec.HTTPBackends {
		require.Nil(t, backend.BackendRetryBudget)
		require.Nil(t, backend.ServiceSessionPersistence)
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
			require.NotNil(t, backend.ServiceSessionPersistence)
			require.NotEmpty(t, backend.ServiceSessionPersistence.CookieName)
		} else {
			require.Nil(t, backend.BackendRetryBudget)
			require.Nil(t, backend.ServiceSessionPersistence)
		}
	}
	require.NoError(t, f.client.Delete(t.Context(), grant))
	require.ErrorIs(t, after.Validate(t.Context()), errHTTPRouteChanged)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.route), f.route))
	for _, backend := range f.observe(t).Nodes[0].Spec.HTTPBackends {
		require.Nil(t, backend.BackendRetryBudget)
		require.Nil(t, backend.ServiceSessionPersistence)
	}
	status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
	desired, err := status.observeRetryPolicy(t.Context(), policy)
	require.NoError(t, err)
	require.Empty(t, desired)
	updated := policy.DeepCopy()
	updated.Status.Ancestors = mergeRetryPolicyAncestors(policy, f.reconciler.Config.ControllerName, desired)
	patch, err := controller.OptimisticMergeFrom(policy).Data(updated)
	require.NoError(t, err)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(patch, &body))
	require.JSONEq(t, `{"ancestors":[]}`, string(body["status"]))
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

func TestServiceRetryBudgetReservesAncestorForAuthenticatedBackends(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	require.NoError(t, err)
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))

	for _, kind := range []gatewayv1.Kind{model.HTTPRouteKind, model.GRPCRouteKind} {
		t.Run(string(kind), func(t *testing.T) {
			f := newRouteModelInputFixture(t)
			enableRetryBudgetTest(t, f)
			f.reconciler.Config.BackendTLSPolicy = true
			f.reconciler.BackendRetryBudgetAvailable = func(context.Context) (bool, error) { return true, nil }
			if kind == model.HTTPRouteKind {
				f.service.Spec.Ports[0].AppProtocol = ptr.To("https")
			} else {
				f.reconciler.Config.BackendH2C, f.reconciler.Config.FixedHTTPResponses = true, true
				require.NoError(t, f.client.Delete(t.Context(), f.route))
				rule := grpcRule(gatewayv1.ObjectName(f.service.Name))
				rule.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
				f.create(t, &gatewayv1.GRPCRoute{
					ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: "grpc", UID: "grpc-uid"},
					Spec: gatewayv1.GRPCRouteSpec{
						CommonRouteSpec: f.route.Spec.CommonRouteSpec, Rules: []gatewayv1.GRPCRouteRule{rule},
					},
				})
			}
			require.NoError(t, f.client.Update(t.Context(), f.service))
			target := gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)}
			ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "ca", UID: "ca-uid"},
				Data: map[string]string{"ca.crt": certificate}}
			tls := &gatewayv1.BackendTLSPolicy{
				ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "tls", UID: "tls-policy-uid"},
				Spec: gatewayv1.BackendTLSPolicySpec{
					TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{LocalPolicyTargetReference: target}},
					Validation: gatewayv1.BackendTLSPolicyValidation{
						Hostname: "backend.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "ca"}},
					},
				},
			}
			retry := &gatewayx.XBackendTrafficPolicy{
				ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "budget", UID: "budget-policy-uid"},
				Spec:       gatewayx.BackendTrafficPolicySpec{TargetRefs: []gatewayx.LocalPolicyTargetReference{target}, RetryConstraint: &gatewayx.RetryConstraint{}},
			}
			f.create(t, ca, tls, retry)
			status := &backendRetryBudgetStatusReconciler{routes: f.reconciler}
			desired, err := status.observeRetryPolicy(t.Context(), retry)
			require.NoError(t, err)
			key := retryPolicyAncestor{group: gatewayv1.GroupName, kind: "Gateway", namespace: f.gateway.Namespace, name: f.gateway.Name}
			require.Equal(t, metav1.ConditionTrue, desired[key].Status)
		})
	}
}

func TestServiceRetryBudgetVisitsAdmittedAndSampledBackends(t *testing.T) {
	backend := func(name string, weight int32) model.BackendGroup {
		return model.BackendGroup{Service: types.NamespacedName{Namespace: "app", Name: name}, ServiceUID: types.UID(name), Weight: weight}
	}
	mirror := func(name string, numerator int32) model.RequestMirror {
		return model.RequestMirror{Backend: backend(name, 1), Numerator: numerator, Denominator: 100}
	}
	primary := backend("primary", 1)
	primary.RequestMirrors = []model.RequestMirror{mirror("target-copy", 100)}
	inactive := backend("inactive-target", 0)
	inactive.RequestMirrors = []model.RequestMirror{mirror("inactive-target-copy", 100)}
	rules := []model.Rule{
		{Backends: []model.BackendGroup{primary, inactive}, RequestMirrors: []model.RequestMirror{mirror("rule-copy", 50), mirror("unsampled", 0)}},
		{Backends: []model.BackendGroup{backend("unmatched-rule", 1)}},
	}
	matches := []routeLoweredMatch{{RouteMatch: model.RouteMatch{RuleIndex: 0}}}
	var names []string
	for observed := range activeRetryBudgetBackends(matches, rules, retryBudgetTransportSelectable) {
		names = append(names, observed.Service.Name)
	}
	require.ElementsMatch(t, []string{"primary", "rule-copy", "target-copy"}, names)
	inputs := routeModelInputs{retryBudgets: map[types.NamespacedName]routeRetryBudget{
		{Namespace: "app", Name: "target-copy"}: {budget: cloud.BackendRetryBudget{ServiceUID: "target-copy"}, slot: backendtls.AncestorSlotFull},
	}}
	require.True(t, inputs.retryBudgetAncestorFull(matches, rules, false))
}

func TestServiceRetryBudgetStatusWakesWhenForeignSlotIsReleased(t *testing.T) {
	f := newRouteModelInputFixture(t)
	before := &gatewayx.XBackendTrafficPolicy{}
	for range backendtls.MaxAncestors {
		before.Status.Ancestors = append(before.Status.Ancestors, gatewayv1.PolicyAncestorStatus{ControllerName: "other.example.com/gateway"})
	}
	after := before.DeepCopy()
	predicate := f.reconciler.retryBudgetStatusPredicate()
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}))
	after.Status.Ancestors = after.Status.Ancestors[1:]
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}))
}

func TestServiceRetryBudgetDiscoveryRequiresExplicitOptIn(t *testing.T) {
	r := Reconciler{}
	version := schema.GroupVersion{Group: gatewayx.GroupVersion.Group, Version: gatewayx.GroupVersion.Version}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{version})
	require.NoError(t, r.discoverBackendRetryPolicy(mapper))
	r.Config.GatewayAPIChannel = features.FeatureChannelExperimental
	require.NoError(t, r.discoverBackendRetryPolicy(mapper))
	r.Config.BackendRetryBudget = true
	require.ErrorContains(t, r.discoverBackendRetryPolicy(mapper), "discover XBackendTrafficPolicy")
	mapper.Add(version.WithKind("XBackendTrafficPolicy"), meta.RESTScopeNamespace)
	require.NoError(t, r.discoverBackendRetryPolicy(mapper))
	require.True(t, r.retryBudgetPolicyServed)
}
