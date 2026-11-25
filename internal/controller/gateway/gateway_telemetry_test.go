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
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/stretchr/testify/require"
)

type telemetryGraphProviderFunc func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error)

func (f telemetryGraphProviderFunc) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	return f(ctx, spec)
}

func telemetryTestPolicy(name string, timestamp int64) *gatewayx.XTelemetryPolicy {
	return &gatewayx.XTelemetryPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name, UID: types.UID(name), Generation: 4,
			CreationTimestamp: metav1.NewTime(time.Unix(timestamp, 0))},
		Spec: gatewayx.TelemetryPolicySpec{
			TargetRefs: []gatewayv1.LocalObjectReference{{Group: gatewayv1.GroupName, Kind: "Gateway", Name: "edge"}},
			Tracing:    gatewayx.TracingConfig{Mode: gatewayx.TracingModeDisabled},
		},
	}
}

func TestTelemetryPolicyPrecedenceAndStatus(t *testing.T) {
	cfg := testConfig()
	cfg.TelemetryPolicy, cfg.GatewayAPIChannel = true, features.FeatureChannelExperimental
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "edge"}}
	old := telemetryTestPolicy("b", 10)
	equalTime := telemetryTestPolicy("a", 10)
	young := telemetryTestPolicy("first-lexically", 20)
	young.Spec.Tracing.Mode = gatewayx.TracingModeEnabled
	invalid := telemetryTestPolicy("invalid-kind", 25)
	invalid.Spec.Tracing = gatewayx.TracingConfig{Mode: gatewayx.TracingModeEnabled,
		Provider: gatewayx.TracingProvider{BackendRef: gatewayv1.BackendObjectReference{Kind: ptr.To(gatewayv1.Kind("Secret")), Name: "collector"}}}
	denied := telemetryTestPolicy("no-grant", 30)
	denied.Spec.Tracing = gatewayx.TracingConfig{Mode: gatewayx.TracingModeEnabled,
		Provider: gatewayx.TracingProvider{BackendRef: gatewayv1.BackendObjectReference{Namespace: ptr.To(gatewayv1.Namespace("collector")), Name: "collector"}}}
	foreign := gatewayv1.PolicyAncestorStatus{
		AncestorRef: gatewayv1.ParentReference{Name: "foreign-gateway"}, ControllerName: "other.example/gateway",
		Conditions: []metav1.Condition{controller.Condition("Accepted", metav1.ConditionTrue, "Accepted", "Managed by other controller", 1)},
	}
	young.Status.Ancestors = []gatewayv1.PolicyAncestorStatus{foreign}
	c := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(old, equalTime, young, invalid, denied).
		WithStatusSubresource(&gatewayx.XTelemetryPolicy{}).Build()
	r := &Reconciler{Config: cfg, Client: c, APIReader: c}
	report := func(winner string) {
		observation, err := r.observeGatewayTelemetry(t.Context(), gateway, true, &gatewayMutationSnapshot{gateway: gateway})
		require.NoError(t, err)
		require.Equal(t, winner, observation.winner.Name)
		require.NoError(t, r.reportTelemetryPolicies(t.Context(), gateway, observation, cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil))
	}
	report("a")
	for _, policy := range []*gatewayx.XTelemetryPolicy{equalTime, old, young} {
		current := &gatewayx.XTelemetryPolicy{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), current))
		entry := current.Status.Ancestors[len(current.Status.Ancestors)-1]
		condition := meta.FindStatusCondition(entry.Conditions, "Accepted")
		require.NotNil(t, condition)
		require.Equal(t, policy.Generation, condition.ObservedGeneration)
		require.Equal(t, cfg.ControllerName, entry.ControllerName)
		require.Equal(t, gatewayv1.ObjectName("edge"), entry.AncestorRef.Name)
		if policy == equalTime {
			require.Equal(t, metav1.ConditionTrue, condition.Status)
		} else {
			require.Equal(t, "Conflicted", condition.Reason)
		}
		if policy == young {
			require.Equal(t, foreign, current.Status.Ancestors[0])
		}
	}

	require.NoError(t, c.Delete(t.Context(), equalTime))
	report("b")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(old), old))
	condition := meta.FindStatusCondition(old.Status.Ancestors[0].Conditions, "Accepted")
	require.Equal(t, metav1.ConditionTrue, condition.Status)

	// The stale winner is unimplementable when other ancestors fill all slots.
	for i := range 16 {
		other := foreign
		other.AncestorRef.Name = gatewayv1.ObjectName(fmt.Sprintf("foreign-%d", i))
		if i == 0 {
			old.Status.Ancestors = nil
		}
		old.Status.Ancestors = append(old.Status.Ancestors, other)
	}
	require.NoError(t, c.Status().Update(t.Context(), old))
	report("b")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(old), old))
	require.Len(t, old.Status.Ancestors, 16)
	require.Equal(t, foreign.ControllerName, old.Status.Ancestors[15].ControllerName)

	for _, tc := range []struct {
		policy *gatewayx.XTelemetryPolicy
		reason string
	}{{young, "BackendNotFound"}, {invalid, "InvalidKind"}, {denied, "RefNotPermitted"}} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc.policy), tc.policy))
		entry := tc.policy.Status.Ancestors[len(tc.policy.Status.Ancestors)-1]
		require.Equal(t, "Conflicted", meta.FindStatusCondition(entry.Conditions, "Accepted").Reason)
		resolved := meta.FindStatusCondition(entry.Conditions, "ResolvedRefs")
		require.Equal(t, tc.reason, resolved.Reason)
		require.Equal(t, tc.policy.Generation, resolved.ObservedGeneration)
	}
	observed, err := r.observeGatewayTelemetry(t.Context(), gateway, true, &gatewayMutationSnapshot{gateway: gateway})
	require.NoError(t, err)
	require.NoError(t, r.reportTelemetryPolicies(t.Context(), gateway, observed, cloud.GatewayGraphResult{
		Outcome: cloud.ReadyOutcome(), TelemetryCapabilityUnavailable: true,
	}, nil))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(young), young))
	require.Equal(t, []gatewayv1.PolicyAncestorStatus{foreign}, young.Status.Ancestors)
	report("b")
	r.Config.TelemetryPolicy = false
	require.NoError(t, r.reportTelemetryWithoutBinding(t.Context(), client.ObjectKeyFromObject(gateway)))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(young), young))
	require.Equal(t, []gatewayv1.PolicyAncestorStatus{foreign}, young.Status.Ancestors)
}

func TestTelemetrySamplingDefaultsAndInvalidFractions(t *testing.T) {
	tracing := gatewayx.TracingConfig{}
	full := cloud.TelemetryRate{Numerator: 1, Denominator: 1}
	sampling, valid := telemetrySampling(tracing)
	require.True(t, valid)
	require.Nil(t, sampling, "legacy full-rate tracing uses the original Octavia payload")

	tracing.ParentBasedSampling.Mode = gatewayx.ParentBasedSamplingModeEnabled
	sampling, valid = telemetrySampling(tracing)
	require.True(t, valid)
	require.Equal(t, &cloud.TelemetrySampling{Base: full, Parent: &full}, sampling)

	tracing.ParentBasedSampling.SamplingRate = &gatewayv1.Fraction{Numerator: 101}
	_, valid = telemetrySampling(tracing)
	require.False(t, valid)
	tracing.ParentBasedSampling.Mode = gatewayx.ParentBasedSamplingModeDisabled
	tracing.ParentBasedSampling.SamplingRate = &gatewayv1.Fraction{Numerator: 50}
	_, valid = telemetrySampling(tracing)
	require.False(t, valid, "a parent rate requires parent-based sampling")
}

func TestTelemetryPolicyGrantRevocationAndRecoveryFencesCollector(t *testing.T) {
	cfg := testConfig()
	cfg.TelemetryPolicy, cfg.GatewayAPIChannel = true, features.FeatureChannelExperimental
	cfg.BackendTLSPolicy = true
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "internal"},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "edge", UID: "edge", Generation: 1,
			Annotations: gatewayBindingAnnotations(cfg, "80"), Finalizers: []string{cfg.GatewayFinalizer()}},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "internal", Listeners: []gatewayv1.Listener{{Name: "http", Protocol: "HTTP", Port: 80}}},
	}
	gateway.Annotations[cfg.GatewayTelemetryAnnotation()] = "v1"
	topology := testBoundTopology()
	topology.Topology.MemberMode = cloud.MemberModeEndpoint
	require.NoError(t, controller.BindGatewayTopology(cfg, gateway, topology))
	service, endpoints := testSNIBackend("observability", "collector", "192.0.2.12")
	policy := telemetryTestPolicy("collector", 10)
	policy.Spec.Tracing = gatewayx.TracingConfig{
		Mode: gatewayx.TracingModeEnabled, ServiceName: "edge.apps", SpanName: "Edge request",
		Attributes: []gatewayx.Attribute{
			{Name: "tenant", SourceType: gatewayx.AttributeSourceHeader, HeaderName: "X-Test-Tenant"},
			{Name: "code", SourceType: gatewayx.AttributeSourceAttribute, AttributeKey: "http.response.status_code"},
			{Name: "environment", SourceType: gatewayx.AttributeSourceLiteral, LiteralValue: "test"},
		},
		SamplingRate: &gatewayv1.Fraction{Numerator: 25},
		ParentBasedSampling: gatewayx.ParentBasedSampling{
			Mode: gatewayx.ParentBasedSamplingModeEnabled, SamplingRate: &gatewayv1.Fraction{Numerator: 1, Denominator: ptr.To(int32(2))},
		},
		Provider: gatewayx.TracingProvider{BackendRef: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(service.Name),
			Namespace: ptr.To(gatewayv1.Namespace(service.Namespace)), Port: ptr.To(gatewayv1.PortNumber(8443))}},
	}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "collector"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.Group(gatewayx.GroupVersion.Group), Kind: "XTelemetryPolicy", Namespace: "apps"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(service.Name))}},
		},
	}
	tlsPolicy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "tls", UID: "collector-tls", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "collector.example.test", WellKnownCACertificates: ptr.To(gatewayv1.WellKnownCACertificatesSystem),
			},
		},
		Status: gatewayv1.PolicyStatus{Ancestors: []gatewayv1.PolicyAncestorStatus{{
			AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))},
			ControllerName: cfg.ControllerName,
			Conditions: []metav1.Condition{
				controller.Condition(string(gatewayv1.PolicyConditionAccepted), metav1.ConditionTrue, "Accepted", "", 1),
				controller.Condition(string(gatewayv1.BackendTLSPolicyConditionResolvedRefs), metav1.ConditionTrue, "ResolvedRefs", "", 1),
			},
		}}},
	}
	c := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(gateway, class, service, endpoints, policy, grant, tlsPolicy).
		WithStatusSubresource(&gatewayx.XTelemetryPolicy{}).Build()
	var calls []*cloud.GatewayTelemetry
	var beforeProviderValidation func()
	provider := telemetryGraphProviderFunc(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		require.Equal(t, cloud.GraphReconcileTelemetry, spec.Gateway.Disposition)
		require.Empty(t, spec.Routes)
		require.False(t, spec.Gateway.BackendCAOwned)
		require.Nil(t, spec.Gateway.ClaimBackendCA)
		if beforeProviderValidation != nil {
			beforeProviderValidation()
			beforeProviderValidation = nil
		}
		if err := spec.ValidateMutation(ctx); err != nil {
			return cloud.GatewayGraphResult{}, err
		}
		calls = append(calls, spec.Gateway.Telemetry)
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	r := newTestReconciler(Reconciler{Config: cfg, Client: c, APIReader: c, Coordinator: &graph.Coordinator{}}, provider)
	request := graph.Request{Action: graph.ReconcileTelemetry, Gateway: gateway, Identity: controller.StoredGatewayIdentity(cfg, gateway)}
	run := func() { _, err := r.Writer.ReconcileGraph(t.Context(), request); require.NoError(t, err) }
	run()
	require.Nil(t, calls[0], "status slot precedes the first exporter write")
	run()
	require.NotNil(t, calls[1], "the accepted tracing policy reaches the exporter")
	require.Equal(t, &cloud.BackendTLSValidation{Trust: cloud.BackendTLSTrustSystem, Hostname: "collector.example.test"}, calls[1].BackendTLS)
	require.Equal(t, &cloud.TelemetrySampling{Base: cloud.TelemetryRate{Numerator: 25, Denominator: 100},
		Parent: &cloud.TelemetryRate{Numerator: 1, Denominator: 2}}, calls[1].Sampling)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.12", Port: 9443}}, calls[1].Members)
	require.Equal(t, "edge.apps", calls[1].ServiceName)
	require.Equal(t, "Edge request", calls[1].SpanName)
	require.Equal(t, []cloud.TelemetryAttribute{
		{Name: "code", SourceType: "Attribute", AttributeKey: "http.response.status_code"},
		{Name: "environment", SourceType: "Literal", LiteralValue: "test"},
		{Name: "tenant", SourceType: "Header", HeaderName: "x-test-tenant"},
	}, calls[1].Attributes)
	require.Equal(t, []cloud.TelemetryListener{{Port: 80, Protocol: "HTTP"}}, calls[1].Listeners)

	beforeProviderValidation = func() { require.NoError(t, c.Delete(t.Context(), grant)) }
	_, err := r.Writer.ReconcileGraph(t.Context(), request)
	require.ErrorIs(t, err, errGatewayChanged)
	require.Len(t, calls, 2, "the authorization change must interrupt a stale write")
	run()
	require.Nil(t, calls[2])
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), policy))
	resolved := meta.FindStatusCondition(policy.Status.Ancestors[0].Conditions, "ResolvedRefs")
	require.Equal(t, "RefNotPermitted", resolved.Reason)
	require.Equal(t, policy.Generation, resolved.ObservedGeneration)

	renewed := grant.DeepCopy()
	renewed.ResourceVersion = ""
	renewed.Name = "restored"
	require.NoError(t, c.Create(t.Context(), renewed))
	run()
	require.Equal(t, calls[1], calls[3])
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), policy))
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(policy.Status.Ancestors[0].Conditions, "ResolvedRefs").Status)

	// Turning off BackendTLSPolicy cannot downgrade an authenticated collector.
	r.Config.BackendTLSPolicy = false
	run()
	require.Nil(t, calls[4])
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), policy))
	require.Equal(t, "UnsupportedValue", meta.FindStatusCondition(policy.Status.Ancestors[0].Conditions, "Accepted").Reason)

	// Removing the last policy while the checkpoint lacks the Barbican
	// finalizer still reaches the provider's withdrawal path.
	r.Config.BackendTLSPolicy = true
	require.NoError(t, c.Delete(t.Context(), policy))
	scope, _, err := r.newGatewayScope(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)})
	require.NoError(t, err)
	_, err = r.reconcileGatewayTelemetry(t.Context(), scope, false)
	require.NoError(t, err)
	require.Len(t, calls, 6)
	require.Nil(t, calls[5])
}
