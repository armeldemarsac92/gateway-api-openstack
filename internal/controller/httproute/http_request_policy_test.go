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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package httproute

import (
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestHTTPRequestPolicyAdmissionAndLowering(t *testing.T) {
	f := newRouteModelInputFixture(t)
	rule := &f.route.Spec.Rules[0]
	rule.Timeouts = &gatewayv1.HTTPRouteTimeouts{Request: ptr.To(gatewayv1.Duration("0s"))}
	rule.Retry = &gatewayv1.HTTPRouteRetry{}
	_, err := f.reconciler.validateRouteStructure(f.route)
	require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), statusForRouteBuildError(err).acceptedReason)

	f.reconciler.Config.HTTPRequestPolicy = true
	for _, test := range []struct {
		retry   *gatewayv1.HTTPRouteRetry
		message string
	}{
		{&gatewayv1.HTTPRouteRetry{Backoff: ptr.To(gatewayv1.Duration("1s"))}, "retry.backoff"},
		{&gatewayv1.HTTPRouteRetry{Codes: []gatewayv1.HTTPRouteRetryStatusCode{501}}, "retry code 501"},
		{&gatewayv1.HTTPRouteRetry{Attempts: ptr.To(2147483648)}, "retry.attempts"},
	} {
		rule.Retry = test.retry
		_, err := f.reconciler.validateRouteStructure(f.route)
		require.ErrorContains(t, err, test.message)
		require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), statusForRouteBuildError(err).acceptedReason)
	}
	rule.Retry = &gatewayv1.HTTPRouteRetry{}
	spec, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
	require.NoError(t, err)
	require.Len(t, spec.HTTPBackends, 1)
	require.Empty(t, spec.WeightedHTTPRules)
	policy := spec.HTTPBackends[0].Matches[0].RequestPolicy
	require.Zero(t, *policy.RequestTimeoutMS)
	require.Equal(t, 1, *policy.Retry.Attempts)

	rule.Timeouts, rule.Retry = nil, nil
	spec, err = f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
	require.NoError(t, err)
	require.Empty(t, spec.HTTPBackends)
}

func TestHTTPRequestPolicyAdmissionWaitsForActiveReadback(t *testing.T) {
	config := controller.Config{ControllerName: "example.net/gateway"}
	parent := gatewayv1.ParentReference{Name: "gw"}
	route := &gatewayv1.HTTPRoute{Spec: gatewayv1.HTTPRouteSpec{Rules: []gatewayv1.HTTPRouteRule{
		{Retry: &gatewayv1.HTTPRouteRetry{}},
	}}}
	sync := func(status routeReconcileStatus) *metav1.Condition {
		plan := routeStatusPlan{updates: []parentStatusUpdate{{parent: parent, status: status}}}
		plan.apply(config, route)
		return meta.FindStatusCondition(route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
	}
	waiting := sync(pendingRouteStatus("Waiting for active Octavia policy"))
	require.Equal(t, metav1.ConditionUnknown, waiting.Status)
	require.Equal(t, string(gatewayv1.RouteReasonPending), waiting.Reason)
	require.Equal(t, metav1.ConditionTrue, sync(programmedRouteStatus()).Status)

	route.Spec.Rules[0].Retry = nil
	require.Equal(t, metav1.ConditionTrue, sync(pendingRouteStatus("Gateway is pending")).Status)
}

func TestHTTPRequestPolicyRegionalWithdrawalPreservesAcceptedRule(t *testing.T) {
	f := newRouteModelInputFixture(t)
	r := f.reconciler
	id := controller.RouteIdentity(r.Config, f.gateway, f.route)
	key := client.ObjectKeyFromObject(f.route)
	f.route.Generation = 7
	f.route.Spec.Rules[0].Retry = &gatewayv1.HTTPRouteRetry{}
	f.route.Spec.Rules = append(f.route.Spec.Rules, gatewayv1.HTTPRouteRule{
		Matches: []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{
			Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/ordinary"),
		}}},
		BackendRefs: f.route.Spec.Rules[0].BackendRefs,
	})
	r.Config.HTTPRequestPolicy = true
	require.NoError(t, f.client.Update(t.Context(), f.route))

	message := "Octavia does not advertise http_request_policy_v1"
	for _, test := range []struct {
		name        string
		result      cloud.RouteResult
		accepted    metav1.ConditionStatus
		reason      string
		programmed  metav1.ConditionStatus
		progReason  string
		wantPartial bool
	}{
		{name: "waiting to program surviving rule", result: cloud.RouteResult{Outcome: cloud.ProgressingOutcome("Waiting for readback", 0), PartiallyInvalid: message},
			accepted: metav1.ConditionUnknown, reason: "Pending", programmed: metav1.ConditionFalse, progReason: "Pending"},
		{name: "surviving rule verified during withdrawal", result: cloud.RouteResult{Outcome: cloud.ProgressingOutcome("Waiting for timed policy withdrawal", 0), PartiallyInvalid: message, ImplementedRules: true},
			accepted: metav1.ConditionTrue, reason: "Accepted", programmed: metav1.ConditionFalse, progReason: "Pending", wantPartial: true},
		{name: "remaining rule programmed", result: cloud.RouteResult{Outcome: cloud.ReadyOutcome(), PartiallyInvalid: message},
			accepted: metav1.ConditionTrue, reason: "Accepted", programmed: metav1.ConditionTrue, progReason: "Programmed", wantPartial: true},
		{name: "no remaining rule", result: cloud.RouteResult{Outcome: cloud.ReadyOutcome(), UnsupportedMatch: message},
			accepted: metav1.ConditionFalse, reason: "UnsupportedValue", programmed: metav1.ConditionFalse, progReason: "Invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r.Writer = fixedRouteGraph{result: cloud.GatewayGraphResult{Outcome: test.result.Outcome,
				Routes: []cloud.RouteGraphResult{{Identity: id, Result: test.result}}}}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			current := &gatewayv1.HTTPRoute{}
			require.NoError(t, f.client.Get(t.Context(), key, current))
			require.Len(t, current.Status.Parents, 1)
			conditions := current.Status.Parents[0].Conditions
			accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
			programmed := meta.FindStatusCondition(conditions, r.Config.Domain()+"/Programmed")
			require.NotNil(t, accepted)
			require.NotNil(t, programmed)
			require.Equal(t, test.accepted, accepted.Status, accepted.Message)
			require.Equal(t, test.reason, accepted.Reason)
			require.EqualValues(t, 7, accepted.ObservedGeneration)
			require.Equal(t, test.programmed, programmed.Status)
			require.Equal(t, test.progReason, programmed.Reason)
			require.EqualValues(t, 7, programmed.ObservedGeneration)
			partial := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionPartiallyInvalid))
			if !test.wantPartial {
				require.Nil(t, partial)
				return
			}
			require.NotNil(t, partial)
			require.Equal(t, metav1.ConditionTrue, partial.Status)
			require.Equal(t, "UnsupportedValue", partial.Reason)
			require.Equal(t, message, partial.Message)
			require.EqualValues(t, 7, partial.ObservedGeneration)
		})
	}
}
