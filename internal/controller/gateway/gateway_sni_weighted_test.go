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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func TestWeightedSNIRouteGrantWithdrawalAndRestore(t *testing.T) {
	cfg := testConfig()
	cfg.TLSSNIPassthrough, cfg.TCPConnectionRejection = true, true
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway"}}
	listener := testSNIListener()
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
	route := testSNIRoute(gateway, "release", gateway.Namespace, "release.example.test")
	sibling := testSNIRoute(gateway, "sibling", gateway.Namespace, "sibling.example.test")
	primary, primarySlice := testSNIBackend(gateway.Namespace, "stable", "192.0.2.101")
	primarySlice.Endpoints = append(primarySlice.Endpoints,
		discoveryv1.Endpoint{Addresses: []string{"192.0.2.102"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}},
		discoveryv1.Endpoint{Addresses: []string{"192.0.2.103"}, Conditions: discoveryv1.EndpointConditions{Serving: ptr.To(true), Terminating: ptr.To(true)}},
	)
	canary, canarySlice := testSNIBackend("partner", "canary", "192.0.2.201")
	stableRef := route.Spec.Rules[0].BackendRefs[0]
	stableRef.Weight = ptr.To(int32(3))
	canaryRef := stableRef
	canaryRef.Namespace, canaryRef.Weight = ptr.To(gatewayv1.Namespace(canary.Namespace)), nil
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{stableRef, canaryRef}
	sibling.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(42))
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: canary.Namespace, Name: "canary"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: tlsRouteKind, Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(canary.Name))}},
		},
	}
	grantUnreadable, grantRevoked := false, false
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(route, sibling).
		WithObjects(route, sibling, primary, primarySlice, canary, canarySlice, grant).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*gatewayv1.ReferenceGrantList); ok && grantUnreadable {
					return errors.New("reference grant inventory unavailable")
				}
				return c.List(ctx, list, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
				if _, ok := object.(*corev1.Service); ok && key == client.ObjectKeyFromObject(canary) && (grantUnreadable || grantRevoked) {
					t.Error("canary Service was read without a live ReferenceGrant")
				}
				return c.Get(ctx, key, object, opts...)
			},
		}).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	for _, dependency := range []client.Object{canary, canarySlice, grant} {
		requests := r.gatewayRequestsForSNIDependency(t.Context(), dependency)
		require.Len(t, requests, 1)
		require.Equal(t, client.ObjectKeyFromObject(gateway), requests[0].NamespacedName)
	}

	observe := func() sniRouteObservation {
		t.Helper()
		observation, err := r.observeSNIRoutes(t.Context(), snapshot)
		require.NoError(t, err)
		require.Len(t, observation, 2)
		require.Equal(t, gatewayv1.RouteReasonAccepted, observation[0].acceptedReason)
		require.Len(t, observation[1].targets, 1)
		legacy := observation[1].targets[0]
		require.Equal(t, string(primary.UID), legacy.ServiceUID)
		require.Zero(t, legacy.ServicePort)
		require.Empty(t, legacy.BackendServices)
		require.Zero(t, legacy.Members[0].Weight)
		return observation
	}
	report := func(observation sniRouteObservation, provider cloud.RouteResult, reason gatewayv1.RouteConditionReason, programmed bool) {
		t.Helper()
		result := cloud.GatewayGraphResult{Routes: []cloud.RouteGraphResult{
			{Identity: controller.RouteIdentity(cfg, gateway, route), Result: provider},
		}}
		require.NoError(t, r.reportSNIRoutes(t.Context(), snapshot, observation, result, nil))
		var latest gatewayv1.TLSRoute
		require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(route), &latest))
		c := latest.Status.Parents[0].Conditions
		require.True(t, meta.IsStatusConditionTrue(c, "Accepted"))
		require.Equal(t, string(reason), meta.FindStatusCondition(c, "ResolvedRefs").Reason)
		require.Equal(t, programmed, meta.IsStatusConditionTrue(c, cfg.Domain()+"/Programmed"))
	}
	ready := cloud.RouteReadyResult(cloud.RouteState{})
	deferred := cloud.RouteResult{ObservationDeferred: true}
	original := observe()
	require.Len(t, original[0].targets, 1)
	target := original[0].targets[0]
	require.Empty(t, target.ServiceUID)
	require.Zero(t, target.ServicePort)
	require.Nil(t, target.TCPConnectionRejection)
	require.Equal(t, []cloud.TLSBackendService{{UID: "canary", Port: 8443}, {UID: "stable", Port: 8443}}, target.BackendServices)
	require.Equal(t, []cloud.Member{
		{Address: "192.0.2.101", Port: 9443, Weight: 3},
		{Address: "192.0.2.102", Port: 9443, Weight: 3},
		{Address: "192.0.2.201", Port: 9443, Weight: 2},
	}, target.Members)
	report(original, ready, gatewayv1.RouteReasonResolvedRefs, true)

	grantUnreadable = true
	pending := observe()
	require.Empty(t, pending[0].targets)
	report(pending, deferred, gatewayv1.RouteReasonPending, false)
	grantUnreadable = false
	report(observe(), ready, gatewayv1.RouteReasonResolvedRefs, true)

	require.NoError(t, kube.Delete(t.Context(), grant))
	grantRevoked = true
	revoked := observe()
	require.False(t, original.sameInputs(revoked))
	require.Len(t, revoked[0].targets, 1)
	partial := revoked[0].targets[0]
	require.Equal(t, &cloud.TCPConnectionRejection{RejectWeight: 1, TotalWeight: 4}, partial.TCPConnectionRejection)
	require.Equal(t, []cloud.TLSBackendService{{UID: "stable", Port: 8443}}, partial.BackendServices)
	require.Equal(t, []cloud.Member{
		{Address: "192.0.2.101", Port: 9443, Weight: 1},
		{Address: "192.0.2.102", Port: 9443, Weight: 1},
	}, partial.Members)
	report(revoked, deferred, gatewayv1.RouteReasonRefNotPermitted, false)
	report(observe(), ready, gatewayv1.RouteReasonRefNotPermitted, true)
	report(observe(), deferred, gatewayv1.RouteReasonRefNotPermitted, true)

	r.Config.TCPConnectionRejection = false
	closed := observe()
	require.Empty(t, closed[0].targets)
	require.Equal(t, original[1].targets, closed[1].targets)
	report(closed, deferred, gatewayv1.RouteReasonRefNotPermitted, false)

	r.Config.TCPConnectionRejection = true
	grantRevoked = false
	grant.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), grant))
	restored := observe()
	require.Equal(t, original.targets(), restored.targets())
	report(restored, ready, gatewayv1.RouteReasonResolvedRefs, true)
}

func TestWeightedSNIRouteAvailability(t *testing.T) {
	for _, tc := range []struct {
		name             string
		otherWeight      int32
		omitService      bool
		policyRequired   bool
		primaryWeight    int32
		wantReason       gatewayv1.RouteConditionReason
		wantRejectWeight int
		wantMembers      bool
	}{
		{name: "empty EndpointSlices", otherWeight: 1, primaryWeight: 3, wantReason: gatewayv1.RouteReasonResolvedRefs, wantRejectWeight: 1, wantMembers: true},
		{name: "missing Service", otherWeight: 1, primaryWeight: 3, omitService: true, wantReason: gatewayv1.RouteReasonBackendNotFound, wantRejectWeight: 1, wantMembers: true},
		{name: "missing zero weight Service", primaryWeight: 3, omitService: true, wantReason: gatewayv1.RouteReasonBackendNotFound, wantMembers: true},
		{name: "all weights zero", wantReason: gatewayv1.RouteReasonResolvedRefs},
		{name: "all traffic points to an empty Service", otherWeight: 1, wantReason: gatewayv1.RouteReasonResolvedRefs},
		{name: "backend authentication required", otherWeight: 1, primaryWeight: 3, policyRequired: true, wantReason: gatewayv1.RouteReasonUnsupportedProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.TCPTLSTermination, cfg.BackendTLSPolicy, cfg.TCPConnectionRejection = true, true, true
			gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway"}}
			listener := testSNIListener()
			listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
			route := testSNIRoute(gateway, "release", gateway.Namespace, "release.example.test")
			primary, primarySlice := testSNIBackend(gateway.Namespace, "stable", "192.0.2.101")
			other := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "other", UID: "other"}, Spec: primary.Spec}
			refs := route.Spec.Rules[0].BackendRefs
			refs[0].Weight = ptr.To(tc.primaryWeight)
			otherRef := refs[0]
			otherRef.Name, otherRef.Weight = gatewayv1.ObjectName(other.Name), ptr.To(tc.otherWeight)
			route.Spec.Rules[0].BackendRefs = append(refs, otherRef)
			objects := []client.Object{route, primary, primarySlice}
			if !tc.omitService {
				objects = append(objects, other)
			}
			if tc.policyRequired {
				objects = append(objects, &gatewayv1.BackendTLSPolicy{
					ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "strict"},
					Spec: gatewayv1.BackendTLSPolicySpec{
						TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
							LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(primary.Name)},
						}},
					},
				})
			}
			kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
			r := &Reconciler{APIReader: kube, Config: cfg}
			observation, err := r.observeSNIRoutes(t.Context(), &gatewayMutationSnapshot{gateway: gateway, listener: &listener})
			require.NoError(t, err)
			require.Len(t, observation, 1)
			require.Equal(t, gatewayv1.RouteReasonAccepted, observation[0].acceptedReason)
			require.Equal(t, tc.wantReason, observation[0].resolvedReason)
			if !tc.wantMembers {
				require.Empty(t, observation[0].targets)
				require.Len(t, observation[0].denied, 1)
				return
			}
			require.Len(t, observation[0].targets, 1)
			target := observation[0].targets[0]
			require.Equal(t, []cloud.TLSBackendService{{UID: string(primary.UID), Port: 8443}}, target.BackendServices)
			require.Equal(t, []cloud.Member{{Address: "192.0.2.101", Port: 9443, Weight: 1}}, target.Members)
			if tc.wantRejectWeight == 0 {
				require.Nil(t, target.TCPConnectionRejection)
			} else {
				require.Equal(t, &cloud.TCPConnectionRejection{RejectWeight: tc.wantRejectWeight, TotalWeight: 4}, target.TCPConnectionRejection)
			}
		})
	}
}
