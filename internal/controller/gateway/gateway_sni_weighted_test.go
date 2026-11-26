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
	"slices"
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
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"

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
	require.Len(t, target.BackendServiceMembers, 2)
	require.Equal(t, "stable", target.BackendServiceMembers[0].ServiceUID)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.101", Port: 9443}, {Address: "192.0.2.102", Port: 9443}}, target.BackendServiceMembers[0].Members)
	require.Equal(t, "canary", target.BackendServiceMembers[1].ServiceUID)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.201", Port: 9443}}, target.BackendServiceMembers[1].Members)
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
	require.Equal(t, []cloud.TLSSNIBackendPool{target.BackendServiceMembers[0], {Weight: 1}}, partial.BackendServiceMembers)
	require.Equal(t, []cloud.Member{
		{Address: "192.0.2.101", Port: 9443, Weight: 1},
		{Address: "192.0.2.102", Port: 9443, Weight: 1},
	}, partial.Members)
	report(revoked, deferred, gatewayv1.RouteReasonRefNotPermitted, false)
	report(observe(), ready, gatewayv1.RouteReasonRefNotPermitted, true)
	report(observe(), deferred, gatewayv1.RouteReasonRefNotPermitted, true)

	r.Config.TCPConnectionRejection = false
	closed := observe()
	require.Len(t, closed[0].targets, 1)
	require.Equal(t, partial.BackendServiceMembers, closed[0].targets[0].BackendPools)
	require.Equal(t, original[1].targets, closed[1].targets)
	report(closed, deferred, gatewayv1.RouteReasonRefNotPermitted, false)

	r.Config.TCPConnectionRejection = true
	grantRevoked = false
	grant.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), grant))
	restored := observe()
	require.Equal(t, original.targets(), restored.targets())
	report(restored, ready, gatewayv1.RouteReasonResolvedRefs, true)

	// 257 / 2 : 128 / 1 reduces to 257:256 per endpoint, beyond
	// Octavia's member range despite the healthy backend having two endpoints.
	var current gatewayv1.TLSRoute
	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(route), &current))
	current.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(257))
	current.Spec.Rules[0].BackendRefs[1].Weight = ptr.To(int32(128))
	current.Generation++
	require.NoError(t, kube.Update(t.Context(), &current))
	large := observe()
	require.Len(t, large[0].targets, 1)
	wantPools := slices.Clone(target.BackendServiceMembers)
	wantPools[0].Weight, wantPools[1].Weight = 257, 128
	require.Equal(t, wantPools, large[0].targets[0].BackendPools)
	require.Empty(t, large[0].targets[0].Members)
	require.Equal(t, original[1].targets, large[1].targets)

	require.NoError(t, kube.Delete(t.Context(), grant))
	grantRevoked = true
	r.Config.TCPConnectionRejection = false
	largeRevoked := observe()
	require.Len(t, largeRevoked[0].targets, 1)
	require.Equal(t, []cloud.TLSSNIBackendPool{wantPools[0], {Weight: 128}}, largeRevoked[0].targets[0].BackendPools)
	report(largeRevoked, ready, gatewayv1.RouteReasonRefNotPermitted, true)
	grantRevoked = false
	grant.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), grant))
	require.Equal(t, large.targets(), observe().targets())
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
		{name: "invalid backend authentication policy", otherWeight: 1, primaryWeight: 3, policyRequired: true, wantReason: "InvalidBackendTLSPolicy"},
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

func TestZeroWeightServiceImportLeavesPositiveSNIBackendsUnchanged(t *testing.T) {
	cfg := testConfig()
	cfg.MulticlusterServiceImports, cfg.TLSSNIPassthrough, cfg.TCPConnectionRejection = true, true, true
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway"}}
	listener := testSNIListener()
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
	route := testSNIRoute(gateway, "app", gateway.Namespace, "api.example.test")
	plain, plainEndpoints := testSNIBackend(gateway.Namespace, "plain", "192.0.2.10")
	protected, protectedEndpoints := testSNIBackend("partner", "protected", "192.0.2.20")
	policy := &gatewayv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: protected.Namespace, Name: "require-tls"},
		Spec: gatewayv1.BackendTLSPolicySpec{TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{
			{LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(protected.Name)}},
		}},
	}
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: protected.Namespace, Name: "allow"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: tlsRouteKind, Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(protected.Name))}},
		},
	}
	imported := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "backend", UID: "import-uid"},
		Spec: mcs.ServiceImportSpec{Type: mcs.ClusterSetIP, IPs: []string{"10.4.0.21"}, Ports: []mcs.ServicePort{{Port: 8443, Protocol: corev1.ProtocolTCP}}},
	}
	ordinary := route.Spec.Rules[0].BackendRefs[0]
	authenticated := ordinary
	authenticated.Namespace = ptr.To(gatewayv1.Namespace(protected.Namespace))
	zero := ordinary
	zero.Weight = ptr.To(int32(0))
	zero.Group, zero.Kind = ptr.To(gatewayv1.Group(mcs.GroupName)), ptr.To(gatewayv1.Kind("ServiceImport"))
	scheme := testScheme(t)
	require.NoError(t, mcs.Install(scheme))
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route, plain, plainEndpoints, protected, protectedEndpoints, policy, grant, imported).Build()
	r := &Reconciler{APIReader: kube, Config: cfg}
	observe := func(refs ...gatewayv1.BackendRef) sniRouteStatus {
		t.Helper()
		route.Spec.Rules[0].BackendRefs = refs
		require.NoError(t, kube.Update(t.Context(), route))
		observation, err := r.observeSNIRoutes(t.Context(), snapshot)
		require.NoError(t, err)
		require.Len(t, observation, 1)
		require.Equal(t, gatewayv1.RouteReasonAccepted, observation[0].acceptedReason)
		return observation[0]
	}
	weighted := observe(ordinary, authenticated)
	require.Empty(t, weighted.targets)
	require.NotNil(t, weighted.programmingDiagnostic)
	withZero := observe(ordinary, authenticated, zero)
	require.Empty(t, withZero.targets)
	require.Equal(t, weighted.programmingDiagnostic, withZero.programmingDiagnostic)

	single := observe(ordinary)
	require.Len(t, single.targets, 1)
	require.Equal(t, single.targets, observe(ordinary, zero).targets)
	zero.Name = "missing"
	missing := observe(ordinary, zero)
	require.Equal(t, gatewayv1.RouteReasonBackendNotFound, missing.resolvedReason)
	require.Equal(t, single.targets, missing.targets)

	listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	r.Config.TCPTLSTermination, r.Config.BackendTLSPolicy = true, true
	r.Config.TCPConnectionRejection = false
	zero.Name = gatewayv1.ObjectName(imported.Name)
	terminating := observe(ordinary, authenticated)
	require.Empty(t, terminating.targets)
	require.NotNil(t, terminating.programmingDiagnostic)
	terminatingWithZero := observe(ordinary, authenticated, zero)
	require.Empty(t, terminatingWithZero.targets)
	require.Equal(t, terminating.programmingDiagnostic, terminatingWithZero.programmingDiagnostic)
}

func TestWeightedTerminatingSNIRouteResolvesEachBackendTLSIdentity(t *testing.T) {
	cfg := testConfig()
	cfg.TCPTLSTermination, cfg.BackendTLSPolicy, cfg.TCPConnectionRejection = true, true, true
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "gateway", UID: "gateway"}}
	listener := testSNIListener()
	listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	snapshot := &gatewayMutationSnapshot{gateway: gateway, listener: &listener}
	route := testSNIRoute(gateway, "release", gateway.Namespace, "release.example.test")
	stable, stableEndpoints := testSNIBackend(gateway.Namespace, "stable", "192.0.2.101")
	canary, canaryEndpoints := testSNIBackend("partner", "canary", "192.0.2.201")
	plain := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "plain", UID: "plain"}, Spec: stable.Spec}
	plainEndpoints := stableEndpoints.DeepCopy()
	plainEndpoints.Name = "plain-1"
	plainEndpoints.Labels[discoveryv1.LabelServiceName] = plain.Name
	plainEndpoints.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(plain, corev1.SchemeGroupVersion.WithKind("Service"))}
	plainEndpoints.Endpoints[0].Addresses[0] = "192.0.2.103"
	stableRef := route.Spec.Rules[0].BackendRefs[0]
	stableRef.Weight = ptr.To(int32(3))
	canaryRef := stableRef
	canaryRef.Namespace, canaryRef.Weight = ptr.To(gatewayv1.Namespace(canary.Namespace)), ptr.To(int32(2))
	duplicate := canaryRef
	duplicate.Weight = ptr.To(int32(1))
	zero := stableRef
	zero.Weight = ptr.To(int32(0))
	plainRef := stableRef
	plainRef.Name, plainRef.Weight = gatewayv1.ObjectName(plain.Name), nil
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{stableRef, canaryRef, duplicate, zero, plainRef}
	policyFor := func(service *corev1.Service, hostname gatewayv1.PreciseHostname) (*gatewayv1.BackendTLSPolicy, *corev1.ConfigMap) {
		ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "backend-ca", UID: service.UID + "-ca"},
			Data: map[string]string{"ca.crt": frontendTestCA(t)}}
		policy := &gatewayv1.BackendTLSPolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "require-tls", UID: service.UID + "-policy"},
			Spec: gatewayv1.BackendTLSPolicySpec{
				TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
					LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(service.Name)},
				}},
				Validation: gatewayv1.BackendTLSPolicyValidation{
					Hostname: hostname, CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name)}},
				},
			},
			Status: gatewayv1.PolicyStatus{Ancestors: []gatewayv1.PolicyAncestorStatus{{
				ControllerName: cfg.ControllerName,
				AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), Namespace: ptr.To(gatewayv1.Namespace(gateway.Namespace))},
			}}},
		}
		return policy, ca
	}
	stablePolicy, stableCA := policyFor(stable, "stable.example.test")
	canaryPolicy, canaryCA := policyFor(canary, "canary.example.test")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: canary.Namespace, Name: "canary"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: tlsRouteKind, Namespace: gatewayv1.Namespace(route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(canary.Name))}},
		},
	}
	caUnreadable, grantRevoked := false, false
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(canaryPolicy).WithObjects(route,
		stable, stableEndpoints, stablePolicy, stableCA, canary, canaryEndpoints, canaryPolicy, canaryCA, plain, plainEndpoints, grant).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
				if _, ok := object.(*corev1.Service); ok && key == client.ObjectKeyFromObject(canary) && grantRevoked {
					t.Error("read the canary Service after grant revocation")
				}
				if _, ok := object.(*corev1.ConfigMap); ok && key == client.ObjectKeyFromObject(canaryCA) && caUnreadable {
					return errors.New("CA ConfigMap API unavailable")
				}
				return c.Get(ctx, key, object, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*discoveryv1.EndpointSliceList); ok && (&client.ListOptions{}).ApplyOptions(opts).Namespace == canary.Namespace && grantRevoked {
					t.Error("read canary EndpointSlices after grant revocation")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg}
	observe := func(reason gatewayv1.RouteConditionReason) []cloud.TLSSNIBackendPool {
		t.Helper()
		observation, err := r.observeSNIRoutes(t.Context(), snapshot)
		require.NoError(t, err)
		require.Len(t, observation, 1)
		require.Equal(t, reason, observation[0].resolvedReason)
		require.Len(t, observation[0].targets, 1)
		target := observation[0].targets[0]
		require.Empty(t, target.ServiceUID)
		require.Empty(t, target.BackendServices)
		require.Empty(t, target.Members)
		require.Nil(t, target.BackendTLS)
		require.Nil(t, target.TCPConnectionRejection)
		require.Len(t, target.BackendPools, 4)
		return target.BackendPools
	}
	require.Len(t, r.gatewayRequestsForSNIDependency(t.Context(), canaryPolicy), 1)
	healthy := observe(gatewayv1.RouteReasonResolvedRefs)
	require.Equal(t, cloud.TLSSNIBackendPool{Weight: 1, ServiceUID: "plain", ServicePort: 8443, Members: []cloud.Member{{Address: "192.0.2.103", Port: 9443}}}, healthy[3])
	require.Equal(t, "stable.example.test", healthy[0].BackendTLS.Hostname)
	require.Equal(t, "canary.example.test", healthy[1].BackendTLS.Hostname)
	require.NotEqual(t, healthy[0].BackendTLS.CADigest, healthy[1].BackendTLS.CADigest)
	require.Equal(t, string(stableCA.UID), healthy[0].BackendTLS.CAConfigMapUID)
	require.Equal(t, string(canaryCA.UID), healthy[1].BackendTLS.CAConfigMapUID)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.101", Port: 9443}}, healthy[0].Members)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.201", Port: 9443}}, healthy[1].Members)
	require.Equal(t, 3, healthy[0].Weight)
	require.Equal(t, 2, healthy[1].Weight)
	duplicatePool := healthy[1]
	duplicatePool.Weight = 1
	require.Equal(t, duplicatePool, healthy[2])
	denied := []cloud.TLSSNIBackendPool{healthy[0], {Weight: 2}, {Weight: 1}, healthy[3]}

	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(canaryPolicy), canaryPolicy))
	ancestors := canaryPolicy.Status.Ancestors
	canaryPolicy.Status.Ancestors = nil
	require.NoError(t, kube.Status().Update(t.Context(), canaryPolicy))
	require.Equal(t, denied, observe(gatewayv1.RouteReasonPending))
	canaryPolicy.Status.Ancestors = ancestors
	require.NoError(t, kube.Status().Update(t.Context(), canaryPolicy))
	caUnreadable = true
	require.Equal(t, denied, observe(gatewayv1.RouteReasonPending))
	caUnreadable = false
	require.NoError(t, kube.Delete(t.Context(), canaryCA))
	require.Equal(t, denied, observe("InvalidBackendTLSPolicy"))
	canaryCA.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), canaryCA))
	require.NoError(t, kube.Delete(t.Context(), canaryEndpoints))
	require.Equal(t, denied, observe(gatewayv1.RouteReasonResolvedRefs))
	canaryEndpoints.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), canaryEndpoints))
	require.NoError(t, kube.Delete(t.Context(), grant))
	grantRevoked = true
	require.Equal(t, denied, observe(gatewayv1.RouteReasonRefNotPermitted))
	r.Config.TCPConnectionRejection = false
	closed, err := r.observeSNIRoutes(t.Context(), snapshot)
	require.NoError(t, err)
	require.Empty(t, closed[0].targets)
	require.Len(t, closed[0].denied, 1)
	r.Config.TCPConnectionRejection = true
	grantRevoked = false
	grant.ResourceVersion = ""
	require.NoError(t, kube.Create(t.Context(), grant))
	require.Equal(t, healthy, observe(gatewayv1.RouteReasonResolvedRefs))

	r.Config.TLSSNIPassthrough, r.Config.TCPTLSMixedMode = true, true
	listener.Hostname = ptr.To(gatewayv1.Hostname("*.example.test"))
	passthrough := testSNIListener()
	passthrough.Name = "pass"
	passthrough.Hostname = ptr.To(gatewayv1.Hostname("passthrough.example.test"))
	gateway.Spec.Listeners = []gatewayv1.Listener{passthrough, listener}
	snapshot.listeners, err = SupportedListenersForConfig(r.Config, gateway)
	require.NoError(t, err)
	snapshot.listener = snapshot.listeners[0]
	mixed, err := r.observeSNIRoutes(t.Context(), snapshot)
	require.NoError(t, err)
	require.Len(t, mixed.targets(), 1)
	require.False(t, mixed.targets()[0].Passthrough)
	require.Equal(t, healthy, mixed.targets()[0].BackendPools)

	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(route), route))
	route.Spec.Rules[0].BackendRefs = []gatewayv1.BackendRef{stableRef}
	require.NoError(t, kube.Update(t.Context(), route))
	native, err := r.observeSNIRoutes(t.Context(), snapshot)
	require.NoError(t, err)
	require.Len(t, native.targets(), 1)
	require.Equal(t, healthy[0].BackendTLS, native.targets()[0].BackendTLS)

	canaryRef.Weight = ptr.To(int32(0))
	route.Spec.Rules[0].BackendRefs = append(route.Spec.Rules[0].BackendRefs, canaryRef)
	require.NoError(t, kube.Update(t.Context(), route))
	withZero, err := r.observeSNIRoutes(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, gatewayv1.RouteReasonResolvedRefs, withZero.forParent(route.Spec.ParentRefs[0]).resolvedReason)
	require.Equal(t, native.targets(), withZero.targets())

	require.NoError(t, kube.Delete(t.Context(), grant))
	grantRevoked = true
	withoutZeroGrant, err := r.observeSNIRoutes(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, gatewayv1.RouteReasonRefNotPermitted, withoutZeroGrant.forParent(route.Spec.ParentRefs[0]).resolvedReason)
	require.Equal(t, native.targets(), withoutZeroGrant.targets())
}
