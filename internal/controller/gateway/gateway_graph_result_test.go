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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestGatewayRejectsMissingComponentOutcome(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Status = gatewayv1.GatewayStatus{}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}}}, nil)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err == nil {
		t.Fatal("missing Gateway component outcome was accepted")
	}
	current := &gatewayv1.Gateway{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Status, gateway.Status) {
		t.Fatalf("invalid component result changed status: %#v", current.Status)
	}
}

func TestGatewayDoesNotReportNewHostnameProgrammedWhileRoutesProgress(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Generation++
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("*.example.test"))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ProgressingOutcome("Disabling Octavia routing policy", time.Second),
		Gateway: cloud.GatewayResult{
			Outcome: cloud.ReadyOutcome(),
			State:   cloud.GatewayState{LoadBalancerID: "load-balancer", ListenerID: "listener", VIPAddress: "192.0.2.10"},
		},
	}
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{result: result}}, nil)
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
		t.Fatal(err)
	}
	current := &gatewayv1.Gateway{}
	if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(gateway), current); err != nil {
		t.Fatal(err)
	}
	accepted := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != gateway.Generation ||
		programmed == nil || (programmed.Status == metav1.ConditionTrue && programmed.ObservedGeneration == gateway.Generation) {
		t.Fatalf("new hostname should be accepted while its policy update is pending: %v", current.Status.Conditions)
	}
}

func TestGatewayWithdrawsCurrentHTTPReadinessOnPolicyFailure(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("first.example.test"))
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, gatewayv1.Listener{
		Name: "second", Protocol: gatewayv1.HTTPProtocolType, Port: gateway.Spec.Listeners[0].Port,
		Hostname: ptr.To(gatewayv1.Hostname("second.example.test")),
	})
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	gatewayState := cloud.GatewayState{LoadBalancerID: "load-balancer", ListenerID: "listener", VIPAddress: "192.0.2.10"}
	pending := cloud.ProgressingOutcome("Waiting for updated HTTP hostname rules", time.Second)
	graphFailure := &cloud.GraphError{Identity: cloud.Identity{RouteUID: "route-uid"}, Err: errors.New("failed to disable policy")}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Writer: fixedGatewayGraph{
		result: cloud.GatewayGraphResult{Outcome: pending, Gateway: cloud.GatewayResult{
			State: gatewayState, Outcome: pending, HTTPPolicyTransition: true,
		}}, err: graphFailure,
	}}, nil)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if _, err := r.Reconcile(t.Context(), request); !errors.Is(err, graphFailure) {
		t.Fatalf("reconcile on failed HTTP policy change = %v, want the provider error", err)
	}
	current := &gatewayv1.Gateway{}
	if err := kube.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Generation != gateway.Generation ||
		!meta.IsStatusConditionFalse(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)) ||
		len(current.Status.Listeners) != 2 ||
		!meta.IsStatusConditionFalse(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("failed HTTP policy change left current readiness published: %#v", current.Status)
	}

	r.Writer = fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(gatewayState)}}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Generation != gateway.Generation ||
		!meta.IsStatusConditionTrue(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)) ||
		!meta.IsStatusConditionTrue(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("observed HTTP policy readiness was not recovered: %#v", current.Status)
	}
}

func TestGatewayExcludesRemovedHostnameFromSingletonAttachedRoutes(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	survivor := &gateway.Spec.Listeners[0]
	survivor.Hostname = ptr.To(gatewayv1.Hostname("survivor.example.test"))
	removed := survivor.DeepCopy()
	removed.Name, removed.Hostname = "old", ptr.To(gatewayv1.Hostname("old.example.test"))
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "old", Generation: 1},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{parent}},
			Hostnames:       []gatewayv1.Hostname{*removed.Hostname},
		},
	}
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef: parent, ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{{Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionTrue, ObservedGeneration: route.Generation}},
	}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(route).Build()
	r := &Reconciler{Client: kube, Config: cfg}
	counts, err := r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{survivor, removed})
	if err != nil || counts.counts[removed.Name] != 1 || counts.counts[survivor.Name] != 0 {
		t.Fatalf("shared counts = %v, %v", counts, err)
	}
	counts, err = r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{survivor})
	if err != nil || counts.counts[survivor.Name] != 0 {
		t.Fatalf("singleton counts before the old route status is refreshed = %v, %v", counts, err)
	}
}

func TestGatewayCountsUnreportedAcceptedParentAtRouteStatusLimit(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayAPIChannel = features.FeatureChannelExperimental
	gateway := programmedTestGateway(cfg)
	gateway.Spec.DefaultScope = gatewayv1.GatewayDefaultScopeAll
	listener := &gateway.Spec.Listeners[0]
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	base := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "implicit", Generation: 1},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{UseDefaultGateways: gatewayv1.GatewayDefaultScopeAll},
			Rules: []gatewayv1.HTTPRouteRule{{}}},
	}
	for i := range controller.RouteParentStatusLimit {
		ref := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("foreign-%d", i))}
		base.Status.Parents = append(base.Status.Parents, gatewayv1.RouteParentStatus{ParentRef: ref, ControllerName: "foreign.example/controller"})
	}
	reported := base.DeepCopy()
	reported.Name = "already-counted"
	reported.Status.Parents[0] = gatewayv1.RouteParentStatus{ParentRef: parent, ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, ObservedGeneration: 1}}}
	rejected := reported.DeepCopy()
	rejected.Name, rejected.Status.Parents[0].Conditions[0].Status = "rejected", metav1.ConditionFalse
	denied := base.DeepCopy()
	denied.Name, denied.Namespace = "other-namespace", "apps"
	unsupported := base.DeepCopy()
	unsupported.Name, unsupported.Spec.Rules = "unsupported", nil
	scheme := testScheme(t)
	require.NoError(t, gatewayx.Install(scheme))
	kube := indexedFakeClientBuilder(scheme, cfg).WithObjects(base, reported, rejected, denied, unsupported).Build()
	r := &Reconciler{Client: kube, APIReader: kube, Config: cfg, AdmitsHTTPRouteListener: func(route *gatewayv1.HTTPRoute, _ *gatewayv1.Gateway, _ gatewayv1.ParentReference, _ *gatewayv1.Listener, _ []corev1.Namespace) bool {
		return len(route.Spec.Rules) != 0
	}}
	got, err := r.attachedRoutes(t.Context(), gateway, []*gatewayv1.Listener{listener})
	require.NoError(t, err)
	require.EqualValues(t, 2, got.counts[listener.Name])
}

func TestGatewayReportsGRPCBackendTLSAncestorLimitUntilRouteRecovers(t *testing.T) {
	cfg := testConfig()
	cfg.BackendTLSPolicy, cfg.BackendH2C, cfg.FixedHTTPResponses = true, true, true
	gateway := programmedTestGateway(cfg)
	parent := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}
	route := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "rpc", Generation: 1},
		Spec: gatewayv1.GRPCRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{parent}},
		},
	}
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef: parent, ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{
			{Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionTrue, ObservedGeneration: route.Generation},
			{Type: cfg.Domain() + "/Programmed", Status: metav1.ConditionFalse, ObservedGeneration: route.Generation, Reason: backendtls.AncestorLimitReason},
		},
	}}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway, route).WithObjects(class, gateway, route).Build()
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ReadyOutcome(),
		Gateway: cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "load-balancer", ListenerID: "listener", VIPAddress: "192.0.2.10"}),
	}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Writer: fixedGatewayGraph{result: result}}, nil)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	status := func() *metav1.Condition {
		t.Helper()
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, kube.Get(t.Context(), request.NamespacedName, gateway))
		require.Len(t, gateway.Status.Listeners, 1)
		require.EqualValues(t, 1, gateway.Status.Listeners[0].AttachedRoutes)
		require.True(t, meta.IsStatusConditionTrue(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)))
		return meta.FindStatusCondition(gateway.Status.Conditions, cfg.Domain()+"/BackendTLSPoliciesReady")
	}
	full := status()
	require.NotNil(t, full)
	require.Equal(t, metav1.ConditionFalse, full.Status)
	require.Equal(t, backendtls.AncestorLimitReason, full.Reason)
	require.Equal(t, gateway.Generation, full.ObservedGeneration)

	route.Status.Parents[0].Conditions[1].Status = metav1.ConditionTrue
	route.Status.Parents[0].Conditions[1].Reason = "Programmed"
	require.NoError(t, kube.Status().Update(t.Context(), route))
	require.Nil(t, status())
}

func TestGatewayReportsTerminatingTLSRouteBackendTLSAncestorLimit(t *testing.T) {
	cfg := testConfig()
	cfg.TCPTLSTermination, cfg.BackendTLSPolicy = true, true
	gateway := programmedTestGateway(cfg)
	listener := testSNIListener()
	listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	gateway.Spec.Listeners = []gatewayv1.Listener{listener}
	route := testSNIRoute(gateway, "tls", gateway.Namespace, "tls.example.com")
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef: route.Spec.ParentRefs[0], ControllerName: cfg.ControllerName,
		Conditions: []metav1.Condition{
			controller.Condition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, "Accepted", "Attached", route.Generation),
			controller.Condition(cfg.Domain()+"/Programmed", metav1.ConditionFalse, backendtls.AncestorLimitReason, backendtls.AncestorLimitMessage, route.Generation),
		},
	}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(route).Build()
	r := &Reconciler{APIReader: kube, Config: cfg}
	status := func() *metav1.Condition {
		t.Helper()
		require.NoError(t, r.setGatewayListenerStatuses(t.Context(), gateway, &listener, metav1.ConditionTrue,
			gatewayv1.GatewayReasonProgrammed, gatewayv1.ListenerReasonProgrammed, "Ready", "Ready", true, nil))
		require.EqualValues(t, 1, gateway.Status.Listeners[0].AttachedRoutes)
		return meta.FindStatusCondition(gateway.Status.Conditions, cfg.Domain()+"/BackendTLSPoliciesReady")
	}
	full := status()
	require.NotNil(t, full)
	require.Equal(t, metav1.ConditionFalse, full.Status)
	require.Equal(t, backendtls.AncestorLimitReason, full.Reason)
	require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(route), route))
	route.Status.Parents[0].Conditions[1].Status = metav1.ConditionTrue
	meta.SetStatusCondition(&route.Status.Parents[0].Conditions,
		controller.Condition(string(gatewayv1.RouteConditionResolvedRefs), metav1.ConditionFalse, backendtls.AncestorLimitReason, backendtls.AncestorLimitMessage, route.Generation))
	require.NoError(t, kube.Status().Update(t.Context(), route))
	require.NotNil(t, status(), "a weighted route can program healthy shares while one policy has no slot")
	meta.RemoveStatusCondition(&route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
	require.NoError(t, kube.Status().Update(t.Context(), route))
	require.Nil(t, status())
}

func TestGatewayReportsProvenUDPListenerWhenSiblingFails(t *testing.T) {
	cfg := testConfig()
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{
			{Name: "dns", Protocol: gatewayv1.UDPProtocolType, Port: 53},
			{Name: "radius", Protocol: gatewayv1.UDPProtocolType, Port: 1812},
		}},
	}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if result, err := reconciler.Reconcile(t.Context(), request); err != nil || !result.Requeue {
		t.Fatalf("binding checkpoint = %v, %v", result, err)
	}
	component := cloud.GatewayProgressingResult("Waiting for the Gateway UDP listeners", time.Second)
	component.State = cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"}
	component.ScopedListenerReadyPorts = []int{53}
	providerErr := cloud.NewProviderError(cloud.ErrorCategoryResourceFailure, errors.New("Octavia UDP listener provisioning status is ERROR"))
	reconciler.Writer = fixedGatewayGraph{result: cloud.GatewayGraphResult{Gateway: component}, err: providerErr}
	if result, err := reconciler.Reconcile(t.Context(), request); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("provider failure = %v, %v", result, err)
	}
	var current gatewayv1.Gateway
	if err := kubeClient.Get(t.Context(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.Reason != "ResourceFailed" {
		t.Fatalf("aggregate failure condition = %#v", programmed)
	}
	if len(current.Status.Listeners) != 2 ||
		!meta.IsStatusConditionTrue(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) ||
		!meta.IsStatusConditionFalse(current.Status.Listeners[1].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("listener conditions = %+v", current.Status.Listeners)
	}
	if len(current.Status.Addresses) != 1 || current.Status.Addresses[0].Value != "192.0.2.10" {
		t.Fatalf("proven address = %+v", current.Status.Addresses)
	}

	reconciler.Writer = fixedGatewayGraph{err: providerErr}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(t.Context(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionFalse(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("listener inherited historical proof after an unproven failure: %+v", current.Status.Listeners[0])
	}
}

func TestGatewayRouteFailurePreservesStatusAndRedactsDetails(t *testing.T) {
	for _, category := range []cloud.ErrorCategory{cloud.ErrorCategoryRateLimit, cloud.ErrorCategoryAuthorization, ""} {
		t.Run(string(category), func(t *testing.T) {
			cfg := testConfig()
			gateway := programmedTestGateway(cfg)
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
			current := &gatewayv1.Gateway{}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
				t.Fatal(err)
			}
			before := current.DeepCopy()
			cause := errors.New("response=private-body token=secret-token")
			providerErr := cause
			if category != "" {
				providerErr = cloud.NewProviderError(category, cause)
			}
			graphErr := &cloud.GraphError{Identity: cloud.Identity{RouteUID: "route-uid"}, Err: providerErr}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{err: graphErr}}, nil)
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)})
			if category == cloud.ErrorCategoryAuthorization {
				if err != nil || result.RequeueAfter <= 0 {
					t.Fatalf("authorization result=%#v error=%v", result, err)
				}
			} else if !errors.Is(err, cause) || strings.Contains(err.Error(), "private-body") || strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("route error=%v", err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(current.Status, before.Status) {
				t.Fatalf("route failure changed Gateway status: before=%#v after=%#v", before.Status, current.Status)
			}
		})
	}
}

func TestGatewayTransportRouteFailureRefreshesAttachments(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		protocol gatewayv1.ProtocolType
		renamed  bool
	}{
		{name: "TCP", protocol: gatewayv1.TCPProtocolType},
		{name: "UDP", protocol: gatewayv1.UDPProtocolType},
		{name: "renamed UDP listener", protocol: gatewayv1.UDPProtocolType, renamed: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			gateway := programmedTestGateway(cfg)
			listener := &gateway.Spec.Listeners[0]
			listener.Protocol = scenario.protocol
			listener.Name = gatewayv1.SectionName(strings.ToLower(string(scenario.protocol)))
			gateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(scenario.protocol)
			gateway.Status.Listeners = []gatewayv1.ListenerStatus{{
				Name: listener.Name, AttachedRoutes: 1, SupportedKinds: listenerSupportedRouteKinds(cfg, *listener),
				Conditions: []metav1.Condition{Condition(string(gatewayv1.ListenerConditionProgrammed), metav1.ConditionTrue,
					string(gatewayv1.ListenerReasonProgrammed), "Octavia listener is active", gateway.Generation)},
			}}
			if scenario.renamed {
				listener.Name = "renamed"
				gateway.Generation++
			}
			ref := gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name), SectionName: &listener.Name}
			spec := gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{ref}}
			newRoute := func(name, reason string) client.Object {
				metadata := metav1.ObjectMeta{Namespace: gateway.Namespace, Name: name, Generation: 1}
				status := gatewayv1.RouteStatus{Parents: []gatewayv1.RouteParentStatus{{
					ParentRef: ref, ControllerName: cfg.ControllerName,
					Conditions: []metav1.Condition{
						Condition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, string(gatewayv1.RouteReasonAccepted), "Route is accepted", 1),
						Condition(cfg.Domain()+"/Programmed", metav1.ConditionFalse, reason, "Route is not programmed", 1),
					},
				}}}
				if scenario.protocol == gatewayv1.TCPProtocolType {
					return &gatewayv1.TCPRoute{ObjectMeta: metadata, Spec: gatewayv1.TCPRouteSpec{CommonRouteSpec: spec}, Status: gatewayv1.TCPRouteStatus{RouteStatus: status}}
				}
				return &gatewayv1.UDPRoute{ObjectMeta: metadata, Spec: gatewayv1.UDPRouteSpec{CommonRouteSpec: spec}, Status: gatewayv1.UDPRouteStatus{RouteStatus: status}}
			}
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			older := newRoute("older", controller.ProviderReasonInvalidConfiguration)
			older.SetUID("older-route")
			newer := newRoute("newer", "NotSelected")
			kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway, older, newer).Build()
			graphErr := &cloud.GraphError{Identity: cloud.Identity{RouteUID: string(older.GetUID())}, Err: cloud.ErrTerminalValidation}
			reconciler := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Writer: fixedGatewayGraph{err: graphErr}}, nil)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
			var current gatewayv1.Gateway
			require.NoError(t, kube.Get(ctx, request.NamespacedName, &current))
			want := current.Status.DeepCopy()
			want.Listeners[0].AttachedRoutes = 2

			_, err := reconciler.Reconcile(ctx, request)
			require.NoError(t, err)

			require.NoError(t, kube.Get(ctx, request.NamespacedName, &current))
			if scenario.renamed {
				require.Equal(t, want.Conditions, current.Status.Conditions)
				require.Len(t, current.Status.Listeners, 1)
				status := current.Status.Listeners[0]
				require.Equal(t, listener.Name, status.Name)
				require.EqualValues(t, 2, status.AttachedRoutes)
				require.Equal(t, want.Listeners[0].SupportedKinds, status.SupportedKinds)
				require.True(t, meta.IsStatusConditionTrue(status.Conditions, string(gatewayv1.ListenerConditionAccepted)))
				programmed := meta.FindStatusCondition(status.Conditions, string(gatewayv1.ListenerConditionProgrammed))
				require.NotNil(t, programmed)
				require.Equal(t, metav1.ConditionFalse, programmed.Status)
				require.Equal(t, string(gatewayv1.ListenerReasonPending), programmed.Reason)
				require.Equal(t, gateway.Generation, programmed.ObservedGeneration)
				return
			}
			require.Equal(t, *want, current.Status)
		})
	}
}

func TestGatewayReplacementReportsImmutableAddress(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Spec.Listeners[0].Port++
	gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Value: "192.0.2.11"}}
	gateway.Generation++
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	component := cloud.GatewayProgressingResult("Blocked replacement", time.Second)
	component.State = cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"}
	component.AddressIssue = "Restore the currently bound VIP 192.0.2.10"
	r := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg,
		Writer: fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: component.Outcome, Gateway: component}}}, nil)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	current := &gatewayv1.Gateway{}
	if err := kubeClient.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.ObservedGeneration != gateway.Generation || programmed.Status != metav1.ConditionFalse ||
		programmed.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) || len(current.Status.Addresses) != 1 || current.Status.Addresses[0].Value != "192.0.2.10" {
		t.Fatalf("status = %#v, want actual address and current-generation address rejection", current.Status)
	}
}

func TestExperimentalAddressReplacementWaitsForActivityReadback(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayAPIChannel = features.FeatureChannelExperimental
	gateway := programmedTestGateway(cfg)
	setExperimentalStatusAddress(gateway, "192.0.2.10", sentinelRoutability)
	gateway.Spec.Listeners[0].Port++
	gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Routability: "example.com/unknown"}}
	gateway.Generation++
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	component := cloud.GatewayProgressingResult("Waiting to observe the pending Octavia load balancer", time.Second)
	component.State.LoadBalancerID = "lb-1"
	component.AddressIssue = "Unsupported address scope"
	writer := &fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: component.Outcome, Gateway: component}}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Writer: writer}, nil)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	result, err := r.Reconcile(t.Context(), request)
	if err != nil || result.RequeueAfter <= 0 || result.RequeueAfter >= cfg.OpenStackResyncInterval/2 {
		t.Fatalf("incomplete replacement observation = %v, %v; want a prompt recheck", result, err)
	}
	current := &gatewayv1.Gateway{}
	if err := kube.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	addresses := current.Status.Addresses
	assignment := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	if len(addresses) != 1 || addresses[0].Value != "192.0.2.10" ||
		ptr.Deref(addresses[0].Routability, "") != sentinelRoutability || assignment == nil ||
		assignment.Reason != string(gatewayv1.GatewayReasonAddressesNotAssigned) || assignment.ObservedGeneration != gateway.Generation {
		t.Fatalf("incomplete observation lost the retained sentinel address or counted it as assigned: %+v", current.Status)
	}

	// A subsequent complete observation finds a disabled frontend, while
	// the VIP itself has not yet been released by Octavia.
	writer.result.Gateway.AddressActivityObserved = true
	writer.result.Gateway.AddressBlocked = true
	writer.result.Gateway.State.VIPAddress = "192.0.2.10"
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if len(current.Status.Addresses) != 0 || programmed == nil || programmed.Status != metav1.ConditionFalse ||
		programmed.Reason != string(gatewayv1.GatewayReasonAddressNotAssigned) {
		t.Fatalf("confirmed inactivity must clear the retained address: %+v", current.Status)
	}
}

func TestExperimentalAddressRejectionComposesWithDeniedHTTPSReference(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayAPIChannel = features.FeatureChannelExperimental
	cfg.OperatorHTTPS = true
	gateway := programmedTestGateway(cfg)
	gateway.Status = gatewayv1.GatewayStatus{}
	gateway.Annotations, gateway.Finalizers = nil, nil
	gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Routability: "example.com/unknown"}}
	gateway.Spec.Listeners = append(gateway.Spec.Listeners, testHTTPSListener("https", 443))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	component := cloud.GatewayProgressingResult("Address scope cannot be assigned", time.Second)
	component.AddressIssue, component.AddressAbsent, component.AddressActivityObserved = component.Outcome.Message, true, true
	writer := &fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: component.Outcome, Gateway: component}}
	authorized := false
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Writer: writer,
		Coordinator: &graph.Coordinator{},
		Certificates: resolveHTTPSFunc(func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			if !authorized {
				return "", gatewayv1.ListenerReasonRefNotPermitted
			}
			return "approved-certificate", ""
		})}, &recordingProvider{})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if result, err := r.Reconcile(t.Context(), request); err != nil || !result.Requeue {
		t.Fatalf("initial binding checkpoint = %v, %v", result, err)
	}
	verify := func(programmedReason, assignmentReason gatewayv1.GatewayConditionReason, refsReason gatewayv1.ListenerConditionReason) {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(t.Context(), request.NamespacedName, gateway); err != nil {
			t.Fatal(err)
		}
		programmed := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		assignment := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
		refs := meta.FindStatusCondition(existingListenerConditions(gateway, "https"), string(gatewayv1.ListenerConditionResolvedRefs))
		wantProgrammed := metav1.ConditionFalse
		if programmedReason == gatewayv1.GatewayReasonProgrammed {
			wantProgrammed = metav1.ConditionTrue
		}
		if programmed == nil || programmed.Status != wantProgrammed || programmed.Reason != string(programmedReason) ||
			assignment == nil || assignment.Reason != string(assignmentReason) || refs == nil || refs.Reason != string(refsReason) {
			t.Fatalf("Gateway and listener conditions = %+v, want %s, %s, and %s", gateway.Status, programmedReason, assignmentReason, refsReason)
		}
	}
	verify(gatewayv1.GatewayReasonAddressNotAssigned, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.ListenerReasonRefNotPermitted)

	gateway.Spec.Addresses[0].Routability = sentinelRoutability
	gateway.Generation++
	if err := kube.Update(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	ready := cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"})
	ready.ScopedListenerReadyPorts = []int{80, 443}
	writer.result = cloud.GatewayGraphResult{Outcome: ready.Outcome, Gateway: ready}
	verify(gatewayv1.GatewayReasonListenersNotValid, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.ListenerReasonRefNotPermitted)
	authorized = true
	verify(gatewayv1.GatewayReasonProgrammed, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.ListenerReasonResolvedRefs)
}

type fixedGatewayGraph struct {
	result cloud.GatewayGraphResult
	err    error
}

func (w fixedGatewayGraph) ReconcileGraph(context.Context, graph.Request) (cloud.GatewayGraphResult, error) {
	return w.result, w.err
}

func TestWithdrawalPreservesOnlyCurrentProgrammedSiblings(t *testing.T) {
	g := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	for i, name := range []gatewayv1.SectionName{"withdrawing", "serving", "older", "new"} {
		port := gatewayv1.PortNumber(443)
		if i > 0 {
			port = 8443
		}
		g.Spec.Listeners = append(g.Spec.Listeners, gatewayv1.Listener{Name: name, Port: port})
		g.Status.Listeners = append(g.Status.Listeners, gatewayv1.ListenerStatus{Name: name, Conditions: []metav1.Condition{{
			Type: string(gatewayv1.ListenerConditionProgrammed), Status: metav1.ConditionFalse,
			ObservedGeneration: 3, Reason: "Pending", LastTransitionTime: metav1.Unix(100, 0),
		}}})
	}
	previous := g.Status.DeepCopy()
	for i := range previous.Listeners[:3] {
		c := &previous.Listeners[i].Conditions[0]
		c.Status, c.Reason, c.LastTransitionTime = metav1.ConditionTrue, "Programmed", metav1.Unix(50, 0)
	}
	previous.Listeners[2].Conditions[0].ObservedGeneration = 2
	preserveProgrammedListeners(g, *previous, []int{8443})
	for i, section := range g.Status.Listeners {
		condition := section.Conditions[0]
		if (condition.Status == metav1.ConditionTrue) != (i == 1) {
			t.Fatalf("unexpected Programmed condition for %s: %v", section.Name, condition)
		}
		if i == 1 && !condition.LastTransitionTime.Equal(&previous.Listeners[i].Conditions[0].LastTransitionTime) {
			t.Fatal("the retained sibling acquired a new transition timestamp")
		}
	}
}

func TestDeniedCertificatePreservesGatewayProviderFailure(t *testing.T) {
	g := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	g.Spec.Listeners = []gatewayv1.Listener{{Name: "web", Port: 443, Protocol: gatewayv1.HTTPSProtocolType}}
	g.Status.Conditions = []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionFalse,
		ObservedGeneration: 1, Reason: "NoResources", Message: "The provider quota is exhausted"}}
	previous := g.Status.DeepCopy()
	issues := map[gatewayv1.SectionName]httpsListenerStatusIssue{"web": {
		primary: httpsListenerIssue{
			condition: gatewayv1.ListenerConditionResolvedRefs, reason: gatewayv1.ListenerReasonInvalidCertificateRef,
			message: "No approved certificate is available for this listener",
		},
	}}
	(&Reconciler{}).applyHTTPSListenerStatus(g, *previous, issues, nil, true)
	aggregate := meta.FindStatusCondition(g.Status.Conditions, "Programmed")
	if aggregate.Reason != "NoResources" || aggregate.Message != previous.Conditions[0].Message ||
		!meta.IsStatusConditionFalse(g.Status.Listeners[0].Conditions, "ResolvedRefs") {
		t.Fatalf("expected both provider failure and listener certificate status, got %v", g.Status)
	}
}
