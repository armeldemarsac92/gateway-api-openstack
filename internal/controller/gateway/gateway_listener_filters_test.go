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
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/stretchr/testify/require"
)

func TestListenerFiltersResolveGatewayGrantAndKeepDeclaredOrder(t *testing.T) {
	service, endpoints := testSNIBackend("security", "auth-service", "192.0.2.5")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "authorize"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: "app"}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(service.Name))}},
		},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, gatewayv1.Install(scheme))
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(service, endpoints, grant).Build()
	config := controller.Config{GatewayListenerFilters: true, GatewayAPIChannel: features.FeatureChannelExperimental}
	r := &Reconciler{Config: config, APIReader: reader}
	step := func(prefix string) gatewayv1.ListenerFilter {
		return gatewayv1.ListenerFilter{Type: gatewayv1.ListenerFilterExternalAuth, ExternalAuth: &gatewayv1.HTTPExternalAuthFilter{
			ExternalAuthProtocol: gatewayv1.HTTPRouteExternalAuthHTTPProtocol,
			BackendRef:           gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(service.Name), Namespace: ptr.To(gatewayv1.Namespace(service.Namespace)), Port: ptr.To(gatewayv1.PortNumber(8443))},
			HTTPAuthConfig:       &gatewayv1.HTTPAuthConfig{Path: prefix, AllowedResponseHeaders: []string{"X-Identity", "Host"}},
		}}
	}
	filtered := gatewayv1.Listener{Name: "secured", Protocol: gatewayv1.HTTPProtocolType, Port: 80, Hostname: ptr.To(gatewayv1.Hostname("protected.example.test")),
		Filters: &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{step("/first"), step("/second")}}}
	peer := gatewayv1.Listener{Name: "ordinary", Protocol: gatewayv1.HTTPProtocolType, Port: 80}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}, Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{filtered, peer}}}
	topology := classconfig.Resolved{Topology: cloud.Topology{MemberMode: cloud.MemberModeEndpoint}}
	listeners := []*gatewayv1.Listener{&gateway.Spec.Listeners[0], &gateway.Spec.Listeners[1]}
	got, err := r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Empty(t, got.issues)
	require.Equal(t, "*", got.ports[80][0].Hostname)
	require.Empty(t, got.ports[80][0].Requests)
	chain := got.ports[80][1].Requests
	require.Len(t, chain, 2)
	require.Equal(t, []string{"/first", "/second"}, []string{chain[0].PathPrefix, chain[1].PathPrefix})
	require.Equal(t, []string{"host", "x-identity"}, chain[0].AllowedResponseHeaders)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.5", Port: 9443}}, chain[0].Members)

	require.NoError(t, reader.Delete(t.Context(), endpoints))
	got, err = r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Contains(t, got.unavailable, filtered.Name)
	require.Empty(t, got.ports[80][1].Requests[0].Members)

	require.NoError(t, reader.Delete(t.Context(), grant))
	gateway.Status.Listeners = []gatewayv1.ListenerStatus{{Name: filtered.Name}, {Name: peer.Name}}
	got, err = r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.True(t, got.ports[80][1].Reject)
	r.applyListenerFilterStatus(gateway, gatewayv1.GatewayStatus{}, got, cloud.GatewayResult{})
	condition := meta.FindStatusCondition(gateway.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
	require.NotNil(t, condition)
	require.Equal(t, string(gatewayv1.ListenerReasonRefNotPermitted), condition.Reason)
	require.Contains(t, condition.Message, "filters.requests[0]")
	require.Empty(t, gateway.Status.Listeners[1].Conditions)

	require.NoError(t, reader.Create(t.Context(), &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: "renewed"}, Spec: grant.Spec}))
	require.NoError(t, reader.Delete(t.Context(), service))
	got, err = r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Equal(t, string(gatewayv1.RouteReasonBackendNotFound), string(got.issues[filtered.Name].reason))
	require.True(t, got.ports[80][1].Reject)
}

func TestListenerFiltersRejectUnknownExtensionOnHTTPS(t *testing.T) {
	config := controller.Config{OperatorHTTPS: true, GatewayListenerFilters: true, GatewayAPIChannel: features.FeatureChannelExperimental}
	r := &Reconciler{Config: config}
	gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{
		Name: "secured", Protocol: gatewayv1.HTTPSProtocolType, Port: 443,
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "cert"}}},
		Filters: &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{{
			Type: gatewayv1.ListenerFilterExtensionRef, ExtensionRef: &gatewayv1.LocalObjectReference{Group: "custom.example", Kind: "Filter", Name: "unknown"},
		}}},
	}}}}
	require.True(t, hasHTTPFamilyListenerFilters(gateway))
	listeners, err := SupportedListenersForConfig(config, gateway)
	require.NoError(t, err)
	got, err := r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, classconfig.Resolved{})
	require.NoError(t, err)
	require.True(t, got.ports[443][0].Reject)
	gateway.Status.Listeners = []gatewayv1.ListenerStatus{{Name: "secured"}}
	r.applyListenerFilterStatus(gateway, gatewayv1.GatewayStatus{}, got, cloud.GatewayResult{})
	conditions := gateway.Status.Listeners[0].Conditions
	index := slices.IndexFunc(conditions, func(condition metav1.Condition) bool {
		return condition.Type == string(gatewayv1.ListenerConditionAccepted)
	})
	require.NotEqual(t, -1, index)
	require.Equal(t, "UnsupportedValue", conditions[index].Reason)
	require.Contains(t, conditions[index].Message, "filters.requests[0]")
}

func TestListenerFiltersValidateOctaviaHeaderSelection(t *testing.T) {
	require.True(t, validAuthHeaders([]string{strings.Repeat("x", 256), "X-Identity", "Host", ":authority"}))
	require.False(t, validAuthHeaders([]string{strings.Repeat("x", 257)}))
	require.False(t, validAuthHeaders([]string{"X Identity"}))
	require.False(t, validAuthHeaders([]string{"X-K"}))
	require.Nil(t, normalizeAuthHeaders(nil))
	require.Equal(t, []string{"host"}, normalizeAuthHeaders([]string{":authority", "Host"}))
	require.Equal(t, []string{"authority", "host", "x-identity"}, normalizeAuthHeaders([]string{"X-Identity", "Host", "Authority"}))
}

func TestListenerFiltersAggregateProgrammedAndKeepTransitionTimes(t *testing.T) {
	r := &Reconciler{}
	configured := gatewayv1.Listener{Name: "protected", Port: 80, Filters: &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{{Type: gatewayv1.ListenerFilterExternalAuth}}}}
	plain := gatewayv1.Listener{Name: "plain", Port: 80}
	for _, tc := range []struct {
		name       string
		provider   cloud.GatewayResult
		accepted   metav1.ConditionStatus
		programmed gatewayv1.GatewayConditionReason
	}{
		{"discovery temporarily unavailable", cloud.GatewayResult{ListenerFilterCapabilityPendingPorts: []int{80}}, metav1.ConditionTrue, gatewayv1.GatewayReasonPending},
		{"capability unsupported", cloud.GatewayResult{ListenerFilterUnavailablePorts: []int{80}}, metav1.ConditionFalse, gatewayv1.GatewayReasonListenersNotValid},
		{"authorization backend recovering", cloud.GatewayResult{ListenerFilterRecoveringSections: map[int][]string{80: {"*"}}}, metav1.ConditionTrue, gatewayv1.GatewayReasonPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			programmed := string(gatewayv1.ListenerConditionProgrammed)
			accepted := string(gatewayv1.ListenerConditionAccepted)
			priorTime := metav1.NewTime(time.Unix(30, 0))
			priorProgrammed := metav1.Condition{Type: programmed, Status: metav1.ConditionFalse, LastTransitionTime: priorTime}
			priorAccepted := metav1.Condition{Type: accepted, Status: tc.accepted, LastTransitionTime: priorTime}
			previous := gatewayv1.GatewayStatus{
				Conditions: []metav1.Condition{priorProgrammed},
				Listeners:  []gatewayv1.ListenerStatus{{Name: configured.Name, Conditions: []metav1.Condition{priorProgrammed, priorAccepted}}},
			}
			gateway := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Generation: 4},
				Spec:       gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{configured, plain}},
			}
			controller.SetCondition(&gateway.Status.Conditions, controller.Condition(programmed, metav1.ConditionTrue, "Programmed", "Provider is ready", 4))
			for _, listener := range gateway.Spec.Listeners {
				status := gatewayv1.ListenerStatus{Name: listener.Name}
				setAcceptedListenerConditions(&status.Conditions, 4)
				controller.SetCondition(&status.Conditions, controller.Condition(programmed, metav1.ConditionTrue, "Programmed", "Octavia is ready", 4))
				gateway.Status.Listeners = append(gateway.Status.Listeners, status)
			}

			r.applyListenerFilterStatus(gateway, previous, listenerFilterObservation{}, tc.provider)

			condition := meta.FindStatusCondition(gateway.Status.Conditions, programmed)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, string(tc.programmed), condition.Reason)
			require.Equal(t, int64(4), condition.ObservedGeneration)
			require.Equal(t, priorTime, condition.LastTransitionTime)
			protectedConditions := gateway.Status.Listeners[0].Conditions
			require.Equal(t, priorTime, meta.FindStatusCondition(protectedConditions, programmed).LastTransitionTime)
			require.Equal(t, tc.accepted, meta.FindStatusCondition(protectedConditions, accepted).Status)
			require.Equal(t, priorTime, meta.FindStatusCondition(protectedConditions, accepted).LastTransitionTime)
			require.True(t, meta.IsStatusConditionTrue(gateway.Status.Listeners[1].Conditions, programmed))
		})
	}
}
