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
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
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
	filtered.Filters.Requests[1].ExternalAuth.ForwardBody = &gatewayv1.ForwardBodyConfig{MaxSize: 4096}
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
	require.Zero(t, chain[0].ForwardBodyMaxSize)
	require.EqualValues(t, 4096, chain[1].ForwardBodyMaxSize)
	require.False(t, GatewayOwnsListenerRequestFilters(controller.Config{GatewayListenerFilters: true,
		GatewayAPIChannel: features.FeatureChannelStandard}, &filtered))
	https := filtered
	https.Protocol = gatewayv1.HTTPSProtocolType
	require.False(t, GatewayOwnsListenerRequestFilters(config, &https))
	config.OperatorHTTPS = true
	require.True(t, GatewayOwnsListenerRequestFilters(config, &https))
	https.Port = 443
	https.TLS = &gatewayv1.ListenerTLSConfig{
		CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "cert"}},
	}
	tlsGateway := gateway.DeepCopy()
	tlsGateway.Spec.Listeners = []gatewayv1.Listener{https}
	r.Config = config
	httpsListeners, err := SupportedListenersForConfig(config, tlsGateway)
	require.NoError(t, err)
	tlsFilters, err := r.observeListenerFilters(t.Context(), tlsGateway, httpsListeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Empty(t, tlsFilters.issues)
	require.Equal(t, got.ports[80][1:2], tlsFilters.ports[443])
	require.Equal(t, []string{"host", "x-identity"}, chain[0].AllowedResponseHeaders)
	require.Equal(t, []cloud.Member{{Address: "192.0.2.5", Port: 9443}}, chain[0].Members)

	require.NoError(t, reader.Delete(t.Context(), endpoints))
	got, err = r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Contains(t, got.unavailable, filtered.Name)
	require.Empty(t, got.ports[80][1].Requests[0].Members)

	require.NoError(t, reader.Delete(t.Context(), grant))
	tlsFilters, err = r.observeListenerFilters(t.Context(), tlsGateway, httpsListeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.Equal(t, []cloud.ListenerRequestFilterSection{{Hostname: string(*https.Hostname), Reject: true}}, tlsFilters.ports[443])
	gateway.Status.Listeners = []gatewayv1.ListenerStatus{{Name: filtered.Name}, {Name: peer.Name}}
	got, err = r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, topology)
	require.NoError(t, err)
	require.True(t, got.ports[80][1].Reject)
	r.applyListenerFilterStatus(gateway, gatewayv1.GatewayStatus{}, got, cloud.GatewayResult{}, nil)
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
	require.True(t, hasHTTPFamilyListenerFilters(config, gateway))
	listeners, err := SupportedListenersForConfig(config, gateway)
	require.NoError(t, err)
	got, err := r.observeListenerFilters(t.Context(), gateway, listeners, ListenerSetProjection{}, classconfig.Resolved{})
	require.NoError(t, err)
	require.True(t, got.ports[443][0].Reject)
	gateway.Status.Listeners = []gatewayv1.ListenerStatus{{Name: "secured"}}
	r.applyListenerFilterStatus(gateway, gatewayv1.GatewayStatus{}, got, cloud.GatewayResult{}, nil)
	conditions := gateway.Status.Listeners[0].Conditions
	index := slices.IndexFunc(conditions, func(condition metav1.Condition) bool {
		return condition.Type == string(gatewayv1.ListenerConditionAccepted)
	})
	require.NotEqual(t, -1, index)
	require.Equal(t, "UnsupportedValue", conditions[index].Reason)
	require.Contains(t, conditions[index].Message, "filters.requests[0]")
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

			r.applyListenerFilterStatus(gateway, previous, listenerFilterObservation{}, tc.provider, nil)

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

func TestGatewayListenerAuthorizerTransportsReconcileAndWithdrawTrust(t *testing.T) {
	cfg := testConfig()
	cfg.GatewayAPIChannel = features.FeatureChannelExperimental
	cfg.GatewayListenerFilters, cfg.OperatorHTTPS, cfg.BackendTLSPolicy, cfg.HTTPSListenerIsolation = true, true, true, true
	gw := programmedTestGateway(cfg)
	gw.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.HTTPProtocolType)
	topology := testBoundTopology()
	topology.Topology.MemberMode = cloud.MemberModeEndpoint
	require.NoError(t, controller.BindGatewayTopology(cfg, gw, topology))
	controller.SetGatewayListenerScope(cfg, gw, gatewayv1.HTTPProtocolType)

	h2c, h2cEndpoints := testSNIBackend(gw.Namespace, "auth-h2c", "192.0.2.5")
	h2c.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	secure, secureEndpoints := testSNIBackend("security", "auth-tls", "192.0.2.6")
	secure.Spec.Ports[0].AppProtocol = ptr.To("https")
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: secure.Namespace, Name: "listener-auth"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: gatewayv1.Namespace(gw.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(secure.Name))}},
		},
	}
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: secure.Namespace, Name: "backend-ca", UID: "ca-uid"},
		Data: map[string]string{"ca.crt": frontendTestCA(t)}}
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: secure.Namespace, Name: "listener-auth", UID: "policy-uid", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(secure.Name)},
				SectionName:                ptr.To(gatewayv1.SectionName("tls")),
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{Hostname: "auth.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: gatewayv1.ObjectName(ca.Name)}}},
		},
		Status: gatewayv1.PolicyStatus{Ancestors: []gatewayv1.PolicyAncestorStatus{{
			AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gw.Name), Namespace: ptr.To(gatewayv1.Namespace(gw.Namespace))},
			ControllerName: cfg.ControllerName,
		}}},
	}
	request := func(protocol gatewayv1.HTTPRouteExternalAuthProtocol, service *corev1.Service) gatewayv1.ListenerFilter {
		return gatewayv1.ListenerFilter{Type: gatewayv1.ListenerFilterExternalAuth, ExternalAuth: &gatewayv1.HTTPExternalAuthFilter{
			ExternalAuthProtocol: protocol,
			BackendRef: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(service.Name),
				Namespace: ptr.To(gatewayv1.Namespace(service.Namespace)), Port: ptr.To(gatewayv1.PortNumber(8443))},
		}}
	}
	filters := &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{
		request(gatewayv1.HTTPRouteExternalAuthGRPCProtocol, h2c),
		request(gatewayv1.HTTPRouteExternalAuthHTTPProtocol, secure),
		request(gatewayv1.HTTPRouteExternalAuthGRPCProtocol, secure),
	}}
	filters.Requests[0].ExternalAuth.GRPCAuthConfig = &gatewayv1.GRPCAuthConfig{AllowedRequestHeaders: []string{"X-User", ":authority"}}
	filters.Requests[0].ExternalAuth.ForwardBody = &gatewayv1.ForwardBodyConfig{MaxSize: 4096}
	filters.Requests[1].ExternalAuth.HTTPAuthConfig = &gatewayv1.HTTPAuthConfig{Path: "/check"}
	filters.Requests[2].ExternalAuth.GRPCAuthConfig = &gatewayv1.GRPCAuthConfig{}
	httpProtected := gatewayv1.Listener{Name: "http-protected", Protocol: gatewayv1.HTTPProtocolType, Port: 80,
		Hostname: ptr.To(gatewayv1.Hostname("protected.example.test")), Filters: filters}
	httpsPublic := testHTTPSListener("https-public", 443)
	httpsProtected := testHTTPSListener("https-protected", 443)
	httpsProtected.Hostname, httpsProtected.Filters = httpProtected.Hostname, filters.DeepCopy()
	gw.Spec.Listeners = append(gw.Spec.Listeners, httpProtected, httpsPublic, httpsProtected)
	gw.Annotations[cfg.GatewayHTTPSIsolatedPortsAnnotation()] = desiredHTTPSIsolatedPorts(cfg, gw)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack", Generation: 1}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName},
		Status: gatewayv1.GatewayClassStatus{Conditions: []metav1.Condition{
			controller.Condition(string(gatewayv1.GatewayClassConditionStatusSupportedVersion), metav1.ConditionTrue, "SupportedVersion", "Supported bundle", 1),
		}},
	}
	kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gw, class, policy).
		WithObjects(gw, class, policy, ca, grant, h2c, h2cEndpoints, secure, secureEndpoints).Build()
	ready := cloud.GatewayResult{Outcome: cloud.ReadyOutcome(), ScopedListenerReadyPorts: []int{80, 443}}
	provider := &recordingProvider{gatewayResult: &ready}
	r := newTestReconciler(Reconciler{Client: kube, APIReader: kube, Config: cfg, Coordinator: &graph.Coordinator{},
		HTTPSListenerIsolationAvailable: func(context.Context) (bool, error) { return true, nil },
		Certificates: resolveHTTPSFunc(func(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
			return "server-certificate", ""
		})}, provider)
	reconcileKey := client.ObjectKeyFromObject(gw)
	reconcile := func() map[int][]cloud.ListenerRequestFilterSection {
		t.Helper()
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: reconcileKey})
		require.NoError(t, err)
		require.NoError(t, kube.Get(t.Context(), reconcileKey, gw))
		require.NotEmpty(t, provider.gatewaySpecs, "Gateway status: %#v", gw.Status)
		return provider.gatewaySpecs[len(provider.gatewaySpecs)-1].RequestFilterSections
	}
	condition := func(name gatewayv1.SectionName, kind gatewayv1.ListenerConditionType) *metav1.Condition {
		t.Helper()
		index := slices.IndexFunc(gw.Status.Listeners, func(status gatewayv1.ListenerStatus) bool { return status.Name == name })
		require.GreaterOrEqual(t, index, 0)
		result := meta.FindStatusCondition(gw.Status.Listeners[index].Conditions, string(kind))
		require.NotNil(t, result)
		return result
	}
	consumers, err := r.ObserveListenerBackendTLSPolicy(t.Context(), policy)
	require.NoError(t, err)
	require.Len(t, consumers[reconcileKey], 4)
	require.True(t, slices.ContainsFunc(consumers[reconcileKey], func(consumer ListenerBackendTLSPolicyConsumer) bool {
		return consumer.Protocol == cloud.ExternalAuthProtocolGRPC && consumer.Compatible && consumer.Result.Validation != nil
	}))
	require.Equal(t, []ctrl.Request{{NamespacedName: reconcileKey}}, r.gatewayRequestsForListenerAuthCA(t.Context(), ca))

	sections := reconcile()
	require.Equal(t, sections[80], sections[443])
	require.Equal(t, cloud.ListenerRequestFilterSection{Hostname: "*"}, sections[80][0])
	chain := sections[80][1].Requests
	require.Len(t, chain, 3)
	require.Equal(t, cloud.ExternalAuthProtocolGRPC, chain[0].Protocol)
	require.Nil(t, chain[0].BackendTLS)
	require.Equal(t, []string{":authority", "x-user"}, chain[0].AllowedRequestHeaders)
	require.EqualValues(t, 4096, chain[0].ForwardBodyMaxSize)
	require.Equal(t, "/check", chain[1].PathPrefix)
	require.Equal(t, "auth.example.test", chain[1].BackendTLS.Hostname)
	require.False(t, chain[1].BackendTLS.RequireH2)
	require.Equal(t, string(ca.UID), chain[1].BackendTLS.CAConfigMapUID)
	require.Equal(t, cloud.ExternalAuthProtocolGRPC, chain[2].Protocol)
	require.True(t, chain[2].BackendTLS.RequireH2)

	ca.Data["ca.crt"] = frontendTestCA(t)
	require.NoError(t, kube.Update(t.Context(), ca))
	rotated := reconcile()[443][1].Requests
	require.NotEqual(t, chain[1].BackendTLS.CADigest, rotated[1].BackendTLS.CADigest)
	require.Equal(t, rotated[1].BackendTLS.CADigest, rotated[2].BackendTLS.CADigest)

	denied := []cloud.ListenerRequestFilterSection{{Hostname: "*"}, {Hostname: "protected.example.test", Reject: true}}
	h2c.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/wss")
	require.NoError(t, kube.Update(t.Context(), h2c))
	sections = reconcile()
	require.Equal(t, denied, sections[80])
	require.Equal(t, denied, sections[443])
	require.Contains(t, condition("https-protected", gatewayv1.ListenerConditionResolvedRefs).Message, "appProtocol")
	h2c.Spec.Ports[0].AppProtocol = nil
	require.NoError(t, kube.Update(t.Context(), h2c))
	implicit := reconcile()[443][1].Requests[0]
	require.Equal(t, cloud.ExternalAuthProtocolGRPC, implicit.Protocol)
	require.Nil(t, implicit.BackendTLS)

	writer := r.Writer.(*graph.Writer)
	r.APIReader = unavailableListenerAuthCA{Reader: kube, key: client.ObjectKeyFromObject(ca)}
	require.Equal(t, denied, reconcile()[443])
	require.Equal(t, metav1.ConditionUnknown, condition("https-protected", gatewayv1.ListenerConditionResolvedRefs).Status)
	require.Equal(t, string(gatewayv1.ListenerReasonPending), condition("https-protected", gatewayv1.ListenerConditionProgrammed).Reason)

	var withdrew bool
	writer.Routes = failingHTTPSRouteSource{backendtls.ErrClusterTrustBundleUnavailable}
	writer.Provider = httpsGraphProvider(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		require.Equal(t, cloud.GraphWithdrawListenerFilters, spec.Gateway.Disposition)
		require.Equal(t, map[int][]cloud.ListenerRequestFilterSection{
			80: {{Hostname: "protected.example.test", Reject: true}}, 443: {{Hostname: "protected.example.test", Reject: true}},
		}, spec.Gateway.Spec.RequestFilterSections)
		require.NoError(t, spec.ValidateMutation(ctx))
		withdrew = true
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: reconcileKey})
	require.ErrorIs(t, err, backendtls.ErrClusterTrustBundleUnavailable)
	require.True(t, withdrew)
	require.NoError(t, kube.Get(t.Context(), reconcileKey, gw))
	refs := condition("https-protected", gatewayv1.ListenerConditionResolvedRefs)
	require.Equal(t, metav1.ConditionUnknown, refs.Status)
	require.Equal(t, string(gatewayv1.ListenerReasonPending), refs.Reason)
	require.Contains(t, refs.Message, "Waiting to read the referenced CA ConfigMap")
	require.Equal(t, string(gatewayv1.ListenerReasonPending), condition("https-protected", gatewayv1.ListenerConditionProgrammed).Reason)
	writer.Routes, writer.Provider, r.APIReader = emptyTestRoutes{}, provider, kube

	ready.ListenerFilterTransportUnavailableSections = map[int][]string{443: {"protected.example.test"}}
	_ = reconcile()
	require.Equal(t, string(gatewayv1.ListenerReasonUnsupportedValue), condition("https-protected", gatewayv1.ListenerConditionAccepted).Reason)
	require.Equal(t, metav1.ConditionTrue, condition("https-public", gatewayv1.ListenerConditionProgrammed).Status)
	ready.ListenerFilterTransportUnavailableSections = nil

	for _, withdraw := range []client.Object{ca, policy, grant} {
		require.NoError(t, kube.Delete(t.Context(), withdraw))
		sections = reconcile()
		for _, port := range []int{80, 443} {
			require.Equal(t, denied, sections[port])
		}
		require.Equal(t, metav1.ConditionFalse, condition("https-protected", gatewayv1.ListenerConditionResolvedRefs).Status)
		require.Equal(t, metav1.ConditionFalse, condition("https-protected", gatewayv1.ListenerConditionProgrammed).Status)
		require.Equal(t, metav1.ConditionTrue, condition("https-public", gatewayv1.ListenerConditionProgrammed).Status)
	}
	require.Equal(t, string(gatewayv1.ListenerReasonRefNotPermitted), condition("https-protected", gatewayv1.ListenerConditionResolvedRefs).Reason)
}

type unavailableListenerAuthCA struct {
	client.Reader
	key client.ObjectKey
}

func (r unavailableListenerAuthCA) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, configMap := object.(*corev1.ConfigMap); configMap && key == r.key {
		return errors.New("authorization CA lookup temporarily unavailable")
	}
	return r.Reader.Get(ctx, key, object, opts...)
}
