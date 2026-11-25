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

package model

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestXBackendSelectsReadyPodIPsOnItsOwnPort(t *testing.T) {
	backend := gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector,
			Port: gatewayx.BackendPort{Number: 8088},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "web", "stage": "prod"},
			}},
		},
	}
	ready := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Labels: map[string]string{"app": "web", "stage": "prod"}},
		Status: corev1.PodStatus{
			PodIP: "10.2.0.8", PodIPs: []corev1.PodIP{{IP: "10.2.0.8"}, {IP: "2001:db8::1"}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	missingLabel := ready.DeepCopy()
	delete(missingLabel.Labels, "stage")
	missingLabel.Status.PodIPs = nil
	missingLabel.Status.PodIP = "10.2.0.9"
	notReady := ready.DeepCopy()
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse
	notReady.Status.PodIPs = nil
	notReady.Status.PodIP = "10.2.0.10"
	deleting := ready.DeepCopy()
	deleting.DeletionTimestamp = ptr.To(metav1.Now())
	deleting.Status.PodIPs = nil
	deleting.Status.PodIP = "10.2.0.11"
	foreign := ready.DeepCopy()
	foreign.Namespace = "other"
	foreign.Status.PodIPs = nil
	foreign.Status.PodIP = "10.2.0.12"
	hostNetwork := ready.DeepCopy()
	hostNetwork.Spec.HostNetwork = true
	hostNetwork.Status.PodIPs = nil
	hostNetwork.Status.PodIP = "10.2.0.13"

	endpoints := ResolveXBackendEndpoints(&backend, []corev1.Pod{ready, *missingLabel, *notReady, *deleting, *foreign, *hostNetwork, ready})
	if len(endpoints) != 1 || endpoints[0].Address != "10.2.0.8" || endpoints[0].Port != 8088 {
		t.Fatalf("selected endpoints = %+v, want just 10.2.0.8:8088", endpoints)
	}

	ref := gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind(XBackendKind)), Name: "web",
	}}}
	input := Input{
		EnableXBackend: true, XBackendProfile: XBackendProfile{MemberMode: cloud.MemberModeEndpoint},
		XBackends: []XBackendObservation{{Key: types.NamespacedName{Namespace: "apps", Name: "web"}, Spec: backend.Spec, Endpoints: endpoints}},
	}
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "apps"}}
	group := resolveBackend(route, ref, input)
	if group.State != BackendResolved || group.XBackend.Name != "web" || group.Service.Name != "" || len(group.Endpoints) != 1 {
		t.Fatalf("group = %+v, want resolved XBackend", group)
	}
	input.XBackendProfile.MemberMode = cloud.MemberModeNodePort
	group = resolveBackend(route, ref, input)
	if group.State != BackendInvalid || group.Diagnostic.Reason != string(gatewayv1.RouteReasonUnsupportedValue) || len(group.Endpoints) != 0 {
		t.Fatalf("group in NodePort mode = %+v", group)
	}
	ref.Namespace = ptr.To(gatewayv1.Namespace("other"))
	group = resolveBackend(route, ref, input)
	if group.Authorization != BackendAuthorizationDenied || group.Diagnostic.Reason != string(gatewayv1.RouteReasonRefNotPermitted) {
		t.Fatalf("foreign reference = %+v, want RefNotPermitted", group)
	}
}

func TestXBackendExternalHostnameProfile(t *testing.T) {
	ref := gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind(XBackendKind)), Name: "origin",
	}}}
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "apps"}}
	spec := gatewayx.BackendSpec{
		Type:             gatewayx.BackendTypeExternalHostname,
		Port:             gatewayx.BackendPort{Number: 8089},
		ExternalHostname: &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"},
	}
	input := Input{
		EnableXBackend: true, XBackendProfile: XBackendProfile{MemberMode: cloud.MemberModeEndpoint, ExternalHostname: true, ServiceDNSDomains: []string{"svc.home.test"}},
		XBackends: []XBackendObservation{{Key: types.NamespacedName{Namespace: "apps", Name: "origin"}, Spec: spec}},
	}
	if group := resolveBackend(route, ref, input); group.State != BackendResolved || !group.XBackendRoutedExternal || group.XBackendPort != 8089 {
		t.Fatalf("valid hostname on opted-in Gateway did not resolve: %+v", group)
	}
	for _, hostname := range []gatewayv1.PreciseHostname{
		"192.0.2.5", "2001:db8::5", "127.1", "*.example.test", "service", "origin.example.test.",
		"cluster.local", "svc.apps.svc.cluster.local", "svc.home.test", "svc.apps.svc.home.test",
	} {
		spec.ExternalHostname.Hostname = hostname
		if got := ValidateXBackend(spec, input.XBackendProfile); got == nil || got.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
			t.Errorf("%q must fail hostname validation: %+v", hostname, got)
		}
	}
	spec.ExternalHostname.Hostname = "external-svc.home.test"
	if got := ValidateXBackend(spec, input.XBackendProfile); got != nil {
		t.Fatalf("service suffix comparison must match entire labels: %+v", got)
	}
	for _, mode := range []gatewayx.BackendTLSMode{gatewayx.BackendTLSModeServerOnly, gatewayx.BackendTLSModeClientAndServer} {
		spec.TLS = &gatewayx.BackendTLS{Mode: mode}
		if got := ValidateXBackend(spec, input.XBackendProfile); got == nil {
			t.Errorf("TLS mode %s must fail closed", mode)
		}
	}
	spec.TLS = nil
	ref.Port = ptr.To(gatewayv1.PortNumber(8089))
	if group := resolveBackend(route, ref, input); group.State != BackendInvalid || group.Diagnostic.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
		t.Fatalf("duplicating the XBackend port must be rejected: %+v", group)
	}
	ref.Port = nil
	input.XBackendProfile.MemberMode = cloud.MemberModeNodePort
	if group := resolveBackend(route, ref, input); group.State != BackendInvalid || group.Diagnostic.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
		t.Fatalf("external backend must not use the worker member role: %+v", group)
	}
	input.XBackendProfile.MemberMode = cloud.MemberModeEndpoint
	input.XBackendProfile.ExternalHostname = false
	if group := resolveBackend(route, ref, input); group.State != BackendInvalid || group.Diagnostic.Reason != string(gatewayv1.RouteReasonUnsupportedValue) {
		t.Fatalf("Experimental needs a separate external backend opt-in: %+v", group)
	}
	input.EnableXBackend = false
	if group := resolveBackend(route, ref, input); group.State != BackendInvalid || group.Diagnostic.Reason != string(gatewayv1.RouteReasonInvalidKind) {
		t.Fatalf("Standard must reject Experimental XBackend references: %+v", group)
	}
}
