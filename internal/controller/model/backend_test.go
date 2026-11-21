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
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

func TestBackendGroupsPreserveWeightsAndOverlappingEndpoints(t *testing.T) {
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}}
	rule := gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{
		backendReference("a", ptr.To(int32(80))), backendReference("b", ptr.To(int32(20))),
		backendReference("missing", ptr.To(int32(10))), backendReference("a", ptr.To(int32(0))),
		backendReference("empty", nil),
	}}
	input := Input{
		Services: []corev1.Service{backendService("a"), backendService("b"), backendService("empty")},
		EndpointSlices: []discoveryv1.EndpointSlice{
			backendSlice("a", "192.0.2.10", "192.0.2.11"), backendSlice("b", "192.0.2.10"),
		},
	}
	groups := ResolveBackends(route, rule, input)
	if len(groups) != len(rule.BackendRefs) {
		t.Fatalf("got %d groups for %d backend references", len(groups), len(rule.BackendRefs))
	}
	for index, weight := range []int32{80, 20, 10, 0, 1} {
		if groups[index].Weight != weight {
			t.Errorf("group %d weight = %d, want %d", index, groups[index].Weight, weight)
		}
	}
	if groups[0].Endpoints[0] != groups[1].Endpoints[0] || len(groups[0].Endpoints) != 2 || len(groups[1].Endpoints) != 1 {
		t.Fatalf("overlapping endpoints were not retained per group: %#v", groups)
	}
	if groups[2].State != BackendInvalid || groups[2].Diagnostic.Reason != string(gatewayv1.RouteReasonBackendNotFound) {
		t.Fatalf("missing backend = %#v", groups[2])
	}
	if groups[3].State != BackendResolved || len(groups[3].Endpoints) != 2 {
		t.Fatalf("zero-weight group was discarded: %#v", groups[3])
	}
	if groups[4].State != BackendResolved || len(groups[4].Endpoints) != 0 || groups[4].Diagnostic != nil {
		t.Fatalf("valid empty Service was marked invalid: %#v", groups[4])
	}

	// Endpoint changes do not alter a Service's share or remove the empty group.
	input.EndpointSlices[0].Endpoints = input.EndpointSlices[0].Endpoints[:1]
	input.EndpointSlices[1].Endpoints = nil
	updated := ResolveBackends(route, rule, input)
	for index := range groups {
		if updated[index].Weight != groups[index].Weight || updated[index].State != groups[index].State {
			t.Errorf("endpoint change altered group %d selection: before=%#v, after=%#v", index, groups[index], updated[index])
		}
	}
	if len(updated[0].Endpoints) != 1 || len(updated[1].Endpoints) != 0 {
		t.Fatalf("endpoint change was not observed: %#v", updated)
	}
}

func TestBackendReferenceGrantAuthorization(t *testing.T) {
	grant := gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backend"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: "app"}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("echo"))}},
		},
	}
	cases := []struct {
		name   string
		mutate func(*gatewayv1.ReferenceGrant)
		allow  bool
	}{
		{name: "exact grant", allow: true},
		{name: "any Service name", allow: true, mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.To[0].Name = nil }},
		{name: "wrong grant namespace", mutate: func(g *gatewayv1.ReferenceGrant) { g.Namespace = "app" }},
		{name: "wrong source group", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.From[0].Group = "" }},
		{name: "wrong source kind", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.From[0].Kind = "Gateway" }},
		{name: "wrong source namespace", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.From[0].Namespace = "other" }},
		{name: "wrong target group", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.To[0].Group = gatewayv1.GroupName }},
		{name: "wrong target kind", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.To[0].Kind = "Secret" }},
		{name: "wrong target name", mutate: func(g *gatewayv1.ReferenceGrant) { g.Spec.To[0].Name = ptr.To(gatewayv1.ObjectName("other")) }},
		{name: "alternatives within grant", allow: true, mutate: func(g *gatewayv1.ReferenceGrant) {
			g.Spec.From = append([]gatewayv1.ReferenceGrantFrom{{Group: "", Kind: "Gateway", Namespace: "other"}}, g.Spec.From...)
			g.Spec.To = append([]gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Secret"}}, g.Spec.To...)
		}},
	}
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}}
	reference := backendReference("echo", ptr.To(int32(7)))
	reference.Namespace = ptr.To(gatewayv1.Namespace("backend"))
	rule := gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{reference}}
	service := backendService("echo")
	service.Namespace = "backend"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := grant.DeepCopy()
			if tc.mutate != nil {
				tc.mutate(candidate)
			}
			input := Input{Services: []corev1.Service{service}, ReferenceGrants: []gatewayv1.ReferenceGrant{*candidate}}
			group := ResolveBackends(route, rule, input)[0]
			if tc.allow {
				if group.State != BackendResolved || group.Authorization != BackendAuthorizationAllowed {
					t.Fatalf("allowed backend = %#v", group)
				}
				return
			}
			if group.Authorization != BackendAuthorizationDenied || group.Diagnostic.Reason != string(gatewayv1.RouteReasonRefNotPermitted) {
				t.Fatalf("denied backend = %#v", group)
			}
			if group.ServiceUID != "" || group.Port != nil || len(group.Endpoints) != 0 {
				t.Fatal("denied reference exposed target observations")
			}
			// Absence of the unauthorized target must not change the result.
			input.Services = nil
			if missing := ResolveBackends(route, rule, input)[0]; !reflect.DeepEqual(group, missing) {
				t.Fatal("target existence influenced an unauthorized reference")
			}
		})
	}
	t.Run("grant revocation retains failed share", func(t *testing.T) {
		input := Input{Services: []corev1.Service{service}, ReferenceGrants: []gatewayv1.ReferenceGrant{grant}}
		before := ResolveBackends(route, rule, input)[0]
		input.ReferenceGrants = nil
		after := ResolveBackends(route, rule, input)[0]
		if before.State != BackendResolved || after.State != BackendInvalid || after.Weight != before.Weight || after.Authorization != BackendAuthorizationDenied {
			t.Fatalf("grant revocation = %#v -> %#v", before, after)
		}
	})
	t.Run("grants do not combine source and target halves", func(t *testing.T) {
		wrongFrom, wrongTo := grant.DeepCopy(), grant.DeepCopy()
		wrongFrom.Spec.From[0].Namespace = "other"
		wrongTo.Spec.To[0].Kind = "Secret"
		group := ResolveBackends(route, rule, Input{ReferenceGrants: []gatewayv1.ReferenceGrant{*wrongFrom, *wrongTo}})[0]
		if group.Authorization != BackendAuthorizationDenied {
			t.Fatalf("unrelated grants authorized target: %#v", group)
		}
	})
}

func TestBackendPortAndReferenceFailures(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*gatewayv1.HTTPBackendRef, *corev1.Service)
		reason gatewayv1.RouteConditionReason
	}{
		{name: "unsupported kind", reason: gatewayv1.RouteReasonInvalidKind, mutate: func(ref *gatewayv1.HTTPBackendRef, _ *corev1.Service) {
			ref.Kind = ptr.To(gatewayv1.Kind("Secret"))
		}},
		{name: "unsupported group", reason: gatewayv1.RouteReasonInvalidKind, mutate: func(ref *gatewayv1.HTTPBackendRef, _ *corev1.Service) {
			ref.Group = ptr.To(gatewayv1.Group("example.test"))
		}},
		{name: "missing port", reason: gatewayv1.RouteReasonBackendNotFound, mutate: func(ref *gatewayv1.HTTPBackendRef, _ *corev1.Service) { ref.Port = nil }},
		{name: "unknown port", reason: gatewayv1.RouteReasonBackendNotFound, mutate: func(ref *gatewayv1.HTTPBackendRef, _ *corev1.Service) { ref.Port = ptr.To(gatewayv1.PortNumber(81)) }},
		{name: "UDP", reason: gatewayv1.RouteReasonUnsupportedProtocol, mutate: func(_ *gatewayv1.HTTPBackendRef, service *corev1.Service) {
			service.Spec.Ports[0].Protocol = corev1.ProtocolUDP
		}},
		{name: "unsupported appProtocol", reason: gatewayv1.RouteReasonUnsupportedProtocol, mutate: func(_ *gatewayv1.HTTPBackendRef, service *corev1.Service) {
			service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
		}},
		{name: "OCCM Service", reason: gatewayv1.RouteReasonUnsupportedValue, mutate: func(_ *gatewayv1.HTTPBackendRef, service *corev1.Service) {
			service.Spec.Type = corev1.ServiceTypeLoadBalancer
		}},
		{name: "ExternalName", reason: gatewayv1.RouteReasonUnsupportedValue, mutate: func(_ *gatewayv1.HTTPBackendRef, service *corev1.Service) {
			service.Spec.Type = corev1.ServiceTypeExternalName
		}},
	}
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, service := backendReference("echo", nil), backendService("echo")
			tc.mutate(&ref, &service)
			group := ResolveBackends(route, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{ref}}, Input{Services: []corev1.Service{service}})[0]
			if group.State != BackendInvalid || group.Diagnostic == nil || group.Diagnostic.Reason != string(tc.reason) || group.Weight != 1 {
				t.Fatalf("invalid backend = %#v", group)
			}
		})
	}
	t.Run("TCP selected independently of Service port order", func(t *testing.T) {
		service := backendService("echo")
		udp := service.Spec.Ports[0]
		udp.Name, udp.Protocol = "udp", corev1.ProtocolUDP
		service.Spec.Ports = append([]corev1.ServicePort{udp}, service.Spec.Ports...)
		group := ResolveBackends(route, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backendReference("echo", nil)}}, Input{Services: []corev1.Service{service}})[0]
		if group.State != BackendResolved || group.Port.Name != "http" || group.Port.Protocol != corev1.ProtocolTCP {
			t.Fatalf("TCP backend = %#v", group)
		}
	})
}

func TestUDPBackendRequiresExplicitUDPServiceAndEndpointSlicePorts(t *testing.T) {
	service := backendService("dns")
	number := ptr.To(gatewayv1.PortNumber(80))
	if _, diagnostic := ResolveTransportServicePort(&service, number, corev1.ProtocolUDP); diagnostic == nil || diagnostic.Reason != string(gatewayv1.RouteReasonUnsupportedProtocol) {
		t.Fatalf("an omitted Service protocol admitted UDP: %v", diagnostic)
	}
	service.Spec.Ports = append(service.Spec.Ports, corev1.ServicePort{Name: "dns-udp", Port: 80, Protocol: corev1.ProtocolUDP})
	port, diagnostic := ResolveTransportServicePort(&service, number, corev1.ProtocolUDP)
	if diagnostic != nil || port.Name != "dns-udp" {
		t.Fatalf("UDP port = %v, diagnostic = %v", port, diagnostic)
	}
	tcpPort, diagnostic := ResolveTCPServicePort(&service, number)
	if diagnostic != nil || tcpPort.Name != "http" {
		t.Fatalf("TCP port = %v, diagnostic = %v", tcpPort, diagnostic)
	}

	udp := backendSlice("dns", "192.0.2.10")
	udp.Ports[0].Name, udp.Ports[0].Protocol = ptr.To(port.Name), ptr.To(corev1.ProtocolUDP)
	defaultTCP := *udp.DeepCopy()
	defaultTCP.Ports[0].Protocol = nil
	defaultTCP.Endpoints[0].Addresses = []string{"192.0.2.11"}
	wrongName := *udp.DeepCopy()
	wrongName.Ports[0].Name = ptr.To("dns-tcp")
	wrongName.Endpoints[0].Addresses = []string{"192.0.2.12"}
	endpoints := ResolveServiceEndpoints(&service, port, []discoveryv1.EndpointSlice{udp, defaultTCP, wrongName})
	if len(endpoints) != 1 || endpoints[0].Address != "192.0.2.10" || endpoints[0].Port != 8080 {
		t.Fatalf("UDP endpoints = %v", endpoints)
	}
}

func TestBackendEndpointPortAndFamilyResolution(t *testing.T) {
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}}
	rule := gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backendReference("echo", nil)}}
	service := backendService("echo")
	service.Spec.ClusterIP = corev1.ClusterIPNone
	service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}
	first, second := backendSlice("echo", "192.0.2.10"), backendSlice("echo", "192.0.2.11")
	second.Ports[0].Port = ptr.To(int32(8081))
	ipv6 := backendSlice("echo", "2001:db8::1")
	ipv6.AddressType = discoveryv1.AddressTypeIPv6
	wrongName, wrongProtocol, unresolved, unrelated, wrongNamespace, fqdn :=
		*first.DeepCopy(), *first.DeepCopy(), *first.DeepCopy(), *first.DeepCopy(), *first.DeepCopy(), *first.DeepCopy()
	wrongName.Ports[0].Name = ptr.To("container-http")
	wrongProtocol.Ports[0].Protocol = ptr.To(corev1.ProtocolUDP)
	unresolved.Ports[0].Port = nil
	unrelated.Labels[discoveryv1.LabelServiceName] = "other"
	wrongNamespace.Namespace = "other"
	fqdn.AddressType, fqdn.Endpoints[0].Addresses = discoveryv1.AddressTypeFQDN, []string{"echo.example.test"}
	input := Input{Services: []corev1.Service{service}, EndpointSlices: []discoveryv1.EndpointSlice{
		wrongName, wrongProtocol, unresolved, unrelated, wrongNamespace, fqdn, second, ipv6, first,
	}}
	group := ResolveBackends(route, rule, input)[0]
	want := []Endpoint{
		{Address: "192.0.2.10", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: true},
		{Address: "192.0.2.11", Port: 8081, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: true},
		{Address: "2001:db8::1", Port: 8080, AddressType: discoveryv1.AddressTypeIPv6, Ready: true, Serving: true},
	}
	if group.State != BackendResolved || !reflect.DeepEqual(group.Endpoints, want) {
		t.Fatalf("resolved endpoints = %#v, want %#v", group, want)
	}
	// An EndpointSlice does not need a Service owner reference. The Service's
	// declared families still bound its address families.
	input.Services[0].Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	if actual := ResolveBackends(route, rule, input)[0].Endpoints; !reflect.DeepEqual(actual, want[:2]) {
		t.Fatalf("IPv4 endpoints = %#v, want %#v", actual, want[:2])
	}
}

func TestBackendEndpointObservationsRetainConditions(t *testing.T) {
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app"}}
	rule := gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backendReference("echo", nil)}}
	first := backendSlice("echo", "192.0.2.10", "192.0.2.11", "192.0.2.12")
	first.Endpoints[1].Conditions = discoveryv1.EndpointConditions{Ready: ptr.To(false), Serving: ptr.To(true), Terminating: ptr.To(true)}
	first.Endpoints[2].Conditions = discoveryv1.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(false)}
	duplicate, conflicting := *first.DeepCopy(), *first.DeepCopy()
	conflicting.Endpoints = conflicting.Endpoints[:1]
	conflicting.Endpoints[0].Conditions.Ready = ptr.To(false)
	input := Input{Services: []corev1.Service{backendService("echo")}, EndpointSlices: []discoveryv1.EndpointSlice{first, duplicate, conflicting}}
	group := ResolveBackends(route, rule, input)[0]
	want := []Endpoint{
		{Address: "192.0.2.10", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: false, Serving: true},
		{Address: "192.0.2.10", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: true},
		{Address: "192.0.2.11", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: false, Serving: true, Terminating: true},
		{Address: "192.0.2.12", Port: 8080, AddressType: discoveryv1.AddressTypeIPv4, Ready: true, Serving: false},
	}
	if group.State != BackendResolved || !reflect.DeepEqual(group.Endpoints, want) {
		t.Fatalf("observations = %#v, want %#v", group.Endpoints, want)
	}
	slices.Reverse(input.EndpointSlices)
	for index := range input.EndpointSlices {
		slices.Reverse(input.EndpointSlices[index].Endpoints)
	}
	if actual := ResolveBackends(route, rule, input)[0]; !reflect.DeepEqual(actual, group) {
		t.Fatalf("input order changed observations: %#v", actual)
	}
}

func TestRecreatedServiceIgnoresEndpointSlicesOwnedByPreviousService(t *testing.T) {
	service := backendService("echo")
	service.UID = "current"
	currentOwner := metav1.NewControllerRef(&service, corev1.SchemeGroupVersion.WithKind("Service"))
	staleOwner, foreignOwner, nonServiceOwner, nonController := *currentOwner, *currentOwner, *currentOwner, *currentOwner
	staleOwner.UID = "previous"
	foreignOwner.Name = "other"
	nonServiceOwner.APIVersion = "example.com/v1"
	nonController.UID, nonController.Controller = "unrelated", ptr.To(false)

	current := backendSlice("echo", "192.0.2.10")
	current.OwnerReferences = []metav1.OwnerReference{nonController, *currentOwner}
	manual := backendSlice("echo", "192.0.2.11")
	managed := backendSlice("echo", "192.0.2.12")
	managed.OwnerReferences = []metav1.OwnerReference{nonServiceOwner}
	stale := backendSlice("echo", "192.0.2.13")
	stale.OwnerReferences = []metav1.OwnerReference{staleOwner}
	foreign := backendSlice("echo", "192.0.2.14")
	foreign.OwnerReferences = []metav1.OwnerReference{foreignOwner}

	endpointSlices := []discoveryv1.EndpointSlice{current, manual, managed, stale, foreign}
	endpoints := ResolveServiceEndpoints(&service, &service.Spec.Ports[0], endpointSlices)
	if want := []string{"192.0.2.10", "192.0.2.11", "192.0.2.12"}; !slices.EqualFunc(endpoints, want, func(endpoint Endpoint, address string) bool {
		return endpoint.Address == address
	}) {
		t.Fatalf("endpoints from recreated Service = %v, want addresses %v", endpoints, want)
	}
	service.UID = ""
	if endpoints = ResolveServiceEndpoints(&service, &service.Spec.Ports[0], endpointSlices); len(endpoints) != len(endpointSlices) {
		t.Fatalf("endpoints without a Service UID = %v, want all %d slices", endpoints, len(endpointSlices))
	}
}

func TestBackendModelDoesNotAliasInputs(t *testing.T) {
	reference := backendReference("echo", ptr.To(int32(5)))
	reference.Group = ptr.To(gatewayv1.Group(""))
	reference.Filters = []gatewayv1.HTTPRouteFilter{{
		Type:                  gatewayv1.HTTPRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{Remove: []string{"x-private"}},
	}}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app"},
		Spec:       gatewayv1.HTTPRouteSpec{Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{reference}}}},
	}
	service := backendService("echo")
	service.Spec.Ports[0].AppProtocol = ptr.To("http")
	input := Input{Services: []corev1.Service{service}, EndpointSlices: []discoveryv1.EndpointSlice{backendSlice("echo", "192.0.2.10")}}
	policies := []backendtls.PolicyResult{{Validation: &backendtls.Validation{
		Hostname: "echo.example", CA: backendtls.CA{PEM: []byte("public test CA")},
	}}}
	key := ServicePortKey{Service: types.NamespacedName{Namespace: service.Namespace, Name: service.Name}, Port: 80}
	input.BackendTLSPolicies = map[ServicePortKey]backendtls.Selection{key: {Policies: policies, Winner: &policies[0]}}
	routeBefore := route.DeepCopy()
	inputBefore, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	group := ResolveBackends(route, route.Spec.Rules[0], input)[0]
	group.BackendTLS.Winner.Validation.CA.PEM[0] = 'x'
	group.BackendTLS.Policies[0].Validation.Hostname = "changed.example"
	if string(policies[0].Validation.CA.PEM) != "public test CA" || policies[0].Validation.Hostname != "echo.example" {
		t.Fatal("compiled backend aliases the input policy or CA")
	}
	*group.Reference.Weight = 90
	*group.Reference.Port = 90
	*group.Reference.Group = "other.test"
	group.Reference.Filters[0].RequestHeaderModifier.Remove[0] = "changed"
	*group.Port.AppProtocol = "changed"
	group.Port.Name = "changed"
	group.Endpoints[0].Address = "192.0.2.20"
	inputAfter, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(route, routeBefore) || string(inputAfter) != string(inputBefore) {
		t.Fatal("resolved model aliases or mutates its Kubernetes inputs")
	}
}

func backendReference(name string, weight *int32) gatewayv1.HTTPBackendRef {
	return gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{
		BackendObjectReference: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(name), Port: ptr.To(gatewayv1.PortNumber(80))},
		Weight:                 weight,
	}}
}

func backendService(name string) corev1.Service {
	return corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID("service-" + name)},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("container-http")}},
		},
	}
}

func backendSlice(name string, addresses ...string) discoveryv1.EndpointSlice {
	endpointSlice := discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "app", Name: name, Labels: map[string]string{discoveryv1.LabelServiceName: name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(8080))}},
	}
	for _, address := range addresses {
		endpointSlice.Endpoints = append(endpointSlice.Endpoints, discoveryv1.Endpoint{Addresses: []string{address}})
	}
	return endpointSlice
}

func TestTCPBackendReferenceRequiresTCPRouteGrant(t *testing.T) {
	target := types.NamespacedName{Namespace: "data", Name: "db"}
	grant := gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: target.Namespace},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: "tenant"}},
			To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(target.Name))}},
		},
	}
	if BackendReferenceAllowedFor("TCPRoute", "tenant", target, []gatewayv1.ReferenceGrant{grant}) {
		t.Fatal("HTTPRoute grant authorized a TCPRoute")
	}
	grant.Spec.From[0].Kind = "TCPRoute"
	if !BackendReferenceAllowedFor("TCPRoute", "tenant", target, []gatewayv1.ReferenceGrant{grant}) || BackendReferenceAllowed("tenant", target, []gatewayv1.ReferenceGrant{grant}) {
		t.Fatal("grant did not apply exclusively to its source kind")
	}
}
