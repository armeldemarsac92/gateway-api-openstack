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
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestNodePortRouteUsesLogicalMatchAndSharedEndpointObservation(t *testing.T) {
	label := strings.Repeat("a", 60)
	longSuffix := strings.Join([]string{label, label, label, strings.Repeat("b", 39)}, ".")
	for _, test := range []struct {
		name     string
		listener gatewayv1.Hostname
		routes   []gatewayv1.Hostname
		want     string
	}{
		{"nested wildcards", "*.svc.example.test", []gatewayv1.Hostname{"*.example.test"}, "*.svc.example.test"},
		{"listener-only hostname", "api.example.test", nil, "api.example.test"},
		{"long wildcard narrowed to an exact hostname", gatewayv1.Hostname("*." + longSuffix), []gatewayv1.Hostname{gatewayv1.Hostname("x." + longSuffix)}, "x." + longSuffix},
		{"exclude disjoint declaration", "*.example.test", []gatewayv1.Hostname{"api.example.test", "other.test"}, "api.example.test"},
		{"deduplicate collapsed intersections", "api.example.test", []gatewayv1.Hostname{"*.example.test", "api.example.test", "api.example.test"}, "api.example.test"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			_, gateway := providerRouteParentObjects(cfg)
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
			route.UID = "route-uid"
			gateway.Spec.Listeners[0].Hostname = ptr.To(test.listener)
			gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Type: ptr.To(gatewayv1.IPAddressType)}}
			route.Spec.Hostnames = test.routes
			route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{
				Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/api/"),
			}}}
			service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
			endpoints.Endpoints[0].Addresses = []string{"198.51.100.20"}
			reader := &fanoutCountingClient{Client: indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(service, endpoints, node).Build()}
			r := &Reconciler{Config: cfg}
			desired, err := r.buildRouteModelWithReader(t.Context(), reader, route, gateway)
			if err != nil {
				t.Fatal(err)
			}
			if desired.Hostname != test.want || desired.PathType != cloud.PathMatchPrefix || desired.PathValue != "/api" {
				t.Fatalf("cloud match did not use normalized logical model: %#v", desired)
			}
			want := []cloud.Member{{Address: "192.0.2.10", Port: 30080}}
			if !slices.Equal(desired.Members, want) {
				t.Fatalf("members = %v, want existing NodePort transport %v", desired.Members, want)
			}
			if reader.gets != 1 || reader.lists != 2 {
				t.Fatalf("reads = %d gets, %d lists, want one Service, EndpointSlice list, and Node list", reader.gets, reader.lists)
			}
		})
	}
}

func TestLogicalModelCannotEnableUnmappedNodePortBehavior(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*gatewayv1.HTTPRoute)
	}{
		{name: "multiple backends", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs = append(route.Spec.Rules[0].BackendRefs, route.Spec.Rules[0].BackendRefs[0])
		}},
		{name: "unresolved filter", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].Filters = []gatewayv1.HTTPRouteFilter{{Type: gatewayv1.HTTPRouteFilterExtensionRef}}
		}},
		{name: "missing backend port", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].BackendRefs[0].Port = nil
		}},
		{name: "header predicate", change: func(route *gatewayv1.HTTPRoute) {
			route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "x-version", Value: "v2"}}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, gateway := providerRouteParentObjects(testConfig())
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
			route.UID = "route-uid"
			service, endpoints, _ := validNodePortBackend(gateway.Namespace, "backend")
			test.change(route)
			logical, err := model.Compile(model.Input{
				Gateway: gateway, Routes: []gatewayv1.HTTPRoute{*route},
				Services: []corev1.Service{*service}, EndpointSlices: []discoveryv1.EndpointSlice{*endpoints},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = supportedRouteModel(logical, []*gatewayv1.Listener{&gateway.Spec.Listeners[0]}, false, false, false, false, false)
			var rejected *routeBuildError
			if !errors.As(err, &rejected) || rejected.kind != routeErrorUnsupported {
				t.Fatalf("unmapped logical model escaped NodePort lowering: %v", err)
			}
		})
	}
}

func TestExactHTTPHeadersLowerDistinctAlternativesAtTheirRank(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPHeaderExact = true
	_, gateway := providerRouteParentObjects(cfg)
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
	route.UID = "route-uid"
	route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{
		{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "X-Version", Value: "One, Two"}}},
		{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "X-Version", Value: "Three"}, {Name: "x-version", Type: ptr.To(gatewayv1.HeaderMatchRegularExpression), Value: "ignored"}, {Name: "color", Value: "orange"}}},
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/explicit")}},
	}
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(service, endpoints, node).Build()
	r := &Reconciler{Config: cfg}
	desired, err := r.buildRouteModelWithReader(t.Context(), reader, route, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.HTTPBackends) != 1 || len(desired.HTTPBackends[0].Matches) != 3 {
		t.Fatalf("want three independent policies on the same backend, got %#v", desired.HTTPBackends)
	}
	matches := desired.HTTPBackends[0].Matches
	if matches[0].PathType != cloud.PathMatchExact || matches[0].Precedence != 0 ||
		matches[1].Precedence != 1 || !slices.Equal(matches[1].Headers, []cloud.HTTPHeaderMatch{{Name: "color", Value: "orange"}, {Name: "x-version", Value: "Three"}}) ||
		matches[2].Precedence != 2 || !slices.Equal(matches[2].Headers, []cloud.HTTPHeaderMatch{{Name: "x-version", Value: "One, Two"}}) {
		t.Fatalf("incorrect effective predicates or precedence: %#v", matches)
	}
}

func TestExactHTTPHeadersRejectUnsupportedPredicates(t *testing.T) {
	for _, test := range []struct {
		name  string
		optIn bool
		match gatewayv1.HTTPRouteMatch
		want  string
	}{
		{name: "disabled", match: gatewayv1.HTTPRouteMatch{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "version", Value: "one"}}}, want: "--octavia-http-header-exact"},
		{name: "regex", optIn: true, match: gatewayv1.HTTPRouteMatch{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "version", Type: ptr.To(gatewayv1.HeaderMatchRegularExpression), Value: "one"}}}, want: "RegularExpression"},
		{name: "method", optIn: true, match: gatewayv1.HTTPRouteMatch{Method: ptr.To(gatewayv1.HTTPMethodGet)}, want: "method"},
		{name: "query", optIn: true, match: gatewayv1.HTTPRouteMatch{QueryParams: []gatewayv1.HTTPQueryParamMatch{{Name: "version", Value: "one"}}}, want: "query parameter"},
		{name: "name beyond HAProxy limit", optIn: true, match: gatewayv1.HTTPRouteMatch{Headers: []gatewayv1.HTTPHeaderMatch{{Name: gatewayv1.HTTPHeaderName(strings.Repeat("x", 256)), Value: "one"}}}, want: "255 bytes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.HTTPHeaderExact = test.optIn
			_, gateway := providerRouteParentObjects(cfg)
			route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
			route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{test.match}
			r := &Reconciler{Config: cfg}
			_, err := r.validateRouteStructure(route)
			var rejected *routeBuildError
			if !errors.As(err, &rejected) || rejected.kind != routeErrorUnsupported || !strings.Contains(rejected.message, test.want) {
				t.Fatalf("expected UnsupportedValue mentioning %q, got %v", test.want, err)
			}
		})
	}
}

func TestHTTPMethodLoweringRetainsConjunctionsAlternativesAndPrecedence(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPMethodMatch, cfg.HTTPHeaderExact, cfg.FixedHTTPResponses = true, true, true
	_, gateway := providerRouteParentObjects(cfg)
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("api.example.test"))
	route := mapperTestHTTPRoute(gateway.Namespace, "api", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
	route.UID = "route-uid"
	route.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{
		{Headers: []gatewayv1.HTTPHeaderMatch{{Name: "Version", Value: "four"}, {Name: "Color", Value: "blue"}}},
		{Method: ptr.To(gatewayv1.HTTPMethodGet)},
		{Method: ptr.To(gatewayv1.HTTPMethodPost)},
		{Method: ptr.To(gatewayv1.HTTPMethodPost), Headers: []gatewayv1.HTTPHeaderMatch{{Name: "Version", Value: "two"}}},
		{Method: ptr.To(gatewayv1.HTTPMethodPost)}, // duplicate alternative
		{Path: &gatewayv1.HTTPPathMatch{Value: ptr.To("/shop")}},
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/")}},
		{}, // separate unrestricted fallback
	}
	service, endpoints, node := validNodePortBackend(gateway.Namespace, "backend")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(service, endpoints, node).Build()
	desired, err := (&Reconciler{Config: cfg}).buildRouteModelWithReader(t.Context(), reader, route, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.HTTPBackends) != 1 {
		t.Fatalf("got %d backends, want one", len(desired.HTTPBackends))
	}
	type predicate struct {
		pathType     cloud.PathMatchType
		path, method string
		headers      int
	}
	var got []predicate
	for _, match := range desired.HTTPBackends[0].Matches {
		if match.Hostname != "api.example.test" {
			t.Fatalf("missing listener hostname on %#v", match)
		}
		got = append(got, predicate{match.PathType, match.PathValue, match.Method, len(match.Headers)})
	}
	want := []predicate{
		{cloud.PathMatchExact, "/", "", 0},
		{cloud.PathMatchPrefix, "/shop", "", 0},
		{cloud.PathMatchPrefix, "/", "POST", 1},
		{cloud.PathMatchPrefix, "/", "GET", 0},
		{cloud.PathMatchPrefix, "/", "POST", 0},
		{cloud.PathMatchPrefix, "/", "", 2},
		{cloud.PathMatchPrefix, "/", "", 0},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ranked predicates = %v, want %v", got, want)
	}
}

func TestHTTPMethodAndQueryValidationAreIndependent(t *testing.T) {
	for _, test := range []struct {
		method gatewayv1.HTTPMethod
		query  []gatewayv1.HTTPQueryParamMatch
		want   string
	}{
		{method: "get", want: "unknown HTTP method"},
		{method: gatewayv1.HTTPMethodGet, query: []gatewayv1.HTTPQueryParamMatch{{Name: "q", Value: "one"}}, want: "query parameter"},
	} {
		_, _, err := supportedMatch(gatewayv1.HTTPRouteMatch{Method: &test.method, QueryParams: test.query}, false, true)
		var rejected *routeBuildError
		if !errors.As(err, &rejected) || rejected.kind != routeErrorUnsupported || !strings.Contains(rejected.message, test.want) {
			t.Fatalf("match = %#v, expected UnsupportedValue mentioning %q, got %v", test, test.want, err)
		}
	}
}
