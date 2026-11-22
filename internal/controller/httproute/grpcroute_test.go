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
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestGRPCExactHeadersKeepAlternativesAndPrecedenceOnH2CBackends(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.HTTPHeaderExact = true, true, true
	_, gateway := providerRouteParentObjects(cfg)
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "echo", UID: "route-uid"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name)}}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{
		grpcRule("v2", gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{{Name: "version", Value: "two"}}}),
		grpcRule("v1", gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{{Name: "Version", Value: "two"}, {Name: "color", Value: "orange"}}}),
		grpcRule("v1",
			gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{{Name: "color", Value: "blue"}}},
			gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{{Name: "color", Value: "green"}}},
		),
		grpcRule("v2", gatewayv1.GRPCRouteMatch{
			Method:  &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Echo")},
			Headers: []gatewayv1.GRPCHeaderMatch{{Name: "version", Value: "two"}},
		}),
	}
	v1, endpointsV1, node := validNodePortBackend(gateway.Namespace, "v1")
	v2, endpointsV2, _ := validNodePortBackend(gateway.Namespace, "v2")
	v1.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	v2.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(v1, endpointsV1, v2, endpointsV2, node).Build()
	desired, err := (&Reconciler{Config: cfg}).buildRouteModelWithReader(t.Context(), reader, model.ProjectGRPCRoute(route), gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.HTTPBackends) != 2 || !desired.TCPHealthMonitor {
		t.Fatalf("want two gRPC backends with TCP health checks, got %#v", desired)
	}
	want := map[string][]cloud.HTTPPathMatch{
		"v1": {
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 1, Headers: []cloud.HTTPHeaderMatch{{Name: "color", Value: "orange"}, {Name: "version", Value: "two"}}},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 3, Headers: []cloud.HTTPHeaderMatch{{Name: "color", Value: "blue"}}},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 4, Headers: []cloud.HTTPHeaderMatch{{Name: "color", Value: "green"}}},
		},
		"v2": {
			{PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 0, Headers: []cloud.HTTPHeaderMatch{{Name: "version", Value: "two"}}},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 2, Headers: []cloud.HTTPHeaderMatch{{Name: "version", Value: "two"}}},
		},
	}
	for _, backend := range desired.HTTPBackends {
		if !backend.BackendH2C || len(backend.Members) != 1 || !reflect.DeepEqual(backend.Matches, want[backend.Name]) {
			t.Errorf("backend %s = %#v, want h2c, one member and matches %#v", backend.Name, backend, want[backend.Name])
		}
	}
}

func TestGRPCExactHeadersRejectDisabledCapabilityAndUnsupportedMatches(t *testing.T) {
	exact := gatewayv1.GRPCHeaderMatch{Name: "version", Value: "two"}
	for _, test := range []struct {
		name     string
		enrolled bool
		match    gatewayv1.GRPCRouteMatch
		message  string
	}{
		{name: "opt-in disabled", match: gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{exact}}, message: "--octavia-http-header-exact"},
		{name: "regex", enrolled: true, match: gatewayv1.GRPCRouteMatch{Headers: []gatewayv1.GRPCHeaderMatch{{Type: ptr.To(gatewayv1.GRPCHeaderMatchRegularExpression), Name: "version", Value: "t.*"}}}, message: "RegularExpression"},
		{name: "partial method", enrolled: true, match: gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo")}, Headers: []gatewayv1.GRPCHeaderMatch{exact}}, message: "exact service and method"},
		{name: "catch-all", enrolled: true, message: "exact service and method"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.HTTPHeaderExact = true, true, test.enrolled
			route := &gatewayv1.GRPCRoute{}
			route.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule("v1", test.match)}
			_, err := (&Reconciler{Config: cfg}).validateRouteStructure(model.ProjectGRPCRoute(route))
			var rejected *routeBuildError
			if !errors.As(err, &rejected) || rejected.kind != routeErrorUnsupported || !strings.Contains(rejected.message, test.message) {
				t.Fatalf("want UnsupportedValue mentioning %q, got %v", test.message, err)
			}
		})
	}
}

func grpcRule(backend gatewayv1.ObjectName, matches ...gatewayv1.GRPCRouteMatch) gatewayv1.GRPCRouteRule {
	return gatewayv1.GRPCRouteRule{
		Matches: matches,
		BackendRefs: []gatewayv1.GRPCBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: backend, Port: ptr.To(gatewayv1.PortNumber(80)),
		}}}},
	}
}

func TestHTTPAndGRPCConflictOnSecondHTTPHostname(t *testing.T) {
	http := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "a-http"}}
	http.Spec.Hostnames = []gatewayv1.Hostname{"*.first.test", "*.shared.test"}
	grpc := &gatewayv1.HTTPRoute{TypeMeta: metav1.TypeMeta{Kind: "GRPCRoute"}, ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "b-grpc"}}
	grpc.Spec.Hostnames = []gatewayv1.Hostname{"svc.shared.test"}
	listener := &gatewayv1.Listener{Protocol: gatewayv1.HTTPProtocolType, Port: 80}
	candidates := []routeProfileCandidate{{route: http, listeners: []*gatewayv1.Listener{listener}}, {route: grpc, listeners: []*gatewayv1.Listener{listener}}}
	if conflicts := electRouteProfiles(candidates); !conflicts[routeKey(grpc)] || conflicts[routeKey(http)] {
		t.Fatalf("the older HTTPRoute must own every declared hostname: %v", conflicts)
	}
	grpc.Spec.Hostnames = []gatewayv1.Hostname{"shared.test"}
	if conflicts := electRouteProfiles(candidates); len(conflicts) != 0 {
		t.Fatalf("the wildcard cannot reserve the suffix apex: %v", conflicts)
	}
}

func TestSharedPlainHTTPModelLowersGRPCAuthorityOnBothWildcards(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses = true, true
	_, gateway := providerRouteParentObjects(cfg)
	port := gateway.Spec.Listeners[0].Port
	gateway.Spec.Listeners = []gatewayv1.Listener{
		{Name: "exact", Port: port, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("foo.bar.com"))},
		{Name: "wildcard-bar", Port: port, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("*.bar.com"))},
		{Name: "wildcard-foo", Port: port, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("*.foo.com"))},
	}
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "echo", UID: "route-uid"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name), Port: &port}}
	route.Spec.Hostnames = []gatewayv1.Hostname{"*.bar.com", "*.foo.com"}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule("v1", gatewayv1.GRPCRouteMatch{
		Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To("Echo")},
	})}
	backend, endpoints, node := validNodePortBackend(gateway.Namespace, "v1")
	backend.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(backend, endpoints, node).Build()

	desired, err := (&Reconciler{Config: cfg}).buildRouteModelWithReader(t.Context(), reader, model.ProjectGRPCRoute(route), gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.HTTPBackends) != 1 || !desired.TCPHealthMonitor || !desired.HTTPBackends[0].BackendH2C {
		t.Fatalf("want a single h2c backend with TCP health checks, got %#v", desired)
	}
	want := []cloud.HTTPPathMatch{
		{HTTPListenerHostname: "foo.bar.com", Hostname: "foo.bar.com", PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 0},
		{HTTPListenerHostname: "*.bar.com", Hostname: "*.bar.com", PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 1},
		{HTTPListenerHostname: "*.foo.com", Hostname: "*.foo.com", PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 2},
	}
	if got := desired.HTTPBackends[0].Matches; !reflect.DeepEqual(got, want) {
		t.Fatalf("gRPC authority and section constraints = %#v, want %#v", got, want)
	}

	exact := &gateway.Spec.Listeners[0]
	wildcard := &gateway.Spec.Listeners[1]
	http := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "http-on-wildcard"}}
	candidates := []routeProfileCandidate{
		{route: http, listeners: []*gatewayv1.Listener{wildcard}},
		{route: model.ProjectGRPCRoute(route), listeners: []*gatewayv1.Listener{exact}},
	}
	if conflicts := electRouteProfiles(candidates); len(conflicts) != 0 {
		t.Fatalf("different listener sections must not compete across HTTP and gRPC: %v", conflicts)
	}
}

func TestGatewayGraphPreservesBoundGRPCRouteWithSameNameAsHTTPRoute(t *testing.T) {
	cfg := testConfig()
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "edge", UID: "gateway"}}
	http := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "echo", UID: "http"}}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "echo", UID: "grpc"}}
	controller.ApplyRouteBinding(cfg, http, controller.RouteIdentity(cfg, gateway, http))
	controller.ApplyRouteBinding(cfg, grpc, controller.RouteIdentity(cfg, gateway, grpc))
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(http, grpc).Build()
	r := &Reconciler{Config: cfg, APIReader: kube}
	request := graph.Request{Action: graph.DeleteGateway, Identity: controller.GatewayIdentity(cfg, gateway)}
	input, err := r.ObserveRouteGraph(t.Context(), request, gateway)
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{"http", "grpc"} {
		found := false
		for _, node := range input.Nodes {
			found = found || node.Disposition == cloud.GraphPreserve && node.Spec.Identity.RouteUID == uid
		}
		if !found {
			t.Errorf("Gateway deletion inventory lacks the bound %s route: %#v", uid, input.Nodes)
		}
	}
}
