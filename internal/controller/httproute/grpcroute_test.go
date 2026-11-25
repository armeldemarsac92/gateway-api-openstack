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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestGRPCRouteIgnoresListenerSetAlongsideDirectParent(t *testing.T) {
	f, _, grpc := newRouteElectionFixture(t)
	f.service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	if err := f.client.Update(t.Context(), f.service); err != nil {
		t.Fatal(err)
	}
	direct, delegated := grpc.Spec.ParentRefs[0], f.route.Spec.ParentRefs[1]
	grpc.Spec.ParentRefs = []gatewayv1.ParentReference{direct, delegated}
	grpc.Spec.Hostnames = f.route.Spec.Hostnames
	route := model.ProjectGRPCRoute(grpc)
	if err := f.reconciler.applyRouteBinding(route, f.gateway); err != nil {
		t.Fatal(err)
	}
	grpc.Annotations, grpc.Finalizers = route.Annotations, route.Finalizers
	f.create(t, grpc)
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider).forRouteKind(model.GRPCRouteKind)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(grpc)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Get(t.Context(), request.NamespacedName, grpc); err != nil {
		t.Fatal(err)
	}
	route = model.ProjectGRPCRoute(grpc)
	assertElectionParentAccepted(t, route, direct, metav1.ConditionTrue)
	if len(grpc.Status.Parents) != 1 ||
		findRouteParentCondition(route, r.Config.ControllerName, delegated, string(gatewayv1.RouteConditionAccepted)) != nil {
		t.Fatalf("expected status only for the direct parent, got %v", grpc.Status.Parents)
	}
	if len(provider.routeSpecs) != 2 || !provider.routeSpecs[0].GRPC && !provider.routeSpecs[1].GRPC {
		t.Fatalf("expected both eligible route fragments, including the GRPCRoute, got %v", provider.routeSpecs)
	}
}

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
	if len(desired.HTTPBackends) != 2 || !desired.GRPC {
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

func TestGRPCRequestHeaderModifierFollowsItsMethod(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses = true, true
	_, gateway := providerRouteParentObjects(cfg)
	match := func(method string) gatewayv1.GRPCRouteMatch {
		return gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To(method)}}
	}
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "echo", UID: "route-uid"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name)}}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{grpcRule("v1", match("Marked")), grpcRule("v1", match("Plain"))}
	route.Spec.Rules[0].Filters = []gatewayv1.GRPCRouteFilter{{
		Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
			Set:    []gatewayv1.HTTPHeader{{Name: "X-Mode", Value: "blue"}},
			Add:    []gatewayv1.HTTPHeader{{Name: "X-Trace", Value: "one"}},
			Remove: []string{"X-Legacy"},
		},
	}}
	// The same method and backend select only the first rule's operations.
	shadow := route.Spec.Rules[0].DeepCopy()
	shadow.Filters[0].RequestHeaderModifier.Set[0].Value = "shadow"
	route.Spec.Rules = append(route.Spec.Rules, *shadow)
	projected := model.ProjectGRPCRoute(route)
	if _, err := (&Reconciler{Config: cfg}).validateRouteStructure(projected); err == nil {
		t.Fatal("gRPC request header operations must require operator enrollment")
	}
	cfg.RequestHeaderModifier = true
	svc, endpoints, node := validNodePortBackend(gateway.Namespace, "v1")
	svc.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(svc, endpoints, node).Build()
	desired, err := (&Reconciler{Config: cfg}).buildRouteModelWithReader(t.Context(), reader, projected, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.HTTPBackends) != 1 || !desired.HTTPBackends[0].BackendH2C {
		t.Fatalf("want one h2c backend, got %#v", desired.HTTPBackends)
	}
	got := make(map[string][]cloud.HTTPHeaderOperation)
	for _, match := range desired.HTTPBackends[0].Matches {
		got[match.PathValue] = match.RequestHeaderOperations
	}
	want := map[string][]cloud.HTTPHeaderOperation{
		"/example.Echo/Marked": {
			{Operation: cloud.HTTPHeaderSet, Name: "x-mode", Value: "blue"},
			{Operation: cloud.HTTPHeaderAdd, Name: "x-trace", Value: "one"},
			{Operation: cloud.HTTPHeaderRemove, Name: "x-legacy"},
		},
		"/example.Echo/Plain": nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request headers by method = %#v, want %#v", got, want)
	}

	response := &gatewayv1.HTTPHeaderFilter{
		Set:    []gatewayv1.HTTPHeader{{Name: "X-Reply", Value: "blue"}},
		Add:    []gatewayv1.HTTPHeader{{Name: "Trace-Bin", Value: "YQ==, Yg"}},
		Remove: []string{"X-Private"},
	}
	route.Spec.Rules[0].Filters = append(route.Spec.Rules[0].Filters, gatewayv1.GRPCRouteFilter{
		Type: gatewayv1.GRPCRouteFilterResponseHeaderModifier, ResponseHeaderModifier: response,
	})
	r := &Reconciler{Config: cfg}
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil {
		t.Fatal("response header operations require their own enrollment")
	}
	r.Config.ResponseHeaderModifier = true
	desired, err = r.buildRouteModelWithReader(t.Context(), reader, model.ProjectGRPCRoute(route), gateway)
	if err != nil {
		t.Fatal(err)
	}
	wantReply := []cloud.HTTPHeaderOperation{
		{Operation: cloud.HTTPHeaderSet, Name: "x-reply", Value: "blue"},
		{Operation: cloud.HTTPHeaderAdd, Name: "trace-bin", Value: "YQ==, Yg"},
		{Operation: cloud.HTTPHeaderRemove, Name: "x-private"},
	}
	if len(desired.HTTPBackends) != 1 || len(desired.HTTPBackends[0].Matches) != 2 {
		t.Fatalf("want both methods on their shared backend, got %#v", desired.HTTPBackends)
	}
	for _, match := range desired.HTTPBackends[0].Matches {
		var reply []cloud.HTTPHeaderOperation
		if match.PathValue == "/example.Echo/Marked" {
			reply = wantReply
		}
		if !reflect.DeepEqual(match.ResponseHeaderOperations, reply) || !reflect.DeepEqual(match.RequestHeaderOperations, want[match.PathValue]) {
			t.Fatalf("incorrect request or response metadata for %s: %#v", match.PathValue, match)
		}
	}
	for _, invalid := range []gatewayv1.HTTPHeader{
		{Name: "GRPC-Status", Value: "0"}, {Name: "Content-Type", Value: "text/plain"},
		{Name: "bad+name", Value: "v"}, {Name: "trace-bin", Value: "%%%"},
		{Name: "trace-bin", Value: "Yh=="}, {Name: "trace-bin", Value: "Yh"},
		{Name: "reply", Value: "café"},
	} {
		response.Set = []gatewayv1.HTTPHeader{invalid}
		response.Add = nil
		_, err := r.validateRouteStructure(model.ProjectGRPCRoute(route))
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(string(invalid.Name))) {
			t.Errorf("want a diagnostic for %q, got %v", invalid.Name, err)
		}
	}
	response.Set = []gatewayv1.HTTPHeader{
		{Name: "replace-bin", Value: strings.Repeat("YQ==, ", 22) + "YQ=="},
		{Name: "x-reply", Value: "blue"},
	}
	response.Add = []gatewayv1.HTTPHeader{{Name: "trace-bin", Value: strings.Repeat("Yg, ", 23) + "Yg"}}
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err != nil {
		t.Fatalf("48 output fields plus REMOVE should be accepted: %v", err)
	}
	response.Add[0].Value += ", Yg"
	_, err = r.validateRouteStructure(model.ProjectGRPCRoute(route))
	if unsupported, ok := err.(*routeBuildError); !ok || unsupported.kind != routeErrorUnsupported || !strings.Contains(err.Error(), "48 fields") {
		t.Fatalf("49 output fields should be rejected as unsupported: %v", err)
	}
}

func TestGRPCRouteWeightedH2CBackends(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets = true, true, true
	_, gateway := providerRouteParentObjects(cfg)
	v1, ep1, node := validNodePortBackend(gateway.Namespace, "v1")
	v2, ep2, _ := validNodePortBackend(gateway.Namespace, "v2")
	v3, ep3, _ := validNodePortBackend(gateway.Namespace, "v3")
	for _, service := range []*corev1.Service{v1, v2, v3} {
		service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	}
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(v1, ep1, v2, ep2, v3, ep3, node).Build()
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "weighted", UID: "route-uid"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name)}}
	rule := grpcRule("v1")
	rule.BackendRefs = append(rule.BackendRefs, grpcRule("v2").BackendRefs[0], grpcRule("v3").BackendRefs[0])
	for index, weight := range []int32{70, 30, 0} {
		rule.BackendRefs[index].Weight = ptr.To(weight)
	}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{rule}
	projected := model.ProjectGRPCRoute(route)
	r := &Reconciler{Config: cfg}
	desired, err := r.buildRouteModelWithReader(t.Context(), reader, projected, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !desired.GRPC || len(desired.HTTPBackends) != 3 || len(desired.WeightedHTTPRules) != 1 {
		t.Fatalf("expected three backend pools and a weighted gRPC rule, got %#v", desired)
	}
	for index, backend := range desired.HTTPBackends {
		if backend.Name != string(rule.BackendRefs[index].Name) || !backend.BackendH2C {
			t.Fatalf("unexpected gRPC pool: %#v", backend)
		}
	}
	wantTargets := []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 70}, {BackendIndex: ptr.To(1), Weight: 30}, {BackendIndex: ptr.To(2), Weight: 0}}
	if !reflect.DeepEqual(desired.WeightedHTTPRules[0].Targets, wantTargets) {
		t.Fatalf("weighted gRPC targets = %#v", desired.WeightedHTTPRules[0].Targets)
	}

	denied := projected.DeepCopy()
	denied.Spec.Rules[0].BackendRefs[1].Namespace = ptr.To(gatewayv1.Namespace("restricted"))
	desired, err = r.buildRouteModelWithReader(t.Context(), reader, denied, gateway)
	if err != nil {
		t.Fatal(err)
	}
	wantTargets = []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 70}, {Weight: 30}, {BackendIndex: ptr.To(1), Weight: 0}}
	if len(desired.HTTPBackends) != 2 || len(desired.WeightedHTTPRules) != 1 ||
		!reflect.DeepEqual(desired.WeightedHTTPRules[0].Targets, wantTargets) {
		t.Fatalf("unauthorized gRPC target did not retain its failure share: %#v", desired)
	}

	r.Config.WeightedHTTPTargets = false
	if _, err := r.buildRouteModelWithReader(t.Context(), reader, projected, gateway); err == nil || !strings.Contains(err.Error(), "--octavia-weighted-http-targets") {
		t.Fatalf("multi-backend GRPCRoute requires the weighted provider opt-in: %v", err)
	}
}

func TestGRPCRouteBackendRequestHeaderModifierStaysOnSelectedTarget(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets = true, true, true
	cfg.RequestHeaderModifier, cfg.BackendRequestFilters = true, true
	_, gateway := providerRouteParentObjects(cfg)
	v1, ep1, node := validNodePortBackend(gateway.Namespace, "v1")
	v2, ep2, _ := validNodePortBackend("services", "v2")
	for _, service := range []*corev1.Service{v1, v2} {
		service.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	}
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: v2.Namespace, Name: "grant"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: model.GRPCRouteKind, Namespace: gatewayv1.Namespace(gateway.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(v2.Name))}},
		},
	}
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(v1, ep1, v2, ep2, node, grant).Build()
	match := func(method string) gatewayv1.GRPCRouteMatch {
		return gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("example.Echo"), Method: ptr.To(method)}}
	}
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "request-metadata", UID: "route-uid"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name)}}
	rule := grpcRule("v1", match("Filtered"))
	rule.Filters = []gatewayv1.GRPCRouteFilter{{
		Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
			Set: []gatewayv1.HTTPHeader{{Name: "X-Mode", Value: "rule"}, {Name: "Trace-Bin", Value: "YQ=="}},
		},
	}}
	target := grpcRule("v2").BackendRefs[0]
	target.Namespace, target.Weight = ptr.To(gatewayv1.Namespace(v2.Namespace)), ptr.To[int32](25)
	target.Filters = []gatewayv1.GRPCRouteFilter{{
		Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
			Set:    []gatewayv1.HTTPHeader{{Name: "X-Mode", Value: "target"}},
			Add:    []gatewayv1.HTTPHeader{{Name: "Trace-Bin", Value: "Yg, Yw=="}},
			Remove: []string{"X-Legacy"},
		},
	}}
	rule.BackendRefs[0].Weight = ptr.To[int32](75)
	rule.BackendRefs = append(rule.BackendRefs, target)
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{rule, grpcRule("v1", match("Plain"))}
	r := &Reconciler{Config: cfg}
	build := func() *cloud.RouteSpec {
		t.Helper()
		desired, err := r.buildRouteModelWithReader(t.Context(), reader, model.ProjectGRPCRoute(route), gateway)
		if err != nil {
			t.Fatal(err)
		}
		if !desired.GRPC || len(desired.WeightedHTTPRules) != 1 {
			t.Fatalf("expected one weighted gRPC rule: %#v", desired)
		}
		return &desired
	}
	desired := build()
	weighted := desired.WeightedHTTPRules[0]
	wantRule := []cloud.HTTPHeaderOperation{
		{Operation: cloud.HTTPHeaderSet, Name: "x-mode", Value: "rule"},
		{Operation: cloud.HTTPHeaderSet, Name: "trace-bin", Value: "YQ=="},
	}
	wantTarget := []cloud.HTTPHeaderOperation{
		{Operation: cloud.HTTPHeaderSet, Name: "x-mode", Value: "target"},
		{Operation: cloud.HTTPHeaderAdd, Name: "trace-bin", Value: "Yg, Yw=="},
		{Operation: cloud.HTTPHeaderRemove, Name: "x-legacy"},
	}
	if len(weighted.Matches) != 1 || !reflect.DeepEqual(weighted.Matches[0].RequestHeaderOperations, wantRule) ||
		len(weighted.Targets) != 2 || weighted.Targets[0].Weight != 75 || weighted.Targets[1].Weight != 25 ||
		len(weighted.Targets[0].RequestHeaderOperations) != 0 || !reflect.DeepEqual(weighted.Targets[1].RequestHeaderOperations, wantTarget) {
		t.Fatalf("incorrect rule and target scopes: %#v", weighted)
	}
	for i, ref := range rule.BackendRefs {
		index := weighted.Targets[i].BackendIndex
		if index == nil || desired.HTTPBackends[*index].Name != string(ref.Name) || !desired.HTTPBackends[*index].BackendH2C {
			t.Fatalf("target %d lost the selected h2c Service: %#v", i, desired.HTTPBackends)
		}
	}
	plain := desired.HTTPBackends[*weighted.Targets[0].BackendIndex].Matches
	if len(plain) != 1 || plain[0].PathValue != "/example.Echo/Plain" || len(plain[0].RequestHeaderOperations) != 0 {
		t.Fatalf("metadata leaked into the plain method on a shared Service: %#v", plain)
	}

	route.Spec.Rules[0].BackendRefs = []gatewayv1.GRPCBackendRef{target}
	weighted = build().WeightedHTTPRules[0]
	if len(weighted.Targets) != 1 || !reflect.DeepEqual(weighted.Targets[0].RequestHeaderOperations, wantTarget) ||
		len(weighted.Matches) != 1 || !reflect.DeepEqual(weighted.Matches[0].RequestHeaderOperations, wantRule) {
		t.Fatalf("a sole filtered backend should still use its own weighted target: %#v", weighted)
	}
	r.Config.WeightedHTTPTargets = false
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil || !strings.Contains(err.Error(), "--octavia-weighted-http-targets") {
		t.Fatalf("want the weighted target opt-in diagnostic, got %v", err)
	}
	r.Config.WeightedHTTPTargets = true
	r.Config.BackendRequestFilters = false
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil || !strings.Contains(err.Error(), "--octavia-http-backend-request-filters") {
		t.Fatalf("want the backend request filter opt-in diagnostic, got %v", err)
	}
	r.Config.BackendRequestFilters = true

	route.Spec.Rules[0].BackendRefs = rule.BackendRefs
	grant.Spec.From[0].Kind = model.HTTPRouteKind
	if err := reader.Update(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	weighted = build().WeightedHTTPRules[0]
	revoked := weighted.Targets[1]
	if revoked.Weight != 25 || revoked.BackendIndex != nil || len(revoked.RequestHeaderOperations) != 0 || weighted.Targets[0].BackendIndex == nil {
		t.Fatalf("withdrawing the GRPCRoute grant must retain the failure share: %#v", weighted.Targets)
	}
}

func TestGRPCBackendResponseMetadataValidation(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets, cfg.ResponseHeaderModifier = true, true, true, true
	r := &Reconciler{Config: cfg}
	route := &gatewayv1.GRPCRoute{}
	rule := grpcRule("v1")
	filter := &gatewayv1.HTTPHeaderFilter{Add: []gatewayv1.HTTPHeader{{Name: "trace-bin", Value: "YQ==, Yg"}}}
	rule.BackendRefs[0].Filters = []gatewayv1.GRPCRouteFilter{{Type: gatewayv1.GRPCRouteFilterResponseHeaderModifier, ResponseHeaderModifier: filter}}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{rule}
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err != nil {
		t.Fatalf("want permitted repeated binary response metadata on the backend: %v", err)
	}
	filter.Add = nil
	filter.Remove = []string{"grpc-status"}
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil || !strings.Contains(err.Error(), "grpc-status") {
		t.Fatalf("want reserved response metadata rejected, got %v", err)
	}
}

func TestGRPCRequestMetadataValidatesRuleAndBackendScopes(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses, cfg.WeightedHTTPTargets = true, true, true
	cfg.RequestHeaderModifier, cfg.BackendRequestFilters = true, true
	r := &Reconciler{Config: cfg}
	route := &gatewayv1.GRPCRoute{}
	ruleFilter := &gatewayv1.HTTPHeaderFilter{Set: []gatewayv1.HTTPHeader{{Name: "trace-bin", Value: strings.Repeat("YQ==, ", 47) + "YQ=="}}}
	targetFilter := &gatewayv1.HTTPHeaderFilter{Add: []gatewayv1.HTTPHeader{{Name: "trace-bin", Value: strings.Repeat("Yg, ", 47) + "Yg"}}, Remove: []string{"private-bin"}}
	rule := grpcRule("v1")
	rule.Filters = []gatewayv1.GRPCRouteFilter{{Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier, RequestHeaderModifier: ruleFilter}}
	rule.BackendRefs[0].Filters = []gatewayv1.GRPCRouteFilter{{Type: gatewayv1.GRPCRouteFilterRequestHeaderModifier, RequestHeaderModifier: targetFilter}}
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{rule}
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err != nil {
		t.Fatalf("the rule and target each have an independent 48-field budget: %v", err)
	}
	targetFilter.Add[0].Value += ", Yg"
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil || !strings.Contains(err.Error(), "48 fields") {
		t.Fatalf("want the expanded field limit diagnostic, got %v", err)
	}
	targetFilter.Add = nil
	for _, invalid := range []gatewayv1.HTTPHeader{
		{Name: "GRPC-Timeout", Value: "1S"}, {Name: "Content-Type", Value: "text/plain"},
		{Name: "Host", Value: "example.test"}, {Name: "TE", Value: "trailers"}, {Name: "Content-Length", Value: "1"},
		{Name: "bad+name", Value: "blue"}, {Name: "trace-bin", Value: "Yh=="}, {Name: "mode", Value: "café"},
	} {
		ruleFilter.Set = []gatewayv1.HTTPHeader{invalid}
		if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(string(invalid.Name))) {
			t.Errorf("want a diagnostic for %q, got %v", invalid.Name, err)
		}
	}
	ruleFilter.Set = nil
	route.Spec.Rules[0].BackendRefs[0].Filters[0].Type = gatewayv1.GRPCRouteFilterResponseHeaderModifier
	if _, err := r.validateRouteStructure(model.ProjectGRPCRoute(route)); err == nil {
		t.Fatal("unsupported backend filters must reject the route")
	}
}

func TestGRPCDefaultAndServiceRulesUseMethodPrecedence(t *testing.T) {
	cfg := testConfig()
	cfg.BackendH2C, cfg.FixedHTTPResponses = true, true
	_, gateway := providerRouteParentObjects(cfg)
	route := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "echo", UID: "grpc"}}
	route.Spec.ParentRefs = []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gateway.Name)}}
	service := ptr.To("example.Echo")
	route.Spec.Rules = []gatewayv1.GRPCRouteRule{
		grpcRule("v1"),
		grpcRule("v2", gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: service}}),
		grpcRule("v1", gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Service: service, Method: ptr.To("Echo")}}),
		grpcRule("v2", gatewayv1.GRPCRouteMatch{}),
	}
	v1, ep1, node := validNodePortBackend(gateway.Namespace, "v1")
	v2, ep2, _ := validNodePortBackend(gateway.Namespace, "v2")
	v1.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	v2.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
	reader := indexedFakeClientBuilder(testScheme(t), cfg).WithObjects(v1, ep1, v2, ep2, node).Build()
	desired, err := (&Reconciler{Config: cfg}).buildRouteModelWithReader(t.Context(), reader, model.ProjectGRPCRoute(route), gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !desired.GRPC {
		t.Fatal("gRPC service and default policies must be marked for native media type matching")
	}
	got := make(map[string][]cloud.HTTPPathMatch)
	for _, backend := range desired.HTTPBackends {
		got[backend.Name] = backend.Matches
	}
	want := map[string][]cloud.HTTPPathMatch{
		"v1": {
			{PathType: cloud.PathMatchExact, PathValue: "/example.Echo/Echo", Precedence: 0},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 2},
		},
		"v2": {
			{PathType: cloud.PathMatchPrefix, PathValue: "/example.Echo", Precedence: 1},
			{PathType: cloud.PathMatchPrefix, PathValue: "/", Precedence: 3},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gRPC matches by backend = %#v, want %#v", got, want)
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
		{name: "method without a service", enrolled: true, match: gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{Method: ptr.To("Echo")}, Headers: []gatewayv1.GRPCHeaderMatch{exact}}, message: "exact service"},
		{name: "empty method object", enrolled: true, match: gatewayv1.GRPCRouteMatch{Method: &gatewayv1.GRPCMethodMatch{}}, message: "exact service"},
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
	candidates := []routeProfileCandidate{{route: http, selection: routeSelection{{listeners: []*gatewayv1.Listener{listener}}}}, {route: grpc, selection: routeSelection{{listeners: []*gatewayv1.Listener{listener}}}}}
	if conflicts := electRouteProfiles(candidates); len(conflicts[routeKey(grpc)]) != 1 || len(conflicts[routeKey(http)]) != 0 {
		t.Fatalf("the older HTTPRoute must own every declared hostname: %v", conflicts)
	}
	grpc.Spec.Hostnames = []gatewayv1.Hostname{"shared.test"}
	if conflicts := electRouteProfiles(candidates); len(conflicts) != 0 {
		t.Fatalf("the wildcard cannot reserve the suffix apex: %v", conflicts)
	}
}

func TestGRPCConflictOnlyRejectsTheOwningHTTPParent(t *testing.T) {
	const section gatewayv1.SectionName = "http"
	direct := routeParentSelection{
		parent: managedRouteParent{ref: gatewayv1.ParentReference{Name: "edge", SectionName: ptr.To(section)}},
		listeners: []*gatewayv1.Listener{{
			Name: section, Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("api.example.com")),
		}},
	}
	delegated := routeParentSelection{
		parent: managedRouteParent{
			ref:         gatewayv1.ParentReference{Kind: ptr.To(gatewayv1.Kind("ListenerSet")), Name: "public", SectionName: ptr.To(section)},
			listenerSet: &gatewayv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "public"}},
		},
		listeners: []*gatewayv1.Listener{{
			Name: section, Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: ptr.To(gatewayv1.Hostname("*.example.com")),
		}},
	}
	http := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "http", CreationTimestamp: metav1.Unix(2, 0)}}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "grpc", CreationTimestamp: metav1.Unix(1, 0)}}
	grpc.Spec.Hostnames = []gatewayv1.Hostname{"api.example.com"}
	selection := routeSelection{direct, delegated}
	conflicts := electRouteProfiles([]routeProfileCandidate{
		{route: http, selection: selection},
		{route: model.ProjectGRPCRoute(grpc), selection: routeSelection{direct}},
	})
	scope := httpRouteScope{parentSelection: selection, rejectedSections: conflicts[routeKey(http)]}
	updates := scope.completeParentUpdates([]parentStatusUpdate{{parent: direct.parent.ref, status: programmedRouteStatus()}})
	if len(updates) != 2 {
		t.Fatalf("got %d parent status updates, want both references", len(updates))
	}
	if updates[0].parent.Name != "edge" || updates[0].status.accepted != metav1.ConditionFalse ||
		updates[0].status.acceptedReason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
		t.Errorf("Gateway parent should report the older GRPCRoute conflict: %+v", updates[0])
	}
	if updates[1].parent.Name != "public" || updates[1].status != programmedRouteStatus() {
		t.Errorf("ListenerSet parent should keep serving its wildcard: %+v", updates[1])
	}
	if len(selection[0].listeners) != 1 || selection[0].rejection != nil {
		t.Error("reporting a profile conflict changed the observed parent selection")
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
	if len(desired.HTTPBackends) != 1 || !desired.GRPC || !desired.HTTPBackends[0].BackendH2C {
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
		{route: http, selection: routeSelection{{listeners: []*gatewayv1.Listener{wildcard}}}},
		{route: model.ProjectGRPCRoute(route), selection: routeSelection{{listeners: []*gatewayv1.Listener{exact}}}},
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
