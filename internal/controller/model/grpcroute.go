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
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const (
	HTTPRouteKind gatewayv1.Kind = "HTTPRoute"
	GRPCRouteKind gatewayv1.Kind = "GRPCRoute"
)

var grpcServiceName = regexp.MustCompile(`^\.?[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
var grpcMethodName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// HTTPProfileRouteKind identifies the original Kubernetes kind of a route in the HTTP graph.
func HTTPProfileRouteKind(route *gatewayv1.HTTPRoute) gatewayv1.Kind {
	if route.Kind == string(GRPCRouteKind) {
		return GRPCRouteKind
	}
	return HTTPRouteKind
}

// ProjectGRPCRoute creates a request-local HTTP graph input. Kubernetes status
// and ownership remain on the GRPCRoute; an unsupported rule yields no rules
// so it cannot accidentally program an HTTP catch-all or a partial route.
func ProjectGRPCRoute(route *gatewayv1.GRPCRoute) *gatewayv1.HTTPRoute {
	projected := &gatewayv1.HTTPRoute{
		TypeMeta:   metav1.TypeMeta{Kind: string(GRPCRouteKind), APIVersion: gatewayv1.GroupVersion.String()},
		ObjectMeta: *route.ObjectMeta.DeepCopy(),
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: *route.Spec.CommonRouteSpec.DeepCopy(),
			Hostnames:       append([]gatewayv1.Hostname(nil), route.Spec.Hostnames...),
		},
		Status: gatewayv1.HTTPRouteStatus{RouteStatus: *route.Status.RouteStatus.DeepCopy()},
	}
	rules := make([]gatewayv1.HTTPRouteRule, 0, len(route.Spec.Rules))
	for _, rule := range route.Spec.Rules {
		if len(rule.BackendRefs) == 0 ||
			rule.SessionPersistence != nil { //nolint:staticcheck // Still served in the experimental bundle.
			return projected
		}
		lowered := gatewayv1.HTTPRouteRule{
			BackendRefs: make([]gatewayv1.HTTPBackendRef, 0, len(rule.BackendRefs)),
		}
		for _, backend := range rule.BackendRefs {
			if len(backend.Filters) != 0 {
				return projected
			}
			lowered.BackendRefs = append(lowered.BackendRefs, gatewayv1.HTTPBackendRef{BackendRef: *backend.BackendRef.DeepCopy()})
		}
		for _, filter := range rule.Filters {
			var header gatewayv1.HTTPRouteFilter
			switch filter.Type {
			case gatewayv1.GRPCRouteFilterRequestHeaderModifier:
				header.Type = gatewayv1.HTTPRouteFilterRequestHeaderModifier
				header.RequestHeaderModifier = filter.RequestHeaderModifier.DeepCopy()
			case gatewayv1.GRPCRouteFilterResponseHeaderModifier:
				header.Type = gatewayv1.HTTPRouteFilterResponseHeaderModifier
				header.ResponseHeaderModifier = filter.ResponseHeaderModifier.DeepCopy()
			default:
				return projected
			}
			lowered.Filters = append(lowered.Filters, header)
		}
		for _, match := range rule.Matches {
			projectedMatch, ok := projectGRPCMatch(match)
			if !ok {
				return projected
			}
			lowered.Matches = append(lowered.Matches, projectedMatch)
		}
		rules = append(rules, lowered)
	}
	projected.Spec.Rules = rules
	return projected
}

// gRPC status can share a headers block with application metadata on an
// immediate error. Leave protocol fields under the origin and transport's control.
func validateGRPCResponseMetadata(operations []cloud.HTTPHeaderOperation) *Diagnostic {
	fields := 0
	for _, op := range operations {
		switch {
		case op.Name == "content-type" || strings.HasPrefix(op.Name, "grpc-"):
			return unsupported(fmt.Sprintf("response header %q is managed by gRPC", op.Name), ScopeRule)
		case !cloud.ValidGRPCMetadataName(op.Name):
			return unsupported(fmt.Sprintf("invalid gRPC response metadata name %q", op.Name), ScopeRule)
		case op.Operation == cloud.HTTPHeaderRemove:
			continue
		case strings.HasSuffix(op.Name, "-bin"):
			fields += 1 + strings.Count(op.Value, ",")
			for value := range strings.SplitSeq(op.Value, ",") {
				value = strings.Trim(value, " ")
				encoding := base64.RawStdEncoding
				if strings.HasSuffix(value, "=") {
					encoding = base64.StdEncoding
				}
				if _, err := encoding.Strict().DecodeString(value); err != nil {
					return unsupported(fmt.Sprintf("gRPC binary metadata %q requires base64 values", op.Name), ScopeRule)
				}
			}
		case strings.ContainsFunc(op.Value, func(r rune) bool { return r < ' ' || r > '~' }):
			return unsupported(fmt.Sprintf("gRPC response metadata %q requires printable ASCII", op.Name), ScopeRule)
		default:
			fields++
		}
		if fields > cloud.MaxHTTPHeaderOperations {
			return unsupported(fmt.Sprintf("gRPC response metadata can set or add at most %d fields after expanding binary values", cloud.MaxHTTPHeaderOperations), ScopeRule)
		}
	}
	return nil
}

func projectGRPCMatch(match gatewayv1.GRPCRouteMatch) (gatewayv1.HTTPRouteMatch, bool) {
	var projected gatewayv1.HTTPRouteMatch
	if method := match.Method; method != nil {
		if ptr.Deref(method.Type, gatewayv1.GRPCMethodMatchExact) != gatewayv1.GRPCMethodMatchExact ||
			method.Service == nil || !grpcServiceName.MatchString(*method.Service) {
			return projected, false
		}
		path := "/" + *method.Service
		pathType := gatewayv1.PathMatchPathPrefix
		if method.Method != nil {
			if !grpcMethodName.MatchString(*method.Method) {
				return projected, false
			}
			path += "/" + *method.Method
			pathType = gatewayv1.PathMatchExact
		}
		projected.Path = &gatewayv1.HTTPPathMatch{Type: &pathType, Value: &path}
	}
	for _, header := range match.Headers {
		// The HTTP normalizer applies the same first-entry and exact-match rules.
		projected.Headers = append(projected.Headers, gatewayv1.HTTPHeaderMatch{
			Type: ptr.To(gatewayv1.HeaderMatchType(ptr.Deref(header.Type, gatewayv1.GRPCHeaderMatchExact))),
			Name: gatewayv1.HTTPHeaderName(header.Name), Value: header.Value,
		})
	}
	return projected, true
}
