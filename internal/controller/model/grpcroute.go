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
	"regexp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
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
		if len(rule.Matches) == 0 || len(rule.Filters) != 0 || len(rule.BackendRefs) != 1 ||
			rule.SessionPersistence != nil { //nolint:staticcheck // Still served in the experimental bundle.
			return projected
		}
		backend := rule.BackendRefs[0]
		if len(backend.Filters) != 0 {
			return projected
		}
		lowered := gatewayv1.HTTPRouteRule{
			BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: *backend.BackendRef.DeepCopy()}},
		}
		for _, match := range rule.Matches {
			method := match.Method
			if len(match.Headers) != 0 || method == nil || ptr.Deref(method.Type, gatewayv1.GRPCMethodMatchExact) != gatewayv1.GRPCMethodMatchExact ||
				method.Service == nil || method.Method == nil || !grpcServiceName.MatchString(*method.Service) || !grpcMethodName.MatchString(*method.Method) {
				return projected
			}
			lowered.Matches = append(lowered.Matches, gatewayv1.HTTPRouteMatch{
				Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/" + *method.Service + "/" + *method.Method)},
			})
		}
		rules = append(rules, lowered)
	}
	projected.Spec.Rules = rules
	return projected
}
