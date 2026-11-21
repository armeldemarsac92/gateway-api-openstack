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
	"fmt"
	"reflect"
	"slices"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestNamedRouteRulesPreserveForwardingAndBinding(t *testing.T) {
	const grpcService = "/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/"
	for _, test := range []struct {
		fixture string
		kind    gatewayv1.Kind
		route   client.Object
		paths   [2]string
	}{
		{"httproute-named-rule.yaml", model.HTTPRouteKind, &gatewayv1.HTTPRoute{}, [2]string{"/named", "/unnamed"}},
		{"grpcroute-named-rule.yaml", model.GRPCRouteKind, &gatewayv1.GRPCRoute{}, [2]string{grpcService + "Echo", grpcService + "EchoTwo"}},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			data, err := conformance.Manifests.ReadFile("tests/" + test.fixture)
			if err != nil {
				t.Fatal(err)
			}
			if err := yaml.UnmarshalStrict(data, test.route); err != nil {
				t.Fatal(err)
			}
			test.route.SetUID(types.UID(test.route.GetName()))
			test.route.SetGeneration(1)

			cfg := testConfig()
			grpc := test.kind == model.GRPCRouteKind
			cfg.BackendH2C, cfg.FixedHTTPResponses = grpc, grpc
			pathType, backendPrefix, precedence := cloud.PathMatchPrefix, "", [2]int{1, 0}
			if grpc {
				pathType, backendPrefix, precedence = cloud.PathMatchExact, "grpc-", [2]int{0, 1}
			}
			class, gateway, _, service, endpoints := endpointRouteFixture(t)
			gateway.Namespace, gateway.Name = test.route.GetNamespace(), "same-namespace"
			objects := []client.Object{class, gateway, test.route}
			for i := range test.paths {
				backend, slice := service.DeepCopy(), endpoints.DeepCopy()
				name := fmt.Sprintf("%sinfra-backend-v%d", backendPrefix, i+1)
				backend.Namespace, backend.Name, backend.UID = gateway.Namespace, name, types.UID(name)
				backend.Spec.Ports[0].Port = 8080
				if grpc {
					backend.Spec.Ports[0].AppProtocol = ptr.To("kubernetes.io/h2c")
				}
				slice.Namespace, slice.Name, slice.Labels[discoveryv1.LabelServiceName] = gateway.Namespace, name, name
				slice.Endpoints[0].Addresses = []string{fmt.Sprintf("192.0.2.%d", 20+i)}
				objects = append(objects, backend, slice)
			}
			kube := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(test.route).WithObjects(objects...).Build()
			provider := &recordingProvider{}
			r := newTestReconciler(Reconciler{Config: cfg, Client: kube, APIReader: kube, Coordinator: &graph.Coordinator{}}, provider).forRouteKind(test.kind)
			key := client.ObjectKeyFromObject(test.route)
			reconcile := func(name gatewayv1.SectionName) cloud.RouteSpec {
				t.Helper()
				provider.routeSpecs = nil
				request := ctrl.Request{NamespacedName: key}
				result, err := r.Reconcile(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				// A newly attached route persists its binding before programming.
				if result.Requeue {
					if _, err := r.Reconcile(t.Context(), request); err != nil {
						t.Fatal(err)
					}
				}
				if err := kube.Get(t.Context(), key, test.route); err != nil {
					t.Fatal(err)
				}
				var parents []gatewayv1.RouteParentStatus
				var first, second *gatewayv1.SectionName
				switch route := test.route.(type) {
				case *gatewayv1.HTTPRoute:
					parents = route.Status.Parents
					first, second = route.Spec.Rules[0].Name, route.Spec.Rules[1].Name
				case *gatewayv1.GRPCRoute:
					parents = route.Status.Parents
					first, second = route.Spec.Rules[0].Name, route.Spec.Rules[1].Name
				}
				if first == nil || *first != name || second != nil {
					t.Fatalf("rule names changed on the Kubernetes resource: %#v", test.route)
				}
				if len(parents) != 1 ||
					!meta.IsStatusConditionTrue(parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) ||
					!meta.IsStatusConditionTrue(parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)) {
					t.Fatalf("route parent status = %#v", parents)
				}
				if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 {
					t.Fatalf("route graph ensures = %v, deletes = %v", provider.routeSpecs, provider.deletedRoutes)
				}
				return provider.routeSpecs[0]
			}

			spec := reconcile("named-rule")
			if spec.Identity != controller.RouteIdentity(cfg, gateway, test.route) || len(spec.HTTPBackends) != len(test.paths) {
				t.Fatalf("incorrect identity or missing rules in provider graph: %#v", spec)
			}
			for i, backend := range spec.HTTPBackends {
				match := cloud.HTTPPathMatch{PathType: pathType, PathValue: test.paths[i], Precedence: precedence[i]}
				member := cloud.Member{Address: fmt.Sprintf("192.0.2.%d", 20+i), Port: 8080}
				if backend.Name != fmt.Sprintf("%sinfra-backend-v%d", backendPrefix, i+1) ||
					!reflect.DeepEqual(backend.Matches, []cloud.HTTPPathMatch{match}) ||
					!slices.Equal(backend.Members, []cloud.Member{member}) || backend.BackendH2C != grpc {
					t.Fatalf("incorrect forwarding for rule %d: %#v", i, backend)
				}
			}
			finalizers := slices.Clone(test.route.GetFinalizers())
			if len(finalizers) == 0 || test.route.GetAnnotations()[cfg.RouteLifecycleAnnotation()] != controller.RouteAttached {
				t.Fatalf("named route lacks a durable Gateway binding: %v, %v", finalizers, test.route.GetAnnotations())
			}
			const renamed = gatewayv1.SectionName("renamed-rule")
			switch route := test.route.(type) {
			case *gatewayv1.HTTPRoute:
				route.Spec.Rules[0].Name = ptr.To(renamed)
			case *gatewayv1.GRPCRoute:
				route.Spec.Rules[0].Name = ptr.To(renamed)
			}
			test.route.SetGeneration(2)
			if err := kube.Update(t.Context(), test.route); err != nil {
				t.Fatal(err)
			}
			if after := reconcile(renamed); !reflect.DeepEqual(after, spec) || !slices.Equal(test.route.GetFinalizers(), finalizers) {
				t.Fatalf("renaming a rule changed its forwarding graph or finalizers: %#v", after)
			}
		})
	}
}
