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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

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
