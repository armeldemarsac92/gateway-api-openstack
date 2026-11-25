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

package controller

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestTransportForwardingTargetsPreserveSingletonAndSurviveGatewayEnrollment(t *testing.T) {
	config := Config{ControllerName: "example.com/openstack", ClusterID: "cluster", OpenStackProjectID: "project"}
	route := &gatewayv1.UDPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "route", UID: "route-uid", Generation: 3}}
	east := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "east", UID: "east-uid"}}
	west := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "west", UID: "west-uid"}}
	eastBinding := RouteGatewayBinding{
		Identity: RouteIdentity(config, east, route), Lifecycle: RouteAttached, ExplicitScope: true, Anchor: 8080,
		Forwarding: RouteGatewayTarget{Value: "east-target", Generation: 3, Observed: true},
	}
	if err := ApplyRouteGatewayBinding(config, route, nil, eastBinding); err != nil {
		t.Fatal(err)
	}
	if legacy, found, err := StoredRouteIdentity(config, route); err != nil || !found || legacy != eastBinding.Identity || HasRouteGatewayBindings(config, route) {
		t.Fatalf("sealed forwarding altered the singleton identity representation: %+v, found %t, %v", legacy, found, err)
	}
	mutated := route.DeepCopy()
	controllerutil.RemoveFinalizer(mutated, routeForwardingFinalizer(config, eastBinding))
	if _, err := StoredRouteGatewayBindings(config, mutated); !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("accepted an unsealed singleton forwarding target: %v", err)
	}
	westBinding := RouteGatewayBinding{
		Identity: RouteIdentity(config, west, route), Lifecycle: RouteAttached, ExplicitScope: true,
		Forwarding: RouteGatewayTarget{Value: "west-target", Generation: 3},
	}
	bindings, err := StoredRouteGatewayBindings(config, route)
	if err != nil || len(bindings) != 1 || bindings[0] != eastBinding {
		t.Fatalf("wanted sealed singleton, got %+v, %v", bindings, err)
	}
	if err := ApplyRouteGatewayBinding(config, route, bindings, westBinding); err != nil {
		t.Fatal(err)
	}
	bindings, err = StoredRouteGatewayBindings(config, route)
	if err != nil || len(bindings) != 2 || bindings[0] != eastBinding || bindings[1] != westBinding {
		t.Fatalf("wanted independently sealed bindings, got %+v, %v", bindings, err)
	}
	if err := RemoveRouteGatewayBinding(config, route, bindings, eastBinding.Identity); err != nil {
		t.Fatal(err)
	}
	bindings, err = StoredRouteGatewayBindings(config, route)
	if err != nil || len(bindings) != 1 || bindings[0] != westBinding ||
		controllerutil.ContainsFinalizer(route, routeForwardingFinalizer(config, eastBinding)) {
		t.Fatalf("removing east changed west or kept the old seal: %+v, %v", bindings, err)
	}
}
