//go:build envtest

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

package controller_test

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

// Exercise the production watches, ownership checkpoints and shared graph writer
// against the release CRDs. The provider records the complete forwarding intent.
func testTCPGatewaySelection(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	const name = "tcp-tenants"
	const edge = "controller-envtest"
	params := envtestClassParameters(name)
	params.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: gatewayv1.GatewayClassSpec{
		ControllerName: cfg.ControllerName,
		ParametersRef:  &gatewayv1.ParametersReference{Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: params.Name},
	}}
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: edge}, Spec: gatewayv1.GatewaySpec{
		GatewayClassName: name, Listeners: []gatewayv1.Listener{{Name: "database", Protocol: gatewayv1.TCPProtocolType, Port: 5432,
			AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"edge": name}}}},
		}},
	}}
	create := func(objects ...client.Object) {
		t.Helper()
		for _, object := range objects {
			if err := c.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
		}
	}
	create(params, class, gw)
	grant := &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Name: "database-tcp", Namespace: edge}, Spec: gatewayv1.ReferenceGrantSpec{
		From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "TCPRoute", Namespace: "tcp-accounts"}},
		To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName("database"))}},
	}}
	httpGrant := grant.DeepCopy()
	httpGrant.Name, httpGrant.Spec.From[0].Kind = "database-http", "HTTPRoute"
	create(grant, httpGrant)
	var tenants []*corev1.Namespace
	var routes []*gatewayv1.TCPRoute
	addresses := []string{"10.1.2.11", "10.1.2.12"}
	for i, ns := range []string{"tcp-accounts", "tcp-catalog"} {
		tenant := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{"edge": name}}}
		backendNS := ns
		if i == 0 {
			backendNS = edge
		}
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: backendNS}, Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "db", Port: 5432, Protocol: corev1.ProtocolTCP, AppProtocol: ptr.To("postgresql")}},
		}}
		endpoints := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: backendNS, Labels: map[string]string{discoveryv1.LabelServiceName: svc.Name}},
			AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: ptr.To("db"), Port: ptr.To(int32(15432))}},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{addresses[i]}}},
		}
		route := &gatewayv1.TCPRoute{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: ns}, Spec: gatewayv1.TCPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: name, Namespace: ptr.To(gatewayv1.Namespace(edge)), SectionName: ptr.To(gatewayv1.SectionName("database"))}}},
			Rules: []gatewayv1.TCPRouteRule{{BackendRefs: []gatewayv1.BackendRef{{BackendObjectReference: gatewayv1.BackendObjectReference{
				Name: gatewayv1.ObjectName(svc.Name), Namespace: ptr.To(gatewayv1.Namespace(backendNS)), Port: ptr.To(gatewayv1.PortNumber(5432)),
			}}}}},
		}}
		create(tenant, svc, endpoints, route)
		tenants = append(tenants, tenant)
		routes = append(routes, route)
	}
	type expectedRoute struct {
		accepted, resolved, programmed metav1.ConditionStatus
		reason                         string
	}
	yes, no := metav1.ConditionTrue, metav1.ConditionFalse
	check := func(t *testing.T, count int32, selection int, expectations []expectedRoute) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			var live gatewayv1.Gateway
			if err := c.Get(ctx, client.ObjectKeyFromObject(gw), &live); err != nil {
				return false, err
			}
			if len(live.Status.Listeners) != 1 || live.Status.Listeners[0].AttachedRoutes != count {
				return false, nil
			}
			for i, expect := range expectations {
				var current gatewayv1.TCPRoute
				if err := c.Get(ctx, client.ObjectKeyFromObject(routes[i]), &current); err != nil {
					return false, err
				}
				if len(current.Status.Parents) != 1 || current.Status.Parents[0].ControllerName != cfg.ControllerName {
					return false, nil
				}
				conditions := current.Status.Parents[0].Conditions
				for _, field := range []struct {
					typ    string
					status metav1.ConditionStatus
				}{{"Accepted", expect.accepted}, {"ResolvedRefs", expect.resolved}, {cfg.Domain() + "/Programmed", expect.programmed}} {
					if field.status == "" {
						continue
					}
					condition := meta.FindStatusCondition(conditions, field.typ)
					if condition == nil || condition.Status != field.status || condition.ObservedGeneration != current.Generation {
						return false, nil
					}
				}
				if expect.reason != "" && !slices.ContainsFunc(conditions, func(c metav1.Condition) bool { return c.Reason == expect.reason }) {
					return false, nil
				}
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			// The last complete graph for this Gateway must contain exactly the
			// selected UID and endpoint, or contain no forwarding route at all.
			for i := len(provider.graphSpecs) - 1; i >= 0; i-- {
				g := provider.graphSpecs[i]
				if g.Identity.GatewayUID != string(gw.UID) {
					continue
				}
				if g.Gateway.Disposition != cloud.GraphEnsure {
					return false, nil
				}
				var ensured []cloud.RouteSpec
				for _, node := range g.Routes {
					if node.Disposition == cloud.GraphEnsure {
						ensured = append(ensured, node.Spec)
					}
				}
				if selection < 0 {
					return len(ensured) == 0, nil
				}
				return len(ensured) == 1 && ensured[0].Identity.RouteUID == string(routes[selection].UID) &&
					slices.Equal(ensured[0].Members, []cloud.Member{{Address: addresses[selection], Port: 15432}}), nil
			}
			return false, nil
		})
	}
	winner := expectedRoute{accepted: yes, resolved: yes, programmed: yes}
	standby := expectedRoute{accepted: yes, resolved: yes, programmed: no, reason: "NotSelected"}
	check(t, 2, 0, []expectedRoute{winner, standby})
	if err := c.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	check(t, 2, -1, []expectedRoute{{accepted: yes, resolved: no, programmed: no, reason: "RefNotPermitted"}, standby})
	old := tenants[0].DeepCopy()
	delete(tenants[0].Labels, "edge")
	if err := c.Patch(t.Context(), tenants[0], client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	check(t, 1, 1, []expectedRoute{{accepted: no, programmed: no, reason: "NotAllowedByListeners"}, winner})
}
