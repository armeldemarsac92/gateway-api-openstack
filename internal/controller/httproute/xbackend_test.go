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
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func TestXBackendExternalHostnameRefreshKeepsServiceAndRouteStatus(t *testing.T) {
	f := newRouteModelInputFixture(t)
	cfg := &f.reconciler.Config
	cfg.GatewayAPIChannel = "experimental"
	cfg.XBackendExternalHostnameClasses = []string{string(f.gateway.Spec.GatewayClassName)}
	cfg.XBackendExternalHostnameCIDRs = []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}
	for _, name := range cfg.GatewayAPICRDNames() {
		var definition apiextensionsv1.CustomResourceDefinition
		if err := f.client.Get(t.Context(), types.NamespacedName{Name: name}, &definition); err != nil {
			definition = *gatewayAPICRD(name, gatewayconsts.BundleVersion)
			definition.Annotations[gatewayconsts.ChannelAnnotation] = cfg.GatewayAPIChannel
			f.create(t, &definition)
		} else {
			definition.Annotations[gatewayconsts.ChannelAnnotation] = cfg.GatewayAPIChannel
			if err := f.client.Update(t.Context(), &definition); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := gatewayx.Install(f.client.Scheme()); err != nil {
		t.Fatal(err)
	}
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: f.service.Name, UID: "routed-xbackend", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type:             gatewayx.BackendTypeExternalHostname,
			Port:             gatewayx.BackendPort{Number: 8089},
			ExternalHostname: &gatewayx.ExternalHostnameBackend{Hostname: "origin.example.test"},
		},
	}
	f.create(t, backend)
	external := pathBackendRule("/external", gatewayv1.PathMatchExact, gatewayv1.ObjectName(backend.Name))
	ref := &external.BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind(model.XBackendKind)), nil
	f.route.Spec.Rules = append(f.route.Spec.Rules, external)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}

	provider := &recordingProvider{}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	var serviceBefore cloud.HTTPBackend
	for _, phase := range []struct {
		name      string
		addresses []string
		dnsErr    error
		want      []cloud.Member
	}{
		{name: "multi-A", addresses: []string{"198.51.100.20", "::ffff:198.51.100.9", "198.51.100.9", "2001:db8::1", "192.0.2.20"},
			want: []cloud.Member{{Address: "198.51.100.9", Port: 8089}, {Address: "198.51.100.20", Port: 8089}}},
		{name: "rotation", addresses: []string{"198.51.100.21"}, want: []cloud.Member{{Address: "198.51.100.21", Port: 8089}}},
		{name: "NXDOMAIN", dnsErr: &net.DNSError{IsNotFound: true}},
		{name: "timeout", dnsErr: &net.DNSError{IsTimeout: true}},
		{name: "restoration", addresses: []string{"198.51.100.9"}, want: []cloud.Member{{Address: "198.51.100.9", Port: 8089}}},
	} {
		t.Run(phase.name, func(t *testing.T) {
			lookups := 0
			f.reconciler.externalHostnameLookup = func(ctx context.Context, family, hostname string) ([]netip.Addr, error) {
				lookups++
				if family != "ip4" || hostname != "origin.example.test." {
					t.Errorf("unexpected resolver arguments %q %q", family, hostname)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("DNS lookup has no deadline")
				}
				addresses := make([]netip.Addr, 0, len(phase.addresses))
				for _, address := range phase.addresses {
					addresses = append(addresses, netip.MustParseAddr(address))
				}
				return addresses, phase.dnsErr
			}
			r := newTestReconciler(*f.reconciler, provider)
			before := len(provider.routeSpecs)
			result, err := r.Reconcile(t.Context(), request)
			wantDelay := controller.OpenStackResyncAfter(externalHostnameRefresh, f.route.UID)
			if err != nil || result.RequeueAfter != wantDelay || len(provider.routeSpecs) != before+1 || lookups != 1 {
				var current gatewayv1.HTTPRoute
				_ = f.client.Get(t.Context(), request.NamespacedName, &current)
				t.Fatalf("reconcile = %+v, %v, %d writes, %d lookups, want bounded refresh %s and one observation; parents %+v", result, err, len(provider.routeSpecs)-before, lookups, wantDelay, current.Status.Parents)
			}
			pools := provider.routeSpecs[before].HTTPBackends
			if len(pools) != 2 || !pools[1].RoutedExternal || pools[1].InvalidReference || pools[1].Unavailable != (len(phase.want) == 0) ||
				!slices.Equal(pools[1].Members, phase.want) {
				t.Fatalf("external destination did not converge for %s: %+v", phase.name, pools)
			}
			if serviceBefore.Name == "" {
				serviceBefore = pools[0]
			}
			if pools[0].RoutedExternal || !reflect.DeepEqual(pools[0], serviceBefore) ||
				!slices.Equal(pools[0].Members, []cloud.Member{{Address: "192.0.2.20", Port: 8080}}) {
				t.Fatalf("DNS refresh changed the sibling Service pool: %+v", pools[0])
			}
			var current gatewayv1.HTTPRoute
			if err := f.client.Get(t.Context(), request.NamespacedName, &current); err != nil {
				t.Fatal(err)
			}
			if len(current.Status.Parents) != 1 || !meta.IsStatusConditionTrue(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)) ||
				!meta.IsStatusConditionTrue(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted)) {
				t.Fatalf("DNS data changed a valid HTTPRoute reference: %+v", current.Status.Parents)
			}
		})
	}

	f.reconciler.externalHostnameLookup = func(context.Context, string, string) ([]netip.Addr, error) { return nil, nil }
	before := f.observe(t)
	backend.Spec.ExternalHostname.Hostname = "replacement.example.test"
	backend.Generation++
	if err := f.client.Update(t.Context(), backend); err != nil {
		t.Fatal(err)
	}
	if err := before.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("a changed XBackend must invalidate the planned mutation, got %v", err)
	}

	cfg.XBackendExternalHostnameClasses = []string{"another-class"}
	f.reconciler.externalHostnameLookup = func(context.Context, string, string) ([]netip.Addr, error) {
		t.Error("a class without opt-in must never query DNS")
		return nil, nil
	}
	after := f.observe(t)
	pools := after.Nodes[0].Spec.HTTPBackends
	if len(pools) != 2 || !pools[1].InvalidReference || pools[1].RoutedExternal || !reflect.DeepEqual(pools[0], serviceBefore) {
		t.Fatalf("a class without opt-in must reject only the routed backend: %+v", pools)
	}
	profile, err := f.reconciler.xBackendProfile(f.gateway)
	if err != nil {
		t.Fatal(err)
	}
	if condition := xBackendAcceptedCondition(backend, profile); condition.Status != metav1.ConditionFalse {
		t.Fatalf("XBackend status must reject the non-opted-in class: %+v", condition)
	}
}

func TestXBackendAndSameNamedServiceKeepSeparateWeightedTargets(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.GatewayAPIChannel = "experimental"
	f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.WeightedHTTPTargets = true, true
	if err := gatewayx.Install(f.client.Scheme()); err != nil {
		t.Fatal(err)
	}
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: f.service.Name, UID: "xbackend-uid", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector,
			Port: gatewayx.BackendPort{Number: gatewayx.PortNumber(f.service.Spec.Ports[0].Port)},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "direct"},
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "direct", Labels: map[string]string{"app": "direct"}},
		Status: corev1.PodStatus{
			PodIP: "192.0.2.39", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	f.create(t, backend, pod)
	rule := &f.route.Spec.Rules[0]
	rule.BackendRefs[0].Weight = ptr.To(int32(3))
	xref := gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{
		Weight: ptr.To(int32(7)),
		BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: gatewayv1.ObjectName(backend.Name), Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind("XBackend")),
		},
	}}
	rule.BackendRefs = append(rule.BackendRefs, xref)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	before := f.observe(t)
	spec := before.Nodes[0].Spec
	if len(spec.HTTPBackends) != 2 || spec.HTTPBackends[0].Kind != "" || spec.HTTPBackends[1].Kind != cloud.HTTPBackendKindXBackend ||
		identity.HTTPBackendPoolKey(spec.HTTPBackends[0]) == identity.HTTPBackendPoolKey(spec.HTTPBackends[1]) ||
		!reflect.DeepEqual(spec.HTTPBackends[1].Members, []cloud.Member{{Address: pod.Status.PodIP, Port: int(backend.Spec.Port.Number)}}) {
		t.Fatalf("expected two distinct pools, got %+v", spec.HTTPBackends)
	}
	want := []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {BackendIndex: ptr.To(1), Weight: 7}}
	if len(spec.WeightedHTTPRules) != 1 || !reflect.DeepEqual(spec.WeightedHTTPRules[0].Targets, want) {
		t.Fatalf("expected weighted targets %+v, got %+v", want, spec.WeightedHTTPRules)
	}
	inputs, err := f.reconciler.collectRouteModelInputs(t.Context(), f.reader, f.gateway, []gatewayv1.HTTPRoute{*f.route}, nil)
	if err != nil {
		t.Fatal(err)
	}
	delete(pod.Labels, "app")
	if err := f.client.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if err := inputs.validateBackends(t.Context(), f.reader); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("label removal did not invalidate the cloud snapshot: %v", err)
	}

	// A ReferenceGrant never authorizes a foreign XBackend and none of its
	// dependencies are read. Its old weight is reserved for errors.
	grant := serviceReferenceGrant("foreign", f.route.Namespace, backend.Name)
	grant.Spec.To[0].Group = gatewayv1.Group(gatewayx.GroupName)
	grant.Spec.To[0].Kind = "XBackend"
	f.create(t, grant)
	f.route.Spec.Rules[0].BackendRefs[1].Namespace = ptr.To(gatewayv1.Namespace(grant.Namespace))
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	clear(f.reader.counts)
	spec = f.observe(t).Nodes[0].Spec
	if len(spec.HTTPBackends) != 1 || spec.HTTPBackends[0].Kind != "" ||
		!reflect.DeepEqual(spec.WeightedHTTPRules[0].Targets, []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {Weight: 7}}) ||
		f.reader.counts["XBackend"] != 0 || f.reader.counts["Pod"] != 0 || f.reader.counts["ReferenceGrant"] != 0 {
		t.Fatalf("foreign XBackend changed the authorized Service or caused reads: graph %+v, reads %v", spec, f.reader.counts)
	}
}

func TestXBackendAncestorLimitKeepsForeignStatusDuringDetach(t *testing.T) {
	const controllerName gatewayv1.GatewayController = "example.net/controller"
	backend := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Generation: 3}}
	var foreign []gatewayx.BackendAncestorStatus
	for i := range maxXBackendAncestors - 1 {
		foreign = append(foreign, gatewayx.BackendAncestorStatus{
			ControllerName: "example.net/foreign",
			AncestorRef:    gatewayv1.ParentReference{Name: gatewayv1.ObjectName(fmt.Sprintf("foreign-%d", i))},
		})
	}
	backend.Status.Ancestors = append(foreign, gatewayx.BackendAncestorStatus{
		ControllerName: controllerName, AncestorRef: gatewayv1.ParentReference{Name: "detached"},
	})
	condition := metav1.Condition{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", Message: "accepted", ObservedGeneration: backend.Generation}
	desired := map[types.NamespacedName]metav1.Condition{
		{Namespace: "apps", Name: "edge-a"}: condition,
		{Namespace: "apps", Name: "edge-b"}: condition,
	}
	got := mergeXBackendAncestors(backend, controllerName, desired)
	if len(got) != maxXBackendAncestors || !reflect.DeepEqual(got[:len(foreign)], foreign) {
		t.Fatalf("merge lost foreign entries or exceeded the ancestor limit: %v", got)
	}
	entry := got[len(foreign)]
	if entry.AncestorRef.Name != "edge-a" || entry.Conditions[0].ObservedGeneration != backend.Generation {
		t.Fatalf("unexpected replacement for the detached ancestor: %v", entry)
	}
}

func TestXBackendStatusDuringDeletion(t *testing.T) {
	for _, tc := range []struct {
		name               string
		deleteClass        bool
		wantOwnedAncestors int
	}{
		{name: "referenced GatewayClass is terminating", deleteClass: true, wantOwnedAncestors: 1},
		{name: "XBackend held by a foreign finalizer is terminating", wantOwnedAncestors: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class, gateway, route, _, _ := endpointRouteFixture(t)
			ref := &route.Spec.Rules[0].BackendRefs[0]
			ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
			foreign := gatewayx.BackendAncestorStatus{
				ControllerName: "example.net/foreign", AncestorRef: gatewayv1.ParentReference{Name: "foreign"},
			}
			backend := &gatewayx.XBackend{
				ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: string(ref.Name), Generation: 2},
				Spec: gatewayx.BackendSpec{
					Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8087},
					EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
						MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "backend"},
					}},
				},
				Status: gatewayx.BackendStatus{Ancestors: []gatewayx.BackendAncestorStatus{foreign}},
			}
			cfg := testConfig()
			cfg.GatewayAPIChannel = "experimental"
			if tc.deleteClass {
				class.DeletionTimestamp = ptr.To(metav1.Now())
				class.Finalizers = []string{gatewayv1.GatewayClassFinalizerGatewaysExist}
			} else {
				backend.DeletionTimestamp = ptr.To(metav1.Now())
				backend.Finalizers = []string{"example.net/cleanup"}
				backend.Status.Ancestors = append(backend.Status.Ancestors, gatewayx.BackendAncestorStatus{
					ControllerName: cfg.ControllerName, AncestorRef: gatewayAncestorRef(client.ObjectKeyFromObject(gateway)),
					Conditions: []metav1.Condition{xBackendAcceptedCondition(backend, model.XBackendProfile{MemberMode: cloud.MemberModeEndpoint})},
				})
			}
			scheme := testScheme(t)
			if err := gatewayx.Install(scheme); err != nil {
				t.Fatal(err)
			}
			kube := indexedFakeClientBuilder(scheme, cfg).WithStatusSubresource(backend).WithObjects(class, gateway, route, backend).Build()
			status := &xBackendStatusReconciler{routes: &Reconciler{Client: kube, APIReader: kube, Config: cfg}}
			key := client.ObjectKeyFromObject(backend)
			if _, err := status.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if err := kube.Get(t.Context(), key, backend); err != nil {
				t.Fatal(err)
			}
			ancestors := backend.Status.Ancestors
			if len(ancestors) != 1+tc.wantOwnedAncestors || !reflect.DeepEqual(ancestors[0], foreign) {
				t.Fatalf("ancestors = %+v, want foreign entry and %d owned entries", ancestors, tc.wantOwnedAncestors)
			}
			if tc.wantOwnedAncestors == 1 {
				condition := meta.FindStatusCondition(ancestors[1].Conditions, "Accepted")
				if ancestors[1].ControllerName != cfg.ControllerName || condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != backend.Generation {
					t.Fatalf("active Gateway lost current acceptance: %+v", ancestors[1])
				}
			}
		})
	}
}
