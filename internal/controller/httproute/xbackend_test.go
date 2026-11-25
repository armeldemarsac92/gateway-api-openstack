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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func newExternalHostnameFixture(t *testing.T) *routeModelInputFixture {
	t.Helper()
	configuration := testConfig()
	configuration.GatewayAPIChannel = "experimental"
	f := newRouteModelInputFixtureWithConfig(t, configuration)
	cfg := &f.reconciler.Config
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
	f.reconciler.ExternalHostnames = &ExternalHostnameCache{}
	return f
}

func addExternalHostnameRule(t *testing.T, f *routeModelInputFixture, route *gatewayv1.HTTPRoute, name string) *gatewayx.XBackend {
	t.Helper()
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: route.Namespace, Name: name, UID: types.UID("routed-" + name), Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type:             gatewayx.BackendTypeExternalHostname,
			Port:             gatewayx.BackendPort{Number: 8089},
			ExternalHostname: &gatewayx.ExternalHostnameBackend{Hostname: gatewayv1.PreciseHostname(name + ".example.test")},
		},
	}
	f.create(t, backend)
	external := pathBackendRule("/external-"+name, gatewayv1.PathMatchExact, gatewayv1.ObjectName(backend.Name))
	ref := &external.BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind(model.XBackendKind)), nil
	route.Spec.Rules = append(route.Spec.Rules, external)
	return backend
}

func TestXBackendExternalHostnameRefreshKeepsServiceAndRouteStatus(t *testing.T) {
	f := newExternalHostnameFixture(t)
	cfg := &f.reconciler.Config
	cache := f.reconciler.ExternalHostnames
	now := time.Now()
	cache.now = func() time.Time { return now }
	backend := addExternalHostnameRule(t, f, f.route, f.service.Name)
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
			now = now.Add(time.Minute)
			lookups := 0
			cache.lookup = func(ctx context.Context, family, hostname string) ([]netip.Addr, error) {
				lookups++
				if family != "ip4" || hostname != f.service.Name+".example.test." {
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
			wantDelay := controller.OpenStackResyncAfter(cfg.OpenStackResyncInterval, f.route.UID)
			if err != nil || result.RequeueAfter != wantDelay || len(provider.routeSpecs) != before+1 || lookups != 1 {
				var current gatewayv1.HTTPRoute
				_ = f.client.Get(t.Context(), request.NamespacedName, &current)
				t.Fatalf("reconcile = %+v, %v, %d writes, %d lookups, want normal route resync %s and one observation; parents %+v", result, err, len(provider.routeSpecs)-before, lookups, wantDelay, current.Status.Parents)
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

	cache.lookup = func(context.Context, string, string) ([]netip.Addr, error) { return nil, nil }
	before := f.observe(t)
	backend.Spec.ExternalHostname.Hostname = "replacement.example.test"
	backend.Generation++
	if err := f.client.Update(t.Context(), backend); err != nil {
		t.Fatal(err)
	}
	if err := before.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("a changed XBackend must invalidate the planned mutation, got %v", err)
	}
	cache.lookup = func(_ context.Context, _, hostname string) ([]netip.Addr, error) {
		if hostname != "replacement.example.test." {
			t.Errorf("renamed backend queried %q", hostname)
		}
		return []netip.Addr{netip.MustParseAddr("198.51.100.42")}, nil
	}
	changed := f.observe(t).Nodes[0].Spec.HTTPBackends[1]
	if !slices.Equal(changed.Members, []cloud.Member{{Address: "198.51.100.42", Port: 8089}}) {
		t.Fatalf("new hostname must replace the previous answer inside the window: %+v", changed)
	}

	cfg.XBackendExternalHostnameClasses = nil
	cache.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
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

type gatewayRefreshRequests struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]
	pending map[reconcile.Request]time.Duration
}

func (q *gatewayRefreshRequests) AddAfter(request reconcile.Request, after time.Duration) {
	q.pending[request] = after
}

func TestXBackendExternalHostnameSharesGatewayRefreshAcrossMutations(t *testing.T) {
	f := newExternalHostnameFixture(t)
	routes := []*gatewayv1.HTTPRoute{f.route}
	for _, name := range []string{"inventory", "payments"} {
		peer := f.candidate(name)
		peer.Spec.Rules = nil
		addExternalHostnameRule(t, f, peer, name)
		if err := f.reconciler.applyRouteBinding(peer, f.gateway); err != nil {
			t.Fatal(err)
		}
		f.create(t, peer)
		routes = append(routes, peer)
	}
	addExternalHostnameRule(t, f, f.route, "accounts")
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	writer := r.Writer.(*graph.Writer)
	writer.GatewayRefresh = &graph.GatewayRefreshQueue{}
	queue := &gatewayRefreshRequests{pending: make(map[reconcile.Request]time.Duration)}
	if err := writer.GatewayRefresh.Start(t.Context(), queue); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r.ExternalHostnames.now = func() time.Time { return now }
	var mu sync.Mutex
	lookups := map[string]int{}
	r.ExternalHostnames.lookup = func(_ context.Context, _, hostname string) ([]netip.Addr, error) {
		mu.Lock()
		defer mu.Unlock()
		lookups[hostname]++
		// A resolver may alternate singleton answers rather than just shuffle them.
		if lookups[hostname]%2 == 1 {
			return []netip.Addr{netip.MustParseAddr("198.51.100.10")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("198.51.100.20")}, nil
	}
	assertGraphs := func(address string) {
		t.Helper()
		if len(provider.routeSpecs) != len(routes) {
			t.Fatalf("graph ensured %d routes, want %d", len(provider.routeSpecs), len(routes))
		}
		var count int
		for _, spec := range provider.routeSpecs {
			for _, backend := range spec.HTTPBackends {
				if backend.RoutedExternal {
					count++
					if !slices.Equal(backend.Members, []cloud.Member{{Address: address, Port: 8089}}) {
						t.Fatalf("route %s changed members mid-plan: %+v", spec.Identity.RouteName, backend.Members)
					}
				}
			}
		}
		if count != len(routes) {
			t.Fatalf("observed %d routed pools, want %d", count, len(routes))
		}
		provider.routeSpecs = nil
	}
	for i, route := range routes {
		// The provider writer rebuilds the graph before each asynchronous mutation.
		if i < 2 {
			provider.routeResult = new(cloud.RouteResult{Outcome: cloud.ProgressingOutcome("Programming routed members", time.Second)})
		} else {
			provider.routeResult = nil
		}
		result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)})
		if err != nil {
			t.Fatal(err)
		}
		if provider.routeResult == nil && result.RequeueAfter != controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, route.UID) {
			t.Fatalf("ready route must keep its ordinary resync, got %s", result.RequeueAfter)
		}
		assertGraphs("198.51.100.10")
	}
	wantDelay := controller.OpenStackResyncAfter(externalHostnameRefresh, f.gateway.UID)
	gatewayRequest := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(f.gateway)}
	if len(queue.pending) != 1 || queue.pending[gatewayRequest] <= 0 || queue.pending[gatewayRequest] > wantDelay ||
		!reflect.DeepEqual(lookups, map[string]int{"accounts.example.test.": 1, "inventory.example.test.": 1, "payments.example.test.": 1}) {
		t.Fatalf("three route passes must share a single Gateway refresh and one query per name, got %+v and %+v", queue.pending, lookups)
	}
	now = now.Add(wantDelay)
	if _, err := writer.ReconcileGraph(t.Context(), graph.Request{
		Identity: controller.GatewayIdentity(r.Config, f.gateway), Action: graph.Ensure, Gateway: f.gateway,
	}); err != nil {
		t.Fatal(err)
	}
	assertGraphs("198.51.100.20")
	if !reflect.DeepEqual(lookups, map[string]int{"accounts.example.test.": 2, "inventory.example.test.": 2, "payments.example.test.": 2}) {
		t.Fatalf("Gateway expiry should refresh each name once, got %+v", lookups)
	}
}

func TestXBackendExternalHostnameReferencesUseActualGatewayMode(t *testing.T) {
	f := newExternalHostnameFixture(t)
	addExternalHostnameRule(t, f, f.route, "origin")
	gateway := f.gateway.DeepCopy()
	controller.ClearGatewayTopology(f.reconciler.Config, gateway)
	var status routeReconcileStatus
	if err := f.reconciler.resolveRouteReferences(t.Context(), f.route, gateway, &status); err != nil || status.resolved != metav1.ConditionUnknown {
		t.Fatalf("without a Gateway topology the reference should be pending, got %+v and %v", status, err)
	}
	if err := controller.BindGatewayTopology(f.reconciler.Config, gateway, testBoundTopology()); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.resolveRouteReferences(t.Context(), f.route, gateway, &status); err != nil ||
		status.resolved != metav1.ConditionFalse || status.resolvedReason != string(gatewayv1.RouteReasonUnsupportedValue) {
		t.Fatalf("NodePort topology must reject the reference before its listener is ready, got %+v and %v", status, err)
	}
}

func TestXBackendAndSameNamedServiceKeepSeparateWeightedTargets(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.GatewayAPIChannel = "experimental"
	f.reconciler.Config.XBackendSessionPersistence = true
	f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.WeightedHTTPTargets = true, true
	if err := gatewayx.Install(f.client.Scheme()); err != nil {
		t.Fatal(err)
	}
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: f.service.Name, UID: "xbackend-uid", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector,
			Port: gatewayx.BackendPort{Number: gatewayx.PortNumber(f.service.Spec.Ports[0].Port)},
			SessionPersistence: &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{
				Name: ptr.To(gatewayv1.CookieName("reports_session")), Path: ptr.To("/"),
			}},
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
		spec.HTTPBackends[0].SessionCookieName != "" || spec.HTTPBackends[1].SessionCookieName != "reports_session" ||
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

func TestXBackendCookieNameAndCurrentGenerationStatus(t *testing.T) {
	f := newExternalHostnameFixture(t)
	f.reconciler.Config.XBackendClientCertificate, f.reconciler.Config.BackendTLSPolicy = true, true
	f.reconciler.Config.XBackendSessionPersistence = true
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: "reports", UID: "reports-uid", Generation: 4},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8081},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "reports"},
			}},
			Protocol:           ptr.To(gatewayx.BackendProtocolHTTP11),
			SessionPersistence: &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{}},
		},
	}
	other := backend.DeepCopy()
	other.Name, other.UID = "other", "other-uid"
	f.create(t, backend, other)
	makeRule := func(path string, backend *gatewayx.XBackend) gatewayv1.HTTPRouteRule {
		rule := pathBackendRule(path, gatewayv1.PathMatchExact, gatewayv1.ObjectName(backend.Name))
		ref := &rule.BackendRefs[0]
		ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind(model.XBackendKind)), nil
		return rule
	}
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{makeRule("/reports", backend)}
	require.NoError(t, f.client.Update(t.Context(), f.route))
	sibling := f.candidate("sibling")
	sibling.Spec.Rules = []gatewayv1.HTTPRouteRule{makeRule("/sibling", backend), makeRule("/other", other)}
	require.NoError(t, f.reconciler.applyRouteBinding(sibling, f.gateway))
	f.create(t, sibling)
	input := f.observe(t)
	var cookieNames []string
	var otherName string
	for _, node := range input.Nodes {
		for _, pool := range node.Spec.HTTPBackends {
			require.True(t, cloud.ValidHTTPSessionCookieName(pool.SessionCookieName))
			if pool.Name == backend.Name {
				cookieNames = append(cookieNames, pool.SessionCookieName)
			} else {
				otherName = pool.SessionCookieName
			}
		}
	}
	require.Len(t, cookieNames, 2)
	require.Equal(t, cookieNames[0], cookieNames[1])
	require.NotEmpty(t, otherName)
	require.NotEqual(t, cookieNames[0], otherName)

	for _, phase := range []struct {
		issue     cloud.SessionPersistenceIssue
		condition metav1.ConditionStatus
		reason    string
		err       error
	}{
		{cloud.SessionPersistenceUnsupported, metav1.ConditionFalse, "UnsupportedValue", nil},
		{cloud.SessionPersistenceUnverified, metav1.ConditionUnknown, "Pending", errors.New("API timed out")},
		{cloud.SessionPersistenceAvailable, metav1.ConditionTrue, "", nil},
	} {
		f.reconciler.XBackendCookieAvailable = func(context.Context) (bool, error) {
			return phase.issue == cloud.SessionPersistenceAvailable, phase.err
		}
		backendKey := client.ObjectKeyFromObject(backend)
		_, err := (&xBackendStatusReconciler{routes: f.reconciler}).Reconcile(t.Context(), ctrl.Request{NamespacedName: backendKey})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), backendKey, backend))
		require.Len(t, backend.Status.Ancestors, 1)
		condition := meta.FindStatusCondition(backend.Status.Ancestors[0].Conditions, "Accepted")
		require.NotNil(t, condition)
		require.Equal(t, backend.Generation, condition.ObservedGeneration)
		require.Equal(t, phase.condition, condition.Status)
		provider := &recordingProvider{}
		if phase.issue != cloud.SessionPersistenceAvailable {
			result := cloud.RouteProgressingResult("Waiting for Octavia cookie support", time.Second)
			result.BackendSessionPersistenceIssue = phase.issue
			provider.routeResult = &result
			require.Equal(t, phase.reason, condition.Reason)
		}
		r := newTestReconciler(*f.reconciler, provider)
		key := client.ObjectKeyFromObject(f.route)
		_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, f.route))
		require.Len(t, f.route.Status.Parents, 1)
		resolved := meta.FindStatusCondition(f.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
		require.NotNil(t, resolved)
		require.Equal(t, f.route.Generation, resolved.ObservedGeneration)
		require.Equal(t, phase.condition, resolved.Status)
		if phase.reason != "" {
			require.Equal(t, phase.reason, resolved.Reason)
		}
	}

	// A sibling Octavia update can skip the pool and capability inventory.
	// Keep the proved status for this generation until a check can run.
	deferred := cloud.RouteProgressingResult("Octavia load balancer is PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	deferred.BackendSessionPersistenceIssue = cloud.SessionPersistenceNotObserved
	r := newTestReconciler(*f.reconciler, &recordingProvider{routeResult: &deferred})
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	conditions := f.route.Status.Parents[0].Conditions
	require.True(t, meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionResolvedRefs)))
	require.True(t, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed"))
}

func TestXBackendCookieRejectsUnsupportedContract(t *testing.T) {
	profile := model.XBackendProfile{MemberMode: cloud.MemberModeEndpoint, SessionPersistence: true, H2C: true}
	backend := gatewayx.BackendSpec{
		Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8081},
		EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
			MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "reports"},
		}},
		SessionPersistence: &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{}},
	}
	require.Nil(t, model.ValidateXBackend(backend, profile))
	for _, tc := range []struct {
		name string
		edit func(*gatewayx.BackendSpec, *model.XBackendProfile)
	}{
		{"disabled", func(_ *gatewayx.BackendSpec, p *model.XBackendProfile) { p.SessionPersistence = false }},
		{"header", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Type = ptr.To(gatewayv1.HeaderBasedSessionPersistence)
		}},
		{"absolute timeout", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.AbsoluteTimeout = ptr.To(gatewayv1.Duration("10s"))
		}},
		{"permanent cookie", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.LifetimeType = ptr.To(gatewayv1.PermanentCookieLifetimeType)
		}},
		{"path", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Path = ptr.To("/reports")
		}},
		{"reserved cookie name", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("__HoSt-reports"))
		}},
		{"unsafe cookie name", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("invalid;name"))
		}},
		{"h2c", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.Protocol = ptr.To(gatewayx.BackendProtocolH2C)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, profile := backend.DeepCopy(), profile
			tc.edit(spec, &profile)
			require.NotNil(t, model.ValidateXBackend(*spec, profile))
		})
	}
}

func TestXBackendServerOnlyExternalHostnameUsesItsOwnValidation(t *testing.T) {
	f := newExternalHostnameFixture(t)
	r := f.reconciler
	r.Config.BackendTLSPolicy = true
	r.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
	sanAvailable := true
	var sanErr error
	r.XBackendTLSAvailable = func(_ context.Context, requireSAN bool) (bool, error) {
		require.True(t, requireSAN)
		return sanAvailable, sanErr
	}
	backend := addExternalHostnameRule(t, f, f.route, f.service.Name)
	const identityName = "identity.remote.example.test"
	backend.Spec.Port.Number = gatewayx.PortNumber(f.service.Spec.Ports[0].Port)
	backend.Spec.TLS = &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeServerOnly, Validation: &gatewayv1.BackendTLSPolicyValidation{
		Hostname: identityName, CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "external-ca"}},
		SubjectAltNames: []gatewayv1.SubjectAltName{{Type: "URI", URI: "spiffe://example.test/backend"}},
	}}
	require.NoError(t, f.client.Update(t.Context(), backend))
	require.NoError(t, f.client.Update(t.Context(), f.route))
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: f.service.Name, UID: "service-policy"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{Hostname: "service.example.test",
				CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "service-ca"}}},
		},
	}
	f.create(t, policy)
	policy.Status.Ancestors = []gatewayv1.PolicyAncestorStatus{{
		ControllerName: r.Config.ControllerName, AncestorRef: gatewayAncestorRef(client.ObjectKeyFromObject(f.gateway)),
	}}
	require.NoError(t, f.client.Status().Update(t.Context(), policy))
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	require.NoError(t, err)
	ca := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: backend.Namespace, Name: "external-ca", UID: "external-ca-uid"},
		Data: map[string]string{"ca.crt": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}}
	serviceCA := ca.DeepCopy()
	serviceCA.Name, serviceCA.UID = "service-ca", "service-ca-uid"
	f.create(t, ca, serviceCA)

	lookups := 0
	r.ExternalHostnames.lookup = func(_ context.Context, family, hostname string) ([]netip.Addr, error) {
		lookups++
		if family != "ip4" || hostname != string(backend.Spec.ExternalHostname.Hostname)+"." {
			t.Errorf("expected the DNS destination independently of TLS identity, got %q %q", family, hostname)
		}
		return []netip.Addr{netip.MustParseAddr("198.51.100.15")}, nil
	}
	before := f.observe(t)
	require.NoError(t, before.Validate(t.Context()))
	targets := before.Nodes[0].Spec.HTTPBackends
	require.Len(t, targets, 2)
	service, external := targets[0], targets[1]
	require.NotEqual(t, identity.HTTPBackendPoolKey(service), identity.HTTPBackendPoolKey(external))
	require.Equal(t, "service.example.test", service.BackendTLS.Hostname)
	require.Equal(t, string(serviceCA.UID), service.BackendTLS.CAConfigMapUID)
	require.Equal(t, identityName, external.BackendTLS.Hostname)
	require.Equal(t, string(ca.UID), external.BackendTLS.CAConfigMapUID)
	require.Equal(t, []cloud.BackendTLSSubjectAltName{{Type: "URI", URI: "spiffe://example.test/backend"}}, external.BackendTLS.SubjectAltNames)
	require.Equal(t, []cloud.Member{{Address: "198.51.100.15", Port: int(backend.Spec.Port.Number)}}, external.Members)

	assertClosed := func() {
		t.Helper()
		targets := f.observe(t).Nodes[0].Spec.HTTPBackends
		require.Equal(t, service, targets[0])
		require.Equal(t, identity.HTTPBackendPoolKey(external), identity.HTTPBackendPoolKey(targets[1]))
		require.Equal(t, &cloud.BackendTLSValidation{Denied: true}, targets[1].BackendTLS)
		require.Empty(t, targets[1].Members)
		require.Equal(t, 1, lookups)
	}
	checkStatus := func(want metav1.ConditionStatus, reason string) {
		t.Helper()
		waiting := pendingRouteStatus("Waiting for test listener")
		require.NoError(t, r.resolveRouteReferences(t.Context(), f.route, f.gateway, &waiting))
		require.Equal(t, want, waiting.resolved)
		require.Equal(t, reason, waiting.resolvedReason)
		if reason == "UnsupportedProvider" {
			require.Contains(t, waiting.resolvedMessage, "2.24")
		}

		active := newTestReconciler(*r, &recordingProvider{})
		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
		_, err := active.Reconcile(t.Context(), request)
		require.NoError(t, err)
		var current gatewayv1.HTTPRoute
		require.NoError(t, f.client.Get(t.Context(), request.NamespacedName, &current))
		require.Len(t, current.Status.Parents, 1)
		condition := meta.FindStatusCondition(current.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
		require.NotNil(t, condition)
		require.Equal(t, want, condition.Status)
		require.Equal(t, reason, condition.Reason)
		require.Equal(t, waiting.resolvedMessage, condition.Message)
		require.Equal(t, current.Generation, condition.ObservedGeneration)
	}
	sanErr = errors.New("regional capability discovery incomplete")
	assertClosed()
	checkStatus(metav1.ConditionUnknown, string(gatewayv1.RouteReasonPending))
	sanErr = nil
	sanAvailable = false
	assertClosed()
	checkStatus(metav1.ConditionFalse, "UnsupportedProvider")
	sanAvailable = true

	require.NoError(t, f.client.Delete(t.Context(), ca))
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)
	assertClosed()
	checkStatus(metav1.ConditionFalse, string(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef))
	ca.ResourceVersion, ca.UID = "", "restored-ca-uid"
	f.create(t, ca)
	targets = f.observe(t).Nodes[0].Spec.HTTPBackends
	require.Equal(t, service, targets[0])
	require.Equal(t, external.Members, targets[1].Members)
	require.Equal(t, string(ca.UID), targets[1].BackendTLS.CAConfigMapUID)
	require.Equal(t, 2, lookups)
	t.Run("XBackend client identity withdrawal and rotation", func(t *testing.T) {
		original := backend.Spec.DeepCopy()
		backend.Spec.TLS.Mode = gatewayx.BackendTLSModeClientAndServer
		backend.Spec.TLS.ClientCertificateRef = &gatewayv1.SecretObjectReference{Name: "client-identity"}
		require.NoError(t, f.client.Update(t.Context(), backend))
		defer func() {
			backend.Spec = *original
			r.Config.XBackendClientCertificate, r.XBackendClientCertificates = false, nil
			require.NoError(t, f.client.Update(t.Context(), backend))
		}()
		denied := f.observe(t).Nodes[0].Spec.HTTPBackends
		require.True(t, denied[1].BackendTLS.Denied)
		require.Empty(t, denied[1].Members)
		checkStatus(metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedValue))

		r.Config.XBackendClientCertificate = true
		const first = "https://barbican.example/v1/secrets/xbackend-first"
		const rotated = "https://barbican.example/v1/secrets/xbackend-rotated"
		clientIdentity := first
		r.XBackendClientCertificates = xBackendCertificateResolverFunc(func(_ context.Context, class gatewayv1.ObjectName, observed *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string) {
			require.Equal(t, f.gateway.Spec.GatewayClassName, class)
			require.Equal(t, backend.Spec.TLS.ClientCertificateRef, observed.Spec.TLS.ClientCertificateRef)
			if clientIdentity == "" {
				return "", gatewayv1.RouteReasonRefNotPermitted, "The XBackend certificate grant was revoked"
			}
			return clientIdentity, "", ""
		})
		firstObservation := f.observe(t)
		enrolled := firstObservation.Nodes[0].Spec.HTTPBackends
		require.Equal(t, service, enrolled[0])
		require.Equal(t, external.Members, enrolled[1].Members)
		require.Equal(t, first, enrolled[1].XBackendClientCertificateReference)
		checkStatus(metav1.ConditionTrue, string(gatewayv1.RouteReasonResolvedRefs))
		deferred := cloud.RouteProgressingResult("Octavia load balancer is PENDING_UPDATE", time.Second)
		deferred.ObservationDeferred = true
		active := newTestReconciler(*r, &recordingProvider{routeResult: &deferred})
		key := client.ObjectKeyFromObject(f.route)
		var live gatewayv1.HTTPRoute
		require.NoError(t, f.client.Get(t.Context(), key, &live))
		require.True(t, meta.IsStatusConditionTrue(live.Status.Parents[0].Conditions, r.Config.Domain()+"/Programmed"))
		clientIdentity = rotated
		_, err := active.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, &live))
		require.False(t, meta.IsStatusConditionTrue(live.Status.Parents[0].Conditions, r.Config.Domain()+"/Programmed"))

		clientIdentity = ""
		t.Run("withdrawal survives a neighboring Service policy list failure", func(t *testing.T) {
			controllerutil.AddFinalizer(f.gateway, r.Config.BackendCAFinalizer())
			require.NoError(t, f.client.Update(t.Context(), f.gateway))
			value := *r
			value.APIReader = unavailableServicePolicies{value.APIReader}
			provider := &xBackendWithdrawalTestProvider{}
			active := newTestReconciler(value, provider)
			request := ctrl.Request{NamespacedName: key}
			provider.beforeFence = func() { clientIdentity = first }
			_, _ = active.Reconcile(t.Context(), request)
			require.ErrorContains(t, provider.fenceErr, "withdrawal authority changed")
			require.Empty(t, provider.withdrawals)

			provider.beforeFence = nil
			clientIdentity = ""
			_, _ = active.Reconcile(t.Context(), request)
			require.NoError(t, provider.fenceErr)
			require.Equal(t, []cloud.XBackendClientWithdrawal{{Route: controller.RouteIdentity(r.Config, f.gateway, f.route), BackendName: backend.Name}}, provider.withdrawals)

			t.Run("unreadable XBackend leaves its own pool untouched", func(t *testing.T) {
				require.NoError(t, f.client.Get(t.Context(), key, f.route))
				other := addExternalHostnameRule(t, f, f.route, "zz-unreadable")
				require.NoError(t, f.client.Update(t.Context(), f.route))
				defer func() {
					require.NoError(t, f.client.Get(t.Context(), key, f.route))
					f.route.Spec.Rules = f.route.Spec.Rules[:len(f.route.Spec.Rules)-1]
					require.NoError(t, f.client.Update(t.Context(), f.route))
				}()
				readErr := errors.New("second XBackend is temporarily unreadable")
				value.APIReader = unavailableXBackend{Reader: value.APIReader, key: client.ObjectKeyFromObject(other), err: readErr}
				provider := &xBackendWithdrawalTestProvider{beforeFence: func() { clientIdentity = first }}
				active := newTestReconciler(value, provider)
				_, err := active.Reconcile(t.Context(), request)
				require.ErrorIs(t, err, readErr)
				require.ErrorContains(t, provider.fenceErr, "withdrawal authority changed")
				require.Empty(t, provider.withdrawals)

				provider.beforeFence = nil
				clientIdentity = ""
				_, err = active.Reconcile(t.Context(), request)
				require.ErrorIs(t, err, readErr)
				require.NoError(t, provider.fenceErr)
				require.Equal(t, []cloud.XBackendClientWithdrawal{{Route: controller.RouteIdentity(r.Config, f.gateway, f.route), BackendName: backend.Name}}, provider.withdrawals)
			})
		})
		require.ErrorIs(t, firstObservation.Validate(t.Context()), errHTTPRouteChanged)
		withdrawn := f.observe(t).Nodes[0].Spec.HTTPBackends
		require.Equal(t, service, withdrawn[0])
		require.True(t, withdrawn[1].BackendTLS.Denied)
		require.Empty(t, withdrawn[1].Members)
		require.Empty(t, withdrawn[1].XBackendClientCertificateReference)
		checkStatus(metav1.ConditionFalse, string(gatewayv1.RouteReasonRefNotPermitted))

		clientIdentity = rotated
		require.ErrorIs(t, firstObservation.Validate(t.Context()), errHTTPRouteChanged)
		restored := f.observe(t).Nodes[0].Spec.HTTPBackends
		require.Equal(t, service, restored[0])
		require.Equal(t, rotated, restored[1].XBackendClientCertificateReference)
		require.Equal(t, external.Members, restored[1].Members)

		f.create(t, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: backend.Namespace, Name: "mutual-tls-origin", Labels: map[string]string{"app": "mutual-tls"}},
			Status:     corev1.PodStatus{PodIP: "192.0.2.39", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		})
		backend.Spec.Type, backend.Spec.ExternalHostname = gatewayx.BackendTypeEndpointSelector, nil
		backend.Spec.EndpointSelector = &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
			MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "mutual-tls"},
		}}
		require.NoError(t, f.client.Update(t.Context(), backend))
		selected := f.observe(t).Nodes[0].Spec.HTTPBackends
		require.Equal(t, service, selected[0])
		require.False(t, selected[1].RoutedExternal)
		require.Equal(t, rotated, selected[1].XBackendClientCertificateReference)
		require.Equal(t, []cloud.Member{{Address: "192.0.2.39", Port: int(backend.Spec.Port.Number)}}, selected[1].Members)
	})

	// The native bundle reader is initialized but has not yet discovered
	// the API. Inline trust remains pending until discovery can complete.
	r.Config.ClusterTrustBundles, err = backendtls.NewClusterTrustBundles(&rest.Config{Host: "http://127.0.0.1"}, nil)
	require.NoError(t, err)
	backend.Spec.TLS.Validation.CACertificateRefs = []gatewayv1.LocalObjectReference{{
		Group: "certificates.k8s.io", Kind: "ClusterTrustBundle", Name: "external-bundle",
	}}
	require.NoError(t, f.client.Update(t.Context(), backend))
	checkStatus(metav1.ConditionUnknown, string(gatewayv1.RouteReasonPending))
}

type xBackendCertificateResolverFunc func(context.Context, gatewayv1.ObjectName, *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string)

type unavailableServicePolicies struct{ client.Reader }

func (r unavailableServicePolicies) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*gatewayv1.BackendTLSPolicyList); ok {
		return errors.New("Service policy inventory unavailable")
	}
	return r.Reader.List(ctx, list, opts...)
}

type unavailableXBackend struct {
	client.Reader
	key client.ObjectKey
	err error
}

func (r unavailableXBackend) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*gatewayx.XBackend); ok && key == r.key {
		return r.err
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

type xBackendWithdrawalTestProvider struct {
	recordingProvider
	beforeFence func()
	fenceErr    error
	withdrawals []cloud.XBackendClientWithdrawal
}

func (p *xBackendWithdrawalTestProvider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	if spec.Gateway.XBackendClientWithdrawals == nil {
		return p.recordingProvider.ReconcileGatewayGraph(ctx, spec)
	}
	if p.beforeFence != nil {
		p.beforeFence()
	}
	p.fenceErr = spec.ValidateMutation(ctx)
	if p.fenceErr != nil {
		return cloud.GatewayGraphResult{}, p.fenceErr
	}
	p.withdrawals = spec.Gateway.XBackendClientWithdrawals
	return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
}

func (f xBackendCertificateResolverFunc) ResolveXBackendClientCertificate(ctx context.Context, class gatewayv1.ObjectName, backend *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string) {
	return f(ctx, class, backend)
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
