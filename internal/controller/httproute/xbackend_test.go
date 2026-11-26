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
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/l4route"
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

func TestXBackendExternalHostnameGRPCRequiresH2CProviderSupport(t *testing.T) {
	f := newExternalHostnameFixture(t)
	r := f.reconciler
	r.Config.BackendH2C = true
	backend := addExternalHostnameRule(t, f, f.route, "grpc-origin")
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolGRPC)
	backend.Spec.TLS = &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeNone}
	require.NoError(t, f.client.Update(t.Context(), backend))
	require.NoError(t, f.client.Update(t.Context(), f.route))

	lookups := 0
	r.ExternalHostnames.lookup = func(_ context.Context, _, _ string) ([]netip.Addr, error) {
		lookups++
		return []netip.Addr{netip.MustParseAddr("198.51.100.15"), netip.MustParseAddr("192.0.2.200")}, nil
	}
	var sibling cloud.HTTPBackend
	for _, tc := range []struct {
		name      string
		available bool
		err       error
		status    metav1.ConditionStatus
		reason    string
		lookups   int
	}{
		{"available", true, nil, metav1.ConditionTrue, "", 1},
		{"discovery failed", false, errors.New("Octavia capability read failed"), metav1.ConditionUnknown, string(gatewayv1.RouteReasonPending), 1},
		{"unsupported", false, nil, metav1.ConditionFalse, string(gatewayv1.RouteReasonUnsupportedProtocol), 1},
		{"restored", true, nil, metav1.ConditionTrue, "", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.BackendH2CAvailable = func(context.Context) (bool, error) { return tc.available, tc.err }
			targets := f.observe(t).Nodes[0].Spec.HTTPBackends
			require.Len(t, targets, 2)
			if sibling.Name == "" {
				sibling = targets[0]
			}
			require.Equal(t, sibling, targets[0])
			origin := targets[1]
			require.Equal(t, tc.available || tc.err != nil, origin.BackendH2C)
			require.Equal(t, tc.err != nil, origin.BackendH2CPending)
			require.Nil(t, origin.BackendTLS)
			require.True(t, origin.XBackendTCPHealthMonitor)
			require.False(t, targets[0].XBackendTCPHealthMonitor)
			require.Equal(t, tc.available || tc.err != nil, origin.RoutedExternal)
			if tc.available || tc.err != nil {
				require.Equal(t, []cloud.Member{{Address: "198.51.100.15", Port: int(backend.Spec.Port.Number)}}, origin.Members)
			} else {
				require.Empty(t, origin.Members)
			}
			require.Equal(t, tc.lookups, lookups)

			routeStatus := pendingRouteStatus("Waiting for test listener")
			require.NoError(t, r.resolveRouteReferences(t.Context(), f.route, f.gateway, &routeStatus))
			require.Equal(t, tc.status, routeStatus.resolved)
			ancestors, _, err := (&xBackendStatusReconciler{routes: r}).observeAncestors(t.Context(), client.ObjectKeyFromObject(backend), backend)
			require.NoError(t, err)
			require.Len(t, ancestors, 1)
			ancestor := ancestors[client.ObjectKeyFromObject(f.gateway)]
			require.Equal(t, tc.status, ancestor.Status)
			require.Equal(t, backend.Generation, ancestor.ObservedGeneration)
			if tc.reason != "" {
				require.Equal(t, tc.reason, routeStatus.resolvedReason)
				require.Equal(t, tc.reason, ancestor.Reason)
			}
		})
	}
}

func TestXBackendTransportChangeInvalidatesDeferredParentStatus(t *testing.T) {
	f := newExternalHostnameFixture(t)
	f.reconciler.Config.BackendH2C = true
	f.reconciler.BackendH2CAvailable = func(context.Context) (bool, error) { return true, nil }
	f.reconciler.ExternalHostnames.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("198.51.100.15")}, nil
	}
	backend := addExternalHostnameRule(t, f, f.route, "transport-origin")
	require.NoError(t, f.client.Update(t.Context(), f.route))
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key, generation := client.ObjectKeyFromObject(f.route), f.route.Generation
	check := func(programmed bool) {
		t.Helper()
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, f.route))
		require.Equal(t, generation, f.route.Generation)
		require.Len(t, f.route.Status.Parents, 1)
		conditions := f.route.Status.Parents[0].Conditions
		require.Equal(t, programmed, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed"))
		proof := meta.FindStatusCondition(conditions, r.Config.Domain()+xBackendTransportRevisionCondition)
		if programmed {
			require.NotNil(t, proof)
		} else {
			require.Nil(t, proof)
		}
	}
	check(true)
	deferred := cloud.RouteProgressingResult("Octavia load balancer is PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	provider.routeResult = &deferred
	check(true)
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolH2C)
	require.NoError(t, f.client.Update(t.Context(), backend))
	check(false)
	provider.routeResult = nil
	check(true)
	provider.routeResult = &deferred
	check(true)
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

func TestXBackendApplicationHeaderPersistence(t *testing.T) {
	f := newExternalHostnameFixture(t)
	r := f.reconciler
	r.Config.XBackendSessionPersistence, r.Config.BackendH2C, r.Config.BackendTLSPolicy = true, true, true
	r.XBackendHeaderAvailable = func(context.Context) (bool, error) { return true, nil }
	r.BackendH2CAvailable = func(context.Context) (bool, error) { return true, nil }
	r.BackendTLSSystemCAAvailable = func(context.Context, bool, bool, bool) (bool, error) { return true, nil }
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.route.Namespace, Name: f.service.Name, UID: "app-protocol", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 8080},
			EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
				MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "direct"},
			}},
			SessionPersistence: &gatewayv1.SessionPersistence{
				Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{},
			},
		},
	}
	f.create(t, backend)
	ref := &f.route.Spec.Rules[0].BackendRefs[0]
	ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind(model.XBackendKind)), nil
	require.NoError(t, f.client.Update(t.Context(), f.route))

	for _, test := range []struct {
		name     string
		protocol gatewayx.BackendProtocol
		tls      *gatewayx.BackendTLS
		blocked  gatewayv1.HeaderName
	}{
		{name: "MCP", protocol: gatewayx.BackendProtocolMCP, blocked: "mCp-SeSsIoN-iD"},
		{name: "gRPC over h2c", protocol: gatewayx.BackendProtocolGRPC, blocked: "X-Session-BIN"},
		{name: "gRPC over TLS", protocol: gatewayx.BackendProtocolGRPC, blocked: "x-session-bin", tls: &gatewayx.BackendTLS{
			Mode: gatewayx.BackendTLSModeServerOnly, Validation: &gatewayv1.BackendTLSPolicyValidation{
				Hostname: "backend.example.test", WellKnownCACertificates: ptr.To(gatewayv1.WellKnownCACertificatesSystem),
			},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend.Spec.Protocol, backend.Spec.TLS = ptr.To(test.protocol), test.tls
			for _, header := range []gatewayv1.HeaderName{test.blocked, "X-Backend-Affinity"} {
				backend.Spec.SessionPersistence.Header.Name = header
				require.NoError(t, f.client.Update(t.Context(), backend))

				status := programmedRouteStatus()
				require.NoError(t, r.resolveRouteReferences(t.Context(), f.route, f.gateway, &status))
				ancestors, _, err := (&xBackendStatusReconciler{routes: r}).observeAncestors(t.Context(), client.ObjectKeyFromObject(backend), backend)
				require.NoError(t, err)
				ancestor := ancestors[client.ObjectKeyFromObject(f.gateway)]
				backends := f.observe(t).Nodes[0].Spec.HTTPBackends
				require.Len(t, backends, 1)
				if header == test.blocked {
					require.Equal(t, metav1.ConditionFalse, status.resolved)
					require.Equal(t, string(gatewayv1.RouteReasonUnsupportedValue), status.resolvedReason)
					require.Equal(t, metav1.ConditionFalse, ancestor.Status)
					require.Equal(t, status.resolvedReason, ancestor.Reason)
					require.Equal(t, status.resolvedMessage, ancestor.Message)
					require.Nil(t, backends[0].SessionHeader)
				} else {
					require.Equal(t, metav1.ConditionTrue, status.resolved)
					require.Equal(t, metav1.ConditionTrue, ancestor.Status)
					require.Equal(t, &cloud.HTTPSessionHeaderPersistence{Name: "x-backend-affinity"}, backends[0].SessionHeader)
				}
			}
		})
	}
}

func TestXBackendAndSameNamedServiceKeepSeparateWeightedTargets(t *testing.T) {
	t.Run("Cookie", func(t *testing.T) {
		testWeightedXBackendPersistence(t, &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{
			Name: ptr.To(gatewayv1.CookieName("reports_session")), Path: ptr.To("/"),
		}}, nil)
	})
	t.Run("Cookie over h2c", func(t *testing.T) {
		testWeightedXBackendPersistence(t, &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{
			Name: ptr.To(gatewayv1.CookieName("reports_session")), Path: ptr.To("/"),
		}}, ptr.To(gatewayx.BackendProtocolH2C))
	})
	t.Run("Header", func(t *testing.T) {
		testWeightedXBackendPersistence(t, &gatewayv1.SessionPersistence{
			Type: ptr.To(gatewayv1.HeaderBasedSessionPersistence), Header: &gatewayv1.HeaderConfig{Name: "X-Reports-Session"},
			AbsoluteTimeout: ptr.To(gatewayv1.Duration("2s")),
		}, nil)
	})
}

func testWeightedXBackendPersistence(t *testing.T, persistence *gatewayv1.SessionPersistence, protocol *gatewayx.BackendProtocol) {
	t.Helper()
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.GatewayAPIChannel = "experimental"
	f.reconciler.Config.XBackendSessionPersistence = true
	h2c := ptr.Deref(protocol, "") == gatewayx.BackendProtocolH2C
	f.reconciler.Config.BackendH2C = h2c
	f.reconciler.BackendH2CAvailable = func(context.Context) (bool, error) { return true, nil }
	f.reconciler.Config.FixedHTTPResponses, f.reconciler.Config.WeightedHTTPTargets = true, true
	if err := gatewayx.Install(f.client.Scheme()); err != nil {
		t.Fatal(err)
	}
	backend := &gatewayx.XBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: f.service.Name, UID: "xbackend-uid", Generation: 1},
		Spec: gatewayx.BackendSpec{
			Type:               gatewayx.BackendTypeEndpointSelector,
			Port:               gatewayx.BackendPort{Number: gatewayx.PortNumber(f.service.Spec.Ports[0].Port)},
			SessionPersistence: persistence,
			Protocol:           protocol,
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
		spec.HTTPBackends[0].SessionCookieName != "" || spec.HTTPBackends[0].SessionHeader != nil || spec.HTTPBackends[1].SessionCookiePath != nil ||
		identity.HTTPBackendPoolKey(spec.HTTPBackends[0]) == identity.HTTPBackendPoolKey(spec.HTTPBackends[1]) ||
		!reflect.DeepEqual(spec.HTTPBackends[1].Members, []cloud.Member{{Address: pod.Status.PodIP, Port: int(backend.Spec.Port.Number)}}) {
		t.Fatalf("expected two distinct pools, got %+v", spec.HTTPBackends)
	}
	require.Equal(t, h2c, spec.HTTPBackends[1].BackendH2C)
	if model.IsHeaderSessionPersistence(*persistence) {
		require.Empty(t, spec.HTTPBackends[1].SessionCookieName)
		require.Equal(t, &cloud.HTTPSessionHeaderPersistence{Name: "x-reports-session", TimeoutMS: ptr.To(int64(2000))}, spec.HTTPBackends[1].SessionHeader)
		for _, condition := range []struct {
			available bool
			err       error
			want      metav1.ConditionStatus
		}{
			{false, nil, metav1.ConditionFalse},
			{false, context.DeadlineExceeded, metav1.ConditionUnknown},
			{true, nil, metav1.ConditionTrue},
		} {
			f.reconciler.XBackendHeaderAvailable = func(context.Context) (bool, error) { return condition.available, condition.err }
			status := programmedRouteStatus()
			require.NoError(t, f.reconciler.resolveRouteReferences(t.Context(), f.route, f.gateway, &status))
			require.Equal(t, condition.want, status.resolved)
		}
	} else {
		require.Equal(t, "reports_session", spec.HTTPBackends[1].SessionCookieName)
		require.Nil(t, spec.HTTPBackends[1].SessionHeader)
	}
	want := []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {BackendIndex: ptr.To(1), Weight: 7}}
	if len(spec.WeightedHTTPRules) != 1 || !reflect.DeepEqual(spec.WeightedHTTPRules[0].Targets, want) {
		t.Fatalf("expected weighted targets %+v, got %+v", want, spec.WeightedHTTPRules)
	}
	if h2c && !model.IsHeaderSessionPersistence(*persistence) {
		testXBackendCookieH2CCapabilities(t, f)
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

func testXBackendCookieH2CCapabilities(t *testing.T, f *routeModelInputFixture) {
	t.Helper()
	r := f.reconciler
	for _, phase := range []struct {
		name   string
		h2c    bool
		cookie bool
		reason gatewayv1.RouteConditionReason
	}{
		{"h2c unavailable", false, true, gatewayv1.RouteReasonUnsupportedProtocol},
		{"cookie unavailable", true, false, gatewayv1.RouteReasonUnsupportedValue},
		{"both available", true, true, gatewayv1.RouteReasonResolvedRefs},
	} {
		t.Run(phase.name, func(t *testing.T) {
			r.BackendH2CAvailable = func(context.Context) (bool, error) { return phase.h2c, nil }
			r.XBackendCookieAvailable = func(_ context.Context, requirements cloud.HTTPSessionCookieRequirements) (bool, error) {
				require.Equal(t, cloud.HTTPSessionCookieNamed, requirements)
				return phase.cookie, nil
			}
			want := metav1.ConditionTrue
			if !phase.h2c || !phase.cookie {
				want = metav1.ConditionFalse
			}
			status := programmedRouteStatus()
			require.NoError(t, r.resolveRouteReferences(t.Context(), f.route, f.gateway, &status))
			require.Equal(t, want, status.resolved)
			require.Equal(t, string(phase.reason), status.resolvedReason)
		})
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
			Protocol: ptr.To(gatewayx.BackendProtocolHTTP11),
			SessionPersistence: &gatewayv1.SessionPersistence{
				Cookie:          &gatewayv1.CookieConfig{LifetimeType: ptr.To(gatewayv1.PermanentCookieLifetimeType), Path: ptr.To("/reports")},
				AbsoluteTimeout: ptr.To(gatewayv1.Duration("1s500ms")),
			},
		},
	}
	other := backend.DeepCopy()
	other.Name, other.UID = "other", "other-opaque-uid"
	other.Spec.SessionPersistence.AbsoluteTimeout = ptr.To(gatewayv1.Duration("0s"))
	other.Spec.SessionPersistence.Cookie.LifetimeType = nil
	other.Spec.SessionPersistence.Cookie.Path = ptr.To("/")
	other.Spec.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("other_session"))
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
				require.Equal(t, ptr.To("/reports"), pool.SessionCookiePath)
				require.Equal(t, &cloud.HTTPSessionCookieDeadline{
					TimeoutMS: 1500, Scope: fmt.Sprintf("%x", sha256.Sum256([]byte(backend.UID))), Permanent: true,
				}, pool.SessionCookieDeadline)
			} else {
				otherName = pool.SessionCookieName
				require.Nil(t, pool.SessionCookiePath)
				require.Equal(t, &cloud.HTTPSessionCookieDeadline{
					TimeoutMS: 0, Scope: fmt.Sprintf("%x", sha256.Sum256([]byte(other.UID))),
				}, pool.SessionCookieDeadline)
			}
		}
	}
	require.Len(t, cookieNames, 2)
	require.Equal(t, fmt.Sprintf("xbackend-%x", sha256.Sum256([]byte(backend.UID))), cookieNames[0])
	require.Equal(t, cookieNames[0], cookieNames[1])
	require.NotEmpty(t, otherName)
	require.Equal(t, "other_session", otherName)
	require.NotEqual(t, cookieNames[0], otherName)

	for _, phase := range []struct {
		issue    cloud.SessionPersistenceIssue
		accepted metav1.ConditionStatus
		resolved metav1.ConditionStatus
		reason   string
		err      error
	}{
		{cloud.SessionPersistencePathUnsupported, metav1.ConditionFalse, metav1.ConditionFalse, "UnsupportedValue", nil},
		{cloud.SessionPersistencePathUnverified, metav1.ConditionUnknown, metav1.ConditionUnknown, "Pending", errors.New("API timed out")},
		{cloud.SessionPersistencePathReadbackUnverified, metav1.ConditionTrue, metav1.ConditionUnknown, "Pending", nil},
		{cloud.SessionPersistenceAvailable, metav1.ConditionTrue, metav1.ConditionTrue, "", nil},
	} {
		f.reconciler.XBackendCookieAvailable = func(_ context.Context, requirements cloud.HTTPSessionCookieRequirements) (bool, error) {
			require.Equal(t, cloud.HTTPSessionCookiePermanent|cloud.HTTPSessionCookiePath, requirements)
			return phase.accepted == metav1.ConditionTrue, phase.err
		}
		backendKey := client.ObjectKeyFromObject(backend)
		_, err := (&xBackendStatusReconciler{routes: f.reconciler}).Reconcile(t.Context(), ctrl.Request{NamespacedName: backendKey})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), backendKey, backend))
		require.Len(t, backend.Status.Ancestors, 1)
		condition := meta.FindStatusCondition(backend.Status.Ancestors[0].Conditions, "Accepted")
		require.NotNil(t, condition)
		require.Equal(t, backend.Generation, condition.ObservedGeneration)
		require.Equal(t, phase.accepted, condition.Status)
		provider := &recordingProvider{}
		if phase.issue != cloud.SessionPersistenceAvailable {
			result := cloud.RouteProgressingResult("Waiting for Octavia cookie support", time.Second)
			result.BackendSessionPersistenceIssue = phase.issue
			provider.routeResult = &result
			if phase.accepted != metav1.ConditionTrue {
				require.Equal(t, phase.reason, condition.Reason)
			}
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
		require.Equal(t, phase.resolved, resolved.Status)
		require.Equal(t, phase.issue == cloud.SessionPersistenceAvailable,
			meta.IsStatusConditionTrue(f.route.Status.Parents[0].Conditions, r.Config.Domain()+"/Programmed"))
		if phase.reason != "" {
			require.Equal(t, phase.reason, resolved.Reason)
		}
	}

	provider := &xBackendCookieStatusTestProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	reconcile := func(route *gatewayv1.HTTPRoute, programmed bool) []metav1.Condition {
		t.Helper()
		key, generation := client.ObjectKeyFromObject(route), route.Generation
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, f.client.Get(t.Context(), key, route))
		require.Equal(t, generation, route.Generation)
		require.Len(t, route.Status.Parents, 1)
		conditions := route.Status.Parents[0].Conditions
		require.Equal(t, programmed, meta.IsStatusConditionTrue(conditions, r.Config.Domain()+"/Programmed"))
		return conditions
	}
	reconcile(f.route, true)
	reconcile(sibling, true)
	deferred := cloud.RouteProgressingResult("Octavia load balancer is PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	deferred.BackendSessionPersistenceIssue = cloud.SessionPersistenceNotObserved
	provider.routeResult = &deferred
	reconcile(f.route, true)
	reconcile(sibling, true)

	// A Kubernetes spec change can arrive while the provider is still busy,
	// even after the controller assembled the graph from the previous revision.
	provider.beforeGraph = func() {
		backend.Generation++
		backend.Spec.SessionPersistence.Cookie.Path = ptr.To("/")
		require.NoError(t, f.client.Update(t.Context(), backend))
	}
	for _, route := range []*gatewayv1.HTTPRoute{f.route, sibling} {
		conditions := reconcile(route, false)
		require.True(t, meta.IsStatusConditionPresentAndEqual(conditions, string(gatewayv1.RouteConditionResolvedRefs), metav1.ConditionUnknown))
	}
	provider.routeSpecs = nil
	provider.routeResult = nil
	reconcile(f.route, true)
	reconcile(sibling, true)
	require.NotEmpty(t, provider.routeSpecs)
	for _, spec := range provider.routeSpecs {
		for _, pool := range spec.HTTPBackends {
			require.Nil(t, pool.SessionCookiePath)
		}
	}

	// An unrelated busy update can now reuse the newly confirmed default.
	provider.routeResult = &deferred
	reconcile(f.route, true)
	reconcile(sibling, true)
}

type xBackendCookieStatusTestProvider struct {
	recordingProvider
	beforeGraph func()
}

func (p *xBackendCookieStatusTestProvider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	if hook := p.beforeGraph; hook != nil {
		p.beforeGraph = nil
		hook()
	}
	return p.recordingProvider.ReconcileGatewayGraph(ctx, spec)
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
	timed := backend.DeepCopy()
	timed.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("reports_session"))
	timed.SessionPersistence.AbsoluteTimeout = ptr.To(gatewayv1.Duration("99999h99999h99999h99999h"))
	require.Nil(t, model.ValidateXBackend(*timed, profile))
	permanent := backend.DeepCopy()
	permanent.SessionPersistence.Cookie.LifetimeType = ptr.To(gatewayv1.PermanentCookieLifetimeType)
	permanent.SessionPersistence.AbsoluteTimeout = ptr.To(gatewayv1.Duration("0s"))
	require.Nil(t, model.ValidateXBackend(*permanent, profile))
	for _, cookie := range []struct {
		spec  *gatewayx.BackendSpec
		path  string
		extra cloud.HTTPSessionCookieRequirements
	}{
		{&backend, "/", cloud.HTTPSessionCookieNamed},
		{&backend, "/reports", cloud.HTTPSessionCookiePath},
		{timed, "", cloud.HTTPSessionCookieTimed | cloud.HTTPSessionCookiePath},
		{permanent, strings.Repeat("~", 1024), cloud.HTTPSessionCookiePermanent | cloud.HTTPSessionCookiePath},
	} {
		spec := cookie.spec.DeepCopy()
		spec.SessionPersistence.Cookie.Path = ptr.To(cookie.path)
		require.Nil(t, model.ValidateXBackend(*spec, profile))
		require.Equal(t, cookie.extra, model.XBackendCookieRequirements(*spec.SessionPersistence))
	}
	missingUID := &gatewayx.XBackend{Spec: *timed}
	require.Equal(t, metav1.ConditionFalse, xBackendAcceptedCondition(missingUID, profile).Status)
	observation := model.XBackendObservation{Spec: *timed}
	require.Contains(t, model.CheckXBackendObservation(types.NamespacedName{}, &observation, profile).Message, "UID")
	observation.Spec.SessionPersistence.AbsoluteTimeout = nil
	require.Nil(t, model.CheckXBackendObservation(types.NamespacedName{}, &observation, profile))
	authenticated := backend.DeepCopy()
	authenticated.Protocol = ptr.To(gatewayx.BackendProtocolHTTP2)
	authenticated.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("reports_session"))
	authenticated.TLS = &gatewayx.BackendTLS{
		Mode: gatewayx.BackendTLSModeServerOnly, Validation: &gatewayv1.BackendTLSPolicyValidation{Hostname: "reports.example.test"},
	}
	authenticatedProfile := profile
	authenticatedProfile.ServerTLS, authenticatedProfile.ClientTLS = true, true
	require.Nil(t, model.ValidateXBackend(*authenticated, authenticatedProfile))
	authenticated.TLS.Mode = gatewayx.BackendTLSModeClientAndServer
	authenticated.TLS.ClientCertificateRef = &gatewayv1.SecretObjectReference{Name: "reports-client"}
	require.Nil(t, model.ValidateXBackend(*authenticated, authenticatedProfile))
	for _, tc := range []struct {
		name string
		edit func(*gatewayx.BackendSpec, *model.XBackendProfile)
	}{
		{"cookie disabled", func(b *gatewayx.BackendSpec, p *model.XBackendProfile) {
			b.Protocol = ptr.To(gatewayx.BackendProtocolH2C)
			p.SessionPersistence = false
		}},
		{"header", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Type = ptr.To(gatewayv1.HeaderBasedSessionPersistence)
		}},
		{"invalid absolute timeout", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.AbsoluteTimeout = ptr.To(gatewayv1.Duration("0.5s"))
		}},
		{"permanent without absolute timeout", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.LifetimeType = ptr.To(gatewayv1.PermanentCookieLifetimeType)
		}},
		{"unknown cookie lifetime", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.LifetimeType = ptr.To(gatewayv1.CookieLifetimeType("Unknown"))
		}},
		{"path with cookie attribute separator", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Path = ptr.To("/reports;admin")
		}},
		{"path with non-ASCII byte", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Path = ptr.To("/café")
		}},
		{"path with control character", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Path = ptr.To("/line\n")
		}},
		{"path exceeding the provider limit", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Path = ptr.To(strings.Repeat("x", 1025))
		}},
		{"reserved cookie name", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("__HoSt-reports"))
		}},
		{"unsafe cookie name", func(b *gatewayx.BackendSpec, _ *model.XBackendProfile) {
			b.SessionPersistence.Cookie.Name = ptr.To(gatewayv1.CookieName("invalid;name"))
		}},
		{"h2c disabled", func(b *gatewayx.BackendSpec, p *model.XBackendProfile) {
			b.Protocol = ptr.To(gatewayx.BackendProtocolH2C)
			p.H2C = false
		}},
		{"HTTP2 without TLS", func(b *gatewayx.BackendSpec, p *model.XBackendProfile) {
			b.Protocol = ptr.To(gatewayx.BackendProtocolHTTP2)
			p.ServerTLS = true
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
	requireHTTP2, h2Available := false, false
	r.XBackendTLSAvailable = func(_ context.Context, requireH2, requireSAN bool) (bool, error) {
		require.True(t, requireSAN)
		require.Equal(t, requireHTTP2, requireH2)
		return sanAvailable && (!requireH2 || h2Available), sanErr
	}
	backend := addExternalHostnameRule(t, f, f.route, f.service.Name)
	const identityName = "identity.remote.example.test"
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolWSS)
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
	require.False(t, external.BackendTLS.RequireH2)
	require.True(t, external.XBackendTCPHealthMonitor)
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

	// Changing only the XBackend selects the h2 capability and forces a new
	// observation even when the HTTPRoute and pool identity are unchanged.
	requireHTTP2 = true
	backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolGRPC)
	require.NoError(t, f.client.Update(t.Context(), backend))
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)
	targets = f.observe(t).Nodes[0].Spec.HTTPBackends
	require.Equal(t, service, targets[0])
	require.True(t, targets[1].BackendTLS.Denied)
	require.Empty(t, targets[1].Members)
	checkStatus(metav1.ConditionFalse, "UnsupportedProvider")

	h2Available = true
	routeSpec := f.observe(t).Nodes[0].Spec
	targets = routeSpec.HTTPBackends
	require.Equal(t, identity.HTTPBackendPoolKey(external), identity.HTTPBackendPoolKey(targets[1]))
	require.True(t, targets[1].BackendTLS.RequireH2)
	require.False(t, targets[1].BackendH2C)
	require.False(t, routeSpec.GRPC)
	require.Equal(t, identityName, targets[1].BackendTLS.Hostname)
	require.Equal(t, external.Members, targets[1].Members)
	checkStatus(metav1.ConditionTrue, string(gatewayv1.RouteReasonResolvedRefs))

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
		require.True(t, enrolled[1].BackendTLS.RequireH2)
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

			t.Run("rotation observes the authorized certificate during the list failure", func(t *testing.T) {
				clientIdentity = rotated
				defer func() { clientIdentity = "" }()
				provider.withdrawals = nil
				_, err := active.Reconcile(t.Context(), request)
				require.Error(t, err)
				require.NoError(t, provider.fenceErr)
				require.Equal(t, []cloud.XBackendClientWithdrawal{{Route: controller.RouteIdentity(r.Config, f.gateway, f.route),
					BackendName: backend.Name, AuthorizedReference: rotated}}, provider.withdrawals)

				provider.withdrawals = nil
				provider.beforeFence = func() { clientIdentity = first }
				defer func() { provider.beforeFence = nil }()
				_, _ = active.Reconcile(t.Context(), request)
				require.ErrorContains(t, provider.fenceErr, "withdrawal authority changed")
				require.Empty(t, provider.withdrawals)
			})

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
		require.True(t, selected[1].BackendTLS.RequireH2)
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

func TestXBackendStatusIsolatesInvalidGatewayTopology(t *testing.T) {
	for _, invalid := range []string{"tcp", "http"} {
		t.Run(invalid, func(t *testing.T) {
			ctx := t.Context()
			cfg := testConfig()
			cfg.GatewayAPIChannel = "experimental"
			class, httpGateway, httpRoute, _, _ := endpointRouteFixture(t)
			tcpGateway := httpGateway.DeepCopy()
			tcpGateway.Name, tcpGateway.UID = "tcp-edge", "tcp-gateway-uid"
			tcpGateway.Spec.Listeners = []gatewayv1.Listener{{Name: "tcp", Protocol: gatewayv1.TCPProtocolType, Port: 5432}}
			tcpGateway.Annotations[cfg.GatewayListenerPortAnnotation()] = "5432"
			tcpGateway.Annotations[cfg.GatewayListenerProtocolAnnotation()] = string(gatewayv1.TCPProtocolType)
			ref := &httpRoute.Spec.Rules[0].BackendRefs[0]
			ref.Group, ref.Kind, ref.Port = ptr.To(gatewayv1.Group(gatewayx.GroupName)), ptr.To(gatewayv1.Kind("XBackend")), nil
			tcpRoute := &gatewayv1.TCPRoute{
				ObjectMeta: metav1.ObjectMeta{Namespace: httpRoute.Namespace, Name: "tcp-route"},
				Spec: gatewayv1.TCPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(tcpGateway.Name)}}},
					Rules:           []gatewayv1.TCPRouteRule{{BackendRefs: []gatewayv1.BackendRef{ref.BackendRef}}},
				},
			}
			backend := &gatewayx.XBackend{
				ObjectMeta: metav1.ObjectMeta{Namespace: httpRoute.Namespace, Name: string(ref.Name), Generation: 1},
				Spec: gatewayx.BackendSpec{
					Type: gatewayx.BackendTypeEndpointSelector, Port: gatewayx.BackendPort{Number: 5432},
					EndpointSelector: &gatewayx.EndpointSelectorBackend{LabelSelector: gatewayx.LabelSelector{
						MatchLabels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{"app": "backend"},
					}},
				},
			}
			scheme := testScheme(t)
			require.NoError(t, gatewayx.Install(scheme))
			kube := indexedFakeClientBuilder(scheme, cfg).WithStatusSubresource(backend).
				WithObjects(class, httpGateway, httpRoute, tcpGateway, tcpRoute, backend).Build()
			status := &xBackendStatusReconciler{routes: &Reconciler{
				Client: kube, APIReader: kube, Config: cfg,
				TCPRoutes: &l4route.Reconciler{Client: kube, APIReader: kube, Config: cfg},
			}}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(backend)}
			conditions := func() map[gatewayv1.ObjectName]metav1.Condition {
				t.Helper()
				require.NoError(t, kube.Get(ctx, request.NamespacedName, backend))
				require.Len(t, backend.Status.Ancestors, 2)
				byGateway := make(map[gatewayv1.ObjectName]metav1.Condition)
				for _, ancestor := range backend.Status.Ancestors {
					require.Equal(t, cfg.ControllerName, ancestor.ControllerName)
					condition := meta.FindStatusCondition(ancestor.Conditions, string(gatewayx.BackendConditionAccepted))
					require.NotNil(t, condition)
					require.Equal(t, backend.Generation, condition.ObservedGeneration, "Gateway %s", ancestor.AncestorRef.Name)
					byGateway[ancestor.AncestorRef.Name] = *condition
				}
				return byGateway
			}

			_, err := status.Reconcile(ctx, request)
			require.NoError(t, err)
			bothAccepted := conditions()
			httpKey, tcpKey := gatewayv1.ObjectName(httpGateway.Name), gatewayv1.ObjectName(tcpGateway.Name)
			require.Equal(t, metav1.ConditionTrue, bothAccepted[httpKey].Status)
			require.Equal(t, metav1.ConditionTrue, bothAccepted[tcpKey].Status)

			damaged := tcpGateway
			if invalid == "http" {
				damaged = httpGateway
			}
			damagedKey := client.ObjectKeyFromObject(damaged)
			previousStatus := backend.Status.DeepCopy()
			backend.Spec.Protocol = ptr.To(gatewayx.BackendProtocolTCP)
			backend.Generation = 2
			require.NoError(t, kube.Update(ctx, backend))

			timeout := apierrors.NewTimeoutError("reading Gateway", 0)
			reader := interceptor.NewClient(kube, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					if _, isGateway := object.(*gatewayv1.Gateway); isGateway && key == damagedKey {
						return timeout
					}
					return c.Get(ctx, key, object, opts...)
				},
			})
			status.routes.APIReader, status.routes.TCPRoutes.APIReader = reader, reader
			_, err = status.Reconcile(ctx, request)
			require.ErrorIs(t, err, timeout)
			require.NoError(t, kube.Get(ctx, request.NamespacedName, backend))
			require.Equal(t, *previousStatus, backend.Status)
			status.routes.APIReader, status.routes.TCPRoutes.APIReader = kube, kube

			require.NoError(t, kube.Get(ctx, damagedKey, damaged))
			delete(damaged.Annotations, cfg.GatewayTopologyAnnotation())
			require.NoError(t, kube.Update(ctx, damaged))
			_, err = controller.StoredGatewayTopology(cfg, damaged)
			require.ErrorIs(t, err, cloud.ErrOwnershipConflict)
			_, err = status.Reconcile(ctx, request)
			require.NoError(t, err)
			current := conditions()
			if invalid == "tcp" {
				require.Equal(t, metav1.ConditionFalse, current[httpKey].Status)
				require.Equal(t, string(gatewayv1.RouteReasonUnsupportedProtocol), current[httpKey].Reason)
			} else {
				require.Equal(t, metav1.ConditionTrue, current[tcpKey].Status)
				require.Equal(t, string(gatewayx.BackendReasonAccepted), current[tcpKey].Reason)
			}
			conflict := current[gatewayv1.ObjectName(damaged.Name)]
			require.Equal(t, metav1.ConditionUnknown, conflict.Status)
			require.Equal(t, controller.ProviderReasonOwnershipConflict, conflict.Reason)
		})
	}
}
