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
	"slices"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestFixedRedirectProgramsWithoutBackendReads(t *testing.T) {
	f := newRouteModelInputFixture(t)
	rule := fixedRedirectRule("moved.example.test")
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{rule}
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	f.reader.forbid = true
	result := cloud.RouteReadyResult(cloud.RouteState{L7PolicyIDs: []string{"redirect"}})
	provider := &recordingProvider{routeResult: &result}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(provider.routeSpecs) != 1 {
		t.Fatalf("expected one redirect route to program, got %#v", provider.routeSpecs)
	}
	spec := provider.routeSpecs[0]
	if len(spec.HTTPBackends) != 0 || len(spec.Members) != 0 || spec.PathType != "" || len(spec.HTTPRedirects) != 1 {
		t.Fatalf("redirect acquired a forwarding target: %#v", spec)
	}
	redirect := spec.HTTPRedirects[0]
	if redirect.RuleIndex != 0 || redirect.Prefix != "http://moved.example.test" || redirect.StatusCode != 302 ||
		!slices.Equal(redirect.Matches, []cloud.HTTPPathMatch{{PathType: cloud.PathMatchPrefix, PathValue: "/"}}) {
		t.Fatalf("redirect = %#v", redirect)
	}
	if len(f.reader.counts) != 0 {
		t.Fatalf("redirect read backend dependencies: %v", f.reader.counts)
	}
	assertRedirectRouteStatus(t, f, metav1.ConditionTrue, string(gatewayv1.RouteReasonResolvedRefs))
}

func TestFixedRedirectDefaultsUseSelectedListener(t *testing.T) {
	f := newRouteModelInputFixture(t)
	cfg := f.reconciler.Config
	cfg.OperatorHTTPS = true
	f.reconciler.Config = cfg
	secure := gatewayv1.SectionName("secure")
	controller.SetGatewayListenerScope(cfg, f.gateway, gatewayv1.HTTPProtocolType)
	f.gateway.Spec.Listeners = append(f.gateway.Spec.Listeners, gatewayv1.Listener{
		Name: secure, Protocol: gatewayv1.HTTPSProtocolType, Port: 8443,
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "certificate"}}},
	})
	f.route.Spec.ParentRefs[0].SectionName = &secure
	for _, test := range []struct {
		name   string
		scheme *string
		port   *gatewayv1.PortNumber
		want   string
	}{
		{name: "selected listener protocol and port", want: "https://moved.example.test:8443"},
		{name: "explicit scheme well-known port", scheme: ptr.To("http"), want: "http://moved.example.test"},
		{name: "explicit well-known port", port: ptr.To(gatewayv1.PortNumber(443)), want: "https://moved.example.test"},
		{name: "explicit non-default port", scheme: ptr.To("http"), port: ptr.To(gatewayv1.PortNumber(8080)), want: "http://moved.example.test:8080"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule := fixedRedirectRule("moved.example.test")
			redirect := rule.Filters[0].RequestRedirect
			redirect.Scheme, redirect.Port = test.scheme, test.port
			f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{rule}
			spec, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(spec.ScopedListenerPorts, []int{8443}) ||
				len(spec.HTTPRedirects) != 1 || spec.HTTPRedirects[0].Prefix != test.want {
				t.Fatalf("want prefix %q on selected port, got %#v", test.want, spec)
			}
		})
	}
}

func TestMixedFixedRedirectRetainsRuleAndListenerPrecedence(t *testing.T) {
	f := newRouteModelInputFixture(t)
	host := gatewayv1.Hostname("app.example.test")
	f.route.Spec.Hostnames = []gatewayv1.Hostname{host}
	redirect := fixedRedirectRule("moved.example.test")
	redirect.Filters[0].RequestRedirect.StatusCode = ptr.To(308)
	redirect.Matches = []gatewayv1.HTTPRouteMatch{
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/old/")}},
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/gone")}},
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/old")}},
	}
	backend := pathBackendRule("/stay", gatewayv1.PathMatchExact, "missing")
	second := fixedRedirectRule("archive.example.test")
	second.Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Value: ptr.To("/archive")}}}
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{redirect, backend, second}
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	peer := f.candidate("peer")
	peer.Spec.Hostnames = []gatewayv1.Hostname{host}
	peerRule := pathBackendRule("/old/peer", gatewayv1.PathMatchExact, gatewayv1.ObjectName(f.service.Name))
	peerRule.BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	peer.Spec.Rules = []gatewayv1.HTTPRouteRule{peerRule}
	f.reconciler.applyRouteBinding(peer, f.gateway)
	f.create(t, peer)
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}); err != nil {
		t.Fatal(err)
	}
	if len(provider.routeSpecs) != 2 {
		t.Fatalf("expected both bound routes to program, got %#v", provider.routeSpecs)
	}
	spec := provider.routeSpecs[0]
	if spec.Identity.RouteUID != string(f.route.UID) || len(spec.HTTPBackends) != 1 || len(spec.HTTPRedirects) != 2 {
		t.Fatalf("mixed route = %#v", spec)
	}
	group := spec.HTTPBackends[0]
	if group.Name != "missing" || !group.Unavailable ||
		!slices.Equal(group.Matches, []cloud.HTTPPathMatch{{Hostname: string(host), PathType: cloud.PathMatchExact, PathValue: "/stay", Precedence: 1}}) {
		t.Fatalf("backend = %#v", group)
	}
	first, archive := spec.HTTPRedirects[0], spec.HTTPRedirects[1]
	if first.RuleIndex != 0 || first.Prefix != "http://moved.example.test" || first.StatusCode != 308 ||
		!slices.Equal(first.Matches, []cloud.HTTPPathMatch{
			{Hostname: string(host), PathType: cloud.PathMatchExact, PathValue: "/gone", Precedence: 0},
			{Hostname: string(host), PathType: cloud.PathMatchPrefix, PathValue: "/old", Precedence: 4},
		}) {
		t.Fatalf("first redirect = %#v", first)
	}
	if archive.RuleIndex != 2 || archive.Prefix != "http://archive.example.test" ||
		!slices.Equal(archive.Matches, []cloud.HTTPPathMatch{{Hostname: string(host), PathType: cloud.PathMatchPrefix, PathValue: "/archive", Precedence: 3}}) ||
		provider.routeSpecs[1].Precedence != 2 {
		t.Fatalf("archive = %#v, peer = %#v", archive, provider.routeSpecs[1])
	}
	assertRedirectRouteStatus(t, f, metav1.ConditionFalse, string(gatewayv1.RouteReasonBackendNotFound))
}

func assertRedirectRouteStatus(t *testing.T, f *routeModelInputFixture, resolvedStatus metav1.ConditionStatus, resolvedReason string) {
	t.Helper()
	var current gatewayv1.HTTPRoute
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(f.route), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Parents) != 1 {
		t.Fatalf("route status = %#v", current.Status)
	}
	conditions := current.Status.Parents[0].Conditions
	accepted := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionAccepted))
	resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	programmed := meta.FindStatusCondition(conditions, f.reconciler.Config.Domain()+"/Programmed")
	if accepted == nil || accepted.Status != metav1.ConditionTrue ||
		resolved == nil || resolved.Status != resolvedStatus || resolved.Reason != resolvedReason ||
		programmed == nil || programmed.Status != metav1.ConditionTrue {
		t.Fatalf("route conditions = %#v", conditions)
	}
}

func fixedRedirectRule(hostname string) gatewayv1.HTTPRouteRule {
	return gatewayv1.HTTPRouteRule{Filters: []gatewayv1.HTTPRouteFilter{{
		Type:            gatewayv1.HTTPRouteFilterRequestRedirect,
		RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{Hostname: ptr.To(gatewayv1.PreciseHostname(hostname))},
	}}}
}

func TestUnsupportedAmphoraActionsDoNotBindOrProvision(t *testing.T) {
	redirect := fixedRedirectRule("example.test").Filters[0]
	longDestination := strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 57)}, ".")
	for _, test := range []struct {
		name           string
		filters        []gatewayv1.HTTPRouteFilter
		includeBackend bool
		deniedBackend  bool
		message        string
	}{
		{name: "request headers", filters: []gatewayv1.HTTPRouteFilter{{
			Type: gatewayv1.HTTPRouteFilterRequestHeaderModifier,
			RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
				Add:    []gatewayv1.HTTPHeader{{Name: "X-Added", Value: "one"}},
				Set:    []gatewayv1.HTTPHeader{{Name: "X-Replaced", Value: "two"}},
				Remove: []string{"X-Removed"},
			},
		}}, message: "route-scoped header operations"},
		{name: "request hostname redirect", filters: []gatewayv1.HTTPRouteFilter{{
			Type:            gatewayv1.HTTPRouteFilterRequestRedirect,
			RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{Scheme: ptr.To("https")},
		}}, message: "request-hostname preservation"},
		{name: "request hostname redirect and denied backend", filters: []gatewayv1.HTTPRouteFilter{{
			Type:            gatewayv1.HTTPRouteFilterRequestRedirect,
			RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{Scheme: ptr.To("https")},
		}}, deniedBackend: true, message: "request-hostname preservation"},
		{name: "redirect path modifier", filters: []gatewayv1.HTTPRouteFilter{{
			Type:            gatewayv1.HTTPRouteFilterRequestRedirect,
			RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{Hostname: ptr.To(gatewayv1.PreciseHostname("example.test")), Path: &gatewayv1.HTTPPathModifier{Type: gatewayv1.FullPathHTTPPathModifier, ReplaceFullPath: ptr.To("/moved")}},
		}}, message: "RequestRedirect path modifiers"},
		{name: "redirect and rewrite", filters: []gatewayv1.HTTPRouteFilter{
			redirect, {Type: gatewayv1.HTTPRouteFilterURLRewrite},
		}, message: "must be the only filter"},
		{name: "redirect with backend", filters: []gatewayv1.HTTPRouteFilter{redirect}, includeBackend: true, message: "cannot be combined with backendRefs"},
		{name: "wildcard hostname", filters: fixedRedirectRule("*.example.test").Filters, message: "fixed DNS name"},
		{name: "prefix exceeds Octavia column", filters: fixedRedirectRule(longDestination).Filters, message: "prefix must fit in 255 characters"},
		{name: "unknown redirect scheme", filters: []gatewayv1.HTTPRouteFilter{{
			Type: gatewayv1.HTTPRouteFilterRequestRedirect,
			RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{
				Hostname: ptr.To(gatewayv1.PreciseHostname("example.test")), Scheme: ptr.To("custom"),
			},
		}}, message: "scheme \"custom\" is unsupported"},
		{name: "unknown redirect status", filters: []gatewayv1.HTTPRouteFilter{{
			Type: gatewayv1.HTTPRouteFilterRequestRedirect,
			RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{
				Hostname: ptr.To(gatewayv1.PreciseHostname("example.test")), StatusCode: ptr.To(304),
			},
		}}, message: "status code 304 is unsupported"},
		{name: "omitted backend", message: "HTTP 500"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			class, gateway := providerRouteParentObjects(cfg)
			route := mapperTestHTTPRoute(gateway.Namespace, "route", gatewayv1.ParentReference{Name: gatewayv1.ObjectName(gateway.Name)}, "backend")
			route.UID, route.Generation = "route-uid", 1
			route.Spec.Rules[0].Filters = test.filters
			if !test.includeBackend {
				route.Spec.Rules[0].BackendRefs = nil
			}
			if test.deniedBackend {
				backend := pathBackendRule("/private", gatewayv1.PathMatchExact, "backend")
				backend.BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("shared"))
				route.Spec.Rules = append(route.Spec.Rules, backend)
			}
			route.Annotations = map[string]string{"example.test/foreign": "preserve"}
			route.Finalizers = []string{"example.test/foreign"}
			foreignParent := gatewayv1.RouteParentStatus{
				ParentRef: gatewayv1.ParentReference{Name: "other"}, ControllerName: "example.test/foreign",
				Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted", LastTransitionTime: metav1.NewTime(time.Unix(1, 0).UTC())}},
			}
			route.Status.Parents = []gatewayv1.RouteParentStatus{foreignParent}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(route).WithObjects(class, gateway, route).Build()
			provider := &recordingProvider{}
			reader := &routeModelDependencyReader{Reader: kubeClient, counts: map[string]int{}}
			r := newTestReconciler(Reconciler{Client: kubeClient, APIReader: reader, Config: cfg, Coordinator: &graph.Coordinator{}}, provider)
			_, err := r.buildRouteModelWithReader(t.Context(), kubeClient, route, gateway)
			var unsupported *routeBuildError
			if !errors.As(err, &unsupported) || unsupported.kind != routeErrorUnsupported || !strings.Contains(unsupported.message, test.message) {
				t.Fatalf("unsupported action error=%v", err)
			}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(route)}
			for range 2 {
				if _, err := r.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			var current gatewayv1.HTTPRoute
			if err := kubeClient.Get(t.Context(), request.NamespacedName, &current); err != nil {
				t.Fatal(err)
			}
			if !apiequality.Semantic.DeepEqual(current.Annotations, route.Annotations) || !apiequality.Semantic.DeepEqual(current.Finalizers, route.Finalizers) || len(current.Status.Parents) != 2 {
				t.Fatalf("unsupported route acquired binding metadata or lost foreign status: %#v", current)
			}
			for _, parent := range current.Status.Parents {
				if parent.ControllerName == foreignParent.ControllerName {
					if !apiequality.Semantic.DeepEqual(parent, foreignParent) {
						t.Fatalf("foreign parent status changed: got=%#v want=%#v", parent, foreignParent)
					}
					continue
				}
				accepted := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionAccepted))
				if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.RouteReasonUnsupportedValue) || !strings.Contains(accepted.Message, test.message) {
					t.Fatalf("unsupported action status=%#v", parent)
				}
				if test.deniedBackend {
					resolved := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionResolvedRefs))
					if resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonRefNotPermitted) ||
						!strings.Contains(resolved.Message, "shared/backend") {
						t.Fatalf("denied reference status = %#v", resolved)
					}
					if reader.counts["Service"] != 0 {
						t.Fatalf("denied Service was read: %v", reader.counts)
					}
				}
			}
			if len(provider.gatewaySpecs)+len(provider.routeSpecs)+len(provider.deletedGateways)+len(provider.deletedRoutes) != 0 {
				t.Fatal("unsupported action called the cloud provider")
			}
		})
	}
}
