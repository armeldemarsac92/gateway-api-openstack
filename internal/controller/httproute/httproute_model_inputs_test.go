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
	"maps"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestHTTPRoutePathMatchOrderGroupsThreeBackends(t *testing.T) {
	f := newRouteModelInputFixture(t)
	exact, prefix := gatewayv1.PathMatchExact, gatewayv1.PathMatchPathPrefix
	f.route.Spec.Rules = nil
	for _, entry := range []struct {
		path    string
		kind    gatewayv1.PathMatchType
		service gatewayv1.ObjectName
	}{
		{"/match", exact, "infra-backend-v1"},
		{"/match/exact", exact, "infra-backend-v2"},
		{"/match/exact/one", exact, "infra-backend-v3"},
		{"/match/", prefix, "infra-backend-v3"},
		{"/match/prefix/", prefix, "infra-backend-v1"},
		{"/match/prefix/one", prefix, "infra-backend-v2"},
	} {
		rule := pathBackendRule(entry.path, entry.kind, entry.service)
		f.route.Spec.Rules = append(f.route.Spec.Rules, rule)
	}
	hosts := []gatewayv1.Hostname{"app.example.test"}
	f.route.Spec.Hostnames = hosts
	peer := f.candidate("other")
	peer.Spec.Rules = []gatewayv1.HTTPRouteRule{pathBackendRule("/another-exact", exact, gatewayv1.ObjectName(f.service.Name))}
	peer.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(f.service.Spec.Ports[0].Port))
	f.reconciler.applyRouteBinding(peer, f.gateway)
	f.create(t, peer)
	for i, name := range []string{"infra-backend-v1", "infra-backend-v2", "infra-backend-v3"} {
		service, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
		service.Name, service.UID, service.ResourceVersion = name, types.UID(name), ""
		service.Spec.Ports[0].Port = 8080
		endpoints.Name, endpoints.UID, endpoints.ResourceVersion = name, types.UID(name+"-slice"), ""
		endpoints.Labels[discoveryv1.LabelServiceName] = name
		endpoints.Endpoints[0].Addresses = []string{fmt.Sprintf("192.0.2.%d", 30+i)}
		f.create(t, service, endpoints)
	}
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("reconcile: %v (cause: %v)", err, errors.Unwrap(err))
	}
	if len(provider.routeSpecs) != 2 {
		t.Fatalf("provider route specs = %#v, want one node per bound route", provider.routeSpecs)
	}
	grouped := provider.routeSpecs[0]
	if grouped.Identity.RouteUID != string(f.route.UID) || grouped.PathType != "" || len(grouped.Members) != 0 || len(grouped.HTTPBackends) != 3 {
		t.Fatalf("grouped provider spec = %#v", grouped)
	}
	want := map[string]int{
		"/match": 0, "/match/exact": 1, "/match/exact/one": 2, "/match/prefix/one": 4, "/match/prefix": 5,
	}
	for i, backend := range grouped.HTTPBackends {
		if backend.Namespace != f.route.Namespace || backend.Name != fmt.Sprintf("infra-backend-v%d", i+1) ||
			backend.Port != 8080 || backend.Unavailable || len(backend.Members) != 1 || len(backend.Matches) != 2 ||
			backend.Members[0].Address != fmt.Sprintf("192.0.2.%d", 30+i) {
			t.Fatalf("group %d = %#v", i, backend)
		}
		for _, match := range backend.Matches {
			rank, ok := want[match.PathValue]
			if match.PathType == cloud.PathMatchPrefix && match.PathValue == "/match" {
				rank, ok = 6, true
			}
			if !ok || match.Precedence != rank || match.Hostname != string(hosts[0]) {
				t.Fatalf("ranked match = %#v, want precedence %d", match, rank)
			}
		}
	}
	if peerSpec := provider.routeSpecs[1]; peerSpec.Precedence != 3 || len(peerSpec.HTTPBackends) != 0 {
		t.Fatalf("legacy peer spec = %#v", peerSpec)
	}
}

func TestRecoveredBundleRetainsHealthyPathsAfterSecondaryGrantLoss(t *testing.T) {
	f := newRouteModelInputFixture(t)
	secondary := pathBackendRule("/private", gatewayv1.PathMatchExact, "secondary")
	secondary.BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("shared"))
	f.route.Spec.Rules = append(f.route.Spec.Rules, secondary)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	reader := liveReaderWithUnsupportedVersion(f.client)
	f.reconciler.APIReader = reader
	owner := controller.RouteIdentity(f.reconciler.Config, f.gateway, f.route)
	cleanup, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, owner)
	if err != nil || cleanup.disposition != routeCleanupRequired {
		t.Fatalf("cleanup under version skew = %#v, %v", cleanup, err)
	}
	reader.useClientCRDs = true
	if err := f.reconciler.sealRouteCleanupSnapshot(t.Context(), cleanup); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("obsolete full-route cleanup after recovery = %v", err)
	}
	current, err := f.reconciler.buildRouteCleanupSnapshot(t.Context(), f.route, owner)
	if err != nil || current.disposition != routeCleanupStillDesired {
		t.Fatalf("healthy paths after recovery = %#v, %v", current, err)
	}
}

func TestHTTPRouteRevokesGrantForSecondaryBackend(t *testing.T) {
	f := newRouteModelInputFixture(t)
	remote, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	remote.Namespace, remote.Name, remote.ResourceVersion = "shared", "secondary", ""
	remote.Spec.Ports[0].Port = 8080
	endpoints.Namespace, endpoints.ResourceVersion = remote.Namespace, ""
	endpoints.Labels[discoveryv1.LabelServiceName] = remote.Name
	grant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: "secondary"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: gatewayv1.Namespace(f.route.Namespace)}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(remote.Name))}},
		},
	}
	f.create(t, remote, endpoints, grant)
	secondary := pathBackendRule("/private", gatewayv1.PathMatchExact, gatewayv1.ObjectName(remote.Name))
	secondary.BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace(remote.Namespace))
	f.route.Spec.Rules = append(f.route.Spec.Rules, secondary)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	before := f.observe(t)
	if len(before.Nodes) != 1 || len(before.Nodes[0].Spec.HTTPBackends) != 2 || before.Nodes[0].Spec.HTTPBackends[1].Unavailable {
		t.Fatalf("authorized fragment = %#v", before.Nodes)
	}
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("reconcile before revocation: %v (cause: %v)", err, errors.Unwrap(err))
	}
	if err := f.client.Get(t.Context(), request.NamespacedName, f.route); err != nil {
		t.Fatal(err)
	}
	if len(f.route.Status.Parents) != 1 || r.currentRouteProgrammedCondition(f.route, f.route.Spec.ParentRefs[0]) == nil {
		t.Fatalf("authorized route status = %#v", f.route.Status.Parents)
	}
	if err := f.client.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	clear(f.reader.counts)
	if err := before.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("secondary grant revocation passed the mutation fence: %v", err)
	}
	if f.reader.counts["Service"] != 1 || f.reader.counts["EndpointSlice"] != 1 {
		t.Fatalf("revocation allowed reads of the secondary Service or EndpointSlice: %v", f.reader.counts)
	}
	provider.routeSpecs = nil
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("reconcile: %v (cause: %v)", err, errors.Unwrap(err))
	}
	if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 || len(provider.routeSpecs[0].HTTPBackends) != 2 {
		t.Fatalf("revocation lost healthy rules: ensure %#v, delete %#v", provider.routeSpecs, provider.deletedRoutes)
	}
	local, denied := provider.routeSpecs[0].HTTPBackends[0], provider.routeSpecs[0].HTTPBackends[1]
	if local.Unavailable || len(local.Members) != 1 || !denied.Unavailable || len(denied.Members) != 0 ||
		len(denied.Matches) != 1 || denied.Matches[0].PathValue != "/private" || denied.Matches[0].Precedence >= local.Matches[0].Precedence {
		t.Fatalf("revoked route groups = %#v", provider.routeSpecs[0].HTTPBackends)
	}
	if err := f.client.Get(t.Context(), request.NamespacedName, f.route); err != nil {
		t.Fatal(err)
	}
	if len(f.route.Status.Parents) != 1 {
		t.Fatalf("parent statuses = %#v", f.route.Status.Parents)
	}
	conditions := f.route.Status.Parents[0].Conditions
	resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)) ||
		resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonRefNotPermitted) ||
		resolved.ObservedGeneration != f.route.Generation {
		t.Fatalf("conditions after secondary grant revocation = %#v", conditions)
	}
}

func TestHTTPRouteOneRuleWithAlternativeMatches(t *testing.T) {
	f := newRouteModelInputFixture(t)
	rule := &f.route.Spec.Rules[0]
	rule.Matches = []gatewayv1.HTTPRouteMatch{
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchPathPrefix), Value: ptr.To("/api/")}},
		{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/health")}},
	}
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	input := f.observe(t)
	spec := input.Nodes[0].Spec
	want := []cloud.HTTPPathMatch{
		{PathType: cloud.PathMatchExact, PathValue: "/health", Precedence: 0},
		{PathType: cloud.PathMatchPrefix, PathValue: "/api", Precedence: 1},
	}
	if len(spec.HTTPBackends) != 1 || !slices.Equal(spec.HTTPBackends[0].Matches, want) {
		t.Fatalf("alternative matches = %#v, want %v", spec.HTTPBackends, want)
	}
}

func pathBackendRule(path string, kind gatewayv1.PathMatchType, service gatewayv1.ObjectName) gatewayv1.HTTPRouteRule {
	return gatewayv1.HTTPRouteRule{
		Matches: []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(kind), Value: ptr.To(path)}}},
		BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
			Name: service, Port: ptr.To(gatewayv1.PortNumber(8080)),
		}}}},
	}
}

func TestRouteGraphOrdersTwoBoundRoutes(t *testing.T) {
	f := newRouteModelInputFixture(t)
	exact := f.candidate("z-accounts")
	exact.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{
		Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/accounts"),
	}}}
	f.reconciler.applyRouteBinding(exact, f.gateway)
	f.create(t, exact)
	input := f.observe(t)
	if len(input.Nodes) != 2 {
		t.Fatalf("got %d route fragments, want both bound routes", len(input.Nodes))
	}
	for _, node := range input.Nodes {
		want := 1
		if node.Spec.Identity.RouteUID == string(exact.UID) {
			want = 0
		}
		if node.Disposition != cloud.GraphEnsure || node.Spec.Precedence != want {
			t.Fatalf("route %s = %s rank %d, want Ensure rank %d", node.Spec.Identity.RouteUID, node.Disposition, node.Spec.Precedence, want)
		}
	}
}

func TestRouteGraphRevokesHTTPPolicyAfterNamespaceLabelChange(t *testing.T) {
	f := newRouteModelInputFixture(t)
	first := &f.gateway.Spec.Listeners[0]
	first.Hostname = ptr.To(gatewayv1.Hostname("blue.example.test"))
	first.AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
		From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "blue"}},
	}}
	second := first.DeepCopy()
	second.Name, second.Hostname = "green", ptr.To(gatewayv1.Hostname("green.example.test"))
	second.AllowedRoutes.Namespaces.Selector.MatchLabels["team"] = "green"
	f.gateway.Spec.Listeners = append(f.gateway.Spec.Listeners, *second)
	if err := f.client.Update(t.Context(), f.gateway); err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.route.Namespace, Labels: map[string]string{"team": "blue"}}}
	f.create(t, namespace)
	if !gatewayProgrammedForGeneration(f.gateway) {
		t.Fatal("Gateway must already be programmed for the current generation")
	}
	input := f.observe(t)
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.Hostname != "blue.example.test" {
		t.Fatalf("initial attachment = %#v", input.Nodes)
	}

	namespace.Labels["team"] = "green"
	if err := f.client.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	input = f.observe(t)
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.Hostname != "green.example.test" {
		t.Fatalf("updated attachment = %#v", input.Nodes)
	}

	namespace.Labels["team"] = "other"
	if err := f.client.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	request := graph.Request{Identity: controller.GatewayIdentity(f.reconciler.Config, f.gateway), Action: graph.Ensure}
	input, err := f.reconciler.ObserveRouteGraph(t.Context(), request, f.gateway)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphPreserve || !input.Nodes[0].WaitForHTTPPolicyRemoval {
		t.Fatalf("revoked route has no withdrawal readiness hold before its detach checkpoint: %#v", input.Nodes)
	}
}

func TestRouteGraphSharesBackendObservationAcrossCandidates(t *testing.T) {
	f := newRouteModelInputFixture(t)
	unsupported := f.candidate("a-unsupported")
	unsupported.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(80))
	other := *unsupported.Spec.Rules[0].BackendRefs[0].DeepCopy()
	other.Weight = ptr.To(int32(20))
	unsupported.Spec.Rules[0].BackendRefs = append(unsupported.Spec.Rules[0].BackendRefs, other)
	candidate := f.candidate("z-candidate")
	candidate.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("unbound-backend"))
	f.create(t, unsupported, candidate)

	routes, err := f.reconciler.liveGraphRoutes(t.Context(), controller.GatewayIdentity(testConfig(), f.gateway))
	if err != nil {
		t.Fatal(err)
	}
	models, err := f.reconciler.collectRouteModelInputs(t.Context(), f.reader, f.gateway, routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(models.logical.Routes) != 3 || len(models.logical.Routes[0].Rules) != 1 {
		t.Fatalf("complete logical inventory lost routes or rules: %#v", models.logical.Routes)
	}
	groups := models.logical.Routes[0].Rules[0].Backends
	if len(groups) != 2 || groups[0].Weight != 80 || groups[1].Weight != 20 {
		t.Fatalf("complete logical inventory lost backend groups: %#v", models.logical.Routes)
	}
	clear(f.reader.counts)
	input := f.observe(t)
	if len(input.Nodes) != 1 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.Identity.RouteUID != string(f.route.UID) {
		t.Fatalf("unsupported candidate displaced the selected route: %#v", input.Nodes)
	}
	if !maps.Equal(f.reader.counts, map[string]int{"Service": 1, "EndpointSlice": 1}) {
		t.Fatalf("backend reads during observation = %v", f.reader.counts)
	}
	if err := input.Validate(t.Context()); err != nil {
		t.Fatalf("unchanged graph failed validation: %v", err)
	}
	if !maps.Equal(f.reader.counts, map[string]int{"Service": 2, "EndpointSlice": 2}) {
		t.Fatalf("backend reads after the mutation fence = %v", f.reader.counts)
	}
}

func TestRouteGraphFenceDetectsBackendDependencyChanges(t *testing.T) {
	for _, change := range []string{
		"Service replacement", "EndpointSlice addition", "EndpointSlice deletion", "EndpointSlice replacement",
		"EndpointSlice label move", "missing Service creation", "ReferenceGrant revocation",
	} {
		t.Run(change, func(t *testing.T) {
			f := newRouteModelInputFixture(t)
			var grant *gatewayv1.ReferenceGrant
			switch change {
			case "missing Service creation":
				candidate := f.candidate("z-missing")
				candidate.Spec.Rules[0].BackendRefs[0].Name = "missing"
				f.reconciler.applyRouteBinding(candidate, f.gateway)
				f.create(t, candidate)
			case "ReferenceGrant revocation":
				candidate := f.candidate("z-cross-namespace")
				candidate.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("shared"))
				f.reconciler.applyRouteBinding(candidate, f.gateway)
				service := f.service.DeepCopy()
				service.Namespace, service.ResourceVersion, service.UID = "shared", "", "shared-service"
				grant = &gatewayv1.ReferenceGrant{
					ObjectMeta: metav1.ObjectMeta{Namespace: "shared", Name: "allow"},
					Spec: gatewayv1.ReferenceGrantSpec{
						From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: gatewayv1.Namespace(f.route.Namespace)}},
						To:   []gatewayv1.ReferenceGrantTo{{Group: "", Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(service.Name))}},
					},
				}
				f.create(t, candidate, service, grant)
			}
			input := f.observe(t)
			if len(input.Nodes) == 0 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[0].Spec.Identity.RouteUID != string(f.route.UID) {
				t.Fatalf("selected route was not ensured: %#v", input.Nodes)
			}
			switch change {
			case "Service replacement":
				replaceRouteModelInputObject(t, f.client, f.service)
			case "EndpointSlice addition":
				added := f.endpoints.DeepCopy()
				added.Name, added.UID, added.ResourceVersion = "new-slice", "new-slice", ""
				f.create(t, added)
			case "EndpointSlice deletion":
				if err := f.client.Delete(t.Context(), f.endpoints); err != nil {
					t.Fatal(err)
				}
			case "EndpointSlice replacement":
				replaceRouteModelInputObject(t, f.client, f.endpoints)
			case "EndpointSlice label move":
				f.endpoints.Labels[discoveryv1.LabelServiceName] = "other"
				if err := f.client.Update(t.Context(), f.endpoints); err != nil {
					t.Fatal(err)
				}
			case "missing Service creation":
				created := f.service.DeepCopy()
				created.Name, created.UID, created.ResourceVersion = "missing", "new-service", ""
				f.create(t, created)
			case "ReferenceGrant revocation":
				if err := f.client.Delete(t.Context(), grant); err != nil {
					t.Fatal(err)
				}
			}
			if err := input.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
				t.Fatalf("changed dependency passed the mutation fence: %v", err)
			}
		})
	}
}

func TestRouteModelFenceDetectsNamespaceLabelChanges(t *testing.T) {
	f := newRouteModelInputFixture(t)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.route.Namespace, Labels: map[string]string{"team": "application"}}}
	f.create(t, namespace)
	f.gateway.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
		From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "application"}},
	}}
	if err := f.client.Update(t.Context(), f.gateway); err != nil {
		t.Fatal(err)
	}
	input := f.observe(t)
	if err := input.Validate(t.Context()); err != nil {
		t.Fatalf("unchanged Namespace failed validation: %v", err)
	}
	namespace.Labels["team"] = "another"
	if err := f.client.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("changed Namespace labels passed the mutation fence: %v", err)
	}
}

func TestRouteGraphPreservesRevokedTenantUntilDetachCheckpoint(t *testing.T) {
	f := newRouteModelInputFixture(t)
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(f.gateway), f.gateway); err != nil {
		t.Fatal(err)
	}
	f.gateway.Spec.Listeners[0].AllowedRoutes = &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
		From: ptr.To(gatewayv1.NamespacesFromSelector), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"edge": "shared"}},
	}}
	if err := f.client.Update(t.Context(), f.gateway); err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}}
	sibling := f.candidate(f.route.Name)
	sibling.Namespace, sibling.UID = namespace.Name, "tenant-sibling"
	sibling.Spec.ParentRefs[0].Namespace = ptr.To(gatewayv1.Namespace(f.gateway.Namespace))
	sibling.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("foreign"))
	f.reconciler.applyRouteBinding(sibling, f.gateway)
	f.create(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.route.Namespace, Labels: map[string]string{"edge": "shared"}}}, namespace, sibling)
	input := f.observe(t)
	if len(input.Nodes) != 2 || input.Nodes[0].Disposition != cloud.GraphEnsure || input.Nodes[1].Disposition != cloud.GraphPreserve || input.Nodes[1].Spec.Identity.RouteUID != string(sibling.UID) {
		t.Fatalf("expected to ensure the permitted route and hold the revoked tenant fragment for its cleanup: %#v", input.Nodes)
	}
	if f.reader.counts["ReferenceGrant"] != 0 || f.reader.counts["Service"] != 1 || f.reader.counts["EndpointSlice"] != 1 {
		t.Fatalf("revoked tenant caused backend reads: %v", f.reader.counts)
	}
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(sibling), sibling); err != nil {
		t.Fatal(err)
	}
	sibling.Status.Parents = []gatewayv1.RouteParentStatus{{ControllerName: "other.example/controller", ParentRef: sibling.Spec.ParentRefs[0]}}
	if err := f.client.Status().Update(t.Context(), sibling); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); err != nil {
		t.Fatalf("denied tenant status churn invalidated the healthy route: %v", err)
	}
	namespace.Labels = map[string]string{"edge": "shared"}
	if err := f.client.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := input.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("restored scope did not cancel provider plan: %v", err)
	}
}

func TestRouteModelPreservesPendingServiceDeletion(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.service.Finalizers = []string{"example.test/hold"}
	if err := f.client.Update(t.Context(), f.service); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), f.service); err != nil {
		t.Fatal(err)
	}
	_, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
	var buildError *routeBuildError
	if !errors.As(err, &buildError) || buildError.kind != routeErrorPending {
		t.Fatalf("deleting Service must remain pending: %v", err)
	}
	status := statusForRouteBuildError(err)
	if status.resolved != metav1.ConditionTrue || status.programmedReason != string(gatewayv1.RouteReasonPending) {
		t.Fatalf("deleting Service changed reference status: %#v", status)
	}
	if !maps.Equal(f.reader.counts, map[string]int{"Service": 1}) {
		t.Fatalf("deleting Service caused endpoint reads: %v", f.reader.counts)
	}
}

func TestRouteBindingFenceDetectsNewCandidate(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.clearRouteBinding(f.route)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.reconciler.buildRouteValidationSnapshot(t.Context(), f.route, f.gateway, routeSnapshotForBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.sealRouteMutationSnapshot(t.Context(), snapshot, routeBindingOptional); err != nil {
		t.Fatalf("unchanged binding snapshot failed validation: %v", err)
	}
	f.create(t, f.candidate("a-earlier"))
	if err := f.reconciler.sealRouteMutationSnapshot(t.Context(), snapshot, routeBindingOptional); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("new candidate was not observed before binding: %v", err)
	}
}

func TestRouteGraphCleanupDoesNotReadBackendDependencies(t *testing.T) {
	for _, action := range []graph.Action{graph.DeleteRoute, graph.DeleteGateway} {
		t.Run(string(action), func(t *testing.T) {
			f := newRouteModelInputFixture(t)
			f.route.Annotations[testConfig().RouteLifecycleAnnotation()] = controller.RouteDetaching
			if err := f.client.Update(t.Context(), f.route); err != nil {
				t.Fatal(err)
			}
			f.reader.forbid = true
			input, err := f.reconciler.ObserveRouteGraph(t.Context(), graph.Request{
				Identity: controller.RouteIdentity(testConfig(), f.gateway, f.route), Action: action, Route: f.route,
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := input.Validate(t.Context()); err != nil {
				t.Fatalf("cleanup required backend dependencies: %v", err)
			}
			if len(f.reader.counts) != 0 {
				t.Fatalf("cleanup read backend dependencies: %v", f.reader.counts)
			}
		})
	}
}

type routeModelInputFixture struct {
	reconciler *Reconciler
	client     client.Client
	reader     *routeModelDependencyReader
	gateway    *gatewayv1.Gateway
	route      *gatewayv1.HTTPRoute
	service    *corev1.Service
	endpoints  *discoveryv1.EndpointSlice
}

func newRouteModelInputFixture(t *testing.T) *routeModelInputFixture {
	t.Helper()
	class, gateway, route, service, endpoints := endpointRouteFixture(t)
	endpoints.UID = "slice-uid"
	r := &Reconciler{Config: testConfig(), Coordinator: &graph.Coordinator{}}
	r.applyRouteBinding(route, gateway)
	kubeClient := indexedFakeClientBuilder(testScheme(t), r.Config).WithStatusSubresource(route).WithObjects(class, gateway, route, service, endpoints).Build()
	for _, object := range []client.Object{gateway, route, service, endpoints} {
		if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
	}
	reader := &routeModelDependencyReader{Reader: kubeClient, counts: map[string]int{}}
	r.Client, r.APIReader = kubeClient, reader
	return &routeModelInputFixture{reconciler: r, client: kubeClient, reader: reader, gateway: gateway, route: route, service: service, endpoints: endpoints}
}

func (f *routeModelInputFixture) candidate(name string) *gatewayv1.HTTPRoute {
	route := f.route.DeepCopy()
	route.Name, route.UID, route.ResourceVersion = name, types.UID(name), ""
	route.Annotations, route.Finalizers = nil, nil
	return route
}

func (f *routeModelInputFixture) create(t *testing.T, objects ...client.Object) {
	t.Helper()
	for _, object := range objects {
		if err := f.client.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *routeModelInputFixture) observe(t *testing.T) graph.RouteInputs {
	t.Helper()
	input, err := f.reconciler.ObserveRouteGraph(t.Context(), graph.Request{
		Identity: controller.RouteIdentity(testConfig(), f.gateway, f.route), Action: graph.Ensure, Route: f.route,
	}, f.gateway)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func replaceRouteModelInputObject(t *testing.T, kubeClient client.Client, object client.Object) {
	t.Helper()
	if err := kubeClient.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	object.SetResourceVersion("")
	object.SetUID("replacement")
	if err := kubeClient.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
}

type routeModelDependencyReader struct {
	client.Reader
	counts map[string]int
	forbid bool
}

func (r *routeModelDependencyReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	var kind string
	switch object.(type) {
	case *corev1.Service:
		kind = "Service"
	case *corev1.Namespace:
		kind = "Namespace"
	case *corev1.Node:
		return errors.New("Endpoint mode must not read Nodes")
	}
	if kind != "" {
		r.counts[kind]++
		if r.forbid {
			return fmt.Errorf("unexpected %s read during cleanup", kind)
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *routeModelDependencyReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	var kind string
	switch list.(type) {
	case *discoveryv1.EndpointSliceList:
		kind = "EndpointSlice"
	case *gatewayv1.ReferenceGrantList:
		kind = "ReferenceGrant"
	case *corev1.NodeList:
		return errors.New("Endpoint mode must not list Nodes")
	}
	if kind != "" {
		r.counts[kind]++
		if r.forbid {
			return fmt.Errorf("unexpected %s list during cleanup", kind)
		}
	}
	return r.Reader.List(ctx, list, options...)
}

func TestHTTPSRouteSelectsDedicatedSectionAndInheritsHostname(t *testing.T) {
	f := newRouteModelInputFixture(t)
	cfg := f.reconciler.Config
	cfg.OperatorHTTPS = true
	f.reconciler.Config = cfg
	section := gatewayv1.SectionName("secure")
	port := gatewayv1.PortNumber(443)
	f.gateway.Spec.Listeners = append(f.gateway.Spec.Listeners, gatewayv1.Listener{
		Name: section, Port: port, Protocol: gatewayv1.HTTPSProtocolType, Hostname: ptr.To(gatewayv1.Hostname("app.example.test")),
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "certificate"}}},
	})
	controller.SetGatewayListenerScope(cfg, f.gateway, gatewayv1.HTTPProtocolType)
	f.route.Spec.ParentRefs[0].SectionName = &section
	f.route.Spec.ParentRefs[0].Port = &port
	spec, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(spec.ScopedListenerPorts, []int{443}) || spec.Hostname != "app.example.test" ||
		spec.GatewayRequirements.ListenerProtocol != cloud.ListenerProtocolHTTP {
		t.Fatalf("HTTPS route lost its port or hostname constraint: %#v", spec)
	}
	f.route.Spec.Hostnames = []gatewayv1.Hostname{"other.example.test"}
	_, err = f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway)
	var buildError *routeBuildError
	if !errors.As(err, &buildError) || buildError.kind != routeErrorNoMatchingHostname {
		t.Fatalf("route with a mismatched hostname was admitted: %v", err)
	}
}

func TestFixedHTTPResponseRequiresInvalidBackendReference(t *testing.T) {
	for _, target := range []string{"ready", "empty", "prohibited"} {
		t.Run(target, func(t *testing.T) {
			f := newRouteModelInputFixture(t)
			f.reconciler.Config.FixedHTTPResponses = true
			switch target {
			case "empty":
				if err := f.client.Delete(t.Context(), f.endpoints); err != nil {
					t.Fatal(err)
				}
			case "prohibited":
				f.route.Spec.Rules[0].BackendRefs[0].Namespace = ptr.To(gatewayv1.Namespace("private"))
				if err := f.client.Update(t.Context(), f.route); err != nil {
					t.Fatal(err)
				}
			}
			got := f.observe(t)
			if len(got.Nodes) != 1 || len(got.Nodes[0].Spec.HTTPBackends) != 1 {
				t.Fatalf("expected one observed backend group")
			}
			b := got.Nodes[0].Spec.HTTPBackends[0]
			if b.InvalidReference != (target == "prohibited") || b.Unavailable != (target != "ready") {
				t.Fatalf("invalid reference = %v, unavailable = %v, target = %s", b.InvalidReference, b.Unavailable, target)
			}
		})
	}
}
