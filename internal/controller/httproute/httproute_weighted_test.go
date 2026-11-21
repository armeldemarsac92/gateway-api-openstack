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
	"reflect"
	"slices"
	"strings"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestWeightedHTTPRouteRevokesAndRestoresOnlyDeniedShare(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.FixedHTTPResponses = true
	f.reconciler.Config.WeightedHTTPTargets = true
	remote, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	remote.Namespace, remote.Name, remote.UID, remote.ResourceVersion = "shared", "canary", "canary-uid", ""
	endpoints.Namespace, endpoints.UID, endpoints.ResourceVersion = remote.Namespace, "canary-slice-uid", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = remote.Name
	grant := serviceReferenceGrant(remote.Namespace, f.route.Namespace, remote.Name)
	f.create(t, remote, endpoints, grant)

	baseline := f.route.Spec.Rules[0]
	weighted := pathBackendRule("/canary/", gatewayv1.PathMatchPathPrefix, gatewayv1.ObjectName(f.service.Name))
	weighted.Matches = append(weighted.Matches, pathBackendRule("/preview", gatewayv1.PathMatchExact, gatewayv1.ObjectName(f.service.Name)).Matches[0])
	weighted.BackendRefs[0] = *baseline.BackendRefs[0].DeepCopy()
	weighted.BackendRefs[0].Weight = ptr.To(int32(3))
	canary := *baseline.BackendRefs[0].DeepCopy()
	canary.Name, canary.Namespace = gatewayv1.ObjectName(remote.Name), ptr.To(gatewayv1.Namespace(remote.Namespace))
	weighted.BackendRefs = append(weighted.BackendRefs, canary)
	f.route.Spec.Rules = append(f.route.Spec.Rules, weighted)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}

	before := f.observe(t)
	spec := before.Nodes[0].Spec
	wantMatches := []cloud.HTTPPathMatch{
		{PathType: cloud.PathMatchExact, PathValue: "/preview", Precedence: 0},
		{PathType: cloud.PathMatchPrefix, PathValue: "/canary", Precedence: 1},
	}
	if len(spec.WeightedHTTPRules) != 1 || spec.WeightedHTTPRules[0].RuleIndex != 1 ||
		!reflect.DeepEqual(spec.WeightedHTTPRules[0].Matches, wantMatches) {
		t.Fatalf("weighted matches = %#v", spec.WeightedHTTPRules)
	}
	if len(spec.HTTPBackends) != 2 || len(spec.HTTPBackends[0].Matches) != 1 ||
		spec.HTTPBackends[0].Matches[0].Precedence != 2 || len(spec.HTTPBackends[1].Matches) != 0 {
		t.Fatalf("service pools or shared match = %#v", spec.HTTPBackends)
	}

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	reconcile := func(resolved metav1.ConditionStatus, reason gatewayv1.RouteConditionReason) cloud.RouteSpec {
		t.Helper()
		provider.routeSpecs = nil
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if err := f.client.Get(t.Context(), request.NamespacedName, f.route); err != nil {
			t.Fatal(err)
		}
		if len(provider.routeSpecs) != 1 || len(provider.deletedRoutes) != 0 || len(f.route.Status.Parents) != 1 {
			t.Fatalf("route binding did not remain active: provider = %#v, parents = %#v", provider, f.route.Status.Parents)
		}
		conditions := f.route.Status.Parents[0].Conditions
		condition := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
		if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)) ||
			condition == nil || condition.Status != resolved || condition.Reason != string(reason) {
			t.Fatalf("route conditions = %#v", conditions)
		}
		return provider.routeSpecs[0]
	}

	allowed := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	targets := allowed.WeightedHTTPRules[0].Targets
	if !reflect.DeepEqual(targets, []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {BackendIndex: ptr.To(1), Weight: 1}}) {
		t.Fatalf("initial targets = %#v", targets)
	}
	if err := f.client.Delete(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	clear(f.reader.counts)
	if err := before.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("grant revocation passed the mutation fence: %v", err)
	}
	if f.reader.counts["Service"] != 1 || f.reader.counts["EndpointSlice"] != 1 {
		t.Fatalf("revocation read more than the local Service and EndpointSlices: %v", f.reader.counts)
	}
	if requests := r.enqueueHTTPRoutesForReferenceGrant(t.Context(), grant); !slices.Equal(requests, []ctrl.Request{request}) {
		t.Fatalf("ReferenceGrant event did not reach the weighted route: %v", requests)
	}

	denied := reconcile(metav1.ConditionFalse, gatewayv1.RouteReasonRefNotPermitted)
	if len(denied.HTTPBackends) != 1 || !reflect.DeepEqual(denied.HTTPBackends[0], allowed.HTTPBackends[0]) ||
		len(denied.WeightedHTTPRules) != 1 || denied.WeightedHTTPRules[0].RuleIndex != 1 ||
		!reflect.DeepEqual(denied.WeightedHTTPRules[0].Matches, wantMatches) ||
		!reflect.DeepEqual(denied.WeightedHTTPRules[0].Targets, []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 3}, {Weight: 1}}) {
		t.Fatalf("revoked spec lost or redistributed a share: %#v", denied)
	}
	clear(f.reader.counts)
	revoked := f.observe(t)
	if f.reader.counts["Service"] != 1 || f.reader.counts["EndpointSlice"] != 1 {
		t.Fatalf("denied graph read more than the local Service and EndpointSlices: %v", f.reader.counts)
	}
	f.create(t, serviceReferenceGrant(remote.Namespace, f.route.Namespace, remote.Name))
	if err := revoked.Validate(t.Context()); !errors.Is(err, errHTTPRouteChanged) {
		t.Fatalf("grant restoration passed the mutation fence: %v", err)
	}
	restored := reconcile(metav1.ConditionTrue, gatewayv1.RouteReasonResolvedRefs)
	if !reflect.DeepEqual(restored, allowed) {
		t.Fatalf("restored graph = %#v, want %#v", restored, allowed)
	}
}

func TestWeightedHTTPRouteResolvesZeroWeightAndKeepsAnEmptyServicePool(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.WeightedHTTPTargets = true
	rule := &f.route.Spec.Rules[0]
	rule.BackendRefs[0].Weight = ptr.To(int32(1))
	ignored := *rule.BackendRefs[0].DeepCopy()
	ignored.Kind, ignored.Weight = ptr.To(gatewayv1.Kind("UnsupportedBackend")), ptr.To(int32(0))
	rule.BackendRefs = append(rule.BackendRefs, ignored)
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), f.endpoints); err != nil {
		t.Fatal(err)
	}

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(provider.routeSpecs) != 1 {
		t.Fatalf("weighted route was not programmed: %#v", provider.routeSpecs)
	}
	spec := provider.routeSpecs[0]
	if len(spec.HTTPBackends) != 1 || len(spec.HTTPBackends[0].Members) != 0 || len(spec.HTTPBackends[0].Matches) != 0 ||
		!spec.HTTPBackends[0].Unavailable || spec.HTTPBackends[0].InvalidReference ||
		len(spec.WeightedHTTPRules) != 1 || !reflect.DeepEqual(spec.WeightedHTTPRules[0].Targets, []cloud.HTTPWeightedTarget{{BackendIndex: ptr.To(0), Weight: 1}, {Weight: 0}}) {
		t.Fatalf("weighted targets lost the empty pool or zero share: %#v", spec)
	}
	if err := f.client.Get(t.Context(), request.NamespacedName, f.route); err != nil {
		t.Fatal(err)
	}
	if len(f.route.Status.Parents) != 1 {
		t.Fatalf("route parent status = %#v", f.route.Status.Parents)
	}
	conditions := f.route.Status.Parents[0].Conditions
	resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
	if !meta.IsStatusConditionTrue(conditions, string(gatewayv1.RouteConditionAccepted)) ||
		resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.RouteReasonInvalidKind) ||
		!strings.Contains(resolved.Message, string(*ignored.Kind)) {
		t.Fatalf("zero-weight backend was not resolved: %#v", conditions)
	}
}

func TestWeightedHTTPRouteRejectsZeroTotalAndRequiresOptIn(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.FixedHTTPResponses = true
	rule := &f.route.Spec.Rules[0]
	rule.BackendRefs = append(rule.BackendRefs, *rule.BackendRefs[0].DeepCopy())
	if _, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway); err == nil ||
		!strings.Contains(err.Error(), "exactly one backendRef") {
		t.Fatalf("fixed-response-only profile accepted weighted refs: %v", err)
	}
	f.reconciler.Config.WeightedHTTPTargets = true
	for i := range rule.BackendRefs {
		rule.BackendRefs[i].Weight = ptr.To(int32(0))
	}
	if _, err := f.reconciler.buildRouteModelWithReader(t.Context(), f.reader, f.route, f.gateway); err == nil ||
		!strings.Contains(err.Error(), "positive total weight") {
		t.Fatalf("zero-total weighted rule was not rejected explicitly: %v", err)
	}
	if len(f.reader.counts) != 0 {
		t.Fatalf("structurally rejected routes read backend dependencies: %v", f.reader.counts)
	}
}

func TestOmittedBackendGetsFixedResponseWithoutUnresolvedReference(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.FixedHTTPResponses = true
	f.route.Spec.Rules = []gatewayv1.HTTPRouteRule{{}}
	if err := f.client.Update(t.Context(), f.route); err != nil {
		t.Fatal(err)
	}
	snapshot := f.observe(t)
	if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].Disposition != cloud.GraphEnsure {
		t.Fatalf("omitted backend should program the route: %#v", snapshot.Nodes)
	}
	spec := snapshot.Nodes[0].Spec
	if len(spec.FixedHTTPRules) != 1 || len(spec.FixedHTTPRules[0].Matches) != 1 ||
		spec.FixedHTTPRules[0].Matches[0].PathValue != "/" || len(spec.HTTPBackends) != 0 || len(spec.WeightedHTTPRules) != 0 {
		t.Fatalf("omitted backend should lower to a single pool-free fixed action: %#v", spec)
	}
	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.route)}
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Get(t.Context(), request.NamespacedName, f.route); err != nil {
		t.Fatal(err)
	}
	if len(f.route.Status.Parents) != 1 || !meta.IsStatusConditionTrue(f.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)) {
		t.Fatalf("omitted backend unexpectedly produced an unresolved reference: %#v", f.route.Status.Parents)
	}
}
