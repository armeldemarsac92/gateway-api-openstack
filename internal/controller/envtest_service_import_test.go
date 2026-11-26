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
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	mcscrd "sigs.k8s.io/mcs-api/config/crd"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
	"sigs.k8s.io/yaml"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

type serviceImportAPITestProbe struct {
	failGet           atomic.Bool
	failedGets        atomic.Int64
	backendGets       atomic.Int64
	discoveryRequests atomic.Int64
}

type serviceImportAPITestTransport struct {
	http.RoundTripper
	probe *serviceImportAPITestProbe
}

func (p *serviceImportAPITestProbe) wrap(next http.RoundTripper) http.RoundTripper {
	return serviceImportAPITestTransport{RoundTripper: next, probe: p}
}

func (t serviceImportAPITestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	const groupPath = "/apis/multicluster.x-k8s.io/v1beta1"
	path := request.URL.Path
	switch {
	case path == groupPath || path == "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/serviceimports.multicluster.x-k8s.io":
		t.probe.discoveryRequests.Add(1)
	case request.Method == http.MethodGet && strings.HasPrefix(path, groupPath+"/namespaces/") && strings.Contains(path, "/serviceimports/"):
		t.probe.backendGets.Add(1)
		if t.probe.failGet.Load() {
			t.probe.failedGets.Add(1)
			return nil, errors.New("fixture: transient ServiceImport GET failure")
		}
	}
	return t.RoundTripper.RoundTrip(request)
}

func testServiceImportBackends(t *testing.T, restConfig *rest.Config, c client.Client, cfg controller.Config, provider *recordingProvider, probe *serviceImportAPITestProbe) {
	t.Helper()
	ctx := t.Context()
	const app, targets, shared = "import-routes", "import-backends", "shared"
	const serviceEndpoint, initialVIP, updatedVIP = "10.1.0.11", "10.2.0.21", "10.2.0.22"
	parameters := envtestClassParameters(app)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: app},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: app, Name: "edge"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: app, Listeners: []gatewayv1.Listener{
			{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80},
			{Name: "grpc", Protocol: gatewayv1.HTTPProtocolType, Port: 81},
		}},
	}
	h2c := "kubernetes.io/h2c"
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "h2c", Port: 80, AppProtocol: &h2c}}}}
	endpoints := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: targets, Name: shared, Labels: map[string]string{discoveryv1.LabelServiceName: shared}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("h2c"), Port: ptr.To(int32(8080))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{serviceEndpoint}}},
	}
	http := envtestHTTPRoute(app, "http", gateway.Name, shared)
	http.Spec.ParentRefs[0].SectionName = ptr.To(gatewayv1.SectionName("http"))
	local := http.Spec.Rules[0].BackendRefs[0].BackendRef
	local.Namespace, local.Weight = ptr.To(gatewayv1.Namespace(targets)), ptr.To(int32(2))
	imported := *local.DeepCopy()
	imported.Group, imported.Kind = ptr.To(gatewayv1.Group(mcs.GroupName)), ptr.To(gatewayv1.Kind("ServiceImport"))
	imported.Weight = ptr.To(int32(3))
	http.Spec.Rules[0].BackendRefs = []gatewayv1.HTTPBackendRef{{BackendRef: local}, {BackendRef: imported}}
	grpc := &gatewayv1.GRPCRoute{ObjectMeta: metav1.ObjectMeta{Namespace: app, Name: "grpc"},
		Spec: gatewayv1.GRPCRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "edge", SectionName: ptr.To(gatewayv1.SectionName("grpc"))}}},
			Rules:           []gatewayv1.GRPCRouteRule{{BackendRefs: []gatewayv1.GRPCBackendRef{{BackendRef: local}, {BackendRef: imported}}}},
		},
	}
	coreGrant := &gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-service"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{
				{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: app},
				{Group: gatewayv1.GroupName, Kind: "GRPCRoute", Namespace: app},
			},
			To: []gatewayv1.ReferenceGrantTo{{Kind: "Service", Name: ptr.To(gatewayv1.ObjectName(shared))}},
		},
	}
	importGrant := coreGrant.DeepCopy()
	importGrant.Name = "allow-import"
	importGrant.Spec.To[0].Group, importGrant.Spec.To[0].Kind = mcs.GroupName, "ServiceImport"
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: app}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targets}},
		parameters, class, gateway, service, endpoints, coreGrant, importGrant, http, grpc,
	} {
		require.NoError(t, c.Create(ctx, object), "%T %s", object, object.GetName())
	}
	assertMembers := func(reason gatewayv1.RouteConditionReason, programmed bool, members ...cloud.Member) {
		t.Helper()
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			for _, route := range []client.Object{http, grpc} {
				if err := c.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
					return false, err
				}
				_, parents, _ := controller.RouteParents(route)
				if len(parents) != 1 || parents[0].ControllerName != cfg.ControllerName {
					return false, nil
				}
				refs := meta.FindStatusCondition(parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
				wantStatus := metav1.ConditionFalse
				switch reason {
				case gatewayv1.RouteReasonResolvedRefs:
					wantStatus = metav1.ConditionTrue
				case gatewayv1.RouteReasonPending:
					wantStatus = metav1.ConditionUnknown
				}
				programmedStatus := metav1.ConditionFalse
				if programmed {
					programmedStatus = metav1.ConditionTrue
				}
				if refs == nil || refs.Status != wantStatus || refs.Reason != string(reason) || refs.ObservedGeneration != route.GetGeneration() ||
					!meta.IsStatusConditionTrue(parents[0].Conditions, "Accepted") ||
					!meta.IsStatusConditionPresentAndEqual(parents[0].Conditions, cfg.Domain()+"/Programmed", programmedStatus) ||
					!serviceImportTestGraph(provider, route, serviceEndpoint, reason == gatewayv1.RouteReasonResolvedRefs, members) {
					return false, nil
				}
			}
			return true, nil
		})
	}
	assertRoutes := func(reason gatewayv1.RouteConditionReason, vip string, programmed bool) {
		t.Helper()
		if vip == "" {
			assertMembers(reason, programmed)
			return
		}
		assertMembers(reason, programmed, cloud.Member{Address: vip, Port: 80, RoutedExternal: true})
	}

	// Start with the API unserved, then let the optional watch discover it.
	assertRoutes(gatewayv1.RouteReasonBackendNotFound, "", true)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(mcscrd.ServiceImportCRD, &crd))
	_, err := envtest.InstallCRDs(restConfig, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{&crd}})
	require.NoError(t, err)
	spec := mcs.ServiceImportSpec{Type: mcs.ClusterSetIP, IPs: []string{initialVIP}, Ports: []mcs.ServicePort{{Name: "h2c", Port: 80, AppProtocol: &h2c}}}
	object := &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: spec}
	require.NoError(t, c.Create(ctx, object))
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)
	t.Run("Headless HTTP and gRPC endpoint lifecycle", func(t *testing.T) {
		const remoteIP = "10.2.0.31"
		remote := mcsEndpointSlice(targets, shared, "east", "h2c", remoteIP, 9080)
		require.NoError(t, c.Create(ctx, remote))
		// Publishing a slice alongside a ClusterSetIP preserves the VIP target.
		assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)
		object.Spec.Type = mcs.Headless
		object.Spec.IPs = nil
		require.NoError(t, c.Update(ctx, object))
		member := cloud.Member{Address: remoteIP, Port: 9080, RoutedExternal: true}
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		remote.Labels[mcs.LabelServiceName] = "another"
		require.NoError(t, c.Update(ctx, remote))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true)
		remote.Labels[mcs.LabelServiceName] = shared
		require.NoError(t, c.Update(ctx, remote))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		remote.Labels[discoveryv1.LabelServiceName] = shared
		delete(remote.Labels, mcs.LabelSourceCluster)
		require.NoError(t, c.Update(ctx, remote))
		// The remaining MCS labels keep the ownerless slice separate from
		// the homonymous Service while the import withdraws its address.
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true)
		remote.Labels[mcs.LabelSourceCluster] = "east"
		delete(remote.Labels, discoveryv1.LabelServiceName)
		require.NoError(t, c.Update(ctx, remote))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		object.Status.EndpointSliceObjects = mcs.EndpointSliceObjectsAbsent
		require.NoError(t, c.Status().Update(ctx, object))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true)
		object.Status.EndpointSliceObjects = mcs.EndpointSliceObjectsPresent
		require.NoError(t, c.Status().Update(ctx, object))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		object.Spec.Ports[0].Port = 81
		require.NoError(t, c.Update(ctx, object))
		assertMembers(gatewayv1.RouteReasonBackendNotFound, true)
		object.Spec.Ports = spec.DeepCopy().Ports
		require.NoError(t, c.Update(ctx, object))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		require.NoError(t, c.Delete(ctx, importGrant))
		assertMembers(gatewayv1.RouteReasonRefNotPermitted, true)
		importGrant = &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-import"}, Spec: importGrant.Spec}
		require.NoError(t, c.Create(ctx, importGrant))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)
		require.NoError(t, c.Delete(ctx, object))
		assertMembers(gatewayv1.RouteReasonBackendNotFound, true)
		object = &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: object.Spec}
		require.NoError(t, c.Create(ctx, object))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true, member)

		require.NoError(t, c.Delete(ctx, remote))
		assertMembers(gatewayv1.RouteReasonResolvedRefs, true)

		object.Spec = *spec.DeepCopy()
		require.NoError(t, c.Update(ctx, object))
		assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)
	})

	ready := mcs.NewServiceImportCondition(mcs.ServiceImportConditionReady, metav1.ConditionFalse, mcs.ServiceImportReasonIPFamilyNotSupported, "fixture: cannot route this address family")
	ready.ObservedGeneration = object.Generation
	object.Status.Conditions = []metav1.Condition{ready}
	require.NoError(t, c.Status().Update(ctx, object))
	assertRoutes(gatewayv1.RouteReasonUnsupportedValue, "", true)
	ready.Status, ready.Reason = metav1.ConditionTrue, string(mcs.ServiceImportReasonReady)
	object.Status.Conditions = []metav1.Condition{ready}
	require.NoError(t, c.Status().Update(ctx, object))
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)

	discovered := probe.discoveryRequests.Load()
	require.Never(t, func() bool { return probe.discoveryRequests.Load() != discovered }, 6*time.Second, 100*time.Millisecond,
		"A stable ServiceImport CRD must not trigger recurring discovery")

	require.NoError(t, c.Delete(ctx, importGrant))
	assertRoutes(gatewayv1.RouteReasonRefNotPermitted, "", true)
	importGrant = &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-import"}, Spec: importGrant.Spec}
	require.NoError(t, c.Create(ctx, importGrant))
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)

	deferred := cloud.RouteProgressingResult("PENDING_UPDATE", time.Second)
	deferred.ObservationDeferred = true
	provider.mu.Lock()
	provider.routeResults = map[string]cloud.RouteResult{string(http.UID): deferred, string(grpc.UID): deferred}
	provider.mu.Unlock()
	object.Spec.IPs = []string{updatedVIP}
	require.NoError(t, c.Update(ctx, object))
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, updatedVIP, false)
	provider.mu.Lock()
	provider.routeResults = nil
	provider.mu.Unlock()
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, updatedVIP, true)

	probe.failGet.Store(true)
	object.Spec.IPs = []string{initialVIP}
	require.NoError(t, c.Update(ctx, object))
	assertRoutes(gatewayv1.RouteReasonPending, "", true)
	require.GreaterOrEqual(t, probe.failedGets.Load(), int64(2))
	probe.failGet.Store(false)
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)

	require.NoError(t, c.Delete(ctx, object))
	assertRoutes(gatewayv1.RouteReasonBackendNotFound, "", true)
	object = &mcs.ServiceImport{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: shared}, Spec: spec}
	require.NoError(t, c.Create(ctx, object))
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)

	if cfg.RouteSessionPersistenceEnabled() {
		http.Spec.Rules = append(http.Spec.Rules, gatewayv1.HTTPRouteRule{
			Name:               ptr.To(gatewayv1.SectionName("persist-import")),
			Matches:            []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Type: ptr.To(gatewayv1.PathMatchExact), Value: ptr.To("/persist")}}},
			BackendRefs:        []gatewayv1.HTTPBackendRef{{BackendRef: imported}},
			SessionPersistence: &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{}}, //nolint:staticcheck // Experimental v1.7 field.
		})
		grpc.Spec.Rules = append(grpc.Spec.Rules, gatewayv1.GRPCRouteRule{
			Name:               ptr.To(gatewayv1.SectionName("persist-import")),
			Matches:            []gatewayv1.GRPCRouteMatch{{Method: &gatewayv1.GRPCMethodMatch{Service: ptr.To("test.Persistence"), Method: ptr.To("Persist")}}},
			BackendRefs:        []gatewayv1.GRPCBackendRef{{BackendRef: imported}},
			SessionPersistence: &gatewayv1.SessionPersistence{Cookie: &gatewayv1.CookieConfig{}}, //nolint:staticcheck // Experimental v1.7 field.
		})
		for _, route := range []client.Object{http, grpc} {
			require.NoError(t, c.Update(ctx, route))
		}
		assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)
		for _, route := range []client.Object{http, grpc} {
			_, parents, _ := controller.RouteParents(route)
			condition := meta.FindStatusCondition(parents[0].Conditions, string(gatewayv1.RouteConditionPartiallyInvalid))
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionTrue, condition.Status)
			require.Contains(t, condition.Message, "persist-import (rule sessionPersistence requires core Service backendRefs)")
		}
	}

	// Toggle the beta served version while alpha keeps the objects available.
	setBetaServed := func(served bool) {
		t.Helper()
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(&crd), &crd))
		for i := range crd.Spec.Versions {
			version := &crd.Spec.Versions[i]
			beta := version.Name == mcs.SchemeGroupVersion.Version
			version.Storage = beta == served
			if beta {
				version.Served = served
			}
		}
		require.NoError(t, c.Update(ctx, &crd))
	}
	setBetaServed(false)
	assertRoutes(gatewayv1.RouteReasonBackendNotFound, "", true)
	setBetaServed(true)
	assertRoutes(gatewayv1.RouteReasonResolvedRefs, initialVIP, true)

	t.Run("RequestMirror destinations", func(t *testing.T) {
		copyBackend := imported.BackendObjectReference
		httpMirror := func(percent int32) gatewayv1.HTTPRouteFilter {
			return gatewayv1.HTTPRouteFilter{Type: gatewayv1.HTTPRouteFilterRequestMirror,
				RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{BackendRef: copyBackend, Percent: &percent}}
		}
		grpcMirror := func(percent int32) gatewayv1.GRPCRouteFilter {
			return gatewayv1.GRPCRouteFilter{Type: gatewayv1.GRPCRouteFilterRequestMirror,
				RequestMirror: &gatewayv1.HTTPRequestMirrorFilter{BackendRef: copyBackend, Percent: &percent}}
		}
		zero := *local.DeepCopy()
		zero.Weight = ptr.To(int32(0))
		http.Spec.Rules = []gatewayv1.HTTPRouteRule{{
			Filters: []gatewayv1.HTTPRouteFilter{httpMirror(23), httpMirror(0)},
			BackendRefs: []gatewayv1.HTTPBackendRef{
				{BackendRef: local, Filters: []gatewayv1.HTTPRouteFilter{httpMirror(41)}},
				{BackendRef: zero, Filters: []gatewayv1.HTTPRouteFilter{httpMirror(99)}},
			},
		}}
		grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{{
			Filters: []gatewayv1.GRPCRouteFilter{grpcMirror(23), grpcMirror(0)},
			BackendRefs: []gatewayv1.GRPCBackendRef{
				{BackendRef: local, Filters: []gatewayv1.GRPCRouteFilter{grpcMirror(41)}},
				{BackendRef: zero, Filters: []gatewayv1.GRPCRouteFilter{grpcMirror(99)}},
			},
		}}
		require.NoError(t, c.Update(ctx, http))
		require.NoError(t, c.Update(ctx, grpc))

		const resolved = gatewayv1.RouteReasonResolvedRefs
		const denied = gatewayv1.RouteReasonRefNotPermitted
		assertMirrors := func(httpReason, grpcReason gatewayv1.RouteConditionReason, members ...cloud.Member) {
			t.Helper()
			waitForEnvtest(t, func(ctx context.Context) (bool, error) {
				for _, entry := range []struct {
					route  client.Object
					reason gatewayv1.RouteConditionReason
				}{{http, httpReason}, {grpc, grpcReason}} {
					if err := c.Get(ctx, client.ObjectKeyFromObject(entry.route), entry.route); err != nil {
						return false, err
					}
					_, parents, _ := controller.RouteParents(entry.route)
					if len(parents) != 1 || !meta.IsStatusConditionTrue(parents[0].Conditions, "Accepted") ||
						!meta.IsStatusConditionTrue(parents[0].Conditions, cfg.Domain()+"/Programmed") {
						return false, nil
					}
					refs := meta.FindStatusCondition(parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs))
					if refs == nil || refs.Reason != string(entry.reason) || refs.ObservedGeneration != entry.route.GetGeneration() ||
						(refs.Status == metav1.ConditionTrue) != (entry.reason == resolved) {
						return false, nil
					}
					wantMembers := members
					if entry.reason != resolved {
						wantMembers = nil
					}
					if !serviceImportMirrorTestGraph(provider, entry.route, serviceEndpoint, wantMembers) {
						return false, nil
					}
				}
				return true, nil
			})
		}
		assertMirrors(resolved, resolved, cloud.Member{Address: initialVIP, Port: 80, RoutedExternal: true})

		bothKinds := slices.Clone(importGrant.Spec.From)
		importGrant.Spec.From = bothKinds[1:]
		require.NoError(t, c.Update(ctx, importGrant))
		assertMirrors(denied, resolved, cloud.Member{Address: initialVIP, Port: 80, RoutedExternal: true})
		require.NoError(t, c.Delete(ctx, importGrant))
		assertMirrors(denied, denied)
		gets := probe.backendGets.Load()
		require.Never(t, func() bool { return probe.backendGets.Load() != gets }, 500*time.Millisecond, 20*time.Millisecond)

		importGrant = &gatewayv1.ReferenceGrant{ObjectMeta: metav1.ObjectMeta{Namespace: targets, Name: "allow-import"}, Spec: importGrant.Spec}
		importGrant.Spec.From = bothKinds
		require.NoError(t, c.Create(ctx, importGrant))
		assertMirrors(resolved, resolved, cloud.Member{Address: initialVIP, Port: 80, RoutedExternal: true})

		remote := mcsEndpointSlice(targets, shared, "east", "h2c", "10.2.0.31", 9080)
		remote.Labels[discoveryv1.LabelServiceName] = shared
		require.NoError(t, c.Create(ctx, remote))
		object.Spec.Type, object.Spec.IPs = mcs.Headless, nil
		require.NoError(t, c.Update(ctx, object))
		assertMirrors(resolved, resolved, cloud.Member{Address: "10.2.0.31", Port: 9080, RoutedExternal: true})

		remote.Ports[0].Port = ptr.To(int32(9081))
		remote.Endpoints[0].Addresses[0] = "10.2.0.32"
		require.NoError(t, c.Update(ctx, remote))
		assertMirrors(resolved, resolved, cloud.Member{Address: "10.2.0.32", Port: 9081, RoutedExternal: true})
		remote.Endpoints[0].Conditions.Ready = ptr.To(false)
		require.NoError(t, c.Update(ctx, remote))
		assertMirrors(resolved, resolved)

		remote.Endpoints[0].Conditions.Ready = ptr.To(true)
		require.NoError(t, c.Update(ctx, remote))
		assertMirrors(resolved, resolved, cloud.Member{Address: "10.2.0.32", Port: 9081, RoutedExternal: true})

		manager := remote.Labels[discoveryv1.LabelManagedBy]
		delete(remote.Labels, discoveryv1.LabelManagedBy)
		require.NoError(t, c.Update(ctx, remote))
		assertMirrors(resolved, resolved)
		remote.Labels[discoveryv1.LabelManagedBy] = manager
		require.NoError(t, c.Update(ctx, remote))
		assertMirrors(resolved, resolved, cloud.Member{Address: "10.2.0.32", Port: 9081, RoutedExternal: true})

		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(http), http))
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(grpc), grpc))
		http.Spec.Rules = []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: local}, {BackendRef: imported}}}}
		grpc.Spec.Rules = []gatewayv1.GRPCRouteRule{{BackendRefs: []gatewayv1.GRPCBackendRef{{BackendRef: local}, {BackendRef: imported}}}}
		require.NoError(t, c.Update(ctx, http))
		require.NoError(t, c.Update(ctx, grpc))
	})

	require.NoError(t, c.Delete(ctx, &crd))
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		err := c.Get(ctx, client.ObjectKeyFromObject(&crd), &crd)
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	assertRoutes(gatewayv1.RouteReasonBackendNotFound, "", true)
}

func serviceImportTestGraph(provider *recordingProvider, route client.Object, serviceEndpoint string, resolved bool, members []cloud.Member) bool {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, observation := range slices.Backward(provider.graphSpecs) {
		for _, node := range observation.Routes {
			if node.Spec.Identity.RouteUID != string(route.GetUID()) {
				continue
			}
			spec := node.Spec
			_, grpc := route.(*gatewayv1.GRPCRoute)
			if node.Disposition != cloud.GraphEnsure || spec.GRPC != grpc || len(spec.WeightedHTTPRules) != 1 {
				return false
			}
			targets := spec.WeightedHTTPRules[0].Targets
			if len(targets) != 2 || targets[0].Weight != 2 || targets[1].Weight != 3 || targets[0].BackendIndex == nil {
				return false
			}
			service := spec.HTTPBackends[*targets[0].BackendIndex]
			if service.Kind != "" || service.RoutedExternal || len(service.Members) != 1 ||
				service.Members[0].Address != serviceEndpoint || service.Members[0].Port != 8080 {
				return false
			}
			if !resolved {
				return targets[1].BackendIndex == nil
			}
			if targets[1].BackendIndex == nil {
				return false
			}
			imported := spec.HTTPBackends[*targets[1].BackendIndex]
			return imported.Kind == cloud.HTTPBackendKindServiceImport && imported.RoutedExternal && imported.BackendH2C &&
				imported.Namespace == service.Namespace && imported.Name == service.Name && imported.Port == service.Port &&
				identity.HTTPBackendPoolKey(imported) != identity.HTTPBackendPoolKey(service) &&
				imported.Unavailable == (len(members) == 0) && slices.Equal(imported.Members, members)
		}
	}
	return false
}

func serviceImportMirrorTestGraph(provider *recordingProvider, route client.Object, serviceEndpoint string, members []cloud.Member) bool {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, observation := range slices.Backward(provider.graphSpecs) {
		for _, node := range observation.Routes {
			spec := node.Spec
			if spec.Identity.RouteUID != string(route.GetUID()) {
				continue
			}
			_, grpc := route.(*gatewayv1.GRPCRoute)
			if node.Disposition != cloud.GraphEnsure || spec.GRPC != grpc || len(spec.WeightedHTTPRules) != 1 {
				return false
			}
			rule := spec.WeightedHTTPRules[0]
			if len(rule.Matches) != 1 || len(rule.Targets) != 2 || rule.Targets[0].Weight != 2 || rule.Targets[1].Weight != 0 ||
				rule.Targets[0].BackendIndex == nil || len(rule.Targets[1].RequestMirrors) != 0 {
				return false
			}
			service := spec.HTTPBackends[*rule.Targets[0].BackendIndex]
			if service.Kind != "" || service.RoutedExternal || len(service.Members) != 1 ||
				service.Members[0].Address != serviceEndpoint || service.Members[0].Port != 8080 {
				return false
			}
			ruleCopies, backendCopies := rule.Matches[0].RequestMirrors, rule.Targets[0].RequestMirrors
			if len(members) == 0 {
				return len(ruleCopies) == 0 && len(backendCopies) == 0 && len(spec.HTTPBackends) == 1
			}
			if len(ruleCopies) != 1 || len(backendCopies) != 1 || ruleCopies[0].BackendIndex != backendCopies[0].BackendIndex ||
				ruleCopies[0].Numerator != 23 || ruleCopies[0].Denominator != 100 ||
				backendCopies[0].Numerator != 41 || backendCopies[0].Denominator != 100 {
				return false
			}
			imported := spec.HTTPBackends[ruleCopies[0].BackendIndex]
			return imported.Kind == cloud.HTTPBackendKindServiceImport && imported.RoutedExternal && imported.BackendH2C &&
				imported.Namespace == service.Namespace && imported.Name == service.Name && imported.Port == service.Port &&
				identity.HTTPBackendPoolKey(imported) != identity.HTTPBackendPoolKey(service) && slices.Equal(imported.Members, members)
		}
	}
	return false
}

func mcsEndpointSlice(namespace, service, cluster, portName, address string, port int32) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: service + "-" + cluster, Labels: map[string]string{
			mcs.LabelServiceName: service, mcs.LabelSourceCluster: cluster, discoveryv1.LabelManagedBy: "mcs.example.test",
		}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To(portName), Port: ptr.To(port)}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{address}}},
	}
}
