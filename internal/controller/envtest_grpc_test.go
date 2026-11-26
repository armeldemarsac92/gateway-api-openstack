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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/yaml"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testGRPCExactMethodRoute(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider) {
	t.Helper()
	ctx := t.Context()
	const namespace = "grpc-exact-method"
	parameters := envtestClassParameters(namespace)
	parameters.Spec.MemberMode = configv1alpha1.MemberModeEndpoint
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
		Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName, ParametersRef: &gatewayv1.ParametersReference{
			Group: configv1alpha1.GroupName, Kind: configv1alpha1.GatewayClassConfigKind, Name: parameters.Name,
		}},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "same-namespace"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(class.Name),
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	for _, object := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, parameters, class, gateway} {
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	wantBackend := map[string]string{
		"grpc-infra-backend-v1": "/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo",
		"grpc-infra-backend-v2": "/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/EchoTwo",
	}
	for backend := range wantBackend {
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: backend},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "grpc", Port: 8080, AppProtocol: ptr.To("kubernetes.io/h2c")}}},
		}
		endpoints := &discoveryv1.EndpointSlice{
			ObjectMeta:  metav1.ObjectMeta{Namespace: namespace, Name: backend, Labels: map[string]string{discoveryv1.LabelServiceName: backend}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("grpc"), Port: ptr.To(int32(8080))}},
			Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.11"}}},
		}
		for _, object := range []client.Object{service, endpoints} {
			if err := c.Create(ctx, object); err != nil {
				t.Fatal(err)
			}
		}
	}

	data, err := conformance.Manifests.ReadFile(tests.GRPCExactMethodMatching.Manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	route := &gatewayv1.GRPCRoute{}
	if err := yaml.Unmarshal(data, route); err != nil {
		t.Fatal(err)
	}
	route.Namespace = namespace
	if err := c.Create(ctx, route); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(route)
	defer func() {
		if t.Failed() {
			t.Logf("last observed GRPCRoute status: %+v", route.Status)
		}
	}()
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, key, route); err != nil {
			return false, err
		}
		if len(route.Status.Parents) != 1 || route.Status.Parents[0].ControllerName != cfg.ControllerName {
			return false, nil
		}
		conditions := route.Status.Parents[0].Conditions
		for _, name := range []string{"Accepted", "ResolvedRefs", cfg.Domain() + "/Programmed"} {
			condition := meta.FindStatusCondition(conditions, name)
			if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != route.Generation {
				return false, nil
			}
		}
		return true, nil
	})
	if route.Annotations[cfg.RouteLifecycleAnnotation()] != controller.RouteAttached {
		t.Fatalf("GRPCRoute lacks its durable Gateway binding: %v", route.Annotations)
	}
	if err := c.Get(ctx, key, &gatewayv1.HTTPRoute{}); !apierrors.IsNotFound(err) {
		t.Fatalf("no HTTPRoute should be written for the GRPCRoute: %v", err)
	}
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(gateway), gateway); err != nil {
			return false, err
		}
		return len(gateway.Status.Listeners) == 1 && gateway.Status.Listeners[0].AttachedRoutes == 1, nil
	})

	var desired *cloud.RouteSpec
	provider.mu.Lock()
	for _, observation := range provider.graphSpecs {
		for _, node := range observation.Routes {
			if node.Spec.Identity.RouteUID == string(route.UID) && node.Disposition == cloud.GraphEnsure {
				copy := node.Spec
				desired = &copy
			}
		}
	}
	provider.mu.Unlock()
	if desired == nil || desired.Identity.GatewayUID != string(gateway.UID) || len(desired.HTTPBackends) != len(wantBackend) || !desired.GRPC {
		t.Fatalf("shared Gateway writer did not observe both exact method rules with TCP health checks: %#v", desired)
	}
	for _, backend := range desired.HTTPBackends {
		if len(backend.Matches) != 1 || backend.Matches[0].PathType != cloud.PathMatchExact ||
			backend.Matches[0].PathValue != wantBackend[backend.Name] || !backend.BackendH2C || len(backend.Members) != 1 {
			t.Errorf("exact method backend %s has incorrect desired graph: %#v", backend.Name, backend)
		}
	}

	if !cfg.XBackendEnabled() {
		supported := string(route.Spec.Rules[0].BackendRefs[0].Name)
		unsupported := string(route.Spec.Rules[1].BackendRefs[0].Name)
		route.Spec.Rules[1].BackendRefs[0].BackendObjectReference = gatewayv1.BackendObjectReference{
			Group: ptr.To(gatewayv1.Group(gatewayx.GroupName)), Kind: ptr.To(gatewayv1.Kind("XBackend")), Name: "experimental-kind",
		}
		if err := c.Update(ctx, route); err != nil {
			t.Fatal(err)
		}
		waitForEnvtest(t, func(ctx context.Context) (bool, error) {
			if err := c.Get(ctx, key, route); err != nil {
				return false, err
			}
			if len(route.Status.Parents) != 1 {
				return false, nil
			}
			conditions := route.Status.Parents[0].Conditions
			programmed := meta.FindStatusCondition(conditions, cfg.Domain()+"/Programmed")
			resolved := meta.FindStatusCondition(conditions, string(gatewayv1.RouteConditionResolvedRefs))
			if programmed == nil || programmed.Status != metav1.ConditionTrue || programmed.ObservedGeneration != route.Generation ||
				resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.ObservedGeneration != route.Generation ||
				resolved.Reason != string(gatewayv1.RouteReasonInvalidKind) {
				return false, nil
			}
			provider.mu.Lock()
			defer provider.mu.Unlock()
			for _, observation := range slices.Backward(provider.graphSpecs) {
				for _, node := range observation.Routes {
					spec := node.Spec
					if spec.Identity.RouteUID != string(route.UID) || node.Disposition != cloud.GraphEnsure {
						continue
					}
					return spec.GRPC && len(spec.HTTPBackends) == 1 && spec.HTTPBackends[0].Kind == "" &&
						spec.HTTPBackends[0].Name == supported && spec.HTTPBackends[0].BackendH2C && len(spec.HTTPBackends[0].Members) == 1 &&
						len(spec.HTTPBackends[0].Matches) == 1 && spec.HTTPBackends[0].Matches[0].PathValue == wantBackend[supported] &&
						len(spec.FixedHTTPRules) == 1 && len(spec.FixedHTTPRules[0].Matches) == 1 &&
						spec.FixedHTTPRules[0].Matches[0].PathValue == wantBackend[unsupported], nil
				}
			}
			return false, nil
		})
	}

	// This younger catch-all must wait for the GRPCRoute to detach. Otherwise
	// it could consume the unknown RPC that should reach the default 404.
	fallback := envtestHTTPRoute(namespace, "z-http-fallback", gateway.Name, "grpc-infra-backend-v1")
	fallback.Spec.Rules[0].BackendRefs[0].Port = ptr.To(gatewayv1.PortNumber(8080))
	if err := c.Create(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(fallback), fallback); err != nil {
			return false, err
		}
		if len(fallback.Status.Parents) != 1 {
			return false, nil
		}
		accepted := meta.FindStatusCondition(fallback.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionAccepted))
		return accepted != nil && accepted.Status == metav1.ConditionFalse && accepted.Reason == string(gatewayv1.RouteReasonNotAllowedByListeners), nil
	})
	if err := c.Delete(ctx, route); err != nil {
		t.Fatal(err)
	}
	waitForEnvtest(t, func(ctx context.Context) (bool, error) {
		err := c.Get(ctx, key, &gatewayv1.GRPCRoute{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
}
