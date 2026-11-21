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

package gateway

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestGatewayRejectsMissingComponentOutcome(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Status = gatewayv1.GatewayStatus{}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{result: cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}}}, nil)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err == nil {
		t.Fatal("missing Gateway component outcome was accepted")
	}
	current := &gatewayv1.Gateway{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Status, gateway.Status) {
		t.Fatalf("invalid component result changed status: %#v", current.Status)
	}
}

func TestGatewayDoesNotReportNewHostnameProgrammedWhileRoutesProgress(t *testing.T) {
	cfg := testConfig()
	gateway := programmedTestGateway(cfg)
	gateway.Generation++
	gateway.Spec.Listeners[0].Hostname = ptr.To(gatewayv1.Hostname("*.example.test"))
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	result := cloud.GatewayGraphResult{
		Outcome: cloud.ProgressingOutcome("Disabling Octavia routing policy", time.Second),
		Gateway: cloud.GatewayResult{
			Outcome: cloud.ReadyOutcome(),
			State:   cloud.GatewayState{LoadBalancerID: "load-balancer", ListenerID: "listener", VIPAddress: "192.0.2.10"},
		},
	}
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{result: result}}, nil)
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}); err != nil {
		t.Fatal(err)
	}
	current := &gatewayv1.Gateway{}
	if err := kubeClient.Get(t.Context(), client.ObjectKeyFromObject(gateway), current); err != nil {
		t.Fatal(err)
	}
	accepted := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != gateway.Generation ||
		programmed == nil || (programmed.Status == metav1.ConditionTrue && programmed.ObservedGeneration == gateway.Generation) {
		t.Fatalf("new hostname should be accepted while its policy update is pending: %v", current.Status.Conditions)
	}
}

func TestGatewayReportsProvenUDPListenerWhenSiblingFails(t *testing.T) {
	cfg := testConfig()
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{
			{Name: "dns", Protocol: gatewayv1.UDPProtocolType, Port: 53},
			{Name: "radius", Protocol: gatewayv1.UDPProtocolType, Port: 1812},
		}},
	}
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Coordinator: &graph.Coordinator{}}, &recordingProvider{})
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)}
	if result, err := reconciler.Reconcile(t.Context(), request); err != nil || !result.Requeue {
		t.Fatalf("binding checkpoint = %v, %v", result, err)
	}
	component := cloud.GatewayProgressingResult("Waiting for the Gateway UDP listeners", time.Second)
	component.State = cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: "192.0.2.10"}
	component.UDPReadyPorts = []int{53}
	providerErr := cloud.NewProviderError(cloud.ErrorCategoryResourceFailure, errors.New("Octavia UDP listener provisioning status is ERROR"))
	reconciler.Writer = fixedGatewayGraph{result: cloud.GatewayGraphResult{Gateway: component}, err: providerErr}
	if result, err := reconciler.Reconcile(t.Context(), request); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("provider failure = %v, %v", result, err)
	}
	var current gatewayv1.Gateway
	if err := kubeClient.Get(t.Context(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	programmed := meta.FindStatusCondition(current.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.Reason != "ResourceFailed" {
		t.Fatalf("aggregate failure condition = %#v", programmed)
	}
	if len(current.Status.Listeners) != 2 ||
		!meta.IsStatusConditionTrue(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) ||
		!meta.IsStatusConditionFalse(current.Status.Listeners[1].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("listener conditions = %+v", current.Status.Listeners)
	}
	if len(current.Status.Addresses) != 1 || current.Status.Addresses[0].Value != "192.0.2.10" {
		t.Fatalf("proven address = %+v", current.Status.Addresses)
	}

	reconciler.Writer = fixedGatewayGraph{err: providerErr}
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(t.Context(), request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionFalse(current.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionProgrammed)) {
		t.Fatalf("listener inherited historical proof after an unproven failure: %+v", current.Status.Listeners[0])
	}
}

func TestGatewayRouteFailurePreservesStatusAndRedactsDetails(t *testing.T) {
	for _, category := range []cloud.ErrorCategory{cloud.ErrorCategoryRateLimit, cloud.ErrorCategoryAuthorization, ""} {
		t.Run(string(category), func(t *testing.T) {
			cfg := testConfig()
			gateway := programmedTestGateway(cfg)
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
			current := &gatewayv1.Gateway{}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
				t.Fatal(err)
			}
			before := current.DeepCopy()
			cause := errors.New("response=private-body token=secret-token")
			providerErr := cause
			if category != "" {
				providerErr = cloud.NewProviderError(category, cause)
			}
			graphErr := &cloud.GraphError{Identity: cloud.Identity{RouteUID: "route-uid"}, Err: providerErr}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg, Writer: fixedGatewayGraph{err: graphErr}}, nil)
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gateway)})
			if category == cloud.ErrorCategoryAuthorization {
				if err != nil || result.RequeueAfter <= 0 {
					t.Fatalf("authorization result=%#v error=%v", result, err)
				}
			} else if !errors.Is(err, cause) || strings.Contains(err.Error(), "private-body") || strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("route error=%v", err)
			}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(gateway), current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(current.Status, before.Status) {
				t.Fatalf("route failure changed Gateway status: before=%#v after=%#v", before.Status, current.Status)
			}
		})
	}
}

type fixedGatewayGraph struct {
	result cloud.GatewayGraphResult
	err    error
}

func (w fixedGatewayGraph) ReconcileGraph(context.Context, graph.Request) (cloud.GatewayGraphResult, error) {
	return w.result, w.err
}
