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
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func TestGatewayReconcileProgramsAddressAndConditions(t *testing.T) {
	scheme := testScheme(t)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: testConfig().ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
		Status: gatewayv1.GatewayStatus{Listeners: []gatewayv1.ListenerStatus{{Name: "http", Conditions: []metav1.Condition{{
			Type: "example.net/Custom", Status: metav1.ConditionTrue, Reason: "Preserve", LastTransitionTime: metav1.Now(),
		}}}}},
	}
	client := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(class, gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: client, Coordinator: &graph.Coordinator{}, APIReader: client, Config: testConfig()}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "edge"}}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result != (ctrl.Result{Requeue: true}) || len(provider.gatewaySpecs) != 0 {
		t.Fatalf("binding checkpoint result = %#v, EnsureGateway calls = %d", result, len(provider.gatewaySpecs))
	}
	secondResult, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	wantResync := controller.OpenStackResyncAfter(reconciler.Config.OpenStackResyncInterval, gateway.UID)
	if secondResult.RequeueAfter != wantResync {
		t.Fatalf("second Reconcile() RequeueAfter = %s, want %s", secondResult.RequeueAfter, wantResync)
	}
	var got gatewayv1.Gateway
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "edge"}, &got); err != nil {
		t.Fatal(err)
	}
	programmed := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if programmed == nil || programmed.Status != metav1.ConditionTrue {
		t.Fatalf("Programmed condition = %#v", programmed)
	}
	if len(got.Status.Addresses) != 1 || got.Status.Addresses[0].Value != "192.0.2.10" {
		t.Fatalf("Gateway addresses = %#v", got.Status.Addresses)
	}
	if len(got.Status.Listeners) != 1 || meta.FindStatusCondition(got.Status.Listeners[0].Conditions, "example.net/Custom") == nil {
		t.Fatalf("custom Listener condition was not preserved: %#v", got.Status.Listeners)
	}
	if len(provider.gatewaySpecs) != 1 || provider.gatewaySpecs[0].Provider != "amphora" {
		t.Fatalf("gateway specs = %#v", provider.gatewaySpecs)
	}
	resourceVersion := got.ResourceVersion
	thirdResult, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("third Reconcile() error = %v", err)
	}
	if thirdResult.RequeueAfter != wantResync || len(provider.gatewaySpecs) != 2 {
		t.Fatalf("third Reconcile() result = %#v, EnsureGateway calls = %d", thirdResult, len(provider.gatewaySpecs))
	}
	if err := client.Get(context.Background(), request.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.ResourceVersion != resourceVersion {
		t.Fatalf("converged resync changed Gateway resourceVersion from %s to %s", resourceVersion, got.ResourceVersion)
	}
}

func TestGatewayProgramsEmptyIPAddressUsingClassTopology(t *testing.T) {
	const externalNetworkID = "55555555-5555-5555-5555-555555555555"
	for _, tc := range []struct {
		name            string
		addressType     *gatewayv1.AddressType
		externalNetwork string
		floatingIP      string
		wantAddress     string
	}{
		{name: "default type and private VIP", wantAddress: "192.0.2.11"},
		{name: "explicit type and floating IP", addressType: ptr.To(gatewayv1.IPAddressType),
			externalNetwork: externalNetworkID, floatingIP: "198.51.100.42", wantAddress: "198.51.100.42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			parameters := testClassParameters()
			if tc.externalNetwork != "" {
				parameters.Spec.FloatingIP = configv1alpha1.FloatingIPConfig{
					Mode: configv1alpha1.FloatingIPAllocate, ExternalNetwork: &configv1alpha1.ResourceReference{ID: tc.externalNetwork},
				}
			}
			gateway := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid"},
				Spec: gatewayv1.GatewaySpec{
					GatewayClassName: "openstack",
					Addresses:        []gatewayv1.GatewaySpecAddress{{Type: tc.addressType}},
					Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
				},
			}
			kubeClient := indexedFakeClientBuilder(testScheme(t), cfg).WithStatusSubresource(gateway).WithObjects(class, parameters, gateway).Build()
			ready := cloud.GatewayReadyResult(cloud.GatewayState{
				LoadBalancerID: "lb-1", ListenerID: "listener-1", VIPAddress: "192.0.2.11", FloatingIPAddress: tc.floatingIP,
			})
			provider := &recordingProvider{gatewayResult: &ready}
			reconciler := newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}
			if result, err := reconciler.Reconcile(t.Context(), request); err != nil || !result.Requeue || len(provider.gatewaySpecs) != 0 {
				t.Fatalf("binding checkpoint = %#v, error = %v, EnsureGateway calls = %d", result, err, len(provider.gatewaySpecs))
			}
			if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if len(provider.gatewaySpecs) != 1 {
				t.Fatalf("EnsureGateway calls = %d, want one", len(provider.gatewaySpecs))
			}
			spec := provider.gatewaySpecs[0]
			if spec.Identity.GatewayUID != string(gateway.UID) || spec.VIPSubnetID != testVIPSubnetID || spec.ExternalNetworkID != tc.externalNetwork {
				t.Fatalf("provider received unexpected Gateway identity or topology: %#v", spec)
			}
			if err := kubeClient.Get(t.Context(), request.NamespacedName, gateway); err != nil {
				t.Fatal(err)
			}
			if !meta.IsStatusConditionTrue(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)) {
				t.Fatalf("Gateway conditions = %#v", gateway.Status.Conditions)
			}
			got := gateway.Status.Addresses
			if len(got) != 1 || got[0].Type == nil || *got[0].Type != gatewayv1.IPAddressType || got[0].Value != tc.wantAddress {
				t.Fatalf("Gateway addresses = %#v, want IPAddress %s", got, tc.wantAddress)
			}
		})
	}
}

func TestGatewayInvalidSpecCleanupPreservesTopologyForRecovery(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	cfg.GatewayAPIChannel = "experimental"
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	version := Condition(string(gatewayv1.GatewayClassConditionStatusSupportedVersion), metav1.ConditionFalse, string(gatewayv1.GatewayClassReasonUnsupportedVersion), "unrelated bundle skew", class.Generation)
	class.Status.Conditions = []metav1.Condition{version}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Finalizers: []string{cfg.GatewayFinalizer()}, Annotations: gatewayBindingAnnotations(cfg, "443")},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 443,
				Filters: &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{{
					Type:         gatewayv1.ListenerFilterExtensionRef,
					ExtensionRef: &gatewayv1.LocalObjectReference{Group: "example.net", Kind: "Policy", Name: "authorization"},
				}}},
			}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, cfg).WithStatusSubresource(class, gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: liveReaderWithUnsupportedVersion(kubeClient), Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	} else if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedGateways) != 0 {
		t.Fatalf("DeleteGateway calls before status checkpoint = %d, want 0", len(provider.deletedGateways))
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(provider.deletedGateways) != 1 || provider.deletedGateways[0].GatewayUID != string(gateway.UID) {
		t.Fatalf("deleted Gateway identities = %#v", provider.deletedGateways)
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.GatewayFinalizer()) || !controller.GatewayTopologyMetadataEqual(cfg, gateway, &got) {
		t.Fatal("cleanup discarded the managed Gateway's frozen topology")
	}
	accepted := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse {
		t.Fatalf("Accepted condition = %#v", accepted)
	}
	if err := kubeClient.Delete(context.Background(), testClassParameters()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(t.Context(), types.NamespacedName{Name: class.Name}, class); err != nil {
		t.Fatal(err)
	}
	class.Status.Conditions[0].Status = metav1.ConditionTrue
	class.Status.Conditions[0].Reason = string(gatewayv1.GatewayClassReasonSupportedVersion)
	if err := kubeClient.Status().Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	got.Spec.Listeners[0].Filters = nil
	got.Generation++
	if err := kubeClient.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	// A fresh reconciler recovers without resolving the deleted parameter object.
	restarted := newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := restarted.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("recover Gateway after invalid spec: %v", err)
	}
	if len(provider.gatewaySpecs) != 1 || provider.gatewaySpecs[0].VIPSubnetID != testVIPSubnetID {
		t.Fatalf("recovered Gateway specs = %#v", provider.gatewaySpecs)
	}
}

func TestGatewayInvalidSpecPublishesStatusBeforeCleanupRetry(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Finalizers: []string{cfg.GatewayFinalizer()}, Annotations: gatewayBindingAnnotations(cfg, "443")},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "https", Protocol: gatewayv1.HTTPSProtocolType, Port: 443}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{gatewayDeleteErr: errors.New("temporary delete failure")}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	} else if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("first Reconcile() result = %#v, want status checkpoint", result)
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	accepted := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.GatewayReasonListenersNotValid) {
		t.Fatalf("Accepted condition = %#v", accepted)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.GatewayFinalizer()) {
		t.Fatal("cleanup failure removed the safety finalizer")
	}
	if len(provider.deletedGateways) != 0 {
		t.Fatalf("DeleteGateway calls before status checkpoint = %d, want 0", len(provider.deletedGateways))
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err == nil {
		t.Fatal("second Reconcile() unexpectedly succeeded")
	}
}

func TestGatewayClassHandoffCleansControllerResources(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: "other.example/controller"}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid", Finalizers: []string{cfg.GatewayFinalizer()}, Annotations: gatewayBindingAnnotations(cfg, "80")},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "other",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(provider.deletedGateways) != 1 {
		t.Fatalf("DeleteGateway calls = %d, want 1", len(provider.deletedGateways))
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&got, cfg.GatewayFinalizer()) {
		t.Fatalf("Gateway finalizers = %#v", got.Finalizers)
	}
}

func TestGatewayListenerPortChangeReplacesOwnedGraph(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid",
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 8080}},
		},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	parameters := testClassParameters()
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: parameters.Name}, parameters); err != nil {
		t.Fatal(err)
	}
	parameters.Spec.VIPSubnet.ID = "44444444-4444-4444-4444-444444444444"
	parameters.Generation++
	if err := kubeClient.Update(context.Background(), parameters); err != nil {
		t.Fatal(err)
	}
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result != (ctrl.Result{Requeue: true}) {
		t.Fatalf("Reconcile() result = %#v, want status checkpoint", result)
	}
	if len(provider.deletedGateways) != 0 || len(provider.gatewaySpecs) != 0 {
		t.Fatalf("replacement deletes = %d, ensures = %d", len(provider.deletedGateways), len(provider.gatewaySpecs))
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(provider.deletedGateways) != 1 || len(provider.gatewaySpecs) != 0 {
		t.Fatalf("replacement deletes = %d, ensures = %d", len(provider.deletedGateways), len(provider.gatewaySpecs))
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.GatewayFinalizer()) || got.Annotations[cfg.GatewayListenerPortAnnotation()] != "8080" || !controller.GatewayTopologyMetadataEqual(cfg, gateway, &got) {
		t.Fatalf("Gateway replacement metadata = finalizers %#v, annotations %#v", got.Finalizers, got.Annotations)
	}
	restarted := newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := restarted.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("third Reconcile() error = %v", err)
	}
	if len(provider.gatewaySpecs) != 1 || provider.gatewaySpecs[0].ListenerPort != 8080 || provider.gatewaySpecs[0].VIPSubnetID != testVIPSubnetID {
		t.Fatalf("Gateway ensures = %#v", provider.gatewaySpecs)
	}
}

func TestGatewayBindingRejectsClusterIdentityDrift(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid",
			Finalizers: []string{cfg.GatewayFinalizer()},
			Annotations: map[string]string{
				cfg.GatewayListenerPortAnnotation(): "80",
				cfg.GatewayClusterIDAnnotation():    "original-cluster",
				cfg.GatewayProjectIDAnnotation():    cfg.OpenStackProjectID,
			},
		},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	provider := &recordingProvider{}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(provider.gatewaySpecs) != 0 || len(provider.deletedGateways) != 0 {
		t.Fatalf("provider calls after identity drift: ensures %d, deletes %d", len(provider.gatewaySpecs), len(provider.deletedGateways))
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	accepted := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionFalse || accepted.Reason != string(gatewayv1.GatewayReasonInvalidParameters) {
		t.Fatalf("Accepted condition = %#v", accepted)
	}
	if !controllerutil.ContainsFinalizer(&got, cfg.GatewayFinalizer()) {
		t.Fatal("identity drift removed the safety finalizer")
	}
}

func TestGatewayOpenStackFailurePreservesAttachedRouteCount(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1,
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}},
	}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "api", Generation: 1},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
			Name: "edge",
		}}}},
		Status: gatewayv1.HTTPRouteStatus{RouteStatus: gatewayv1.RouteStatus{Parents: []gatewayv1.RouteParentStatus{{
			ParentRef: gatewayv1.ParentReference{Name: "edge"}, ControllerName: cfg.ControllerName,
			Conditions: []metav1.Condition{{
				Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionTrue,
				ObservedGeneration: 1, Reason: string(gatewayv1.RouteReasonAccepted), LastTransitionTime: metav1.Now(),
			}},
		}}}},
	}
	wrongPort := gatewayv1.PortNumber(81)
	otherRoute := route.DeepCopy()
	otherRoute.Name = "wrong-port"
	otherRoute.Status.Parents[0].ParentRef.Port = &wrongPort
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway, route, otherRoute).WithObjects(class, gateway, route, otherRoute).Build()
	provider := &recordingProvider{gatewayErr: cloud.NewProviderError(cloud.ErrorCategoryRetryableService, errors.New("temporary Octavia failure"))}
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, Coordinator: &graph.Coordinator{}, APIReader: kubeClient, Config: cfg}, provider)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}); err == nil {
		t.Fatal("Reconcile() unexpectedly succeeded")
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Listeners) != 1 || got.Status.Listeners[0].AttachedRoutes != 1 {
		t.Fatalf("Listener status = %#v", got.Status.Listeners)
	}
}

func TestGatewayRefusedAddressesPublishSpecificConditions(t *testing.T) {
	emptyHostname := gatewayv1.GatewaySpecAddress{Type: ptr.To(gatewayv1.HostnameAddressType)}
	staticIP := gatewayv1.GatewaySpecAddress{Value: "192.0.2.20"}
	for _, tc := range []struct {
		name            string
		addresses       []gatewayv1.GatewaySpecAddress
		invalidListener bool
		accepted        gatewayv1.GatewayConditionReason
		programmed      gatewayv1.GatewayConditionReason
	}{
		{name: "static IP", addresses: []gatewayv1.GatewaySpecAddress{staticIP},
			accepted: gatewayv1.GatewayReasonAccepted, programmed: gatewayv1.GatewayReasonAddressNotUsable},
		{name: "multiple static IPs", addresses: []gatewayv1.GatewaySpecAddress{staticIP, {Value: "192.0.2.21"}},
			accepted: gatewayv1.GatewayReasonAccepted, programmed: gatewayv1.GatewayReasonAddressNotUsable},
		{name: "empty hostname", addresses: []gatewayv1.GatewaySpecAddress{emptyHostname},
			accepted: gatewayv1.GatewayReasonUnsupportedAddress, programmed: gatewayv1.GatewayReasonAddressNotAssigned},
		{name: "static IP with empty hostname", addresses: []gatewayv1.GatewaySpecAddress{staticIP, emptyHostname},
			accepted: gatewayv1.GatewayReasonUnsupportedAddress, programmed: gatewayv1.GatewayReasonAddressNotAssigned},
		{name: "multiple dynamic addresses", addresses: []gatewayv1.GatewaySpecAddress{{}, {}},
			accepted: gatewayv1.GatewayReasonInvalid, programmed: gatewayv1.GatewayReasonAddressNotAssigned},
		{name: "cluster routability", addresses: []gatewayv1.GatewaySpecAddress{{Routability: gatewayv1.GatewayAddressRoutabilityCluster}},
			accepted: gatewayv1.GatewayReasonInvalid, programmed: gatewayv1.GatewayReasonAddressNotAssigned},
		{name: "static IP with invalid listener", addresses: []gatewayv1.GatewaySpecAddress{staticIP}, invalidListener: true,
			accepted: gatewayv1.GatewayReasonListenersNotValid, programmed: gatewayv1.GatewayReasonInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := testScheme(t)
			cfg := testConfig()
			class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
			gateway := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid"},
				Spec: gatewayv1.GatewaySpec{
					GatewayClassName: "openstack",
					Addresses:        tc.addresses,
					Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
				},
			}
			if tc.invalidListener {
				gateway.Spec.Listeners[0].Protocol = gatewayv1.HTTPSProtocolType
			}
			kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
			provider := &recordingProvider{}
			reconciler := *newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, provider)
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}
			for range 2 {
				if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
					t.Fatalf("Reconcile() error = %v", err)
				}
			}
			var got gatewayv1.Gateway
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
				t.Fatal(err)
			}
			accepted := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionAccepted))
			wantAccepted := metav1.ConditionFalse
			if tc.accepted == gatewayv1.GatewayReasonAccepted {
				wantAccepted = metav1.ConditionTrue
			}
			if accepted == nil || accepted.Status != wantAccepted || accepted.Reason != string(tc.accepted) {
				t.Fatalf("Gateway Accepted condition = %#v, want %s with reason %s", accepted, wantAccepted, tc.accepted)
			}
			programmed := meta.FindStatusCondition(got.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
			if programmed == nil || programmed.Status != metav1.ConditionFalse || programmed.Reason != string(tc.programmed) {
				t.Fatalf("Gateway Programmed condition = %#v, want False with reason %s", programmed, tc.programmed)
			}
			if len(got.Status.Addresses) != 0 {
				t.Fatalf("rejected Gateway addresses = %#v", got.Status.Addresses)
			}
			if len(got.Status.Listeners) != 1 {
				t.Fatalf("Listener status = %#v", got.Status.Listeners)
			}
			listenerAccepted := meta.FindStatusCondition(got.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionAccepted))
			wantListenerAccepted := metav1.ConditionTrue
			if tc.invalidListener {
				wantListenerAccepted = metav1.ConditionFalse
			}
			if listenerAccepted == nil || listenerAccepted.Status != wantListenerAccepted {
				t.Fatalf("Listener Accepted condition = %#v", listenerAccepted)
			}
			if len(got.Finalizers) != 0 || len(provider.gatewaySpecs) != 0 || len(provider.deletedGateways) != 0 {
				t.Fatalf("unbound Gateway acquired finalizers %v or made provider calls: %d ensures, %d deletes",
					got.Finalizers, len(provider.gatewaySpecs), len(provider.deletedGateways))
			}
		})
	}
}

func TestValidateGatewayRejectsUnsupportedAllowedRouteKinds(t *testing.T) {
	group := gatewayv1.Group(gatewayv1.GroupName)
	gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{
		Name:     "http",
		Protocol: gatewayv1.HTTPProtocolType,
		Port:     80,
		AllowedRoutes: &gatewayv1.AllowedRoutes{Kinds: []gatewayv1.RouteGroupKind{{
			Group: &group,
			Kind:  "GRPCRoute",
		}}},
	}}}}
	if _, err := Validate(gateway); err == nil {
		t.Fatal("validateGateway() accepted unsupported GRPCRoute kind")
	}
}

func TestValidateGatewayRejectsNonIsolatedHTTP(t *testing.T) {
	for _, peer := range []gatewayv1.Listener{
		{Name: "other-http", Port: 81, Protocol: gatewayv1.HTTPProtocolType},
		{Name: "same-port", Port: 80, Protocol: "custom.example/test"},
	} {
		t.Run(string(peer.Name), func(t *testing.T) {
			gateway := &gatewayv1.Gateway{Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}, peer,
			}}}
			if _, err := Validate(gateway); err == nil {
				t.Fatal("Gateway accepted HTTP from an ambiguous listener set")
			}
		})
	}
}

func TestValidateGatewayRejectsUnsupportedGatewayWideFields(t *testing.T) {
	base := gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}}
	tests := []struct {
		name   string
		mutate func(*gatewayv1.GatewaySpec)
	}{
		{name: "infrastructure", mutate: func(spec *gatewayv1.GatewaySpec) { spec.Infrastructure = &gatewayv1.GatewayInfrastructure{} }},
		{name: "allowed listeners", mutate: func(spec *gatewayv1.GatewaySpec) { spec.AllowedListeners = &gatewayv1.AllowedListeners{} }},
		{name: "gateway TLS", mutate: func(spec *gatewayv1.GatewaySpec) { spec.TLS = &gatewayv1.GatewayTLSConfig{} }},
		{name: "listener authorization", mutate: func(spec *gatewayv1.GatewaySpec) {
			spec.Listeners[0].Filters = &gatewayv1.ListenerFilters{Requests: []gatewayv1.ListenerFilter{{Type: gatewayv1.ListenerFilterExternalAuth}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := *base.DeepCopy()
			test.mutate(&spec)
			if _, err := Validate(&gatewayv1.Gateway{Spec: spec}); err == nil {
				t.Fatal("validateGateway() unexpectedly accepted unsupported field")
			}
		})
	}
}

func TestGatewayInvalidAllowedRouteKindsSetsResolvedRefs(t *testing.T) {
	scheme := testScheme(t)
	cfg := testConfig()
	group := gatewayv1.Group(gatewayv1.GroupName)
	class := &gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "openstack"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: cfg.ControllerName}}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "edge", UID: "gateway-uid"},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "openstack", Listeners: []gatewayv1.Listener{{
			Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80,
			AllowedRoutes: &gatewayv1.AllowedRoutes{Kinds: []gatewayv1.RouteGroupKind{{Group: &group, Kind: "GRPCRoute"}}},
		}}},
	}
	kubeClient := indexedFakeClientBuilder(scheme, testConfig()).WithStatusSubresource(gateway).WithObjects(class, gateway).Build()
	reconciler := *newTestReconciler(Reconciler{Client: kubeClient, APIReader: kubeClient, Config: cfg}, &recordingProvider{})
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got gatewayv1.Gateway
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: gateway.Namespace, Name: gateway.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Listeners) != 1 || len(got.Status.Listeners[0].SupportedKinds) != 0 {
		t.Fatalf("Listener status = %#v", got.Status.Listeners)
	}
	resolved := meta.FindStatusCondition(got.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
	if resolved == nil || resolved.Status != metav1.ConditionFalse || resolved.Reason != string(gatewayv1.ListenerReasonInvalidRouteKinds) {
		t.Fatalf("ResolvedRefs condition = %#v", resolved)
	}
}

func programmedTestGateway(cfg Config) *gatewayv1.Gateway {
	return &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "edge", UID: "gateway-uid", Generation: 1,
			Finalizers:  []string{cfg.GatewayFinalizer()},
			Annotations: gatewayBindingAnnotations(cfg, "80"),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "openstack",
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
		},
		Status: gatewayv1.GatewayStatus{Conditions: []metav1.Condition{{
			Type:               string(gatewayv1.GatewayConditionProgrammed),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: 1,
			Reason:             string(gatewayv1.GatewayReasonProgrammed),
			LastTransitionTime: metav1.Now(),
		}}},
	}
}
