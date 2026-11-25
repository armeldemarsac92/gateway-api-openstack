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

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	"sigs.k8s.io/gateway-api/pkg/features"
	"sigs.k8s.io/yaml"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/ambientmesh"
)

func TestAmbientMeshEnvtest(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	crds := os.Getenv("GATEWAY_API_CRD_PATH")
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if assets == "" || crds == "" || (channel != features.FeatureChannelStandard && channel != features.FeatureChannelExperimental) {
		t.Fatal("set KUBEBUILDER_ASSETS, GATEWAY_API_CRD_PATH and GATEWAY_API_CHANNEL as in make test-envtest")
	}
	scheme := runtime.NewScheme()
	for _, install := range []func(*runtime.Scheme) error{appsv1.AddToScheme, corev1.AddToScheme, gatewayv1.Install, gatewayx.Install} {
		if err := install(scheme); err != nil {
			t.Fatal(err)
		}
	}
	environment := &envtest.Environment{
		Scheme:                scheme,
		UseExistingCluster:    ptr.To(false),
		CRDDirectoryPaths:     []string{crds},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assets,
	}
	restConfig, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})

	experimental := channel == features.FeatureChannelExperimental
	if err := requireGatewayAPIs(restConfig, controller.Config{GatewayAPIChannel: channel}, experimental); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if !experimental {
		err := kube.List(t.Context(), &gatewayx.XMeshList{})
		if !meta.IsNoMatchError(err) {
			t.Fatalf("Standard API unexpectedly serves XMesh: %v", err)
		}
		return
	}

	const instance = "Hibiscus Istio ambient envtest in namespace openstack-gateway-system"
	newManager := func(defaultName string) ctrl.Manager {
		t.Helper()
		manager, err := ctrl.NewManager(restConfig, ctrl.Options{
			Scheme:                 scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             runtimeconfig.Controller{SkipNameValidation: ptr.To(true)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ambientmesh.SetupWithManager(t.Context(), manager, defaultName, instance); err != nil {
			t.Fatal(err)
		}
		return manager
	}
	create := func(object client.Object) {
		t.Helper()
		if err := kube.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	get := func(object client.Object) {
		t.Helper()
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
	}
	update := func(object client.Object) {
		t.Helper()
		if err := kube.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	updateStatus := func(object client.Object) {
		t.Helper()
		if err := kube.Status().Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}

	manager := newManager(ambientmesh.DefaultName)
	primary := &gatewayx.XMesh{ObjectMeta: metav1.ObjectMeta{Name: "hibiscus-istio-ambient"}}
	get(primary)
	if primary.Spec.ControllerName != "gateway-api-openstack.github.io/istio-ambient" || primary.Spec.ParametersRef != nil || primary.Spec.Description != nil {
		t.Fatalf("unexpected default XMesh spec: %+v", primary.Spec)
	}
	custom := &gatewayx.XMesh{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-mesh"},
		Spec: gatewayx.MeshSpec{
			ControllerName: ambientmesh.ControllerName,
			Description:    ptr.To("Operator description"),
		},
	}
	create(custom)
	foreign := &gatewayx.XMesh{
		ObjectMeta: metav1.ObjectMeta{Name: "foreign-mesh"},
		Spec:       gatewayx.MeshSpec{ControllerName: "another.example/mesh"},
	}
	create(foreign)
	customRV, foreignRV := custom.ResourceVersion, foreign.ResourceVersion
	// Exercise the configured startup name with objects already in the API.
	newManager(custom.Name)
	newManager(foreign.Name)
	get(custom)
	get(foreign)
	if custom.ResourceVersion != customRV || foreign.ResourceVersion != foreignRV {
		t.Fatalf("startup rewrote an existing XMesh: owned %s -> %s, foreign %s -> %s", customRV, custom.ResourceVersion, foreignRV, foreign.ResourceVersion)
	}

	managerContext, stopManager := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Start(managerContext) }()
	t.Cleanup(func() {
		stopManager()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	awaitStatus := func(mesh *gatewayx.XMesh, accepted metav1.ConditionStatus, acceptedReason string, programmed metav1.ConditionStatus, featureCount int) {
		t.Helper()
		err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			if err := kube.Get(ctx, client.ObjectKeyFromObject(mesh), mesh); err != nil {
				return false, err
			}
			a := meta.FindStatusCondition(mesh.Status.Conditions, "Accepted")
			p := meta.FindStatusCondition(mesh.Status.Conditions, "Programmed")
			return a != nil && p != nil && a.ObservedGeneration == mesh.Generation && p.ObservedGeneration == mesh.Generation &&
				a.Status == accepted && a.Reason == acceptedReason && strings.Contains(a.Message, instance) &&
				p.Status == programmed && len(mesh.Status.SupportedFeatures) == featureCount, nil
		})
		if err != nil {
			t.Fatalf("XMesh %s generation %d status %+v: %v", mesh.Name, mesh.Generation, mesh.Status, err)
		}
		if !slices.IsSortedFunc(mesh.Status.SupportedFeatures, func(a, b gatewayv1.SupportedFeature) int {
			return strings.Compare(string(a.Name), string(b.Name))
		}) {
			t.Fatalf("unsorted features: %v", mesh.Status.SupportedFeatures)
		}
	}
	const stockFeatureCount = 16
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)
	awaitStatus(custom, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)

	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "istio-waypoint"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "istio.io/mesh-controller"},
	}
	create(class)
	acceptClass := func() {
		t.Helper()
		meta.SetStatusCondition(&class.Status.Conditions, metav1.Condition{
			Type: "Accepted", Status: metav1.ConditionTrue, Reason: "Accepted",
			Message: "Accepted by the Istio test fixture", ObservedGeneration: class.Generation,
		})
		updateStatus(class)
	}

	create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "istio-system"}})
	pilot := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "istiod", Namespace: "istio-system"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "istiod"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "istiod"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "discovery",
					Image: ambientPilotImage(t, "istiod.yaml"),
					Env:   []corev1.EnvVar{{Name: "PILOT_ENABLE_AMBIENT", Value: "true"}},
				}}},
			},
		},
	}
	create(pilot)
	markRolledOut := func() {
		t.Helper()
		pilot.Status = appsv1.DeploymentStatus{
			ObservedGeneration: pilot.Generation,
			Replicas:           1,
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
			AvailableReplicas:  1,
		}
		updateStatus(pilot)
	}
	markRolledOut()

	acceptClass()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)
	awaitStatus(custom, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)
	hasConsumer := func() bool {
		return slices.Contains(primary.Status.SupportedFeatures, gatewayv1.SupportedFeature{Name: "MeshConsumerRoute"})
	}
	if hasConsumer() {
		t.Fatal("stock pilot advertised MeshConsumerRoute")
	}

	primary.Spec.ParametersRef = &gatewayv1.ParametersReference{Kind: "ConfigMap", Name: "ambient-parameters"}
	update(primary)
	awaitStatus(primary, metav1.ConditionFalse, "InvalidParameters", metav1.ConditionFalse, 0)
	primary.Spec.ParametersRef = nil
	update(primary)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)

	class.Spec.Description = ptr.To("Current class generation is not yet accepted")
	update(class)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)
	acceptClass()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)

	pilot.Spec.Template.Spec.Containers[0].Image = ambientPilotImage(t, "consumer-routes.yaml")
	update(pilot)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)
	markRolledOut()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount+1)
	if !hasConsumer() {
		t.Fatal("qualified consumer pilot did not advertise MeshConsumerRoute")
	}

	if err := kube.Delete(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)
	awaitStatus(custom, metav1.ConditionTrue, "Accepted", metav1.ConditionFalse, 0)
	get(foreign)
	if foreign.ResourceVersion != foreignRV {
		t.Fatalf("reconciling changed the foreign XMesh: %s -> %s", foreignRV, foreign.ResourceVersion)
	}
}

func ambientPilotImage(t *testing.T, valuesFile string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "integrations", "istio-ambient", valuesFile))
	if err != nil {
		t.Fatal(err)
	}
	var values struct{ Image string }
	if err := yaml.Unmarshal(data, &values); err != nil || values.Image == "" {
		t.Fatalf("read pinned pilot image from %s: %v", valuesFile, err)
	}
	return values.Image
}
