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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	experimental := channel == features.FeatureChannelExperimental
	config := controller.Config{GatewayAPIChannel: channel}
	scheme := newControllerScheme(config, experimental)
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

	if err := requireGatewayAPIs(restConfig, config, experimental); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if !experimental {
		err := kube.Get(t.Context(), client.ObjectKey{Name: "xmeshes.gateway.networking.x-k8s.io"}, &apiextensionsv1.CustomResourceDefinition{})
		if !apierrors.IsNotFound(err) {
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

	awaitStatus := func(mesh *gatewayx.XMesh, accepted metav1.ConditionStatus, acceptedReason string, programmed metav1.ConditionStatus, featureCount int, detail ...string) {
		t.Helper()
		err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			if err := kube.Get(ctx, client.ObjectKeyFromObject(mesh), mesh); err != nil {
				return false, err
			}
			a := meta.FindStatusCondition(mesh.Status.Conditions, "Accepted")
			p := meta.FindStatusCondition(mesh.Status.Conditions, "Programmed")
			return a != nil && p != nil && a.ObservedGeneration == mesh.Generation && p.ObservedGeneration == mesh.Generation &&
				a.Status == accepted && a.Reason == acceptedReason && strings.Contains(a.Message, instance) &&
				p.Status == programmed && len(mesh.Status.SupportedFeatures) == featureCount &&
				(len(detail) == 0 || strings.Contains(p.Message, detail[0])), nil
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
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)
	awaitStatus(custom, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)

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
					Image: ambientComponentImage(t, "istiod.yaml"),
					Env: []corev1.EnvVar{
						{Name: "PILOT_ENABLE_AMBIENT", Value: "true"},
						{Name: "PILOT_ENABLE_GATEWAY_API_GATEWAYCLASS_CONTROLLER", Value: "false"},
					},
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

	cniConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "istio-cni-config", Namespace: "istio-system"},
		Data:       map[string]string{"AMBIENT_ENABLED": "false", "AMBIENT_IPV6": "false"},
	}
	create(cniConfig)
	createDaemonSet := func(name string, container corev1.Container) *appsv1.DaemonSet {
		t.Helper()
		labels := map[string]string{"app": name}
		ds := &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "istio-system"},
			Spec: appsv1.DaemonSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
				},
			},
		}
		create(ds)
		return ds
	}
	markDaemonSetRolledOut := func(ds *appsv1.DaemonSet) {
		t.Helper()
		ds.Status = appsv1.DaemonSetStatus{
			ObservedGeneration:     ds.Generation,
			CurrentNumberScheduled: 1, DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1,
			NumberReady: 1, NumberAvailable: 1,
		}
		updateStatus(ds)
	}
	cni := createDaemonSet("istio-cni-node", corev1.Container{
		Name:  "install-cni",
		Image: ambientComponentImage(t, "cni.yaml"),
		EnvFrom: []corev1.EnvFromSource{{
			ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "istio-cni-config"}},
		}},
	})
	ztunnel := createDaemonSet("ztunnel", corev1.Container{
		Name:  "istio-proxy",
		Image: ambientComponentImage(t, "ztunnel.yaml"),
		Env:   []corev1.EnvVar{{Name: "IPV6_ENABLED", Value: "false"}},
	})
	markDaemonSetRolledOut(ztunnel)
	acceptClass()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "istio-cni-node rollout")
	markDaemonSetRolledOut(cni)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "ambient IPv4 settings")

	// Changing envFrom data does not update the environment of running CNI Pods.
	cniConfig.Data["AMBIENT_ENABLED"] = "true"
	update(cniConfig)
	primary.Spec.Description = ptr.To("Observe the existing CNI rollout after its ConfigMap changed")
	update(primary)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "ambient IPv4 settings")

	cni.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "AMBIENT_ENABLED", Value: "true"},
		{Name: "AMBIENT_IPV6", Value: "false"},
	}
	update(cni)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "istio-cni-node rollout")
	markDaemonSetRolledOut(cni)
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
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)
	acceptClass()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)

	// An existing installation can retain the published producer-only ServiceImport pilot.
	for _, image := range []string{
		ambientComponentImage(t, "consumer-routes.yaml"),
		"ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:ec5e60e39ceda1929d9b28d299a816c88e7fbac62602f97bc114fd19c9e86849",
		ambientComponentImage(t, "service-import-parents.yaml"),
	} {
		pilot.Spec.Template.Spec.Containers[0].Image = image
		update(pilot)
		awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)
		markRolledOut()
		awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)
		if hasConsumer() {
			t.Fatalf("%s advertised MeshConsumerRoute", image)
		}
	}

	classVersion := class.ResourceVersion
	discovery := &pilot.Spec.Template.Spec.Containers[0]
	discovery.Env = append(discovery.Env, corev1.EnvVar{Name: "PILOT_ENABLE_AMBIENT_WAYPOINTS", Value: "false"})
	update(pilot)
	markRolledOut()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "ambient and Gateway API settings")

	discovery = &pilot.Spec.Template.Spec.Containers[0]
	discovery.Env = discovery.Env[:len(discovery.Env)-1]
	update(pilot)
	markRolledOut()
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)

	ztunnel.Spec.Template.Annotations = map[string]string{"example.com/restarted": "true"}
	update(ztunnel)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0, "ztunnel rollout")
	markDaemonSetRolledOut(ztunnel)
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionTrue, stockFeatureCount)
	get(class)
	if class.ResourceVersion != classVersion {
		t.Fatal("withdrawing features changed the Istio GatewayClass")
	}

	if err := kube.Delete(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	awaitStatus(primary, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)
	awaitStatus(custom, metav1.ConditionTrue, "Accepted", metav1.ConditionUnknown, 0)
	get(foreign)
	if foreign.ResourceVersion != foreignRV {
		t.Fatalf("reconciling changed the foreign XMesh: %s -> %s", foreignRV, foreign.ResourceVersion)
	}
}

func ambientComponentImage(t *testing.T, valuesFile string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "integrations", "istio-ambient", valuesFile))
	if err != nil {
		t.Fatal(err)
	}
	var values struct{ Image string }
	if err := yaml.Unmarshal(data, &values); err != nil || values.Image == "" {
		t.Fatalf("read pinned Istio image from %s: %v", valuesFile, err)
	}
	return values.Image
}
