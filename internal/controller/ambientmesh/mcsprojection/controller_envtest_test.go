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

package mcsprojection

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcscrd "sigs.k8s.io/mcs-api/config/crd"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
	"sigs.k8s.io/yaml"
)

func TestServiceImportProjectionEnvtest(t *testing.T) {
	assets, gatewayCRDs := os.Getenv("KUBEBUILDER_ASSETS"), os.Getenv("GATEWAY_API_CRD_PATH")
	require.NotEmpty(t, assets, "set KUBEBUILDER_ASSETS as in make test-envtest")
	require.NotEmpty(t, gatewayCRDs, "set GATEWAY_API_CRD_PATH as in make test-envtest")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	require.NoError(t, mcs.Install(scheme))
	environment := &envtest.Environment{
		Scheme: scheme, BinaryAssetsDirectory: assets, UseExistingCluster: ptr.To(false),
		CRDDirectoryPaths: []string{gatewayCRDs}, ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })

	kube, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	manager, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, Metrics: metrics.Options{BindAddress: "0"}})
	require.NoError(t, err)
	require.NoError(t, SetupWithManager(manager))
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan error, 1)
	go func() { finished <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-finished:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("projector manager did not stop")
		}
	})
	select {
	case <-manager.Elected():
	case <-time.After(5 * time.Second):
		t.Fatal("projector manager did not start with the optional CRDs missing")
	}

	template := exampleImport()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: template.Namespace}}
	require.NoError(t, kube.Create(ctx, ns))
	coreService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: template.Name, Labels: map[string]string{waypointLabel: "core-waypoint"}},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "web", Port: 80}}},
	}
	require.NoError(t, kube.Create(ctx, coreService))
	coreSlice := exampleSlice(template, "core", "10.13.1.100")
	coreSlice.Labels = map[string]string{discoveryv1.LabelServiceName: coreService.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}
	coreSlice.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(coreService, corev1.SchemeGroupVersion.WithKind("Service"))}
	require.NoError(t, kube.Create(ctx, coreSlice))

	var importCRD apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(mcscrd.ServiceImportCRD, &importCRD))
	entryCRD := testServiceEntryCRD(t)
	_, err = envtest.InstallCRDs(config, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{entryCRD, &importCRD}})
	require.NoError(t, err)

	projected, reason := derive(template, nil)
	require.Empty(t, reason)
	foreign := projected.entry.DeepCopy()
	foreign.SetOwnerReferences(nil)
	require.NoError(t, kube.Create(ctx, foreign))
	key := client.ObjectKeyFromObject(foreign)
	fetch := func() (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetGroupVersionKind(entryKind)
		return object, kube.Get(ctx, key, object)
	}
	waitEntry := func(matches func(*unstructured.Unstructured) bool) *unstructured.Unstructured {
		t.Helper()
		var result *unstructured.Unstructured
		require.Eventually(t, func() bool {
			entry, err := fetch()
			if err != nil || !matches(entry) {
				return false
			}
			result = entry
			return true
		}, 10*time.Second, 50*time.Millisecond)
		return result
	}
	absent := func() bool {
		_, err := fetch()
		return apierrors.IsNotFound(err)
	}
	newImport := func() *mcs.ServiceImport {
		t.Helper()
		imported := template.DeepCopy()
		imported.UID = ""
		imported.Status = mcs.ServiceImportStatus{}
		require.NoError(t, kube.Create(ctx, imported))
		return imported
	}
	ownedBy := func(imported *mcs.ServiceImport) func(*unstructured.Unstructured) bool {
		return func(entry *unstructured.Unstructured) bool {
			owner := projectionOwner(entry)
			return owner != nil && owner.UID == imported.UID
		}
	}
	setInventory := func(imported *mcs.ServiceImport) {
		t.Helper()
		imported.Status = template.Status
		require.NoError(t, kube.Status().Update(ctx, imported))
	}

	imported := newImport()
	setInventory(imported)
	member := exampleSlice(imported, "export-west", "10.13.1.1")
	require.NoError(t, kube.Create(ctx, member))
	stillForeign := waitEntry(func(entry *unstructured.Unstructured) bool { return entry.GetUID() == foreign.GetUID() })
	require.Equal(t, foreign.GetResourceVersion(), stillForeign.GetResourceVersion())
	require.NoError(t, kube.Delete(ctx, foreign))

	entry := waitEntry(func(entry *unstructured.Unstructured) bool {
		return ownedBy(imported)(entry) && len(readEndpoints(t, entry)) == 1
	})
	require.Equal(t, projected.entry.Object["spec"].(map[string]any)["hosts"], entry.Object["spec"].(map[string]any)["hosts"])
	require.Equal(t, "store-waypoint", entry.GetLabels()[waypointLabel])
	require.Equal(t, imported.Annotations[sourceAnnotation], entry.GetAnnotations()[sourceAnnotation])
	require.Equal(t, []any{map[string]any{"address": "10.13.1.1", "ports": map[string]any{"web": int64(8085)}}}, readEndpoints(t, entry))
	observedCore := &corev1.Service{}
	require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(coreService), observedCore))
	require.Equal(t, coreService.ResourceVersion, observedCore.ResourceVersion)

	const externalAnnotation = "example.com/owner"
	original := entry.DeepCopy()
	annotations := entry.GetAnnotations()
	annotations[externalAnnotation] = "operator"
	entry.SetAnnotations(annotations)
	require.NoError(t, kube.Patch(ctx, entry, client.MergeFrom(original)))

	imported.Annotations[sourceAnnotation] = "replacement.example.io"
	imported.Spec.IPs = []string{"10.141.0.4"}
	imported.Spec.Ports[0].Port = 81
	require.NoError(t, kube.Update(ctx, imported))
	rotated := waitEntry(func(entry *unstructured.Unstructured) bool {
		addresses, _, _ := unstructured.NestedStringSlice(entry.Object, "spec", "addresses")
		ports, _, _ := unstructured.NestedSlice(entry.Object, "spec", "ports")
		return entry.GetAnnotations()[sourceAnnotation] == "replacement.example.io" &&
			len(readEndpoints(t, entry)) == 0 && len(addresses) == 1 && addresses[0] == "10.141.0.4" &&
			len(ports) == 1 && ports[0].(map[string]any)["number"] == int64(81)
	})
	require.Equal(t, entry.GetUID(), rotated.GetUID())
	require.Equal(t, "operator", rotated.GetAnnotations()[externalAnnotation])
	require.NoError(t, kube.Delete(ctx, member))
	waitEntry(func(entry *unstructured.Unstructured) bool {
		return ownedBy(imported)(entry) && len(readEndpoints(t, entry)) == 0
	})

	imported.Spec.IPs = []string{coreService.Spec.ClusterIP}
	require.NoError(t, kube.Update(ctx, imported))
	require.Eventually(t, absent, 10*time.Second, 50*time.Millisecond)
	imported.Spec.IPs = []string{"10.141.0.4"}
	require.NoError(t, kube.Update(ctx, imported))
	previous := waitEntry(ownedBy(imported))

	require.NoError(t, kube.Delete(ctx, imported))
	next := newImport()
	setInventory(next)
	replaced := waitEntry(ownedBy(next))
	require.NotEqual(t, previous.GetUID(), replaced.GetUID())
	require.NotEqual(t, imported.UID, next.UID)

	delete(next.Annotations, sourceAnnotation)
	require.NoError(t, kube.Update(ctx, next))
	require.Eventually(t, absent, 10*time.Second, 50*time.Millisecond)
	next.Annotations = template.Annotations
	require.NoError(t, kube.Update(ctx, next))
	waitEntry(ownedBy(next))

	require.NoError(t, kube.Get(ctx, client.ObjectKey{Name: importCRD.Name}, &importCRD))
	for i := range importCRD.Spec.Versions {
		version := &importCRD.Spec.Versions[i]
		version.Served = version.Name == "v1alpha1"
	}
	require.NoError(t, kube.Update(ctx, &importCRD))
	waitEntry(func(entry *unstructured.Unstructured) bool {
		owner := projectionOwner(entry)
		return owner != nil && owner.UID == next.UID && owner.APIVersion == mcs.GroupName+"/v1alpha1"
	})

	require.NoError(t, kube.Delete(ctx, &importCRD))
	require.Eventually(t, absent, 10*time.Second, 50*time.Millisecond)
}

func testServiceEntryCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	if path := os.Getenv("ISTIO_SERVICE_ENTRY_CRD_PATH"); path != "" {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		var crd apiextensionsv1.CustomResourceDefinition
		require.NoError(t, yaml.Unmarshal(data, &crd))
		require.Equal(t, entryResource.GroupResource().String(), crd.Name)
		return &crd
	}
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: entryResource.GroupResource().String()},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: entryResource.Group, Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: entryResource.Resource, Kind: entryKind.Kind},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: entryResource.Version, Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"spec": {Type: "object", XPreserveUnknownFields: ptr.To(true)},
					},
				}},
			}},
		},
	}
}
