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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
)

func exampleImport() *mcs.ServiceImport {
	return &mcs.ServiceImport{
		TypeMeta: metav1.TypeMeta{APIVersion: mcs.GroupVersion.String(), Kind: mcs.ServiceImportKindName},
		ObjectMeta: metav1.ObjectMeta{
			Name: "store", Namespace: "shop", UID: "import-uid",
			Annotations: map[string]string{sourceAnnotation: "mcs.example.io"},
			Labels:      map[string]string{waypointLabel: "store-waypoint", waypointNamespaceLabel: "gateways"},
		},
		Spec: mcs.ServiceImportSpec{
			Type: mcs.ClusterSetIP, IPs: []string{"10.141.0.3"},
			Ports: []mcs.ServicePort{{Name: "web", AppProtocol: ptr.To("http"), Port: 80}},
		},
		Status: mcs.ServiceImportStatus{
			EndpointSliceObjects: mcs.EndpointSliceObjectsPresent,
			Clusters:             []mcs.ClusterStatus{{Cluster: "west"}, {Cluster: "east"}},
		},
	}
}

func exampleSlice(imported *mcs.ServiceImport, name, address string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: imported.Namespace,
			Labels: map[string]string{
				mcs.LabelServiceName:       imported.Name,
				mcs.LabelSourceCluster:     "west",
				discoveryv1.LabelManagedBy: imported.Annotations[sourceAnnotation],
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(imported, mcs.SchemeGroupVersion.WithKind(mcs.ServiceImportKindName))},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("web"), Port: ptr.To(int32(8085))}},
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{address}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
	}
}

func TestProjectionUsesOnlyCurrentAuthoritativeEndpointSlices(t *testing.T) {
	imported := exampleImport()
	valid := exampleSlice(imported, "mcs", "10.13.1.1")
	stale := exampleSlice(imported, "stale", "10.13.1.2")
	stale.OwnerReferences[0].UID = "previous-import"
	foreign := exampleSlice(imported, "foreign", "10.13.1.3")
	foreign.Labels[discoveryv1.LabelManagedBy] = "other.example.io"
	sourceUnknown := exampleSlice(imported, "unknown-cluster", "10.13.1.4")
	sourceUnknown.Labels[mcs.LabelSourceCluster] = "unknown"
	terminating := exampleSlice(imported, "terminating", "10.13.1.5")
	terminating.Endpoints[0].Conditions.Terminating = ptr.To(true)

	projection, reason := derive(imported, []discoveryv1.EndpointSlice{*valid, *stale, *foreign, *sourceUnknown, *terminating})
	require.Empty(t, reason)
	require.Equal(t, "store.shop.svc.clusterset.local", projection.host)
	require.Equal(t, imported.UID, projectionOwner(projection.entry).UID)
	require.Equal(t, "gateways", projection.entry.GetLabels()[waypointNamespaceLabel])
	require.Equal(t, imported.Annotations[sourceAnnotation], projection.entry.GetAnnotations()[sourceAnnotation])
	require.Equal(t, []any{map[string]any{"address": "10.13.1.1", "ports": map[string]any{"web": int64(8085)}}}, readEndpoints(t, projection.entry))

	ambiguous := valid.DeepCopy()
	ambiguous.Labels[mcs.LabelSourceCluster] = "east"
	_, reason = derive(imported, []discoveryv1.EndpointSlice{*valid, *ambiguous})
	require.Equal(t, "AmbiguousEndpointInventory", reason)

	projection, reason = derive(imported, nil)
	require.Empty(t, reason)
	require.Empty(t, readEndpoints(t, projection.entry))
	imported.Status.EndpointSliceObjects = mcs.EndpointSliceObjectsAbsent
	_, reason = derive(imported, nil)
	require.Equal(t, "NonAuthoritativeEndpointInventory", reason)

	imported.Spec.Type = mcs.Headless
	_, reason = derive(imported, nil)
	require.Equal(t, "SingleIPv4ClusterSetIPRequired", reason)
}

func TestProjectionUsesReadyReplacementDuringSliceRollover(t *testing.T) {
	imported := exampleImport()
	running := exampleSlice(imported, "current", "10.13.1.1")
	draining := running.DeepCopy()
	draining.Name = "previous"
	draining.Endpoints[0].Conditions.Terminating = ptr.To(true)

	for _, slices := range [][]discoveryv1.EndpointSlice{{*draining, *running}, {*running, *draining}} {
		item, reason := derive(imported, slices)
		require.Empty(t, reason)
		require.Equal(t, []any{map[string]any{"address": "10.13.1.1", "ports": map[string]any{"web": int64(8085)}}}, readEndpoints(t, item.entry))
	}
}

func TestProjectionRequiresClusterTrafficPolicy(t *testing.T) {
	imported := exampleImport()
	remote := exampleSlice(imported, "remote", "10.13.1.9")
	imported.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyLocal)
	_, reason := derive(imported, []discoveryv1.EndpointSlice{*remote})
	require.Equal(t, "ClusterTrafficPolicyRequired", reason)
	imported.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyCluster)
	item, reason := derive(imported, []discoveryv1.EndpointSlice{*remote})
	require.Empty(t, reason)
	require.Len(t, readEndpoints(t, item.entry), 1)
}

func TestUnavailableServiceImportInventoryWithdrawsOnlyOwnedEntries(t *testing.T) {
	imported := exampleImport()
	item, reason := derive(imported, nil)
	require.Empty(t, reason)
	foreign := item.entry.DeepCopy()
	foreign.SetName("ordinary-entry")
	foreign.SetOwnerReferences(nil)
	resource := importResource.WithVersion("v1beta1")
	kube := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{resource: "ServiceImportList", entryResource: "ServiceEntryList"},
		item.entry, foreign)
	unavailable := errors.New("import inventory unavailable")
	kube.PrependReactor("list", resource.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, unavailable
	})
	p := projector{api: kube}
	require.ErrorIs(t, p.reconcile(t.Context(), servedAPI{resource: resource, uid: "crd"}), unavailable)
	entries := kube.Resource(entryResource).Namespace(imported.Namespace)
	_, err := entries.Get(t.Context(), item.entry.GetName(), metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	preserved, err := entries.Get(t.Context(), foreign.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, foreign.Object, preserved.Object)
}

func TestProjectionRejectsConflictingServiceEntryClaims(t *testing.T) {
	imported := exampleImport()
	item, reason := derive(imported, nil)
	require.Empty(t, reason)

	// The same object name remains reserved even if a foreign object uses the marker.
	foreign := item.entry.DeepCopy()
	foreign.SetOwnerReferences(nil)
	require.Equal(t, "ServiceEntryNameConflict", item.collision(nil, []unstructured.Unstructured{*foreign}, nil))

	foreign.SetName("existing")
	require.NoError(t, unstructured.SetNestedStringSlice(foreign.Object, []string{"*.shop.svc.clusterset.local"}, "spec", "hosts"))
	require.Equal(t, "ServiceEntryHostConflict", item.collision(nil, []unstructured.Unstructured{*foreign}, nil))

	require.NoError(t, unstructured.SetNestedStringSlice(foreign.Object, []string{"example.com"}, "spec", "hosts"))
	require.NoError(t, unstructured.SetNestedStringSlice(foreign.Object, []string{"10.141.0.0/24"}, "spec", "addresses"))
	require.Equal(t, "ServiceEntryVIPConflict", item.collision(nil, []unstructured.Unstructured{*foreign}, nil))
}

func readEndpoints(t *testing.T, entry *unstructured.Unstructured) []any {
	t.Helper()
	endpoints, _, err := unstructured.NestedSlice(entry.Object, "spec", "endpoints")
	require.NoError(t, err)
	return endpoints
}
