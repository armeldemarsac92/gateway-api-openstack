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
	"crypto/sha256"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const (
	managedByLabel         = "gateway-api-openstack.github.io/managed-by"
	managedByValue         = "hibiscus-mcs-projector"
	sourceAnnotation       = "gateway-api-openstack.github.io/mcs-endpoint-slice-managed-by"
	waypointLabel          = "istio.io/use-waypoint"
	waypointNamespaceLabel = "istio.io/use-waypoint-namespace"
)

var (
	entryResource = schema.GroupVersionResource{Group: "networking.istio.io", Version: "v1", Resource: "serviceentries"}
	entryKind     = entryResource.GroupVersion().WithKind("ServiceEntry")
)

type projection struct {
	entry *unstructured.Unstructured
	host  string
	vip   netip.Addr
}

func entryName(importName string) string {
	hash := sha256.Sum256([]byte(importName))
	return fmt.Sprintf("hibiscus-mcs-%x", hash[:16])
}

// projectionOwner recognizes only the producer's own Kubernetes objects.
// The owner UID distinguishes different incarnations of the same import.
func projectionOwner(entry *unstructured.Unstructured) *metav1.OwnerReference {
	owners := entry.GetOwnerReferences()
	if len(owners) != 1 || entry.GetLabels()[managedByLabel] != managedByValue {
		return nil
	}
	owner := &owners[0]
	if owner.Name == "" || owner.UID == "" || entry.GetName() != entryName(owner.Name) ||
		!ptr.Deref(owner.Controller, false) || owner.Kind != mcs.ServiceImportKindName ||
		!isImportVersion(owner.APIVersion) {
		return nil
	}
	return owner
}

func isImportVersion(version string) bool {
	return version == mcs.GroupName+"/v1beta1" || version == mcs.GroupName+"/v1alpha1"
}

func ipv4(value string) (netip.Addr, bool) {
	address, err := netip.ParseAddr(value)
	return address, err == nil && address.Is4() && address.IsGlobalUnicast() &&
		!address.IsLoopback() && !address.IsLinkLocalUnicast()
}

func derive(imported *mcs.ServiceImport, endpointSlices []discoveryv1.EndpointSlice) (projection, string) {
	provider := imported.Annotations[sourceAnnotation]
	if provider == "" || !imported.DeletionTimestamp.IsZero() {
		return projection{}, "NotEnrolled"
	}
	if !validManagedBy(provider) || imported.UID == "" {
		return projection{}, "InvalidSliceProvider"
	}

	waypoint := imported.Labels[waypointLabel]
	labels := map[string]string{managedByLabel: managedByValue, waypointLabel: waypoint}
	if waypoint == "none" || len(validation.IsDNS1123Subdomain(waypoint)) != 0 {
		return projection{}, "ExplicitWaypointRequired"
	}
	if namespace, specified := imported.Labels[waypointNamespaceLabel]; specified {
		if len(validation.IsDNS1123Label(namespace)) != 0 {
			return projection{}, "InvalidWaypointNamespace"
		}
		labels[waypointNamespaceLabel] = namespace
	}

	if imported.Spec.Type != mcs.ClusterSetIP || len(imported.Spec.IPs) != 1 {
		return projection{}, "SingleIPv4ClusterSetIPRequired"
	}
	if ptr.Deref(imported.Spec.InternalTrafficPolicy, corev1.ServiceInternalTrafficPolicyCluster) != corev1.ServiceInternalTrafficPolicyCluster {
		return projection{}, "ClusterTrafficPolicyRequired"
	}
	vip, valid := ipv4(imported.Spec.IPs[0])
	if !valid {
		return projection{}, "SingleIPv4ClusterSetIPRequired"
	}
	if len(imported.Spec.Ports) != 1 {
		return projection{}, "SingleHTTPPortRequired"
	}
	port := imported.Spec.Ports[0]
	// An explicitly enrolled import can declare HTTP using appProtocol=http
	// or use the conventional http[-suffix] port name with no appProtocol.
	appProtocol := ptr.Deref(port.AppProtocol, "")
	if port.Port < 1 || port.Port > 65535 ||
		port.Protocol != "" && port.Protocol != corev1.ProtocolTCP ||
		appProtocol != "http" && !(appProtocol == "" && (port.Name == "http" || strings.HasPrefix(port.Name, "http-"))) {
		return projection{}, "SingleHTTPPortRequired"
	}
	portName := port.Name
	if portName == "" {
		portName = "http"
	}
	if len(validation.IsDNS1123Label(portName)) != 0 {
		return projection{}, "InvalidPortName"
	}
	if model.ServiceImportReadiness(imported) != nil {
		return projection{}, "ImportNotReady"
	}
	if imported.Status.EndpointSliceObjects != mcs.EndpointSliceObjectsPresent {
		return projection{}, "NonAuthoritativeEndpointInventory"
	}

	endpoints, ambiguous := destinations(imported, endpointSlices, provider, port, portName, vip)
	if ambiguous {
		return projection{}, "AmbiguousEndpointInventory"
	}
	host := imported.Name + "." + imported.Namespace + ".svc.clusterset.local"
	if len(validation.IsDNS1123Subdomain(host)) != 0 {
		return projection{}, "InvalidClusterSetHostname"
	}
	entry := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"hosts":      []any{host},
			"addresses":  []any{vip.String()},
			"ports":      []any{map[string]any{"name": portName, "number": int64(port.Port), "protocol": "HTTP"}},
			"location":   "MESH_INTERNAL",
			"resolution": "STATIC",
		},
	}}
	if len(endpoints) != 0 {
		entry.Object["spec"].(map[string]any)["endpoints"] = endpoints
	}
	entry.SetGroupVersionKind(entryKind)
	entry.SetNamespace(imported.Namespace)
	entry.SetName(entryName(imported.Name))
	entry.SetLabels(labels)
	entry.SetAnnotations(map[string]string{sourceAnnotation: provider})
	entry.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: imported.APIVersion,
		Kind:       mcs.ServiceImportKindName,
		Name:       imported.Name,
		UID:        imported.UID,
		Controller: ptr.To(true),
	}})
	return projection{entry: entry, host: host, vip: vip}, ""
}

func validManagedBy(provider string) bool {
	switch provider {
	case "", "endpointslice-controller.k8s.io", "endpointslicemirroring-controller.k8s.io":
		return false
	}
	return len(validation.IsValidLabelValue(provider)) == 0
}

type endpointRecord struct {
	port   int32
	source string
	ready  bool
}

func destinations(imported *mcs.ServiceImport, endpointSlices []discoveryv1.EndpointSlice, provider string, port mcs.ServicePort, portName string, vip netip.Addr) ([]any, bool) {
	records := make(map[netip.Addr]endpointRecord)
	slicePort := &corev1.ServicePort{Name: port.Name, Protocol: corev1.ProtocolTCP}
	for i := range endpointSlices {
		slice := &endpointSlices[i]
		source := slice.Labels[mcs.LabelSourceCluster]
		if !belongsToImport(slice, imported, provider, source) {
			continue
		}
		target := model.EndpointSlicePort(slicePort, slice.Ports)
		if target < 1 || target > 65535 {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			ready := ptr.Deref(endpoint.Conditions.Ready, true) &&
				ptr.Deref(endpoint.Conditions.Serving, true) && !ptr.Deref(endpoint.Conditions.Terminating, false)
			for _, value := range endpoint.Addresses {
				address, valid := ipv4(value)
				if !valid {
					continue
				}
				previous, exists := records[address]
				if address == vip || exists && (previous.source != source || previous.port != target) {
					// WDS represents a single inline workload per address in this ServiceEntry.
					return nil, true
				}
				records[address] = endpointRecord{port: target, source: source, ready: ready || previous.ready}
			}
		}
	}
	var addresses []netip.Addr
	for address, record := range records {
		if record.ready {
			addresses = append(addresses, address)
		}
	}
	slices.SortFunc(addresses, netip.Addr.Compare)
	var endpoints []any
	for _, address := range addresses {
		endpoints = append(endpoints, map[string]any{
			"address": address.String(),
			"ports":   map[string]any{portName: int64(records[address].port)},
		})
	}
	return endpoints, false
}

func belongsToImport(slice *discoveryv1.EndpointSlice, imported *mcs.ServiceImport, provider, source string) bool {
	if model.ServiceImportEndpointSliceKey(slice) != (types.NamespacedName{Namespace: imported.Namespace, Name: imported.Name}) ||
		slice.AddressType != discoveryv1.AddressTypeIPv4 || !slice.DeletionTimestamp.IsZero() ||
		slice.Labels[discoveryv1.LabelManagedBy] != provider || source == "" ||
		!slices.Contains(imported.Status.Clusters, mcs.ClusterStatus{Cluster: source}) {
		return false
	}
	owner := metav1.GetControllerOfNoCopy(slice)
	return owner != nil && owner.Kind == mcs.ServiceImportKindName && isImportVersion(owner.APIVersion) &&
		owner.Name == imported.Name && owner.UID == imported.UID
}

func (p projection) collision(services []corev1.Service, entries []unstructured.Unstructured, imports []mcs.ServiceImport) string {
	// Ambient determines the service by network and VIP before looking at its port.
	for _, service := range services {
		if slices.Contains(service.Spec.ClusterIPs, p.vip.String()) || service.Spec.ClusterIP == p.vip.String() ||
			slices.Contains(service.Spec.ExternalIPs, p.vip.String()) || service.Spec.LoadBalancerIP == p.vip.String() ||
			slices.ContainsFunc(service.Status.LoadBalancer.Ingress, func(ingress corev1.LoadBalancerIngress) bool { return ingress.IP == p.vip.String() }) {
			return "KubernetesServiceVIPConflict"
		}
	}
	for i := range imports {
		other := &imports[i]
		if other.UID != p.entry.GetOwnerReferences()[0].UID && slices.Contains(other.Spec.IPs, p.vip.String()) {
			return "ServiceImportVIPConflict"
		}
	}
	for i := range entries {
		other := &entries[i]
		if other.GetName() == p.entry.GetName() && other.GetNamespace() == p.entry.GetNamespace() {
			if owner := projectionOwner(other); owner != nil && owner.UID == p.entry.GetOwnerReferences()[0].UID {
				continue
			}
			return "ServiceEntryNameConflict"
		}
		hosts, _, _ := unstructured.NestedStringSlice(other.Object, "spec", "hosts")
		for _, host := range hosts {
			host = strings.TrimSuffix(strings.ToLower(host), ".")
			if host == p.host || strings.HasPrefix(host, "*") && strings.HasSuffix(p.host, host[1:]) {
				return "ServiceEntryHostConflict"
			}
		}
		addresses, _, _ := unstructured.NestedStringSlice(other.Object, "spec", "addresses")
		for _, value := range addresses {
			prefix, _ := netip.ParsePrefix(value)
			if value == p.vip.String() || prefix.Contains(p.vip) {
				return "ServiceEntryVIPConflict"
			}
		}
		allocated, _, _ := unstructured.NestedSlice(other.Object, "status", "addresses")
		for _, record := range allocated {
			if address, ok := record.(map[string]any); ok && address["value"] == p.vip.String() {
				return "ServiceEntryVIPConflict"
			}
		}
	}
	return ""
}
