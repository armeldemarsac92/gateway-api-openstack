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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Digests record the values last written by this controller. They make
// removal safe across restarts without duplicating the annotation payload.
type discoveryMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

func (r *Reconciler) applyDiscoveryMetadata(gateway *gatewayv1.Gateway, object client.Object, reservedLabels map[string]string) error {
	var previous discoveryMetadata
	if state := object.GetAnnotations()[discoveryMetadataAnnotation]; state != "" {
		if err := json.Unmarshal([]byte(state), &previous); err != nil {
			return fmt.Errorf("read managed Gateway discovery metadata on %T %s", object, client.ObjectKeyFromObject(object))
		}
	}
	labels, annotations := object.GetLabels(), object.GetAnnotations()
	if labels == nil {
		labels = make(map[string]string)
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}

	desiredLabels, desiredAnnotations := make(map[string]string), make(map[string]string)
	if metadata := gateway.Spec.Infrastructure; metadata != nil {
		for key, value := range metadata.Labels {
			if !reservedDiscoveryLabel(string(key)) {
				desiredLabels[string(key)] = string(value)
			}
		}
		for key, value := range metadata.Annotations {
			if !reservedDiscoveryAnnotation(string(key)) {
				desiredAnnotations[string(key)] = string(value)
			}
		}
	}
	next := discoveryMetadata{
		Labels:      reconcileDiscoveryMetadataKeys(labels, previous.Labels, desiredLabels, reservedDiscoveryLabel),
		Annotations: reconcileDiscoveryMetadataKeys(annotations, previous.Annotations, desiredAnnotations, reservedDiscoveryAnnotation),
	}
	delete(labels, gatewayv1.GatewayNameLabelKey)
	delete(labels, gatewayv1.GatewayClassNameLabelKey)
	if len(validation.IsValidLabelValue(gateway.Name)) == 0 {
		labels[gatewayv1.GatewayNameLabelKey] = gateway.Name
	}
	if name := string(gateway.Spec.GatewayClassName); len(validation.IsValidLabelValue(name)) == 0 {
		labels[gatewayv1.GatewayClassNameLabelKey] = name
	}
	maps.Copy(labels, reservedLabels)
	annotations[discoveryControllerAnnotation] = string(r.Config.ControllerName)
	if len(next.Labels)+len(next.Annotations) == 0 {
		delete(annotations, discoveryMetadataAnnotation)
	} else {
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		annotations[discoveryMetadataAnnotation] = string(encoded)
	}
	object.SetLabels(labels)
	object.SetAnnotations(annotations)
	return nil
}

func reconcileDiscoveryMetadataKeys(current, previous, desired map[string]string, reserved func(string) bool) map[string]string {
	for key, digest := range previous {
		value, exists := current[key]
		_, wanted := desired[key]
		if !reserved(key) && !wanted && exists && metadataDigest(value) == digest {
			delete(current, key)
		}
	}
	managed := make(map[string]string)
	for key, value := range desired {
		if existing, exists := current[key]; exists && metadataDigest(existing) != previous[key] {
			// A separately added or overwritten key belongs to its author.
			continue
		}
		current[key] = value
		managed[key] = metadataDigest(value)
	}
	return managed
}

func metadataDigest(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func reservedDiscoveryLabel(key string) bool {
	switch key {
	case gatewayv1.GatewayNameLabelKey, gatewayv1.GatewayClassNameLabelKey, discoveryv1.LabelServiceName,
		discoveryv1.LabelManagedBy, discoveryv1.LabelSkipMirror, corev1.IsHeadlessService:
		return true
	}
	return false
}

func reservedDiscoveryAnnotation(key string) bool {
	return key == discoveryControllerAnnotation || key == discoveryMetadataAnnotation
}
