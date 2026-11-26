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

package controller

import (
	"context"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// XBackendsForPod finds selectors matched by a ready Pod in its namespace.
func XBackendsForPod(ctx context.Context, reader client.Reader, object client.Object) []reconcile.Request {
	pod, ok := object.(*corev1.Pod)
	if !ok || !model.XBackendPodReady(pod) {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	for key, value := range pod.Labels {
		var backends gatewayx.XBackendList
		if err := reader.List(ctx, &backends, client.InNamespace(pod.Namespace), client.MatchingFields{
			IndexXBackendByPodLabel: PodLabelIndexKey(key, value),
		}); err != nil {
			log.FromContext(ctx).Error(err, "Could not list XBackends matching Pod labels", "pod", client.ObjectKeyFromObject(pod))
			continue
		}
		for i := range backends.Items {
			backend := &backends.Items[i]
			if backend.DeletionTimestamp.IsZero() && model.XBackendSelectsLabels(backend.Spec, pod.Labels) {
				keys[client.ObjectKeyFromObject(backend)] = struct{}{}
			}
		}
	}
	return SortedRequests(keys)
}

// XBackendPodPredicate matches changes that can affect XBackend destinations.
func XBackendPodPredicate() predicate.Predicate {
	ready := func(object client.Object) bool {
		pod, ok := object.(*corev1.Pod)
		return ok && model.XBackendPodReady(pod)
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return ready(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool { return ready(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK {
				return false
			}
			oldReady, newReady := model.XBackendPodReady(oldPod), model.XBackendPodReady(newPod)
			return oldReady != newReady || newReady &&
				(!maps.Equal(oldPod.Labels, newPod.Labels) || oldPod.Status.PodIP != newPod.Status.PodIP ||
					!slices.Equal(oldPod.Status.PodIPs, newPod.Status.PodIPs))
		},
	}
}
