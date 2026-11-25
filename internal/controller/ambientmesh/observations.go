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

package ambientmesh

import (
	"context"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	waypointClassName                              = "istio-waypoint"
	waypointController gatewayv1.GatewayController = "istio.io/mesh-controller"
	istioNamespace                                 = "istio-system"

	// These images match the pinned Istio 1.31.0 module and its consumer pilot.
	stockPilot    = "docker.io/istio/pilot@sha256:17b581d9d75a1ab319d2816230626f06d65b48e3e798f98e3092a031843b3fe0"
	consumerPilot = "ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:fdb9d02e2b427f81d2763fce329361ac32432080cc14b7115315bed4508bbcc5"
	cniImage      = "docker.io/istio/install-cni@sha256:fe7081ed2605166fef573a7987f183e91fd45c2a302f90998c879ca20112426e"
	ztunnelImage  = "docker.io/istio/ztunnel@sha256:efa1115b58acd969bf876dfcee27751714e3ae317b2518e1a1d61f3ef037587e"
	cniConfigName = "istio-cni-config"
)

func (r *reconciler) observeIstio(ctx context.Context) (ready bool, detail string, err error) {
	var class gatewayv1.GatewayClass
	if err := r.apiReader.Get(ctx, client.ObjectKey{Name: waypointClassName}, &class); err != nil {
		return false, "waiting for GatewayClass/istio-waypoint", client.IgnoreNotFound(err)
	}
	if class.Spec.ControllerName != waypointController || !class.DeletionTimestamp.IsZero() {
		return false, "waiting for GatewayClass/istio-waypoint under istio.io/mesh-controller", nil
	}
	accepted := meta.FindStatusCondition(class.Status.Conditions, string(gatewayv1.GatewayClassConditionStatusAccepted))
	if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != class.Generation {
		return false, "waiting for GatewayClass/istio-waypoint to be accepted at its current generation", nil
	}

	var istiod appsv1.Deployment
	if err := r.apiReader.Get(ctx, client.ObjectKey{Namespace: istioNamespace, Name: "istiod"}, &istiod); err != nil {
		return false, "waiting for Deployment istio-system/istiod", client.IgnoreNotFound(err)
	}
	if !deploymentReady(&istiod) {
		return false, "waiting for the current istio-system/istiod rollout to finish", nil
	}
	pilot := findContainer(istiod.Spec.Template.Spec, "discovery")
	if !pilotConfigured(pilot) {
		return false, "waiting for the qualified ambient and Gateway API settings in istio-system/istiod", nil
	}

	cni, err := r.observeDaemonSet(ctx, "istio-cni-node", "install-cni", cniImage)
	if cni == nil || err != nil {
		return false, "waiting for the qualified istio-system/istio-cni-node rollout", err
	}
	configured, err := r.cniConfigured(ctx, cni)
	if !configured || err != nil {
		return false, "waiting for ambient IPv4 settings in istio-system/istio-cni-node", err
	}

	ztunnel, err := r.observeDaemonSet(ctx, "ztunnel", "istio-proxy", ztunnelImage)
	if ztunnel == nil || err != nil {
		return false, "waiting for the qualified istio-system/ztunnel rollout", err
	}
	if len(ztunnel.EnvFrom) != 0 || envValue(ztunnel.Env, "IPV6_ENABLED", "") != "false" {
		return false, "waiting for IPv4 settings in istio-system/ztunnel", nil
	}
	return true, "ready with the accepted istio-waypoint class and qualified Istio rollouts", nil
}

func deploymentReady(deployment *appsv1.Deployment) bool {
	desired := deployment.Spec.Replicas
	return deployment.DeletionTimestamp.IsZero() && desired != nil && *desired > 0 &&
		deployment.Status.ObservedGeneration == deployment.Generation &&
		deployment.Status.Replicas == *desired && deployment.Status.UpdatedReplicas == *desired &&
		deployment.Status.AvailableReplicas == *desired
}

func (r *reconciler) observeDaemonSet(ctx context.Context, name, containerName, image string) (*corev1.Container, error) {
	var daemonSet appsv1.DaemonSet
	if err := r.apiReader.Get(ctx, client.ObjectKey{Namespace: istioNamespace, Name: name}, &daemonSet); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	status := daemonSet.Status
	desired := status.DesiredNumberScheduled
	if !daemonSet.DeletionTimestamp.IsZero() || status.ObservedGeneration != daemonSet.Generation ||
		desired == 0 || status.NumberMisscheduled != 0 || status.CurrentNumberScheduled != desired ||
		status.UpdatedNumberScheduled != desired || status.NumberAvailable != desired {
		return nil, nil
	}
	container := findContainer(daemonSet.Spec.Template.Spec, containerName)
	if container == nil || container.Image != image {
		return nil, nil
	}
	return container, nil
}

func findContainer(podSpec corev1.PodSpec, name string) *corev1.Container {
	index := slices.IndexFunc(podSpec.Containers, func(container corev1.Container) bool {
		return container.Name == name
	})
	if index == -1 {
		return nil
	}
	return &podSpec.Containers[index]
}

func pilotConfigured(pilot *corev1.Container) bool {
	if pilot == nil || len(pilot.EnvFrom) != 0 || (pilot.Image != stockPilot && pilot.Image != consumerPilot) {
		return false
	}
	// Use the registered defaults in the pinned Istio versions for flags the
	// chart leaves unset. A reference which we cannot resolve yields Unknown.
	return envValue(pilot.Env, "PILOT_ENABLE_AMBIENT", "false") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_AMBIENT_WAYPOINTS", "true") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_SENDING_HBONE", "true") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_GATEWAY_API", "true") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_GATEWAY_API_STATUS", "true") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_GATEWAY_API_DEPLOYMENT_CONTROLLER", "true") == "true" &&
		envValue(pilot.Env, "PILOT_ENABLE_GATEWAY_API_GATEWAYCLASS_CONTROLLER", "true") == "false"
}

func (r *reconciler) cniConfigured(ctx context.Context, container *corev1.Container) (bool, error) {
	if len(container.EnvFrom) != 1 {
		return false, nil
	}
	source := container.EnvFrom[0]
	if source.Prefix != "" || source.ConfigMapRef == nil || source.ConfigMapRef.Name != cniConfigName {
		return false, nil
	}
	var config corev1.ConfigMap
	if err := r.apiReader.Get(ctx, client.ObjectKey{Namespace: istioNamespace, Name: cniConfigName}, &config); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return config.DeletionTimestamp.IsZero() &&
		envValue(container.Env, "AMBIENT_ENABLED", config.Data["AMBIENT_ENABLED"]) == "true" &&
		envValue(container.Env, "AMBIENT_IPV6", config.Data["AMBIENT_IPV6"]) == "false", nil
}

func envValue(env []corev1.EnvVar, name, fallback string) string {
	for _, variable := range env {
		if variable.Name == name {
			fallback = variable.Value
		}
	}
	return fallback
}
