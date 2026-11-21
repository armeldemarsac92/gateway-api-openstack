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

package l4route

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

func (r *Reconciler) forwardingTargetType() string { return r.Config.Domain() + "/ForwardingTarget" }

// forwardingTarget hashes the desired destinations with the identities and
// routing semantics of the Services that contribute to the pool.
func forwardingTarget(backends []serviceBackend, groups []weightedServiceMembers, weighted bool, spec cloud.RouteSpec) (string, error) {
	ports := spec.UDPListenerPorts
	spec.UDPListenerPorts = nil
	target, err := forwardingBackendTarget(backends, groups, weighted, spec)
	if err != nil || len(ports) == 0 {
		return target, err
	}
	scope, err := hashForwardingTarget("udp", ports)
	if err != nil {
		return "", err
	}
	return target + "/" + scope, nil
}

func forwardingBackendTarget(backends []serviceBackend, groups []weightedServiceMembers, weighted bool, spec cloud.RouteSpec) (string, error) {
	for _, backend := range backends {
		if backend.service.UID == "" {
			return "", fmt.Errorf("cannot identify the forwarding target for Service %s", backend.ref.key)
		}
	}
	// A rolling controller upgrade does not change the serving destinations.
	spec.Identity.ControllerVersion = ""
	if weighted {
		type weightedService struct {
			Service       client.ObjectKey
			ServiceUID    types.UID
			ServiceType   corev1.ServiceType
			TrafficPolicy corev1.ServiceExternalTrafficPolicy
			Port          *corev1.ServicePort
			Weight        int32
			Members       []cloud.Member
		}
		services := make([]weightedService, 0, len(backends))
		for i, backend := range backends {
			services = append(services, weightedService{
				Service: backend.ref.key, ServiceUID: backend.service.UID, ServiceType: backend.service.Spec.Type,
				TrafficPolicy: backend.service.Spec.ExternalTrafficPolicy, Port: backend.port,
				Weight: backend.ref.weight, Members: groups[i].members,
			})
		}
		return hashForwardingTarget("v2", struct {
			Services []weightedService
			Route    cloud.RouteSpec
		}{services, spec})
	}
	// Preserve the existing singleton checkpoint across controller upgrades.
	backend := backends[0]
	return hashForwardingTarget("v1", struct {
		ServiceUID    types.UID
		ServiceType   corev1.ServiceType
		TrafficPolicy corev1.ServiceExternalTrafficPolicy
		Port          *corev1.ServicePort
		Route         cloud.RouteSpec
	}{backend.service.UID, backend.service.Spec.Type, backend.service.Spec.ExternalTrafficPolicy, backend.port, spec})
}

func hashForwardingTarget(version string, target any) (string, error) {
	input, err := json.Marshal(target)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%x", version, sha256.Sum256(input)), nil
}

func (r *Reconciler) checkpointTarget(ctx context.Context, expected *transportRoute, identity cloud.Identity, target string) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	current, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	plan, err := r.planRoute(ctx, current, identity, true)
	if err != nil {
		return err
	}
	if plan.desired == nil || plan.target != target {
		return controller.ErrGraphInputChanged
	}
	// Replace the previous proof before either route- or Gateway-triggered
	// writes can change the destinations of the bound pool.
	plan.targetStatus = routeCondition(current, r.forwardingTargetType(), metav1.ConditionFalse, "Pending", target)
	return r.patchStatuses(ctx, current, plan)
}

func (r *Reconciler) currentTarget(route *transportRoute, parent gatewayv1.ParentReference, target string) *metav1.Condition {
	condition := r.currentParentCondition(route, parent, r.forwardingTargetType())
	if condition == nil || condition.Message != target {
		return nil
	}
	return condition
}

// hasCurrentBackendTarget allows a sealed graph to update a route's listener
// edges while retaining its existing checkpoint for the backend pool.
func (r *Reconciler) hasCurrentBackendTarget(route *transportRoute, parent gatewayv1.ParentReference, target string) bool {
	condition := r.currentParentCondition(route, parent, r.forwardingTargetType())
	if condition == nil {
		return false
	}
	desired, _, scoped := strings.Cut(target, "/")
	if !scoped {
		return condition.Message == target
	}
	current, _, _ := strings.Cut(condition.Message, "/")
	return current == desired
}

func (r *Reconciler) planVersionSkew(ctx context.Context, route *transportRoute, plan *routePlan, backends []serviceBackend, bound bool) error {
	if bound {
		confirmed, err := r.hasConfirmedForwarding(ctx, route, plan, backends)
		if err != nil {
			return err
		}
		plan.detach = !confirmed
	}
	if !bound || plan.detach {
		plan.programmed = routeCondition(route, "Programmed", metav1.ConditionUnknown, "UnsupportedVersion", controller.UnsupportedGatewayAPIVersionMessage)
	}
	plan.requeue = controller.GatewayAPIVersionRequeueAfter(route.GetUID())
	return nil
}

func (r *Reconciler) hasConfirmedForwarding(ctx context.Context, route *transportRoute, plan *routePlan, backends []serviceBackend) (bool, error) {
	parent := plan.parents[0]
	confirmed := r.currentParentCondition(route, parent.ref, r.forwardingTargetType())
	if confirmed == nil || confirmed.Status != metav1.ConditionTrue || !r.retainProgrammedCondition(route, plan) ||
		plan.programmed.Status != metav1.ConditionTrue {
		return false, nil
	}
	topology, ready, err := r.gatewayTopology(parent.gateway, parent.listeners[0])
	if err != nil || !ready {
		return false, err
	}
	_, target, diagnostic, err := r.routeSpec(ctx, route, parent.gateway, parent.listeners[0], backends, topology, plan.listenerPorts...)
	if err != nil || diagnostic != nil {
		return false, err
	}
	return confirmed.Message == target, nil
}
