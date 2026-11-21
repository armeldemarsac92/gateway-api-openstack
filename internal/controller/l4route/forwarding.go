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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

func (r *Reconciler) forwardingTargetType() string { return r.Config.Domain() + "/ForwardingTarget" }

// forwardingTarget hashes the actual desired Octavia destinations together
// with the identity and forwarding semantics of the Service that owns them.
func (r *Reconciler) forwardingTarget(service *corev1.Service, ref serviceReference, spec cloud.RouteSpec) (string, error) {
	port, diagnostic := model.ResolveTransportServicePort(service, ref.port, corev1.Protocol(r.protocol()))
	if service.UID == "" || diagnostic != nil {
		return "", fmt.Errorf("cannot identify the forwarding target for Service %s", ref.key)
	}
	// A rolling controller upgrade does not change the serving destinations.
	spec.Identity.ControllerVersion = ""
	input, err := json.Marshal(struct {
		ServiceUID    types.UID
		ServiceType   corev1.ServiceType
		TrafficPolicy corev1.ServiceExternalTrafficPolicy
		Port          *corev1.ServicePort
		Route         cloud.RouteSpec
	}{service.UID, service.Spec.Type, service.Spec.ExternalTrafficPolicy, port, spec})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("v1:%x", sha256.Sum256(input)), nil
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

func (r *Reconciler) planVersionSkew(ctx context.Context, route *transportRoute, plan *routePlan, ref serviceReference, service *corev1.Service, bound bool) error {
	if bound {
		confirmed, err := r.hasConfirmedForwarding(ctx, route, plan, ref, service)
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

func (r *Reconciler) hasConfirmedForwarding(ctx context.Context, route *transportRoute, plan *routePlan, ref serviceReference, service *corev1.Service) (bool, error) {
	parent := plan.parents[0]
	confirmed := r.currentParentCondition(route, parent.ref, r.forwardingTargetType())
	if confirmed == nil || confirmed.Status != metav1.ConditionTrue || !r.retainProgrammedCondition(route, plan) ||
		plan.programmed.Status != metav1.ConditionTrue {
		return false, nil
	}
	topology, ready, err := r.gatewayTopology(parent.gateway, parent.listener)
	if err != nil || !ready {
		return false, err
	}
	spec, diagnostic, err := r.routeSpec(ctx, route, parent.gateway, parent.listener, ref, service, topology)
	if err != nil || diagnostic != nil {
		return false, err
	}
	target, err := r.forwardingTarget(service, ref, spec)
	return err == nil && confirmed.Message == target, err
}
