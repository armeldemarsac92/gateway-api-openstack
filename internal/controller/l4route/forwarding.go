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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

func (r *Reconciler) forwardingTargetType() string { return r.Config.Domain() + "/ForwardingTarget" }

// forwardingTarget hashes the desired destinations with the identities and
// routing semantics of the backends that contribute to the pool.
func forwardingTarget(backends []transportBackend, groups []routebackend.WeightedServiceMembers, weighted bool, spec cloud.RouteSpec) (string, error) {
	ports := spec.ScopedListenerPorts
	spec.ScopedListenerPorts = nil
	target, err := forwardingBackendTarget(backends, groups, weighted, spec)
	if err != nil || len(ports) == 0 {
		return target, err
	}
	scope, err := hashForwardingTarget(strings.ToLower(spec.GatewayRequirements.ListenerProtocol), ports)
	if err != nil {
		return "", err
	}
	return target + "/" + scope, nil
}

func forwardingBackendTarget(backends []transportBackend, groups []routebackend.WeightedServiceMembers, weighted bool, spec cloud.RouteSpec) (string, error) {
	// A rolling controller upgrade does not change the serving destinations.
	spec.Identity.ControllerVersion = ""
	type weightedService struct {
		Service       client.ObjectKey
		ServiceUID    types.UID
		ServiceType   corev1.ServiceType
		TrafficPolicy corev1.ServiceExternalTrafficPolicy
		Port          *corev1.ServicePort
		Weight        int32
		Members       []cloud.Member
	}
	type weightedXBackend struct {
		Key     client.ObjectKey
		UID     types.UID
		Port    gatewayx.PortNumber
		Weight  int32
		Members []cloud.Member
	}
	type weightedImport struct {
		Key     client.ObjectKey
		UID     types.UID
		Port    *corev1.ServicePort
		Weight  int32
		Members []cloud.Member
	}
	var imports []weightedImport
	var xBackends []weightedXBackend
	services := make([]weightedService, 0, len(backends))
	for i, backend := range backends {
		if backend.ref.serviceImport {
			if backend.importUID == "" {
				return "", fmt.Errorf("cannot identify the forwarding target for ServiceImport %s", backend.ref.key)
			}
			imports = append(imports, weightedImport{
				Key: backend.ref.key, UID: backend.importUID, Port: backend.port,
				Weight: backend.ref.weight, Members: groups[i].Members,
			})
			continue
		}
		if backend.xBackend != nil {
			if backend.xBackend.UID == "" {
				return "", fmt.Errorf("cannot identify the forwarding target for XBackend %s", backend.ref.key)
			}
			xBackends = append(xBackends, weightedXBackend{
				Key: backend.ref.key, UID: backend.xBackend.UID, Port: backend.xBackend.Spec.Port.Number,
				Weight: backend.ref.weight, Members: groups[i].Members,
			})
			continue
		}
		if backend.service.UID == "" {
			return "", fmt.Errorf("cannot identify the forwarding target for Service %s", backend.ref.key)
		}
		if weighted {
			services = append(services, weightedService{
				Service: backend.ref.key, ServiceUID: backend.service.UID, ServiceType: backend.service.Spec.Type,
				TrafficPolicy: backend.service.Spec.ExternalTrafficPolicy, Port: backend.port,
				Weight: backend.ref.weight, Members: groups[i].Members,
			})
		}
	}
	if len(imports) != 0 {
		return hashForwardingTarget("v4", struct {
			Services  []weightedService
			XBackends []weightedXBackend
			Imports   []weightedImport
			Route     cloud.RouteSpec
		}{services, xBackends, imports, spec})
	}
	if len(xBackends) != 0 {
		return hashForwardingTarget("v3", struct {
			Services  []weightedService
			XBackends []weightedXBackend
			Route     cloud.RouteSpec
		}{services, xBackends, spec})
	}
	if weighted {
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
	bindings, err := r.routeBindings(current)
	if err != nil {
		return err
	}
	binding := boundGateway(bindings, identity)
	if binding.Identity != identity || binding.Lifecycle != controller.RouteAttached {
		return controller.ErrGraphInputChanged
	}
	if r.needsDurableTarget(current) || binding.Forwarding.Value != "" {
		binding.Forwarding = controller.RouteGatewayTarget{Value: target, Generation: current.GetGeneration()}
		if err := plan.serviceImports.validate(ctx, r); err != nil {
			return err
		}
		if err := r.writeForwardingTarget(ctx, current, bindings, binding); err != nil {
			return err
		}
		*expected = *current
	}
	plan.targetStatus = routeCondition(current, r.forwardingTargetType(), metav1.ConditionFalse, "Pending", target)
	if err := r.patchParentStatuses(ctx, current, plan); err != nil {
		return err
	}
	*expected = *current
	return nil
}

func (r *Reconciler) needsDurableTarget(route *transportRoute) bool {
	return r.Config.DefaultGatewaysEnabled() || len(route.status.Parents) >= controller.RouteParentStatusLimit
}

func (r *Reconciler) recordObservedTarget(ctx context.Context, expected *transportRoute, identity cloud.Identity, target controller.RouteGatewayTarget, imports serviceImportInputs) error {
	release, err := graph.Acquire(ctx, r.Coordinator, identity.GatewayUID)
	if err != nil {
		return err
	}
	defer release()
	current, err := r.liveRoute(ctx, expected)
	if err != nil {
		return err
	}
	bindings, err := r.routeBindings(current)
	if err != nil {
		return err
	}
	binding := boundGateway(bindings, identity)
	if binding.Identity != identity || binding.Lifecycle != controller.RouteAttached || binding.Forwarding != target {
		return controller.ErrGraphInputChanged
	}
	if err := imports.validate(ctx, r); err != nil {
		return err
	}
	binding.Forwarding.Observed = true
	if err := r.writeForwardingTarget(ctx, current, bindings, binding); err != nil {
		return err
	}
	*expected = *current
	return nil
}

func (r *Reconciler) writeForwardingTarget(ctx context.Context, route *transportRoute, bindings []controller.RouteGatewayBinding, binding controller.RouteGatewayBinding) error {
	base := route.copyObject()
	if err := controller.ApplyRouteGatewayBinding(r.Config, route.Object, bindings, binding); err != nil {
		return err
	}
	return r.Patch(ctx, route.Object, controller.OptimisticMergeFrom(base))
}

func (r *Reconciler) targetCheckpointed(route *transportRoute, plan *routePlan, checkpoint controller.RouteGatewayTarget) bool {
	if checkpoint.Value != "" {
		return checkpoint.Generation == route.GetGeneration() && checkpoint.Value == plan.target
	}
	return r.currentTarget(route, plan.parent.ref, plan.target) != nil
}

func (r *Reconciler) retainObservedProgrammed(route *transportRoute, plan *routePlan, checkpoint controller.RouteGatewayTarget) bool {
	if checkpoint.Value != "" {
		if !checkpoint.Observed || !r.targetCheckpointed(route, plan, checkpoint) {
			return false
		}
		plan.programmed = r.programmedCondition(route)
		return true
	}
	target := r.currentTarget(route, plan.parent.ref, plan.target)
	return target != nil && target.Status == metav1.ConditionTrue && r.retainProgrammedCondition(route, plan)
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
	return condition != nil && matchesBackendTarget(condition.Message, target)
}

func matchesBackendTarget(checkpoint, target string) bool {
	desired, _, scoped := strings.Cut(target, "/")
	if !scoped {
		return checkpoint == target
	}
	current, _, _ := strings.Cut(checkpoint, "/")
	return current == desired
}

// A previous target can preserve freshly authorized forwarding across a
// ParentRef edit. UDP requires an identical target, including listener ports.
func (r *Reconciler) canRetainObservedTarget(route *transportRoute, target string, checkpoint controller.RouteGatewayTarget) bool {
	matches := func(previous string) bool {
		return previous == target || r.protocol() == gatewayv1.TCPProtocolType && matchesBackendTarget(previous, target)
	}
	if checkpoint.Value != "" {
		return checkpoint.Observed && checkpoint.Generation < route.GetGeneration() && matches(checkpoint.Value)
	}
	for _, parent := range route.status.Parents {
		if parent.ControllerName != r.Config.ControllerName {
			continue
		}
		condition := meta.FindStatusCondition(parent.Conditions, r.forwardingTargetType())
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Observed" ||
			condition.ObservedGeneration >= route.GetGeneration() {
			continue
		}
		if matches(condition.Message) {
			return true
		}
	}
	return false
}

func (r *Reconciler) planVersionSkew(ctx context.Context, route *transportRoute, plan *routePlan, backends []transportBackend, rejectWeight int, binding controller.RouteGatewayBinding) error {
	bound := binding.Identity.GatewayUID != ""
	if bound {
		confirmed, err := r.hasConfirmedForwarding(ctx, route, plan, backends, rejectWeight, binding.Forwarding)
		if err != nil && rejectWeight == 0 {
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

func (r *Reconciler) hasConfirmedForwarding(ctx context.Context, route *transportRoute, plan *routePlan, backends []transportBackend, rejectWeight int, checkpoint controller.RouteGatewayTarget) (bool, error) {
	parent := plan.parent
	expectedTarget := checkpoint.Value
	if expectedTarget != "" {
		if checkpoint.Generation != route.GetGeneration() || !checkpoint.Observed {
			return false, nil
		}
	} else {
		confirmed := r.currentParentCondition(route, parent.ref, r.forwardingTargetType())
		if confirmed == nil || confirmed.Status != metav1.ConditionTrue || !r.retainProgrammedCondition(route, plan) ||
			plan.programmed.Status != metav1.ConditionTrue {
			return false, nil
		}
		expectedTarget = confirmed.Message
	}
	topology, ready, err := r.gatewayTopology(parent.gateway, parent.listeners[0])
	if err != nil || !ready {
		return false, err
	}
	// A manager that cannot verify the installed API version cannot refresh
	// the destinations of an external hostname.
	if slices.ContainsFunc(backends, func(backend transportBackend) bool { return backend.externalHostname() != "" }) {
		return false, nil
	}
	_, target, diagnostic, err := r.buildRouteSpec(ctx, route, parent.gateway, parent.listeners[0], backends, rejectWeight, topology, nil, plan.listenerPorts...)
	if err != nil || diagnostic != nil {
		return false, err
	}
	if expectedTarget != target {
		return false, nil
	}
	if checkpoint.Value != "" {
		plan.programmed = r.programmedCondition(route)
	}
	return true, nil
}
