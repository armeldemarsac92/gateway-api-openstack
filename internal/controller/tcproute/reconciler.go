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

package tcproute

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

// Reconciler converges the TCPRoute component of an Octavia Gateway graph.
type Reconciler struct {
	client.Client
	APIReader   client.Reader
	Config      controller.Config
	Writer      graph.Reconciler
	Coordinator *graph.Coordinator
}

type routePlan struct {
	parents    []routeParent
	resolved   metav1.Condition
	programmed metav1.Condition
	desired    *cloud.RouteSpec
	detach     bool
	requeue    time.Duration
}

// Reconcile evaluates listener attachment and backend references independently.
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes/finalizers,verbs=update
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.APIReader == nil {
		return ctrl.Result{}, controller.ErrAPIReaderRequired
	}
	route := &gatewayv1.TCPRoute{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, route); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	binding, bound, err := r.routeBinding(route)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !route.DeletionTimestamp.IsZero() || route.Annotations[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
		if !bound {
			return ctrl.Result{}, nil
		}
		return r.detach(ctx, route, binding)
	}
	plan, err := r.planRoute(ctx, route, binding, bound)
	if err != nil {
		return ctrl.Result{}, err
	}
	if plan.detach {
		return r.detach(ctx, route, binding)
	}
	result := ctrl.Result{RequeueAfter: plan.requeue}
	if plan.desired != nil {
		if !bound {
			return r.bind(ctx, route, plan.desired.Identity)
		}
		result, err = r.ensure(ctx, route, &plan)
		if errors.Is(err, controller.ErrGraphInputChanged) || errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return result, err
		}
	}
	return result, errors.Join(err, r.patchStatuses(ctx, route, plan))
}

func (r *Reconciler) planRoute(ctx context.Context, route *gatewayv1.TCPRoute, binding cloud.Identity, bound bool) (routePlan, error) {
	plan := routePlan{}
	plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "Pending", "TCP forwarding has not been programmed")
	plan.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionUnknown, "Pending", "Backend references have not been resolved")
	if bound && !routeStillReferences(route, binding) {
		plan.detach = true
		return plan, nil
	}
	var err error
	plan.parents, err = r.managedParents(ctx, route)
	if err != nil {
		return routePlan{}, err
	}
	if len(plan.parents) == 0 {
		plan.detach = bound
		return plan, nil
	}
	if len(route.Spec.ParentRefs) != 1 {
		for i := range plan.parents {
			plan.parents[i].listener = nil
			plan.parents[i].rejected = model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "TCP forwarding supports exactly one Gateway parentRef"}
		}
		return plan, nil
	}
	parent := plan.parents[0]
	if bound && (parent.listener == nil || !controller.SameGatewayIdentity(binding, controller.RouteIdentity(r.Config, parent.gateway, route))) {
		plan.detach = true
		return plan, nil
	}
	ref, diagnostic, err := r.authorizedBackend(ctx, route)
	if err != nil {
		return routePlan{}, err
	}
	if diagnostic != nil || ref.disabled && bound {
		plan.rejectBackend(route, diagnostic)
		plan.detach = bound
		return plan, nil
	}
	if parent.listener != nil {
		routes, err := r.listRoutes(ctx)
		if err != nil {
			return routePlan{}, err
		}
		selected, err := r.selectedRoute(ctx, parent.gateway, parent.listener, routes)
		if err != nil {
			return routePlan{}, err
		}
		if selected == nil || selected.UID != route.UID {
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "NotSelected", "An older TCPRoute takes precedence on this listener")
			plan.detach = bound
		}
	}
	if plan.detach {
		return plan, nil
	}
	service, diagnostic, err := r.backendService(ctx, ref)
	if err != nil {
		return routePlan{}, err
	}
	plan.rejectBackend(route, diagnostic)
	if diagnostic != nil {
		plan.detach = bound
		return plan, nil
	}
	err = r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader)
	if err != nil && !errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
		return routePlan{}, err
	}
	if err != nil || !controller.GatewayClassSupportsInstalledVersion(parent.class) {
		if bound {
			_, current, err := r.gatewayTopology(parent.gateway, parent.listener)
			if err != nil {
				return routePlan{}, err
			}
			plan.detach = !current || !r.retainProgrammedCondition(route, &plan) || plan.programmed.Status != metav1.ConditionTrue
		}
		if !bound || plan.detach {
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionUnknown, "UnsupportedVersion", controller.UnsupportedGatewayAPIVersionMessage)
		}
		plan.requeue = controller.GatewayAPIVersionRequeueAfter(route.UID)
		return plan, nil
	}
	if ref.disabled {
		plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "ZeroWeight", "The only backend has weight zero")
		return plan, nil
	}
	if parent.listener == nil || plan.programmed.Reason == "NotSelected" {
		return plan, nil
	}
	topology, ready, err := r.gatewayTopology(parent.gateway, parent.listener)
	if err != nil {
		return routePlan{}, err
	}
	if !ready || !bound && !gatewayProgrammed(parent.gateway) {
		plan.requeue = 5 * time.Second
		return plan, nil
	}
	desired, diagnostic, err := r.routeSpec(ctx, route, parent.gateway, parent.listener, ref, service, topology)
	if err != nil {
		return routePlan{}, err
	}
	if diagnostic != nil {
		if diagnostic.Reason == string(gatewayv1.RouteReasonPending) {
			// The Service and port resolved. Member readiness controls forwarding.
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
		} else {
			plan.rejectBackend(route, diagnostic)
		}
		plan.detach = bound
		return plan, nil
	}
	plan.desired = &desired
	return plan, nil
}

func routeStillReferences(route *gatewayv1.TCPRoute, identity cloud.Identity) bool {
	if len(route.Spec.ParentRefs) != 1 {
		return false
	}
	ref := route.Spec.ParentRefs[0]
	key := gatewayKey(route, ref)
	return controller.IsGatewayParentRef(ref) && key.Namespace == identity.GatewayNamespace && key.Name == identity.GatewayName
}

// gatewayTopology admits only the current TCP listener and checkpointed topology.
func (r *Reconciler) gatewayTopology(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (classconfig.Resolved, bool, error) {
	if !controllerutil.ContainsFinalizer(gateway, r.Config.GatewayFinalizer()) ||
		gateway.Annotations[r.Config.GatewayListenerProtocolAnnotation()] != string(gatewayv1.TCPProtocolType) ||
		gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(int(listener.Port)) ||
		!gateway.DeletionTimestamp.IsZero() {
		return classconfig.Resolved{}, false, nil
	}
	if err := controller.ValidateGatewayBinding(r.Config, gateway); err != nil {
		return classconfig.Resolved{}, false, err
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	return topology, err == nil, err
}

func gatewayProgrammed(gateway *gatewayv1.Gateway) bool {
	condition := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == gateway.Generation
}

func (r *Reconciler) routeSpec(ctx context.Context, route *gatewayv1.TCPRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, ref serviceReference, service *corev1.Service, topology classconfig.Resolved) (cloud.RouteSpec, *model.Diagnostic, error) {
	members, diagnostic, err := routebackend.TCPMembers(ctx, r.APIReader, service, ref.port, topology)
	if err != nil || diagnostic != nil {
		return cloud.RouteSpec{}, diagnostic, err
	}
	return cloud.RouteSpec{
		Identity: controller.RouteIdentity(r.Config, gateway, route),
		GatewayRequirements: cloud.GatewayRequirements{
			Provider: "amphora", ListenerProtocol: cloud.ListenerProtocolTCP, ListenerPort: int(listener.Port),
			VIPSubnetID: topology.Topology.VIPSubnetID, ExternalNetworkID: topology.Topology.ExternalNetworkID,
			FlavorID: topology.Topology.FlavorID, AvailabilityZone: topology.Topology.AvailabilityZone,
			VIPSecurityGroupIDs: topology.Topology.VIPSecurityGroupIDs,
		},
		MemberMode: topology.Topology.MemberMode, MemberSubnetID: topology.Topology.MemberSubnetID,
		BackendSecurityGroupIDs: topology.Topology.BackendSecurityGroupIDs,
		HealthMonitor:           topology.HealthMonitor, Members: members,
	}, nil, nil
}

func (r *Reconciler) ensure(ctx context.Context, route *gatewayv1.TCPRoute, plan *routePlan) (ctrl.Result, error) {
	if r.Writer == nil {
		return ctrl.Result{}, errors.New("gateway graph writer is required")
	}
	identity := plan.desired.Identity
	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{Identity: identity, Action: graph.Ensure, Gateway: plan.parents[0].gateway, TCPRoute: route})
	if err != nil {
		if errors.Is(err, controller.ErrGraphInputChanged) || errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return ctrl.Result{}, err
		}
		if siblingFailure(err, identity) {
			return ctrl.Result{}, controller.SafeProviderReconcileError(controller.ProviderFailurePolicy{Message: "Another route in the Gateway graph could not be reconciled"}, err)
		}
		policy, classified := controller.ProviderFailurePolicyFor(err)
		if !classified {
			policy = controller.ProviderFailurePolicy{
				ConditionStatus: metav1.ConditionUnknown, Reason: controller.ProviderReasonUnavailable,
				Message: "OpenStack could not program the TCPRoute", ReturnError: true,
			}
		}
		plan.programmed = routeCondition(route, "Programmed", policy.ConditionStatus, policy.Reason, policy.Message)
		return controller.ProviderFailureResult(policy, err, string(route.UID))
	}
	component, err := routeResult(result, identity)
	if err != nil {
		return ctrl.Result{}, err
	}
	if component.Outcome.State == cloud.OutcomeProgressing {
		if !component.ObservationDeferred || !r.retainProgrammedCondition(route, plan) {
			message := controller.ProviderProgressMessage(component.Outcome, "Octavia is programming the TCP forwarding configuration")
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "Pending", message)
		}
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(component.Outcome, route.UID)}, nil
	}
	plan.programmed = routeCondition(route, "Programmed", metav1.ConditionTrue, "Programmed", "Octavia programmed the TCP forwarding configuration")
	return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, route.UID)}, nil
}

func siblingFailure(err error, expected cloud.Identity) bool {
	var fragment *cloud.GraphError
	if !errors.As(err, &fragment) || fragment.Identity.RouteUID == "" || fragment.Identity.RouteUID == expected.RouteUID {
		return false
	}
	parent := fragment.Identity
	parent.RouteNamespace, parent.RouteName, parent.RouteUID = expected.RouteNamespace, expected.RouteName, expected.RouteUID
	return parent == expected
}

func routeResult(result cloud.GatewayGraphResult, expected cloud.Identity) (cloud.RouteResult, error) {
	var found *cloud.RouteResult
	for _, component := range result.Routes {
		if component.Identity.RouteUID != expected.RouteUID {
			continue
		}
		if component.Identity != expected || found != nil {
			return cloud.RouteResult{}, errors.New("provider returned an inconsistent TCPRoute component identity")
		}
		found = &component.Result
	}
	if found == nil {
		if result.Outcome.State == cloud.OutcomeProgressing {
			return cloud.RouteResult{Outcome: result.Outcome, ObservationDeferred: true}, nil
		}
		return cloud.RouteResult{}, errors.New("provider omitted the TCPRoute component result")
	}
	if err := found.Outcome.Validate(); err != nil {
		return cloud.RouteResult{}, fmt.Errorf("validate TCPRoute component outcome: %w", err)
	}
	if found.ObservationDeferred && found.Outcome.State != cloud.OutcomeProgressing {
		return cloud.RouteResult{}, errors.New("deferred TCPRoute observation cannot certify completion")
	}
	return *found, nil
}

func sameRouteRevision(current, expected *gatewayv1.TCPRoute) bool {
	return current.UID == expected.UID && current.Generation == expected.Generation &&
		current.DeletionTimestamp.Equal(expected.DeletionTimestamp) && reflect.DeepEqual(current.Spec, expected.Spec)
}
