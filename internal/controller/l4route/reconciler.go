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
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
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

// Reconciler converges one transport route kind in an Octavia Gateway graph.
type Reconciler struct {
	client.Client
	APIReader                       client.Reader
	Config                          controller.Config
	Writer                          graph.Reconciler
	Coordinator                     *graph.Coordinator
	Protocol                        gatewayv1.ProtocolType // TCP (including the zero value) or UDP.
	TCPConnectionRejectionAvailable func(context.Context) (bool, error)
	UDPPacketDropAvailable          func(context.Context) (bool, error)
}

type routePlan struct {
	parents       []routeParent
	resolved      metav1.Condition
	programmed    metav1.Condition
	desired       *cloud.RouteSpec
	target        string
	targetStatus  metav1.Condition
	listenerPorts []int
	detach        bool
	requeue       time.Duration
	readError     error
}

// Reconcile evaluates listener attachment and backend references independently.
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tcproutes/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=udproutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=udproutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=udproutes/finalizers,verbs=update
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.APIReader == nil {
		return ctrl.Result{}, controller.ErrAPIReaderRequired
	}
	object := r.newRouteObject()
	if err := r.APIReader.Get(ctx, req.NamespacedName, object); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	route := viewRoute(object)
	binding, bound, err := r.routeBinding(route)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !route.GetDeletionTimestamp().IsZero() || route.GetAnnotations()[r.Config.RouteLifecycleAnnotation()] == controller.RouteDetaching {
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
		if r.currentTarget(route, plan.parents[0].ref, plan.target) == nil {
			return ctrl.Result{Requeue: true}, r.checkpointTarget(ctx, route, binding, plan.target)
		}
		result, err = r.ensure(ctx, route, &plan)
		if errors.Is(err, controller.ErrGraphInputChanged) || errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return result, err
		}
	}
	if plan.requeue != 0 && (result.RequeueAfter == 0 || plan.requeue < result.RequeueAfter) {
		result.RequeueAfter = plan.requeue
	}
	return result, errors.Join(err, plan.readError, r.patchStatuses(ctx, route, plan))
}

func (r *Reconciler) tcpConnectionRejectionEnabled() bool {
	return r.protocol() == gatewayv1.TCPProtocolType && r.Config.TCPConnectionRejection
}

// A pool with UDP packet discard can serve exactly one IPv4 Endpoint listener.
func (r *Reconciler) udpPacketDropEnabled(gateway *gatewayv1.Gateway, topology classconfig.Resolved, ports []int) bool {
	if r.protocol() != gatewayv1.UDPProtocolType || !r.Config.UDPPacketDrop ||
		topology.Topology.MemberMode != cloud.MemberModeEndpoint || len(ports) != 1 || len(gateway.Status.Addresses) != 1 {
		return false
	}
	address := gateway.Status.Addresses[0]
	ip, err := netip.ParseAddr(address.Value)
	return err == nil && ip.Is4() && !ip.IsUnspecified() && (address.Type == nil || *address.Type == gatewayv1.IPAddressType)
}

func (r *Reconciler) planRoute(ctx context.Context, route *transportRoute, binding cloud.Identity, bound bool) (routePlan, error) {
	plan := routePlan{}
	plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "Pending", fmt.Sprintf("%s forwarding has not been programmed", r.protocol()))
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
	if len(route.parentRefs) != 1 {
		for i := range plan.parents {
			plan.parents[i].listeners = nil
			plan.parents[i].rejected = model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: fmt.Sprintf("%s forwarding supports exactly one Gateway parentRef", r.protocol())}
		}
		return plan, nil
	}
	parent := plan.parents[0]
	if bound && (len(parent.listeners) == 0 || !controller.SameGatewayIdentity(binding, controller.RouteIdentity(r.Config, parent.gateway, route))) {
		plan.detach = true
		return plan, nil
	}
	if bound {
		routePort := controller.ScopedRouteAnchor(r.Config, route, r.protocol())
		if routePort != 0 && (parent.gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(routePort) ||
			parent.gateway.Annotations[r.Config.GatewayListenerProtocolAnnotation()] != string(r.protocol())) {
			if err := controller.ValidateGatewayBinding(r.Config, parent.gateway); err != nil {
				return routePlan{}, err
			}
			plan.detach = true
			return plan, nil
		}
	}
	collectTCP := r.tcpConnectionRejectionEnabled()
	// Collect known UDP denials before election so a failed election read
	// cannot leave a revoked target serving. Full eligibility follows election.
	collectUDP := r.protocol() == gatewayv1.UDPProtocolType && r.Config.UDPPacketDrop
	refs, authRejections, err := r.authorizedBackends(ctx, route, true, collectTCP || collectUDP)
	if err != nil {
		return routePlan{}, err
	}
	collect := (collectTCP || collectUDP) && authRejections.rejectWeight > 0
	if authRejections.active != nil && !collect {
		plan.rejectBackend(route, authRejections.active)
		plan.detach = bound
		return plan, nil
	}
	if len(parent.listeners) != 0 {
		plan.listenerPorts, err = r.wonListenerPorts(ctx, parent.gateway, parent.listeners, route)
		if err != nil {
			if collect {
				plan.holdUnreadBackends(route, authRejections.active, bound, err)
				return plan, nil
			}
			return routePlan{}, err
		}
		if len(plan.listenerPorts) == 0 {
			message := fmt.Sprintf("An older %s takes precedence on this listener", r.kind())
			if len(parent.listeners) > 1 {
				message = fmt.Sprintf("An older %s takes precedence on each selected listener", r.kind())
			}
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "NotSelected", message)
			plan.detach = bound
		}
	}
	if plan.detach {
		return plan, nil
	}
	// Validate disabled Services in the Gateway's member mode without reading
	// their destinations. Before a topology is bound, keep the Endpoint checks
	// used for multiple references.
	var inactiveMode, activeMode cloud.MemberMode
	var collectUDPShare bool
	if len(route.backendRefs) > 1 {
		inactiveMode = cloud.MemberModeEndpoint
	}
	if (inactiveMode != "" || collectTCP || collectUDP) && controller.HasGatewayTopologyBinding(r.Config, parent.gateway) {
		topology, err := controller.StoredGatewayTopology(r.Config, parent.gateway)
		if err != nil {
			if collect {
				plan.holdUnreadBackends(route, authRejections.active, bound, err)
				return plan, nil
			}
			return routePlan{}, err
		}
		if inactiveMode != "" {
			inactiveMode = topology.Topology.MemberMode
		}
		collectUDPShare = r.udpPacketDropEnabled(parent.gateway, topology, plan.listenerPorts)
		if collectTCP || collectUDPShare {
			activeMode = topology.Topology.MemberMode
		}
	}
	collectShare := collectTCP || collectUDPShare
	if authRejections.active != nil && !collectShare {
		plan.rejectBackend(route, authRejections.active)
		plan.detach = bound
		return plan, nil
	}
	backends, serviceRejections, err := r.backendServices(ctx, refs, inactiveMode, activeMode, collectShare)
	if err != nil {
		return routePlan{}, err
	}
	activeRejection := cmp.Or(authRejections.active, serviceRejections.active)
	inactiveRejection := cmp.Or(authRejections.inactive, serviceRejections.inactive)
	rejectWeight := authRejections.rejectWeight + serviceRejections.rejectWeight
	plan.setResolvedRefs(route, cmp.Or(activeRejection, inactiveRejection))
	if authRejections.inactivePending || serviceRejections.inactivePending {
		if activeRejection == nil && inactiveRejection == nil {
			plan.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionUnknown, "Pending", "A disabled backend reference could not be read")
		}
		plan.requeue = 5 * time.Second
	}
	if activeRejection != nil && !collectShare {
		plan.rejectBackend(route, activeRejection)
		plan.detach = bound
		return plan, nil
	}
	if readErr := errors.Join(authRejections.readError, serviceRejections.readError); readErr != nil {
		plan.holdUnreadBackends(route, activeRejection, bound, readErr)
		return plan, nil
	}
	if len(backends) == 0 && rejectWeight == 0 {
		if inactiveRejection != nil {
			plan.rejectBackend(route, inactiveRejection)
		} else if plan.programmed.Reason != "NotSelected" {
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "ZeroWeight", "All backend references have weight zero")
		}
		plan.detach = bound
		return plan, nil
	}
	err = r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader)
	if err != nil && !errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
		if activeRejection != nil && collectShare {
			plan.holdUnreadBackends(route, activeRejection, bound, err)
			return plan, nil
		}
		return routePlan{}, err
	}
	if err != nil || !controller.GatewayClassSupportsInstalledVersion(parent.class) {
		err := r.planVersionSkew(ctx, route, &plan, backends, rejectWeight, bound)
		return plan, err
	}
	if len(parent.listeners) == 0 || plan.programmed.Reason == "NotSelected" {
		return plan, nil
	}
	topology, ready, err := r.gatewayTopology(parent.gateway, parent.listeners[0])
	if err != nil {
		if activeRejection != nil && collectShare {
			plan.holdUnreadBackends(route, activeRejection, bound, err)
			return plan, nil
		}
		return routePlan{}, err
	}
	if !ready || !bound && !gatewayProgrammed(parent.gateway) {
		if activeRejection != nil {
			plan.rejectBackend(route, activeRejection)
			plan.detach = bound
		}
		plan.requeue = 5 * time.Second
		return plan, nil
	}
	desired, target, diagnostic, err := r.buildRouteSpec(ctx, route, parent.gateway, parent.listeners[0], backends, rejectWeight, topology, plan.listenerPorts...)
	if err != nil {
		if !collectShare {
			return routePlan{}, err
		}
		plan.holdUnreadBackends(route, activeRejection, bound, err)
		return plan, nil
	}
	if diagnostic != nil {
		if activeRejection != nil {
			plan.rejectBackend(route, activeRejection)
		} else if diagnostic.Condition == "Programmed" || diagnostic.Reason == string(gatewayv1.RouteReasonPending) {
			// The backend references are evaluated separately from pool readiness and limits.
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, diagnostic.Reason, diagnostic.Message)
		} else {
			plan.rejectBackend(route, diagnostic)
		}
		plan.detach = bound
		if collectShare {
			plan.requeue = 5 * time.Second
		}
		return plan, nil
	}
	plan.target = target
	plan.desired = &desired
	return plan, nil
}

func (p *routePlan) holdUnreadBackends(route *transportRoute, active *model.Diagnostic, bound bool, err error) {
	if active != nil {
		p.rejectBackend(route, active)
		p.detach = bound
		if bound {
			return
		}
	} else {
		p.resolved = routeCondition(route, "ResolvedRefs", metav1.ConditionUnknown, "Pending", "An active backend could not be read")
	}
	p.readError = err
}

func (r *Reconciler) wonListenerPorts(ctx context.Context, gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener, route *transportRoute) ([]int, error) {
	routes, err := r.readRoutes(ctx)
	if err != nil {
		return nil, err
	}
	var ports []int
	namespaces := make(routeNamespaceCache)
	for _, listener := range listeners {
		selected, err := r.selectedRoute(ctx, gateway, listener, listenerCandidates(gateway, listener, routes), namespaces)
		if err != nil {
			return nil, err
		}
		if selected != nil && selected.GetUID() == route.GetUID() {
			ports = append(ports, int(listener.Port))
		}
	}
	slices.Sort(ports)
	return ports, nil
}

func routeStillReferences(route *transportRoute, identity cloud.Identity) bool {
	if len(route.parentRefs) != 1 {
		return false
	}
	ref := route.parentRefs[0]
	key := gatewayKey(route, ref)
	return controller.IsGatewayParentRef(ref) && key.Namespace == identity.GatewayNamespace && key.Name == identity.GatewayName
}

// gatewayTopology requires the current transport listener and checkpointed topology.
func (r *Reconciler) gatewayTopology(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (classconfig.Resolved, bool, error) {
	if listener.Protocol != r.protocol() || !controllerutil.ContainsFinalizer(gateway, r.Config.GatewayFinalizer()) ||
		gateway.Annotations[r.Config.GatewayListenerProtocolAnnotation()] != string(r.protocol()) ||
		!gateway.DeletionTimestamp.IsZero() {
		return classconfig.Resolved{}, false, nil
	}
	if err := controller.ValidateGatewayBinding(r.Config, gateway); err != nil {
		return classconfig.Resolved{}, false, err
	}
	if controller.GatewayListenerScope(r.Config, gateway) != r.protocol() &&
		(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()] != strconv.Itoa(int(listener.Port)) || len(gateway.Spec.Listeners) > 1) {
		return classconfig.Resolved{}, false, nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	return topology, err == nil, err
}

func gatewayProgrammed(gateway *gatewayv1.Gateway) bool {
	condition := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == gateway.Generation
}

func (r *Reconciler) routeSpec(ctx context.Context, route *transportRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, backends []serviceBackend, topology classconfig.Resolved, listenerPorts ...int) (cloud.RouteSpec, string, *model.Diagnostic, error) {
	return r.buildRouteSpec(ctx, route, gateway, listener, backends, 0, topology, listenerPorts...)
}

func (r *Reconciler) buildRouteSpec(ctx context.Context, route *transportRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, backends []serviceBackend, rejectWeight int, topology classconfig.Resolved, listenerPorts ...int) (cloud.RouteSpec, string, *model.Diagnostic, error) {
	weighted := len(route.backendRefs) > 1
	udpDrop := r.udpPacketDropEnabled(gateway, topology, listenerPorts)
	matchVIPFamily := weighted || r.protocol() == gatewayv1.UDPProtocolType
	groups := make([]routebackend.WeightedServiceMembers, 0, len(backends))
	serving := make([]serviceBackend, 0, len(backends))
	var noEndpoints *model.Diagnostic
	for _, backend := range backends {
		members, diagnostic, err := routebackend.TransportMembers(ctx, r.APIReader, backend.service, backend.ref.port, corev1.Protocol(r.protocol()), topology, gateway.Status.Addresses, matchVIPFamily)
		if err == nil && (r.tcpConnectionRejectionEnabled() || udpDrop) && (diagnostic != nil && diagnostic.NoReadyEndpoints ||
			diagnostic == nil && !slices.ContainsFunc(members, func(member cloud.Member) bool { return !member.Draining })) {
			rejectWeight += int(backend.ref.weight)
			if noEndpoints == nil {
				noEndpoints = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("Waiting for eligible endpoints in a positive-weight %s backend", r.protocol())}
			}
			continue
		}
		if err != nil || diagnostic != nil {
			return cloud.RouteSpec{}, "", diagnostic, err
		}
		serving = append(serving, backend)
		groups = append(groups, routebackend.WeightedServiceMembers{Weight: backend.ref.weight, Members: members})
	}
	rejection, drop, diagnostic, err := r.backendUnavailableShare(ctx, route, rejectWeight, udpDrop, noEndpoints)
	if diagnostic != nil || err != nil {
		return cloud.RouteSpec{}, "", diagnostic, err
	}

	var members []cloud.Member
	if weighted {
		var err error
		members, err = routebackend.WeightedMembers(string(r.protocol()), groups)
		if errors.Is(err, routebackend.ErrBackendWithoutReadyEndpoints) {
			return cloud.RouteSpec{}, "", &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("Waiting for eligible endpoints in every positive-weight %s backend", r.protocol())}, nil
		}
		if err != nil {
			return cloud.RouteSpec{}, "", &model.Diagnostic{Condition: "Programmed", Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: err.Error()}, nil
		}
	} else if len(groups) != 0 {
		members = groups[0].Members
	}
	spec := cloud.RouteSpec{
		Identity: controller.RouteIdentity(r.Config, gateway, route),
		GatewayRequirements: cloud.GatewayRequirements{
			Provider: "amphora", ListenerProtocol: string(r.protocol()), ListenerPort: int(listener.Port),
			VIPSubnetID: topology.Topology.VIPSubnetID, ExternalNetworkID: topology.Topology.ExternalNetworkID,
			FlavorID: topology.Topology.FlavorID, AvailabilityZone: topology.Topology.AvailabilityZone,
			VIPSecurityGroupIDs: topology.Topology.VIPSecurityGroupIDs,
		},
		MemberMode: topology.Topology.MemberMode, MemberSubnetID: topology.Topology.MemberSubnetID,
		BackendSecurityGroupIDs: topology.Topology.BackendSecurityGroupIDs,
		HealthMonitor:           topology.HealthMonitor, Members: members,
		TCPConnectionRejection: rejection, UDPPacketDrop: drop,
	}
	if drop != nil {
		// Octavia's guarded UDP pool uses EndpointSlice readiness only.
		spec.HealthMonitor = cloud.HealthMonitorConfig{}
	}
	if controller.GatewayListenerScope(r.Config, gateway) == r.protocol() {
		// The frozen port identifies the former unscoped listener. The route's
		// current listener edges are the election result, independent of it.
		spec.GatewayRequirements.ListenerPort, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		spec.ScopedListenerPorts = slices.Clone(listenerPorts)
	}
	target, err := forwardingTarget(serving, groups, weighted || len(serving) == 0, spec)
	return spec, target, nil, err
}

func (r *Reconciler) backendUnavailableShare(ctx context.Context, route *transportRoute, rejectWeight int, udpDrop bool, noEndpoints *model.Diagnostic) (*cloud.TCPConnectionRejection, *cloud.UDPPacketDrop, *model.Diagnostic, error) {
	if rejectWeight == 0 && !udpDrop {
		return nil, nil, nil, nil
	}
	var total int
	for _, ref := range route.backendRefs {
		total += int(ptr.Deref(ref.Weight, 1))
	}
	tcpShare := cloud.TCPConnectionRejection{RejectWeight: rejectWeight, TotalWeight: total}
	if !tcpShare.Valid() {
		diagnostic := &model.Diagnostic{Condition: "Programmed", Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: fmt.Sprintf("Invalid %s backend weight share", r.protocol())}
		return nil, nil, diagnostic, nil
	}
	available := r.TCPConnectionRejectionAvailable
	message := "Waiting for regional Octavia TCP connection rejection support"
	if udpDrop {
		available = r.UDPPacketDropAvailable
		message = "Waiting for regional Octavia UDP packet drop support"
	}
	if available != nil {
		ok, err := available(ctx)
		if err != nil && !udpDrop && noEndpoints == nil {
			return nil, nil, nil, err
		}
		// Keep sealing the current authorized UDP intent on an uncertain
		// discovery read. The provider decides whether to hold or quarantine.
		if ok || udpDrop && err != nil {
			if udpDrop {
				share := cloud.UDPPacketDrop{Numerator: rejectWeight, Denominator: total}
				if rejectWeight == 0 || share.DropsAll() {
					share.Denominator = 1
					share.Numerator = min(rejectWeight, 1)
				}
				return nil, &share, nil, nil
			}
			return &tcpShare, nil, nil, nil
		}
	}
	if rejectWeight == 0 {
		return nil, nil, nil, nil
	}
	diagnostic := cmp.Or(noEndpoints, &model.Diagnostic{Condition: "Programmed", Reason: "Pending", Message: message})
	return nil, nil, diagnostic, nil
}

func (r *Reconciler) ensure(ctx context.Context, route *transportRoute, plan *routePlan) (ctrl.Result, error) {
	if r.Writer == nil {
		return ctrl.Result{}, errors.New("gateway graph writer is required")
	}
	identity := plan.desired.Identity
	request := route.graphRequest(identity, graph.Ensure, plan.parents[0].gateway)
	if r.protocol() == gatewayv1.TCPProtocolType && len(plan.desired.ScopedListenerPorts) != 0 {
		request.ForwardingTarget = plan.target
	}
	result, err := r.Writer.ReconcileGraph(ctx, request)
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
				Message: fmt.Sprintf("OpenStack could not program the %s", r.kind()), ReturnError: true,
			}
		}
		plan.programmed = routeCondition(route, "Programmed", policy.ConditionStatus, policy.Reason, policy.Message)
		return controller.ProviderFailureResult(policy, err, string(route.GetUID()))
	}
	// Scoped listeners need a route-specific deferred observation. Whole-graph
	// progress alone does not show which listener is pending.
	component, err := r.routeResult(result, identity, len(plan.desired.ScopedListenerPorts) == 0)
	if err != nil {
		return ctrl.Result{}, err
	}
	if component.Outcome.State == cloud.OutcomeProgressing {
		target := r.currentTarget(route, plan.parents[0].ref, plan.target)
		if !component.ObservationDeferred || target == nil || target.Status != metav1.ConditionTrue || !r.retainProgrammedCondition(route, plan) {
			message := controller.ProviderProgressMessage(component.Outcome, fmt.Sprintf("Octavia is programming the %s forwarding configuration", r.protocol()))
			plan.programmed = routeCondition(route, "Programmed", metav1.ConditionFalse, "Pending", message)
		}
		return ctrl.Result{RequeueAfter: controller.ProviderProgressRequeueAfter(component.Outcome, route.GetUID())}, nil
	}
	plan.targetStatus = routeCondition(route, r.forwardingTargetType(), metav1.ConditionTrue, "Observed", plan.target)
	plan.programmed = routeCondition(route, "Programmed", metav1.ConditionTrue, "Programmed", fmt.Sprintf("Octavia programmed the %s forwarding configuration", r.protocol()))
	return ctrl.Result{RequeueAfter: controller.OpenStackResyncAfter(r.Config.OpenStackResyncInterval, route.GetUID())}, nil
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

func (r *Reconciler) routeResult(result cloud.GatewayGraphResult, expected cloud.Identity, allowDeferredFallback bool) (cloud.RouteResult, error) {
	var found *cloud.RouteResult
	for _, component := range result.Routes {
		if component.Identity.RouteUID != expected.RouteUID {
			continue
		}
		if component.Identity != expected || found != nil {
			return cloud.RouteResult{}, fmt.Errorf("provider returned an inconsistent %s component identity", r.kind())
		}
		found = &component.Result
	}
	if found == nil {
		if result.Outcome.State == cloud.OutcomeProgressing {
			return cloud.RouteResult{Outcome: result.Outcome, ObservationDeferred: allowDeferredFallback}, nil
		}
		return cloud.RouteResult{}, fmt.Errorf("provider omitted the %s component result", r.kind())
	}
	if err := found.Outcome.Validate(); err != nil {
		return cloud.RouteResult{}, fmt.Errorf("validate %s component outcome: %w", r.kind(), err)
	}
	if found.ObservationDeferred && found.Outcome.State != cloud.OutcomeProgressing {
		return cloud.RouteResult{}, fmt.Errorf("deferred %s observation cannot certify completion", r.kind())
	}
	return *found, nil
}
