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
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

// ObserveGatewayGraph validates the live Gateway component under the writer lock.
func (r *Reconciler) ObserveGatewayGraph(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	if r.APIReader == nil {
		return graph.GatewayInput{}, controller.ErrAPIReaderRequired
	}
	if request.Action == graph.DeleteRoute {
		return r.observeGatewayForRouteDeletion(ctx, request)
	}
	if request.Gateway == nil {
		return graph.GatewayInput{}, errors.New("expected Gateway revision is required")
	}
	if request.Action == graph.WithdrawTLS {
		return r.observeGatewayTLSWithdrawal(ctx, request)
	}
	if request.Action == graph.DeleteGateway {
		snapshot, err := r.observeGatewayCleanupSnapshot(ctx, request.Gateway)
		if err != nil {
			return graph.GatewayInput{}, err
		}
		if snapshot.disposition == gatewayCleanupHold || snapshot.identity != request.Identity {
			return graph.GatewayInput{}, errGatewayChanged
		}
		port, _ := strconv.Atoi(snapshot.boundListenerPort)
		spec := cloud.GatewaySpec{ListenerProtocol: string(snapshot.boundListenerProtocol), ListenerPort: port}
		// A listener replacement must not release an existing VIP when the
		// same request changes the fixed address. Deletion has no such guard.
		if snapshot.listenerPort != "" {
			setGatewayAddressRequest(&spec, request.Gateway)
			if spec.RequestedVIPAddress != "" || spec.AddressRejection != "" {
				topology, err := controller.StoredGatewayTopology(r.Config, request.Gateway)
				if err != nil {
					return graph.GatewayInput{}, err
				}
				spec.ExternalNetworkID = topology.Topology.ExternalNetworkID
			}
		}
		if snapshot.scopedListeners {
			spec.ScopedListeners = []cloud.ScopedListener{}
		}
		input := graph.GatewayInput{
			Object: request.Gateway.DeepCopy(),
			Node: cloud.GatewayGraphNode{
				Disposition:    cloud.GraphDelete,
				Spec:           spec,
				BackendCAOwned: controllerutil.ContainsFinalizer(request.Gateway, r.Config.BackendCAFinalizer()),
			},
			Validate: func(ctx context.Context) error {
				latest, err := r.observeGatewayCleanupSnapshot(ctx, request.Gateway)
				if err != nil {
					return err
				}
				if *latest != *snapshot {
					return errGatewayChanged
				}
				return nil
			},
		}
		if snapshot.boundListenerProtocol == gatewayv1.TLSProtocolType || snapshot.boundListenerProtocol == cloud.ListenerProtocolTLSTerminate {
			input.ReportRoutes = func(ctx context.Context, result cloud.GatewayGraphResult, providerErr error) error {
				if providerErr != nil || result.Outcome.State != cloud.OutcomeReady || result.Gateway.AddressIssue != "" {
					return nil
				}
				if err := input.Validate(ctx); err != nil {
					return err
				}
				return r.clearSNIRouteParentStatuses(ctx, request.Gateway)
			}
		}
		return input, nil
	}
	if request.Action != graph.Ensure {
		return graph.GatewayInput{}, errors.New("invalid Gateway graph action")
	}
	snapshot, err := r.observeGatewayMutationSnapshot(ctx, request.Gateway, gatewayBindingRequired, "")
	binding := request.Gateway
	if snapshot != nil {
		binding = snapshot.gateway
	}
	var input graph.GatewayInput
	if controller.GatewayListenerScope(r.Config, binding) == gatewayv1.HTTPProtocolType {
		input.ObserveTLS = func(ctx context.Context) (graph.GatewayInput, error) {
			return r.tlsWithdrawalInput(ctx, binding, request.Identity)
		}
	} else if r.boundListenerProtocol(binding) == cloud.ListenerProtocolTLSTerminate {
		input.ObserveTLS = func(ctx context.Context) (graph.GatewayInput, error) {
			return r.rawTLSWithdrawalInput(ctx, binding, request.Identity)
		}
	}
	if err != nil {
		return input, err
	}
	input.Object = snapshot.gateway
	input.Node = cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: r.gatewaySpec(snapshot.gateway, snapshot.listeners, snapshot.topology)}
	sets, err := ObserveListenerSets(ctx, r.APIReader, r.Config, snapshot.gateway)
	if err != nil {
		return input, err
	}
	for _, set := range sets.sets {
		for _, entry := range set.entries {
			if entry.rejection != "" {
				continue
			}
			port := int(entry.listener.Port)
			if !slices.ContainsFunc(input.Node.Spec.ScopedListeners, func(listener cloud.ScopedListener) bool { return listener.Port == port }) {
				input.Node.Spec.ScopedListeners = append(input.Node.Spec.ScopedListeners, cloud.ScopedListener{
					Port: port, Name: resourceDisplayName(strconv.Itoa(port), snapshot.gateway.Namespace, snapshot.gateway.Name, string(entry.listener.Name)),
				})
			}
		}
	}
	input.Node.BackendCAOwned = controllerutil.ContainsFinalizer(snapshot.gateway, r.Config.BackendCAFinalizer())
	if r.Config.BackendTLSPolicy && !input.Node.BackendCAOwned {
		input.Node.ClaimBackendCA = func(ctx context.Context) error {
			desired := snapshot.gateway.DeepCopy()
			controllerutil.AddFinalizer(desired, r.Config.BackendCAFinalizer())
			return r.Patch(ctx, desired, controller.OptimisticMergeFrom(snapshot.gateway))
		}
	}
	input.Validate = func(ctx context.Context) error {
		if err := r.sealGatewayMutationSnapshot(ctx, snapshot); err != nil {
			return err
		}
		current, err := ObserveListenerSets(ctx, r.APIReader, r.Config, snapshot.gateway)
		if err != nil {
			return err
		}
		if !sets.sameInputs(current) {
			return errGatewayChanged
		}
		return nil
	}
	if snapshot.listener.Protocol == gatewayv1.TLSProtocolType {
		if terminatesTLS(snapshot.listener) {
			input.Node.TCPTLSCertificateRef = r.resolveCertificate(ctx, snapshot.gateway, snapshot.listener).tls.CertificateRef
		}
		observed, err := r.observeSNIRoutes(ctx, snapshot)
		if err != nil {
			return input, err
		}
		input.Node.TLSSNIRoutes = observed.targets()
		validate := func(ctx context.Context) error {
			latest, err := r.observeSNIRoutes(ctx, snapshot)
			if err != nil {
				return err
			}
			if !observed.sameInputs(latest) {
				return errGatewayChanged
			}
			return r.sealGatewayMutationSnapshot(ctx, snapshot)
		}
		input.Validate = validate
		input.ReportRoutes = func(ctx context.Context, result cloud.GatewayGraphResult, providerErr error) error {
			if err := validate(ctx); err != nil {
				return err
			}
			if terminatesTLS(snapshot.listener) {
				current, err := input.ObserveTLS(ctx)
				if err != nil {
					return err
				}
				if current.Node.TCPTLSCertificateRef == "" || current.Node.TCPTLSCertificateRef != input.Node.TCPTLSCertificateRef {
					result.Routes = nil
				}
			}
			return r.reportSNIRoutes(ctx, snapshot, observed, result, providerErr)
		}
	}
	if usesHTTPSProfile(r.Config, snapshot.gateway) {
		if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
			return input, err
		}
		r.addTLSGraphInput(ctx, snapshot, &input)
	}
	return input, nil
}

func (r *Reconciler) observeGatewayForRouteDeletion(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	input := graph.GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve}}
	var route client.Object
	var routeProtocol gatewayv1.ProtocolType
	switch {
	case request.Route != nil:
		route, routeProtocol = request.Route, gatewayv1.HTTPProtocolType
	case request.TCPRoute != nil:
		route, routeProtocol = request.TCPRoute, gatewayv1.TCPProtocolType
	case request.UDPRoute != nil:
		route, routeProtocol = request.UDPRoute, gatewayv1.UDPProtocolType
	}
	var routeAnchor int
	if route != nil {
		routeAnchor = controller.ScopedRouteAnchor(r.Config, route, routeProtocol)
	}
	if route != nil && (routeProtocol != gatewayv1.HTTPProtocolType || routeAnchor != 0) {
		identity, present, err := controller.StoredRouteIdentity(r.Config, route)
		if err != nil {
			return graph.GatewayInput{}, err
		}
		if !present || identity != request.Identity ||
			!controllerutil.ContainsFinalizer(route, r.Config.RouteFinalizer()) ||
			route.GetAnnotations()[r.Config.RouteClusterIDAnnotation()] != identity.ClusterID ||
			route.GetAnnotations()[r.Config.RouteProjectIDAnnotation()] != identity.OpenStackProjectID ||
			route.GetAnnotations()[r.Config.RouteLifecycleAnnotation()] != controller.RouteDetaching {
			return graph.GatewayInput{}, errGatewayChanged
		}
		if routeAnchor != 0 {
			input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: string(routeProtocol), ListenerPort: routeAnchor, ScopedListeners: []cloud.ScopedListener{}}
		}
	}

	var current gatewayv1.Gateway
	key := client.ObjectKey{Namespace: request.Identity.GatewayNamespace, Name: request.Identity.GatewayName}
	if err := r.APIReader.Get(ctx, key, &current); err != nil {
		return input, client.IgnoreNotFound(err)
	}
	if string(current.UID) != request.Identity.GatewayUID {
		return input, nil
	}
	input.Object = &current
	boundProtocol := r.boundListenerProtocol(&current)
	scopedBinding := controller.HasGatewayListenerScopeBinding(r.Config, &current)
	if !scopedBinding && boundProtocol != gatewayv1.UDPProtocolType && routeAnchor == 0 && !r.requestsListenerScope(&current) {
		return input, nil
	}
	input.Validate = func(ctx context.Context) error {
		var latest gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, key, &latest); err != nil {
			return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if latest.UID != current.UID || !controller.GatewayBindingMetadataEqual(r.Config, &current, &latest) {
			return errGatewayChanged
		}
		return nil
	}
	if !controller.GatewayHasControllerBinding(r.Config, &current) {
		// A fully cleared Gateway binding can outlive its old routes after
		// teardown. Their sealed anchors still authorize route-owned cleanup.
		return input, nil
	}
	if !scopedBinding && routeAnchor == 0 && route != nil && !r.requestsListenerScope(&current) {
		// The legacy singleton cleanup is authorized by the route's durable
		// ownership alone. It does not provision from the Gateway topology.
		return input, nil
	}
	if err := controller.ValidateGatewayBinding(r.Config, &current); err != nil {
		// Scoped ownership comes only from a verified Gateway or route
		// checkpoint. Never reinterpret a damaged current binding as legacy.
		return graph.GatewayInput{}, err
	}
	owner := request.Identity
	owner.RouteNamespace, owner.RouteName, owner.RouteUID = "", "", ""
	port, err := strconv.Atoi(current.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if err != nil || port < 1 || port > 65535 || !controllerutil.ContainsFinalizer(&current, r.Config.GatewayFinalizer()) ||
		controller.StoredGatewayIdentity(r.Config, &current) != owner {
		if scopedBinding || routeAnchor != 0 {
			return graph.GatewayInput{}, fmt.Errorf("%w: Gateway cleanup binding is incomplete or belongs to a different owner", cloud.ErrOwnershipConflict)
		}
		return input, nil
	}
	scope := controller.GatewayListenerScope(r.Config, &current)
	switch {
	case scope != "":
		if routeProtocol == scope && routeAnchor != port {
			return graph.GatewayInput{}, fmt.Errorf("%w: %s route cleanup lacks the enrolled Gateway's frozen anchor", cloud.ErrOwnershipConflict, scope)
		}
		input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: string(scope), ListenerPort: port, ScopedListeners: []cloud.ScopedListener{}}
	case routeAnchor != 0 && boundProtocol != routeProtocol:
		// Changing a bound protocol required verified teardown of the old graph.
		input.Node.Spec = cloud.GatewaySpec{}
	case routeAnchor != 0 && routeAnchor != port:
		return graph.GatewayInput{}, fmt.Errorf("%w: %s route cleanup disagrees with the Gateway's frozen anchor", cloud.ErrOwnershipConflict, routeProtocol)
	case routeAnchor != port:
		input.Node.Spec = cloud.GatewaySpec{ListenerProtocol: string(boundProtocol), ListenerPort: port}
	}
	return input, nil
}
