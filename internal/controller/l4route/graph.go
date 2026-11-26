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
	"reflect"
	"slices"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

var _ graph.RouteSource = (*Reconciler)(nil)

type routeGraphSnapshot struct {
	nodes     []cloud.RouteGraphNode
	revisions []string
	elections []listenerElection
	backends  []routeBackendRevisions
	gateway   *gatewayv1.Gateway
}

type listenerElection struct {
	port       int
	candidates []routeCandidateRevision
	winner     types.UID
}

type routeBackendRevisions struct {
	class     objectRevision
	grants    []objectRevision
	services  []objectRevision
	xBackends []xBackendRevision
	imports   []serviceImportRevision
}

// routeCandidateRevision excludes status so observing an unbound route's
// conditions does not change the inputs to listener precedence.
type routeCandidateRevision struct {
	key        client.ObjectKey
	uid        types.UID
	generation int64
	created    metav1.Time
	spec       any
}

type objectRevision struct {
	uid             types.UID
	resourceVersion string
}

func revisionOf(object client.Object) objectRevision {
	return objectRevision{uid: object.GetUID(), resourceVersion: object.GetResourceVersion()}
}

// ObserveRouteGraph constructs and fences this transport route kind's
// fragment of the graph while the writer holds the Gateway UID lock.
func (r *Reconciler) ObserveRouteGraph(ctx context.Context, request graph.Request, gateway *gatewayv1.Gateway) (graph.RouteInputs, error) {
	if r.APIReader == nil {
		return graph.RouteInputs{}, controller.ErrAPIReaderRequired
	}
	var dns tcpExternalHostnameObservations
	snapshot, err := r.observeRoutes(ctx, request, gateway, &dns)
	if err != nil {
		return graph.RouteInputs{}, err
	}
	dns.sealed = true
	return graph.RouteInputs{
		Nodes: snapshot.nodes,
		Validate: func(ctx context.Context) error {
			latest, err := r.observeRoutes(ctx, request, gateway, &dns)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(snapshot, latest) {
				return controller.ErrGraphInputChanged
			}
			return nil
		},
	}, nil
}

func (r *Reconciler) observeRoutes(ctx context.Context, request graph.Request, expectedGateway *gatewayv1.Gateway, dns *tcpExternalHostnameObservations) (routeGraphSnapshot, error) {
	var snapshot routeGraphSnapshot
	routes, err := r.readRoutes(ctx)
	if err != nil {
		return snapshot, err
	}
	slices.SortFunc(routes, func(left, right *transportRoute) int {
		return cmp.Or(cmp.Compare(left.GetNamespace(), right.GetNamespace()), cmp.Compare(left.GetName(), right.GetName()))
	})

	var listeners []*gatewayv1.Listener
	var observedGateway *gatewayv1.Gateway
	winners := make(map[types.UID][]int)
	scoped := false
	var anchor int
	if request.Action == graph.Ensure {
		gateway, err := r.liveGateway(ctx, request.Identity, expectedGateway)
		if err != nil {
			return snapshot, err
		}
		// Store only safety-sensitive inputs. A status update from an unrelated
		// controller must not invalidate this route observation.
		observedGateway = gateway
		snapshot.gateway = gateway.DeepCopy()
		snapshot.gateway.ResourceVersion = ""
		snapshot.gateway.Status = gatewayv1.GatewayStatus{}
		scoped = controller.GatewayListenerScope(r.Config, gateway) == r.protocol()
		if scoped {
			if err := controller.ValidateGatewayBinding(r.Config, gateway); err != nil {
				return snapshot, err
			}
			anchor, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		}
		supported, err := gatewaycontroller.SupportedListenersForConfig(r.Config, gateway)
		if err == nil && supported[0].Protocol == r.protocol() && gateway.DeletionTimestamp.IsZero() {
			listeners = supported
			namespaces := make(routeNamespaceCache)
			for _, listener := range listeners {
				candidates := r.listenerCandidates(gateway, listener, routes)
				election := listenerElection{port: int(listener.Port)}
				for _, candidate := range candidates {
					election.candidates = append(election.candidates, routeCandidateRevision{
						key: client.ObjectKeyFromObject(candidate), uid: candidate.GetUID(), generation: candidate.GetGeneration(),
						created: candidate.GetCreationTimestamp(), spec: candidate.spec,
					})
				}
				winner, err := r.selectedRoute(ctx, gateway, listener, candidates, namespaces)
				if err != nil {
					return snapshot, err
				}
				if winner != nil {
					election.winner = winner.GetUID()
					winners[election.winner] = append(winners[election.winner], election.port)
				}
				snapshot.elections = append(snapshot.elections, election)
			}
		}
	}

	trigger := r.requestRoute(request)
	requestedIndex := -1
	type winningNode struct {
		index      int
		route      *transportRoute
		checkpoint controller.RouteGatewayTarget
	}
	var winningNodes []winningNode
	peerAttached := false
	for _, route := range routes {
		if !controller.RouteClaimsGateway(r.Config, route.Object, request.Identity) {
			continue
		}
		bindings, err := r.routeBindings(route)
		if err != nil {
			return snapshot, err
		}
		binding := boundGateway(bindings, request.Identity)
		identity := binding.Identity
		if identity.GatewayUID == "" {
			return snapshot, fmt.Errorf("%w: %s binding does not match the Gateway graph", cloud.ErrOwnershipConflict, r.kind())
		}
		if scoped && binding.Anchor != anchor {
			return snapshot, fmt.Errorf("%w: %s does not carry the enrolled Gateway's frozen anchor", cloud.ErrOwnershipConflict, r.kind())
		}
		requested := trigger != nil && route.GetUID() == trigger.GetUID()
		if requested {
			expectedBinding, err := controller.StoredRouteGatewayBinding(r.Config, trigger.Object, request.Identity)
			if err != nil {
				return snapshot, err
			}
			if request.Identity != identity || !sameRouteRevision(route, trigger) || route.GetResourceVersion() != trigger.GetResourceVersion() ||
				binding != expectedBinding {
				return snapshot, controller.ErrGraphInputChanged
			}
			requestedIndex = len(snapshot.nodes)
		}
		node := cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{
			Identity: identity, GatewayRequirements: cloud.GatewayRequirements{ListenerProtocol: string(r.protocol())},
		}}
		switch {
		case binding.Lifecycle == controller.RouteDetaching:
			if request.Action == graph.Ensure || request.Action == graph.DeleteRoute && requested {
				node.Disposition = cloud.GraphDelete
			}
		case len(winners[route.GetUID()]) != 0 && route.GetDeletionTimestamp().IsZero():
			winningNodes = append(winningNodes, winningNode{index: len(snapshot.nodes), route: route, checkpoint: binding.Forwarding})
		default:
			peerAttached = true
			if request.Action == graph.Ensure && !dns.sealed && r.tcpXBackendsEnabled() {
				// A Gateway reconciliation can observe loss of attachment before
				// the route checkpoints its detach.
				r.InvalidateExternalHostnames(identity)
			}
		}
		if scoped && node.Disposition == cloud.GraphPreserve {
			// A bound route with no fresh authorized target cannot keep a
			// default on any port. Scope withdrawal to its proven edges.
			node.RetainListenerPorts = []int{}
		}
		snapshot.nodes = append(snapshot.nodes, node)
		snapshot.revisions = append(snapshot.revisions, route.GetResourceVersion())
	}
	// The singleton path keeps its conservative handoff. Listener scope lets a
	// provider preserve a peer's occupied edges while converging other ports.
	if peerAttached && !scoped {
		winningNodes = nil
	}
	for _, winner := range winningNodes {
		ports := winners[winner.route.GetUID()]
		spec, target, backends, err := r.observeRouteBackend(ctx, winner.route, observedGateway, listeners[0], ports, dns)
		if err != nil {
			return snapshot, err
		}
		snapshot.backends = append(snapshot.backends, backends)
		if spec == nil {
			continue
		}
		node := &snapshot.nodes[winner.index]
		checkpoint := winner.checkpoint
		currentTarget := checkpoint.Generation == winner.route.GetGeneration() && matchesBackendTarget(checkpoint.Value, target)
		if checkpoint.Value == "" {
			currentTarget = slices.ContainsFunc(r.gatewayRefs(winner.route, observedGateway), func(ref gatewayv1.ParentReference) bool {
				return r.hasCurrentBackendTarget(winner.route, ref, target) && slices.ContainsFunc(listeners, func(listener *gatewayv1.Listener) bool {
					return slices.Contains(ports, int(listener.Port)) && parentSelects(ref, listener)
				})
			})
		}
		if currentTarget {
			if winner.index == requestedIndex && request.ForwardingTarget != "" && request.ForwardingTarget != target {
				return snapshot, controller.ErrGraphInputChanged
			}
			node.Disposition = cloud.GraphEnsure
			node.Spec = *spec
			node.RetainListenerPorts = nil
		} else if len(spec.TCPWeightedBackends) != 0 || r.canRetainObservedTarget(winner.route, target, checkpoint) {
			// For weighted routes the new snapshot can withdraw individual shares
			// before the forwarding checkpoint permits admission. The provider
			// confirms each retained pool against the fresh backend snapshot.
			node.Spec = *spec
			node.RetainListenerPorts = ports
		} else if scoped && spec.UDPPacketDrop != nil {
			// The grant or endpoint set may change before this route gets
			// the writer lock. Use the live target to stop revoked traffic
			// while the route's checkpoint still controls new forwarding.
			node.Spec = *spec
			node.RetainListenerPorts = ports
			node.RestrictUDPPacketDrop = true
		}
	}
	if trigger != nil {
		if requestedIndex < 0 ||
			request.Action == graph.Ensure && snapshot.nodes[requestedIndex].Disposition != cloud.GraphEnsure ||
			request.Action == graph.DeleteRoute && snapshot.nodes[requestedIndex].Disposition != cloud.GraphDelete {
			return snapshot, controller.ErrGraphInputChanged
		}
	}
	return snapshot, nil
}

func (r *Reconciler) liveGateway(ctx context.Context, identity cloud.Identity, expected *gatewayv1.Gateway) (*gatewayv1.Gateway, error) {
	if expected == nil || identity.GatewayUID != string(expected.UID) ||
		identity.GatewayNamespace != expected.Namespace || identity.GatewayName != expected.Name {
		return nil, controller.ErrGraphInputChanged
	}
	gateway := &gatewayv1.Gateway{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), gateway); err != nil {
		return nil, errors.Join(controller.ErrGraphInputChanged, client.IgnoreNotFound(err))
	}
	if !sameGatewayRevision(r.Config, expected, gateway) {
		return nil, controller.ErrGraphInputChanged
	}
	return gateway, nil
}

func (r *Reconciler) observeRouteBackend(ctx context.Context, route *transportRoute, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, ports []int, dns *tcpExternalHostnameObservations) (*cloud.RouteSpec, string, routeBackendRevisions, error) {
	var revisions routeBackendRevisions
	class, err := r.gatewayClass(ctx, gateway)
	if err != nil || class == nil {
		return nil, "", revisions, err
	}
	revisions.class = revisionOf(class)
	if !controller.GatewayClassSupportsInstalledVersion(class) {
		return nil, "", revisions, nil
	}
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			err = nil
		}
		return nil, "", revisions, err
	}
	topology, valid, err := r.gatewayTopology(gateway, listener)
	if err != nil || !valid {
		return nil, "", revisions, err
	}
	// Zero-weight references affect status only. Their authorization and
	// revisions cannot change the serving destinations of the sealed graph.
	collect := r.tcpWeightedCandidate(route) || r.tcpConnectionRejectionEnabled() || r.udpPacketDropEnabled(gateway, topology, ports)
	refs, auth, err := r.authorizedBackends(ctx, route, false, collect)
	revisions.grants = auth.grants
	if err != nil || auth.active != nil && auth.rejectWeight == 0 || auth.active != nil && !collect {
		return nil, "", revisions, err
	}
	var mode cloud.MemberMode
	if collect {
		mode = topology.Topology.MemberMode
	}
	backends, invalid, err := r.resolveBackends(ctx, refs, "", mode, r.tcpXBackendProfile(gateway, topology.Topology.MemberMode), gateway.Spec.GatewayClassName, r.tcpXBackendTLSEligible(route), collect)
	revisions.services = invalid.services
	revisions.xBackends = invalid.xBackends
	revisions.imports = invalid.imports
	if err != nil || invalid.active != nil && !collect {
		return nil, "", revisions, err
	}
	rejectWeight := auth.rejectWeight + invalid.rejectWeight
	if readErr := errors.Join(auth.readError, invalid.readError); readErr != nil {
		if rejectWeight > 0 {
			return nil, "", revisions, nil
		}
		return nil, "", revisions, readErr
	}
	if len(backends) == 0 && rejectWeight == 0 {
		return nil, "", revisions, nil
	}
	spec, target, diagnostic, err := r.buildRouteSpec(ctx, route, gateway, listener, backends, rejectWeight, topology, dns, ports...)
	if err != nil && rejectWeight > 0 {
		return nil, "", revisions, nil
	}
	if err != nil || diagnostic != nil {
		return nil, "", revisions, err
	}
	return &spec, target, revisions, nil
}
