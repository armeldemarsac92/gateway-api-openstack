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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

const tlsRouteKind gatewayv1.Kind = "TLSRoute"

func hasTLSListener(gateway *gatewayv1.Gateway) bool {
	return slices.ContainsFunc(gateway.Spec.Listeners, func(l gatewayv1.Listener) bool {
		return l.Protocol == gatewayv1.TLSProtocolType
	})
}

func terminatesTLS(listener *gatewayv1.Listener) bool {
	return listener.Protocol == gatewayv1.TLSProtocolType && listener.TLS != nil &&
		(listener.TLS.Mode == nil || *listener.TLS.Mode == gatewayv1.TLSModeTerminate)
}

func listenerBindingProtocol(config controller.Config, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) gatewayv1.ProtocolType {
	if listener.Protocol == gatewayv1.TLSProtocolType && mixedTLSBinding(config, gateway, listener.Port) {
		return cloud.ListenerProtocolTLSMixed
	}
	if terminatesTLS(listener) {
		return cloud.ListenerProtocolTLSTerminate
	}
	return listenerFamily(listener)
}

func tlsListenerModeEnabled(config controller.Config, listener *gatewayv1.Listener) bool {
	if terminatesTLS(listener) {
		return config.TCPTLSTermination
	}
	return config.TLSSNIPassthrough && listener.TLS != nil &&
		listener.TLS.Mode != nil && *listener.TLS.Mode == gatewayv1.TLSModePassthrough
}

func allowsSNISuffix(config controller.Config, listener *gatewayv1.Listener) bool {
	return terminatesTLS(listener) || config.TLSSNISuffix
}

func mixedTLSBinding(config controller.Config, gateway *gatewayv1.Gateway, port gatewayv1.PortNumber) bool {
	if !config.TCPTLSMixedMode {
		return false
	}
	var terminate, passthrough bool
	for _, listener := range gateway.Spec.Listeners {
		if listener.Port != port || listener.Protocol != gatewayv1.TLSProtocolType {
			continue
		}
		if terminatesTLS(&listener) {
			terminate = true
		} else {
			passthrough = true
		}
	}
	boundMixed := gateway.Annotations[config.GatewayListenerProtocolAnnotation()] == cloud.ListenerProtocolTLSMixed &&
		gateway.Annotations[config.GatewayListenerPortAnnotation()] == strconv.Itoa(int(port))
	// A single remaining section can use its own profile. Several
	// terminating sections can still share their existing mixed frontend.
	return terminate && (passthrough || boundMixed && len(gateway.Spec.Listeners) > 1)
}

func mixedTLSModes(listeners []gatewayv1.Listener, listener gatewayv1.Listener) bool {
	return listener.Protocol == gatewayv1.TLSProtocolType && slices.ContainsFunc(listeners, func(peer gatewayv1.Listener) bool {
		return peer.Protocol == gatewayv1.TLSProtocolType && peer.Port == listener.Port && terminatesTLS(&peer) != terminatesTLS(&listener)
	})
}

func validateSNITopology(listener *gatewayv1.Listener, topology classconfig.Resolved) error {
	if listener.Protocol == gatewayv1.TLSProtocolType && topology.Topology.MemberMode != cloud.MemberModeEndpoint {
		return &classconfig.InvalidParametersError{Message: "TLSRoute requires Endpoint member mode"}
	}
	return nil
}

func (r *Reconciler) validateBoundSNITopology(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) error {
	if listener.Protocol != gatewayv1.TLSProtocolType || !controller.HasGatewayTopologyBinding(r.Config, gateway) {
		return nil
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return err
	}
	return validateSNITopology(listener, topology)
}

type sniRouteStatus struct {
	route           *gatewayv1.TLSRoute
	listener        *gatewayv1.Listener
	parents         []gatewayv1.ParentReference
	hostnames       []string
	acceptedReason  gatewayv1.RouteConditionReason
	acceptedMessage string
	resolvedReason  gatewayv1.RouteConditionReason
	resolvedMessage string
	targets         []cloud.TLSSNIRoute
	denied          []cloud.TLSSNIRoute
}

type sniRouteObservation []sniRouteStatus

func (observation sniRouteObservation) targets() []cloud.TLSSNIRoute {
	var targets []cloud.TLSSNIRoute
	for _, entry := range observation {
		targets = append(targets, entry.targets...)
		targets = append(targets, entry.denied...)
	}
	return targets
}

// sameInputs ignores status-only writes. Targets include effective hostnames,
// authorized Service UIDs, ready members and claims requiring a denial.
func (observation sniRouteObservation) sameInputs(other sniRouteObservation) bool {
	return slices.EqualFunc(observation, other, func(a, b sniRouteStatus) bool {
		if a.route.UID != b.route.UID || a.route.Generation != b.route.Generation ||
			!a.route.CreationTimestamp.Equal(&b.route.CreationTimestamp) ||
			!equality.Semantic.DeepEqual(a.route.Spec, b.route.Spec) {
			return false
		}
		a.route, b.route = nil, nil
		return reflect.DeepEqual(a, b)
	})
}

func (r *Reconciler) readLiveSNIRoutes(ctx context.Context) ([]gatewayv1.TLSRoute, error) {
	var routes gatewayv1.TLSRouteList
	if err := r.APIReader.List(ctx, &routes); err != nil {
		return nil, err
	}
	if routes.Continue != "" {
		return nil, fmt.Errorf("incomplete TLSRoute inventory")
	}
	slices.SortFunc(routes.Items, func(a, b gatewayv1.TLSRoute) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return routes.Items, nil
}

func (r *Reconciler) observeSNIRoutes(ctx context.Context, snapshot *gatewayMutationSnapshot) (sniRouteObservation, error) {
	routes, err := r.readLiveSNIRoutes(ctx)
	if err != nil {
		return nil, err
	}
	observation, err := r.admitSNIRoutes(ctx, snapshot, routes)
	if err != nil {
		return nil, err
	}
	mixed := snapshot.listener != nil && mixedTLSBinding(r.Config, snapshot.gateway, snapshot.listener.Port)
	var tls *graph.BackendTLSObserver
	if snapshot.listener != nil {
		tls = graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	}
	owners := observation.hostnameOwners()
	for i := range observation {
		entry := &observation[i]
		if entry.acceptedReason != gatewayv1.RouteReasonAccepted {
			continue
		}
		entry.hostnames = slices.DeleteFunc(entry.hostnames, func(hostname string) bool { return owners[hostname].index != i })
		if err := r.resolveSNIBackend(ctx, snapshot, entry, tls, mixed); err != nil {
			return nil, err
		}
		if (mixed || allowsSNISuffix(r.Config, entry.listener)) && len(entry.targets) == 0 {
			for _, hostname := range entry.hostnames {
				needsDenial := mixed || terminatesTLS(entry.listener)
				for broader := range owners {
					if needsDenial || amphora.SNISuffixCovers(broader, hostname) {
						needsDenial = true
						break
					}
				}
				if needsDenial {
					entry.denied = append(entry.denied, cloud.TLSSNIRoute{
						Identity: controller.RouteIdentity(r.Config, snapshot.gateway, entry.route),
						Hostname: hostname, Deny: true, Passthrough: mixed && !terminatesTLS(entry.listener),
					})
				}
			}
		}
	}
	return observation, nil
}

// Reserve the Hostname of each logical listener even when its selected
// routes only cover a subset, or when the listener has no matching routes.
func appendMixedSNIListenerGuards(identity cloud.Identity, listeners []*gatewayv1.Listener, targets []cloud.TLSSNIRoute) []cloud.TLSSNIRoute {
	for _, listener := range listeners {
		hostname := string(*listener.Hostname)
		if slices.ContainsFunc(targets, func(target cloud.TLSSNIRoute) bool { return target.Hostname == hostname }) {
			continue
		}
		guard := identity
		guard.RouteNamespace = identity.GatewayNamespace
		guard.RouteName = string(listener.Name)
		guard.RouteUID = identity.GatewayUID + "/listener/" + string(listener.Name)
		targets = append(targets, cloud.TLSSNIRoute{Identity: guard, Hostname: hostname, Deny: true, Passthrough: !terminatesTLS(listener)})
	}
	return targets
}

func (r *Reconciler) admitSNIRoutes(ctx context.Context, snapshot *gatewayMutationSnapshot, routes []gatewayv1.TLSRoute) (sniRouteObservation, error) {
	if len(snapshot.listeners) < 2 {
		return r.admitSNIRoutesForListener(ctx, snapshot, routes)
	}
	var admitted sniRouteObservation
	for _, listener := range snapshot.listeners {
		section := *snapshot
		section.listener = listener
		observation, err := r.admitSNIRoutesForListener(ctx, &section, routes)
		if err != nil {
			return nil, err
		}
		for i := range observation {
			entry := &observation[i]
			entry.hostnames = slices.DeleteFunc(entry.hostnames, func(hostname string) bool {
				return slices.ContainsFunc(snapshot.listeners, func(peer *gatewayv1.Listener) bool {
					selector := string(*peer.Hostname)
					return model.CompareHostnameSpecificity(selector, string(*listener.Hostname)) < 0 &&
						(selector == hostname || amphora.SNISuffixCovers(selector, hostname))
				})
			})
		}
		admitted = append(admitted, observation...)
	}
	return admitted, nil
}

func (r *Reconciler) admitSNIRoutesForListener(ctx context.Context, snapshot *gatewayMutationSnapshot, routes []gatewayv1.TLSRoute) (sniRouteObservation, error) {
	var observation sniRouteObservation
	for i := range routes {
		route := &routes[i]
		if !route.DeletionTimestamp.IsZero() {
			continue
		}
		parents := slices.DeleteFunc(slices.Clone(route.Spec.ParentRefs), func(ref gatewayv1.ParentReference) bool {
			return !sniParentTargetsGateway(route.Namespace, ref, snapshot.gateway)
		})
		ownsStatus := slices.ContainsFunc(route.Status.Parents, func(status gatewayv1.RouteParentStatus) bool {
			return r.ownsSNIParent(route, snapshot.gateway, status)
		})
		if len(parents) == 0 && !ownsStatus {
			continue
		}
		entry := sniRouteStatus{route: route, listener: snapshot.listener, parents: parents}
		if len(parents) > 0 {
			if err := r.admitSNIRoute(ctx, snapshot, &entry); err != nil {
				return nil, err
			}
		}
		observation = append(observation, entry)
	}
	return observation, nil
}

type sniHostnameOwner struct {
	index    int
	original string
}

// hostnameOwners selects the winner for each SNI name before resolving any
// backend. Losing authorization must withdraw the winner's forwarding target.
func (observation sniRouteObservation) hostnameOwners() map[string]sniHostnameOwner {
	owners := make(map[string]sniHostnameOwner)
	for i, entry := range observation {
		for _, hostname := range entry.hostnames {
			original := model.MatchingRouteHostname(hostname, entry.route.Spec.Hostnames)
			if owner, ok := owners[hostname]; ok {
				old := observation[owner.index].route
				route := entry.route
				if cmp.Or(model.CompareHostnameSpecificity(original, owner.original),
					route.CreationTimestamp.Compare(old.CreationTimestamp.Time),
					cmp.Compare(route.Namespace+"/"+route.Name, old.Namespace+"/"+old.Name)) >= 0 {
					continue
				}
			}
			owners[hostname] = sniHostnameOwner{index: i, original: original}
		}
	}
	return owners
}

func sniParentTargetsGateway(namespace string, ref gatewayv1.ParentReference, gateway *gatewayv1.Gateway) bool {
	return controller.IsGatewayParentRef(ref) && string(ref.Name) == gateway.Name &&
		string(ptr.Deref(ref.Namespace, gatewayv1.Namespace(namespace))) == gateway.Namespace
}

func (entry *sniRouteStatus) reject(reason gatewayv1.RouteConditionReason, message string) {
	entry.acceptedReason, entry.acceptedMessage = reason, message
}

func (r *Reconciler) admitSNIRoute(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus) error {
	listener, route := snapshot.listener, entry.route
	if listener == nil {
		entry.reject(gatewayv1.RouteReasonUnsupportedValue, "The Gateway has no supported TLS SNI listener")
		return nil
	}
	if !slices.ContainsFunc(entry.parents, func(ref gatewayv1.ParentReference) bool { return sniParentSelectsListener(ref, *listener) }) {
		entry.reject(gatewayv1.RouteReasonNoMatchingParent, sniParentNoMatchingListenerMessage)
		return nil
	}
	if len(route.Spec.ParentRefs) != 1 {
		entry.reject(gatewayv1.RouteReasonUnsupportedValue, "TLSRoute requires one Gateway parentRef")
		return nil
	}
	var namespaces []corev1.Namespace
	if model.ObservesRouteNamespace(listener) {
		namespace, err := graph.ReadRouteNamespace(ctx, r.APIReader, route.Namespace)
		if err != nil {
			return err
		}
		if namespace != nil && namespace.DeletionTimestamp.IsZero() {
			namespaces = append(namespaces, *namespace)
		}
	}
	if !listenerAllowsRouteKind(*listener, tlsRouteKind) || !model.AllowsRouteNamespace(listener, snapshot.gateway.Namespace, route.Namespace, namespaces) {
		entry.reject(gatewayv1.RouteReasonNotAllowedByListeners, "The listener does not allow this TLSRoute kind or namespace")
		return nil
	}
	hostnames, err := intersectSNIHostnames(listener, route, allowsSNISuffix(r.Config, listener))
	if err != nil {
		entry.reject(gatewayv1.RouteReasonUnsupportedValue, err.Error())
		return nil
	}
	if len(hostnames) == 0 {
		entry.reject(gatewayv1.RouteReasonNoMatchingListenerHostname, "No TLSRoute hostname intersects the listener hostname")
		return nil
	}
	if len(route.Spec.Rules) != 1 || len(route.Spec.Rules[0].BackendRefs) != 1 ||
		ptr.Deref(route.Spec.Rules[0].BackendRefs[0].Weight, 1) != 1 {
		entry.reject(gatewayv1.RouteReasonUnsupportedValue, "TLSRoute requires one rule and one backendRef of weight 1")
		return nil
	}
	entry.hostnames = hostnames
	entry.reject(gatewayv1.RouteReasonAccepted, "The TLSRoute is attached to the TLS SNI listener")
	return nil
}

func intersectSNIHostnames(listener *gatewayv1.Listener, route *gatewayv1.TLSRoute, allowSuffix bool) ([]string, error) {
	hostnames := model.IntersectHostnames(listener.Hostname, route.Spec.Hostnames)
	for _, hostname := range hostnames {
		if allowSuffix {
			if err := amphora.ValidateSNIHostname(hostname); err != nil {
				return nil, fmt.Errorf("cannot forward the intersecting hostname %q through a DNS SNI rule", hostname)
			}
		} else if err := amphora.ValidateExactHostname(hostname); err != nil {
			return nil, fmt.Errorf("cannot forward the intersecting hostname %q through an exact SNI rule", hostname)
		}
	}
	return hostnames, nil
}

func (r *Reconciler) observeSNIService(ctx context.Context, entry *sniRouteStatus) (*corev1.Service, *corev1.ServicePort, error) {
	route := entry.route
	ref := route.Spec.Rules[0].BackendRefs[0]
	key, supported := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
	if !supported {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonInvalidKind, "The backendRef must refer to a core Service"
		return nil, nil, nil
	}
	if key.Namespace != route.Namespace {
		grants, err := graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
		if err != nil {
			return nil, nil, err
		}
		grants = slices.DeleteFunc(grants, func(grant gatewayv1.ReferenceGrant) bool { return !grant.DeletionTimestamp.IsZero() })
		if !model.BackendReferenceAllowedFor(tlsRouteKind, route.Namespace, key, grants) {
			entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonRefNotPermitted, "No ReferenceGrant authorizes this TLSRoute to use the Service"
			return nil, nil, nil
		}
	}
	var service corev1.Service
	if err := r.APIReader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonBackendNotFound, "The backend Service was not found"
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if !service.DeletionTimestamp.IsZero() || service.UID == "" {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonBackendNotFound, "The backend Service has no live identity"
		return nil, nil, nil
	}
	port, diagnostic := model.ResolveTransportServicePort(&service, ref.Port, corev1.ProtocolTCP)
	if diagnostic == nil {
		diagnostic = routebackend.ValidateEndpointService(&service)
	}
	if diagnostic != nil {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message
		return nil, nil, nil
	}
	return &service, port, nil
}

func (r *Reconciler) resolveSNIBackend(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus, tls *graph.BackendTLSObserver, mixed bool) error {
	route := entry.route
	service, port, err := r.observeSNIService(ctx, entry)
	if err != nil {
		if tls == nil && !terminatesTLS(entry.listener) {
			return err
		}
		return sniBackendReadPending(ctx, entry, "Waiting for a live Service and ReferenceGrant inventory")
	}
	if service == nil {
		return nil
	}
	var validation *cloud.BackendTLSValidation
	if tls != nil && len(entry.hostnames) != 0 && (!terminatesTLS(entry.listener) || !r.Config.BackendTLSPolicy) {
		applies, err := tls.HasPolicy(ctx, service, port)
		if err != nil {
			return sniBackendReadPending(ctx, entry, "Waiting for a live BackendTLSPolicy inventory")
		}
		if applies {
			entry.resolvedReason = gatewayv1.RouteReasonUnsupportedProtocol
			entry.resolvedMessage = "The passthrough listener cannot authenticate the Service using BackendTLSPolicy"
			if terminatesTLS(entry.listener) {
				entry.resolvedMessage = "TCP backend authentication requires the BackendTLSPolicy controller option"
			}
			return nil
		}
	} else if tls != nil && len(entry.hostnames) != 0 {
		ancestor := &backendtls.GatewayAncestor{
			Key: client.ObjectKeyFromObject(snapshot.gateway), ControllerName: r.Config.ControllerName,
		}
		selection, err := tls.Observe(ctx, service, port, ancestor)
		if errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable) {
			return err
		}
		if err != nil {
			return sniBackendReadPending(ctx, entry, "Waiting for a live BackendTLSPolicy inventory")
		}
		if winner := selection.Winner; winner != nil {
			switch {
			case winner.Validation == nil:
				entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason("InvalidBackendTLSPolicy"), winner.Accepted.Message
				return nil
			case len(winner.Validation.SubjectAltNames) != 0:
				entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonUnsupportedProtocol, "TCP backend TLS supports hostname validation"
				return nil
			case BackendClientCertificateRef(snapshot.gateway) != nil:
				entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonUnsupportedProtocol, tcpBackendClientCertificateMessage
				return nil
			case winner.AncestorSlot != backendtls.AncestorSlotHeld:
				entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonPending, "Waiting for a BackendTLSPolicy Gateway ancestor slot"
				if winner.AncestorSlot == backendtls.AncestorSlotFull {
					entry.resolvedReason, entry.resolvedMessage = backendtls.AncestorLimitReason, backendtls.AncestorLimitMessage
				}
				return nil
			}
			validation = winner.Validation.CloudValidation()
		}
	}
	var endpoints discoveryv1.EndpointSliceList
	if err := r.APIReader.List(ctx, &endpoints, client.InNamespace(service.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}); err != nil {
		if tls == nil && !terminatesTLS(entry.listener) {
			return err
		}
		return sniBackendReadPending(ctx, entry, "Waiting for a complete backend EndpointSlice inventory")
	}
	if endpoints.Continue != "" {
		if tls == nil && !terminatesTLS(entry.listener) {
			return fmt.Errorf("incomplete TLSRoute EndpointSlice inventory")
		}
		return sniBackendReadPending(ctx, entry, "Waiting for a complete backend EndpointSlice inventory")
	}
	members, diagnostic := routebackend.EndpointMembers(service, model.ResolveServiceEndpoints(service, port, endpoints.Items))
	if diagnostic != nil {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message
		return nil
	}
	entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonResolvedRefs, "The backend Service reference is resolved"
	members = slices.DeleteFunc(members, func(member cloud.Member) bool { return member.Draining })
	if len(members) == 0 {
		return nil
	}
	topology := snapshot.topology.Topology
	target := cloud.TLSSNIRoute{
		Identity:   controller.RouteIdentity(r.Config, snapshot.gateway, route),
		ServiceUID: string(service.UID), BackendSubnetID: topology.MemberSubnetID,
		BackendSecurityGroupIDs: topology.BackendSecurityGroupIDs, Members: members, BackendTLS: validation,
	}
	if mixed || terminatesTLS(entry.listener) {
		target.ServicePort = int(port.Port)
	}
	target.Passthrough = mixed && !terminatesTLS(entry.listener)
	for _, hostname := range entry.hostnames {
		target.Hostname = hostname
		entry.targets = append(entry.targets, target)
	}
	return nil
}

// A failed dependency read withholds this route's forwarding without
// preventing withdrawals proven for another route.
func sniBackendReadPending(ctx context.Context, entry *sniRouteStatus, message string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonPending, message
	return nil
}

func (r *Reconciler) reportSNIRoutes(ctx context.Context, snapshot *gatewayMutationSnapshot, observation sniRouteObservation, result cloud.GatewayGraphResult, providerErr error) error {
	outcomes := make(map[string]cloud.RouteResult)
	for _, route := range result.Routes {
		if providerErr == nil || route.Result.ObservationDeferred {
			outcomes[route.Identity.RouteUID] = route.Result
		}
	}
	var missing *octavia.MissingGatewayCapability
	mixedUnavailable := errors.As(providerErr, &missing) && missing.Capability == octavia.GatewayCapabilityTCPTLSMixedMode
	programmedType := r.Config.Domain() + "/Programmed"
	targetType := r.Config.Domain() + "/ForwardingTarget"
	for _, entry := range observation.parentResults() {
		base := entry.route
		target := base.DeepCopy()
		fingerprint, err := sniForwardingFingerprint(snapshot.gateway, entry.targets)
		if err != nil {
			return err
		}
		outcome := outcomes[string(base.UID)]
		r.updateSNIRouteParents(target, snapshot.gateway, func(status *gatewayv1.RouteParentStatus) bool {
			if mixedUnavailable && entry.acceptedReason == gatewayv1.RouteReasonAccepted {
				r.setRejectedSNIParent(status, gatewayv1.RouteReasonUnsupportedValue, "Octavia does not advertise same-port mixed TLS in this region", base.Generation)
				return true
			}
			accepted, resolved, programmed := metav1.ConditionFalse, metav1.ConditionUnknown, metav1.ConditionFalse
			if entry.acceptedReason == gatewayv1.RouteReasonAccepted {
				accepted = metav1.ConditionTrue
			}
			resolvedReason, resolvedMessage := entry.resolvedReason, entry.resolvedMessage
			switch resolvedReason {
			case gatewayv1.RouteReasonPending:
			case "":
				resolvedReason, resolvedMessage = gatewayv1.RouteReasonPending, "The TLSRoute is not attached"
			case gatewayv1.RouteReasonResolvedRefs:
				resolved = metav1.ConditionTrue
			default:
				resolved = metav1.ConditionFalse
			}
			checkpoint := meta.FindStatusCondition(status.Conditions, targetType)
			previouslyProgrammed := meta.FindStatusCondition(status.Conditions, programmedType)
			retain := outcome.ObservationDeferred && checkpoint != nil && checkpoint.Status == metav1.ConditionTrue &&
				checkpoint.ObservedGeneration == base.Generation && checkpoint.Message == fingerprint &&
				previouslyProgrammed != nil && previouslyProgrammed.Status == metav1.ConditionTrue &&
				previouslyProgrammed.ObservedGeneration == base.Generation
			programmedReason, programmedMessage := "Pending", "Waiting for a coherent Octavia listener, policy, rule and backend readback"
			switch {
			case accepted == metav1.ConditionFalse:
				programmedReason, programmedMessage = string(entry.acceptedReason), entry.acceptedMessage
			case resolved == metav1.ConditionFalse:
				programmedReason, programmedMessage = string(resolvedReason), resolvedMessage
			case len(entry.hostnames) == 0:
				programmedReason, programmedMessage = "Conflicted", "Higher-priority TLSRoutes match every SNI hostname of this route"
			case resolvedReason == gatewayv1.RouteReasonPending:
				programmedMessage = resolvedMessage
			case len(entry.targets) == 0:
				programmedMessage = "The backend Service has no ready IPv4 endpoints"
			case outcome.Outcome.State == cloud.OutcomeReady || retain:
				programmed, programmedReason, programmedMessage = metav1.ConditionTrue, "Programmed", "Octavia readback confirms forwarding for the selected SNI hostnames and their backend"
			}
			controller.SetCondition(&status.Conditions, controller.Condition("Accepted", accepted, string(entry.acceptedReason), entry.acceptedMessage, base.Generation))
			controller.SetCondition(&status.Conditions, controller.Condition("ResolvedRefs", resolved, string(resolvedReason), resolvedMessage, base.Generation))
			controller.SetCondition(&status.Conditions, controller.Condition(programmedType, programmed, programmedReason, programmedMessage, base.Generation))
			if programmed == metav1.ConditionTrue {
				controller.SetCondition(&status.Conditions, controller.Condition(targetType, metav1.ConditionTrue, "Observed", fingerprint, base.Generation))
			} else {
				meta.RemoveStatusCondition(&status.Conditions, targetType)
			}
			return true
		})
		if err := r.patchSNIRouteStatus(ctx, base, target); err != nil {
			return err
		}
	}
	return nil
}

// One Gateway parent status represents all logical sections selected on its
// shared port. The accepted attachment and every effective target contribute.
func (observation sniRouteObservation) parentResults() sniRouteObservation {
	var results sniRouteObservation
	byRoute := make(map[client.ObjectKey]int)
	for _, entry := range observation {
		key := client.ObjectKeyFromObject(entry.route)
		i, exists := byRoute[key]
		if !exists {
			byRoute[key] = len(results)
			results = append(results, entry)
			continue
		}
		current := &results[i]
		if current.acceptedReason != gatewayv1.RouteReasonAccepted {
			if entry.acceptedReason == gatewayv1.RouteReasonAccepted ||
				current.acceptedReason == gatewayv1.RouteReasonNoMatchingParent {
				*current = entry
			}
			continue
		}
		if entry.acceptedReason != gatewayv1.RouteReasonAccepted {
			continue
		}
		current.hostnames = append(slices.Clone(current.hostnames), entry.hostnames...)
		current.targets = append(slices.Clone(current.targets), entry.targets...)
		if current.resolvedReason == "" ||
			current.resolvedReason == gatewayv1.RouteReasonResolvedRefs && entry.resolvedReason != "" {
			current.resolvedReason, current.resolvedMessage = entry.resolvedReason, entry.resolvedMessage
		}
	}
	return results
}

func sniForwardingFingerprint(gateway *gatewayv1.Gateway, targets []cloud.TLSSNIRoute) (string, error) {
	if len(targets) == 0 {
		return "", nil
	}
	routes := slices.Clone(targets)
	for i := range routes {
		routes[i].Identity.ControllerVersion = ""
	}
	slices.SortFunc(routes, func(a, b cloud.TLSSNIRoute) int { return cmp.Compare(a.Hostname, b.Hostname) })
	content := struct {
		GatewayGeneration int64
		Route             *cloud.TLSSNIRoute  `json:",omitempty"`
		Routes            []cloud.TLSSNIRoute `json:",omitempty"`
	}{GatewayGeneration: gateway.Generation}
	version := "v2"
	if len(routes) == 1 {
		// Keep the persisted fingerprint for existing single-hostname routes.
		content.Route, version = &routes[0], "v1"
	} else {
		content.Routes = routes
	}
	data, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%x", version, sha256.Sum256(data)), nil
}

func (r *Reconciler) clearSNIRouteParentStatuses(ctx context.Context, gateway *gatewayv1.Gateway) error {
	var routes gatewayv1.TLSRouteList
	if err := r.APIReader.List(ctx, &routes); err != nil {
		// With the CRD gone, no status remains to clean up.
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if routes.Continue != "" {
		return fmt.Errorf("incomplete TLSRoute inventory before Gateway cleanup")
	}
	for i := range routes.Items {
		base := &routes.Items[i]
		target := base.DeepCopy()
		r.removeOwnedSNIParents(target, gateway)
		if err := r.patchSNIRouteStatus(ctx, base, target); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) patchSNIRouteStatus(ctx context.Context, base, target *gatewayv1.TLSRoute) error {
	if equality.Semantic.DeepEqual(base.Status, target.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Patch(ctx, target, controller.OptimisticMergeFrom(base)))
}

func (r *Reconciler) gatewayRequestsForSNIDependency(ctx context.Context, dependency client.Object) []reconcile.Request {
	var routes gatewayv1.TLSRouteList
	if err := r.List(ctx, &routes); err != nil {
		return nil // The periodic live observation also checks these inputs.
	}
	keys := make(map[client.ObjectKey]struct{})
	for i := range routes.Items {
		route := &routes.Items[i]
		affected := false
		switch object := dependency.(type) {
		case *corev1.Namespace:
			affected = route.Namespace == object.Name
		default:
			for _, rule := range route.Spec.Rules {
				for _, ref := range rule.BackendRefs {
					key, ok := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
					if !ok || key.Namespace != dependency.GetNamespace() {
						continue
					}
					switch object := dependency.(type) {
					case *gatewayv1.BackendTLSPolicy:
						affected = affected || slices.Contains(backendtls.ServiceKeys(object), key)
					case *corev1.Service:
						affected = affected || key.Name == object.Name
					case *discoveryv1.EndpointSlice:
						affected = affected || key.Name == object.Labels[discoveryv1.LabelServiceName]
					case *gatewayv1.ReferenceGrant:
						affected = affected || key.Namespace != route.Namespace
					}
				}
			}
		}
		if affected {
			for _, request := range r.gatewayRequestsForRoute(ctx, route) {
				keys[request.NamespacedName] = struct{}{}
			}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) gatewayRequestsForSNIBackendCA(ctx context.Context, object client.Object) []reconcile.Request {
	var policies gatewayv1.BackendTLSPolicyList
	key := client.ObjectKeyFromObject(object)
	if err := r.List(ctx, &policies, client.InNamespace(key.Namespace), client.MatchingFields{
		controller.IndexBackendTLSPolicyByCA: controller.ObjectKeyString(key),
	}); err != nil {
		return nil
	}
	keys := make(map[client.ObjectKey]struct{})
	for i := range policies.Items {
		for _, request := range r.gatewayRequestsForSNIDependency(ctx, &policies.Items[i]) {
			keys[request.NamespacedName] = struct{}{}
		}
	}
	return controller.SortedRequests(keys)
}
