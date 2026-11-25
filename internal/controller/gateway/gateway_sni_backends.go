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
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

func (r *Reconciler) observeSNIService(ctx context.Context, route *gatewayv1.TLSRoute, ref gatewayv1.BackendRef) (*corev1.Service, *corev1.ServicePort, *model.Diagnostic, error) {
	key, supported := model.BackendServiceReference(route.Namespace, ref.BackendObjectReference)
	if !supported {
		return nil, nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonInvalidKind), Message: "The backendRef must refer to a core Service"}, nil
	}
	if key.Namespace != route.Namespace {
		grants, err := graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
		if err != nil {
			return nil, nil, nil, err
		}
		grants = slices.DeleteFunc(grants, func(grant gatewayv1.ReferenceGrant) bool { return !grant.DeletionTimestamp.IsZero() })
		if !model.BackendReferenceAllowedFor(tlsRouteKind, route.Namespace, key, grants) {
			return nil, nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonRefNotPermitted), Message: "No ReferenceGrant authorizes this TLSRoute to use the Service"}, nil
		}
	}
	var service corev1.Service
	if err := r.APIReader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: "The backend Service was not found"}, nil
		}
		return nil, nil, nil, err
	}
	if !service.DeletionTimestamp.IsZero() || service.UID == "" {
		return nil, nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: "The backend Service has no live identity"}, nil
	}
	port, diagnostic := model.ResolveTransportServicePort(&service, ref.Port, corev1.ProtocolTCP)
	if diagnostic == nil {
		diagnostic = routebackend.ValidateEndpointService(&service)
	}
	if diagnostic != nil {
		return nil, nil, diagnostic, nil
	}
	return &service, port, nil, nil
}

func (r *Reconciler) resolveSNIBackend(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus, tls *graph.BackendTLSObserver, mixed bool) error {
	refs := entry.route.Spec.Rules[0].BackendRefs
	if len(refs) > 1 {
		return r.resolveWeightedSNIBackends(ctx, snapshot, entry, tls, mixed)
	}
	service, port, diagnostic, err := r.observeSNIService(ctx, entry.route, refs[0])
	if err != nil {
		return sniBackendReadPending(ctx, entry, "Waiting for a live Service and ReferenceGrant inventory")
	}
	if diagnostic != nil {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message
		return nil
	}
	if ptr.Deref(refs[0].Weight, 1) == 0 {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonResolvedRefs, "The backend Service reference is resolved"
		entry.programmingDiagnostic = &model.Diagnostic{Reason: "Pending", Message: "The TLSRoute has no positive-weight backend"}
		return nil
	}
	var validation *cloud.BackendTLSValidation
	if len(entry.hostnames) != 0 && (!terminatesTLS(entry.listener) || !r.Config.BackendTLSPolicy) {
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
	} else if len(entry.hostnames) != 0 {
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
		validation, diagnostic = sniBackendTLSValidation(snapshot.gateway, selection.Winner)
		if diagnostic != nil {
			entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message
			return nil
		}
	}
	members, diagnostic, err := r.readSNIMembers(ctx, service, port)
	if err != nil {
		return sniBackendReadPending(ctx, entry, "Waiting for a complete backend EndpointSlice inventory")
	}
	if diagnostic != nil {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message
		return nil
	}
	entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonResolvedRefs, "The backend Service reference is resolved"
	if len(members) == 0 {
		return nil
	}
	topology := snapshot.topology.Topology
	target := cloud.TLSSNIRoute{
		Identity:   controller.RouteIdentity(r.Config, snapshot.gateway, entry.route),
		ServiceUID: string(service.UID), BackendSubnetID: topology.MemberSubnetID,
		BackendSecurityGroupIDs: topology.BackendSecurityGroupIDs, Members: members, BackendTLS: validation,
	}
	if mixed || terminatesTLS(entry.listener) {
		target.ServicePort = int(port.Port)
	}
	target.Passthrough = mixed && !terminatesTLS(entry.listener)
	appendSNIForwardingTargets(entry, target)
	return nil
}

func sniBackendTLSValidation(gateway *gatewayv1.Gateway, winner *backendtls.PolicyResult) (*cloud.BackendTLSValidation, *model.Diagnostic) {
	switch {
	case winner == nil:
		return nil, nil
	case winner.Accepted.Status == metav1.ConditionUnknown:
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: winner.Accepted.Message}
	case winner.Validation == nil:
		return nil, &model.Diagnostic{Reason: "InvalidBackendTLSPolicy", Message: winner.Accepted.Message}
	case len(winner.Validation.SubjectAltNames) != 0:
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: "TCP backend TLS supports hostname validation"}
	case BackendClientCertificateRef(gateway) != nil:
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: tcpBackendClientCertificateMessage}
	case winner.AncestorSlot == backendtls.AncestorSlotFull:
		return nil, &model.Diagnostic{Reason: string(backendtls.AncestorLimitReason), Message: backendtls.AncestorLimitMessage}
	case winner.AncestorSlot != backendtls.AncestorSlotHeld:
		return nil, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: "Waiting for a BackendTLSPolicy Gateway ancestor slot"}
	}
	return winner.Validation.CloudValidation(), nil
}

const sniWeightedBackendTLSUnsupported = "BackendTLSPolicy cannot authenticate a TLSRoute with multiple backendRefs"

func (r *Reconciler) resolveWeightedSNIBackends(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus, tls *graph.BackendTLSObserver, mixed bool) error {
	refs := entry.route.Spec.Rules[0].BackendRefs
	pools := make([]cloud.TLSSNIBackendPool, 0, len(refs))
	var total, rejected int
	var invalid []string
	var requiresBackendTLS bool
	var inventoryPending string
	authenticate := terminatesTLS(entry.listener) && r.Config.BackendTLSPolicy
	for i, ref := range refs {
		pool, applies, diagnostic := r.observeWeightedSNIBackend(ctx, snapshot, entry, ref, tls, authenticate)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		requiresBackendTLS = requiresBackendTLS || applies
		if diagnostic != nil {
			if !applies && diagnostic.Reason == string(gatewayv1.RouteReasonPending) && inventoryPending == "" {
				inventoryPending = diagnostic.Message
			}
			recordSNIRefDiagnostic(entry, &invalid, i, diagnostic)
		}
		if pool.Weight > 0 {
			total += pool.Weight
			if len(pool.Members) == 0 {
				rejected += pool.Weight
			}
			pools = append(pools, pool)
		}
	}
	if len(invalid) == 0 {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonResolvedRefs, "The backend Service references are resolved"
	} else {
		entry.resolvedMessage = strings.Join(invalid, ". ")
	}
	authenticated := authenticate && requiresBackendTLS
	switch {
	case !authenticated && inventoryPending != "":
		return sniBackendReadPending(ctx, entry, inventoryPending)
	case !authenticate && requiresBackendTLS:
		entry.programmingDiagnostic = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: sniWeightedBackendTLSUnsupported}
		return nil
	case len(entry.hostnames) == 0:
		return nil
	case total == 0:
		entry.programmingDiagnostic = &model.Diagnostic{Reason: "Pending", Message: "The TLSRoute has no positive-weight backend"}
		return nil
	case rejected == total:
		return nil
	}

	if rejected > 0 && !r.Config.TCPConnectionRejection {
		entry.programmingDiagnostic = &model.Diagnostic{Reason: "Pending", Message: "Proportional TLSRoute rejection requires the TCP connection rejection controller option"}
		return nil
	}
	topology := snapshot.topology.Topology
	target := cloud.TLSSNIRoute{
		Identity: controller.RouteIdentity(r.Config, snapshot.gateway, entry.route), Passthrough: mixed && !terminatesTLS(entry.listener),
		BackendSubnetID: topology.MemberSubnetID, BackendSecurityGroupIDs: topology.BackendSecurityGroupIDs,
	}
	switch {
	case authenticated && len(pools) == 1:
		// Zero-weight references affect resolution status only. The sole
		// positive share uses the native authenticated Service pool.
		pool := pools[0]
		target.ServiceUID, target.ServicePort, target.Members, target.BackendTLS = pool.ServiceUID, pool.ServicePort, pool.Members, pool.BackendTLS
	case authenticated:
		target.BackendPools = pools
	default:
		share := cloud.TCPConnectionRejection{RejectWeight: rejected, TotalWeight: total}
		if diagnostic := flattenWeightedSNIPools(&target, pools, share); diagnostic != nil {
			entry.programmingDiagnostic = diagnostic
			return nil
		}
	}
	appendSNIForwardingTargets(entry, target)
	return nil
}

// Each positive reference starts as a denial. Only a fully resolved Service,
// its effective transport, and ready endpoints can populate that share.
func (r *Reconciler) observeWeightedSNIBackend(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus, ref gatewayv1.BackendRef, tls *graph.BackendTLSObserver, authenticate bool) (cloud.TLSSNIBackendPool, bool, *model.Diagnostic) {
	pool := cloud.TLSSNIBackendPool{Weight: int(ptr.Deref(ref.Weight, 1))}
	service, port, diagnostic, err := r.observeSNIService(ctx, entry.route, ref)
	if err != nil {
		return pool, false, &model.Diagnostic{Reason: "Pending", Message: "Waiting for a live Service and ReferenceGrant inventory"}
	}
	if diagnostic != nil || pool.Weight == 0 || len(entry.hostnames) == 0 {
		return pool, false, diagnostic
	}
	var applies bool
	var validation *cloud.BackendTLSValidation
	if authenticate {
		ancestor := &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(snapshot.gateway), ControllerName: r.Config.ControllerName}
		selection, err := tls.ObserveWeightedService(ctx, service, port, ancestor)
		if err != nil {
			return pool, false, &model.Diagnostic{Reason: "Pending", Message: "Waiting for a live BackendTLSPolicy inventory"}
		}
		applies = selection.Winner != nil
		validation, diagnostic = sniBackendTLSValidation(snapshot.gateway, selection.Winner)
		if diagnostic != nil {
			return pool, true, diagnostic
		}
	} else {
		applies, err := tls.HasPolicy(ctx, service, port)
		if err != nil {
			return pool, false, &model.Diagnostic{Reason: "Pending", Message: "Waiting for a live BackendTLSPolicy inventory"}
		}
		if applies {
			return pool, true, &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: sniWeightedBackendTLSUnsupported}
		}
	}
	members, diagnostic, err := r.readSNIMembers(ctx, service, port)
	if err != nil {
		return pool, applies, &model.Diagnostic{Reason: "Pending", Message: "Waiting for a complete backend EndpointSlice inventory"}
	}
	if diagnostic == nil && len(members) > 0 {
		pool.ServiceUID, pool.ServicePort, pool.Members, pool.BackendTLS = string(service.UID), int(port.Port), members, validation
	}
	return pool, applies, diagnostic
}

func flattenWeightedSNIPools(target *cloud.TLSSNIRoute, pools []cloud.TLSSNIBackendPool, share cloud.TCPConnectionRejection) *model.Diagnostic {
	if share.RejectWeight > 0 {
		if !share.Valid() {
			return &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "Invalid TLSRoute backend weight share"}
		}
		target.TCPConnectionRejection = &share
	}
	groups := make([]routebackend.WeightedServiceMembers, 0, len(pools))
	services := make([]cloud.TLSBackendService, 0, len(pools))
	for _, pool := range pools {
		if len(pool.Members) > 0 {
			groups = append(groups, routebackend.WeightedServiceMembers{Weight: int32(pool.Weight), Members: pool.Members})
			services = append(services, cloud.TLSBackendService{UID: pool.ServiceUID, Port: pool.ServicePort})
			target.BackendServiceMembers = append(target.BackendServiceMembers, pool)
		}
	}
	members, err := routebackend.WeightedMembers(cloud.ListenerProtocolTCP, groups)
	if err != nil {
		return &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: err.Error()}
	}
	slices.SortFunc(services, func(a, b cloud.TLSBackendService) int {
		return cmp.Or(cmp.Compare(a.UID, b.UID), cmp.Compare(a.Port, b.Port))
	})
	target.BackendServices, target.Members = slices.Compact(services), members
	return nil
}

func recordSNIRefDiagnostic(entry *sniRouteStatus, messages *[]string, index int, diagnostic *model.Diagnostic) {
	reason := gatewayv1.RouteConditionReason(diagnostic.Reason)
	if sniRefReasonPriority(reason) > sniRefReasonPriority(entry.resolvedReason) {
		entry.resolvedReason = reason
	}
	*messages = append(*messages, fmt.Sprintf("backendRef %d (%s)", index+1, diagnostic.Message))
}

func sniRefReasonPriority(reason gatewayv1.RouteConditionReason) int {
	switch reason {
	case "":
		return 0
	case gatewayv1.RouteReasonResolvedRefs:
		return 1
	case gatewayv1.RouteReasonPending:
		return 2
	case backendtls.AncestorLimitReason:
		return 4
	case gatewayv1.RouteReasonRefNotPermitted:
		return 5
	default:
		return 3
	}
}

func (r *Reconciler) readSNIMembers(ctx context.Context, service *corev1.Service, port *corev1.ServicePort) ([]cloud.Member, *model.Diagnostic, error) {
	var endpoints discoveryv1.EndpointSliceList
	if err := r.APIReader.List(ctx, &endpoints, client.InNamespace(service.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}); err != nil {
		return nil, nil, err
	}
	if endpoints.Continue != "" {
		return nil, nil, fmt.Errorf("incomplete TLSRoute EndpointSlice inventory")
	}
	members, diagnostic := routebackend.EndpointMembers(service, model.ResolveServiceEndpoints(service, port, endpoints.Items))
	return slices.DeleteFunc(members, func(member cloud.Member) bool { return member.Draining }), diagnostic, nil
}

func appendSNIForwardingTargets(entry *sniRouteStatus, target cloud.TLSSNIRoute) {
	for _, hostname := range entry.hostnames {
		target.Hostname = hostname
		entry.targets = append(entry.targets, target)
	}
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
