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

func (r *Reconciler) resolveWeightedSNIBackends(ctx context.Context, snapshot *gatewayMutationSnapshot, entry *sniRouteStatus, tls *graph.BackendTLSObserver, mixed bool) error {
	const backendTLSUnsupported = "BackendTLSPolicy cannot authenticate a TLSRoute with multiple backendRefs"
	refs := entry.route.Spec.Rules[0].BackendRefs
	groups := make([]routebackend.WeightedServiceMembers, 0, len(refs))
	services := make([]cloud.TLSBackendService, 0, len(refs))
	var total, rejected int
	var invalid []string
	var requiresBackendTLS bool
	for i, ref := range refs {
		weight := ptr.Deref(ref.Weight, 1)
		total += int(weight)
		service, port, diagnostic, err := r.observeSNIService(ctx, entry.route, ref)
		if err != nil {
			return sniBackendReadPending(ctx, entry, "Waiting for a live Service and ReferenceGrant inventory")
		}
		if diagnostic != nil {
			recordSNIRefDiagnostic(entry, &invalid, i, diagnostic)
			rejected += int(weight)
			continue
		}
		if weight == 0 || len(entry.hostnames) == 0 {
			continue
		}
		applies, err := tls.HasPolicy(ctx, service, port)
		if err != nil {
			return sniBackendReadPending(ctx, entry, "Waiting for a live BackendTLSPolicy inventory")
		}
		if applies {
			requiresBackendTLS = true
			recordSNIRefDiagnostic(entry, &invalid, i, &model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: backendTLSUnsupported,
			})
			continue
		}
		members, diagnostic, err := r.readSNIMembers(ctx, service, port)
		if err != nil {
			return sniBackendReadPending(ctx, entry, "Waiting for a complete backend EndpointSlice inventory")
		}
		if diagnostic != nil {
			recordSNIRefDiagnostic(entry, &invalid, i, diagnostic)
		}
		if len(members) == 0 {
			rejected += int(weight)
			continue
		}
		groups = append(groups, routebackend.WeightedServiceMembers{Weight: weight, Members: members})
		services = append(services, cloud.TLSBackendService{UID: string(service.UID), Port: int(port.Port)})
	}
	if len(invalid) == 0 {
		entry.resolvedReason, entry.resolvedMessage = gatewayv1.RouteReasonResolvedRefs, "The backend Service references are resolved"
	} else {
		entry.resolvedMessage = strings.Join(invalid, ". ")
	}
	switch {
	case requiresBackendTLS:
		entry.programmingDiagnostic = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol), Message: backendTLSUnsupported}
		return nil
	case len(entry.hostnames) == 0:
		return nil
	case total == 0:
		entry.programmingDiagnostic = &model.Diagnostic{Reason: "Pending", Message: "The TLSRoute has no positive-weight backend"}
		return nil
	case len(groups) == 0:
		return nil
	}

	var rejection *cloud.TCPConnectionRejection
	if rejected > 0 {
		if !r.Config.TCPConnectionRejection {
			entry.programmingDiagnostic = &model.Diagnostic{Reason: "Pending", Message: "Proportional TLSRoute rejection requires the TCP connection rejection controller option"}
			return nil
		}
		share := cloud.TCPConnectionRejection{RejectWeight: rejected, TotalWeight: total}
		if !share.Valid() {
			entry.programmingDiagnostic = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "Invalid TLSRoute backend weight share"}
			return nil
		}
		rejection = &share
	}
	members, err := routebackend.WeightedMembers(cloud.ListenerProtocolTCP, groups)
	if err != nil {
		entry.programmingDiagnostic = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: err.Error()}
		return nil
	}
	slices.SortFunc(services, func(a, b cloud.TLSBackendService) int {
		return cmp.Or(cmp.Compare(a.UID, b.UID), cmp.Compare(a.Port, b.Port))
	})
	topology := snapshot.topology.Topology
	appendSNIForwardingTargets(entry, cloud.TLSSNIRoute{
		Identity: controller.RouteIdentity(r.Config, snapshot.gateway, entry.route), Passthrough: mixed && !terminatesTLS(entry.listener),
		BackendServices: slices.Compact(services), Members: members, TCPConnectionRejection: rejection,
		BackendSubnetID: topology.MemberSubnetID, BackendSecurityGroupIDs: topology.BackendSecurityGroupIDs,
	})
	return nil
}

func recordSNIRefDiagnostic(entry *sniRouteStatus, messages *[]string, index int, diagnostic *model.Diagnostic) {
	reason := gatewayv1.RouteConditionReason(diagnostic.Reason)
	if entry.resolvedReason == "" || reason == gatewayv1.RouteReasonRefNotPermitted {
		entry.resolvedReason = reason
	}
	*messages = append(*messages, fmt.Sprintf("backendRef %d (%s)", index+1, diagnostic.Message))
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
