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
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

type listenerSetCertificateResolver interface {
	ResolveListenerSet(context.Context, *gatewayv1.Gateway, *gatewayv1.ListenerSet, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason)
}

// ObserveListenerSets includes the live certificate decisions needed by both
// the frontend graph and its HTTPRoute consumers.
func (r *Reconciler) ObserveListenerSets(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway) (ListenerSetProjection, error) {
	projection, err := ObserveListenerSets(ctx, reader, r.Config, gateway)
	if err != nil {
		return ListenerSetProjection{}, err
	}
	r.resolveListenerSetHTTPS(ctx, gateway, &projection)
	return projection, nil
}

type listenerSetCertificateReferenceResolver interface {
	ListenerSetReferenceStatus(context.Context, *gatewayv1.Gateway, *gatewayv1.ListenerSet, *gatewayv1.Listener) gatewayv1.ListenerConditionReason
}

func (r *Reconciler) resolveListenerSetHTTPS(ctx context.Context, gateway *gatewayv1.Gateway, projection *ListenerSetProjection) {
	if len(projection.sets) == 0 {
		return
	}
	primary := httpsSections(httpsProfileListeners(r.Config, gateway))
	enrolled := strings.Split(gateway.Annotations[r.Config.GatewayHTTPSIsolatedPortsAnnotation()], ",")
	clientValidation := make(map[int]tlsResolution)
	projection.https = make(map[int]cloud.ListenerTLS)
	projection.gatewayTLS = make(map[int]tlsResolution)
	resolver, _ := r.Certificates.(listenerSetCertificateResolver)
	var isolationChecked, isolationAvailable bool

	for i := range projection.sets {
		set := &projection.sets[i]
		for j := range set.entries {
			entry := &set.entries[j]
			if entry.listener.Protocol != gatewayv1.HTTPSProtocolType {
				continue
			}
			if entry.conflicted {
				entry.observeCertificateReference(ctx, gateway, set.object, resolver)
			}
			if entry.rejection != "" {
				continue
			}

			port := int(entry.listener.Port)
			peers := primary[port]
			validation, observed := clientValidation[port]
			if !observed {
				validation = r.resolveFrontendClientValidation(ctx, gateway, entry.listener.Port)
				clientValidation[port] = validation
				// Record the HTTPS port even when it has no authorized hosts.
				projection.https[port] = cloud.ListenerTLS{}
				if len(peers) != 0 {
					parent := r.resolveGatewayServingCertificate(ctx, gateway, peers, validation)
					projection.gatewayTLS[port] = parent
					if parent.issue == nil {
						projection.https[port] = parent.tls
					}
				}
			}
			if validation.issue != nil {
				entry.rejection = validation.issue.reason
				if validation.resolvedRefs != nil {
					entry.referenceIssue = &httpsListenerIssue{
						condition: gatewayv1.ListenerConditionResolvedRefs,
						reason:    gatewayv1.ListenerConditionReason(validation.resolvedRefs.Reason),
						message:   validation.resolvedRefs.Message,
					}
				} else {
					entry.observeCertificateReference(ctx, gateway, set.object, resolver)
				}
				continue
			}

			reference := entry.observeCertificateReference(ctx, gateway, set.object, resolver)
			if !entry.certificateResolved {
				entry.rejection = entry.referenceIssue.reason
				continue
			}
			if !isolationChecked {
				isolationChecked = true
				if r.HTTPSListenerIsolationAvailable != nil {
					available, err := r.HTTPSListenerIsolationAvailable(ctx)
					isolationAvailable = available && err == nil
				}
			}
			if !isolationAvailable {
				entry.rejection = gatewayv1.ListenerReasonUnsupportedValue
				continue
			}
			tls := projection.https[port]
			if tls.CertificateRef != "" && tls.CertificateRef != reference {
				entry.rejection = gatewayv1.ListenerReasonUnsupportedValue
				continue
			}
			if len(peers) != 0 && !slices.Contains(enrolled, strconv.Itoa(port)) {
				projection.isolationPorts = append(projection.isolationPorts, strconv.Itoa(port))
				entry.rejection = gatewayv1.ListenerReasonPending
				continue
			}

			tls.CertificateRef = reference
			tls.ClientValidation = validation.tls.ClientValidation
			tls.VirtualHosts = append(slices.Clone(tls.VirtualHosts), HTTPVirtualHostname(&entry.listener))
			slices.Sort(tls.VirtualHosts)
			projection.https[port] = tls
		}
	}
	slices.Sort(projection.isolationPorts)
	projection.isolationPorts = slices.Compact(projection.isolationPorts)
}

func (e *listenerSetEntry) observeCertificateReference(ctx context.Context, gateway *gatewayv1.Gateway, set *gatewayv1.ListenerSet, resolver listenerSetCertificateResolver) string {
	reference, reason := "", gatewayv1.ListenerReasonInvalidCertificateRef
	if statusResolver, ok := resolver.(listenerSetCertificateReferenceResolver); ok && e.rejection != "" {
		reason = statusResolver.ListenerSetReferenceStatus(ctx, gateway, set, &e.listener)
	} else if resolver != nil {
		reference, reason = resolver.ResolveListenerSet(ctx, gateway, set, &e.listener)
		if reference == "" && reason == "" {
			reason = gatewayv1.ListenerReasonInvalidCertificateRef
		}
	}
	if reason != "" {
		e.referenceIssue = certificateReferenceIssue(reason)
	} else {
		e.certificateResolved = true
	}
	return reference
}

func (r *Reconciler) checkpointListenerSetHTTPS(ctx context.Context, gateway *gatewayv1.Gateway, projection ListenerSetProjection) error {
	isolationKey := r.Config.GatewayHTTPSIsolatedPortsAnnotation()
	ports := slices.Clone(projection.isolationPorts)
	if existing := gateway.Annotations[isolationKey]; existing != "" {
		ports = append(ports, strings.Split(existing, ",")...)
	}
	slices.Sort(ports)
	desired := gateway.DeepCopy()
	desired.Annotations[isolationKey] = strings.Join(slices.Compact(ports), ",")
	return r.Patch(ctx, desired, controller.OptimisticMergeFrom(gateway))
}
