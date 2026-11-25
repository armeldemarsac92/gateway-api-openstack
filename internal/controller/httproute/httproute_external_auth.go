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

package httproute

import (
	"context"
	"maps"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

type routeExternalAuthObservation struct {
	config     *cloud.ListenerExternalAuth
	diagnostic model.Diagnostic
	capability model.Diagnostic
}

func (r *Reconciler) checkRouteExternalAuth(rule gatewayv1.HTTPRouteRule, kind gatewayv1.Kind) bool {
	return kind == model.HTTPRouteKind && r.routeExternalAuthRulesEnabled() && model.HasHTTPRouteExternalAuth(rule)
}

// The fixed-response profile retains the match while the filter is unavailable,
// including after the operator turns off the authorization flag.
func (r *Reconciler) routeExternalAuthRulesEnabled() bool {
	return r.Config.FixedHTTPResponses && r.Config.GatewayAPIChannel == features.FeatureChannelExperimental
}

func (inputs *routeModelInputs) observeExternalAuth(ctx context.Context, reader client.Reader, config controller.Config) (map[*model.HTTPExternalAuth]routeExternalAuthObservation, error) {
	if !inputs.input.CheckRouteExternalAuth {
		return nil, nil
	}
	// Each compiled filter is immutable and retains its identity when rule
	// projection renumbers the rules before lowering.
	var result map[*model.HTTPExternalAuth]routeExternalAuthObservation
	topology := sync.OnceValues(func() (classconfig.Resolved, error) {
		return controller.StoredGatewayTopology(config, inputs.gateway)
	})
	tlsObserver := graph.NewBackendTLSObserver(reader, config.ClusterTrustBundles)
	clientCertificate := sync.OnceValue(func() cloud.BackendClientCertificate {
		if gatewaycontroller.BackendClientCertificateRef(inputs.gateway) == nil {
			return cloud.BackendClientCertificate{}
		}
		if inputs.backendClientCertificate == nil {
			return cloud.BackendClientCertificate{Denied: true}
		}
		certificate := inputs.backendClientCertificate(ctx, inputs.gateway)
		certificate.Denied = certificate.Denied || certificate.Reference == ""
		return certificate
	})
	for _, route := range inputs.logical.Routes {
		for _, rule := range route.Rules {
			if rule.ExternalAuth == nil {
				continue
			}
			resolvedTopology, err := topology()
			if err != nil {
				return nil, err
			}
			auth, diagnostic, err := inputs.resolveExternalAuth(ctx, reader, tlsObserver, route.Source.Namespace, *rule.ExternalAuth, resolvedTopology)
			var certificate cloud.BackendClientCertificate
			if auth.BackendTLS != nil && diagnostic == nil && err == nil {
				certificate = clientCertificate()
			}
			var observation routeExternalAuthObservation
			switch {
			case ctx.Err() != nil:
				return nil, ctx.Err()
			case err != nil:
				observation.diagnostic = pendingExternalAuth("Waiting to read the ExternalAuth backend configuration")
			case diagnostic != nil:
				observation.diagnostic = *diagnostic
			case certificate.Denied:
				observation.diagnostic = pendingExternalAuth("Waiting for an authorized Gateway backend client certificate")
			case len(auth.Members) == 0:
				observation.diagnostic = pendingExternalAuth("ExternalAuth backend has no ready members")
			default:
				observation.config = &auth
			}
			if result == nil {
				result = make(map[*model.HTTPExternalAuth]routeExternalAuthObservation)
			}
			result[rule.ExternalAuth] = observation
		}
	}
	return result, nil
}

func pendingExternalAuth(message string) model.Diagnostic {
	return model.Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs),
		Reason: string(gatewayv1.RouteReasonPending), Message: message, Scope: model.ScopeRule}
}

func (inputs *routeModelInputs) resolveExternalAuth(ctx context.Context, reader client.Reader, tlsObserver *graph.BackendTLSObserver,
	namespace string, filter model.HTTPExternalAuth, topology classconfig.Resolved) (cloud.ListenerExternalAuth, *model.Diagnostic, error) {
	auth, service, port, diagnostic, err := resolveRouteExternalAuthTransport(ctx, reader, tlsObserver, inputs.backendTLSAncestor, namespace, filter)
	if diagnostic != nil || err != nil {
		return auth, diagnostic, err
	}
	return routebackend.ResolveExternalAuthMembers(ctx, reader, service, port, auth, topology)
}

func resolveRouteExternalAuthTransport(ctx context.Context, reader client.Reader, tlsObserver *graph.BackendTLSObserver,
	ancestor *backendtls.GatewayAncestor, namespace string, filter model.HTTPExternalAuth) (cloud.ListenerExternalAuth, *corev1.Service, *corev1.ServicePort, *model.Diagnostic, error) {
	auth := filter.Config
	service, port, diagnostic, err := routebackend.ObserveExternalAuthService(ctx, reader, model.HTTPRouteKind, namespace, filter)
	if diagnostic != nil || err != nil {
		return auth, nil, nil, diagnostic, err
	}
	if ancestor == nil {
		required, err := tlsObserver.HasPolicy(ctx, service, port)
		if err != nil {
			return auth, nil, nil, nil, err
		}
		if required {
			return auth, nil, nil, &model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
				Message: "ExternalAuth backend TLS requires --octavia-backend-tls-policy", Scope: model.ScopeRule}, nil
		}
	} else {
		selection, err := tlsObserver.Observe(ctx, service, port, ancestor)
		if err != nil {
			return auth, nil, nil, nil, err
		}
		if winner := selection.Winner; winner != nil {
			if winner.Validation == nil || winner.AncestorSlot != backendtls.AncestorSlotHeld {
				diagnostic := pendingExternalAuth("Waiting for an authorized ExternalAuth BackendTLSPolicy")
				return auth, nil, nil, &diagnostic, nil
			}
			auth.BackendTLS = winner.Validation.CloudValidation()
			auth.BackendTLS.RequireH2 = auth.Protocol == cloud.ExternalAuthProtocolGRPC
		}
	}
	return auth, service, port, externalAuthTransportDiagnostic(service, port, filter, auth.BackendTLS != nil), nil
}

func externalAuthTransportDiagnostic(service *corev1.Service, port *corev1.ServicePort, filter model.HTTPExternalAuth, secured bool) *model.Diagnostic {
	var issue *model.Diagnostic
	if filter.Config.Protocol == cloud.ExternalAuthProtocolGRPC {
		if !secured && strings.EqualFold(ptr.Deref(port.AppProtocol, ""), "https") || secured && model.ServicePortUsesH2C(port) {
			issue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedProtocol),
				Message: "The gRPC ExternalAuth Service appProtocol conflicts with its required transport"}
		}
	} else {
		_, issue = model.ResolveServicePortForBackendTLS(service, filter.BackendRef.Port, false, secured)
	}
	if issue != nil {
		return &model.Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs),
			Reason: issue.Reason, Message: issue.Message, Scope: model.ScopeRule}
	}
	return nil
}

func (r *Reconciler) checkExternalAuthCapabilities(ctx context.Context, observations map[*model.HTTPExternalAuth]routeExternalAuthObservation) {
	type requirements struct {
		protocol      string
		tls, systemCA bool
		san, weighted bool
		bodySize      int32
	}
	checked := map[requirements]model.Diagnostic{}
	for filter, auth := range observations {
		if auth.config == nil {
			continue
		}
		key := requirements{protocol: auth.config.Protocol, weighted: filter.Weighted, bodySize: auth.config.ForwardBodyMaxSize}
		if tls := auth.config.BackendTLS; tls != nil {
			key.tls = true
			key.systemCA = tls.Trust == cloud.BackendTLSTrustSystem
			key.san = len(tls.SubjectAltNames) > 0
		}
		diagnostic, found := checked[key]
		if !found {
			diagnostic = pendingExternalAuth("Waiting to verify regional Octavia HTTPRoute ExternalAuth support")
			if probe := r.RouteExternalAuthAvailable; probe != nil {
				if available, err := probe(ctx, *auth.config, key.weighted); err == nil {
					switch {
					case available:
						diagnostic = model.Diagnostic{}
					case key.bodySize > 0:
						diagnostic = model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
							Message: "The regional Octavia provider does not support this ExternalAuth rule with the requested body limit", Scope: model.ScopeRule}
					case key.protocol == cloud.ExternalAuthProtocolGRPC:
						diagnostic = model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
							Message: "The regional Octavia provider does not advertise the required gRPC ExternalAuth capabilities", Scope: model.ScopeRule}
					case key.tls:
						diagnostic = model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
							Message: "The regional Octavia provider does not advertise the required HTTP ExternalAuth backend TLS capabilities", Scope: model.ScopeRule}
					default:
						diagnostic = model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
							Message: "The regional Octavia provider does not advertise HTTPRoute ExternalAuth", Scope: model.ScopeRule}
					}
				}
			}
			checked[key] = diagnostic
		}
		auth.capability = diagnostic
		observations[filter] = auth
	}
}

func (inputs *routeModelInputs) validateExternalAuth(ctx context.Context, reader client.Reader, config controller.Config) error {
	current, err := inputs.observeExternalAuth(ctx, reader, config)
	if err == nil && !maps.EqualFunc(current, inputs.externalAuth, func(left, right routeExternalAuthObservation) bool {
		return left.diagnostic == right.diagnostic && equality.Semantic.DeepEqual(left.config, right.config)
	}) {
		return errHTTPRouteChanged
	}
	return err
}

func (inputs *routeModelInputs) externalAuthForRule(rule model.Rule) routeExternalAuthObservation {
	if rule.ExternalAuthDiagnostic != nil {
		return routeExternalAuthObservation{diagnostic: *rule.ExternalAuthDiagnostic}
	}
	if rule.ExternalAuth == nil {
		return routeExternalAuthObservation{}
	}
	auth, found := inputs.externalAuth[rule.ExternalAuth]
	if !found {
		return routeExternalAuthObservation{diagnostic: pendingExternalAuth("Waiting to read the ExternalAuth backend configuration")}
	}
	if auth.config != nil && auth.capability.Reason != "" {
		return routeExternalAuthObservation{diagnostic: auth.capability}
	}
	return auth
}

func (inputs *routeModelInputs) externalAuthRejections(route *gatewayv1.HTTPRoute, matches []routeLoweredMatch, rules []model.Rule) []string {
	if !inputs.input.CheckRouteExternalAuth {
		return nil
	}
	var dropped []string
	seen := make(map[int]bool)
	for _, match := range matches {
		index := match.RuleIndex
		if seen[index] {
			continue
		}
		seen[index] = true
		diagnostic := inputs.externalAuthForRule(rules[index]).diagnostic
		if diagnostic.Condition != string(gatewayv1.RouteConditionAccepted) {
			continue
		}
		original := inputs.input.OriginalRulePositions[route.UID]
		if index < len(original) {
			index = original[index]
		}
		dropped = append(dropped, droppedRule(route.Spec.Rules[index], index, diagnostic.Message))
	}
	return dropped
}
