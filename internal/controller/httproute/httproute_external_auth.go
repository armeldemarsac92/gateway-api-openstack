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
	"sync"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

type routeExternalAuthObservation struct {
	config     *cloud.ListenerExternalAuth
	diagnostic model.Diagnostic
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
	for _, route := range inputs.logical.Routes {
		for _, rule := range route.Rules {
			if rule.ExternalAuth == nil {
				continue
			}
			resolvedTopology, err := topology()
			if err != nil {
				return nil, err
			}
			auth, diagnostic, err := routebackend.ResolveHTTPExternalAuth(ctx, reader, model.HTTPRouteKind, route.Source.Namespace, *rule.ExternalAuth, resolvedTopology)
			var observation routeExternalAuthObservation
			switch {
			case ctx.Err() != nil:
				return nil, ctx.Err()
			case err != nil:
				observation.diagnostic = pendingExternalAuth("Waiting to read the ExternalAuth backend configuration")
			case diagnostic != nil:
				observation.diagnostic = *diagnostic
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

func (r *Reconciler) externalAuthCapability(ctx context.Context, observations map[*model.HTTPExternalAuth]routeExternalAuthObservation) model.Diagnostic {
	for _, auth := range observations {
		if auth.config == nil {
			continue
		}
		if r.RouteExternalAuthAvailable != nil {
			if available, err := r.RouteExternalAuthAvailable(ctx); err == nil {
				if available {
					return model.Diagnostic{}
				}
				return model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
					Message: "The regional Octavia provider does not advertise HTTPRoute ExternalAuth", Scope: model.ScopeRule}
			}
		}
		return pendingExternalAuth("Waiting to verify regional Octavia HTTPRoute ExternalAuth support")
	}
	return model.Diagnostic{}
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
	if auth.config != nil && inputs.externalAuthCapability.Reason != "" {
		return routeExternalAuthObservation{diagnostic: inputs.externalAuthCapability}
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
