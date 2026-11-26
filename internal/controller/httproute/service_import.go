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
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func serviceImportTransportIssue(backend model.BackendGroup, mode cloud.MemberMode, h2cIssue model.Diagnostic) model.Diagnostic {
	if mode != "" && mode != cloud.MemberModeEndpoint {
		return model.Diagnostic{Reason: string(gatewayv1.RouteReasonUnsupportedValue), Message: "ServiceImport backends require IPv4 Endpoint member mode"}
	}
	if backend.UsesH2C() {
		return h2cIssue
	}
	return model.Diagnostic{}
}

func (r *Reconciler) serviceImportReferenceStatus(ctx context.Context, backend model.BackendGroup, gateway *gatewayv1.Gateway, h2cIssue func() model.Diagnostic) (model.Diagnostic, error) {
	if backend.Diagnostic != nil {
		return *backend.Diagnostic, nil
	}
	var mode cloud.MemberMode
	if controller.HasGatewayTopologyBinding(r.Config, gateway) {
		topology, err := controller.StoredGatewayTopology(r.Config, gateway)
		if err != nil {
			return model.Diagnostic{}, err
		}
		mode = topology.Topology.MemberMode
	}
	var issue model.Diagnostic
	if backend.UsesH2C() {
		issue = h2cIssue()
	}
	return serviceImportTransportIssue(backend, mode, issue), nil
}

// routeServiceImportSnapshot retains the admitted primary imports of one route
// for the final status check. Core Services and peer routes need no extra reads.
type routeServiceImportSnapshot struct {
	route    gatewayv1.HTTPRoute
	observed []model.ServiceImportObservation
}

func (r *Reconciler) snapshotServiceImports(route *gatewayv1.HTTPRoute, input model.Input) *routeServiceImportSnapshot {
	if !input.EnableServiceImport {
		return nil
	}
	kind := model.HTTPProfileRouteKind(route)
	rules, _ := r.projectedHTTPFilterRules(route)
	filtered := *route
	filtered.Spec.Rules = nil
	authorized := make(map[types.NamespacedName]bool)
	for _, rule := range rules {
		var imports gatewayv1.HTTPRouteRule
		for _, ref := range rule.BackendRefs {
			if key, imported := model.ServiceImportReference(route.Namespace, ref.BackendObjectReference); imported {
				imports.BackendRefs = append(imports.BackendRefs, ref)
				authorized[key] = model.ServiceImportReferenceAllowed(kind, route.Namespace, key, input.ReferenceGrants)
			}
		}
		if len(imports.BackendRefs) != 0 {
			filtered.Spec.Rules = append(filtered.Spec.Rules, imports)
		}
	}
	if len(filtered.Spec.Rules) == 0 {
		return nil
	}
	snapshot := &routeServiceImportSnapshot{route: filtered}
	for _, observation := range input.ServiceImports {
		if authorized[observation.Key] {
			snapshot.observed = append(snapshot.observed, observation)
		}
	}
	return snapshot
}

func (s *routeServiceImportSnapshot) retryRead() bool {
	return s != nil && slices.ContainsFunc(s.observed, func(observation model.ServiceImportObservation) bool {
		return observation.RetryRead
	})
}

func (s *routeServiceImportSnapshot) validate(ctx context.Context, reader client.Reader) error {
	current := model.Input{EnableServiceImport: true, Routes: []gatewayv1.HTTPRoute{s.route}}
	if err := graph.CollectBackendReferenceInputs(ctx, reader, &current); err != nil {
		return err
	}
	if !equality.Semantic.DeepEqual(s.observed, current.ServiceImports) {
		return errHTTPRouteChanged
	}
	return nil
}

func (r *Reconciler) enqueueRoutesForServiceImport(ctx context.Context, object client.Object) []reconcile.Request {
	return r.enqueueRoutesForServiceImportKey(ctx, client.ObjectKeyFromObject(object))
}

func (r *Reconciler) enqueueRoutesForServiceImportKey(ctx context.Context, key types.NamespacedName) []reconcile.Request {
	value := controller.ServiceImportIndexAll
	if key.Name != "" {
		value = controller.ObjectKeyString(key)
	}
	routes, err := r.listRoutes(ctx, client.MatchingFields{controller.IndexHTTPRouteByServiceImport: value})
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list routes for ServiceImport", "serviceImport", key)
		return nil
	}
	return r.enqueueHTTPRouteConsumers(ctx, routes)
}
