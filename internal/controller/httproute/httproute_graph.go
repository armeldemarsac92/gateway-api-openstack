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
	"errors"
	"fmt"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type routeBindingState uint8

const (
	routeBindingUnknown routeBindingState = iota
	routeBindingGatewayPending
	routeBindingPersisted
	routeBindingReady
)

type routeGraphResult struct {
	bindingState              routeBindingState
	selection                 routeSelection
	rejectedSections          []model.ListenerSection
	backendObserved           bool
	usesXBackendClientTLS     bool
	persistenceRevision       *string
	xBackendTransportRevision *string
	backendDiagnostic         model.Diagnostic
	corsFallback              bool
	backendTLSAncestorSlot    backendtls.AncestorSlot
	retryBudgetAncestorFull   bool
	cloud.RouteResult
}

type routeBindingCheckpointError struct {
	err error
}

func (e *routeBindingCheckpointError) Error() string {
	return e.err.Error()
}

func (e *routeBindingCheckpointError) Unwrap() error {
	return e.err
}

func (r *Reconciler) ensureRouteGraph(
	ctx context.Context,
	expectedRoute *gatewayv1.HTTPRoute,
	expectedGateway *gatewayv1.Gateway,
) (routeGraphResult, error) {
	if r.APIReader == nil {
		return routeGraphResult{}, controller.ErrAPIReaderRequired
	}
	if r.Writer == nil {
		return routeGraphResult{}, fmt.Errorf("gateway graph writer is required")
	}
	prepared, err := r.prepareRouteGraphBinding(ctx, expectedRoute, expectedGateway)
	expectedIdentity := controller.RouteIdentity(r.Config, expectedGateway, expectedRoute)
	if err != nil && r.Config.XBackendEnabled() && controllerutil.ContainsFinalizer(expectedGateway, r.Config.BackendCAFinalizer()) {
		_, withdrawErr := r.Writer.ReconcileGraph(ctx, graph.Request{
			Identity: expectedIdentity, Action: graph.WithdrawXBackendClientCertificates, Gateway: expectedGateway,
		})
		if withdrawErr != nil {
			err = errors.Join(err, withdrawErr)
		}
	}
	if err != nil || prepared.bindingState != routeBindingReady {
		return prepared, err
	}
	request := graph.Request{
		Identity: expectedIdentity,
		Action:   graph.Ensure, Gateway: expectedGateway, Route: expectedRoute,
		HTTPRouteBackendDiagnostic:           &prepared.backendDiagnostic,
		HTTPRouteCORSFallback:                &prepared.corsFallback,
		HTTPRouteRejectedSections:            &prepared.rejectedSections,
		HTTPRouteRetryBudgetAncestorFull:     &prepared.retryBudgetAncestorFull,
		HTTPRouteUsesXBackendClientTLS:       &prepared.usesXBackendClientTLS,
		HTTPRouteXBackendPersistenceRevision: prepared.persistenceRevision,
		HTTPRouteXBackendTransportRevision:   prepared.xBackendTransportRevision,
	}
	if r.Config.BackendTLSPolicy {
		request.HTTPRouteBackendTLSAncestorSlot = &prepared.backendTLSAncestorSlot
	}
	result, err := r.Writer.ReconcileGraph(ctx, request)
	var tlsChange *graph.TLSAuthorizationChanged
	if err != nil {
		// A guarded listener can return an inventory error and still know
		// which route observations were deferred. Require an exact component
		// so the aggregate progress cannot certify an unobserved sibling.
		if !result.Gateway.HTTPPolicyTransition || !errors.Is(err, cloud.ErrDependencyNotReady) ||
			errors.As(err, &tlsChange) || !slices.ContainsFunc(result.Routes, func(route cloud.RouteGraphResult) bool {
			return route.Identity == expectedIdentity
		}) {
			return prepared, err
		}
	}
	component, err := routeComponentResult(result, expectedIdentity)
	prepared.RouteResult = component
	return prepared, err
}

func routeComponentResult(result cloud.GatewayGraphResult, expectedIdentity cloud.Identity) (cloud.RouteResult, error) {
	var component *cloud.RouteGraphResult
	for i := range result.Routes {
		route := &result.Routes[i]
		if route.Identity.RouteUID != expectedIdentity.RouteUID {
			continue
		}
		if route.Identity != expectedIdentity || component != nil {
			return cloud.RouteResult{}, fmt.Errorf("provider returned an inconsistent HTTPRoute component identity")
		}
		component = route
	}
	if component != nil {
		if err := component.Result.Outcome.Validate(); err != nil {
			return cloud.RouteResult{}, fmt.Errorf("validate HTTPRoute component outcome: %w", err)
		}
		if component.Result.ObservationDeferred && component.Result.Outcome.State != cloud.OutcomeProgressing {
			return cloud.RouteResult{}, fmt.Errorf("deferred HTTPRoute observation cannot certify completion")
		}
		return component.Result, nil
	}
	if result.Outcome.State == cloud.OutcomeProgressing {
		return cloud.RouteResult{Outcome: result.Outcome, ObservationDeferred: true}, nil
	}
	return cloud.RouteResult{}, fmt.Errorf("provider omitted the HTTPRoute component result")
}

// prepareRouteGraphBinding persists the binding before the writer can create resources.
func (r *Reconciler) prepareRouteGraphBinding(ctx context.Context, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) (routeGraphResult, error) {
	release, err := graph.Acquire(ctx, r.Coordinator, string(gateway.UID))
	if err != nil {
		return routeGraphResult{}, fmt.Errorf("acquire Gateway graph: %w", err)
	}
	defer release()
	snapshot, err := r.buildRouteValidationSnapshot(ctx, route, gateway, routeSnapshotForEnsure)
	prepared := routeGraphResult{
		selection:               snapshot.selection,
		rejectedSections:        snapshot.rejectedSections,
		backendObserved:         snapshot.backends != nil,
		backendDiagnostic:       snapshot.backendDiagnostic,
		corsFallback:            snapshot.corsFallback,
		backendTLSAncestorSlot:  snapshot.backendTLSAncestorSlot,
		retryBudgetAncestorFull: snapshot.retryBudgetAncestorFull,
	}
	if snapshot.backends != nil {
		prepared.usesXBackendClientTLS = snapshot.backends.routeUsesXBackendClientTLS(route)
		if r.Config.XBackendEnabled() && model.HTTPProfileRouteKind(route) == model.HTTPRouteKind && len(model.LocalXBackendReferences(route)) != 0 {
			revision := xBackendTransportRevision(route, snapshot.backends.input.XBackends)
			prepared.xBackendTransportRevision = &revision
			if r.Config.XBackendSessionPersistence {
				revision := xBackendPersistenceRevision(route, snapshot.backends.input.XBackends)
				prepared.persistenceRevision = &revision
			}
		}
	}
	if err != nil {
		return prepared, err
	}
	switch snapshot.disposition {
	case routeValidationGatewayPending:
		prepared.bindingState = routeBindingGatewayPending
	case routeValidationBindingRequired:
		if _, err := r.bindRouteFromSnapshot(ctx, route, gateway, snapshot); err != nil {
			return routeGraphResult{}, &routeBindingCheckpointError{err: err}
		}
		prepared.bindingState = routeBindingPersisted
	case routeValidationReady:
		prepared.bindingState = routeBindingReady
	default:
		return routeGraphResult{}, fmt.Errorf("invalid HTTPRoute validation disposition %d", snapshot.disposition)
	}
	return prepared, nil
}
