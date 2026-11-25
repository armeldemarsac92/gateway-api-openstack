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

package graph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// Action identifies the durable lifecycle intent that the sources must verify.
type Action string

const (
	// Ensure reconciles the complete desired Gateway graph.
	Ensure Action = "Ensure"
	// DeleteGateway removes a graph after validating Gateway cleanup authority.
	DeleteGateway Action = "DeleteGateway"
	// DeleteRoute removes a durably detaching route and preserves its Gateway.
	DeleteRoute Action = "DeleteRoute"
	// WithdrawTLS disables owned TLS frontends without granting graph mutation authority.
	WithdrawTLS Action = "WithdrawTLS"
	// WithdrawBackendClientCertificate closes owned pools with stale identities.
	WithdrawBackendClientCertificate Action = "WithdrawBackendClientCertificate"
)

// Request carries the identity and Kubernetes revision that triggered a write.
// It is not mutation authority until the sources validate fresh objects.
type Request struct {
	Identity cloud.Identity
	Action   Action
	Gateway  *gatewayv1.Gateway
	// Route holds a native HTTPRoute or an in-memory GRPCRoute projection.
	Route    *gatewayv1.HTTPRoute
	TCPRoute *gatewayv1.TCPRoute
	UDPRoute *gatewayv1.UDPRoute
	// ForwardingTarget ties a TCPRoute-triggered status update to the exact
	// backend and listener election rebuilt by the writer.
	ForwardingTarget string
	// HTTPRouteBackendDiagnostic ties ResolvedRefs to the backend observation
	// that the graph writer uses for the requesting HTTPRoute.
	HTTPRouteBackendDiagnostic *model.Diagnostic
	// HTTPRouteCORSFallback binds partial rule status to the same observation.
	HTTPRouteCORSFallback *bool
	// HTTPRouteBackendTLSAncestorSlot binds route status to the writer's slot observation.
	HTTPRouteBackendTLSAncestorSlot *backendtls.AncestorSlot
	// HTTPRouteRetryBudgetAncestorFull binds its status to the live capacity check.
	HTTPRouteRetryBudgetAncestorFull *bool
	// HTTPRouteRejectedSections ties per-parent status to the graph's profile election.
	HTTPRouteRejectedSections *[]model.ListenerSection
}

// Reconciler is the sole Kubernetes-facing graph mutation boundary.
type Reconciler interface {
	ReconcileGraph(context.Context, Request) (cloud.GatewayGraphResult, error)
}

// GatewayInput contains one request-local observation and its mutation fence.
type GatewayInput struct {
	Object   *gatewayv1.Gateway
	Node     cloud.GatewayGraphNode
	Validate func(context.Context) error
	// ReportRoutes publishes verified status for the Gateway-owned TLSRoute set.
	ReportRoutes func(context.Context, cloud.GatewayGraphResult, error) error
	// ObserveTLS rebuilds withdrawal authority even if ordinary graph input is incomplete.
	ObserveTLS func(context.Context) (GatewayInput, error)
}

// TLSAuthorizationChanged identifies ports whose observed authorization became stale.
type TLSAuthorizationChanged struct {
	Ports  []int
	scoped []cloud.ScopedListener
}

func (*TLSAuthorizationChanged) Error() string {
	return "HTTPS listener authorization changed during graph reconciliation"
}

// RouteInputs contains the complete route set and its mutation fence.
type RouteInputs struct {
	Nodes    []cloud.RouteGraphNode
	Validate func(context.Context) error
}

// GatewaySource observes and validates the Gateway portion of a graph.
type GatewaySource interface {
	ObserveGatewayGraph(context.Context, Request) (GatewayInput, error)
}

// RouteSource observes a complete live set of bound route fragments.
type RouteSource interface {
	ObserveRouteGraph(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error)
}

// RouteSources combines all served route kinds under the same Gateway writer lock.
type RouteSources []RouteSource

// ObserveRouteGraph collects ownership even for a kind no longer served by the listener.
// Every source validates its observation before the provider can mutate the graph.
func (sources RouteSources) ObserveRouteGraph(ctx context.Context, request Request, gateway *gatewayv1.Gateway) (RouteInputs, error) {
	var combined RouteInputs
	var fences []func(context.Context) error
	for _, source := range sources {
		input, err := source.ObserveRouteGraph(ctx, request, gateway)
		if err != nil {
			return RouteInputs{}, err
		}
		combined.Nodes = append(combined.Nodes, input.Nodes...)
		if input.Validate != nil {
			fences = append(fences, input.Validate)
		}
	}
	combined.Validate = func(ctx context.Context) error {
		for _, validate := range fences {
			if err := validate(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	return combined, nil
}

// Writer serializes fresh graph construction and provider operations.
// All fields are immutable process dependencies.
type Writer struct {
	Coordinator *Coordinator
	Gateway     GatewaySource
	Routes      RouteSource
	Provider    cloud.GraphProvider
	Observer    StageObserver
}

// ReconcileGraph rebuilds the graph under its UID lock on every call.
func (w *Writer) ReconcileGraph(ctx context.Context, request Request) (cloud.GatewayGraphResult, error) {
	if w == nil || w.Gateway == nil || w.Routes == nil || w.Provider == nil {
		return cloud.GatewayGraphResult{}, errors.New("gateway graph writer dependencies are required")
	}
	if err := cloud.ValidateGatewayIdentity(request.Identity); err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	started := time.Now()
	release, err := Acquire(ctx, w.Coordinator, request.Identity.GatewayUID)
	w.observeStage(StageLockWait, started, err)
	if err != nil {
		return cloud.GatewayGraphResult{}, fmt.Errorf("acquire Gateway graph: %w", err)
	}
	defer release()
	started = time.Now()
	gateway, err := w.Gateway.ObserveGatewayGraph(ctx, request)
	w.observeStage(StageGatewayInput, started, err)
	if err != nil {
		return w.finishFailedEnsure(ctx, request, gateway, cloud.GatewayGraphResult{}, err)
	}
	if request.Action == WithdrawTLS {
		return w.withdrawTLS(ctx, request.Identity, gateway, nil)
	}
	if request.Action == WithdrawBackendClientCertificate {
		return w.withdrawBackendClientCertificate(ctx, request.Identity, gateway)
	}
	started = time.Now()
	var routes RouteInputs
	if !gateway.Node.AddressWithdrawalOnly() {
		routes, err = w.Routes.ObserveRouteGraph(ctx, request, gateway.Object)
	}
	w.observeStage(StageRouteInput, started, err)
	if err != nil {
		return w.finishFailedEnsure(ctx, request, gateway, cloud.GatewayGraphResult{}, err)
	}
	identity := request.Identity
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	spec := cloud.GatewayGraphSpec{Identity: identity, Gateway: gateway.Node, Routes: routes.Nodes}
	gateway, routes, spec.HTTPSPolicyAuthority = w.observeHTTPSCleanupAuthority(ctx, request, identity, gateway, routes)
	started = time.Now()
	err = validateInputs(ctx, gateway, routes)
	w.observeStage(StageValidation, started, err)
	if err != nil {
		return w.finishFailedEnsure(ctx, request, gateway, cloud.GatewayGraphResult{}, err)
	}
	started = time.Now()
	if gateway.Validate != nil || routes.Validate != nil || gateway.ObserveTLS != nil {
		spec.ValidateMutation = func(ctx context.Context) error { return validateInputs(ctx, gateway, routes) }
	}
	result, err := w.Provider.ReconcileGatewayGraph(ctx, spec)
	if err == nil {
		if outcomeErr := result.Outcome.Validate(); outcomeErr != nil {
			err = fmt.Errorf("validate Gateway graph outcome: %w", outcomeErr)
		}
	}
	w.observeStage(StageProvider, started, err)
	if gateway.ReportRoutes != nil {
		err = errors.Join(err, gateway.ReportRoutes(ctx, result, err))
	}
	if err != nil {
		return w.finishFailedEnsure(ctx, request, gateway, result, err)
	}
	return w.finishTLS(ctx, request.Identity, gateway, result, err)
}

// A route can still be detaching when its section is reauthorized. An
// ownership-only cleanup cannot prove that the other section's policies
// belong on the shared bind. Use the complete, fenced graph when available.
func (w *Writer) observeHTTPSCleanupAuthority(ctx context.Context, request Request, identity cloud.Identity, gateway GatewayInput, routes RouteInputs) (GatewayInput, RouteInputs, *cloud.GatewayGraphSpec) {
	if request.Action != DeleteRoute || request.Route == nil || gateway.Object == nil ||
		gateway.Node.Disposition != cloud.GraphPreserve || gateway.Node.Spec.ListenerProtocol != cloud.ListenerProtocolHTTP ||
		gateway.Node.Spec.ScopedListeners == nil {
		return gateway, routes, nil
	}
	ensure := Request{Action: Ensure, Identity: identity, Gateway: gateway.Object}
	live, err := w.Gateway.ObserveGatewayGraph(ctx, ensure)
	if err != nil || live.Object == nil || live.Node.Disposition != cloud.GraphEnsure ||
		!slices.ContainsFunc(live.Node.Spec.ScopedListeners, authorizedHTTPSScopes) {
		return gateway, routes, nil
	}
	current, err := w.Routes.ObserveRouteGraph(ctx, ensure, live.Object)
	if err != nil || !slices.ContainsFunc(current.Nodes, func(node cloud.RouteGraphNode) bool {
		return node.Disposition == cloud.GraphDelete && node.Spec.Identity == request.Identity
	}) {
		return gateway, routes, nil
	}
	validate := routes.Validate
	routes.Validate = func(ctx context.Context) error {
		for _, fence := range []func(context.Context) error{gateway.Validate, validate, current.Validate} {
			if fence != nil {
				if err := fence(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return live, routes, &cloud.GatewayGraphSpec{Identity: identity, Gateway: live.Node, Routes: current.Nodes}
}

func (w *Writer) finishFailedEnsure(ctx context.Context, request Request, gateway GatewayInput, result cloud.GatewayGraphResult, graphErr error) (cloud.GatewayGraphResult, error) {
	var backendErr error
	if request.Action == Ensure && gateway.Node.BackendCAOwned &&
		cloud.NormalizeListenerProtocol(gateway.Node.Spec.ListenerProtocol) == cloud.ListenerProtocolHTTP {
		request.Action = WithdrawBackendClientCertificate
		request.Gateway = gateway.Object
		input, err := w.Gateway.ObserveGatewayGraph(ctx, request)
		if err == nil {
			_, err = w.withdrawBackendClientCertificate(ctx, request.Identity, input)
		}
		backendErr = err
	}
	if gateway.Object == nil {
		gateway.Object = request.Gateway
	}
	result, err := w.finishTLS(ctx, request.Identity, gateway, result, graphErr)
	if backendErr != nil {
		err = errors.Join(err, backendErr)
	}
	return result, err
}

func (w *Writer) withdrawBackendClientCertificate(ctx context.Context, identity cloud.Identity, input GatewayInput) (cloud.GatewayGraphResult, error) {
	if input.Node.Disposition != cloud.GraphWithdrawBackendClientCertificate || input.Validate == nil {
		return cloud.GatewayGraphResult{}, errors.New("invalid backend client certificate withdrawal authority")
	}
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	result, err := w.Provider.ReconcileGatewayGraph(ctx, cloud.GatewayGraphSpec{
		Identity: identity, Gateway: input.Node, ValidateMutation: input.Validate,
	})
	if err == nil {
		err = result.Outcome.Validate()
	}
	return result, err
}
func (w *Writer) finishTLS(ctx context.Context, identity cloud.Identity, gateway GatewayInput, result cloud.GatewayGraphResult, graphErr error) (cloud.GatewayGraphResult, error) {
	if gateway.ObserveTLS == nil || (graphErr == nil && !hasTLSListeners(gateway)) {
		return result, graphErr
	}
	input, err := gateway.ObserveTLS(ctx)
	if err != nil {
		return result, errors.Join(graphErr, err)
	}
	changed := changedTLSListeners(gateway, input)
	if graphErr == nil && len(changed) == 0 {
		return result, nil
	}
	// Keep a change detected by the provider's mutation fence even if the
	// certificate is authorized again by the time we observe withdrawal.
	var previous *TLSAuthorizationChanged
	if errors.As(graphErr, &previous) {
		changed = append(changed, previous.Ports...)
		slices.Sort(changed)
		changed = slices.Compact(changed)
		// Keep unrelated failures when the error has multiple causes.
		for cause := graphErr; cause != nil; cause = errors.Unwrap(cause) {
			if cause == previous {
				graphErr = nil
				break
			}
		}
	}
	if (graphErr == nil || previous != nil) && len(changed) != 0 && input.Node.Spec.ListenerProtocol == cloud.ListenerProtocolHTTP {
		// A request with a known authorization change does not imply that
		// route policy ownership on unrelated HTTPS ports became unreadable.
		input.Node.TLSWithdrawalPorts = changed
		if gateway.Node.Disposition == cloud.GraphEnsure {
			input.Node.TLSWithdrawalPreviousHosts = previousHTTPSHosts(gateway, input, changed, previous)
		}
	}
	tlsOnly := input
	var authority *cloud.GatewayGraphSpec
	if len(input.Node.TLSWithdrawalPreviousHosts) != 0 || len(input.Node.TLSWithdrawalPorts) == 0 && graphErr != nil {
		input, authority = w.observeHTTPSWithdrawalAuthority(ctx, identity, gateway.Object, input)
	}
	if authority == nil {
		input.Node.TLSWithdrawalPreviousHosts = nil
	}
	withdrawal, err := w.withdrawTLS(ctx, identity, input, authority)
	if err != nil && authority != nil {
		// A stale route fence cannot authorize preserving the remaining
		// traffic. Refresh TLS independently before attempting strict closure.
		strict, readErr := gateway.ObserveTLS(ctx)
		if readErr != nil {
			strict = tlsOnly
		}
		strict.Node.TLSWithdrawalPorts = slices.Clone(tlsOnly.Node.TLSWithdrawalPorts)
		if len(strict.Node.TLSWithdrawalPorts) != 0 {
			strict.Node.TLSWithdrawalPorts = append(strict.Node.TLSWithdrawalPorts, changedTLSListeners(input, strict)...)
			slices.Sort(strict.Node.TLSWithdrawalPorts)
			strict.Node.TLSWithdrawalPorts = slices.Compact(strict.Node.TLSWithdrawalPorts)
		}
		strict.Node.TLSWithdrawalPreviousHosts = nil
		var fallbackErr error
		withdrawal, fallbackErr = w.withdrawTLS(ctx, identity, strict, nil)
		err = errors.Join(err, readErr, fallbackErr)
	}
	if authority == nil || err != nil {
		result.Gateway.ScopedListenerServingPorts = nil
	} else {
		result.Gateway.ScopedListenerServingPorts = withdrawal.Gateway.ScopedListenerServingPorts
	}
	if err == nil && withdrawal.Gateway.ScopedListenerDeferredPorts != nil {
		ports := withdrawal.Gateway.ScopedListenerDeferredPorts
		if result.Gateway.HTTPPolicyTransition {
			// The TLS observation can further restrict which ports retain
			// status. It cannot certify a frontend guarded by the HTTP pass.
			ports = slices.DeleteFunc(slices.Clone(ports), func(port int) bool {
				return !slices.Contains(result.Gateway.ScopedListenerDeferredPorts, port)
			})
		}
		result.Gateway.ScopedListenerDeferredPorts = ports
	}
	if len(changed) != 0 {
		result.Gateway.ScopedListenerReadyPorts = slices.DeleteFunc(result.Gateway.ScopedListenerReadyPorts, func(port int) bool {
			return slices.Contains(changed, port)
		})
		if withdrawal.Gateway.ScopedListenerDeferredPorts == nil || err != nil {
			result.Gateway.ScopedListenerDeferredPorts = slices.DeleteFunc(result.Gateway.ScopedListenerDeferredPorts, func(port int) bool {
				return slices.Contains(changed, port)
			})
		}
		change := &TLSAuthorizationChanged{Ports: changed}
		if graphErr == nil && err == nil {
			return result, change
		}
		return result, errors.Join(graphErr, change, err)
	}
	if err == nil {
		return result, graphErr
	}
	return result, errors.Join(graphErr, err)
}

func hasTLSListeners(input GatewayInput) bool {
	return cloud.IsTCPTLSListenerProtocol(input.Node.Spec.ListenerProtocol) || slices.ContainsFunc(input.Node.Spec.ScopedListeners, func(listener cloud.ScopedListener) bool {
		return listener.TLS != nil
	})
}

func changedTLSListeners(observed, current GatewayInput) []int {
	if cloud.IsTCPTLSListenerProtocol(observed.Node.Spec.ListenerProtocol) {
		if observed.Node.TCPTLSCertificateRef != current.Node.TCPTLSCertificateRef {
			return []int{observed.Node.Spec.ListenerPort}
		}
		return nil
	}
	listeners := make(map[int]cloud.ListenerTLS, len(current.Node.Spec.ScopedListeners))
	for _, listener := range current.Node.Spec.ScopedListeners {
		if listener.TLS != nil {
			listeners[listener.Port] = *listener.TLS
		}
	}
	var ports []int
	for _, listener := range observed.Node.Spec.ScopedListeners {
		current := listeners[listener.Port]
		if listener.TLS != nil && !listener.TLS.Equal(&current) {
			ports = append(ports, listener.Port)
		}
	}
	return ports
}

func (w *Writer) withdrawTLS(ctx context.Context, identity cloud.Identity, input GatewayInput, authority *cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	if input.Node.Disposition != cloud.GraphWithdrawTLS || (input.Node.Spec.ScopedListeners == nil && !cloud.IsTCPTLSListenerProtocol(input.Node.Spec.ListenerProtocol)) || input.Validate == nil {
		return cloud.GatewayGraphResult{}, errors.New("invalid TLS-only withdrawal authority")
	}
	if err := input.Validate(ctx); err != nil {
		return cloud.GatewayGraphResult{}, err
	}
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	spec := cloud.GatewayGraphSpec{Identity: identity, Gateway: input.Node, ValidateMutation: input.Validate, HTTPSPolicyAuthority: authority}
	result, err := w.Provider.ReconcileGatewayGraph(ctx, spec)
	if err == nil {
		err = result.Outcome.Validate()
	}
	return result, err
}

// A normal reconcile can lose its snapshot while a separate writer is
// already converging a section withdrawal. Rebuild the current bindings
// without the stale route request before treating them as unreadable.
func (w *Writer) observeHTTPSWithdrawalAuthority(ctx context.Context, identity cloud.Identity, gateway *gatewayv1.Gateway, input GatewayInput) (GatewayInput, *cloud.GatewayGraphSpec) {
	if gateway == nil || input.Validate == nil || input.Node.Spec.ListenerProtocol != cloud.ListenerProtocolHTTP ||
		!slices.ContainsFunc(input.Node.Spec.ScopedListeners, authorizedHTTPSScopes) {
		return input, nil
	}
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	request := Request{Action: Ensure, Identity: identity, Gateway: gateway}
	live, err := w.Gateway.ObserveGatewayGraph(ctx, request)
	if err != nil || live.Object == nil || live.Node.Disposition != cloud.GraphEnsure ||
		len(changedTLSListeners(live, input)) != 0 || len(changedTLSListeners(input, live)) != 0 {
		return input, nil
	}
	routes, err := w.Routes.ObserveRouteGraph(ctx, request, live.Object)
	if err != nil || validateInputs(ctx, live, routes) != nil {
		return input, nil
	}
	validate := input.Validate
	input.Validate = func(ctx context.Context) error {
		if err := validateInputs(ctx, live, routes); err != nil {
			return err
		}
		return validate(ctx)
	}
	return input, &cloud.GatewayGraphSpec{Identity: identity, Gateway: live.Node, Routes: routes.Nodes}
}

func authorizedHTTPSScopes(listener cloud.ScopedListener) bool {
	return listener.TLS != nil && listener.TLS.CertificateRef != "" && len(listener.TLS.VirtualHosts) != 0
}

func previousHTTPSHosts(observed, current GatewayInput, changed []int, interrupted *TLSAuthorizationChanged) map[int][]string {
	tlsAt := func(listeners []cloud.ScopedListener, port int) *cloud.ListenerTLS {
		for _, listener := range listeners {
			if listener.Port == port {
				return listener.TLS
			}
		}
		return nil
	}
	var previous map[int][]string
	for _, port := range changed {
		before := tlsAt(observed.Node.Spec.ScopedListeners, port)
		after := tlsAt(current.Node.Spec.ScopedListeners, port)
		if before == nil || after == nil || len(before.VirtualHosts) == 0 || len(after.VirtualHosts) == 0 ||
			slices.Equal(before.VirtualHosts, after.VirtualHosts) || before.CertificateRef == "" ||
			before.CertificateRef != after.CertificateRef || !before.ClientValidation.Equal(after.ClientValidation) {
			continue
		}
		if interrupted != nil && slices.Contains(interrupted.Ports, port) && !after.Equal(tlsAt(interrupted.scoped, port)) {
			continue
		}
		if previous == nil {
			previous = make(map[int][]string)
		}
		previous[port] = before.VirtualHosts
	}
	return previous
}

func validateInputs(ctx context.Context, gateway GatewayInput, routes RouteInputs) error {
	for _, validate := range []func(context.Context) error{routes.Validate, gateway.Validate} {
		if validate == nil {
			continue
		}
		if err := validate(ctx); err != nil {
			// A grant change can invalidate the route or Gateway projection
			// before the final TLS fence is reached.
			var changed *TLSAuthorizationChanged
			if tlsErr := validateTLSInputs(ctx, gateway); errors.As(tlsErr, &changed) {
				return errors.Join(err, tlsErr)
			}
			return err
		}
	}
	return validateTLSInputs(ctx, gateway)
}

func validateTLSInputs(ctx context.Context, gateway GatewayInput) error {
	if gateway.ObserveTLS == nil || !hasTLSListeners(gateway) {
		return nil
	}
	current, err := gateway.ObserveTLS(ctx)
	if err != nil {
		return err
	}
	if ports := changedTLSListeners(gateway, current); len(ports) != 0 {
		return &TLSAuthorizationChanged{Ports: ports, scoped: current.Node.Spec.ScopedListeners}
	}
	return nil
}

func (w *Writer) observeStage(stage Stage, started time.Time, err error) {
	if w.Observer != nil {
		w.Observer.ObserveGraphStage(stage, time.Since(started), err != nil)
	}
}
