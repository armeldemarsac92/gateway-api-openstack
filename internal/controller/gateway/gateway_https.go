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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

const httpsPollInterval = 5 * time.Second

// CertificateResolver resolves a listener's exact live authorization and returns
// a provider reference or a listener condition reason without certificate material.
type CertificateResolver interface {
	Resolve(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason)
}

func hasHTTPS(gateway *gatewayv1.Gateway) bool {
	return slices.ContainsFunc(gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
		return listener.Protocol == gatewayv1.HTTPSProtocolType
	})
}

func usesHTTPSProfile(config controller.Config, gateway *gatewayv1.Gateway) bool {
	return hasHTTPS(gateway) && (config.OperatorHTTPS || controller.GatewayListenerScope(config, gateway) == gatewayv1.HTTPProtocolType)
}

type httpsListenerIssue struct {
	condition gatewayv1.ListenerConditionType
	reason    gatewayv1.ListenerConditionReason
	message   string
}

func httpsProfileListeners(config controller.Config, gateway *gatewayv1.Gateway) []*gatewayv1.Listener {
	var listeners []*gatewayv1.Listener
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		validPort := listener.Port > 0 && listener.Port <= 65535
		// Include invalid HTTPS frontends at real ports as withdrawal intents.
		if (listener.Protocol == gatewayv1.HTTPSProtocolType && validPort) ||
			(listener.Protocol == gatewayv1.HTTPProtocolType && httpsProfileIssue(config, gateway, listener) == nil) {
			listeners = append(listeners, listener)
		}
	}
	slices.SortStableFunc(listeners, func(a, b *gatewayv1.Listener) int { return cmp.Compare(a.Port, b.Port) })
	return listeners
}

func httpsProfileIssue(config controller.Config, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) *httpsListenerIssue {
	invalid := func(reason gatewayv1.ListenerConditionReason, message string) *httpsListenerIssue {
		return &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, reason, message}
	}
	refError := func(message string) *httpsListenerIssue {
		return &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonInvalidCertificateRef, message}
	}
	if listener.Protocol == gatewayv1.HTTPSProtocolType && !config.OperatorHTTPS {
		return invalid(gatewayv1.ListenerReasonUnsupportedProtocol, "Operator-provisioned HTTPS is disabled")
	}
	if listener.Port < 1 || listener.Port > 65535 {
		return invalid(gatewayv1.ListenerReasonPortUnavailable, "Listener port must be between 1 and 65535")
	}
	for i := range gateway.Spec.Listeners {
		peer := &gateway.Spec.Listeners[i]
		if peer == listener {
			continue
		}
		if peer.Port != listener.Port {
			continue
		}
		if listener.Protocol == gatewayv1.HTTPSProtocolType || peer.Protocol == gatewayv1.HTTPSProtocolType {
			return &httpsListenerIssue{gatewayv1.ListenerConditionConflicted, gatewayv1.ListenerReasonProtocolConflict, "HTTPS requires a dedicated listener port"}
		}
		if peer.Protocol != gatewayv1.HTTPProtocolType || listener.Hostname == nil || peer.Hostname == nil ||
			strings.HasPrefix(string(*listener.Hostname), "*.") || strings.HasPrefix(string(*peer.Hostname), "*.") ||
			*peer.Hostname == *listener.Hostname {
			return &httpsListenerIssue{gatewayv1.ListenerConditionConflicted, gatewayv1.ListenerReasonHostnameConflict, "Same-port HTTP sections require distinct exact hostnames"}
		}
	}
	if listener.Hostname != nil && amphora.ValidateHostname(string(*listener.Hostname)) != nil {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, "Listener hostname is unsupported")
	}
	if listener.Filters != nil && len(listener.Filters.Requests) != 0 {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, "Listener request filters are unsupported")
	}
	if listener.AllowedRoutes != nil && slices.ContainsFunc(listener.AllowedRoutes.Kinds, func(ref gatewayv1.RouteGroupKind) bool { return !isRouteGroupKind(ref, "HTTPRoute") }) {
		return &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonInvalidRouteKinds, "Only HTTPRoute is supported by this listener"}
	}
	tls := listener.TLS
	if listener.Protocol == gatewayv1.HTTPProtocolType {
		if tls != nil {
			return invalid(gatewayv1.ListenerReasonUnsupportedValue, "TLS configuration is unsupported on an HTTP listener")
		}
		return nil
	}
	if tls == nil {
		return refError("HTTPS requires one core Secret certificate reference")
	}
	if tls.Mode != nil && *tls.Mode != gatewayv1.TLSModeTerminate || len(tls.Options) != 0 {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, "HTTPS supports Terminate mode without TLS options")
	}
	if len(tls.CertificateRefs) != 1 {
		return refError("HTTPS requires one core Secret certificate reference")
	}
	ref := tls.CertificateRefs[0]
	if ref.Name == "" || ref.Group != nil && *ref.Group != "" || ref.Kind != nil && *ref.Kind != "Secret" {
		return refError("HTTPS requires one core Secret certificate reference")
	}
	return nil
}

// HTTPRouteListeners selects sections that may attach HTTPRoutes. Certificate
// authorization controls frontend programming independently of route attachment.
func HTTPRouteListeners(config controller.Config, gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, error) {
	if !usesHTTPSProfile(config, gateway) {
		return HTTPListeners(gateway)
	}
	listeners, err := SupportedListenersForConfig(config, gateway)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(listeners, func(listener *gatewayv1.Listener) bool {
		return httpsProfileIssue(config, gateway, listener) != nil
	}), nil
}

// ValidatedHTTPRouteListeners verifies the Gateway profile before selecting
// its route-attachment sections.
func ValidatedHTTPRouteListeners(config controller.Config, gateway *gatewayv1.Gateway) ([]*gatewayv1.Listener, error) {
	if !usesHTTPSProfile(config, gateway) {
		return SupportedListeners(gateway)
	}
	return HTTPRouteListeners(config, gateway)
}

func listenerFamily(listener *gatewayv1.Listener) gatewayv1.ProtocolType {
	if listener.Protocol == gatewayv1.HTTPSProtocolType {
		return gatewayv1.HTTPProtocolType
	}
	return listener.Protocol
}

// RequiresHTTPListenerScope identifies HTTP-family configurations that require
// the frozen per-port ownership checkpoint before any route can program.
func RequiresHTTPListenerScope(config controller.Config, gateway *gatewayv1.Gateway) bool {
	return usesHTTPSProfile(config, gateway) || HasMultipleHTTPListenerPorts(gateway)
}

type tlsResolution struct {
	ref   string
	issue *httpsListenerIssue
}

func (r *Reconciler) resolveTLS(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) tlsResolution {
	if issue := httpsProfileIssue(r.Config, gateway, listener); issue != nil {
		return tlsResolution{issue: issue}
	}
	var ref string
	var reason gatewayv1.ListenerConditionReason
	if r.Certificates != nil {
		ref, reason = r.Certificates.Resolve(ctx, gateway, listener)
	}
	if ref != "" && reason == "" {
		return tlsResolution{ref: ref}
	}
	message := "No approved certificate is available for this listener"
	if reason == gatewayv1.ListenerReasonRefNotPermitted {
		message = "Certificate reference is not permitted by a ReferenceGrant"
	} else {
		reason = gatewayv1.ListenerReasonInvalidCertificateRef
	}
	return tlsResolution{issue: &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, reason, message}}
}

func (r *Reconciler) addTLSGraphInput(ctx context.Context, snapshot *gatewayMutationSnapshot, input *graph.GatewayInput) {
	resolutions := make(map[int]tlsResolution)
	for _, listener := range snapshot.listeners {
		if listener.Protocol == gatewayv1.HTTPSProtocolType {
			resolutions[int(listener.Port)] = r.resolveTLS(ctx, snapshot.gateway, listener)
		}
	}
	for i := range input.Node.Spec.ScopedListeners {
		listener := &input.Node.Spec.ScopedListeners[i]
		if resolution, https := resolutions[listener.Port]; https {
			listener.TLS = &cloud.ListenerTLS{CertificateRef: resolution.ref}
		}
	}
	input.TLS = &graph.TLSInput{
		Check: func(ctx context.Context) (changed, denied []int) {
			// Status must not attest a frontend from an obsolete Gateway
			// revision or from a class/bundle that can no longer authorize it.
			if err := r.sealGatewayMutationSnapshot(ctx, snapshot); err != nil {
				for port := range resolutions {
					changed = append(changed, port)
				}
				slices.Sort(changed)
				return changed, changed
			}
			seen := make(map[int]bool, len(resolutions))
			for _, listener := range snapshot.listeners {
				port := int(listener.Port)
				if listener.Protocol != gatewayv1.HTTPSProtocolType || seen[port] {
					continue
				}
				seen[port] = true
				current := r.resolveTLS(ctx, snapshot.gateway, listener)
				if current.ref != resolutions[port].ref {
					changed = append(changed, port)
				}
				if current.ref == "" {
					denied = append(denied, port)
				}
			}
			return changed, denied
		},
		Withdraw: func(ctx context.Context, ports []int, denied func(context.Context) []int) (graph.GatewayInput, error) {
			return r.tlsWithdrawalInput(ctx, snapshot.gateway, input.Node.Spec.Identity, ports, func(ctx context.Context) error {
				current := denied(ctx)
				if !slices.ContainsFunc(ports, func(port int) bool { return !slices.Contains(current, port) }) {
					return nil
				}
				return errGatewayChanged
			})
		},
	}
}

func (r *Reconciler) tlsWithdrawalInput(ctx context.Context, expected *gatewayv1.Gateway, identity cloud.Identity, ports []int, authority func(context.Context) error) (graph.GatewayInput, error) {
	port, err := strconv.Atoi(expected.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if err != nil || port < 1 || port > 65535 {
		return graph.GatewayInput{}, errGatewayChanged
	}
	validate := func(ctx context.Context) error {
		var live gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
			return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		owner := identity
		owner.RouteNamespace, owner.RouteName, owner.RouteUID = "", "", ""
		if live.UID != expected.UID || !controllerutil.ContainsFinalizer(&live, r.Config.GatewayFinalizer()) ||
			controller.ValidateGatewayBinding(r.Config, &live) != nil ||
			controller.GatewayListenerScope(r.Config, &live) != gatewayv1.HTTPProtocolType ||
			!controller.GatewayBindingMetadataEqual(r.Config, expected, &live) ||
			controller.StoredGatewayIdentity(r.Config, &live) != owner {
			return errGatewayChanged
		}
		return authority(ctx)
	}
	spec := cloud.GatewaySpec{Identity: identity, ListenerProtocol: cloud.ListenerProtocolHTTP,
		ListenerPort: port, ScopedListeners: make([]cloud.ScopedListener, 0, len(ports))}
	for _, port := range ports {
		spec.ScopedListeners = append(spec.ScopedListeners, cloud.ScopedListener{Port: port, TLS: &cloud.ListenerTLS{}})
	}
	return graph.GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS, Spec: spec}, Validate: validate}, nil
}

func (r *Reconciler) observeGatewayTLSWithdrawal(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	expected := request.Gateway
	// A Gateway-only withdrawal is for cleanup or for an invalid release/spec.
	// Never permit a route trigger to use that authority.
	if expected == nil || request.Route != nil || request.TCPRoute != nil || request.UDPRoute != nil ||
		request.Identity.RouteUID != "" || request.Identity != controller.StoredGatewayIdentity(r.Config, expected) {
		return graph.GatewayInput{}, errGatewayChanged
	}
	return r.tlsWithdrawalInput(ctx, expected, request.Identity, nil, func(ctx context.Context) error {
		var live gatewayv1.Gateway
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
			return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if live.UID != expected.UID || live.Generation != expected.Generation || !reflect.DeepEqual(live.Spec, expected.Spec) {
			return errGatewayChanged
		}
		if !live.DeletionTimestamp.IsZero() {
			return nil
		}
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(live.Spec.GatewayClassName)}, &class); err != nil {
			return err
		}
		if !controller.GatewayClassSupportsInstalledVersion(&class) || class.Spec.ControllerName != r.Config.ControllerName ||
			!class.DeletionTimestamp.IsZero() {
			return nil
		}
		if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
			if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
				return nil
			}
			return err
		}
		if _, err := SupportedListenersForConfig(r.Config, &live); err != nil {
			return nil
		}
		return errGatewayChanged
	})
}

func httpsPorts(gateway *gatewayv1.Gateway) []int {
	var ports []int
	for _, listener := range gateway.Spec.Listeners {
		if listener.Protocol == gatewayv1.HTTPSProtocolType {
			ports = append(ports, int(listener.Port))
		}
	}
	return ports
}

func (r *Reconciler) withdrawBoundTLS(ctx context.Context, gateway *gatewayv1.Gateway) (ctrl.Result, bool, error) {
	if controller.GatewayListenerScope(r.Config, gateway) != gatewayv1.HTTPProtocolType {
		return ctrl.Result{}, true, nil
	}
	if r.Writer == nil {
		return ctrl.Result{}, false, errors.New("gateway graph writer is required")
	}
	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{
		Identity: controller.StoredGatewayIdentity(r.Config, gateway), Action: graph.WithdrawTLS, Gateway: gateway,
	})
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if err := result.Outcome.Validate(); err != nil {
		return ctrl.Result{}, false, err
	}
	if result.Outcome.State == cloud.OutcomeProgressing {
		return ctrl.Result{RequeueAfter: min(httpsPollInterval, controller.ProviderProgressRequeueAfter(result.Outcome, gateway.UID))}, false, nil
	}
	return ctrl.Result{}, true, nil
}

func (r *Reconciler) listenerTLSIssues(ctx context.Context, gateway *gatewayv1.Gateway) map[gatewayv1.SectionName]httpsListenerIssue {
	issues := make(map[gatewayv1.SectionName]httpsListenerIssue)
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		switch listener.Protocol {
		case gatewayv1.HTTPSProtocolType:
			if resolution := r.resolveTLS(ctx, gateway, listener); resolution.issue != nil {
				issues[listener.Name] = *resolution.issue
			}
		case gatewayv1.HTTPProtocolType:
			if issue := httpsProfileIssue(r.Config, gateway, listener); issue != nil {
				issues[listener.Name] = *issue
			}
		}
	}
	return issues
}
