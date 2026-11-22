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
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// CertificateResolver checks a listener's exact live authorization without
// reading certificate material. ReferenceStatus also skips provider discovery.
type CertificateResolver interface {
	Resolve(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason)
	ReferenceStatus(context.Context, *gatewayv1.Gateway, *gatewayv1.Listener) gatewayv1.ListenerConditionReason
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
			if config.HTTPSListenerIsolation && listener.Protocol == gatewayv1.HTTPSProtocolType && peer.Protocol == gatewayv1.HTTPSProtocolType {
				if listenerHostname(listener) == listenerHostname(peer) {
					return &httpsListenerIssue{gatewayv1.ListenerConditionConflicted, gatewayv1.ListenerReasonHostnameConflict, "HTTPS sections on a shared port require distinct hostnames"}
				}
				continue
			}
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
	if err := validateRouteNamespacePolicy(listener); err != nil {
		return invalid(gatewayv1.ListenerReasonUnsupportedValue, err.Error())
	}
	if issue := httpRouteKindsIssue(config, listener); issue != nil {
		return issue
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

func httpRouteKindsIssue(config controller.Config, listener *gatewayv1.Listener) *httpsListenerIssue {
	if listener.AllowedRoutes != nil && slices.ContainsFunc(listener.AllowedRoutes.Kinds, func(ref gatewayv1.RouteGroupKind) bool { return !listenerSupportsRouteKind(config, *listener, ref) }) {
		return &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, gatewayv1.ListenerReasonInvalidRouteKinds, "Only HTTPRoute is supported by this listener"}
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
		return SupportedListenersForConfig(config, gateway)
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
	tls   cloud.ListenerTLS
	issue *httpsListenerIssue
}

func listenerHostname(listener *gatewayv1.Listener) string {
	if listener.Hostname == nil {
		return ""
	}
	return string(*listener.Hostname)
}

// HTTPSVirtualHostname encodes the catch-all selector explicitly for Octavia.
func HTTPSVirtualHostname(listener *gatewayv1.Listener) string {
	if hostname := listenerHostname(listener); hostname != "" {
		return hostname
	}
	return "*"
}

// SharedHTTPSListener identifies logical sections that need a virtual-host
// selector when their port is programmed.
func SharedHTTPSListener(config controller.Config, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) bool {
	if !config.OperatorHTTPS || !config.HTTPSListenerIsolation || listener.Protocol != gatewayv1.HTTPSProtocolType {
		return false
	}
	enrolled := gateway.Annotations[config.GatewayHTTPSIsolatedPortsAnnotation()]
	return slices.Contains(strings.Split(enrolled, ","), strconv.Itoa(int(listener.Port))) ||
		slices.ContainsFunc(gateway.Spec.Listeners, func(peer gatewayv1.Listener) bool {
			return peer.Protocol == listener.Protocol && peer.Port == listener.Port && peer.Name != listener.Name
		})
}

// HTTPRouteSectionsSharePort checks that two admitted sections can use the same
// physical listener. Plain HTTP admission already requires distinct exact hostnames
// on a shared port. HTTPS also needs the regional virtual-host capability.
func HTTPRouteSectionsSharePort(config controller.Config, gateway *gatewayv1.Gateway, first, next *gatewayv1.Listener) bool {
	return first.Protocol == next.Protocol && first.Port == next.Port &&
		(first.Protocol == gatewayv1.HTTPProtocolType || SharedHTTPSListener(config, gateway, next))
}

// Include only ports still assigned to HTTPS. A current singleton retains the
// original scoped policy model through edits and controller restarts.
func desiredHTTPSIsolatedPorts(config controller.Config, gateway *gatewayv1.Gateway) string {
	if !config.OperatorHTTPS || !config.HTTPSListenerIsolation {
		return gateway.Annotations[config.GatewayHTTPSIsolatedPortsAnnotation()]
	}
	var ports []string
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if SharedHTTPSListener(config, gateway, listener) {
			ports = append(ports, strconv.Itoa(int(listener.Port)))
		}
	}
	slices.Sort(ports)
	return strings.Join(slices.Compact(ports), ",")
}

func httpsSections(listeners []*gatewayv1.Listener) map[int][]*gatewayv1.Listener {
	ports := make(map[int][]*gatewayv1.Listener)
	for _, listener := range listeners {
		if listener.Protocol == gatewayv1.HTTPSProtocolType {
			port := int(listener.Port)
			ports[port] = append(ports[port], listener)
		}
	}
	return ports
}

func certificateSecret(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) client.ObjectKey {
	ref := listener.TLS.CertificateRefs[0]
	target := client.ObjectKey{Namespace: gateway.Namespace, Name: string(ref.Name)}
	if ref.Namespace != nil {
		target.Namespace = string(*ref.Namespace)
	}
	return target
}

func (r *Reconciler) resolveHTTPSPort(ctx context.Context, gateway *gatewayv1.Gateway, sections []*gatewayv1.Listener) tlsResolution {
	for _, listener := range sections {
		if issue := httpsProfileIssue(r.Config, gateway, listener); issue != nil {
			return tlsResolution{issue: issue}
		}
	}
	first := sections[0]
	if len(sections) == 1 && !SharedHTTPSListener(r.Config, gateway, first) {
		return r.resolveCertificate(ctx, gateway, first)
	}
	target := certificateSecret(gateway, first)
	hosts := make([]string, 0, len(sections))
	for _, listener := range sections {
		if certificateSecret(gateway, listener) != target {
			return tlsResolution{issue: &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue, "HTTPS sections on a shared port require the same certificate Secret"}}
		}
		hosts = append(hosts, HTTPSVirtualHostname(listener))
	}
	// All sections share the same authorization and metadata revision. Resolve
	// the common target once so a snapshot can never combine revisions.
	resolution := r.resolveCertificate(ctx, gateway, first)
	if resolution.issue != nil {
		return resolution
	}
	available := false
	if r.HTTPSListenerIsolationAvailable != nil {
		supported, err := r.HTTPSListenerIsolationAvailable(ctx)
		available = supported && err == nil
	}
	if !available {
		return tlsResolution{issue: &httpsListenerIssue{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerReasonUnsupportedValue, "Octavia HTTPS listener isolation is unavailable in this region"}}
	}
	slices.Sort(hosts)
	resolution.tls.VirtualHosts = hosts
	return resolution
}

func (r *Reconciler) resolveCertificate(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) tlsResolution {
	var ref string
	var reason gatewayv1.ListenerConditionReason
	if r.Certificates != nil {
		ref, reason = r.Certificates.Resolve(ctx, gateway, listener)
	}
	if ref != "" && reason == "" {
		return tlsResolution{tls: cloud.ListenerTLS{CertificateRef: ref}}
	}
	return tlsResolution{issue: certificateReferenceIssue(reason)}
}

func certificateReferenceIssue(reason gatewayv1.ListenerConditionReason) *httpsListenerIssue {
	message := "No approved certificate is available for this listener"
	if reason == gatewayv1.ListenerReasonRefNotPermitted {
		message = "Certificate reference is not permitted by a ReferenceGrant"
	} else {
		reason = gatewayv1.ListenerReasonInvalidCertificateRef
	}
	return &httpsListenerIssue{gatewayv1.ListenerConditionResolvedRefs, reason, message}
}

func (r *Reconciler) addTLSGraphInput(ctx context.Context, snapshot *gatewayMutationSnapshot, input *graph.GatewayInput) {
	sections := httpsSections(snapshot.listeners)
	for i := range input.Node.Spec.ScopedListeners {
		listener := &input.Node.Spec.ScopedListeners[i]
		if peers := sections[listener.Port]; len(peers) != 0 {
			resolution := r.resolveHTTPSPort(ctx, snapshot.gateway, peers)
			listener.TLS = &resolution.tls
		}
	}
}

func (r *Reconciler) tlsWithdrawalInput(ctx context.Context, expected *gatewayv1.Gateway, identity cloud.Identity) (graph.GatewayInput, error) {
	identity.RouteNamespace, identity.RouteName, identity.RouteUID = "", "", ""
	port, err := strconv.Atoi(expected.Annotations[r.Config.GatewayListenerPortAnnotation()])
	if err != nil || port < 1 || port > 65535 {
		return graph.GatewayInput{}, errGatewayChanged
	}
	listeners, err := r.liveTLSListeners(ctx, expected, identity)
	if err != nil {
		return graph.GatewayInput{}, err
	}
	validate := func(ctx context.Context) error {
		latest, err := r.liveTLSListeners(ctx, expected, identity)
		if err != nil {
			return err
		}
		if !slices.EqualFunc(listeners, latest, func(a, b cloud.ScopedListener) bool {
			return a.Port == b.Port && a.TLS.CertificateRef == b.TLS.CertificateRef && slices.Equal(a.TLS.VirtualHosts, b.TLS.VirtualHosts)
		}) {
			return errGatewayChanged
		}
		return nil
	}
	spec := cloud.GatewaySpec{Identity: identity, ListenerProtocol: cloud.ListenerProtocolHTTP,
		ListenerPort: port, ScopedListeners: listeners}
	return graph.GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS, Spec: spec}, Validate: validate}, nil
}

// liveTLSListeners authorizes each HTTPS port against the current Gateway,
// independent of the triggering revision and the ordinary route graph.
func (r *Reconciler) liveTLSListeners(ctx context.Context, expected *gatewayv1.Gateway, identity cloud.Identity) ([]cloud.ScopedListener, error) {
	var live gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
		return nil, errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
	}
	if live.UID != expected.UID || !controllerutil.ContainsFinalizer(&live, r.Config.GatewayFinalizer()) ||
		controller.ValidateGatewayBinding(r.Config, &live) != nil ||
		controller.GatewayListenerScope(r.Config, &live) != gatewayv1.HTTPProtocolType ||
		r.boundListenerProtocol(&live) != gatewayv1.HTTPProtocolType ||
		!controller.GatewayBindingMetadataEqual(r.Config, expected, &live) ||
		controller.StoredGatewayIdentity(r.Config, &live) != identity {
		return nil, errGatewayChanged
	}
	listeners := []cloud.ScopedListener{}
	if !live.DeletionTimestamp.IsZero() {
		return listeners, nil
	}
	var class gatewayv1.GatewayClass
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(live.Spec.GatewayClassName)}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return listeners, nil
		}
		return nil, err
	}
	if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() ||
		!controller.GatewayClassSupportsInstalledVersion(&class) || !hasHTTPS(&live) {
		return listeners, nil
	}
	if err := r.Config.ValidateInstalledGatewayAPIVersion(ctx, r.APIReader); err != nil {
		if errors.Is(err, controller.ErrUnsupportedGatewayAPIVersion) {
			return listeners, nil
		}
		return nil, err
	}
	profile, profileErr := SupportedListenersForConfig(r.Config, &live)
	if profileErr != nil {
		// A rejected Gateway spec cannot authorize termination, but its
		// HTTPS ports still cannot fall back to a plaintext frontend.
		profile = httpsProfileListeners(r.Config, &live)
	}
	sections := httpsSections(profile)
	for _, listener := range profile {
		if listener.Protocol != gatewayv1.HTTPSProtocolType {
			continue
		}
		port := int(listener.Port)
		if len(listeners) != 0 && listeners[len(listeners)-1].Port == port {
			continue
		}
		var tls cloud.ListenerTLS
		if profileErr == nil {
			tls = r.resolveHTTPSPort(ctx, &live, sections[port]).tls
		}
		listeners = append(listeners, cloud.ScopedListener{
			Name: resourceDisplayName(strconv.Itoa(port), live.Namespace, live.Name, string(listener.Name)),
			Port: port,
			TLS:  &tls,
		})
	}
	return listeners, nil
}

func (r *Reconciler) observeGatewayTLSWithdrawal(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	expected := request.Gateway
	if expected == nil || request.Route != nil || request.TCPRoute != nil || request.UDPRoute != nil ||
		request.Identity != controller.StoredGatewayIdentity(r.Config, expected) {
		return graph.GatewayInput{}, errGatewayChanged
	}
	if r.boundListenerProtocol(expected) == cloud.ListenerProtocolTLSTerminate {
		return r.rawTLSWithdrawalInput(ctx, expected, request.Identity)
	}
	return r.tlsWithdrawalInput(ctx, expected, request.Identity)
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
	if controller.GatewayListenerScope(r.Config, gateway) != gatewayv1.HTTPProtocolType &&
		r.boundListenerProtocol(gateway) != cloud.ListenerProtocolTLSTerminate {
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

type httpsListenerStatusIssue struct {
	primary      httpsListenerIssue
	resolvedRefs *metav1.Condition
}

func (r *Reconciler) listenerTLSIssues(ctx context.Context, gateway *gatewayv1.Gateway) map[gatewayv1.SectionName]httpsListenerStatusIssue {
	issues := make(map[gatewayv1.SectionName]httpsListenerStatusIssue)
	resolutions := make(map[int]tlsResolution)
	for port, sections := range httpsSections(httpsProfileListeners(r.Config, gateway)) {
		resolutions[port] = r.resolveHTTPSPort(ctx, gateway, sections)
	}
	for i := range gateway.Spec.Listeners {
		listener := &gateway.Spec.Listeners[i]
		if listener.Protocol != gatewayv1.HTTPProtocolType && listener.Protocol != gatewayv1.HTTPSProtocolType {
			continue
		}
		issue := httpsProfileIssue(r.Config, gateway, listener)
		var resolvedRefs *metav1.Condition
		if listener.Protocol == gatewayv1.HTTPSProtocolType {
			if issue == nil {
				issue = resolutions[int(listener.Port)].issue
			}
			if issue != nil && issue.condition == gatewayv1.ListenerConditionConflicted {
				resolvedRefs = r.conflictedHTTPSReferenceCondition(ctx, gateway, listener)
			}
		}
		if issue != nil {
			issues[listener.Name] = httpsListenerStatusIssue{primary: *issue, resolvedRefs: resolvedRefs}
		}
	}
	return issues
}

func (r *Reconciler) conflictedHTTPSReferenceCondition(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) *metav1.Condition {
	// Keep the conflict as the reason this listener cannot program. Resolve
	// its references independently for status, without using the provider ref.
	issue := httpRouteKindsIssue(r.Config, listener)
	if issue == nil {
		reason := gatewayv1.ListenerReasonInvalidCertificateRef
		if r.Certificates != nil {
			reason = r.Certificates.ReferenceStatus(ctx, gateway, listener)
		}
		if reason != "" {
			issue = certificateReferenceIssue(reason)
		}
	}
	status, reason, message := metav1.ConditionTrue, gatewayv1.ListenerReasonResolvedRefs, "Listener references are resolved"
	if issue != nil {
		status, reason, message = metav1.ConditionFalse, issue.reason, issue.message
	}
	condition := controller.Condition(string(gatewayv1.ListenerConditionResolvedRefs), status, string(reason), message, gateway.Generation)
	return &condition
}
