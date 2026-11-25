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
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

const telemetryAncestorLimit = "TelemetryPolicyAncestorLimit"

var (
	telemetryServiceName = regexp.MustCompile(`^[a-zA-Z0-9_.:/-]+$`)
	telemetrySpanName    = regexp.MustCompile(`^[a-zA-Z0-9_.:/-]+( [a-zA-Z0-9_.:/-]+)*$`)
)

type telemetryIssue struct {
	reason, message string
}

type telemetryReference struct {
	service *corev1.Service
	port    *corev1.ServicePort
	issue   telemetryIssue
	err     error
}

type gatewayTelemetryObservation struct {
	policies             []gatewayx.XTelemetryPolicy
	winner               *gatewayx.XTelemetryPolicy
	desired              *cloud.GatewayTelemetry
	rejected, unresolved telemetryIssue
	pending              string
	referencesPending    bool
	references           map[string]telemetryReference
	backendTLS           backendtls.Selection
}

func telemetryTargets(policy *gatewayx.XTelemetryPolicy, gateway *gatewayv1.Gateway) bool {
	return policy.Namespace == gateway.Namespace && policy.DeletionTimestamp.IsZero() &&
		slices.ContainsFunc(policy.Spec.TargetRefs, func(ref gatewayv1.LocalObjectReference) bool {
			return ref.Group == gatewayv1.GroupName && ref.Kind == "Gateway" && string(ref.Name) == gateway.Name
		})
}

func (r *Reconciler) listTelemetryPolicies(ctx context.Context, reader client.Reader, namespace string) ([]gatewayx.XTelemetryPolicy, error) {
	var list gatewayx.XTelemetryPolicyList
	var opts []client.ListOption
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := reader.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	if list.Continue != "" {
		return nil, errors.New("incomplete XTelemetryPolicy list")
	}
	return list.Items, nil
}

func telemetryPrecedence(a, b gatewayx.XTelemetryPolicy) int {
	return cmp.Or(a.CreationTimestamp.Compare(b.CreationTimestamp.Time), cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
}

func (r *Reconciler) observeGatewayTelemetry(ctx context.Context, gateway *gatewayv1.Gateway, managed bool, snapshot *gatewayMutationSnapshot) (observation gatewayTelemetryObservation, err error) {
	enabled := r.Config.TelemetryPolicyEnabled()
	if !enabled && !r.Config.XBackendEnabled() && gateway.Annotations[r.Config.GatewayTelemetryAnnotation()] != "v1" {
		return observation, nil
	}
	observation.policies, err = r.listTelemetryPolicies(ctx, r.APIReader, gateway.Namespace)
	if err != nil {
		if !enabled && (meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) || apierrors.IsNotFound(err) || apierrors.IsForbidden(err)) {
			return observation, nil
		}
		return observation, err
	}
	slices.SortFunc(observation.policies, telemetryPrecedence)
	if !managed || !enabled {
		return observation, nil
	}
	observation.references = make(map[string]telemetryReference)
	for i := range observation.policies {
		policy := &observation.policies[i]
		if !telemetryTargets(policy, gateway) {
			continue
		}
		if observation.winner == nil {
			observation.winner = policy
		}
		if policy.Spec.Tracing.Mode == gatewayx.TracingModeEnabled {
			observation.references[policy.Name] = r.resolveTelemetryReference(ctx, policy)
		}
	}
	if observation.winner == nil {
		return observation, nil
	}
	ancestor := backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
	slot := ancestor.SlotFor(observation.winner.Namespace, observation.winner.Status.Ancestors)
	if slot == backendtls.AncestorSlotFull {
		return observation, nil
	}

	tracing := observation.winner.Spec.Tracing
	switch tracing.Mode {
	case "", gatewayx.TracingModeDisabled, gatewayx.TracingModeImplementationDefault:
		return observation, nil
	case gatewayx.TracingModeEnabled:
	default:
		observation.rejected = telemetryIssue{"UnsupportedValue", "The tracing mode is unsupported"}
		return observation, nil
	}
	if snapshot == nil {
		observation.pending = "Waiting for a usable Gateway listener binding"
		return observation, nil
	}
	sampling, valid := telemetrySampling(tracing)
	attributes, validAttributes := telemetryAttributes(tracing.Attributes)
	if !valid {
		observation.rejected = telemetryIssue{"UnsupportedValue", "Invalid tracing sampling fraction or parent-based sampling configuration"}
	} else if !validAttributes {
		observation.rejected = telemetryIssue{"UnsupportedValue", "The tracing policy contains an unsupported span attribute"}
	} else if len(tracing.Provider.Headers) != 0 {
		observation.rejected = telemetryIssue{"UnsupportedValue", "Octavia tracing does not support custom provider headers"}
	}
	if (tracing.ServiceName != "" && (len(tracing.ServiceName) > 253 || !telemetryServiceName.MatchString(tracing.ServiceName))) ||
		(tracing.SpanName != "" && (len(tracing.SpanName) > 256 || !telemetrySpanName.MatchString(tracing.SpanName))) {
		observation.rejected = telemetryIssue{"UnsupportedValue", "The serviceName or spanName has an unsupported form"}
	}
	// Resolve the collector reference even if an independent option was rejected.
	reference := observation.references[observation.winner.Name]
	observation.unresolved = reference.issue
	if reference.err != nil || reference.issue.reason != "" {
		return observation, reference.err
	}
	var backendTLS backendtls.Selection
	desired, rejected, pending, err := r.resolveTelemetryCollector(ctx, snapshot, observation.winner, reference, &backendTLS)
	if observation.rejected.reason == "" {
		observation.backendTLS = backendTLS
		observation.rejected = rejected
	}
	observation.pending = pending
	if err == nil && observation.unresolved.reason == "" && observation.rejected.reason == "" && desired != nil {
		desired.Sampling = sampling
		desired.Attributes = attributes
		sets, err := r.ObserveListenerSets(ctx, r.APIReader, gateway)
		if err != nil {
			return observation, err
		}
		desired.Listeners = r.telemetryListeners(snapshot, sets)
		if len(desired.Listeners) == 0 {
			observation.rejected = telemetryIssue{"UnsupportedValue", "Tracing requires an HTTP or terminated HTTPS listener"}
		} else if len(desired.Members) == 0 {
			observation.pending = "Waiting for ready Octavia collector members"
		} else if slot == backendtls.AncestorSlotPending {
			observation.pending = "Waiting for this Gateway to claim a policy ancestor slot"
		} else if observation.pending == "" {
			observation.desired = desired
		}
	}
	return observation, err
}

func telemetryAttributes(attrs []gatewayx.Attribute) ([]cloud.TelemetryAttribute, bool) {
	values := make([]cloud.TelemetryAttribute, 0, len(attrs))
	for _, a := range attrs {
		values = append(values, cloud.TelemetryAttribute{Name: string(a.Name), SourceType: string(a.SourceType),
			LiteralValue: a.LiteralValue, HeaderName: string(a.HeaderName), AttributeKey: a.AttributeKey})
	}
	return cloud.CanonicalTelemetryAttributes(values)
}

func telemetrySampling(tracing gatewayx.TracingConfig) (*cloud.TelemetrySampling, bool) {
	rate := func(fraction *gatewayv1.Fraction) cloud.TelemetryRate {
		if fraction == nil {
			return cloud.TelemetryRate{Numerator: 1, Denominator: 1}
		}
		return cloud.TelemetryRate{Numerator: fraction.Numerator, Denominator: ptr.Deref(fraction.Denominator, 100)}
	}
	base := rate(tracing.SamplingRate)
	if !base.Valid() {
		return nil, false
	}
	parent := tracing.ParentBasedSampling
	switch parent.Mode {
	case gatewayx.ParentBasedSamplingModeEnabled:
		governor := rate(parent.SamplingRate)
		if !governor.Valid() {
			return nil, false
		}
		return &cloud.TelemetrySampling{Base: base, Parent: &governor}, true
	case "", gatewayx.ParentBasedSamplingModeImplementationDefault, gatewayx.ParentBasedSamplingModeDisabled:
		if parent.SamplingRate != nil {
			return nil, false
		}
		if base.Numerator == base.Denominator {
			return nil, true
		}
		return &cloud.TelemetrySampling{Base: base}, true
	default:
		return nil, false
	}
}

func (r *Reconciler) telemetryListeners(snapshot *gatewayMutationSnapshot, sets ListenerSetProjection) []cloud.TelemetryListener {
	var listeners []cloud.TelemetryListener
	add := func(listener *gatewayv1.Listener) {
		protocol := cloud.ListenerProtocolHTTP
		switch listener.Protocol {
		case gatewayv1.HTTPProtocolType:
		case gatewayv1.HTTPSProtocolType:
			protocol = cloud.ListenerProtocolTerminatedHTTPS
		default:
			return
		}
		target := cloud.TelemetryListener{Port: int(listener.Port), Protocol: protocol}
		if !slices.Contains(listeners, target) {
			listeners = append(listeners, target)
		}
	}
	for _, listener := range snapshot.listeners {
		if !usesHTTPSProfile(r.Config, snapshot.gateway) || httpsProfileIssue(r.Config, snapshot.gateway, listener) == nil {
			add(listener)
		}
	}
	for _, set := range sets.sets {
		for _, entry := range set.entries {
			if entry.rejection == "" {
				add(&entry.listener)
			}
		}
	}
	slices.SortFunc(listeners, func(a, b cloud.TelemetryListener) int {
		return cmp.Or(cmp.Compare(a.Port, b.Port), cmp.Compare(a.Protocol, b.Protocol))
	})
	return listeners
}

func (r *Reconciler) resolveTelemetryReference(ctx context.Context, policy *gatewayx.XTelemetryPolicy) telemetryReference {
	ref := policy.Spec.Tracing.Provider.BackendRef
	key, serviceRef := model.BackendServiceReference(policy.Namespace, ref)
	if !serviceRef {
		return telemetryReference{issue: telemetryIssue{"InvalidKind", "The tracing backendRef must refer to a core Service"}}
	}
	if key.Name == "" {
		return telemetryReference{issue: telemetryIssue{"BackendNotFound", "The collector Service reference is absent"}}
	}
	if key.Namespace != policy.Namespace {
		grants, err := graph.ListBackendReferenceGrants(ctx, r.APIReader, key.Namespace)
		if err != nil {
			return telemetryReference{err: err}
		}
		if !model.BackendReferenceAllowedForGroup(gatewayv1.Group(gatewayx.GroupVersion.Group), "XTelemetryPolicy", policy.Namespace, key, grants) {
			return telemetryReference{issue: telemetryIssue{"RefNotPermitted", "No ReferenceGrant authorizes this XTelemetryPolicy to use the collector Service"}}
		}
	}
	var service corev1.Service
	if err := r.APIReader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			return telemetryReference{issue: telemetryIssue{"BackendNotFound", "The collector Service was not found"}}
		}
		return telemetryReference{err: err}
	}
	if !service.DeletionTimestamp.IsZero() || service.UID == "" {
		return telemetryReference{issue: telemetryIssue{"BackendNotFound", "The collector Service has no live identity"}}
	}
	port, diagnostic := model.ResolveTCPServicePort(&service, ref.Port)
	if diagnostic != nil {
		return telemetryReference{issue: telemetryIssue{"BackendNotFound", diagnostic.Message}}
	}
	return telemetryReference{service: &service, port: port}
}

func (r *Reconciler) resolveTelemetryCollector(ctx context.Context, snapshot *gatewayMutationSnapshot, policy *gatewayx.XTelemetryPolicy,
	reference telemetryReference, backendTLS *backendtls.Selection) (
	desired *cloud.GatewayTelemetry, rejected telemetryIssue, pending string, err error,
) {
	tracing := policy.Spec.Tracing
	ref := tracing.Provider.BackendRef
	service, port := reference.service, reference.port

	var transport *cloud.BackendTLSValidation
	observer := graph.NewBackendTLSObserver(r.APIReader, r.Config.ClusterTrustBundles)
	if r.Config.BackendTLSPolicy {
		ancestor := &backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(snapshot.gateway), ControllerName: r.Config.ControllerName}
		selection, readErr := observer.Observe(ctx, service, port, ancestor)
		if readErr != nil {
			err = readErr
			return
		}
		*backendTLS = selection
		if selection.Winner != nil {
			if selection.Winner.Validation == nil || selection.Winner.AncestorSlot == backendtls.AncestorSlotFull {
				rejected = telemetryIssue{"UnsupportedValue", "The collector requires an applicable, verifiable BackendTLSPolicy"}
				return
			}
			if selection.Winner.Validation.Trust == cloud.BackendTLSTrustSystem {
				rejected = telemetryIssue{"UnsupportedValue", "The collector backend TLS profile requires an explicit CA reference"}
				return
			}
			transport = selection.Winner.Validation.CloudValidation()
			var live gatewayv1.BackendTLSPolicy
			if err = r.APIReader.Get(ctx, selection.Winner.Key, &live); err != nil {
				return
			}
			if live.UID != selection.Winner.UID || live.Generation != selection.Winner.Generation || !live.DeletionTimestamp.IsZero() {
				err = errGatewayChanged
				return
			}
			if !ancestor.AcceptsCurrentPolicy(&live) {
				pending = "Waiting for the BackendTLSPolicy to accept the Gateway at its current generation"
			}
		}
	} else {
		var requiresTLS bool
		requiresTLS, err = observer.HasPolicy(ctx, service, port)
		if err != nil {
			return
		}
		if requiresTLS {
			rejected = telemetryIssue{"UnsupportedValue", "The collector requires --octavia-backend-tls-policy for authenticated transport"}
			return
		}
	}
	if _, diagnostic := model.ResolveServicePortForBackendTLS(service, ref.Port, false, transport != nil); diagnostic != nil {
		rejected = telemetryIssue{"UnsupportedValue", diagnostic.Message}
		return
	}
	if ptr.Deref(port.AppProtocol, "") == "kubernetes.io/ws" {
		rejected = telemetryIssue{"UnsupportedValue", "The collector Service must speak OTLP over HTTP/1.1"}
		return
	}
	members, diagnostic, readErr := routebackend.TransportMembers(ctx, r.APIReader, service, ref.Port, corev1.ProtocolTCP, snapshot.topology, nil, false)
	if readErr != nil {
		err = readErr
		return
	}
	if diagnostic != nil {
		if diagnostic.Reason == string(gatewayv1.RouteReasonPending) {
			pending = diagnostic.Message
		} else {
			rejected = telemetryIssue{"UnsupportedValue", diagnostic.Message}
		}
		return
	}
	desired = &cloud.GatewayTelemetry{
		ServiceUID: string(service.UID), ServicePort: int(port.Port),
		ServiceName: tracing.ServiceName, SpanName: tracing.SpanName,
		MemberSubnetID:          snapshot.topology.Topology.MemberSubnetID,
		BackendSecurityGroupIDs: snapshot.topology.Topology.BackendSecurityGroupIDs,
		Members:                 slices.DeleteFunc(members, func(member cloud.Member) bool { return member.Draining }),
		BackendTLS:              transport,
	}
	if desired.ServiceName == "" {
		desired.ServiceName = snapshot.gateway.Namespace + "/" + snapshot.gateway.Name
		if len(desired.ServiceName) > 253 {
			digest := sha256.Sum256([]byte(desired.ServiceName))
			desired.ServiceName = fmt.Sprintf("%s-%x", desired.ServiceName[:236], digest[:8])
		}
	}
	if desired.SpanName == "" {
		desired.SpanName = "gateway.request"
	}
	return
}

// telemetryBinding remains usable for withdrawal during Gateway teardown,
// unsupported configurations and installations switching off the opt-in.
func (r *Reconciler) telemetryBinding(ctx context.Context, request graph.Request) (*gatewayv1.Gateway, error) {
	var current gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(request.Gateway), &current); err != nil {
		return nil, errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
	}
	if current.UID != request.Gateway.UID || current.Generation != request.Gateway.Generation ||
		!reflect.DeepEqual(current.Spec, request.Gateway.Spec) ||
		current.DeletionTimestamp.IsZero() != request.Gateway.DeletionTimestamp.IsZero() ||
		!controller.GatewayBindingMetadataEqual(r.Config, request.Gateway, &current) ||
		current.Annotations[r.Config.GatewayTelemetryAnnotation()] != "v1" ||
		!r.gatewayBindingComplete(&current) || controller.StoredGatewayIdentity(r.Config, &current) != request.Identity {
		return nil, errGatewayChanged
	}
	if err := controller.ValidateGatewayBinding(r.Config, &current); err != nil {
		return nil, err
	}
	return &current, nil
}

func (r *Reconciler) observeTelemetryGraph(ctx context.Context, request graph.Request) (graph.GatewayInput, error) {
	observe := func(ctx context.Context) (*gatewayv1.Gateway, gatewayTelemetryObservation, error, error) {
		gateway, err := r.telemetryBinding(ctx, request)
		if err != nil {
			return nil, gatewayTelemetryObservation{}, nil, err
		}
		var snapshot *gatewayMutationSnapshot
		var readErr error
		managed := false
		if !request.WithdrawTelemetry && r.Config.TelemetryPolicyEnabled() {
			snapshot, readErr = r.observeGatewayMutationSnapshot(ctx, gateway, gatewayBindingRequired, "")
			if readErr == nil {
				readErr = r.sealGatewayMutationSnapshot(ctx, snapshot)
			}
			if readErr != nil {
				snapshot = nil
			}
			managed = snapshot != nil
		} else if r.Config.TelemetryPolicyEnabled() {
			managed, readErr = r.telemetryOwnsAncestor(ctx, gateway)
		}
		observation, err := r.observeGatewayTelemetry(ctx, gateway, managed, snapshot)
		return gateway, observation, errors.Join(readErr, err), nil
	}
	gateway, observed, observationErr, err := observe(ctx)
	if err != nil {
		return graph.GatewayInput{}, err
	}
	input := graph.GatewayInput{Object: gateway, Node: cloud.GatewayGraphNode{
		Disposition:            cloud.GraphReconcileTelemetry,
		Telemetry:              observed.desired,
		TelemetryPolicyPresent: observed.winner != nil,
		BackendCAOwned:         controllerutil.ContainsFinalizer(gateway, r.Config.BackendCAFinalizer()),
	}}
	if observed.desired != nil {
		input.Node.Spec.Identity = request.Identity
		input.Node.BackendClientCertificate, _, _ = r.observeBackendClientCertificate(ctx, gateway)
	}
	input.Validate = func(ctx context.Context) error {
		latest, current, currentErr, err := observe(ctx)
		if err != nil {
			return err
		}
		// A revocation or loss of a required read must still be able to
		// withdraw existing exports. Restored authority gets a fresh plan.
		if !reflect.DeepEqual(observed.desired, current.desired) || (observationErr == nil) != (currentErr == nil) ||
			telemetryWinnerRevision(observed.winner) != telemetryWinnerRevision(current.winner) ||
			controllerutil.ContainsFinalizer(latest, r.Config.BackendCAFinalizer()) != input.Node.BackendCAOwned {
			return errGatewayChanged
		}
		if observed.desired != nil {
			identity, _, _ := r.observeBackendClientCertificate(ctx, latest)
			if identity != input.Node.BackendClientCertificate {
				return errGatewayChanged
			}
		}
		return nil
	}
	input.ReportTelemetry = func(ctx context.Context, result cloud.GatewayGraphResult, providerErr error) error {
		if err := input.Validate(ctx); err != nil {
			return err
		}
		if observationErr != nil && observed.winner != nil {
			observed.pending = "Waiting for a live inventory of collector dependencies"
			observed.referencesPending = true
		}
		if observationErr != nil && observed.winner == nil {
			return observationErr
		}
		return errors.Join(observationErr, r.reportTelemetryPolicies(ctx, gateway, observed, result, providerErr))
	}
	return input, nil
}

func telemetryWinnerRevision(policy *gatewayx.XTelemetryPolicy) string {
	if policy == nil {
		return ""
	}
	return fmt.Sprintf("%s/%d", policy.UID, policy.Generation)
}

func (r *Reconciler) reportTelemetryPolicies(ctx context.Context, gateway *gatewayv1.Gateway, observed gatewayTelemetryObservation, result cloud.GatewayGraphResult, providerErr error) error {
	var updateErr error
	ancestor := backendtls.GatewayAncestor{Key: client.ObjectKeyFromObject(gateway), ControllerName: r.Config.ControllerName}
	for i := range observed.policies {
		policy := &observed.policies[i]
		slot := ancestor.SlotFor(policy.Namespace, policy.Status.Ancestors)
		if slot == backendtls.AncestorSlotFull {
			if telemetryTargets(policy, gateway) && r.Recorder != nil {
				r.Recorder.Eventf(gateway, nil, corev1.EventTypeWarning, telemetryAncestorLimit, "ReconcileTelemetry", "XTelemetryPolicy %s has no room for this Gateway in its ancestor status", policy.Name)
			}
			continue
		}
		if slot == backendtls.AncestorSlotPending && (observed.winner == nil || !telemetryTargets(policy, gateway)) {
			continue
		}
		updated := policy.DeepCopy()
		matching := func(entry gatewayv1.PolicyAncestorStatus) bool {
			return entry.ControllerName == r.Config.ControllerName &&
				backendtls.GatewayAncestorKey(entry.AncestorRef, policy.Namespace) == ancestor.Key
		}
		index := slices.IndexFunc(updated.Status.Ancestors, matching)
		if result.TelemetryCapabilityUnavailable || observed.winner == nil || !telemetryTargets(policy, gateway) {
			updated.Status.Ancestors = slices.DeleteFunc(updated.Status.Ancestors, matching)
		} else {
			entry := gatewayv1.PolicyAncestorStatus{
				AncestorRef: gatewayv1.ParentReference{Group: ptr.To(gatewayv1.Group(gatewayv1.GroupName)),
					Kind: ptr.To(gatewayv1.Kind("Gateway")), Name: gatewayv1.ObjectName(gateway.Name)},
				ControllerName: r.Config.ControllerName,
			}
			if index >= 0 {
				entry = updated.Status.Ancestors[index]
			}
			applyTelemetryConditions(&entry, policy, observed, result, providerErr)
			if index < 0 {
				updated.Status.Ancestors = append(updated.Status.Ancestors, entry)
			} else {
				updated.Status.Ancestors[index] = entry
			}
		}
		if !reflect.DeepEqual(policy.Status, updated.Status) {
			updateErr = errors.Join(updateErr, client.IgnoreNotFound(r.Status().Patch(ctx, updated, controller.OptimisticMergeFrom(policy))))
		}
	}
	return updateErr
}

func applyTelemetryConditions(entry *gatewayv1.PolicyAncestorStatus, policy *gatewayx.XTelemetryPolicy, observed gatewayTelemetryObservation, result cloud.GatewayGraphResult, providerErr error) {
	accepted := telemetryIssue{"Accepted", "The tracing policy is accepted"}
	resolved := telemetryIssue{"ResolvedRefs", "The tracing policy references are resolved"}
	acceptedStatus, resolvedStatus := metav1.ConditionTrue, metav1.ConditionTrue
	switch {
	case observed.winner.Name != policy.Name:
		acceptedStatus = metav1.ConditionFalse
		accepted = telemetryIssue{"Conflicted", "An older or lexically earlier XTelemetryPolicy has precedence for this Gateway"}
	case observed.unresolved.reason != "":
		resolvedStatus, resolved = metav1.ConditionFalse, observed.unresolved
		if observed.rejected.reason != "" {
			acceptedStatus, accepted = metav1.ConditionFalse, observed.rejected
		}
	case observed.rejected.reason != "":
		acceptedStatus, accepted = metav1.ConditionFalse, observed.rejected
	case result.TelemetryUnsupported != "":
		acceptedStatus, accepted = metav1.ConditionFalse, telemetryIssue{"UnsupportedValue", result.TelemetryUnsupported}
	case observed.pending != "":
		acceptedStatus, accepted = metav1.ConditionFalse, telemetryIssue{"Pending", observed.pending}
		if observed.referencesPending {
			resolvedStatus, resolved = metav1.ConditionUnknown, telemetryIssue{"Pending", observed.pending}
		}
	case providerErr != nil || result.Outcome.State != cloud.OutcomeReady:
		acceptedStatus, accepted = metav1.ConditionFalse, telemetryIssue{"Pending", "Waiting for verified Octavia listener tracing configuration"}
	}
	if reference := observed.references[policy.Name]; reference.issue.reason != "" {
		resolvedStatus, resolved = metav1.ConditionFalse, reference.issue
	} else if reference.err != nil {
		resolvedStatus, resolved = metav1.ConditionUnknown, telemetryIssue{"Pending", "Waiting for a live inventory of collector references"}
	}
	meta.SetStatusCondition(&entry.Conditions, controller.Condition("Accepted", acceptedStatus, accepted.reason, accepted.message, policy.Generation))
	meta.SetStatusCondition(&entry.Conditions, controller.Condition("ResolvedRefs", resolvedStatus, resolved.reason, resolved.message, policy.Generation))
}

func (r *Reconciler) markGatewayTelemetry(ctx context.Context, scope *gatewayScope, claimCA bool) error {
	desired := scope.gateway.DeepCopy()
	if desired.Annotations == nil {
		desired.Annotations = map[string]string{}
	}
	desired.Annotations[r.Config.GatewayTelemetryAnnotation()] = "v1"
	if claimCA {
		controllerutil.AddFinalizer(desired, r.Config.BackendCAFinalizer())
	}
	if err := r.Patch(ctx, desired, controller.OptimisticMergeFrom(scope.original)); err != nil {
		return err
	}
	return scope.refresh(desired)
}

// The telemetry action runs before forwarding graph observation. Failure to
// read a route, or a pending application backend, cannot defer a revocation.
func (r *Reconciler) reconcileGatewayTelemetry(ctx context.Context, scope *gatewayScope, withdraw bool) (time.Duration, error) {
	gateway := scope.gateway
	marked := gateway.Annotations[r.Config.GatewayTelemetryAnnotation()] == "v1"
	if !r.gatewayBindingComplete(gateway) || withdraw && !marked {
		return 0, r.reportTelemetryWithoutBinding(ctx, client.ObjectKeyFromObject(gateway))
	}
	if !marked && !r.Config.TelemetryPolicyEnabled() {
		return 0, r.reportTelemetryWithoutBinding(ctx, client.ObjectKeyFromObject(gateway))
	}
	claimCA := !withdraw && r.Config.TelemetryPolicyEnabled() && r.Config.BackendTLSPolicy &&
		!controllerutil.ContainsFinalizer(gateway, r.Config.BackendCAFinalizer())
	if !marked || claimCA {
		policies, err := r.listTelemetryPolicies(ctx, r.APIReader, gateway.Namespace)
		hasTarget := slices.ContainsFunc(policies, func(policy gatewayx.XTelemetryPolicy) bool { return telemetryTargets(&policy, gateway) })
		if err == nil && hasTarget {
			if err := r.markGatewayTelemetry(ctx, scope, claimCA); err != nil {
				return 0, err
			}
			gateway = scope.gateway
		} else if !marked {
			return 0, err
		}
		// An existing checkpoint still authorizes withdrawal after an API
		// read failure or deletion of the last policy.
	}

	result, err := r.Writer.ReconcileGraph(ctx, graph.Request{Action: graph.ReconcileTelemetry, Gateway: gateway,
		Identity: controller.StoredGatewayIdentity(r.Config, gateway), WithdrawTelemetry: withdraw})
	if err != nil {
		return 0, err
	}
	if result.Outcome.State == cloud.OutcomeProgressing {
		return result.Outcome.RequeueAfter, nil
	}
	if withdraw || !r.Config.TelemetryPolicyEnabled() {
		return 0, nil
	}
	return httpsPollInterval, nil
}

func (r *Reconciler) telemetryOwnsAncestor(ctx context.Context, gateway *gatewayv1.Gateway) (bool, error) {
	if !gateway.DeletionTimestamp.IsZero() || gateway.Spec.GatewayClassName == "" {
		return false, nil
	}
	var class gatewayv1.GatewayClass
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return class.Spec.ControllerName == r.Config.ControllerName && class.DeletionTimestamp.IsZero(), nil
}

func (r *Reconciler) reportTelemetryWithoutBinding(ctx context.Context, key client.ObjectKey) error {
	if !r.Config.XBackendEnabled() {
		return nil
	}
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, key, &gateway); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		gateway.Name, gateway.Namespace = key.Name, key.Namespace
	}
	managed, err := r.telemetryOwnsAncestor(ctx, &gateway)
	if err != nil {
		return err
	}
	observed, err := r.observeGatewayTelemetry(ctx, &gateway, managed, nil)
	if err != nil {
		return err
	}
	return r.reportTelemetryPolicies(ctx, &gateway, observed, cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil)
}

func gatewayRequestsForTelemetryPolicy(_ context.Context, object client.Object) []reconcile.Request {
	policy, ok := object.(*gatewayx.XTelemetryPolicy)
	if !ok {
		return nil
	}
	keys := map[client.ObjectKey]struct{}{}
	for _, ref := range policy.Spec.TargetRefs {
		if ref.Group == gatewayv1.GroupName && ref.Kind == "Gateway" && ref.Name != "" {
			keys[client.ObjectKey{Namespace: policy.Namespace, Name: string(ref.Name)}] = struct{}{}
		}
	}
	return controller.SortedRequests(keys)
}

func (r *Reconciler) gatewayRequestsForTelemetryDependency(ctx context.Context, dependency client.Object) []reconcile.Request {
	policies, err := r.listTelemetryPolicies(ctx, r.Client, "")
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not list XTelemetryPolicies for collector dependency")
		return nil
	}
	keys := map[client.ObjectKey]struct{}{}
	for i := range policies {
		policy := &policies[i]
		if policy.Spec.Tracing.Mode != gatewayx.TracingModeEnabled {
			continue
		}
		service, valid := model.BackendServiceReference(policy.Namespace, policy.Spec.Tracing.Provider.BackendRef)
		if valid && listenerAuthDependsOn(service, dependency) {
			for _, request := range gatewayRequestsForTelemetryPolicy(ctx, policy) {
				keys[request.NamespacedName] = struct{}{}
			}
		}
	}
	return controller.SortedRequests(keys)
}

func boundTelemetryResult(result *ctrl.Result, interval time.Duration) {
	if interval > 0 && (result.RequeueAfter == 0 || result.RequeueAfter > interval) {
		result.RequeueAfter = interval
	}
}
