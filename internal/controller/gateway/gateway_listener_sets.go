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
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type listenerSetEntry struct {
	listener          gatewayv1.Listener
	rejection         gatewayv1.ListenerConditionReason
	conflicted        bool
	invalidRouteKinds bool
}

type admittedListenerSet struct {
	namespaceVersion string
	object           *gatewayv1.ListenerSet
	allowed          bool
	entries          []listenerSetEntry
}

type ListenerSetProjection struct {
	sets []admittedListenerSet
}

// ObserveListenerSets resolves admission and precedence from the same live
// inventory used to authorize frontend and route graph changes.
func ObserveListenerSets(ctx context.Context, reader client.Reader, config controller.Config, gateway *gatewayv1.Gateway) (ListenerSetProjection, error) {
	if model.GatewayDisallowsListenerSets(gateway) {
		return ListenerSetProjection{}, nil
	}
	var inventory gatewayv1.ListenerSetList
	if err := reader.List(ctx, &inventory); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return ListenerSetProjection{}, nil
		}
		return ListenerSetProjection{}, fmt.Errorf("list ListenerSets: %w", err)
	}
	if inventory.Continue != "" {
		return ListenerSetProjection{}, fmt.Errorf("incomplete ListenerSet inventory")
	}
	gatewayKey := client.ObjectKeyFromObject(gateway)
	sets := slices.DeleteFunc(inventory.Items, func(set gatewayv1.ListenerSet) bool {
		return !set.DeletionTimestamp.IsZero() || model.ListenerSetParentKey(&set) != gatewayKey
	})
	slices.SortFunc(sets, func(a, b gatewayv1.ListenerSet) int {
		return cmp.Or(a.CreationTimestamp.Compare(b.CreationTimestamp.Time), cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	listeners, validationErr := SupportedListenersForConfig(config, gateway)
	plainHTTP := validationErr == nil && len(listeners) == len(gateway.Spec.Listeners) &&
		len(listeners) != 0 && !slices.ContainsFunc(listeners, func(listener *gatewayv1.Listener) bool {
		return listener.Protocol != gatewayv1.HTTPProtocolType
	})
	winners := make(map[gatewayv1.PortNumber][]*gatewayv1.Listener)
	for _, listener := range listeners {
		winners[listener.Port] = append(winners[listener.Port], listener)
	}
	projection := ListenerSetProjection{}
	namespaces := make(map[string]*corev1.Namespace)
	for i := range sets {
		set := &sets[i]
		if *gateway.Spec.AllowedListeners.Namespaces.From == gatewayv1.NamespacesFromSelector {
			if _, observed := namespaces[set.Namespace]; !observed {
				var namespace corev1.Namespace
				if err := reader.Get(ctx, types.NamespacedName{Name: set.Namespace}, &namespace); err == nil {
					namespaces[set.Namespace] = &namespace
				} else if !apierrors.IsNotFound(err) {
					return ListenerSetProjection{}, err
				} else {
					namespaces[set.Namespace] = nil
				}
			}
		}
		admitted := admittedListenerSet{object: set, allowed: model.AllowsListenerSetNamespace(gateway, set.Namespace, namespaces[set.Namespace])}
		if namespace := namespaces[set.Namespace]; namespace != nil {
			admitted.namespaceVersion = namespace.ResourceVersion
		}
		if admitted.allowed {
			for _, spec := range set.Spec.Listeners {
				entry := projectListenerSetEntry(spec, plainHTTP, winners[spec.Port])
				admitted.entries = append(admitted.entries, entry)
				if entry.rejection == "" {
					winners[spec.Port] = append(winners[spec.Port], &entry.listener)
				}
			}
		}
		projection.sets = append(projection.sets, admitted)
	}
	return projection, nil
}

// Sections returns the admitted HTTP listeners keyed by their owning ListenerSet.
func (p ListenerSetProjection) Sections() map[types.NamespacedName][]gatewayv1.Listener {
	sections := make(map[types.NamespacedName][]gatewayv1.Listener)
	for _, set := range p.sets {
		key := client.ObjectKeyFromObject(set.object)
		// Keep the owner in the projection even if it has no winning section.
		sections[key] = nil
		for _, entry := range set.entries {
			if entry.rejection == "" {
				sections[key] = append(sections[key], entry.listener)
			}
		}
	}
	return sections
}

func (p ListenerSetProjection) sameInputs(other ListenerSetProjection) bool {
	return slices.EqualFunc(p.sets, other.sets, func(a, b admittedListenerSet) bool {
		return a.object.UID == b.object.UID && client.ObjectKeyFromObject(a.object) == client.ObjectKeyFromObject(b.object) &&
			a.object.Generation == b.object.Generation && equality.Semantic.DeepEqual(a.object.Spec, b.object.Spec) &&
			a.namespaceVersion == b.namespaceVersion && a.allowed == b.allowed && equality.Semantic.DeepEqual(a.entries, b.entries)
	})
}

func (p ListenerSetProjection) count() int32 {
	var count int32
	for _, set := range p.sets {
		if slices.ContainsFunc(set.entries, func(entry listenerSetEntry) bool { return entry.rejection == "" }) {
			count++
		}
	}
	return count
}

func projectListenerSetEntry(spec gatewayv1.ListenerEntry, plainHTTP bool, siblings []*gatewayv1.Listener) listenerSetEntry {
	listener := gatewayv1.Listener{
		Name: spec.Name, Port: spec.Port, Protocol: spec.Protocol, Hostname: spec.Hostname,
		TLS: spec.TLS, AllowedRoutes: spec.AllowedRoutes, Filters: spec.Filters,
	}
	entry := listenerSetEntry{listener: listener}
	if listener.AllowedRoutes != nil && len(listener.AllowedRoutes.Kinds) != 0 {
		for _, ref := range listener.AllowedRoutes.Kinds {
			entry.invalidRouteKinds = entry.invalidRouteKinds || listener.Protocol != gatewayv1.HTTPProtocolType || !isRouteGroupKind(ref, model.HTTPRouteKind)
		}
	}
	for _, sibling := range siblings {
		if sibling.Protocol != listener.Protocol {
			entry.rejection, entry.conflicted = gatewayv1.ListenerReasonProtocolConflict, true
			return entry
		}
		if listenerHostname(sibling) == listenerHostname(&listener) {
			entry.rejection, entry.conflicted = gatewayv1.ListenerReasonHostnameConflict, true
			return entry
		}
	}
	switch {
	case !plainHTTP || listener.Protocol != gatewayv1.HTTPProtocolType:
		entry.rejection = gatewayv1.ListenerReasonUnsupportedProtocol
	case listener.Port < 1 || listener.Port > 65535:
		entry.rejection = gatewayv1.ListenerReasonPortUnavailable
	case listener.TLS != nil || listener.Filters != nil && len(listener.Filters.Requests) != 0 || validateRouteNamespacePolicy(&listener) != nil:
		entry.rejection = gatewayv1.ListenerReasonUnsupportedValue
	case entry.invalidRouteKinds && !listenerAllowsRouteKind(listener, model.HTTPRouteKind):
		entry.rejection = gatewayv1.ListenerReasonInvalidRouteKinds
	default:
		host := listenerHostname(&listener)
		if err := amphora.ValidateHostname(host); host != "" && err != nil {
			entry.rejection = gatewayv1.ListenerReasonUnsupportedValue
		}
		if len(siblings) != 0 && (!sharesPlainHTTPPort(&listener) || slices.ContainsFunc(siblings, func(sibling *gatewayv1.Listener) bool { return !sharesPlainHTTPPort(sibling) })) {
			entry.rejection = gatewayv1.ListenerReasonPortUnavailable
		}
	}
	if entry.rejection == "" && entry.invalidRouteKinds {
		entry.listener.AllowedRoutes = listener.AllowedRoutes.DeepCopy()
		entry.listener.AllowedRoutes.Kinds = supportedRouteKinds(model.HTTPRouteKind)
	}
	return entry
}

func sharesPlainHTTPPort(listener *gatewayv1.Listener) bool {
	host := listenerHostname(listener)
	if host == "" || host[0] == '*' {
		return false
	}
	_, err := amphora.HostnameRuleValue(host)
	return err == nil
}

func (r *Reconciler) reportListenerSets(ctx context.Context, gateway *gatewayv1.Gateway, projection ListenerSetProjection, result cloud.GatewayGraphResult) error {
	var liveGateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(gateway), &liveGateway); err != nil {
		return err
	}
	if liveGateway.UID != gateway.UID || liveGateway.Generation != gateway.Generation ||
		!liveGateway.DeletionTimestamp.IsZero() || !equality.Semantic.DeepEqual(liveGateway.Spec, gateway.Spec) {
		return errGatewayChanged
	}
	var class gatewayv1.GatewayClass
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(liveGateway.Spec.GatewayClassName)}, &class); err != nil {
		return err
	}
	if class.Spec.ControllerName != r.Config.ControllerName || !class.DeletionTimestamp.IsZero() {
		return errGatewayChanged
	}
	current, err := ObserveListenerSets(ctx, r.APIReader, r.Config, &liveGateway)
	if err != nil {
		return err
	}
	if !projection.sameInputs(current) {
		return errGatewayChanged
	}
	for _, set := range current.sets {
		desired := set.object.DeepCopy()
		desired.Status.Listeners = nil
		accepted, programmed := metav1.ConditionFalse, metav1.ConditionFalse
		acceptedReason, programmedReason := gatewayv1.ListenerSetReasonNotAllowed, gatewayv1.ListenerSetReasonNotAllowed
		if set.allowed {
			acceptedReason, programmedReason = gatewayv1.ListenerSetReasonListenersNotValid, gatewayv1.ListenerSetReasonListenersNotValid
			ready := true
			for _, entry := range set.entries {
				status, err := r.listenerSetEntryStatus(ctx, set.object, entry, result)
				if err != nil {
					return err
				}
				desired.Status.Listeners = append(desired.Status.Listeners, status)
				if entry.rejection != "" {
					continue
				}
				accepted, acceptedReason = metav1.ConditionTrue, gatewayv1.ListenerSetReasonAccepted
				ready = ready && meta.IsStatusConditionTrue(status.Conditions, string(gatewayv1.ListenerEntryConditionProgrammed))
			}
			if accepted == metav1.ConditionTrue {
				if ready {
					programmed, programmedReason = metav1.ConditionTrue, gatewayv1.ListenerSetReasonProgrammed
				} else {
					programmedReason = "ParentNotProgrammed"
				}
			}
		}
		message := "ListenerSet " + string(acceptedReason) + " (controller " + string(r.Config.ControllerName) + ")"
		controller.SetCondition(&desired.Status.Conditions, controller.Condition(string(gatewayv1.ListenerSetConditionAccepted), accepted, string(acceptedReason), message, desired.Generation))
		message = "ListenerSet " + string(programmedReason) + " (controller " + string(r.Config.ControllerName) + ")"
		controller.SetCondition(&desired.Status.Conditions, controller.Condition(string(gatewayv1.ListenerSetConditionProgrammed), programmed, string(programmedReason), message, desired.Generation))
		if !equality.Semantic.DeepEqual(desired.Status, set.object.Status) {
			if err := r.Status().Patch(ctx, desired, controller.OptimisticMergeFrom(set.object)); err != nil {
				return client.IgnoreNotFound(err)
			}
		}
	}
	gateway.Status.AttachedListenerSets = ptr.To(current.count())
	return nil
}

func (r *Reconciler) listenerSetEntryStatus(ctx context.Context, set *gatewayv1.ListenerSet, entry listenerSetEntry, result cloud.GatewayGraphResult) (gatewayv1.ListenerEntryStatus, error) {
	listener := &entry.listener
	status := gatewayv1.ListenerEntryStatus{Name: listener.Name}
	if listener.Protocol == gatewayv1.HTTPProtocolType && listenerAllowsRouteKind(*listener, model.HTTPRouteKind) {
		status.SupportedKinds = supportedRouteKinds(model.HTTPRouteKind)
	}
	setAcceptedListenerConditions(&status.Conditions, set.Generation)
	if entry.rejection != "" {
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerEntryConditionAccepted), metav1.ConditionFalse, string(entry.rejection), "The listener cannot be configured on the parent Gateway", set.Generation))
	}
	if entry.conflicted {
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerEntryConditionConflicted), metav1.ConditionTrue, string(entry.rejection), "A higher precedence listener owns this endpoint", set.Generation))
	}
	if entry.invalidRouteKinds {
		controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerEntryConditionResolvedRefs), metav1.ConditionFalse, string(gatewayv1.ListenerEntryReasonInvalidRouteKinds), "The listener supports HTTPRoute on delegated HTTP listeners", set.Generation))
	}
	programmed, reason := metav1.ConditionFalse, gatewayv1.ListenerReasonPending
	if entry.rejection != "" {
		reason = entry.rejection
	} else {
		count, err := r.attachedListenerSetRoutes(ctx, set, listener)
		if err != nil {
			return status, err
		}
		status.AttachedRoutes = count
		if result.Gateway.AddressIssue == "" && slices.Contains(result.Gateway.ScopedListenerReadyPorts, int(listener.Port)) {
			programmed, reason = metav1.ConditionTrue, gatewayv1.ListenerReasonProgrammed
		}
	}
	controller.SetCondition(&status.Conditions, controller.Condition(string(gatewayv1.ListenerEntryConditionProgrammed), programmed, string(reason), "Listener "+string(reason), set.Generation))
	for _, previous := range set.Status.Listeners {
		if previous.Name == status.Name {
			conditions := slices.Clone(previous.Conditions)
			for _, condition := range status.Conditions {
				controller.SetCondition(&conditions, condition)
			}
			status.Conditions = conditions
			break
		}
	}
	return status, nil
}

func (r *Reconciler) attachedListenerSetRoutes(ctx context.Context, set *gatewayv1.ListenerSet, listener *gatewayv1.Listener) (int32, error) {
	key := client.ObjectKeyFromObject(set)
	var routes gatewayv1.HTTPRouteList
	if err := r.List(ctx, &routes, client.MatchingFields{controller.IndexHTTPRouteByListenerSet: key.String()}); err != nil {
		return 0, err
	}
	var count int32
	namespaces := make(map[string][]corev1.Namespace)
	for _, route := range routes.Items {
		if !route.DeletionTimestamp.IsZero() || len(model.IntersectHostnames(listener.Hostname, route.Spec.Hostnames)) == 0 {
			continue
		}
		if !slices.ContainsFunc(route.Status.Parents, func(parent gatewayv1.RouteParentStatus) bool {
			ref := parent.ParentRef
			if parent.ControllerName != r.Config.ControllerName || model.ListenerSetRefKey(ref, route.Namespace) != key ||
				ref.SectionName != nil && *ref.SectionName != listener.Name || ref.Port != nil && *ref.Port != listener.Port {
				return false
			}
			accepted := meta.FindStatusCondition(parent.Conditions, string(gatewayv1.RouteConditionAccepted))
			return accepted != nil && accepted.Status == metav1.ConditionTrue && accepted.ObservedGeneration == route.Generation &&
				slices.ContainsFunc(route.Spec.ParentRefs, func(candidate gatewayv1.ParentReference) bool {
					return controller.ParentRefsEqual(ref, route.Namespace, candidate, route.Namespace)
				})
		}) {
			continue
		}
		if _, observed := namespaces[route.Namespace]; !observed && model.ObservesRouteNamespace(listener) {
			namespace, err := graph.ReadRouteNamespace(ctx, r.APIReader, route.Namespace)
			if err != nil {
				return 0, err
			}
			namespaces[route.Namespace] = nil
			if namespace != nil {
				namespaces[route.Namespace] = []corev1.Namespace{*namespace}
			}
		}
		if model.AllowsRouteNamespace(listener, set.Namespace, route.Namespace, namespaces[route.Namespace]) {
			count++
		}
	}
	return count, nil
}
