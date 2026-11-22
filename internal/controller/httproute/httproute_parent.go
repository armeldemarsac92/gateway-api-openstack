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
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type managedRouteParent struct {
	ref          gatewayv1.ParentReference
	gateway      *gatewayv1.Gateway
	gatewayClass *gatewayv1.GatewayClass
	listenerSet  *gatewayv1.ListenerSet
}

type routeParentObservation struct {
	managedRouteParent
	managed bool
}

func (r *Reconciler) managedParents(ctx context.Context, route *gatewayv1.HTTPRoute) ([]managedRouteParent, error) {
	return r.managedParentsWithReader(ctx, r.Client, route)
}

func (r *Reconciler) managedParentsWithReader(
	ctx context.Context,
	reader client.Reader,
	route *gatewayv1.HTTPRoute,
) ([]managedRouteParent, error) {
	observations, err := r.observeRouteParents(ctx, reader, route)
	if err != nil {
		return nil, err
	}
	return managedParentsFromObservations(observations), nil
}

func (r *Reconciler) observeRouteParents(ctx context.Context, reader client.Reader, route *gatewayv1.HTTPRoute) ([]routeParentObservation, error) {
	parents := make([]routeParentObservation, 0, len(route.Spec.ParentRefs))
	for _, parentRef := range route.Spec.ParentRefs {
		if !controller.IsGatewayParentRef(parentRef) && (model.HTTPProfileRouteKind(route) != model.HTTPRouteKind || model.ListenerSetRefKey(parentRef, route.Namespace).Name == "") {
			continue
		}
		parent, err := r.observeRouteParent(ctx, reader, route.Namespace, parentRef)
		if err != nil {
			return nil, err
		}
		parents = append(parents, parent)
	}
	return parents, nil
}

func (r *Reconciler) observeRouteParent(ctx context.Context, reader client.Reader, namespace string, ref gatewayv1.ParentReference) (routeParentObservation, error) {
	parent := routeParentObservation{managedRouteParent: managedRouteParent{ref: ref}}
	key := types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}
	if ref.Namespace != nil {
		key.Namespace = string(*ref.Namespace)
	}
	if setKey := model.ListenerSetRefKey(ref, namespace); setKey.Name != "" {
		var set gatewayv1.ListenerSet
		if err := reader.Get(ctx, setKey, &set); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				return parent, nil
			}
			return routeParentObservation{}, fmt.Errorf("get parent ListenerSet: %w", err)
		}
		key = model.ListenerSetParentKey(&set)
		parent.listenerSet = &set
		if key.Name == "" {
			return parent, nil
		}
	}
	var gateway gatewayv1.Gateway
	if err := reader.Get(ctx, key, &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return parent, nil
		}
		return routeParentObservation{}, fmt.Errorf("get parent Gateway: %w", err)
	}
	parent.gateway = &gateway
	var class gatewayv1.GatewayClass
	if err := reader.Get(ctx, types.NamespacedName{Name: string(gateway.Spec.GatewayClassName)}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return parent, nil
		}
		return routeParentObservation{}, fmt.Errorf("get parent GatewayClass: %w", err)
	}
	parent.gatewayClass = &class
	parent.managed = class.Spec.ControllerName == r.Config.ControllerName
	return parent, nil
}

func managedParentsFromObservations(observations []routeParentObservation) []managedRouteParent {
	parents := make([]managedRouteParent, 0, len(observations))
	for _, observation := range observations {
		if observation.managed {
			parents = append(parents, observation.managedRouteParent)
		}
	}
	return parents
}

// parentsForGateway validates that controller-owned parent references share
// one Gateway graph. The existing single-Gateway-parent profile is preserved
// for routes that never reference a ListenerSet.
func (r *Reconciler) parentsForGateway(ctx context.Context, route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway) ([]managedRouteParent, error) {
	var selected []managedRouteParent
	for _, ref := range route.Spec.ParentRefs {
		if routeTargetsGateway(route, ref, gateway.Namespace, gateway.Name) {
			selected = append(selected, managedRouteParent{ref: ref, gateway: gateway})
			continue
		}
		if !controller.IsGatewayParentRef(ref) && (model.HTTPProfileRouteKind(route) != model.HTTPRouteKind || model.ListenerSetRefKey(ref, route.Namespace).Name == "") {
			continue
		}
		parent, err := r.observeRouteParent(ctx, r.APIReader, route.Namespace, ref)
		if err != nil {
			return nil, err
		}
		if !parent.managed {
			continue
		}
		if parent.gateway.UID != gateway.UID || client.ObjectKeyFromObject(parent.gateway) != client.ObjectKeyFromObject(gateway) {
			return nil, nil
		}
		parent.gateway = gateway
		selected = append(selected, parent.managedRouteParent)
	}
	if len(selected) > 1 && !slices.ContainsFunc(selected, func(p managedRouteParent) bool { return p.listenerSet != nil }) {
		return nil, nil
	}
	return selected, nil
}

func (p managedRouteParent) namespace() string {
	if p.listenerSet != nil {
		return p.listenerSet.Namespace
	}
	return p.gateway.Namespace
}

// routeParentCandidates applies the parentRef selectors before reading namespace
// authorization. Both selectors must match the same logical section.
func routeParentCandidates(route *gatewayv1.HTTPRoute, parent managedRouteParent, http []*gatewayv1.Listener) ([]*gatewayv1.Listener, error) {
	if parent.gateway.DeletionTimestamp != nil {
		return nil, newRouteBuildError(routeErrorPending, "parent Gateway is being deleted")
	}
	selects := func(listener gatewayv1.Listener) bool {
		return (parent.ref.SectionName == nil || *parent.ref.SectionName == listener.Name) &&
			(parent.ref.Port == nil || *parent.ref.Port == listener.Port)
	}
	if parent.listenerSet != nil {
		if !slices.ContainsFunc(parent.listenerSet.Spec.Listeners, func(entry gatewayv1.ListenerEntry) bool {
			return (parent.ref.SectionName == nil || *parent.ref.SectionName == entry.Name) && (parent.ref.Port == nil || *parent.ref.Port == entry.Port)
		}) {
			return nil, newRouteBuildError(routeErrorNoMatchingParent, "parentRef does not select a ListenerSet section")
		}
	} else if !slices.ContainsFunc(parent.gateway.Spec.Listeners, selects) {
		return nil, newRouteBuildError(routeErrorNoMatchingParent, "parentRef does not select a Gateway listener")
	}
	kind := model.HTTPProfileRouteKind(route)
	if parent.listenerSet == nil && len(http) == 0 && slices.ContainsFunc(parent.gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
		return selects(listener) && (listener.Protocol == gatewayv1.HTTPProtocolType || listener.Protocol == gatewayv1.HTTPSProtocolType)
	}) {
		return nil, newRouteBuildError(routeErrorUnsupported, "parent Gateway does not have a supported HTTP listener configuration")
	}
	var candidates []*gatewayv1.Listener
	for _, listener := range http {
		if !selects(*listener) {
			continue
		}
		if kind == model.GRPCRouteKind && listener.Protocol != gatewayv1.HTTPProtocolType {
			continue
		}
		if listener.Protocol != gatewayv1.HTTPProtocolType && listener.Protocol != gatewayv1.HTTPSProtocolType {
			continue
		}
		if listener.Protocol == gatewayv1.HTTPProtocolType && listener.TLS != nil {
			return nil, newRouteBuildError(routeErrorUnsupported, "parent listener does not support HTTPRoute")
		}
		if listener.AllowedRoutes != nil && len(listener.AllowedRoutes.Kinds) != 0 &&
			!slices.ContainsFunc(listener.AllowedRoutes.Kinds, func(ref gatewayv1.RouteGroupKind) bool {
				return ref.Kind == kind && (ref.Group == nil || *ref.Group == gatewayv1.GroupName)
			}) {
			continue
		}
		candidates = append(candidates, listener)
	}
	if len(candidates) == 0 {
		return nil, newRouteBuildError(routeErrorNotAllowed, "selected listener does not support %s", kind)
	}
	return candidates, nil
}

func routeTargetsGateway(route *gatewayv1.HTTPRoute, parent gatewayv1.ParentReference, namespace, name string) bool {
	parentNamespace := route.Namespace
	if parent.Namespace != nil {
		parentNamespace = string(*parent.Namespace)
	}
	return controller.IsGatewayParentRef(parent) && parentNamespace == namespace && string(parent.Name) == name
}

func (r *Reconciler) selectRouteAttachments(route *gatewayv1.HTTPRoute, parent managedRouteParent, candidates []*gatewayv1.Listener, namespaces []corev1.Namespace) ([]*gatewayv1.Listener, error) {
	var selected []*gatewayv1.Listener
	rejection := newRouteBuildError(routeErrorNotAllowed, "listener allowedRoutes does not allow this HTTPRoute namespace")
	for _, listener := range candidates {
		if !model.AllowsRouteNamespace(listener, parent.namespace(), route.Namespace, namespaces) {
			continue
		}
		if err := validateRouteHostnames(listener.Hostname, route.Spec.Hostnames); err != nil {
			rejection = err
			continue
		}
		if len(selected) != 0 && !gatewaycontroller.HTTPRouteSectionsSharePort(r.Config, parent.gateway, selected[0], listener) {
			return nil, newRouteBuildError(routeErrorUnsupported, "the selected sections must share a physical HTTP or HTTPS listener")
		}
		if err := validateRouteRedirectPrefixes(route, *listener); err != nil {
			return nil, err
		}
		selected = append(selected, listener)
	}
	if selected == nil {
		return nil, rejection
	}
	return selected, nil
}

func readRouteNamespaces(ctx context.Context, reader client.Reader, candidates []*gatewayv1.Listener, routeNamespace string) ([]corev1.Namespace, error) {
	if !slices.ContainsFunc(candidates, model.ObservesRouteNamespace) {
		return nil, nil
	}
	if reader == nil {
		return nil, controller.ErrAPIReaderRequired
	}
	namespace, err := graph.ReadRouteNamespace(ctx, reader, routeNamespace)
	if err != nil || namespace == nil {
		return nil, err
	}
	return []corev1.Namespace{*namespace}, nil
}

func (r *Reconciler) boundRouteAttachments(route *gatewayv1.HTTPRoute, gateway *gatewayv1.Gateway, http []*gatewayv1.Listener, namespaces []corev1.Namespace) ([]*gatewayv1.Listener, error) {
	rejection := newRouteBuildError(routeErrorNoMatchingParent, "HTTPRoute no longer targets the bound Gateway")
	for _, ref := range route.Spec.ParentRefs {
		if !routeTargetsGateway(route, ref, gateway.Namespace, gateway.Name) {
			continue
		}
		parent := managedRouteParent{ref: ref, gateway: gateway}
		candidates, err := routeParentCandidates(route, parent, http)
		if err == nil {
			var listeners []*gatewayv1.Listener
			listeners, err = r.selectRouteAttachments(route, parent, candidates, namespaces)
			if err == nil {
				return listeners, nil
			}
		}
		rejection = err
	}
	return nil, rejection
}

type boundHTTPAttachment struct {
	listeners         []*gatewayv1.Listener
	gatewayGeneration int64
}

// liveBoundRouteAttachment observes the authorization for established
// forwarding independently of GatewayClass and bundle readiness.
func (r *Reconciler) liveBoundRouteAttachment(ctx context.Context, route *gatewayv1.HTTPRoute, identity cloud.Identity) (attachment boundHTTPAttachment, rejection, readErr error) {
	if model.HTTPProfileRouteKind(route) == model.GRPCRouteKind && !r.Config.GRPCExactMethodEnabled() {
		return boundHTTPAttachment{}, grpcProfileDisabled(), nil
	}
	var gateway gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: identity.GatewayNamespace, Name: identity.GatewayName}, &gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return boundHTTPAttachment{}, newRouteBuildError(routeErrorNoMatchingParent, "the bound Gateway no longer exists"), nil
		}
		return boundHTTPAttachment{}, nil, fmt.Errorf("get bound Gateway attachment policy: %w", err)
	}
	if string(gateway.UID) != identity.GatewayUID {
		return boundHTTPAttachment{}, newRouteBuildError(routeErrorNoMatchingParent, "the bound Gateway was replaced"), nil
	}
	parents, err := r.parentsForGateway(ctx, route, &gateway)
	if err != nil {
		return boundHTTPAttachment{}, nil, err
	}
	sets, err := gatewaycontroller.ObserveListenerSets(ctx, r.APIReader, r.Config, &gateway)
	if err != nil {
		return boundHTTPAttachment{}, nil, err
	}
	selection, _, err := r.selectRouteParents(ctx, route, parents, sets.Sections())
	if err != nil {
		return boundHTTPAttachment{}, nil, err
	}
	selected := selection.listeners()
	if len(selected) == 0 {
		if len(selection) != 0 {
			return boundHTTPAttachment{}, selection[0].rejection, nil
		}
		return boundHTTPAttachment{}, newRouteBuildError(routeErrorNoMatchingParent, "HTTPRoute no longer targets an available listener on the bound Gateway"), nil
	}
	return boundHTTPAttachment{listeners: selected, gatewayGeneration: gateway.Generation}, nil, nil
}

type routeParentSelection struct {
	parent    managedRouteParent
	listeners []*gatewayv1.Listener
	rejection error
}

func (p routeParentSelection) section(name gatewayv1.SectionName) model.ListenerSection {
	section := model.ListenerSection{Name: name}
	if p.parent.listenerSet != nil {
		section.ListenerSet = client.ObjectKeyFromObject(p.parent.listenerSet)
	}
	return section
}

type routeSelection []routeParentSelection

func (s routeSelection) withoutProfileConflicts(rejected []model.ListenerSection) routeSelection {
	if len(rejected) == 0 {
		return s
	}
	selected := slices.Clone(s)
	for i := range selected {
		parent := &selected[i]
		if parent.rejection != nil {
			continue
		}
		parent.listeners = slices.DeleteFunc(slices.Clone(parent.listeners), func(listener *gatewayv1.Listener) bool {
			return slices.Contains(rejected, parent.section(listener.Name))
		})
		if len(parent.listeners) == 0 {
			parent.rejection = routeProfileConflict()
		}
	}
	return selected
}

func (s routeSelection) listeners() []*gatewayv1.Listener {
	var listeners []*gatewayv1.Listener
	for _, parent := range s {
		if parent.rejection == nil {
			for _, listener := range parent.listeners {
				if !slices.ContainsFunc(listeners, func(previous *gatewayv1.Listener) bool {
					return listenerSectionIdentity(*previous) == listenerSectionIdentity(*listener)
				}) {
					listeners = append(listeners, listener)
				}
			}
		}
	}
	return listeners
}

func (r *Reconciler) selectRouteParents(ctx context.Context, route *gatewayv1.HTTPRoute, parents []managedRouteParent, sets map[types.NamespacedName][]gatewayv1.Listener) (routeSelection, []corev1.Namespace, error) {
	return r.selectRouteParentsWithNamespaces(ctx, route, parents, sets, make(map[string][]corev1.Namespace))
}

func (r *Reconciler) selectRouteParentsWithNamespaces(ctx context.Context, route *gatewayv1.HTTPRoute, parents []managedRouteParent, sets map[types.NamespacedName][]gatewayv1.Listener, cache map[string][]corev1.Namespace) (routeSelection, []corev1.Namespace, error) {
	var selection routeSelection
	if len(parents) == 0 {
		return nil, nil, nil
	}
	gateway := parents[0].gateway
	http, _ := gatewaycontroller.HTTPRouteListeners(r.Config, gateway)
	namespaces, namespaceObserved := cache[route.Namespace]
	for _, parent := range parents {
		available := http
		if parent.listenerSet != nil {
			available = nil
			for _, section := range sets[client.ObjectKeyFromObject(parent.listenerSet)] {
				available = append(available, &section)
			}
		}
		candidates, rejection := routeParentCandidates(route, parent, available)
		var selected []*gatewayv1.Listener
		if rejection == nil {
			if !namespaceObserved && slices.ContainsFunc(candidates, model.ObservesRouteNamespace) {
				var err error
				namespaces, err = readRouteNamespaces(ctx, r.APIReader, candidates, route.Namespace)
				if err != nil {
					return nil, nil, err
				}
				namespaceObserved = true
				cache[route.Namespace] = namespaces
			}
			selected, rejection = r.selectRouteAttachments(route, parent, candidates, namespaces)
		}
		selection = append(selection, routeParentSelection{parent: parent, listeners: selected, rejection: rejection})
	}
	listeners := selection.listeners()
	if len(listeners) > 1 && slices.ContainsFunc(listeners[1:], func(listener *gatewayv1.Listener) bool {
		return !gatewaycontroller.HTTPRouteSectionsSharePort(r.Config, gateway, listeners[0], listener)
	}) {
		for i := range selection {
			if selection[i].rejection == nil {
				selection[i].rejection = newRouteBuildError(routeErrorUnsupported, "the selected sections must share a physical HTTP or HTTPS listener")
			}
		}
	}
	return selection, namespaces, nil
}

type sectionIdentity struct {
	name     gatewayv1.SectionName
	port     gatewayv1.PortNumber
	protocol gatewayv1.ProtocolType
	hostname string
}

func listenerSectionIdentity(listener gatewayv1.Listener) sectionIdentity {
	result := sectionIdentity{name: listener.Name, port: listener.Port, protocol: listener.Protocol}
	if listener.Hostname != nil {
		result.hostname = string(*listener.Hostname)
	}
	return result
}

func routeProfileConflict() error {
	return newRouteBuildError(routeErrorNotAllowed, "an older Route of a different kind owns an overlapping hostname on this listener")
}

func grpcProfileDisabled() error {
	return newRouteBuildError(routeErrorUnsupported, "GRPCRoute requires Octavia backend h2c and fixed responses")
}

func validateRouteHostnames(listener *gatewayv1.Hostname, hostnames []gatewayv1.Hostname) error {
	if listener != nil {
		if err := amphora.ValidateHostname(string(*listener)); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid listener hostname: %s", err)
		}
	}
	for _, hostname := range hostnames {
		if err := amphora.ValidateHostname(string(hostname)); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "invalid HTTPRoute hostname: %s", err)
		}
	}

	effective := model.IntersectHostnames(listener, hostnames)
	if len(effective) == 0 {
		return newRouteBuildError(routeErrorNoMatchingHostname, "HTTPRoute and listener hostnames do not intersect")
	}
	for _, hostname := range effective {
		if hostname == "" {
			continue
		}
		if _, err := amphora.HostnameRuleValue(hostname); err != nil {
			return newRouteBuildError(routeErrorUnsupported, "%s", err)
		}
	}
	return nil
}
