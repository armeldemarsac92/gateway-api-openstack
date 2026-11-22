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

package controller

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// RouteGatewayBinding describes the ownership checkpoint for one route fragment.
type RouteGatewayBinding struct {
	Identity  cloud.Identity
	Lifecycle string
	Anchor    int
	// ExplicitScope records whether the cluster and project were both persisted.
	ExplicitScope bool
}

type persistedRouteGatewayBinding struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	Cluster   string `json:"cluster"`
	Project   string `json:"project"`
	Lifecycle string `json:"lifecycle"`
	Anchor    int    `json:"anchor,omitempty"`
}

// RouteGatewayBindingsAnnotation records multiple HTTP-profile bindings after enrollment.
func (c Config) RouteGatewayBindingsAnnotation() string {
	return c.Domain() + "/route-gateway-bindings-" + c.controllerKey()
}

// HasRouteGatewayBindings reports whether an HTTP-profile route has the multi-parent checkpoint.
func HasRouteGatewayBindings(config Config, route client.Object) bool {
	_, exists := route.GetAnnotations()[config.RouteGatewayBindingsAnnotation()]
	return exists && scopedRouteProtocol(route) == gatewayv1.HTTPProtocolType
}

func unvalidatedRouteGatewayBindings(config Config, route client.Object) []persistedRouteGatewayBinding {
	if !HasRouteGatewayBindings(config, route) {
		return nil
	}
	var bindings []persistedRouteGatewayBinding
	_ = json.Unmarshal([]byte(route.GetAnnotations()[config.RouteGatewayBindingsAnnotation()]), &bindings)
	return bindings
}

// RouteBoundGatewayKeys maps durable claims for watches. Mutation callers must
// validate those claims with StoredRouteGatewayBinding.
func RouteBoundGatewayKeys(config Config, route client.Object) (keys []types.NamespacedName, uids []string) {
	annotations := route.GetAnnotations()
	appendKey := func(namespace, name, uid string) {
		if namespace != "" && name != "" && uid != "" {
			keys = append(keys, types.NamespacedName{Namespace: namespace, Name: name})
			uids = append(uids, uid)
		}
	}
	appendKey(annotations[config.RouteGatewayNamespaceAnnotation()], annotations[config.RouteGatewayNameAnnotation()], annotations[config.RouteGatewayUIDAnnotation()])
	for _, binding := range unvalidatedRouteGatewayBindings(config, route) {
		appendKey(binding.Namespace, binding.Name, binding.UID)
	}
	return keys, uids
}

// StoredRouteGatewayBindings validates the legacy singleton or the list of
// independently anchored Gateway fragments before returning any authority.
func StoredRouteGatewayBindings(config Config, route client.Object) ([]RouteGatewayBinding, error) {
	annotations := route.GetAnnotations()
	if !HasRouteGatewayBindings(config, route) {
		identity, present, err := StoredRouteIdentity(config, route)
		if err != nil || !present {
			return nil, err
		}
		return []RouteGatewayBinding{{
			Identity:      identity,
			Lifecycle:     annotations[config.RouteLifecycleAnnotation()],
			Anchor:        ScopedRouteAnchor(config, route, scopedRouteProtocol(route)),
			ExplicitScope: annotations[config.RouteClusterIDAnnotation()] != "" && annotations[config.RouteProjectIDAnnotation()] != "",
		}}, nil
	}

	invalid := fmt.Errorf("%w: invalid Gateway bindings for route %s/%s", cloud.ErrOwnershipConflict, route.GetNamespace(), route.GetName())
	if routeHasBindingAnnotations(config, route) ||
		hasScopedRouteAnchor(config, route) || annotations[config.RouteLifecycleAnnotation()] != "" ||
		!controllerutil.ContainsFinalizer(route, config.RouteFinalizer()) {
		return nil, invalid
	}
	var encoded []persistedRouteGatewayBinding
	if err := json.Unmarshal([]byte(annotations[config.RouteGatewayBindingsAnnotation()]), &encoded); err != nil || len(encoded) == 0 {
		return nil, invalid
	}
	bindings := make([]RouteGatewayBinding, 0, len(encoded))
	checkpoints := make(map[string]struct{}, len(encoded))
	uids := make(map[string]struct{}, len(encoded))
	for _, item := range encoded {
		identity := cloud.Identity{
			Controller: string(config.ControllerName), ControllerVersion: config.ControllerVersion,
			ClusterID: item.Cluster, OpenStackProjectID: item.Project,
			GatewayNamespace: item.Namespace, GatewayName: item.Name, GatewayUID: item.UID,
			RouteNamespace: route.GetNamespace(), RouteName: route.GetName(), RouteUID: string(route.GetUID()),
		}
		if item.Cluster != config.ClusterID || item.Project != config.OpenStackProjectID ||
			(item.Lifecycle != RouteAttached && item.Lifecycle != RouteDetaching) ||
			item.Anchor < 0 || item.Anchor > 65535 || cloud.ValidateRouteIdentity(identity) != nil {
			return nil, invalid
		}
		if _, exists := uids[item.UID]; exists {
			return nil, invalid
		}
		uids[item.UID] = struct{}{}
		binding := RouteGatewayBinding{Identity: identity, Lifecycle: item.Lifecycle, Anchor: item.Anchor, ExplicitScope: true}
		checkpoint := routeGatewayBindingFinalizer(config, route, binding)
		if !controllerutil.ContainsFinalizer(route, checkpoint) {
			return nil, invalid
		}
		checkpoints[checkpoint] = struct{}{}
		bindings = append(bindings, binding)
	}
	for _, finalizer := range route.GetFinalizers() {
		if _, exists := checkpoints[finalizer]; !exists && strings.HasPrefix(finalizer, config.RouteBindingFinalizerPrefix()) {
			return nil, invalid
		}
	}
	return bindings, nil
}

// StoredRouteGatewayBinding selects one binding by its Gateway identity.
func StoredRouteGatewayBinding(config Config, route client.Object, gateway cloud.Identity) (RouteGatewayBinding, error) {
	bindings, err := StoredRouteGatewayBindings(config, route)
	if err != nil {
		return RouteGatewayBinding{}, err
	}
	for _, binding := range bindings {
		if SameGatewayIdentity(binding.Identity, gateway) {
			return binding, nil
		}
	}
	return RouteGatewayBinding{}, nil
}

// RouteGatewayBindingSealed checks the route finalizer and this fragment's checkpoint.
func RouteGatewayBindingSealed(config Config, route client.Object, binding RouteGatewayBinding) bool {
	return controllerutil.ContainsFinalizer(route, config.RouteFinalizer()) &&
		controllerutil.ContainsFinalizer(route, routeGatewayBindingFinalizer(config, route, binding))
}

func routeGatewayBindingFinalizer(config Config, route client.Object, binding RouteGatewayBinding) string {
	id := binding.Identity
	checkpoint := config.RouteBindingFinalizer(id.ClusterID, id.OpenStackProjectID, id.GatewayNamespace, id.GatewayName, id.GatewayUID)
	if binding.Anchor != 0 {
		return routeBindingFinalizerWithAnchor(route, scopedRouteProtocol(route), checkpoint, strconv.Itoa(binding.Anchor))
	}
	return checkpoint
}

// ApplyHTTPRouteGatewayBinding updates or appends one checkpoint. The first
// binding retains its compatible singleton representation until a second is added.
func ApplyHTTPRouteGatewayBinding(config Config, route client.Object, bindings []RouteGatewayBinding, binding RouteGatewayBinding) error {
	if !HasRouteGatewayBindings(config, route) &&
		(len(bindings) == 0 || len(bindings) == 1 && SameGatewayIdentity(bindings[0].Identity, binding.Identity)) {
		ApplyRouteBinding(config, route, binding.Identity)
		route.GetAnnotations()[config.RouteLifecycleAnnotation()] = binding.Lifecycle
		if binding.Anchor != 0 {
			SealRouteAnchor(config, route, binding.Identity, binding.Anchor)
		} else {
			delete(route.GetAnnotations(), config.RouteListenerPortAnnotation(gatewayv1.HTTPProtocolType))
		}
		return nil
	}
	// Enrollment must never infer a missing durable checkpoint for an older parent.
	for _, existing := range bindings {
		if existing.Lifecycle != RouteAttached && existing.Lifecycle != RouteDetaching ||
			!RouteGatewayBindingSealed(config, route, existing) {
			return fmt.Errorf("%w: incomplete HTTP route binding checkpoint", cloud.ErrOwnershipConflict)
		}
	}
	index := slices.IndexFunc(bindings, func(existing RouteGatewayBinding) bool {
		return SameGatewayIdentity(existing.Identity, binding.Identity)
	})
	if index < 0 {
		bindings = append(bindings, binding)
	} else {
		bindings[index] = binding
	}
	return storeRouteGatewayBindings(config, route, bindings)
}

func storeRouteGatewayBindings(config Config, route client.Object, bindings []RouteGatewayBinding) error {
	var encoded []persistedRouteGatewayBinding
	for _, binding := range bindings {
		id := binding.Identity
		encoded = append(encoded, persistedRouteGatewayBinding{
			Namespace: id.GatewayNamespace, Name: id.GatewayName, UID: id.GatewayUID,
			Cluster: id.ClusterID, Project: id.OpenStackProjectID, Lifecycle: binding.Lifecycle, Anchor: binding.Anchor,
		})
	}
	contents, err := json.Marshal(encoded)
	if err != nil {
		return err
	}
	// Only the singleton format identifies the owner of an unscoped reason.
	// The multi-parent format keeps one diagnostic per Gateway UID.
	var legacyFailure, legacyGatewayUID string
	if !HasRouteGatewayBindings(config, route) {
		legacyFailure = route.GetAnnotations()[config.RouteCleanupFailureAnnotation()]
		legacyGatewayUID = route.GetAnnotations()[config.RouteGatewayUIDAnnotation()]
	}
	ClearRouteBinding(config, route)
	delete(route.GetAnnotations(), config.RouteGatewayBindingsAnnotation())
	if len(bindings) == 0 {
		return nil
	}
	annotations := route.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[config.RouteGatewayBindingsAnnotation()] = string(contents)
	for _, binding := range bindings {
		controllerutil.AddFinalizer(route, routeGatewayBindingFinalizer(config, route, binding))
		if legacyFailure != "" && binding.Identity.GatewayUID == legacyGatewayUID {
			annotations[config.RouteGatewayCleanupFailureAnnotation(legacyGatewayUID)] = legacyFailure
		}
	}
	route.SetAnnotations(annotations)
	controllerutil.AddFinalizer(route, config.RouteFinalizer())
	return nil
}

// RemoveRouteGatewayBinding releases a fragment after provider cleanup.
func RemoveRouteGatewayBinding(config Config, route client.Object, bindings []RouteGatewayBinding, identity cloud.Identity) error {
	if !HasRouteGatewayBindings(config, route) {
		ClearRouteBinding(config, route)
		return nil
	}
	bindings = slices.DeleteFunc(bindings, func(binding RouteGatewayBinding) bool { return binding.Identity == identity })
	if err := storeRouteGatewayBindings(config, route, bindings); err != nil {
		return err
	}
	delete(route.GetAnnotations(), config.RouteGatewayCleanupFailureAnnotation(identity.GatewayUID))
	return nil
}

// SealRouteGatewayAnchor updates only the checkpoint for the enrolling Gateway.
func SealRouteGatewayAnchor(config Config, route client.Object, identity cloud.Identity, port int) error {
	if !HasRouteGatewayBindings(config, route) {
		SealRouteAnchor(config, route, identity, port)
		return nil
	}
	bindings, err := StoredRouteGatewayBindings(config, route)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if SameGatewayIdentity(identity, binding.Identity) {
			binding.Anchor = port
			return ApplyHTTPRouteGatewayBinding(config, route, bindings, binding)
		}
	}
	return fmt.Errorf("%w: route has no checkpoint for the enrolling Gateway", cloud.ErrOwnershipConflict)
}

// MarkRouteGatewayDetaching records the deletion boundary only for the
// authorized fragment and leaves other bindings available to their graphs.
func MarkRouteGatewayDetaching(config Config, route client.Object, identity cloud.Identity) error {
	if !HasRouteGatewayBindings(config, route) {
		route.GetAnnotations()[config.RouteLifecycleAnnotation()] = RouteDetaching
		return nil
	}
	bindings, err := StoredRouteGatewayBindings(config, route)
	if err != nil {
		return err
	}
	for i := range bindings {
		if bindings[i].Identity == identity {
			bindings[i].Lifecycle = RouteDetaching
			return storeRouteGatewayBindings(config, route, bindings)
		}
	}
	return fmt.Errorf("%w: route has no checkpoint for the detaching Gateway", cloud.ErrOwnershipConflict)
}
