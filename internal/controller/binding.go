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
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// ValidateGatewayBinding verifies the complete durable binding before lifecycle work.
func ValidateGatewayBinding(config Config, gateway *gatewayv1.Gateway) error {
	if err := ValidateGatewayIdentityBinding(config, gateway); err != nil {
		return err
	}
	if GatewayHasControllerBinding(config, gateway) {
		switch gateway.Annotations[config.GatewayListenerProtocolAnnotation()] {
		case "", string(gatewayv1.HTTPProtocolType), string(gatewayv1.TCPProtocolType), string(gatewayv1.UDPProtocolType):
		default:
			return fmt.Errorf("%w: invalid bound Gateway listener protocol", cloud.ErrOwnershipConflict)
		}
		if _, err := StoredGatewayTopology(config, gateway); err != nil {
			return err
		}
	}
	return nil
}

// ValidateGatewayIdentityBinding validates only the immutable ownership tuple.
// The advisory audit does not need provisioning parameters to inventory identity.
// This check alone never authorizes graph mutation or finalizer removal.
func ValidateGatewayIdentityBinding(config Config, gateway *gatewayv1.Gateway) error {
	bindingValues := []string{
		gateway.Annotations[config.GatewayListenerPortAnnotation()],
		gateway.Annotations[config.GatewayClusterIDAnnotation()],
		gateway.Annotations[config.GatewayProjectIDAnnotation()],
	}
	populatedBindingCount := 0
	for _, value := range bindingValues {
		if value != "" {
			populatedBindingCount++
		}
	}
	if populatedBindingCount != len(bindingValues) && GatewayHasControllerBinding(config, gateway) {
		return fmt.Errorf("gateway %s/%s has an incomplete stored OpenStack resource binding; restore the original annotations before reconciling", gateway.Namespace, gateway.Name)
	}
	if stored := gateway.Annotations[config.GatewayClusterIDAnnotation()]; stored != "" && stored != config.ClusterID {
		return fmt.Errorf("controller cluster identity differs from this Gateway's existing resource binding; restore the original cluster ID before reconciling")
	}
	if stored := gateway.Annotations[config.GatewayProjectIDAnnotation()]; stored != "" && stored != config.OpenStackProjectID {
		return fmt.Errorf("authenticated OpenStack project differs from this Gateway's existing resource binding; restore access to the original project before reconciling")
	}
	return nil
}

// GatewayHasControllerBinding reports whether the Gateway contains any part
// of this controller's durable binding.
func GatewayHasControllerBinding(config Config, gateway *gatewayv1.Gateway) bool {
	if controllerutil.ContainsFinalizer(gateway, config.GatewayFinalizer()) || HasGatewayTopologyBinding(config, gateway) {
		return true
	}
	for _, key := range []string{
		config.GatewayListenerPortAnnotation(),
		config.GatewayListenerProtocolAnnotation(),
		config.GatewayClusterIDAnnotation(),
		config.GatewayProjectIDAnnotation(),
	} {
		if gateway.Annotations[key] != "" {
			return true
		}
	}
	return false
}

// GatewayBindingMetadataEqual compares only Gateway metadata owned by this
// controller.
func GatewayBindingMetadataEqual(config Config, left, right *gatewayv1.Gateway) bool {
	if !GatewayTopologyMetadataEqual(config, left, right) {
		return false
	}
	if controllerutil.ContainsFinalizer(left, config.GatewayFinalizer()) !=
		controllerutil.ContainsFinalizer(right, config.GatewayFinalizer()) {
		return false
	}
	for _, key := range []string{
		config.GatewayListenerPortAnnotation(),
		config.GatewayListenerProtocolAnnotation(),
		config.GatewayClusterIDAnnotation(),
		config.GatewayProjectIDAnnotation(),
	} {
		if left.Annotations[key] != right.Annotations[key] {
			return false
		}
	}
	return true
}

// StoredGatewayIdentity returns the Gateway identity using durable binding
// values when they are present.
func StoredGatewayIdentity(config Config, gateway *gatewayv1.Gateway) cloud.Identity {
	identity := GatewayIdentity(config, gateway)
	if value := gateway.Annotations[config.GatewayClusterIDAnnotation()]; value != "" {
		identity.ClusterID = value
	}
	if value := gateway.Annotations[config.GatewayProjectIDAnnotation()]; value != "" {
		identity.OpenStackProjectID = value
	}
	return identity
}

// SameGatewayIdentity compares the Gateway portion of two cloud identities.
func SameGatewayIdentity(left, right cloud.Identity) bool {
	return left.GatewayNamespace == right.GatewayNamespace &&
		left.GatewayName == right.GatewayName &&
		left.GatewayUID == right.GatewayUID
}

// RouteClaimsGateway identifies complete or partial route ownership checkpoints
// for one Gateway. The caller validates the full identity before acting on them.
func RouteClaimsGateway(config Config, route client.Object, identity cloud.Identity) bool {
	boundUID := route.GetAnnotations()[config.RouteGatewayUIDAnnotation()]
	checkpoint := config.RouteBindingFinalizer(identity.ClusterID, identity.OpenStackProjectID, identity.GatewayNamespace, identity.GatewayName, identity.GatewayUID)
	return boundUID != "" && boundUID == identity.GatewayUID || slices.Contains(route.GetFinalizers(), checkpoint) ||
		boundUID == "" && slices.Contains(route.GetFinalizers(), config.RouteFinalizer()) &&
			route.GetAnnotations()[config.RouteGatewayNamespaceAnnotation()] == identity.GatewayNamespace &&
			route.GetAnnotations()[config.RouteGatewayNameAnnotation()] == identity.GatewayName
}

// StoredRouteIdentity validates and returns the Gateway identity recorded by
// a Route binding.
func StoredRouteIdentity(config Config, route client.Object) (cloud.Identity, bool, error) {
	annotations := route.GetAnnotations()
	values := []string{
		annotations[config.RouteGatewayNamespaceAnnotation()],
		annotations[config.RouteGatewayNameAnnotation()],
		annotations[config.RouteGatewayUIDAnnotation()],
	}
	empty := 0
	for _, value := range values {
		if value == "" {
			empty++
		}
	}
	if empty == len(values) {
		if annotations[config.RouteClusterIDAnnotation()] != "" || annotations[config.RouteProjectIDAnnotation()] != "" || hasRouteBindingFinalizer(config, route) {
			return cloud.Identity{}, false, fmt.Errorf("route %s/%s has an incomplete stored Gateway identity", route.GetNamespace(), route.GetName())
		}
		return cloud.Identity{}, false, nil
	}
	if empty != 0 {
		return cloud.Identity{}, false, fmt.Errorf("route %s/%s has an incomplete stored Gateway identity", route.GetNamespace(), route.GetName())
	}
	clusterID := annotations[config.RouteClusterIDAnnotation()]
	if clusterID == "" {
		clusterID = config.ClusterID
	}
	projectID := annotations[config.RouteProjectIDAnnotation()]
	if projectID == "" {
		projectID = config.OpenStackProjectID
	}
	identity := cloud.Identity{
		OpenStackProjectID: projectID,
		ClusterID:          clusterID,
		Controller:         string(config.ControllerName),
		ControllerVersion:  config.ControllerVersion,
		GatewayNamespace:   values[0],
		GatewayName:        values[1],
		GatewayUID:         values[2],
		RouteNamespace:     route.GetNamespace(),
		RouteName:          route.GetName(),
		RouteUID:           string(route.GetUID()),
	}
	if identity.ClusterID != config.ClusterID {
		return cloud.Identity{}, false, fmt.Errorf("controller cluster identity differs from Route %s/%s's existing resource binding; restore the original cluster ID before reconciling", route.GetNamespace(), route.GetName())
	}
	if identity.OpenStackProjectID != config.OpenStackProjectID {
		return cloud.Identity{}, false, fmt.Errorf("authenticated OpenStack project differs from Route %s/%s's existing resource binding; restore access to the original project before reconciling", route.GetNamespace(), route.GetName())
	}
	if err := cloud.ValidateRouteIdentity(identity); err != nil {
		return cloud.Identity{}, false, err
	}
	expectedBindingFinalizer := config.RouteBindingFinalizer(
		identity.ClusterID,
		identity.OpenStackProjectID,
		identity.GatewayNamespace,
		identity.GatewayName,
		identity.GatewayUID,
	)
	if controllerutil.ContainsFinalizer(route, config.RouteFinalizer()) && !controllerutil.ContainsFinalizer(route, expectedBindingFinalizer) {
		return cloud.Identity{}, false, fmt.Errorf("route %s/%s stored Gateway identity does not match its binding finalizer; restore the original binding before reconciling", route.GetNamespace(), route.GetName())
	}
	for _, finalizer := range route.GetFinalizers() {
		if strings.HasPrefix(finalizer, config.RouteBindingFinalizerPrefix()) && finalizer != expectedBindingFinalizer {
			return cloud.Identity{}, false, fmt.Errorf("route %s/%s has a conflicting Gateway binding finalizer", route.GetNamespace(), route.GetName())
		}
	}
	return identity, true, nil
}

// hasRouteBindingFinalizer reports whether any finalizer anchors a Route
// binding owned by this controller.
func hasRouteBindingFinalizer(config Config, route client.Object) bool {
	for _, finalizer := range route.GetFinalizers() {
		if strings.HasPrefix(finalizer, config.RouteBindingFinalizerPrefix()) {
			return true
		}
	}
	return false
}

// routeHasBindingAnnotations reports whether a Route contains any stored
// Gateway identity annotation owned by this controller.
func routeHasBindingAnnotations(config Config, route client.Object) bool {
	for _, key := range []string{
		config.RouteGatewayNamespaceAnnotation(),
		config.RouteGatewayNameAnnotation(),
		config.RouteGatewayUIDAnnotation(),
		config.RouteClusterIDAnnotation(),
		config.RouteProjectIDAnnotation(),
	} {
		if route.GetAnnotations()[key] != "" {
			return true
		}
	}
	return false
}

// RouteHasControllerBinding reports whether a Route contains any part of
// this controller's durable binding.
func RouteHasControllerBinding(config Config, route client.Object) bool {
	return controllerutil.ContainsFinalizer(route, config.RouteFinalizer()) ||
		hasRouteBindingFinalizer(config, route) ||
		routeHasBindingAnnotations(config, route)
}

// ApplyRouteBinding persists the supplied identity in the object's metadata. The caller
// checks authority, holds the Gateway lock and patches the observed revision.
func ApplyRouteBinding(config Config, route client.Object, id cloud.Identity) {
	annotations := route.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[config.RouteLifecycleAnnotation()] = RouteAttached
	annotations[config.RouteGatewayNamespaceAnnotation()] = id.GatewayNamespace
	annotations[config.RouteGatewayNameAnnotation()] = id.GatewayName
	annotations[config.RouteGatewayUIDAnnotation()] = id.GatewayUID
	annotations[config.RouteClusterIDAnnotation()] = id.ClusterID
	annotations[config.RouteProjectIDAnnotation()] = id.OpenStackProjectID
	delete(annotations, config.RouteCleanupFailureAnnotation())
	route.SetAnnotations(annotations)
	checkpoint := config.RouteBindingFinalizer(id.ClusterID, id.OpenStackProjectID, id.GatewayNamespace, id.GatewayName, id.GatewayUID)
	route.SetFinalizers(slices.DeleteFunc(route.GetFinalizers(), func(f string) bool {
		return f != checkpoint && strings.HasPrefix(f, config.RouteBindingFinalizerPrefix())
	}))
	controllerutil.AddFinalizer(route, checkpoint)
	controllerutil.AddFinalizer(route, config.RouteFinalizer())
}

// ClearRouteBinding releases only this controller's metadata after verified cloud cleanup.
func ClearRouteBinding(config Config, route client.Object) {
	route.SetFinalizers(slices.DeleteFunc(route.GetFinalizers(), func(f string) bool {
		return f == config.RouteFinalizer() || strings.HasPrefix(f, config.RouteBindingFinalizerPrefix())
	}))
	annotations := route.GetAnnotations()
	for _, key := range []string{config.RouteLifecycleAnnotation(), config.RouteGatewayNamespaceAnnotation(),
		config.RouteGatewayNameAnnotation(), config.RouteGatewayUIDAnnotation(), config.RouteClusterIDAnnotation(),
		config.RouteProjectIDAnnotation(), config.RouteCleanupFailureAnnotation()} {
		delete(annotations, key)
	}
	route.SetAnnotations(annotations)
}
