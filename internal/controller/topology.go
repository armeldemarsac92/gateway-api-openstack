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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
)

// GatewayTopologyAnnotation records the resolved class configuration in the
// durable Gateway binding. It contains infrastructure IDs, never credentials.
func (c Config) GatewayTopologyAnnotation() string {
	return c.Domain() + "/gateway-topology-" + c.controllerKey()
}

// GatewayTopologyFinalizerPrefix identifies the topology integrity finalizer.
func (c Config) GatewayTopologyFinalizerPrefix() string {
	return c.Domain() + "/gateway-topology-" + c.controllerKey() + "-"
}

// GatewayTopologyFinalizer binds the exact serialized topology to metadata
// persisted before cloud creation. Like other binding finalizers, this detects
// accidental edits but is not a security boundary against finalizer editors.
func (c Config) GatewayTopologyFinalizer(value string) string {
	digest := sha256.Sum256([]byte(value))
	return c.GatewayTopologyFinalizerPrefix() + fmt.Sprintf("%x", digest[:16])
}

// BindGatewayTopology records a new resolved topology and its integrity
// finalizer. A bound Gateway never changes its infrastructure through this helper.
// The caller owns the object and must persist the patch before cloud mutation.
func BindGatewayTopology(config Config, gateway *gatewayv1.Gateway, resolved classconfig.Resolved) error {
	if err := validateStoredTopology(resolved); err != nil {
		return err
	}
	if HasGatewayTopologyBinding(config, gateway) {
		existing, err := StoredGatewayTopology(config, gateway)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(existing, resolved) {
			return fmt.Errorf("%w: Gateway topology binding is immutable", cloud.ErrOwnershipConflict)
		}
		return nil
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		return fmt.Errorf("encode Gateway topology: %w", err)
	}
	if gateway.Annotations == nil {
		gateway.Annotations = map[string]string{}
	}
	gateway.Annotations[config.GatewayTopologyAnnotation()] = string(encoded)
	controllerutil.AddFinalizer(gateway, config.GatewayTopologyFinalizer(string(encoded)))
	return nil
}

// StoredGatewayTopology validates the complete persisted topology without
// reading a GatewayClass, parameter object, or cloud resource.
func StoredGatewayTopology(config Config, gateway *gatewayv1.Gateway) (classconfig.Resolved, error) {
	raw := gateway.Annotations[config.GatewayTopologyAnnotation()]
	finalizers := topologyFinalizers(config, gateway)
	if raw == "" || len(finalizers) != 1 || finalizers[0] != gatewayTopologyCheckpoint(config, gateway, raw) {
		return classconfig.Resolved{}, fmt.Errorf("%w: Gateway topology binding is missing, incomplete, or changed", cloud.ErrOwnershipConflict)
	}
	var resolved classconfig.Resolved
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&resolved); err != nil {
		return classconfig.Resolved{}, fmt.Errorf("%w: Gateway topology binding is invalid", cloud.ErrOwnershipConflict)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return classconfig.Resolved{}, fmt.Errorf("%w: Gateway topology binding contains trailing data", cloud.ErrOwnershipConflict)
	}
	if err := validateStoredTopology(resolved); err != nil {
		return classconfig.Resolved{}, err
	}
	return resolved, nil
}

const gatewayListenerModeV1 = "v1"

// GatewayListenerScope returns the enrolled protocol after binding validation.
func GatewayListenerScope(config Config, gateway *gatewayv1.Gateway) gatewayv1.ProtocolType {
	for _, protocol := range []gatewayv1.ProtocolType{gatewayv1.TCPProtocolType, gatewayv1.UDPProtocolType} {
		if gateway.Annotations[config.GatewayListenerModeAnnotation(protocol)] == gatewayListenerModeV1 {
			return protocol
		}
	}
	return ""
}

// HasGatewayListenerScopeBinding detects even a partial scoped checkpoint.
func HasGatewayListenerScopeBinding(config Config, gateway *gatewayv1.Gateway) bool {
	_, tcp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.TCPProtocolType)]
	_, udp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.UDPProtocolType)]
	return tcp || udp || slices.ContainsFunc(gateway.Finalizers, func(finalizer string) bool {
		return strings.HasPrefix(finalizer, config.GatewayTopologyFinalizerPrefix()+"u")
	})
}

// SetGatewayListenerScope changes the mode and its integrity checkpoint
// together. Callers validate the original binding before changing its metadata.
func SetGatewayListenerScope(config Config, gateway *gatewayv1.Gateway, protocol gatewayv1.ProtocolType) {
	if protocol == GatewayListenerScope(config, gateway) {
		return
	}
	for _, finalizer := range topologyFinalizers(config, gateway) {
		controllerutil.RemoveFinalizer(gateway, finalizer)
	}
	for _, transport := range []gatewayv1.ProtocolType{gatewayv1.TCPProtocolType, gatewayv1.UDPProtocolType} {
		delete(gateway.Annotations, config.GatewayListenerModeAnnotation(transport))
	}
	if protocol != "" {
		gateway.Annotations[config.GatewayListenerModeAnnotation(protocol)] = gatewayListenerModeV1
	}
	raw := gateway.Annotations[config.GatewayTopologyAnnotation()]
	controllerutil.AddFinalizer(gateway, gatewayTopologyCheckpoint(config, gateway, raw))
}

func gatewayTopologyCheckpoint(config Config, gateway *gatewayv1.Gateway, raw string) string {
	mode, udp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.UDPProtocolType)]
	tcpMode, tcp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.TCPProtocolType)]
	if !udp && !tcp {
		return config.GatewayTopologyFinalizer(raw)
	}
	// Retain the existing UDP digest. The distinct TCP format uses the same
	// leading u that S7 checks to require topology validation before route cleanup.
	prefix := "u"
	if tcp {
		mode, prefix = tcpMode, "ut"
	}
	input := strings.Join([]string{
		raw, mode,
		gateway.Namespace, gateway.Name, string(gateway.UID),
		gateway.Annotations[config.GatewayClusterIDAnnotation()], gateway.Annotations[config.GatewayProjectIDAnnotation()],
		gateway.Annotations[config.GatewayListenerProtocolAnnotation()], gateway.Annotations[config.GatewayListenerPortAnnotation()],
	}, "\x00")
	digest := sha256.Sum256([]byte(input))
	return config.GatewayTopologyFinalizerPrefix() + prefix + fmt.Sprintf("%x", digest[:15])
}

func validateGatewayListenerScope(config Config, gateway *gatewayv1.Gateway) error {
	_, tcp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.TCPProtocolType)]
	_, udp := gateway.Annotations[config.GatewayListenerModeAnnotation(gatewayv1.UDPProtocolType)]
	if !tcp && !udp {
		return nil
	}
	protocol := GatewayListenerScope(config, gateway)
	port, err := strconv.Atoi(gateway.Annotations[config.GatewayListenerPortAnnotation()])
	if tcp && udp || protocol == "" || gateway.Annotations[config.GatewayListenerProtocolAnnotation()] != string(protocol) ||
		err != nil || port < 1 || port > 65535 || !controllerutil.ContainsFinalizer(gateway, config.GatewayFinalizer()) {
		return fmt.Errorf("%w: Gateway listener mode binding is invalid", cloud.ErrOwnershipConflict)
	}
	return nil
}

// HasGatewayTopologyBinding detects any component of the persisted topology.
func HasGatewayTopologyBinding(config Config, gateway *gatewayv1.Gateway) bool {
	_, exists := gateway.Annotations[config.GatewayTopologyAnnotation()]
	return exists || len(topologyFinalizers(config, gateway)) != 0
}

// GatewayTopologyMetadataEqual compares only this controller's topology binding.
func GatewayTopologyMetadataEqual(config Config, left, right *gatewayv1.Gateway) bool {
	leftValue, leftExists := left.Annotations[config.GatewayTopologyAnnotation()]
	rightValue, rightExists := right.Annotations[config.GatewayTopologyAnnotation()]
	return leftExists == rightExists && leftValue == rightValue && slices.Equal(topologyFinalizers(config, left), topologyFinalizers(config, right))
}

// ClearGatewayTopology removes only the topology binding after owned resources
// are confirmed absent. The caller persists this with its finalizer patch.
func ClearGatewayTopology(config Config, gateway *gatewayv1.Gateway) {
	delete(gateway.Annotations, config.GatewayTopologyAnnotation())
	for _, finalizer := range topologyFinalizers(config, gateway) {
		controllerutil.RemoveFinalizer(gateway, finalizer)
	}
}

func topologyFinalizers(config Config, gateway *gatewayv1.Gateway) []string {
	var values []string
	for _, finalizer := range gateway.Finalizers {
		if strings.HasPrefix(finalizer, config.GatewayTopologyFinalizerPrefix()) {
			values = append(values, finalizer)
		}
	}
	slices.Sort(values)
	return values
}

func validateStoredTopology(resolved classconfig.Resolved) error {
	if resolved.ParametersName == "" || resolved.ParametersUID == "" || resolved.ParametersGeneration < 1 {
		return fmt.Errorf("%w: Gateway topology has an incomplete parameter identity", cloud.ErrOwnershipConflict)
	}
	monitor := resolved.HealthMonitor
	delay, timeout, retries, retriesDown := int32(monitor.Delay), int32(monitor.Timeout), int32(monitor.MaxRetries), int32(monitor.MaxRetriesDown)
	spec := configv1alpha1.OpenStackGatewayClassConfigSpec{
		MemberMode:      configv1alpha1.MemberMode(resolved.Topology.MemberMode),
		VIPSubnet:       configv1alpha1.ResourceReference{ID: resolved.Topology.VIPSubnetID},
		MemberSubnet:    configv1alpha1.ResourceReference{ID: resolved.Topology.MemberSubnetID},
		NodeAddressType: string(resolved.NodeAddressType), FlavorID: resolved.Topology.FlavorID, AvailabilityZone: resolved.Topology.AvailabilityZone,
		HealthMonitor: configv1alpha1.HealthMonitorConfig{Path: monitor.Path, Delay: &delay, Timeout: &timeout, MaxRetries: &retries, MaxRetriesDown: &retriesDown},
	}
	if resolved.Topology.ExternalNetworkID != "" {
		spec.FloatingIP = configv1alpha1.FloatingIPConfig{Mode: configv1alpha1.FloatingIPAllocate, ExternalNetwork: &configv1alpha1.ResourceReference{ID: resolved.Topology.ExternalNetworkID}}
	}
	if len(resolved.Topology.VIPSecurityGroupIDs) != 0 {
		spec.FrontendSecurity = configv1alpha1.FrontendSecurityConfig{Mode: configv1alpha1.FrontendReferenced, Groups: securityGroupReferences(resolved.Topology.VIPSecurityGroupIDs)}
	}
	if len(resolved.Topology.BackendSecurityGroupIDs) != 0 {
		spec.BackendSecurity = configv1alpha1.BackendSecurityConfig{Mode: configv1alpha1.BackendReferenced, Groups: securityGroupReferences(resolved.Topology.BackendSecurityGroupIDs)}
	}
	request, addressType, validatedMonitor, err := classconfig.Validate(spec)
	if err != nil || request.MemberMode != resolved.Topology.MemberMode || addressType != resolved.NodeAddressType || validatedMonitor != monitor ||
		!slices.IsSorted(resolved.Topology.VIPSecurityGroupIDs) || !slices.IsSorted(resolved.Topology.BackendSecurityGroupIDs) {
		return fmt.Errorf("%w: Gateway topology contains an invalid resolved configuration", cloud.ErrOwnershipConflict)
	}
	return nil
}

func securityGroupReferences(ids []string) []configv1alpha1.ResourceReference {
	references := make([]configv1alpha1.ResourceReference, len(ids))
	for i, id := range ids {
		references[i] = configv1alpha1.ResourceReference{ID: id}
	}
	return references
}
