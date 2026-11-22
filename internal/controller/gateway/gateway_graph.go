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
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
)

const openStackResourceNamePrefix = "gateway-api-openstack"

func (r *Reconciler) ensureGateway(ctx context.Context, expected *gatewayv1.Gateway) (cloud.GatewayGraphResult, error) {
	if r.Writer == nil {
		return cloud.GatewayGraphResult{}, fmt.Errorf("gateway graph writer is required")
	}
	return r.Writer.ReconcileGraph(ctx, graph.Request{Identity: controller.GatewayIdentity(r.Config, expected), Action: graph.Ensure, Gateway: expected})
}

func (r *Reconciler) gatewaySpec(gateway *gatewayv1.Gateway, listeners []*gatewayv1.Listener, topology classconfig.Resolved) cloud.GatewaySpec {
	listener := listeners[0]
	var protocol string
	if isL4Listener(listener.Protocol) || listener.Protocol == gatewayv1.TLSProtocolType {
		protocol = string(listenerBindingProtocol(listener))
	}
	spec := cloud.GatewaySpec{
		Identity:            controller.GatewayIdentity(r.Config, gateway),
		Provider:            "amphora",
		VIPSubnetID:         topology.Topology.VIPSubnetID,
		ExternalNetworkID:   topology.Topology.ExternalNetworkID,
		ListenerName:        resourceDisplayName(gateway.Namespace, gateway.Name, string(listener.Name)),
		ListenerPort:        int(listener.Port),
		ListenerProtocol:    protocol,
		FlavorID:            topology.Topology.FlavorID,
		AvailabilityZone:    topology.Topology.AvailabilityZone,
		VIPSecurityGroupIDs: slices.Clone(topology.Topology.VIPSecurityGroupIDs),
	}
	setGatewayAddressRequest(&spec, gateway)
	if controller.GatewayListenerScope(r.Config, gateway) != "" {
		// Snapshot validation already verified the frozen listener port and topology.
		spec.ListenerProtocol = string(listenerFamily(listener))
		spec.ListenerPort, _ = strconv.Atoi(gateway.Annotations[r.Config.GatewayListenerPortAnnotation()])
		spec.ScopedListeners = []cloud.ScopedListener{}
		seenPorts := make(map[gatewayv1.PortNumber]bool, len(listeners))
		for _, listener := range listeners {
			if seenPorts[listener.Port] {
				continue
			}
			seenPorts[listener.Port] = true
			spec.ScopedListeners = append(spec.ScopedListeners, cloud.ScopedListener{
				Name: resourceDisplayName(strconv.Itoa(int(listener.Port)), gateway.Namespace, gateway.Name, string(listener.Name)),
				Port: int(listener.Port),
			})
		}
	}
	return spec
}

func setGatewayAddressRequest(spec *cloud.GatewaySpec, gateway *gatewayv1.Gateway) {
	if rejection := validateAddresses(gateway.Spec.Addresses); rejection != nil {
		spec.AddressRejection = rejection.message
	} else if len(gateway.Spec.Addresses) == 1 {
		spec.RequestedVIPAddress = gateway.Spec.Addresses[0].Value
	}
}

func resourceDisplayName(parts ...string) string {
	name := strings.Join(append([]string{openStackResourceNamePrefix}, parts...), "-")
	if len(name) > 200 {
		return name[:200]
	}
	return name
}
