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
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

var tcpTLSProfileTag = openstackidentity.Tag("profile", "tcp-tls-termination-v1")

func isSNIProtocol(protocol string) bool {
	return protocol == cloud.ListenerProtocolTLSSNI || protocol == cloud.ListenerProtocolTLSTerminate
}

func sniListenerProfileTag(protocol string) string {
	if protocol == cloud.ListenerProtocolTLSTerminate {
		return tcpTLSProfileTag
	}
	return sniProfileTag
}

func otherSNIListenerProfileTag(protocol string) string {
	if protocol == cloud.ListenerProtocolTLSTerminate {
		return sniProfileTag
	}
	return tcpTLSProfileTag
}

// Observe the frontend before Neutron or route children can block withdrawal.
// A TLS extension or certificate readback is traffic proof only. Ownership
// comes from the independent Gateway identity, profile and parent relations.
func (p *Provider) observeRawTLSFrontend(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, inventory octavia.ListenerInventory, active bool) (bool, error) {
	lb := actual.loadBalancer
	if !slices.Contains(lb.Tags, tcpTLSProfileTag) || slices.Contains(lb.Tags, sniProfileTag) {
		return true, fmt.Errorf("%w: load balancer lacks the TCP TLS termination profile identity", cloud.ErrOwnershipConflict)
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return true, err
	}
	listener, err := p.managedGatewayListener(desired.identity, lb.ID, inventory.Items)
	if err != nil {
		return true, err
	}
	if listener != nil {
		if listener.ID == "" || listener.Protocol != cloud.ListenerProtocolTCP || listener.ProtocolPort != desired.spec.Gateway.Spec.ListenerPort ||
			!slices.Contains(listener.Tags, tcpTLSProfileTag) || slices.Contains(listener.Tags, sniProfileTag) ||
			len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID || !actual.adminObserved[listener.ID] {
			return true, fmt.Errorf("%w: listener differs from the TCP TLS termination profile binding", cloud.ErrOwnershipConflict)
		}
		actual.listener, actual.state.ListenerID = listener, listener.ID
	}
	dedicated := listener != nil && inventory.DefaultPoolsObserved && listener.DefaultPoolID == "" &&
		inventory.SNICertificatesObserved[listener.ID] && len(listener.SniContainerRefs) == 0
	certificate := desired.spec.Gateway.TCPTLSCertificateRef
	if !p.tcpTLSTermination {
		certificate = ""
	}
	disposition := desired.spec.Gateway.Disposition
	var dependencyErr error
	closeMessage := ""
	switch {
	case disposition == cloud.GraphDelete:
		closeMessage = "Closing the TCP TLS listener before Gateway cleanup"
	case certificate == "":
		closeMessage = "Waiting for an authorized TCP TLS certificate"
	default:
		dependencyErr = p.verifySNICapability(ctx, desired, *actual)
		actual.rawTLSCapabilityVerified = dependencyErr == nil
		if dependencyErr != nil {
			closeMessage = "Closing the TCP TLS listener until Octavia advertises its capability"
		} else if listener != nil && !inventory.TCPTLSTermination[listener.ID] {
			closeMessage = "Waiting for verified immutable TCP TLS listener mode"
		} else if listener != nil && !dedicated {
			closeMessage = "Waiting for the dedicated TCP TLS listener configuration"
		} else if listener != nil && listener.DefaultTlsContainerRef != certificate {
			closeMessage = "Waiting for the authorized TCP TLS certificate readback"
		} else {
			actual.rawTLSVerified = true
		}
	}

	// Octavia serializes provisioning writes. A pending disable becomes a
	// withdrawal only after the authoritative listener readback is ACTIVE.
	if listener != nil && active && listener.AdminStateUp && closeMessage != "" {
		actual.withdrawTLS = &graphMutation{kind: graphDisableListener, redactTLSError: true, sniWithdrawal: true, message: closeMessage}
		return true, dependencyErr
	}
	if disposition == cloud.GraphDelete {
		return false, nil
	}
	if disposition == cloud.GraphWithdrawTLS {
		if listener != nil && listener.ProvisioningStatus != "ACTIVE" {
			actual.outcome = p.progressingOutcome("Waiting to verify the TCP TLS listener withdrawal")
		}
		return true, nil
	}
	if closeMessage == "" {
		return false, nil
	}
	actual.outcome = p.progressingOutcome(closeMessage)
	if listener != nil && active && listener.ProvisioningStatus == "ACTIVE" && certificate != "" && dependencyErr == nil &&
		inventory.TCPTLSTermination[listener.ID] && dedicated && listener.DefaultTlsContainerRef != certificate {
		actual.withdrawTLS = &graphMutation{kind: graphUpdateListenerCertificate, redactTLSError: true, message: "Updating the authorized certificate on the closed TCP TLS listener"}
	}
	return true, dependencyErr
}

// validateRawTLSDefault rejects stock adoption if a provider exposes native
// termination on a listener without the required, known profile markers.
func validateRawTLSDefault(inventory octavia.ListenerInventory) error {
	for _, listener := range inventory.Items {
		if inventory.TCPTLSTermination[listener.ID] || slices.Contains(listener.Tags, tcpTLSProfileTag) {
			return fmt.Errorf("%w: found a TCP TLS listener outside its bound Gateway profile", cloud.ErrOwnershipConflict)
		}
	}
	return nil
}
