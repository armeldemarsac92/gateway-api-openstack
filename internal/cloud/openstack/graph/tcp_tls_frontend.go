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
var tcpTLSMixedProfileTag = openstackidentity.Tag("profile", "tcp-tls-mixed-mode-v1")

func isSNIProtocol(protocol string) bool {
	return protocol == cloud.ListenerProtocolTLSSNI || cloud.IsTCPTLSListenerProtocol(protocol)
}

func sniListenerProfileTag(protocol string) string {
	switch protocol {
	case cloud.ListenerProtocolTLSMixed:
		return tcpTLSMixedProfileTag
	case cloud.ListenerProtocolTLSTerminate:
		return tcpTLSProfileTag
	default:
		return sniProfileTag
	}
}

func hasSNIProfile(tags []string) bool {
	return slices.Contains(tags, sniProfileTag) || slices.Contains(tags, tcpTLSProfileTag) || slices.Contains(tags, tcpTLSMixedProfileTag)
}

func matchesSNIProfile(tags []string, protocol string) bool {
	wanted := sniListenerProfileTag(protocol)
	return slices.Contains(tags, wanted) && !slices.ContainsFunc(tags, func(tag string) bool {
		return tag != wanted && (tag == sniProfileTag || tag == tcpTLSProfileTag || tag == tcpTLSMixedProfileTag)
	})
}

// A load balancer can keep its original profile tag across a same-port
// replacement. The current listener must still match its own profile exactly.
func taggedSNIProfile(tags []string) string {
	for _, protocol := range []string{cloud.ListenerProtocolTLSSNI, cloud.ListenerProtocolTLSTerminate, cloud.ListenerProtocolTLSMixed} {
		if matchesSNIProfile(tags, protocol) {
			return protocol
		}
	}
	return ""
}

func isPassthroughSNIListener(listener listeners.Listener, inventory octavia.ListenerInventory) bool {
	return matchesSNIProfile(listener.Tags, cloud.ListenerProtocolTLSSNI) &&
		!inventory.TCPTLSTermination[listener.ID] && !inventory.TCPTLSMixedMode[listener.ID] &&
		listener.DefaultTlsContainerRef == "" && len(listener.SniContainerRefs) == 0
}

func (p *Provider) managedSNIListener(identity Identity, lb *octavia.LoadBalancer, inventory octavia.ListenerInventory, port int, protocol string) (*listeners.Listener, error) {
	if taggedSNIProfile(lb.Tags) == "" {
		return nil, fmt.Errorf("%w: load balancer lacks the TCP TLS termination profile identity", cloud.ErrOwnershipConflict)
	}
	if err := crossCheckCompleteChildInventory("load balancer", lb.ID, "listener", lb.Listeners, inventory.Items, func(listener listeners.Listener) string { return listener.ID }); err != nil {
		return nil, err
	}
	listener, err := p.managedGatewayListener(identity, lb.ID, inventory.Items)
	if err != nil || listener == nil {
		return listener, err
	}
	if listener.ID == "" || listener.Protocol != cloud.ListenerProtocolTCP || listener.ProtocolPort < 1 || listener.ProtocolPort > 65535 ||
		port != 0 && listener.ProtocolPort != port || !matchesSNIProfile(listener.Tags, protocol) || len(listener.Loadbalancers) != 1 || listener.Loadbalancers[0].ID != lb.ID {
		return nil, fmt.Errorf("%w: listener differs from the TCP TLS termination profile binding", cloud.ErrOwnershipConflict)
	}
	return listener, nil
}

// Observe the frontend before Neutron or route children can block withdrawal.
// A TLS extension or certificate readback is traffic proof only. Ownership
// comes from the independent Gateway identity, profile and parent relations.
func (p *Provider) observeRawTLSFrontend(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph, inventory octavia.ListenerInventory, active bool) (bool, error) {
	listener, err := p.managedSNIListener(desired.identity, actual.loadBalancer, inventory, desired.spec.Gateway.Spec.ListenerPort, desired.protocol)
	if err != nil {
		return true, err
	}
	if listener != nil {
		actual.listener, actual.state.ListenerID = listener, listener.ID
	}
	mixed := desired.protocol == cloud.ListenerProtocolTLSMixed
	dedicated := listener != nil && inventory.DefaultPoolsObserved && listener.DefaultPoolID == "" &&
		inventory.SNICertificatesObserved[listener.ID] && (mixed || len(listener.SniContainerRefs) == 0)
	certificate := desired.spec.Gateway.TCPTLSCertificateRef
	certificatesMatch := listener != nil && listener.DefaultTlsContainerRef == certificate && len(listener.SniContainerRefs) == 0
	if !p.tcpTLSTermination || mixed && !p.tcpTLSMixedMode {
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
		} else if listener != nil && (!actual.adminObserved[listener.ID] || listener.ProvisioningStatus == "ERROR") {
			closeMessage = "Waiting for verified TCP TLS listener administrative state"
		} else if listener != nil && (!inventory.TCPTLSTermination[listener.ID] || inventory.TCPTLSMixedMode[listener.ID] != mixed) {
			closeMessage = "Waiting for verified immutable TCP TLS listener mode"
		} else if listener != nil && !dedicated {
			closeMessage = "Waiting for the dedicated TCP TLS listener configuration"
		} else if listener != nil && !certificatesMatch {
			closeMessage = "Waiting for the authorized TCP TLS certificate readback"
		} else {
			actual.rawTLSVerified = true
		}
	}

	if listener != nil && closeMessage != "" &&
		(listener.ProvisioningStatus != "ACTIVE" || listener.AdminStateUp || !actual.adminObserved[listener.ID]) {
		// ERROR can report admin_state_up=false before Amphora closes
		// the listener. Retry the close once the parent permits writes.
		if active && (listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR") {
			actual.withdrawTLS = &graphMutation{kind: graphDisableListener, redactTLSError: true, sniWithdrawal: true, message: closeMessage}
		} else {
			actual.outcome = p.progressingOutcome("Waiting to verify the TCP TLS listener withdrawal")
		}
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
		inventory.TCPTLSTermination[listener.ID] && inventory.TCPTLSMixedMode[listener.ID] == mixed && dedicated && !certificatesMatch {
		actual.withdrawTLS = &graphMutation{kind: graphUpdateListenerCertificate, redactTLSError: true, message: "Updating the authorized certificate on the closed TCP TLS listener"}
	}
	return true, dependencyErr
}

// validateRawTLSDefault rejects stock adoption if a provider exposes native
// termination on a listener without the required, known profile markers.
func validateRawTLSDefault(inventory octavia.ListenerInventory) error {
	for _, listener := range inventory.Items {
		if inventory.TCPTLSTermination[listener.ID] || inventory.TCPTLSMixedMode[listener.ID] || slices.Contains(listener.Tags, tcpTLSProfileTag) || slices.Contains(listener.Tags, tcpTLSMixedProfileTag) {
			return fmt.Errorf("%w: found a TCP TLS listener outside its bound Gateway profile", cloud.ErrOwnershipConflict)
		}
	}
	return nil
}
