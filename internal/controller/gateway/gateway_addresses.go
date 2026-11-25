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
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

const sentinelRoutability gatewayv1.GatewayAddressRoutabilityType = "testing.x-k8s.io/sentinel"

// A list containing only fixed values with default routability retains the
// existing static-address admission contract. An automatic address or a
// required scope opts into the Experimental assignment behavior.
func (r *Reconciler) usesLegacyAddressAdmission(gateway *gatewayv1.Gateway) bool {
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		return true
	}
	addresses := gateway.Spec.Addresses
	return len(addresses) != 0 && !slices.ContainsFunc(addresses, func(request gatewayv1.GatewaySpecAddress) bool {
		return request.Value == "" || request.Routability != gatewayv1.GatewayAddressRoutabilityDefault
	})
}

func supportedAddressRequest(request gatewayv1.GatewaySpecAddress) (value string, supported bool) {
	if ptr.Deref(request.Type, gatewayv1.IPAddressType) != gatewayv1.IPAddressType ||
		(request.Routability != gatewayv1.GatewayAddressRoutabilityDefault && request.Routability != sentinelRoutability) {
		return "", false
	}
	if request.Value == "" {
		return "", true
	}
	ip, err := netip.ParseAddr(request.Value)
	if err != nil || !ip.Is4() {
		return "", false
	}
	return ip.String(), true
}

func requestedVIPCandidates(requests []gatewayv1.GatewaySpecAddress) []string {
	candidates := make([]string, 0, len(requests))
	automatic := len(requests) == 0
	for _, request := range requests {
		value, supported := supportedAddressRequest(request)
		if !supported {
			continue
		}
		if value == "" {
			automatic = true
		} else {
			candidates = append(candidates, value)
		}
	}
	slices.Sort(candidates)
	candidates = slices.Compact(candidates)
	if automatic {
		candidates = append(candidates, "")
	}
	return candidates
}

func matchingAddressRequest(requests []gatewayv1.GatewaySpecAddress, address string, fixedAllowed bool) int {
	match := -1
	for i, request := range requests {
		value, supported := supportedAddressRequest(request)
		if !supported {
			continue
		}
		if value != "" {
			if fixedAllowed && value == address {
				return i
			}
			continue
		}
		// Match a fixed request first. With only automatic requests, prefer a
		// constrained scope to the unconstrained default, regardless of order.
		if match == -1 || request.Routability == sentinelRoutability {
			match = i
		}
	}
	return match
}

func setExperimentalStatusAddress(gateway *gatewayv1.Gateway, address string, routability gatewayv1.GatewayAddressRoutabilityType) {
	gateway.Status.Addresses = []gatewayv1.GatewayStatusAddress{{
		Type: ptr.To(gatewayv1.IPAddressType), Value: address, Routability: ptr.To(routability),
	}}
}

func (r *Reconciler) addressRejectionPreventsProgramming(gateway *gatewayv1.Gateway) bool {
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		return false
	}
	programmed := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	return programmed != nil && programmed.Status == metav1.ConditionFalse && programmed.ObservedGeneration == gateway.Generation &&
		programmed.Reason == string(gatewayv1.GatewayReasonAddressNotAssigned)
}

func setAddressAssignmentCondition(gateway *gatewayv1.Gateway, satisfied int, providerIssue string, cluster clusterAddressAssignments) {
	var unmet []string
	for i, request := range gateway.Spec.Addresses {
		if i == satisfied || cluster.addresses[i] != "" {
			continue
		}
		entry := fmt.Sprintf("spec.addresses[%d] (type=%s, value=%q, routability=%q)", i,
			ptr.Deref(request.Type, gatewayv1.IPAddressType), request.Value, request.Routability)
		if issue := cluster.issues[i]; issue != "" {
			entry += ": " + issue
		}
		unmet = append(unmet, entry)
	}
	// Use the published v1.7 API reason AddressesAssigned. The prose of
	// GEP-5093 also refers to the success reason as Assigned.
	status := metav1.ConditionTrue
	reason := gatewayv1.GatewayReasonAddressesAssigned
	message := "All Gateway address requests are assigned"
	if len(gateway.Spec.Addresses) == 0 {
		message = "The default Gateway address is assigned"
	}
	if len(unmet) > 0 || len(gateway.Spec.Addresses) == 0 && providerIssue != "" {
		status = metav1.ConditionFalse
		reason = gatewayv1.GatewayReasonAddressesNotAssigned
		if satisfied >= 0 || len(cluster.addresses) != 0 {
			reason = gatewayv1.GatewayReasonAddressesPartiallyAssigned
		}
		message = "Unassigned address requests: " + strings.Join(unmet, ", ")
		if providerIssue != "" {
			message += ". " + providerIssue
		}
	}
	controller.SetCondition(&gateway.Status.Conditions, controller.Condition(
		string(gatewayv1.GatewayConditionAddressesAssigned), status, string(reason), message, gateway.Generation,
	))
}

// observeAddressAssignment publishes only an observed assignment. It returns
// whether the graph continues to serve an address rejected by the new spec.
func (r *Reconciler) observeAddressAssignment(gateway *gatewayv1.Gateway, observation cloud.GatewayResult, cluster clusterAddressAssignments) bool {
	hadCluster := hasClusterStatusAddress(gateway)
	clearClusterStatusAddresses(gateway)
	previous := gateway.Status.Addresses
	appendCluster := func() {
		appendAddress := func(value string) {
			gateway.Status.Addresses = append(gateway.Status.Addresses, gatewayv1.GatewayStatusAddress{
				Type: ptr.To(gatewayv1.IPAddressType), Value: value, Routability: ptr.To(gatewayv1.GatewayAddressRoutabilityCluster),
			})
		}
		for i := range gateway.Spec.Addresses {
			if value := cluster.addresses[i]; value != "" {
				appendAddress(value)
			}
		}
		for _, value := range cluster.retained {
			appendAddress(value)
		}
	}
	if observation.AddressAbsent {
		gateway.Status.Addresses = nil
		if observation.AddressIssue == "" && !cluster.requested {
			meta.RemoveStatusCondition(&gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
			return false
		}
	}
	topology, err := controller.StoredGatewayTopology(r.Config, gateway)
	if err != nil {
		return false
	}
	fixedAllowed := topology.Topology.ExternalNetworkID == ""
	address := observation.State.VIPAddress
	if !fixedAllowed {
		address = observation.State.FloatingIPAddress
	}
	if ip, err := netip.ParseAddr(address); err == nil {
		address = ip.String()
	}
	if hadCluster && !cluster.requested && observation.AddressIssue == "" && address == "" && len(previous) == 0 {
		meta.RemoveStatusCondition(&gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
		return false
	}
	if address != "" && !observation.AddressAbsent && observation.AddressIssue == "" {
		match := matchingAddressRequest(gateway.Spec.Addresses, address, fixedAllowed)
		if match >= 0 || len(gateway.Spec.Addresses) == 0 {
			routability := gatewayv1.GatewayAddressRoutabilityDefault
			if match >= 0 {
				routability = gateway.Spec.Addresses[match].Routability
			}
			setExperimentalStatusAddress(gateway, address, routability)
			appendCluster()
			setAddressAssignmentCondition(gateway, match, "", cluster)
			return false
		}
	}
	retained := address != "" && !observation.AddressAbsent && observation.AddressActive
	if len(gateway.Spec.Addresses) != 0 {
		// A private frontend needed by a ClusterIP is not an independently
		// assigned address. Keep an old provider address only when the
		// controller actually advertised it and no replacement was assigned.
		retained = retained && len(cluster.addresses) == 0 && slices.ContainsFunc(previous, func(prior gatewayv1.GatewayStatusAddress) bool {
			return prior.Value == address
		})
	}
	if observation.AddressIssue == "" && !retained && !cluster.requested && !hadCluster {
		return false
	}
	if !observation.AddressAbsent && !observation.AddressActive && !observation.AddressActivityObserved {
		// In particular, Octavia may report PENDING_UPDATE before we can
		// re-read enabled listeners or the Floating IP during replacement.
		if observation.AddressIssue != "" || address == "" && observation.Outcome.State != cloud.OutcomeReady {
			match := -1
			if observation.AddressIssue == "" && len(previous) != 0 {
				match = matchingAddressRequest(gateway.Spec.Addresses, previous[0].Value, fixedAllowed)
			}
			appendCluster()
			if observation.AddressIssue != "" || len(cluster.addresses) > 0 || len(cluster.issues) > 0 || hadCluster {
				setAddressAssignmentCondition(gateway, match, observation.AddressIssue, cluster)
			}
			return len(previous) != 0 && observation.AddressIssue != ""
		}
	}
	if retained {
		routability := gatewayv1.GatewayAddressRoutabilityDefault
		for _, prior := range previous {
			if prior.Value == address && ptr.Deref(prior.Routability, "") == sentinelRoutability {
				routability = sentinelRoutability
				break
			}
		}
		setExperimentalStatusAddress(gateway, address, routability)
	} else {
		gateway.Status.Addresses = nil
	}
	appendCluster()
	setAddressAssignmentCondition(gateway, -1, observation.AddressIssue, cluster)
	return retained || len(cluster.retained) != 0
}
