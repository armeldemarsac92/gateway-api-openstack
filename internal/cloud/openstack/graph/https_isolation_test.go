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
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

func TestLiveHTTPSPolicyRechecksRevokedBackendActions(t *testing.T) {
	const host = "kept.example.test"
	const listenerID = "listener-1"
	bind := func(route *desiredRoute, policy *observedRoutePolicy) {
		route.spec.ScopedListenerPorts = []int{443}
		route.policies[0].httpsListenerHostname = host
		policy.httpsListenerHostname, policy.policy.ListenerID = host, listenerID
		policy.adminObserved, policy.policy.ProvisioningStatus = true, "ACTIVE"
		policy.rules[0].flagsObserved, policy.rules[0].rule.ProvisioningStatus = true, "ACTIVE"
	}
	t.Run("local failure replaces a revoked backend", func(t *testing.T) {
		route, observed := convergedRoutePlanFixture()
		policy := &observed.policies[0]
		bind(&route, policy)
		desired, actual := fixedHTTPPlanFixture(route, observed)
		actual.scopedListeners = []listeners.Listener{{ID: listenerID, ProtocolPort: 443}}
		authorized := func() bool { return liveHTTPSPolicies(desired, actual)[policy.policy.ID].authorized }
		if !authorized() {
			t.Fatal("valid forwarding should keep the neighboring host live")
		}
		desired.routes[0].fixedResponse = true
		if authorized() {
			t.Fatal("an old pool must not survive revocation to a fixed response")
		}
		policy.fixedResponse = true
		policy.policy.Action, policy.policy.RedirectPoolID = string(octavia.ActionFixedResponse), ""
		if !authorized() {
			t.Fatal("the confirmed local error may remain on the bind")
		}
	})
	t.Run("another backend retains its share beside a fixed 500", func(t *testing.T) {
		desired, actual := weightedPlanFixture()
		parts := desired.routes[0].httpParts
		weighted := &parts[2]
		policy := &actual.routes[0].httpParts[weighted.backendKey].policies[0]
		bind(weighted, policy)
		actual.scoped = true
		actual.scopedListeners = []listeners.Listener{{ID: listenerID, ProtocolPort: 443, AdminStateUp: true, ProvisioningStatus: "ACTIVE"}}
		actual.loadBalancer.ProvisioningStatus = "ACTIVE"
		authorized := func() bool { return liveHTTPSPolicies(desired, actual)[policy.policy.ID].authorized }
		if !authorized() {
			t.Fatal("the original weighted routes should match")
		}
		weighted.weightedTargets[1].backendKey = ""
		if authorized() {
			t.Fatal("the revoked pool still receives part of the traffic")
		}
		policy.weightedTargets[1] = fixedWeightedHTTPTarget(1)
		if !authorized() {
			t.Fatal("a valid fixed response must not block the healthy share")
		}
		policy.weightedTargets[0].Weight = 2
		if authorized() {
			t.Fatal("the observed weights differ from the authorized weights")
		}
	})
}

func TestLiveHTTPSRestoreLetsOnlyTheDetachingRouteCleanUp(t *testing.T) {
	hostnames := []string{"kept.example.test", "restored.example.test"}
	listener := listeners.Listener{ID: "listener", ProtocolPort: 443, Protocol: terminatedHTTPS, AdminStateUp: true,
		DefaultTlsContainerRef: "certificate", Tags: []string{httpsIsolationTag}, SniContainerRefs: []string{}}
	spec := cloud.ScopedListener{Port: 443, TLS: &cloud.ListenerTLS{CertificateRef: "certificate", VirtualHosts: hostnames}}
	actual := observedGatewayGraph{httpsVirtualHosts: map[string][]string{listener.ID: hostnames}, adminObserved: map[string]bool{listener.ID: true},
		httpsIsolationAvailable: true}
	detaching := cloud.Identity{RouteUID: "detaching"}
	desired := desiredGatewayGraph{spec: cloud.GatewayGraphSpec{Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphPreserve},
		Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{Identity: detaching}}},
	}}
	active := true
	inventory := httpsPolicyInventory{complete: true}
	proof := make(map[string]httpsLivePolicy)
	for _, host := range hostnames {
		policy := l7policies.L7Policy{ID: host, AdminStateUp: active, ProvisioningStatus: "ACTIVE"}
		inventory.items = append(inventory.items, octavia.L7PolicyObservation{Policy: policy, HTTPSListenerHostname: host, AdminState: &active})
		proof[host] = httpsLivePolicy{observed: observedRoutePolicy{policy: policy, adminObserved: true},
			authorized: host == hostnames[0], detaching: host == hostnames[1], owner: detaching}
	}
	plan := func() (*graphMutation, bool, bool) {
		return planLiveHTTPSHostTransition(desired, actual, listener, spec, inventory,
			func() map[string]httpsLivePolicy { return proof }, nil, true, false)
	}
	if step, serving, safe := plan(); step != nil || !serving || !safe {
		t.Fatalf("the verified detaching route should own cleanup while the sibling serves: %+v, %t, %t", step, serving, safe)
	}
	desired.spec.Gateway.Disposition = cloud.GraphEnsure
	if step, serving, safe := plan(); step == nil || !serving || !safe || step.deletion.resourceID != hostnames[1] {
		t.Fatalf("the Gateway writer should remove only the detaching policy: %+v, %t, %t", step, serving, safe)
	}
	desired.spec.Gateway.Disposition = cloud.GraphPreserve
	desired.spec.Routes = slices.Delete(desired.spec.Routes, 0, 1)
	if step, serving, safe := plan(); step != nil || serving || safe {
		t.Fatalf("cleanup lacks authority over the active detaching route: %+v, %t, %t", step, serving, safe)
	}
}
