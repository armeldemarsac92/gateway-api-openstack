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
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestScopedTCPGetterReportsAnUnverifiedAnchor(t *testing.T) {
	f := newTCPGraphFixture(t)
	f.spec.Routes[0].Spec.MemberMode = cloud.MemberModeEndpoint
	f.spec.Routes[0].Spec.Members[0].Weight = 7
	p := f.provider()
	f.settleTransportGraph(p)
	f.enableMultiUDP()
	f.settleTransportGraph(p)
	result, found, err := p.GetGateway(t.Context(), f.spec.Identity)
	if !found || err != nil || result.Outcome.State != cloud.OutcomeProgressing || result.State.LoadBalancerID == "" {
		t.Fatalf("identity-only observation = %+v, %t, %v", result, found, err)
	}
}

func TestScopedTransportRetainsOnlyEdgesWithAnUnchangedBackend(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol+"/unchanged backend", func(t *testing.T) { testPreservedTransportEdges(t, protocol, true, false) })
		t.Run(protocol+"/changed member", func(t *testing.T) { testPreservedTransportEdges(t, protocol, true, true) })
	}
}

func TestSingletonTransportPreservesAnObservedPoolUntilReadbackChanges(t *testing.T) {
	for _, protocol := range []string{"TCP", "UDP"} {
		t.Run(protocol+"/unchanged backend", func(t *testing.T) { testPreservedTransportEdges(t, protocol, false, false) })
		t.Run(protocol+"/changed member", func(t *testing.T) { testPreservedTransportEdges(t, protocol, false, true) })
	}
}

func testPreservedTransportEdges(t *testing.T, protocol string, scoped, drift bool) {
	t.Helper()
	f := newTransportGraphFixture(t, protocol)
	if scoped {
		f.enableMultiUDP()
	}
	p := f.provider()
	f.settleTransportGraph(p)
	node := &f.spec.Routes[0]
	node.Disposition = cloud.GraphPreserve
	node.RetainListenerPorts = []int{f.spec.Gateway.Spec.ListenerPort}
	if scoped {
		node.Spec.ScopedListenerPorts = node.RetainListenerPorts
	}
	if drift {
		f.inventory[tcpMember]["address"] = "10.0.0.99"
	}

	before := len(f.writes)
	f.settleTransportGraph(p)
	for _, listener := range f.children(tcpListeners) {
		keep := !drift && listener["id"] == "listener"
		if (listener["default_pool_id"] != nil) != keep {
			t.Fatalf("listener %s default pool = %v, want retained %t", listener["id"], listener["default_pool_id"], keep)
		}
	}
	for _, request := range f.writes[before:] {
		if !strings.HasPrefix(request, "PUT "+tcpListener) {
			t.Fatalf("preservation changed the backend: %v", f.writes[before:])
		}
	}
}
