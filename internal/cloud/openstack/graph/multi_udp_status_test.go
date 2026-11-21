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
)

func TestMultiUDPDeferredObservationRequiresServingRouteEdges(t *testing.T) {
	f := newTransportGraphFixture(t, "UDP")
	f.enableMultiUDP()
	ports := []int{f.spec.Gateway.Spec.ListenerPort, 5353}
	f.spec.Routes[0].Spec.UDPListenerPorts = ports
	provider := f.provider()
	f.settleUDP(provider)
	writes := len(f.writes)

	sibling := f.inventory[tcpListener+"-1812"]
	sibling["provisioning_status"] = "PENDING_UPDATE"
	result := f.reconcile(provider)
	if len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
		!result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("pending unrelated sibling deferred no proven UDP route: %+v", result.Routes)
	}

	pool := f.inventory[tcpPool]
	pool["provisioning_status"] = "ERROR"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("failed pool deferred a UDP route with obsolete forwarding proof: %+v", result.Routes)
	}
	pool["provisioning_status"] = "ACTIVE"

	member := f.inventory[tcpMember]
	member["provisioning_status"] = "ERROR"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("failed member deferred a UDP route with obsolete forwarding proof: %+v", result.Routes)
	}
	member["provisioning_status"] = "ACTIVE"

	// A formerly winning port remains attached to the pool until the graph
	// can perform the withdrawal. That stale edge cannot support deferral.
	f.spec.Routes[0].Spec.UDPListenerPorts = ports[:1]
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("superseded default deferred a UDP route: %+v", result.Routes)
	}
	f.spec.Routes[0].Spec.UDPListenerPorts = ports

	winning := f.inventory[tcpListener+"-5353"]
	winning["provisioning_status"] = "PENDING_UPDATE"
	result = f.reconcile(provider)
	if result.Routes[0].Result.ObservationDeferred {
		t.Fatalf("pending winning listener deferred a UDP route: %+v", result.Routes)
	}
	winning["provisioning_status"] = "ACTIVE"

	sibling["provisioning_status"] = "ERROR"
	result, err := provider.ReconcileGatewayGraph(t.Context(), f.spec)
	if err == nil || result.Gateway.Outcome.State != cloud.OutcomeProgressing || !slices.Equal(result.Gateway.UDPReadyPorts, ports) ||
		result.Gateway.State.Address() == "" || len(result.Routes) != 0 || len(f.writes) != writes {
		t.Fatalf("UDP sibling ERROR returned result %+v, error %v, writes %v", result, err, f.writes[writes:])
	}
}
