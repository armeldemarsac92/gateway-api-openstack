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
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
)

func TestGatewayCreatesRequestedPrivateVIP(t *testing.T) {
	const loadBalancers = "/lbaas/loadbalancers"
	for _, test := range []struct {
		name, address, externalNetwork, fault, issue string
		status                                       int
		creates                                      bool
		retryable                                    bool
	}{
		{name: "dynamic", status: http.StatusCreated, creates: true},
		{name: "static", address: "192.0.2.20", status: http.StatusCreated, creates: true},
		{name: "outside subnet", address: "198.51.100.20", issue: "outside the GatewayClass VIP subnet"},
		{name: "external network", address: "192.0.2.20", externalNetwork: "external-id", issue: "without a Floating IP"},
		{name: "occupied", address: "192.0.2.20", status: http.StatusConflict, fault: "The VIP IP address is already in use: The IP address 192.0.2.20 is in use.", issue: "192.0.2.20 is already in use", creates: true},
		{name: "other conflict", address: "192.0.2.20", status: http.StatusConflict, fault: "Another operation is in progress", creates: true, retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := &gatewayTransitionRequests{}
			identity := safetyTestIdentity(t)
			lb := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.record(req)
				switch {
				case req.Method == http.MethodGet && req.URL.Path == loadBalancers:
					safetyWriteJSON(t, w, map[string]any{"loadbalancers": []any{}})
				case req.Method == http.MethodPost && req.URL.Path == loadBalancers:
					var body struct {
						LoadBalancer loadbalancers.CreateOpts `json:"loadbalancer"`
					}
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if opts := body.LoadBalancer; opts.VipAddress != test.address || opts.VipSubnetID != "vip-subnet" || opts.Provider != "amphora" {
						t.Fatalf("Octavia create requested %q on %q using %q, want VIP %q on the class subnet", opts.VipAddress, opts.VipSubnetID, opts.Provider, test.address)
					}
					w.WriteHeader(test.status)
					if test.fault != "" {
						safetyWriteJSON(t, w, map[string]any{"faultstring": test.fault})
						return
					}
					safetyWriteJSON(t, w, map[string]any{"loadbalancer": gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "PENDING_CREATE")})
				default:
					t.Fatalf("unexpected Octavia request %s %s", req.Method, req.URL)
				}
			})
			floatingIPs := emptyFloatingIPHandler(t, requests)
			network := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodGet && req.URL.Path == "/subnets/vip-subnet" {
					safetyWriteJSON(t, w, map[string]any{"subnet": map[string]any{
						"id": "vip-subnet", "cidr": "192.0.2.0/24", "ip_version": 4, "network_id": "vip-network", "project_id": "project-a",
					}})
					return
				}
				floatingIPs.ServeHTTP(w, req)
			})
			spec := completeGraphSpec(identity)
			spec.Routes = nil
			spec.Gateway.Spec.RequestedVIPAddress = test.address
			spec.Gateway.Spec.ExternalNetworkID = test.externalNetwork
			got, err := newTestProvider(t, ServiceClients{ProjectID: "project-a", LoadBalancer: safetyServiceClient(lb), Network: safetyServiceClient(network)}, ProviderConfig{}).
				ReconcileGatewayGraph(t.Context(), spec)
			if test.retryable {
				if !errors.Is(err, cloud.ErrRetryableService) {
					t.Fatalf("reconcile error = %v, want retryable Octavia conflict", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			issue := got.Gateway.AddressIssue
			if (test.issue == "" && issue != "") || !strings.Contains(issue, test.issue) || got.Gateway.State.Address() != "" {
				t.Fatalf("Gateway result = %#v, want address issue %q and no observed assignment during create", got.Gateway, test.issue)
			}
			var want []string
			if test.creates {
				want = []string{loadBalancers}
			}
			if writes := requests.mutationPaths(); !reflect.DeepEqual(writes, want) {
				t.Fatalf("mutations = %v, want %v", writes, want)
			}
		})
	}
}

func TestGatewayEditedPrivateVIPPreservesActiveGraph(t *testing.T) {
	identity := safetyTestIdentity(t)
	requests := &gatewayTransitionRequests{}
	handler := convergedRouteGraphHandler(t, requests, identity, true, true)
	spec := completeGraphSpec(identity)
	for _, address := range []string{"192.0.2.20", "192.0.2.10"} {
		spec.Gateway.Spec.RequestedVIPAddress = address
		// Restart between passes. Provider observation is the source of truth.
		result, err := completeGraphProvider(t, handler, requests).ReconcileGatewayGraph(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if result.Gateway.State.Address() != "192.0.2.10" || result.Gateway.Outcome.State != cloud.OutcomeReady ||
			len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("reconcile for %s = %#v, want the observed VIP and active route", address, result)
		}
		if address == "192.0.2.20" && !strings.Contains(result.Gateway.AddressIssue, "192.0.2.20") ||
			address == "192.0.2.10" && result.Gateway.AddressIssue != "" {
			t.Fatalf("reconcile for %s reported %q", address, result.Gateway.AddressIssue)
		}
	}
	if writes := requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("editing and restoring the fixed address mutated the existing graph: %v", writes)
	}
}

func TestGatewayRejectsReplacementWithDifferentFixedVIP(t *testing.T) {
	identity := safetyTestIdentity(t)
	requests := &gatewayTransitionRequests{}
	handler := convergedRouteGraphHandler(t, requests, identity, true, true)
	spec := completeGraphSpec(identity)
	// A simultaneous change to the listener port would normally rebuild the graph.
	spec.Gateway.Disposition = cloud.GraphDelete
	spec.Gateway.Spec.RequestedVIPAddress = "192.0.2.20"
	spec.Routes[0].Disposition = cloud.GraphPreserve
	result, err := completeGraphProvider(t, handler, requests).ReconcileGatewayGraph(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Gateway.State.Address() != "192.0.2.10" || !strings.Contains(result.Gateway.AddressIssue, "192.0.2.20") {
		t.Fatalf("result = %#v, want observed VIP and a replacement block", result)
	}
	if writes := requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("mutations with rejected replacement = %v, want none", writes)
	}
}

func TestGatewayRejectsStaticAddressBeforeAllocatingFloatingIP(t *testing.T) {
	identity := safetyTestIdentity(t)
	requests := &gatewayTransitionRequests{}
	listener := map[string]any{
		"id": "listener-1", "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
		"tags": safetyGatewayTags(t, identity, roleListener),
	}
	handler := gatewayDriftHandler(t, requests, safetyGatewayTags(t, identity, roleLoadBalancer), nil, true, []any{listener})
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.ExternalNetworkID = "external-id"
	spec.Gateway.Spec.RequestedVIPAddress = "192.0.2.20"
	result, err := completeGraphProvider(t, handler, requests).ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || !strings.Contains(result.Gateway.AddressIssue, "without a Floating IP") {
		t.Fatalf("reconcile = %#v, %v, want rejection of static address on a Floating IP class", result, err)
	}
	if writes := requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("mutations with rejected static address = %v, want none", writes)
	}
}
