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
		name, address, subnet, externalNetwork, fault, issue string
		status                                               int
		creates                                              bool
		alternatives                                         []string
		fallback                                             bool
		wantError                                            error
	}{
		{name: "dynamic", status: http.StatusCreated, creates: true},
		{name: "static", address: "192.0.2.20", status: http.StatusCreated, creates: true},
		{name: "outside subnet", address: "198.51.100.20", issue: "outside the GatewayClass VIP subnet"},
		{name: "network address", address: "192.0.2.0", issue: "reserved"},
		{name: "broadcast address", address: "192.0.2.255", issue: "reserved"},
		{name: "first slash 31 address", subnet: "192.0.2.0/31", address: "192.0.2.0", issue: "reserved"},
		{name: "second slash 31 address", subnet: "192.0.2.0/31", address: "192.0.2.1", status: http.StatusCreated, creates: true},
		{name: "sole slash 32 address", subnet: "192.0.2.20/32", address: "192.0.2.20", issue: "reserved"},
		{name: "external network", address: "192.0.2.20", externalNetwork: "external-id", issue: "without a Floating IP"},
		{name: "occupied", address: "192.0.2.20", status: http.StatusConflict, fault: "The VIP IP address is already in use: The IP address 192.0.2.20 is in use.", issue: "192.0.2.20 is already in use", creates: true},
		{name: "neutron allocation conflict", address: "192.0.2.20", status: http.StatusConflict, fault: "The VIP IP address is already in use: IP address 192.0.2.20 already allocated in subnet provider-subnet", issue: "192.0.2.20 is already in use", creates: true},
		{name: "neutron port quota", address: "192.0.2.20", status: http.StatusConflict, fault: "The VIP IP address is already in use: Quota exceeded for resources: ['port'].", creates: true, wantError: cloud.ErrQuotaExceeded},
		{name: "conflict for another IP", address: "192.0.2.20", status: http.StatusConflict, fault: "The VIP IP address is already in use: IP address 192.0.2.21 already allocated in subnet provider-subnet", creates: true, wantError: cloud.ErrRetryableService},
		{name: "other conflict", address: "192.0.2.20", status: http.StatusConflict, fault: "Another operation is in progress", creates: true, wantError: cloud.ErrRetryableService},
		{name: "experimental unsupported requests", alternatives: []string{}, issue: "No address request is supported"},
		{name: "automatic fallback after a confirmed conflict", address: "192.0.2.20", alternatives: []string{"198.51.100.20", "192.0.2.20", ""},
			status: http.StatusConflict, fault: "The VIP IP address is already in use: The IP address 192.0.2.20 is in use.", creates: true, fallback: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := &gatewayTransitionRequests{}
			identity := safetyTestIdentity(t)
			creates := 0
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
					address, status, fault := test.address, test.status, test.fault
					if test.fallback && creates == 1 {
						address, status, fault = "", http.StatusCreated, ""
					}
					creates++
					if opts := body.LoadBalancer; opts.VipAddress != address || opts.VipSubnetID != "vip-subnet" || opts.Provider != "amphora" {
						t.Fatalf("Octavia create requested %q on %q using %q, want VIP %q on the class subnet", opts.VipAddress, opts.VipSubnetID, opts.Provider, address)
					}
					if !test.creates {
						t.Error("Octavia received a create for an unusable VIP")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					w.WriteHeader(status)
					if fault != "" {
						safetyWriteJSON(t, w, map[string]any{"faultstring": fault})
						return
					}
					safetyWriteJSON(t, w, map[string]any{"loadbalancer": gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "PENDING_CREATE")})
				default:
					t.Fatalf("unexpected Octavia request %s %s", req.Method, req.URL)
				}
			})
			floatingIPs := emptyFloatingIPHandler(t, requests)
			subnet := test.subnet
			if subnet == "" {
				subnet = "192.0.2.0/24"
			}
			network := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodGet && req.URL.Path == "/subnets/vip-subnet" {
					safetyWriteJSON(t, w, map[string]any{"subnet": map[string]any{
						"id": "vip-subnet", "cidr": subnet, "ip_version": 4, "network_id": "vip-network", "project_id": "project-a",
					}})
					return
				}
				floatingIPs.ServeHTTP(w, req)
			})
			spec := completeGraphSpec(identity)
			spec.Routes = nil
			spec.Gateway.Spec.RequestedVIPAddress = test.address
			spec.Gateway.Spec.RequestedVIPAddresses = test.alternatives
			spec.Gateway.Spec.ExternalNetworkID = test.externalNetwork
			got, err := newTestProvider(t, ServiceClients{ProjectID: "project-a", LoadBalancer: safetyServiceClient(lb), Network: safetyServiceClient(network)}, ProviderConfig{}).
				ReconcileGatewayGraph(t.Context(), spec)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("reconcile error = %v, want %v", err, test.wantError)
			}
			issue := got.Gateway.AddressIssue
			if (test.issue == "" && issue != "") || !strings.Contains(issue, test.issue) || strings.Contains(issue, "provider-subnet") || got.Gateway.State.Address() != "" {
				t.Fatalf("Gateway result = %#v, want address issue %q and no observed assignment during create", got.Gateway, test.issue)
			}
			var want []string
			if test.creates {
				want = []string{loadBalancers}
			}
			if test.fallback {
				want = append(want, loadBalancers)
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
	for _, edit := range []struct{ address, rejected, issue string }{
		{address: "192.0.2.20", issue: "192.0.2.20"},
		{rejected: "a fixed Gateway address must be IPv4", issue: "IPv4"},
		{address: "192.0.2.10"},
	} {
		spec.Gateway.Spec.RequestedVIPAddress = edit.address
		spec.Gateway.Spec.AddressRejection = edit.rejected
		// Restart between passes. Provider observation is the source of truth.
		result, err := completeGraphProvider(t, handler, requests).ReconcileGatewayGraph(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if result.Gateway.State.Address() != "192.0.2.10" || result.Gateway.Outcome.State != cloud.OutcomeReady ||
			len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
			t.Fatalf("reconcile for %+v = %#v, want the observed VIP and active route", edit, result)
		}
		if !strings.Contains(result.Gateway.AddressIssue, edit.issue) || edit.issue == "" && result.Gateway.AddressIssue != "" {
			t.Fatalf("reconcile for %+v reported %q", edit, result.Gateway.AddressIssue)
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

func TestExperimentalReplacementReportsIncompleteActivityObservation(t *testing.T) {
	identity := safetyTestIdentity(t)
	requests := &gatewayTransitionRequests{}
	base := convergedRouteGraphHandler(t, requests, identity, true, true)
	provisioningStatus, adminUp := "PENDING_UPDATE", true
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/lbaas/loadbalancers/load-balancer-1" {
			lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), provisioningStatus)
			lb["admin_state_up"] = adminUp
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
			return
		}
		base.ServeHTTP(w, req)
	})
	spec := completeGraphSpec(identity)
	spec.Gateway.Disposition = cloud.GraphDelete
	spec.Gateway.Spec.RequestedVIPAddresses = []string{}
	spec.Routes[0].Disposition = cloud.GraphPreserve
	provider := completeGraphProvider(t, handler, requests)
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.AddressIssue == "" || result.Gateway.AddressActivityObserved ||
		result.Gateway.AddressAbsent || result.Gateway.AddressBlocked || result.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("pending load balancer = %+v, %v, want rejection awaiting activity readback", result.Gateway, err)
	}

	provisioningStatus, adminUp = "ACTIVE", false
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.AddressIssue == "" || !result.Gateway.AddressActivityObserved || result.Gateway.AddressActive {
		t.Fatalf("closed load balancer = %+v, %v, want confirmed inactivity", result.Gateway, err)
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
	floatingIP := detachedGatewayFloatingIP(identity)
	floatingIP["floating_network_id"] = "external-network"
	network := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.record(req)
		if req.Method == http.MethodGet && req.URL.Path == "/floatingips" {
			safetyWriteJSON(t, w, map[string]any{"floatingips": []any{floatingIP}})
			return
		}
		http.Error(w, "unexpected request", http.StatusNotFound)
	})
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.ExternalNetworkID = floatingIP["floating_network_id"]
	spec.Gateway.Spec.RequestedVIPAddress = "192.0.2.20"
	provider := newTestProvider(t, ServiceClients{ProjectID: "project-a", LoadBalancer: safetyServiceClient(handler), Network: safetyServiceClient(network)}, ProviderConfig{})
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || !strings.Contains(result.Gateway.AddressIssue, "without a Floating IP") || !result.Gateway.AddressAbsent || !result.Gateway.AddressBlocked {
		t.Fatalf("reconcile = %#v, %v, want rejection of static address on a Floating IP class", result, err)
	}

	floatingIP["port_id"] = "vip-port"
	floatingIP["fixed_ip_address"] = "192.0.2.10"
	delete(floatingIP, "floating_ip_address")
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.AddressAbsent || result.Gateway.AddressBlocked || result.Gateway.Outcome.State != cloud.OutcomeProgressing {
		t.Fatalf("incomplete Floating IP = %#v, %v, want pending observation", result.Gateway, err)
	}
	floatingIP["floating_ip_address"] = "198.51.100.20"
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.Outcome.State != cloud.OutcomeReady || result.Gateway.State.FloatingIPAddress != "198.51.100.20" {
		t.Fatalf("complete Floating IP = %#v, %v, want ready Gateway with observed Floating IP", result.Gateway, err)
	}
	if writes := requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("mutations with rejected static address = %v, want none", writes)
	}
}

func TestGatewayWaitsForObservedVIPBeforeReportingReady(t *testing.T) {
	identity := safetyTestIdentity(t)
	requests := &gatewayTransitionRequests{}
	listener := map[string]any{
		"id": "listener-1", "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
		"tags": safetyGatewayTags(t, identity, roleListener),
	}
	var vip string
	base := gatewayDriftHandler(t, requests, safetyGatewayTags(t, identity, roleLoadBalancer), nil, true, []any{listener})
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/lbaas/loadbalancers/load-balancer-1" {
			lb := gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "ACTIVE")
			lb["vip_address"] = vip
			safetyWriteJSON(t, w, map[string]any{"loadbalancer": lb})
			return
		}
		base.ServeHTTP(w, req)
	})
	spec := completeGraphSpec(identity)
	spec.Routes = nil
	spec.Gateway.Spec.RequestedVIPAddress = "192.0.2.10"
	provider := completeGraphProvider(t, handler, requests)
	result, err := provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.Outcome.State != cloud.OutcomeProgressing || result.Gateway.State.VIPAddress != "" {
		t.Fatalf("missing VIP = %#v, %v, want an unprogrammed Gateway without a VIP", result.Gateway, err)
	}
	vip = spec.Gateway.Spec.RequestedVIPAddress
	result, err = provider.ReconcileGatewayGraph(t.Context(), spec)
	if err != nil || result.Gateway.Outcome.State != cloud.OutcomeReady || result.Gateway.State.VIPAddress != vip {
		t.Fatalf("observed VIP = %#v, %v, want the ready Gateway with VIP %s", result.Gateway, err, vip)
	}
	if writes := requests.mutationPaths(); len(writes) != 0 {
		t.Fatalf("waiting to observe the VIP mutated the graph: %v", writes)
	}
}
