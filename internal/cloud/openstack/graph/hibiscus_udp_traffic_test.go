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
	"net/http"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
)

// The Python qualification fixture starts the tagged Octavia WSGI app on an
// ephemeral loopback port and records the stock worker's UDP configurations.
func TestHibiscusUDPStockAPI(t *testing.T) {
	endpoint := os.Getenv("HIBISCUS_UDP_FIXTURE")
	if endpoint == "" {
		t.Skip("run the stock-udp.py qualification with --controller-test to start the Octavia fixture")
	}
	address, err := netip.ParseAddrPort(endpoint)
	if err != nil || !address.Addr().IsLoopback() || address.Port() == 0 {
		t.Fatal("the stock Octavia fixture must be a loopback address with an explicit port")
	}
	base := "http://" + address.String() + "/"
	project, vip := os.Getenv("HIBISCUS_UDP_PROJECT"), os.Getenv("HIBISCUS_UDP_VIP_SUBNET")
	memberSubnet := os.Getenv("HIBISCUS_UDP_MEMBER_SUBNET")
	if project == "" || vip == "" || memberSubnet == "" {
		t.Fatal("the Octavia fixture must supply its generated project and subnet IDs")
	}
	var writes int
	newAdapter := func() *Provider {
		client := &gophercloud.ProviderClient{}
		client.HTTPClient = http.Client{Timeout: 5 * time.Second, Transport: safetyRoundTripper(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet {
				writes++
			}
			return http.DefaultTransport.RoundTrip(req)
		})}
		octavia := &gophercloud.ServiceClient{ProviderClient: client, Endpoint: base, ResourceBase: base + "v2/"}
		// The upstream fixture owns Octavia. The usual local Neutron stub
		// supplies the empty floating-IP inventory for this private Gateway.
		return newTestProvider(t, ServiceClients{LoadBalancer: octavia, ProjectID: project}, ProviderConfig{})
	}
	identity := safetyTestIdentity(t).Value()
	identity.OpenStackProjectID = project
	gateway := identity
	gateway.RouteName, gateway.RouteNamespace, gateway.RouteUID = "", "", ""
	spec := cloud.GatewayGraphSpec{
		Identity: gateway, Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.GatewaySpec{
			Identity: gateway, Provider: "amphora", VIPSubnetID: vip, ListenerProtocol: "UDP", ListenerPort: 18053,
		}},
		Routes: []cloud.RouteGraphNode{{Disposition: cloud.GraphEnsure, Spec: cloud.RouteSpec{
			Identity: identity, MemberMode: cloud.MemberModeEndpoint, MemberSubnetID: memberSubnet,
			GatewayRequirements: cloud.GatewayRequirements{Provider: "amphora", VIPSubnetID: vip, ListenerProtocol: "UDP", ListenerPort: 18053},
			Members:             []cloud.Member{{Address: "198.51.100.11", Port: 15001}},
		}}},
	}
	converge := func(adapter *Provider) cloud.GatewayGraphResult {
		t.Helper()
		for range 24 {
			result, err := adapter.ReconcileGatewayGraph(t.Context(), spec)
			if err != nil {
				t.Fatalf("stock Octavia graph after %d writes: %v", writes, err)
			}
			if result.Outcome.State == cloud.OutcomeReady {
				return result
			}
		}
		t.Fatal("the UDP graph did not converge")
		return cloud.GatewayGraphResult{}
	}
	initial := converge(newAdapter())
	if len(initial.Routes) != 1 || initial.Routes[0].Result.State.PoolID == "" || initial.Gateway.State.VIPAddress != "192.0.2.30" {
		t.Fatalf("initial UDP graph = %+v", initial)
	}
	before := writes
	adapter := newAdapter()
	recovered := converge(adapter)
	if writes != before || recovered.Gateway.State != initial.Gateway.State || len(recovered.Routes) != 1 ||
		recovered.Routes[0].Result.State.PoolID != initial.Routes[0].Result.State.PoolID {
		t.Fatal("a cold adapter did not rediscover the serving UDP graph without writes")
	}
	spec.Routes[0].Spec.Members = []cloud.Member{{Address: "198.51.100.12", Port: 15002}}
	converge(adapter)
	spec.Routes[0].Spec.HealthMonitor = cloud.HealthMonitorConfig{Path: "/healthz", Delay: 10, Timeout: 5, MaxRetries: 3, MaxRetriesDown: 3}
	monitored := converge(adapter)
	if len(monitored.Routes) != 1 || monitored.Routes[0].Result.State.MonitorID == "" {
		t.Fatal("the UDP health monitor was not observed through the stock API")
	}
	spec.Routes[0].Disposition = cloud.GraphDelete
	converge(adapter)
	spec.Gateway.Disposition, spec.Routes = cloud.GraphDelete, nil
	converge(newAdapter())
	t.Logf("stock UDP graph created, rediscovered, switched and removed in %d public Octavia writes", writes)
}
