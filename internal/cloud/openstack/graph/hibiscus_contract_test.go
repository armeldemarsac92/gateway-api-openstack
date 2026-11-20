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
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
)

// Exercise the public Octavia v2 shapes with the real adapter and SDK. The
// fixture represents settled inventory at each pass, including a lost reply
// after an applied delete. See docs/qualification/v1.7.0/cloud-contract.md.
func TestHibiscusHTTPContractRecoversAndDeletesOnlyOwnedGraph(t *testing.T) {
	ctx := t.Context()
	identity := safetyTestIdentity(t)
	const (
		projectID   = "project-a"
		lbList      = "/v2/lbaas/loadbalancers"
		gatewayLB   = lbList + "/load-balancer-1"
		magnumLB    = lbList + "/magnum-api-server"
		occmLB      = lbList + "/occm-service"
		listeners   = "/v2/lbaas/listeners"
		listener    = listeners + "/listener-1"
		policies    = "/v2/lbaas/l7policies"
		policy      = policies + "/policy-1"
		rules       = policy + "/rules"
		rule        = rules + "/rule-1"
		pools       = "/v2/lbaas/pools"
		pool        = pools + "/pool-1"
		members     = pool + "/members"
		member      = members + "/member-1"
		monitors    = "/v2/lbaas/healthmonitors"
		monitor     = monitors + "/monitor-1"
		floatingIPs = "/v2.0/floatingips"
		floatingIP  = floatingIPs + "/floating-ip-1"
	)
	parents := func(id string) []map[string]string {
		return []map[string]string{{"id": id}}
	}
	inventory := map[string]map[string]any{
		gatewayLB: gatewayTransitionLoadBalancer(safetyGatewayTags(t, identity, roleLoadBalancer), "ACTIVE"),
		magnumLB:  {"name": "magnum-api-server", "provider": "amphora", "vip_address": "192.0.2.20", "tags": []string{"magnum"}},
		occmLB:    {"name": "occm-service", "provider": "amphora", "vip_address": "192.0.2.30", "tags": []string{"kube_service"}},
		listener: {
			"loadbalancers": parents(path.Base(gatewayLB)), "protocol": "HTTP", "protocol_port": 80, "admin_state_up": true,
			"tags": safetyGatewayTags(t, identity, roleListener),
		},
		policy: {
			"listener_id": path.Base(listener), "action": "REDIRECT_TO_POOL", "redirect_pool_id": path.Base(pool),
			"position": 1, "admin_state_up": true, "tags": safetyRouteTags(t, identity, rolePolicyExact),
		},
		rule: {
			"type": "PATH", "compare_type": "EQUAL_TO", "value": "/api", "admin_state_up": true,
			"tags": safetyRouteTags(t, identity, roleRulePath),
		},
		pool: {
			"loadbalancers": parents(path.Base(gatewayLB)), "protocol": "HTTP", "lb_algorithm": "ROUND_ROBIN",
			"admin_state_up": true, "tags": safetyRouteTags(t, identity, rolePool),
		},
		member: {
			"address": "10.0.0.10", "protocol_port": 30080, "subnet_id": "member-subnet", "admin_state_up": true,
			"tags": safetyRouteTags(t, identity, roleMember),
		},
		monitor: {
			"pools": parents(path.Base(pool)), "type": "HTTP", "delay": 10, "timeout": 5, "max_retries": 3,
			"max_retries_down": 3, "url_path": "/healthz", "http_method": "GET", "expected_codes": "200-399",
			"admin_state_up": true, "tags": safetyRouteTags(t, identity, roleMonitor),
		},
		floatingIP: {
			"port_id": "vip-port", "fixed_ip_address": "192.0.2.10", "floating_ip_address": "198.51.100.10",
			"floating_network_id": "public-network", "description": identity.GatewayDescription(roleFloatingIP),
		},
	}
	for resourcePath, data := range inventory {
		data["id"], data["project_id"] = path.Base(resourcePath), projectID
	}
	collections := map[string]struct {
		key, resource, parentFilter, parentID string
	}{
		listeners:   {"listeners", listener, "loadbalancer_id", path.Base(gatewayLB)},
		policies:    {"l7policies", policy, "listener_id", path.Base(listener)},
		rules:       {"rules", rule, "", ""},
		pools:       {"pools", pool, "loadbalancer_id", path.Base(gatewayLB)},
		members:     {"members", member, "", ""},
		monitors:    {"healthmonitors", monitor, "pool_id", path.Base(pool)},
		floatingIPs: {"floatingips", floatingIP, "", ""},
	}
	discoveryTags, err := identity.GatewayDiscoveryTags(roleLoadBalancer)
	if err != nil {
		t.Fatal(err)
	}
	requireQuery := func(w http.ResponseWriter, request *http.Request, want url.Values) bool {
		if got := request.URL.Query(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s query = %v, want %v", request.URL.Path, got, want)
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return false
		}
		return true
	}
	slices.Sort(discoveryTags)
	projectQuery := url.Values{"project_id": {projectID}}
	wantDeletes := []string{rule, policy, monitor, member, pool, listener, floatingIP, gatewayLB}
	var deletes []string
	discoveries := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		resource := request.URL.Path
		switch {
		case request.Method == http.MethodGet && resource == lbList:
			query := request.URL.Query()
			var tags []string
			if query.Has("tags") {
				if !requireQuery(w, request, url.Values{"project_id": {projectID}, "tags": query["tags"]}) {
					return
				}
				// Octavia accepts both repeated and comma-separated values.
				for _, group := range query["tags"] {
					tags = append(tags, strings.Split(group, ",")...)
				}
				slices.Sort(tags)
				if !slices.Equal(tags, discoveryTags) {
					t.Errorf("load balancer discovery tags = %v, want %v", tags, discoveryTags)
					http.Error(w, "incorrect tag filter", http.StatusBadRequest)
					return
				}
				discoveries++
			} else if !requireQuery(w, request, projectQuery) {
				return
			}
			items := []map[string]any{}
			for _, lb := range []string{gatewayLB, magnumLB, occmLB} {
				if data := inventory[lb]; data != nil && openstackidentity.HasAllTags(data["tags"].([]string), tags) {
					items = append(items, data)
				}
			}
			safetyWriteJSON(t, w, map[string]any{"loadbalancers": items})
		case request.Method == http.MethodGet && resource == gatewayLB && inventory[gatewayLB] != nil:
			if requireQuery(w, request, url.Values{}) {
				safetyWriteJSON(t, w, map[string]any{"loadbalancer": inventory[gatewayLB]})
			}
		case request.Method == http.MethodGet && collections[resource].key != "":
			collection := collections[resource]
			query := url.Values{"project_id": {projectID}}
			if collection.parentFilter != "" {
				query.Set(collection.parentFilter, collection.parentID)
			}
			if !requireQuery(w, request, query) {
				return
			}
			items := []map[string]any{}
			if data := inventory[collection.resource]; data != nil {
				items = append(items, data)
			}
			safetyWriteJSON(t, w, map[string]any{collection.key: items})
		case request.Method == http.MethodDelete && len(deletes) < len(wantDeletes) && resource == wantDeletes[len(deletes)]:
			if !requireQuery(w, request, url.Values{}) {
				return
			}
			delete(inventory, resource)
			deletes = append(deletes, resource)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected cloud request: %s %s", request.Method, request.URL.RequestURI())
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	})
	newProvider := func(dropDeleteResponse bool) *Provider {
		lb := safetyServiceClient(handler)
		lb.ResourceBase = lb.Endpoint + "v2/"
		if dropDeleteResponse {
			transport := lb.HTTPClient.Transport
			lb.HTTPClient.Transport = safetyRoundTripper(func(request *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(request)
				if err == nil && request.Method == http.MethodDelete && request.URL.Path == rule && response.StatusCode == http.StatusNoContent {
					_ = response.Body.Close()
					return nil, io.ErrUnexpectedEOF
				}
				return response, err
			})
		}
		network := safetyServiceClient(handler)
		network.ResourceBase = network.Endpoint + "v2.0/"
		return newTestProvider(t, ServiceClients{LoadBalancer: lb, Network: network, ProjectID: projectID}, ProviderConfig{})
	}
	provider := newProvider(true)
	spec := completeGraphSpec(identity)
	spec.Routes[0].Spec.Gateway = cloud.GatewayState{}
	spec.Routes[0].Spec.Hostname = ""
	spec.Gateway.Spec.ExternalNetworkID = "public-network"
	spec.Routes[0].Spec.GatewayRequirements.ExternalNetworkID = "public-network"

	before, err := provider.octavia.ListLoadBalancers(ctx, loadbalancers.ListOpts{ProjectID: projectID})
	if err != nil || len(before) != 3 || before[0].ID != path.Base(gatewayLB) ||
		before[1].ID != path.Base(magnumLB) || before[2].ID != path.Base(occmLB) {
		t.Fatalf("initial project inventory = %#v, err = %v", before, err)
	}
	result, err := provider.ReconcileGatewayGraph(ctx, spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady || result.Gateway.State.LoadBalancerID != path.Base(gatewayLB) ||
		len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady ||
		len(deletes) != 0 || discoveries != 1 {
		t.Fatalf("initial Gateway graph = %#v, err = %v, deletes = %v, discoveries = %d", result, err, deletes, discoveries)
	}
	spec = cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphDelete}}
	if _, err := provider.ReconcileGatewayGraph(ctx, spec); !errors.Is(err, io.ErrUnexpectedEOF) || !slices.Equal(deletes, wantDeletes[:1]) {
		t.Fatalf("lost response = %v, applied deletes = %v", err, deletes)
	}

	provider = newProvider(false)
	for index, want := range wantDeletes[1:] {
		previousDiscoveries := discoveries
		result, err = provider.ReconcileGatewayGraph(ctx, spec)
		if err != nil || result.Outcome.State != cloud.OutcomeProgressing || discoveries != previousDiscoveries+1 ||
			!slices.Equal(deletes, wantDeletes[:index+2]) {
			t.Fatalf("apply DELETE %s after restart: outcome = %#v, err = %v, deletes = %v", want, result.Outcome, err, deletes)
		}
	}
	result, err = provider.ReconcileGatewayGraph(ctx, spec)
	if err != nil || result.Outcome.State != cloud.OutcomeReady || !slices.Equal(deletes, wantDeletes) {
		t.Fatalf("Gateway finalization = %#v, err = %v, deletes = %v", result.Outcome, err, deletes)
	}

	after, err := provider.octavia.ListLoadBalancers(ctx, loadbalancers.ListOpts{ProjectID: projectID})
	if err != nil || !reflect.DeepEqual(after, before[1:]) || len(inventory) != 2 {
		t.Fatalf("final inventory = %#v, err = %v; want both original control load balancers", after, err)
	}
}
