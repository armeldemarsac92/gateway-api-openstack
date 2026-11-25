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

// Package graph observes, validates, and reconciles the complete OpenStack
// resource graph owned by Gateway API objects.
package graph

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/stretchr/testify/require"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
)

func TestOctaviaServiceRetryBudgetPoolLifecycle(t *testing.T) {
	currentBudget := ""
	var writes []string
	client := safetyServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /lbaas/providers/amphora/gateway_capabilities":
			safetyWriteJSON(t, w, map[string]any{"gateway_capabilities": []string{octavia.GatewayCapabilityBackendRetryBudget}})
		case "GET /lbaas/pools":
			item := map[string]any{"id": "pool", "protocol": "HTTP"}
			if currentBudget != "" {
				item["backend_retry_budget"] = json.RawMessage(currentBudget)
			}
			safetyWriteJSON(t, w, map[string]any{"pools": []any{item}})
		case "POST /lbaas/pools", "PUT /lbaas/pools/pool":
			var request struct {
				Pool map[string]json.RawMessage `json:"pool"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			currentBudget = string(request.Pool["backend_retry_budget"])
			writes = append(writes, currentBudget)
			if r.Method == http.MethodPost {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"pool":{"id":"pool"}}`))
				return
			}
			safetyWriteJSON(t, w, map[string]any{"pool": map[string]any{"id": "pool"}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	p := NewProvider(ServiceClients{LoadBalancer: client, ProjectID: "project-a"}, ProviderConfig{BackendRetryBudget: true})
	spec := completeGraphSpec(safetyTestIdentity(t))
	budget := cloud.BackendRetryBudget{ServiceUID: "api-uid", BudgetPercent: 20, BudgetIntervalMS: 10000, MinRetryCount: 10, MinRetryIntervalMS: 1000}
	spec.Routes[0].Spec.BackendRetryBudget = &budget
	desired, err := p.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	actual := observedGatewayGraph{loadBalancer: safetyObservedLoadBalancer()}
	actual.loadBalancer.Provider = "amphora"
	p.observeBackendRetryBudgetCapability(t.Context(), &desired, actual)
	wanted := desired.routes[0]
	require.True(t, wanted.backendRetryBudgetAvailable)
	wanted.spec.Gateway.LoadBalancerID = actual.loadBalancer.ID
	require.NoError(t, p.executeRouteMutation(t.Context(), wanted, routeMutation{kind: routeMutationCreatePool}))
	require.JSONEq(t, `{"service_uid":"api-uid","budget_percent":20,"budget_interval_ms":10000,"min_retry_count":10,"min_retry_interval_ms":1000}`, writes[0])

	observe := func() observedRouteGraph {
		inventory, err := p.octavia.ListPoolInventory(t.Context(), pools.ListOpts{})
		require.NoError(t, err)
		require.Len(t, inventory.Items, 1)
		return observedRouteGraph{pool: &inventory.Items[0], backendRetryBudget: inventory.BackendRetryBudget["pool"]}
	}
	step, err := planBackendRetryBudget(wanted, observe())
	require.NoError(t, err)
	require.Nil(t, step)
	amended := budget
	amended.BudgetPercent = 0
	wanted.spec.BackendRetryBudget = &amended
	step, err = planBackendRetryBudget(wanted, observe())
	require.NoError(t, err)
	require.NotNil(t, step)
	require.NoError(t, p.executeRouteMutation(t.Context(), wanted, *step))
	require.JSONEq(t, `{"service_uid":"api-uid","budget_percent":0,"budget_interval_ms":10000,"min_retry_count":10,"min_retry_interval_ms":1000}`, writes[1])

	original := currentBudget
	currentBudget = `{"service_uid":"api-uid"}`
	step, err = planBackendRetryBudget(wanted, observe())
	require.ErrorIs(t, err, errBackendRetryBudgetReadback)
	require.Nil(t, step)
	currentBudget = original

	wanted.spec.BackendRetryBudget = nil
	wanted.backendRetryBudgetAvailable = false
	step, err = planBackendRetryBudget(wanted, observe())
	require.NoError(t, err)
	require.NotNil(t, step)
	require.NoError(t, p.executeRouteMutation(t.Context(), wanted, *step))
	require.Equal(t, "null", writes[2])
	step, err = planBackendRetryBudget(wanted, observe())
	require.NoError(t, err)
	require.Nil(t, step)
}

func TestOctaviaServiceRetryBudgetHoldsOnlyAffectedFragment(t *testing.T) {
	identity := safetyTestIdentity(t)
	siblingValue := identity.Value()
	siblingValue.RouteUID, siblingValue.RouteName = "sibling-uid", "sibling"
	sibling, err := NewIdentity(siblingValue)
	require.NoError(t, err)
	revokedValue := identity.Value()
	revokedValue.RouteUID, revokedValue.RouteName = "revoked-uid", "revoked"
	revoked, err := NewIdentity(revokedValue)
	require.NoError(t, err)

	provider := &Provider{projectID: identity.Value().OpenStackProjectID}
	spec := completeGraphSpec(identity)
	spec.Gateway.Spec.ExternalNetworkID = ""
	spec.Routes[0].Spec.GatewayRequirements.ExternalNetworkID = ""
	budget := cloud.BackendRetryBudget{ServiceUID: "service-b", BudgetPercent: 20, BudgetIntervalMS: 10000, MinRetryCount: 10, MinRetryIntervalMS: 1000}
	spec.Routes[0].Spec.BackendRetryBudget = &budget
	siblingSpec := spec.Routes[0].Spec
	siblingSpec.Identity, siblingSpec.PathValue, siblingSpec.BackendRetryBudget = sibling.Value(), "/sibling", nil
	siblingSpec.Precedence = 1
	revokedSpec := cloud.RouteSpec{Identity: revoked.Value()}
	spec.Routes = append(spec.Routes, cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: siblingSpec},
		cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: revokedSpec})
	desired, err := provider.buildDesiredGatewayGraph(spec)
	require.NoError(t, err)
	actual := observedGatewayGraph{
		loadBalancer: safetyObservedLoadBalancer(),
		listener:     &listeners.Listener{ID: "listener-1", Protocol: "HTTP", ProtocolPort: 80, AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		protocol:     cloud.ListenerProtocolHTTP,
		state:        cloud.GatewayState{LoadBalancerID: "load-balancer-1", ListenerID: "listener-1"},
		routes: []observedRouteGraph{{}, {}, {policies: []observedRoutePolicy{{
			policy: l7policies.L7Policy{ID: "revoked-policy", ListenerID: "listener-1", AdminStateUp: true, ProvisioningStatus: "ACTIVE"},
		}}}},
	}
	actual.loadBalancer.AdminStateUp, actual.loadBalancer.ProvisioningStatus = true, "ACTIVE"
	priorBudget := budget
	priorBudget.ServiceUID = "service-a"
	priorPool := &pools.Pool{ID: "route-pool", Protocol: "HTTP", LBMethod: "ROUND_ROBIN", AdminStateUp: true, ProvisioningStatus: "ACTIVE"}
	for _, test := range []struct {
		name     string
		observed observedRouteGraph
		hold     error
	}{
		{name: "reused pool still belongs to Service A", observed: observedRouteGraph{pool: priorPool,
			backendRetryBudget: octavia.PoolBackendRetryBudget{Present: true, Value: &priorBudget}}, hold: errBackendRetryBudgetCapability},
		{name: "provider budget is malformed", observed: observedRouteGraph{pool: priorPool,
			backendRetryBudget: octavia.PoolBackendRetryBudget{Present: true}}, hold: errBackendRetryBudgetReadback},
		{name: "new pool requires the capability", hold: errBackendRetryBudgetCapability},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := desired.routes[0]
			actual.routes[0] = test.observed
			steps, _, err := buildRouteEnsurePlan(route, test.observed)
			require.ErrorIs(t, err, test.hold)
			require.Empty(t, steps)

			plan, result, err := provider.buildGatewayGraphPlan(desired, actual)
			require.NoError(t, err)
			require.NotEmpty(t, plan)
			require.Equal(t, routeMutationDisablePolicy, plan[0].route.kind)
			require.Equal(t, "revoked-policy", plan[0].route.id)
			require.Equal(t, test.hold.Error(), result.Routes[0].Result.Outcome.Message)

			withoutRevocation := actual
			withoutRevocation.routes = append([]observedRouteGraph(nil), actual.routes...)
			withoutRevocation.routes[2] = observedRouteGraph{}
			plan, _, err = provider.buildGatewayGraphPlan(desired, withoutRevocation)
			require.NoError(t, err)
			require.NotEmpty(t, plan)
			require.Equal(t, 1, plan[0].routeIndex)
			require.Equal(t, routeMutationCreatePool, plan[0].route.kind)
		})
	}
	verified := observedRouteGraph{pool: priorPool, backendRetryBudget: octavia.PoolBackendRetryBudget{Present: true, Value: &budget}}
	steps, _, err := buildRouteEnsurePlan(desired.routes[0], verified)
	require.NoError(t, err)
	require.NotEmpty(t, steps)
	require.Equal(t, routeMutationCreateMember, steps[0].kind)
}
