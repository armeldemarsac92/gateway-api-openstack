//go:build envtest

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

package controller_test

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	gatewaycontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	httproutecontroller "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/httproute"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	recordingProviderVIP       = "192.0.2.10"
	recordingProviderStaticVIP = "192.0.2.30"
)

type recordingProvider struct {
	mu               sync.Mutex
	gatewaySpecs     []cloud.GatewaySpec
	graphSpecs       []cloud.GatewayGraphSpec
	gatewayStates    map[string]cloud.GatewayState
	gatewayResults   map[string]cloud.GatewayResult
	gatewayDeleteOut *cloud.Outcome
	deletedGateways  []cloud.Identity
}

func (p *recordingProvider) EnsureGateway(_ context.Context, spec cloud.GatewaySpec) (cloud.GatewayResult, error) {
	p.gatewaySpecs = append(p.gatewaySpecs, spec)
	if result, overridden := p.gatewayResults[spec.Identity.GatewayUID]; overridden {
		return result, nil
	}
	if spec.RequestedVIPAddresses != nil {
		return p.ensureExperimentalGatewayAddress(spec), nil
	}
	vip := recordingProviderVIP
	if spec.RequestedVIPAddress != "" {
		if spec.RequestedVIPAddress != recordingProviderStaticVIP {
			result := cloud.GatewayProgressingResult("Fixture cannot allocate the requested VIP", time.Second)
			result.AddressIssue = result.Outcome.Message
			return result, nil
		}
		vip = recordingProviderStaticVIP
	}
	return p.recordGatewayAddress(spec.Identity.GatewayUID, vip), nil
}

func (p *recordingProvider) ensureExperimentalGatewayAddress(spec cloud.GatewaySpec) cloud.GatewayResult {
	if existing, ok := p.gatewayStates[spec.Identity.GatewayUID]; ok {
		result := cloud.GatewayReadyResult(existing)
		result.AddressActive = true
		if !slices.Contains(spec.RequestedVIPAddresses, "") && !slices.Contains(spec.RequestedVIPAddresses, existing.VIPAddress) {
			result.AddressIssue = "Fixture cannot replace the existing VIP"
		}
		return result
	}
	for _, candidate := range spec.RequestedVIPAddresses {
		switch candidate {
		case recordingProviderStaticVIP:
			return p.recordGatewayAddress(spec.Identity.GatewayUID, recordingProviderStaticVIP)
		case "":
			return p.recordGatewayAddress(spec.Identity.GatewayUID, recordingProviderVIP)
		}
	}
	result := cloud.GatewayProgressingResult("Fixture cannot allocate a requested VIP", time.Second)
	result.AddressIssue = result.Outcome.Message
	result.AddressAbsent, result.AddressBlocked = true, true
	return result
}

func (p *recordingProvider) recordGatewayAddress(uid, vip string) cloud.GatewayResult {
	state := cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: vip, ListenerID: "listener-1"}
	if p.gatewayStates == nil {
		p.gatewayStates = make(map[string]cloud.GatewayState)
	}
	p.gatewayStates[uid] = state
	return cloud.GatewayReadyResult(state)
}

func (p *recordingProvider) DeleteGateway(_ context.Context, identity cloud.Identity) (cloud.Outcome, error) {
	p.deletedGateways = append(p.deletedGateways, identity)
	if p.gatewayDeleteOut != nil {
		return *p.gatewayDeleteOut, nil
	}
	delete(p.gatewayStates, identity.GatewayUID)
	return cloud.ReadyOutcome(), nil
}

func (*recordingProvider) EnsureRoute(_ context.Context, spec cloud.RouteSpec) (cloud.RouteResult, error) {
	return cloud.RouteReadyResult(cloud.RouteState{PoolID: "pool-" + spec.Identity.RouteUID}), nil
}

func (*recordingProvider) DeleteRoute(context.Context, cloud.Identity) (cloud.Outcome, error) {
	return cloud.ReadyOutcome(), nil
}

func testConfig() controller.Config {
	return controller.Config{
		ControllerName:          "example.com/gateway-api-openstack",
		ControllerVersion:       "test",
		OpenStackProjectID:      "project-a",
		ClusterID:               "cluster-a",
		OpenStackResyncInterval: time.Minute,
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := configv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := gatewayv1.Install(scheme); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("GATEWAY_API_CHANNEL") == "experimental" {
		if err := gatewayx.Install(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func (*recordingProvider) ResolveTopology(_ context.Context, request cloud.TopologyRequest) (cloud.Topology, error) {
	topology := cloud.Topology{VIPSubnetID: request.VIPSubnet.ID, MemberSubnetID: request.MemberSubnet.ID, FlavorID: request.FlavorID, AvailabilityZone: request.AvailabilityZone}
	if request.ExternalNetwork != nil {
		topology.ExternalNetworkID = request.ExternalNetwork.ID
	}
	for _, reference := range request.VIPSecurityGroups {
		topology.VIPSecurityGroupIDs = append(topology.VIPSecurityGroupIDs, reference.ID)
	}
	for _, reference := range request.BackendSecurityGroups {
		topology.BackendSecurityGroupIDs = append(topology.BackendSecurityGroupIDs, reference.ID)
	}
	return topology, nil
}

func newEnvtestGatewayReconciler(kubeClient client.Client, reader client.Reader, provider *recordingProvider, config controller.Config) *gatewaycontroller.Reconciler {
	coordinator := &graph.Coordinator{}
	gateway := &gatewaycontroller.Reconciler{Client: kubeClient, APIReader: reader, TopologyResolver: provider, Coordinator: coordinator, Config: config}
	route := &httproutecontroller.Reconciler{Client: kubeClient, APIReader: reader, Coordinator: coordinator, Config: config}
	writer := &graph.Writer{Coordinator: coordinator, Gateway: gateway, Routes: route, Provider: provider}
	gateway.Writer = writer
	route.Writer = writer
	return gateway
}

func (p *recordingProvider) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.graphSpecs = append(p.graphSpecs, spec)
	result := cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1", VIPAddress: recordingProviderVIP, ListenerID: "listener-1"})}
	if spec.Gateway.Disposition == cloud.GraphDelete {
		outcome, err := p.DeleteGateway(ctx, spec.Identity)
		result.Outcome = outcome
		result.Gateway.Outcome = outcome
		return result, err
	}
	if spec.Gateway.Disposition == cloud.GraphEnsure {
		gatewayResult, err := p.EnsureGateway(ctx, spec.Gateway.Spec)
		result.Gateway, result.Outcome = gatewayResult, gatewayResult.Outcome
		if err != nil || result.Outcome.State != cloud.OutcomeReady {
			return result, err
		}
		for _, listener := range spec.Gateway.Spec.ScopedListeners {
			result.Gateway.ScopedListenerReadyPorts = append(result.Gateway.ScopedListenerReadyPorts, listener.Port)
		}
	}
	for _, route := range spec.Routes {
		switch route.Disposition {
		case cloud.GraphDelete:
			outcome, err := p.DeleteRoute(ctx, route.Spec.Identity)
			result.Outcome = outcome
			result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: route.Spec.Identity, Result: cloud.RouteResult{Outcome: outcome}})
			if err != nil {
				return result, err
			}
		case cloud.GraphEnsure:
			routeResult, err := p.EnsureRoute(ctx, route.Spec)
			result.Outcome = routeResult.Outcome
			result.Routes = append(result.Routes, cloud.RouteGraphResult{Identity: route.Spec.Identity, Result: routeResult})
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
