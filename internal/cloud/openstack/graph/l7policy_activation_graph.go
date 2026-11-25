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
	"maps"
	"net/netip"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

func activationLoadBalancerMatches(current, proven *octavia.LoadBalancer) bool {
	return current.ID == proven.ID && current.ProjectID == proven.ProjectID &&
		current.AdminStateUp && proven.AdminStateUp && current.ProvisioningStatus == "ACTIVE" &&
		current.Provider == proven.Provider && current.FlavorID == proven.FlavorID && current.AvailabilityZone == proven.AvailabilityZone &&
		current.VipAddress == proven.VipAddress && current.VipPortID == proven.VipPortID &&
		current.VipSubnetID == proven.VipSubnetID && current.VipNetworkID == proven.VipNetworkID &&
		current.VipQosPolicyID == proven.VipQosPolicyID && slices.Equal(current.AdditionalVips, proven.AdditionalVips) &&
		sameActivationTags(current.VIPSecurityGroupIDs, proven.VIPSecurityGroupIDs) && sameActivationTags(current.Tags, proven.Tags) &&
		sameActivationReferences(proven.Listeners, current.Listeners, func(row listeners.Listener) string { return row.ID }) &&
		sameActivationReferences(proven.Pools, current.Pools, func(row pools.Pool) string { return row.ID })
}

func activationListenerMatches(current *octavia.PolicyActivationListener, proven *listeners.Listener, defaultResponse int, virtualHosts []string) bool {
	code := 0
	if current.DefaultHTTPResponse != nil {
		code = current.DefaultHTTPResponse.StatusCode
		if code != 404 {
			return false
		}
	}
	return current.ID == proven.ID && current.ProjectID == proven.ProjectID &&
		current.AdminStateUp && proven.AdminStateUp && current.ProvisioningStatus == "ACTIVE" &&
		current.Protocol == proven.Protocol && current.ProtocolPort == proven.ProtocolPort &&
		string(current.DefaultPool) == "null" && proven.DefaultPoolID == "" &&
		code == defaultResponse && slices.Equal(current.HTTPSVirtualHosts, virtualHosts) &&
		sameActivationTags(current.AllowedCIDRs, proven.AllowedCIDRs) && maps.Equal(current.InsertHeaders, proven.InsertHeaders) &&
		current.ConnLimit == proven.ConnLimit && current.TimeoutClientData == proven.TimeoutClientData &&
		current.TimeoutMemberConnect == proven.TimeoutMemberConnect && current.TimeoutMemberData == proven.TimeoutMemberData &&
		current.TimeoutTCPInspect == proven.TimeoutTCPInspect && sameActivationTags(current.Tags, proven.Tags) &&
		sameActivationReferences(proven.Loadbalancers, current.Loadbalancers, func(row listeners.LoadBalancerID) string { return row.ID }) &&
		sameActivationReferences(proven.L7Policies, current.L7Policies, func(row l7policies.L7Policy) string { return row.ID })
}

func plainActivationPool(route observedRouteGraph) bool {
	pool := route.pool
	return pool.Protocol == cloud.ListenerProtocolHTTP && !pool.TLSEnabled && !route.backendH2C &&
		pool.Persistence == (pools.SessionPersistence{}) && !slices.Contains(pool.Tags, xBackendCookieTag) && !enrolledHeader(route) &&
		!hasObservedBackendTLS(*pool, route.backendTLS)
}

func activationDestinationsMatch(snapshot []octavia.PolicyActivationPoolInventory, destinations map[string]observedRouteGraph) bool {
	if len(snapshot) != len(destinations) {
		return false
	}
	for _, item := range snapshot {
		if item.Pool == nil || item.Members == nil {
			return false
		}
		proven, ok := destinations[item.Pool.ID]
		if !ok || !proven.backendTLS.AdminObserved || !proven.backendTLS.EnabledObserved || !proven.backendTLS.H2CObserved ||
			!activationPoolMatches(item.Pool, proven.pool) ||
			!activationMembersMatch(item.Members, proven) || !activationMonitorMatches(item.Pool.HealthMonitor, proven.monitor) {
			return false
		}
		delete(destinations, item.Pool.ID)
	}
	return true
}

func activationPoolMatches(current *octavia.PolicyActivationPool, proven *pools.Pool) bool {
	persistence := string(current.Persistence)
	return activationPoolCoreMatches(current, proven) && current.TLSEnabled != nil && !*current.TLSEnabled &&
		current.BackendH2C != nil && !*current.BackendH2C && current.BackendTLSHost == "" && !current.BackendRequireH2 &&
		(persistence == "" || persistence == "null")
}

func activationPoolCoreMatches(current *octavia.PolicyActivationPool, proven *pools.Pool) bool {
	return current.ID == proven.ID && current.ProjectID == proven.ProjectID &&
		current.AdminState != nil && *current.AdminState == proven.AdminStateUp && current.ProvisioningStatus != "" &&
		current.ProvisioningStatus == proven.ProvisioningStatus &&
		current.Protocol == proven.Protocol && current.LBMethod == proven.LBMethod &&
		current.TLSContainerRef == proven.TLSContainerRef && current.CATLSContainerRef == proven.CATLSContainerRef &&
		current.CRLContainerRef == proven.CRLContainerRef && current.TLSCiphers == proven.TLSCiphers &&
		slices.Equal(current.TLSVersions, proven.TLSVersions) && slices.Equal(current.ALPNProtocols, proven.ALPNProtocols) &&
		current.MonitorID == proven.MonitorID && sameActivationTags(current.Tags, proven.Tags) &&
		sameActivationReferences(proven.Loadbalancers, current.Loadbalancers, func(row pools.LoadBalancerID) string { return row.ID }) &&
		sameActivationReferences(proven.Listeners, current.Listeners, func(row pools.ListenerID) string { return row.ID }) &&
		sameActivationReferences(proven.Members, current.Members, func(row pools.Member) string { return row.ID })
}

func activationMembersMatch(snapshot []octavia.PolicyActivationMember, route observedRouteGraph) bool {
	if len(route.memberBatchUnreported) != 0 || len(snapshot) != len(route.members) {
		return false
	}
	members := make(map[string]pools.Member, len(route.members))
	for _, member := range route.members {
		members[member.ID] = member
	}
	for _, row := range snapshot {
		current := row.Member
		proven, ok := members[current.ID]
		address, err := netip.ParseAddr(current.Address)
		if !ok || err != nil || address.String() != proven.Address || current.ProtocolPort != proven.ProtocolPort ||
			current.ProjectID != route.pool.ProjectID || current.ProjectID != proven.ProjectID || current.PoolID != route.pool.ID ||
			current.SubnetID != proven.SubnetID || row.AdminStateUp == nil || *row.AdminStateUp != proven.AdminStateUp ||
			row.Backup == nil || *row.Backup != proven.Backup || row.Weight == nil || *row.Weight != proven.Weight ||
			current.ProvisioningStatus == "" || current.ProvisioningStatus != proven.ProvisioningStatus ||
			current.MonitorAddress != proven.MonitorAddress || current.MonitorPort != proven.MonitorPort ||
			!sameActivationTags(current.Tags, proven.Tags) {
			return false
		}
		delete(members, current.ID)
	}
	return true
}

func activationMonitorMatches(current, proven *monitors.Monitor) bool {
	if current == nil || proven == nil {
		return current == nil && proven == nil
	}
	return current.ID == proven.ID && current.ProjectID == proven.ProjectID &&
		current.AdminStateUp && proven.AdminStateUp && current.ProvisioningStatus == "ACTIVE" &&
		current.Type == proven.Type && current.Delay == proven.Delay && current.Timeout == proven.Timeout &&
		current.MaxRetries == proven.MaxRetries && current.MaxRetriesDown == proven.MaxRetriesDown &&
		current.HTTPMethod == proven.HTTPMethod && current.URLPath == proven.URLPath && current.ExpectedCodes == proven.ExpectedCodes &&
		current.HTTPVersion == proven.HTTPVersion && current.DomainName == proven.DomainName &&
		sameActivationTags(current.Tags, proven.Tags)
}

func sameActivationReferences[T any](proven, current []T, idOf func(T) string) bool {
	return crossCheckCompleteChildInventory("activation", "graph", "reference", proven, current, idOf) == nil
}
