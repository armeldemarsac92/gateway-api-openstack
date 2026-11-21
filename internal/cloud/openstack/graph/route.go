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
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
)

type desiredPolicy struct {
	role        string
	position    int32
	compareType l7policies.CompareType
	path        string
}

type desiredRule struct {
	role        string
	ruleType    l7policies.RuleType
	compareType l7policies.CompareType
	value       string
}

type desiredRoute struct {
	identity          Identity
	spec              cloud.RouteSpec
	members           []cloud.Member
	healthMonitor     cloud.HealthMonitorConfig
	policies          []desiredPolicy
	hostnameRuleValue string
}

func (r desiredRoute) protocol() string {
	return cloud.NormalizeListenerProtocol(r.spec.GatewayRequirements.ListenerProtocol)
}

func (r desiredRoute) monitorType() string {
	if r.protocol() == cloud.ListenerProtocolUDP {
		return monitors.TypeUDPConnect
	}
	return r.protocol()
}

func (p *Provider) buildDesiredRoute(spec cloud.RouteSpec) (desiredRoute, error) {
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return desiredRoute{}, err
	}
	invalid := func(err error) (desiredRoute, error) {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	for _, role := range []string{
		rolePool,
		roleMember,
		roleMonitor,
		rolePolicyExact,
		rolePolicyPrefix,
		roleRulePath,
		roleRuleHost,
	} {
		if _, err := identity.RouteTags(role); err != nil {
			return invalid(fmt.Errorf("build %s identity tags: %w", role, err))
		}
	}
	if strings.TrimSpace(spec.GatewayRequirements.Provider) == "" {
		return invalid(errors.New("gateway provider must not be empty"))
	}
	if strings.TrimSpace(spec.GatewayRequirements.VIPSubnetID) == "" {
		return invalid(errors.New("gateway VIP subnet ID must not be empty"))
	}
	if spec.GatewayRequirements.ListenerPort < 1 || spec.GatewayRequirements.ListenerPort > 65535 {
		return invalid(fmt.Errorf("gateway listener port %d is outside 1-65535", spec.GatewayRequirements.ListenerPort))
	}
	switch spec.MemberMode {
	case "", cloud.MemberModeNodePort:
		spec.MemberMode = cloud.MemberModeNodePort
	case cloud.MemberModeEndpoint:
		if len(spec.BackendSecurityGroupIDs) != 0 {
			return invalid(errors.New("endpoint members require unmanaged backend security"))
		}
	default:
		return invalid(fmt.Errorf("unsupported member mode %q", spec.MemberMode))
	}
	if len(spec.Members) == 0 && spec.MemberMode != cloud.MemberModeEndpoint {
		return invalid(errors.New("at least one ready backend member is required"))
	}
	if strings.TrimSpace(spec.MemberSubnetID) == "" {
		return invalid(errors.New("member subnet ID must not be empty"))
	}
	var policies []desiredPolicy
	var hostnameRuleValue string
	protocol := cloud.NormalizeListenerProtocol(spec.GatewayRequirements.ListenerProtocol)
	switch protocol {
	case cloud.ListenerProtocolHTTP:
		if spec.PathType != cloud.PathMatchExact && spec.PathType != cloud.PathMatchPrefix {
			return invalid(fmt.Errorf("unsupported path match type %q", spec.PathType))
		}
		exactPath, prefixPath, err := amphora.PathRuleValues(spec.PathValue, spec.PathType == cloud.PathMatchPrefix)
		if err != nil {
			return invalid(err)
		}
		policies = policiesFor(exactPath, prefixPath)
		if spec.Hostname != "" {
			hostnameRuleValue, err = amphora.HostnameRuleValue(spec.Hostname)
			if err != nil {
				return invalid(err)
			}
		}
	case cloud.ListenerProtocolTCP, cloud.ListenerProtocolUDP:
		if spec.Hostname != "" || spec.PathType != "" || spec.PathValue != "" {
			return invalid(fmt.Errorf("a %s route cannot contain HTTP matches", protocol))
		}
	default:
		return invalid(fmt.Errorf("unsupported listener protocol %q", protocol))
	}

	membersByKey := make(map[string]cloud.Member, len(spec.Members))
	for _, member := range spec.Members {
		if member.Port < 1 || member.Port > 65535 {
			return invalid(fmt.Errorf("member %s has invalid port %d", member.Address, member.Port))
		}
		address, err := netip.ParseAddr(member.Address)
		if err != nil {
			return invalid(fmt.Errorf("member address %q is not a valid IP address: %w", member.Address, err))
		}
		if spec.MemberMode == cloud.MemberModeEndpoint {
			if !address.Is4() || !address.IsGlobalUnicast() || address.IsLoopback() || address.IsLinkLocalUnicast() {
				return invalid(errors.New("endpoint members require unicast IPv4 destinations"))
			}
			if member.ProviderID != "" {
				return invalid(errors.New("endpoint members must not identify a worker provider instance"))
			}
		} else if member.Draining {
			return invalid(errors.New("draining members require endpoint mode"))
		}
		member.Address = address.String()
		key := memberKey(member.Address, member.Port)
		if existing, duplicate := membersByKey[key]; duplicate && (existing.ProviderID != member.ProviderID || existing.Draining != member.Draining) {
			return invalid(errors.New("members with the same address and port must have consistent state"))
		}
		membersByKey[key] = member
	}
	keys := make([]string, 0, len(membersByKey))
	for key := range membersByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	members := make([]cloud.Member, 0, len(keys))
	for _, key := range keys {
		members = append(members, membersByKey[key])
	}

	monitor := spec.HealthMonitor
	if protocol == cloud.ListenerProtocolHTTP && !strings.HasPrefix(monitor.Path, "/") {
		return invalid(fmt.Errorf("health check path %q must start with /", monitor.Path))
	}
	if monitor != (cloud.HealthMonitorConfig{}) &&
		(monitor.Delay < 1 || monitor.Timeout < 1 || monitor.Timeout >= monitor.Delay ||
			monitor.MaxRetries < 1 || monitor.MaxRetries > 10 || monitor.MaxRetriesDown < 1 || monitor.MaxRetriesDown > 10) {
		return invalid(errors.New("health monitor requires positive intervals, timeout below delay, and retry counts within 1-10"))
	}
	return desiredRoute{
		identity:          identity,
		spec:              spec,
		members:           members,
		healthMonitor:     monitor,
		policies:          policies,
		hostnameRuleValue: hostnameRuleValue,
	}, nil
}

func policiesFor(exactPath, prefixPath string) []desiredPolicy {
	var policies []desiredPolicy
	if exactPath != "" {
		policies = append(policies, desiredPolicy{role: rolePolicyExact, position: 1, compareType: l7policies.CompareTypeEqual, path: exactPath})
	}
	if prefixPath != "" {
		policies = append(policies, desiredPolicy{role: rolePolicyPrefix, position: int32(len(policies) + 1), compareType: l7policies.CompareTypeStartWith, path: prefixPath})
	}
	return policies
}

func rulesFor(policy desiredPolicy, hostnameRuleValue string) []desiredRule {
	rules := []desiredRule{{
		role:        roleRulePath,
		ruleType:    l7policies.TypePath,
		compareType: policy.compareType,
		value:       policy.path,
	}}
	if hostnameRuleValue != "" {
		rules = append(rules, desiredRule{
			role:        roleRuleHost,
			ruleType:    l7policies.TypeHostName,
			compareType: l7policies.CompareTypeRegex,
			value:       hostnameRuleValue,
		})
	}
	return rules
}

func memberKey(address string, port int) string {
	parsed, err := netip.ParseAddr(address)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Sprintf("%s:%d", address, port)
	}
	return netip.AddrPortFrom(parsed, uint16(port)).String()
}
