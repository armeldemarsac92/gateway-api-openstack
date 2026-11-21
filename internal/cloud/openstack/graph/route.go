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
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
)

// Octavia's public member create and update APIs accept weights from 0 to 256.
const maxOctaviaMemberWeight = 256

type desiredPolicy struct {
	role              string
	position          int32
	compareType       l7policies.CompareType
	path              string
	precedence        int
	backendKey        string
	matchKey          string
	hostnameRuleValue string
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
	httpBackends      []desiredRoute
	backendKey        string
	unavailable       bool
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
	if len(spec.HTTPBackends) != 0 {
		return p.buildDesiredHTTPBackends(spec)
	}
	route, err := p.buildDesiredRoutePart(spec, "", false)
	for index := range route.policies {
		route.policies[index].precedence = spec.Precedence
	}
	return route, err
}

func (p *Provider) buildDesiredRoutePart(spec cloud.RouteSpec, backendKey string, unavailable bool) (desiredRoute, error) {
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
	if len(spec.Members) == 0 && spec.MemberMode != cloud.MemberModeEndpoint && !unavailable {
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
		if backendKey == "" {
			policies, hostnameRuleValue, err = httpPathPolicies(cloud.HTTPPathMatch{
				Hostname: spec.Hostname, PathType: spec.PathType, PathValue: spec.PathValue,
			}, false)
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
		if member.Weight != 0 && (spec.MemberMode != cloud.MemberModeEndpoint ||
			(protocol != cloud.ListenerProtocolTCP && protocol != cloud.ListenerProtocolUDP)) {
			return invalid(errors.New("explicit member weights require TCP or UDP endpoint mode"))
		}
		if member.Weight < 0 || member.Weight > maxOctaviaMemberWeight {
			return invalid(fmt.Errorf("explicit member weight %d is outside 1-%d", member.Weight, maxOctaviaMemberWeight))
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
		if existing, duplicate := membersByKey[key]; duplicate && existing != member {
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
		backendKey:        backendKey,
		unavailable:       unavailable,
	}, nil
}

func (p *Provider) buildDesiredHTTPBackends(spec cloud.RouteSpec) (desiredRoute, error) {
	invalid := func(err error) (desiredRoute, error) {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	if cloud.NormalizeListenerProtocol(spec.GatewayRequirements.ListenerProtocol) != cloud.ListenerProtocolHTTP ||
		spec.Hostname != "" || spec.PathType != "" || spec.PathValue != "" || len(spec.Members) != 0 {
		return invalid(errors.New("HTTP backend groups require HTTP routing without scalar matches or members"))
	}
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return desiredRoute{}, err
	}
	route := desiredRoute{identity: identity, spec: spec}
	backendKeys := make(map[string]bool, len(spec.HTTPBackends))
	for _, backend := range spec.HTTPBackends {
		if strings.TrimSpace(backend.Namespace) == "" || strings.TrimSpace(backend.Name) == "" || backend.Port < 1 || backend.Port > 65535 ||
			len(backend.Matches) == 0 || backend.Unavailable && len(backend.Members) != 0 {
			return invalid(errors.New("each HTTP backend requires a Service namespace, name, valid port and matches, and an unavailable backend must have no members"))
		}
		key := openstackidentity.HTTPBackendKey(backend.Namespace, backend.Name, backend.Port)
		if backendKeys[key] {
			return invalid(errors.New("duplicate HTTP backend within one route"))
		}
		backendKeys[key] = true
		partSpec := spec
		partSpec.HTTPBackends, partSpec.Members = nil, backend.Members
		part, err := p.buildDesiredRoutePart(partSpec, key, backend.Unavailable)
		if err != nil {
			return desiredRoute{}, err
		}
		for _, role := range []string{rolePool, roleMember, roleMonitor} {
			if _, err := part.identity.RouteTags(part.httpRole(role, "")); err != nil {
				return invalid(err)
			}
		}
		matchKeys := make(map[string]bool, len(backend.Matches))
		for _, match := range backend.Matches {
			if match.PathType == cloud.PathMatchPrefix {
				match.PathValue = strings.TrimRight(match.PathValue, "/")
			}
			if match.PathValue == "" {
				match.PathValue = "/"
			}
			policies, hostnameRuleValue, err := httpPathPolicies(match, true)
			if err != nil {
				return invalid(err)
			}
			matchKey := openstackidentity.HTTPMatchKey(match.Hostname, match.PathType, match.PathValue)
			if matchKeys[matchKey] {
				return invalid(errors.New("duplicate HTTP match within one backend"))
			}
			matchKeys[matchKey] = true
			for _, policy := range policies {
				policy.backendKey, policy.matchKey = key, matchKey
				policy.hostnameRuleValue, policy.precedence = hostnameRuleValue, match.Precedence
				policy.role = part.httpRole(policy.role, matchKey)
				if _, err := part.identity.RouteTags(policy.role); err != nil {
					return invalid(err)
				}
				for _, rule := range rulesFor(policy, "") {
					if _, err := part.identity.RouteTags(rule.role); err != nil {
						return invalid(err)
					}
				}
				part.policies = append(part.policies, policy)
			}
		}
		route.httpBackends = append(route.httpBackends, part)
	}
	sort.Slice(route.httpBackends, func(i, j int) bool { return route.httpBackends[i].backendKey < route.httpBackends[j].backendKey })
	return route, nil
}

func (r desiredRoute) policySets() [][]desiredPolicy {
	if len(r.httpBackends) == 0 {
		return [][]desiredPolicy{r.policies}
	}
	policies := make([][]desiredPolicy, 0, len(r.httpBackends))
	for _, backend := range r.httpBackends {
		policies = append(policies, backend.policies)
	}
	return policies
}

func (r desiredRoute) httpRole(base, match string) string {
	return httpResourceRole(base, r.backendKey, match)
}

func httpResourceRole(base, backend, match string) string {
	if backend == "" {
		return base
	}
	return openstackidentity.HTTPFragmentRole(base, backend, match)
}

func httpPathPolicies(match cloud.HTTPPathMatch, grouped bool) ([]desiredPolicy, string, error) {
	if match.PathType != cloud.PathMatchExact && match.PathType != cloud.PathMatchPrefix {
		return nil, "", fmt.Errorf("unsupported path match type %q", match.PathType)
	}
	exactPath, prefixPath, err := amphora.PathRuleValues(match.PathValue, match.PathType == cloud.PathMatchPrefix)
	if err != nil {
		return nil, "", err
	}
	var hostnameRuleValue string
	if match.Hostname != "" {
		hostnameRuleValue, err = amphora.HostnameRuleValue(match.Hostname)
		if err != nil {
			return nil, "", err
		}
	}
	if grouped && match.PathType == cloud.PathMatchPrefix {
		if pattern, ok := amphora.LiteralPathPrefixPattern(exactPath); ok {
			return []desiredPolicy{{role: rolePolicyPrefix, position: 1, compareType: l7policies.CompareTypeRegex, path: pattern}}, hostnameRuleValue, nil
		}
	}
	return policiesFor(exactPath, prefixPath), hostnameRuleValue, nil
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
	if policy.matchKey != "" {
		hostnameRuleValue = policy.hostnameRuleValue
	}
	rules := []desiredRule{{
		role:        httpResourceRole(roleRulePath, policy.backendKey, policy.matchKey),
		ruleType:    l7policies.TypePath,
		compareType: policy.compareType,
		value:       policy.path,
	}}
	if hostnameRuleValue != "" {
		rules = append(rules, desiredRule{
			role:        httpResourceRole(roleRuleHost, policy.backendKey, policy.matchKey),
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
