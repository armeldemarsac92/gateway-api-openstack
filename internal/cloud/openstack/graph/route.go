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

const (
	httpHeaderExactRuleType l7policies.RuleType = "HTTP_HEADER_EXACT"
	httpMethodRuleType      l7policies.RuleType = "HTTP_METHOD"
)

type desiredPolicy struct {
	role                     string
	position                 int32
	compareType              l7policies.CompareType
	path                     string
	pathInvert               bool
	precedence               int
	backendKey               string
	matchKey                 string
	hostnameRuleValue        string
	hostname                 string
	httpListenerHostname     string
	httpHostExclusions       []redirectGuard
	httpsListenerHostname    string
	redirect                 *desiredRedirectPolicy
	weighted                 bool
	weightedPlan             *weightedHTTPPolicyPlan
	requestHeaderOperations  []cloud.HTTPHeaderOperation
	responseHeaderOperations []cloud.HTTPHeaderOperation
	method                   string
	headers                  []cloud.HTTPHeaderMatch
}

type desiredRule struct {
	role        string
	ruleType    l7policies.RuleType
	compareType l7policies.CompareType
	value       string
	key         string
	invert      bool
}

type desiredRoute struct {
	identity               Identity
	spec                   cloud.RouteSpec
	members                []cloud.Member
	healthMonitor          cloud.HealthMonitorConfig
	policies               []desiredPolicy
	hostnameRuleValue      string
	httpParts              []desiredRoute
	backendKey             string
	unavailable            bool
	redirect               bool
	redirectMatches        []cloud.HTTPPathMatch
	redirectIssue          error
	fixedResponse          bool
	weightedTargets        []desiredHTTPWeightedTarget
	targetsPending         bool
	zeroWeightOnly         bool
	weightedAuthorizations map[string]map[string]int
	backendTLS             backendTLSPlan
}

func (r desiredRoute) protocol() string {
	return cloud.NormalizeListenerProtocol(r.spec.GatewayRequirements.ListenerProtocol)
}

func (r desiredRoute) monitorType() string {
	if r.protocol() == cloud.ListenerProtocolUDP {
		return monitors.TypeUDPConnect
	}
	if r.spec.TCPHealthMonitor {
		return monitors.TypeTCP
	}
	return r.protocol()
}

func (p *Provider) buildDesiredRoute(spec cloud.RouteSpec) (desiredRoute, error) {
	if len(spec.HTTPRedirects) != 0 {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("HTTP redirects require the complete Gateway routing snapshot"))
	}
	if len(spec.HTTPBackends) != 0 || len(spec.WeightedHTTPRules) != 0 || len(spec.FixedHTTPRules) != 0 {
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
	if spec.BackendTLS != nil && spec.BackendTLS.Denied {
		unavailable, spec.Members = true, nil
		spec.BackendH2C = false
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
	if spec.BackendTLS != nil && protocol != cloud.ListenerProtocolHTTP {
		return invalid(errors.New("BackendTLSPolicy requires an HTTP pool"))
	}
	if spec.BackendH2C && spec.BackendTLS == nil && (protocol != cloud.ListenerProtocolHTTP || !p.backendH2C) {
		return invalid(errors.New("h2c requires an HTTP pool and --octavia-backend-h2c"))
	}
	switch protocol {
	case cloud.ListenerProtocolHTTP:
		if backendKey == "" {
			match := cloud.HTTPPathMatch{
				Hostname: spec.Hostname, PathType: spec.PathType, PathValue: spec.PathValue,
			}
			policies, hostnameRuleValue, err = p.httpPathPolicies(match, false)
			if err != nil {
				return invalid(err)
			}
			for i := range policies {
				policies[i].hostname = spec.Hostname
				policies[i].httpListenerHostname = spec.HTTPListenerHostname
			}
			if scope := spec.HTTPSListenerHostname; scope != "" {
				for i := range policies {
					policy := &policies[i]
					policy.httpsListenerHostname = scope
					policy.matchKey = openstackidentity.HTTPListenerMatchKey(scope, match)
					policy.hostnameRuleValue = hostnameRuleValue
					policy.role = policyRuleRole(*policy, policy.role, "")
				}
			}
		}
	case cloud.ListenerProtocolTCP, cloud.ListenerProtocolUDP:
		if spec.Hostname != "" || spec.PathType != "" || spec.PathValue != "" || spec.HTTPSListenerHostname != "" || spec.HTTPListenerHostname != "" {
			return invalid(fmt.Errorf("a %s route cannot contain HTTP matches", protocol))
		}
	default:
		return invalid(fmt.Errorf("unsupported listener protocol %q", protocol))
	}

	supportsMemberWeights := usesDefaultPool(protocol)
	membersByKey := make(map[string]cloud.Member, len(spec.Members))
	for _, member := range spec.Members {
		if member.Port < 1 || member.Port > 65535 {
			return invalid(fmt.Errorf("member %s has invalid port %d", member.Address, member.Port))
		}
		if member.Weight != 0 && !supportsMemberWeights {
			return invalid(errors.New("explicit member weights require TCP or UDP"))
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
		spec.Hostname != "" || spec.PathType != "" || spec.PathValue != "" || len(spec.Members) != 0 || spec.BackendH2C || spec.BackendTLS != nil {
		return invalid(errors.New("HTTP backend groups require HTTP routing without scalar matches or members"))
	}
	identity, err := p.identity(spec.Identity)
	if err != nil {
		return desiredRoute{}, err
	}
	route := desiredRoute{identity: identity, spec: spec}
	weighted, targetsUsed, err := p.buildDesiredWeightedHTTPParts(spec)
	if err != nil {
		return desiredRoute{}, err
	}
	backendKeys := make(map[string]bool, len(spec.HTTPBackends))
	for index, backend := range spec.HTTPBackends {
		fixedResponse := p.fixedHTTPResponses && backend.InvalidReference
		unavailable := backend.Unavailable || fixedResponse
		validPort := backend.Port >= 1 && backend.Port <= 65535 || backend.Kind == cloud.HTTPBackendKindXBackend && backend.InvalidReference && backend.Port == 0
		deniedTLS := backend.BackendTLS != nil && backend.BackendTLS.Denied
		if strings.TrimSpace(backend.Namespace) == "" || strings.TrimSpace(backend.Name) == "" ||
			(backend.Kind != "" && backend.Kind != cloud.HTTPBackendKindXBackend) || !validPort ||
			len(backend.Matches) == 0 && !targetsUsed[index] || unavailable && !deniedTLS && len(backend.Members) != 0 {
			return invalid(errors.New("each HTTP backend requires a supported reference and a match or weighted reference, and an unavailable backend must have no members"))
		}
		key := openstackidentity.HTTPBackendPoolKey(backend)
		if backendKeys[key] {
			return invalid(errors.New("duplicate HTTP backend within one route"))
		}
		backendKeys[key] = true
		partSpec := spec
		partSpec.HTTPBackends, partSpec.HTTPRedirects, partSpec.FixedHTTPRules, partSpec.WeightedHTTPRules, partSpec.Members = nil, nil, nil, nil, backend.Members
		partSpec.BackendH2C, partSpec.BackendTLS = backend.BackendH2C, backend.BackendTLS
		part, err := p.buildDesiredRoutePart(partSpec, key, unavailable)
		if err != nil {
			return desiredRoute{}, err
		}
		part.fixedResponse = fixedResponse
		for _, role := range []string{rolePool, roleMember, roleMonitor} {
			if _, err := part.identity.RouteTags(part.httpRole(role, "")); err != nil {
				return invalid(err)
			}
		}
		if err := p.buildHTTPMatchPolicies(&part, backend.Matches); err != nil {
			return invalid(err)
		}
		route.httpParts = append(route.httpParts, part)
	}
	fixed, err := p.buildDesiredFixedHTTPParts(spec)
	if err != nil {
		return desiredRoute{}, err
	}
	route.httpParts = append(route.httpParts, fixed...)
	route.httpParts = append(route.httpParts, weighted...)
	sort.Slice(route.httpParts, func(i, j int) bool { return route.httpParts[i].backendKey < route.httpParts[j].backendKey })
	return route, nil
}

func (p *Provider) buildDesiredFixedHTTPParts(spec cloud.RouteSpec) ([]desiredRoute, error) {
	if len(spec.FixedHTTPRules) == 0 {
		return nil, nil
	}
	invalid := func(err error) ([]desiredRoute, error) {
		return nil, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	if !p.fixedHTTPResponses {
		return invalid(errors.New("HTTP fixed responses require the Octavia fixed HTTP capability"))
	}
	var parts []desiredRoute
	seen := make(map[int]bool, len(spec.FixedHTTPRules))
	partSpec := spec
	partSpec.HTTPBackends, partSpec.HTTPRedirects, partSpec.FixedHTTPRules, partSpec.WeightedHTTPRules = nil, nil, nil, nil
	for _, rule := range spec.FixedHTTPRules {
		if rule.RuleIndex < 0 || seen[rule.RuleIndex] || len(rule.Matches) == 0 {
			return invalid(errors.New("fixed HTTP rules require a unique rule index and at least one match"))
		}
		seen[rule.RuleIndex] = true
		part, err := p.buildDesiredRoutePart(partSpec, openstackidentity.HTTPFixedRuleKey(rule.RuleIndex), true)
		if err != nil {
			return nil, err
		}
		part.fixedResponse = true
		if err := p.buildHTTPMatchPolicies(&part, rule.Matches); err != nil {
			return invalid(err)
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func (p *Provider) buildHTTPMatchPolicies(r *desiredRoute, matches []cloud.HTTPPathMatch) error {
	matchKeys := make(map[string]bool, len(matches))
	for _, match := range matches {
		match, err := p.normalizeHTTPMatch(match)
		if err != nil {
			return err
		}
		policies, hostnameRuleValue, err := p.httpPathPolicies(match, true)
		if err != nil {
			return err
		}
		scope := httpMatchScope(r.spec, match)
		matchKey := openstackidentity.HTTPListenerMatchKey(scope, match)
		if matchKeys[matchKey] {
			return errors.New("duplicate HTTP match within one backend or weighted rule")
		}
		matchKeys[matchKey] = true
		operations, err := cloud.NormalizeHTTPHeaderOperations(match.RequestHeaderOperations)
		if err != nil {
			return err
		}
		if len(operations) != 0 && !p.requestHeaderModifier {
			return errors.New("request header operations require controller opt-in for Octavia")
		}
		if r.fixedResponse {
			operations = nil
		}
		responseOperations, err := cloud.NormalizeHTTPResponseHeaderOperations(match.ResponseHeaderOperations)
		if err != nil {
			return err
		}
		if len(responseOperations) != 0 && !p.responseHeaderModifier {
			return errors.New("response header operations require controller opt-in for Octavia")
		}
		for _, policy := range policies {
			policy.backendKey, policy.matchKey = r.backendKey, matchKey
			policy.httpsListenerHostname = scope
			policy.httpListenerHostname, policy.hostname = match.HTTPListenerHostname, match.Hostname
			policy.hostnameRuleValue, policy.precedence = hostnameRuleValue, match.Precedence
			policy.weighted = r.weightedTargets != nil
			policy.requestHeaderOperations = operations
			policy.responseHeaderOperations = responseOperations
			policy.role = policyRuleRole(policy, policy.role, "")
			if _, err := r.identity.RouteTags(policy.role); err != nil {
				return err
			}
			for _, rule := range rulesFor(policy, "") {
				if _, err := r.identity.RouteTags(rule.role); err != nil {
					return err
				}
			}
			r.policies = append(r.policies, policy)
		}
	}
	return nil
}

func (r desiredRoute) policySets() [][]desiredPolicy {
	if len(r.httpParts) == 0 {
		return [][]desiredPolicy{r.policies}
	}
	policies := make([][]desiredPolicy, 0, len(r.httpParts))
	for _, backend := range r.httpParts {
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

func (p *Provider) normalizeHTTPMatch(match cloud.HTTPPathMatch) (cloud.HTTPPathMatch, error) {
	if len(match.Headers) != 0 && !p.httpHeaderExact {
		return cloud.HTTPPathMatch{}, errors.New("exact HTTP header matching requires --octavia-http-header-exact")
	}
	if match.Method != "" {
		if !p.httpMethodMatch || !p.fixedHTTPResponses {
			return cloud.HTTPPathMatch{}, errors.New("HTTP method matching requires --octavia-http-method-match and --octavia-fixed-http-responses")
		}
		if !cloud.ValidHTTPMethod(match.Method) {
			return cloud.HTTPPathMatch{}, fmt.Errorf("unknown HTTP method %q", match.Method)
		}
	}
	headers, err := cloud.NormalizeHTTPHeaderMatches(match.Headers)
	if err != nil {
		return cloud.HTTPPathMatch{}, err
	}
	match.Headers = headers
	if match.PathType == cloud.PathMatchPrefix {
		match.PathValue = strings.TrimRight(match.PathValue, "/")
	}
	if match.PathValue == "" {
		match.PathValue = "/"
	}
	return match, nil
}

// A method-only root prefix has no path constraint. In particular, CONNECT
// authority targets and OPTIONS * do not begin with a slash.
func usesHTTPPathPredicate(match cloud.HTTPPathMatch) bool {
	return match.Method == "" || match.PathType != cloud.PathMatchPrefix || match.PathValue != "/"
}

// Inverting this impossible PATH regex also matches request targets for which
// HAProxy has no path sample.
const impossibleHTTPPathPattern = "(^$)."

func (p *Provider) httpPathPolicies(match cloud.HTTPPathMatch, grouped bool) ([]desiredPolicy, string, error) {
	policies, hostname, err := httpPathPolicies(match, grouped)
	if err == nil && p.httpMethodMatch && match.Method == "" && match.PathType == cloud.PathMatchPrefix && strings.TrimRight(match.PathValue, "/") == "" {
		policies[0].path = impossibleHTTPPathPattern
		policies[0].compareType = l7policies.CompareTypeRegex
		policies[0].pathInvert = true
	}
	return policies, hostname, err
}

func httpPathPolicies(match cloud.HTTPPathMatch, grouped bool) ([]desiredPolicy, string, error) {
	if match.PathType != cloud.PathMatchExact && match.PathType != cloud.PathMatchPrefix {
		return nil, "", fmt.Errorf("unsupported path match type %q", match.PathType)
	}
	pathRules, err := amphora.PathRuleValues(match.PathValue, match.PathType == cloud.PathMatchPrefix)
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
	if !usesHTTPPathPredicate(match) {
		return []desiredPolicy{{role: rolePolicyPrefix, position: 1, method: match.Method, headers: match.Headers}}, hostnameRuleValue, nil
	}
	role := rolePolicyExact
	pattern := pathRules.Regex
	if match.PathType == cloud.PathMatchPrefix {
		role = rolePolicyPrefix
		if pattern == "" && grouped {
			pattern, _ = amphora.LiteralPathPrefixPattern(pathRules.Exact)
		}
	}
	var policies []desiredPolicy
	if pattern != "" {
		policies = []desiredPolicy{{role: role, position: 1, compareType: l7policies.CompareTypeRegex, path: pattern}}
	} else {
		policies = policiesFor(pathRules.Exact, pathRules.Prefix)
	}
	for i := range policies {
		policies[i].method = match.Method
		policies[i].headers = match.Headers
	}
	return policies, hostnameRuleValue, nil
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
	var rules []desiredRule
	if policy.path != "" {
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, roleRulePath, ""),
			ruleType:    l7policies.TypePath,
			compareType: policy.compareType,
			value:       policy.path,
			invert:      policy.pathInvert,
		})
	}
	if hostnameRuleValue != "" {
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, roleRuleHost, ""),
			ruleType:    l7policies.TypeHostName,
			compareType: l7policies.CompareTypeRegex,
			value:       hostnameRuleValue,
		})
	}
	if policy.method != "" {
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, openstackidentity.RoleRuleHTTPMethod, ""),
			ruleType:    httpMethodRuleType,
			compareType: l7policies.CompareTypeEqual,
			value:       policy.method,
		})
	}
	for _, header := range policy.headers {
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, openstackidentity.RoleRuleHTTPHeaderExact, openstackidentity.HTTPHeaderRuleKey(header.Name)),
			ruleType:    httpHeaderExactRuleType,
			compareType: l7policies.CompareTypeEqual,
			key:         header.Name,
			value:       header.Value,
		})
	}
	for _, guard := range policy.httpHostExclusions {
		rules = append(rules, desiredRule{
			role:     policyRuleRole(policy, openstackidentity.RoleRuleHTTPHostExclusion, redirectGuardKey(guard)),
			ruleType: guard.ruleType, compareType: guard.compareType, value: guard.value, invert: true,
		})
	}
	if policy.redirect != nil {
		for _, guard := range policy.redirect.guards {
			rules = append(rules, desiredRule{
				role:     policyRuleRole(policy, roleRuleRedirectGuard, redirectGuardKey(guard)),
				ruleType: guard.ruleType, compareType: guard.compareType, value: guard.value, invert: true,
			})
		}
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
