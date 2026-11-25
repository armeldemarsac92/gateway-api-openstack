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
	"slices"
	"sort"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
)

// Octavia's public member create and update APIs accept weights from 0 to 256.
const maxOctaviaMemberWeight = 256

const (
	httpHeaderExactRuleType     l7policies.RuleType = "HTTP_HEADER_EXACT"
	httpQueryParamExactRuleType l7policies.RuleType = "HTTP_QUERY_PARAM_EXACT"
	httpMethodRuleType          l7policies.RuleType = "HTTP_METHOD"
	grpcContentTypeRuleType     l7policies.RuleType = "GRPC_CONTENT_TYPE"
)

// Match gRPC media types with a POSIX ERE so HAProxy can use either libc or
// PCRE. Octavia's stock HAProxy template cannot pass #, & or ' through portably,
// so the optional codec accepts the rest of the HTTP token alphabet. Including
// a space in the blank class avoids a nested-set warning from Octavia's Python
// regex validator.
const portableGRPCContentTypePattern = "^[Aa][Pp][Pp][Ll][Ii][Cc][Aa][Tt][Ii][Oo][Nn]/[Gg][Rr][Pp][Cc]([+][A-Za-z0-9!$%*+.^_`|~-]+)?[ [:blank:]]*(;|$)"

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
	requestURLRewrite        *octavia.RequestURLRewrite
	requestPolicy            *cloud.HTTPRequestPolicy
	externalAuth             *cloud.ListenerExternalAuth
	resolvedExternalAuth     *octavia.PolicyExternalAuth
	requestMirrors           []desiredHTTPMirror
	resolvedRequestMirrors   []octavia.HTTPRequestMirror
	requestCORS              *cloud.HTTPCORS
	targetFilters            []octavia.WeightedHTTPTarget
	localRewriteFallback     bool
	method                   string
	headers                  []cloud.HTTPHeaderMatch
	queryParams              []cloud.HTTPQueryParamMatch
	grpcContentTypeRule      l7policies.RuleType
}

type desiredRule struct {
	role        string
	ruleType    l7policies.RuleType
	compareType l7policies.CompareType
	value       string
	key         string
	invert      bool
}

func (rule desiredRule) matchesKey(key string) bool {
	switch rule.ruleType {
	case httpMethodRuleType, grpcContentTypeRuleType:
		return key == ""
	default:
		return rule.key == "" || key == rule.key
	}
}

type desiredRoute struct {
	identity                    Identity
	spec                        cloud.RouteSpec
	members                     []cloud.Member
	healthMonitor               cloud.HealthMonitorConfig
	policies                    []desiredPolicy
	hostnameRuleValue           string
	httpParts                   []desiredRoute
	mirrorAvailability          requestMirrorAvailability
	mirrorUnavailableCapability string
	requestPolicyWithdrawals    map[string]bool
	backendKey                  string
	unavailable                 bool
	routedExternal              bool
	xBackendH2CPending          bool
	xBackend                    bool
	xBackendPlaintext           bool
	inlineSessionPersistence    bool
	sessionHeader               *cloud.HTTPSessionHeaderPersistence
	sessionHeaderCapability     xBackendCookieCapability
	headerReadbackPending       bool
	sessionCookieName           string
	sessionCookiePath           *string
	cookiePathReadbackPending   bool
	sessionCookieDeadline       *cloud.HTTPSessionCookieDeadline
	cookieReadbackPending       bool
	sessionCookieCapability     xBackendCookieCapability
	redirect                    bool
	redirectMatches             []cloud.HTTPPathMatch
	redirectIssue               error
	fixedResponse               bool
	weightedTargets             []desiredHTTPWeightedTarget
	targetsPending              bool
	zeroWeightOnly              bool
	mirrorOnly                  bool
	grpcFailurePoolID           string
	weightedAuthorizations      map[string]map[string]int
	backendTLS                  backendTLSPlan
	backendRetryBudgetAvailable bool
	tcpRejectionUnavailable     bool
	udpDropUnverified           bool
	udpDropUnavailable          bool

	xBackendClientCertificateReference string
}

func (r desiredRoute) protocol() string {
	return cloud.NormalizeListenerProtocol(r.spec.GatewayRequirements.ListenerProtocol)
}

func (r desiredRoute) monitorType() string {
	if r.protocol() == cloud.ListenerProtocolUDP {
		return monitors.TypeUDPConnect
	}
	if r.spec.GRPC {
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
	if spec.GRPC {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("gRPC routing requires HTTP backend groups"))
	}
	route, err := p.buildDesiredRoutePart(spec, "", false, false)
	for index := range route.policies {
		route.policies[index].precedence = spec.Precedence
	}
	return route, err
}

// scopeHTTPKey keeps the legacy roles on the anchor and encodes all other
// ports in existing HTTP role formats. The same route and port always produce
// the same keys, regardless of listener order or changes on another port.
func scopeHTTPKey(spec cloud.RouteSpec, key string) string {
	if !spec.ScopeHTTPResources || len(spec.ScopedListenerPorts) != 1 {
		return key
	}
	return openstackidentity.HTTPBackendKey("listener-port", key, spec.ScopedListenerPorts[0])
}

func scopeHTTPScalar(route desiredRoute) desiredRoute {
	if !route.spec.ScopeHTTPResources || len(route.httpParts) != 0 {
		return route
	}
	part := route
	part.backendKey = scopeHTTPKey(route.spec, "")
	matchKey := openstackidentity.HTTPListenerMatchKey(route.spec.HTTPSListenerHostname, cloud.HTTPPathMatch{
		HTTPListenerHostname: route.spec.HTTPListenerHostname, Hostname: route.spec.Hostname,
		PathType: route.spec.PathType, PathValue: route.spec.PathValue,
	})
	for i := range part.policies {
		policy := &part.policies[i]
		base := policy.role
		if parsed, _, _, ok := openstackidentity.ParseHTTPFragmentRole(base); ok {
			base = parsed
		}
		policy.backendKey, policy.matchKey = part.backendKey, matchKey
		policy.hostnameRuleValue = route.hostnameRuleValue
		policy.role = policyRuleRole(*policy, base, "")
	}
	route.policies, route.httpParts = nil, []desiredRoute{part}
	return route
}

func (p *Provider) buildDesiredRoutePart(spec cloud.RouteSpec, backendKey string, unavailable, routedExternal bool) (desiredRoute, error) {
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
	protocol := cloud.NormalizeListenerProtocol(spec.GatewayRequirements.ListenerProtocol)
	if (spec.ServiceSessionPersistence != nil || spec.ServiceSessionPersistencePending) &&
		(protocol != cloud.ListenerProtocolHTTP || spec.MemberMode != cloud.MemberModeEndpoint || routedExternal) {
		return invalid(errors.New("service session persistence requires an HTTP Endpoint pool"))
	}
	if persistence := spec.ServiceSessionPersistence; persistence != nil && !validServiceSessionPersistence(*persistence, spec.GRPC) {
		return invalid(errors.New("invalid Service session persistence"))
	}
	fraction := spec.TCPConnectionRejection
	if fraction != nil {
		canForward := slices.ContainsFunc(spec.Members, func(member cloud.Member) bool { return !member.Draining })
		full := fraction.RejectWeight == fraction.TotalWeight
		if protocol != cloud.ListenerProtocolTCP || !fraction.Valid() || fraction.RejectWeight == 0 ||
			full && len(spec.Members) != 0 || !full && !canForward {
			return invalid(errors.New("TCP connection rejection requires a positive TCP share and its complete real-member set"))
		}
	}
	if drop := spec.UDPPacketDrop; drop != nil {
		canForward := slices.ContainsFunc(spec.Members, func(member cloud.Member) bool { return !member.Draining })
		if protocol != cloud.ListenerProtocolUDP || !drop.Valid() || spec.MemberMode != cloud.MemberModeEndpoint ||
			spec.HealthMonitor != (cloud.HealthMonitorConfig{}) || len(spec.ScopedListenerPorts) > 1 ||
			drop.DropsAll() && len(spec.Members) != 0 || !drop.DropsAll() && !canForward {
			return invalid(errors.New("UDP packet drop requires a dedicated UDP Endpoint listener, no monitor and a complete real-member set"))
		}
	}
	if len(spec.Members) == 0 && spec.MemberMode != cloud.MemberModeEndpoint && !unavailable && fraction == nil {
		return invalid(errors.New("at least one ready backend member is required"))
	}
	if routedExternal {
		spec.MemberSubnetID = ""
	} else if strings.TrimSpace(spec.MemberSubnetID) == "" {
		return invalid(errors.New("member subnet ID must not be empty"))
	}
	var policies []desiredPolicy
	var hostnameRuleValue string
	if spec.BackendRetryBudget != nil && (protocol != cloud.ListenerProtocolHTTP || !spec.BackendRetryBudget.Valid()) {
		return invalid(errors.New("backend retry budgets require a valid Service budget on an HTTP pool"))
	}
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
	desired := desiredRoute{
		identity:          identity,
		spec:              spec,
		members:           members,
		healthMonitor:     monitor,
		policies:          policies,
		hostnameRuleValue: hostnameRuleValue,
		backendKey:        backendKey,
		unavailable:       unavailable,
		routedExternal:    routedExternal,
	}
	if persistence := spec.ServiceSessionPersistence; persistence != nil {
		desired.sessionCookieName, desired.sessionCookieDeadline, desired.sessionHeader =
			persistence.CookieName, persistence.CookieDeadline, persistence.Header
		if path := persistence.CookiePath; path != nil && *path != "/" {
			desired.sessionCookiePath = path
		}
	}
	return desired, nil
}

func (p *Provider) buildDesiredHTTPBackends(spec cloud.RouteSpec) (desiredRoute, error) {
	invalid := func(err error) (desiredRoute, error) {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	if cloud.NormalizeListenerProtocol(spec.GatewayRequirements.ListenerProtocol) != cloud.ListenerProtocolHTTP ||
		spec.TCPConnectionRejection != nil || spec.UDPPacketDrop != nil ||
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
	mirrorsUsed := make(map[int]bool)
	if err := collectHTTPMirrorPools(spec, mirrorsUsed); err != nil {
		return invalid(err)
	}
	backendKeys := make(map[string]bool, len(spec.HTTPBackends))
	for index, backend := range spec.HTTPBackends {
		fixedResponse := p.fixedHTTPResponses && backend.InvalidReference
		unavailable := backend.Unavailable || fixedResponse
		deniedTLS := backend.BackendTLS != nil && backend.BackendTLS.Denied
		xBackend := backend.Kind == cloud.HTTPBackendKindXBackend
		validPort := backend.Port >= 1 && backend.Port <= 65535 || xBackend && (backend.InvalidReference || deniedTLS) && backend.Port == 0
		if strings.TrimSpace(backend.Namespace) == "" || strings.TrimSpace(backend.Name) == "" ||
			(backend.Kind != "" && backend.Kind != cloud.HTTPBackendKindXBackend) || !validPort ||
			len(backend.Matches) == 0 && !targetsUsed[index] && !mirrorsUsed[index] || unavailable && !deniedTLS && len(backend.Members) != 0 {
			return invalid(errors.New("each HTTP backend requires a supported reference and a match or weighted reference, and an unavailable backend must have no members"))
		}
		if backend.BackendH2CPending && (!xBackend || !backend.BackendH2C || spec.GRPC || backend.BackendTLS != nil) {
			return invalid(errors.New("pending h2c requires a cleartext HTTP XBackend pool"))
		}
		if backend.RoutedExternal && (!xBackend || spec.GRPC || spec.MemberMode != cloud.MemberModeEndpoint ||
			len(spec.BackendSecurityGroupIDs) != 0 || backend.BackendRetryBudget != nil || backend.InvalidReference) {
			return invalid(errors.New("routed external members require an unmanaged HTTP XBackend pool"))
		}
		if xBackend && backend.BackendTLS != nil && !deniedTLS &&
			(spec.GRPC || spec.MemberMode != cloud.MemberModeEndpoint || len(spec.BackendSecurityGroupIDs) != 0 || backend.BackendH2C) {
			return invalid(errors.New("authenticated XBackends require an unmanaged HTTP endpoint pool"))
		}
		if backend.XBackendClientCertificateReference != "" && (!xBackend || backend.BackendTLS == nil || deniedTLS || backend.InvalidReference || mirrorsUsed[index]) {
			return invalid(errors.New("an XBackend client certificate requires an authenticated XBackend forwarding pool"))
		}
		if backend.SessionCookieName != "" && (!xBackend || spec.GRPC || spec.MemberMode != cloud.MemberModeEndpoint ||
			len(spec.BackendSecurityGroupIDs) != 0 || backend.RoutedExternal ||
			mirrorsUsed[index] || !cloud.ValidHTTPSessionCookieName(backend.SessionCookieName)) {
			return invalid(errors.New("HTTP session persistence requires a valid cookie name and an unmanaged HTTP XBackend EndpointSelector forwarding pool"))
		}
		if header := backend.SessionHeader; header != nil && (!xBackend || spec.GRPC || spec.MemberMode != cloud.MemberModeEndpoint ||
			len(spec.BackendSecurityGroupIDs) != 0 || backend.RoutedExternal || backend.SessionCookieName != "" ||
			mirrorsUsed[index] || !header.Valid()) {
			return invalid(errors.New("HTTP header persistence requires a valid header and an unmanaged HTTP XBackend EndpointSelector forwarding pool"))
		}
		if path := backend.SessionCookiePath; path != nil && (backend.SessionCookieName == "" || !cloud.ValidHTTPSessionCookiePath(*path)) {
			return invalid(errors.New("HTTP session persistence path requires a cookie name and a valid cookie path"))
		}
		if deadline := backend.SessionCookieDeadline; deadline != nil && (backend.SessionCookieName == "" || !deadline.Valid()) {
			return invalid(errors.New("HTTP session persistence absolute timeout requires a cookie name, valid timeout and XBackend scope"))
		}
		key := scopeHTTPKey(spec, openstackidentity.HTTPBackendPoolKey(backend))
		if backend.SessionPersistenceRule != "" && (xBackend || backend.ServiceSessionPersistencePending ||
			mirrorsUsed[index] || backend.BackendH2C && backend.BackendTLS != nil && !deniedTLS) {
			return invalid(errors.New("rule session persistence requires a primary HTTP Service Endpoint pool with a compatible transport"))
		}
		if backendKeys[key] {
			return invalid(errors.New("duplicate HTTP backend within one route"))
		}
		backendKeys[key] = true
		partSpec := spec
		partSpec.HTTPBackends, partSpec.HTTPRedirects, partSpec.FixedHTTPRules, partSpec.WeightedHTTPRules, partSpec.Members = nil, nil, nil, nil, backend.Members
		partSpec.BackendH2C, partSpec.BackendTLS = backend.BackendH2C, backend.BackendTLS
		partSpec.BackendRetryBudget = backend.BackendRetryBudget
		partSpec.BackendRetryBudgetPending = backend.BackendRetryBudgetPending
		partSpec.ServiceSessionPersistence = backend.ServiceSessionPersistence
		partSpec.ServiceSessionPersistencePending = backend.ServiceSessionPersistencePending
		if (partSpec.ServiceSessionPersistence != nil || partSpec.ServiceSessionPersistencePending) &&
			(xBackend || backend.InvalidReference || deniedTLS || len(backend.Matches) == 0 && !targetsUsed[index]) {
			return invalid(errors.New("service session persistence requires an eligible primary Service destination"))
		}
		part, err := p.buildDesiredRoutePart(partSpec, key, unavailable, backend.RoutedExternal)
		if err != nil {
			return desiredRoute{}, err
		}
		part.fixedResponse = fixedResponse
		part.inlineSessionPersistence = backend.SessionPersistenceRule != ""
		part.xBackend = xBackend
		part.xBackendH2CPending = backend.BackendH2CPending
		part.xBackendPlaintext = xBackend && !backend.InvalidReference && backend.BackendTLS == nil
		part.xBackendClientCertificateReference = backend.XBackendClientCertificateReference
		// Invalid backends keep their match for the rejection response. Leaving
		// the header unset withdraws any persistence from the former pool.
		if xBackend {
			if !backend.InvalidReference {
				part.sessionHeader = backend.SessionHeader
			}
			part.sessionCookieName = backend.SessionCookieName
			if path := backend.SessionCookiePath; path != nil && *path != "/" {
				part.sessionCookiePath = path
			}
			part.sessionCookieDeadline = backend.SessionCookieDeadline
		}
		for _, role := range []string{rolePool, roleMember, roleMonitor} {
			if _, err := part.identity.RouteTags(part.httpRole(role, "")); err != nil {
				return invalid(err)
			}
		}
		if err := p.buildHTTPMatchPolicies(&part, backend.Matches, spec.HTTPBackends); err != nil {
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
		part, err := p.buildDesiredRoutePart(partSpec, scopeHTTPKey(spec, openstackidentity.HTTPFixedRuleKey(rule.RuleIndex)), true, false)
		if err != nil {
			return nil, err
		}
		part.fixedResponse = true
		if err := p.buildHTTPMatchPolicies(&part, rule.Matches, spec.HTTPBackends); err != nil {
			return invalid(err)
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func (p *Provider) buildHTTPMatchPolicies(r *desiredRoute, matches []cloud.HTTPPathMatch, backends []cloud.HTTPBackend) error {
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
		if r.fixedResponse && len(match.RequestMirrors) == 0 {
			operations = nil
		}
		responseOperations, err := cloud.NormalizeHTTPResponseHeaderOperations(match.ResponseHeaderOperations)
		if err != nil {
			return err
		}
		if len(responseOperations) != 0 && !p.responseHeaderModifier {
			return errors.New("response header operations require controller opt-in for Octavia")
		}
		rewrite, err := compileRequestURLRewrite(match)
		if err != nil {
			return err
		}
		if rewrite != nil && (!p.httpRequestURLRewrite || r.spec.GRPC || r.redirect) {
			return errors.New("forwarding URL rewrites require --octavia-request-url-rewrite on an HTTPRoute")
		}
		if r.fixedResponse && len(match.RequestMirrors) == 0 {
			rewrite = nil
		}
		targetFilters, err := compileWeightedTargetFilters(match, r.weightedTargets)
		if err != nil {
			return err
		}
		if match.ExternalAuth != nil {
			if !p.routeAuthorizerEnabled(*match.ExternalAuth) || !p.fixedHTTPResponses || r.spec.GRPC || r.redirect ||
				len(match.RequestHeaderOperations) != 0 || len(match.ResponseHeaderOperations) != 0 ||
				match.CORS != nil || match.URLRewrite != nil || match.RequestPolicy != nil || len(match.RequestMirrors) != 0 ||
				slices.ContainsFunc(targetFilters, func(target octavia.WeightedHTTPTarget) bool {
					return target.HasRequestFilters() || len(target.ResponseHeaderOperations) != 0 || len(target.RequestMirrors) != 0
				}) {
				return errors.New("HTTPRoute ExternalAuth requires opt-in, fixed responses and a forwarding HTTP rule without additional transformations")
			}
			if err := validateRouteAuthorizer(*match.ExternalAuth); err != nil {
				return err
			}
		}
		if r.spec.GRPC {
			if err := cloud.ValidateGRPCRequestMetadata(operations); err != nil {
				return err
			}
			if err := cloud.ValidateGRPCResponseMetadata(responseOperations); err != nil {
				return err
			}
			for _, target := range targetFilters {
				if err := cloud.ValidateGRPCResponseMetadata(target.ResponseHeaderOperations); err != nil {
					return err
				}
				if err := cloud.ValidateGRPCRequestMetadata(target.RequestHeaderOperations); err != nil {
					return err
				}
			}
		}
		if match.CORS != nil {
			if !p.httpCORS || r.spec.GRPC || r.redirect || r.fixedResponse {
				return errors.New("CORS requires --octavia-request-cors and a forwarding HTTPRoute policy")
			}
			if err := match.CORS.Validate(); err != nil {
				return err
			}
			if err := cloud.ValidateHTTPCORSComposition(operations, responseOperations); err != nil {
				return err
			}
			for _, target := range targetFilters {
				if err := cloud.ValidateHTTPCORSComposition(target.RequestHeaderOperations, target.ResponseHeaderOperations); err != nil {
					return err
				}
			}
		}
		requestPolicy := match.RequestPolicy
		if requestPolicy != nil {
			if !p.httpRequestPolicy || r.spec.GRPC || r.redirect {
				return errors.New("HTTP request policy requires --octavia-http-request-policy on a forwarding HTTPRoute")
			}
			if err := requestPolicy.Validate(); err != nil {
				return err
			}
			if r.fixedResponse {
				requestPolicy = nil
			}
		}
		mirrorSpec := r.spec
		mirrorSpec.HTTPBackends = backends
		mirrors, err := p.compileHTTPMirrors(mirrorSpec, match.RequestMirrors)
		if err != nil {
			return err
		}
		for _, policy := range policies {
			if !r.fixedResponse {
				policy.externalAuth = match.ExternalAuth
			}
			policy.requestMirrors = mirrors
			policy.requestPolicy = requestPolicy
			policy.requestCORS = match.CORS
			policy.targetFilters = targetFilters
			policy.backendKey, policy.matchKey = r.backendKey, matchKey
			policy.httpsListenerHostname = scope
			policy.httpListenerHostname, policy.hostname = match.HTTPListenerHostname, match.Hostname
			policy.hostnameRuleValue, policy.precedence = hostnameRuleValue, match.Precedence
			policy.weighted = r.weightedTargets != nil
			policy.requestHeaderOperations = operations
			policy.responseHeaderOperations = responseOperations
			policy.requestURLRewrite = rewrite
			if r.spec.GRPC {
				policy.grpcContentTypeRule = l7policies.TypeHeader
				// The native predicate identifies policies whose binary
				// metadata must be sent as repeated fields.
				if p.grpcContentType || hasGRPCBinaryMetadataList(operations) ||
					hasGRPCBinaryMetadataList(responseOperations) || hasGRPCBinaryTargetRequestMetadata(targetFilters) ||
					hasGRPCBinaryTargetResponseMetadata(targetFilters) {
					policy.grpcContentTypeRule = grpcContentTypeRuleType
				}
			}
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
	policies := make([][]desiredPolicy, 0, 1+len(r.httpParts))
	if len(r.policies) != 0 {
		policies = append(policies, r.policies)
	}
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
	if len(match.QueryParams) != 0 && (!p.httpQueryParamExact || !p.fixedHTTPResponses) {
		return cloud.HTTPPathMatch{}, errors.New("exact HTTP query parameter matching requires --octavia-http-query-param-exact and --octavia-fixed-http-responses")
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
	if match.QueryParams, err = cloud.NormalizeHTTPQueryParamMatches(match.QueryParams); err != nil {
		return cloud.HTTPPathMatch{}, err
	}
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
		return []desiredPolicy{{role: rolePolicyPrefix, position: 1, method: match.Method, headers: match.Headers, queryParams: match.QueryParams}}, hostnameRuleValue, nil
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
		policies[i].queryParams = match.QueryParams
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
	switch policy.grpcContentTypeRule {
	case grpcContentTypeRuleType:
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, openstackidentity.RoleRuleGRPCContentType, ""),
			ruleType:    grpcContentTypeRuleType,
			compareType: l7policies.CompareTypeEqual,
			value:       "application/grpc",
		})
	case l7policies.TypeHeader:
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, openstackidentity.RoleRuleGRPCContentType, ""),
			ruleType:    l7policies.TypeHeader,
			compareType: l7policies.CompareTypeRegex,
			key:         "content-type",
			value:       portableGRPCContentTypePattern,
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
	for _, param := range policy.queryParams {
		rules = append(rules, desiredRule{
			role:        policyRuleRole(policy, openstackidentity.RoleRuleHTTPQueryParamExact, openstackidentity.HTTPQueryParamRuleKey(param.Name)),
			ruleType:    httpQueryParamExactRuleType,
			compareType: l7policies.CompareTypeEqual,
			key:         param.Name,
			value:       param.Value,
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
			base := roleRuleRedirectGuard
			if guard.ruleType == httpQueryParamExactRuleType {
				base = openstackidentity.RoleRuleHTTPQueryParamExact
			}
			rules = append(rules, desiredRule{
				role:     policyRuleRole(policy, base, redirectGuardKey(guard)),
				ruleType: guard.ruleType, compareType: guard.compareType, key: guard.key, value: guard.value, invert: true,
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
