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
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

const roleRuleRedirectGuard = openstackidentity.RoleRuleRedirectGuard

type desiredRedirectPolicy struct {
	matchKey   string
	prefix     string
	request    *octavia.RequestRedirect
	statusCode int32
	guards     []redirectGuard
}

func redirectPartKey(matchKey string) string {
	return "redirect/" + matchKey
}

func (p *Provider) buildDesiredGatewayRoute(spec cloud.RouteSpec, forwards []cloud.HTTPPathMatch) (desiredRoute, error) {
	if len(spec.HTTPRedirects) == 0 {
		return p.buildDesiredRoute(spec)
	}
	route, err := p.buildDesiredHTTPBackends(spec)
	if err != nil {
		return desiredRoute{}, err
	}
	invalid := func(err error) (desiredRoute, error) {
		return desiredRoute{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, err)
	}
	indexes := make(map[int]bool, len(spec.HTTPRedirects))
	type rankedPart struct{ index, precedence int }
	matches := make(map[string]rankedPart)
	for _, redirect := range spec.HTTPRedirects {
		if redirect.RuleIndex < 0 || indexes[redirect.RuleIndex] || len(redirect.Matches) == 0 {
			return invalid(errors.New("each HTTP redirect requires a distinct rule index and at least one match"))
		}
		if redirect.Request != nil && !p.httpRequestRedirect {
			return invalid(errors.New("request-derived redirects require --octavia-http-request-redirect"))
		}
		if err := validateRedirectDestination(redirect); err != nil {
			return invalid(err)
		}
		indexes[redirect.RuleIndex] = true
		for _, match := range redirect.Matches {
			if match.URLRewrite != nil {
				return invalid(errors.New("URLRewrite cannot be combined with RequestRedirect"))
			}
			if len(match.RequestHeaderOperations) != 0 || len(match.ResponseHeaderOperations) != 0 {
				return invalid(errors.New("header modifiers cannot be combined with RequestRedirect in Octavia"))
			}
			match, err := p.normalizeHTTPMatch(match)
			if err != nil {
				return invalid(err)
			}
			if _, _, err := httpPathPolicies(match, true); err != nil {
				return invalid(err)
			}
			request, err := compileRequestRedirect(redirect.Request, match)
			if err != nil {
				return invalid(err)
			}
			match.HTTPSListenerHostname = httpMatchScope(spec, match)
			key := openstackidentity.HTTPListenerMatchKey(match.HTTPSListenerHostname, match)
			route.redirectMatches = append(route.redirectMatches, match)
			selected, found := matches[key]
			if found && selected.precedence <= match.Precedence {
				continue
			}
			if !found {
				selected.index = len(route.httpParts)
				route.httpParts = append(route.httpParts, desiredRoute{})
			}
			selected.precedence = match.Precedence
			matches[key] = selected
			part := desiredRoute{identity: route.identity, spec: route.spec, backendKey: redirectPartKey(key), redirect: true}
			part.spec.HTTPSListenerHostname = match.HTTPSListenerHostname
			cells, err := redirectCells(match, forwards)
			if err != nil {
				part.redirectIssue = err
				route.httpParts[selected.index] = part
				continue
			}
			for _, cell := range cells {
				positive := match
				positive.Hostname = cell.hostname
				if err := p.appendRedirectPolicies(&part, redirect, request, positive, key, cell.guards); err != nil {
					return invalid(err)
				}
			}
			route.httpParts[selected.index] = part
		}
	}
	return route, nil
}

func validateRedirectDestination(redirect cloud.HTTPRedirect) error {
	if redirect.StatusCode < 301 || redirect.StatusCode > 308 || !octavia.SupportedRedirectCode(int32(redirect.StatusCode)) {
		return errors.New("HTTP redirect code is outside the supported Octavia status codes")
	}
	if redirect.Request != nil {
		if redirect.Prefix != "" {
			return errors.New("HTTP redirect must specify a fixed prefix or a request-derived destination")
		}
		return nil
	}
	destination, err := url.Parse(redirect.Prefix)
	if err != nil || len(redirect.Prefix) > 255 || destination.Scheme != "http" && destination.Scheme != "https" || destination.Host == "" ||
		destination.User != nil || destination.Path != "" || redirect.Prefix != destination.Scheme+"://"+destination.Host ||
		strings.HasSuffix(destination.Host, ":") || strings.Contains(destination.Hostname(), "*") {
		return errors.New("HTTP redirect prefix must be a fixed HTTP or HTTPS authority")
	}
	if err := amphora.ValidateHostname(destination.Hostname()); err != nil {
		return errors.New("HTTP redirect prefix requires a valid destination hostname")
	}
	if port := destination.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return errors.New("HTTP redirect port is outside 1-65535")
		}
	}
	return nil
}

func compileRequestRedirect(redirect *cloud.HTTPRedirectRequest, match cloud.HTTPPathMatch) (*octavia.RequestRedirect, error) {
	if redirect == nil {
		return nil, nil
	}
	request := &octavia.RequestRedirect{Scheme: redirect.Scheme, Port: redirect.Port, Hostname: redirect.Hostname, ReplaceFullPath: redirect.ReplaceFullPath}
	if redirect.ReplacePrefixMatch != nil {
		if match.PathType != cloud.PathMatchPrefix {
			return nil, errors.New("HTTP prefix replacement requires a PathPrefix match")
		}
		request.ReplacePrefix = &octavia.PathPrefixReplacement{Match: pathOf(match).value, Replacement: *redirect.ReplacePrefixMatch}
	}
	if !request.Valid() {
		return nil, errors.New("invalid HTTP request redirect destination")
	}
	return request, nil
}

func (p *Provider) appendRedirectPolicies(part *desiredRoute, redirect cloud.HTTPRedirect, request *octavia.RequestRedirect, positive cloud.HTTPPathMatch, matchKey string, guards []redirectGuard) error {
	policies, hostname, err := p.httpPathPolicies(positive, true)
	if err != nil {
		return err
	}
	guardKeys := make([]string, 0, len(guards))
	for _, guard := range guards {
		guardKeys = append(guardKeys, redirectGuardKey(guard))
	}
	slices.Sort(guardKeys)
	for _, policy := range policies {
		policy.matchKey = openstackidentity.HTTPRedirectKey(matchKey, positive.Hostname, policy.role+"/"+strings.Join(guardKeys, "/"))
		policy.hostnameRuleValue, policy.precedence = hostname, positive.Precedence
		policy.httpsListenerHostname = positive.HTTPSListenerHostname
		policy.httpListenerHostname, policy.hostname = positive.HTTPListenerHostname, positive.Hostname
		policy.role = openstackidentity.HTTPRedirectRole(policy.role, matchKey, policy.matchKey, "")
		policy.redirect = &desiredRedirectPolicy{
			matchKey: matchKey, prefix: redirect.Prefix, request: request, statusCode: int32(redirect.StatusCode), guards: guards,
		}
		if _, err := part.identity.RouteTags(policy.role); err != nil {
			return err
		}
		for _, rule := range rulesFor(policy, "") {
			if _, err := part.identity.RouteTags(rule.role); err != nil {
				return err
			}
		}
		part.policies = append(part.policies, policy)
	}
	return nil
}

func redirectGuardKey(guard redirectGuard) string {
	if guard.ruleType == httpQueryParamExactRuleType {
		return openstackidentity.HTTPRedirectKey("query-exclusion", guard.key, guard.value)
	}
	return openstackidentity.HTTPRedirectKey(string(guard.ruleType), string(guard.compareType), guard.value)
}

func policyRuleRole(policy desiredPolicy, base, guard string) string {
	if policy.redirect != nil {
		return openstackidentity.HTTPRedirectRole(base, policy.redirect.matchKey, policy.matchKey, guard)
	}
	var predicateKeys []string
	if guard != "" {
		predicateKeys = []string{guard}
	}
	if policy.weighted {
		return openstackidentity.HTTPWeightedPolicyRole(base, policy.backendKey, policy.matchKey, predicateKeys...)
	}
	if base == openstackidentity.RoleRuleHTTPHostExclusion && policy.backendKey == "" {
		return openstackidentity.ScalarHTTPHostExclusionRole(guard)
	}
	if len(predicateKeys) != 0 {
		return openstackidentity.HTTPFragmentRole(base, policy.backendKey, policy.matchKey, predicateKeys...)
	}
	if policy.backendKey == "" && policy.httpsListenerHostname != "" {
		return openstackidentity.ScopedHTTPPolicyRole(base, policy.matchKey)
	}
	return httpResourceRole(base, policy.backendKey, policy.matchKey)
}

func desiredPolicyActionMatches(wanted desiredPolicy, current observedRoutePolicy, poolID string) bool {
	observed := current.policy
	if wanted.redirect == nil {
		return poolID != "" && observed.Action == string(l7policies.ActionRedirectToPool) && observed.RedirectPoolID == poolID
	}
	if wanted.redirect.request != nil {
		return current.requestRedirect.Equal(wanted.redirect.request) && observed.RedirectHttpCode == wanted.redirect.statusCode
	}
	return !current.requestRedirectPresent && observed.Action == string(l7policies.ActionRedirectPrefix) && observed.RedirectPoolID == "" && observed.RedirectURL == "" &&
		observed.RedirectPrefix == wanted.redirect.prefix && observed.RedirectHttpCode == wanted.redirect.statusCode
}

func containsObservedRedirect(routes []observedRouteGraph) bool {
	for _, route := range routes {
		for _, part := range route.httpParts {
			if slices.ContainsFunc(part.policies, func(policy observedRoutePolicy) bool { return policy.redirect }) {
				return true
			}
		}
	}
	return false
}
