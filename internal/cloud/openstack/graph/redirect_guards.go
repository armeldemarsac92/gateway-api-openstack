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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
)

// Amphora evaluates redirect actions before pool selection. Inverted stock L7
// rules exclude requests belonging to higher-priority forward actions.
type redirectGuard struct {
	ruleType    l7policies.RuleType
	compareType l7policies.CompareType
	value       string
}

var (
	errExactHeaderRedirectPrecedence     = errors.New("cannot preserve Octavia redirect precedence with an overlapping higher-priority forwarding rule that adds an exact HTTP header condition")
	errExactQueryParamRedirectPrecedence = errors.New("cannot preserve Octavia redirect precedence with an overlapping higher-priority forwarding rule that adds an exact HTTP query parameter condition")
	errHTTPMethodRedirectPrecedence      = errors.New("cannot preserve Octavia redirect precedence with an overlapping higher-priority forwarding rule that adds an HTTP method condition")
)

type redirectCell struct {
	hostname string
	guards   []redirectGuard
}

func redirectForwardingByPort(spec cloud.GatewayGraphSpec) map[int][]cloud.HTTPPathMatch {
	result := make(map[int][]cloud.HTTPPathMatch)
	for _, node := range spec.Routes {
		route := node.Spec
		if node.Disposition != cloud.GraphEnsure || cloud.NormalizeListenerProtocol(route.GatewayRequirements.ListenerProtocol) != cloud.ListenerProtocolHTTP {
			continue
		}
		port := route.GatewayRequirements.ListenerPort
		if len(route.ScopedListenerPorts) == 1 {
			port = route.ScopedListenerPorts[0]
		}
		add := func(matches []cloud.HTTPPathMatch) {
			for _, match := range matches {
				match.HTTPSListenerHostname = httpMatchScope(route, match)
				result[port] = append(result[port], match)
			}
		}
		for _, backend := range route.HTTPBackends {
			add(backend.Matches)
		}
		for _, rule := range route.WeightedHTTPRules {
			add(rule.Matches)
		}
		for _, rule := range route.FixedHTTPRules {
			add(rule.Matches)
		}
		if route.PathType != "" {
			add([]cloud.HTTPPathMatch{{HTTPListenerHostname: route.HTTPListenerHostname, Hostname: route.Hostname, PathType: route.PathType, PathValue: route.PathValue, Precedence: route.Precedence}})
		}
	}
	return result
}

type redirectPath struct {
	kind  cloud.PathMatchType
	value string
}

func pathOf(match cloud.HTTPPathMatch) redirectPath {
	path := match.PathValue
	if match.PathType == cloud.PathMatchPrefix {
		path = strings.TrimRight(path, "/")
	}
	if path == "" {
		path = "/"
	}
	return redirectPath{match.PathType, path}
}

func (outer redirectPath) contains(inner redirectPath) bool {
	if outer.kind == cloud.PathMatchExact {
		return inner.kind == cloud.PathMatchExact && inner.value == outer.value
	}
	return outer.value == "/" || outer.value == inner.value || strings.HasPrefix(inner.value, outer.value+"/")
}

func hostContains(outer, inner string) bool {
	if outer == "" || outer == inner {
		return true
	}
	suffix, wildcard := strings.CutPrefix(outer, "*.")
	return wildcard && strings.HasSuffix(strings.TrimPrefix(inner, "*."), "."+suffix)
}

// DNS exact and leading-wildcard domains are disjoint or nested. Give each
// domain only the additional exceptions introduced there. This keeps a
// host-specific forwarding rule from suppressing a redirect on other hosts.
type redirectDomain struct {
	host     string
	paths    []redirectPath
	shadowed bool
	active   *redirectDomain
	children []string
}

func parentDomain(host, root string, domains map[string]*redirectDomain) *redirectDomain {
	suffix := strings.TrimPrefix(host, "*.")
	for {
		_, rest, ok := strings.Cut(suffix, ".")
		if !ok {
			return domains[root]
		}
		if parent := domains["*."+rest]; parent != nil {
			return parent
		}
		suffix = rest
	}
}

func minimalRedirectPaths(paths []redirectPath) []redirectPath {
	prefixes := make(map[string]bool)
	for _, p := range paths {
		if p.kind == cloud.PathMatchPrefix {
			prefixes[p.value] = true
		}
	}
	keep := make([]redirectPath, 0, len(paths))
	seen := make(map[redirectPath]bool)
	for _, p := range paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		covered := p.kind == cloud.PathMatchExact && prefixes[p.value]
		for i := 1; !covered && i < len(p.value); i++ {
			if p.value[i] == '/' {
				covered = prefixes[p.value[:i]]
			}
		}
		if !covered {
			keep = append(keep, p)
		}
	}
	slices.SortFunc(keep, func(a, b redirectPath) int {
		if c := strings.Compare(a.value, b.value); c != 0 {
			return c
		}
		return strings.Compare(string(a.kind), string(b.kind))
	})
	return keep
}

func redirectCells(match cloud.HTTPPathMatch, forwards []cloud.HTTPPathMatch) ([]redirectCell, error) {
	source := pathOf(match)
	root := &redirectDomain{host: match.Hostname}
	domains := map[string]*redirectDomain{root.host: root}
	ordered := []*redirectDomain{root}
	var unsupportedMethod, unsupportedHeaders, unsupportedQueryParams bool
	for _, f := range forwards {
		if f.HTTPListenerHostname != match.HTTPListenerHostname || f.HTTPSListenerHostname != match.HTTPSListenerHostname || f.Precedence >= match.Precedence || f.Method != "" && match.Method != "" && f.Method != match.Method {
			continue
		}
		path := pathOf(f)
		covers := path.contains(source)
		if !covers && !source.contains(path) {
			continue
		}
		host := f.Hostname
		if hostContains(host, root.host) {
			host = root.host
		} else if !hostContains(root.host, host) {
			continue
		}
		forwardParams, err := cloud.NormalizeHTTPQueryParamMatches(f.QueryParams)
		if err != nil {
			return nil, err
		}
		var queryDisjoint, queryAddsCondition bool
		for _, param := range forwardParams {
			i := slices.IndexFunc(match.QueryParams, func(other cloud.HTTPQueryParamMatch) bool {
				return other.Name == param.Name
			})
			if i < 0 {
				queryAddsCondition = true
			} else if match.QueryParams[i].Value != param.Value {
				queryDisjoint = true
				break
			}
		}
		if queryDisjoint {
			continue
		}
		if f.Method != "" && match.Method == "" {
			unsupportedMethod = true
			continue
		}
		if slices.ContainsFunc(f.Headers, func(header cloud.HTTPHeaderMatch) bool {
			return !slices.Contains(match.Headers, header)
		}) {
			unsupportedHeaders = true
			continue
		}
		if queryAddsCondition {
			unsupportedQueryParams = true
			continue
		}
		d := domains[host]
		if d == nil {
			d = &redirectDomain{host: host}
			domains[host] = d
			ordered = append(ordered, d)
		}
		d.shadowed = d.shadowed || covers
		d.paths = append(d.paths, path)
	}
	if root.shadowed {
		return nil, nil
	}
	if unsupportedMethod {
		return nil, errHTTPMethodRedirectPrecedence
	}
	if unsupportedHeaders {
		return nil, errExactHeaderRedirectPrecedence
	}
	if unsupportedQueryParams {
		return nil, errExactQueryParamRedirectPrecedence
	}
	// A wildcard ancestor has fewer literal DNS labels than its descendants.
	sort.Slice(ordered[1:], func(i, j int) bool {
		a, b := ordered[1+i].host, ordered[1+j].host
		n, m := strings.Count(strings.TrimPrefix(a, "*."), "."), strings.Count(strings.TrimPrefix(b, "*."), ".")
		if n != m {
			return n < m
		}
		return a < b
	})
	root.paths = minimalRedirectPaths(root.paths)
	root.active = root
	for _, d := range ordered[1:] {
		parent := parentDomain(d.host, root.host, domains)
		if parent.shadowed {
			d.shadowed = true
			continue
		}
		d.paths = minimalRedirectPaths(append(d.paths, parent.paths...))
		if !d.shadowed && slices.Equal(d.paths, parent.paths) {
			d.active = parent.active
			continue
		}
		parent.active.children = append(parent.active.children, d.host)
		d.active = d
	}
	var cells []redirectCell
	for _, d := range ordered {
		if d.shadowed || d.active != d {
			continue
		}
		cell := redirectCell{hostname: d.host}
		for _, host := range d.children {
			regex, err := amphora.HostnameRuleValue(host)
			if err != nil {
				return nil, err
			}
			cell.guards = append(cell.guards, redirectGuard{l7policies.TypeHostName, l7policies.CompareTypeRegex, regex})
		}
		for _, path := range d.paths {
			policies, _, err := httpPathPolicies(cloud.HTTPPathMatch{PathType: path.kind, PathValue: path.value}, true)
			if err != nil {
				return nil, err
			}
			for _, policy := range policies {
				cell.guards = append(cell.guards, redirectGuard{l7policies.TypePath, policy.compareType, policy.path})
			}
		}
		positives := len(match.Headers) + len(match.QueryParams)
		if usesHTTPPathPredicate(match) {
			positives++
		}
		if match.Method != "" {
			positives++
		}
		if d.host != "" {
			positives++
		}
		if len(cell.guards)+positives > maxOctaviaL7RulesPerPolicy {
			cell.guards = compactRedirectGuards(cell.guards)
		}
		if count := len(cell.guards) + positives; count > maxOctaviaL7RulesPerPolicy {
			return nil, fmt.Errorf("controller encoding of redirect for host %q and path %q requires %d rules in one policy, exceeding the Octavia limit of %d", d.host, match.PathValue, count, maxOctaviaL7RulesPerPolicy)
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

// HAProxy evaluates every rule on a policy as a conjunction. Several inverted
// predicates of the same type can therefore share one inverted disjunction.
func compactRedirectGuards(guards []redirectGuard) []redirectGuard {
	const maxValueLength = 255
	var result []redirectGuard
	for _, kind := range []l7policies.RuleType{l7policies.TypeHostName, l7policies.TypePath} {
		pattern := ""
		length := 0
		for _, g := range guards {
			if g.ruleType != kind {
				continue
			}
			expr := g.value
			switch g.compareType {
			case l7policies.CompareTypeEqual:
				expr = "^" + compactLiteral(expr) + "$"
			case l7policies.CompareTypeStartWith:
				expr = "^" + compactLiteral(expr)
			}
			size := utf8.RuneCountInString(expr)
			if size > maxValueLength {
				result = append(result, g)
				continue
			}
			if length != 0 && length+1+size > maxValueLength {
				result = append(result, redirectGuard{kind, l7policies.CompareTypeRegex, pattern})
				pattern = ""
				length = 0
			}
			if length > 0 {
				pattern += "|"
				length++
			}
			pattern += expr
			length += size
		}
		if length > 0 {
			result = append(result, redirectGuard{kind, l7policies.CompareTypeRegex, pattern})
		}
	}
	return result
}

// compactLiteral preserves literal matching while using POSIX repetition when
// it shortens a repeated rune. Octavia and the stock host rules accept ERE
// intervals. A path literal is already bounded by the 255-character API field.
func compactLiteral(value string) string {
	var out strings.Builder
	for chars := []rune(value); len(chars) > 0; {
		count := 1
		for count < len(chars) && chars[count] == chars[0] {
			count++
		}
		atom := regexp.QuoteMeta(string(chars[0]))
		interval := "{" + strconv.Itoa(count) + "}"
		if len(atom)+len(interval) < len(atom)*count {
			out.WriteString(atom)
			out.WriteString(interval)
		} else {
			out.WriteString(strings.Repeat(atom, count))
		}
		chars = chars[count:]
	}
	return out.String()
}
