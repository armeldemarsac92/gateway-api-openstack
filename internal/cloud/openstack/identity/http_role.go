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

// Package identity encodes and validates immutable OpenStack ownership
// metadata for the resources managed by this controller.
package identity

import (
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// HTTPBackendKey identifies one Service port within the immutable HTTPRoute owner.
func HTTPBackendKey(namespace, name string, port int) string {
	return httpTupleKey(namespace, name, strconv.Itoa(port))
}

// HTTPBackendPoolKey keeps Service identities stable and gives an XBackend
// its own identity across edits to its port.
func HTTPBackendPoolKey(backend cloud.HTTPBackend) string {
	if backend.Kind == cloud.HTTPBackendKindXBackend {
		return httpTupleKey(string(backend.Kind), backend.Namespace, backend.Name)
	}
	return HTTPBackendKey(backend.Namespace, backend.Name, backend.Port)
}

// HTTPFixedRuleKey identifies a rule that serves a local fixed response.
func HTTPFixedRuleKey(index int) string {
	return httpTupleKey("", "fixed-rule", strconv.Itoa(index))
}

// HTTPWeightedRuleKey identifies a rule independently of its targets.
func HTTPWeightedRuleKey(index int) string {
	return httpTupleKey("", "weighted-rule", strconv.Itoa(index))
}

// HTTPMatchKey identifies a logical predicate across list edits and rank changes.
func HTTPMatchKey(hostname string, pathType cloud.PathMatchType, path, method string, headers ...cloud.HTTPHeaderMatch) string {
	var predicate strings.Builder
	predicate.WriteString(path)
	if method != "" {
		// The empty field name separates method matches from existing header-only keys.
		predicate.WriteString("\x00\x00")
		predicate.WriteString(method)
	}
	for _, header := range headers {
		// Canonical HTTP header names and values cannot contain NUL.
		predicate.WriteByte(0)
		predicate.WriteString(header.Name)
		predicate.WriteByte(0)
		predicate.WriteString(header.Value)
	}
	return httpTupleKey(hostname, string(pathType), predicate.String())
}

// HTTPListenerMatchKey scopes a logical HTTP match without changing existing keys.
func HTTPListenerMatchKey(scope string, match cloud.HTTPPathMatch) string {
	key := HTTPMatchKey(match.Hostname, match.PathType, match.PathValue, match.Method, match.Headers...)
	if scope == "" {
		return key
	}
	return httpTupleKey("https-listener", scope, key)
}

const scopedHTTPRolePrefix = "http-https-listener-v1/"

// ScopedHTTPPolicyRole preserves scalar route pool identity while scoping its policies and rules.
func ScopedHTTPPolicyRole(base, match string) string {
	if !validHTTPKey(match) {
		return ""
	}
	switch base {
	case RolePolicyExact, RolePolicyPrefix, RoleRulePath, RoleRuleHost:
		return scopedHTTPRolePrefix + base + "/" + match
	default:
		return ""
	}
}

// RoleRuleHTTPHeaderExact identifies a predicate on a normalized header name.
const RoleRuleHTTPHeaderExact = "l7-header-exact"

// RoleRuleHTTPMethod identifies the single method predicate in a match.
const RoleRuleHTTPMethod = "l7-http-method"

// RoleRuleGRPCContentType identifies the native gRPC media type predicate.
const RoleRuleGRPCContentType = "l7-grpc-content-type"

// HTTPHeaderRuleKey identifies one field within an exact-header conjunction.
func HTTPHeaderRuleKey(name string) string {
	return httpTupleKey("exact-header", name, "")
}

func httpTupleKey(first, second, third string) string {
	// Neither a Kubernetes namespace nor a hostname or API match type contains NUL.
	sum := sha256.Sum256([]byte(first + "\x00" + second + "\x00" + third))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func validHTTPKey(key string) bool {
	if len(key) != base64.RawURLEncoding.EncodedLen(sha256.Size) {
		return false
	}
	value, err := base64.RawURLEncoding.Strict().DecodeString(key)
	return err == nil && len(value) == sha256.Size
}

// HTTPFragmentRole preserves the route ownership tuple and distinguishes its
// backend and path resources. Invalid combinations have no resource role.
func HTTPFragmentRole(base, backendKey, matchKey string, predicateKeys ...string) string {
	if !validHTTPKey(backendKey) {
		return ""
	}
	role := "http-v2/" + base + "/" + backendKey
	switch base {
	case RolePool, RoleMember, RoleMonitor:
		if matchKey == "" && len(predicateKeys) == 0 {
			return role
		}
	default:
		return httpMatchRole(role, base, matchKey, predicateKeys)
	}
	return ""
}

const weightedHTTPRolePrefix = "http-weighted-v1/"

// HTTPWeightedPolicyRole identifies a policy or predicate owned by a rule.
func HTTPWeightedPolicyRole(base, ruleKey, matchKey string, predicateKeys ...string) string {
	if !validHTTPKey(ruleKey) {
		return ""
	}
	return httpMatchRole(weightedHTTPRolePrefix+base+"/"+ruleKey, base, matchKey, predicateKeys)
}

func httpMatchRole(role, base, match string, predicateKeys []string) string {
	if !validHTTPKey(match) {
		return ""
	}
	switch base {
	case RolePolicyExact, RolePolicyPrefix, RoleRulePath, RoleRuleHost, RoleRuleHTTPMethod, RoleRuleGRPCContentType:
		if len(predicateKeys) == 0 {
			return role + "/" + match
		}
	case RoleRuleHTTPHeaderExact:
		if len(predicateKeys) == 1 && validHTTPKey(predicateKeys[0]) {
			return role + "/" + match + "/" + predicateKeys[0]
		}
	}
	return ""
}

func IsHTTPWeightedPolicyRole(role string) bool {
	return strings.HasPrefix(role, weightedHTTPRolePrefix)
}

// ParseHTTPFragmentRole recognizes only roles the HTTP path writer can create.
func ParseHTTPFragmentRole(raw string) (base, backendKey, matchKey string, ok bool) {
	if scoped, found := strings.CutPrefix(raw, scopedHTTPRolePrefix); found {
		base, match, _ := strings.Cut(scoped, "/")
		if ScopedHTTPPolicyRole(base, match) == raw {
			return base, "", match, true
		}
		return "", "", "", false
	}
	prefix, formatRole := "http-v2/", HTTPFragmentRole
	if IsHTTPWeightedPolicyRole(raw) {
		prefix, formatRole = weightedHTTPRolePrefix, HTTPWeightedPolicyRole
	}
	rest, ok := strings.CutPrefix(raw, prefix)
	if !ok {
		return "", "", "", false
	}
	base, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", false
	}
	backendKey, rest, _ = strings.Cut(rest, "/")
	matchKey, predicate, hasPredicate := strings.Cut(rest, "/")
	var predicateKeys []string
	if hasPredicate {
		predicateKeys = []string{predicate}
	}
	if formatRole(base, backendKey, matchKey, predicateKeys...) != raw {
		return "", "", "", false
	}
	return base, backendKey, matchKey, true
}

const RoleRuleRedirectGuard = "l7-rule-redirect-guard"

// HTTPRedirectKey identifies a physical redirect predicate or one of its guards.
func HTTPRedirectKey(first, second, third string) string {
	return httpTupleKey(first, second, third)
}

// HTTPRedirectRole identifies a physical predicate within a logical redirect
// match. Its identity is independent of position and destination changes.
func HTTPRedirectRole(base, match, cell, guard string) string {
	if !validHTTPKey(match) || !validHTTPKey(cell) {
		return ""
	}
	role := "http-redirect-v1/" + base + "/" + match + "/" + cell
	switch base {
	case RolePolicyExact, RolePolicyPrefix, RoleRulePath, RoleRuleHost, RoleRuleHTTPMethod:
		if guard == "" {
			return role
		}
	case RoleRuleRedirectGuard, RoleRuleHTTPHeaderExact:
		if validHTTPKey(guard) {
			return role + "/" + guard
		}
	}
	return ""
}

// ParseHTTPRedirectRole accepts only the canonical form written by the controller.
func ParseHTTPRedirectRole(raw string) (base, match, cell, guard string, ok bool) {
	rest, ok := strings.CutPrefix(raw, "http-redirect-v1/")
	if !ok {
		return "", "", "", "", false
	}
	fields := strings.Split(rest, "/")
	if len(fields) < 3 || len(fields) > 4 {
		return "", "", "", "", false
	}
	if len(fields) == 4 {
		guard = fields[3]
	}
	base, match, cell = fields[0], fields[1], fields[2]
	if HTTPRedirectRole(base, match, cell, guard) != raw {
		return "", "", "", "", false
	}
	return base, match, cell, guard, true
}

// ResourceRole returns the unique, canonically encoded role from provider tags.
func ResourceRole(tags []string) (string, bool) {
	encoded, ok := uniqueEncodedTag(tags, "role")
	if !ok {
		return "", false
	}
	value, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(value) == 0 || encode(string(value)) != encoded {
		return "", false
	}
	return string(value), true
}

func uniqueEncodedTag(tags []string, wanted string) (string, bool) {
	var value string
	for _, tag := range tags {
		key, encoded, ok := parseIdentityTag(tag)
		if ok && key == wanted {
			if value != "" || encoded == "" {
				return "", false
			}
			value = encoded
		}
	}
	return value, value != ""
}

// RouteOwnerProof can compare descendants within an already verified Gateway.
// It retains the exact encoded ownership tuple, including length-limited hashes.
type RouteOwnerProof struct {
	namespace string
	name      string
	uid       string
}

// RouteOwner verifies the parent Gateway, role and unique immutable route tags.
// Its result identifies the owner for comparing independently observed children.
func (id Identity) RouteOwner(tags []string, role string) (RouteOwnerProof, bool) {
	switch role {
	case RolePool, RoleMember, RoleMonitor, RolePolicyExact, RolePolicyPrefix, RoleRulePath, RoleRuleHost:
	default:
		_, _, _, backend := ParseHTTPFragmentRole(role)
		if _, _, _, _, redirect := ParseHTTPRedirectRole(role); !backend && !redirect {
			return RouteOwnerProof{}, false
		}
	}
	if id.value.OpenStackProjectID == "" || !id.MatchesGateway(tags, role) {
		return RouteOwnerProof{}, false
	}
	ownerTag := func(key string) (string, bool) {
		encoded, ok := uniqueEncodedTag(tags, key)
		if !ok {
			return "", false
		}
		if digest, hashed := strings.CutPrefix(encoded, "sha256-"); hashed {
			return encoded, validHTTPKey(digest)
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
		return encoded, err == nil && len(raw) > 0 && encode(string(raw)) == encoded
	}
	namespace, nsOK := ownerTag("route-namespace")
	name, nameOK := ownerTag("route-name")
	uid, uidOK := ownerTag("route-uid")
	if !nsOK || !nameOK || !uidOK {
		return RouteOwnerProof{}, false
	}
	return RouteOwnerProof{namespace: namespace, name: name, uid: uid}, true
}
