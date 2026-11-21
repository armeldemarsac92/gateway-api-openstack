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

// HTTPMatchKey identifies a logical predicate across list edits and rank changes.
func HTTPMatchKey(hostname string, pathType cloud.PathMatchType, path string) string {
	return httpTupleKey(hostname, string(pathType), path)
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
func HTTPFragmentRole(base, backendKey, matchKey string) string {
	if !validHTTPKey(backendKey) {
		return ""
	}
	role := "http-v2/" + base + "/" + backendKey
	switch base {
	case RolePool, RoleMember, RoleMonitor:
		if matchKey == "" {
			return role
		}
	case RolePolicyExact, RolePolicyPrefix, RoleRulePath, RoleRuleHost:
		if validHTTPKey(matchKey) {
			return role + "/" + matchKey
		}
	}
	return ""
}

// ParseHTTPFragmentRole recognizes only roles the HTTP path writer can create.
func ParseHTTPFragmentRole(raw string) (base, backendKey, matchKey string, ok bool) {
	rest, ok := strings.CutPrefix(raw, "http-v2/")
	if !ok {
		return "", "", "", false
	}
	base, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", false
	}
	backendKey, matchKey, _ = strings.Cut(rest, "/")
	if HTTPFragmentRole(base, backendKey, matchKey) != raw {
		return "", "", "", false
	}
	return base, backendKey, matchKey, true
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
		if _, _, _, ok := ParseHTTPFragmentRole(role); !ok {
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
