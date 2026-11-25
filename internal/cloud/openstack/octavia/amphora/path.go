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

package amphora

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// PathRules contains the stock pair or one raw value for HTTP_PATH_LITERAL.
// The provider applies the segment boundary when comparing a literal prefix.
type PathRules struct {
	Exact   string
	Prefix  string
	Literal string
}

// PathRuleValues keeps the stock encoding when its rule values fit and the
// legacy renderer can pass the path to HAProxy unchanged.
func PathRuleValues(path string, isPrefix, allowLiteralPaths bool) (PathRules, error) {
	if path == "" {
		path = "/"
	}
	if !cloud.ValidHTTPPath(path) {
		return PathRules{}, fmt.Errorf("path %q is not a valid absolute Gateway API path of at most 1024 ASCII bytes", path)
	}
	if isPrefix && path != "/" {
		path = strings.TrimSuffix(path, "/")
	}

	stockLength := len(path)
	if isPrefix && path != "/" {
		stockLength++
	}
	// Octavia's legacy Jinja template HTML-escapes these otherwise legal
	// characters before HAProxy parses the rule. Percent escapes stay raw.
	if stockLength > maxL7RuleValueLength || strings.ContainsAny(path, "'&") {
		if allowLiteralPaths {
			return PathRules{Literal: path}, nil
		}
		if stockLength > maxL7RuleValueLength {
			return PathRules{}, fmt.Errorf("path requires an Octavia L7 rule value of %d characters, exceeding the stock %d character limit. Enable --octavia-literal-http-paths", stockLength, maxL7RuleValueLength)
		}
		return PathRules{}, errors.New("HTTP paths containing apostrophes or ampersands require --octavia-literal-http-paths because the stock Octavia renderer changes those characters")
	}

	if !isPrefix {
		return PathRules{Exact: path}, nil
	}
	if path == "/" {
		return PathRules{Prefix: path}, nil
	}
	return PathRules{Exact: path, Prefix: path + "/"}, nil
}

var literalPathSegments = regexp.MustCompile(`^/[a-zA-Z0-9_/-]+$`)

// LiteralPathPrefixPattern maps a normalized, ordinary path prefix to one
// Octavia PATH rule. Other paths retain their literal equality/prefix pair.
func LiteralPathPrefixPattern(path string) (string, bool) {
	const boundary = "($|/)"
	if len(path)+1+len(boundary) > maxL7RuleValueLength || !literalPathSegments.MatchString(path) {
		return "", false
	}
	return "^" + path + boundary, true
}
