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
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// PathRules carries either an anchored REGEX rule or the literal L7 rules.
type PathRules struct {
	Exact  string
	Prefix string
	Regex  string
}

// PathRuleValues validates the Octavia rule values for a path match. A non-root
// prefix uses two literal rules or an anchored regex with a segment boundary.
func PathRuleValues(path string, isPrefix bool) (PathRules, error) {
	if path == "" {
		path = "/"
	}
	if isPrefix {
		path = strings.TrimRight(path, "/")
	}
	rules := PathRules{Exact: path}
	if isPrefix {
		rules.Prefix = path + "/"
	}
	if strings.Contains(path, "'") {
		// The stock Amphora template leaves apostrophes unescaped. Hex
		// notation preserves the literal character through HAProxy parsing.
		end := "$"
		if isPrefix {
			end = "($|/)"
		}
		literal := strings.ReplaceAll(regexp.QuoteMeta(path), "'", `\x27`)
		rules = PathRules{Regex: "^" + literal + end}
	}
	length := max(utf8.RuneCountInString(rules.Exact), utf8.RuneCountInString(rules.Prefix), utf8.RuneCountInString(rules.Regex))
	if length > maxL7RuleValueLength {
		return PathRules{}, fmt.Errorf("path requires an Octavia L7 rule value of %d characters, exceeding the %d character limit", length, maxL7RuleValueLength)
	}
	return rules, nil
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
