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
	"regexp"
	"strings"
	"testing"
)

func TestLiteralPathPrefixPattern(t *testing.T) {
	pattern, ok := LiteralPathPrefixPattern("/shop/A-2_items")
	if !ok || pattern != "^/shop/A-2_items($|/)" {
		t.Fatalf("literal prefix pattern = %q, %v", pattern, ok)
	}
	for _, path := range []string{"/", "/v1.0", "/book+list", "/" + strings.Repeat("a", 250)} {
		if pattern, ok := LiteralPathPrefixPattern(path); ok {
			t.Fatalf("expected the original path mapping for %q, got %q", path, pattern)
		}
	}
}

func TestApostrophePathRules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		path      string
		isPrefix  bool
		pattern   string
		matches   []string
		fallsThru []string
	}{
		{
			name: "exact", path: "/o'clock+books", pattern: `^/o"'"clock\+books$`,
			matches: []string{"/o'clock+books"}, fallsThru: []string{"/o'clock+books/", "/o'clockbooks", "/o%27clock+books"},
		},
		{
			name: "prefix", path: "/o'clock/", isPrefix: true, pattern: `^/o"'"clock($|/)`,
			matches: []string{"/o'clock", "/o'clock/", "/o'clock/noon"}, fallsThru: []string{"/o'clockish", "/O'clock"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules, err := PathRuleValues(tc.path, tc.isPrefix)
			if err != nil || rules != (PathRules{Regex: tc.pattern}) {
				t.Fatalf("path rules = %+v, %v, want only regex %q", rules, err, tc.pattern)
			}
			pattern := regexp.MustCompile(strings.ReplaceAll(rules.Regex, `"'"`, "'"))
			for _, path := range tc.matches {
				if !pattern.MatchString(path) {
					t.Errorf("pattern does not match %q", path)
				}
			}
			for _, path := range tc.fallsThru {
				if pattern.MatchString(path) {
					t.Errorf("pattern matches %q", path)
				}
			}
		})
	}
}

func TestApostrophePathRuleValueLength(t *testing.T) {
	path := "/o'clock/" + strings.Repeat("a", 238)
	if rules, err := PathRuleValues(path, true); err != nil || len(rules.Regex) != 255 {
		t.Fatalf("path rules = %+v, %v, want a 255-character regex", rules, err)
	}
	if _, err := PathRuleValues(path+"a", true); err == nil || !strings.Contains(err.Error(), "256 characters") {
		t.Fatalf("error = %v, want encoded rule value length exceeding 255", err)
	}
}
