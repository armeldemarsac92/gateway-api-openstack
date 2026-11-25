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

func TestPathRuleValuesUsesLiteralOnlyWhenNeeded(t *testing.T) {
	exact255 := "/" + strings.Repeat("a", 254)
	exact1024 := "/" + strings.Repeat("a", 1023)
	for _, tc := range []struct {
		name   string
		path   string
		prefix bool
		want   PathRules
	}{
		{name: "stock exact at limit", path: exact255, want: PathRules{Exact: exact255}},
		{name: "stock percent escape", path: "/%41", want: PathRules{Exact: "/%41"}},
		{name: "stock root", path: "/", prefix: true, want: PathRules{Prefix: "/"}},
		{name: "stock prefix", path: "/api/", prefix: true, want: PathRules{Exact: "/api", Prefix: "/api/"}},
		{name: "prefix with long segment", path: exact255, prefix: true, want: PathRules{Literal: exact255}},
		{name: "maximum exact", path: exact1024, want: PathRules{Literal: exact1024}},
		{name: "maximum prefix", path: exact1024, prefix: true, want: PathRules{Literal: exact1024}},
		{name: "maximum prefix with trailing slash", path: exact1024[:1023] + "/", prefix: true, want: PathRules{Literal: exact1024[:1023]}},
		{name: "short apostrophe", path: "/o'clock", want: PathRules{Literal: "/o'clock"}},
		{name: "short ampersand", path: "/a&b/", prefix: true, want: PathRules{Literal: "/a&b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules, err := PathRuleValues(tc.path, tc.prefix, true)
			if err != nil || rules != tc.want {
				t.Fatalf("enabled rules = %+v, %v, want %+v", rules, err, tc.want)
			}
			rules, err = PathRuleValues(tc.path, tc.prefix, false)
			if tc.want.Literal != "" {
				if err == nil || !strings.Contains(err.Error(), "--octavia-literal-http-paths") {
					t.Fatalf("expected an explicit opt-in error, got %+v, %v", rules, err)
				}
			} else if err != nil || rules != tc.want {
				t.Fatalf("stock rules = %+v, %v, want %+v", rules, err, tc.want)
			}
		})
	}

	if _, err := PathRuleValues(exact1024+"/", true, true); err == nil {
		t.Fatal("accepted a 1025-byte path before prefix normalization")
	}
}
