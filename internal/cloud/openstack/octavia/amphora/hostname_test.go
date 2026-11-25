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

func TestHostnameRuleValueChecksOctaviaLengthLimit(t *testing.T) {
	// The hostname, escaped separators, and optional port expression all count.
	label := strings.Repeat("a", 63)
	hostname := strings.Join([]string{label, label, label, strings.Repeat("b", 48)}, ".")
	value, err := HostnameRuleValue(hostname, false)
	if err != nil || len(value) != 255 {
		t.Fatalf("encoding the boundary hostname returned length %d and error %v, want 255 and nil", len(value), err)
	}
	if _, err := HostnameRuleValue(hostname+"b", false); err == nil {
		t.Fatal("expected the 256-character encoding to be rejected")
	}
}

func TestHostnameRuleValueRejectsInvalidHostname(t *testing.T) {
	if _, err := HostnameRuleValue("api.example.com|other.example.com", true); err == nil {
		t.Fatal("expected a hostname outside the DNS label grammar to be rejected")
	}
}

func TestLongHostnameRuleMatchesStockAuthoritySyntax(t *testing.T) {
	// Single-character labels maximize the number of escaped dots.
	exact := strings.Repeat("a.", 126) + "b"
	suffix := strings.Repeat("a.", 125) + "b"
	for _, tc := range []struct {
		name     string
		hostname string
		matching []string
		outside  []string
	}{
		{"exact", exact, []string{exact, strings.ToUpper(exact) + ":8080"}, []string{exact + ":http", "c." + exact}},
		{"wildcard", "*." + suffix, []string{"x." + suffix, "A.b-2." + strings.ToUpper(suffix) + ":8080"},
			[]string{suffix, "-x." + suffix, "x-." + suffix, "x." + suffix + ":http"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := HostnameRuleValue(tc.hostname, true)
			if len(tc.hostname) != 253 || err != nil || !IsLongHostnameRuleValue(value) {
				t.Fatalf("expected a long expression for a 253-character name, got %d characters: %v", len(value), err)
			}
			pattern := regexp.MustCompile("(?i)" + value)
			for _, input := range tc.matching {
				if !pattern.MatchString(input) {
					t.Errorf("did not match %q", input)
				}
			}
			for _, input := range tc.outside {
				if pattern.MatchString(input) {
					t.Errorf("unexpectedly matched %q", input)
				}
			}
		})
	}
}
