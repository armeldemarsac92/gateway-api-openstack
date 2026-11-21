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
