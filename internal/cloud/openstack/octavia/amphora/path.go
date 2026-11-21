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
	"strings"
	"unicode/utf8"
)

// PathRuleValues returns the values of the optional EQUAL_TO and STARTS_WITH
// rules. Non-root prefixes need both to enforce element-wise matching.
func PathRuleValues(path string, isPrefix bool) (exact, prefix string, err error) {
	if path == "" {
		path = "/"
	}
	exact = path
	if isPrefix {
		exact = strings.TrimRight(path, "/")
		prefix = exact + "/"
	}
	length := max(utf8.RuneCountInString(exact), utf8.RuneCountInString(prefix))
	if length > maxL7RuleValueLength {
		return "", "", fmt.Errorf("path requires an Octavia L7 rule value of %d characters, exceeding the %d character limit", length, maxL7RuleValueLength)
	}
	return exact, prefix, nil
}
