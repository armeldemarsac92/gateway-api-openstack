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

package cloud

import (
	"cmp"
	"errors"
	"slices"
	"unicode/utf8"
)

// HTTPQueryParamMatch compares a case-sensitive parameter name and a decoded literal value.
type HTTPQueryParamMatch struct {
	Name  string
	Value string
}

// NormalizeHTTPQueryParamMatches validates the provider limits and preserves
// the first entry for each name before sorting the conjunction by name.
func NormalizeHTTPQueryParamMatches(params []HTTPQueryParamMatch) ([]HTTPQueryParamMatch, error) {
	if len(params) == 0 {
		return nil, nil
	}
	if len(params) > 16 {
		return nil, errors.New("HTTPRoute supports at most 16 exact query parameter matches per alternative")
	}
	normalized := make([]HTTPQueryParamMatch, 0, len(params))
	seen := make(map[string]bool, len(params))
	for _, param := range params {
		if seen[param.Name] {
			continue
		}
		seen[param.Name] = true
		if !httpHeaderName.MatchString(param.Name) {
			return nil, errors.New("an exact HTTP query parameter match requires a valid name of 1–256 ASCII token characters")
		}
		if param.Value == "" || !utf8.ValidString(param.Value) || utf8.RuneCountInString(param.Value) > 1024 {
			return nil, errors.New("an exact HTTP query parameter match requires a valid UTF-8 value of 1–1024 characters")
		}
		normalized = append(normalized, param)
	}
	slices.SortFunc(normalized, func(a, b HTTPQueryParamMatch) int { return cmp.Compare(a.Name, b.Name) })
	return normalized, nil
}
