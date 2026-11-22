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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package cloud

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// HTTPHeaderAction is one HTTP header modification.
type HTTPHeaderAction string

const (
	HTTPHeaderSet    HTTPHeaderAction = "SET"
	HTTPHeaderAdd    HTTPHeaderAction = "ADD"
	HTTPHeaderRemove HTTPHeaderAction = "REMOVE"
)

// HTTPHeaderOperation applies an action with an optional literal value.
type HTTPHeaderOperation struct {
	Operation HTTPHeaderAction
	Name      string
	Value     string
}

var httpHeaderName = regexp.MustCompile(`^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]{1,256}$`)

// HTTPHeaderMatch compares a case-insensitive field name and a literal value.
type HTTPHeaderMatch struct {
	Name  string
	Value string
}

// NormalizeHTTPHeaderMatches validates the Amphora HTTP field limits and
// canonicalizes the conjunction independently of declaration order.
func NormalizeHTTPHeaderMatches(headers []HTTPHeaderMatch) ([]HTTPHeaderMatch, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	if len(headers) > 16 {
		return nil, errors.New("HTTPRoute supports at most 16 exact header matches per alternative")
	}
	normalized := make([]HTTPHeaderMatch, 0, len(headers))
	seen := make(map[string]bool, len(headers))
	for _, header := range headers {
		if !httpHeaderName.MatchString(header.Name) || len(header.Name) > 255 {
			return nil, errors.New("an exact HTTP header match requires a valid field name of at most 255 bytes on Amphora")
		}
		header.Name = strings.ToLower(header.Name)
		if seen[header.Name] {
			continue
		}
		seen[header.Name] = true
		if header.Value == "" || !utf8.ValidString(header.Value) || utf8.RuneCountInString(header.Value) > 4096 ||
			strings.Trim(header.Value, " \t") != header.Value ||
			strings.ContainsFunc(header.Value, func(c rune) bool { return c < ' ' && c != '\t' || c == 127 }) {
			return nil, errors.New("an exact HTTP header match requires a valid field value of 1–4096 characters without surrounding whitespace")
		}
		normalized = append(normalized, header)
	}
	slices.SortFunc(normalized, func(a, b HTTPHeaderMatch) int { return cmp.Compare(a.Name, b.Name) })
	return normalized, nil
}

// NormalizeHTTPHeaderOperations validates literal operations and canonicalizes names.
func NormalizeHTTPHeaderOperations(operations []HTTPHeaderOperation) ([]HTTPHeaderOperation, error) {
	return normalizeHTTPHeaderOperations(operations, false)
}

// NormalizeHTTPResponseHeaderOperations also checks fields managed by Octavia.
func NormalizeHTTPResponseHeaderOperations(operations []HTTPHeaderOperation) ([]HTTPHeaderOperation, error) {
	return normalizeHTTPHeaderOperations(operations, true)
}

func normalizeHTTPHeaderOperations(operations []HTTPHeaderOperation, response bool) ([]HTTPHeaderOperation, error) {
	direction := "request"
	if response {
		direction = "response"
	}
	if len(operations) == 0 {
		return nil, nil
	}
	if len(operations) > 48 {
		return nil, fmt.Errorf("%s header modifier has more than 48 operations", direction)
	}
	seen := make(map[string]bool, len(operations))
	normalized := make([]HTTPHeaderOperation, 0, len(operations))
	for _, operation := range operations {
		if !httpHeaderName.MatchString(operation.Name) {
			return nil, fmt.Errorf("invalid %s header name", direction)
		}
		operation.Name = strings.ToLower(operation.Name)
		if response {
			switch operation.Name {
			case "connection", "content-length", "keep-alive", "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade":
				return nil, fmt.Errorf("response header %q is managed by the Octavia listener", operation.Name)
			}
		}
		if seen[operation.Name] {
			return nil, fmt.Errorf("%s header operation names must be unique regardless of case", direction)
		}
		seen[operation.Name] = true
		switch operation.Operation {
		case HTTPHeaderSet, HTTPHeaderAdd:
			if !utf8.ValidString(operation.Value) || utf8.RuneCountInString(operation.Value) > 4096 ||
				strings.ContainsFunc(operation.Value, func(c rune) bool { return c < ' ' && c != 9 || c == 127 }) {
				return nil, fmt.Errorf("invalid %s header value", direction)
			}
		case HTTPHeaderRemove:
			if operation.Value != "" {
				return nil, fmt.Errorf("REMOVE takes a %s header name without a value", direction)
			}
		default:
			return nil, fmt.Errorf("unrecognized %s header operation", direction)
		}
		normalized = append(normalized, operation)
	}
	return normalized, nil
}
