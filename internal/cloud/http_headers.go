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
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"
)

// HTTPHeaderAction is one request header modification.
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

// NormalizeHTTPHeaderOperations validates literal operations and canonicalizes names.
func NormalizeHTTPHeaderOperations(operations []HTTPHeaderOperation) ([]HTTPHeaderOperation, error) {
	if len(operations) == 0 {
		return nil, nil
	}
	if len(operations) > 48 {
		return nil, errors.New("request header modifier has more than 48 operations")
	}
	seen := make(map[string]bool, len(operations))
	normalized := make([]HTTPHeaderOperation, 0, len(operations))
	for _, operation := range operations {
		if !httpHeaderName.MatchString(operation.Name) {
			return nil, errors.New("invalid request header name")
		}
		operation.Name = strings.ToLower(operation.Name)
		if seen[operation.Name] {
			return nil, errors.New("request header operation names must be unique regardless of case")
		}
		seen[operation.Name] = true
		switch operation.Operation {
		case HTTPHeaderSet, HTTPHeaderAdd:
			if !utf8.ValidString(operation.Value) || utf8.RuneCountInString(operation.Value) > 4096 ||
				strings.ContainsFunc(operation.Value, func(c rune) bool { return c < ' ' && c != 9 || c == 127 }) {
				return nil, errors.New("invalid request header value")
			}
		case HTTPHeaderRemove:
			if operation.Value != "" {
				return nil, errors.New("REMOVE takes a request header name without a value")
			}
		default:
			return nil, errors.New("unrecognized request header operation")
		}
		normalized = append(normalized, operation)
	}
	return normalized, nil
}
