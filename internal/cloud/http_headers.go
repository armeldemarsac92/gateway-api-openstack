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
	"encoding/base64"
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

// MaxHTTPHeaderOperations is the maximum size of an Amphora modifier list.
const MaxHTTPHeaderOperations = 48

// HTTPHeaderOperation applies an action with an optional literal value.
type HTTPHeaderOperation struct {
	Operation HTTPHeaderAction
	Name      string
	Value     string
}

var httpHeaderName = regexp.MustCompile(`^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]{1,256}$`)

var grpcMetadataName = regexp.MustCompile(`^[a-z0-9_.-]+$`)

// ValidGRPCMetadataName checks the lowercase alphabet accepted by gRPC.
// Each use decides which reserved keys it can modify.
func ValidGRPCMetadataName(name string) bool {
	return grpcMetadataName.MatchString(name)
}

// ValidateGRPCRequestMetadata checks a normalized request modifier list.
func ValidateGRPCRequestMetadata(operations []HTTPHeaderOperation) error {
	return validateGRPCMetadata(operations, true)
}

// ValidateGRPCResponseMetadata checks a normalized response modifier list.
// The RPC status can share a headers block with application metadata.
func ValidateGRPCResponseMetadata(operations []HTTPHeaderOperation) error {
	return validateGRPCMetadata(operations, false)
}

func validateGRPCMetadata(operations []HTTPHeaderOperation, request bool) error {
	direction := "response"
	if request {
		direction = "request"
	}
	fields := 0
	for _, op := range operations {
		if op.Name == "content-type" || strings.HasPrefix(op.Name, "grpc-") {
			return fmt.Errorf("%s header %q is managed by gRPC", direction, op.Name)
		}
		if request {
			switch op.Name {
			case "connection", "content-length", "host", "keep-alive", "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade":
				return fmt.Errorf("request header %q is managed by the gRPC transport", op.Name)
			}
		}
		if !ValidGRPCMetadataName(op.Name) {
			return fmt.Errorf("invalid gRPC %s metadata name %q", direction, op.Name)
		}
		if op.Operation == HTTPHeaderRemove {
			continue
		}
		if strings.HasSuffix(op.Name, "-bin") {
			fields += 1 + strings.Count(op.Value, ",")
			for value := range strings.SplitSeq(op.Value, ",") {
				value = strings.Trim(value, " ")
				encoding := base64.RawStdEncoding
				if strings.HasSuffix(value, "=") {
					encoding = base64.StdEncoding
				}
				if _, err := encoding.Strict().DecodeString(value); err != nil {
					return fmt.Errorf("gRPC binary metadata %q requires base64 values", op.Name)
				}
			}
		} else {
			if strings.ContainsFunc(op.Value, func(r rune) bool { return r < ' ' || r > '~' }) {
				return fmt.Errorf("gRPC %s metadata %q requires printable ASCII", direction, op.Name)
			}
			fields++
		}
		if fields > MaxHTTPHeaderOperations {
			return fmt.Errorf("gRPC %s metadata can set or add at most %d fields after expanding binary values", direction, MaxHTTPHeaderOperations)
		}
	}
	return nil
}

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
	if len(operations) > MaxHTTPHeaderOperations {
		return nil, fmt.Errorf("%s header modifier has more than %d operations", direction, MaxHTTPHeaderOperations)
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

// ValidateHTTPBackendRequestFilterComposition checks the order supported by
// Octavia: rule headers, rule URL, target headers, target URL.
func ValidateHTTPBackendRequestFilterComposition(rule, target []HTTPHeaderOperation, ruleURL, targetURL *HTTPURLRewrite) error {
	if len(target) == 0 && targetURL == nil {
		return nil
	}
	if ruleURL != nil && targetURL != nil {
		if ruleURL.Hostname != "" && targetURL.Hostname != "" {
			return errors.New("a rule and its backendRef cannot both rewrite the hostname")
		}
		if (ruleURL.ReplaceFullPath != nil || ruleURL.ReplacePrefixMatch != nil) &&
			(targetURL.ReplaceFullPath != nil || targetURL.ReplacePrefixMatch != nil) {
			return errors.New("a rule and its backendRef cannot both rewrite the path")
		}
	}
	if ruleURL != nil && ruleURL.Hostname != "" || targetURL != nil && targetURL.Hostname != "" {
		changesHost := func(operation HTTPHeaderOperation) bool { return strings.EqualFold(operation.Name, "Host") }
		if slices.ContainsFunc(rule, changesHost) || slices.ContainsFunc(target, changesHost) {
			return errors.New("a Host header operation cannot be combined with hostname rewriting on a filtered backendRef")
		}
	}
	return nil
}
