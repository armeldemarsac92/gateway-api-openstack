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
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"k8s.io/utils/ptr"
)

// HTTPCORS configures CORS on a winning forwarding policy. A non-nil empty
// object deliberately denies every origin.
type HTTPCORS struct {
	AllowOrigins     []string `json:"allow_origins,omitempty"`
	AllowMethods     []string `json:"allow_methods,omitempty"`
	AllowHeaders     []string `json:"allow_headers,omitempty"`
	ExposeHeaders    []string `json:"expose_headers,omitempty"`
	AllowCredentials *bool    `json:"allow_credentials,omitempty"`
	MaxAge           int32    `json:"max_age,omitempty"`
}

var corsOrigin = regexp.MustCompile(`^https?://(?:\*|(?:\*\.)?(?:[a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+)(?::([0-9]{1,5}))?$`)
var corsMethods = []string{"*", "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"}

func validCORSOrigin(origin string) bool {
	if origin == "*" {
		return true
	}
	match := corsOrigin.FindStringSubmatch(origin)
	if len(origin) > 253 || match == nil {
		return false
	}
	if match[1] == "" {
		return true
	}
	port, _ := strconv.Atoi(match[1])
	return port >= 1 && port <= 65535
}

// Validate checks the Octavia request_cors_v1 field contract.
func (cors HTTPCORS) Validate() error {
	if cors.MaxAge < 0 {
		return fmt.Errorf("CORS maxAge must be positive when set")
	}
	for _, field := range []struct {
		name   string
		values []string
		limit  int
		valid  func(string) bool
	}{
		{"allowOrigins", cors.AllowOrigins, 64, validCORSOrigin},
		{"allowMethods", cors.AllowMethods, 9, func(method string) bool { return slices.Contains(corsMethods, method) }},
		{"allowHeaders", cors.AllowHeaders, 64, httpHeaderName.MatchString},
		{"exposeHeaders", cors.ExposeHeaders, 64, httpHeaderName.MatchString},
	} {
		if len(field.values) > field.limit || field.name != "exposeHeaders" && len(field.values) > 1 && slices.Contains(field.values, "*") {
			return fmt.Errorf("CORS %s exceeds its item limit or combines a wildcard with other values", field.name)
		}
		seen := make(map[string]bool, len(field.values))
		for _, value := range field.values {
			if !field.valid(value) || seen[value] {
				return fmt.Errorf("CORS %s contains an invalid or repeated value", field.name)
			}
			seen[value] = true
		}
	}
	return nil
}

func (cors *HTTPCORS) Equal(other *HTTPCORS) bool {
	if cors == nil || other == nil {
		return cors == other
	}
	equalSet := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for _, value := range a {
			if !slices.Contains(b, value) {
				return false
			}
		}
		return true
	}
	age := func(value int32) int32 {
		if value == 0 {
			return 5
		}
		return value
	}
	return ptr.Deref(cors.AllowCredentials, false) == ptr.Deref(other.AllowCredentials, false) &&
		age(cors.MaxAge) == age(other.MaxAge) && equalSet(cors.AllowOrigins, other.AllowOrigins) &&
		equalSet(cors.AllowMethods, other.AllowMethods) && equalSet(cors.AllowHeaders, other.AllowHeaders) &&
		equalSet(cors.ExposeHeaders, other.ExposeHeaders)
}

// ValidateHTTPCORSComposition checks header operations which would affect, or
// be overwritten by, Amphora's policy-level CORS processing.
func ValidateHTTPCORSComposition(request, response []HTTPHeaderOperation) error {
	for _, operation := range request {
		switch strings.ToLower(operation.Name) {
		case "origin", "access-control-request-method", "access-control-request-headers", "cookie", "authorization":
			return fmt.Errorf("operation on request header %s conflicts with the Octavia CORS policy", operation.Name)
		}
	}
	for _, operation := range response {
		switch strings.ToLower(operation.Name) {
		case "access-control-allow-origin", "access-control-allow-credentials", "access-control-allow-methods",
			"access-control-allow-headers", "access-control-expose-headers", "access-control-max-age", "vary":
			return fmt.Errorf("operation on response header %s conflicts with the Octavia CORS policy", operation.Name)
		}
	}
	return nil
}
