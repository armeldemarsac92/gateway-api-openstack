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
	"fmt"
	"math"
	"slices"

	"k8s.io/utils/ptr"
)

// HTTPRequestPolicy is Octavia's rule-scoped policy. A nil duration uses
// the provider default; a pointer to zero explicitly disables the deadline.
type HTTPRequestPolicy struct {
	RequestTimeoutMS        *int64     `json:"request_timeout_ms,omitempty"`
	BackendRequestTimeoutMS *int64     `json:"backend_request_timeout_ms,omitempty"`
	Retry                   *HTTPRetry `json:"retry,omitempty"`
}

// HTTPRetry holds additional attempts. A nil count defaults to one and an
// empty code set retries eligible connection failures only.
type HTTPRetry struct {
	Attempts *int  `json:"attempts,omitempty"`
	Codes    []int `json:"codes,omitempty"`
}

func (p HTTPRequestPolicy) Validate() error {
	request, backend := ptr.Deref(p.RequestTimeoutMS, 0), ptr.Deref(p.BackendRequestTimeoutMS, 0)
	if request < 0 || backend < 0 {
		return errors.New("HTTPRoute deadlines must be nonnegative")
	}
	if request > 0 && backend > request {
		return errors.New("HTTPRoute backendRequest timeout cannot exceed request timeout")
	}
	if p.Retry == nil {
		return nil
	}
	if attempts := ptr.Deref(p.Retry.Attempts, 1); attempts < 0 || attempts > math.MaxInt32 {
		return fmt.Errorf("HTTPRoute retry.attempts must be between 0 and %d", math.MaxInt32)
	}
	var seen map[int]bool
	if len(p.Retry.Codes) != 0 {
		seen = make(map[int]bool, len(p.Retry.Codes))
	}
	for _, code := range p.Retry.Codes {
		switch code {
		case 500, 502, 503, 504:
		default:
			return fmt.Errorf("HTTPRoute retry code %d is unsupported", code)
		}
		if seen[code] {
			return fmt.Errorf("HTTPRoute retry code %d is duplicated", code)
		}
		seen[code] = true
	}
	return nil
}

func (p *HTTPRequestPolicy) Equal(other *HTTPRequestPolicy) bool {
	if p == nil || other == nil {
		return p == other
	}
	return ptr.Equal(p.RequestTimeoutMS, other.RequestTimeoutMS) &&
		ptr.Equal(p.BackendRequestTimeoutMS, other.BackendRequestTimeoutMS) && p.Retry.equal(other.Retry)
}

func (p *HTTPRetry) equal(other *HTTPRetry) bool {
	if p == nil || other == nil {
		return p == other
	}
	return ptr.Deref(p.Attempts, 1) == ptr.Deref(other.Attempts, 1) && slices.Equal(p.Codes, other.Codes)
}
