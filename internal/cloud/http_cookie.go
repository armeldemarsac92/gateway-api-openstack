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
	"regexp"
	"strings"
)

var httpSessionCookieToken = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,255}$`)
var httpSessionCookieScope = regexp.MustCompile(`^[a-f0-9]{64}$`)

// HTTPSessionCookieDeadline scopes the absolute lifetime to a single backend object.
// A nil deadline leaves the provider's existing session cookie behavior in place.
type HTTPSessionCookieDeadline struct {
	TimeoutMS int64  `json:"cookie_absolute_timeout_ms"`
	Scope     string `json:"cookie_scope"`
	Permanent bool   `json:"cookie_permanent,omitempty"`
}

type HTTPSessionCookieRequirements uint8

const (
	HTTPSessionCookieNamed     HTTPSessionCookieRequirements = 0
	HTTPSessionCookieTimed     HTTPSessionCookieRequirements = 1 << 0
	HTTPSessionCookiePermanent HTTPSessionCookieRequirements = 1 << 1
	HTTPSessionCookiePath      HTTPSessionCookieRequirements = 1 << 2
)

func (deadline *HTTPSessionCookieDeadline) Requirements() HTTPSessionCookieRequirements {
	switch {
	case deadline == nil:
		return HTTPSessionCookieNamed
	case deadline.Permanent:
		return HTTPSessionCookiePermanent
	default:
		return HTTPSessionCookieTimed
	}
}

// WithPath adds the regional requirement for a custom cookie path.
func (requirements HTTPSessionCookieRequirements) WithPath(path *string) HTTPSessionCookieRequirements {
	if path != nil && *path != "/" {
		return requirements | HTTPSessionCookiePath
	}
	return requirements
}

func (deadline HTTPSessionCookieDeadline) Valid() bool {
	// The Gateway API duration grammar allows up to four groups of five digits.
	const maxTimeoutMS = 4 * 99999 * 3600000
	return deadline.TimeoutMS >= 0 && deadline.TimeoutMS <= maxTimeoutMS && httpSessionCookieScope.MatchString(deadline.Scope)
}

// ValidHTTPSessionCookieName checks the Octavia named HTTP_COOKIE contract.
func ValidHTTPSessionCookieName(name string) bool {
	lower := strings.ToLower(name)
	return httpSessionCookieToken.MatchString(name) &&
		!strings.HasPrefix(lower, "__host-") && !strings.HasPrefix(lower, "__secure-") &&
		!strings.HasPrefix(lower, "__http-")
}

// ValidHTTPSessionCookiePath checks the RFC 6265 path-value byte grammar
// and the Octavia length limit. An explicit empty value is valid.
func ValidHTTPSessionCookiePath(path string) bool {
	if len(path) > 1024 {
		return false
	}
	for i := 0; i < len(path); i++ {
		if b := path[i]; b < 0x20 || b > 0x7e || b == ';' {
			return false
		}
	}
	return true
}
