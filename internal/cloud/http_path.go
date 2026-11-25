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
	"unicode/utf8"
)

var externalAuthPath = regexp.MustCompile("^(?:[-A-Za-z0-9/._~!$&'()*+,;=:@]|%[0-9a-fA-F]{2})+$")

// ValidExternalAuthPathPrefix accepts the optional prefix sent to the HTTP authorization service.
func ValidExternalAuthPathPrefix(value string) bool {
	return value == "" || len(value) <= 1024 && strings.HasPrefix(value, "/") && externalAuthPath.MatchString(value)
}

// ValidHTTPPathReplacement checks the length and encoding of a redirect or
// rewrite replacement. Octavia encodes literal characters for the data plane.
func ValidHTTPPathReplacement(value string) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) <= 1024
}

// ValidHTTPPath checks the absolute path syntax used by Gateway API Exact and PathPrefix matches.
func ValidHTTPPath(path string) bool {
	if len(path) > 1024 || !strings.HasPrefix(path, "/") || strings.Contains(path, "//") || strings.Contains(path, "/./") || strings.Contains(path, "/../") ||
		strings.Contains(strings.ToLower(path), "%2f") || strings.Contains(path, "#") || strings.HasSuffix(path, "/.") || strings.HasSuffix(path, "/..") {
		return false
	}
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-/_~.!$&'()*+,;=:@", rune(c)) {
			continue
		}
		if c != '%' || i+2 >= len(path) || !hex(path[i+1]) || !hex(path[i+2]) {
			return false
		}
		i += 2
	}
	return true
}

func hex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
