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

const HTTPHeaderPersistenceType = "HTTP_HEADER"

// HTTPSessionHeaderPersistence is the policy enrolled on an HTTP pool.
type HTTPSessionHeaderPersistence struct {
	Name      string
	TimeoutMS *int64
}

func (p HTTPSessionHeaderPersistence) Valid() bool {
	return ValidHTTPSessionHeaderName(p.Name) &&
		(p.TimeoutMS == nil || *p.TimeoutMS >= 0 && *p.TimeoutMS <= 1439985600000)
}

// ValidForGRPC requires a custom ASCII metadata key that can carry Octavia's
// text token. Binary metadata keys expect a base64 payload.
func (p HTTPSessionHeaderPersistence) ValidForGRPC() bool {
	name := strings.ToLower(p.Name)
	return p.Valid() && name != "user-agent" && !strings.HasSuffix(name, "-bin") && grpcSessionHeaderToken.MatchString(name)
}

var grpcSessionHeaderToken = regexp.MustCompile(`^[a-z0-9_.-]+$`)

var sessionHeaderToken = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,256}$")

// ValidHTTPSessionHeaderName checks the Octavia header affinity contract.
func ValidHTTPSessionHeaderName(name string) bool {
	if !sessionHeaderToken.MatchString(name) {
		return false
	}
	name = strings.ToLower(name)
	switch name {
	case "authorization", "cache-control", "connection", "content-encoding", "content-length", "content-location",
		"content-range", "content-type", "cookie", "expect", "forwarded", "host", "keep-alive", "location", "proxy-authenticate",
		"proxy-authorization", "proxy-connection", "set-cookie", "strict-transport-security", "te", "trailer", "transfer-encoding",
		"upgrade", "www-authenticate", "x-client-ip", "x-real-ip":
		return false
	}
	for _, prefix := range []string{"access-control-", "grpc-", "sec-", "x-forwarded-", "x-ssl-"} {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	return true
}
