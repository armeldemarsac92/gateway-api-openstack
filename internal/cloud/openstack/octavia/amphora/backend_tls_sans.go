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
	"cmp"
	"errors"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const uriPathCharacter = `[A-Za-z0-9._~!$&'()*+,;=:@/-]|%[0-9A-Fa-f]{2}`

var sanURI = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:(?://[A-Za-z0-9._~!$&'()*+,;=:@%\[\]-]+)?` +
	`(?:` + uriPathCharacter + `)*(?:\?(?:` + uriPathCharacter + `|[?])*)?(?:#(?:` + uriPathCharacter + `|[?])*)?$`)

// NormalizeBackendTLSSubjectAltNames validates the provider's DNS and URI
// syntax. The list matches any identity, so order and identical entries have
// no effect. URI case and percent escapes remain exactly as supplied.
func NormalizeBackendTLSSubjectAltNames(names []cloud.BackendTLSSubjectAltName) ([]cloud.BackendTLSSubjectAltName, error) {
	switch {
	case len(names) == 0:
		return nil, nil
	case len(names) > 5:
		return nil, errors.New("subjectAltNames supports at most five identities")
	}
	names = slices.Clone(names)
	for i := range names {
		name := &names[i]
		switch name.Type {
		case cloud.BackendTLSSANHostname:
			name.Hostname = strings.ToLower(name.Hostname)
			if name.URI == "" && ValidateSNIHostname(name.Hostname) == nil {
				continue
			}
		case cloud.BackendTLSSANURI:
			if name.Hostname == "" && validBackendTLSSANURI(name.URI) {
				continue
			}
		}
		return nil, errors.New("each subjectAltNames entry must specify a DNS Hostname or an absolute URI supported by Octavia")
	}
	slices.SortFunc(names, func(a, b cloud.BackendTLSSubjectAltName) int {
		return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Hostname, b.Hostname), cmp.Compare(a.URI, b.URI))
	})
	return slices.Compact(names), nil
}

func validBackendTLSSANURI(value string) bool {
	if len(value) > maxDNSHostnameLength || !sanURI.MatchString(value) {
		return false
	}
	_, rest, _ := strings.Cut(value, ":")
	identity, _, _ := strings.Cut(rest, "#")
	if identity == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	if !strings.HasPrefix(rest, "//") {
		return true
	}
	host := parsed.Hostname()
	if strings.HasPrefix(parsed.Host, "[") {
		addr, err := netip.ParseAddr(host)
		return err == nil && addr.Is6() && addr.Zone() == ""
	}
	return strings.Contains(host, ".") && exactDNSHostname.MatchString(strings.ToLower(strings.TrimSuffix(host, ".")))
}
