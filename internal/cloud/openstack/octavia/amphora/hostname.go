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

// Package amphora defines routing rules supported by stock Octavia Amphora.
package amphora

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
)

const (
	maxL7RuleValueLength = 255
	maxDNSHostnameLength = 253
	dnsLabel             = `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`
)

var exactDNSHostname = regexp.MustCompile(`^` + dnsLabel + `(\.` + dnsLabel + `)*$`)

// ValidateHostname checks a DNS name or leading wildcard label before
// attachment. Only the resulting intersection has to fit in an L7 rule.
func ValidateHostname(hostname string) error {
	if _, err := netip.ParseAddr(hostname); err == nil ||
		len(hostname) > maxDNSHostnameLength ||
		!exactDNSHostname.MatchString(strings.TrimPrefix(hostname, "*.")) {
		return errors.New("hostname must be a DNS name with an optional leading wildcard label")
	}
	return nil
}

// ValidateExactHostname rejects wildcards and accepts an exact DNS name.
func ValidateExactHostname(hostname string) error {
	if strings.HasPrefix(hostname, "*.") {
		return errors.New("hostname must be an exact DNS name")
	}
	return ValidateHostname(hostname)
}

// HostnameRuleValue encodes one effective hostname for an Amphora HOST_NAME
// REGEX rule. The stock renderer applies case-insensitive matching to the
// Host value. Match an optional numeric port without changing the header.
func HostnameRuleValue(hostname string) (string, error) {
	if err := ValidateHostname(hostname); err != nil {
		return "", err
	}
	prefix := "^"
	suffix, wildcard := strings.CutPrefix(hostname, "*.")
	if wildcard {
		// At least one complete label must precede the suffix. A wildcard
		// matches deeper subdomains as well, while excluding the suffix apex.
		prefix += `(` + dnsLabel + `\.)+`
	}
	value := prefix + regexp.QuoteMeta(suffix) + `(:[0-9]+)?$`
	// The public Octavia L7 rule API limits the encoded value. Both the
	// DNS name and its regex encoding are ASCII.
	if len(value) > maxL7RuleValueLength {
		return "", fmt.Errorf("hostname requires an Octavia L7 rule value of %d characters, exceeding the %d character limit", len(value), maxL7RuleValueLength)
	}
	return value, nil
}
