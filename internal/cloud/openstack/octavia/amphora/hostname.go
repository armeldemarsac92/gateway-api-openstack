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
)

const maxL7RuleValueLength = 255

var exactDNSHostname = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// ExactHostnameRuleValue encodes one exact DNS hostname for an Amphora HOST_NAME
// REGEX rule. The stock renderer applies case-insensitive matching to the
// Host value. Match an optional numeric port without changing the header.
func ExactHostnameRuleValue(hostname string) (string, error) {
	if _, err := netip.ParseAddr(hostname); err == nil || !exactDNSHostname.MatchString(hostname) {
		return "", errors.New("only exact DNS hostnames are supported by the Octavia HTTP profile")
	}

	value := "^" + regexp.QuoteMeta(hostname) + `(:[0-9]+)?$`
	// The public Octavia L7 rule API limits the encoded value rather than the
	// input hostname. Both the DNS name and its regex encoding are ASCII.
	if len(value) > maxL7RuleValueLength {
		return "", fmt.Errorf("exact hostname requires an Octavia L7 rule value of %d characters, exceeding the %d character limit", len(value), maxL7RuleValueLength)
	}
	return value, nil
}
