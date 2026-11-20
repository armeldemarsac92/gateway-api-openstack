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
	"strings"
	"testing"
)

func TestHostnameRuleValueChecksOctaviaLengthLimit(t *testing.T) {
	// The hostname, escaped separators, and optional port expression all count.
	label := strings.Repeat("a", 63)
	hostname := strings.Join([]string{label, label, label, strings.Repeat("b", 48)}, ".")
	value, err := HostnameRuleValue(hostname)
	if err != nil || len(value) != 255 {
		t.Fatalf("encoding the boundary hostname returned length %d and error %v, want 255 and nil", len(value), err)
	}
	if _, err := HostnameRuleValue(hostname + "b"); err == nil {
		t.Fatal("expected the 256-character encoding to be rejected")
	}
}

func TestHostnameRuleValueRejectsInvalidHostname(t *testing.T) {
	if _, err := HostnameRuleValue("api.example.com|other.example.com"); err == nil {
		t.Fatal("expected a hostname outside the DNS label grammar to be rejected")
	}
}
