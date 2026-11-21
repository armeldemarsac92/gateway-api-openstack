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

package model

import (
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// NormalizeRequestRedirect validates the fixed-destination redirect profile.
// The scheme and port remain unset until a listener is selected.
func NormalizeRequestRedirect(rule gatewayv1.HTTPRouteRule) (RequestRedirect, error) {
	if len(rule.Filters) != 1 || rule.Filters[0].Type != gatewayv1.HTTPRouteFilterRequestRedirect {
		return RequestRedirect{}, errors.New("RequestRedirect must be the only filter in its rule")
	}
	filter := rule.Filters[0].RequestRedirect
	if filter == nil || filter.Hostname == nil {
		return RequestRedirect{}, errors.New("RequestRedirect without hostname requires request-hostname preservation in Octavia")
	}
	if len(rule.BackendRefs) != 0 {
		return RequestRedirect{}, errors.New("RequestRedirect cannot be combined with backendRefs")
	}
	if filter.Path != nil {
		return RequestRedirect{}, errors.New("RequestRedirect path modifiers are not supported")
	}
	hostname := string(*filter.Hostname)
	if !validRedirectHostname(hostname) {
		return RequestRedirect{}, errors.New("RequestRedirect hostname must be a fixed DNS name")
	}
	redirect := RequestRedirect{Hostname: hostname, StatusCode: http.StatusFound}
	if filter.Scheme != nil {
		switch *filter.Scheme {
		case "http", "https":
			redirect.Scheme = *filter.Scheme
		default:
			return RequestRedirect{}, fmt.Errorf("RequestRedirect scheme %q is unsupported", *filter.Scheme)
		}
	}
	if filter.Port != nil {
		if *filter.Port < 1 || *filter.Port > 65535 {
			return RequestRedirect{}, errors.New("RequestRedirect port must be between 1 and 65535")
		}
		redirect.Port = int(*filter.Port)
	}
	if filter.StatusCode != nil {
		switch *filter.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			redirect.StatusCode = *filter.StatusCode
		default:
			return RequestRedirect{}, fmt.Errorf("RequestRedirect status code %d is unsupported", *filter.StatusCode)
		}
	}
	return redirect, nil
}

func validRedirectHostname(hostname string) bool {
	if _, err := netip.ParseAddr(hostname); err == nil || len(validation.IsDNS1123Subdomain(hostname)) != 0 {
		return false
	}
	for label := range strings.SplitSeq(hostname, ".") {
		if len(label) > validation.DNS1123LabelMaxLength {
			return false
		}
	}
	return true
}
