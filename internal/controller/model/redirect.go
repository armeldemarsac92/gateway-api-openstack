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
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/validation"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// NormalizeRequestRedirect validates the redirect before a listener is selected.
func NormalizeRequestRedirect(rule gatewayv1.HTTPRouteRule, requestAction bool) (RequestRedirect, *Diagnostic) {
	reject := func(message string) (RequestRedirect, *Diagnostic) {
		return RequestRedirect{}, unsupported(message, ScopeRule)
	}
	invalid := func(message string) (RequestRedirect, *Diagnostic) {
		return RequestRedirect{}, &Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: "Invalid", Message: message, Scope: ScopeRule}
	}
	if len(rule.Filters) != 1 || rule.Filters[0].Type != gatewayv1.HTTPRouteFilterRequestRedirect {
		return reject("RequestRedirect must be the only filter in its rule")
	}
	filter := rule.Filters[0].RequestRedirect
	if filter == nil {
		return reject("RequestRedirect filter must specify requestRedirect")
	}
	if len(rule.BackendRefs) != 0 {
		return reject("RequestRedirect cannot be combined with backendRefs")
	}

	redirect := RequestRedirect{StatusCode: http.StatusFound}
	if filter.Hostname != nil {
		redirect.Hostname = string(*filter.Hostname)
		if !validRedirectHostname(redirect.Hostname) {
			return reject("RequestRedirect hostname must be a fixed DNS name")
		}
	}
	if filter.Scheme != nil {
		switch *filter.Scheme {
		case "http", "https":
			redirect.Scheme = *filter.Scheme
		default:
			return reject(fmt.Sprintf("RequestRedirect scheme %q is unsupported", *filter.Scheme))
		}
	}
	if filter.Port != nil {
		if *filter.Port < 1 || *filter.Port > 65535 {
			return reject("RequestRedirect port must be between 1 and 65535")
		}
		redirect.Port = int(*filter.Port)
	}
	if filter.StatusCode != nil {
		switch *filter.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			redirect.StatusCode = *filter.StatusCode
		default:
			return reject(fmt.Sprintf("RequestRedirect status code %d is unsupported", *filter.StatusCode))
		}
	}
	if path := filter.Path; path != nil {
		switch path.Type {
		case gatewayv1.FullPathHTTPPathModifier:
			if path.ReplaceFullPath == nil || path.ReplacePrefixMatch != nil || utf8.RuneCountInString(*path.ReplaceFullPath) > 1024 {
				return invalid("RequestRedirect ReplaceFullPath requires only a replaceFullPath value of up to 1024 characters")
			}
			replacement := *path.ReplaceFullPath
			redirect.ReplaceFullPath = &replacement
		case gatewayv1.PrefixMatchHTTPPathModifier:
			if path.ReplacePrefixMatch == nil || path.ReplaceFullPath != nil || utf8.RuneCountInString(*path.ReplacePrefixMatch) > 1024 {
				return invalid("RequestRedirect ReplacePrefixMatch requires only a replacePrefixMatch value of up to 1024 characters")
			}
			for _, match := range rule.Matches {
				if match.Path == nil || match.Path.Type == nil {
					continue
				}
				switch *match.Path.Type {
				case gatewayv1.PathMatchPathPrefix:
				case gatewayv1.PathMatchExact, gatewayv1.PathMatchRegularExpression:
					return invalid("RequestRedirect ReplacePrefixMatch requires PathPrefix in every match")
				default:
					return reject(fmt.Sprintf("RequestRedirect ReplacePrefixMatch does not support path match type %q", *match.Path.Type))
				}
			}
			replacement := *path.ReplacePrefixMatch
			redirect.ReplacePrefixMatch = &replacement
		default:
			return reject(fmt.Sprintf("RequestRedirect path modifier type %q is unsupported", path.Type))
		}
	}
	if !requestAction && redirect.NeedsRequestAction() {
		if redirect.Hostname == "" {
			return reject("RequestRedirect without hostname requires request-hostname preservation via --octavia-http-request-redirect")
		}
		return reject("RequestRedirect path modifiers require --octavia-http-request-redirect")
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
