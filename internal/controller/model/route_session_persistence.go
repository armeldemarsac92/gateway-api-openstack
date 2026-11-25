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
	"crypto/sha256"
	"fmt"
	"slices"
	"strconv"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// RouteSessionPersistence records the rule that owns a Service forwarding pool.
type RouteSessionPersistence struct {
	Rule     string
	Settings *cloud.ServiceSessionPersistence
}

func RouteRulePersistence(rule gatewayv1.HTTPRouteRule) *gatewayv1.SessionPersistence {
	return rule.SessionPersistence //nolint:staticcheck // Still served in the v1.7 Experimental bundle.
}

// ValidateRouteSessionPersistence checks the inline field independently of backend reference resolution.
func ValidateRouteSessionPersistence(rule gatewayv1.HTTPRouteRule, kind gatewayv1.Kind, enabled bool) *Diagnostic {
	persistence := RouteRulePersistence(rule)
	if persistence == nil {
		return nil
	}
	if !enabled {
		return unsupported("rule sessionPersistence requires --experimental-route-session-persistence and the Experimental Gateway API channel", ScopeRule)
	}
	redirect := slices.ContainsFunc(rule.Filters, func(filter gatewayv1.HTTPRouteFilter) bool {
		return filter.Type == gatewayv1.HTTPRouteFilterRequestRedirect
	})
	if len(rule.BackendRefs) == 0 || redirect {
		return unsupported("rule sessionPersistence requires a forwarding rule with backendRefs", ScopeRule)
	}
	if kind == HTTPRouteKind && slices.ContainsFunc(rule.BackendRefs, func(ref gatewayv1.HTTPBackendRef) bool {
		_, supported := XBackendReference("", ref.BackendObjectReference)
		return supported
	}) {
		return unsupported("rule sessionPersistence requires core Service backendRefs", ScopeRule)
	}
	if diagnostic := validateSessionPersistence(*persistence, string(kind)+" rule"); diagnostic != nil {
		return unsupported(diagnostic.Message, ScopeRule)
	}
	if kind == GRPCRouteKind && IsHeaderSessionPersistence(*persistence) {
		header := cloud.HTTPSessionHeaderPersistence{Name: string(persistence.Header.Name)}
		if !header.ValidForGRPC() {
			return unsupported("GRPCRoute rule sessionPersistence requires a custom ASCII metadata key without the -bin suffix", ScopeRule)
		}
	}
	return nil
}

func routeSessionPersistence(uid types.UID, name *gatewayv1.SectionName, position int, backend BackendGroup, persistence gatewayv1.SessionPersistence) *RouteSessionPersistence {
	rule := "index/" + strconv.Itoa(position)
	if name != nil {
		rule = "name/" + string(*name)
	}
	scope := fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%q %q %q %q %d %q",
		uid, rule, backend.Service.Namespace, backend.Service.Name, ptr.Deref(backend.Reference.Port, 0), backend.ServiceUID)))
	return &RouteSessionPersistence{Rule: rule, Settings: poolSessionPersistence(persistence, "route", scope)}
}
