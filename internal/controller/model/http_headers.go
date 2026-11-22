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

// Package cloud defines the provider-neutral boundary used by the Kubernetes
// reconcilers. Provider SDK types must not cross this package boundary.
package model

import (
	"fmt"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// NormalizeHTTPHeaderFilters compiles at most one filter for each direction.
// Request and response operations act on independent messages.
func NormalizeHTTPHeaderFilters(filters []gatewayv1.HTTPRouteFilter, requestEnabled, responseEnabled bool) (request, response []cloud.HTTPHeaderOperation, err error) {
	seen := make(map[gatewayv1.HTTPRouteFilterType]bool, len(filters))
	for _, filter := range filters {
		var enabled bool
		var headerFilter *gatewayv1.HTTPHeaderFilter
		var operations *[]cloud.HTTPHeaderOperation
		switch filter.Type {
		case gatewayv1.HTTPRouteFilterRequestHeaderModifier:
			enabled, headerFilter, operations = requestEnabled, filter.RequestHeaderModifier, &request
		case gatewayv1.HTTPRouteFilterResponseHeaderModifier:
			enabled, headerFilter, operations = responseEnabled, filter.ResponseHeaderModifier, &response
		default:
			return nil, nil, fmt.Errorf("HTTPRoute filter %s is not supported in this combination", filter.Type)
		}
		if !enabled {
			return nil, nil, fmt.Errorf("%s requires route-scoped header operations in Octavia", filter.Type)
		}
		if seen[filter.Type] || headerFilter == nil {
			return nil, nil, fmt.Errorf("%s must specify one header filter per rule", filter.Type)
		}
		seen[filter.Type] = true
		*operations, err = normalizeHTTPHeaderFilter(filter.Type, headerFilter)
		if err != nil {
			return nil, nil, err
		}
	}
	return request, response, nil
}

func normalizeHTTPHeaderFilter(kind gatewayv1.HTTPRouteFilterType, filter *gatewayv1.HTTPHeaderFilter) ([]cloud.HTTPHeaderOperation, error) {
	if len(filter.Set) > 16 || len(filter.Add) > 16 || len(filter.Remove) > 16 {
		return nil, fmt.Errorf("%s supports up to 16 headers per operation type", kind)
	}

	operations := make([]cloud.HTTPHeaderOperation, 0, len(filter.Set)+len(filter.Add)+len(filter.Remove))
	for _, header := range filter.Set {
		operations = append(operations, cloud.HTTPHeaderOperation{Operation: cloud.HTTPHeaderSet, Name: string(header.Name), Value: header.Value})
	}
	for _, header := range filter.Add {
		operations = append(operations, cloud.HTTPHeaderOperation{Operation: cloud.HTTPHeaderAdd, Name: string(header.Name), Value: header.Value})
	}
	for _, header := range filter.Remove {
		operations = append(operations, cloud.HTTPHeaderOperation{Operation: cloud.HTTPHeaderRemove, Name: header})
	}
	if kind == gatewayv1.HTTPRouteFilterResponseHeaderModifier {
		return cloud.NormalizeHTTPResponseHeaderOperations(operations)
	}
	return cloud.NormalizeHTTPHeaderOperations(operations)
}
