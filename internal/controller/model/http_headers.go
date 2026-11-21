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
	"errors"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// NormalizeRequestHeaderModifier lowers the rule's single supported request filter.
func NormalizeRequestHeaderModifier(filters []gatewayv1.HTTPRouteFilter) ([]cloud.HTTPHeaderOperation, error) {
	if len(filters) != 1 || filters[0].Type != gatewayv1.HTTPRouteFilterRequestHeaderModifier ||
		filters[0].RequestHeaderModifier == nil {
		return nil, errors.New("a RequestHeaderModifier must specify one request header filter")
	}
	filter := filters[0].RequestHeaderModifier
	if len(filter.Set) > 16 || len(filter.Add) > 16 || len(filter.Remove) > 16 {
		return nil, errors.New("request header modifier supports up to 16 headers per operation type")
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
	return cloud.NormalizeHTTPHeaderOperations(operations)
}
