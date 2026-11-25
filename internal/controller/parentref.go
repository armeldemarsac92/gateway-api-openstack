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

package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ListenerSetServed checks discovery before registering a ListenerSet informer.
func ListenerSetServed(mapper meta.RESTMapper) (bool, error) {
	kind := schema.GroupKind{Group: gatewayv1.GroupName, Kind: "ListenerSet"}
	_, err := mapper.RESTMapping(kind, gatewayv1.GroupVersion.Version)
	if meta.IsNoMatchError(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("discover ListenerSet v1: %w", err)
	}
	return true, nil
}

// DefaultGatewayParents returns additional default parents in a stable order.
// The caller checks controller ownership and normal listener admission. Complete
// discovery matters when withdrawing a previously selected Gateway.
func DefaultGatewayParents(ctx context.Context, reader client.Reader, namespace string, spec gatewayv1.CommonRouteSpec) ([]gatewayv1.ParentReference, error) {
	if spec.UseDefaultGateways != gatewayv1.GatewayDefaultScopeAll {
		return nil, nil
	}
	var parents []gatewayv1.ParentReference
	for token := ""; ; {
		var page gatewayv1.GatewayList
		if err := reader.List(ctx, &page, client.Continue(token)); err != nil {
			return nil, fmt.Errorf("list default Gateways: %w", err)
		}
		for i := range page.Items {
			if parent, ok := model.DefaultGatewayParent(spec, namespace, &page.Items[i]); ok {
				parents = append(parents, parent)
			}
		}
		if page.Continue == "" {
			break
		}
		if page.Continue == token {
			return nil, fmt.Errorf("default Gateway list returned a repeated continuation token")
		}
		token = page.Continue
	}
	slices.SortFunc(parents, func(a, b gatewayv1.ParentReference) int {
		return cmp.Or(cmp.Compare(*a.Namespace, *b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return parents, nil
}

// IsGatewayParentRef reports whether a ParentReference targets a Gateway.
func IsGatewayParentRef(parent gatewayv1.ParentReference) bool {
	return (parent.Group == nil || *parent.Group == gatewayv1.Group(gatewayv1.GroupName)) &&
		(parent.Kind == nil || *parent.Kind == gatewayv1.Kind("Gateway"))
}

// ParentRefsEqual reports whether two ParentReferences select the same parent
// and section after applying their route namespace defaults.
func ParentRefsEqual(left gatewayv1.ParentReference, leftRouteNamespace string, right gatewayv1.ParentReference, rightRouteNamespace string) bool {
	leftGroup, rightGroup := gatewayv1.Group(gatewayv1.GroupName), gatewayv1.Group(gatewayv1.GroupName)
	if left.Group != nil {
		leftGroup = *left.Group
	}
	if right.Group != nil {
		rightGroup = *right.Group
	}
	leftKind, rightKind := gatewayv1.Kind("Gateway"), gatewayv1.Kind("Gateway")
	if left.Kind != nil {
		leftKind = *left.Kind
	}
	if right.Kind != nil {
		rightKind = *right.Kind
	}
	leftNamespace, rightNamespace := leftRouteNamespace, rightRouteNamespace
	if left.Namespace != nil {
		leftNamespace = string(*left.Namespace)
	}
	if right.Namespace != nil {
		rightNamespace = string(*right.Namespace)
	}
	return leftGroup == rightGroup && leftKind == rightKind && leftNamespace == rightNamespace && left.Name == right.Name &&
		stringPointerEqual(left.SectionName, right.SectionName) && int32PointerEqual(left.Port, right.Port)
}

// SameGatewayTarget reports whether two ParentReferences target the same
// Gateway, without comparing section names.
func SameGatewayTarget(left gatewayv1.ParentReference, leftRouteNamespace string, right gatewayv1.ParentReference, rightRouteNamespace string) bool {
	leftCopy, rightCopy := left, right
	leftCopy.SectionName, leftCopy.Port = nil, nil
	rightCopy.SectionName, rightCopy.Port = nil, nil
	return ParentRefsEqual(leftCopy, leftRouteNamespace, rightCopy, rightRouteNamespace)
}

func stringPointerEqual[T ~string](left, right *T) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func int32PointerEqual(left, right *gatewayv1.PortNumber) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
