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

package routebackend

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// ResolveHTTPExternalAuth authorizes the source before reading the Service and
// its transport dependencies. Both listener and route filters use it.
func ResolveHTTPExternalAuth(ctx context.Context, reader client.Reader, kind gatewayv1.Kind, namespace string, filter model.HTTPExternalAuth, topology classconfig.Resolved) (cloud.ListenerExternalAuth, *model.Diagnostic, error) {
	auth := filter.Config
	service, port, diagnostic, err := ObserveHTTPExternalAuthService(ctx, reader, kind, namespace, filter)
	if diagnostic != nil || err != nil {
		return auth, diagnostic, err
	}
	members, diagnostic, err := TransportMembers(ctx, reader, service, filter.BackendRef.Port, corev1.ProtocolTCP, topology, nil, false)
	if err != nil {
		return auth, nil, err
	}
	if diagnostic != nil && diagnostic.Reason != string(gatewayv1.RouteReasonPending) {
		return auth, externalAuthReferenceIssue(gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message), nil
	}
	auth.ServiceUID, auth.ServicePort = string(service.UID), int(port.Port)
	auth.MemberSubnetID = topology.Topology.MemberSubnetID
	auth.BackendSecurityGroups = topology.Topology.BackendSecurityGroupIDs
	auth.Members = slices.DeleteFunc(members, func(member cloud.Member) bool { return member.Draining })
	return auth, nil, nil
}

// ObserveHTTPExternalAuthService resolves the Service and cleartext HTTP port
// without requiring the Gateway's member topology or ready EndpointSlices.
func ObserveHTTPExternalAuthService(ctx context.Context, reader client.Reader, kind gatewayv1.Kind, namespace string, filter model.HTTPExternalAuth) (*corev1.Service, *corev1.ServicePort, *model.Diagnostic, error) {
	key, _ := model.BackendServiceReference(namespace, filter.BackendRef)
	if key.Namespace != namespace {
		grants, err := graph.ListBackendReferenceGrants(ctx, reader, key.Namespace)
		if err != nil {
			return nil, nil, nil, err
		}
		if !model.BackendReferenceAllowedFor(kind, namespace, key, grants) {
			return nil, nil, externalAuthReferenceIssue(gatewayv1.RouteReasonRefNotPermitted, fmt.Sprintf("No ReferenceGrant authorizes this %s to use Service %s", kind, key)), nil
		}
	}
	var service corev1.Service
	if err := reader.Get(ctx, key, &service); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, externalAuthReferenceIssue(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("ExternalAuth backend Service %s was not found", key)), nil
		}
		return nil, nil, nil, err
	}
	if !service.DeletionTimestamp.IsZero() || service.UID == "" {
		return nil, nil, externalAuthReferenceIssue("Invalid", fmt.Sprintf("ExternalAuth backend Service %s has no live identity", key)), nil
	}
	port, diagnostic := model.ResolveServicePort(&service, filter.BackendRef.Port, false)
	if diagnostic != nil {
		return nil, nil, externalAuthReferenceIssue(gatewayv1.RouteConditionReason(diagnostic.Reason), diagnostic.Message), nil
	}
	var policies gatewayv1.BackendTLSPolicyList
	if err := reader.List(ctx, &policies, client.InNamespace(service.Namespace)); err != nil {
		return nil, nil, nil, err
	}
	if policies.Continue != "" {
		return nil, nil, nil, fmt.Errorf("incomplete BackendTLSPolicy inventory for ExternalAuth backend Service %s", key)
	}
	if slices.ContainsFunc(policies.Items, func(policy gatewayv1.BackendTLSPolicy) bool {
		return policy.DeletionTimestamp.IsZero() && backendtls.AppliesToServicePort(&policy, &service, port)
	}) {
		return nil, nil, &model.Diagnostic{Condition: string(gatewayv1.RouteConditionAccepted), Reason: string(gatewayv1.RouteReasonUnsupportedValue),
			Message: "ExternalAuth backend TLS is unsupported", Scope: model.ScopeRule}, nil
	}
	return &service, port, nil, nil
}

func externalAuthReferenceIssue(reason gatewayv1.RouteConditionReason, message string) *model.Diagnostic {
	return &model.Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs), Reason: string(reason), Message: message, Scope: model.ScopeRule}
}
