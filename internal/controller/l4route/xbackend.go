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

package l4route

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/classconfig"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

type xBackendRevision struct {
	uid        types.UID
	generation int64
}

func (r *Reconciler) tcpXBackendsEnabled() bool {
	return r.protocol() == gatewayv1.TCPProtocolType && r.Config.XBackendEnabled()
}

func (r *Reconciler) readTCPXBackend(ctx context.Context, key client.ObjectKey, mode cloud.MemberMode) (*gatewayx.XBackend, *model.Diagnostic, error) {
	backend := &gatewayx.XBackend{}
	err := r.APIReader.Get(ctx, key, backend)
	switch {
	case apierrors.IsNotFound(err) || err == nil && !backend.DeletionTimestamp.IsZero():
		return nil, backendRejection(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("XBackend %s was not found", key)), nil
	case err != nil:
		return nil, nil, fmt.Errorf("get TCP XBackend %s: %w", key, err)
	default:
		return backend, model.ValidateTCPXBackend(backend.Spec, mode), nil
	}
}

func (r *Reconciler) transportMembers(ctx context.Context, backend transportBackend, topology classconfig.Resolved, addresses []gatewayv1.GatewayStatusAddress, matchVIPFamily bool) ([]cloud.Member, *model.Diagnostic, error) {
	if backend.ref.serviceImport {
		if diagnostic := serviceImportModeIssue(topology.Topology.MemberMode); diagnostic != nil {
			return nil, diagnostic, nil
		}
		if len(backend.importMembers) == 0 {
			return nil, &model.Diagnostic{
				Reason: string(gatewayv1.RouteReasonPending), Message: "Waiting for eligible IPv4 endpoints in the backend ServiceImport",
				NoReadyEndpoints: true,
			}, nil
		}
		return backend.importMembers, nil, nil
	}
	if backend.xBackend == nil {
		return routebackend.TransportMembers(ctx, r.APIReader, backend.service, backend.ref.port, corev1.Protocol(r.protocol()), topology, addresses, matchVIPFamily)
	}
	endpoints, err := graph.ObserveXBackendEndpoints(ctx, r.APIReader, backend.xBackend)
	if err != nil {
		return nil, nil, err
	}
	var members []cloud.Member
	for _, endpoint := range endpoints {
		members = append(members, cloud.Member{Address: endpoint.Address, Port: int(endpoint.Port)})
	}
	return members, nil, nil
}
