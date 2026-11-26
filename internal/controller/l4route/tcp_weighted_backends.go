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
	"crypto/sha256"
	"fmt"

	"k8s.io/utils/ptr"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

func (r *Reconciler) tcpWeightedCandidate(route *transportRoute) bool {
	if !r.tcpXBackendsEnabled() || !r.Config.TCPWeightedDefaultPools || !r.Config.TCPDefaultPoolTLS {
		return false
	}
	positive, hasXBackend := 0, false
	for _, ref := range route.backendRefs {
		if ptr.Deref(ref.Weight, 1) <= 0 {
			continue
		}
		positive++
		_, xBackend := model.XBackendReference(route.GetNamespace(), ref.BackendObjectReference)
		hasXBackend = hasXBackend || xBackend
	}
	return hasXBackend && positive > 1
}

// A missing or unauthorized XBackend has no safe cleartext interpretation.
// Retain its declared share even though its transport can no longer be read.
func (r *Reconciler) needsTCPWeightedPools(route *transportRoute, backends []transportBackend) bool {
	if !r.tcpWeightedCandidate(route) {
		return false
	}
	byPosition := make(map[int]transportBackend, len(backends))
	for _, backend := range backends {
		byPosition[backend.ref.position] = backend
	}
	for index, ref := range route.backendRefs {
		_, xBackend := model.XBackendReference(route.GetNamespace(), ref.BackendObjectReference)
		backend := byPosition[index+1]
		if ptr.Deref(ref.Weight, 1) > 0 && xBackend && (backend.xBackend == nil || backend.backendTLS != nil) {
			return true
		}
	}
	return false
}

func buildTCPWeightedBackends(route *transportRoute, backends []transportBackend, groups []routebackend.WeightedServiceMembers) []cloud.TCPWeightedBackend {
	byPosition := make(map[int]cloud.TCPWeightedBackend, len(backends))
	duplicates := make(map[string]int)
	for index, backend := range backends {
		var kind, uid string
		switch {
		case backend.xBackend != nil:
			kind, uid = "XBackend", string(backend.xBackend.UID)
		case backend.ref.serviceImport:
			kind, uid = "ServiceImport", string(backend.importUID)
		default:
			kind, uid = "Service", string(backend.service.UID)
		}
		ref := fmt.Sprintf("%s/%s/%s/%d", kind, backend.ref.key, uid, ptr.Deref(backend.ref.port, 0))
		duplicates[ref]++
		digest := sha256.Sum256(fmt.Appendf(nil, "%s/%d", ref, duplicates[ref]))
		byPosition[backend.ref.position] = cloud.TCPWeightedBackend{
			Key: fmt.Sprintf("%x", digest[:12]), Members: groups[index].Members,
			BackendTLS: backend.backendTLS, ClientCertificateReference: backend.clientCertificate,
		}
	}
	var targets []cloud.TCPWeightedBackend
	for index, ref := range route.backendRefs {
		if weight := int(ptr.Deref(ref.Weight, 1)); weight > 0 {
			target := byPosition[index+1]
			target.Weight = weight
			targets = append(targets, target)
		}
	}
	return targets
}

func (r *Reconciler) tcpXBackendTLSEligible(route *transportRoute) bool {
	return singletonTCPReference(route) || r.tcpWeightedCandidate(route)
}

func (r *Reconciler) tcpXBackendTLSCapability(route *transportRoute) (func(context.Context) (bool, error), string) {
	if r.tcpWeightedCandidate(route) {
		return r.TCPWeightedDefaultPoolsAvailable, "Waiting for regional Octavia TCP weighted default pool support"
	}
	return r.TCPDefaultPoolTLSAvailable, "Waiting for regional Octavia TCP default pool TLS support"
}
