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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

type backendReference struct {
	key           client.ObjectKey
	port          *gatewayv1.PortNumber
	weight        int32
	xBackend      bool
	serviceImport bool
}

type transportBackend struct {
	ref               backendReference
	service           *corev1.Service
	port              *corev1.ServicePort
	xBackend          *gatewayx.XBackend
	backendTLS        *cloud.BackendTLSValidation
	clientCertificate string
	importUID         types.UID
	importMembers     []cloud.Member
}

type backendRejections struct {
	active          *model.Diagnostic
	inactive        *model.Diagnostic
	rejectWeight    int
	readError       error
	inactivePending bool
	grants          []objectRevision
	services        []objectRevision
	xBackends       []xBackendRevision
	imports         []serviceImportRevision
	importGrants    map[string]serviceImportGrantRevision
	retryImports    bool
	retryTLS        bool
}

func (r *backendRejections) reject(weight int32, diagnostic *model.Diagnostic, collect bool) (blocksForwarding bool) {
	if weight != 0 {
		if r.active == nil {
			r.active = diagnostic
		}
		r.rejectWeight += int(weight)
		return !collect
	}
	if r.inactive == nil {
		r.inactive = diagnostic
	}
	return false
}

func (r *backendRejections) recordReadError(weight int32, err error, collect bool) bool {
	if !collect {
		return true
	}
	if weight == 0 {
		r.inactivePending = true
	} else {
		r.readError = errors.Join(r.readError, err)
	}
	return false
}

func backendRejection(reason gatewayv1.RouteConditionReason, message string) *model.Diagnostic {
	return &model.Diagnostic{Condition: string(gatewayv1.RouteConditionResolvedRefs), Reason: string(reason), Message: message}
}

func (r *Reconciler) authorizedBackends(ctx context.Context, route *transportRoute, includeZeroWeight, collect bool) ([]backendReference, backendRejections, error) {
	if len(route.backendRefs) == 0 {
		return nil, backendRejections{active: backendRejection(gatewayv1.RouteReasonUnsupportedValue, fmt.Sprintf("%s forwarding requires one rule with backendRefs", r.protocol()))}, nil
	}
	type backendKey struct {
		key           client.ObjectKey
		port          gatewayv1.PortNumber
		xBackend      bool
		serviceImport bool
	}
	refs := make(map[backendKey]backendReference, len(route.backendRefs))
	grants := make(map[string][]gatewayv1.ReferenceGrant)
	grantErrors := make(map[string]error)
	var rejections backendRejections
	for _, backend := range route.backendRefs {
		weight := ptr.Deref(backend.Weight, 1)
		if weight == 0 && !includeZeroWeight {
			continue
		}
		if key, supported := model.XBackendReference(route.GetNamespace(), backend.BackendObjectReference); supported && r.tcpXBackendsEnabled() {
			var diagnostic *model.Diagnostic
			switch {
			case key.Namespace != route.GetNamespace():
				diagnostic = backendRejection(gatewayv1.RouteReasonRefNotPermitted, "XBackend must be in the same namespace as its TCPRoute")
			case backend.Port != nil:
				diagnostic = backendRejection(gatewayv1.RouteReasonUnsupportedValue, "backendRef.port must be omitted for XBackend")
			}
			if diagnostic != nil {
				if rejections.reject(weight, diagnostic, collect) {
					return nil, rejections, nil
				}
				continue
			}
			id := backendKey{key: key, xBackend: true}
			refs[id] = backendReference{key: key, xBackend: true, weight: weight + refs[id].weight}
			continue
		}
		key, supported := model.BackendServiceReference(route.GetNamespace(), backend.BackendObjectReference)
		imported := false
		if !supported && r.Config.MulticlusterServiceImports {
			key, imported = model.ServiceImportReference(route.GetNamespace(), backend.BackendObjectReference)
			supported = imported
		}
		if !supported {
			if rejections.reject(weight, backendRejection(gatewayv1.RouteReasonInvalidKind, r.backendKindMessage()), collect) {
				return nil, rejections, nil
			}
			continue
		}
		if key.Namespace != route.GetNamespace() {
			if _, found := grants[key.Namespace]; !found {
				list, err := r.backendGrants(ctx, key.Namespace)
				grantErrors[key.Namespace] = err
				grants[key.Namespace] = list
				// The writer compares the fresh authorization snapshot before
				// applying any graph mutation, even if replacement grants agree.
				if err == nil && !includeZeroWeight {
					rejections.grants = append(rejections.grants, grantRevisions(list)...)
				}
			}
			list, err := grants[key.Namespace], grantErrors[key.Namespace]
			if imported && includeZeroWeight {
				if rejections.importGrants == nil {
					rejections.importGrants = make(map[string]serviceImportGrantRevision)
				}
				rejections.importGrants[key.Namespace] = serviceImportGrantRevision{grants: grantRevisions(list), failed: err != nil}
			}
			allowed := model.BackendReferenceAllowedFor
			kind := "Service"
			if imported {
				allowed, kind = model.ServiceImportReferenceAllowed, "ServiceImport"
			}
			var diagnostic *model.Diagnostic
			switch {
			case err != nil && imported:
				if ctx.Err() != nil {
					return nil, rejections, ctx.Err()
				}
				rejections.retryImports = true
				diagnostic = backendRejection(gatewayv1.RouteReasonPending, "Waiting for live ReferenceGrants for the backend ServiceImport")
			case err != nil:
				if rejections.recordReadError(weight, err, collect) {
					return nil, rejections, err
				}
				continue
			case !allowed(r.kind(), route.GetNamespace(), key, list):
				diagnostic = backendRejection(gatewayv1.RouteReasonRefNotPermitted, fmt.Sprintf("no ReferenceGrant permits this %s to use its backend %s", r.kind(), kind))
			}
			if diagnostic != nil {
				if rejections.reject(weight, diagnostic, collect) {
					return nil, rejections, nil
				}
				continue
			}
		}
		id := backendKey{key: key, port: ptr.Deref(backend.Port, 0), serviceImport: imported}
		refs[id] = backendReference{key: key, port: backend.Port, weight: weight + refs[id].weight, serviceImport: imported}
	}
	ordered := make([]backendReference, 0, len(refs))
	for _, ref := range refs {
		ordered = append(ordered, ref)
	}
	slices.SortFunc(ordered, func(left, right backendReference) int {
		byKey := cmp.Or(cmp.Compare(left.key.Namespace, right.key.Namespace), cmp.Compare(left.key.Name, right.key.Name))
		if byKey != 0 {
			return byKey
		}
		if left.serviceImport != right.serviceImport {
			if left.serviceImport {
				return 1
			}
			return -1
		}
		if left.xBackend != right.xBackend {
			if left.xBackend {
				return 1
			}
			return -1
		}
		return cmp.Compare(ptr.Deref(left.port, 0), ptr.Deref(right.port, 0))
	})
	return ordered, rejections, nil
}

// resolveBackends validates authorized references and returns the backends that carry traffic.
func (r *Reconciler) resolveBackends(ctx context.Context, refs []backendReference, inactiveMode, activeMode cloud.MemberMode, profile model.XBackendProfile, class gatewayv1.ObjectName, singleton, collect bool) ([]transportBackend, backendRejections, error) {
	services := make(map[client.ObjectKey]*corev1.Service, len(refs))
	readErrors := make(map[client.ObjectKey]error)
	imports := make(map[client.ObjectKey]serviceImportRevision)
	activeImports := make(map[client.ObjectKey]bool)
	for _, ref := range refs {
		if ref.serviceImport && ref.weight != 0 {
			activeImports[ref.key] = true
		}
	}
	backends := make([]transportBackend, 0, len(refs))
	var rejections backendRejections
	for _, ref := range refs {
		if ref.xBackend {
			backend, diagnostic, err := r.readTCPXBackend(ctx, ref.key, profile)
			if err != nil && r.Config.TCPDefaultPoolTLS && ref.weight > 0 && ctx.Err() == nil {
				// Without a live XBackend, its previous transport and identity
				// cannot authorize any traffic.
				diagnostic = backendRejection(gatewayv1.RouteReasonPending, "Waiting to read the backend XBackend")
				rejections.retryTLS = true
				err = nil
			}
			if err != nil {
				if rejections.recordReadError(ref.weight, err, collect) {
					return nil, rejections, err
				}
				continue
			}
			if backend != nil && ref.weight != 0 {
				rejections.xBackends = append(rejections.xBackends, xBackendRevision{uid: backend.UID, generation: backend.Generation})
			}
			var tls *cloud.BackendTLSValidation
			var certificate string
			if diagnostic == nil && model.XBackendRequiresTLS(backend.Spec) {
				rejections.retryTLS = rejections.retryTLS || singleton && backend.Spec.TLS.Mode == gatewayx.BackendTLSModeClientAndServer
				tls, certificate, diagnostic, err = r.observeTCPXBackendTLS(ctx, backend, class, singleton || ref.weight == 0)
				if err != nil {
					return nil, rejections, err
				}
			}
			if diagnostic != nil {
				if rejections.reject(ref.weight, diagnostic, collect) {
					return nil, rejections, nil
				}
				continue
			}
			if ref.weight != 0 {
				backends = append(backends, transportBackend{ref: ref, xBackend: backend, backendTLS: tls, clientCertificate: certificate})
			}
			continue
		}
		if ref.serviceImport {
			observation, read := imports[ref.key]
			if !read {
				var err error
				observation, err = readServiceImport(ctx, r.APIReader, ref.key, activeImports[ref.key])
				if err != nil {
					return nil, rejections, err
				}
				imports[ref.key] = observation
				rejections.imports = append(rejections.imports, observation)
				rejections.retryImports = rejections.retryImports || observation.RetryRead
			}
			port, destinations, diagnostic := observation.Destinations(ref.port, r.kind(), false)
			if diagnostic == nil {
				diagnostic = serviceImportModeIssue(profile.MemberMode)
			}
			if diagnostic != nil {
				if rejections.reject(ref.weight, diagnostic, collect) {
					return nil, rejections, nil
				}
			} else if ref.weight != 0 {
				backends = append(backends, transportBackend{
					ref: ref, port: port, importUID: observation.UID,
					importMembers: routebackend.ServiceImportMembers(destinations),
				})
			}
			continue
		}
		service, read := services[ref.key]
		if !read {
			service = &corev1.Service{}
			err := r.APIReader.Get(ctx, ref.key, service)
			readErrors[ref.key] = err
			services[ref.key] = service
		}
		err := readErrors[ref.key]
		if (err == nil && !service.DeletionTimestamp.IsZero()) || apierrors.IsNotFound(err) {
			if rejections.reject(ref.weight, backendRejection(gatewayv1.RouteReasonBackendNotFound, fmt.Sprintf("backend Service %s was not found", ref.key)), collect) {
				return nil, rejections, nil
			}
			continue
		}
		if err != nil {
			if rejections.recordReadError(ref.weight, err, collect) {
				return nil, rejections, err
			}
			continue
		}
		if ref.weight != 0 {
			rejections.services = append(rejections.services, revisionOf(service))
		}
		port, diagnostic := model.ResolveTransportServicePort(service, ref.port, corev1.Protocol(r.protocol()))
		if diagnostic == nil {
			mode := inactiveMode
			if ref.weight != 0 {
				mode = activeMode
			}
			switch mode {
			case cloud.MemberModeEndpoint:
				diagnostic = routebackend.ValidateEndpointService(service)
			case cloud.MemberModeNodePort:
				diagnostic = routebackend.ValidateNodePortService(service, port)
			}
		}
		if diagnostic != nil {
			if rejections.reject(ref.weight, diagnostic, collect) {
				return nil, rejections, nil
			}
			continue
		}
		if ref.weight != 0 {
			backends = append(backends, transportBackend{ref: ref, service: service, port: port})
		}
	}
	return backends, rejections, nil
}

func (r *Reconciler) backendKindMessage() string {
	message := fmt.Sprintf("the %s backendRef must refer to a core Service", r.kind())
	if r.Config.MulticlusterServiceImports {
		message += " or a ServiceImport"
	}
	if r.tcpXBackendsEnabled() {
		message += " or an Experimental XBackend"
	}
	return message
}
