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
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

type backendReference struct {
	key      client.ObjectKey
	port     *gatewayv1.PortNumber
	weight   int32
	xBackend bool
}

type transportBackend struct {
	ref      backendReference
	service  *corev1.Service
	port     *corev1.ServicePort
	xBackend *gatewayx.XBackend
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
		key      client.ObjectKey
		port     gatewayv1.PortNumber
		xBackend bool
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
		if !supported {
			if rejections.reject(weight, backendRejection(gatewayv1.RouteReasonInvalidKind, r.backendKindMessage()), collect) {
				return nil, rejections, nil
			}
			continue
		}
		if key.Namespace != route.GetNamespace() {
			if _, found := grants[key.Namespace]; !found {
				var list gatewayv1.ReferenceGrantList
				err := r.APIReader.List(ctx, &list, client.InNamespace(key.Namespace))
				if err == nil && list.Continue != "" {
					err = fmt.Errorf("incomplete %s ReferenceGrant list", r.kind())
				}
				grantErrors[key.Namespace] = err
				grants[key.Namespace] = list.Items
				// The writer compares the fresh authorization snapshot before
				// applying any graph mutation, even if replacement grants agree.
				if err == nil && !includeZeroWeight {
					slices.SortFunc(list.Items, func(left, right gatewayv1.ReferenceGrant) int { return cmp.Compare(left.Name, right.Name) })
					for i := range list.Items {
						rejections.grants = append(rejections.grants, revisionOf(&list.Items[i]))
					}
				}
			}
			if err := grantErrors[key.Namespace]; err != nil {
				if rejections.recordReadError(weight, err, collect) {
					return nil, rejections, err
				}
				continue
			}
			if !model.BackendReferenceAllowedFor(r.kind(), route.GetNamespace(), key, grants[key.Namespace]) {
				if rejections.reject(weight, backendRejection(gatewayv1.RouteReasonRefNotPermitted, fmt.Sprintf("no ReferenceGrant permits this %s to use its backend Service", r.kind())), collect) {
					return nil, rejections, nil
				}
				continue
			}
		}
		id := backendKey{key: key, port: ptr.Deref(backend.Port, 0)}
		refs[id] = backendReference{key: key, port: backend.Port, weight: weight + refs[id].weight}
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
func (r *Reconciler) resolveBackends(ctx context.Context, refs []backendReference, inactiveMode, activeMode, xBackendMode cloud.MemberMode, collect bool) ([]transportBackend, backendRejections, error) {
	services := make(map[client.ObjectKey]*corev1.Service, len(refs))
	readErrors := make(map[client.ObjectKey]error)
	backends := make([]transportBackend, 0, len(refs))
	var rejections backendRejections
	for _, ref := range refs {
		if ref.xBackend {
			backend, diagnostic, err := r.readTCPXBackend(ctx, ref.key, xBackendMode)
			if err != nil {
				if rejections.recordReadError(ref.weight, err, collect) {
					return nil, rejections, err
				}
				continue
			}
			if backend != nil && ref.weight != 0 {
				rejections.xBackends = append(rejections.xBackends, xBackendRevision{uid: backend.UID, generation: backend.Generation})
			}
			if diagnostic != nil {
				if rejections.reject(ref.weight, diagnostic, collect) {
					return nil, rejections, nil
				}
				continue
			}
			if ref.weight != 0 {
				backends = append(backends, transportBackend{ref: ref, xBackend: backend})
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
	if r.tcpXBackendsEnabled() {
		return "the TCPRoute backendRef must refer to a core Service or an Experimental XBackend"
	}
	return fmt.Sprintf("the %s backendRef must refer to a core Service", r.kind())
}
