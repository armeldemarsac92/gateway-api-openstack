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

package graph

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

// CollectBackendInputs observes the dependencies of the supplied route inventory.
// The caller establishes inventory completeness and mutation authority separately.
// Reads are shared only within this observation, and a failed read returns no partial input.
func CollectBackendInputs(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute) (model.Input, error) {
	input := model.Input{Gateway: gateway, Routes: routes}
	if gateway == nil {
		return model.Input{}, fmt.Errorf("gateway is required to collect backend inputs")
	}
	if err := collectRouteNamespaces(ctx, reader, &input); err != nil {
		return model.Input{}, err
	}
	return CollectBackendInputsWithNamespaces(ctx, reader, gateway, routes, input.Namespaces, false, false)
}

// CollectBackendInputsWithNamespaces reads the already-admitted route dependencies.
func CollectBackendInputsWithNamespaces(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace, enableServiceImport, enableRequestMirror bool) (model.Input, error) {
	input := model.Input{Gateway: gateway.DeepCopy(), Routes: make([]gatewayv1.HTTPRoute, 0, len(routes)), Namespaces: namespaces, EnableServiceImport: enableServiceImport, HTTPRequestMirror: enableRequestMirror}
	for i := range routes {
		input.Routes = append(input.Routes, *routes[i].DeepCopy())
	}
	slices.SortFunc(input.Routes, func(a, b gatewayv1.HTTPRoute) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID))
	})
	if err := collectBackendReferenceInputs(ctx, reader, &input, true); err != nil {
		return model.Input{}, err
	}
	slices.SortFunc(input.EndpointSlices, func(a, b discoveryv1.EndpointSlice) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return input, nil
}

// CollectBackendReferenceInputs observes backend objects and their ReferenceGrants
// for reference resolution. Headless ServiceImport observations also carry
// their MCS-published EndpointSlices for consistency checks.
func CollectBackendReferenceInputs(ctx context.Context, reader client.Reader, input *model.Input) error {
	return collectBackendReferenceInputs(ctx, reader, input, false)
}

func collectBackendReferenceInputs(ctx context.Context, reader client.Reader, input *model.Input, includeEndpointSlices bool) error {
	references := backendReferences(input.Routes, input.EnableServiceImport, input.HTTPRequestMirror)
	grantNamespaces := map[string]struct{}{}
	for target, sources := range references {
		for source := range sources {
			if source.namespace != target.key.Namespace {
				grantNamespaces[target.key.Namespace] = struct{}{}
			}
		}
	}
	for _, namespace := range slices.Sorted(maps.Keys(grantNamespaces)) {
		grants, err := ListBackendReferenceGrants(ctx, reader, namespace)
		if err != nil {
			return err
		}
		input.ReferenceGrants = append(input.ReferenceGrants, grants...)
	}
	slices.SortFunc(input.ReferenceGrants, func(a, b gatewayv1.ReferenceGrant) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})

	targets := slices.SortedFunc(maps.Keys(references), func(a, b backendReferenceTarget) int {
		return cmp.Or(cmp.Compare(a.key.Namespace, b.key.Namespace), cmp.Compare(a.key.Name, b.key.Name), cmp.Compare(a.kind, b.kind))
	})
	for _, target := range targets {
		key := target.key
		authorize := model.BackendReferenceAllowedFor
		if target.kind == model.ServiceImportKind {
			authorize = model.ServiceImportReferenceAllowed
		}
		allowed := false
		for source := range references[target] {
			if authorize(source.kind, source.namespace, key, input.ReferenceGrants) {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		if target.kind == model.ServiceImportKind {
			observation, err := ObserveServiceImport(ctx, reader, key)
			if err != nil {
				return err
			}
			input.ServiceImports = append(input.ServiceImports, observation)
			continue
		}
		var service corev1.Service
		if err := reader.Get(ctx, key, &service); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get backend Service %s: %w", key, err)
		}
		// Keep deleting Services so callers can distinguish pending deletion from absence.
		input.Services = append(input.Services, *service.DeepCopy())
		if !includeEndpointSlices || !service.DeletionTimestamp.IsZero() ||
			service.Spec.Type == corev1.ServiceTypeLoadBalancer || service.Spec.Type == corev1.ServiceTypeExternalName {
			continue
		}
		var endpointSlices discoveryv1.EndpointSliceList
		if err := reader.List(ctx, &endpointSlices, client.InNamespace(key.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: key.Name}); err != nil {
			return fmt.Errorf("list EndpointSlices for Service %s: %w", key, err)
		}
		if endpointSlices.Continue != "" {
			return fmt.Errorf("incomplete EndpointSlice list for Service %s", key)
		}
		input.EndpointSlices = append(input.EndpointSlices, endpointSlices.DeepCopy().Items...)
	}
	return nil
}

// CollectXBackendInputs reads local XBackend references and the matching Pods
// needed to lower them. A foreign reference never reaches the API reader.
func CollectXBackendInputs(ctx context.Context, reader client.Reader, input *model.Input, tlsObserver *BackendTLSObserver, certificates XBackendClientCertificateResolver) error {
	return collectXBackendReferences(ctx, reader, input, tlsObserver, certificates, true)
}

// CollectXBackendReferenceInputs observes primary references without waiting for Pod endpoints.
func CollectXBackendReferenceInputs(ctx context.Context, reader client.Reader, input *model.Input, tlsObserver *BackendTLSObserver, certificates XBackendClientCertificateResolver) error {
	return collectXBackendReferences(ctx, reader, input, tlsObserver, certificates, false)
}

func collectXBackendReferences(ctx context.Context, reader client.Reader, input *model.Input, tlsObserver *BackendTLSObserver, certificates XBackendClientCertificateResolver, includeEndpoints bool) error {
	if !input.EnableXBackend {
		return nil
	}
	profiles := map[types.NamespacedName][]model.XBackendProfile{}
	for i := range input.Routes {
		route := &input.Routes[i]
		kind := model.HTTPProfileRouteKind(route)
		for _, key := range model.LocalXBackendReferences(route) {
			if !slices.ContainsFunc(profiles[key], func(profile model.XBackendProfile) bool { return profile.RouteKind == kind }) {
				profile := input.XBackendProfile
				profile.RouteKind = kind
				profiles[key] = append(profiles[key], profile)
			}
		}
	}
	if len(profiles) == 0 {
		return nil
	}
	input.XBackends = make([]model.XBackendObservation, 0, len(profiles))
	for _, key := range slices.SortedFunc(maps.Keys(profiles), func(a, b types.NamespacedName) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}) {
		observation, err := tlsObserver.ObserveXBackendReference(ctx, key, input.Gateway.Spec.GatewayClassName, certificates, profiles[key]...)
		if err != nil {
			return err
		}
		if observation == nil {
			continue
		}
		spec := observation.Spec
		if includeEndpoints && spec.Type == gatewayx.BackendTypeEndpointSelector && slices.ContainsFunc(profiles[key], func(profile model.XBackendProfile) bool {
			return model.CheckXBackendObservation(key, observation, profile) == nil
		}) {
			backend := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}, Spec: spec}
			observation.Endpoints, err = ObserveXBackendEndpoints(ctx, reader, backend)
			if err != nil {
				if !model.XBackendRequiresTLS(spec) || ctx.Err() != nil {
					return err
				}
				observation.EndpointReadIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("XBackend %s Pod endpoints could not be read completely", key)}
			}
		}
		input.XBackends = append(input.XBackends, *observation)
	}
	return nil
}

// ObserveXBackendEndpoints lists the complete Pod inventory for a validated EndpointSelector.
func ObserveXBackendEndpoints(ctx context.Context, reader client.Reader, backend *gatewayx.XBackend) ([]model.Endpoint, error) {
	selector := make(client.MatchingLabels, len(backend.Spec.EndpointSelector.MatchLabels))
	for label, value := range backend.Spec.EndpointSelector.MatchLabels {
		selector[string(label)] = string(value)
	}
	key := client.ObjectKeyFromObject(backend)
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(key.Namespace), selector); err != nil {
		return nil, fmt.Errorf("list Pods for XBackend %s: %w", key, err)
	}
	if pods.Continue != "" {
		return nil, fmt.Errorf("incomplete Pod list for XBackend %s", key)
	}
	return model.ResolveXBackendEndpoints(backend, pods.Items), nil
}

// XBackendClientCertificateResolver authorizes the operator-enrolled client identity.
type XBackendClientCertificateResolver interface {
	ResolveXBackendClientCertificate(context.Context, gatewayv1.ObjectName, *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string)
}

// ObserveXBackendReference reads an authorized XBackend and its TLS inputs without
// depending on backend members or a provisioned listener.
func (o *BackendTLSObserver) ObserveXBackendReference(ctx context.Context, key types.NamespacedName, class gatewayv1.ObjectName, certificates XBackendClientCertificateResolver, profiles ...model.XBackendProfile) (*model.XBackendObservation, error) {
	backend, err := readXBackend(ctx, o.reader, key)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// The previous transport is no longer authoritative. Keep this
		// pool empty until the live XBackend can be read again.
		return &model.XBackendObservation{Key: key, ReadIssue: &model.Diagnostic{
			Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("XBackend %s could not be read", key),
		}}, nil
	}
	if backend == nil {
		return nil, nil
	}
	observation := &model.XBackendObservation{Key: key, UID: backend.UID, Generation: backend.Generation, Spec: backend.Spec}
	if model.XBackendRequiresTLS(backend.Spec) && slices.ContainsFunc(profiles, func(profile model.XBackendProfile) bool {
		return model.ValidateXBackend(backend.Spec, profile) == nil
	}) {
		err = o.ObserveXBackendTLS(ctx, observation, backend, class, certificates)
	}
	return observation, err
}

// ObserveXBackendTLS reads trust and identity for a validated XBackend TLS spec.
func (o *BackendTLSObserver) ObserveXBackendTLS(ctx context.Context, observation *model.XBackendObservation, backend *gatewayx.XBackend, class gatewayv1.ObjectName, certificates XBackendClientCertificateResolver) error {
	tls, err := o.ObserveValidation(ctx, backend.Namespace, *backend.Spec.TLS.Validation)
	if err != nil {
		return err
	}
	observation.TLS = tls
	if tls.Validation == nil || backend.Spec.TLS.Mode != gatewayx.BackendTLSModeClientAndServer {
		return nil
	}
	if certificates == nil {
		observation.ClientCertificateIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: "The operator XBackend client certificate resolver is unavailable"}
		return nil
	}
	reference, reason, message := certificates.ResolveXBackendClientCertificate(ctx, class, backend)
	if reason != "" {
		observation.ClientCertificateIssue = &model.Diagnostic{Reason: string(reason), Message: message}
	} else if reference == "" {
		observation.ClientCertificateIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: "Waiting to resolve the XBackend client certificate"}
	} else {
		observation.ClientCertificateReference = reference
	}
	return ctx.Err()
}

// readXBackend distinguishes a missing or terminating backend from a failed read.
func readXBackend(ctx context.Context, reader client.Reader, key types.NamespacedName) (*gatewayx.XBackend, error) {
	var backend gatewayx.XBackend
	if err := reader.Get(ctx, key, &backend); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get XBackend %s: %w", key, err)
	}
	if !backend.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return &backend, nil
}

// ListBackendReferenceGrants reads a target namespace before backend access.
func ListBackendReferenceGrants(ctx context.Context, reader client.Reader, namespace string) ([]gatewayv1.ReferenceGrant, error) {
	var grants gatewayv1.ReferenceGrantList
	if err := reader.List(ctx, &grants, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list backend ReferenceGrants in namespace %s: %w", namespace, err)
	}
	if grants.Continue != "" {
		return nil, fmt.Errorf("incomplete backend ReferenceGrant list in namespace %s", namespace)
	}
	return grants.DeepCopy().Items, nil
}

type backendReferenceSource struct {
	namespace string
	kind      gatewayv1.Kind
}

type backendReferenceTarget struct {
	key  types.NamespacedName
	kind gatewayv1.Kind // Only core Service and multicluster.x-k8s.io ServiceImport enter this inventory.
}

func backendReferences(routes []gatewayv1.HTTPRoute, enableServiceImport, enableRequestMirror bool) map[backendReferenceTarget]map[backendReferenceSource]struct{} {
	references := map[backendReferenceTarget]map[backendReferenceSource]struct{}{}
	add := func(target backendReferenceTarget, source backendReferenceSource) {
		if references[target] == nil {
			references[target] = map[backendReferenceSource]struct{}{}
		}
		references[target][source] = struct{}{}
	}
	for _, route := range routes {
		source := backendReferenceSource{namespace: route.Namespace, kind: model.HTTPProfileRouteKind(&route)}
		for _, rule := range route.Spec.Rules {
			for _, reference := range model.HTTPBackendReferences(rule, true, enableRequestMirror) {
				if key, supported := model.BackendServiceReference(route.Namespace, reference); supported {
					add(backendReferenceTarget{key: key, kind: "Service"}, source)
				} else if key, imported := model.ServiceImportReference(route.Namespace, reference); imported && enableServiceImport {
					add(backendReferenceTarget{key: key, kind: model.ServiceImportKind}, source)
				}
			}
		}
	}
	return references
}

// ObserveServiceImport uses a live reader after the caller checks authorization.
// An absent API or a failed read withdraws this destination independently of Services.
func ObserveServiceImport(ctx context.Context, reader client.Reader, key types.NamespacedName) (model.ServiceImportObservation, error) {
	observation := model.ServiceImportObservation{Key: key}
	var backend mcs.ServiceImport
	err := reader.Get(ctx, key, &backend)
	switch {
	case err == nil:
		if backend.DeletionTimestamp.IsZero() && backend.UID != "" {
			observation.UID, observation.Generation, observation.Spec = backend.UID, backend.Generation, &backend.Spec
			observation.ReadyIssue = model.ServiceImportReadiness(&backend)
			if backend.Spec.Type == mcs.Headless && observation.ReadyIssue == nil && backend.Status.EndpointSliceObjects != mcs.EndpointSliceObjectsAbsent {
				observation.SourceClusters = backend.Status.Clusters
				err := observeServiceImportEndpointSlices(ctx, reader, &observation)
				if ctx.Err() != nil {
					return observation, ctx.Err()
				}
				if err != nil {
					observation.RetryRead = true
					observation.ReadIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("Waiting for a complete EndpointSlice inventory for ServiceImport %s", key)}
				}
			}
		}
	case ctx.Err() != nil:
		return observation, ctx.Err()
	case meta.IsNoMatchError(err), apierrors.IsNotFound(err):
		message := fmt.Sprintf("backend ServiceImport %s was not found", key)
		if meta.IsNoMatchError(err) || serviceImportResourceNotFound(err) {
			message = "The multicluster.x-k8s.io/v1beta1 ServiceImport API is unavailable"
		}
		observation.ReadIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonBackendNotFound), Message: message}
	default:
		observation.RetryRead = true
		observation.ReadIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("backend ServiceImport %s could not be read", key)}
	}
	return observation, nil
}

func observeServiceImportEndpointSlices(ctx context.Context, reader client.Reader, observation *model.ServiceImportObservation) error {
	var endpoints discoveryv1.EndpointSliceList
	if err := reader.List(ctx, &endpoints, client.InNamespace(observation.Key.Namespace), client.MatchingLabels{mcs.LabelServiceName: observation.Key.Name}); err != nil {
		return err
	}
	if endpoints.Continue != "" {
		return fmt.Errorf("incomplete EndpointSlice list for ServiceImport %s", observation.Key)
	}
	slices.SortFunc(endpoints.Items, func(a, b discoveryv1.EndpointSlice) int { return cmp.Compare(a.Name, b.Name) })
	observation.EndpointSlices = endpoints.Items
	return nil
}

func serviceImportResourceNotFound(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	return details == nil || details.Name == ""
}

func collectRouteNamespaces(ctx context.Context, reader client.Reader, input *model.Input) error {
	needsNamespaces := slices.ContainsFunc(input.Gateway.Spec.Listeners, func(listener gatewayv1.Listener) bool {
		return model.ObservesRouteNamespace(&listener)
	})
	if !needsNamespaces {
		return nil
	}
	namespaces := map[string]struct{}{}
	for _, route := range input.Routes {
		namespaces[route.Namespace] = struct{}{}
	}
	for _, name := range slices.Sorted(maps.Keys(namespaces)) {
		namespace, err := ReadRouteNamespace(ctx, reader, name)
		if err != nil {
			return err
		}
		if namespace != nil {
			input.Namespaces = append(input.Namespaces, *namespace)
		}
	}
	return nil
}

// ReadRouteNamespace distinguishes a missing Namespace from a failed read.
func ReadRouteNamespace(ctx context.Context, reader client.Reader, name string) (*corev1.Namespace, error) {
	var namespace corev1.Namespace
	if err := reader.Get(ctx, client.ObjectKey{Name: name}, &namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get HTTPRoute Namespace %s: %w", name, err)
	}
	return namespace.DeepCopy(), nil
}
