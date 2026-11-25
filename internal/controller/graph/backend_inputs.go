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
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

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
	return CollectBackendInputsWithNamespaces(ctx, reader, gateway, routes, input.Namespaces)
}

// CollectBackendInputsWithNamespaces reads the already-admitted route dependencies.
func CollectBackendInputsWithNamespaces(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, routes []gatewayv1.HTTPRoute, namespaces []corev1.Namespace) (model.Input, error) {
	input := model.Input{Gateway: gateway.DeepCopy(), Routes: make([]gatewayv1.HTTPRoute, 0, len(routes)), Namespaces: namespaces}
	for i := range routes {
		input.Routes = append(input.Routes, *routes[i].DeepCopy())
	}
	slices.SortFunc(input.Routes, func(a, b gatewayv1.HTTPRoute) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID))
	})
	if err := collectBackendServiceInputs(ctx, reader, &input, true); err != nil {
		return model.Input{}, err
	}
	slices.SortFunc(input.EndpointSlices, func(a, b discoveryv1.EndpointSlice) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return input, nil
}

// CollectBackendServiceInputs observes Services and their ReferenceGrants for
// reference resolution, which does not depend on EndpointSlice availability.
func CollectBackendServiceInputs(ctx context.Context, reader client.Reader, input *model.Input) error {
	return collectBackendServiceInputs(ctx, reader, input, false)
}

func collectBackendServiceInputs(ctx context.Context, reader client.Reader, input *model.Input, includeEndpointSlices bool) error {
	references := backendServiceReferences(input.Routes)
	grantNamespaces := map[string]struct{}{}
	for target, sources := range references {
		for source := range sources {
			if source.namespace != target.Namespace {
				grantNamespaces[target.Namespace] = struct{}{}
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

	keys := slices.SortedFunc(maps.Keys(references), func(a, b types.NamespacedName) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	for _, key := range keys {
		allowed := false
		for source := range references[key] {
			if model.BackendReferenceAllowedFor(source.kind, source.namespace, key, input.ReferenceGrants) {
				allowed = true
				break
			}
		}
		if !allowed {
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
	if !input.EnableXBackend {
		return nil
	}
	keys := map[types.NamespacedName]struct{}{}
	for _, route := range input.Routes {
		if model.HTTPProfileRouteKind(&route) != model.HTTPRouteKind {
			continue
		}
		for _, key := range model.LocalXBackendReferences(&route) {
			keys[key] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	input.XBackends = make([]model.XBackendObservation, 0, len(keys))
	for _, key := range slices.SortedFunc(maps.Keys(keys), func(a, b types.NamespacedName) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}) {
		observation, err := tlsObserver.ObserveXBackendReference(ctx, key, input.XBackendProfile, input.Gateway.Spec.GatewayClassName, certificates)
		if err != nil {
			return err
		}
		if observation == nil {
			continue
		}
		spec := observation.Spec
		if spec.Type == gatewayx.BackendTypeEndpointSelector && model.CheckXBackendObservation(key, observation, input.XBackendProfile) == nil {
			selector := make(client.MatchingLabels, len(spec.EndpointSelector.MatchLabels))
			for label, value := range spec.EndpointSelector.MatchLabels {
				selector[string(label)] = string(value)
			}
			var pods corev1.PodList
			err := reader.List(ctx, &pods, client.InNamespace(key.Namespace), selector)
			switch {
			case err == nil && pods.Continue == "":
				backend := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace}, Spec: spec}
				observation.Endpoints = model.ResolveXBackendEndpoints(backend, pods.Items)
			case model.XBackendRequiresTLS(spec) && ctx.Err() == nil:
				observation.ReadIssue = &model.Diagnostic{Reason: string(gatewayv1.RouteReasonPending), Message: fmt.Sprintf("XBackend %s Pod endpoints could not be read completely", key)}
			case err != nil:
				return fmt.Errorf("list Pods for XBackend %s: %w", key, err)
			default:
				return fmt.Errorf("incomplete Pod list for XBackend %s", key)
			}
		}
		input.XBackends = append(input.XBackends, *observation)
	}
	return nil
}

// XBackendClientCertificateResolver authorizes the operator-enrolled client identity.
type XBackendClientCertificateResolver interface {
	ResolveXBackendClientCertificate(context.Context, gatewayv1.ObjectName, *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string)
}

// ObserveXBackendReference reads an authorized XBackend and its TLS inputs without
// depending on backend members or a provisioned listener.
func (o *BackendTLSObserver) ObserveXBackendReference(ctx context.Context, key types.NamespacedName, profile model.XBackendProfile, class gatewayv1.ObjectName, certificates XBackendClientCertificateResolver) (*model.XBackendObservation, error) {
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
	if model.ValidateXBackend(backend.Spec, profile) == nil && model.XBackendRequiresTLS(backend.Spec) {
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

// ListBackendReferenceGrants reads a Service target namespace before backend access.
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

func backendServiceReferences(routes []gatewayv1.HTTPRoute) map[types.NamespacedName]map[backendReferenceSource]struct{} {
	references := map[types.NamespacedName]map[backendReferenceSource]struct{}{}
	for _, route := range routes {
		for _, rule := range route.Spec.Rules {
			for _, reference := range model.HTTPServiceBackendReferences(rule, true) {
				key, supported := model.BackendServiceReference(route.Namespace, reference)
				if !supported {
					continue
				}
				if references[key] == nil {
					references[key] = map[backendReferenceSource]struct{}{}
				}
				references[key][backendReferenceSource{namespace: route.Namespace, kind: model.HTTPProfileRouteKind(&route)}] = struct{}{}
			}
		}
	}
	return references
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
