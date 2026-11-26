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
	"context"
	"fmt"
	"slices"
	"sort"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
)

const (
	// IndexGatewayClassByController indexes GatewayClasses by controller name.
	IndexGatewayClassByController = "spec.controllerName"
	// IndexGatewayClassByParameters indexes exact supported class parameter references.
	IndexGatewayClassByParameters = "spec.openStackParameters"
	// IndexGatewayByClass indexes Gateways by GatewayClass name.
	IndexGatewayByClass = "spec.gatewayClassName"
	// IndexGatewayByDefaultScope finds Gateways that opt into implicit attachment.
	IndexGatewayByDefaultScope = "spec.defaultScope"
	// IndexRouteByDefaultGateways finds Routes that request the matching scope.
	IndexRouteByDefaultGateways = "spec.useDefaultGateways"
	// IndexRouteAtParentStatusLimit finds Routes that can omit an admitted parent.
	IndexRouteAtParentStatusLimit = "status.parentLimitReached"
	// IndexHTTPRouteByParentGateway indexes HTTPRoutes by parent Gateway key.
	IndexHTTPRouteByParentGateway = "spec.parentGateway"
	// IndexHTTPRouteByListenerSet indexes HTTPRoutes by their declared ListenerSet parents.
	IndexHTTPRouteByListenerSet = "spec.parentListenerSet"
	IndexListenerSetByGateway   = "spec.listenerSetParentGateway"
	// IndexHTTPRouteByStatusGateway indexes HTTPRoutes by status parent Gateway key.
	IndexHTTPRouteByStatusGateway = "status.parentGateway"
	// IndexHTTPRouteByBackendService indexes HTTPRoutes by backend Service key.
	IndexHTTPRouteByBackendService         = "spec.backendService"
	IndexHTTPRouteByServiceImport          = "spec.backendServiceImport"
	IndexHTTPRouteByServiceImportNamespace = "spec.backendServiceImportNamespace"
	ServiceImportIndexAll                  = "*"
	// IndexHTTPRouteByXBackend indexes same-namespace XBackend reference targets on HTTPRoute and GRPCRoute.
	IndexHTTPRouteByXBackend = "spec.xBackend"
	// IndexTCPRouteByXBackend indexes same-namespace TCPRoute XBackend reference targets.
	IndexTCPRouteByXBackend = "spec.tcpXBackend"
	// IndexXBackendByPodLabel indexes each matchLabels pair in an EndpointSelector.
	IndexXBackendByPodLabel = "spec.podLabel"
	// IndexXBackendByCA indexes the scoped trust reference in inline TLS validation.
	IndexXBackendByCA = "spec.tls.caCertificateRef"
	// IndexBackendTLSPolicyByService indexes local core Service targets.
	IndexBackendTLSPolicyByService = "spec.targetService"
	// IndexBackendTLSPolicyByCA indexes supported CA references by scoped object key.
	IndexBackendTLSPolicyByCA = "spec.caConfigMap"
	// IndexHTTPRouteByBackendNamespace indexes cross-namespace Service references.
	IndexHTTPRouteByBackendNamespace = "spec.backendNamespace"
	// IndexHTTPRouteByBoundGateway indexes HTTPRoutes by bound Gateway key.
	IndexHTTPRouteByBoundGateway = "metadata.boundGateway"
	// indexHTTPRouteByBoundGatewayUID indexes HTTPRoutes by bound Gateway UID.
	indexHTTPRouteByBoundGatewayUID = "metadata.boundGatewayUID"
	// IndexHTTPRouteByNodeBackend indexes routes that use Node backends.
	IndexHTTPRouteByNodeBackend = "spec.nodeBackend"
	// IndexL4RouteByParentGateway indexes TCPRoutes and UDPRoutes by parent Gateway key.
	IndexL4RouteByParentGateway = "spec.l4RouteParentGateway"
	// IndexL4RouteByStatusGateway indexes TCPRoutes and UDPRoutes by status parent Gateway key.
	IndexL4RouteByStatusGateway = "status.l4RouteParentGateway"
	// IndexL4RouteByBackendService indexes TCPRoutes and UDPRoutes by backend Service key.
	IndexL4RouteByBackendService = "spec.l4RouteBackendService"
	// IndexL4RouteByBackendNamespace indexes cross-namespace TCPRoute and UDPRoute Service references.
	IndexL4RouteByBackendNamespace = "spec.l4RouteBackendNamespace"
	// IndexL4RouteByBoundGateway indexes TCPRoutes and UDPRoutes by bound Gateway key.
	IndexL4RouteByBoundGateway = "metadata.l4RouteBoundGateway"
	// IndexTLSRouteByGateway indexes declared and controller-owned status parents.
	IndexTLSRouteByGateway = "tlsRouteGateway"
	// IndexTLSRouteByServiceImport indexes primary TLSRoute import references.
	IndexTLSRouteByServiceImport = "spec.tlsRouteServiceImport"
	// IndexEndpointSliceByService indexes EndpointSlices by Service key.
	IndexEndpointSliceByService = "metadata.service"
	// NodeBackendIndexValue is the field value for routes that use Node backends.
	NodeBackendIndexValue = "true"
)

type controllerFieldIndex struct {
	object  client.Object
	field   string
	extract client.IndexerFunc
}

// SetupIndexes registers the field indexes shared by the Gateway API
// reconcilers and their watch mappers.
func SetupIndexes(ctx context.Context, indexer client.FieldIndexer, config Config) error {
	for _, index := range controllerFieldIndexes(config) {
		if err := indexer.IndexField(ctx, index.object, index.field, index.extract); err != nil {
			return fmt.Errorf("register %q field index for %T: %w", index.field, index.object, err)
		}
	}
	return nil
}

func controllerFieldIndexes(config Config) []controllerFieldIndex {
	indexes := []controllerFieldIndex{
		{object: &gatewayv1.GatewayClass{}, field: IndexGatewayClassByParameters, extract: gatewayClassParametersKeys},
		{
			object: &gatewayv1.GatewayClass{},
			field:  IndexGatewayClassByController,
			extract: func(object client.Object) []string {
				gatewayClass, ok := object.(*gatewayv1.GatewayClass)
				if !ok || gatewayClass.Spec.ControllerName == "" {
					return nil
				}
				return []string{string(gatewayClass.Spec.ControllerName)}
			},
		},
		{
			object: &gatewayv1.Gateway{},
			field:  IndexGatewayByClass,
			extract: func(object client.Object) []string {
				gateway, ok := object.(*gatewayv1.Gateway)
				if !ok || gateway.Spec.GatewayClassName == "" {
					return nil
				}
				return []string{string(gateway.Spec.GatewayClassName)}
			},
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  IndexHTTPRouteByParentGateway,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				return ParentGatewayKeys(route)
			},
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  IndexHTTPRouteByStatusGateway,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				return statusParentGatewayKeys(config, route)
			},
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  IndexHTTPRouteByBackendService,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				return backendServiceKeys(route)
			},
		},
		{
			object:  &gatewayv1.HTTPRoute{},
			field:   IndexHTTPRouteByBackendNamespace,
			extract: backendNamespaceKeys,
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  IndexHTTPRouteByBoundGateway,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				return boundGatewayKeys(config, route)
			},
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  indexHTTPRouteByBoundGatewayUID,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				_, uids := RouteBoundGatewayKeys(config, route)
				return uids
			},
		},
		{
			object: &gatewayv1.HTTPRoute{},
			field:  IndexHTTPRouteByNodeBackend,
			extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok || !hasCoreServiceBackend(route) {
					return nil
				}
				return []string{NodeBackendIndexValue}
			},
		},
		{
			object: &discoveryv1.EndpointSlice{},
			field:  IndexEndpointSliceByService,
			extract: func(object client.Object) []string {
				endpointSlice, ok := object.(*discoveryv1.EndpointSlice)
				if !ok {
					return nil
				}
				serviceName := endpointSlice.Labels[discoveryv1.LabelServiceName]
				if endpointSlice.Namespace == "" || serviceName == "" {
					return nil
				}
				return []string{ObjectKeyString(types.NamespacedName{
					Namespace: endpointSlice.Namespace,
					Name:      serviceName,
				})}
			},
		},
	}
	indexes = append(indexes, controllerFieldIndex{object: &gatewayv1.HTTPRoute{}, field: IndexHTTPRouteByListenerSet, extract: func(object client.Object) []string {
		refs, _, _ := RouteParents(object)
		var keys []string
		for _, ref := range refs {
			if key := model.ListenerSetRefKey(ref, object.GetNamespace()); key.Name != "" {
				keys = append(keys, key.String())
			}
		}
		return keys
	}})
	if config.MulticlusterServiceImports {
		indexes = append(indexes,
			controllerFieldIndex{object: &gatewayv1.HTTPRoute{}, field: IndexHTTPRouteByServiceImport, extract: func(object client.Object) []string {
				return serviceImportBackendKeys(object.(*gatewayv1.HTTPRoute), false, config.HTTPRequestMirror)
			}},
			controllerFieldIndex{object: &gatewayv1.HTTPRoute{}, field: IndexHTTPRouteByServiceImportNamespace, extract: func(object client.Object) []string {
				return serviceImportBackendKeys(object.(*gatewayv1.HTTPRoute), true, config.HTTPRequestMirror)
			}},
		)
	}
	// The field names are scoped to the Kubernetes kind by the cache.
	for _, index := range indexes {
		if _, ok := index.object.(*gatewayv1.HTTPRoute); !ok {
			continue
		}
		indexes = append(indexes, controllerFieldIndex{object: &gatewayv1.GRPCRoute{}, field: index.field, extract: func(object client.Object) []string {
			route, ok := object.(*gatewayv1.GRPCRoute)
			if !ok {
				return nil
			}
			return index.extract(model.ProjectGRPCRoute(route))
		}})
	}
	for _, route := range []client.Object{&gatewayv1.TCPRoute{}, &gatewayv1.UDPRoute{}} {
		indexes = append(indexes,
			controllerFieldIndex{object: route, field: IndexL4RouteByParentGateway, extract: func(object client.Object) []string {
				parents, _, _ := RouteParents(object)
				return parentGatewayKeys(object.GetNamespace(), parents)
			}},
			controllerFieldIndex{object: route, field: IndexL4RouteByStatusGateway, extract: func(object client.Object) []string {
				_, statuses, _ := RouteParents(object)
				return statusGatewayKeys(config, object.GetNamespace(), statuses)
			}},
			controllerFieldIndex{object: route, field: IndexL4RouteByBackendService, extract: func(object client.Object) []string {
				return l4BackendKeys(object, false)
			}},
			controllerFieldIndex{object: route, field: IndexL4RouteByBackendNamespace, extract: func(object client.Object) []string {
				return l4BackendKeys(object, true)
			}},
			controllerFieldIndex{object: route, field: IndexL4RouteByBoundGateway, extract: func(object client.Object) []string {
				return boundGatewayKeys(config, object)
			}},
		)
	}
	if config.BackendTLSPolicy {
		indexes = append(indexes,
			controllerFieldIndex{object: &gatewayv1.BackendTLSPolicy{}, field: IndexBackendTLSPolicyByService, extract: func(object client.Object) []string {
				var keys []string
				for _, key := range backendtls.ServiceKeys(object.(*gatewayv1.BackendTLSPolicy)) {
					keys = append(keys, ObjectKeyString(key))
				}
				return keys
			}},
			controllerFieldIndex{object: &gatewayv1.BackendTLSPolicy{}, field: IndexBackendTLSPolicyByCA, extract: func(object client.Object) []string {
				if key, supported := backendtls.CAKey(object.(*gatewayv1.BackendTLSPolicy), config.ClusterTrustBundles != nil); supported {
					return []string{ObjectKeyString(key)}
				}
				return nil
			}},
		)
	}
	if config.XBackendEnabled() {
		indexes = append(indexes,
			controllerFieldIndex{object: &gatewayv1.HTTPRoute{}, field: IndexHTTPRouteByXBackend, extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.HTTPRoute)
				if !ok {
					return nil
				}
				keys := make(map[string]struct{})
				for _, key := range model.LocalXBackendReferences(route) {
					keys[ObjectKeyString(key)] = struct{}{}
				}
				return sortedIndexKeys(keys)
			}},
			controllerFieldIndex{object: &gatewayv1.GRPCRoute{}, field: IndexHTTPRouteByXBackend, extract: func(object client.Object) []string {
				route := model.ProjectGRPCRoute(object.(*gatewayv1.GRPCRoute))
				keys := make(map[string]struct{})
				for _, key := range model.LocalXBackendReferences(route) {
					keys[ObjectKeyString(key)] = struct{}{}
				}
				return sortedIndexKeys(keys)
			}},
			controllerFieldIndex{object: &gatewayv1.TCPRoute{}, field: IndexTCPRouteByXBackend, extract: func(object client.Object) []string {
				route, ok := object.(*gatewayv1.TCPRoute)
				if !ok {
					return nil
				}
				keys := make(map[string]struct{})
				for _, key := range model.LocalTCPXBackendReferences(route) {
					keys[ObjectKeyString(key)] = struct{}{}
				}
				return sortedIndexKeys(keys)
			}},
			controllerFieldIndex{object: &gatewayx.XBackend{}, field: IndexXBackendByPodLabel, extract: func(object client.Object) []string {
				backend, ok := object.(*gatewayx.XBackend)
				if !ok || backend.Spec.Type != gatewayx.BackendTypeEndpointSelector || backend.Spec.EndpointSelector == nil {
					return nil
				}
				keys := make(map[string]struct{})
				for key, value := range backend.Spec.EndpointSelector.MatchLabels {
					keys[PodLabelIndexKey(string(key), string(value))] = struct{}{}
				}
				return sortedIndexKeys(keys)
			}},
		)
		if config.BackendTLSPolicy {
			indexes = append(indexes, controllerFieldIndex{object: &gatewayx.XBackend{}, field: IndexXBackendByCA, extract: func(object client.Object) []string {
				if key, supported := model.XBackendTLSCAKey(object.(*gatewayx.XBackend), config.ClusterTrustBundles != nil); supported {
					return []string{ObjectKeyString(key)}
				}
				return nil
			}})
		}
	}
	if config.TLSRoutesEnabled() {
		indexes = append(indexes, controllerFieldIndex{object: &gatewayv1.TLSRoute{}, field: IndexTLSRouteByGateway, extract: func(object client.Object) []string {
			parents, statuses, _ := RouteParents(object)
			keys := append(parentGatewayKeys(object.GetNamespace(), parents), statusGatewayKeys(config, object.GetNamespace(), statuses)...)
			slices.Sort(keys)
			return slices.Compact(keys)
		}})
	}
	if config.TLSRoutesEnabled() && config.MulticlusterServiceImports {
		indexes = append(indexes, controllerFieldIndex{object: &gatewayv1.TLSRoute{}, field: IndexTLSRouteByServiceImport, extract: tlsServiceImportBackendKeys})
	}
	if config.DefaultGatewaysEnabled() {
		indexes = append(indexes, controllerFieldIndex{object: &gatewayv1.Gateway{}, field: IndexGatewayByDefaultScope, extract: func(object client.Object) []string {
			gateway := object.(*gatewayv1.Gateway)
			if gateway.Spec.DefaultScope == gatewayv1.GatewayDefaultScopeAll && gateway.DeletionTimestamp.IsZero() {
				return []string{string(gateway.Spec.DefaultScope)}
			}
			return nil
		}})
		kinds := []client.Object{&gatewayv1.HTTPRoute{}, &gatewayv1.GRPCRoute{}, &gatewayv1.TCPRoute{}, &gatewayv1.UDPRoute{}}
		if config.TLSRoutesEnabled() {
			kinds = append(kinds, &gatewayv1.TLSRoute{})
		}
		for _, kind := range kinds {
			indexes = append(indexes, controllerFieldIndex{object: kind, field: IndexRouteByDefaultGateways, extract: func(object client.Object) []string {
				spec, _ := RouteSpecAndParentStatuses(object)
				if spec != nil && spec.UseDefaultGateways == gatewayv1.GatewayDefaultScopeAll {
					return []string{string(spec.UseDefaultGateways)}
				}
				return nil
			}}, controllerFieldIndex{object: kind, field: IndexRouteAtParentStatusLimit, extract: func(object client.Object) []string {
				_, parents := RouteSpecAndParentStatuses(object)
				if len(parents) >= RouteParentStatusLimit {
					return []string{"true"}
				}
				return nil
			}})
		}
	}
	return indexes
}

// PodLabelIndexKey encodes one Kubernetes label selector equality.
func PodLabelIndexKey(key, value string) string {
	return key + "=" + value
}

// RouteSpecAndParentStatuses returns the common attachment fields of a supported Route.
func RouteSpecAndParentStatuses(object client.Object) (*gatewayv1.CommonRouteSpec, []gatewayv1.RouteParentStatus) {
	switch route := object.(type) {
	case *gatewayv1.HTTPRoute:
		return &route.Spec.CommonRouteSpec, route.Status.Parents
	case *gatewayv1.GRPCRoute:
		return &route.Spec.CommonRouteSpec, route.Status.Parents
	case *gatewayv1.TCPRoute:
		return &route.Spec.CommonRouteSpec, route.Status.Parents
	case *gatewayv1.UDPRoute:
		return &route.Spec.CommonRouteSpec, route.Status.Parents
	case *gatewayv1.TLSRoute:
		return &route.Spec.CommonRouteSpec, route.Status.Parents
	default:
		return nil, nil
	}
}

// RouteParents returns the declared and reported parents of a supported Route.
func RouteParents(object client.Object) ([]gatewayv1.ParentReference, []gatewayv1.RouteParentStatus, bool) {
	spec, statuses := RouteSpecAndParentStatuses(object)
	if spec == nil {
		return nil, nil, false
	}
	return spec.ParentRefs, statuses, true
}

// ObjectKeyString returns the stable namespace and name representation used by
// field indexes.
func ObjectKeyString(key types.NamespacedName) string {
	return key.String()
}

// ParentGatewayKeys returns the distinct Gateway keys referenced by an
// HTTPRoute, in deterministic order.
func ParentGatewayKeys(route *gatewayv1.HTTPRoute) []string {
	return parentGatewayKeys(route.Namespace, route.Spec.ParentRefs)
}

func parentGatewayKeys(routeNamespace string, parents []gatewayv1.ParentReference) []string {
	keys := make(map[string]struct{}, len(parents))
	for _, parent := range parents {
		if !IsGatewayParentRef(parent) || parent.Name == "" {
			continue
		}
		namespace := routeNamespace
		if parent.Namespace != nil {
			namespace = string(*parent.Namespace)
		}
		if namespace == "" {
			continue
		}
		keys[ObjectKeyString(types.NamespacedName{
			Namespace: namespace,
			Name:      string(parent.Name),
		})] = struct{}{}
	}
	return sortedIndexKeys(keys)
}

func statusParentGatewayKeys(config Config, route *gatewayv1.HTTPRoute) []string {
	return statusGatewayKeys(config, route.Namespace, route.Status.Parents)
}

func statusGatewayKeys(config Config, routeNamespace string, parents []gatewayv1.RouteParentStatus) []string {
	keys := make(map[string]struct{}, len(parents))
	for _, parent := range parents {
		if parent.ControllerName != config.ControllerName || !IsGatewayParentRef(parent.ParentRef) || parent.ParentRef.Name == "" {
			continue
		}
		namespace := routeNamespace
		if parent.ParentRef.Namespace != nil {
			namespace = string(*parent.ParentRef.Namespace)
		}
		if namespace == "" {
			continue
		}
		keys[ObjectKeyString(types.NamespacedName{
			Namespace: namespace,
			Name:      string(parent.ParentRef.Name),
		})] = struct{}{}
	}
	return sortedIndexKeys(keys)
}

func backendServiceKeys(route *gatewayv1.HTTPRoute) []string {
	keys := make(map[string]struct{})
	for _, rule := range route.Spec.Rules {
		for _, backend := range append(model.HTTPBackendReferences(rule, true, true), model.HTTPRouteExternalAuthReferences(rule)...) {
			key, supported := model.BackendServiceReference(route.Namespace, backend)
			if supported && key.Namespace != "" && key.Name != "" {
				keys[ObjectKeyString(key)] = struct{}{}
			}
		}
	}
	return sortedIndexKeys(keys)
}

func serviceImportBackendKeys(route *gatewayv1.HTTPRoute, namespacesOnly, includeMirrors bool) []string {
	keys := make(map[string]struct{})
	for _, rule := range route.Spec.Rules {
		for _, backend := range model.HTTPBackendReferences(rule, true, includeMirrors) {
			key, imported := model.ServiceImportReference(route.Namespace, backend)
			if !imported || key.Namespace == "" || key.Name == "" {
				continue
			}
			if namespacesOnly {
				if key.Namespace != route.Namespace {
					keys[key.Namespace] = struct{}{}
				}
			} else {
				keys[ServiceImportIndexAll] = struct{}{}
				keys[ObjectKeyString(key)] = struct{}{}
			}
		}
	}
	return sortedIndexKeys(keys)
}

func tlsServiceImportBackendKeys(object client.Object) []string {
	route := object.(*gatewayv1.TLSRoute)
	keys := make(map[string]struct{})
	for _, rule := range route.Spec.Rules {
		for _, backend := range rule.BackendRefs {
			if key, imported := model.ServiceImportReference(route.Namespace, backend.BackendObjectReference); imported && key.Namespace != "" && key.Name != "" {
				keys[ServiceImportIndexAll] = struct{}{}
				keys[ObjectKeyString(key)] = struct{}{}
			}
		}
	}
	return sortedIndexKeys(keys)
}

func backendNamespaceKeys(object client.Object) []string {
	route, ok := object.(*gatewayv1.HTTPRoute)
	if !ok {
		return nil
	}
	keys := make(map[string]struct{})
	for _, rule := range route.Spec.Rules {
		for _, backend := range append(model.HTTPBackendReferences(rule, true, true), model.HTTPRouteExternalAuthReferences(rule)...) {
			key, supported := model.BackendServiceReference(route.Namespace, backend)
			if supported && key.Namespace != "" && key.Name != "" && key.Namespace != route.Namespace {
				keys[key.Namespace] = struct{}{}
			}
		}
	}
	return sortedIndexKeys(keys)
}

func isCoreServiceBackend(backend gatewayv1.BackendObjectReference) bool {
	return (backend.Group == nil || *backend.Group == gatewayv1.Group("")) &&
		(backend.Kind == nil || *backend.Kind == gatewayv1.Kind("Service"))
}

func hasCoreServiceBackend(route *gatewayv1.HTTPRoute) bool {
	for _, rule := range route.Spec.Rules {
		if slices.ContainsFunc(model.HTTPBackendReferences(rule, true, true), isCoreServiceBackend) {
			return true
		}
	}
	return false
}

func l4BackendKeys(object client.Object, namespacesOnly bool) []string {
	namespace := object.GetNamespace()
	keys := make(map[string]struct{})
	addBackends := func(backends []gatewayv1.BackendRef) {
		for _, backend := range backends {
			key, supported := model.BackendServiceReference(namespace, backend.BackendObjectReference)
			if !supported || key.Namespace == "" || key.Name == "" {
				continue
			}
			if !namespacesOnly {
				keys[ObjectKeyString(key)] = struct{}{}
			} else if key.Namespace != namespace {
				keys[key.Namespace] = struct{}{}
			}
		}
	}
	switch route := object.(type) {
	case *gatewayv1.TCPRoute:
		for _, rule := range route.Spec.Rules {
			addBackends(rule.BackendRefs)
		}
	case *gatewayv1.UDPRoute:
		for _, rule := range route.Spec.Rules {
			addBackends(rule.BackendRefs)
		}
	}
	return sortedIndexKeys(keys)
}

func boundGatewayKeys(config Config, route client.Object) []string {
	keys, _ := RouteBoundGatewayKeys(config, route)
	var index []string
	for _, key := range keys {
		index = append(index, ObjectKeyString(key))
	}
	return index
}

func sortedIndexKeys(keys map[string]struct{}) []string {
	if len(keys) == 0 {
		return nil
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func gatewayClassParametersKeys(object client.Object) []string {
	class, ok := object.(*gatewayv1.GatewayClass)
	if !ok || class.Spec.ParametersRef == nil {
		return nil
	}
	ref := class.Spec.ParametersRef
	if ref.Group != gatewayv1.Group(configv1alpha1.GroupName) || ref.Kind != gatewayv1.Kind(configv1alpha1.GatewayClassConfigKind) || ref.Namespace != nil || ref.Name == "" {
		return nil
	}
	return []string{ref.Name}
}
