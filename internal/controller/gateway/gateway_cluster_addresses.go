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

package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

type clusterAddressRequest struct {
	index int
	value string
	key   client.ObjectKey
}

type clusterAddressAssignments struct {
	requested bool
	addresses map[int]string
	issues    map[int]string
	retained  []string
}

// A rejected allocation remains a per-request outcome and can be retried on resync.
type clusterAddressUnavailable struct{ error }

func serviceIPUnavailable(err error) bool {
	var status apierrors.APIStatus
	if !apierrors.IsInvalid(err) || !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	return details != nil && slices.ContainsFunc(details.Causes, func(cause metav1.StatusCause) bool {
		field, _, _ := strings.Cut(cause.Field, "[")
		return field == "spec.clusterIP" || field == "spec.clusterIPs" || field == "spec.ipFamilies"
	})
}

func supportedClusterAddressRequest(request gatewayv1.GatewaySpecAddress) (string, bool) {
	if request.Routability != gatewayv1.GatewayAddressRoutabilityCluster ||
		ptr.Deref(request.Type, gatewayv1.IPAddressType) != gatewayv1.IPAddressType {
		return "", false
	}
	if request.Value == "" {
		return "", true
	}
	ip, err := netip.ParseAddr(request.Value)
	if err != nil || !ip.IsGlobalUnicast() || ip.Is4In6() || ip.Zone() != "" {
		return "", false
	}
	return ip.String(), true
}

func clusterAddressServicePrefix(gateway *gatewayv1.Gateway) string {
	digest := sha256.Sum256([]byte(gateway.UID))
	return fmt.Sprintf("gw-cluster-%x-", digest[:10])
}

func (r *Reconciler) clusterAddressRequests(ctx context.Context, gateway *gatewayv1.Gateway) ([]clusterAddressRequest, error) {
	if !r.Config.GatewayAddressRoutabilityClusterEnabled() {
		return nil, nil
	}
	prefix := clusterAddressServicePrefix(gateway)
	var fixed, dynamic []clusterAddressRequest
	for index, address := range gateway.Spec.Addresses {
		value, supported := supportedClusterAddressRequest(address)
		if !supported {
			continue
		}
		request := clusterAddressRequest{index: index, value: value, key: client.ObjectKey{Namespace: gateway.Namespace}}
		if value == "" {
			dynamic = append(dynamic, request)
		} else {
			digest := sha256.Sum256([]byte(value))
			request.key.Name = fmt.Sprintf("%s%x", prefix, digest[:10])
			fixed = append(fixed, request)
		}
	}
	if len(fixed)+len(dynamic) == 0 {
		return nil, nil
	}
	services, err := r.clusterAddressServices(ctx, gateway)
	if err != nil {
		return nil, err
	}
	used := make(map[string]bool)
	for i := range fixed {
		// Pin an IP already allocated to this Gateway by keeping the same
		// Service. Releasing it would race the cluster allocator.
		for _, service := range services {
			if service.Spec.ClusterIP == fixed[i].value && service.DeletionTimestamp.IsZero() {
				fixed[i].key.Name = service.Name
				break
			}
		}
		used[fixed[i].key.Name] = true
	}
	slot := 0
	for i := range dynamic {
		for {
			name := prefix + "auto-" + strconv.Itoa(slot)
			slot++
			if !used[name] {
				dynamic[i].key.Name = name
				break
			}
		}
	}
	return append(fixed, dynamic...), nil
}

func (r *Reconciler) clusterAddressServices(ctx context.Context, gateway *gatewayv1.Gateway) ([]corev1.Service, error) {
	var services corev1.ServiceList
	if err := r.APIReader.List(ctx, &services, client.InNamespace(gateway.Namespace),
		client.MatchingLabels{clusterAddressGatewayLabel: string(gateway.UID)}); err != nil {
		return nil, fmt.Errorf("list Gateway ClusterIP Services: %w", err)
	}
	prefix := clusterAddressServicePrefix(gateway)
	return slices.DeleteFunc(services.Items, func(service corev1.Service) bool {
		return !strings.HasPrefix(service.Name, prefix) || !r.ownsDiscoveryObject(gateway, &service)
	}), nil
}

type clusterAddressReservation struct {
	publication discoveryPublication
	address     string
	uid         types.UID
	retained    bool
}

// An allocated Service is the prerequisite for acquiring an Octavia VIP solely
// for Cluster traffic. All Gateway and Route graph requests use this check.
func (r *Reconciler) admitClusterAddressRequest(ctx context.Context, gateway *gatewayv1.Gateway, primary *gatewayv1.Listener,
	spec *cloud.GatewaySpec,
) ([]clusterAddressReservation, error) {
	if slices.Contains(spec.RequestedVIPAddresses, "") {
		return nil, nil
	}
	requests, err := r.clusterAddressRequests(ctx, gateway)
	if err != nil || len(requests) == 0 {
		return nil, err
	}
	resolver, ok := r.TopologyResolver.(cloud.VIPSubnetFamilyResolver)
	if !ok {
		return nil, errors.New("ClusterIP addresses require a VIP subnet family resolver")
	}
	version, err := resolver.VIPSubnetIPVersion(ctx, spec.VIPSubnetID)
	if err != nil {
		return nil, err
	}
	var family discoveryv1.AddressType
	switch version {
	case 4:
		family = discoveryv1.AddressTypeIPv4
	case 6:
		family = discoveryv1.AddressTypeIPv6
	default:
		return nil, fmt.Errorf("unsupported VIP subnet IP version %d", version)
	}
	if err := r.checkDiscoveryGateway(ctx, gateway, true); err != nil {
		return nil, err
	}
	// A Service requires a port to reserve its IP. Only the EndpointSlice
	// can make this port reachable, after the provider confirms readiness.
	ports := discoveryPorts(r.Config, gateway, primary, cloud.GatewayResult{ScopedListenerReadyPorts: []int{int(primary.Port)}})
	var reservations []clusterAddressReservation
	var operationalError error
	for _, request := range requests {
		if ip, _ := netip.ParseAddr(request.value); ip.IsValid() && discoveryAddressFamily(ip) != family {
			continue
		}
		publication := discoveryPublication{key: request.key, clusterIP: request.value, ports: ports, reserveFamily: family}
		address, err := r.publishGatewayDiscovery(ctx, gateway, publication)
		if err != nil {
			var unavailable clusterAddressUnavailable
			if !errors.As(err, &unavailable) {
				operationalError = errors.Join(operationalError, err)
			}
			continue
		}
		var service corev1.Service
		if err := r.APIReader.Get(ctx, request.key, &service); err != nil {
			operationalError = errors.Join(operationalError, err)
			continue
		}
		if address != "" && address == service.Spec.ClusterIP && r.ownsDiscoveryObject(gateway, &service) &&
			publication.usableService(&service, family) && clusterServiceProxyEnabled(&service) {
			reservations = append(reservations, clusterAddressReservation{publication: publication, address: address, uid: service.UID})
		}
	}
	if len(reservations) != 0 {
		spec.RequestedVIPAddresses = append(spec.RequestedVIPAddresses, "")
	}
	if operationalError != nil && len(spec.RequestedVIPAddresses) == 0 {
		return nil, operationalError
	}
	return reservations, nil
}

func (r *Reconciler) observeClusterAddressLoss(ctx context.Context, gateway *gatewayv1.Gateway, spec *cloud.GatewaySpec) ([]clusterAddressReservation, error) {
	if !r.Config.GatewayAddressRoutabilityClusterEnabled() || slices.Contains(spec.RequestedVIPAddresses, "") ||
		!slices.ContainsFunc(gateway.Spec.Addresses, func(address gatewayv1.GatewaySpecAddress) bool {
			return address.Routability == gatewayv1.GatewayAddressRoutabilityCluster
		}) {
		return nil, nil
	}
	loss := &cloud.ClusterAddressLoss{}
	spec.ClusterAddressLoss = loss
	for _, address := range gateway.Status.Addresses {
		if ptr.Deref(address.Routability, "") != gatewayv1.GatewayAddressRoutabilityCluster {
			loss.RetainedProviderAddresses = append(loss.RetainedProviderAddresses, address.Value)
		}
	}
	if !hasClusterStatusAddress(gateway) {
		return nil, nil
	}
	requests, err := r.clusterAddressRequests(ctx, gateway)
	if err != nil || len(requests) == 0 {
		return nil, err
	}
	services, err := r.clusterAddressServices(ctx, gateway)
	if err != nil {
		return nil, err
	}
	var retained []clusterAddressReservation
	for i := range services {
		service := &services[i]
		key := client.ObjectKeyFromObject(service)
		if slices.ContainsFunc(requests, func(request clusterAddressRequest) bool { return request.key == key }) {
			continue
		}
		publication := discoveryPublication{key: key}
		address, err := r.observePublishedClusterAddress(ctx, gateway, publication, true)
		if err != nil {
			var collision discoverySliceCollision
			if errors.As(err, &collision) {
				continue
			}
			return nil, err
		}
		if address == service.Spec.ClusterIP && address != "" {
			vip := service.Annotations[clusterFrontendVIPAnnotation]
			publication.vip, _ = netip.ParseAddr(vip)
			loss.RetainedClusterVIPs = append(loss.RetainedClusterVIPs, vip)
			retained = append(retained, clusterAddressReservation{publication: publication, address: address, uid: service.UID, retained: true})
		}
	}
	return retained, nil
}

func (r *Reconciler) validateClusterAddressReservations(ctx context.Context, gateway *gatewayv1.Gateway, all []clusterAddressReservation) error {
	var err error
	for _, reservation := range all {
		var service corev1.Service
		if readErr := r.APIReader.Get(ctx, reservation.publication.key, &service); readErr != nil {
			err = errors.Join(err, client.IgnoreNotFound(readErr))
			continue
		}
		if reservation.retained {
			vip := service.Annotations[clusterFrontendVIPAnnotation]
			if service.UID != reservation.uid || vip != reservation.publication.vip.String() {
				continue
			}
			address, readErr := r.observePublishedClusterAddress(ctx, gateway, reservation.publication, true)
			if readErr == nil && address == reservation.address {
				return nil
			}
			err = errors.Join(err, readErr)
			continue
		}
		if service.UID == reservation.uid && service.Spec.ClusterIP == reservation.address && r.ownsDiscoveryObject(gateway, &service) &&
			reservation.publication.usableService(&service, reservation.publication.reserveFamily) && clusterServiceProxyEnabled(&service) {
			if conflict := r.checkDiscoverySliceAssociation(ctx, gateway, reservation.publication.key, &service, true); conflict == nil {
				return nil
			} else {
				err = errors.Join(err, conflict)
			}
		}
	}
	return errors.Join(errGatewayChanged, err)
}

func hasClusterStatusAddress(gateway *gatewayv1.Gateway) bool {
	return slices.ContainsFunc(gateway.Status.Addresses, func(address gatewayv1.GatewayStatusAddress) bool {
		return ptr.Deref(address.Routability, "") == gatewayv1.GatewayAddressRoutabilityCluster
	})
}

func clusterAddressRejection(gateway *gatewayv1.Gateway, assignments clusterAddressAssignments) bool {
	if !assignments.requested || len(gateway.Status.Addresses) != 0 {
		return false
	}
	condition := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	return condition != nil && condition.ObservedGeneration == gateway.Generation && condition.Status == metav1.ConditionFalse &&
		condition.Reason == string(gatewayv1.GatewayReasonAddressesNotAssigned)
}

func clearClusterStatusAddresses(gateway *gatewayv1.Gateway) {
	gateway.Status.Addresses = slices.DeleteFunc(gateway.Status.Addresses, func(address gatewayv1.GatewayStatusAddress) bool {
		return ptr.Deref(address.Routability, "") == gatewayv1.GatewayAddressRoutabilityCluster
	})
}

// Withdraw stale Services before publishing the current spec. A graph
// replacement can retain the Service allocations with their endpoints empty.
func (r *Reconciler) withdrawClusterAddresses(ctx context.Context, gateway *gatewayv1.Gateway, keep []clusterAddressRequest, replacing bool) error {
	labels := client.MatchingLabels{clusterAddressGatewayLabel: string(gateway.UID)}
	namespace := client.InNamespace(gateway.Namespace)
	services, err := r.clusterAddressServices(ctx, gateway)
	if err != nil {
		return err
	}
	var endpointSlices discoveryv1.EndpointSliceList
	if err := r.APIReader.List(ctx, &endpointSlices, namespace, labels); err != nil {
		return fmt.Errorf("list Gateway ClusterIP EndpointSlices: %w", err)
	}
	keys := make(map[client.ObjectKey]struct{}, len(services))
	prefix := clusterAddressServicePrefix(gateway)
	for i := range services {
		keys[client.ObjectKeyFromObject(&services[i])] = struct{}{}
	}
	// A slice whose Service was removed still has a Gateway owner. Include it
	// in cleanup even when the Service no longer appears in the listing.
	for i := range endpointSlices.Items {
		slice := &endpointSlices.Items[i]
		serviceName := slice.Labels[discoveryv1.LabelServiceName]
		key := client.ObjectKey{Namespace: slice.Namespace, Name: serviceName}
		if strings.HasPrefix(serviceName, prefix) && slice.Name == discoverySliceKey(key, slice.AddressType).Name && r.ownsDiscoveryObject(gateway, slice) {
			keys[key] = struct{}{}
		}
	}
	for key := range keys {
		desired := slices.ContainsFunc(keep, func(request clusterAddressRequest) bool { return key == request.key })
		if !desired || replacing {
			err = errors.Join(err, r.withdrawDiscoveryPublication(ctx, gateway, key, !desired))
		}
	}
	return err
}

func (r *Reconciler) reconcileClusterAddresses(ctx context.Context, gateway *gatewayv1.Gateway, primary *gatewayv1.Listener,
	observation cloud.GatewayResult, observed bool,
) (clusterAddressAssignments, error) {
	if !r.Config.GatewayAddressRoutabilityEnabled() {
		return clusterAddressAssignments{}, nil
	}
	if err := r.checkDiscoveryGateway(ctx, gateway, true); err != nil {
		return clusterAddressAssignments{}, err
	}
	requests, err := r.clusterAddressRequests(ctx, gateway)
	if err != nil {
		return clusterAddressAssignments{}, err
	}
	assignments := clusterAddressAssignments{requested: len(requests) != 0, addresses: make(map[int]string), issues: make(map[int]string)}
	vip, _ := netip.ParseAddr(observation.State.VIPAddress)
	floatingIP, _ := netip.ParseAddr(observation.State.FloatingIPAddress)
	ports := discoveryPorts(r.Config, gateway, primary, observation)
	used := make(map[string]bool)
	for _, request := range requests {
		publication := discoveryPublication{key: request.key, clusterIP: request.value, vip: vip.Unmap(), floatingIP: floatingIP.Unmap(), ports: ports}
		var address string
		var publishErr error
		fixed, _ := netip.ParseAddr(request.value)
		switch {
		case publication.conflictsWithFrontend(request.value):
			publishErr = clusterAddressUnavailable{fmt.Errorf("requested ClusterIP %s conflicts with the Octavia frontend", fixed)}
			err = errors.Join(err, r.withdrawDiscoveryPublication(ctx, gateway, request.key, true))
		case fixed.IsValid() && vip.IsValid() && fixed.Is4() != vip.Unmap().Is4():
			publishErr = clusterAddressUnavailable{fmt.Errorf("requested ClusterIP %s is incompatible with the Octavia frontend", fixed)}
			err = errors.Join(err, r.withdrawDiscoveryPublication(ctx, gateway, request.key, false))
		case observed:
			address, publishErr = r.publishGatewayDiscovery(ctx, gateway, publication)
		default:
			address, publishErr = r.observePublishedClusterAddress(ctx, gateway, publication, false)
		}
		switch {
		case publishErr != nil:
			assignments.issues[request.index] = publishErr.Error()
			var unavailable clusterAddressUnavailable
			if !errors.As(publishErr, &unavailable) {
				err = errors.Join(err, publishErr)
			}
		case address != "" && !used[address]:
			assignments.addresses[request.index] = address
			used[address] = true
		}
	}
	// If an update cannot be applied, leave a still-serving ClusterIP in
	// place. Explicitly removing the last Cluster request still withdraws it.
	if assignments.requested && len(assignments.addresses) == 0 && hasClusterStatusAddress(gateway) &&
		observation.AddressActive && vip.IsValid() && (observation.AddressIssue != "" ||
		len(assignments.issues) != 0 && len(requestedVIPCandidates(gateway.Spec.Addresses)) == 0) {
		services, listErr := r.clusterAddressServices(ctx, gateway)
		err = errors.Join(err, listErr)
		for i := range services {
			service := &services[i]
			key := client.ObjectKeyFromObject(service)
			if slices.ContainsFunc(requests, func(request clusterAddressRequest) bool { return request.key == key }) {
				continue
			}
			address, checkErr := r.observePublishedClusterAddress(ctx, gateway, discoveryPublication{key: key, vip: vip.Unmap(), floatingIP: floatingIP.Unmap()}, true)
			err = errors.Join(err, checkErr)
			if address != "" {
				assignments.retained = append(assignments.retained, address)
				requests = append(requests, clusterAddressRequest{key: key})
			}
		}
		slices.Sort(assignments.retained)
	}
	return assignments, errors.Join(err, r.withdrawClusterAddresses(ctx, gateway, requests, false))
}

func (r *Reconciler) observePublishedClusterAddress(ctx context.Context, gateway *gatewayv1.Gateway, publication discoveryPublication, retaining bool) (string, error) {
	var service corev1.Service
	if err := r.APIReader.Get(ctx, publication.key, &service); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if !r.ownsDiscoveryObject(gateway, &service) {
		return "", nil
	}
	if err := r.checkDiscoverySliceAssociation(ctx, gateway, publication.key, &service, true); err != nil {
		return "", err
	}
	if !publication.usableService(&service, "") || !clusterServiceProxyEnabled(&service) {
		return "", r.withdrawDiscoveryPublication(ctx, gateway, publication.key, false)
	}
	condition := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
	if condition == nil || !retaining && condition.ObservedGeneration != gateway.Generation ||
		!slices.ContainsFunc(gateway.Status.Addresses, func(address gatewayv1.GatewayStatusAddress) bool {
			return address.Value == service.Spec.ClusterIP && ptr.Deref(address.Routability, "") == gatewayv1.GatewayAddressRoutabilityCluster
		}) {
		return "", nil
	}
	family := discoveryv1.AddressType(service.Spec.IPFamilies[0])
	var endpointSlice discoveryv1.EndpointSlice
	if err := r.APIReader.Get(ctx, discoverySliceKey(publication.key, family), &endpointSlice); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if r.validClusterEndpointSlice(gateway, &service, &endpointSlice, family) &&
		(!publication.vip.IsValid() || publication.vip.String() == service.Annotations[clusterFrontendVIPAnnotation]) {
		return service.Spec.ClusterIP, nil
	}
	return "", r.withdrawDiscoveryPublication(ctx, gateway, publication.key, false)
}

func (r *Reconciler) validClusterEndpointSlice(gateway *gatewayv1.Gateway, service *corev1.Service,
	endpointSlice *discoveryv1.EndpointSlice, family discoveryv1.AddressType,
) bool {
	_, headless := endpointSlice.Labels[corev1.IsHeadlessService]
	if !endpointSlice.DeletionTimestamp.IsZero() || endpointSlice.AddressType != family ||
		!r.ownsDiscoveryObject(gateway, endpointSlice) || !sliceHasServiceOwner(endpointSlice, service) ||
		headless || endpointSlice.Labels[discoveryv1.LabelServiceName] != service.Name || endpointSlice.Labels[discoveryv1.LabelManagedBy] != discoveryManager ||
		len(service.Spec.Ports) == 0 || !equality.Semantic.DeepEqual(endpointSlice.Ports, discoveryEndpointPorts(service.Spec.Ports)) ||
		len(endpointSlice.Endpoints) != 1 {
		return false
	}
	endpoint := endpointSlice.Endpoints[0]
	vip := service.Annotations[clusterFrontendVIPAnnotation]
	address, _ := netip.ParseAddr(vip)
	return vip != service.Spec.ClusterIP && discoveryAddressFamily(address) == family &&
		slices.Equal(endpoint.Addresses, []string{vip}) && ptr.Deref(endpoint.Conditions.Ready, false) &&
		!ptr.Deref(endpoint.Conditions.Terminating, false)
}

func (r *Reconciler) withdrawGatewayPublications(ctx context.Context, gateway *gatewayv1.Gateway, removeService bool) error {
	err := r.withdrawGatewayDiscovery(ctx, gateway, removeService)
	if !r.Config.GatewayInfrastructureMetadata || !r.Config.GatewayAddressRoutabilityEnabled() {
		return err
	}
	if checkErr := r.checkDiscoveryGateway(ctx, gateway, false); checkErr != nil {
		return errors.Join(err, checkErr)
	}
	var keep []clusterAddressRequest
	if !removeService {
		var listErr error
		keep, listErr = r.clusterAddressRequests(ctx, gateway)
		if listErr != nil {
			return errors.Join(err, listErr)
		}
	}
	if hasClusterStatusAddress(gateway) {
		clearClusterStatusAddresses(gateway)
		if assignment := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned)); assignment != nil &&
			assignment.Status == metav1.ConditionTrue {
			meta.RemoveStatusCondition(&gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
		}
	}
	return errors.Join(err, r.withdrawClusterAddresses(ctx, gateway, keep, true))
}
