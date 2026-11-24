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

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

const (
	discoveryControllerAnnotation = "gateway-api-openstack.github.io/discovery-controller"
	discoveryMetadataAnnotation   = "gateway-api-openstack.github.io/discovery-metadata"
	discoveryManager              = "gateway-api-openstack.github.io"
	maxDiscoveryPorts             = 20000 // EndpointSlice API validation limit.
)

var discoveryFamilies = [...]discoveryv1.AddressType{discoveryv1.AddressTypeIPv4, discoveryv1.AddressTypeIPv6}

func discoveryServiceKey(gateway *gatewayv1.Gateway) client.ObjectKey {
	name := "gateway-" + gateway.Name
	if len(validation.IsDNS1035Label(name)) != 0 {
		hash := sha256.Sum256([]byte(gateway.Name))
		name = fmt.Sprintf("gw-%x", hash[:16])
	}
	return client.ObjectKey{Namespace: gateway.Namespace, Name: name}
}

func discoverySliceKey(serviceKey client.ObjectKey, family discoveryv1.AddressType) client.ObjectKey {
	suffix := "-ipv4"
	if family == discoveryv1.AddressTypeIPv6 {
		suffix = "-ipv6"
	}
	return client.ObjectKey{Namespace: serviceKey.Namespace, Name: serviceKey.Name + suffix}
}

func discoveryOwner(gateway *gatewayv1.Gateway) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: gatewayv1.GroupVersion.String(), Kind: "Gateway", Name: gateway.Name, UID: gateway.UID, Controller: ptr.To(true)}
}

func (r *Reconciler) ownsDiscoveryObject(gateway *gatewayv1.Gateway, object client.Object) bool {
	owner := metav1.GetControllerOfNoCopy(object)
	expected := discoveryOwner(gateway)
	return gateway.UID != "" && owner != nil && owner.UID == expected.UID && owner.Name == expected.Name &&
		owner.Kind == expected.Kind && owner.APIVersion == expected.APIVersion &&
		object.GetAnnotations()[discoveryControllerAnnotation] == string(r.Config.ControllerName)
}

func (r *Reconciler) checkDiscoveryGateway(ctx context.Context, expected *gatewayv1.Gateway, publish bool) error {
	var live gatewayv1.Gateway
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &live); err != nil {
		if !publish && apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
	}
	if live.UID != expected.UID || live.Generation != expected.Generation || !live.DeletionTimestamp.Equal(expected.DeletionTimestamp) {
		return errGatewayChanged
	}
	if publish {
		var class gatewayv1.GatewayClass
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: string(live.Spec.GatewayClassName)}, &class); err != nil {
			return errors.Join(errGatewayChanged, client.IgnoreNotFound(err))
		}
		if class.Spec.ControllerName != r.Config.ControllerName || !controller.GatewayBindingMetadataEqual(r.Config, &live, expected) {
			return errGatewayChanged
		}
	}
	return nil
}

// The Service resolves to the private Octavia VIP. The Gateway address can
// independently select a Floating IP for callers outside the cluster.
func (r *Reconciler) reconcileGatewayDiscovery(ctx context.Context, gateway *gatewayv1.Gateway, primary *gatewayv1.Listener, observation cloud.GatewayResult) error {
	if err := r.checkDiscoveryGateway(ctx, gateway, true); err != nil {
		return err
	}
	address, _ := netip.ParseAddr(observation.State.VIPAddress)
	address = address.Unmap()
	ports := discoveryPorts(r.Config, gateway, primary, observation)
	if len(ports) > maxDiscoveryPorts {
		return fmt.Errorf("gateway serves more than %d ports, the EndpointSlice limit", maxDiscoveryPorts)
	}
	var family discoveryv1.AddressType
	if address.IsGlobalUnicast() && address.Zone() == "" && len(ports) > 0 {
		family = discoveryv1.AddressTypeIPv6
		if address.Is4() {
			family = discoveryv1.AddressTypeIPv4
		}
	}

	key := discoveryServiceKey(gateway)
	var service corev1.Service
	err := r.APIReader.Get(ctx, key, &service)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Gateway discovery Service: %w", err)
	}
	current := err == nil
	owned := current && r.ownsDiscoveryObject(gateway, &service)
	usable := owned && usableDiscoveryService(&service)
	if owned || !current && family != "" {
		if err := r.checkDiscoverySliceAssociation(ctx, gateway, key, &service, owned); err != nil {
			return err
		}
	}

	var active *discoveryv1.EndpointSlice
	for _, addressFamily := range discoveryFamilies {
		var endpointSlice discoveryv1.EndpointSlice
		sliceKey := discoverySliceKey(key, addressFamily)
		if err := r.APIReader.Get(ctx, sliceKey, &endpointSlice); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get Gateway discovery EndpointSlice: %w", err)
		}
		owned := r.ownsDiscoveryObject(gateway, &endpointSlice)
		if addressFamily == family {
			if !owned || endpointSlice.AddressType != family {
				return fmt.Errorf("discovery EndpointSlice %s is already in use", sliceKey)
			}
			if usable && sliceHasServiceOwner(&endpointSlice, &service) && endpointSlice.DeletionTimestamp.IsZero() {
				active = &endpointSlice
				continue
			}
		}
		if owned {
			if err := r.withdrawDiscoverySlice(ctx, &endpointSlice); err != nil {
				return err
			}
		}
	}

	if current && !usable {
		return fmt.Errorf("discovery Service %s is already in use or has an incompatible spec", key)
	}
	if !current && family == "" {
		return nil
	}
	if !current {
		service = corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, OwnerReferences: []metav1.OwnerReference{discoveryOwner(gateway)}},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: corev1.ClusterIPNone},
		}
	}
	base := service.DeepCopy()
	service.Spec.Ports = ports
	if err := r.applyDiscoveryMetadata(gateway, &service, nil); err != nil {
		return err
	}
	if err := r.writeDiscoveryObject(ctx, base, &service); err != nil || family == "" {
		return err
	}

	if active == nil {
		sliceKey := discoverySliceKey(key, family)
		active = &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: sliceKey.Namespace, Name: sliceKey.Name,
				OwnerReferences: []metav1.OwnerReference{
					discoveryOwner(gateway),
					{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service", Name: service.Name, UID: service.UID},
				},
			},
			AddressType: family,
		}
	}
	baseSlice := active.DeepCopy()
	active.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{address.String()}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}}
	active.Ports = make([]discoveryv1.EndpointPort, 0, len(ports))
	for _, port := range ports {
		active.Ports = append(active.Ports, discoveryv1.EndpointPort{Name: ptr.To(port.Name), Port: ptr.To(port.Port), Protocol: ptr.To(port.Protocol)})
	}
	reserved := map[string]string{
		discoveryv1.LabelServiceName: service.Name,
		discoveryv1.LabelManagedBy:   discoveryManager,
		corev1.IsHeadlessService:     "",
	}
	if err := r.applyDiscoveryMetadata(gateway, active, reserved); err != nil {
		return err
	}
	return r.writeDiscoveryObject(ctx, baseSlice, active)
}

// Check an existing publication even when no new provider result is usable.
func (r *Reconciler) checkPublishedDiscoveryService(ctx context.Context, gateway *gatewayv1.Gateway) error {
	key := discoveryServiceKey(gateway)
	var service corev1.Service
	if err := r.APIReader.Get(ctx, key, &service); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !r.ownsDiscoveryObject(gateway, &service) {
		return nil
	}
	if err := r.checkDiscoveryGateway(ctx, gateway, true); err != nil {
		return err
	}
	return r.checkDiscoverySliceAssociation(ctx, gateway, key, &service, true)
}

// Kubernetes associates slices to Services by label, regardless of their
// owner. Withdraw our publication when someone else associates a slice.
func (r *Reconciler) checkDiscoverySliceAssociation(ctx context.Context, gateway *gatewayv1.Gateway, serviceKey client.ObjectKey, service *corev1.Service, owned bool) error {
	var linked discoveryv1.EndpointSliceList
	if err := r.APIReader.List(ctx, &linked, client.InNamespace(serviceKey.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: serviceKey.Name}); err != nil {
		return fmt.Errorf("list EndpointSlices for Gateway discovery Service: %w", err)
	}
	for _, endpointSlice := range linked.Items {
		if endpointSlice.Name == discoverySliceKey(serviceKey, endpointSlice.AddressType).Name && r.ownsDiscoveryObject(gateway, &endpointSlice) {
			continue
		}
		collision := fmt.Errorf("discovery Service %s has an EndpointSlice managed elsewhere: %s", serviceKey, endpointSlice.Name)
		if owned {
			// Removing the Service takes its name out of DNS even while the
			// foreign slice is still present. The delete is fenced by its UID
			// and resource version from the ownership check.
			collision = errors.Join(collision, r.deleteDiscoveryObject(ctx, service))
		}
		return errors.Join(collision, r.withdrawGatewayDiscovery(ctx, gateway, false))
	}
	return nil
}

func discoveryPorts(config controller.Config, gateway *gatewayv1.Gateway, primary *gatewayv1.Listener, observation cloud.GatewayResult) []corev1.ServicePort {
	numbers := slices.Clone(observation.ScopedListenerReadyPorts)
	for _, listener := range gateway.Spec.Listeners {
		port := int(listener.Port)
		if slices.Contains(observation.ScopedListenerDeferredPorts, port) &&
			programmedListenerAtGeneration(gateway.Status.Listeners, listener.Name, gateway.Generation) != nil {
			numbers = append(numbers, port)
		}
	}
	if controller.GatewayListenerScope(config, gateway) == "" && observation.Outcome.State == cloud.OutcomeReady {
		numbers = append(numbers, int(primary.Port))
	}
	slices.Sort(numbers)
	protocol := corev1.ProtocolTCP
	if primary.Protocol == gatewayv1.UDPProtocolType {
		protocol = corev1.ProtocolUDP
	}
	var ports []corev1.ServicePort
	for _, port := range slices.Compact(numbers) {
		if port >= 1 && port <= 65535 {
			name := "tcp-"
			if protocol == corev1.ProtocolUDP {
				name = "udp-"
			}
			ports = append(ports, corev1.ServicePort{
				Name: name + strconv.Itoa(port), Protocol: protocol,
				Port: int32(port), TargetPort: intstr.FromInt(port),
			})
		}
	}
	return ports
}

func usableDiscoveryService(service *corev1.Service) bool {
	return service.DeletionTimestamp.IsZero() && service.Spec.Type == corev1.ServiceTypeClusterIP &&
		service.Spec.ClusterIP == corev1.ClusterIPNone && len(service.Spec.Selector) == 0 &&
		len(service.Spec.ExternalIPs) == 0 && !service.Spec.PublishNotReadyAddresses
}

func sliceHasServiceOwner(slice *discoveryv1.EndpointSlice, service *corev1.Service) bool {
	return slices.ContainsFunc(slice.OwnerReferences, func(owner metav1.OwnerReference) bool {
		return owner.UID == service.UID && owner.Name == service.Name && owner.Kind == "Service" &&
			owner.APIVersion == corev1.SchemeGroupVersion.String()
	})
}

// Withdrawal during a graph replacement keeps the stable Service name. A
// deleted or rejected Gateway no longer owns a discovery name.
func (r *Reconciler) withdrawGatewayDiscovery(ctx context.Context, gateway *gatewayv1.Gateway, removeService bool) error {
	if !r.Config.GatewayInfrastructureMetadata {
		return nil
	}
	if err := r.checkDiscoveryGateway(ctx, gateway, false); err != nil {
		return err
	}
	key := discoveryServiceKey(gateway)
	objects := []client.Object{
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: discoverySliceKey(key, discoveryv1.AddressTypeIPv4).Name}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: discoverySliceKey(key, discoveryv1.AddressTypeIPv6).Name}},
	}
	if removeService {
		objects = append(objects, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}})
	}
	for _, object := range objects {
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("read Gateway discovery %T for withdrawal: %w", object, err)
		}
		if !r.ownsDiscoveryObject(gateway, object) {
			continue
		}
		if endpointSlice, ok := object.(*discoveryv1.EndpointSlice); ok {
			if err := r.withdrawDiscoverySlice(ctx, endpointSlice); err != nil {
				return err
			}
		} else if err := r.deleteDiscoveryObject(ctx, object); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) withdrawDiscoverySlice(ctx context.Context, endpointSlice *discoveryv1.EndpointSlice) error {
	if len(endpointSlice.Endpoints) != 0 {
		// Deletion may leave a finalizing slice visible. Clear its DNS
		// addresses before requesting deletion.
		base := endpointSlice.DeepCopy()
		endpointSlice.Endpoints = []discoveryv1.Endpoint{}
		if err := r.Patch(ctx, endpointSlice, controller.OptimisticMergeFrom(base)); err != nil {
			return fmt.Errorf("clear Gateway discovery EndpointSlice addresses: %w", err)
		}
	}
	return r.deleteDiscoveryObject(ctx, endpointSlice)
}

func (r *Reconciler) deleteDiscoveryObject(ctx context.Context, object client.Object) error {
	uid, version := object.GetUID(), object.GetResourceVersion()
	err := r.Delete(ctx, object, client.Preconditions{UID: &uid, ResourceVersion: &version})
	if err := client.IgnoreNotFound(err); err != nil {
		return fmt.Errorf("withdraw Gateway discovery %T: %w", object, err)
	}
	return nil
}

func (r *Reconciler) writeDiscoveryObject(ctx context.Context, base, desired client.Object) error {
	var err error
	switch {
	case base.GetResourceVersion() == "":
		err = r.Create(ctx, desired)
	case !equality.Semantic.DeepEqual(base, desired):
		err = r.Patch(ctx, desired, controller.OptimisticMergeFrom(base))
	}
	if err != nil {
		return fmt.Errorf("publish Gateway discovery %T: %w", desired, err)
	}
	return nil
}
