//go:build envtest

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

package controller_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

func testGatewayAddressRoutability(t *testing.T, c client.Client, cfg controller.Config, provider *recordingProvider, className string) {
	t.Helper()
	const sentinel gatewayv1.GatewayAddressRoutabilityType = "testing.x-k8s.io/sentinel"
	defaultScope := ptr.To(gatewayv1.GatewayAddressRoutabilityDefault)
	key := client.ObjectKey{Namespace: suite.InfrastructureNamespace, Name: "gateway-address-routability"}
	class := &gatewayv1.GatewayClass{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: className}, class); err != nil {
		t.Fatal(err)
	}
	var wantFeatures []gatewayv1.SupportedFeature
	if cfg.GatewayAddressRoutabilityEnabled() {
		wantFeatures = []gatewayv1.SupportedFeature{{Name: gatewayv1.FeatureName(features.SupportGatewayAddressRoutability)}}
	}
	if cfg.GatewayInfrastructureMetadata {
		wantFeatures = append(wantFeatures, gatewayv1.SupportedFeature{Name: gatewayv1.FeatureName(features.SupportGatewayInfrastructure)})
	}
	if !slices.Equal(class.Status.SupportedFeatures, wantFeatures) {
		t.Fatalf("published GatewayClass features = %+v, want %+v", class.Status.SupportedFeatures, wantFeatures)
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(className),
			Listeners:        []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}},
			Addresses:        []gatewayv1.GatewaySpecAddress{{Routability: sentinel}},
		},
	}
	if err := c.Create(t.Context(), gateway); err != nil {
		t.Fatal(err)
	}
	if !cfg.GatewayAddressRoutabilityEnabled() {
		programmed := waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, nil, "", gatewayv1.GatewayReasonProgrammed)
		if programmed.Spec.Addresses[0].Routability != "" {
			t.Fatalf("Standard API did not prune routability: %+v", programmed.Spec.Addresses)
		}
		return
	}

	waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, ptr.To(sentinel), gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)
	patchAddresses := func(value string) {
		t.Helper()
		if err := c.Patch(t.Context(), gateway, client.RawPatch(types.MergePatchType, []byte(value))); err != nil {
			t.Fatal(err)
		}
	}
	// The existing private VIP matches the fixed request. The automatic
	// default and sentinel requests cannot both use that same endpoint.
	patchAddresses(`{"spec":{"addresses":[{"routability":"testing.x-k8s.io/sentinel"},{"routability":""},{"value":"192.0.2.10"},{"routability":"example.com/unknown"}]}}`)
	partial := waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, defaultScope, gatewayv1.GatewayReasonAddressesPartiallyAssigned, gatewayv1.GatewayReasonProgrammed)
	message := meta.FindStatusCondition(partial.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned)).Message
	for _, unmet := range []string{"spec.addresses[0]", "testing.x-k8s.io/sentinel", "spec.addresses[1]", "spec.addresses[3]", "example.com/unknown"} {
		if !strings.Contains(message, unmet) || strings.Contains(message, "spec.addresses[2]") {
			t.Fatalf("partial assignment message %q does not enumerate exactly the unsatisfied requests", message)
		}
	}

	patchAddresses(`{"spec":{"addresses":[{"routability":"Cluster"},{"routability":"testing.x-k8s.io/other"}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderVIP, defaultScope, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonProgrammed)
	// Simulate provider unassignment. An unsupported scope must not replace
	// the missing VIP with a new default address.
	provider.mu.Lock()
	delete(provider.gatewayStates, string(gateway.UID))
	provider.mu.Unlock()
	patchAddresses(`{"spec":{"addresses":[{"routability":"example.com/unknown"}]}}`)
	waitGatewayAddressAssignment(t, c, key, "", nil, gatewayv1.GatewayReasonAddressesNotAssigned, gatewayv1.GatewayReasonAddressNotAssigned)

	// A new static sentinel can be acquired after the previous address is gone.
	patchAddresses(`{"spec":{"addresses":[{"value":"192.0.2.30","routability":"testing.x-k8s.io/sentinel"}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, ptr.To(sentinel), gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)

	// Explicit empty routability restores the default on the same VIP.
	patchAddresses(`{"spec":{"addresses":[{"routability":""}]}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, defaultScope, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)

	patchAddresses(`{"spec":{"addresses":null}}`)
	waitGatewayAddressAssignment(t, c, key, recordingProviderStaticVIP, defaultScope, gatewayv1.GatewayReasonAddressesAssigned, gatewayv1.GatewayReasonProgrammed)
}

func waitGatewayAddressAssignment(t *testing.T, c client.Client, key client.ObjectKey, value string, scope *gatewayv1.GatewayAddressRoutabilityType, assignmentReason, programmedReason gatewayv1.GatewayConditionReason) *gatewayv1.Gateway {
	t.Helper()
	gateway := &gatewayv1.Gateway{}
	programmedStatus := metav1.ConditionFalse
	if programmedReason == gatewayv1.GatewayReasonProgrammed {
		programmedStatus = metav1.ConditionTrue
	}
	assignmentStatus := metav1.ConditionFalse
	if assignmentReason == gatewayv1.GatewayReasonAddressesAssigned {
		assignmentStatus = metav1.ConditionTrue
	}
	err := wait.PollUntilContextTimeout(t.Context(), 25*time.Millisecond, envtestWaitTimeout, true, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, key, gateway); err != nil {
			return false, err
		}
		programmed := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
		if programmed == nil || programmed.Status != programmedStatus || programmed.Reason != string(programmedReason) || programmed.ObservedGeneration != gateway.Generation {
			return false, nil
		}
		assignment := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionAddressesAssigned))
		if assignmentReason == "" {
			if assignment != nil {
				return false, nil
			}
		} else if assignment == nil || assignment.Status != assignmentStatus || assignment.Reason != string(assignmentReason) || assignment.ObservedGeneration != gateway.Generation {
			return false, nil
		}
		addresses := gateway.Status.Addresses
		if value == "" {
			return len(addresses) == 0, nil
		}
		return len(addresses) == 1 && addresses[0].Value == value && ptr.Equal(addresses[0].Routability, scope), nil
	})
	if err != nil {
		t.Fatalf("wait for address %q (routability %v, AddressesAssigned %s, Programmed %s) at generation %d: addresses=%+v conditions=%+v: %v",
			value, ptr.Deref(scope, "<absent>"), assignmentReason, programmedReason, gateway.Generation, gateway.Status.Addresses, gateway.Status.Conditions, err)
	}
	return gateway
}
