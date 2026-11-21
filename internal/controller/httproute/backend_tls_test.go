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

package httproute

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestBackendTLSPolicyProtectsOneWeightedShare(t *testing.T) {
	f := newRouteModelInputFixture(t)
	f.reconciler.Config.BackendTLSPolicy = true
	f.reconciler.Config.WeightedHTTPTargets = true
	f.reconciler.BackendTLSAvailable = func(context.Context) (bool, error) { return true, nil }
	healthy, endpoints := f.service.DeepCopy(), f.endpoints.DeepCopy()
	healthy.Name, healthy.UID, healthy.ResourceVersion = "healthy", "healthy-uid", ""
	endpoints.Name, endpoints.UID, endpoints.ResourceVersion = "healthy", "healthy-slice-uid", ""
	endpoints.Labels[discoveryv1.LabelServiceName] = healthy.Name
	f.create(t, healthy, endpoints)

	selected := f.route.Spec.Rules[0].BackendRefs[0].DeepCopy()
	selected.Weight = ptr.To(int32(3))
	other := selected.DeepCopy()
	other.Name, other.Weight = gatewayv1.ObjectName(healthy.Name), ptr.To(int32(1))
	f.route.Spec.Rules[0].BackendRefs = []gatewayv1.HTTPBackendRef{*selected, *other}
	require.NoError(t, f.client.Update(t.Context(), f.route))

	before := f.observe(t)
	require.NoError(t, before.Validate(t.Context()))
	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.service.Namespace, Name: "require-tls", UID: "policy-uid", Generation: 1},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: gatewayv1.ObjectName(f.service.Name)},
			}},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname: "api.example.test", CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "Secret", Name: "invalid"}},
			},
		},
	}
	f.create(t, policy)
	require.ErrorIs(t, before.Validate(t.Context()), errHTTPRouteChanged)

	provider := &recordingProvider{}
	r := newTestReconciler(*f.reconciler, provider)
	key := client.ObjectKeyFromObject(f.route)
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Len(t, provider.routeSpecs, 1)
	spec := provider.routeSpecs[0]
	require.Len(t, spec.WeightedHTTPRules, 1)
	require.Len(t, spec.WeightedHTTPRules[0].Targets, 2)
	require.Len(t, spec.HTTPBackends, 2)
	denied, allowed := spec.WeightedHTTPRules[0].Targets[0], spec.WeightedHTTPRules[0].Targets[1]
	require.Equal(t, 3, denied.Weight)
	require.Equal(t, 1, allowed.Weight)
	require.NotNil(t, denied.BackendIndex)
	require.NotNil(t, allowed.BackendIndex)
	rejected := spec.HTTPBackends[*denied.BackendIndex]
	require.NotNil(t, rejected.BackendTLS)
	require.True(t, rejected.BackendTLS.Denied)
	require.True(t, rejected.Unavailable)
	require.Empty(t, rejected.Members)
	require.False(t, rejected.InvalidReference)
	require.Nil(t, spec.HTTPBackends[*allowed.BackendIndex].BackendTLS)
	require.NotEmpty(t, spec.HTTPBackends[*allowed.BackendIndex].Members)

	require.NoError(t, f.client.Get(t.Context(), key, f.route))
	require.Len(t, f.route.Status.Parents, 1)
	require.True(t, meta.IsStatusConditionTrue(f.route.Status.Parents[0].Conditions, string(gatewayv1.RouteConditionResolvedRefs)))
}
