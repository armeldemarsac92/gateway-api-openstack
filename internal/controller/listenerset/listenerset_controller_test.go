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

package listenerset

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestReconcilePreservesForeignListenerSetStatus(t *testing.T) {
	const controllerName = "example.com/openstack"
	scheme := runtime.NewScheme()
	if err := gatewayv1.Install(scheme); err != nil {
		t.Fatal(err)
	}
	parent := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "edge", Name: "shared"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "foreign"},
	}
	local := parent.DeepCopy()
	local.Namespace = "apps"
	local.Spec.GatewayClassName = "managed"
	foreignClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "foreign"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "elsewhere.example/controller"},
	}
	ourClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "managed"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: controllerName},
	}
	listenerSet := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "delegated", Generation: 2},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Namespace: ptr.To(gatewayv1.Namespace(parent.Namespace)),
				Name:      gatewayv1.ObjectName(parent.Name),
			},
		},
		Status: gatewayv1.ListenerSetStatus{
			Conditions: []metav1.Condition{
				{Type: string(gatewayv1.ListenerSetConditionAccepted), Status: metav1.ConditionTrue, Reason: "Accepted", ObservedGeneration: 2},
				{Type: string(gatewayv1.ListenerSetConditionProgrammed), Status: metav1.ConditionTrue, Reason: "Programmed", ObservedGeneration: 2},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(listenerSet).
		WithObjects(listenerSet, parent, local, foreignClass, ourClass).Build()
	r := &reconciler{Client: kube, apiReader: kube, controllerName: controllerName}
	key := client.ObjectKeyFromObject(listenerSet)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var actual gatewayv1.ListenerSet
	if err := kube.Get(t.Context(), key, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual.Status, listenerSet.Status) {
		t.Fatalf("foreign ListenerSet status changed from %#v to %#v", listenerSet.Status, actual.Status)
	}
}
