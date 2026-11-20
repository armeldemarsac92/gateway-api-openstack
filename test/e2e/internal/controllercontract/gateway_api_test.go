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

package controllercontract

import (
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
)

func TestRequireGatewayAPICRDsChecksEveryInstalledDefinition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		change  func([]client.Object) []client.Object
		wantErr bool
	}{
		{name: "pinned Standard bundle"},
		{name: "missing required CRD", change: func(objects []client.Object) []client.Object { return objects[1:] }, wantErr: true},
		{name: "CRD from previous release", change: func(objects []client.Object) []client.Object {
			objects[0].GetAnnotations()[gatewayconsts.BundleVersionAnnotation] = "v1.6.1"
			return objects
		}, wantErr: true},
		{name: "mixed channels", change: func(objects []client.Object) []client.Object {
			objects[0].GetAnnotations()[gatewayconsts.ChannelAnnotation] = "experimental"
			return objects
		}, wantErr: true},
		{name: "unexpected Gateway API CRD", change: func(objects []client.Object) []client.Object {
			extra := objects[0].DeepCopyObject().(client.Object)
			extra.SetName("xbackends.gateway.networking.x-k8s.io")
			return append(objects, extra)
		}, wantErr: true},
		{name: "deleting CRD", change: func(objects []client.Object) []client.Object {
			now := metav1.Now()
			objects[0].SetDeletionTimestamp(&now)
			objects[0].SetFinalizers([]string{"test"})
			return objects
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var objects []client.Object
			for _, name := range []string{"backendtlspolicies", "gatewayclasses", "gateways", "grpcroutes", "httproutes", "listenersets", "referencegrants", "tcproutes", "tlsroutes", "udproutes"} {
				objects = append(objects, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{
					Name:        name + ".gateway.networking.k8s.io",
					Annotations: map[string]string{gatewayconsts.BundleVersionAnnotation: gatewayconsts.BundleVersion, gatewayconsts.ChannelAnnotation: "standard"},
				}})
			}
			objects = append(objects, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "foreign.example.test"}})
			if test.change != nil {
				objects = test.change(objects)
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			label, err := RequireGatewayAPICRDs(t.Context(), reader)
			if test.wantErr {
				if err == nil || label != "" {
					t.Fatalf("RequireGatewayAPICRDs() = %q, %v, want error and no label", label, err)
				}
			} else if err != nil || label != "v1.7.0 Standard Channel" {
				t.Fatalf("RequireGatewayAPICRDs() = %q, %v, want v1.7.0 Standard Channel", label, err)
			}
		})
	}
}
