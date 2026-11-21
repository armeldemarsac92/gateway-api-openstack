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
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
)

// RequireGatewayAPICRDs returns the release label after verifying the installed
// Standard CRD inventory, version and channel against the pinned release.
func RequireGatewayAPICRDs(ctx context.Context, reader client.Reader) (string, error) {
	observed, err := (controller.Config{}).ObserveGatewayAPIVersion(ctx, reader)
	if err != nil {
		return "", err
	}
	if !observed.Supported {
		return "", controller.ErrUnsupportedGatewayAPIVersion
	}
	return observed.Version + " Standard Channel", nil
}
