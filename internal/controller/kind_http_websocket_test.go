//go:build envtest && kindtest

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
	"io/fs"
	"os"
	"testing"
	"time"

	"sigs.k8s.io/gateway-api/conformance"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"
)

// TestKindUpstreamHTTPWebSocket runs the upstream WebSocket traffic case from
// a guest that can reach the stock Octavia fixture VIPs and the kind Pod network.
func TestKindUpstreamHTTPWebSocket(t *testing.T) {
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if channel != "standard" && channel != "experimental" {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	baseManifests := "base/manifests.yaml"
	manifestFS := []fs.FS{&conformance.Manifests}
	if baseDir := os.Getenv("HIBISCUS_HTTP_BASE_DIR"); baseDir != "" {
		baseManifests = "tcp-status-base.yaml"
		manifestFS = append(manifestFS, os.DirFS(baseDir))
		t.Log("Using the scoped base with the unrelated HTTPS Gateway omitted")
	}

	webSocket := tests.HTTPRouteBackendProtocolWebSocket
	t.Logf("Selected upstream %s on %s (base %s)", webSocket.ShortName, channel, baseManifests)
	runKindTrafficConformance(t, channel, "HIBISCUS_HTTP", suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName:     "kind-upstream-http-websocket",
			CleanupBaseResources: true,
			CleanupTestResources: true,
			DisableParallelTests: true,
			SupportedFeatures: []features.FeatureName{
				features.SupportGateway,
				features.SupportHTTPRoute,
				features.SupportHTTPRouteBackendProtocolWebSocket,
			},
			TimeoutConfig: config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second},
		},
		BaseManifests: baseManifests,
		ManifestFS:    manifestFS,
	}, []suite.ConformanceTest{webSocket})
}
