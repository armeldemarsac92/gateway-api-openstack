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
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/gateway-api/conformance/tests"
	"sigs.k8s.io/gateway-api/conformance/utils/config"
	"sigs.k8s.io/gateway-api/conformance/utils/suite"
	"sigs.k8s.io/gateway-api/pkg/features"
)

// TestKindUpstreamHTTPAttachment selects three focused upstream traffic cases
// or attempts the full GATEWAY-HTTP Core profile against the stock Octavia fixture.
func TestKindUpstreamHTTPAttachment(t *testing.T) {
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if channel != "standard" && channel != "experimental" {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	options := suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName:     "kind-upstream-http-attachment",
			CleanupBaseResources: true,
			CleanupTestResources: true,
			DisableParallelTests: true,
		},
	}

	var cases []suite.ConformanceTest
	baseDir := os.Getenv("HIBISCUS_HTTP_BASE_DIR")
	switch os.Getenv("HIBISCUS_HTTP_CASES") {
	case "attachments":
		if baseDir == "" {
			t.Fatal("HIBISCUS_HTTP_CASES=attachments requires HIBISCUS_HTTP_BASE_DIR with tcp-status-base.yaml")
		}
		options.TimeoutConfig = config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second}
		options.SupportedFeatures = []features.FeatureName{features.SupportGateway, features.SupportHTTPRoute, features.SupportReferenceGrant}
		cases = []suite.ConformanceTest{
			tests.HTTPRouteSimpleSameNamespace,
			tests.HTTPRouteCrossNamespace,
			tests.HTTPRouteReferenceGrant,
		}
		t.Logf("Selected %s, %s and %s on %s with the scoped base", cases[0].ShortName, cases[1].ShortName, cases[2].ShortName, channel)
	case "profile":
		if baseDir != "" {
			t.Fatal("HIBISCUS_HTTP_CASES=profile requires the original upstream base and an unset HIBISCUS_HTTP_BASE_DIR")
		}
		options.ConformanceProfiles = []suite.ConformanceProfileName{suite.GatewayHTTPConformanceProfileName}
		options.SupportedFeatures = sets.List(suite.GatewayHTTPConformanceProfile.CoreFeatures)
		cases = tests.ConformanceTests
		t.Logf("Upstream GATEWAY-HTTP Core profile attempt on %s with the original upstream base", channel)
	default:
		t.Fatal("HIBISCUS_HTTP_CASES must be attachments or profile")
	}
	options.BaseManifests, options.ManifestFS = kindConformanceManifests(t, baseDir)
	runKindTrafficConformance(t, channel, "HIBISCUS_HTTP", options, cases)
}
