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
)

// TestKindUpstreamTCPTraffic runs the selected upstream cases through the
// public Octavia fixture and its stock HAProxy renderer.
func TestKindUpstreamTCPTraffic(t *testing.T) {
	channel := os.Getenv("GATEWAY_API_CHANNEL")
	if channel != "standard" && channel != "experimental" {
		t.Fatal("GATEWAY_API_CHANNEL must be standard or experimental")
	}
	options := suite.ConformanceOptions{
		ConfigurableOptions: suite.ConfigurableOptions{
			GatewayClassName:     "kind-upstream-tcp-traffic",
			CleanupBaseResources: true,
			CleanupTestResources: true,
			DisableParallelTests: true,
			SupportedFeatures:    sets.List(suite.GatewayTCPConformanceProfile.CoreFeatures),
		},
	}

	var cases []suite.ConformanceTest
	selection := os.Getenv("HIBISCUS_TCP_CASES")
	baseDir := os.Getenv("HIBISCUS_TCP_BASE_DIR")
	switch selection {
	case "traffic":
		cases = []suite.ConformanceTest{tests.TCPRouteMultipleRoutesAttachment, tests.TCPRouteReferenceGrant}
	case "weighted":
		cases = []suite.ConformanceTest{tests.TCPRouteWeightedRouting}
	case "multiport":
		cases = []suite.ConformanceTest{tests.TCPRouteParentRefAttachAll, tests.TCPRouteParentRefPortAndSectionName}
	case "status":
		cases = kindTCPStatusCases
	case "profile":
		if baseDir != "" {
			t.Fatal("HIBISCUS_TCP_CASES=profile requires the original upstream base and an unset HIBISCUS_TCP_BASE_DIR")
		}
		options.ConformanceProfiles = []suite.ConformanceProfileName{suite.GatewayTCPConformanceProfileName}
		cases = tests.ConformanceTests
	default:
		t.Fatal("HIBISCUS_TCP_CASES must be traffic, status, weighted, multiport or profile")
	}
	if selection != "profile" {
		if baseDir == "" {
			t.Fatal("a focused TCP selection requires HIBISCUS_TCP_BASE_DIR with tcp-status-base.yaml")
		}
		options.TimeoutConfig = config.TimeoutConfig{NamespacesMustBeReady: 90 * time.Second}
	}
	options.BaseManifests, options.ManifestFS = kindConformanceManifests(t, baseDir)
	t.Logf("Upstream GATEWAY-TCP %s selection on %s using %s", selection, channel, options.BaseManifests)
	runKindTrafficConformance(t, channel, "HIBISCUS_TCP", options, cases)
}
