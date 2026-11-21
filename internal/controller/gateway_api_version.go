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

package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
	"sigs.k8s.io/gateway-api/pkg/features"
)

// GatewayAPICRDNames returns the CRDs shipped in the configured release channel.
func (c Config) GatewayAPICRDNames() []string {
	names := []string{
		"backendtlspolicies.gateway.networking.k8s.io",
		"gatewayclasses.gateway.networking.k8s.io",
		"gateways.gateway.networking.k8s.io",
		"grpcroutes.gateway.networking.k8s.io",
		"httproutes.gateway.networking.k8s.io",
		"listenersets.gateway.networking.k8s.io",
		"referencegrants.gateway.networking.k8s.io",
		"tcproutes.gateway.networking.k8s.io",
		"tlsroutes.gateway.networking.k8s.io",
		"udproutes.gateway.networking.k8s.io",
	}
	if c.GatewayAPIChannel == features.FeatureChannelExperimental {
		names = append(names,
			"xbackends.gateway.networking.x-k8s.io",
			"xbackendtrafficpolicies.gateway.networking.x-k8s.io",
			"xmeshes.gateway.networking.x-k8s.io",
			"xtelemetrypolicies.gateway.networking.x-k8s.io",
		)
	}
	return names
}

const (
	// UnsupportedGatewayAPIVersionMessage is the stable status message used
	// when the installed Gateway API CRDs do not match the supported bundle.
	UnsupportedGatewayAPIVersionMessage = "GatewayClass does not support the installed Gateway API CRD version"
	gatewayAPIExperimentalGroup         = "gateway.networking.x-k8s.io"
	gatewayAPIVersionRequeue            = time.Minute
)

// GatewayAPIVersionRequeueAfter returns the stable, jittered retry interval for
// checking the installed Gateway API bundle again.
func GatewayAPIVersionRequeueAfter(uid types.UID) time.Duration {
	return stableJitter(gatewayAPIVersionRequeue, string(uid)+"/gateway-api-version")
}

// ErrUnsupportedGatewayAPIVersion indicates that the installed Gateway API
// CRDs do not match the bundle supported by this controller.
var ErrUnsupportedGatewayAPIVersion = errors.New("installed Gateway API CRD version is unsupported")

// GatewayAPIVersionObservation describes the compatibility of the installed
// Gateway API CRD bundle.
type GatewayAPIVersionObservation struct {
	// Supported reports whether every required CRD uses the supported bundle.
	Supported bool
	// Version is the observed bundle annotation, populated only for a supported installation.
	Version string
	// Message summarizes the observed bundle state for status reporting.
	Message string
}

// ObserveGatewayAPIVersion reads Gateway API CRD metadata and evaluates the
// installed bundle version.
func (c Config) ObserveGatewayAPIVersion(ctx context.Context, reader client.Reader) (GatewayAPIVersionObservation, error) {
	var definitions metav1.PartialObjectMetadataList
	definitions.SetGroupVersionKind(
		apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinitionList"),
	)
	if err := reader.List(ctx, &definitions); err != nil {
		return GatewayAPIVersionObservation{}, fmt.Errorf("list Gateway API custom resource definitions: %w", err)
	}

	expected := c.GatewayAPICRDNames()
	channel := cmp.Or(c.GatewayAPIChannel, features.FeatureChannelStandard)
	var observedVersion string
	present := make(map[string]struct{}, len(expected))
	issues := make([]string, 0)
	for _, definition := range definitions.Items {
		if !IsGatewayAPICRDName(definition.Name) {
			continue
		}
		present[definition.Name] = struct{}{}
		if !definition.DeletionTimestamp.IsZero() {
			issues = append(issues, fmt.Sprintf("%s is being deleted", definition.Name))
			continue
		}
		installedVersion := definition.Annotations[gatewayconsts.BundleVersionAnnotation]
		installedChannel := definition.Annotations[gatewayconsts.ChannelAnnotation]
		switch {
		case installedVersion == "":
			issues = append(issues, fmt.Sprintf("%s has no bundle version annotation", definition.Name))
		case installedVersion != gatewayconsts.BundleVersion:
			issues = append(issues, fmt.Sprintf("%s uses bundle version %s", definition.Name, installedVersion))
		case installedChannel == "":
			issues = append(issues, fmt.Sprintf("%s has no channel annotation", definition.Name))
		case installedChannel != channel:
			issues = append(issues, fmt.Sprintf("%s uses channel %s (expected %s)", definition.Name, installedChannel, channel))
		case !slices.Contains(expected, definition.Name):
			issues = append(issues, fmt.Sprintf("%s is outside the %s bundle", definition.Name, channel))
		default:
			observedVersion = installedVersion
		}
	}
	for _, name := range expected {
		if _, found := present[name]; !found {
			issues = append(issues, fmt.Sprintf("%s is not installed", name))
		}
	}

	if len(issues) == 0 {
		return GatewayAPIVersionObservation{
			Supported: true,
			Version:   observedVersion,
			Message:   fmt.Sprintf("Installed Gateway API CRDs use supported bundle version %s (%s)", gatewayconsts.BundleVersion, channel),
		}, nil
	}
	sort.Strings(issues)
	return GatewayAPIVersionObservation{
		Message: fmt.Sprintf(
			"Installed Gateway API CRDs do not match supported bundle version %s: %s",
			gatewayconsts.BundleVersion,
			strings.Join(issues, ", "),
		),
	}, nil
}

// IsGatewayAPICRDName reports whether a CRD name belongs to a Gateway API
// group.
func IsGatewayAPICRDName(name string) bool {
	resource, group, found := strings.Cut(name, ".")
	return found && resource != "" && isGatewayAPIGroup(group)
}

func isGatewayAPIGroup(group string) bool {
	return group == gatewayv1.GroupName || group == gatewayAPIExperimentalGroup
}

// GatewayClassSupportsInstalledVersion reports whether the GatewayClass has a
// current SupportedVersion=True condition.
func GatewayClassSupportsInstalledVersion(gatewayClass *gatewayv1.GatewayClass) bool {
	condition := meta.FindStatusCondition(
		gatewayClass.Status.Conditions,
		string(gatewayv1.GatewayClassConditionStatusSupportedVersion),
	)
	return condition != nil && condition.Status == metav1.ConditionTrue &&
		condition.ObservedGeneration == gatewayClass.Generation
}

// ValidateInstalledGatewayAPIVersion returns an error unless a live read shows
// a supported Gateway API CRD bundle.
func (c Config) ValidateInstalledGatewayAPIVersion(ctx context.Context, reader client.Reader) error {
	if reader == nil {
		return ErrAPIReaderRequired
	}
	observation, err := c.ObserveGatewayAPIVersion(ctx, reader)
	if err != nil {
		return err
	}
	if !observation.Supported {
		return ErrUnsupportedGatewayAPIVersion
	}
	return nil
}
