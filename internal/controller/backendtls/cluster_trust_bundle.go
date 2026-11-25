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

package backendtls

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// ErrClusterTrustBundleUnavailable marks an unconfirmed trust observation.
var ErrClusterTrustBundleUnavailable = errors.New("ClusterTrustBundle API unavailable")

// CABundleTooLargeReason reports a valid CA bundle exceeding the Barbican limit.
const CABundleTooLargeReason = "CACertificateBundleTooLarge"

var ErrCABundleTooLarge = errors.New("normalized spec.trustBundle exceeds the 128 KiB Barbican CA payload limit")

var clusterTrustBundleKind = schema.GroupKind{Group: "certificates.k8s.io", Kind: "ClusterTrustBundle"}

func IsClusterTrustBundle(group gatewayv1.Group, kind gatewayv1.Kind) bool {
	return string(group) == clusterTrustBundleKind.Group && string(kind) == clusterTrustBundleKind.Kind
}

func ValidClusterTrustBundleName(name string) bool {
	return name != "" && len(content.IsPathSegmentName(name)) == 0
}

// CAObservation records either verified trust or a definitive reference error.
// API/discovery failures instead return separately so callers preserve traffic.
type CAObservation struct {
	CA      CA
	Invalid error
}

func (b *ClusterTrustBundles) Read(ctx context.Context, name string) (CAObservation, error) {
	if !ValidClusterTrustBundleName(name) {
		return CAObservation{Invalid: errors.New("name is invalid")}, nil
	}
	b.mu.RLock()
	resource, discoveryErr := b.resource, b.discoveryErr
	b.mu.RUnlock()
	if resource.Empty() {
		if discoveryErr != nil {
			return CAObservation{}, fmt.Errorf("%w: discovery: %w", ErrClusterTrustBundleUnavailable, discoveryErr)
		}
		return CAObservation{Invalid: errors.New("cannot be resolved because the native ClusterTrustBundle API is unserved")}, nil
	}
	key := types.NamespacedName{Name: name}
	object, err := b.objects.Resource(resource).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		var status apierrors.APIStatus
		if apierrors.IsNotFound(err) && errors.As(err, &status) {
			details := status.Status().Details
			if details != nil && details.Name == name && details.Group == resource.Group && details.Kind == resource.Resource {
				return CAObservation{Invalid: errors.New("was not found")}, nil
			}
		}
		return CAObservation{}, fmt.Errorf("%w: read %q: %w", ErrClusterTrustBundleUnavailable, name, err)
	}
	if object.GetName() != name || object.GetNamespace() != "" || object.GetUID() == "" || object.GetDeletionTimestamp() != nil {
		return CAObservation{Invalid: errors.New("has no live cluster-scoped revision")}, nil
	}
	bundle, found, err := unstructured.NestedString(object.Object, "spec", "trustBundle")
	if err != nil || !found {
		return CAObservation{Invalid: errors.New("has no spec.trustBundle")}, nil
	}
	normalized, err := normalizeTrustBundle([]byte(bundle))
	if err != nil {
		return CAObservation{Invalid: err}, nil
	}
	digest := sha256.Sum256(normalized)
	return CAObservation{CA: CA{Key: key, UID: object.GetUID(), Digest: hex.EncodeToString(digest[:]), PEM: normalized}}, nil
}

func normalizeTrustBundle(data []byte) ([]byte, error) {
	var certificates [][]byte
	for data = bytes.TrimSpace(data); len(data) != 0; data = bytes.TrimSpace(data) {
		block, rest := pem.Decode(data)
		if block == nil || !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) ||
			bytes.Count(data[:len(data)-len(rest)], []byte("-----BEGIN ")) != 1 {
			return nil, errors.New("spec.trustBundle must contain only PEM certificates")
		}
		cert, err := parseCACertificate(block)
		if err != nil {
			return nil, fmt.Errorf("spec.trustBundle: %w", err)
		}
		certificates = append(certificates, cert)
		data = rest
	}
	if len(certificates) == 0 {
		return nil, errors.New("spec.trustBundle has no CA certificates")
	}
	slices.SortFunc(certificates, bytes.Compare)
	certificates = slices.CompactFunc(certificates, bytes.Equal)
	var normalized []byte
	for _, certificate := range certificates {
		normalized = append(normalized, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})...)
	}
	if len(normalized) > cloud.MaxCAPEMBytes {
		return nil, ErrCABundleTooLarge
	}
	return normalized, nil
}
