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
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
)

// Validation requires a backend SNI hostname and a trusted CA.
type Validation struct {
	Hostname        string
	SubjectAltNames []cloud.BackendTLSSubjectAltName
	CA              CA
}

// CloudValidation carries the exact selected trust and identity to the provider.
func (v *Validation) CloudValidation() *cloud.BackendTLSValidation {
	return &cloud.BackendTLSValidation{
		Hostname: v.Hostname, SubjectAltNames: slices.Clone(v.SubjectAltNames),
		CAConfigMapUID: string(v.CA.UID), CADigest: v.CA.Digest, CAPEM: slices.Clone(v.CA.PEM),
	}
}

// CA identifies the CA source and the exact normalized trust payload.
type CA struct {
	Key    types.NamespacedName
	UID    types.UID
	Digest string
	PEM    []byte `json:"-"`
}

const caKey = "ca.crt"

func validate(result *PolicyResult, spec gatewayv1.BackendTLSPolicySpec, inputs CAInputs) {
	policy := spec.Validation
	switch {
	case policy.WellKnownCACertificates != nil:
		message := "Well-known CA certificate sets are unsupported"
		if len(policy.CACertificateRefs) > 0 {
			message = "Specify either caCertificateRefs or wellKnownCACertificates"
			result.rejectRef(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef, message)
		}
		result.reject(gatewayv1.PolicyReasonInvalid, message)
		return
	case len(policy.CACertificateRefs) == 0:
		message := "A CA certificate reference is required"
		result.reject(gatewayv1.PolicyReasonInvalid, message)
		result.rejectRef(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef, message)
		return
	case len(policy.CACertificateRefs) > 1:
		message := "BackendTLSPolicy supports a single CA certificate reference"
		result.reject(gatewayv1.PolicyReasonInvalid, message)
		result.rejectRef("UnsupportedCACertificateRefs", message)
		return
	}

	ref := policy.CACertificateRefs[0]
	var observation CAObservation
	switch {
	case ref.Group == "" && ref.Kind == "ConfigMap":
		key := types.NamespacedName{Namespace: result.Key.Namespace, Name: string(ref.Name)}
		observation.CA, observation.Invalid = LoadConfigMapCA(key, inputs.ConfigMaps[key])
		if observation.Invalid != nil {
			observation.Invalid = fmt.Errorf("CA ConfigMap %s: %w", key, observation.Invalid)
		}
	case IsClusterTrustBundle(ref.Group, ref.Kind) && inputs.ClusterTrustBundles != nil:
		observation = inputs.ClusterTrustBundles[string(ref.Name)]
		if !ValidClusterTrustBundleName(string(ref.Name)) {
			observation.Invalid = errors.New("name is invalid")
		}
		if observation.Invalid != nil {
			observation.Invalid = fmt.Errorf("CA ClusterTrustBundle %q: %w", ref.Name, observation.Invalid)
		}
	default:
		message := fmt.Sprintf("CA reference %q has unsupported group %q and kind %q", ref.Name, ref.Group, ref.Kind)
		result.reject(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, message)
		result.rejectRef(gatewayv1.BackendTLSPolicyReasonInvalidKind, message)
		return
	}
	if observation.Unavailable != nil {
		for _, condition := range []*metav1.Condition{&result.Accepted, &result.ResolvedRefs} {
			condition.Status = metav1.ConditionUnknown
			condition.Reason = "Pending"
			condition.Message = "Waiting to read the referenced ClusterTrustBundle"
		}
		return
	}
	if observation.Invalid != nil {
		result.reject(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, observation.Invalid.Error())
		reason := gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef
		if errors.Is(observation.Invalid, ErrCABundleTooLarge) {
			reason = gatewayv1.PolicyConditionReason(CABundleTooLargeReason)
		}
		result.rejectRef(reason, observation.Invalid.Error())
		return
	}

	hostname := string(policy.Hostname)
	switch {
	case !isPreciseHostname(hostname):
		result.reject(gatewayv1.PolicyReasonInvalid, "Backend TLS hostname must be a precise DNS hostname")
	case len(spec.Options) > 0:
		result.reject(gatewayv1.PolicyReasonInvalid, "BackendTLSPolicy TLS options are unsupported")
	default:
		names := make([]cloud.BackendTLSSubjectAltName, 0, len(policy.SubjectAltNames))
		for _, name := range policy.SubjectAltNames {
			names = append(names, cloud.BackendTLSSubjectAltName{
				Type: string(name.Type), Hostname: string(name.Hostname), URI: string(name.URI),
			})
		}
		names, err := amphora.NormalizeBackendTLSSubjectAltNames(names)
		if err != nil {
			result.reject(gatewayv1.PolicyReasonInvalid, err.Error())
			return
		}
		result.Validation = &Validation{Hostname: hostname, CA: observation.CA, SubjectAltNames: names}
	}
}

func isPreciseHostname(hostname string) bool {
	if len(validation.IsDNS1123Subdomain(hostname)) > 0 {
		return false
	}
	if _, err := netip.ParseAddr(hostname); err == nil {
		return false
	}
	for label := range strings.SplitSeq(hostname, ".") {
		if len(validation.IsDNS1123Label(label)) > 0 {
			return false
		}
	}
	return true
}

// LoadConfigMapCA validates and normalizes the single core ConfigMap CA.
func LoadConfigMapCA(key types.NamespacedName, configMap *corev1.ConfigMap) (CA, error) {
	if configMap == nil || configMap.Namespace != key.Namespace || configMap.Name != key.Name ||
		!configMap.DeletionTimestamp.IsZero() {
		return CA{}, errors.New("is absent or unreadable")
	}
	text, hasText := configMap.Data[caKey]
	binary, hasBinary := configMap.BinaryData[caKey]
	if hasText && hasBinary {
		return CA{}, errors.New("contains more than one ca.crt value")
	}
	if !hasText && !hasBinary {
		return CA{}, errors.New("is missing ca.crt")
	}
	if hasText {
		binary = []byte(text)
	}
	normalized, err := normalizeCA(binary)
	if err != nil {
		return CA{}, err
	}
	digest := sha256.Sum256(normalized)
	return CA{Key: key, UID: configMap.UID, Digest: hex.EncodeToString(digest[:]), PEM: normalized}, nil
}

func normalizeCA(data []byte) ([]byte, error) {
	data = bytes.TrimSpace(data)
	// pem.Decode searches for a valid block even after unrelated or broken
	// material. Require an exact single certificate before publishing any bytes.
	block, rest := pem.Decode(data)
	if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) ||
		bytes.Count(data, []byte("-----BEGIN ")) != 1 ||
		block == nil || block.Type != "CERTIFICATE" || len(block.Headers) > 0 ||
		len(bytes.TrimSpace(rest)) > 0 {
		return nil, errors.New("ca.crt must contain a single PEM certificate")
	}
	cert, err := parseCACertificate(block)
	if err != nil {
		return nil, fmt.Errorf("ca.crt %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), nil
}

func parseCACertificate(block *pem.Block) ([]byte, error) {
	if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
		return nil, errors.New("must contain only PEM certificates without headers")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !cert.BasicConstraintsValid || !cert.IsCA ||
		(cert.KeyUsage != 0 && cert.KeyUsage&x509.KeyUsageCertSign == 0) {
		return nil, errors.New("must contain an X.509 CA certificate")
	}
	return cert.Raw, nil
}
