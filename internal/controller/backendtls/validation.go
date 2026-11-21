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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Validation requires the hostname for both backend SNI and certificate identity.
type Validation struct {
	Hostname string
	CA       CA
}

// CA identifies the ConfigMap and the exact normalized trust payload.
type CA struct {
	Key    types.NamespacedName
	UID    types.UID
	Digest string
	PEM    []byte
}

const caKey = "ca.crt"

func validate(result *PolicyResult, spec gatewayv1.BackendTLSPolicySpec, configMaps map[types.NamespacedName]*corev1.ConfigMap) {
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
	if ref.Group != "" || ref.Kind != "ConfigMap" {
		message := fmt.Sprintf("CA reference %q has unsupported group %q and kind %q", ref.Name, ref.Group, ref.Kind)
		result.reject(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, message)
		result.rejectRef(gatewayv1.BackendTLSPolicyReasonInvalidKind, message)
		return
	}
	key := types.NamespacedName{Namespace: result.Key.Namespace, Name: string(ref.Name)}
	ca, err := loadCA(key, configMaps[key])
	if err != nil {
		message := fmt.Sprintf("CA ConfigMap %s: %v", key, err)
		result.reject(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, message)
		result.rejectRef(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef, message)
		return
	}

	hostname := string(policy.Hostname)
	switch {
	case len(policy.SubjectAltNames) > 0:
		result.reject(gatewayv1.PolicyReasonInvalid, "Explicit subjectAltNames require unsupported BackendTLSPolicySANValidation")
	case !isPreciseHostname(hostname):
		result.reject(gatewayv1.PolicyReasonInvalid, "Backend TLS hostname must be a precise DNS hostname")
	case len(spec.Options) > 0:
		result.reject(gatewayv1.PolicyReasonInvalid, "BackendTLSPolicy TLS options are unsupported")
	default:
		result.Validation = &Validation{Hostname: hostname, CA: ca}
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

func loadCA(key types.NamespacedName, configMap *corev1.ConfigMap) (CA, error) {
	if configMap == nil || configMap.Namespace != key.Namespace || configMap.Name != key.Name ||
		!configMap.DeletionTimestamp.IsZero() {
		return CA{}, errors.New("does not exist")
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
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !cert.BasicConstraintsValid || !cert.IsCA ||
		(cert.KeyUsage != 0 && cert.KeyUsage&x509.KeyUsageCertSign == 0) {
		return nil, errors.New("ca.crt must contain an X.509 CA certificate")
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), nil
}
