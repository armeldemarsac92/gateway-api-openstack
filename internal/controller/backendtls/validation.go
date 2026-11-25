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

// ValidationResult is the resolved trust and identity, or the diagnostic
// shared by policy status and resources with inline backend TLS validation.
type ValidationResult struct {
	Validation *Validation
	Reason     gatewayv1.PolicyConditionReason
	RefReason  gatewayv1.PolicyConditionReason
	Message    string
	Pending    bool
}

func validate(result *PolicyResult, spec gatewayv1.BackendTLSPolicySpec, inputs CAInputs) {
	validation := EvaluateValidation(result.Key.Namespace, spec.Validation, inputs)
	switch {
	case validation.Pending:
		for _, condition := range []*metav1.Condition{&result.Accepted, &result.ResolvedRefs} {
			condition.Status = metav1.ConditionUnknown
			condition.Reason = string(validation.Reason)
			condition.Message = validation.Message
		}
	case validation.Validation == nil:
		if validation.RefReason != "" {
			result.rejectRef(validation.RefReason, validation.Message)
		}
		result.reject(validation.Reason, validation.Message)
	case len(spec.Options) > 0:
		result.reject(gatewayv1.PolicyReasonInvalid, "BackendTLSPolicy TLS options are unsupported")
	default:
		result.Validation = validation.Validation
	}
}

// EvaluateValidation resolves the CA and certificate identity from live input snapshots.
func EvaluateValidation(namespace string, policy gatewayv1.BackendTLSPolicyValidation, inputs CAInputs) ValidationResult {
	invalid := func(message string, refReason gatewayv1.PolicyConditionReason) ValidationResult {
		return ValidationResult{Reason: gatewayv1.PolicyReasonInvalid, RefReason: refReason, Message: message}
	}
	switch {
	case policy.WellKnownCACertificates != nil:
		if len(policy.CACertificateRefs) > 0 {
			return invalid("Specify either caCertificateRefs or wellKnownCACertificates", gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)
		}
		return invalid("Well-known CA certificate sets are unsupported", "")
	case len(policy.CACertificateRefs) == 0:
		return invalid("A CA certificate reference is required", gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)
	case len(policy.CACertificateRefs) > 1:
		return invalid("Backend TLS supports a single CA certificate reference", "UnsupportedCACertificateRefs")
	}

	ref := policy.CACertificateRefs[0]
	var observation CAObservation
	switch {
	case ref.Group == "" && ref.Kind == "ConfigMap":
		key := types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}
		observation.CA, observation.Invalid = LoadConfigMapCA(key, inputs.ConfigMaps[key])
		if observation.Invalid != nil {
			observation.Invalid = fmt.Errorf("CA ConfigMap %s: %w", key, observation.Invalid)
		}
	case IsClusterTrustBundle(ref.Group, ref.Kind) && inputs.ClusterTrustBundles != nil:
		observation = inputs.ClusterTrustBundles[string(ref.Name)]
		if !ValidClusterTrustBundleName(string(ref.Name)) {
			observation.Invalid = errors.New("name is invalid")
		} else if observation.Unavailable == nil && observation.Invalid == nil && observation.CA.UID == "" {
			observation.Invalid = errors.New("has no live cluster-scoped revision")
		}
		if observation.Invalid != nil {
			observation.Invalid = fmt.Errorf("CA ClusterTrustBundle %q: %w", ref.Name, observation.Invalid)
		}
	default:
		return ValidationResult{Reason: gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, RefReason: gatewayv1.BackendTLSPolicyReasonInvalidKind,
			Message: fmt.Sprintf("CA reference %q has unsupported group %q and kind %q", ref.Name, ref.Group, ref.Kind)}
	}
	if observation.Unavailable != nil {
		return ValidationResult{Reason: "Pending", Pending: true, Message: fmt.Sprintf("Waiting to read the referenced ClusterTrustBundle %q", ref.Name)}
	}
	if observation.Invalid != nil {
		reason := gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef
		if errors.Is(observation.Invalid, ErrCABundleTooLarge) {
			reason = gatewayv1.PolicyConditionReason(CABundleTooLargeReason)
		}
		return ValidationResult{Reason: gatewayv1.BackendTLSPolicyReasonNoValidCACertificate, RefReason: reason, Message: observation.Invalid.Error()}
	}

	hostname := string(policy.Hostname)
	if !IsPreciseHostname(hostname) {
		return invalid("Backend TLS hostname must be a precise DNS hostname", "")
	}
	names := make([]cloud.BackendTLSSubjectAltName, 0, len(policy.SubjectAltNames))
	for _, name := range policy.SubjectAltNames {
		names = append(names, cloud.BackendTLSSubjectAltName{
			Type: string(name.Type), Hostname: string(name.Hostname), URI: string(name.URI),
		})
	}
	names, err := amphora.NormalizeBackendTLSSubjectAltNames(names)
	if err != nil {
		return invalid(err.Error(), "")
	}
	return ValidationResult{Validation: &Validation{Hostname: hostname, CA: observation.CA, SubjectAltNames: names}}
}

// IsPreciseHostname reports whether hostname can be used for backend TLS SNI.
func IsPreciseHostname(hostname string) bool {
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
