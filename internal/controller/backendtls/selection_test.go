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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestEvaluateSectionPrecedence(t *testing.T) {
	service := testService()
	ca := testCertificate(t, true)
	maps := testConfigMaps(ca)
	whole := testPolicy("whole", "")
	youngerWhole := testPolicy("aaa-younger", "")
	youngerWhole.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	named := testPolicy("named-a", "https-1")
	lexicalLoser := testPolicy("named-z", "https-1")
	absentPort := testPolicy("missing-port", "443")
	policies := []gatewayv1.BackendTLSPolicy{lexicalLoser, youngerWhole, named, whole, absentPort}

	first := Evaluate(service, &service.Spec.Ports[0], policies, maps)
	require.NotNil(t, first.Winner)
	require.NotNil(t, first.Winner.Validation)
	require.Equal(t, named.Name, first.Winner.Key.Name)
	requireCondition(t, first.Winner.Accepted, metav1.ConditionTrue, gatewayv1.PolicyReasonAccepted)
	requireCondition(t, findPolicy(t, first, whole.Name).Accepted, metav1.ConditionTrue, gatewayv1.PolicyReasonAccepted)
	requireCondition(t, findPolicy(t, first, youngerWhole.Name).Accepted, metav1.ConditionFalse, gatewayv1.PolicyReasonConflicted)
	requireCondition(t, findPolicy(t, first, lexicalLoser.Name).Accepted, metav1.ConditionFalse, gatewayv1.PolicyReasonConflicted)
	requireCondition(t, findPolicy(t, first, absentPort.Name).ResolvedRefs, metav1.ConditionFalse, gatewayv1.PolicyReasonTargetNotFound)

	second := Evaluate(service, &service.Spec.Ports[1], policies, maps)
	require.NotNil(t, second.Winner)
	require.Equal(t, whole.Name, second.Winner.Key.Name)
	require.Len(t, second.Policies, 3)
}

func TestEvaluateWinnerLifecycle(t *testing.T) {
	service := testService()
	maps := testConfigMaps(testCertificate(t, true))
	whole := testPolicy("whole", "")
	oldest := testPolicy("oldest", "https-1")
	oldest.Spec.Validation.CACertificateRefs[0].Name = "absent"
	younger := testPolicy("younger", "https-1")
	younger.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	policies := []gatewayv1.BackendTLSPolicy{younger, oldest, whole}

	evaluate := func() Selection { return Evaluate(service, &service.Spec.Ports[0], policies, maps) }
	result := evaluate()
	require.NotNil(t, result.Winner)
	require.Nil(t, result.Winner.Validation)
	require.Equal(t, oldest.Name, result.Winner.Key.Name)
	requireCondition(t, result.Winner.Accepted, metav1.ConditionFalse, gatewayv1.BackendTLSPolicyReasonNoValidCACertificate)
	requireCondition(t, findPolicy(t, result, younger.Name).Accepted, metav1.ConditionFalse, gatewayv1.PolicyReasonConflicted)

	policies[1].DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(3, 0)))
	result = evaluate()
	require.NotNil(t, result.Winner)
	require.NotNil(t, result.Winner.Validation)
	require.Equal(t, younger.Name, result.Winner.Key.Name)

	policies[0].DeletionTimestamp = policies[1].DeletionTimestamp
	result = evaluate()
	require.NotNil(t, result.Winner)
	require.Equal(t, whole.Name, result.Winner.Key.Name)
	require.Len(t, result.Policies, 1)
}

func TestEvaluateTargetBoundaries(t *testing.T) {
	service := testService()
	port := &service.Spec.Ports[0]
	maps := testConfigMaps(testCertificate(t, true))
	foreign := testPolicy("foreign", "")
	foreign.Namespace = "other"
	otherKind := testPolicy("not-a-service", "")
	otherKind.Spec.TargetRefs[0].Group = "example.com"
	otherPort := testPolicy("other-port", "https-2")
	policies := []gatewayv1.BackendTLSPolicy{foreign, otherKind, otherPort}
	require.Empty(t, Evaluate(service, port, policies, maps))

	multiple := testPolicy("multiple-targets", "https-1")
	otherTarget := multiple.Spec.TargetRefs[0]
	otherTarget.Name = "other-service"
	multiple.Spec.TargetRefs = append(multiple.Spec.TargetRefs, otherTarget)
	policies = append(policies, multiple)
	result := Evaluate(service, port, policies, maps)
	require.NotNil(t, result.Winner)
	require.Nil(t, result.Winner.Validation)
	requireCondition(t, result.Winner.Accepted, metav1.ConditionFalse, gatewayv1.PolicyReasonInvalid)
	requireCondition(t, result.Winner.ResolvedRefs, metav1.ConditionFalse, "UnsupportedTargetRefs")

	single := testPolicy("single", "https-1")
	policies = append(policies, single)
	result = Evaluate(service, port, policies, maps)
	require.NotNil(t, result.Winner)
	require.NotNil(t, result.Winner.Validation)
	require.Equal(t, single.Name, result.Winner.Key.Name)
}

func TestEvaluateCAAndPolicyUpdates(t *testing.T) {
	service := testService()
	ca := testCertificate(t, true)
	maps := testConfigMaps("\n" + ca + "\n")
	policies := []gatewayv1.BackendTLSPolicy{testPolicy("policy", "")}
	evaluate := func() *PolicyResult {
		t.Helper()
		result := Evaluate(service, &service.Spec.Ports[0], policies, maps)
		require.NotNil(t, result.Winner)
		require.NotNil(t, result.Winner.Validation)
		return result.Winner
	}

	original := evaluate()
	key := types.NamespacedName{Namespace: service.Namespace, Name: "ca"}
	require.Equal(t, key, original.Validation.CA.Key)
	require.Equal(t, ca, string(original.Validation.CA.PEM))
	require.Equal(t, maps[key].UID, original.Validation.CA.UID)
	requireCondition(t, original.ResolvedRefs, metav1.ConditionTrue, gatewayv1.BackendTLSPolicyReasonResolvedRefs)

	maps[key].Data = nil
	maps[key].BinaryData = map[string][]byte{"ca.crt": []byte(ca)}
	require.Equal(t, original.Validation.CA.Digest, evaluate().Validation.CA.Digest)

	maps[key].BinaryData["ca.crt"] = nil
	invalid := Evaluate(service, &service.Spec.Ports[0], policies, maps)
	requireRejectedCA(t, invalid.Winner, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)

	maps[key].BinaryData["ca.crt"] = []byte(testCertificate(t, true))
	rotated := evaluate()
	require.NotEqual(t, original.Validation.CA.Digest, rotated.Validation.CA.Digest)
	require.Equal(t, original.Validation.CA.UID, rotated.Validation.CA.UID)
	require.Equal(t, original.Accepted.ObservedGeneration, rotated.Accepted.ObservedGeneration)

	policies[0].Generation++
	policies[0].Spec.Validation.Hostname = "rotated.example.com"
	changed := evaluate()
	require.Equal(t, "rotated.example.com", changed.Validation.Hostname)
	require.Equal(t, policies[0].Generation, changed.Accepted.ObservedGeneration)
	require.Equal(t, policies[0].Generation, changed.ResolvedRefs.ObservedGeneration)

	maps[key].UID = "replacement"
	recreated := evaluate()
	require.Equal(t, changed.Validation.CA.Digest, recreated.Validation.CA.Digest)
	require.NotEqual(t, changed.Validation.CA.UID, recreated.Validation.CA.UID)
}

func TestEvaluateInvalidCA(t *testing.T) {
	service := testService()
	port := &service.Spec.Ports[0]
	ca := testCertificate(t, true)
	leaf := testCertificate(t, false)
	key := types.NamespacedName{Namespace: service.Namespace, Name: "ca"}
	privateKeyBlock := "-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----\n"
	for _, tt := range []struct {
		name   string
		update func(*gatewayv1.LocalObjectReference, map[types.NamespacedName]*corev1.ConfigMap)
		reason gatewayv1.PolicyConditionReason
	}{
		{"missing object", func(_ *gatewayv1.LocalObjectReference, maps map[types.NamespacedName]*corev1.ConfigMap) {
			delete(maps, key)
		}, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef},
		{"foreign object", func(_ *gatewayv1.LocalObjectReference, maps map[types.NamespacedName]*corev1.ConfigMap) {
			maps[key].Namespace = "other"
		}, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef},
		{"missing key", func(_ *gatewayv1.LocalObjectReference, maps map[types.NamespacedName]*corev1.ConfigMap) {
			delete(maps[key].Data, "ca.crt")
		}, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef},
		{"unsupported kind", func(ref *gatewayv1.LocalObjectReference, _ map[types.NamespacedName]*corev1.ConfigMap) {
			ref.Kind = "Secret"
		}, gatewayv1.BackendTLSPolicyReasonInvalidKind},
		{"unsupported group", func(ref *gatewayv1.LocalObjectReference, _ map[types.NamespacedName]*corev1.ConfigMap) {
			ref.Group = "example.com"
		}, gatewayv1.BackendTLSPolicyReasonInvalidKind},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := testPolicy("policy", "")
			maps := testConfigMaps(ca)
			tt.update(&policy.Spec.Validation.CACertificateRefs[0], maps)
			result := Evaluate(service, port, []gatewayv1.BackendTLSPolicy{policy}, maps)
			requireRejectedCA(t, result.Winner, tt.reason)
		})
	}
	for _, tt := range []struct{ name, content string }{
		{"empty", ""}, {"malformed X509", "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n"},
		{"leaf", leaf}, {"leading private key", privateKeyBlock + ca}, {"trailing private key", ca + privateKeyBlock},
		{"certificate bundle", ca + ca},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Evaluate(service, port, []gatewayv1.BackendTLSPolicy{testPolicy("policy", "")}, testConfigMaps(tt.content))
			requireRejectedCA(t, result.Winner, gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)
		})
	}
}

func TestEvaluateUnsupportedTLSValidation(t *testing.T) {
	service := testService()
	maps := testConfigMaps(testCertificate(t, true))
	for _, tt := range []struct {
		name       string
		change     func(*gatewayv1.BackendTLSPolicyValidation)
		diagnostic string
	}{
		{"system trust", func(v *gatewayv1.BackendTLSPolicyValidation) {
			v.CACertificateRefs = nil
			v.WellKnownCACertificates = ptr.To(gatewayv1.WellKnownCACertificatesSystem)
		}, "Well-known"},
		{"IP SAN", func(v *gatewayv1.BackendTLSPolicyValidation) {
			v.SubjectAltNames = []gatewayv1.SubjectAltName{{Type: gatewayv1.HostnameSubjectAltNameType, Hostname: "192.0.2.1"}}
		}, "subjectAltNames"},
		{"malformed URI SAN", func(v *gatewayv1.BackendTLSPolicyValidation) {
			v.SubjectAltNames = []gatewayv1.SubjectAltName{{Type: gatewayv1.URISubjectAltNameType, URI: "spiffe://id.example.test/workload%2g"}}
		}, "subjectAltNames"},
		{"multiple SAN values in one entry", func(v *gatewayv1.BackendTLSPolicyValidation) {
			v.SubjectAltNames = []gatewayv1.SubjectAltName{{Type: gatewayv1.HostnameSubjectAltNameType, Hostname: "backend.example.com", URI: "urn:example:backend"}}
		}, "subjectAltNames"},
		{"too many SANs", func(v *gatewayv1.BackendTLSPolicyValidation) {
			v.SubjectAltNames = make([]gatewayv1.SubjectAltName, 6)
		}, "at most five"},
		{"wildcard SNI", func(v *gatewayv1.BackendTLSPolicyValidation) { v.Hostname = "*.example.com" }, "precise DNS hostname"},
		{"IP SNI", func(v *gatewayv1.BackendTLSPolicyValidation) { v.Hostname = "192.0.2.1" }, "precise DNS hostname"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := testPolicy("policy", "")
			tt.change(&policy.Spec.Validation)
			result := Evaluate(service, &service.Spec.Ports[0], []gatewayv1.BackendTLSPolicy{policy}, maps)
			require.NotNil(t, result.Winner)
			require.Nil(t, result.Winner.Validation)
			requireCondition(t, result.Winner.Accepted, metav1.ConditionFalse, gatewayv1.PolicyReasonInvalid)
			require.Contains(t, result.Winner.Accepted.Message, tt.diagnostic)
			requireCondition(t, result.Winner.ResolvedRefs, metav1.ConditionTrue, gatewayv1.BackendTLSPolicyReasonResolvedRefs)
		})
	}
}

func TestEvaluateBackendTLSSubjectAltNames(t *testing.T) {
	service := testService()
	policy := testPolicy("policy", "")
	policy.Spec.Validation.Hostname = "sni.example.test"
	policy.Spec.Validation.SubjectAltNames = []gatewayv1.SubjectAltName{
		{Type: gatewayv1.URISubjectAltNameType, URI: "spiffe://ID.example.test/workload%2fa"},
		{Type: gatewayv1.HostnameSubjectAltNameType, Hostname: "*.backend.example.test"},
	}
	result := Evaluate(service, &service.Spec.Ports[0], []gatewayv1.BackendTLSPolicy{policy}, testConfigMaps(testCertificate(t, true)))
	require.NotNil(t, result.Winner)
	requireCondition(t, result.Winner.Accepted, metav1.ConditionTrue, gatewayv1.PolicyReasonAccepted)
	require.NotNil(t, result.Winner.Validation)
	require.Equal(t, "sni.example.test", result.Winner.Validation.Hostname)
	require.Equal(t, []cloud.BackendTLSSubjectAltName{
		{Type: cloud.BackendTLSSANHostname, Hostname: "*.backend.example.test"},
		{Type: cloud.BackendTLSSANURI, URI: "spiffe://ID.example.test/workload%2fa"},
	}, result.Winner.Validation.SubjectAltNames)

	clone := result.Clone()
	clone.Winner.Validation.SubjectAltNames[0].Hostname = "changed.example.test"
	require.Equal(t, "*.backend.example.test", result.Winner.Validation.SubjectAltNames[0].Hostname)
}

func requireRejectedCA(t *testing.T, result *PolicyResult, reason gatewayv1.PolicyConditionReason) {
	t.Helper()
	require.NotNil(t, result)
	require.Nil(t, result.Validation)
	requireCondition(t, result.Accepted, metav1.ConditionFalse, gatewayv1.BackendTLSPolicyReasonNoValidCACertificate)
	requireCondition(t, result.ResolvedRefs, metav1.ConditionFalse, reason)
	require.Contains(t, result.ResolvedRefs.Message, "ca")
}

func requireCondition(t *testing.T, condition metav1.Condition, status metav1.ConditionStatus, reason gatewayv1.PolicyConditionReason) {
	t.Helper()
	require.Equal(t, status, condition.Status)
	require.Equal(t, string(reason), condition.Reason)
}

func findPolicy(t *testing.T, selection Selection, name string) PolicyResult {
	t.Helper()
	for _, policy := range selection.Policies {
		if policy.Key.Name == name {
			return policy
		}
	}
	t.Fatalf("no result for policy %q", name)
	return PolicyResult{}
}

func testService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "backend"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
			{Name: "https-1", Port: 443}, {Name: "https-2", Port: 8443},
		}},
	}
}

func testPolicy(name, section string) gatewayv1.BackendTLSPolicy {
	target := gatewayv1.LocalPolicyTargetReferenceWithSectionName{
		LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{Kind: "Service", Name: "backend"},
	}
	if section != "" {
		target.SectionName = ptr.To(gatewayv1.SectionName(section))
	}
	return gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "infra", Name: name, UID: types.UID(name), Generation: 3,
			CreationTimestamp: metav1.NewTime(time.Unix(1, 0)),
		},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{target},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				Hostname:          "backend.example.com",
				CACertificateRefs: []gatewayv1.LocalObjectReference{{Kind: "ConfigMap", Name: "ca"}},
			},
		},
	}
}

func testConfigMaps(certificate string) map[types.NamespacedName]*corev1.ConfigMap {
	key := types.NamespacedName{Namespace: "infra", Name: "ca"}
	return map[types.NamespacedName]*corev1.ConfigMap{
		key: {
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: "config-map-uid"},
			Data:       map[string]string{"ca.crt": certificate},
		},
	}
}

func testCertificate(t *testing.T, ca bool) string {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), BasicConstraintsValid: true, IsCA: ca,
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(4_100_000_000, 0),
	}
	if ca {
		certificate.KeyUsage = x509.KeyUsageCertSign
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
