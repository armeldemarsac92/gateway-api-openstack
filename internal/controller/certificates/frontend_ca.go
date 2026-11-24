// Package certificates resolves operator-enrolled TLS certificate revisions.
package certificates

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

// ResolveFrontendCA checks the named ReferenceGrant before reading a ConfigMap
// from another namespace. The Reader must be the uncached Kubernetes API reader.
func ResolveFrontendCA(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, refs []gatewayv1.ObjectReference) (*cloud.FrontendClientValidation, gatewayv1.ListenerConditionReason, string) {
	if len(refs) != 1 {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, "Client certificate validation requires exactly one CA ConfigMap reference"
	}
	ref := refs[0]
	if ref.Group != "" || ref.Kind != "ConfigMap" {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateKind, fmt.Sprintf("CA reference kind %q in group %q is unsupported; use a core ConfigMap", ref.Kind, ref.Group)
	}
	target, valid := FrontendCAKey(gateway.Namespace, ref)
	if !valid {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, "Client CA ConfigMap name or namespace is invalid"
	}
	description := "Client CA ConfigMap " + target.String()
	if reader == nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " could not be read"
	}
	resolver := Resolver{Reader: reader}
	if allowed, observed := resolver.referenceAllowed(ctx, "Gateway", gateway.Namespace, target, "", "ConfigMap"); !observed {
		return nil, gatewayv1.ListenerReasonRefNotPermitted, description + " has no verifiable ReferenceGrant"
	} else if !allowed {
		return nil, gatewayv1.ListenerReasonRefNotPermitted, description + " is not permitted by a ReferenceGrant"
	}
	var configMap corev1.ConfigMap
	if err := reader.Get(ctx, target, &configMap); err != nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " is absent or unreadable"
	}
	ca, err := backendtls.LoadConfigMapCA(target, &configMap)
	if err != nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " " + err.Error()
	}
	if ca.UID == "" {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " has no UID"
	}
	return &cloud.FrontendClientValidation{ConfigMapUID: string(ca.UID), Digest: ca.Digest, PEM: ca.PEM}, "", ""
}

// FrontendCAKey returns the exact supported ConfigMap watch target.
func FrontendCAKey(namespace string, ref gatewayv1.ObjectReference) (client.ObjectKey, bool) {
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	target := client.ObjectKey{Namespace: namespace, Name: string(ref.Name)}
	return target, ref.Group == "" && ref.Kind == "ConfigMap" && validName(target)
}
