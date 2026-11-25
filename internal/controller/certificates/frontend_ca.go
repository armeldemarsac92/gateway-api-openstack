// Package certificates resolves operator-enrolled TLS certificate revisions.
package certificates

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

// ResolveFrontendCA checks a ReferenceGrant before reading a ConfigMap from
// another namespace, or resolves a named cluster-scoped trust bundle directly.
// The Reader must be the uncached Kubernetes API reader.
func ResolveFrontendCA(ctx context.Context, reader client.Reader, gateway *gatewayv1.Gateway, refs []gatewayv1.ObjectReference, bundles *backendtls.ClusterTrustBundles) (*cloud.FrontendClientValidation, gatewayv1.ListenerConditionReason, string, error) {
	if len(refs) != 1 {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, "Client certificate validation requires exactly one CA reference", nil
	}
	ref := refs[0]
	if backendtls.IsClusterTrustBundle(ref.Group, ref.Kind) && bundles != nil {
		if ref.Namespace != nil {
			return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, "Client CA ClusterTrustBundle references require an unset namespace", nil
		}
		observation, err := bundles.Read(ctx, string(ref.Name))
		if err != nil {
			return nil, "", "", err
		}
		if observation.Invalid != nil {
			message := fmt.Sprintf("Client CA ClusterTrustBundle %q %v", ref.Name, observation.Invalid)
			reason := gatewayv1.ListenerReasonInvalidCACertificateRef
			if errors.Is(observation.Invalid, backendtls.ErrCABundleTooLarge) {
				reason = gatewayv1.ListenerConditionReason(backendtls.CABundleTooLargeReason)
			}
			return nil, reason, message, nil
		}
		ca := observation.CA
		return &cloud.FrontendClientValidation{ConfigMapUID: string(ca.UID), Digest: ca.Digest, PEM: ca.PEM}, "", "", nil
	}
	if ref.Group != "" || ref.Kind != "ConfigMap" {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateKind, fmt.Sprintf("CA reference kind %q in group %q is unsupported", ref.Kind, ref.Group), nil
	}
	target, valid := FrontendCAKey(gateway.Namespace, ref)
	if !valid {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, "Client CA ConfigMap name or namespace is invalid", nil
	}
	description := "Client CA ConfigMap " + target.String()
	if reader == nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " could not be read", nil
	}
	resolver := Resolver{Reader: reader}
	if allowed, observed := resolver.referenceAllowed(ctx, gatewayv1.GroupName, "Gateway", gateway.Namespace, target, "", "ConfigMap"); !observed {
		return nil, gatewayv1.ListenerReasonRefNotPermitted, description + " has no verifiable ReferenceGrant", nil
	} else if !allowed {
		return nil, gatewayv1.ListenerReasonRefNotPermitted, description + " is not permitted by a ReferenceGrant", nil
	}
	var configMap corev1.ConfigMap
	if err := reader.Get(ctx, target, &configMap); err != nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " is absent or unreadable", nil
	}
	ca, err := backendtls.LoadConfigMapCA(target, &configMap)
	if err != nil {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " " + err.Error(), nil
	}
	if ca.UID == "" {
		return nil, gatewayv1.ListenerReasonInvalidCACertificateRef, description + " has no UID", nil
	}
	return &cloud.FrontendClientValidation{ConfigMapUID: string(ca.UID), Digest: ca.Digest, PEM: ca.PEM}, "", "", nil
}

// FrontendCAKey returns the scoped object key for a supported CA reference.
func FrontendCAKey(namespace string, ref gatewayv1.ObjectReference) (client.ObjectKey, bool) {
	if backendtls.IsClusterTrustBundle(ref.Group, ref.Kind) {
		return client.ObjectKey{Name: string(ref.Name)}, ref.Namespace == nil && backendtls.ValidClusterTrustBundleName(string(ref.Name))
	}
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	target := client.ObjectKey{Namespace: namespace, Name: string(ref.Name)}
	return target, ref.Group == "" && ref.Kind == "ConfigMap" && validName(target)
}
