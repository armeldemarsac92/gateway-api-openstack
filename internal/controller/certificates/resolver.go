// Package certificates resolves operator-enrolled TLS certificate revisions.
package certificates

import (
	"context"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
)

const (
	BackendClientPurpose     = "backend-client"
	XBackendClientPurpose    = "xbackend-client"
	backendClientUnavailable = "BackendClientCertificateUnavailable"
	backendClientNotCurrent  = "BackendClientCertificateNotCurrent"
)

// Resolver reads a protected enrollment snapshot and uses the uncached Reader
// to check current ReferenceGrants and Secret metadata.
type Resolver struct {
	Path               string
	BackendClientPath  string
	XBackendClientPath string
	ClusterID          string
	ProjectID          string
	Region             string
	Endpoint           string
	DiscoverEndpoint   func() (string, error)
	Reader             client.Reader
}

// Resolve returns a Barbican reference only for the live revision of an enrolled
// Secret. The endpoint comes from the authenticated key-manager catalog.
func (r *Resolver) Resolve(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	row, reason := r.authorizedBinding(ctx, gateway, listener)
	if reason != "" {
		return "", reason
	}
	return r.barbicanRef(row)
}

// ResolveListenerSet resolves the serving certificate of a delegated listener.
// The Gateway supplies the enrollment scope, and the ListenerSet authorizes
// the reference as its owner.
func (r *Resolver) ResolveListenerSet(ctx context.Context, gateway *gatewayv1.Gateway, set *gatewayv1.ListenerSet, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	if set == nil {
		return "", gatewayv1.ListenerReasonInvalidCertificateRef
	}
	row, reason := r.authorizedBindingFor(ctx, gateway, "ListenerSet", set.Namespace, listener)
	if reason != "" {
		return "", reason
	}
	return r.barbicanRef(row)
}

// ListenerSetReferenceStatus resolves listener references without contacting Barbican.
func (r *Resolver) ListenerSetReferenceStatus(ctx context.Context, gateway *gatewayv1.Gateway, set *gatewayv1.ListenerSet, listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	if set == nil {
		return gatewayv1.ListenerReasonInvalidCertificateRef
	}
	_, reason := r.authorizedBindingFor(ctx, gateway, "ListenerSet", set.Namespace, listener)
	return reason
}

// ResolveBackendClientCertificate uses the operator's backend identity enrollment
// and checks the grant before reading Secret metadata or discovering Barbican.
func (r *Resolver) ResolveBackendClientCertificate(ctx context.Context, gateway *gatewayv1.Gateway) (string, gatewayv1.GatewayConditionReason, string) {
	if gateway == nil || gateway.Spec.TLS == nil || gateway.Spec.TLS.Backend == nil || gateway.Spec.TLS.Backend.ClientCertificateRef == nil {
		return "", gatewayv1.GatewayReasonInvalidClientCertificateRef, "The Gateway has no backend client certificate reference"
	}
	return r.resolveBackendClientCertificate(ctx, gateway.Spec.GatewayClassName, gatewayv1.GroupName, "Gateway", gateway.Namespace,
		BackendClientPurpose, *gateway.Spec.TLS.Backend.ClientCertificateRef)
}

// ResolveXBackendClientCertificate checks the XBackend's own enrollment and ReferenceGrant.
func (r *Resolver) ResolveXBackendClientCertificate(ctx context.Context, class gatewayv1.ObjectName, backend *gatewayx.XBackend) (string, gatewayv1.RouteConditionReason, string) {
	if backend == nil || backend.Spec.TLS == nil || backend.Spec.TLS.Mode != gatewayx.BackendTLSModeClientAndServer || backend.Spec.TLS.ClientCertificateRef == nil {
		return "", gatewayv1.RouteConditionReason(gatewayv1.GatewayReasonInvalidClientCertificateRef), "The XBackend has no mutual TLS client certificate reference"
	}
	reference, reason, message := r.resolveBackendClientCertificate(ctx, class, gatewayx.GroupName, "XBackend", backend.Namespace,
		XBackendClientPurpose, *backend.Spec.TLS.ClientCertificateRef)
	if reason == backendClientUnavailable {
		return "", gatewayv1.RouteReasonPending, message
	}
	return reference, gatewayv1.RouteConditionReason(reason), message
}

func (r *Resolver) resolveBackendClientCertificate(ctx context.Context, class gatewayv1.ObjectName, sourceGroup gatewayv1.Group, sourceKind gatewayv1.Kind, sourceNamespace string, purpose string, ref gatewayv1.SecretObjectReference) (string, gatewayv1.GatewayConditionReason, string) {
	const invalid = gatewayv1.GatewayReasonInvalidClientCertificateRef
	target, valid := secretKey(sourceNamespace, ref)
	if !valid {
		return "", invalid, "The backend client certificate reference must have a valid Secret name and namespace"
	}
	if r == nil || r.Reader == nil {
		return "", backendClientUnavailable, "The operator backend client certificate resolver is unavailable"
	}
	path := r.BackendClientPath
	if purpose == XBackendClientPurpose {
		path = r.XBackendClientPath
	}
	if path == "" {
		return "", backendClientUnavailable, "The operator backend client certificate resolver is unavailable"
	}
	group, kind := ptr.Deref(ref.Group, ""), ptr.Deref(ref.Kind, "Secret")
	if allowed, observed := r.referenceAllowed(ctx, sourceGroup, sourceKind, sourceNamespace, target, group, kind); !observed {
		return "", backendClientUnavailable, "The ReferenceGrants for the backend client certificate could not be read"
	} else if !allowed {
		return "", gatewayv1.GatewayReasonRefNotPermitted, "No ReferenceGrant permits this " + string(sourceKind) + " to use the backend client certificate reference"
	}
	if group != "" {
		return "", invalid, "The backend client certificate reference must use the core API group"
	}
	if kind != "Secret" {
		return "", invalid, "The backend client certificate reference must use kind Secret"
	}
	row, found, readErr := findBinding(path, purpose, r.scope(class), target)
	if readErr != nil && purpose == XBackendClientPurpose {
		return "", backendClientUnavailable, "The operator XBackend client certificate map could not be read"
	}
	metadata, err := r.secretMetadata(ctx, target)
	if apierrors.IsNotFound(err) || err == nil && !metadata.DeletionTimestamp.IsZero() {
		return "", invalid, "The backend client certificate Secret does not exist or is being deleted"
	}
	if !found {
		return "", invalid, "The backend client certificate Secret has no verified operator enrollment for a kubernetes.io/tls Secret with matching tls.crt and tls.key"
	}
	if err != nil {
		return "", backendClientUnavailable, "The enrolled backend client certificate Secret metadata could not be read"
	}
	if !matchesRevision(metadata, target, row) {
		return "", invalid, "The live backend client certificate Secret UID or resourceVersion differs from its operator enrollment"
	}
	if !row.validAt(time.Now()) {
		return "", backendClientNotCurrent, "The enrolled backend client certificate is outside its validity period"
	}
	reference, reason := r.barbicanRef(row)
	if reason != "" {
		return "", backendClientUnavailable, "The regional Barbican endpoint for the backend client certificate is unavailable"
	}
	return reference, "", ""
}

func (r *Resolver) barbicanRef(row binding) (string, gatewayv1.ListenerConditionReason) {
	const invalid = gatewayv1.ListenerReasonInvalidCertificateRef
	endpoint := r.Endpoint
	if r.DiscoverEndpoint != nil {
		var err error
		endpoint, err = r.DiscoverEndpoint()
		if err != nil {
			return "", invalid
		}
	}
	base, valid := barbicanBase(endpoint)
	if !valid || !row.validAt(time.Now()) {
		return "", invalid
	}
	return base + row.BarbicanSecretID, ""
}

// ReferenceStatus checks a live, enrolled Secret reference without discovering
// the key manager endpoint. An empty reason means the reference is valid.
func (r *Resolver) ReferenceStatus(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) gatewayv1.ListenerConditionReason {
	row, reason := r.authorizedBinding(ctx, gateway, listener)
	if reason == "" && !row.validAt(time.Now()) {
		return gatewayv1.ListenerReasonInvalidCertificateRef
	}
	return reason
}

func (r *Resolver) authorizedBinding(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (binding, gatewayv1.ListenerConditionReason) {
	const invalid = gatewayv1.ListenerReasonInvalidCertificateRef
	if gateway == nil {
		return binding{}, invalid
	}
	return r.authorizedBindingFor(ctx, gateway, "Gateway", gateway.Namespace, listener)
}

func (r *Resolver) authorizedBindingFor(ctx context.Context, gateway *gatewayv1.Gateway, sourceKind gatewayv1.Kind, sourceNamespace string, listener *gatewayv1.Listener) (binding, gatewayv1.ListenerConditionReason) {
	const invalid = gatewayv1.ListenerReasonInvalidCertificateRef
	if gateway == nil || listener == nil {
		return binding{}, invalid
	}
	target, valid := certificateTarget(gateway, sourceNamespace, listener)
	if !valid {
		return binding{}, invalid
	}
	return r.authorizedSecret(ctx, gateway, sourceKind, sourceNamespace, target)
}

func (r *Resolver) authorizedSecret(ctx context.Context, gateway *gatewayv1.Gateway, sourceKind gatewayv1.Kind, sourceNamespace string, target client.ObjectKey) (binding, gatewayv1.ListenerConditionReason) {
	const invalid = gatewayv1.ListenerReasonInvalidCertificateRef
	if r == nil || r.Reader == nil || r.Path == "" {
		return binding{}, invalid
	}
	scope := r.scope(gateway.Spec.GatewayClassName)
	if !scope.valid() {
		return binding{}, invalid
	}
	if allowed, observed := r.referenceAllowed(ctx, gatewayv1.GroupName, sourceKind, sourceNamespace, target, "", "Secret"); !observed {
		return binding{}, invalid
	} else if !allowed {
		return binding{}, gatewayv1.ListenerReasonRefNotPermitted
	}

	row, found, _ := findBinding(r.Path, "", scope, target)
	if !found || !row.validAt(time.Now()) {
		return binding{}, invalid
	}
	metadata, err := r.secretMetadata(ctx, target)
	if err != nil || !matchesRevision(metadata, target, row) {
		return binding{}, invalid
	}
	return row, ""
}

func (r *Resolver) scope(class gatewayv1.ObjectName) bindingScope {
	return bindingScope{Cluster: r.ClusterID, GatewayClass: string(class), Project: r.ProjectID, Region: r.Region}
}

func (r *Resolver) referenceAllowed(ctx context.Context, sourceGroup gatewayv1.Group, sourceKind gatewayv1.Kind, sourceNamespace string, target client.ObjectKey, group gatewayv1.Group, kind gatewayv1.Kind) (allowed, observed bool) {
	if target.Namespace == sourceNamespace {
		return true, true
	}
	var grants gatewayv1.ReferenceGrantList
	if err := r.Reader.List(ctx, &grants, client.InNamespace(target.Namespace)); err != nil || grants.Continue != "" {
		return false, false
	}
	return permitsReference(grants.Items, sourceGroup, sourceKind, sourceNamespace, target, group, kind), true
}

func (r *Resolver) secretMetadata(ctx context.Context, target client.ObjectKey) (*metav1.PartialObjectMetadata, error) {
	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return metadata, r.Reader.Get(ctx, target, metadata)
}

func matchesRevision(metadata *metav1.PartialObjectMetadata, target client.ObjectKey, row binding) bool {
	return metadata.Namespace == target.Namespace && metadata.Name == target.Name &&
		string(metadata.UID) == row.UID && metadata.ResourceVersion == row.ResourceVersion &&
		metadata.DeletionTimestamp.IsZero()
}

func certificateTarget(gateway *gatewayv1.Gateway, sourceNamespace string, listener *gatewayv1.Listener) (client.ObjectKey, bool) {
	tls := listener.TLS
	if (listener.Protocol != gatewayv1.HTTPSProtocolType && listener.Protocol != gatewayv1.TLSProtocolType) || tls == nil ||
		(tls.Mode != nil && *tls.Mode != gatewayv1.TLSModeTerminate) ||
		len(tls.CertificateRefs) != 1 || len(tls.Options) != 0 {
		return client.ObjectKey{}, false
	}
	return secretTarget(sourceNamespace, tls.CertificateRefs[0])
}

func secretTarget(sourceNamespace string, ref gatewayv1.SecretObjectReference) (client.ObjectKey, bool) {
	if (ref.Group != nil && *ref.Group != "") || (ref.Kind != nil && *ref.Kind != "Secret") {
		return client.ObjectKey{}, false
	}
	return secretKey(sourceNamespace, ref)
}

func secretKey(sourceNamespace string, ref gatewayv1.SecretObjectReference) (client.ObjectKey, bool) {
	namespace := sourceNamespace
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	target := client.ObjectKey{Namespace: namespace, Name: string(ref.Name)}
	return target, validName(target) && len(validation.IsDNS1123Label(sourceNamespace)) == 0
}

func permitsReference(grants []gatewayv1.ReferenceGrant, sourceGroup gatewayv1.Group, sourceKind gatewayv1.Kind, namespace string, target client.ObjectKey, group gatewayv1.Group, kind gatewayv1.Kind) bool {
	for _, grant := range grants {
		if grant.Namespace != target.Namespace || !grant.DeletionTimestamp.IsZero() {
			continue
		}
		from := slices.ContainsFunc(grant.Spec.From, func(ref gatewayv1.ReferenceGrantFrom) bool {
			return ref.Group == sourceGroup && ref.Kind == sourceKind && string(ref.Namespace) == namespace
		})
		to := slices.ContainsFunc(grant.Spec.To, func(ref gatewayv1.ReferenceGrantTo) bool {
			return ref.Group == group && ref.Kind == kind && (ref.Name == nil || string(*ref.Name) == target.Name)
		})
		if from && to {
			return true
		}
	}
	return false
}

var servicePathSegment = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)

func barbicanBase(endpoint string) (string, bool) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Opaque != "" || parsed.User != nil ||
		parsed.RawPath != "" || strings.ContainsAny(endpoint, "?#") {
		return "", false
	}
	hostname := parsed.Hostname()
	if hostname == "" || strings.Contains(hostname, "%") || strings.HasSuffix(parsed.Host, ":") {
		return "", false
	}
	if strings.Contains(hostname, ":") {
		if !strings.HasPrefix(parsed.Host, "[") || net.ParseIP(hostname) == nil {
			return "", false
		}
	} else if strings.HasPrefix(parsed.Host, "[") ||
		len(validation.IsDNS1123Subdomain(strings.TrimSuffix(strings.ToLower(hostname), "."))) != 0 {
		return "", false
	}
	if port := parsed.Port(); port != "" {
		if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
			return "", false
		}
	}
	// Keystone may publish a reverse-proxy mount such as /key-manager.
	mount := strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/"), "/v1")
	if mount != "" {
		for _, part := range strings.Split(strings.TrimPrefix(mount, "/"), "/") {
			if !servicePathSegment.MatchString(part) || part == "." || part == ".." || part == "v1" {
				return "", false
			}
		}
	}
	return "https://" + parsed.Host + mount + "/v1/secrets/", true
}
