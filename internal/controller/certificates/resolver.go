// Package certificates resolves operator-enrolled revisions for HTTPS listeners.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Resolver reads a protected enrollment snapshot and uses the uncached Reader
// to check current ReferenceGrants and Secret metadata.
type Resolver struct {
	Path             string
	ClusterID        string
	ProjectID        string
	Region           string
	Endpoint         string
	DiscoverEndpoint func() (string, error)
	Reader           client.Reader
}

// Resolve returns a Barbican reference only for the live revision of an enrolled
// Secret. The endpoint comes from the authenticated key-manager catalog.
func (r *Resolver) Resolve(ctx context.Context, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (string, gatewayv1.ListenerConditionReason) {
	const invalid = gatewayv1.ListenerReasonInvalidCertificateRef
	if r == nil || r.Path == "" || r.Reader == nil || gateway == nil || listener == nil {
		return "", invalid
	}
	scope := bindingScope{Cluster: r.ClusterID, GatewayClass: string(gateway.Spec.GatewayClassName), Project: r.ProjectID, Region: r.Region}
	target, valid := certificateTarget(gateway, listener)
	if !valid || !scope.valid() {
		return "", invalid
	}
	if target.Namespace != gateway.Namespace {
		var grants gatewayv1.ReferenceGrantList
		if err := r.Reader.List(ctx, &grants, client.InNamespace(target.Namespace)); err != nil || grants.Continue != "" {
			return "", invalid
		}
		if !permitsCertificate(grants.Items, gateway.Namespace, target) {
			return "", gatewayv1.ListenerReasonRefNotPermitted
		}
	}

	row, found := findBinding(r.Path, scope, target)
	if !found || !row.validAt(time.Now()) {
		return "", invalid
	}
	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.Reader.Get(ctx, target, metadata); err != nil ||
		metadata.Namespace != target.Namespace || metadata.Name != target.Name ||
		string(metadata.UID) != row.UID || metadata.ResourceVersion != row.ResourceVersion ||
		!metadata.DeletionTimestamp.IsZero() {
		return "", invalid
	}

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

func certificateTarget(gateway *gatewayv1.Gateway, listener *gatewayv1.Listener) (client.ObjectKey, bool) {
	tls := listener.TLS
	if listener.Protocol != gatewayv1.HTTPSProtocolType || tls == nil ||
		(tls.Mode != nil && *tls.Mode != gatewayv1.TLSModeTerminate) ||
		len(tls.CertificateRefs) != 1 || len(tls.Options) != 0 ||
		(gateway.Spec.TLS != nil && (gateway.Spec.TLS.Frontend != nil || gateway.Spec.TLS.Backend != nil)) {
		return client.ObjectKey{}, false
	}
	ref := tls.CertificateRefs[0]
	if (ref.Group != nil && *ref.Group != "") || (ref.Kind != nil && *ref.Kind != "Secret") {
		return client.ObjectKey{}, false
	}
	namespace := gateway.Namespace
	if ref.Namespace != nil {
		namespace = string(*ref.Namespace)
	}
	target := client.ObjectKey{Namespace: namespace, Name: string(ref.Name)}
	return target, validName(target) && len(validation.IsDNS1123Label(gateway.Namespace)) == 0
}

func permitsCertificate(grants []gatewayv1.ReferenceGrant, namespace string, target client.ObjectKey) bool {
	for _, grant := range grants {
		if grant.Namespace != target.Namespace || !grant.DeletionTimestamp.IsZero() {
			continue
		}
		from := slices.ContainsFunc(grant.Spec.From, func(ref gatewayv1.ReferenceGrantFrom) bool {
			return ref.Group == gatewayv1.GroupName && ref.Kind == "Gateway" && string(ref.Namespace) == namespace
		})
		to := slices.ContainsFunc(grant.Spec.To, func(ref gatewayv1.ReferenceGrantTo) bool {
			return ref.Group == "" && ref.Kind == "Secret" && (ref.Name == nil || string(*ref.Name) == target.Name)
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
