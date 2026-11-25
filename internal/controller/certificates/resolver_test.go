package certificates

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
)

const (
	firstID   = "11111111-1111-4111-8111-111111111111"
	secondID  = "22222222-2222-4222-8222-222222222222"
	scopeJSON = `"scope":{"cluster":"cluster","gatewayClass":"octavia","project":"project","region":"RegionOne"}`
)

func TestResolveFollowsOnlyTheEnrolledSecretRevision(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	expectResolution(t, resolver, gateway, listener, firstID, "")

	reader.metadata.ResourceVersion = "rv/beta"
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/beta", secondID))
	expectResolution(t, resolver, gateway, listener, secondID, "")

	reader.metadata.UID = "uid-b"
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	reader.metadata.UID = "uid-a"
	reader.metadata.DeletionTimestamp = ptr.To(metav1.Now())
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	reader.metadata.DeletionTimestamp = nil
	writeMap(t, resolver.Path, "")
	reader.reads = nil
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	if len(reader.reads) != 0 {
		t.Fatalf("looked up an unmapped Secret: %v", reader.reads)
	}
}

func TestBackendClientCertificateUsesItsOwnEnrollmentAndGrant(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	resolver.BackendClientPath = filepath.Join(t.TempDir(), "backend-clients.json")
	writeBackendMap := func(row string) {
		t.Helper()
		data := strings.Replace(mapJSON(row), `"version":1`, `"version":1,"purpose":"backend-client"`, 1)
		writeSnapshot(t, resolver.BackendClientPath, data)
	}
	gateway.Namespace = "application"
	gateway.Spec.TLS = &gatewayv1.GatewayTLSConfig{Backend: &gatewayv1.GatewayBackendTLS{
		ClientCertificateRef: &gatewayv1.SecretObjectReference{Name: "web", Namespace: ptr.To(gatewayv1.Namespace("certs"))},
	}}
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))

	check := func(id string, reason gatewayv1.GatewayConditionReason) {
		t.Helper()
		got, gotReason, message := resolver.ResolveBackendClientCertificate(t.Context(), gateway)
		want := ""
		if id != "" {
			want = "https://barbican.example:443/v1/secrets/" + id
		}
		if got != want || gotReason != reason || (reason != "") != (message != "") {
			t.Fatalf("backend client identity = %q, %q, %q; want %q, %q", got, gotReason, message, want, reason)
		}
	}
	check("", gatewayv1.GatewayReasonRefNotPermitted)
	if !reflect.DeepEqual(reader.reads, []string{"grants certs"}) {
		t.Fatalf("unpermitted client certificate reads = %v, want only grant lookup", reader.reads)
	}
	reader.grants = []gatewayv1.ReferenceGrant{{
		ObjectMeta: metav1.ObjectMeta{Namespace: "certs"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: "application"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Secret", Name: ptr.To(gatewayv1.ObjectName("web"))}},
		},
	}}
	check("", gatewayv1.GatewayReasonInvalidClientCertificateRef)
	writeMap(t, resolver.BackendClientPath, rowJSON("uid-a", "rv/alpha", firstID))
	check("", gatewayv1.GatewayReasonInvalidClientCertificateRef)
	writeBackendMap(rowJSON("uid-a", "rv/alpha", firstID))
	check(firstID, "")
	listener.TLS.CertificateRefs[0].Namespace = ptr.To(gatewayv1.Namespace("certs"))
	expectResolution(t, resolver, gateway, listener, firstID, "")
	frontend := resolver.Path
	resolver.Path = resolver.BackendClientPath
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	resolver.Path = frontend
	reader.metadata.ResourceVersion = "rotated"
	check("", gatewayv1.GatewayReasonInvalidClientCertificateRef)
	writeBackendMap(rowJSON("uid-a", "rotated", secondID))
	check(secondID, "")
	resolver.DiscoverEndpoint = func() (string, error) { return "", nil }
	check("", "BackendClientCertificateUnavailable")
	gateway.Spec.TLS.Backend.ClientCertificateRef.Kind = ptr.To(gatewayv1.Kind("Other"))
	reader.reads = nil
	check("", gatewayv1.GatewayReasonRefNotPermitted)
	reader.grants[0].Spec.To[0].Kind = "Other"
	check("", gatewayv1.GatewayReasonInvalidClientCertificateRef)
	if !reflect.DeepEqual(reader.reads, []string{"grants certs", "grants certs"}) {
		t.Fatalf("unsupported backend client reference caused a Secret read: %v", reader.reads)
	}
	reader.grants = nil
	check("", gatewayv1.GatewayReasonRefNotPermitted)
}

func TestXBackendClientCertificateUsesExperimentalGrantAndEnrollmentRevision(t *testing.T) {
	resolver, gateway, _, reader := resolverFixture(t)
	resolver.BackendClientPath = filepath.Join(t.TempDir(), "gateway-client.json")
	resolver.XBackendClientPath = filepath.Join(t.TempDir(), "xbackend-client.json")
	writePurpose := func(path, purpose, row string) {
		t.Helper()
		writeSnapshot(t, path, strings.Replace(mapJSON(row), `"version":1`, `"version":1,"purpose":"`+purpose+`"`, 1))
	}
	ref := &gatewayv1.SecretObjectReference{Name: "web", Namespace: ptr.To(gatewayv1.Namespace("certs"))}
	backend := &gatewayx.XBackend{ObjectMeta: metav1.ObjectMeta{Namespace: "apps"}, Spec: gatewayx.BackendSpec{
		TLS: &gatewayx.BackendTLS{Mode: gatewayx.BackendTLSModeClientAndServer, ClientCertificateRef: ref},
	}}
	writePurpose(resolver.BackendClientPath, BackendClientPurpose, rowJSON("uid-a", "rv/alpha", firstID))
	writePurpose(resolver.XBackendClientPath, BackendClientPurpose, rowJSON("uid-a", "rv/alpha", firstID))
	reader.grants = []gatewayv1.ReferenceGrant{{
		ObjectMeta: metav1.ObjectMeta{Namespace: "certs"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "XBackend", Namespace: "apps"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Secret", Name: ptr.To(gatewayv1.ObjectName("web"))}},
		},
	}}
	check := func(id string, reason gatewayv1.RouteConditionReason, reads ...string) {
		t.Helper()
		reader.reads = nil
		got, gotReason, message := resolver.ResolveXBackendClientCertificate(t.Context(), gateway.Spec.GatewayClassName, backend)
		want := ""
		if id != "" {
			want = "https://barbican.example:443/v1/secrets/" + id
		}
		if got != want || gotReason != reason || (reason != "") != (message != "") || !reflect.DeepEqual(reads, reader.reads) {
			t.Fatalf("XBackend identity check mismatched: reason %q, message %q, reads %v", gotReason, message, reader.reads)
		}
	}
	const invalid = gatewayv1.RouteConditionReason(gatewayv1.GatewayReasonInvalidClientCertificateRef)
	check("", gatewayv1.RouteReasonRefNotPermitted, "grants certs")
	from := &reader.grants[0].Spec.From[0]
	from.Group, from.Kind = gatewayx.GroupName, "Gateway"
	check("", gatewayv1.RouteReasonRefNotPermitted, "grants certs")
	from.Kind, from.Namespace = "XBackend", "other"
	check("", gatewayv1.RouteReasonRefNotPermitted, "grants certs")
	from.Namespace = "apps"
	check("", invalid, "grants certs", "metadata certs/web")
	writePurpose(resolver.XBackendClientPath, XBackendClientPurpose, rowJSON("uid-a", "rv/alpha", secondID))
	check(secondID, "", "grants certs", "metadata certs/web")
	reader.grantError = errors.New("temporary grant inventory failure")
	check("", gatewayv1.RouteReasonPending, "grants certs")
	reader.grantError = nil
	reader.grants[0].Spec.To[0].Name = ptr.To(gatewayv1.ObjectName("other-secret"))
	check("", gatewayv1.RouteReasonRefNotPermitted, "grants certs")
	reader.grants[0].Spec.To[0].Name = ptr.To(gatewayv1.ObjectName("web"))
	reader.metadata.ResourceVersion = "rv/beta"
	check("", invalid, "grants certs", "metadata certs/web")
	writePurpose(resolver.XBackendClientPath, XBackendClientPurpose, rowJSON("uid-a", "rv/beta", firstID))
	check(firstID, "", "grants certs", "metadata certs/web")
	backend.Namespace, ref.Namespace = "certs", nil
	check(firstID, "", "metadata certs/web")
	gateway.Spec.GatewayClassName = "wrong-class"
	check("", invalid, "metadata certs/web")
	backend.Spec.TLS.Mode = gatewayx.BackendTLSModeServerOnly
	check("", invalid)
}

func TestListenerSetCertificateUsesOwnerBeforeReadingSecret(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	gateway.Namespace = "application"
	set := &gatewayv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "certs"}}
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	check := func(reference string, reason gatewayv1.ListenerConditionReason, reads ...string) {
		t.Helper()
		reader.reads = nil
		got, gotReason := resolver.ResolveListenerSet(t.Context(), gateway, set, listener)
		if got != reference || gotReason != reason || !reflect.DeepEqual(reader.reads, reads) {
			t.Fatalf("delegated resolution = %q, %q, %v; want %q, %q, %v", got, gotReason, reader.reads, reference, reason, reads)
		}
	}
	want := "https://barbican.example:443/v1/secrets/" + firstID
	check(want, "", "metadata certs/web")

	set.Namespace = "delegates"
	listener.TLS.CertificateRefs[0].Namespace = ptr.To(gatewayv1.Namespace("certs"))
	grant := gatewayv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "certs"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: "delegates"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Secret", Name: ptr.To(gatewayv1.ObjectName("web"))}},
		},
	}
	reader.grants = []gatewayv1.ReferenceGrant{grant}
	check("", gatewayv1.ListenerReasonRefNotPermitted, "grants certs")
	reader.grants[0].Spec.From[0].Kind = "ListenerSet"
	reader.grants[0].Spec.From[0].Namespace = gatewayv1.Namespace(gateway.Namespace)
	check("", gatewayv1.ListenerReasonRefNotPermitted, "grants certs")
	reader.grants[0].Spec.From[0].Namespace = gatewayv1.Namespace(set.Namespace)
	check(want, "", "grants certs", "metadata certs/web")
	reader.grants = nil
	check("", gatewayv1.ListenerReasonRefNotPermitted, "grants certs")
}

func TestCrossNamespaceGrantRevocationPrecedesMetadataRead(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	gateway.Namespace = "application"
	listener.TLS.CertificateRefs[0].Namespace = ptr.To(gatewayv1.Namespace("certs"))
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	reader.grants = []gatewayv1.ReferenceGrant{{
		ObjectMeta: metav1.ObjectMeta{Namespace: "certs"},
		Spec: gatewayv1.ReferenceGrantSpec{
			From: []gatewayv1.ReferenceGrantFrom{{Group: gatewayv1.GroupName, Kind: "Gateway", Namespace: "application"}},
			To:   []gatewayv1.ReferenceGrantTo{{Kind: "Secret", Name: ptr.To(gatewayv1.ObjectName("web"))}},
		},
	}}
	expectResolution(t, resolver, gateway, listener, firstID, "")
	if !reflect.DeepEqual(reader.reads, []string{"grants certs", "metadata certs/web"}) {
		t.Fatalf("unexpected read sequence: %v", reader.reads)
	}
	listener.Protocol = gatewayv1.TLSProtocolType
	listener.TLS.Mode = ptr.To(gatewayv1.TLSModeTerminate)
	expectResolution(t, resolver, gateway, listener, firstID, "")
	resolver.DiscoverEndpoint = func() (string, error) {
		t.Fatal("reference status reached Barbican endpoint discovery")
		return "", nil
	}
	reader.reads = nil
	if reason := resolver.ReferenceStatus(t.Context(), gateway, listener); reason != "" {
		t.Fatalf("granted certificate reference status = %q", reason)
	}
	if !reflect.DeepEqual(reader.reads, []string{"grants certs", "metadata certs/web"}) {
		t.Fatalf("unexpected status read sequence: %v", reader.reads)
	}
	reader.grants[0].DeletionTimestamp = ptr.To(metav1.Now())
	reader.reads = nil
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonRefNotPermitted)
	if !reflect.DeepEqual(reader.reads, []string{"grants certs"}) {
		t.Fatalf("read certificate metadata after grant revocation: %v", reader.reads)
	}
	reader.reads = nil
	if reason := resolver.ReferenceStatus(t.Context(), gateway, listener); reason != gatewayv1.ListenerReasonRefNotPermitted {
		t.Fatalf("revoked certificate reference status = %q", reason)
	}
	if !reflect.DeepEqual(reader.reads, []string{"grants certs"}) {
		t.Fatalf("status read certificate metadata after grant revocation: %v", reader.reads)
	}
	reader.grantError = errors.New("forbidden")
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
}

func TestBadMapAndUnsupportedTLSFailClosedBeforeMetadataRead(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	valid := mapJSON(rowJSON("uid-a", "rv/alpha", firstID))
	for _, candidate := range []struct{ name, document string }{
		{"missing file", ""},
		{"wrong scope", strings.Replace(valid, `"project":"project"`, `"project":"other"`, 1)},
		{"duplicate keys", strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)},
		{"unknown fields", strings.Replace(valid, `"version":1`, `"version":1,"unexpected":true`, 1)},
		{"ambiguous rows", mapJSON(rowJSON("uid-a", "rv/alpha", firstID) + "," + rowJSON("uid-a", "rv/beta", secondID))},
		{"expired", strings.Replace(valid, "2050-01-01", "2021-01-01", 1)},
		{"future", strings.Replace(valid, "2020-01-01", "2049-01-01", 1)},
		{"oversized", valid + strings.Repeat(" ", 1<<20)},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			if candidate.document != "" {
				if err := os.WriteFile(resolver.Path, []byte(candidate.document), 0o444); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Remove(resolver.Path); err != nil {
						t.Error(err)
					}
				})
			}
			expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
			if len(reader.reads) != 0 {
				t.Fatalf("unexpected reads: %v", reader.reads)
			}
		})
	}

	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	listener.TLS.Options = map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{"example.org/min-version": "1.3"}
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	listener.TLS.Options = nil
	if len(reader.reads) != 0 {
		t.Fatalf("unexpected reads: %v", reader.reads)
	}
	gateway.Spec.TLS = &gatewayv1.GatewayTLSConfig{Frontend: &gatewayv1.FrontendTLSConfig{}}
	expectResolution(t, resolver, gateway, listener, firstID, "")
}

func TestCatalogEndpointForms(t *testing.T) {
	resolver, gateway, listener, _ := resolverFixture(t)
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	for _, path := range []string{"", "/", "/v1", "/v1/"} {
		resolver.Endpoint = "https://barbican.example:443" + path
		expectResolution(t, resolver, gateway, listener, firstID, "")
	}
	for _, endpoint := range []string{
		"http://barbican.example", "//barbican.example", "https://barbican.example/v1/v1",
		"https://user@barbican.example", "https://barbican.example?", "https://barbican.example#",
		"https://barbican.example/%76%31", "https://barbican.example:99999",
	} {
		resolver.Endpoint = endpoint
		expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	}
}

func TestBarbicanEndpointBehindServicePath(t *testing.T) {
	for _, endpoint := range []string{
		"https://cloud.example/key-manager",
		"https://cloud.example/key-manager/v1/",
	} {
		base, ok := barbicanBase(endpoint)
		if !ok || base != "https://cloud.example/key-manager/v1/secrets/" {
			t.Fatalf("unexpected key-manager collection %q for %q", base, endpoint)
		}
	}
}

func TestCatalogDiscoveryWaitsForCurrentAuthorizedMetadata(t *testing.T) {
	resolver, gateway, listener, reader := resolverFixture(t)
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	discoveries := 0
	resolver.DiscoverEndpoint = func() (string, error) {
		discoveries++
		if discoveries == 1 {
			return "", errors.New("catalog unavailable")
		}
		return "https://barbican.example:443", nil
	}
	reader.metadata.ResourceVersion = "too-new"
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	if discoveries != 0 {
		t.Fatalf("looked up the Barbican catalog for an unenrolled Secret revision")
	}
	reader.metadata.ResourceVersion = "rv/alpha"
	expectResolution(t, resolver, gateway, listener, "", gatewayv1.ListenerReasonInvalidCertificateRef)
	expectResolution(t, resolver, gateway, listener, firstID, "")
}

func TestResolverGetsNativeKubernetesSecretMetadata(t *testing.T) {
	resolver, gateway, listener, _ := resolverFixture(t)
	writeMap(t, resolver.Path, rowJSON("uid-a", "rv/alpha", firstID))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != "GET" || request.URL.Path != "/api/v1/namespaces/certs/secrets/web" {
			t.Errorf("unexpected API request %s %s", request.Method, request.URL.Path)
		}
		if !strings.Contains(request.Header.Get("Accept"), "as=PartialObjectMetadata;g=meta.k8s.io;v=v1") {
			t.Errorf("missing metadata content negotiation: %q", request.Header.Get("Accept"))
		}
		response.Header().Set("Content-Type", "application/json")
		if _, err := fmt.Fprint(response, `{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"namespace":"certs","name":"web","uid":"uid-a","resourceVersion":"rv/alpha"}}`); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.AddSpecific(corev1.SchemeGroupVersion.WithKind("Secret"), corev1.SchemeGroupVersion.WithResource("secrets"), corev1.SchemeGroupVersion.WithResource("secret"), meta.RESTScopeNamespace)
	direct, err := client.New(&rest.Config{Host: server.URL}, client.Options{Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	resolver.Reader = direct
	expectResolution(t, resolver, gateway, listener, firstID, "")
}

func resolverFixture(t *testing.T) (*Resolver, *gatewayv1.Gateway, *gatewayv1.Listener, *certificateReader) {
	t.Helper()
	reader := &certificateReader{metadata: metav1.ObjectMeta{Namespace: "certs", Name: "web", UID: "uid-a", ResourceVersion: "rv/alpha"}}
	resolver := &Resolver{
		Path: filepath.Join(t.TempDir(), "certificates.json"), ClusterID: "cluster", ProjectID: "project",
		Region: "RegionOne", Endpoint: "https://barbican.example:443/v1/", Reader: reader,
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "certs"},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "octavia"},
	}
	listener := &gatewayv1.Listener{
		Protocol: gatewayv1.HTTPSProtocolType, Port: 443,
		TLS: &gatewayv1.ListenerTLSConfig{CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "web"}}},
	}
	return resolver, gateway, listener, reader
}

func mapJSON(rows string) string { return `{"version":1,` + scopeJSON + `,"revisions":[` + rows + `]}` }

func rowJSON(uid, version, id string) string {
	return fmt.Sprintf(`{"namespace":"certs","name":"web","uid":%q,"resourceVersion":%q,"barbicanSecretID":%q,"chainSHA256":[%q],"notBefore":"2020-01-01T00:00:00Z","notAfter":"2050-01-01T00:00:00Z"}`,
		uid, version, id, strings.Repeat("a", 64))
}

func writeMap(t *testing.T, path, rows string) {
	t.Helper()
	writeSnapshot(t, path, mapJSON(rows))
}

func writeSnapshot(t *testing.T, path, data string) {
	t.Helper()
	// Replace the read-only file the same way a new mounted snapshot becomes visible.
	if err := os.WriteFile(path+".new", []byte(data), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
}

func expectResolution(t *testing.T, resolver *Resolver, gateway *gatewayv1.Gateway, listener *gatewayv1.Listener, id string, reason gatewayv1.ListenerConditionReason) {
	t.Helper()
	wantRef := ""
	if id != "" {
		wantRef = "https://barbican.example:443/v1/secrets/" + id
	}
	gotRef, gotReason := resolver.Resolve(context.Background(), gateway, listener)
	if gotRef != wantRef || gotReason != reason {
		t.Fatalf("Resolve = (%q, %q), want (%q, %q)", gotRef, gotReason, wantRef, reason)
	}
}

type certificateReader struct {
	client.Reader
	metadata   metav1.ObjectMeta
	grants     []gatewayv1.ReferenceGrant
	grantError error
	reads      []string
}

func (r *certificateReader) Get(_ context.Context, key client.ObjectKey, object client.Object, _ ...client.GetOption) error {
	r.reads = append(r.reads, "metadata "+key.String())
	metadata, ok := object.(*metav1.PartialObjectMetadata)
	if !ok || metadata.GroupVersionKind() != corev1.SchemeGroupVersion.WithKind("Secret") {
		return fmt.Errorf("expected core Secret metadata, got %T", object)
	}
	metadata.ObjectMeta = *r.metadata.DeepCopy()
	return nil
}

func (r *certificateReader) List(_ context.Context, object client.ObjectList, options ...client.ListOption) error {
	opts := (&client.ListOptions{}).ApplyOptions(options)
	r.reads = append(r.reads, "grants "+opts.Namespace)
	grants, ok := object.(*gatewayv1.ReferenceGrantList)
	if !ok || opts.Namespace != "certs" {
		return fmt.Errorf("expected ReferenceGrants in certs, got %T in %q", object, opts.Namespace)
	}
	grants.Items = r.grants
	return r.grantError
}
