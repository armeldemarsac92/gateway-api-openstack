# Use a named ClusterTrustBundle for Gateway TLS

A Gateway can validate HTTPS clients with a ClusterTrustBundle. A BackendTLSPolicy
can use the same kind of reference to verify the serving certificate of a
Service port. Enable `--gateway-api-cluster-trust-bundle` on the controller
alongside the existing feature option for each consumer. The existing Octavia
capabilities, server-certificate enrollment and Barbican permissions still apply.
[Frontend client validation](operator-https.md#require-a-client-certificate) ·
[Service BackendTLSPolicy](operator-octavia-backend-tls-policy.md)

On Kubernetes 1.36, enable the apiserver feature gate
`ClusterTrustBundle=true` and serve
`certificates.k8s.io/v1beta1/clustertrustbundles=true` via `--runtime-config`.
The controller discovers the served version of the native cluster-scoped
resource and retries discovery when the API is unavailable. Stock Kubernetes grants its ServiceAccounts `get`,
`list` and `watch` through `system:cluster-trust-bundle-discovery` when the
feature gate is enabled. On a cluster that changed these defaults, grant
those three verbs on `certificates.k8s.io/clustertrustbundles` to the
controller ServiceAccount. The bundle metadata watch runs independently of the
controller caches. It starts when discovery confirms the API and retries
after list or watch failures.
[Kubernetes 1.36 API reference](https://v1-36.docs.kubernetes.io/docs/reference/kubernetes-api/certificates/cluster-trust-bundle-v1beta1/)

Create a cluster-scoped `ClusterTrustBundle` with the public X.509 CA
certificates in `spec.trustBundle`. In the Gateway's
`spec.tls.frontend.default.validation` or `spec.tls.frontend.perPort[].tls.validation`, or
in the BackendTLSPolicy's `spec.validation`, set the following reference.

```yaml
caCertificateRefs:
  - group: certificates.k8s.io
    kind: ClusterTrustBundle
    name: catalog-ca
```

Use one named object reference. Omit `namespace` and use the actual
object name even when the bundle is associated with a signer. Namespaced
references still follow their existing ReferenceGrant rules. Gateway API v1.7.0
lists ClusterTrustBundle as Experimental Extended support. Both v1.7.0 CRD
channels admit this exact reference. The Experimental Gateway CRD rejects an
explicit `namespace` at admission. The controller rejects it on both channels.

The controller parses and normalizes the PEM CA certificates, sorting and
deduplicating them. The normalized bundle fits the existing 128 KiB Barbican
CA payload limit. Each revision uses the Kubernetes UID and the digest of the
normalized bundle. A change in trust material rotates the revision. Recreating
a bundle under the same name produces a new revision even with identical trust
material. The controller withdraws the affected listener or backend when it
confirms deletion or invalid content. The corresponding listener or policy reports
`Accepted=False / NoValidCACertificate` and
`ResolvedRefs=False / InvalidCACertificateRef`. A valid bundle exceeding
128 KiB after normalization resolves with `CACertificateBundleTooLarge` and
keeps the affected listener or backend closed. Stale or partial discovery and
transient read failures keep the last confirmed configuration and cause a
retry. A confirmed frontend revocation can still close its HTTPS port while
an unrelated backend trust read is pending. Complete discovery that confirms
the native API is unserved makes the reference invalid.

## Local qualification

The controller's envtest scenario installs the Gateway API v1.7.0 CRDs on a
Kubernetes 1.36.2 apiserver. It exercises absence, creation, rotation, deletion
and same-name recreation on the HTTPS listener and an HTTPRoute forwarding to
the selected TLS Service port. It checks that a bundle above the Barbican
payload limit is rejected by the Gateway, a delegated ListenerSet and the
BackendTLSPolicy with a distinct status reason. It checks listener and policy conditions and the
exact transport, address, port, UID and trust payload submitted to the
recording provider. The suite also checks native discovery with the API unserved
and that an ordinary HTTP Gateway reaches Programmed when access to watch
the optional resource is denied. Run with `KUBEBUILDER_ASSETS` set to the Kubernetes 1.36.2 assets and
`GATEWAY_API_V17_SOURCE` set to the v1.7.0 source checkout at
`e26d183dbbc1bb782b78addbd26faba03955c152`.

```sh
for channel in standard experimental; do
  GATEWAY_API_CHANNEL="$channel" \
    GATEWAY_API_CRD_PATH="$GATEWAY_API_V17_SOURCE/config/crd/$channel" \
    ENVTEST_CLUSTER_TRUST_BUNDLE=1 go test -tags=envtest -count=1 \
      -run '^TestControllerEnvtest$/^ClusterTrustBundle_' -v ./internal/controller
done
```

To check an apiserver with no bundle API, set `ENVTEST_CLUSTER_TRUST_BUNDLE=0`
and select the subtest `^TestControllerEnvtest$/^ClusterTrustBundle_API_unserved$`.
To verify real traffic, use the Octavia installation for each TLS profile.
Test fresh connections with trusted and untrusted certificates before and after
changing the bundle. Confirm that a bundle deletion closes the listener and
fails the backend request without forwarding it upstream. The local scenario
does not contact Octavia or Barbican. The v1.7.0 registry has no
`ClusterTrustBundle` conformance case.
