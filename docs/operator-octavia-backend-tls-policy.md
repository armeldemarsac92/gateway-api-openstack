# Authenticate HTTPRoute and GRPCRoute backends with BackendTLSPolicy

An application can require the Gateway to encrypt its connection to a Service and verify the server's DNS identity. HTTPRoute can attach to an HTTP listener or a listener that terminates HTTPS. The gRPC profile uses an HTTP listener. On the upstream connection, Amphora sends the policy hostname as SNI and verifies the certificate against the CA in the Service namespace.

## Enable the regional capability

1. Build the Amphora image with the strict DNS HAProxy executable and install the matching Amphora agent. Upgrade the Octavia database, octavia-lib, API and workers. Verify that each active and standby Amphora can run the configured binary. [Amphora image build instructions](https://github.com/armeldemarsac92/octavia/blob/ba89a2d6237aa1a1b754ad7234899c12de39a4d2/elements/haproxy-octavia/README.rst) · [Matching Hibiscus packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-hostname-ba89a2d6237a)
2. Set `enabled_backend_tls_hostname_providers = amphora` in Octavia's `[api_settings]`. Using the controller's regional catalog and project, check that `GET /v2/lbaas/providers/amphora/gateway_capabilities` includes `backend_tls_hostname_v1`.
3. Supply a stable cluster ID and a dedicated project-scoped identity with permission to publish, verify and retire its CA revisions in regional Barbican. The Octavia service identity needs access to each configured CA through its normal certificate manager.
4. For a standalone installation render `config/octavia-backend-tls-policy`. For the Magnum workload addon render `integrations/magnum-capi-helm/workload-octavia-backend-tls-policy`. Each overlay sets `--octavia-backend-tls-policy` and Octavia microversion 2.8, and grants access to `BackendTLSPolicy`, its status and ConfigMaps. The controller watches ConfigMap metadata and reads the referenced CA after verifying the authorized Service port. If the installation also uses the Experimental release bundle, include the existing Experimental component. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

For GRPCRoute, enable the existing gRPC profile and install the provider and Amphora builds that enforce negotiated HTTP/2. Set `enabled_backend_tls_h2_alpn_providers = amphora` alongside the hostname setting, then verify that the regional capability response includes `backend_tls_h2_alpn_v1` and `backend_tls_hostname_v1`. The controller requests `backend_tls_require_h2: true` and checks it alongside the configured hostname and CA on the owned pool before enabling it or enrolling members. Amphora closes backend TLS connections that do not negotiate `h2`. Missing capability, missing readback or invalid trust keeps the affected backend unavailable. The Service port may omit `appProtocol` or declare `https`. A port declaring `kubernetes.io/h2c` cannot use this policy.

## Protect a Service port

Place the public CA certificate under `ca.crt` in a ConfigMap in the Service namespace. For example, given a local PEM file for the issuing CA, run the following.

```sh
kubectl -n catalog create configmap catalog-ca --from-file=ca.crt=issuing-ca.pem
```

For a Service called `catalog` with a TCP port named `https`, attach this policy in the same namespace. The backend certificate must chain to the configured CA and have a DNS SAN matching `catalog.internal.example`.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: BackendTLSPolicy
metadata:
  name: catalog
  namespace: catalog
spec:
  targetRefs:
    - group: ""
      kind: Service
      name: catalog
      sectionName: https
  validation:
    hostname: catalog.internal.example
    caCertificateRefs:
      - group: ""
        kind: ConfigMap
        name: catalog-ca
```

HTTPRoute and GRPCRoute continue to refer to the Service port by number. A cross-namespace backend also requires the usual ReferenceGrant in the Service namespace. Omitting `sectionName` applies the policy to the Service as a whole. A matching port-specific policy takes precedence. Competing policies for the same target and section are ordered by creation timestamp and name. [Service grant lifecycle](qualification/v1.7.0/service-grants.md)

## Verify and rotate trust

Read `status.ancestors` on the policy for the managed Gateway and current generation. `Accepted=True` and `ResolvedRefs=True` describe the accepted policy and resolved CA. Send traffic through the Gateway to verify the backend certificate. A server presenting the wrong DNS identity or a certificate from an untrusted CA must receive no HTTP request. The matching client request receives a 5xx response.

A policy can record 16 ancestors across all controllers. The Gateway waits for its own row before forwarding to the protected Service. If all rows belong to other ancestors, check the Route's controller-specific `Programmed` condition and the Gateway's `BackendTLSPoliciesReady` condition. Existing ancestors continue serving. When a row becomes available, the controller records the waiting Gateway and reconciles its backend.

Changing the public CA certificate in `ca.crt` creates a new immutable Barbican revision for this Gateway. The controller withdraws traffic from the affected pool while it changes trust and checks the effective hostname and CA before putting members back in service. A certificate rotation that changes the issuing CA needs coordination with the backend operator. Check a successful request after the rotation and a refusal with the previous trust configuration. An invalid policy or a missing CA keeps the protected backend unavailable. Its Service can still be a resolved reference on the route.

During rollback, keep the upgraded workers, strict Amphora runtime and Barbican identity available until protected routes and their cloud resources have been withdrawn. The controller records CA ownership in a Gateway finalizer before publication. After opt-out it keeps enrolled pools closed and can still retire this Gateway's CAs on deletion. The controller checks regional project pool and listener CA references before retiring an unused revision. A CA still used elsewhere is retained.

## Present a Gateway client certificate

With the BackendTLSPolicy profile enabled, set `--backend-client-certificate-map=/etc/gateway-operator/backend-clients/map.json` on the controller or set `GATEWAY_OPENSTACK_BACKEND_CLIENT_CERTIFICATE_MAP` to that path. Mount a separate operator-controlled ConfigMap directory read-only at that location so kubelet can project updates. For each approved Secret name, give the controller ServiceAccount `get` via `resourceNames` in the Secret's namespace. The controller reads only metadata and needs neither Secret `list` nor `watch`. [Name-scoped Role and RoleBinding example](operator-https.md#enable-the-mode)

On a protected operator host, use the enrollment helper with a dedicated working map. Verify that the TLS Secret contains a matched certificate chain and private key appropriate for TLS client authentication. Store the same chain and key as a new immutable, unencrypted PKCS#12 Barbican secret and grant Octavia read access. The helper compares the retrieved payload, Secret and creation receipt. Publish the resulting version 1 snapshot with top-level `"purpose": "backend-client"`. Both map types require the exact cluster, GatewayClass, project, region, Secret UID and resourceVersion. The purpose field gives the two maps separate enrollment authority. [Enrollment and receipt workflow](../hack/operator-certificates/README.md)

```sh
jq '.purpose = "backend-client"' "$BACKEND_CLIENT_WORKING_MAP" > "$BACKEND_CLIENT_PUBLISHED_MAP"
kubectl -n openstack-gateway-system create configmap openstack-gateway-backend-clients \
  --from-file=map.json="$BACKEND_CLIENT_PUBLISHED_MAP" --dry-run=client -o yaml |
  kubectl apply --server-side --field-manager=gateway-certificate-operator -f -
```

Add the reference to the Gateway spec. A Secret in another namespace requires a ReferenceGrant from `gateway.networking.k8s.io/Gateway` and the Gateway namespace to the core `Secret` in the Secret namespace.

```yaml
spec:
  tls:
    backend:
      clientCertificateRef:
        name: catalog-client
        namespace: certificates
```

The Gateway reports the current-generation `ResolvedRefs` condition. A missing or unverified TLS Secret reports `InvalidClientCertificateRef`, and a missing cross-namespace grant reports `RefNotPermitted`. The controller polls each configured Gateway for map, metadata and validity changes on its five-second certificate interval, subject to kubelet projection, queue and provider latency. Only the Service ports covered by BackendTLSPolicy use the client certificate. HTTP backends without a TLS policy continue to use their configured transport.

Octavia uses the native `tls_container_ref` pool field from microversion 2.8. The existing regional hostname and, for gRPC, negotiated `h2` capabilities still apply. Stock Octavia can present a client certificate, but this profile requires the strict DNS and gRPC validation described above. Verify a fresh request through the Gateway to a server that requires the expected client certificate. Check that Octavia reports the enrolled reference on the active pool. [Octavia pool API](https://docs.openstack.org/api-ref/load-balancer/v2/#show-pool-details)

During rotation or revocation, the controller disables the affected pool and removes its members before replacing or clearing the reference. It reads back the new identity, CA, hostname and required transport before restoring traffic. Revoking the grant or enrollment leaves the affected pool closed. Removing `clientCertificateRef` from the Gateway allows BackendTLSPolicy server authentication to resume after Octavia confirms the client reference was cleared. Turning off the operator option while the Gateway requests an identity keeps its protected pools closed. Keep the previous Barbican object readable until the closed or replaced pool no longer references it and all other consumers have been checked. Confirm withdrawal with Octavia provisioning status and fresh backend connections.

If the controller cannot list BackendTLSPolicy resources, it can still close owned pools that carry a stale client certificate, then remove their owned members and clear the reference. A new Gateway requesting a client certificate is rejected if the controller is configured without BackendTLSPolicy observation. A previously bound Gateway can still withdraw enrolled identities.

This profile currently accepts one local Service target, one local ConfigMap CA reference with one PEM CA certificate, and a fixed DNS hostname for HTTP/1 and gRPC backends. Explicit DNS or URI `subjectAltNames`, system trust roots and TCP backend re-encryption require additional implementation and qualification. Deployed Hibiscus qualification remains open.
