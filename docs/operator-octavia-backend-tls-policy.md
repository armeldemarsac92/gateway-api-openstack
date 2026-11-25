# Authenticate HTTPRoute and GRPCRoute backends with BackendTLSPolicy

An application can require the Gateway to encrypt its connection to a Service and verify the server's certificate identity. HTTPRoute can attach to an HTTP listener or a listener that terminates HTTPS. The gRPC profile uses an HTTP listener. On the upstream connection, Amphora sends the policy hostname as SNI and verifies the certificate against the CA selected by the policy.

## Enable the regional capability

1. Build the Amphora image with the strict DNS HAProxy executable and install the matching Amphora agent. Upgrade the Octavia database, octavia-lib, API and workers. Verify that each active and standby Amphora can run the configured binary. [Amphora image build instructions](https://github.com/armeldemarsac92/octavia/blob/ba89a2d6237aa1a1b754ad7234899c12de39a4d2/elements/haproxy-octavia/README.rst) · [Matching Hibiscus packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-hostname-ba89a2d6237a)
2. Set `enabled_backend_tls_hostname_providers = amphora` in Octavia's `[api_settings]`. Using the controller's regional catalog and project, check that `GET /v2/lbaas/providers/amphora/gateway_capabilities` includes `backend_tls_hostname_v1`.
3. Supply a stable cluster ID and a dedicated project-scoped identity with permission to publish, verify and retire its CA revisions in regional Barbican. The Octavia service identity needs access to each configured CA through its normal certificate manager.
4. For a standalone installation render `config/octavia-backend-tls-policy`. For the Magnum workload addon render `integrations/magnum-capi-helm/workload-octavia-backend-tls-policy`. Each overlay sets `--octavia-backend-tls-policy` and Octavia microversion 2.8, and grants access to `BackendTLSPolicy`, its status and ConfigMaps. The controller watches ConfigMap metadata and reads the referenced CA after verifying the authorized Service port. If the installation also uses the Experimental release bundle, include the existing Experimental component. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

For GRPCRoute, enable the existing gRPC profile and install the provider and Amphora builds that enforce negotiated HTTP/2. Set `enabled_backend_tls_h2_alpn_providers = amphora` alongside the hostname setting, then verify that the regional capability response includes `backend_tls_h2_alpn_v1` and `backend_tls_hostname_v1`. The controller requests `backend_tls_require_h2: true` and checks it alongside the configured hostname and CA on the owned pool before enabling it or enrolling members. Amphora closes backend TLS connections that do not negotiate `h2`. Missing capability, missing readback or invalid trust keeps the affected backend unavailable. The Service port may omit `appProtocol` or declare `https`. A port declaring `kubernetes.io/h2c` cannot use this policy.

For explicit DNS or URI identities, install the Octavia API, worker, database migration, octavia-lib and Amphora builds that support backend SAN validation. Set `enabled_backend_tls_subject_alt_names_providers = amphora` alongside the hostname setting. Using the same regional endpoint, verify that the capability response includes `backend_tls_subject_alt_names_v1`. The existing controller flag `--octavia-backend-tls-policy` handles SAN policies in regions that advertise this capability. For gRPC, the region must also advertise the existing HTTP/2 ALPN capability.

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

A policy can also reference a named cluster-scoped trust bundle with the controller opt-in. [Use a ClusterTrustBundle](operator-cluster-trust-bundle.md)

### Require explicit certificate identities

Add up to five `subjectAltNames` to `spec.validation` to authenticate the leaf certificate by DNS or URI. The following entries allow either the DNS name or the exact URI. The certificate must still chain to the configured CA.

```yaml
    subjectAltNames:
      - type: Hostname
        hostname: catalog.svc.example
      - type: URI
        uri: spiffe://identity.example/workloads/catalog
```

The controller still sends `validation.hostname` as SNI. To allow that hostname as a certificate identity, include it as a DNS entry. Without a non-empty list, Amphora verifies the DNS SAN matching `validation.hostname`. URI case and percent escapes must match the leaf certificate bytes. Use an absolute URI of at most 253 ASCII characters. URI authorities must use a dotted host or a bracketed IPv6 address.

In a region that lacks SAN support, the affected pool stays closed and the policy ancestor reports `Accepted=False` with reason `UnsupportedProvider`. Failed capability discovery reports `Accepted=Unknown` with reason `Pending`. A malformed list reports `Accepted=False` with reason `Invalid`. Other backends in the route keep their configured behavior and weights.

## Protect an Experimental XBackend

The local kind ServerOnly qualification served HTTP 200 with the configured SNI and negotiated ALPN `http/1.1`. Removing the selected Pod backend's CA ConfigMap produced three HTTP 503 responses and zero additional application requests. Restoring the CA resumed traffic. For an ExternalHostname, changing DNS to an origin presenting a different DNS identity produced three HTTP 503 responses. The same-named ordinary Service remained available in all phases. [Pool settings, conditions and traffic](qualification/v1.7.0/provider/xbackend-server-tls-evidence/receipt.json)

Install the Experimental Gateway API bundle and enable the regional backend TLS capability described above. Render `config/octavia-xbackend-server-only` for a standalone installation or `integrations/magnum-capi-helm/workload-octavia-xbackend-server-only` for Magnum. These overlays enable the Experimental controller profile and `--octavia-backend-tls-policy`, and select `--octavia-microversion=2.24`, the first Octavia API version with pool ALPN. In a custom installation, configure one microversion argument at 2.24 or newer. If you compose Kustomize components, put `config/components/octavia-pool-alpn-microversion` after the backend TLS and any frontend client validation components. [Octavia pool API](https://docs.openstack.org/api-ref/load-balancer/v2/#show-pool-details)

Use an Endpoint member topology with Unmanaged backend security. For `EndpointSelector`, add the following to an XBackend with a Pod selector and a literal `spec.port.number`. The HTTPRoute references the XBackend by group, kind and name in its own namespace, omitting `backendRef.port`.

```yaml
spec:
  protocol: HTTP11
  tls:
    mode: ServerOnly
    validation:
      hostname: catalog.internal.example
      caCertificateRefs:
        - group: ""
          kind: ConfigMap
          name: catalog-ca
```

Put one PEM CA certificate in `ca.crt` in the XBackend namespace. You can also use a single [ClusterTrustBundle](operator-cluster-trust-bundle.md) with the same operator opt-in, or specify explicit DNS or URI SANs where the regional capability is available. `validation.hostname` is required and supplies SNI. The certificate must match that DNS name unless you explicitly configure SANs.

`ServerOnly` supports `HTTP` and `HTTP11`. It uses HTTP when you omit `protocol` and offers HTTP/1.1 over ALPN. It also works with an operator-authorized `ExternalHostname` backend. DNS determines the permitted IPv4 destinations according to the existing GatewayClass and CIDR configuration. `validation.hostname` supplies the TLS identity independently of `externalHostname.hostname`. The Gateway-wide Service client certificate is outside the XBackend's `ServerOnly` configuration.

Check the XBackend's current-generation `Accepted` condition under the Gateway in `status.ancestors` and the HTTPRoute's `ResolvedRefs` condition. A missing or invalid CA reports a reference error. A microversion below 2.24 reports `UnsupportedProvider` for ServerOnly. An unconfirmed trust or provider dependency reports `Pending`. An authenticated pool in Octavia `ERROR` is disabled through a pool update before member removal. The controller waits for an `ACTIVE` readback, drains the pool, then verifies the current trust settings and pool ALPN before restoring members. Test both an accepted backend certificate and a rejected identity through the Gateway. The local trial recorded the configured API version, pool ALPN writes and HTTP responses. [Local results](qualification/v1.7.0/provider/xbackend-server-tls-evidence/receipt.json)

`None` is the default TLS mode. Authenticated XBackends support HTTP and HTTP/1.1 with an explicit CA reference. System trust and authenticated HTTP/2 remain open.

### Present an XBackend client certificate

Use `ClientAndServer` on an Experimental HTTPRoute's `EndpointSelector` or operator-authorized `ExternalHostname` XBackend. Enable the same endpoint topology, backend TLS capability and pool ALPN microversion as for `ServerOnly`. Keep the existing server validation, including the `validation.hostname` used as SNI, and add the XBackend's own `clientCertificateRef`.

```yaml
spec:
  protocol: HTTP11
  tls:
    mode: ClientAndServer
    clientCertificateRef:
      name: catalog-client
      namespace: certificates
    validation:
      hostname: catalog.internal.example
      caCertificateRefs:
        - group: ""
          kind: ConfigMap
          name: catalog-ca
```

Set `--xbackend-client-certificate-map=/etc/gateway-operator/xbackend-clients/map.json` or `GATEWAY_OPENSTACK_XBACKEND_CLIENT_CERTIFICATE_MAP` and mount the separate operator-controlled directory read-only. Use the enrollment helper with a dedicated working map. Verify the Secret contains the TLS client certificate and matching key. Store the same chain and key in a new immutable, unencrypted Barbican PKCS#12 Secret and grant Octavia read access. Publish the helper's version 1 snapshot with top-level `"purpose": "xbackend-client"`, the exact cluster, GatewayClass, project and region, and one verified row for the Secret UID and resourceVersion. Give the controller `get` only for the approved Kubernetes Secret names via `resourceNames`. It observes Secret metadata and does not need Secret `list` or `watch`. [Enrollment helper](../hack/operator-certificates/README.md)

An omitted `clientCertificateRef.namespace` selects the XBackend namespace. A cross-namespace reference requires a `ReferenceGrant` in the Secret's namespace. Its `from` entry uses group `gateway.networking.x-k8s.io`, kind `XBackend` and the XBackend namespace. Its `to` entry uses group `""`, kind `Secret` and the client Secret name. The Gateway's `backend-client` enrollment authorizes Service pools. This XBackend enrollment authorizes only `ClientAndServer` pools that explicitly reference the Secret.

Watch the XBackend's `Accepted` condition for the managed ancestor and the HTTPRoute's `ResolvedRefs`. `RefNotPermitted` indicates a missing grant. `InvalidClientCertificateRef` indicates a bad reference, missing or invalid enrollment, or mismatched Secret revision. I/O errors reading the enrollment map report `Unknown` with reason `Pending`. Both enrollment failures withhold the client identity. These reasons extend the XBackend's v1.7.0 `Accepted` contract. HTTPRoute `Programmed` reflects the observed Octavia configuration, which can include an intentionally unavailable backend. In a configured installation the controller revisits referencing HTTPRoutes and mutual-TLS XBackend status at most every five seconds between settled reconciles, subject to map projection, queue and provider latency.

For rotation, publish the newly enrolled revision and keep the previous Barbican object readable until Octavia reports its reference gone from all consumers. To revoke access, remove the grant or the enrollment. The controller closes and drains the affected pool before changing or clearing its client identity, even if it cannot read a neighboring Service's TLS policy. It observes the authorized identity, verified CA and SNI, and ALPN `http/1.1` before restoring members. Switching to `ServerOnly` resumes server-only TLS after clearing the old identity. Switching to `None` with a valid plaintext spec resumes forwarding after Octavia confirms the TLS settings were removed. Gateway-wide identity withdrawal leaves XBackend pool identities alone.

On a local Experimental Gateway, an EndpointSelector origin required a client certificate. It returned HTTP 200 for three requests with certificate A. Removing the cross-namespace ReferenceGrant produced three HTTP 503 responses and zero new requests at the origin. The route stayed accepted and reported `ResolvedRefs=False/RefNotPermitted`. Octavia disabled and emptied the pool and cleared its client certificate. Restoring the grant produced three HTTP 200 responses with A. After rotation, the origin observed certificate B on all three requests. HAProxy verified the origin's configured DNS identity and negotiated HTTP/1.1. A separate Service returned HTTP 200 without a client identity in every phase. [Observed pool configuration, certificate fingerprints and responses](qualification/v1.7.0/provider/xbackend-client-tls-evidence/receipt.json)

The route's controller-specific `Programmed` condition was still `False/Pending` at the first successful traffic observations. That trial measured the traffic and the reference conditions. Standard, where the XBackend CRD is absent, served the ordinary Service. Mutual TLS to an ExternalHostname and a deployed Hibiscus run remain open.

## Verify and rotate trust

Read `status.ancestors` on the policy for the managed Gateway and current generation. `Accepted=True` and `ResolvedRefs=True` describe the accepted policy and resolved CA. Send traffic through the Gateway to verify the backend certificate. A server presenting the wrong DNS identity or a certificate from an untrusted CA must receive no HTTP request. The matching client request receives a 5xx response.

A policy can record 16 ancestors across all controllers. The Gateway waits for its own row before forwarding to the protected Service. If all rows belong to other ancestors, check the Route's controller-specific `Programmed` condition and the Gateway's `BackendTLSPoliciesReady` condition. Existing ancestors continue serving. When a row becomes available, the controller records the waiting Gateway and reconciles its backend.

Changing the public CA certificate in `ca.crt` creates a new immutable Barbican revision for this Gateway. The controller withdraws traffic from the affected pool while it changes trust and checks the effective hostname, CA and any explicit SANs before putting members back in service. A certificate rotation that changes the issuing CA needs coordination with the backend operator. Check a successful request after the rotation and a refusal with the previous trust configuration. An invalid policy or a missing CA keeps the protected backend unavailable. Its Service can still be a resolved reference on the route.

To return a protected backend to hostname authentication, remove `subjectAltNames` from the policy. The controller disables and drains the pool, clears the SAN list on Octavia, and requires an explicit empty list in a separate read before reopening the pool. It also verifies the hostname and CA before restoring traffic. Octavia must keep reporting the empty list for that pool, including after a controller restart or policy deletion. This works after a regional SAN opt-out as long as the upgraded API remains available and the region still advertises the capabilities required by the remaining policy. Upgrade the backend certificate to match the hostname before making the change if it currently relies on a different identity.

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

During rotation or revocation, the controller disables the affected pool and removes its members before replacing or clearing the reference. It reads back the new identity, CA, hostname, explicit SANs when configured, and required transport before restoring traffic. Revoking the grant or enrollment leaves the affected pool closed. Removing `clientCertificateRef` from the Gateway allows BackendTLSPolicy server authentication to resume after Octavia confirms the client reference was cleared. Turning off the operator option while the Gateway requests an identity keeps its protected pools closed. Keep the previous Barbican object readable until the closed or replaced pool no longer references it and all other consumers have been checked. Confirm withdrawal with Octavia provisioning status and fresh backend connections.

If the controller cannot list BackendTLSPolicy resources, it can still close owned pools that carry a stale client certificate, then remove their owned members and clear the reference. A new Gateway requesting a client certificate is rejected if the controller is configured without BackendTLSPolicy observation. A previously bound Gateway can still withdraw enrolled identities.

This profile accepts one local Service target, one local ConfigMap CA reference with one PEM CA certificate, and a fixed DNS hostname for HTTP and gRPC backends. Terminating TLSRoutes can also authenticate a TCP backend when the region advertises `tcp_tls_backend_auth_v1`. System trust roots still need implementation and qualification. [TCP backend configuration and traffic](qualification/v1.7.0/provider/tlsroute-termination.md#authenticate-the-tcp-backend) Deployed Hibiscus qualification remains open.
