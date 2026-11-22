# Authenticate HTTPRoute backends with BackendTLSPolicy

An application can require the Gateway to encrypt its connection to a Service and verify the server's DNS identity. The listener can receive HTTP or terminate HTTPS. On the upstream connection, Amphora sends the policy hostname as SNI and verifies the certificate against the CA in the Service namespace.

## Enable the regional capability

1. Build the Amphora image with the strict DNS HAProxy executable and install the matching Amphora agent. Upgrade the Octavia database, octavia-lib, API and workers. Verify that each active and standby Amphora can run the configured binary. [Amphora image build instructions](https://github.com/armeldemarsac92/octavia/blob/ba89a2d6237aa1a1b754ad7234899c12de39a4d2/elements/haproxy-octavia/README.rst) · [Matching Hibiscus packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-hostname-ba89a2d6237a)
2. Set `enabled_backend_tls_hostname_providers = amphora` in Octavia's `[api_settings]`. Using the controller's regional catalog and project, check that `GET /v2/lbaas/providers/amphora/gateway_capabilities` includes `backend_tls_hostname_v1`.
3. Supply a stable cluster ID and a dedicated project-scoped identity with permission to publish, verify and retire its CA revisions in regional Barbican. The Octavia service identity needs access to each configured CA through its normal certificate manager.
4. For a standalone installation render `config/octavia-backend-tls-policy`. For the Magnum workload addon render `integrations/magnum-capi-helm/workload-octavia-backend-tls-policy`. Each overlay sets `--octavia-backend-tls-policy` and Octavia microversion 2.8, and grants access to `BackendTLSPolicy`, its status and ConfigMaps. The controller watches ConfigMap metadata and reads the referenced CA after verifying the authorized Service port. If the installation also uses the Experimental release bundle, include the existing Experimental component. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

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

An HTTPRoute continues to refer to the Service port by number. A cross-namespace backend also requires the usual ReferenceGrant in the Service namespace. Omitting `sectionName` applies the policy to the Service as a whole. A matching port-specific policy takes precedence. Competing policies for the same target and section are ordered by creation timestamp and name. [Service grant lifecycle](qualification/v1.7.0/service-grants.md)

## Verify and rotate trust

Read `status.ancestors` on the policy for the managed Gateway and current generation. `Accepted=True` and `ResolvedRefs=True` describe the accepted policy and resolved CA. Send traffic through the Gateway to verify the backend certificate. A server presenting the wrong DNS identity or a certificate from an untrusted CA must receive no HTTP request. The matching client request receives a 5xx response.

A policy can record 16 ancestors across all controllers. The Gateway waits for its own row before forwarding to the protected Service. If all rows belong to other ancestors, check the Route's controller-specific `Programmed` condition and the Gateway's `BackendTLSPoliciesReady` condition. Existing ancestors continue serving. When a row becomes available, the controller records the waiting Gateway and reconciles its backend.

Changing the public CA certificate in `ca.crt` creates a new immutable Barbican revision for this Gateway. The controller withdraws traffic from the affected pool while it changes trust and checks the effective hostname and CA before putting members back in service. A certificate rotation that changes the issuing CA needs coordination with the backend operator. Check a successful request after the rotation and a refusal with the previous trust configuration. An invalid policy or a missing CA keeps the protected backend unavailable. Its Service can still be a resolved reference on the route.

During rollback, keep the upgraded workers, strict Amphora runtime and Barbican identity available until protected routes and their cloud resources have been withdrawn. The controller records CA ownership in a Gateway finalizer before publication. After opt-out it keeps enrolled pools closed and can still retire this Gateway's CAs on deletion. The controller checks regional project pool and listener CA references before retiring an unused revision. A CA still used elsewhere is retained.

This profile currently accepts one local Service target, one local ConfigMap CA reference with one PEM CA certificate, and a fixed DNS hostname for HTTP/1 backends. Explicit DNS or URI `subjectAltNames`, system trust roots, client certificates and gRPC or TCP backend re-encryption require additional implementation and qualification. Deployed Hibiscus qualification remains open.
