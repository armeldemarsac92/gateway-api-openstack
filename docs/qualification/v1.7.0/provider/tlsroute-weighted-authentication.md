# Authenticate weighted TLSRoute backends

A terminating TLS listener served two applications on the same port. The first TLSRoute gave weight 3 to its primary Service and weight 1 to a Service across namespaces. Each Service had a BackendTLSPolicy with its own DNS name and CA. A second TLSRoute sent its SNI name to a third Service. The client verified the Gateway’s certificate and each successful reply identified the responding Pod, its backend SNI and the request nonce. [Gateway API v1.7.0 TLSRoute backend rules](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/tlsroute_types.go#L149-L168)

| Channel | State | Primary echoes | Minority echoes | Connections closed | Sibling echoes |
|---|---|---:|---:|---:|---:|
| Standard | Both certificates valid | 364 | 148 | 0 | 40 |
| Standard | Minority certificate has another DNS name | 404 | 0 | 108 | 40 |
| Standard | Minority certificate has an untrusted issuer | 374 | 0 | 138 | 40 |
| Standard | Valid certificate restored | 382 | 130 | 0 | 40 |
| Standard | ReferenceGrant withdrawn | 392 | 0 | 120 | 40 |
| Standard | ReferenceGrant restored | 379 | 133 | 0 | 40 |
| Experimental | Both certificates valid | 390 | 122 | 0 | 40 |
| Experimental | Minority certificate has another DNS name | 383 | 0 | 129 | 40 |
| Experimental | Minority certificate has an untrusted issuer | 394 | 0 | 118 | 40 |
| Experimental | Valid certificate restored | 379 | 133 | 0 | 40 |
| Experimental | ReferenceGrant withdrawn | 388 | 0 | 124 | 40 |
| Experimental | ReferenceGrant restored | 381 | 131 | 0 | 40 |

Each row counts 512 fresh connections to the weighted name and 40 to its sibling after reconciliation. The minority Pod rotated its serving certificate and TLS session-ticket keys together. An independent fresh TLS handshake checked the leaf presented by the Pod and whether the configured trust accepted it before each traffic sample. A certificate issued by its trusted CA for another name and a certificate for its expected name issued by the primary Service’s CA each closed that share. The minority application answered zero requests in both phases. The TLSRoute retained its UID, generation and weights. With the ReferenceGrant withdrawn, its parent reported `Accepted=True` and `ResolvedRefs=False/RefNotPermitted`. The protected Pod accepted zero TCP connections during that sample. Restoring the grant returned `ResolvedRefs=True` and both Services answered. [Observed policies, CA fingerprints, pool weights, status and traffic](tlsroute-weighted-authentication-evidence/receipt.json)

The local run used kind, ServiceAccount permissions, the controller executable from the matching linux/amd64 image, the public Octavia and Barbican APIs, the Amphora-rendered configuration and HAProxy. Cloud identity, Neutron allocation and inventory, asynchronous provisioning and the Amphora agent transport were simulated. Deployment in a Hibiscus region remains to be qualified. [Regional qualification](../live-runs.md) · [Octavia packages and API](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-weighted-pools-3ea393a1a5f4)

The controller tests cover grant withdrawal, weight-zero references, separate Service identities and recovery after pool provisioning fails. [TLSRoute reconciliation](../../../../internal/controller/gateway/gateway_sni_weighted_test.go) · [Pool transitions](../../../../internal/cloud/openstack/graph/tls_sni_backend_pools_test.go)

```sh
go test ./internal/controller/gateway ./internal/cloud/openstack/graph -run 'TestWeightedTerminatingSNIRoute|TestSNIIndependentBackend'
```
