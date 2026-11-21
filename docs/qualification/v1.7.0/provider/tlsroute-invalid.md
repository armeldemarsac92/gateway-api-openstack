# TLSRoute invalid references and listener selection

Four unchanged Gateway API v1.7.0 cases passed on each local release channel with exact-SNI passthrough and operator-managed HTTPS enabled. The controller reports the reason against the specific parent reference and route generation. The replay used a Kubernetes 1.36.2 API server, etcd 3.6.8, wheel-installed Octavia 19.0.1.dev4 and octavia-lib 3.12.1.dev3, Barbican and HAProxy 3.0.19. [Standard results](tlsroute-invalid-evidence/standard.json) · [Experimental results](tlsroute-invalid-evidence/experimental.json)

| Upstream case | Observed result |
| --- | --- |
| `TLSRouteInvalidReferenceGrant` | The attachment was accepted. The cross-namespace backend reference reported `ResolvedRefs=False/RefNotPermitted`. |
| `TLSRouteInvalidBackendRefNonexistent` | The attached route reported `ResolvedRefs=False/BackendNotFound`. A new TLS handshake through the VIP closed before completing. |
| `TLSRouteInvalidBackendRefUnknownKind` | The attached route reported `ResolvedRefs=False/InvalidKind`. A new TLS handshake through the VIP closed before completing. |
| `TLSRouteInvalidNoMatchingListener` | Routes to the HTTP-only and HTTPS-only Gateways reported `Accepted=False/NotAllowedByListeners`. Each listener reported zero attached routes. The nonexistent section on the TLS Gateway reported `Accepted=False/NoMatchingParent`. |

On the preceding image the same three reference cases passed. The HTTP and HTTPS listener assertions failed on both channels because the Route did not publish the two managed parent entries. Both Gateways had reached `Programmed=True`. A separate regression on the current Standard image verified that an exact hostname completed TLS with its intended upstream echo backend and that incompatible hostnames stayed rejected. [Previous Standard](tlsroute-invalid-evidence/baseline-standard.json) · [Previous Experimental](tlsroute-invalid-evidence/baseline-experimental.json) · [Standard hostname regression](tlsroute-invalid-evidence/hostname-standard.json)

The linux/amd64 manager came from `f150a6e006d027106da2aaed6b82cb262bcf1b1a` at manifest `sha256:4328c81191c1c9daba6ec011d39f1f3a049bd477a90e15fe8c8673f5b23002b7`. The isolated provider fixture supplied Keystone, Neutron, provisioning completion and delivery of configuration to HAProxy. The original tests asserted Kubernetes status for the grant and listener cases and made TLS connections for the invalid backends. The live GATEWAY-TLS profile awaits a designated region. [Provider setup](../../../operator-octavia-tls-sni-passthrough.md) · [Build record](../image.json) · [Regional profile ledger](../live-runs.md)

Download the OCI archive and `hibiscus-xbackend-tlsroute-f150a6e006d0.tar.gz` from the release. The source replay has SHA-256 `2a28c64679872123c4b96baf2b501d42f85936dbd445b6db33caf3095940d705`. [Release and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-f150a6e006d027106da2aaed6b82cb262bcf1b1a)
