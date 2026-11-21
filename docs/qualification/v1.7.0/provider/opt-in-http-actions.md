# HTTP actions with the opt-in Amphora provider

The opt-in provider keeps the declared HTTPRoute weights while a cross-namespace ReferenceGrant is revoked or restored. A request assigned to a denied backend gets an empty HTTP 500 without reaching the Service. A request that misses all route rules gets HTTP 404. The immutable controller was qualified with both Gateway API v1.7.0 CRD bundles. [Operator setup](../../../operator-octavia-http-actions.md) · [Stock-provider results](stock-core-http-cases.md) · [Live profile ledger](../live-runs.md)

The controller bytes were extracted from the linux/amd64 OCI image at source `394afddf983752c0683c30c170de363d9ff129dc`. The local Octavia API used the forked Octavia and octavia-lib revisions below. The fixture ran Kubernetes 1.36.2 API server and etcd, Octavia WSGI with SQLite, the Amphora HAProxy renderer and HAProxy 3.0.19. It simulated Keystone, Neutron, worker completion and Amphora transport. The scoped upstream cases used the original case bodies, manifests and echo-basic executable with locally provisioned Services and EndpointSlices. [Image build for the tested image](https://github.com/armeldemarsac92/gateway-api-openstack/blob/4507bf3fc6edeea76727ac6ef47cacf319116f05/docs/qualification/v1.7.0/image-build.md) · [Octavia source](https://github.com/armeldemarsac92/octavia/tree/032e44b82cb58361aec149f874c8eacbe89c275f) · [octavia-lib source](https://github.com/armeldemarsac92/octavia-lib/tree/3c960017bc9c47783607f0efe7a24512cc9c1d2f)

## Original Gateway API cases

| Registered case | Standard | Experimental | Wire and status checks |
| --- | --- | --- | --- |
| `HTTPRouteWeight` | PASS | PASS | The original 70/30/0 statistical test passed with 500 HTTP 200 responses on each channel. |
| `HTTPRouteNoBackendRefs` | PASS | PASS | The forwarding path reached its Service. Both paths with omitted or empty backend references returned HTTP 500. |
| `HTTPRouteExactPathMatching` | PASS | PASS | `/one` and `/two` reached their own Services. The original four case-sensitive or non-exact misses returned HTTP 404. |
| `HTTPRouteInvalidCrossNamespaceBackendRef` | PASS | PASS | The Route reported `ResolvedRefs=False/RefNotPermitted`. A request to the invalid backend returned HTTP 500. |

The native receipt captures conditions when the case returns. The controller-specific route `Programmed` condition can still be `False/Pending` at that point. The weight run also polls it to `True/Programmed`. The lifecycle below waits for settled Kubernetes and Octavia status before sampling. [Gateway API case inventory](../matrix/features-and-cases.md#complete-registered-test-inventory)

## Three additional HTTPRoute Core cases

Three selected original v1.7.0 cases each passed on Standard and Experimental with the same released manager and both opt-in flags enabled. The class used IPv4 Endpoint mode so the upstream ClusterIP Services kept their original type. [Per-case receipts and input digests](opt-in-http-core-results.json)

| Registered case | Standard | Experimental | What the original case checked |
| --- | --- | --- | --- |
| `HTTPRouteObservedGenerationBump` | PASS | PASS | Changing the backend from `infra-backend-v1` to `infra-backend-v2` updated all parent conditions to generation 2. `Accepted` and `ResolvedRefs` were both true. |
| `HTTPRouteReferenceGrant` | PASS (2/2) | PASS (2/2) | The Gateway returned HTTP 200 from `gateway-conformance-web-backend/web-backend` with the grant present, then converged to HTTP 500 after the test deleted that grant through the API. `ResolvedRefs` was `False/RefNotPermitted` at readback and the Route kept generation 1. |
| `HTTPRoutePathMatchOrder` | PASS (6/6) | PASS (6/6) | Exact matches and longer path prefixes reached the expected one of three upstream Services. |

Each channel had its own Kubernetes API server and published release bundle. The upstream case code, manifest application and HTTP assertions were unchanged. A scoped base supplied the original HTTP Gateway and four Services. Seven independent echo-basic processes ran in separate network namespaces, including two replicas with the original `web-backend` identity. The envtest API held Pod readiness records based on successful direct probes and EndpointSlices pointing to those peers. These results cover the selected cases with the opt-in provider. The [earlier stock observations](http-attachments.md), [full local profile attempts](../complete-local-profiles.md) and [deployed Hibiscus profile ledger](../live-runs.md) retain their own results.

## Settled ReferenceGrant lifecycle

The same HTTPRoute keeps its UID, generation, parent and 3:1 weights across grant removal and restoration. The HAProxy samples contain 1,024 requests from fresh connections and 1,024 requests over one keep-alive connection in each phase and channel. Each value below is the count for the one-quarter target, shown as fresh / keep-alive.

| Phase | Minority response | Standard | Experimental |
| --- | --- | --- | --- |
| Grant present | HTTP 200 from protected Service | 252 / 249 | 263 / 269 |
| Grant removed | Empty HTTP 500 | 262 / 272 | 247 / 242 |
| Controller restarted while grant absent | Empty HTTP 500 | 262 / 242 | 290 / 273 |
| Grant restored | HTTP 200 from protected Service | 253 / 258 | 266 / 239 |

The protected Service received zero proxied requests while the grant was absent and still answered a direct probe. The in-place policy update removed the protected target before its pool was deleted, including during the temporary simulated Neutron address-read outage. A cold controller restart recovered the policy and rules without Octavia mutations. Route deletion removed the policy and both pools and left the listener serving an empty 404. Gateway deletion cleared the fixture inventory and stopped HAProxy.

These are scoped local results. The [live ledger](../live-runs.md) still records no completed profile runs on deployed Hibiscus.

The release asset contains all seven receipts, the harness and the replay guide. Its SHA-256 is `f071af67f772ace318558c480ea8b79cc39ea54570a1a1699dcc3469fd3c9723`. [Download qualification archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-394afddf983752c0683c30c170de363d9ff129dc/hibiscus-http-actions-394afddf9837.tar.gz)
