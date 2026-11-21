# Rule-level request header modification with Amphora

A rule-level HTTPRoute `RequestHeaderModifier` sets, adds or removes headers before the selected backend sees the request. The image at controller source `ba6c47689d58a640523fd39e18c3a4bb3a9e0b31` was tested with both Gateway API v1.7.0 bundles. Each run used the matching forked Octavia API on SQLite, the Amphora renderer, HAProxy 3.0.19, the pinned echo-basic executable and a Kubernetes 1.36.2 API server with etcd. The fixture simulated Keystone, Neutron, worker completion and Amphora transport. [Operator setup](octavia-policy-header-proposal.md#operator-setup) · [Image build](../image-build.md) · [Octavia source](https://github.com/armeldemarsac92/octavia/tree/2c7d92f58a3e6c330c94d3149733f7e41d0fd165) · [octavia-lib source](https://github.com/armeldemarsac92/octavia-lib/tree/03e485181dfabc16e52f671bffa6977179e74cf5)

## Original Gateway API case

| Registered case and readback | Standard | Experimental |
| --- | --- | --- |
| `HTTPRouteRequestHeaderModifier` | PASS (7/7 traffic subtests) | PASS (7/7 traffic subtests) |
| Active forwarding policies with stored operations on one pool | 5 | 5 |
| Ordinary miss on the same listener | HTTP 503 | HTTP 503 |

The original traffic checks cover set, add, remove, combined edits and case-insensitive header names. The test enables only `request_header_operations_v1` on the provider and only `--octavia-request-header-modifier` on the controller. The separate fixed-response capability is off, so the unmatched control uses the stock listener fallback. [Original Gateway API case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-request-header-modifier.go) · [Stock provider baseline](stock-core-http-cases.md)

The upstream polling log includes HTTP 503 while the policies converge. The receipt keeps those retries and separately records the passing traffic subtests. At case exit the Gateway was programmed and the Route had `Accepted=True` and `ResolvedRefs=True`. The controller-specific Route `Programmed` condition still read `False/Pending`.

## Independent provider checks

The provider master integration replay observed HTTP 200 with the configured operations on the winning exact policy and HTTP 200 with original headers on the neighboring prefix policy, both using the same pool. Previously matching reject and redirect rules returned HTTP 403 and HTTP 302 using the original headers. Provider opt-out returned HTTP 501 for a new nonempty update and kept applying the stored operations. Clearing the field returned the exact policy to HTTP 200 with the original headers. These checks ran through Octavia WSGI, RPC payload capture and a real local HAProxy process on the Octavia and octavia-lib master integration trees `8f2fcec841ae8d8acb5ffa6ed9fb7abf1745272c` and `6af19d3b6b8f287f789940b5305d7541939a96e4`.

MariaDB 11.4.13 applied migration `45fa4018c609` on the Hibiscus and master provider trees. Each replay preserved a legacy L7 policy with a null value for the new column and read back a newly written JSON array. The Magnum conductor also rendered the pinned parent chart offline with and without the header-only workload overlay. Those runs cover the schema upgrade and artifact rendering, respectively.

## Weighted ReferenceGrant regression

The image was separately tested with only fixed responses and weighted targets enabled. The same HTTPRoute kept its 3:1 weights. Each column shows the minority count in 1,024 fresh connections / 1,024 requests over one keep-alive connection.

| Phase | Minority response | Standard | Experimental |
| --- | --- | --- | --- |
| Grant present | HTTP 200 from protected Service | 265 / 261 | 259 / 280 |
| Grant removed | Empty HTTP 500 | 232 / 249 | 242 / 262 |
| Manager restarted while grant absent | Empty HTTP 500 | 255 / 248 | 244 / 264 |
| Grant restored | HTTP 200 from protected Service | 260 / 238 | 255 / 264 |

The protected backend received no routed traffic while the grant was absent and stayed reachable directly. Restart recovered the policy without Octavia writes. Deleting the route left the listener serving HTTP 404. Deleting the Gateway cleared the provider fixture. The earlier image's four original HTTP action case results are recorded separately. [Earlier HTTP action results](opt-in-http-actions.md)

The archive contains the replay sources and four final-image receipts and the separate MariaDB receipts. Its SHA-256 is `084563e0f1b28164471b08635303399912d928fc5378f902fad30669baa8ec25`. Backend-scoped filters, header-based route matching and the live Gateway and mesh profiles require separate evidence. [Download the replay archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-ba6c47689d58a640523fd39e18c3a4bb3a9e0b31/hibiscus-request-headers-ba6c47689d58.tar.gz) · [Live profile ledger](../live-runs.md)
