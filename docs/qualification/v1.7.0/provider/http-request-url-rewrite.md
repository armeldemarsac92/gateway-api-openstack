# HTTP request URL rewriting

A tenant can attach a rule-level `URLRewrite` to an HTTPRoute on an enrolled Amphora provider. The controller matches the original host, path and headers, selects the backend, and forwards the request with the configured hostname or path. `ReplacePrefixMatch` supports stripping the prefix. The original query string reaches the backend. An operator enables `--octavia-request-url-rewrite` after the region advertises `request_url_rewrite_v1`. A request header filter on the same rule needs its own operator and regional opt-ins. [Operator settings and filter ordering](../../../operator-octavia-http-actions.md#enable-forwarding-url-rewrites) · [Octavia distribution](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-url-rewrite-provider-ebc2e6f04562) · [Provider library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-url-rewrite-provider-3009042e8073)

## Original Gateway API cases

The controller from the linux/amd64 OCI image at `1206fc60da9d` ran the upstream v1.7.0 Go cases and manifests on two local kind clusters. Each cluster supplied real Pods, Services and EndpointSlices. Octavia `19.0.1.dev32` and octavia-lib `3.12.1.dev9` rendered HAProxy. Keystone, Neutron, worker completion and Amphora agent transport used local fixtures. [Image, case reports and checksums](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-1206fc60da9dbbb1f6557ed7cb62ff0c84e8ff00)

| Original case | Standard | Experimental |
|---|---|---|
| `HTTPRouteRewriteHost` | PASS 3/3 | PASS 3/3 |
| `HTTPRouteRewritePath` | PASS 6/6 | PASS 6/6 |
| `HTTPRouteBackendURLRewrite` | FAIL 0/2 | FAIL 0/2 |

Both successful cases reached `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True` for the current generation after the traffic assertions. The host case selected the expected backend and observed the forwarded hostname. The path case verified full replacement, prefix stripping and request header modification on the same rule. The backend-level case was rejected with `Accepted=False/UnsupportedValue` before it sent a request.

The baseline image at `069b0de0ed94` used the previous provider release with URL rewriting unavailable. All three cases stopped before the request assertions on both channels. The evidence archive includes the baseline, the first controller candidate and the reviewed image. [Case receipts and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-1206fc60da9dbbb1f6557ed7cb62ff0c84e8ff00)

## Provider behavior

The installed provider wheels preserved the original query across both HTTP/1 HAProxy engines, including when the selected policy forwarded to either of two weighted pools. On the TLS and HTTP/2 input, the HTTP/1 backend observed the new Host and an absolute request target with that hostname. A mismatched TLS SNI and HTTP authority returned 421. [Master integration](https://github.com/armeldemarsac92/octavia/pull/15) · [Provider payload](https://github.com/armeldemarsac92/octavia-lib/pull/9)
