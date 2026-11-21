# Public HTTP responses and weighted invalid backends

This design would give a direct Amphora Gateway an explicit response when no route matches and preserve the traffic share of an invalid backend. It requires a new public Octavia capability. The tagged policy action can forward to one pool, redirect or reject. The redirect status field accepts redirect codes. The stock v2 API refused 404 and 500 in that field. [Stock schema](stock-api-snapshot.txt) · [Public API probe](stock-http-api.py) · [Gateway API rule outcomes](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L273-L299)

## Observed responses on stock Octavia 19

With a healthy echo server, the matched control returned 200. An unmatched path and an exact rule targeting a pool with no members each returned 503. A policy with the public REJECT action returned 403. For a logical 3:1 split with one invalid reference, omitting its target in the provider pool sent 64 of 64 fresh requests to the healthy weight-3 member. All 68 requests received an HTTP response. These measurements cover this encoding on the public Octavia WSGI API, local SQLite, its Amphora template and HAProxy 3.0.19. The API returned 400 for the trial fixed-response actions and for a 500 response code on REJECT. [Responses and API readback](stock-http-local-replies-results.json) · [Portable provider exercise](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-2b084bc00110b84155c2f8b6df03966186d8a771/hibiscus-http-ports-2b084bc00110.tar.gz)

## Gateway API requirements

| Gateway API input | Expected result |
|---|---|
| No attached rule matches this request | HTTP 404 |
| Matching rule has omitted or empty backendRefs and no response-producing filter | HTTP 500. An omitted reference does not make another resource unresolved |
| Positive-weight backend reference is invalid | Its share receives HTTP 500, including when every reference is invalid. Report `ResolvedRefs=False` with the appropriate reason |
| Valid Service with no Ready endpoints | HTTP 503 is the recommended response for its share |
| A valid Service of weight 3 plus a missing Service of weight 1 | The valid Service keeps three quarters of requests and local HTTP 500 takes one quarter |
| Zero-weight invalid reference alongside a valid positive-weight reference | Zero requests select that invalid reference. Resolve and report references independently of traffic weights |

Backend weights apply to logical references. A second Ready endpoint in the weight-3 Service must not double that Service’s share. A fixed redirect which generates its own response requires no backend selection. [Unmatched request](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L214-L217) · [Omitted backends](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-omitted-backendrefs.go) · [Backend weights](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/shared_types.go#L315-L330)

## Proposed public fields

The field names below are design inputs. No microversion or provider currently advertises them. Discovery would be specific to the selected provider and load-balancer configuration. Support for request-header operations, local HTTP responses and weighted policy targets would be advertised independently. [Related header proposal](octavia-policy-header-proposal.md)

- An optional listener `default_http_response` would select a bounded status when no enabled L7 policy matches. Require an HTTP or terminated HTTPS listener. Reject a competing default pool. Omitting the field on PUT preserves its value, and explicit null restores native listener fallback.
- A policy action `FIXED_RESPONSE` with `fixed_response.status_code` would generate a response when that policy wins. The minimal capability accepts 404, 500 and 503, with an empty application payload and without a redirect Location header. Reject simultaneous redirect or forward targets. GET, update and nested create must agree on the representation.
- A policy action `WEIGHTED_TARGETS` would carry an ordered list of alternatives. Each entry has an integer weight and exactly one of `pool_id` and `fixed_response`. A minimal version can support 1–16 entries with weights from 0 to 1,000,000 and require a positive total. An operator can configure one standalone fixed response when no eligible positive target exists. PUT replaces the list atomically. Readback must preserve weights and the target type. Require project and parent-LB checks on every pool. [Tagged API limit for each backend weight](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/shared_types.go#L315-L331)

For the 3:1 example the proposed winning policy would contain these two alternatives.

```json
{"action":"WEIGHTED_TARGETS","targets":[{"pool_id":"<owned valid pool>","weight":3},{"fixed_response":{"status_code":500},"weight":1}]}
```

The provider must select the winning route using the original request, then apply the rule's response-producing action or select one weighted alternative for that request. A local response bypasses all backend connections. When a missing Service appears, replace its fixed alternative with the validated pool under the same weight. Revoke a cross-namespace backend’s authorization by removing that pool alternative from the serving policy. Retain its weight through a local 500 response. Validate that policy and pool readback agree before removing an unreferenced pool.

## Qualification boundary

Run the original omitted-backend and invalid-reference cases on both channels. Also run a positive 3:1 route, a 3:1 route with one invalid reference, and the same route after the reference becomes valid. Check the response code, application identity or absence of backend calls, and the ratio over sufficiently many independent requests. Repeat over a reused connection to show that selection is per request. A valid two-endpoint Service must keep the same aggregate share. Keep normal unmatched 404 distinct from a backend application's own 404.

A provider upgrade must reject a new field or advertise lack of support while any required worker cannot render it. Require public readback, successful provisioning, packets after an Amphora failover, and preservation of sibling routes when updating or removing one policy. The AMPHORA implementation and live interoperability remain open. This study has no provider code, proxy configuration, or production rollout.
