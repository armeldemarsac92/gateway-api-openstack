# Exact HTTP query parameter matches

An HTTPRoute can select a backend using the name and value of a query parameter. Enable `--octavia-http-query-param-exact` and enroll the regional Amphora provider for `http_query_param_exact_v1`. The original case also uses the exact-header and fixed-response options. Names and values are case-sensitive. Multiple parameters in one match must all agree. Separate matches in a rule are alternatives. Requests retain the original query when forwarded. [Gateway API v1.7.0 case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-query-param-matching.go) · [Octavia packages and provider qualification](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-query-param-exact-8305ad881e62)

## Traffic and convergence

The original case sent 13 requests expecting a backend and six expecting HTTP 404. The test used upstream deadlines, the same controller executable from `6f8f6b31de90`, Octavia 19.0.1.dev25 and octavia-lib 3.12.1.dev7. The two fixture versions differ in one SQL read before the Amphora driver renders the load balancer.

| Local fixture | Standard | Experimental |
| --- | --- | --- |
| Original fixture | FAIL, 17/19 | PASS, 19/19 |
| Duplicate SQL read removed | PASS, 19/19 | FAIL, 17/19 |

In each 17/19 run the whale request and the hydra request with the version header kept receiving 404 through the original deadline. The parent reported `Accepted=True` and `ResolvedRefs=True` while programming was pending. All four runs subsequently reported `Programmed=True` and passed eight additional probes, including those two requests. The final state contained 11 exact query rules in Octavia and 11 ACLs in HAProxy. Each run exercised 51 L7 writes. The fixture called the Amphora renderer for every write and reloaded the native HAProxy on the twelve configuration changes. Deployment and convergence on Hibiscus remain open. [Original reports, receipts and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-query-param-gateway-6f8f6b31de90)
