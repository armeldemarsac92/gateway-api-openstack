# Exact HTTP query parameter matches

An HTTPRoute can select a backend using the name and value of a query parameter. Enable `--octavia-http-query-param-exact` and enroll the regional Amphora provider for `http_query_param_exact_v1`. The original case also uses the exact-header and fixed-response options. Names and values are case-sensitive. Multiple parameters in one match must all agree. Separate matches in a rule are alternatives. Requests retain the original query when forwarded. [Gateway API v1.7.0 case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-query-param-matching.go) · [Octavia packages and provider qualification](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-query-param-exact-8305ad881e62)

## Traffic and convergence

The original case sent 13 requests expecting a backend and six expecting HTTP 404. The test used upstream deadlines, the same controller executable from `6f8f6b31de90`, Octavia 19.0.1.dev25 and octavia-lib 3.12.1.dev7. The two fixture versions differ in one SQL read before the Amphora driver renders the load balancer.

| Local fixture | Standard | Experimental |
| --- | --- | --- |
| Original fixture | FAIL, 17/19 | PASS, 19/19 |
| Duplicate SQL read removed | PASS, 19/19 | FAIL, 17/19 |

In each 17/19 run the whale request and the hydra request with the version header kept receiving 404 through the original deadline. The parent reported `Accepted=True` and `ResolvedRefs=True` while programming was pending. All four runs subsequently reported `Programmed=True` and passed eight additional probes, including those two requests. The final state contained 11 exact query rules in Octavia and 11 ACLs in HAProxy. Each run exercised 51 L7 writes. The fixture called the Amphora renderer for every write and reloaded the native HAProxy on the twelve configuration changes. Deployment and convergence on Hibiscus remain open. [Original reports, receipts and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-query-param-gateway-6f8f6b31de90)

## Kind with libc-regex HAProxy

On controller `6ef34c071eea` with Octavia 19.0.1.dev25, the original query case passed six of 19 checks on both channels. All six expected HTTP 404 responses passed. Each of the thirteen requests that should have reached a backend failed. The Amphora query expressions used PCRE syntax that the kind fixture's libc-regex HAProxy could not compile. The envtest results above used a PCRE2 build. The two fixtures also ran different controller revisions. [Per-channel measurements](shared-http-hosts-results.json) · [Original kind reports](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-shared-hosts-6ef34c071eea)

In the same kind runs, the original HTTP hostname intersection case passed 34 of 37 subtests. Those counts include the upstream groups. Two positive backend requests failed on each channel. The HTTPRoute and GRPCRoute listener hostname cases both stopped during attachment checks before sending traffic. [Registered cases and receipts](shared-http-hosts-results.json)
