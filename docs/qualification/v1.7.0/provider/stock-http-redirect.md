# Fixed-destination HTTP redirects on stock Hibiscus

The public Octavia action `REDIRECT_PREFIX` implements a redirect that changes the authority and leaves the path and query intact. For Gateway API v1.7.0 `RequestRedirect` with explicit `scheme: https`, `hostname: moved.example.test`, `port: 8443` and no `path` modifier, set Octavia's `redirect_prefix` to `https://moved.example.test:8443`. End the prefix immediately after the authority. The stock Amphora renderer emits a frontend `redirect ... prefix` rule, and HAProxy appends the request URI path and query. Gateway API and Octavia default to 302. Explicit 301 also survives the API round trip and is returned to the client. [Gateway API filter and defaults][redirect] · [Octavia policy fields][types] · [Octavia default][sanitize] · [Stock Amphora template][template] · [HAProxy 3.0.19 redirect semantics][haproxy]

Create each policy with `POST /v2/lbaas/l7policies`, including `listener_id`, `position`, `action: REDIRECT_PREFIX`, `redirect_prefix` and, for an explicit status, `redirect_http_code`. Create its match with `POST /v2/lbaas/l7policies/{policy-id}/rules`. The probe received 201 for each creation and 200 for policy and rule readbacks. The Exact path uses `PATH / EQUAL_TO`. A non-root Gateway `PathPrefix` such as `/legacy` uses two policies with the same redirect action, one `EQUAL_TO /legacy` and one `STARTS_WITH /legacy/`. With both rules, `/legacy` and its descendants match, while `/legacyish` and `/Legacy/Ab` go to the sibling echo. An optional `HOST_NAME / REGEX` policy predicate checked `MiXeD.source.example.test:9123` on an Exact path and let a different Host fall through. [Gateway API path matches][path] · [Gateway API Host matching][hostname] · [Octavia ACL and policy rendering][template-acl]

| Request target | HTTP | Observed Location or backend |
| --- | --- | --- |
| `/legacy?q=One` | 302 | `https://moved.example.test:8443/legacy?q=One` |
| `/legacy/` | 302 | `https://moved.example.test:8443/legacy/` |
| `/legacy/Ab%2fCd%3F?q=%2f+One&Q=%2B&q=Second` | 302 | `https://moved.example.test:8443/legacy/Ab%2fCd%3F?q=%2f+One&Q=%2B&q=Second` |
| `/permanent?x=%2F` | 301 | `https://moved.example.test:8443/permanent?x=%2F` |
| `/permanent/`, `/legacyish?x=%2F`, `/Legacy/Ab` | 200 | Sibling echo of each original target |
| `/literal?x=one`, using `REDIRECT_TO_URL` with the literal `https://moved.example.test:8443/literal` | 302 | `https://moved.example.test:8443/literal` |

The encoded request used a different incoming Host with an explicit source port. The returned authority was still the configured one and HAProxy retained the path and query byte for byte. `REDIRECT_TO_URL` emits a literal Location, so its query stayed empty even with a known Exact path. [HAProxy request URI reuse and literal mode][haproxy]

## Fields omitted from the Gateway API filter

- With a fixed hostname and an explicit scheme, an omitted port is the scheme's well-known port. The probe sent `https://moved.example.test` as the prefix and got `https://moved.example.test/default-port`. Gateway API also calls for omitting the numeric port at the usual HTTP and HTTPS ports when the port was explicitly set to that default. [Gateway API port rules][redirect]
- With a fixed hostname and both scheme and port omitted, ordinary HTTP requests on the HTTP listener use its HTTP scheme and its actual listener port. On this run the listener was `58175` and the request `/hostname-redirect` produced `http://example.org:58175/hostname-redirect` despite the source Host carrying port 9123. This used a prefix computed by the caller from known listener values. [Gateway API scheme and port rules][redirect]
- With an omitted hostname, Gateway API uses the incoming request hostname. The API returned 400 for `REDIRECT_PREFIX` with `/` or `https://`, and for `REDIRECT_TO_URL` with `/`. Both URL fields require an absolute HTTP(S) URL with a host. The stock renderer supplies literal prefix/location actions and exposes no action that substitutes an arbitrary request hostname. [Octavia URL validator][url] · [Octavia policy fields][types] · [Stock Amphora template][template]

## Deployment boundary

A forwarding rule can win over a redirect under Gateway API match precedence. In the overlapping test, `REDIRECT_TO_POOL` at position 1 matched Exact `/legacy/stay`; `REDIRECT_PREFIX` at position 3 matched the less specific `/legacy/` branch. The API read back those positions and the renderer put `use_backend` before `redirect`. HAProxy warned that the redirect still executes first and returned `302 Location: https://moved.example.test:8443/legacy/stay`. After disabling the prefix redirect and lower-priority catch-all through `PUT /v2/lbaas/l7policies/{policy-id}` and rendering again, the same request returned 200 from the Exact forwarding policy. Position alone is therefore insufficient. The direct mapping is usable for ordinary requests on an HTTP Amphora listener when Gateway API would select the redirect over every simultaneously matching forwarding policy, or when the redirect predicates exclude those higher-precedence requests. [Gateway API precedence][precedence] · [HAProxy frontend processing order][haproxy-order]

The full prefix, including scheme and numeric port when present, must fit the Octavia SQL model's 255-character column. Gateway API alone permits a 253-character hostname. Its path constraints also extend beyond Octavia's 255-character rule value field. This loopback fixture uses SQLite, so the SQL column sizes are source bounds rather than measured database rejection thresholds. [Octavia model][db] · [Octavia rule schema][rule-schema] · [Gateway API precise hostname][precise-host]

The tagged Core `HTTPRouteRedirectHostAndStatus` fixture supplies only a fixed hostname and optionally 301, with a PathPrefix match. Its omitted scheme and port can be computed for the HTTP listener as above. The tagged scheme and port suites also include cases where hostname is omitted, and the path suite needs replacement rather than preservation. This study exercises the provider mapping with synthetic HTTP/1.1 traffic. Full tagged Gateway API conformance, controller graph reconciliation and status, HTTPS listener ingress, deployed Amphora, and any replacement path or other status code require separate evidence. [Core fixture][host-case] · [Scheme case][scheme-case] · [Port case][port-case] · [Port and scheme case][port-scheme-case] · [Path replacement case][path-case]

## Reproduce

Use the `stock-http-redirect.py` probe in the [qualification source archive](stock-tcp.md#reproduce-the-multiple-listener-probes). Set `STOCK_PROVIDER_FIXTURES` to the absolute path of this repository's `docs/qualification/v1.7.0/provider` directory. Use the pinned Octavia 19.0.0, octavia-lib 3.12.0 and HAProxy 3.0.19 tools, Python 3.14, and the existing `stock-http-headers.py` and `stock-tcp.py` from the same provider fixture directory. The stock v2 WSGI/SQLite fixture calls the real Amphora provider. Neutron, worker completion and the agent upload/reload boundary are simulated. The real Amphora worker renders from the persisted model and HAProxy validates and runs the exact bytes sent to the simulated agent. The test constructs no HAProxy configuration. [Octavia Hibiscus release][hibiscus] · [Worker renderer][worker]

```sh
unshare --user --map-user=65534 --map-group=65534 \
  "$OCTAVIA_PYTHON" -Bu stock-http-redirect.py \
  --fixture-dir "$STOCK_PROVIDER_FIXTURES" --haproxy "$HAPROXY_BIN" \
  --evidence stock-http-redirect-results.json
```

The run passed one test with 13 synthetic requests. [JSON evidence](stock-http-redirect-results.json) contains the requests, unchanged Locations and echo bodies, public L7 operations with their actual statuses, and the rendered lines. Resource UUIDs in the evidence are replaced by fixture labels.

[redirect]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L1231-L1322
[precedence]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L192-L214
[path]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L523-L541
[hostname]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L56-L68
[precise-host]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/shared_types.go#L630-L641
[host-case]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-host-and-status.yaml
[scheme-case]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-scheme.go
[port-case]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-port.go
[port-scheme-case]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-port-and-scheme.go
[path-case]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-path.go
[hibiscus]: https://releases.openstack.org/hibiscus/index.html#octavia
[worker]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py#L118-L194
[types]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7policy.py#L78-L119
[sanitize]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/validate.py#L304-L322
[url]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/validate.py#L51-L68
[rule-schema]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7rule.py#L57-L97
[db]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/db/models.py#L803-L827
[template]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L144-L163
[template-acl]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L81-L163
[haproxy]: https://github.com/haproxy/haproxy/blob/ade9c126248df2b27abc6fbf13ca1db3f46d644a/doc/configuration.txt#L11151-L11209
[haproxy-order]: https://github.com/haproxy/haproxy/blob/ade9c126248df2b27abc6fbf13ca1db3f46d644a/src/stream.c#L2028-L2037
