# Stock Hibiscus Amphora HTTP header matching

The stock Octavia 19.0.0 `HEADER/EQUAL_TO` rule matched tokens in incoming HTTP headers on loopback. A mixed-case header name still matched, a mixed-case value fell back, and two predicates on the same policy both had to match. The public API rejected a 256-character header name and a 256-character value, which are valid lengths in the Gateway API v1.7.0 standard CRD. [Octavia rule schema](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7rule.py#L58-L96) · [Gateway API CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_httproutes.yaml#L2860-L2912)

## Contract and result

Gateway API defaults `HTTPHeaderMatch` to Core `Exact`. It requires case-insensitive names, considers only the first case-equivalent name in a configured match, and combines distinct header predicates with AND. Each `matches` entry is an alternative. At equal path and method specificity, the match with more header predicates takes precedence. The tagged `HTTPRouteHeaderMatching` fixture exercises that overlap with `version: two` and `version: two` plus `color: orange`. It also covers distinct alternatives and 404s for unmatched requests. [Header matching](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L584-L666) · [Conjunction](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L785-L801) · [Alternatives and precedence](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L162-L214) · [Tagged test](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-header-matching.go) · [Tagged manifest](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-header-matching.yaml)

The probe programmed `REDIRECT_TO_POOL` policies with public `HEADER/EQUAL_TO` rules. The two-header policy to A was placed before the overlapping `version: two` policy to B. Another policy sent `version: one` to A. The listener default pool was C. A response from B identifies the generic policy, while a response from C identifies the fallback. The probe read back the rule fields and policy positions and checked the order of the rendered forwarding directives. Amphora emitted `req.hdr(name) -m str` with case-sensitive values and jointly evaluated the policy ACLs. HAProxy evaluates forwarding directives in order and uses the first match. [Octavia policy positions](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/db/models.py#L591-L599) · [Stock ACL and policy template](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L85-L166) · [HAProxy forwarding](https://github.com/haproxy/haproxy/blob/ade9c126248df2b27abc6fbf13ca1db3f46d644a/doc/configuration.txt#L14313-L14346)

| Request headers on `GET /` | Observed response |
|---|---|
| `Version: one` | 200 from A |
| No matching header | 200 from fallback C |
| `vErSiOn: one` | 200 from A |
| `Version: One` | 200 from fallback C |
| `Version: two` alone or with `Color: blue` | 200 from B |
| `Version: two` and `Color: orange` | 200 from A |
| `Color: orange` alone | 200 from fallback C |
| Two `Version` fields, first `one` and then `other` | 200 from A |
| One `Version: one, other` field | 200 from A |
| One 255-character name with its configured 255-character value | 200 from A |

The last two `Version` examples reflect HAProxy `req.hdr` iterating header occurrences and comma-separated values in an ACL. Representation of repeated request fields is implementation-specific in Gateway API. The first-entry rule applies to the configured match list. A comma-separated request shows why this fetch cannot serve as an opaque whole-field comparison. [HAProxy header fetch](https://github.com/haproxy/haproxy/blob/ade9c126248df2b27abc6fbf13ca1db3f46d644a/doc/configuration.txt#L24937-L24975) · [Gateway API header name semantics](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L638-L653)

The v1.7.0 standard CRD permits a 256-character name and a 4,096-character value. Octavia caps both fields at 255. Its public API returned HTTP 400 for the next name and value lengths with `HEADER/EQUAL_TO`, and also for the unquoted literal `blue sky` due to the value validator. These are limits on direct `EQUAL_TO` lowering. Every stock `HEADER` comparison shares the key limit, including `REGEX`. [Gateway API CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_httproutes.yaml#L2869-L2912) · [Octavia rule schema](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7rule.py#L58-L96) · [Octavia validator](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/validate.py#L110-L147) · [Octavia character rules](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/constants.py#L272-L287)

Even a one-word configured value can match a different incoming field. With `Exact: one`, the stock rule selected A for the single field `Version: one, other`. Limiting configured literals to short ASCII tokens leaves that behavior in place. A controller needs a provider operation that compares the entire incoming field before it can advertise that contract. It must keep the existing rejection for a match it cannot express. Full `HTTPRouteHeaderMatching` also needs multiple rules, alternative matches, precedence and the required response on a miss. This probe covers the public API and loopback dataplane. [Observed API and HTTP responses](stock-http-header-results.txt)

## Reproduce

Use Linux with unprivileged user namespaces and Python 3.14. The tested versions were Octavia 19.0.0 at `93689cb7701da65b5aad387bb824c477c64b0f2b`, octavia-lib 3.12.0 at `bc03e1856b60517e1b75db4fdf9b3edf55f3a019`, and HAProxy `3.0.19-ade9c1262`. Build HAProxy and set `HAPROXY_BIN` as in the [stock gRPC probe](stock-grpc.md#reproduce). [Hibiscus Octavia release](https://releases.openstack.org/hibiscus/index.html#octavia) · [Hibiscus octavia-lib release](https://releases.openstack.org/hibiscus/index.html#octavia-lib) · [HAProxy source SHA-256](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz.sha256)

```sh
curl -fsSLo /tmp/octavia-http-header-constraints.txt \
  https://raw.githubusercontent.com/openstack/requirements/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt
python3.14 -m venv /tmp/octavia-http-header-venv
/tmp/octavia-http-header-venv/bin/python -m pip install -c /tmp/octavia-http-header-constraints.txt \
  'octavia==19.0.0' 'octavia-lib==3.12.0' pyroute2 fixtures oslotest WebTest
unshare --user --map-user=65534 --map-group=65534 \
  /tmp/octavia-http-header-venv/bin/python -Bu \
  docs/qualification/v1.7.0/provider/stock-http-headers.py --haproxy "$HAPROXY_BIN"
```

The upstream v2 WSGI/SQLite fixture runs the public API with the real Amphora provider driver. Neutron, messaging and provisioning status transitions are simulated. The real Amphora worker reads the persisted graph and passes its stock configuration to a simulated agent. HAProxy validates and runs the byte-for-byte upload for this loopback test. The listener and echo servers use ephemeral loopback ports. The successful run printed the eleven HTTP 200 results above, three HTTP 400 API rejections, `Ran 1 test` and `OK`. [Amphora worker driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py#L118-L194)
