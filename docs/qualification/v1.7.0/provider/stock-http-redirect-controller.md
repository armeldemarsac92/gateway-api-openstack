# HTTPRoute redirects to a fixed host

A tenant can attach an HTTPRoute with a fixed destination hostname to an HTTP listener. The controller programs a pool-free Octavia `REDIRECT_PREFIX` policy. The public policy carries the scheme, host and optional port. HAProxy appends the original path and query. [Tenant example](../../../getting-started.md#redirect-an-httproute-to-a-fixed-host) · [Provider protocol and limits](stock-http-redirect.md)

On the local Standard and Experimental bundles, the registered Gateway API v1.7.0 case `HTTPRouteRedirectHostAndStatus` passed both of its traffic assertions. Requests to `/hostname-redirect` returned 302 and requests to `/host-and-status` returned 301, both with destination `example.org`. Supplemental requests preserved the original percent encodings, repeated query keys and query order. The previous published controller rejected the Route before either upstream HTTP subtest began. An intermediate source revision, `0665660e81b6`, reached both traffic assertions on Standard and failed them. Octavia rejected its 343-character policy description with HTTP 400. The release writes a short role label and retains the digest of the full identity. [Machine-readable results](stock-http-redirect-controller-results.json)

During the native retries, stock HAProxy returned HTTP 503 before its redirect policies became active. Immediately after the native case, the controller-specific programmed condition was still pending on both channels. A separate run of the original manifest on each channel observed current-generation Accepted, ResolvedRefs and Programmed all true and both correct wire responses.

An independent HTTPRoute lifecycle shared a listener on port 18080 with a more specific exact forwarding rule. For `API.TENANT.EXAMPLE:18080/legacy/stay?x=1` the application answered 200. The same path for another host received the redirect. With only the destination hostname specified, Location included the listener port. Changing the scheme to HTTPS and the status to 301 produced a Location with the standard HTTPS authority. Removing the forwarding Route made its path eligible for that redirect again. Three additional fixed-host rules returned 303, 307 and 308 with the expected encoded query on both channels. The corresponding registered Extended tests omit the hostname and require the incoming authority to be preserved.

| Local observation | Standard | Experimental |
| --- | ---: | ---: |
| Continuous requests during insertion of an unrelated rule | 82 | 76 |
| Unexpected responses during insertion | 0 | 0 |
| Resources matched by the read-only audit | 15 | 15 |
| Existing policies read by the replacement manager | 3 | 3 |
| Octavia writes by the replacement manager | 0 | 0 |
| Managed resources after Gateway deletion | 0 | 0 |

The controller scopes each redirect to requests left after higher-priority forwarding matches on the same port. It uses positive path and host predicates and inverted stock Octavia rules. For a changed or unverified predicate it can withdraw an active redirect pending a fresh read. The controller reports an unprogrammable match if its encoded rules exceed the Octavia policy or field limit. The destination authority must fit Octavia’s 255-character prefix. This slice covers a fixed destination and an unchanged path. Dynamic hosts, path replacement and the default unmatched 404 remain separate requirements. The unmatched control returned 503 on this stock fixture.

The Kubernetes 1.36.2 API server used etcd 3.6.8. Each channel used its own pinned release bundle, Octavia 19.0.0, octavia-lib 3.12.0, and HAProxy 3.0.19. The fixture supplied Keystone identity, Neutron responses, job delivery and the remote Amphora transport. The traffic came through an HTTP listener. The compiler also has a focused test for scheme and port selection on an operator-managed HTTPS listener. Deployed Hibiscus qualification is tracked per profile and channel. [Live-run ledger](../live-runs.md)

Source `c75264dbe44932b430add1c1fd2ffd4fb07ea0e5` produced linux/amd64 manifest `sha256:8f3351cc24d716dd35efb6e67d7fa156530af59d3d65b10ac19017eb2dbaeeda`. An independent uncached build produced the same manifest bytes. The upstream case, baseline, traffic receipts, read-only audit and replay commands accompany the prerelease. [Qualification archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-c75264dbe44932b430add1c1fd2ffd4fb07ea0e5/hibiscus-stock-redirects-c75264dbe449.tar.gz) · [Development prerelease](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-c75264dbe44932b430add1c1fd2ffd4fb07ea0e5)
