# HTTPRoute request redirects

An operator can enable `--octavia-http-request-redirect` in a region where the Amphora driver advertises `http_request_redirect_v1`. A tenant can then redirect to the incoming hostname, replace the full path or a matched path prefix, change the scheme or port, and specify a redirect status code. The original query string is preserved. Fixed destinations that only need a stock prefix redirect continue to use `REDIRECT_PREFIX`. [Operator configuration and overlap limits](../../../operator-octavia-http-actions.md#enable-request-derived-redirects) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-redirect-provider-5529fcc375d4) · [Provider library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-redirect-provider-caa5e17ce80b)

## Registered cases

The Linux amd64 manager extracted from image revision `069b0de0ed94` ran the original Gateway API v1.7.0 cases on independent Standard and Experimental kind clusters. Each run used Octavia `19.0.1.dev30`, octavia-lib `3.12.1.dev8`, the Amphora renderer and HAProxy 3.0.19. The cluster supplied Pods, Services and EndpointSlices. OpenStack identity, Neutron and Amphora transport used local fixtures. [Image, original reports and per-channel receipts](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-069b0de0ed941587f44a2664acfedf50b3459aa3)

| Registered case | Standard | Experimental |
| --- | --- | --- |
| `HTTPRoute303Redirect` | PASS 1/1 | PASS 1/1 |
| `HTTPRoute307Redirect` | PASS 1/1 | PASS 1/1 |
| `HTTPRoute308Redirect` | PASS 1/1 | PASS 1/1 |
| `HTTPRouteRedirectPath` | PASS 6/6 | PASS 6/6 |
| `HTTPRouteRedirectPort` | PASS 4/4 | PASS 4/4 |
| `HTTPRouteRedirectScheme` | PASS 4/4 | PASS 4/4 |
| `HTTPRouteRedirectPortAndScheme` | FAIL during setup, 0/15 | FAIL during setup, 0/15 |

In the combined case, the two HTTP Gateways on ports 80 and 8080 reported `Programmed=True`. The Gateway on port 443 reported `Accepted=True` and `Programmed=False/NoResources`. Its four HTTPS sections remained pending. The HTTPS route parent reported `Accepted=True`, `ResolvedRefs=True` and `Programmed=False/Pending`. The upstream setup deadline expired before the traffic assertions began. [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-port-and-scheme.go)

On the earlier controller `0280a6637d72`, a separate diagnostic continued observing the same HTTPS fixture on Standard after the original case failed. At 120 seconds after the deadline Octavia had created 24 redirect policies with 42 attached rules. One of the 24 was enabled. This observation records programming progress on that earlier revision. Neither revision qualified the combined case or a full Hibiscus profile. [Evidence archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-069b0de0ed941587f44a2664acfedf50b3459aa3)
