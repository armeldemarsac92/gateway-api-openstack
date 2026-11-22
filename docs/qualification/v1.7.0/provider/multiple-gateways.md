# Multiple Gateway parents and shared HTTP listener hostnames

An HTTPRoute or GRPCRoute can select two managed Gateways. Each Gateway has its own provider binding, and the route reports a status entry for each parent. The two original multiple-Gateway cases verified that the shared route reached the same backend through both Gateways and that each Gateway's dedicated route reached its own backend. Several compatible listener sections on one Gateway can also share a physical port. [HTTPRoute case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-multiple-gateways.go) · [GRPCRoute case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/grpcroute-multiple-gateways.go)

The local clusters ran the original v1.7.0 cases with upstream echo Pods, Octavia REST, the Amphora configuration renderer and HAProxy 3.0.19. The fixture supplied OpenStack identity, networking and worker completion. The second build includes the fixes for namespace consent during cleanup, ListenerSet binding migration and mixed gRPC parent references. [Builds, provider wheels, flags, cluster images and individual reports](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-multi-gateway-8dd15941f0ba)

| Upstream case | Standard, first build | Experimental, first build | Standard, reviewed build | Experimental, reviewed build |
| --- | ---: | ---: | ---: | ---: |
| HTTPRouteMultipleGateways | 6/6 | 6/6 | 6/6 | 6/6 |
| GRPCRouteMultipleGateways | 6/6 | 6/6 | 6/6 | 6/6 |
| HTTPRouteListenerHostnameMatching | 8/8 | 8/8 | 8/8 | 8/8 |
| GRPCRouteListenerHostnameMatching | 8/8 | 7/8 | 8/8 | 7/8 |
| HTTPRouteQueryParamMatching | 13/19 | 15/19 | Unrun | Unrun |
| HTTPRouteHostnameIntersection | 34/37 | 34/37 | Unrun | Unrun |

The first build is `2ef09f0c9149` with Octavia 19.0.1.dev27. The reviewed build is `8dd15941f0ba` with Octavia 19.0.1.dev28. On the reviewed Experimental build, the RPC for `bar.com` expired at the 30-second deadline. On the first build, two positive HTTP hostname-intersection requests per channel also expired. The upstream summary includes their failing parent subtest in its count of 37. The query case used a HAProxy build linked to libc for regular expressions. Separate query runs used a different fixture and recorded both 19/19 and 17/19 per channel. [All four earlier query runs](http-query-parameters.md)

Each cell counts subtests in a single registered case. The run receipts and YAML reports record each complete case outcome against its source commit and bundle. Runs on a deployed Hibiscus cluster remain pending. [Live profile ledger](../live-runs.md)
