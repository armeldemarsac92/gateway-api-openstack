# CORS policies on HTTPRoutes

An operator can enable CORS for matching HTTPRoute rules on an Amphora listener. The route author specifies allowed origins, methods and headers. Amphora responds to the preflight and writes the CORS response headers for that rule. The regional provider must advertise `request_cors_v1`, and the controller must run with `--octavia-request-cors`. [Configuration, filter combinations and withdrawal](../../../operator-octavia-http-actions.md#enable-rule-level-cors) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-cors-c9f6c92f6143)

## Registered case

Both fresh kind clusters ran the original v1.7.0 `HTTPRouteCORS` case and Kubernetes manifests. Each used the executable extracted from the archived linux/amd64 image of controller `092e5207eb24b895014553c12a29e2f8281ed87a`. [Upstream assertions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-cors.go) · [Download the original reports, observed responses and replay](<https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-092e5207eb24b895014553c12a29e2f8281ed87a/hibiscus-httproute-cors-092e5207eb24.tar.gz>)

| Channel | Passed assertions | HTTP 204 | HTTP 403 | HTTP 200 |
| --- | ---: | ---: | ---: | ---: |
| Standard | 17 / 17 | 9 | 1 | 7 |
| Experimental | 17 / 17 | 9 | 1 | 7 |

The counts use the final response for each upstream assertion. Both ready echo Pods served application requests in each cluster. The case sent no credential-bearing requests. On both channels the native Octavia L7 worker processed five policy creates, five rule creates and five policy updates. HAProxy's active configuration contained all five CORS policies. The region advertised CORS after opt-in. `GatewayClass.status.supportedFeatures` did not list `HTTPRouteCORS` in either run. [Controller image and qualification](<https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-092e5207eb24b895014553c12a29e2f8281ed87a>) · [Installed provider and wire tests](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-cors-c9f6c92f6143)

The local fixture used the Octavia v2 API with SQLite, Amphora rendering and HAProxy. Keystone, Neutron and the Amphora transport were simulated. The deployed Hibiscus profile runs remain open. [Live qualification](../live-runs.md)
