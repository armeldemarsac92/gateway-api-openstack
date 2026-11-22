# Hostname precedence, invalid TLS references and redirect ports

These three original Gateway API v1.7.0 cases ran against the controller from image `3c8dd9883713`, using a fresh Kind workload cluster for each channel. The local setup used actual echo Pods, Octavia and Barbican APIs, and Amphora-rendered HAProxy. It supplied the network, identity and cloud-worker fixtures. [Controller image](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-3c8dd9883713c57ac152ebfb12210ec9eecbe6ca) · [Case receipts](ingress-hostnames-and-tls-results.json) · [Replay and original reports](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-3c8dd9883713c57ac152ebfb12210ec9eecbe6ca/hibiscus-ingress-case-supplement.tar.gz)

| Registered case | Standard | Experimental |
|---|---|---|
| `GatewayInvalidTLSConfiguration` | PASS, 4/4 | PASS, 4/4 |
| `HTTPRouteMultipleRoutesAttachmentSameHostnameIntersection` | PASS, 6/6 | PASS, 6/6 |
| `HTTPRouteRedirectPortAndScheme` | FAIL during Gateway readiness | FAIL during Gateway readiness |

For a missing Secret, an unsupported group or kind, and a malformed Secret, the original test found `ResolvedRefs=False/InvalidCertificateRef` on the HTTPS listener. In the route precedence case, a newer exact hostname won over an older wildcard, and a narrower wildcard won over a broader one. Requests outside the more specific route's path reached the older route. [Upstream certificate assertions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-invalid-tls-configuration.go) · [Upstream hostname assertions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-multiple-routes-attachment-same-hostname-intersection.go)

For redirect ports and schemes, the HTTP Gateways on ports 80 and 8080 both reached `Programmed=True`. The HTTPS Gateway still reported `Programmed=False/NoResources` when the upstream readiness deadline expired. The suite reached zero of its fifteen traffic subtests on each channel. The redirect responses for this configuration remain unverified. [Upstream redirect case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-redirect-port-and-scheme.go)
