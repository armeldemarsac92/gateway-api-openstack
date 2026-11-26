# HTTPRoute request mirroring

In an Amphora region enrolled in `http_request_mirror_v1`, the controller component `octavia-http-request-mirror` lets an HTTPRoute copy a request to one or more backends. Omitting the sampling ratio sends a copy for each eligible request. Each mirror may instead select a percentage or fraction. Mirror delivery runs asynchronously. The application receives the primary backend's response.

Operators install the regional Octavia extension, driver library, database migration and matching Amphora build before enabling this capability. The signed controller revision is `37ca410a34a179dfbbb561f8f7b5744825f4787a` and its linux/amd64 OCI digest is `sha256:74d5e005b2420c1e75f52763077d083f2b197988013230afa66fa0b6112e9e84`. [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-request-mirror-bafaa0851bf9) · [Driver library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-http-request-mirror-f6a1b8cab341)

## Local results

The pinned Gateway API v1.7.0 suite exercised real Service backend Pods, a Kubernetes API server, Octavia REST and its L7 worker, and the HAProxy data plane. Each scenario also read back the active public policy and matched the destination pool members against the Kubernetes EndpointSlices. The fixture supplied OpenStack provisioning, identity and Amphora agent transport. A deployment on Hibiscus remains pending.

An earlier run resolved the Service references and left the route pending when Octavia reported the mirror pool's listener. The controller now accounts for mirrors at both rule and weighted-backend scope when the sampling numerator is positive. Removing a mirror also removes its contribution to that listener relationship. [Controller change](https://github.com/armeldemarsac92/gateway-api-openstack/commit/37ca410a34a179dfbbb561f8f7b5744825f4787a)

| Upstream case | Standard | Experimental |
|---|---|---|
| `HTTPRouteRequestMirror` | [2 checks passed](http-request-mirror-evidence/standard-HTTPRouteRequestMirror.yaml) | [2 checks passed](http-request-mirror-evidence/experimental-HTTPRouteRequestMirror.yaml) |
| `HTTPRouteRequestMultipleMirrors` | [2 checks passed](http-request-mirror-evidence/standard-HTTPRouteRequestMultipleMirrors.yaml) | [2 checks passed](http-request-mirror-evidence/experimental-HTTPRouteRequestMultipleMirrors.yaml) |
| `HTTPRouteRequestPercentageMirror` | [3 checks passed](http-request-mirror-evidence/standard-HTTPRouteRequestPercentageMirror.yaml) | [3 checks passed](http-request-mirror-evidence/experimental-HTTPRouteRequestPercentageMirror.yaml) |

The percentage case sends 500 requests per distribution check and reads the destination Pod logs. The other cases exercise full copies, multiple destinations, and request header changes. [Observed counts, active policies and image hashes](http-request-mirror-evidence/receipt.json)

A supplemental Gateway API v1.7.0 Standard run exercised a `BackendTLSPolicy` on the mirror Service's named port. The primary returned HTTP 200 while HAProxy rejected the mirror connection when the policy trusted a different CA. [Backend TLS evidence](http-request-mirror-backend-tls/README.md)
