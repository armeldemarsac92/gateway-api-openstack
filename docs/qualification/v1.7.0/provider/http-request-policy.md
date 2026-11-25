# HTTPRoute deadlines and retries

An operator can enable `--octavia-http-request-policy` on the controller when the effective Amphora region advertises `http_request_policy_v1`. A route can set a total request deadline, a deadline for each backend attempt, a maximum retry count and selected HTTP response codes. A timeout of zero disables that deadline. A retry count of zero disables retries. The overall deadline limits the total time spent on attempts and queuing. The controller requires an active public policy readback before reporting programming complete. [Operator configuration](../../../operator-octavia-http-actions.md)

Six original Gateway API v1.7.0 cases passed on each release channel. Both campaigns ran the signed controller manager, native Octavia REST and L7 worker tasks, the Amphora renderer and HAProxy, and unmodified echo Pods in Kind. Keystone, Neutron allocation, and the physical Amphora and worker transports used local fixtures. [Per-case reports, route policy readbacks and Pod checks](http-request-policy-evidence/receipt.json)

| Registered case | Standard | Experimental |
|---|---|---|
| `HTTPRouteRetry` | [PASS](http-request-policy-evidence/standard-HTTPRouteRetry.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteRetry.yaml) |
| `HTTPRouteRetryWithBackendTimeout` | [PASS](http-request-policy-evidence/standard-HTTPRouteRetryWithBackendTimeout.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteRetryWithBackendTimeout.yaml) |
| `HTTPRouteRetryWithRequestTimeout` | [PASS](http-request-policy-evidence/standard-HTTPRouteRetryWithRequestTimeout.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteRetryWithRequestTimeout.yaml) |
| `HTTPRouteTimeoutBackendRequest` | [PASS](http-request-policy-evidence/standard-HTTPRouteTimeoutBackendRequest.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteTimeoutBackendRequest.yaml) |
| `HTTPRouteTimeoutRequest` | [PASS](http-request-policy-evidence/standard-HTTPRouteTimeoutRequest.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteTimeoutRequest.yaml) |
| `HTTPRouteRetryConnectionError` | [PASS](http-request-policy-evidence/standard-HTTPRouteRetryConnectionError.yaml) | [PASS](http-request-policy-evidence/experimental-HTTPRouteRetryConnectionError.yaml) |

The connection-error case used two Pods behind one Service. One refused the connection on its declared Service port and accepted a control connection on its actual serving port. The other accepted the Service connection. The public Octavia policy was active and HAProxy recorded redistribution to the working member. Counts for each member and channel are in the receipt.

The two retry-with-timeout cases share an HTTPRoute manifest. The first case uses upstream cleanup. Both runs recorded its creation and deletion, followed by a fresh creation in the second case. In an earlier Experimental campaign, retaining the first route caused the upstream replacement to clear its controller binding. Three subsequent cases stopped during Gateway readiness before sending traffic. The recorded passes use the same controller, wheels and HAProxy binary with the upstream resource lifecycle restored.

One negative subtest in `HTTPRouteRetryWithBackendTimeout` addresses the upstream request-timeout route. An additional test on the selected HAProxy backend covered expiration of an individual attempt with no HTTP response status selected for retry. The second attempt returned 200. A separate HTTP/2 backend refused a stream before processing. When one retry was enabled it processed one of the two offered streams and returned 200. When the retry limit was zero the gateway returned 502 after the first refusal.

After sending starts, HAProxy can replay a request if the complete request is in its replay buffer. An explicit retry backoff remains unsupported. Real Hibiscus deployment and regional qualification are pending. [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-request-policy-78c80bfe3019) · [Driver library packages](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-http-request-policy-e436d312428e)
