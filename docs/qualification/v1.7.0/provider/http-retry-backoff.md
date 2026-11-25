# Set an interval between HTTPRoute retry attempts

A forwarding rule can set `retry.backoff` when its region advertises `http_request_policy_v1` and `http_retry_backoff_v1`. The first attempt starts immediately. Each eligible retry waits the fixed interval configured on the rule. An explicit zero starts the next eligible attempt without an added wait. Removing the field restores the provider's retry timing. [Operator setup](../../../operator-octavia-http-actions.md#enable-http-request-deadlines-and-retries)

The same route went through five generations in each cluster. A backend Pod counted the calls for each request and timed the interval between their arrivals. A second route on the listener returned 200 with one call to the same Pod in every phase. [Traffic and route conditions](http-retry-backoff-evidence/receipt.json)

| Channel | Route update | Backoff | HTTP result | Calls at Pod | Intervals between calls (ms) |
|---|---|---:|---:|---:|---|
| Standard | Two retryable responses | 220 ms | 200 | 3 | 223.7, 223.4 |
| Standard | Request deadline during the wait | 650 ms | 504 | 1 | — |
| Standard | First backend attempt timed out | 220 ms | 200 | 2 | 349.0 |
| Standard | Explicit zero | 0 ms | 200 | 2 | 0.8 |
| Standard | Restore the provider default | omitted | 200 | 2 | 0.9 |
| Experimental | Two retryable responses | 220 ms | 200 | 3 | 223.3, 223.2 |
| Experimental | Request deadline during the wait | 650 ms | 504 | 1 | — |
| Experimental | First backend attempt timed out | 220 ms | 200 | 2 | 348.7 |
| Experimental | Explicit zero | 0 ms | 200 | 2 | 1.0 |
| Experimental | Restore the provider default | omitted | 200 | 2 | 0.8 |

With a 180 ms total deadline and 650 ms between attempts, the client received 504 after 186.3 ms on Standard and 181.5 ms on Experimental. The backend saw one call. With a 125 ms deadline on the first attempt and a 220 ms retry interval, the second attempt completed the request. Each measurement followed current route conditions, a matching public L7 policy and the loaded Amphora configuration. [Traffic and route conditions](http-retry-backoff-evidence/receipt.json)

A separate loopback test limited the origin to one simultaneous connection. While one request waited 600 ms for a retry, a second request to that origin received 200. The first request then completed on its second attempt. The earlier HAProxy candidate returned 503 to the second request. [Loopback regression](https://github.com/armeldemarsac92/octavia/blob/bf5ab480c018045ac226954724b59260eac694ca/elements/haproxy-octavia/test-backend-retry-budget.py)

The previous controller rejected the explicit interval with `Accepted=False/UnsupportedValue`. The client received HTTP 503 and the Pod received zero calls for that route. The local runs used the published Gateway API v1.7.0 bundles, Kubernetes, the controller from the signed OCI image, installed Octavia API and worker code, the Amphora renderer, and HAProxy. The fixture supplied cloud identity, network inventory, general provisioning and the Amphora transport. Regional deployment is pending. [Regional run ledger](../live-runs.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-retry-backoff-bf5ab480c018)

HAProxy records a delayed retry after the request has left its queue and passed the deadline and budget checks. In the native regression, one HTTP retry on the same server made two requests to the origin and increased `wretr` by two, following HAProxy's HTTP replay and same-server connection accounting. With redispatch, the original server and backend recorded one `wretr` and one `wredis`. Both counters stayed at zero on the new server. A request whose deadline expired during the wait reached the origin once and left both counters at zero. [Loopback regression](https://github.com/armeldemarsac92/octavia/blob/bf5ab480c018045ac226954724b59260eac694ca/elements/haproxy-octavia/test-backend-retry-budget.py)

The installed stable and master packages also recovered an existing backoff policy from `ERROR` to `ACTIVE/ONLINE` while enabling two sibling policies. Replaying the same group activation left both siblings enabled and active, with one Amphora reload in total. [Provider review and observations](https://github.com/armeldemarsac92/octavia/pull/38)
