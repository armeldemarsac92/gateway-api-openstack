# Limit an XBackend affinity cookie to a request path

Three Experimental HTTPRoutes on one listener pointed to the same EndpointSelector XBackend and two ready Pods. The test client used an HTTP cookie jar. It requested `/reports`, then returned to that path, visited `/reports/daily` and visited `/reporting`. The matching requests reached the Pod that issued the cookie. Each issuance contained one Path attribute. [Observed requests, conditions and pools](xbackend-cookie-path-evidence/receipt.json)

| Persistence | Cookie Path | Exact path | Descendant | Neighbor | Max-Age |
|---|---|---|---|---|---:|
| Session at the default path | `/` | Sent | Sent | Sent | Absent |
| Session | `/reports` | Sent | Sent | Omitted | Absent |
| Session with a five-minute timeout | `/reports` | Sent | Sent | Omitted | Absent |
| Permanent with a five-minute timeout | `/reports` | Sent | Sent | Omitted | 300 |
| Session after restoring the default | `/` | Sent | Sent | Sent | Absent |

A second client kept its original cookie jar when the operator narrowed the path from `/` to `/reports`. It sent its old root cookie on 8 of 8 requests to `/reporting`. Both ready Pods answered those requests (pod-a received 4, pod-b received 4). The scoped cookie then kept the client on pod-a for `/reports/daily` and `/reports`. [Transition measurements](xbackend-cookie-path-evidence/receipt.json)

On Standard, the XBackend resource was absent and an ordinary Service returned three successful responses without an affinity cookie. Both local channels used kind, the signed controller image, Octavia’s public pool API and update worker, the Amphora renderer and HAProxy. The harness supplied cloud identity, network inventory, general provisioning and agent transport. Each traffic sample began after the route and its pool converged. [Regional test ledger](../live-runs.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-cookie-path-3d73d50eb78d)
