# Removing HTTPRoute redirect policies after listener closure

Controller `0e729b171594b058f09783aede6b7221eb62e85d` and Octavia 19 `460570fb9060fafcedba2907f7a24dc6ab83e7e8` ran the upstream Gateway API v1.7.0 case `HTTPRouteRedirectPortAndScheme` on local Standard and Experimental clusters. The controller requested revision-checked removal after the Amphora worker observed a closed HTTPS listener. [Provider release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-l7policy-closed-listener-removal-460570fb9060) · [Controller and provider evidence](receipt.json)

| CRD channel | Closed-listener removal | Official case | Other Extended cases skipped | Guarded removals | Time until the next route deletion began | Original upstream report |
| --- | --- | --- | ---: | ---: | ---: | --- |
| Standard | On | PASS | 62 | 24 | 18.412 s | [YAML](standard-on.yaml) |
| Experimental | On | PASS | 63 | 24 | 18.610 s | [YAML](experimental-on.yaml) |
| Standard | Off | FAIL | 62 | 0 | 30.000 s | [YAML](standard-off.yaml) |

All 15 redirect requests passed in each of the three runs across ports 80, 8080 and 443. With the option enabled, route deletion completed inside the original deadline on both channels. On Standard with the option off, the upstream test still found the HTTPS route after 30 seconds and failed the case. The request counts come from the individual success messages in the log. Each interval in the table runs from the upstream deletion log for the 443 route to the next deletion log for the 8080 route. [Request checks and cleanup message](redirect-requests.json)

- Standard with the option on yielded no usable deletion event from the supplemental watch. The exact resource disappearance time is unknown.
- Experimental with the option on recorded one route UID and a Kubernetes NotFound after 18.594 s from the first observed terminating event.
- Standard with the option off yielded no usable deletion event from the supplemental watch. The exact resource disappearance time is unknown.

A separate native worker run exercised reopening the listener. A surviving policy returned its updated HTTP 302 Location after one HAProxy reload. Two retired policies returned 503 on the reopened listener. When the publication marker differed, the API reported the closure as unconfirmed even though the TCP socket was closed. [Worker observations](native-sibling-update.json)

The fixture used kind, the installed Octavia WSGI API with SQLite, the Amphora worker and renderer, and a local HAProxy process. It supplied Keystone identity, Neutron discovery and VIP allocation, worker delivery and Amphora transport. A single local Amphora peer provided the closure observation. Regional active-active operation and database row locking still require a deployed environment. [Regional qualification](../../live-runs.md)

## Removing a route on a shared HTTP listener

On the same source pair, the isolated Standard case `ListenerSetAllowedRoutesNamespaces` passed all 11 request checks. Two routes shared the same active HTTP listener on port 80. Octavia removed the target policy and its rules without requiring listener closure. Three HTTP probes during deletion reached the surviving route’s expected backend. Kubernetes confirmed the target was absent 11.819 seconds after the upstream deletion log, within the case’s 30-second deadline. The full HTTP Extended run on the earlier pair exceeded that deadline. [Original upstream report](shared-listener-standard.yaml) · [Route ownership, policy removal and sibling traffic](shared-listener.json)
