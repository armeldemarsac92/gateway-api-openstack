# Standard HTTP Gateway qualification on controller 597d921f and Octavia 300b2638

On November 26, 2026, two local runs used Gateway API v1.7.0, controller `597d921fd083bb71f9ad3fc024f5f6bf0891e0ba`, and Octavia `300b2638c072c10f9f1e59403eedb792b9cd98a5` (19.0.1.dev109). Staging verified both Git source signatures and the artifact hashes. The controller OCI archive was verified by hash only. The upstream suite executable was built from `e26d183dbbc1bb782b78addbd26faba03955c152` and ran without modifications to the cases or their deadlines. [Input and evidence digests](receipt.json)

| Standard GATEWAY-HTTP selection | Passed | Failed | Skipped in subset | Suite / fixture exit | Upstream report |
| --- | ---: | ---: | ---: | --- | --- |
| Core | 40 | 0 | 0 | 0 / 0 | [Core YAML](gateway-http-standard-core-default.yaml) |
| Extended | 50 | 13 | 0 | 1 / 1 | [Extended YAML](gateway-http-standard-extended-default.yaml) |

Both runs used the default listener configuration for the selection. The Extended run explicitly excluded all 40 Core cases, which appear in the Core section of its report as skips. The Standard channel excluded the case `HTTPRouteInvalidParentRefNotMatchingListenerPort`, which requires `HTTPRouteDestinationPortMatching`. Four other Extended profile features have no registered test: `ClusterTrustBundle`, `GatewayAddressRoutability`, `GatewayAddressRoutabilityCluster`, and `GatewayListenerFilters`. The reports cover Standard GATEWAY-HTTP on this pair. [Profile inventory](../../matrix/README.md)

## Extended observations

- `HTTPRouteRedirectPortAndScheme`, `ListenerSetHTTPRouting`, `GatewayHTTPListenerIsolation`, and `ListenerSetAllowedRoutesNamespaces` passed their request assertions. In each case an HTTPRoute remained present past the suite's 30-second deletion deadline.
- Two `HTTPRouteCORS` preflights, for `/cors-1` and `/cors-wildcard-origin`, timed out at 04:04:19.023 UTC. The first recorded HAProxy reload containing both paths was at 04:04:19.873 UTC, 850 ms later. A subsequent OPTIONS request to `/cors-wildcard-origin` passed at 04:04:19.946 UTC.
- `GatewayStaticAddresses` passed its upstream status and address assertions. The fixture recorded allocation of the usable IP. The additional fixture signal for an allocation attempt on the unusable IP was absent.

The Extended YAML lists all 13 official failures. The trace receipt identifies the log lines and the source hashes for these observations. The records establish the observed timing, with the causes of the cleanup and publication delays undetermined. [Extended YAML](gateway-http-standard-extended-default.yaml) · [Trace receipt](receipt.json)

## Environment and scope

Each run used a disposable kind cluster in a VM, echo Pods, and the controller executable extracted from its OCI archive. It exercised Octavia's installed WSGI API with SQLite, its Amphora driver and renderer, native patched HAProxy 3.0.19, and the stock Barbican WSGI API served locally over HTTPS for certificate enrollment and readback. The fixture supplied Keystone identity and Barbican authentication context, Neutron discovery and VIP allocation, worker dispatch and status completion, and the Amphora agent transport. It invoked the installed conditional policy removal endpoint synchronously. HAProxy served traffic from the rendered configuration to the Kubernetes backends.

The historical 170/170 local cases with a PASS on each channel accumulate evidence across multiple revisions and configurations. The deployed Hibiscus regional ledger remains at 0/14 completed combinations. [Cumulative matrix](../../matrix/README.md#evidence-boundary) · [Regional ledger](../../live-runs.md)
