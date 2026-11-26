# HTTP Gateway qualification on controller 597d921f and Octavia 300b2638

On November 26, 2026, three Standard runs used Gateway API v1.7.0, controller `597d921fd083bb71f9ad3fc024f5f6bf0891e0ba`, and Octavia `300b2638c072c10f9f1e59403eedb792b9cd98a5` (19.0.1.dev109). Staging verified both Git source signatures and the artifact hashes. The controller OCI archive was verified by hash only. The upstream suite executable was built from `e26d183dbbc1bb782b78addbd26faba03955c152` and ran without modifications to the cases or their deadlines. [Input and evidence digests](receipt.json)

| Standard GATEWAY-HTTP selection | Passed | Failed | Skipped in subset | Suite / fixture exit | Upstream report |
| --- | ---: | ---: | ---: | --- | --- |
| Core, default parallelism | 40 | 0 | 0 | 0 / 0 | [Core YAML](gateway-http-standard-core-default.yaml) |
| Extended, default parallelism | 50 | 13 | 0 | 1 / 1 | [Extended YAML](gateway-http-standard-extended-default.yaml) |
| Extended, `-test.parallel=1` | 53 | 10 | 0 | 1 / 1 | [Serial Extended YAML](gateway-http-standard-extended-serial.yaml) |

All three runs used the default listener configuration for their selection. The first two ran Core and Extended separately with default Go test parallelism. The third ran Extended with `-test.parallel=1`. Both Extended runs explicitly excluded all 40 Core cases, which appear in the Core section of its report as skips. The Standard channel excluded the case `HTTPRouteInvalidParentRefNotMatchingListenerPort`, which requires `HTTPRouteDestinationPortMatching`. Four other Extended profile features have no registered test: `ClusterTrustBundle`, `GatewayAddressRoutability`, `GatewayAddressRoutabilityCluster`, and `GatewayListenerFilters`. The three reports above cover Standard GATEWAY-HTTP on this pair. [Profile inventory](../../matrix/README.md)

## Extended observations

- `HTTPRouteRedirectPortAndScheme`, `ListenerSetHTTPRouting`, `GatewayHTTPListenerIsolation`, and `ListenerSetAllowedRoutesNamespaces` passed their request assertions. In each case an HTTPRoute remained present past the suite's 30-second deletion deadline.
- Two `HTTPRouteCORS` preflights, for `/cors-1` and `/cors-wildcard-origin`, timed out at 04:04:19.023 UTC. The first recorded HAProxy reload containing both paths was at 04:04:19.873 UTC, 850 ms later. A subsequent OPTIONS request to `/cors-wildcard-origin` passed at 04:04:19.946 UTC.
- `GatewayStaticAddresses` passed its upstream status and address assertions. The fixture recorded allocation of the usable IP. The additional fixture signal for an allocation attempt on the unusable IP was absent.

The Extended YAML lists all 13 official failures. The trace receipt identifies the log lines and the source hashes for these observations. The records establish the observed timing, with the causes of the cleanup and publication delays undetermined. [Extended YAML](gateway-http-standard-extended-default.yaml) · [Trace receipt](receipt.json)

## Standard HTTP Extended with `-test.parallel=1`

The same 63 Extended cases ran with Go's test parallelism set to one. The upstream executable, deadlines, Gateway manifests, controller and provider versions matched the default run. Core cases were excluded from both Extended selections.

| Default result | Result with `-test.parallel=1` | Cases |
| --- | --- | ---: |
| PASS | PASS | 48 |
| FAIL | PASS | 5 |
| PASS | FAIL | 2 |
| FAIL | FAIL | 8 |

The run with `-test.parallel=1` passed 53 of 63 cases and failed 10. The default run passed 50 and failed 13. Both exited with status 1. [Serial upstream YAML](gateway-http-standard-extended-serial.yaml) · [Per-case comparison and report digests](parallelism-comparison.json)

Five cases passed only with `-test.parallel=1`. `GatewayInvalidFrontendClientCertificateValidation`, `GatewayTLSBackendClientCertificate`, `HTTPRouteCORS`, `ListenerSetAllowedNamespaceSelector`, `ListenerSetHTTPRouting`.

Two cases passed only with the default parallelism. `BackendTLSPolicyInvalidCACertificateRef`, `GatewayFrontendInvalidDefaultClientCertificateValidation`.

Eight cases failed in both runs. `BackendTLSPolicy`, `BackendTLSPolicyInvalidKind`, `BackendTLSPolicySANValidation`, `GatewayFrontendClientCertificateValidation`, `GatewayFrontendClientCertificateValidationInsecureFallback`, `GatewayHTTPListenerIsolation`, `HTTPRouteRedirectPortAndScheme`, `ListenerSetAllowedRoutesNamespaces`.

After `HTTPRouteRedirectPortAndScheme` timed out during cleanup, two TLS cases overlapped on the shared listener. The DNS SAN request in `BackendTLSPolicySANValidation` ran from 06:12:02.685 to 06:12:32.685 CET. The request in `BackendTLSPolicyInvalidKind` ran from 06:12:05.384 to 06:12:35.385 CET, an overlap of 27.3 seconds. These observations come from lines 29972, 30006, 31024 and 31137 of the local upstream log (SHA-256 `aa4b97e0b0b81c89e19381011147e45dd9ac666cb51cf926ebfb5e9bc3da7bc9`). Determining why seven verdicts changed and why eight cases failed twice requires further evidence.

## Experimental HTTP with default parallelism

Two separate local clusters ran the Core and Extended selections with the Experimental CRDs. They used the controller, Octavia version and upstream executable pinned above. [Run inputs and report digests](experimental-http.json)

| Experimental GATEWAY-HTTP selection | Passed | Failed | Skipped in subset | Suite / fixture exit | Upstream report |
| --- | ---: | ---: | ---: | --- | --- |
| Core | 40 | 0 | 0 | 0 / 0 | [Core YAML](gateway-http-experimental-core-default.yaml) |
| Extended | 53 | 11 | 0 | 1 / 1 | [Extended YAML](gateway-http-experimental-extended-default.yaml) |

The Experimental Extended selection contained one case beyond the 63 selected on Standard. `HTTPRouteInvalidParentRefNotMatchingListenerPort` passed. The 40 Core tests were excluded from this Extended selection.

The Experimental Extended report lists these failures (11). `BackendTLSPolicy`, `BackendTLSPolicyInvalidKind`, `BackendTLSPolicySANValidation`, `GatewayFrontendClientCertificateValidation`, `GatewayFrontendClientCertificateValidationInsecureFallback`, `GatewayFrontendInvalidDefaultClientCertificateValidation`, `GatewayHTTPListenerIsolation`, `GatewayTLSBackendClientCertificate`, `HTTPRouteRedirectPortAndScheme`, `ListenerSetAllowedRoutesNamespaces`, `ListenerSetHTTPRouting`.

## Isolated Standard backend TLS case

The three full Extended runs on this pair failed `BackendTLSPolicyInvalidKind`. On a fresh Standard cluster, the same signed pair ran just that case with the original timeouts. It passed, with 62 other Extended cases skipped by selection. The backend TLS policy reported `Accepted=False/NoValidCACertificate` and `ResolvedRefs=False/InvalidKind`. The upstream client saw seven HTTP 404 responses before accepting a 5xx. HAProxy loaded the route to a pool with no servers 1.278 seconds after route creation, and a separate probe received HTTP 503. [Isolated upstream report](gateway-http-standard-extended-invalid-kind-isolated.yaml) · [Conditions, traffic and timings](backend-tls-invalid-kind-isolated.json)

The isolated run completed cleanup. Its final grouped observation could not be decoded, so the exact instant at which Kubernetes stopped returning the HTTPRoute is unknown. The cause of the longer delay during the full suites remains open.

## gRPC, TCP, TLS and UDP on the same revisions

The 18 local gRPC, TCP, TLS and UDP selections recorded 264 passed, 0 failed and 0 skipped case entries across both channels. Cases recur across selections. All 18 upstream selections exited successfully. In 8 Extended selections, the fixture did not observe an allocation attempt for the unusable address in `GatewayStaticAddresses`. [Original protocol reports and address checks](protocols.md)

## Environment and scope

Each run used a disposable kind cluster in a VM, echo Pods, and the controller executable extracted from its OCI archive. It exercised Octavia's installed WSGI API with SQLite, its Amphora driver and renderer, native patched HAProxy 3.0.19, and the stock Barbican WSGI API served locally over HTTPS for certificate enrollment and readback. The fixture supplied Keystone identity and Barbican authentication context, Neutron discovery and VIP allocation, worker dispatch and status completion, and the Amphora agent transport. It invoked the installed conditional policy removal endpoint synchronously. HAProxy served traffic from the rendered configuration to the Kubernetes backends.

The historical 170/170 local cases with a PASS on each channel accumulate evidence across multiple revisions and configurations. The deployed Hibiscus regional ledger remains at 0/14 completed combinations. [Cumulative matrix](../../matrix/README.md#evidence-boundary) · [Regional ledger](../../live-runs.md)
