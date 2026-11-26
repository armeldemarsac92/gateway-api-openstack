# Isolated Experimental BackendTLSPolicySANValidation

The upstream Gateway API v1.7.0 GATEWAY-HTTP Extended case passed on controller revisions `454c1541` and `ae35807b` (PR #145). Each local run selected one case. Each report records one pass, zero failures and 63 skipped Extended cases. All six subtests passed and both the suite and fixture exited with status 0.

| Tested controller | Case duration | Original upstream report | Measurements |
| --- | ---: | --- | --- |
| `454c154167a521956cd043967cf6d27e8755b292` | 44.55 s | [YAML](454c1541-report.yaml) | [JSON](454c1541-measurements.json) |
| `ae35807bd2fc80ea48b87b0ad7840288a74541ce` | 38.94 s | [YAML](ae35807b-report.yaml) | [JSON](ae35807b-measurements.json) |

Both runs used the same upstream executable and Experimental bundle, Octavia `460570fb`, octavia-lib `735564bb` and native HAProxy. The local Amphora fixture enabled closed-listener policy removal and both managers used a two-second OpenStack resync interval. The manager image and executable digests are recorded for each revision. The earlier full Experimental run on `454c1541` recorded a failure for this case. This comparison covers two isolated runs. [Full Experimental run](../454c1541-460570fb/README.md)

## Pool preparation and route publication

The traces record the following order for the first successful Octavia pool `PUT` for each BackendTLSPolicy.

- `454c1541` · `san-uri-mismatch` → `san-uri` → `multiple-mismatch-sans` → `multiple-sans` → `san-dns` → `san-dns-mismatch`.
- `ae35807b` · `san-dns` → `san-dns-mismatch` → `san-uri` → `san-uri-mismatch` → `multiple-sans`.

The reference trace records six pool creations and the PR #145 trace records five. Each records five L7 policy creations with initial rules. In each run, the five published exact paths used their corresponding BackendTLSPolicy pools at HAProxy positions 1 through 5, in the order shown for `ae35807b`. The published TLS settings used a CA file, required verification and the expected SNI and SANs. The reference graph includes the `multiple-mismatch-sans` pool. Both publication traces omit its path.

## HTTP probe timings

The individual timings run from each first logged GET to the logged successful HTTP 200 expectation. The cumulative timings use the first DNS GET as their common start.

| Positive check | Individual wait, `454c1541` → `ae35807b` | HTTP 404s before success | Cumulative wait, `454c1541` → `ae35807b` |
| --- | ---: | ---: | ---: |
| DNS | 16.248 → 4.564 s | 161 → 45 | 16.248 → 4.564 s |
| URI | 8.787 → 9.397 s | 87 → 93 | 25.041 → 13.970 s |
| Multiple SANs | 4.656 → 12.111 s | 46 → 120 | 29.703 → 26.087 s |

On PR #145, the DNS check finished earlier and the URI and multiple-SAN checks started earlier. Their individual waits increased. The final HTTP 200 arrived 3.616 seconds earlier from the first DNS GET in this pair of runs.

For each of the three mismatch checks, the latest HAProxy publication observed at the first GET lacked an active route for its path in both runs. The upstream assertion accepts an HTTP response other than 200, excluding timeout responses. These passes leave the exact response status, the serving HAProxy process and any backend TLS rejection undetermined.
