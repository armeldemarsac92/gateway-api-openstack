# First completed common Gateway qualification cell

The upstream Gateway API v1.7.0 report is dated November 25, 2026 at 22:35 CET. It records the first completed cell of a local campaign with 22 planned profile, channel and transport selections. This run used controller `13a6870cb63a` and Octavia `e62a57c49177`.

| Profile | Channel | Selection | Listener mode | Passed | Failed | Skipped |
| --- | --- | --- | --- | ---: | ---: | ---: |
| GATEWAY-HTTP | Standard | Core | Default | 39 | 1 | 0 |

The upstream suite and local fixture each exited with status 1. The two files preserve the upstream YAML and the campaign collector's portable JSON byte for byte. The JSON records the exact inputs, per-case verdicts and artifact checksums. [Upstream report](gateway-http-standard-core-default.yaml) · [Inputs and results](results.json)

## Hostname intersection cleanup

All 33 HTTP request assertions in `HTTPRouteHostnameIntersection` passed. At 22:29:40.986 CET the suite started deleting the HTTPRoute. Its Kubernetes applier reported the route still present when the official 30-second cleanup deadline expired. The next route deletion began at 22:30:10.987 CET.

The local Octavia trace records four policy updates, eight rule deletes and four policy deletes through the individual Octavia APIs. The final recorded policy DELETE started 30.401 seconds after cleanup began and returned HTTP 204 about 30.584 seconds after cleanup began. The cleanup deadline expired while provider deletion was still in progress. The trace leaves the finalizer removal time and the contribution of each wait undetermined.

## Scope

This snapshot records one completed local cell on kind, the Octavia API and Amphora rendering with native HAProxy. Cloud services and worker delivery use local fixtures. Zero deployed Hibiscus regions were exercised. Other campaign cells and any targeted replay have their own results. [Regional ledger](../../live-runs.md)

The earlier 40/40 local HTTP Core results on each channel used different controller and provider revisions. The 170 registered cases with a local PASS on each channel are a cumulative result across separate executions and revisions. [Earlier HTTP Core reports](../../provider/unified-http-core-598db990b3ba/README.md) · [Evidence matrix](../../matrix/README.md#evidence-boundary)
