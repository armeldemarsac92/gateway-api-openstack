# ListenerSet admission and HTTP routing

The controller admits delegated listeners according to the Gateway namespace policy, reports hostname and protocol conflicts and includes eligible delegated HTTP sections in the Gateway graph. The original tests use Gateway API v1.7.0 revision `e26d183dbbc1bb782b78addbd26faba03955c152`. [Registered case matrix](../matrix/features-and-cases.md)

## Admission and route lifecycle

On `afcc7cdc9074119984058aa03949684ad4abca25`, all seven upstream status cases passed on both Standard and Experimental with Kubernetes 1.36.2 envtest and a recording OpenStack provider.

| Original case | Standard | Experimental |
| --- | --- | --- |
| `ListenerSetDefaultNotAllowed` | PASS | PASS |
| `ListenerSetAllowedNamespaceNone` | PASS | PASS |
| `ListenerSetAllowedNamespaceSame` | PASS | PASS |
| `ListenerSetAllowedNamespaceSelector` | PASS | PASS |
| `ListenerSetHostnameConflict` | PASS | PASS |
| `ListenerSetProtocolConflict` | PASS | PASS |
| `ListenerSetAllowedRoutesSupportedKinds` | PASS | PASS |

The local parent lifecycle also passed on both channels at that revision. It covers parent deletion and replacement, and toggles a Namespace label between allowed and denied values. Each change updates the HTTPRoute's acceptance and the ListenerSet's attached route count. The original `ListenerSetAllowedNamespaceSame` and `ListenerSetAllowedNamespaceSelector` cases also passed on Kind with both channels and both Kind controller revisions. These two cases assert status and issue zero HTTP requests. [Earlier rejection evidence](listenerset-rejection.md)

## HTTP routing on cold Kind

The original `ListenerSetHTTPRouting` checks 36 combinations of hostname, path and Gateway or ListenerSet parent reference. Each channel started a fresh Kind cluster in an isolated KVM guest. The test used the upstream echo Pods and the local Octavia REST, Amphora and HAProxy fixture.

| Controller revision | Standard | Experimental |
| --- | --- | --- |
| `3d3041363d4394bd6a9b8e6115665d327c8271e7` | FAIL (23/36) | FAIL (22/36) |
| `72441149db900992244a02e6664b353b70442b66` | FAIL (24/36) | FAIL (25/36) |

The newer build cherry-picked `9e63cf97a8b9201f19ca241c96cf37b4810f1d77` to share Gateway reads inside each HTTP route graph construction. The binary metadata reports the newer revision and a clean source tree. Its SHA-256 is `47b936644a8c79cc2eea798689339991b551bbba2fa4c0aca4b8838f8fc964c3`. The Gateway, ListenerSet and HTTPRoute controller package tests passed.

- Standard logged 3807 HTTP request attempts, starting 13.83 s after manifest application began. The request attempts spanned 197.32 s. The upstream assertions observed 3735 responses with 404 when the case expected 200. Failed probes elapsed 30.00 s. At the post-case snapshot, 7 HTTPRoute parent conditions still reported `Programmed=False/Pending`.
- Experimental logged 3707 HTTP request attempts, starting 16.93 s after manifest application began. The request attempts spanned 186.60 s. The upstream assertions observed 3632 responses with 404 when the case expected 200. Failed probes elapsed 30.00 s. At the post-case snapshot, 6 HTTPRoute parent conditions still reported `Programmed=False/Pending`.

The shared Kind lock was acquired at `2026-11-22T13:10:34Z`. Standard finished at `2026-11-22T14:15:44+01:00` and Experimental at `2026-11-22T14:20:33+01:00`. The cause of the remaining convergence delay is unresolved. The earlier native failures remain recorded. Prior host and VM attempts without case reports stopped in setup. The matrix counts an HTTP routing PASS only when the original case passes all 36 assertions.

[Machine-readable results](listenerset-positive-results.json) identify each mode, revision, bundle and original report hash. The local `hibiscus-listenerset-72441149db90.tar.gz` archive preserves the reports, logs and replay scripts. Live Hibiscus cloud, managed Amphora behavior and the full Gateway profiles remain unqualified by these runs.
