# ListenerSet admission and HTTP routing

The controller admits delegated listeners according to the Gateway namespace policy, reports hostname and protocol conflicts and includes eligible delegated HTTP sections in the Gateway graph. The original tests use Gateway API v1.7.0 revision `e26d183dbbc1bb782b78addbd26faba03955c152`. [Registered case matrix](../matrix/features-and-cases.md)

## Grouped policy activation

The signed controller image at `0cde3dcd7e49` and the stable Octavia 19.0.1.dev40 wheel passed all 36 `ListenerSetHTTPRouting` checks on Standard and Experimental. Fourteen prepared policies activated in one POST and one HAProxy reload for the group. The first Experimental attempt passed 34 checks. Two requests expired before the grouped reload. The successful rerun used a tmpfs guest overlay and retained the upstream deadline, manifests and binaries. [Results and timestamps](l7-policy-activation/summary.json) · [Standard](l7-policy-activation/gateway-api-v1.7.0-standard.yaml) · [Experimental](l7-policy-activation/gateway-api-v1.7.0-experimental.yaml) · [First Experimental attempt](l7-policy-activation/gateway-api-v1.7.0-experimental-first-attempt.yaml)

The manager ran from the extracted linux/amd64 OCI image and forwarded requests to Kubernetes echo pods through HAProxy. The local setup used Octavia REST, the Amphora worker and rendering code, SQLite and synchronous RPC delivery. A simulated Amphora peer loaded the real HAProxy process. These results cover this Extended case. [Operator settings](../../../operator-octavia-http-actions.md) · [Octavia provider](https://github.com/armeldemarsac92/octavia/pull/19)

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

## Route status for an explicit parent

On controller `ce33dd73a1023f87e61756714f2b288aed1991ba`, `ListenerSetRouteStatusScopedToParentRef` passed on both Standard and Experimental. The Gateway-parented HTTPRoute reported exactly one accepted Gateway parent. The ListenerSet-parented HTTPRoute reported exactly one accepted ListenerSet parent. The upstream case also checked the Gateway's attached ListenerSet count and the ListenerSet's status. It sends no traffic. [Upstream assertions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/listenerset-route-status-scoped-to-parentref.go) · [Per-channel receipts](listenerset-positive-results.json)

Both runs used the controller's reconcilers, a Kubernetes 1.36.2 envtest API server and a recording OpenStack provider. With the envtest prerequisites set for the selected bundle, run `go test -tags envtest -count=1 -run 'TestControllerEnvtest/upstream_Gateway_status_cases/ListenerSet_admission/ListenerSetRouteStatusScopedToParentRef' ./internal/controller`.

## HTTP routing on cold Kind

The original `ListenerSetHTTPRouting` checks 36 combinations of hostname, path and Gateway or ListenerSet parent reference. Each channel started a fresh Kind cluster in an isolated KVM guest. The test used the upstream echo Pods and the local Octavia REST, Amphora and HAProxy fixture.

| Controller revision | Standard | Experimental |
| --- | --- | --- |
| `3d3041363d4394bd6a9b8e6115665d327c8271e7` | FAIL (23/36) | FAIL (22/36) |
| `72441149db900992244a02e6664b353b70442b66` | FAIL (24/36) | FAIL (25/36) |

The newer build cherry-picked `9e63cf97a8b9201f19ca241c96cf37b4810f1d77` to share Gateway reads inside each HTTP route graph construction. The binary metadata reports the newer revision and a clean source tree. Its SHA-256 is `47b936644a8c79cc2eea798689339991b551bbba2fa4c0aca4b8838f8fc964c3`. The Gateway, ListenerSet and HTTPRoute controller package tests passed.

- Standard logged 3807 HTTP request attempts, starting 13.83 s after manifest application began. The request attempts spanned 197.32 s. The upstream assertions observed 3735 responses with 404 when the case expected 200. Failed probes elapsed 30.00 s. At the post-case snapshot, 7 HTTPRoute parent conditions still reported `Programmed=False/Pending`.
- Experimental logged 3707 HTTP request attempts, starting 16.93 s after manifest application began. The request attempts spanned 186.60 s. The upstream assertions observed 3632 responses with 404 when the case expected 200. Failed probes elapsed 30.00 s. At the post-case snapshot, 6 HTTPRoute parent conditions still reported `Programmed=False/Pending`.

The shared Kind lock was acquired at `2026-11-22T13:10:34Z`. Standard finished at `2026-11-22T14:15:44+01:00` and Experimental at `2026-11-22T14:20:33+01:00`. These runs did not identify the cause of the remaining convergence delay. The earlier native failures remain recorded. Prior host and VM attempts without case reports stopped in setup. The matrix counts an HTTP routing PASS only when the original case passes all 36 assertions.

[Machine-readable results](listenerset-positive-results.json) identify each mode, revision, bundle and original report hash. The local `hibiscus-listenerset-72441149db90.tar.gz` archive preserves the reports, logs and replay scripts. Live Hibiscus cloud, managed Amphora behavior and the full Gateway profiles remain unqualified by these runs.

## Grouped initial L7 rules

On controller `7ed5e90f0862`, the registered namespace, dual-parent and missing-section cases passed on both channels. The 36-request HTTP routing case reached 35/36 on Standard and 34/36 on Experimental. Each failing request hit the original 30-second deadline. [Case results, failed requests and replay](listener-hostname-runs/README.md)
