# GATEWAY-HTTP Extended on controller aba071b9

The Standard bundle passed 60 of 63 selected Extended cases with the controller's 10-minute OpenStack resync. Three cases failed, zero were skipped, and both the upstream suite and the fixture exited 1. This selection excludes the 40 Core cases. [Original upstream report](http-standard-extended.yaml) · [Per-case verdicts, inputs and runtime observations](http-standard-extended.json)

| Failed case | Previous run on `454c1541` |
| --- | --- |
| `BackendTLSPolicyInvalidCACertificateRef` | Passed |
| `BackendTLSPolicySANValidation` | Failed |
| `ListenerSetAllowedRoutesNamespaces` | Passed |

The earlier run with the same 10-minute interval also passed 60 of 63. Two cases that previously failed passed on this run, `BackendTLSPolicy` and `BackendTLSPolicyInvalidKind`. [Previous upstream report](../454c1541-460570fb/http-standard-extended-operator-resync.yaml)

## Route deletion

All three cases passed their traffic and status assertions before invoking cleanup. The upstream suite then waited 30 seconds for Kubernetes to return `NotFound` for each HTTPRoute. HAProxy removed the selected paths during those windows, and the final inventory contained zero HTTPRoutes. The poll recorded no `NotFound` before its deadline. The available trace cannot distinguish a route still present from a GET error. For the negative SAN probes, HTTP 200 fails the test, and transport errors and HTTP 408 or 504 trigger retries. A 404 satisfies the assertion. The frontend had not yet published those paths, and the log omits the received status codes. [Upstream deletion check](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/kubernetes/apply.go#L221-L236)

| HTTPRoute | Seconds from logged deletion request to removal of its last selected path in HAProxy |
| --- | ---: |
| `gateway-conformance-infra/backendtlspolicy-san-test` | 4.975 |
| `gateway-conformance-infra/backendtlspolicy-invalid-ca-certificate-ref` | 15.288 |
| `gateway-api-ls-cross-ns/route-in-listenerset-namespace` | 17.044 |

## Test environment

The unmodified v1.7.0 conformance executable exercised a kind cluster with the Standard CRDs. The controller process used the binary from the signed image. The local environment used the Octavia API, the Amphora renderer and HAProxy. Fixtures supplied Keystone identity, Neutron discovery, VIP allocation and Amphora transport. The operator enabled closed-listener policy removal. The record includes the controller and wheel revisions, image digest, executable hashes and discovered CRDs. A deployed Hibiscus qualification is still open. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-aba071b9416112dd29e5852664913d3470013202) · [Regional qualification ledger](../../live-runs.md)
