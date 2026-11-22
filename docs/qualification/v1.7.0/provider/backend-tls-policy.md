# BackendTLSPolicy on Amphora

An HTTPRoute or GRPCRoute can require an authenticated TLS connection to a Service port. The same controller can accept HTTP or terminate HTTPS at the Gateway. On the backend connection, Amphora sends the hostname from the policy as SNI, checks the DNS SAN against that hostname and verifies the chain with the policy's ConfigMap CA. The feature requires operator opt-in and the regional provider extension. [Operator configuration](../../../operator-octavia-backend-tls-policy.md)

## Registered Gateway API cases

The two unchanged upstream status cases ran on separate rootless kind clusters with a Ready echo Pod on each release channel. Updating the policy advanced the observed generation. Reconciliation kept the foreign controller's ancestor entry while updating the managed entry. Each report covers one case. [Standard receipt](backend-tls-policy-evidence/standard-status.json) · [Experimental receipt](backend-tls-policy-evidence/experimental-status.json)

| Registered case | Standard | Experimental |
|---|---|---|
| `BackendTLSPolicyObservedGenerationBump` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-observed-generation-bump-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-observed-generation-bump-report.yaml) |
| `BackendTLSPolicyPreserveForeignStatus` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-preserve-foreign-status-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-preserve-foreign-status-report.yaml) |
| `BackendTLSPolicyInvalidKind` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-invalid-kind-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-invalid-kind-report.yaml) |
| `BackendTLSPolicyInvalidCACertificateRef` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-invalid-ca-certificate-ref-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-invalid-ca-certificate-ref-report.yaml) |
| `BackendTLSPolicyConflictResolution` | [FAIL](backend-tls-policy-evidence/standard-backendtlspolicy-conflict-resolution-report.yaml) | [FAIL](backend-tls-policy-evidence/experimental-backendtlspolicy-conflict-resolution-report.yaml) |

The same released manager ran each of the three additional cases on a fresh kind cluster with a Ready workload. An unsupported CA reference kind produced `ResolvedRefs=False/InvalidKind`. A missing or malformed ConfigMap produced `ResolvedRefs=False/InvalidCACertificateRef`. Both cases checked `Accepted=False/NoValidCACertificate` and required a response from the set 500, 502 and 503. All three subtests for the invalid kind and all eight subtests for the invalid ConfigMaps passed on each channel. The latter count includes the two parent subtests. [Standard receipts](backend-tls-policy-evidence/standard-reference-and-conflict.json) · [Experimental receipts](backend-tls-policy-evidence/experimental-reference-and-conflict.json)

The conflict case verified all six policy conditions on each channel. For competing policies at the same scope, the policy named with the `-1` suffix was accepted and the policy with the `-2` suffix reported `Conflicted`. A port-scoped policy and a Service-wide policy could both be accepted. One of the four HTTP success checks passed on Standard and two passed on Experimental. The remaining HTTP requests exceeded the upstream time limit. These runs started the original case once the GatewayClass was accepted, while the base Gateway could still be provisioning. [Standard conflict report](backend-tls-policy-evidence/standard-backendtlspolicy-conflict-resolution-report.yaml) · [Experimental conflict report](backend-tls-policy-evidence/experimental-backendtlspolicy-conflict-resolution-report.yaml)

The conflict case was repeated with the same released manager after the original base readiness check confirmed the Gateway and workload were ready. On both channels, all six policy conditions and two of the four HTTP success checks passed. Both same-scope conflict requests timed out after 30 seconds. The two requests for a Service-wide policy alongside a port-scoped policy succeeded. The registered case still failed. [Readiness and subtest results](backend-tls-policy-evidence/ordered-conflict.json) · [Standard report](backend-tls-policy-evidence/standard-ordered-conflict-report.yaml) · [Experimental report](backend-tls-policy-evidence/experimental-ordered-conflict-report.yaml)

To replay those three cases, verify the existing controller release archive and apply the additional case selectors in its extracted directory with `git apply`. Set `REGISTERED_CASES` to one of the three exact case names and use the existing `run-kind.sh` instructions for each channel. The Experimental invalid ConfigMap run used `KIND_ETCD_TMPFS=1` after a disposable control plane timed out during bootstrap. The case bodies, manifests and suite timeouts come from Gateway API v1.7.0. For the later conflict run, apply the base-readiness patch after the case-selector patch and use `KIND_ETCD_TMPFS=1` on each channel. [Base-readiness patch](backend-tls-policy-evidence/conflict-base-readiness.patch) [Replay patch](backend-tls-policy-evidence/registered-backend-tls-cases.patch) · [Pinned release archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-577264d263dec1c8428efb487caaa82f549d56a8)

The earlier registered `BackendTLSPolicy` run used manager `939285c` on each channel and timed out before its five traffic assertions. All four HTTPS listeners on port 443 reported `Accepted=False/PortUnavailable`. That revision supported a dedicated HTTPS listener per port. The later shared-listener revision ran all five original requests on both bundles and passed four. [Shared HTTPS results](shared-https.md) The earlier attempts at the two status cases also timed out in envtest. [Standard attempt](backend-tls-policy-evidence/standard-earlier-attempt.json) · [Experimental attempt](backend-tls-policy-evidence/experimental-earlier-attempt.json)

## Traffic observed locally

A subsequent cold replay passed all 13 policy conflict checks on each channel. The shared HTTPS replay exercised the full primary case and CA replacement. The reports retain the outcome of each attempt. [Controller revisions, fixture and reports](shared-https.md#cold-replay)

The separate wire runs used the four rules in one HTTPRoute and a dedicated HTTPS Gateway. A Kubernetes API server stored the policies and ConfigMaps. The controller used the public Octavia and Barbican APIs. Amphora rendered the configuration for native HAProxy, and an isolated echo peer reported its backend SNI. Identity, networking and worker transport used local fixtures. [Standard traffic receipt](backend-tls-policy-evidence/standard-traffic.json) · [Experimental traffic receipt](backend-tls-policy-evidence/experimental-traffic.json)

| Request after reconciliation | Standard | Experimental |
|---|---:|---:|
| HTTP to the Service with the correct DNS name and CA | 200 | 200 |
| HTTP to the Service with a mismatched backend DNS name | 503 | 503 |
| HTTP to the Service with a mismatched backend CA | 503 | 503 |
| HTTP while the referenced CA was invalid | 503 | 503 |
| HTTP after the original CA was restored | 200 | 200 |
| HTTPS for `backend-tls.wildcard.org` with backend SNI `abc.example.com` | 200 | 200 |

Both clients verified the frontend hostname. In the four-rule runs, the first observed 503 after the CA became invalid took 8.20 and 8.75 seconds on Standard and Experimental. After restoration, the first 200 took 21.26 and 23.67 seconds. These are local observations from the probe windows. The matrix records the separate upstream cases listed above.

## gRPC with required backend HTTP/2

The original [`BackendTLSPolicyGRPCRoute` case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/backendtlspolicy-grpcroute.go) passed 3/3 on Standard and Experimental. The linux/amd64 manager came from the OCI at `51ad8cc6316f6aac929961f70866f88053896982`. The gRPC client used the Gateway's HTTP listener and Amphora authenticated the TLS connection to the Service. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982) · [Original reports, receipts and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982/backendtlspolicy-grpcroute-51ad8cc-evidence.tar.gz)

| Backend after convergence | Standard | Experimental |
| --- | --- | --- |
| Valid CA and DNS name, negotiated `h2` | gRPC OK | gRPC OK |
| Wrong CA or DNS name | gRPC UNAVAILABLE | gRPC UNAVAILABLE |
| Valid CA and DNS name, no negotiated ALPN | gRPC UNAVAILABLE, 5 completed TLS handshakes, 0 application bytes | gRPC UNAVAILABLE, 5 completed TLS handshakes, 0 application bytes |

The upstream negative subtests accept a failed RPC. The original client saw HTTP 503 / gRPC UNAVAILABLE for the certificate mismatch and HTTP 404 / gRPC UNIMPLEMENTED for the hostname mismatch. The independent probe then targeted both mismatches with all three redirects and TLS pools active, yielding the gRPC statuses above.

The provider opt-in requires the controller's gRPC profile and `--octavia-backend-tls-policy`, plus Amphora's `backend_tls_hostname_v1` and `backend_tls_h2_alpn_v1` capabilities. Octavia and the Amphora agent must be configured to advertise both capabilities. The local run used Octavia microversion 2.24. Octavia returned `backend_tls_require_h2=true` for all three active pools. The live HAProxy configuration for each pool used `alpn h2 require-alpn-h2`, SNI from the fixed policy hostname and strict DNS verification with the configured CA. [Operator configuration](../../../operator-octavia-backend-tls-policy.md) · [Octavia and Amphora distribution](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-h2-9729fed77683) · [octavia-lib distribution](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-tls-h2-696c5ae9ceb2)

Each policy supports one Service target and one ConfigMap CA in the Service namespace, with a single PEM CA certificate and a fixed DNS hostname checked against the certificate's DNS SAN. Explicit `subjectAltNames`, system trust roots, multi-CA bundles and client certificates remain outside this profile. The gRPC Service port can omit `appProtocol` or use `https`. A `kubernetes.io/h2c` port cannot use this policy.

The routes and policies had current `Accepted=True` and `ResolvedRefs=True`. The controller-specific route condition `gateway-api-openstack.github.io/Programmed` still read `False/Pending` at capture. The fixture used an envtest API server, installed Octavia and Barbican APIs and native HAProxy. It simulated Keystone, Neutron, Octavia RPC completion, Amphora agent transport, Pod readiness and EndpointSlice publication. Deployed Hibiscus qualification remains open.

The preliminary Standard attempt passed the original upstream case. Its supplemental check expected Octavia's legacy `alpn_protocols` list to contain only h2. The new `backend_tls_require_h2` flag does not change that legacy list, so the fixture stopped before the wire probes. The pack preserves that run's original report and receipt separately. The verified Standard and Experimental receipts use the corrected provider readback and the independent post-convergence probes. [Evidence pack](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982/backendtlspolicy-grpcroute-51ad8cc-evidence.tar.gz)

## Revisions

The controller source was `577264d263dec1c8428efb487caaa82f549d56a8`. The tested wheels were Octavia `19.0.1.dev11` and octavia-lib `3.12.1.dev4`, with Barbican `23.0.0` and HAProxy `3.0.19-ade9c1262-octavia-strict-dns-v1`. The controller archive and both local conformance reports are linked in the development release. The deployed Hibiscus profiles remain unrun. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-577264d263dec1c8428efb487caaa82f549d56a8) · [Provider packages and replay](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-hostname-ba89a2d6237a) · [Driver library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-tls-hostname-8ac88a770cc3)
