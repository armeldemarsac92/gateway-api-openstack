# BackendTLSPolicy on Amphora

An HTTPRoute can require an authenticated TLS connection to a Service port. The same controller can accept HTTP or terminate HTTPS at the Gateway. On the backend connection, Amphora sends the hostname from the policy as SNI, checks the DNS SAN against that hostname and verifies the chain with the policy's ConfigMap CA. The feature requires operator opt-in and the regional provider extension. [Operator configuration](../../../operator-octavia-backend-tls-policy.md)

## Registered Gateway API cases

The two unchanged upstream status cases ran on separate rootless kind clusters with a Ready echo Pod on each release channel. Updating the policy advanced the observed generation. Reconciliation kept the foreign controller's ancestor entry while updating the managed entry. Each report covers one case. [Standard receipt](backend-tls-policy-evidence/standard-status.json) · [Experimental receipt](backend-tls-policy-evidence/experimental-status.json)

| Registered case | Standard | Experimental |
|---|---|---|
| `BackendTLSPolicyObservedGenerationBump` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-observed-generation-bump-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-observed-generation-bump-report.yaml) |
| `BackendTLSPolicyPreserveForeignStatus` | [PASS](backend-tls-policy-evidence/standard-backendtlspolicy-preserve-foreign-status-report.yaml) | [PASS](backend-tls-policy-evidence/experimental-backendtlspolicy-preserve-foreign-status-report.yaml) |

The earlier registered `BackendTLSPolicy` run used manager `939285c` on each channel and timed out before its five traffic assertions. All four HTTPS listeners on port 443 reported `Accepted=False/PortUnavailable`. This controller release implements a dedicated HTTPS listener per port. That original case remains failed in the ledger. The earlier attempts at the two status cases also timed out in envtest. [Standard attempt](backend-tls-policy-evidence/standard-earlier-attempt.json) · [Experimental attempt](backend-tls-policy-evidence/experimental-earlier-attempt.json)

## Traffic observed locally

The separate wire runs used the four rules in one HTTPRoute and a dedicated HTTPS Gateway. A Kubernetes API server stored the policies and ConfigMaps. The controller used the public Octavia and Barbican APIs. Amphora rendered the configuration for native HAProxy, and an isolated echo peer reported its backend SNI. Identity, networking and worker transport used local fixtures. [Standard traffic receipt](backend-tls-policy-evidence/standard-traffic.json) · [Experimental traffic receipt](backend-tls-policy-evidence/experimental-traffic.json)

| Request after reconciliation | Standard | Experimental |
|---|---:|---:|
| HTTP to the Service with the correct DNS name and CA | 200 | 200 |
| HTTP to the Service with a mismatched backend DNS name | 503 | 503 |
| HTTP to the Service with a mismatched backend CA | 503 | 503 |
| HTTP while the referenced CA was invalid | 503 | 503 |
| HTTP after the original CA was restored | 200 | 200 |
| HTTPS for `backend-tls.wildcard.org` with backend SNI `abc.example.com` | 200 | 200 |

Both clients verified the frontend hostname. In the four-rule runs, the first observed 503 after the CA became invalid took 8.20 and 8.75 seconds on Standard and Experimental. After restoration, the first 200 took 21.26 and 23.67 seconds. These are local observations from the probe windows. The matrix counts only the two registered status cases.

## Revisions

The controller source was `577264d263dec1c8428efb487caaa82f549d56a8`. The tested wheels were Octavia `19.0.1.dev11` and octavia-lib `3.12.1.dev4`, with Barbican `23.0.0` and HAProxy `3.0.19-ade9c1262-octavia-strict-dns-v1`. The controller archive and both local conformance reports are linked in the development release. The deployed Hibiscus profiles remain unrun. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-577264d263dec1c8428efb487caaa82f549d56a8) · [Provider packages and replay](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-tls-hostname-ba89a2d6237a) · [Driver library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-tls-hostname-8ac88a770cc3)
