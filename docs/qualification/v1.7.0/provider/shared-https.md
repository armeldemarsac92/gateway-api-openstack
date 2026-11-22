# Shared HTTPS listeners on Amphora

Four HTTPS listeners with distinct hostnames shared port 443 on a local Gateway. The operator enrolled one Secret revision and enabled regional listener isolation. Octavia returned four virtual hosts, and Amphora rendered one TLS bind. Six upstream echo Pods and both base Gateways were ready before each bundle's registered cases began. The fixture used kind, Octavia and Barbican WSGI, and native HAProxy. It simulated Keystone, Neutron, worker completion and Amphora transport. [Operator setup](../../../operator-https.md) · [Replay archive and full logs](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-98b0267937081ee4e2ff0c17114c2c08d9766ec8)

| Original v1.7.0 case | Standard | Experimental |
|---|---|---|
| `HTTPRouteHTTPSListener` | [PASS, 3/3](shared-https-evidence/standard-httproute-https-listener-report.yaml) | [PASS, 3/3](shared-https-evidence/experimental-httproute-https-listener-report.yaml) |
| `HTTPRouteHTTPSListenerDetectMisdirectedRequests` | [FAIL, 13/15](shared-https-evidence/standard-httproute-https-listener-detect-misdirected-requests-report.yaml) | [FAIL, 14/15](shared-https-evidence/experimental-httproute-https-listener-detect-misdirected-requests-report.yaml) |
| `BackendTLSPolicy` | [FAIL, 4/5](shared-https-evidence/standard-backendtlspolicy-report.yaml) | [FAIL, 4/5](shared-https-evidence/experimental-backendtlspolicy-report.yaml) |

In the misdirection case all seven HTTP/2 requests whose SNI and authority selected different listeners received HTTP 421 on both channels. The positive requests for the wildcard section on Standard and for `second-example.org` on Experimental exceeded the upstream deadline. Separate TLS 1.2 connections verified actual session reuse and HTTP 421 across sections on both bundles. An earlier Standard capture also recorded HTTP 200 from the expected backend when a resumed request changed names within the same wildcard section. [Standard receipt](shared-https-evidence/standard.json) · [Experimental receipt](shared-https-evidence/experimental.json) · [Wildcard session capture](shared-https-evidence/standard-wildcard-session.json)

In the backend TLS case, the HTTPS request reached the Service over authenticated TLS on both bundles. The plain HTTP request that should also have succeeded exhausted 297 attempts in its 30-second window on each channel. An independent follow-up reached the correct backend with HTTP 200 and observed its SNI `abc.example.com`. A wrong backend name or CA returned HTTP 503. Invalidating the referenced CA produced 503 after 10.83 seconds on Standard and 11.72 seconds on Experimental. Restoration produced 200 after 36.66 and 36.51 seconds respectively. The original run recorded the registered case as failed on each channel. [Standard receipt](shared-https-evidence/standard.json) · [Experimental receipt](shared-https-evidence/experimental.json)

The tested manager came from controller `98b0267937081ee4e2ff0c17114c2c08d9766ec8`. The region used Octavia `19.0.1.dev12`, octavia-lib `3.12.1.dev5`, Barbican 23.0.0 and HAProxy 3.0.19 with the strict backend DNS check. The package and image digests identify the local replay inputs. The seven deployed Hibiscus profiles remain unrun on each channel. [Controller and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-98b0267937081ee4e2ff0c17114c2c08d9766ec8) · [Octavia extension](https://github.com/armeldemarsac92/octavia/pull/9) · [Provider library](https://github.com/armeldemarsac92/octavia-lib/pull/5)

## Cold replay

The shared HTTPS manager at `98b026793708` passed all 15 misdirected-request checks on each Gateway API v1.7.0 channel and all five backend TLS checks on Standard and in the isolated Experimental run. Positive requests reached their selected backend, seven requests that crossed HTTPS listener scopes returned 421, and an unknown host returned 404. A separate run on `696af2a79acc` passed all 13 backend policy conflict checks per channel, including the HTTP requests for the winning policies.

| Upstream case | Standard | Experimental |
| --- | --- | --- |
| `BackendTLSPolicyConflictResolution` | [13/13](https-cold-evidence/standard-backendtlspolicy-conflict-resolution-report.yaml) | [13/13](https-cold-evidence/experimental-backendtlspolicy-conflict-resolution-report.yaml) |
| `BackendTLSPolicy` | [5/5](https-cold-evidence/standard-backendtlspolicy-report.yaml) | [2/5](https-cold-evidence/experimental-backendtlspolicy-report.yaml) · [5/5](https-cold-evidence/experimental-isolated-backendtlspolicy-report.yaml) |
| `HTTPRouteHTTPSListenerDetectMisdirectedRequests` | [15/15](https-cold-evidence/standard-httproute-https-listener-detect-misdirected-requests-report.yaml) | [15/15](https-cold-evidence/experimental-httproute-https-listener-detect-misdirected-requests-report.yaml) |

Each run used a fresh kind cluster with real echo Pods and native Amphora-rendered HAProxy. The local Kubernetes API relay enabled `TCP_NODELAY` and the controller used its 10-minute background resync. In the controlled Standard policy-conflict comparison, enabling that relay option reduced the time to first HTTP 200 from 85.07 seconds to 24.72 seconds with the same 28 Octavia mutations. The test inputs and upstream request deadlines stayed the same. [Standard conflict receipt](https-cold-evidence/standard-conflict.json) · [Experimental conflict receipt](https-cold-evidence/experimental-conflict.json)

The primary backend TLS and HTTPS tests used Octavia 19.0.1.dev12 and octavia-lib 3.12.1.dev5. The conflict test used 19.0.1.dev11 and 3.12.1.dev4. The regional provider and network were local fixtures. [Standard shared listener receipt](https-cold-evidence/standard-shared.json) · [Experimental shared listener receipt](https-cold-evidence/experimental-shared.json) · [Fixture supplement](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-qualification-cold-https-v1.7.0)

In the combined Experimental run, the backend TLS case passed 2/5. The host OOM killer terminated an echo process and CoreDNS when the controller reported a timeout on the plain HTTP route. The HTTPS route reported Programmed. The separate supplemental re-encryption probe also timed out. [Combined Experimental receipt](https-cold-evidence/experimental-shared.json)

The subsequent cold run executed only the original backend TLS case and passed 5/5 upstream checks. [Isolated Experimental receipt](https-cold-evidence/experimental-isolated.json)
