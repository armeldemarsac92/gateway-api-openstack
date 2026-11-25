# Gateway API v1.7.0 TLS and UDP Core on kind

The campaign used the published Hibiscus controller `9388be4580dd0fac002e160c3433d65cf1665a86` with Octavia `041a2becdc07fd5c6c3ab7da56cb858e332589a5` and octavia-lib `eb4c646b59758334629feb99d517f938c8af3712`. Each profile ran on separate Standard and Experimental kind clusters. The upstream CLI applied the original shared base, including its HTTPS Gateway. The local Barbican fixture enrolled the frontend Secret revisions created by the CLI. [Published image](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-9388be4580dd0fac002e160c3433d65cf1665a86) · [Octavia provider release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-l7policy-conditional-removal-041a2becdc07)

| Profile | Channel | Passed | Failed | Skipped | Upstream report |
|---|---|---:|---:|---:|---|
| `GATEWAY-TLS` | Standard | 21 | 0 | 0 | [Original YAML](tls-core-evidence/standard-report.yaml) |
| `GATEWAY-TLS` | Experimental | 21 | 0 | 0 | [Original YAML](tls-core-evidence/experimental-report.yaml) |
| `GATEWAY-UDP` | Standard | 21 | 0 | 0 | [Original YAML](udp-core-evidence/standard-report.yaml) |
| `GATEWAY-UDP` | Experimental | 21 | 0 | 0 | [Original YAML](udp-core-evidence/experimental-report.yaml) |

The Core CLI used the upstream case deadlines and default provisional-test behavior. The receipt records served CRDs, OCI entrypoint and wheel digests, manager flags and eligible cases. [Upstream profiles](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go) · [TLS receipt](tls-core-evidence/receipt.json) · [UDP receipt](udp-core-evidence/receipt.json)

## TLS passthrough

On both channels the origin answered the expected TLS and HTTPS requests for exact and wildcard SNI. The hostname intersection case exercised exact, narrower wildcard, wider wildcard and empty listener hostnames and checked rejection outside the intersection. The simple TLSRoute case used the origin TLS echo response. The receipt records HAProxy activation hashes and the Octavia SNI rules used for those listeners. [Hostname intersection case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tlsroute-hostname-intersection.go) · [Simple TLSRoute case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tlsroute-simple-same-namespace.go)

## UDP forwarding

The preliminary Standard fixture used a two-second monitor delay. Octavia rejected the UDP-CONNECT monitor writes because its configured minimum is three seconds. The fixture then used three seconds for both channels. The table records the runs with the corrected fixture. Octavia returned HTTP 400 for nine monitor writes before the operator stopped the preliminary run.

The Experimental upstream runner returned zero after all 21 cases passed. Its outer fixture returned one because an evidence callback tried to decode the maximum-length Gateway name's valid digest tag as UTF-8. A separate replay of the corrected decoder returned zero on the tag reconstructed from the preserved Gateway name and the published encoder. The report checksum matched before and after the replay. [Decoder replay receipt](udp-core-evidence/experimental-decoder-replay.json)

The original Experimental launch session also exited with code 2 after the guest powered down. The launcher script had been edited while it was running. Both the channel exits and the raw upstream reports had already been written.

The installed Amphora renderer provided the configurations consumed by Keepalived in the guest. The kernel used IPVS NAT to deliver UDP to kind Pod IPs and translate the replies. The receipt records packets captured at kind origins and IPVS counters taken before each affected service was replaced or removed. The weighted upstream case samples 500 datagrams per attempt. It checks a 70/30 split within five percentage points and zero responses from the zero-weight Service. [Weighted routing case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-weighted-routing.go) · [Cross-namespace grant case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-reference-grant.go)

The upstream `UDPRouteNotAllowedByListeners` test body requires `TLSRoute` support in the suite. The literal UDP Core invocation otherwise reports the test as passing before its assertions. The focused upstream invocation supplied that feature and ran both assertions on each cluster. The UDPRoute reported `Accepted=False/NotAllowedByListeners` and the TLS listener reported zero attached routes. [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-not-allowed-by-listeners.go) · [Standard focused report](udp-core-evidence/standard-tls-listener-report.yaml) · [Experimental focused report](udp-core-evidence/experimental-tls-listener-report.yaml)

## Fixture scope

The local manager ran in Endpoint mode against Octavia WSGI and SQLite. The fixture supplied cloud identity, subnet and agent transport. Native HAProxy handled TLS and shared HTTPS. The UDP VIP and Pod packets traveled over a layer-3 tunnel into the guest director. Kubernetes used the released CRD bundle for each channel. Live Hibiscus reachability, Amphora provisioning, agent REST delivery, failover and NodePort forwarding remain outside these runs. [Live qualification ledger](../live-runs.md)

The qualification clusters were deleted and the QEMU guests powered down.
