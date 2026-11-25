# Gateway API v1.7.0 TCP Core on kind

The upstream v1.7.0 registry defines `GATEWAY-TCP` Core with `Gateway`, `ReferenceGrant` and `TCPRoute`. Its Core selection contains 12 shared Gateway cases and eight TCPRoute cases. The official Standard bundle serves `TCPRoute` at `v1`. The official Experimental bundle serves `v1` and `v1alpha2`. Both store `TCPRoute` at `v1`. [Upstream profile registry](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go#L115) · [Case index](tcp-core-evidence/case-index.json)

The campaign used the published Hibiscus controller `9388be4580dd0fac002e160c3433d65cf1665a86`. Each bundle ran in its own disposable kind cluster. The original upstream CLI applied the complete shared base, including the HTTPS Gateway. The fixture enrolled the CLI-generated frontend Secret revisions in its local Barbican and enabled shared HTTPS listener support. The CLI used its original case deadlines and the full Core selection with default provisional-test behavior.

| Bundle | Passed | Failed | Skipped | Upstream report |
|---|---:|---:|---:|---|
| Standard | 20 | 0 | 0 | [Original YAML](tcp-core-evidence/standard-report.yaml) |
| Experimental | 20 | 0 | 0 | [Original YAML](tcp-core-evidence/experimental-report.yaml) |

At setup, `GatewayClass.status.supportedFeatures` held an empty list on Standard and `GatewayAddressRoutability` on Experimental. The CLI received the three upstream Core feature names through its `-supported-features` option.

## TCP traffic and status

On both channels, the upstream client exchanged TCP echo bytes with the selected kind Pod. It verified oldest-route selection, cross-namespace forwarding, attachment to all four listeners, and port and section-name selection on three individual listeners. After deleting the cross-namespace grant, the case checked `ResolvedRefs=False` with `RefNotPermitted`. The weighted case sampled 500 new connections per distribution attempt. It verified the 70/30 split within five percentage points with zero responses from the zero-weight Service. The case logs give the verdict and the fixture receipt records Octavia pool member weights.

The three invalid-route cases covered the missing Service, the unauthorized cross-namespace Service, and a TCPRoute targeting a non-TCP listener. The fixture observed installed TCPRoute versions in live Kubernetes discovery.

On both channels, the weighted TCP listener on port 9300 used two active endpoint members on port 3000 with weights 7 and 3.

On both channels, HAProxy activated a configuration that detached the pool from the cross-namespace listener on port 9320 while the Octavia pool and member were still present.

## Fixture and scope

The upstream client opened TCP connections to the private fixture VIP. Native HAProxy forwarded to the actual kind Pod endpoints over a routed TUN link carried by SSH. The published manager wrote to the local Octavia API. The installed Amphora renderer rendered the HAProxy configuration, and a fixture callback activated it. Octavia used SQLite and local services for Barbican and the required cloud APIs. The receipt includes the pinned artifacts, API discovery, recorded HAProxy activations and ports, echo image identities, and per-case outcomes. [Campaign receipt](tcp-core-evidence/receipt.json)

The run covers local Endpoint mode. The deployed Hibiscus profile, private Amphora VIP reachability, worker NodePort forwarding and Amphora failover require live evidence. [Deployed qualification ledger](../live-runs.md)

Both kind clusters were deleted and the disposable QEMU guest powered down.
