# Preserve a 257:128 split on a TLS passthrough listener

A TLSRoute sent encrypted connections to two Services using weights 257 and 128. Each Service presented its own certificate for the requested name and echoed a unique value after the handshake. The Gateway used a TCP pool for each Service. These weights exceed the range of individual Octavia members. [Operator configuration](../../../operator-octavia-tls-sni-passthrough.md#distribute-connections-among-services)

| Channel | ReferenceGrant | Primary | Minority | Closed | Sibling |
|---|---|---:|---:|---:|---:|
| Standard | Present | 363 | 149 | 0 | 32 |
| Standard | Removed | 330 | 0 | 182 | 32 |
| Standard | Restored | 336 | 176 | 0 | 32 |
| Experimental | Present | 331 | 181 | 0 | 32 |
| Experimental | Removed | 352 | 0 | 160 | 32 |
| Experimental | Restored | 338 | 174 | 0 | 32 |

Each phase opened 512 connections after reconciliation. The expected minority share was 128/385, about 170 of 512 connections. The test allowed a deviation of 43. A second SNI application on the same port received 32 additional connections per phase. The earlier Standard build returned `Programmed=False/UnsupportedValue` and closed all 24 connections to the weighted name. Its sibling served all 32.

The weighted route kept its spec, generation and weights through both ReferenceGrant changes. Its Gateway parent reported `Accepted=True` and the controller's `Programmed=True` throughout. Removing the grant changed `ResolvedRefs` to `False/RefNotPermitted`. Octavia retained weight 128 on an empty, administratively disabled TCP pool. The foreign Pod accepted zero Gateway connections in that phase. A direct connection confirmed that the Pod could still serve its certificate. Restoring the grant returned `ResolvedRefs=True` and restored that Service's share of successful handshakes. [Counts, conditions and pool readbacks](tlsroute-large-weights-evidence/receipt.json)

The local runs used the controller executable extracted from its linux/amd64 image, the stable Octavia wheel, the Kubernetes API, the Amphora renderer, HAProxy and echo Pods. The harness simulated cloud identity, network provisioning, RPC and Amphora transport. Regional Hibiscus qualification is pending. [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-passthrough-weighted-a35658dafc1e)
