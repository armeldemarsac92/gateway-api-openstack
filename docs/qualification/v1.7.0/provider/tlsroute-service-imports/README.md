# Weighted TLSRoute traffic to a multicluster ServiceImport

A passthrough TLS listener received connections for a Service, an imported IPv4 ClusterSetIP, and a hostname that split connections between them with weights 3 and 1. Each origin presented its own test certificate and returned its identity inside the TLS stream.

| Channel | TLSRoute grant | Mixed route ResolvedRefs | Service echoes | Import echoes | Connections closed before an echo | Total |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Standard | Present | `True/ResolvedRefs` | 192 | 64 | 0 | 256 |
| Standard | Removed | `False/RefNotPermitted` | 192 | 0 | 64 | 256 |
| Standard | Restored | `True/ResolvedRefs` | 192 | 64 | 0 | 256 |
| Experimental | Present | `True/ResolvedRefs` | 192 | 64 | 0 | 256 |
| Experimental | Removed | `False/RefNotPermitted` | 199 | 0 | 57 | 256 |
| Experimental | Restored | `True/ResolvedRefs` | 192 | 64 | 0 | 256 |

When the grant was present, Octavia reported the Service and import members with weights 3 and 1. Withdrawal kept the Service member and set the pool to reject one connection in four. The import origin accepted zero TCP connections during the probes after withdrawal converged. After restoration, Octavia reported both weighted members again.

All four TLSRoutes kept their UID, generation and spec. The mixed route stayed accepted and programmed throughout the cycle. During withdrawal, the route that pointed only to the import reported `ResolvedRefs=False/RefNotPermitted` and the controller’s `Programmed=False/RefNotPermitted`. Its 16 probes closed. The separate Service hostname and the wildcard hostname each returned 16 Service echoes per phase. An unmatched hostname closed all four probes per phase.

Two earlier Experimental runs with the same image reached their 95-second deadline while waiting for the grant withdrawal to converge. Neither run sampled withdrawal traffic. At the second deadline Octavia already reported four active pools, an empty pool and the requested one-in-four rejection on the mixed pool. The Gateway still reported that it was withdrawing an obsolete TLS SNI resource. The receipt preserves both runs.

The follow-up Experimental run first observed the requested active HAProxy and Octavia state at 81.947 s after the grant deletion returned. It first observed both that state and the expected Kubernetes conditions at 92.416 s, within the 95 s threshold. Its runner exit was 0. The table reports connections sampled after convergence.

The preceding signed HTTP/gRPC ServiceImport controller reported `InvalidKind` for the TLS import on Standard. The dedicated import hostname closed all 16 probes while its origin passed the independent liveness check.

The completed Standard and Experimental runs used Kubernetes API servers, the MCS v0.5.2 ServiceImport CRD, the manager from the signed-source release image, installed Octavia and Amphora code, and a running HAProxy. Separate local addresses represented the Service endpoint and the imported VIP. Fixtures supplied the cloud and agent transport. The measured traffic used TLS passthrough with TCP connection rejection. Terminating listeners, regional MCS networking and failover across real clusters require separate traffic evidence. [Per-phase route conditions, SNI pools and TLS origin counts](receipt.json) · [Operator configuration](../../../../operator-multicluster-service-imports.md)
