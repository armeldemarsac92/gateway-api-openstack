# Routing to Headless ServiceImport endpoints

An HTTPRoute split traffic between a core Service and a same-named Headless ServiceImport with weights 3 and 1. The import had two IPv4 EndpointSlices from different source clusters. Each slice published a different destination port for the same named HTTP port.

| Channel | Import state | HTTP route ResolvedRefs | Direct import east / west / HTTP 500 | Mixed route Service / east / west / HTTP 500 |
| --- | --- | --- | --- | --- |
| Standard | Grant present | `True/ResolvedRefs` | 8 / 8 / 0 | 46 / 9 / 9 / 0 |
| Standard | West endpoint unready | `True/ResolvedRefs` | 16 / 0 / 0 | Not sampled |
| Standard | West endpoint ready again | `True/ResolvedRefs` | 8 / 8 / 0 | Not sampled |
| Standard | HTTP grant removed | `False/RefNotPermitted` | 0 / 0 / 4 | 45 / 0 / 0 / 19 |
| Standard | HTTP grant restored | `True/ResolvedRefs` | 8 / 8 / 0 | 51 / 7 / 6 / 0 |
| Experimental | Grant present | `True/ResolvedRefs` | 8 / 8 / 0 | 57 / 4 / 3 / 0 |
| Experimental | West endpoint unready | `True/ResolvedRefs` | 16 / 0 / 0 | Not sampled |
| Experimental | West endpoint ready again | `True/ResolvedRefs` | 8 / 8 / 0 | Not sampled |
| Experimental | HTTP grant removed | `False/RefNotPermitted` | 0 / 0 / 4 | 45 / 0 / 0 / 19 |
| Experimental | HTTP grant restored | `True/ResolvedRefs` | 8 / 8 / 0 | 43 / 11 / 10 / 0 |

Each phase also sent two direct requests to the core Service. Both reached its local origin. The imported VIP field was empty. Octavia used the address and actual destination port from each eligible slice. When the HTTP grant was removed, Octavia preserved the import weight with a fixed HTTP 500 and the sampled requests made zero visits to either imported HTTP origin. Each channel also made one native GRPCRoute call under its own grant and received an imported origin’s reply over h2c.

## TLS when the last imported endpoint disappears

The passthrough TLSRoute used an exact SNI name and weights 3 and 1. Removing the only imported EndpointSlice kept the ReferenceGrant in place. Octavia then programmed separate weighted SNI pools for this hostname. The Service pool stayed enabled and the empty pool closed the import’s share. The fixture advertised the weighted SNI pools capability and disabled single-pool TCP rejection.

| Channel | Imported EndpointSlice | TLS route ResolvedRefs | Service echoes | Import echoes | Closed connections |
| --- | --- | --- | ---: | ---: | ---: |
| Standard | Ready | `True/ResolvedRefs` | 192 | 64 | 0 |
| Standard | Removed | `True/ResolvedRefs` | 196 | 0 | 60 |
| Standard | Restored | `True/ResolvedRefs` | 192 | 64 | 0 |
| Experimental | Ready | `True/ResolvedRefs` | 192 | 64 | 0 |
| Experimental | Removed | `True/ResolvedRefs` | 196 | 0 | 60 |
| Experimental | Restored | `True/ResolvedRefs` | 192 | 64 | 0 |

The TLSRoute kept its UID and spec. The withdrawn origin still answered the independent direct probe and accepted zero Gateway connections during the empty-slice sample. After the slice was recreated, both origin addresses returned to the active pool.

The preceding controller reported `ResolvedRefs=False/UnsupportedValue` for the Headless reference in both Standard baseline fixtures. The HTTP baseline returned fixed 500 responses for the import. The TLS baseline closed its assigned share.

The runs used Kubernetes API servers, MCS API v0.5.2 objects, the controller extracted from its release image, installed OpenStack REST and Amphora code, and running HAProxy processes. The fixtures supplied local network addresses and OpenStack service adapters. Probes started after route and provider convergence. Regional MCS reachability, multiple deployed clusters and terminating TLS listeners require separate traffic evidence. [Route conditions, Octavia readbacks and origin counts](receipt.json) · [Operator configuration](../../../../operator-multicluster-service-imports.md)
