# Gateway HTTP and gRPC traffic to an IPv4 ClusterSetIP

An HTTPRoute sent traffic to a Kubernetes Service and a same-named ServiceImport in another namespace. The Service had weight 3 and the import had weight 1. The import used the VIP and TCP port published by the MCS provider. A separate GRPCRoute used the import’s h2c port with its own ReferenceGrant.

| Channel | HTTP grant | ResolvedRefs | HTTP 200 from Service | HTTP 200 from import | HTTP 500 with zero origin requests | Total on the weighted path |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Standard | Present | `True/ResolvedRefs` | 190 | 66 | 0 | 256 |
| Standard | Removed | `False/RefNotPermitted` | 186 | 0 | 70 | 256 |
| Standard | Restored | `True/ResolvedRefs` | 199 | 57 | 0 | 256 |
| Experimental | Present | `True/ResolvedRefs` | 204 | 52 | 0 | 256 |
| Experimental | Removed | `False/RefNotPermitted` | 202 | 0 | 54 | 256 |
| Experimental | Restored | `True/ResolvedRefs` | 182 | 74 | 0 | 256 |

Octavia returned the same target weights at each step. Removing the HTTP grant replaced only the import’s target with a fixed 500 of weight 1. Restoring the grant returned that weight to the import. The HTTP and gRPC Routes kept their UID and generation. The controller reported `Accepted=True` and `Programmed=True` before the traffic probes in each phase.

The dedicated Service path returned 200 throughout. The dedicated import path returned 200, then 500 with zero origin requests, then 200. The gRPC origin received each call over h2c and returned OK throughout the HTTP grant cycle. On the previous controller, the Standard HTTPRoute reported `InvalidKind` and its dedicated import path returned 500.

The local trial used the Kubernetes API server, the v0.5.2 ServiceImport CRD, the manager extracted from its linux/amd64 image, installed Octavia and Amphora code, and a running HAProxy. The ClusterSetIP and the ordinary Service endpoint had separate addresses bound in the test network. Fixtures supplied cloud identity, Neutron, worker delivery, agent transport and service control. Multi-cluster endpoint discovery and reachability from a deployed Amphora still need regional qualification. [Route status, Octavia weights and application responses](receipt.json) · [Operator configuration](../../../../operator-multicluster-service-imports.md)
