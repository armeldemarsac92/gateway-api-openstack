# Mirror HTTP and gRPC traffic to a ServiceImport

An HTTPRoute sent its primary request to a core Service and used a rule-level RequestMirror addressed to a same-named Headless ServiceImport. A GRPCRoute mirrored from a backendRef filter. Each route kind had a separate grant for the import. Its two remote EndpointSlices published different ports. The applications recorded each request using its own identifier.

| Channel | HTTP import grant | Protocol | Primary replies | Copies at east | Copies at west | ResolvedRefs |
| --- | --- | --- | ---: | ---: | ---: | --- |
| Standard | Granted | HTTP | 8 | 4 | 4 | True/ResolvedRefs |
| Standard | Granted | GRPC | 8 | 4 | 4 | True/ResolvedRefs |
| Standard | Withdrawn | HTTP | 8 | 0 | 0 | False/RefNotPermitted |
| Standard | Withdrawn | GRPC | 8 | 4 | 4 | True/ResolvedRefs |
| Standard | Restored | HTTP | 8 | 4 | 4 | True/ResolvedRefs |
| Standard | Restored | GRPC | 8 | 4 | 4 | True/ResolvedRefs |
| Experimental | Granted | HTTP | 8 | 4 | 4 | True/ResolvedRefs |
| Experimental | Granted | GRPC | 8 | 4 | 4 | True/ResolvedRefs |
| Experimental | Withdrawn | HTTP | 8 | 0 | 0 | False/RefNotPermitted |
| Experimental | Withdrawn | GRPC | 8 | 4 | 4 | True/ResolvedRefs |
| Experimental | Restored | HTTP | 8 | 4 | 4 | True/ResolvedRefs |
| Experimental | Restored | GRPC | 8 | 4 | 4 | True/ResolvedRefs |

With both grants present, each request reached its primary application once and exactly one imported endpoint received a copy. The HTTP grant withdrawal removed the HTTP mirror pool from the active policy. The primary continued replying and the independently authorized gRPC copy continued. Recreating the grant restored HTTP copies without editing either route.

The signed Standard baseline returned all primary replies and recorded zero imported copies for either route. Both routes reported InvalidKind for the mirror destination.

These local trials used Kubernetes API servers, MCS objects, the controller extracted from its release image, installed Octavia and Amphora code, HAProxy and separate applications. ClusterSetIP changes were also covered in Kubernetes reconciliation tests. Regional reachability across a real ClusterSet remains open. [Route status and per-origin counts](receipt.json) · [Operator configuration](../../../../operator-multicluster-service-imports.md#mirror-httproute-and-grpcroute-requests)
