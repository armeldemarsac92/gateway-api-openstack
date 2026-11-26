# Native GRPCRoute traffic to XBackend endpoints

An Experimental GRPCRoute matched an exact gRPC method and distributed protobuf calls between a core Service and an EndpointSelector XBackend with weights 3 and 1. Both destinations used h2c. A separate method used a second XBackend with a private server CA, hostname verification and HTTP/2 over TLS.

| Selected Pod | Service replies | XBackend replies | UNAVAILABLE | Calls |
| --- | ---: | ---: | ---: | ---: |
| Ready and selected | 114 | 46 | 0 | 160 |
| Selector label removed | 112 | 0 | 48 | 160 |
| Selector label restored | 112 | 48 | 0 | 160 |

The Pod kept the same IP and stayed Ready. With its label removed, the weighted Octavia policy kept the XBackend's share on an empty pool. Its application received zero RPCs during that sample. Restoring the label restored the member. The GRPCRoutes kept their UID, spec and positive attachment and resolution conditions. The core Service control and the authenticated method each returned their original message in every phase. The secure origin reported TLS and the active Octavia pool required the expected server identity and strict h2 ALPN.

The initial restoration trial returned 116 Service replies and 44 UNAVAILABLE responses, with zero arrivals at the restored XBackend. Octavia already listed the member as active. A separate run with the inherited health monitor, configured for 3,600-second intervals and ten successful checks, found the replacement HAProxy worker still DOWN and awaiting its first check. Fresh gRPC connections reproduced the failure while the origin answered directly.

For the table above, the test Gateway used 2-second TCP checks and required two successes for recovery. After restoring the selector, Octavia listed the active member at 3.375 seconds. The first confirmed XBackend RPC arrived at 5.693 seconds and a second probe confirmed recovery at 10.518 seconds. The worker reported UP and L4OK before and after the final batch. An earlier short-monitor run ended with the mixed route still reporting Programmed=False/Pending after withdrawal. The receipt includes both earlier runs.

On Standard, the API server had no XBackend CRD. Two exact methods shared a single GRPCRoute. The Service method returned its protobuf message. The method with an unknown XBackend returned gRPC UNAVAILABLE with zero application visits. The route reported Accepted=True, ResolvedRefs=False/InvalidKind and Programmed=True.

With the preceding test image, the Experimental mixed route reported InvalidKind. It sent 120 calls to the Service and returned UNAVAILABLE on 40 calls. The secure method returned UNIMPLEMENTED and the ordinary Standard Service control replied.

These local trials used Kubernetes API servers, the controller extracted from its OCI image, installed Octavia REST and Amphora agent code, HAProxy processes and independent gRPC servers. Local adapters supplied cloud services and service control. The secure traffic used server authentication. Client-certificate lifecycle was exercised in controller and provider tests. Regional connectivity remains open. [Conditions, pool configuration and origin counts](receipt.json) · [Operator configuration](../../../../reference/class-configuration.md#forward-a-grpcroute-to-an-xbackend)
