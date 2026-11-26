# ServiceImport parents on an Istio ambient waypoint

An enrolled client called two distinct VIPs with the same Host header. One belonged to an imported service, the other to a same-named local Service. The producer’s HTTPRoute attached to the import on port 80 and sent the exact path `/redirect` to a third echo application. The remaining paths targeted the imported endpoint.

| Channel | Pilot | Before the route | Import /redirect | Import /default | Same-named Service /redirect | Import after route deletion |
| --- | --- | --- | --- | --- | --- | --- |
| Standard | Consumer pilot | import-origin (2/2) | import-origin (2/2) | import-origin (2/2) | core-origin (2/2) | — |
| Standard | ServiceImport pilot | import-origin (2/2) | override-origin (2/2) | import-origin (2/2) | core-origin (2/2) | import-origin (2/2) |
| Experimental | Consumer pilot | import-origin (2/2) | import-origin (2/2) | import-origin (2/2) | core-origin (2/2) | — |
| Experimental | ServiceImport pilot | import-origin (2/2) | override-origin (2/2) | import-origin (2/2) | core-origin (2/2) | import-origin (2/2) |

Both runs used the same controller projection. With the consumer pilot the import forwarded to its original endpoint. With the ServiceImport pilot, Istio published Accepted=True and ResolvedRefs=True for the import parent. Deleting the route restored its default destination. Removing the only provider slice withdrew the imported destination, and recreating the slice restored it. Removing the enrollment annotation deleted the owned ServiceEntry. The same-named core Service remained reachable after slice deletion and after opt-out.

The local kind clusters ran Istio 1.31.0, its waypoint and ztunnel, the signed controller and separate echo applications. The MCS objects, Keystone catalog and OpenStack service version responses were local fixtures. These measurements cover one mesh network, a producer route, one IPv4 ClusterSetIP and one HTTP port. Regional reachability remains open. [Response observations, route conditions and image digests](receipt.json) · [Operator setup](../../../../operator-multicluster-service-imports.md#use-a-serviceimport-as-a-mesh-route-parent) · [Pilot release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-istio-82213f2b4cf5)
