# Consumer HTTPRoutes on an imported mesh Service

Two enrolled client namespaces called the same imported IPv4 VIP and path. Each installed an HTTPRoute pointing directly to the ServiceImport and setting a response header. The waypoint selected the rules from the authenticated caller's namespace. [Pilot, controller, route conditions and HTTP observations](receipt.json)

| Consumer routes | First client | Second client | Same-named Kubernetes Service |
| --- | --- | --- | --- |
| Both installed | Import backend, header `cart` | Import backend, header `storefront` | Core Service backend |
| First route removed | Producer override backend, no consumer header | Import backend, header `storefront` | Core Service backend |
| First route recreated | Import backend, header `cart` | Import backend, header `storefront` | Core Service backend |

On each channel, each table entry represents two consecutive HTTP 200 responses. The recreated route had a new UID. Istio reported the accepted routes and resolved backend and waypoint references at their current generations. The earlier producer-only pilot rejected the two consumer routes on Standard, and both clients reached the producer override backend.

The local environments ran kind, ambient ztunnel, a Service waypoint and application Pods. A reachable application endpoint on the same network represented the imported workload. Multi-network delivery and deployed Hibiscus regions remain to be qualified. [Pilot release and signed sources](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-istio-2ec0ff5eab83) · [Operator installation](../../../../../integrations/istio-ambient/README.md#mesh-serviceimport-opt-in)

## Experimental XMesh reporting

An earlier reporting controller kept XMesh pending and listed zero features with the previously released ServiceImport pilot. The installer now selects the consumer ServiceImport pilot by digest. The controller executable extracted from this release image reported the same 16 capabilities with the earlier pilot ready and with the current pilot ready. During the upgrade it kept the same XMesh accepted, set Programmed to Unknown with reason Pending, and withdrew all 16 until istiod reported its new generation. [XMesh conditions, pilot generations and executable digest](xmesh-reporting.json)

The waypoint traffic trials used the projection controller at `83c1506b09b1`. The reporting test ran the release executable against Kubernetes v1.36.2 Experimental and etcd. The fixture supplied GatewayClass and workload rollout status. A separate controller test covered the stock, Service consumer and both ServiceImport pilots. [Controller image](../../image.json) · [Reporting setup](../../../../../integrations/istio-ambient/README.md#report-mesh-capabilities-on-experimental)
