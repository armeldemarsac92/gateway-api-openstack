# UDP from a private VIP to a Kubernetes worker

A client sent a unique UDP payload to `192.0.2.30:18053` and received the original request and pod identity `hibiscus-worker-udp/udp-echo` in the reply from that VIP. The local rehearsal used a two-node kind v0.33.0 cluster running Kubernetes v1.36.4 with the Standard Gateway API v1.7.0 bundle. The echo pod ran on the dedicated worker. The Service used `externalTrafficPolicy: Local` and an allocated UDP NodePort. [Operator configuration](../../../getting-started.md#forward-udp-to-a-service)

| Observation | Result in the first rehearsal |
| --- | --- |
| Kubernetes | The Ready EndpointSlice identified `10.244.1.2:8080` on worker `10.89.0.2`. Kubernetes allocated UDP NodePort `31716`. The control plane had a different address. The route reported `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True`. |
| Octavia | The production graph writer and Gophercloud adapter submitted the live Kubernetes selection to Octavia 19.0.0. Its public API returned one member at `10.89.0.2:31716`. The stock Amphora worker rendered the attached and detached configurations from its database. |
| Packets | Kernel IPVS installed one UDP `wrr` service with a single NAT destination. A packet capture on the director’s backend interface showed the request and reply at the worker NodePort. The stock `UDP-CONNECT` checker sent a one-byte probe and reported success. |
| Withdrawal | Deleting the UDPRoute cleared the listener's default pool. After replaying the captured detach, a new VIP request returned `ECONNREFUSED`. A direct request to the worker still reached the same pod. Deleting the Gateway restored the initial Octavia project inventory of two control load balancers and their six children. |

The GatewayClass, Gateway and UDPRoute reconcilers ran in a Go test process against the guest’s real Kubernetes API. The harness supplied cloud topology and Neutron responses and simulated worker messaging. It captured the Amphora REST upload and replayed the same file bytes in an isolated Linux director. The Fedora 45 guest ran Linux 7.2.11, Keepalived 2.3.4 and Podman 6.1.3. Keepalived reported the stock template’s script-security warning and its checker reached the worker. The run ended with both kind nodes deleted and the guest powered off. [Stock adapter fixture](stock-udp.md) · [Kubernetes status cases on both channels](udp-kind.md)

Live Amphora provisioning, REST delivery to a deployed agent, Neutron enforcement and a complete GATEWAY-UDP conformance run still require a disposable Hibiscus deployment.

## Reproduction

Use a fresh two-node kind cluster and the stock Octavia Python environment from the provider fixture. Record the UDP Service’s allocated NodePort, the worker address and the pod’s EndpointSlice before submitting the route. Capture the worker-generated configuration on the Octavia side and compare its SHA-256 with the guest copy before starting Keepalived. Send a unique payload from a separate namespace, capture the request and reply at the worker NodePort, delete the route, and repeat the VIP and direct worker requests. The development prerelease identifies the controller source and OCI image for this qualification. [Octavia fixture setup](stock-udp.md#reproduce-the-public-api-and-configuration-check) · [Development prerelease](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-3819e699441970e51d7e546582dd53a827e6a320)
