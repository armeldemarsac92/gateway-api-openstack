# Client-owned HTTPRoutes and GRPCRoutes on an ambient Service

An operator can turn on HTTP request-header changes and native gRPC metadata routing independently. The Service waypoint selects routes by the authenticated caller namespace. Both options use the same [Pilot release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-istio-329c846d3de8).

## Operator-installed waypoint traffic

| CRDs | HTTP probe options | Native gRPC probe options |
| --- | --- | --- |
| Standard | `--consumer-routes` | `--grpc-consumer-routes` |
| Experimental | `--consumer-routes --service-import-parents --grpc-consumer-routes` | `--service-import-parents --grpc-consumer-routes` |

On both channels, the client HTTPRoute forwarded directly and then changed the request headers reaching the backend. The backend received the literal `cart-25%/%REQ(X-Example)%`. The peer namespace kept its original behavior. The Service owner's authorization policy denied the changed request with HTTP 403. Deleting the client route restored the original request, including while the policy remained installed. The HTTP trial used an Istio ambient waypoint. The fixture verified and staged the signed manager on the host.

For the native gRPC probes, the signed manager from the OCI archive ran as a process in the guest VM, outside the kind cluster. It used a simulated OpenStack service catalog. Two client namespaces called the same Service and method and received distinct initial metadata. Removing the first GRPCRoute restored its HTTPRoute fallback. Recreating it restored its gRPC metadata. The other client kept its gRPC route. Standard used a 5,632 MiB guest and removed the gRPC flag from the Pilot Deployment on the same image. An earlier 4,992 MiB Standard attempt recorded 20 successful RPCs across the route changes. The replacement Istiod Pod remained Pending without an assigned node during the option change. Experimental used a 4,992 MiB guest. Experimental exercised ServiceImport HTTP on the same Pilot Pod as the gRPC probe, including a same-named Service with an independent backend. [Per-channel wire observations, route conditions and image digests](mesh-http-grpc-service-consumers-329c846d3de8.json)

## Experimental XMesh rollout

The signed manager kept the same XMesh accepted on a local Kubernetes v1.36.2 API server while the Pilot changed from the previous ServiceImport image to the shared image. For the gRPC and ServiceImport plus gRPC configurations, the XMesh withdrew all 16 capabilities with `Programmed=Unknown/Pending`, then restored the same 16 once the simulated workload statuses reported the current generation ready. The fixture supplied Deployment, DaemonSet and GatewayClass status. [Rollout and XMesh observations](xmesh-reporting.json)

## Registered Gateway API v1.7.0 mesh cases

| CRDs | Passed | Failed | Skipped | Incomplete | Unrun | Suite / guest exit | Go run and fixture | Upstream report |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Standard | 23 | 0 | 0 | 0 | 0 | 0 / 0 | PASS | [YAML](mesh-standard-profiles.yaml) |
| Experimental | 23 | 0 | 0 | 0 | 0 | 0 / 0 | PASS | [YAML](mesh-experimental-profiles.yaml) |

| CRDs | Profile | Tier | Upstream verdict | Passed | Failed | Skipped | Eligible | Comparison with Go |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| Standard | MESH-HTTP | Core | success | 7 | 0 | 0 | 7 | Matches |
| Standard | MESH-HTTP | Extended | success | 7 | 0 | 0 | 7 | Matches |
| Standard | MESH-GRPC | Core | success | 3 | 0 | 0 | 3 | Matches |
| Standard | MESH-GRPC | Extended | no eligible cases | — | — | — | 0 | — |
| Experimental | MESH-HTTP | Core | success | 7 | 0 | 0 | 7 | Matches |
| Experimental | MESH-HTTP | Extended | success | 7 | 0 | 0 | 7 | Matches |
| Experimental | MESH-GRPC | Core | success | 3 | 0 | 0 | 3 | Matches |
| Experimental | MESH-GRPC | Extended | no eligible cases | — | — | — | 0 | — |

The suite used the native gRPC installer options shown above. The Go table counts the original registered cases once per channel. The upstream profile tables cover 16 unique cases and include `MeshBasic` in both Core profiles. Seven of the 23 registered Go cases require features outside the two profile selections, including `MeshConsumerRoute`. The MESH-GRPC Extended tier has no eligible cases. Upstream reports `partial` when a tier has skips and zero failures, and `failure` when a tier has any test failure. An omitted tier with eligible cases is marked `not reported`. The test binary embeds the signed source's reconcilers with a recording provider. Mesh requests use a real waypoint. [Per-case outcomes, upstream failure and skip names, and runtime checks](registered-mesh.json) · [Gateway API v1.7.0 case selection](../mesh-registered-supplement.md)

The dataplane trials ran in disposable two-node kind clusters with single-stack IPv4 on one network. Regional Magnum reachability, real cloud provider operations and multi-network traffic require separate verification. [Installation and enrollment](../../../../../integrations/istio-ambient/README.md) · [Regional ledger](../../live-runs.md) · [Release receipt](receipt.json)
