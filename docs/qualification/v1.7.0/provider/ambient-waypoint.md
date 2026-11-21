# Opt-in Istio ambient mesh qualification

The [workload integration](../../../../integrations/istio-ambient/README.md) installs the pinned Istio 1.31.0 ambient profile and a Service waypoint on an operator-selected Kubernetes cluster. The platform supplies the Gateway API CRDs. Workload owners select the client and destination namespaces for ambient enrollment, then select individual Services for waypoint routing. Istio writes the Service-parent status. The independent Octavia reconcilers retain their Gateway-parent scope. [Istio release inputs](istio-waypoint-inputs.md) · [Plain kind negative control](stock-mesh.md)

## Local traffic and lifecycle

The trial used two disposable Kubernetes v1.36.4 kind clusters in a Linux amd64 KVM guest. Each run used one active two-node cluster with tmpfs-backed etcd and a separate upstream Gateway API v1.7.0 release bundle. The Standard release file contains ten CRDs. Experimental contains fourteen, including the live `XMesh` CRD. Istiod, CNI, ztunnel and the waypoint ran the pinned linux/amd64 manifests. [Gateway API v1.7.0 release](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

Two enrolled client namespaces called the selected destination's normal Service DNS. Before enrollment, each received the original backend and request header on HTTP and gRPC. After applying each producer Route, each received the second backend and the rewritten HTTP header or gRPC metadata. The neighboring Service, a direct PodIP request, and an unenrolled namespace's request to the selected Service kept the original response. Comparing the Services confirmed that their UIDs, selectors, ports, and addresses stayed the same. The waypoint used a ClusterIP.

The route was then updated to distribute 70% of requests to `echo-v1` and 30% to `echo-v2`. These are observed backend counts from 200 requests per protocol and channel.

| Bundle | HTTP `echo-v1 / echo-v2` | gRPC `echo-v1 / echo-v2` |
|---|---:|---:|
| Standard | 134 / 66 | 141 / 59 |
| Experimental | 146 / 54 | 139 / 61 |

`GatewayClass/istio-waypoint` was accepted with controller `istio.io/mesh-controller`. Istio 1.31.0 wrote the Service-parent status using its separate `istio.io/gateway-controller` Route identity. Both HTTPRoute and GRPCRoute reported `Accepted=True`, `ResolvedRefs=True` and `ResolvedWaypoints=True` at both observed generations. [Pinned Istio Route status code](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/config/kube/gateway/conditions.go#L193)

Removing each Route restored the original backend and header before the operator removed the Service label and deleted the waypoint. Requests continued to succeed after unenrolling the namespaces. The operator removed the Istio runtime releases and the dedicated GatewayClass after fixture cleanup. The base release, Istio CRDs and original Gateway API CRDs remained installed until the disposable clusters were deleted.

The initial IPv4 kind run with Istio's default IPv6 settings logged ztunnel DNS loopback bind failures. The module configures both CNI and ztunnel for IPv4. Running both clusters under load at the same time produced Kubernetes API, pod exec and fixture deletion deadlines. The final trial ran the channels separately.

## Upstream Core results

Both test processes exited successfully. Each upstream report includes the shared `MeshBasic` connectivity case in both profiles. The recording adapter counted zero cloud graph operations on each channel.

| Profile | Standard | Experimental |
|---|---:|---:|
| MESH-HTTP | 7 passed | 7 passed |
| MESH-GRPC | 3 passed | 3 passed |

The HTTP cases cover matching, host and status redirects, request headers, the same-namespace case, weighted distribution, traffic splits and basic connectivity. The gRPC cases cover request metadata, weighted distribution and basic connectivity. The trial exercised a producer Route of each kind separately on the same Service. Consumer Routes, direct ClusterIP matching, simultaneous HTTPRoute and GRPCRoute attachments, XMesh semantics and a deployed Magnum cluster remain unqualified. [Per-case results, release checksums and image digests](ambient-waypoint-results.json)

The local archive `hibiscus-istio-ambient-v1.7.0-ab748e2.tar.gz` contains the two upstream reports, wire fixtures, logs and cleanup receipts. Its SHA-256 is `1be73cf3ca8a77a670c81d8c7b1c9bc3cfce01b07c8acbea52d829982a7eed13`.

## Run the upstream Core cases

Use a fresh single-stack IPv4 Linux amd64 kind cluster for each channel and the exact v1.7.0 [upstream release bundle](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0). The local runner also needs the fork's class-configuration CRD and the upstream fixture's pinned `echo-advanced:v0.1.0` image. Mirror the published linux/amd64 manifest using the [image procedure in the stock control](stock-mesh.md#reproduce). Preload the pinned ambient component images or provide registry access to the kind nodes.

For an offline node, use the component repository and digest from `integrations/istio-ambient/*.yaml`. Copy the amd64 manifest and blobs anonymously, import the archive on each node, and add the exact digest reference used by the pod specification. Here `repository` is the full image repository, `digest` includes the `sha256:` prefix, and `node` is the disposable kind node.

```sh
rtk skopeo copy --src-no-creds --preserve-digests "docker://$repository@$digest" \
  "oci-archive:component.oci.tar:$repository:1.31.0-distroless"
rtk sudo podman exec -i "$node" ctr -n k8s.io images import --digests - < component.oci.tar
rtk sudo podman exec "$node" ctr -n k8s.io images tag \
  "$repository:1.31.0-distroless" "$repository@$digest"
```

Install the mesh components and waypoint class using the operator procedure. Leave the two upstream mesh fixture namespaces absent before the test so the suite can apply and remove its fixtures. The runner enrolls both upstream fixture namespaces in ambient mode. It places the operator's Service waypoint in `gateway-conformance-mesh` and assigns only `Service/echo` to it.

Build and run from the repository root. Set `test_binary` to an absolute build output path, `channel` to `standard` or `experimental`, `cluster` to the disposable kind name, and `kubeconfig` to the file containing context `kind-$cluster`.

```sh
rtk env CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o "$test_binary" ./internal/controller
rtk env GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_AMBIENT_WAYPOINT_MANIFEST="$PWD/integrations/istio-ambient/waypoint.yaml" \
  "$test_binary" -test.run '^TestKindAmbientMesh$' -test.v -test.timeout=12m
```

The tagged test uses the unmodified v1.7.0 test registry with the Core features of `MESH-HTTP` and `MESH-GRPC`. It starts the real fork reconcilers with a recording adapter and requires zero cloud graph operations for the mesh fixtures. The verbose log shows the upstream report per profile and channel. MeshConsumerRoute, MeshClusterIPMatching and Experimental XMesh are outside these Core selections. Report consumer-namespace isolation, direct ClusterIP behavior and XMesh separately if exercised.
