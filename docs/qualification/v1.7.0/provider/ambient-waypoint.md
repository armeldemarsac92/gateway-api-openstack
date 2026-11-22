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

The HTTP cases cover matching, host and status redirects, request headers, the same-namespace case, weighted distribution, traffic splits and basic connectivity. The gRPC cases cover request metadata, weighted distribution and basic connectivity. The trial exercised a producer Route of each kind separately on the same Service. [Per-case results, release checksums and image digests](ambient-waypoint-results.json)

The published archive contains the upstream reports, traffic fixtures and counts, runtime inventory, and cleanup receipts from the two clusters. It also includes the Kubernetes API check that namespace unenrollment preserves a later operator assignment. Its SHA-256 is `efef12b5ccfc3cce6c427d8f6c493cfadbc585a3e900ebcdf957d79df8b662ea`. [Download the local Core profile evidence](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-mesh-v1.7.0-4c08237/hibiscus-ambient-core-4c08237.tar.gz)

## Extended results

Both Gateway API v1.7.0 bundles produced the same results on separate disposable IPv4 kind clusters. Each upstream process ran 23 unique registered mesh cases and exited 1 because `MeshConsumerRoute` failed. The fork recorded zero cloud graph operations in both runs. [Per-case outcomes, profile counts and receipt checksums](ambient-waypoint-extended-results.json)

| Scope | Standard | Experimental |
|---|---:|---:|
| All registered mesh cases | 22 passed, 1 failed | 22 passed, 1 failed |
| MESH-HTTP Core in the upstream report | 7 passed | 7 passed |
| MESH-HTTP Extended in the upstream report | 7 passed | 7 passed |
| MESH-GRPC Core in the upstream report | 3 passed | 3 passed |

The upstream report counts `MeshBasic` in both profiles. It omits seven cases that require shared HTTP features outside the mesh profile feature sets, including the failing consumer case. The report's Extended feature list records the features selected for the run.

Two enrolled clients made normal HTTP requests from the producer and consumer namespaces to the fixture Service. The selected Services and the waypoint all used ClusterIP. The direct checks exercised the same upstream HTTPRoute manifests after the upstream suite had removed its fixtures.

`MeshFrontendHostname` targets the resolved Service with a different HTTP `Host`. The direct checks also passed the literal `echo-v2` ClusterIP to the client. Both a literal IP `Host` and an `echo-v1` `Host` returned HTTP 200 from `echo-v2` with `X-Header-Set: set`. A request to the neighboring `echo-v1` ClusterIP with an `echo-v2` `Host` returned HTTP 200 from `echo-v1` without that header. The direct `echo-v2` PodIP also returned HTTP 200 without the header. Istio builds a waypoint route for the Service VIP with a wildcard virtual host for a normal Service. [Upstream hostname test](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/mesh-frontend-hostname.go) · [Pinned Istio waypoint route builder](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/networking/core/listener_waypoint.go#L1111-L1159)

`MeshConsumerRoute` expects a client in the Route namespace to receive a response header. Istio reported `Accepted=True`, `ResolvedRefs=True` and `ResolvedWaypoints=True` at generation 1. Clients from both namespaces received the original HTTP 200 without the header through both Service DNS and the literal ClusterIP. A separate diagnostic with a narrow `ReferenceGrant` to `Service/echo-v1` produced the same result. Istio compiles the Route into a VirtualService in the Route's namespace. The Service waypoint selects routing config from the Service's namespace or its own namespace. Both are the producer namespace in this setup, so the generated waypoint config had no consumer route. Istio's own ambient suite excludes this feature. [Upstream consumer test](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/mesh-consumer-route.go) · [VirtualService namespace](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/config/kube/gateway/route_collections.go#L185-L205) · [Waypoint route selection](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/networking/core/listener_waypoint.go#L1163-L1180) · [Istio ambient suite selection](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/tests/integration/ambient/gateway_conformance_test.go#L100-L120)

The operator removed both fixture namespaces and the three Istio runtime releases on each cluster. The base release and original CRDs remained installed until the clusters were deleted. The Experimental bundle included the live XMesh CRD. XMesh semantics, simultaneous HTTPRoute and GRPCRoute attachments, and a deployed Magnum cluster remain unqualified.

## HTTPRoutes owned by a client namespace

With the operator's `--consumer-routes` option, the Service waypoint uses the caller's authenticated namespace when applying a consumer HTTPRoute. Each channel passed all 23 original Gateway API v1.7.0 mesh cases on its own disposable IPv4 kind cluster. In `MeshConsumerRoute` the client in the route namespace received the configured header and the client in the producer namespace received its normal response. The Gateway controller recorded zero cloud graph operations for these cases. The stock Istio 1.31.0 results above remain available for comparison. [Per-case and traffic results](ambient-consumer-results.json) · [Installation and supported filters](../../../../integrations/istio-ambient/README.md#enable-httproutes-owned-by-a-client-namespace)

The lifecycle checks passed all 19 requests per channel, including route creation, modification and removal, selection of a port and the effect of the producer’s access policy. Further checks passed nine requests on Standard and eight on Experimental. Experimental admission rejected one fixture before traffic. The receipt records the admission result and route conditions in both runs. [Detailed observations](ambient-consumer-results.json)

Both channels stored two differently cased spellings of the same header in their original order. The first exact value activated the consumer route. A request matching only the second value returned the same HTTP 404 as the single-exact control, including when the second entry contained a regex. A standalone regex produced `Accepted=False/UnsupportedValue` and the producer answered HTTP 200. The first supplemental run had expected HTTP 200 for a miss. The corrected check compared the miss with the single-exact control in a fresh VM per channel. All seven requests per channel passed. Both the original exit and the replay are in the receipt. [Receipt](ambient-consumer-results.json)

The local runs loaded the Hibiscus Istio pilot at revision `953e8af367342ea0b230f6f98e99f919aa82372e`. The observed archive index was `sha256:405f70c7e95d72ed6d7c1a06ea37ec3cf293b70038cbf090c96f2192ed814127` and its linux/amd64 manifest was `sha256:fdb9d02e2b427f81d2763fce329361ac32432080cc14b7115315bed4508bbcc5`. The CNI, ztunnel and waypoint proxy used the pinned Istio 1.31.0 images. The tested scope is an enrolled single-network local cluster. Deployment on Magnum and clauses without a registered case, including XMesh behavior, remain open. [Source, image and replay inputs](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-istio-953e8af36734)

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

The tagged test uses the unmodified v1.7.0 test registry with the Core features of `MESH-HTTP` and `MESH-GRPC`. It starts the real fork reconcilers with a recording adapter and requires zero cloud graph operations for the mesh fixtures. The verbose log shows the upstream report per profile and channel.

## Run the upstream Extended cases

Set `HIBISCUS_AMBIENT_MESH_EXTENDED=1` on the same test command and allow `-test.timeout=15m`. This selects every registered mesh case in v1.7.0 and also binds `Service/echo-v1` and `Service/echo-v2` to the same Service waypoint in the fixture. The runner uses the original test manifests and assertions. The test process exits nonzero if any selected case fails, and its output includes the result of each case.

Use the per-case outcomes alongside the `ambient-extended-study` profile report. The v1.7.0 reporter includes a test in a profile only when all of its required features belong to that profile. [Upstream profile feature sets and matching](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go)
