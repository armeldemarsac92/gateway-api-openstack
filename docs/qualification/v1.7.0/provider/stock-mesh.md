# Mesh negative control on plain kind

This design study runs the existing ingress reconcilers at `000d9dd6a3e64748f2b75d34f27e2ad7528749a3` on plain kind with the recording cloud adapter. It checks what ordinary clients see when a Gateway API Route names a Service as its parent. The client runs inside the official upstream echo workload and calls the original Service DNS name. The mesh setup uses the embedded upstream fixture in `gateway-conformance-mesh` and `gateway-conformance-mesh-consumer`. The runner starts the normal GatewayClass, Gateway, HTTPRoute and L4 reconcilers and supplies the combined Core feature sets of `MESH-HTTP` and `MESH-GRPC`. [Upstream profiles](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go) · [Upstream mesh fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/mesh/manifests.yaml)

## Upstream Core results

Both profiles failed on both channels. All Core test bodies ran. The upstream report includes the shared `MeshBasic` connectivity case in both profiles.

| Profile | Standard | Experimental | Failed cases on both channels |
| --- | --- | --- | --- |
| `MESH-HTTP` | 4 passed, 3 failed | 4 passed, 3 failed | `MeshHTTPRouteRedirectHostAndStatus`, `MeshHTTPRouteRequestHeaderModifier`, `MeshHTTPRouteWeight` |
| `MESH-GRPC` | 1 passed, 2 failed | 1 passed, 2 failed | `MeshGRPCRouteRequestHeaderModifier`, `MeshGRPCRouteWeight` |

The redirect requests returned 200 where the rules expected 301 or 302. For the requested 70% share to `echo-v1`, the last HTTP weight trials measured 47.6% on Standard and 48.2% on Experimental, and the last gRPC trials measured 100% on both. [Upstream mesh cases](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh)

`MeshHTTPRouteMatching` and `MeshTrafficSplit` saw wrong-backend replies before passing on both channels. For example, `/v1` in `MeshTrafficSplit` reached `echo-v2` before the expected replies arrived. `MeshHTTPRouteSimpleSameNamespace` saw 17 wrong-backend replies before passing on Standard. On Experimental, its first three replies came from the expected backend and the case passed with zero mismatches. Stock Service selection explains these passes. `MeshBasic` confirms ordinary reachability. [Mesh matching](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/httproute-matching.go) · [Mesh traffic split](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/mesh-split.go)

## Traffic and Route status

On each channel, the upstream HTTP and gRPC request-header rules asked for `X-Header-Set` to become `set-overwrites-values`. Every sampled request succeeded and the selected backend echoed the original value. The direct `echo-v2` pod request also succeeded and echoed the input value. [HTTPRoute fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/httproute-request-header-modifier.yaml) · [GRPCRoute fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/grpcroute-request-header-modifier.yaml)

Numbers show successful replies from `echo-v1` and `echo-v2`, in that order.

| Probe from the upstream `echo-v1` pod | Standard | Experimental |
| --- | ---: | ---: |
| `http://echo/` before the HTTPRoute header case | 3 / 13 | 9 / 7 |
| `http://echo/set` with the upstream header rule in place | 3 / 9 | 5 / 7 |
| `grpc://echo:7070` with the upstream metadata rule in place | 0 / 12 | 12 / 0 |
| Direct HTTP to the `echo-v2` pod on port 8080 | 0 / 1 | 0 / 1 |

The two gRPC batches each landed on a single backend. The HTTP batches reached both backends before and during the header test. The readback for the HTTPRoute naming `Service/echo:80` showed `status.parents: []` during both captured snapshots. The GRPCRoute naming `Service/echo:7070` had an empty status. The Service kept its ClusterIP and two Ready endpoints on both channels. The Service and EndpointSlice JSON were byte-identical before and during the HTTPRoute test in each cluster.

The adapter recorded zero Gateway graph operations in each run. The current HTTPRoute reconciler resolves Gateway parents and skips Service parents. Mesh support would require a controller to own Service-parent status and a data plane to enforce the rules on traffic to that Service. [HTTPRoute parent selection](../../../../internal/controller/httproute/httproute_parent.go#L54)

The evidence archive `hibiscus-stock-mesh-000d9dd6a3e6.tar.gz` contains both upstream ConformanceReport YAML files, per-case results, aggregate traffic observations and file checksums. Archive SHA-256 is `f14453abf5fae0ffb35cccacb5862445a40ccfef99d9afd976543ab35b18aa68`.

## Reproduce

Use two disposable Linux amd64 kind clusters, one per Gateway API v1.7.0 channel. The run uses Kubernetes v1.36.4 and the fork's GatewayClass configuration CRD. Leave the two upstream mesh fixture namespaces absent so the runner can create and clean up its own workloads. The embedded mesh fixture has SHA-256 `4ade870abee6cc31d35d7b5dbc6544c2fdc6851141d353ab9c23eb0352a5f301`. The official registry tag's OCI index is `sha256:fef12228b1dccb5b9eebb1f27b9f5a501355cbe3c4c5e0121bbcd0f638cd6569`. Import its pinned linux/amd64 manifest below into each node as the original tag so the embedded fixture can use its `IfNotPresent` policy. The pod status can show the local OCI archive index as its image ID. Verify that index's manifest and the exact tag's manifest against the pinned digest.

```sh
image=registry.k8s.io/gateway-api/conformance/echo-advanced
manifest=sha256:7b928723a4f261dcc94ef2cc036041d393c2ff4fe02623cc79d2f8691db995af
skopeo copy --preserve-digests "docker://$image@$manifest" "oci-archive:echo-advanced.oci.tar:$image:v0.1.0"
# For each control-plane and worker node, with the kind container runtime.
sudo podman exec -i "$node" ctr -n k8s.io images import --digests - < echo-advanced.oci.tar
sudo podman exec "$node" ctr -n k8s.io images ls
```

From the repository root, build the runner and execute it separately on each cluster. Set `channel` to `standard` or `experimental`. Set `cluster` and `kubeconfig` to identify that disposable cluster. The kubeconfig needs the context `kind-$cluster` and permissions to create fixtures and exec inside their pods.

```sh
CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o /tmp/stock-mesh.test ./internal/controller
GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_MESH_STUDY=core /tmp/stock-mesh.test -test.run '^TestKindStockMeshStudy$' -test.v -test.timeout=12m
```

The runner prints the upstream profile reports and the count of cloud graph operations. It submits the entire upstream registry to the suite, which skips cases requiring features outside the two Core profiles. The v1.7.0 suite runs feature inference before applying profile defaults, so the runner explicitly supplies the union of the two published Core feature sets. Keep the verbose output for per-request outcomes and the exit status for each channel. After the run, both mesh fixture namespaces should be absent.

To repeat the HTTP observation, capture `echo` and its EndpointSlices before the header test and while `mesh-request-header-modifier` is present. Read that Route's `status.parents`. From the upstream `echo-v1` container, run `client http://echo/set -H Some-Other-Header:val -H X-Header-Set:some-other-value --count=12`. Repeat the request against the `echo-v2` pod IP on port 8080 for the direct control.
