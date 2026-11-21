# Gateway API v1.7.0 local profile attempts

One local Gateway suite and one local mesh suite ran per release channel on plain kind. Both used the controller's GatewayClass, Gateway, HTTPRoute, TCPRoute and UDPRoute reconcilers with an in-memory cloud adapter. The adapter records graph requests and returns synthetic provider responses. The mesh suite sent real requests through Kubernetes Services from the upstream echo pods. Real-provider and production qualification remains **0 of 14 profile/channel pairs**. [Gateway runner](../../../internal/controller/kind_gateway_profile_test.go) · [Mesh runner](../../../internal/controller/kind_mesh_study_test.go)

The two Go modules, feature and case registries, case bodies and embedded manifests came from upstream v1.7.0 at `e26d183dbbc1bb782b78addbd26faba03955c152`. Both runners gave the suite the original registry, default per-case deadlines, cleanup and sequential case execution. Each suite selected the union of the declared Core and channel-applicable Extended features. The Standard channel admits Standard Extended features. Experimental admits both Standard and Experimental Extended features. The runner logs the individual profile selections. [Upstream profiles](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go) · [Feature registry](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/pkg/features) · [Case registry](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests) · [Timeout defaults](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/config/timeout.go)

## Gateway

The five profile names shared one original upstream Gateway setup per channel. The counters below give the number of cases whose required features the profile or combined selection covers. Shared cases occur in several profile columns. `HTTPRouteDisallowedKind` is eligible through the combined selection alone. The complete per-profile feature lists and channel assignments are in the linked record. [Machine-readable results](complete-local-profiles-results.json)

| Channel | Selected features | HTTP | gRPC | TLS | TCP | UDP | Eligible unique cases | Case bodies entered |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Standard | 47 | 103 | 30 | 39 | 31 | 32 | 144 | 0 |
| Experimental | 52 | 104 | 30 | 39 | 31 | 32 | 145 | 0 |

The original 300-second shared readiness deadline expired on both channels. The last samples showed 23/23 Ready pods and three accepted, programmed Gateways. `same-namespace-with-https-listener` stayed at generation 1 with `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`. The Gateway status message reported that listeners sharing a port are unsupported. The four HTTPS:443 listeners use upstream TLS termination fixtures. Each had `Accepted=False/UnsupportedProtocol` and `Programmed=False/Invalid` for generation 1. The unsuccessful setup prevented all case bodies from running and produced no Gateway `ConformanceReport`. Both runs drained their class, class configuration and fixture namespaces. [Upstream Gateway fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml) · [Readiness check](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/kubernetes/helpers.go#L272) · [Listener validation](../../../internal/controller/gateway/gateway_validation.go#L263)

The Standard binary ran from `83644649b204d082bb6ea0bf15c5909dcd39e6d8`. That binary left the static-address case's address pools empty. Experimental used `e36efaa7605842f885c7463f8d09569ec077a218`, which supplies the recording adapter's synthetic VIP and an unavailable test-network IP. In a separate fixture check the v1.7.0 Applier produced the invalid address type, the unavailable IP and the adapter's VIP in upstream assertion order. The `GatewayStaticAddresses` case was unrun, so this verifies fixture inputs alone. The controllers used for these grouped runs rejected fixed address requests. A later controller ran the complete static-address case separately. [Private IPv4 results](provider/private-gateway-address.md) The mesh runner is identical across the two revisions. [Upstream static-address case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-static-addresses.go)

## Mesh

Both runs selected 12 features and reached the upstream case loop. The union enabled 16 of the 23 cases that require `Mesh`. Seven skipped because their requirements include a Gateway-profile HTTPRoute feature outside either mesh profile. This includes `MeshConsumerRoute`, which also requires `HTTPRouteResponseHeaderModification`. All nine Extended features declared by each mesh profile are Standard. The single-case `MeshFrontend` mode remains available for that case's separate feature requirements. [Mesh case sources](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh)

On each channel all 16 eligible bodies ran. The same five passed and the same 11 failed. The result record contains the upstream reports, each mesh case's result and feature requirements, and the aggregate count of skips across the full registry. [Machine-readable results](complete-local-profiles-results.json)

| Profile and tier | Standard passed / failed | Experimental passed / failed |
| --- | ---: | ---: |
| `MESH-HTTP` Core | 4 / 3 | 4 / 3 |
| `MESH-HTTP` Extended | 1 / 6 | 1 / 6 |
| `MESH-GRPC` Core | 1 / 2 | 1 / 2 |
| `MESH-GRPC` Extended | No matching bodies | No matching bodies |

The host redirect case received HTTP 200 where it expected 301 or 302. The HTTP weight case expected 70% of requests to reach `echo-v1` and last measured 49.8% on Standard and 48.2% on Experimental. The gRPC case measured 100% on both channels against the same 70% expectation.

The upstream reports count `MeshBasic` in each profile. Its result confirms ordinary Service connectivity. The stock Service can also return the expected backend several times in a row. `MeshHTTPRouteNamedRule` uses an upstream echo comparator that checks the responding backend and HTTP status. It leaves the expected request path unchecked. The linked receipt records wrong-backend replies that preceded passes. Together these are observations of the stock Service path. [Named rule case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/httproute-named-rule.go) · [Echo comparator](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/echo/pod.go#L130)

On each channel, the independent probe received 16 baseline HTTP replies, 12 HTTP replies under the active request-header rule, 12 gRPC replies under its active metadata rule and one direct HTTP reply from the `echo-v2` pod. All 41 requests succeeded. The HTTP and gRPC Service requests kept their supplied `X-Header-Set` values when the routes asked for `set-overwrites-values`. The HTTPRoute naming `Service/echo:80` had no parent statuses and the GRPCRoute naming `Service/echo:7070` had empty status. The ClusterIP Service and its two Ready EndpointSlice entries were byte-identical before and during the HTTPRoute case. [Machine-readable probes and diagnostics](complete-local-profiles-results.json)

Both environments ran the stock Service path. The three sampled mesh pods contained only the upstream echo container, with no init containers. Each pod reported the verified local image index for the pinned `echo-advanced` manifest. The recording adapter saw zero Gateway graph operations. These observations establish connectivity through Kubernetes Services and provide no evidence of Service-parent routing enforcement. [HTTPRoute parent selection](../../../internal/controller/httproute/httproute_parent.go#L54) · [Upstream mesh fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/mesh/manifests.yaml)

## Reproduce

Use a disposable Linux amd64 kind cluster with its upstream v1.7.0 Standard or Experimental CRD bundle and the fork's class-configuration CRD. The runs used one control plane and one worker at a time in a Fedora Cloud 45-1.4 KVM guest with four vCPUs and 4992 MiB RAM, with etcd on tmpfs. Go 1.27.4 built the test binaries. The nodes ran Kubernetes v1.36.4 with containerd 2.3.4 under kind v0.33.0 and rootful Podman 6.1.3. The exact bundle, node and binary hashes, live CRD inventories, and cleanup receipts are in the result record. [Upstream release assets](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0) · [Machine-readable results](complete-local-profiles-results.json)

Preload the official fixture tags on each node with these linux/amd64 digests. Preserve the tags and `IfNotPresent` pull policy in the embedded manifests.

| Image | Manifest digest |
| --- | --- |
| `registry.k8s.io/gateway-api/conformance/echo-basic:v0.1.0` | `sha256:640d843263062f4391ecd74effd68673a69800852eb4ff31b8452a3301eae83b` |
| `registry.k8s.io/gateway-api/conformance/echo-basic:v0.2.0` | `sha256:2e9146b0550f20c9082b1f8490729104ea0120bb0a41763bd02307db93028f20` |
| `registry.k8s.io/gateway-api/conformance/echo-advanced:v0.1.0` | `sha256:7b928723a4f261dcc94ef2cc036041d393c2ff4fe02623cc79d2f8691db995af` |

The [mesh image import instructions](provider/stock-mesh.md#reproduce) show how to import the OCI archive with its pinned manifest. Pod status may show the archive index as the image ID. Verify the manifest referenced by that index and the exact tag on each node.

Build from the repository root at the recorded revision. Set `channel` to `standard` or `experimental` and set `cluster` and `kubeconfig` to the corresponding disposable cluster's name and kubeconfig path. The kubeconfig needs the context `kind-$cluster` and access to create fixtures and exec in the pods. Start with the Gateway and mesh fixture namespaces and the class fixture absent. Run the suites sequentially and wait for the Gateway fixture cleanup before running the mesh study. Save the exit status and verbose output of each invocation.

```sh
CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o /tmp/complete-profiles.test ./internal/controller

GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_GATEWAY_PROFILE=all GOMAXPROCS=2 /tmp/complete-profiles.test \
  -test.v -test.run '^TestKindUpstreamGatewayProfile$' -test.timeout=120m

GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_MESH_STUDY=all GOMAXPROCS=2 /tmp/complete-profiles.test \
  -test.v -test.run '^TestKindStockMeshStudy$' -test.timeout=120m
```

While Gateway setup waits, read GatewayClass and Gateway status and list the pods in the three upstream namespaces. For the mesh probes, record the producer and consumer pod image IDs, the `echo` Service and its EndpointSlices, and status of the active header-modifier Routes. From the upstream `echo-v1` container, request `http://echo/` before the HTTPRoute case. During each header-modifier case, request `grpc://echo:7070` with `x-test-case:set` and `x-header-set:this-value-should-be-overwritten`, or `http://echo/set` with `Some-Other-Header:val` and `X-Header-Set:some-other-value`. Use the `echo-v2` pod's HTTP port 8080 for the direct control. Read the returned header and compare the Service and EndpointSlice snapshots before and during the HTTPRoute case. [Upstream HTTP fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/httproute-request-header-modifier.yaml) · [Upstream gRPC fixture](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/grpcroute-request-header-modifier.yaml)
