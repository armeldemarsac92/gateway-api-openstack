# Istio ambient waypoint inputs for the mesh study

Istio 1.31.0 is the candidate for the opt-in Service waypoint trial. The annotated release tag resolves to `eed52ebb66ade45214088877b0f82855d954060d`. Its published Experimental report covers Gateway API v1.6.0. This study checks source revisions, registry metadata and rendered installation inputs. Both v1.7.0 mesh profiles still need an enrolled workload and observed traffic. [first opt-in mesh experiment](provider-feasibility.md#first-opt-in-mesh-experiment) · [Release report](../reference-implementations.md#service-parent-traffic) · [Tagged source](https://github.com/istio/istio/tree/eed52ebb66ade45214088877b0f82855d954060d)

## Release inputs

Istio distributes 1.31.0 images on Docker Hub and Helm charts at `blob.istio.io/istio-release/charts`. The charts select `docker.io/istio` as their image hub. The ambient overlay enables HBONE and ambient pilot and CNI settings. It sets the global image variant to distroless. The ztunnel chart also has a component-specific variant. The IstioOperator profile enables CNI and ztunnel and disables the default ingress Gateway. A separate Gateway resource selects the waypoint proxy. [1.31.0 announcement](https://istio.io/latest/news/releases/1.31.x/announcing-1.31/) · [ambient Helm profile](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/manifests/helm-profiles/ambient.yaml) · [Operator profile](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/manifests/profiles/ambient.yaml) · [ztunnel image template](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/manifests/charts/ztunnel/templates/daemonset.yaml) · [waypoint injection template](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/manifests/charts/istio-control/istio-discovery/files/waypoint.yaml)

Anonymous Docker Hub manifest requests on 2026-11-21 yielded the following index and linux/amd64 manifest digests. The first digest hashes the retrieved index. The second identifies its linux/amd64 descriptor. The table includes the plain tag and the distroless variant so an operator can compare a rendered image with the recorded inputs.

| Image under `docker.io/istio/` | Tag | Manifest list SHA-256 | linux/amd64 manifest SHA-256 |
|---|---|---|---|
| pilot | 1.31.0 | `1192eb50869b4024d7fbebded9c58daf351d3f9bf675bb89f7e9016598653716` | `c8affda58c9bdc4f13f5c6a70eeec73a471125a790bee7d3579fb073b7f6cf63` |
| pilot | 1.31.0-distroless | `78b5d404521ac0337e556f6cb8f9efa8d401abe8b6e0b74930020a56eea13b8e` | `17b581d9d75a1ab319d2816230626f06d65b48e3e798f98e3092a031843b3fe0` |
| install-cni | 1.31.0 | `8cef43ba08ae1af846d0e474591f625cd2dd6b2c0df0efcb17faef0d978ef246` | `f7833b70458104aee7374b958537e78964e0bbe3dc313f65393e07381bf0ee09` |
| install-cni | 1.31.0-distroless | `d274207c7572adaf81289fd4eb4b430f9f38eba3b3603be3af17ecefbd46ccdd` | `fe7081ed2605166fef573a7987f183e91fd45c2a302f90998c879ca20112426e` |
| ztunnel | 1.31.0 | `c2d2eb58c1b64c9069f460798b3c6528f693a3b6f54f2ae09093555b36449885` | `4cfb5884080d1cd378f5fbd5afc5bea999c70b3b756e856d69cdbaffbe0cdb28` |
| ztunnel | 1.31.0-distroless | `36632f26754ef3828f12c24d7d5f1c1573b1cfaf96ce4485539ceaaab793568b` | `efa1115b58acd969bf876dfcee27751714e3ae317b2518e1a1d61f3ef037587e` |
| proxyv2 | 1.31.0 | `e3b973cce2442c2883188d8cf839dff8a21075fab0e00e5079df6ef28c9caf17` | `aef6565b50449e5c30b69c47574fbed14d85c1d1394c5c1150b6fd51ae5e91a3` |
| proxyv2 | 1.31.0-distroless | `beb916c737348115782a976cba4a08b58a7c12cadb981735d18c67c3d3021353` | `b153f8891c4eab1ef2a58d291e1e9ca00794561833bda82e3e26e26bf59ddebb` |

The registry checks used the Docker Registry v2 manifest API and anonymous repository-scoped access. The offline chart render below identifies the variants selected by the candidate configuration.

## Gateway API versions and Service traffic

Istio 1.31.0 pins the Gateway API and conformance modules at v1.6.0 and the inference extension at v1.5.0. Its release table lists Gateway API v1.6.0 as the build-and-test version and Kubernetes 1.32–1.36 as supported. Istio separately checks minimum installed CRD bundle versions for GRPCRoute (1.1.0), BackendTLSPolicy (1.4.0), TLSRoute (1.5.0) and TCPRoute (1.6.0). Compatibility with the v1.7.0 channel bundles and the deployed CNI needs a workload trial. [go.mod](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/go.mod) · [release table](https://istio.io/latest/docs/releases/supported-releases/) · [minimum CRD bundle checks](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pkg/config/schema/gatewayapi/minimumversions.go)

A producer Route names a Service in its own namespace through `parentRefs`. Clients use that Service's normal address. Ambient enrollment selects workloads. The destination's `istio.io/use-waypoint` label selects a waypoint of class `istio-waypoint` using HBONE on port 15008. A Service-level waypoint label takes precedence over the namespace label. A waypoint in another namespace also needs the waypoint namespace reference and the Gateway's allowed-routes permission. The experiment needs neighboring Service and unenrolled-client controls to measure its actual scope. [waypoint documentation](https://istio.io/latest/docs/ambient/usage/waypoint/)

Istio's converter checks that the Service exists, looks up its Service or namespace waypoint label and can report a waypoint-specific error. It compiles mesh and Gateway routes separately. Collect the actual Service parent's controller name, generation, Accepted, ResolvedRefs and any waypoint condition alongside traffic from the enrolled clients. The implementation uses `istio.io/mesh-controller` as its waypoint Gateway controller name. [pinned converter](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/config/kube/gateway/conversion.go#L624-L662) · [Route compilation](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pilot/pkg/config/kube/gateway/route_collections.go#L643-L683) · [Controller constant](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pkg/config/constants/constants.go#L171-L175)

## Open v1.7 contracts

The XMesh CRD appears in Istio's integration test fixture. A source search found no registration, reconciler or status implementation for XMesh in this revision. A v1.7.0 trial must establish its ownership and behavior. [Fixture](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/tests/integration/pilot/testdata/gateway-api-crd.yaml) · [generated resource inventory](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pkg/config/schema/gvr/resources.gen.go)

Istio's ambient conformance runner explicitly removes `MeshConsumerRouteFeature` and selects MESH-HTTP with listed skips. MESH-GRPC, consumer-namespace isolation, direct ClusterIP traffic, route withdrawal and XMesh remain open for this candidate. [ambient conformance test](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/tests/integration/ambient/gateway_conformance_test.go#L95-L125)

## Offline release-chart render

Helm `v3.19.4+g7cfb6e4` rendered the three official release archives locally with `manifests/helm-profiles/ambient.yaml` from the pinned Istio source. The overlay's SHA-256 is `ca8dad17409b92406cf06764f109e68cf3038e79c1fc64ff5a2e11562b8f5870`. This was an offline render. [official 1.31.0 chart repository](https://blob.istio.io/istio-release/charts/)

| Chart archive | SHA-256 |
|---|---|
| `istiod-1.31.0.tgz` | `df73f8fba27f2ae51c90f9483c5298e164b5241fcd349a04375df32d7f9fe2e7` |
| `cni-1.31.0.tgz` | `1530c595ee1861aab69b4df300cb301b9b6fd52cf0b7d09f1ada5edfa50b9865` |
| `ztunnel-1.31.0.tgz` | `f24c1bccc0a92c2ce11bf95c1cf6423834c007c94057a8c7e69ac25f7ae7c8cc` |

With namespace `istio-system` and the upstream ambient overlay, the istiod Deployment selected `docker.io/istio/pilot:1.31.0-distroless`. The istio-cni-node and ztunnel DaemonSets selected `docker.io/istio/install-cni:1.31.0-distroless` and `docker.io/istio/ztunnel:1.31.0-distroless`. The distroless rows above give the index and linux/amd64 digest for each tag.

The rendered injection ConfigMap sets the hub to `docker.io/istio`, tag to `1.31.0`, variant to `distroless` and proxy image to `proxyv2`. Reading the waypoint template and Istio's image-selection code yields the expected waypoint image `docker.io/istio/proxyv2:1.31.0-distroless` for those values. An explicit image annotation or image-type override can change the result. An actual Pod image ID remains to be observed. [waypoint template](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/manifests/charts/istio-control/istio-discovery/files/waypoint.yaml#L103-L107) · [image construction code](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/pkg/kube/inject/inject.go#L317-L366)

To repeat the render, copy the ambient overlay from the pinned source checkout to an empty directory as `ambient.yaml` and run these commands there with Helm 3.19.4. Compare the four checksums with the values above.

```sh
rtk curl -fsSLO https://blob.istio.io/istio-release/charts/istiod-1.31.0.tgz
rtk curl -fsSLO https://blob.istio.io/istio-release/charts/cni-1.31.0.tgz
rtk curl -fsSLO https://blob.istio.io/istio-release/charts/ztunnel-1.31.0.tgz
rtk sha256sum istiod-1.31.0.tgz cni-1.31.0.tgz ztunnel-1.31.0.tgz ambient.yaml
rtk helm template istiod ./istiod-1.31.0.tgz --namespace istio-system -f ambient.yaml
rtk helm template cni ./cni-1.31.0.tgz --namespace istio-system -f ambient.yaml
rtk helm template ztunnel ./ztunnel-1.31.0.tgz --namespace istio-system -f ambient.yaml
```
