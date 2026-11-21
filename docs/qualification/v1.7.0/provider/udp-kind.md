# UDPRoute status on disposable Kubernetes

The controller ran against separate single-node Kubernetes v1.36.4 clusters with the Gateway API v1.7.0 Standard and Experimental bundles. Both clusters ran in a disposable Fedora 45 VM with kind v0.33.0 and rootless Podman 6.1.3. The suite exercised the controller watches with a recording provider. It used the published upstream case bodies, case manifests and workload readiness checks. Base setup omitted the unrelated `same-namespace-with-https-listener` Gateway, which Hibiscus cannot program. [Go test](../../../../internal/controller/kind_tcp_status_test.go) · [Gateway API v1.7.0 cases](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests)

| Check | Result on Standard and Experimental |
|---|---|
| Upstream `UDPRouteInvalidCrossNamespaceBackendRef` | The route stayed accepted and reported `ResolvedRefs=False/RefNotPermitted` for a cross-namespace Service without a ReferenceGrant. |
| Upstream `UDPRouteInvalidBackendRefNonexistent` | The route reported `ResolvedRefs=False/BackendNotFound` when its Service was missing. |
| Local `UDPRouteRejectedByHTTPListener` | The route reported `Accepted=False/NotAllowedByListeners` and the Gateway reported zero attached routes. |

The upstream `UDPRouteNotAllowedByListeners` case requires `SupportTLSRoute` and a programmed TLS-only Gateway before its assertions run. The local check targets a supported HTTP listener. The other six upstream UDPRoute cases send packets to a Gateway address. [Upstream TLS case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-not-allowed-by-listeners.go)

On each cluster, the upstream UDP echo client received a datagram from the Ready `udp-echo` pod through a temporary NodePort on the kind control-plane node. This verified the Kubernetes backend path directly. A separate Standard rehearsal sent a VIP datagram through a dedicated worker. A complete GATEWAY-UDP profile and a deployed Amphora remain open. [Dedicated worker traffic](udp-worker.md) [Upstream UDP echo client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/udp/udp.go)

## Rerun

Use the [kind TCP setup recipe](../kind-tcp.md) to create a fresh cluster and generate `tcp-status-base.yaml` with the single HTTPS Gateway omitted. Repeat the run on a fresh cluster for each official bundle. Install the chosen bundle and the controller CRD. Wait for `udproutes.gateway.networking.k8s.io` and `openstackgatewayclassconfigs.gateway-api-openstack.github.io` to become established. The Experimental asset SHA-256 is `e6d2d3407717f54b579b22ad4d164b6863895b3524f21250b0da0c1ff9a05d53`. The node image digest and Standard asset hash are in the TCP recipe. On a rootless Podman guest without lingering, keep the session that creates the cluster open through the test and deletion. [Official Gateway API bundles](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

With `KIND_CLUSTER_NAME`, `HIBISCUS_TEST_KUBECONFIG` and `HIBISCUS_STATUS_BASE_DIR` set for that cluster and base fixture, run this from the repository root. Set `GATEWAY_API_CHANNEL=experimental` for the Experimental cluster.

```bash
GATEWAY_API_CHANNEL=standard go test -tags=envtest,kindtest -count=1 -v -timeout=5m \
  -run '^TestKindUpstreamUDPStatus$' ./internal/controller
```
