# Gateway gRPC and TLS profile attempts on kind

The controller source was `00eabae770f40014af8cb6cb53a225f1450ac52a`, built with Go 1.27.4 and the opt-in profile runner at `0b04173620e55c632742318cfde7f5ba4ca08a86`. The runner starts the GatewayClass, Gateway, HTTPRoute, TCPRoute and UDPRoute reconcilers against real Kubernetes. An in-memory adapter records the requested cloud graph and returns synthetic provider responses. The evidence covers Kubernetes reconciliation and upstream setup. [Profile runner](../../../internal/controller/kind_gateway_profile_test.go)

Each invocation passed one published profile to the v1.7.0 suite, with its original case registry and embedded manifests. Both modules were pinned to v1.7.0 from upstream commit `e26d183dbbc1bb782b78addbd26faba03955c152`. The runner used the complete `base/manifests.yaml` (SHA-256 `9f9ef20a1c81851e08380cfb87b7b611dc16390e1e4fe3be0b0a788115465352`) and the upstream 300-second namespace readiness timeout. [Upstream profile registry](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go) · [Shared manifest](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml) · [Timeout defaults](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/config/timeout.go)

## Shared setup

All four invocations reached the shared readiness check and exhausted its 300-second deadline. Ready counts are from the last Kubernetes API sample before the deadline.

| Profile | Channel | Shared setup | Ready upstream Pods | Case bodies entered | Upstream `ConformanceReport` |
|---|---|---|---|---|---|
| `GATEWAY-GRPC` | Standard | Timed out at HTTPS Gateway readiness | 23/23 | 0 | None |
| `GATEWAY-GRPC` | Experimental | Timed out at HTTPS Gateway readiness | 23/23 | 0 | None |
| `GATEWAY-TLS` | Standard | Timed out at HTTPS Gateway readiness | 23/23 | 0 | None |
| `GATEWAY-TLS` | Experimental | Timed out at HTTPS Gateway readiness | 23/23 | 0 | None |

The Ready pods comprised 16 in `gateway-conformance-infra`, five in `gateway-conformance-app-backend` and two in `gateway-conformance-web-backend` in each run. Setup stopped before the case loop, so there are no per-case outcomes. All four runs removed their three upstream namespaces, GatewayClass and class configuration with the controller running through finalizer cleanup.

The upstream readiness check requires all included Gateways to be accepted and programmed on the current generation. In the upstream infrastructure namespace, `same-namespace-with-https-listener` showed `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`, both with `observedGeneration: 1` matching `metadata.generation: 1`. The three other shared Gateways reported `Accepted=True` and `Programmed=True`. [Upstream readiness check](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/kubernetes/helpers.go#L272)

The blocking Gateway declared four `HTTPS:443` listeners. All four shared the upstream `tls-validity-checks-certificate` Secret, used TLS termination, and limited Route attachment to the same namespace. Each returned `Accepted=False/UnsupportedProtocol` and `Programmed=False/Invalid` on observed generation 1.

| Listener | Declared hostname |
|---|---|
| `https` | Unspecified |
| `https-with-hostname` | `second-example.org` |
| `https-with-wildcard-hostname` | `*.wildcard.org` |
| `https-with-hostname-matching-wildcard` | `fourth-example.wildcard.org` |

The local records retain the exact feature selections, setup result, status snapshot hashes and cleanup outcome for all four invocations. [Per-channel records](kind-grpc-tls-results.json) · [Exact runner revision](https://github.com/armeldemarsac92/gateway-api-openstack/tree/0b04173620e55c632742318cfde7f5ba4ca08a86)

## Feature selection

The Core selection is the same on both channels. The Extended selection combines the common set and the additions from the corresponding row. These are inputs to the upstream test suite. The sampled GatewayClasses had an empty `status.supportedFeatures`.

The common Extended features are `BackendTLSPolicy`, `BackendTLSPolicySANValidation`, `GatewayAddressEmpty`, `GatewayBackendClientCertificate`, `GatewayFrontendClientCertificateValidation`, `GatewayFrontendClientCertificateValidationInsecureFallback`, `GatewayHTTPListenerIsolation`, `GatewayHTTPSListenerDetectMisdirectedRequests`, `GatewayInfrastructure`, `GatewayPort8080`, `GatewayRouteHostnameIntersectionPrecedence`, `GatewayStaticAddresses` and `ListenerSet`.

| Profile | Channel | Core features | Extended additions |
|---|---|---|---|
| `GATEWAY-GRPC` | Standard | `Gateway`, `GRPCRoute`, `ReferenceGrant` | None |
| `GATEWAY-GRPC` | Experimental | `Gateway`, `GRPCRoute`, `ReferenceGrant` | `ClusterTrustBundle`, `GatewayAddressRoutability`, `GatewayAddressRoutabilityCluster`, `GatewayListenerFilters` |
| `GATEWAY-TLS` | Standard | `Gateway`, `ReferenceGrant`, `TLSRoute` | `TLSRouteModeMixed`, `TLSRouteModeTerminate` |
| `GATEWAY-TLS` | Experimental | `Gateway`, `ReferenceGrant`, `TLSRoute` | `ClusterTrustBundle`, `GatewayAddressRoutability`, `GatewayAddressRoutabilityCluster`, `GatewayListenerFilters`, `TLSRouteModeMixed`, `TLSRouteModeTerminate` |

The v1.7.0 `GATEWAY-GRPC` profile lists shared Gateway and BackendTLSPolicy features as Extended. Its published Extended set omits `GRPCRouteNamedRouteRule` and `GRPCRouteRequestHeaderModifier`, which remain visible in the [named feature matrix](matrix/features-and-cases.md).

## Reproduce

Use one disposable Linux amd64 kind cluster per channel, each with its official upstream v1.7.0 release bundle and the controller's class-configuration CRD. The runs used kind v0.33.0, rootful Podman 6.1.3 and containerd 2.3.4. Each cluster had a control-plane node and a worker from `kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`. The Standard and Experimental asset SHA-256 values were `4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1` and `e6d2d3407717f54b579b22ad4d164b6863895b3524f21250b0da0c1ff9a05d53`. The class-configuration CRD SHA-256 was `39aa7fa4a30f187d5eed492fe3af821dc23498ab49ec4a37a6d4152cf70b8f16`. [Official release assets](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

The upstream echo image tags were preloaded on both nodes as `registry.k8s.io/gateway-api/conformance/echo-basic:v0.1.0` and `registry.k8s.io/gateway-api/conformance/echo-basic:v0.2.0`, pinned to the linux/amd64 manifests `sha256:640d843263062f4391ecd74effd68673a69800852eb4ff31b8452a3301eae83b` and `sha256:2e9146b0550f20c9082b1f8490729104ea0120bb0a41763bd02307db93028f20`. The sampled Pod image IDs matched these digests. The embedded YAML keeps the upstream image references.

Build the runner from the repository root. Set `channel` to `standard` or `experimental` and set `cluster` and `kubeconfig` to the corresponding disposable cluster. The kubeconfig needs the context `kind-$cluster`. Start with the upstream namespaces and class fixture absent. Repeat the invocation with `profile=GATEWAY-TLS` once gRPC has returned and the fixtures are gone.

```bash
CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o /tmp/gateway-profile-kind.test ./internal/controller
profile=GATEWAY-GRPC
GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_GATEWAY_PROFILE="$profile" /tmp/gateway-profile-kind.test \
  -test.v -test.run '^TestKindUpstreamGatewayProfile$' -test.timeout=120m
```
