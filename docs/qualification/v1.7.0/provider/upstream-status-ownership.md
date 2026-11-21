# Gateway name length and HTTPRoute status ownership

The focused envtest ran the original Gateway API v1.7.0 case bodies and manifests on Kubernetes 1.36.2. Each run used its channel's release CRDs and admission policy, the upstream `same-namespace` Gateway and `infra-backend-v1` Service, and the existing recording provider. The case sources and manifests in the Go module match upstream revision `e26d183dbbc1bb782b78addbd26faba03955c152`. [Upstream conformance cases](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests) · [Run receipt](upstream-status-ownership-results.json)

| Original case | Standard | Experimental |
|---|---|---|
| `GatewayNameMaximumLength` | PASS | PASS |
| `HTTPRoutePreserveForeignStatus` | FAIL | FAIL |

The Gateway case observed the API server's default `Accepted` and `Programmed` conditions at generation 0 before the upstream check passed at generation 1. Its assertion covers generation convergence on a 253-character Gateway name. [Gateway case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-name-maximum-length.go)

The HTTPRoute case reached its initial parent acceptance check and found `Accepted=False/UnsupportedValue` at generation 1 with the message `exactly one Gateway parentRef is supported`. The fixture has one managed Gateway reference (`same-namespace`) and one reference to a deliberately absent Gateway (`unmanaged-gateway`). Parent discovery finds the managed Gateway, but the cached route decision also requires the entire `spec.parentRefs` list to contain one item. The test times out before seeding foreign status or bumping the Route generation. Preservation of the foreign entry, including its generation and transition time, remains unverified by this case. [HTTPRoute case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-preserve-foreign-status.go) · [Controller decision](../../../../internal/controller/httproute/httproute_controller.go)

These runs cover Kubernetes status with a recording provider. Network traffic and full conformance profiles were outside their scope.

## Reproduce locally

Use an isolated checkout of controller source `455798411c13fd42f5c1568357f2c35cec2b2169`. Set `KUBEBUILDER_ASSETS` to the directory with the Kubernetes 1.36.2 envtest binaries and `GATEWAY_API_SOURCE` to the v1.7.0 Gateway API source checkout. Apply the [three-line test overlay](upstream-status-ownership.patch), then run once per channel. The expected exit status on this source is 1 for each channel.

```sh
rtk git apply --unidiff-zero /path/to/upstream-status-ownership.patch
for channel in standard experimental; do
  rtk env USE_EXISTING_CLUSTER=false KUBECONFIG=/dev/null \
    KUBEBUILDER_ASSETS="$KUBEBUILDER_ASSETS" \
    GATEWAY_API_CHANNEL="$channel" GATEWAY_API_CRD_PATH="$GATEWAY_API_SOURCE/config/crd/$channel" \
    go test -json -tags=envtest -count=1 -timeout=5m \
    -run '^TestControllerEnvtest$/^(genuine_Gateway_API_release_bundle|upstream_Gateway_status_cases)$/^(GatewayNameMaximumLength|HTTPRoutePreserveForeignStatus)$' \
    ./internal/controller > "$channel.jsonl" 2>&1
done
```
