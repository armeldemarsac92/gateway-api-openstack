# TCPRoute status on a disposable Kubernetes cluster

Three Gateway API v1.7.0 cases passed on Kubernetes v1.36.4 with the Standard CRDs and the controller source at `1add54b366d431d379d9a59564105b4618d9fa58`. The production controller watches ran against the real API server. The cloud provider recorded the requested graph. All 23 pods in the three upstream namespaces were Ready. [Go test](../../../internal/controller/kind_tcp_status_test.go) · [Tagged upstream cases](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests)

| Unmodified upstream case | Observed result |
|---|---|
| `TCPRouteInvalidNonTCPListener` | The route reported `Accepted=False/NotAllowedByListeners` and the listener had zero attached routes. |
| `TCPRouteInvalidCrossNamespaceBackendRef` | The route reported `ResolvedRefs=False/RefNotPermitted`. |
| `TCPRouteInvalidBackendRefNonexistent` | The route reported `ResolvedRefs=False/BackendNotFound`. |

The first run used the complete upstream base on a development snapshot preceding the final TCP commit. Setup stopped at `same-namespace-with-https-listener`, whose HTTPS listeners reported `UnsupportedProtocol`. None of the three test bodies ran in that attempt. On the final TCP commit, the scoped run omitted that single Gateway from base setup. It kept the other 34 base documents, the original workloads, their readiness checks, all three case manifests and all three case bodies. This record covers the individual cases on Standard. A full GATEWAY-TCP suite on either Hibiscus channel is still outstanding.

An independent client used the upstream `tcp.EchoSendOnce` handshake against a temporary NodePort selecting the original `tcp-backend` Deployment. The response named a Ready pod from that Deployment. The client reached the real worker NodePort directly. A real Amphora VIP and traffic through that VIP require separate qualification. The temporary Service, kind node and disposable VM were removed after the run. [Upstream handshake](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/tcp/tcp.go)

## Reproduce the scoped check

Use a disposable Linux host with kind v0.33.0, kubectl, Python 3 and the repository's Go toolchain. The observed run used Fedora 45 and rootless Podman 6.1.3 in a VM. The Kubernetes node image is pinned by digest. The upstream Standard asset has SHA-256 `4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1`. [kind release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0) · [Official Gateway API bundle](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

Run these commands from the repository root in Bash. The explicit kubeconfig belongs to the cluster created by these commands.

```bash
set -euo pipefail
scratch=$(mktemp -d)
export KIND_CLUSTER_NAME="hibiscus-tcp-$(basename "$scratch" | tr '[:upper:].' '[:lower:]-')"
export KIND_EXPERIMENTAL_PROVIDER=podman
export HIBISCUS_TEST_KUBECONFIG="$scratch/kubeconfig"
kind create cluster --name "$KIND_CLUSTER_NAME" --kubeconfig "$HIBISCUS_TEST_KUBECONFIG" \
  --image kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed
trap 'kind delete cluster --name "$KIND_CLUSTER_NAME" --kubeconfig "$HIBISCUS_TEST_KUBECONFIG"' EXIT
curl -fsSL https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml -o "$scratch/standard.yaml"
echo "4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1  $scratch/standard.yaml" | sha256sum -c -
kubectl --kubeconfig "$HIBISCUS_TEST_KUBECONFIG" apply --server-side -f "$scratch/standard.yaml" \
  -f config/crd/gateway-api-openstack.github.io_openstackgatewayclassconfigs.yaml
kubectl --kubeconfig "$HIBISCUS_TEST_KUBECONFIG" wait --for=condition=Established --timeout=60s \
  crd/tcproutes.gateway.networking.k8s.io crd/openstackgatewayclassconfigs.gateway-api-openstack.github.io

base=$(go list -m -f '{{.Dir}}' sigs.k8s.io/gateway-api/conformance)/base/manifests.yaml
python3 - "$base" "$scratch/tcp-status-base.yaml" <<'PY'
from pathlib import Path
import sys
source, destination = map(Path, sys.argv[1:])
separator = b"\n---\n"
documents = source.read_bytes().split(separator)
selected = [doc for doc in documents if not (
    b"\nkind: Gateway\n" in doc and b"\n  name: same-namespace-with-https-listener\n" in doc)]
assert len(documents) - len(selected) == 1
destination.write_bytes(separator.join(selected))
PY
export HIBISCUS_STATUS_BASE_DIR="$scratch"
go test -tags=envtest,kindtest -count=1 -v -timeout=5m -run '^TestKindUpstreamTCPStatus$' ./internal/controller
```

To exercise the complete upstream base setup, use a fresh cluster and omit `HIBISCUS_STATUS_BASE_DIR`. The test then loads the complete base from the tagged conformance module.
