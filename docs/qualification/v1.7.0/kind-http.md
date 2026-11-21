# HTTP through a kind worker NodePort

HAProxy 3.0.19 forwarded HTTP requests to an upstream echo pod through a dedicated worker's NodePort on Kubernetes v1.36.4. The test constructed the Gateway graph and supplied the worker address and NodePort to the production Go adapter. Gophercloud sent its requests to an in-memory Octavia REST fixture. The stock Octavia 19.0.0 renderer read that inventory. The Kubernetes API supplied the Node, Service, EndpointSlice and pod. [Adapter and traffic assertions](../../../internal/cloud/openstack/graph/hibiscus_traffic_test.go) · [Upstream echo workload](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml)

## Observations

The Standard v1.7.0 bundle was installed in kind v0.33.0 on Fedora 45 with rootless Podman 6.1.3. The cluster had a Ready control-plane node and a separate Ready worker. The Service used `externalTrafficPolicy: Local`. Its Ready EndpointSlice named the same worker and pod that served the direct HTTP control.

| Observation | Result |
|---|---|
| Direct request to the worker, `10.89.0.3:31757/direct` | The response identified pod `hibiscus-worker-http/echo`, port `3000` and path `/direct`. |
| Request to the loopback VIP, `127.0.0.1:44073/api` | With hosts `API.Example.COM:1234` and `api.example.com`, both requests returned HTTP 200. The echo identified the same pod, method `GET`, path `/api` and the exact Host supplied by each client request. |
| New Go adapter against the existing inventory | It rediscovered the graph with zero writes. Both requests to the same VIP returned the same pod and Host again. |
| Unmatched host or path | Both returned 503. Gateway API requires 404. [HTTPRoute requirement](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L216-L217) |
| Disable the fixture load balancer and remove its graph | The VIP refused the connection after disable and after removal. The fixture retained its original Magnum API-server and OCCM load balancers. |
| Repeat with the expected pod changed to `wrong-echo` | The response still identified `echo`. The pod identity assertion failed on the first HTTP 200 as intended. |

The workload fixture created a Namespace, a Pod and a Service. The test supplied the Gateway and route graph directly to the adapter. Gateway and HTTPRoute reconciliation has separate envtest coverage. A controller Pod restart, a private Amphora VIP and the inventory of an actual Magnum cluster still need a cloud-backed run. Both complete GATEWAY-HTTP profiles remain unrun. The worker test's Service, kind cluster, HAProxy processes, disposable VM and temporary credentials were removed. [Channel and profile records](README.md#profile-records)

## Reproduce

Use Linux with Go 1.27.4, kind v0.33.0, kubectl and a client that can reach the kind nodes' private addresses. Install the pinned Octavia Python packages and set `HIBISCUS_OCTAVIA_PYTHON` and `HIBISCUS_HAPROXY` as in the [loopback pilot](cloud-contract.md#http-traffic-from-the-adapters-own-writes). Run these Bash commands in the repository root. They create a disposable cluster with a dedicated worker and install the Standard release bundle. For rootless Podman, export `KIND_EXPERIMENTAL_PROVIDER=podman` first. The observed Fedora run created kind in a delegated user scope with `systemd-run --user --scope --property=Delegate=yes`.

```bash
set -euo pipefail
: "${HIBISCUS_OCTAVIA_PYTHON:?set the path to the pinned Python interpreter}"
: "${HIBISCUS_HAPROXY:?set the path to HAProxy 3.0.19}"
banner=$("$HIBISCUS_HAPROXY" -v)
[[ $banner =~ ^HAProxy[[:space:]]version[[:space:]]3\.0\.19([[:space:]-]|$) ]]
scratch=$(mktemp -d)
name="hibiscus-http-$(basename "$scratch" | tr '[:upper:].' '[:lower:]-')"
export KUBECONFIG="$scratch/kubeconfig"
trap 'kind delete cluster --name "$name" --kubeconfig "$KUBECONFIG"' EXIT
kind create cluster --name "$name" --kubeconfig "$KUBECONFIG" \
  --image kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed \
  --config=- <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
YAML
k() { kubectl --kubeconfig "$KUBECONFIG" --context "kind-$name" "$@"; }
worker="$name-worker"
k wait --for=condition=Ready node/"$worker" --timeout=120s
k label node "$worker" gateway-api-openstack-test-worker=true
curl -fsSL https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml -o "$scratch/standard.yaml"
echo "4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1  $scratch/standard.yaml" | sha256sum -c -
k apply --server-side -f "$scratch/standard.yaml"
k apply -f docs/qualification/v1.7.0/provider/kind-worker-http.yaml
export HIBISCUS_KIND_NAMESPACE=hibiscus-worker-http HIBISCUS_KIND_POD=echo
k -n "$HIBISCUS_KIND_NAMESPACE" wait --for=condition=Ready pod/echo --timeout=180s
test "$(k -n "$HIBISCUS_KIND_NAMESPACE" get pod echo -o jsonpath='{.spec.nodeName}')" = "$worker"
worker_ip=$(k get node "$worker" -o 'jsonpath={.status.addresses[?(@.type=="InternalIP")].address}')
nodeport=$(k -n "$HIBISCUS_KIND_NAMESPACE" get service echo -o 'jsonpath={.spec.ports[0].nodePort}')
export HIBISCUS_KIND_NODEPORT="$worker_ip:$nodeport"
if [[ ${KIND_EXPERIMENTAL_PROVIDER:-} == podman ]]; then
  go test -c -o "$scratch/worker-http.test" ./internal/cloud/openstack/graph
  (cd internal/cloud/openstack/graph && podman unshare --rootless-netns \
    "$scratch/worker-http.test" -test.v -test.run '^TestHibiscusKindWorkerHTTP$')
else
  go test -count=1 -v -run '^TestHibiscusKindWorkerHTTP$' ./internal/cloud/openstack/graph
fi
```

For rootless Podman, the final command enters its network namespace so HAProxy can reach the worker directly. The fixture's pod selector and Service keep the NodePort traffic on the dedicated worker. [Pod and Service](provider/kind-worker-http.yaml)
